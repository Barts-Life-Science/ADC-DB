# Databricks notebook source
dbutils.widgets.text("run_mode", "incremental")
dbutils.widgets.text("target_table", "4_prod.bronze.mill_form_activity")
dbutils.widgets.text("control_schema", "6_mgmt.bronze")   # dev override: 8_dev.powerforms
dbutils.widgets.text("backfill_floor", "2026-06-11 03:07:16.503")
dbutils.widgets.text("n_batches", "8")
dbutils.widgets.text("canary_limit", "0")
dbutils.widgets.text("canary_replay_run_id", "")
dbutils.widgets.text("allow_mass_tombstone", "false")

# COMMAND ----------




RUN_MODE = dbutils.widgets.get("run_mode")
TARGET = dbutils.widgets.get("target_table")
CTRL = dbutils.widgets.get("control_schema")
RUNS = f"{CTRL}.powerforms_pipeline_runs"
STATE = f"{CTRL}.powerforms_pipeline_state"
CANDS = f"{CTRL}.powerforms_pipeline_candidates"
STAGE = f"{CTRL}.powerforms_stage"
N_BATCH = int(dbutils.widgets.get("n_batches"))
CANARY = int(dbutils.widgets.get("canary_limit"))
CANARY_REPLAY_RUN_ID = dbutils.widgets.get("canary_replay_run_id").strip()
ALLOW_MASS_TOMBSTONE = dbutils.widgets.get("allow_mass_tombstone").lower() == "true"

RESPONSE_SOURCES = [
    "4_prod.raw.mill_clinical_event",
    "4_prod.raw.mill_dcp_forms_activity",
    "4_prod.raw.mill_dcp_forms_activity_comp",
    "4_prod.raw.mill_encounter",
    "4_prod.raw.mill_ce_date_result",
    "4_prod.raw.mill_ce_string_result",
    "4_prod.raw.mill_ce_coded_result",
]
LABEL_SOURCES = [
    "4_prod.raw.mill_dcp_forms_ref",
    "4_prod.raw.mill_dcp_forms_def",
    "4_prod.raw.mill_dcp_section_ref",
    "4_prod.raw.mill_dcp_input_ref",
    "4_prod.raw.mill_name_value_prefs",
    "4_prod.raw.mill_discrete_task_assay",
    "3_lookup.mill.mill_code_value",
]
ALL_SOURCES = RESPONSE_SOURCES + LABEL_SOURCES

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat_ws, trim, regexp_replace, row_number, desc, coalesce, greatest
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, LongType, IntegerType, TimestampType, DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

def _mill_code_lookup():
    """Mill code_value -> description lookup for the 11 RDE functions below.
    """
    def _clean(c):
        cleaned = trim(regexp_replace(col(c), "[\\x00\\n\\r\\t\\u00A0]", " "))
        return when(cleaned == "", None).otherwise(cleaned)
    cv = spark.table("3_lookup.mill.mill_code_value").alias("CV")
    return cv.select(
        col("CV.CODE_VALUE").cast(LongType()).cast(StringType()).alias("CODE_VALUE_CD"),
        _clean("CV.DESCRIPTION").alias("CODE_DESC_TXT"),
        _clean("CV.DISPLAY").alias("CODE_DISP_TXT"),
        col("CV.ADC_UPDT").alias("ADC_UPDT"),
    )

def _cv_display_asof(left_df, code_col, asof_col, out_col="CV_DISPLAY"):
    """
    """
    cv = spark.table("3_lookup.mill.mill_code_value").alias("CVD")
    asof = coalesce(col(asof_col), F.current_timestamp())
    return (
        left_df.join(
            cv,
            (col(code_col).cast(LongType()) == col("CVD.CODE_VALUE").cast(LongType())) &
            (col("CVD.ACTIVE_IND") == 1) &
            (col("CVD.BEGIN_EFFECTIVE_DT_TM") <= asof) &
            (col("CVD.END_EFFECTIVE_DT_TM") > asof),
            "left",
        )
        .withColumn(out_col,
                    when(trim(regexp_replace(col("CVD.DISPLAY"), "[\\x00\\n\\r\\t\\u00A0]", " ")) == "", None)
                    .otherwise(trim(regexp_replace(col("CVD.DISPLAY"), "[\\x00\\n\\r\\t\\u00A0]", " "))))
        .drop(col("CVD.CODE_VALUE"), col("CVD.DISPLAY"), col("CVD.ACTIVE_IND"),
              col("CVD.BEGIN_EFFECTIVE_DT_TM"), col("CVD.END_EFFECTIVE_DT_TM"))
    )



def _mill_powerform_element_ref():
    """PowerForms element/form/section/grid metadata, derived directly from Millennium.
    """
    # numeric key segment -> integer-like string ("1252", never "1252.0")
    def _k(c):
        return col(c).cast(LongType()).cast(StringType())
    # CONCAT-faithful join of key parts: explicit '~' separators, NULL -> "" (kept).
    def _key(*parts):
        elems = []
        for i, p in enumerate(parts):
            if i:
                elems.append(lit("~"))
            elems.append(coalesce(p, lit("")))
        return F.concat(*elems)

    base = (
        spark.table("4_prod.raw.mill_dcp_forms_ref").alias("FREF")
        .join(spark.table("4_prod.raw.mill_dcp_forms_def").alias("FDEF"),
              col("FREF.DCP_FORM_INSTANCE_ID") == col("FDEF.DCP_FORM_INSTANCE_ID"))
        .join(spark.table("4_prod.raw.mill_dcp_section_ref").alias("SREF"),
              (col("FDEF.DCP_SECTION_REF_ID") == col("SREF.DCP_SECTION_REF_ID")) &
              (col("SREF.DCP_SECTION_INSTANCE_ID") > 0))
        .join(spark.table("4_prod.raw.mill_dcp_input_ref").alias("DIREF"),
              (col("SREF.DCP_SECTION_REF_ID") == col("DIREF.DCP_SECTION_REF_ID")) &
              (col("SREF.DCP_SECTION_INSTANCE_ID") == col("DIREF.DCP_SECTION_INSTANCE_ID")) &
              (col("DIREF.DCP_SECTION_INSTANCE_ID") > 0))
        .select(
            col("FREF.DCP_FORMS_REF_ID").alias("FORM_REF_ID_NUM"),
            col("FREF.DESCRIPTION").alias("FORM_DESC_TXT"),
            col("SREF.DCP_SECTION_REF_ID").alias("SECTION_REF_ID_NUM"),
            col("SREF.DESCRIPTION").alias("SECTION_DESC_TXT"),
            col("FREF.DCP_FORM_INSTANCE_ID").alias("FORM_INSTANCE_ID_NUM"),
            col("SREF.DCP_SECTION_INSTANCE_ID").alias("DCP_SECTION_INSTANCE_ID"),
            col("DIREF.DCP_INPUT_REF_ID").alias("DCP_INPUT_REF_ID"),
            col("DIREF.INPUT_TYPE").alias("INPUT_TYPE"),
            (when((col("FREF.ACTIVE_IND") == 1) & (col("SREF.ACTIVE_IND") == 1), 1)
             .otherwise(0)).alias("ACTIVE_IND"),
            col("FREF.ADC_UPDT").alias("FREF_ADC"),
            col("SREF.ADC_UPDT").alias("SREF_ADC"),
        ).alias("B")
    )

    # Common projected/carried base columns shared by all branches.
    def _common():
        return [
            col("B.ACTIVE_IND").cast(IntegerType()).alias("ACTIVE_IND"),
            _k("B.FORM_REF_ID_NUM").alias("FORM_REF_ID"),
            col("B.FORM_DESC_TXT").alias("FORM_DESC_TXT"),
            _k("B.SECTION_REF_ID_NUM").alias("SECTION_REF_ID"),
            col("B.SECTION_DESC_TXT").alias("SECTION_DESC_TXT"),
            _k("B.FORM_INSTANCE_ID_NUM").alias("FORM_INSTANCE_ID"),
            col("B.INPUT_TYPE").cast(IntegerType()).alias("INPUT_TYPE_FLG"),
        ]

    nvp = "4_prod.raw.mill_name_value_prefs"
    dta = "4_prod.raw.mill_discrete_task_assay"


    # ---- BRANCH 1: NON-GRID (INPUT_TYPE NOT IN 1,5,14,15,17,19,21; type 2 special) ----
    b1_src = (
        base.filter(~col("B.INPUT_TYPE").isin(1, 5, 14, 15, 17, 19, 21))
        .join(spark.table(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME").like("discrete_task_assay%")) &
              (col("NVP.MERGE_ID") > 0))
        .join(spark.table(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"))
    )
    _w1 = Window.partitionBy("DOC_INPUT_KEY").orderBy(col("DTA.TASK_ASSAY_CD").asc_nulls_last())
    # type 2 -> 4 parts; else 3 parts with 'd'+parent on EVENT_CD=0.
    _b1_key = when(
        col("B.INPUT_TYPE") == 2,
        _key(_k("B.FORM_INSTANCE_ID_NUM"), _k("B.DCP_SECTION_INSTANCE_ID"),
             _k("NVP.PARENT_ENTITY_ID"), _k("DTA.EVENT_CD")),
    ).otherwise(
        _key(_k("B.FORM_INSTANCE_ID_NUM"), _k("B.DCP_SECTION_INSTANCE_ID"),
             when(col("DTA.EVENT_CD") == 0,
                  F.concat(lit("d"), _k("NVP.PARENT_ENTITY_ID")))
             .otherwise(_k("DTA.EVENT_CD")))
    )
    b1 = (
        b1_src
        .withColumn("DOC_INPUT_KEY", _b1_key)
        .withColumn("_RNK", row_number().over(_w1)).filter(col("_RNK") == 1)
        .select(
            col("DOC_INPUT_KEY"),
            *_common(),
            _k("NVP.MERGE_ID").alias("TASK_ASSAY_ID"),
            _k("B.DCP_INPUT_REF_ID").alias("DCP_INPUT_REF_ID"),
            _k("B.DCP_SECTION_INSTANCE_ID").alias("DCP_SECTION_INSTANCE_ID"),
            _k("DTA.EVENT_CD").alias("ELEMENT_EVENT_CD"),
            col("DTA.DESCRIPTION").alias("ELEMENT_DESC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_MNEMONIC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            lit(None).cast("string").alias("GRID_NAME_TXT"),   # non-grid branch: no grid name or grid code
            lit(None).cast("string").alias("GRID_NAME_CD"),    # non-grid branch: no grid name or grid code
            lit(None).cast("string").alias("GRID_COLUMN_DESC_TXT"),
            lit(None).cast("string").alias("GRID_COLUMN_MNEMONIC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_DESC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_MNEMONIC_TXT"),
            lit("0").alias("GRID_EVENT_CD"),  # non-grid branch has no grid event; proc emits literal "0"
            greatest(col("B.FREF_ADC"), col("B.SREF_ADC"), col("DTA.ADC_UPDT")).alias("ADC_UPDT"),
        )
    )

    # ---- BRANCH 2: DISCRETE GRID (INPUT_TYPE = 14) ----
    b2 = (
        base.filter(col("B.INPUT_TYPE") == 14)
        .join(spark.table(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay"))
        .join(spark.table(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(spark.table(nvp).alias("NVP2"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP2.PARENT_ENTITY_ID")) &
              (col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP2.PVC_NAME") == "grid_event_cd"))
        .select(
            _key(_k("B.FORM_INSTANCE_ID_NUM"), _k("B.DCP_SECTION_INSTANCE_ID"),
                 _k("B.DCP_INPUT_REF_ID"), _k("DTA.EVENT_CD")).alias("DOC_INPUT_KEY"),
            *_common(),
            _k("NVP.MERGE_ID").alias("TASK_ASSAY_ID"),
            _k("B.DCP_INPUT_REF_ID").alias("DCP_INPUT_REF_ID"),
            _k("B.DCP_SECTION_INSTANCE_ID").alias("DCP_SECTION_INSTANCE_ID"),
            _k("DTA.EVENT_CD").alias("ELEMENT_EVENT_CD"),
            col("DTA.DESCRIPTION").alias("ELEMENT_DESC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_MNEMONIC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            # GRID_NAME resolved point-in-time as-of PERFORMED_DT_TM in L6; carry the code here.
            lit(None).cast("string").alias("GRID_NAME_TXT"),
            _k("NVP2.MERGE_ID").alias("GRID_NAME_CD"),
            col("DTA.DESCRIPTION").alias("GRID_COLUMN_DESC_TXT"),
            col("DTA.MNEMONIC").alias("GRID_COLUMN_MNEMONIC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_DESC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_MNEMONIC_TXT"),
            _k("NVP2.MERGE_ID").alias("GRID_EVENT_CD"),
            greatest(col("B.FREF_ADC"), col("B.SREF_ADC"),
                     col("DTA.ADC_UPDT")).alias("ADC_UPDT"),
        )
    )

    # ---- BRANCH 3: POWER GRID (INPUT_TYPE = 17) ----
    b3 = (
        base.filter(col("B.INPUT_TYPE") == 17)
        .join(spark.table(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay"))
        .join(spark.table(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(spark.table(nvp).alias("NVP2"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP2.PARENT_ENTITY_ID")) &
              (col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP2.PVC_NAME") == "grid_event_cd"))
        .join(spark.table(nvp).alias("NVP3"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP3.PARENT_ENTITY_ID")) &
              (col("NVP3.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP3.PVC_NAME") == "row_event_cd"))
        .select(
            _key(_k("B.FORM_INSTANCE_ID_NUM"), _k("B.DCP_SECTION_INSTANCE_ID"),
                 _k("NVP2.MERGE_ID"), _k("NVP3.MERGE_ID"), _k("NVP.MERGE_ID")).alias("DOC_INPUT_KEY"),
            *_common(),
            _k("NVP.MERGE_ID").alias("TASK_ASSAY_ID"),
            _k("B.DCP_INPUT_REF_ID").alias("DCP_INPUT_REF_ID"),
            _k("B.DCP_SECTION_INSTANCE_ID").alias("DCP_SECTION_INSTANCE_ID"),
            _k("DTA.EVENT_CD").alias("ELEMENT_EVENT_CD"),
            col("DTA.DESCRIPTION").alias("ELEMENT_DESC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_MNEMONIC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            # GRID_NAME resolved point-in-time as-of PERFORMED_DT_TM in L6; carry the code here.
            lit(None).cast("string").alias("GRID_NAME_TXT"),
            _k("NVP2.MERGE_ID").alias("GRID_NAME_CD"),
            col("DTA.DESCRIPTION").alias("GRID_COLUMN_DESC_TXT"),
            col("DTA.MNEMONIC").alias("GRID_COLUMN_MNEMONIC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_DESC_TXT"),
            lit(None).cast("string").alias("GRID_ROW_MNEMONIC_TXT"),
            _k("NVP2.MERGE_ID").alias("GRID_EVENT_CD"),
            greatest(col("B.FREF_ADC"), col("B.SREF_ADC"),
                     col("DTA.ADC_UPDT")).alias("ADC_UPDT"),
        )
    )

    # ---- BRANCH 4: ULTRA GRID (INPUT_TYPE = 19; column/row DTA roles swap) ----
    #   NVP  = discrete_task_assay2  (column level) -> DTA  (column DTA, supplies EVENT_CD)
    #   NVP2 = discrete_task_assay   (row level)    -> DTA2 (row DTA)
    #   NVP3 = grid_event_cd         (grid name)    -> CV joins NVP3 here (not NVP2)
    b4 = (
        base.filter(col("B.INPUT_TYPE") == 19)
        .join(spark.table(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay2"))
        .join(spark.table(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(spark.table(nvp).alias("NVP2"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP2.PARENT_ENTITY_ID")) &
              (col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP2.PVC_NAME") == "discrete_task_assay") & (col("NVP2.MERGE_ID") > 0))
        .join(spark.table(dta).alias("DTA2"), col("NVP2.MERGE_ID") == col("DTA2.TASK_ASSAY_CD"), "left")
        .join(spark.table(nvp).alias("NVP3"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP3.PARENT_ENTITY_ID")) &
              (col("NVP3.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP3.PVC_NAME") == "grid_event_cd") & (col("NVP3.MERGE_ID") > 0))
        .select(
            _key(_k("B.FORM_INSTANCE_ID_NUM"), _k("B.DCP_SECTION_INSTANCE_ID"),
                 _k("NVP3.MERGE_ID"), _k("DTA.EVENT_CD"), _k("NVP2.MERGE_ID")).alias("DOC_INPUT_KEY"),
            *_common(),
            _k("NVP.MERGE_ID").alias("TASK_ASSAY_ID"),
            _k("B.DCP_INPUT_REF_ID").alias("DCP_INPUT_REF_ID"),
            _k("B.DCP_SECTION_INSTANCE_ID").alias("DCP_SECTION_INSTANCE_ID"),
            _k("DTA.EVENT_CD").alias("ELEMENT_EVENT_CD"),
            col("DTA.DESCRIPTION").alias("ELEMENT_DESC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_MNEMONIC_TXT"),
            col("DTA.MNEMONIC").alias("ELEMENT_LABEL_TXT"),
            # GRID_NAME resolved point-in-time as-of PERFORMED_DT_TM in L6; carry the code here
            # (b4's grid_event_cd is NVP3).
            lit(None).cast("string").alias("GRID_NAME_TXT"),
            _k("NVP3.MERGE_ID").alias("GRID_NAME_CD"),
            col("DTA.DESCRIPTION").alias("GRID_COLUMN_DESC_TXT"),
            col("DTA.MNEMONIC").alias("GRID_COLUMN_MNEMONIC_TXT"),
            col("DTA2.DESCRIPTION").alias("GRID_ROW_DESC_TXT"),
            col("DTA2.MNEMONIC").alias("GRID_ROW_MNEMONIC_TXT"),
            _k("NVP3.MERGE_ID").alias("GRID_EVENT_CD"),
            greatest(col("B.FREF_ADC"), col("B.SREF_ADC"), col("DTA.ADC_UPDT"),
                     col("DTA2.ADC_UPDT")).alias("ADC_UPDT"),
        )
    )

    # UNION (proc UNION) + strict per-key dedup (see below).
    out_cols = [
        "DOC_INPUT_KEY", "ACTIVE_IND", "FORM_REF_ID", "FORM_DESC_TXT",
        "SECTION_REF_ID", "SECTION_DESC_TXT", "TASK_ASSAY_ID", "ELEMENT_DESC_TXT",
        "ELEMENT_MNEMONIC_TXT", "ELEMENT_LABEL_TXT", "GRID_NAME_TXT", "GRID_NAME_CD",
        "GRID_COLUMN_DESC_TXT", "GRID_COLUMN_MNEMONIC_TXT", "GRID_ROW_DESC_TXT",
        "GRID_ROW_MNEMONIC_TXT", "FORM_INSTANCE_ID", "GRID_EVENT_CD",
        "INPUT_TYPE_FLG", "DCP_INPUT_REF_ID", "DCP_SECTION_INSTANCE_ID",
        "ELEMENT_EVENT_CD", "ADC_UPDT",
    ]
    # Keep exactly one row per DOC_INPUT_KEY (active first, then most-recent ADC_UPDT, then a
    # stable label tiebreak) so the key is strictly unique and the downstream consumer LEFT join
    # (DOC.DOC_INPUT_ID == Dref.DOC_INPUT_KEY) cannot fan out responses. A projection-wide DISTINCT
    # would let two rows with the SAME key but drifted labels (audit found 48,777 such keys) BOTH
    # survive and multiply the join. This matches prod pi_lkp_cde_doc_ref, which was unique on
    # DOC_INPUT_KEY (1,933,313 rows = distinct keys).
    _wkey = Window.partitionBy("DOC_INPUT_KEY").orderBy(
        col("ACTIVE_IND").desc_nulls_last(),
        col("ADC_UPDT").desc_nulls_last(),
        col("ELEMENT_LABEL_TXT").asc_nulls_last(),  # stable label tiebreak
    )
    unioned = b1.unionByName(b2).unionByName(b3).unionByName(b4).select(*out_cols)
    return (
        unioned.withColumn("_rn", row_number().over(_wkey)).filter(col("_rn") == 1).select(*out_cols)
    )



def _mill_powerform_response(encntr_ids=None):
    """PowerForms form-response rows, derived directly from Millennium.

    """
    def _s(c):   # double/long id -> clean integer string ("1252", never "1252.0")
        return col(c).cast(LongType()).cast(StringType())
    def _b(c):   # double id -> bigint (for joins to clinical_event's bigint EVENT_ID)
        return col(c).cast(LongType())
    def _key(*parts):  # CONCAT-faithful '~'-join: explicit separators, NULL -> "" kept
        elems = []
        for i, p in enumerate(parts):
            if i:
                elems.append(lit("~"))
            elems.append(coalesce(p, lit("")))
        return F.concat(*elems)
    def _clean_resp(c):  # proc CHAR(160)/9/10/13 strip + trim + ''->null
        cleaned = trim(regexp_replace(col(c), "[\\u00A0\\t\\n\\r]", " "))
        return when(cleaned == "", None).otherwise(cleaned)

    def _prune(df, enc_col):
        # decision #9: bound an encounter-scoped source to the candidate set for incremental runs.
        # None (full build) -> no-op, byte-identical to the pre-prune behaviour. left_semi keeps
        # ONLY the left frame's columns, so the candidate join never pollutes the schema.
        if encntr_ids is None:
            return df
        candidate_keys = (encntr_ids
                          .select(col("ENCNTR_ID").cast(LongType()).alias("_CK_ENCNTR_ID"))
                          .distinct())
        return df.join(candidate_keys,
                       col(enc_col).cast(LongType()) == col("_CK_ENCNTR_ID"),
                       "left_semi")

    # NB: SQL try_cast (via F.expr), NOT F.try_cast — the pyspark.sql.functions form is Spark 3.5+
    # only; SQL try_cast works on all DBR runtimes. split()'s 2nd arg is a regex, so '[.]' = literal dot.
    ref_parse = F.expr("try_cast(split(REFERENCE_NBR, '[.]')[0] as bigint)")

    cv = spark.table("3_lookup.mill.mill_code_value")

    # ---- form-level clinical_event (non-error), key (enc, form_event_id, ref_dfa) ----
    formce = (
        _prune(spark.table("4_prod.raw.mill_clinical_event").alias("FCE"), "FCE.ENCNTR_ID")
        .join(cv.alias("FCV"),
              (col("FCE.RESULT_STATUS_CD") == col("FCV.CODE_VALUE")) & (col("FCV.ACTIVE_IND") == 1),
              "left")
        .filter(col("FCE.VALID_UNTIL_DT_TM") > F.current_timestamp())
        .filter(col("FCV.CDF_MEANING").isNull() |
                ~F.upper(col("FCV.CDF_MEANING")).isin("IN ERROR", "INERROR"))
        .select(
            col("FCE.ENCNTR_ID").cast(LongType()).alias("fce_enc"),
            col("FCE.EVENT_ID").alias("form_event_id"),
            F.expr("try_cast(split(FCE.REFERENCE_NBR, '[.]')[0] as bigint)").alias("fce_ref_dfa"),
        ).dropDuplicates(["fce_enc", "form_event_id", "fce_ref_dfa"])
    )

    # ---- FORM_TEMP: dfa x dfr(version-between) x dfac(CE comp) x form-level CE ----
    form = (
        _prune(spark.table("4_prod.raw.mill_dcp_forms_activity").alias("DFA"), "DFA.ENCNTR_ID")
        .join(spark.table("4_prod.raw.mill_dcp_forms_ref").alias("DFR"),
              (col("DFA.DCP_FORMS_REF_ID") == col("DFR.DCP_FORMS_REF_ID")) &
              (col("DFA.VERSION_DT_TM").between(col("DFR.BEG_EFFECTIVE_DT_TM"),
                                                col("DFR.END_EFFECTIVE_DT_TM"))))
        .join(spark.table("4_prod.raw.mill_dcp_forms_activity_comp").alias("DFAC"),
              (col("DFA.DCP_FORMS_ACTIVITY_ID") == col("DFAC.DCP_FORMS_ACTIVITY_ID")) &
              (col("DFAC.PARENT_ENTITY_NAME") == "CLINICAL_EVENT") &
              (col("DFAC.COMPONENT_CD") == 10891), "left")
        .withColumn("dfa_id", _b("DFA.DCP_FORMS_ACTIVITY_ID"))
        .withColumn("enc_id", _b("DFA.ENCNTR_ID"))
        .join(formce,
              (col("enc_id") == col("fce_enc")) &
              (_b("DFAC.PARENT_ENTITY_ID") == col("form_event_id")) &
              (col("dfa_id") == col("fce_ref_dfa")))
        .select(
            col("dfa_id"),
            col("enc_id"),
            _b("DFA.PERSON_ID").alias("form_person_id"),
            _b("DFR.DCP_FORM_INSTANCE_ID").alias("form_instance"),
            col("form_event_id"),
            _s("DFA.FORM_STATUS_CD").alias("FORM_STATUS_CD"),
            col("DFA.FORM_DT_TM").alias("DOCUMENTATION_DT_TM"),
            col("DFA.BEG_ACTIVITY_DT_TM").alias("FIRST_DOCUMENTED_DT_TM"),
            col("DFA.LAST_ACTIVITY_DT_TM").alias("LAST_DOCUMENTED_DT_TM"),
            col("DFA.ADC_UPDT").alias("dfa_adc"),
        ).dropDuplicates(["dfa_id", "enc_id"])
    )

    # ---- element clinical_event spine ----
    elem = (
        _prune(spark.table("4_prod.raw.mill_clinical_event").alias("CE"), "CE.ENCNTR_ID")
        .filter((col("CE.VALID_UNTIL_DT_TM") > F.current_timestamp()) &
                (col("CE.TASK_ASSAY_CD") != 0))
        .select(
            col("CE.EVENT_ID").alias("element_event_id"),
            col("CE.PARENT_EVENT_ID").alias("elem_parent_event_id"),
            col("CE.ENCNTR_ID").cast(LongType()).alias("ce_enc"),
            col("CE.EVENT_CD").alias("elem_event_cd"),
            col("CE.EVENT_CLASS_CD").alias("EVENT_CLASS_CD"),
            col("CE.TASK_ASSAY_CD").alias("elem_task_assay_cd"),
            col("CE.AUTHENTIC_FLAG").alias("AUTHENTIC_FLAG"),
            col("CE.RESULT_STATUS_CD").alias("ce_result_status_cd"),
            col("CE.PERFORMED_DT_TM").alias("PERFORMED_DT_TM"),
            _s("CE.PERFORMED_PRSNL_ID").alias("PERFORMED_PRSNL_ID"),
            col("CE.VALID_FROM_DT_TM").alias("ce_valid_from"),
            col("CE.ADC_UPDT").alias("ce_adc"),
            ref_parse.alias("elem_ref_dfa"),
        )
    )

    # element result-status meaning (for ACTIVE_IND in-error test)
    elem = (
        elem.join(cv.alias("ECV"),
                  (col("ce_result_status_cd") == col("ECV.CODE_VALUE")) & (col("ECV.ACTIVE_IND") == 1),
                  "left")
            .withColumn("ce_status_meaning", F.upper(col("ECV.CDF_MEANING")))
            .drop("ECV.CDF_MEANING")
    )

    # ---- element <-> form link (drives the row set) ----
    ef = elem.join(form, (col("elem_ref_dfa") == col("dfa_id")) & (col("ce_enc") == col("enc_id")))

    def _prune_result(df):
        """Result ENCNTR_ID is nullable; incremental pruning must use candidate element IDs."""
        if encntr_ids is None:
            return df
        element_ids = (ef.select(col("element_event_id").cast(LongType()).alias("_PF_EVENT_ID"))
                       .distinct())
        return df.join(element_ids,
                       col("EVENT_ID").cast(LongType()) == col("_PF_EVENT_ID"),
                       "left_semi")

    # ---- value results, each deduped to latest (stable tiebreaker after VALID_UNTIL DESC) ----
    # F5: stable surrogate tiebreak (VALID_FROM/UPDT), NOT value text
    wdate = Window.partitionBy("eid").orderBy(col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                              col("VALID_FROM_DT_TM").desc_nulls_last(),
                                              col("UPDT_DT_TM").desc_nulls_last())
    dres = (_prune_result(spark.table("4_prod.raw.mill_ce_date_result"))
            .withColumn("eid", col("EVENT_ID").cast(LongType()))
            .withColumn("_r", row_number().over(wdate)).filter(col("_r") == 1)
            .select(col("eid").alias("d_eid"), col("RESULT_DT_TM").alias("RESULT_DT_TM")))
    # F5: stable surrogate tiebreak (VALID_FROM/UPDT), NOT value text
    wstr = Window.partitionBy("eid").orderBy(col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                             col("VALID_FROM_DT_TM").desc_nulls_last(),
                                             col("UPDT_DT_TM").desc_nulls_last())
    sres = (_prune_result(spark.table("4_prod.raw.mill_ce_string_result"))
            .withColumn("eid", col("EVENT_ID").cast(LongType()))
            .withColumn("_r", row_number().over(wstr)).filter(col("_r") == 1)
            .select(col("eid").alias("s_eid"), col("STRING_RESULT_TEXT").alias("STRING_RESULT_TEXT")))
    # F5: stable surrogate tiebreak (VALID_FROM/UPDT), NOT value text
    wcod = Window.partitionBy("eid", "seq").orderBy(col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                                    col("VALID_FROM_DT_TM").desc_nulls_last(),
                                                    col("UPDT_DT_TM").desc_nulls_last())
    cres = (_prune_result(spark.table("4_prod.raw.mill_ce_coded_result"))
            .withColumn("eid", col("EVENT_ID").cast(LongType()))
            .withColumn("seq", col("SEQUENCE_NBR").cast(LongType()))
            .withColumn("_r", row_number().over(wcod)).filter(col("_r") == 1)
            .select(col("eid").alias("c_eid"), col("seq").alias("c_seq"),
                    col("NOMENCLATURE_ID").alias("NOMENCLATURE_ID"),
                    col("DESCRIPTOR").alias("DESCRIPTOR"),
                    col("RESULT_CD").cast(LongType()).alias("c_result_cd")))

    valued = (
        ef
        .join(dres, col("element_event_id") == col("d_eid"), "left")
        .join(sres, col("element_event_id") == col("s_eid"), "left")
        .join(cres, col("element_event_id") == col("c_eid"), "left")
    )

    # F4 / decision #8: resolve the coded answer DISPLAY as-of the response's PERFORMED_DT_TM
    # (point-in-time), NOT current_timestamp(). c_result_cd is null for non-coded rows -> cv_display null.
    valued = _cv_display_asof(valued, "c_result_cd", "PERFORMED_DT_TM", out_col="cv_display")

    eid_s = col("element_event_id").cast(StringType())  # clinical_event EVENT_ID is bigint -> safe
    is_date  = (col("EVENT_CLASS_CD") == 223) & col("RESULT_DT_TM").isNotNull()
    is_str   = col("EVENT_CLASS_CD").isin(233, 236) & col("STRING_RESULT_TEXT").isNotNull()
    is_coded = col("c_seq").isNotNull()
    is_else  = (col("RESULT_DT_TM").isNull() & (col("EVENT_CLASS_CD") != 223) &
                col("STRING_RESULT_TEXT").isNull() & ~col("EVENT_CLASS_CD").isin(233, 236) &
                col("c_seq").isNull())

    coded_val = when(col("NOMENCLATURE_ID").isNotNull() & (col("NOMENCLATURE_ID") != 0),
                     col("DESCRIPTOR")).otherwise(col("cv_display"))

    # value branches as a UNION (one element -> up to one row per applicable type)
    common_sel = [
        "element_event_id", "elem_parent_event_id", "ce_enc", "elem_event_cd", "EVENT_CLASS_CD",
        "elem_task_assay_cd", "AUTHENTIC_FLAG", "ce_status_meaning", "PERFORMED_DT_TM",
        "PERFORMED_PRSNL_ID", "ce_valid_from", "ce_adc", "dfa_id", "enc_id", "form_person_id",
        "form_instance", "form_event_id", "FORM_STATUS_CD", "DOCUMENTATION_DT_TM",
        "FIRST_DOCUMENTED_DT_TM", "LAST_DOCUMENTED_DT_TM", "dfa_adc",
        # F6: carry the in-scope coded row's NOMEN/CODE_VALUE through every variant (null on non-coded)
        "NOMENCLATURE_ID", "c_result_cd",
    ]
    # F6-fix: c_seq comes from the EVENT_ID-only cres join, so it is NON-NULL on the
    # date/string/else variant rows of any element that ALSO has a coded result (the very
    # fan-out the backfill must NOT inherit). Gate instead on a STRUCTURAL per-variant flag
    # that is literal-True ONLY when this row is built by the coded variant, so it survives
    # unionByName and never leaks onto non-coded variants of a multi-result element.
    def _variant(filt, suffix_col, resp, numeric, resp_dt, string_resp, is_coded_variant=False):
        return (valued.filter(filt)
                .select(*common_sel,
                        lit(bool(is_coded_variant)).alias("_coded_variant"),
                        F.concat(eid_s, lit("~"), suffix_col).alias("DOC_RESPONSE_KEY"),
                        resp.alias("RESPONSE_VALUE_TXT_raw"),
                        numeric.alias("NUMERIC_RESPONSE_NBR"),
                        resp_dt.alias("RESPONSE_DT_TM"),
                        string_resp.alias("STRING_RESPONSE_TXT_raw")))

    v_date = _variant(is_date, lit("-2"),
                      F.date_format(col("RESULT_DT_TM"), "MM/dd/yyyy HH:mm"),
                      lit(None).cast(DoubleType()), col("RESULT_DT_TM"),
                      lit(None).cast(StringType()))
    v_str = _variant(is_str, lit("-1"),
                     when(col("STRING_RESULT_TEXT") == "", None).otherwise(col("STRING_RESULT_TEXT")),
                     when(col("EVENT_CLASS_CD") == 233,
                          F.regexp_replace(col("STRING_RESULT_TEXT"), ",", "").cast(DoubleType())),
                     lit(None).cast(TimestampType()),
                     when(col("EVENT_CLASS_CD") == 236,
                          when(col("STRING_RESULT_TEXT") == "", None).otherwise(col("STRING_RESULT_TEXT"))))
    v_cod = _variant(is_coded, col("c_seq").cast(StringType()),
                     coded_val, lit(None).cast(DoubleType()),
                     lit(None).cast(TimestampType()), lit(None).cast(StringType()),
                     is_coded_variant=True)
    v_else = _variant(is_else, lit("-3"),
                      lit(None).cast(StringType()), lit(None).cast(DoubleType()),
                      lit(None).cast(TimestampType()), lit(None).cast(StringType()))

    variants = v_date.unionByName(v_str).unionByName(v_cod).unionByName(v_else)
    # dedup each variant key to rank-1 by VALID_FROM_DT_TM DESC (stable tiebreaker)
    wk = Window.partitionBy("DOC_RESPONSE_KEY").orderBy(col("ce_valid_from").desc_nulls_last(),
                                                        col("ce_adc").desc_nulls_last())
    resp = (variants.withColumn("_rk", row_number().over(wk)).filter(col("_rk") == 1)
            .filter(col("DOC_RESPONSE_KEY").isNotNull()))

    # ---- grid frame: distinct clinical_event for the element encounters ----
    grid = (
        _prune(spark.table("4_prod.raw.mill_clinical_event").alias("G"), "G.ENCNTR_ID")
        .filter(col("G.VALID_UNTIL_DT_TM") > F.current_timestamp())
        .select(
            col("G.EVENT_ID").alias("g_event_id"),
            col("G.PARENT_EVENT_ID").alias("g_parent_event_id"),
            F.upper(col("G.EVENT_TITLE_TEXT")).alias("g_title"),
            col("G.ENCNTR_ID").cast(LongType()).alias("g_enc"),
            F.expr("try_cast(G.COLLATING_SEQ as bigint)").alias("g_collating_seq"),
            col("G.EVENT_CD").alias("g_event_cd"),
            col("G.TASK_ASSAY_CD").alias("g_task_assay_cd"),
            col("G.UPDT_DT_TM").alias("g_updt_dt_tm"),
        ).dropDuplicates(["g_enc", "g_event_id"])
    )

    # 2-level (element -> CE2 title tracking/discrete)
    two = (
        resp.alias("R")
        .join(grid.alias("G2"),
              (col("G2.g_enc") == col("R.ce_enc")) &
              (col("G2.g_event_id") == col("R.elem_parent_event_id")) &
              col("G2.g_title").isin("TRACKING CONTROL", "DISCRETE GRID"))
        .select(col("R.DOC_RESPONSE_KEY").alias("k2"),
                col("G2.g_parent_event_id").alias("sec2"),
                col("G2.g_event_id").alias("grid2"),
                col("R.element_event_id").alias("row2"),
                col("G2.g_title").alias("title2"))
        .dropDuplicates(["k2"])
    )
    # 3-level (element -> CE2 -> CE3 title power/ultra)
    three = (
        resp.alias("R")
        .join(grid.alias("C1"),
              (col("C1.g_enc") == col("R.ce_enc")) & (col("C1.g_event_id") == col("R.element_event_id")))
        .join(grid.alias("C2"),
              (col("C2.g_enc") == col("C1.g_enc")) & (col("C2.g_event_id") == col("C1.g_parent_event_id")))
        .join(grid.alias("C3"),
              (col("C3.g_enc") == col("C2.g_enc")) & (col("C3.g_event_id") == col("C2.g_parent_event_id")) &
              col("C3.g_title").isin("POWERGRID", "ULTRAGRID"))
        .select(col("R.DOC_RESPONSE_KEY").alias("k3"),
                col("C3.g_parent_event_id").alias("sec3"),
                col("C2.g_parent_event_id").alias("grid3"),
                col("C1.g_parent_event_id").alias("row3"),
                col("C3.g_title").alias("title3"))
        .dropDuplicates(["k3"])
    )

    resolved = (
        resp.alias("R")
        .join(two, col("R.DOC_RESPONSE_KEY") == col("k2"), "left")
        .join(three, col("R.DOC_RESPONSE_KEY") == col("k3"), "left")
        .withColumn("SECTION_EVENT_ID_b",
                    coalesce(col("sec3"), col("sec2"), col("elem_parent_event_id")))
        .withColumn("GRID_EVENT_ID_b", coalesce(col("grid3"), col("grid2")))
        .withColumn("ROW_EVENT_ID_b", coalesce(col("row3"), col("row2")))
        .withColumn("grid_title", coalesce(col("title3"), col("title2")))
    )

    # section CE + section_ref -> DCP_SECTION_INSTANCE_ID
    sec_inst = (
        resolved.alias("X")
        .join(grid.alias("SC"),
              (col("SC.g_enc") == col("X.ce_enc")) & (col("SC.g_event_id") == col("X.SECTION_EVENT_ID_b")), "left")
        .join(spark.table("4_prod.raw.mill_dcp_section_ref").alias("DSR"),
              (col("SC.g_collating_seq") == col("DSR.DCP_SECTION_REF_ID")) &
              (col("SC.g_updt_dt_tm").between(col("DSR.BEG_EFFECTIVE_DT_TM"),
                                              col("DSR.END_EFFECTIVE_DT_TM"))), "left")
    )
    # I-1 GUARD: the DSR version-BETWEEN join can fan out a section CE if a
    # DCP_SECTION_REF_ID has OVERLAPPING effective windows (1571/2181 IDs do today),
    # emitting >1 row per DOC_RESPONSE_KEY and violating the live PK (UNIQUE on
    # DOC_RESPONSE_KEY). Mirror the DFR-stage dropDuplicates guard, but deterministic:
    # keep one section instance per key (newest window end, then highest instance id).
    # Defensive — measured ZERO fan-out on the parity slice (no overlapping window
    # actually contained a real section-CE UPDT_DT_TM); cheap insurance vs future data.
    wsec = Window.partitionBy("DOC_RESPONSE_KEY").orderBy(
        col("DSR.END_EFFECTIVE_DT_TM").desc_nulls_last(),
        col("DSR.DCP_SECTION_INSTANCE_ID").desc_nulls_last())
    sec_inst = (sec_inst.withColumn("_sr", row_number().over(wsec))
                .filter(col("_sr") == 1).drop("_sr"))

    # grid CE (collating_seq + event_cd) and row CE (event_cd) for the DOC_INPUT_ID build
    out = (
        sec_inst
        .join(grid.alias("GC"),
              (col("GC.g_enc") == col("ce_enc")) & (col("GC.g_event_id") == col("GRID_EVENT_ID_b")), "left")
        .join(grid.alias("RC"),
              (col("RC.g_enc") == col("ce_enc")) & (col("RC.g_event_id") == col("ROW_EVENT_ID_b")), "left")
    )

    form_inst_s = _s("form_instance")
    sec_inst_s = _s("DSR.DCP_SECTION_INSTANCE_ID")
    doc_input_id = (
        when(col("grid_title").isin("TRACKING CONTROL", "DISCRETE GRID"),
             _key(form_inst_s, sec_inst_s, _s("GC.g_collating_seq"), _s("RC.g_event_cd")))
        .when(col("grid_title").isin("POWERGRID", "ULTRAGRID"),
              _key(form_inst_s, sec_inst_s, _s("GC.g_event_cd"), _s("RC.g_event_cd"),
                   _s("elem_task_assay_cd")))
        .otherwise(_key(form_inst_s, sec_inst_s, _s("elem_event_cd")))
    )

    # encounter (ACTIVE_IND / PERSON_ID)
    enc = (spark.table("4_prod.raw.mill_encounter")
           .select(col("ENCNTR_ID").cast(LongType()).alias("e_enc"),
                   col("PERSON_ID").cast(LongType()).alias("e_person"),
                   col("ACTIVE_IND").alias("e_active"))
           .dropDuplicates(["e_enc"]))

    active_ind = when(
        (col("e_active") == 0) |
        ((col("AUTHENTIC_FLAG") != 1) & col("ce_status_meaning").isin("IN ERROR", "INERROR")),
        lit(0)).otherwise(lit(1)).cast(IntegerType())

    final = (
        out.join(enc, col("ce_enc") == col("e_enc"), "left")
        .withColumn("DOC_INPUT_ID", doc_input_id)
        .withColumn("ACTIVE_IND", active_ind)
        .withColumn("SECTION_EVENT_ID", col("SECTION_EVENT_ID_b").cast(StringType()))  # PARENT_EVENT_ID is bigint -> direct cast is safe (no .0 risk)
        .withColumn("GRID_EVENT_ID", coalesce(col("GRID_EVENT_ID_b").cast(StringType()), lit("0")))
        .withColumn("ROW_EVENT_ID", coalesce(col("ROW_EVENT_ID_b").cast(StringType()), lit("0")))
        .select(
            col("DOC_RESPONSE_KEY"),
            col("ACTIVE_IND"),
            coalesce(col("e_enc"), col("enc_id")).cast(StringType()).alias("ENCNTR_ID"),
            col("e_person").cast(StringType()).alias("PERSON_ID"),
            col("DOC_INPUT_ID"),
            col("element_event_id").cast(StringType()).alias("ELEMENT_EVENT_ID"),
            coalesce(col("form_event_id"), col("elem_parent_event_id")).cast(StringType()).alias("FORM_EVENT_ID"),
            col("SECTION_EVENT_ID"),
            col("GRID_EVENT_ID"),
            col("ROW_EVENT_ID"),
            col("FORM_STATUS_CD"),
            col("DOCUMENTATION_DT_TM"),
            col("FIRST_DOCUMENTED_DT_TM"),
            col("LAST_DOCUMENTED_DT_TM"),
            col("PERFORMED_PRSNL_ID"),
            col("PERFORMED_DT_TM"),
            _clean_resp("RESPONSE_VALUE_TXT_raw").alias("RESPONSE_VALUE_TXT"),
            col("NUMERIC_RESPONSE_NBR").cast(DoubleType()).alias("NUMERIC_RESPONSE_NBR"),
            col("RESPONSE_DT_TM"),
            _clean_resp("STRING_RESPONSE_TXT_raw").alias("STRING_RESPONSE_TXT"),
            coalesce(when(col("_coded_variant"), _s("NOMENCLATURE_ID")), lit("0")).alias("RESPONSE_NOMEN_ID"),
            coalesce(when(col("_coded_variant"), _s("c_result_cd")),    lit("0")).alias("RESPONSE_CODE_VALUE_CD"),
            col("dfa_id").alias("DCP_FORMS_ACTIVITY_ID"),
            greatest(col("ce_adc"), col("dfa_adc")).alias("ADC_UPDT"),
            col("ce_adc").alias("SOURCE_EVENT_ADC_UPDT"),
            col("dfa_adc").alias("SOURCE_ACTIVITY_ADC_UPDT"),
        )
        # DECISION 7 (tombstone): ACTIVE_IND is CARRIED, not filtered — inactive/in-error rows
        # are kept so an active->inactive flip lands as an ACTIVE_IND=0 update via the Task 9
        # MERGE (a row dropped here would be stranded active forever in the target).
        # DECISION 2: keep only well-formed 3/4/5-part DOC_INPUT_ID composites (drop ~14.5k
        # malformed 1-part live rows); key must be non-null.
        .filter(col("DOC_RESPONSE_KEY").isNotNull())
        .filter(col("DOC_INPUT_ID").isNotNull())
        .filter(~col("DOC_INPUT_ID").rlike(r"(^~)|(~~)|(~$)"))   # F3: no empty key segment (e.g. NULL section-instance -> 'form~~cd')
        .filter(F.size(F.split(col("DOC_INPUT_ID"), "~")).isin(3, 4, 5))
    )
    return final



def _mill_powerform_table(encntr_ids=None):
    """L6 assembly: the complete element/response-grain PowerForms table built from Mill.
    """
    resp = _mill_powerform_response(encntr_ids).alias("R")  # decision #9: pruned to candidate encounters when given
    # Label spine is NOT encounter-scoped (~3.67M rows of reference metadata) — always a full
    # build, never pruned by encntr_ids (decision #9).
    labels = _mill_powerform_element_ref().alias("L")
    status = _mill_code_lookup().alias("S")

    joined = resp.join(labels, col("R.DOC_INPUT_ID") == col("L.DOC_INPUT_KEY"), "left")

    # decision #8: resolve GRID_NAME_TXT from the carried grid code as-of the response's
    # PERFORMED_DT_TM (point-in-time), NOT current_timestamp(). L.GRID_NAME_CD is null on
    # non-grid rows -> cv display null. The resolved column is named GRID_NAME_TXT_resolved
    # (not GRID_NAME_TXT) to avoid colliding with the L spine's carried null GRID_NAME_TXT.
    # LATENT EDGE: a grid row with NULL PERFORMED_DT_TM falls back to current-active resolution
    # (see _cv_display_asof) -> its GRID_NAME_TXT can drift across rebuilds; flagged in the spec's
    # "remaining latent edges". Quantify on the classic-cluster full run before relying on it.
    joined = _cv_display_asof(joined, "L.GRID_NAME_CD", "R.PERFORMED_DT_TM", out_col="GRID_NAME_TXT_resolved")

    # decode FORM_STATUS_CD -> Status (bare lookup, K1 unfiltered)
    joined = joined.join(status, col("R.FORM_STATUS_CD") == col("S.CODE_VALUE_CD"), "left")

    # v3.1: encounter -> organisation / trust. v_encntr_to_org_dedup is unique on
    # ENCNTR_ID (47,964,907 rows = 47,964,907 keys); the dropDuplicates is defensive.
    org = (spark.table("4_prod.raw.v_encntr_to_org_dedup")
           .select(col("ENCNTR_ID").cast(LongType()).alias("o_enc"),
                   col("ORGANIZATION_ID").cast(LongType()).alias("ORGANIZATION_ID"),
                   col("TRUST").alias("TRUST"))
           .dropDuplicates(["o_enc"]))
    joined = joined.join(org, F.expr("try_cast(R.ENCNTR_ID as bigint)") == col("o_enc"), "left")

    return joined.select(
        col("R.DOC_RESPONSE_KEY").alias("DOC_RESPONSE_KEY"),
        col("R.ACTIVE_IND").alias("ACTIVE_IND"),
        col("R.ENCNTR_ID").alias("ENCNTR_ID"),
        col("R.PERSON_ID").alias("PERSON_ID"),
        col("R.DOC_INPUT_ID").alias("DOC_INPUT_ID"),
        col("R.ELEMENT_EVENT_ID").alias("ELEMENT_EVENT_ID"),
        col("R.FORM_EVENT_ID").alias("FORM_EVENT_ID"),
        col("R.SECTION_EVENT_ID").alias("SECTION_EVENT_ID"),
        col("R.GRID_EVENT_ID").alias("GRID_EVENT_ID"),
        col("R.ROW_EVENT_ID").alias("ROW_EVENT_ID"),
        col("R.FORM_STATUS_CD").alias("FORM_STATUS_CD"),
        col("S.CODE_DESC_TXT").alias("STATUS"),
        col("R.DOCUMENTATION_DT_TM").alias("DOCUMENTATION_DT_TM"),
        col("R.FIRST_DOCUMENTED_DT_TM").alias("FIRST_DOCUMENTED_DT_TM"),
        col("R.LAST_DOCUMENTED_DT_TM").alias("LAST_DOCUMENTED_DT_TM"),
        col("R.PERFORMED_PRSNL_ID").alias("PERFORMED_PRSNL_ID"),
        col("R.PERFORMED_DT_TM").alias("PERFORMED_DT_TM"),
        col("R.RESPONSE_VALUE_TXT").alias("RESPONSE_VALUE_TXT"),
        col("R.NUMERIC_RESPONSE_NBR").alias("NUMERIC_RESPONSE_NBR"),
        col("R.RESPONSE_DT_TM").alias("RESPONSE_DT_TM"),
        col("R.STRING_RESPONSE_TXT").alias("STRING_RESPONSE_TXT"),
        col("R.RESPONSE_NOMEN_ID").alias("RESPONSE_NOMEN_ID"),
        col("R.RESPONSE_CODE_VALUE_CD").alias("RESPONSE_CODE_VALUE_CD"),
        col("L.FORM_DESC_TXT").alias("FORM_DESC_TXT"),
        col("L.SECTION_DESC_TXT").alias("SECTION_DESC_TXT"),
        col("L.ELEMENT_LABEL_TXT").alias("ELEMENT_LABEL_TXT"),
        col("GRID_NAME_TXT_resolved").alias("GRID_NAME_TXT"),
        col("L.GRID_COLUMN_DESC_TXT").alias("GRID_COLUMN_DESC_TXT"),
        col("L.ELEMENT_DESC_TXT").alias("ELEMENT_DESC_TXT"),
        col("L.ELEMENT_MNEMONIC_TXT").alias("ELEMENT_MNEMONIC_TXT"),
        col("L.GRID_COLUMN_MNEMONIC_TXT").alias("GRID_COLUMN_MNEMONIC_TXT"),
        col("L.GRID_ROW_DESC_TXT").alias("GRID_ROW_DESC_TXT"),
        col("L.GRID_ROW_MNEMONIC_TXT").alias("GRID_ROW_MNEMONIC_TXT"),
        col("R.DCP_FORMS_ACTIVITY_ID").alias("DCP_FORMS_ACTIVITY_ID"),
        greatest(col("R.ADC_UPDT"), col("L.ADC_UPDT"), col("S.ADC_UPDT")).alias("ADC_UPDT"),
        # ---- v3.1 contract columns (powerform_pipeline SOURCE_COLUMN_TYPES) ----
        F.expr("try_cast(R.ENCNTR_ID as bigint)").alias("ENCNTR_ID_LONG"),
        F.expr("try_cast(R.PERSON_ID as bigint)").alias("PERSON_ID_LONG"),
        F.expr("try_cast(R.ELEMENT_EVENT_ID as bigint)").alias("ELEMENT_EVENT_ID_LONG"),
        F.expr("try_cast(R.PERFORMED_PRSNL_ID as bigint)").alias("PERFORMED_PRSNL_ID_LONG"),
        F.expr("try_cast(R.FORM_STATUS_CD as bigint)").alias("FORM_STATUS_CD_LONG"),
        F.expr("nullif(try_cast(R.RESPONSE_NOMEN_ID as bigint), 0)").alias("RESPONSE_NOMENCLATURE_ID"),
        F.expr("nullif(try_cast(R.RESPONSE_CODE_VALUE_CD as bigint), 0)").alias("RESPONSE_CODE_VALUE_ID"),
        F.expr("CASE WHEN nullif(try_cast(R.RESPONSE_NOMEN_ID as bigint), 0) IS NOT NULL "
               "THEN 1 ELSE 0 END").cast(IntegerType()).alias("RESPONSE_HAS_NOMENCLATURE_IND"),
        F.expr("CASE WHEN nullif(try_cast(R.RESPONSE_CODE_VALUE_CD as bigint), 0) IS NOT NULL "
               "THEN 1 ELSE 0 END").cast(IntegerType()).alias("RESPONSE_HAS_CODE_VALUE_IND"),
        # -1 string / -2 date / -3 empty are sentinels, not sequence numbers; everything
        # else is the coded SEQUENCE_NBR, so this is non-null IFF the response is coded.
        F.expr("CASE WHEN substring_index(R.DOC_RESPONSE_KEY, '~', -1) IN ('-1','-2','-3') "
               "THEN NULL ELSE try_cast(substring_index(R.DOC_RESPONSE_KEY, '~', -1) as bigint) "
               "END").alias("RESPONSE_SEQUENCE_NBR"),
        col("ORGANIZATION_ID").cast(LongType()).alias("ORGANIZATION_ID"),
        col("TRUST").alias("TRUST"),
        F.expr("try_cast(L.FORM_REF_ID as bigint)").alias("FORM_REF_ID"),
        F.expr("try_cast(L.FORM_INSTANCE_ID as bigint)").alias("FORM_INSTANCE_ID"),
        F.expr("try_cast(L.SECTION_REF_ID as bigint)").alias("SECTION_REF_ID"),
        F.expr("try_cast(L.DCP_INPUT_REF_ID as bigint)").alias("DCP_INPUT_REF_ID"),
        F.expr("try_cast(L.DCP_SECTION_INSTANCE_ID as bigint)").alias("DCP_SECTION_INSTANCE_ID"),
        F.expr("try_cast(L.TASK_ASSAY_ID as bigint)").alias("TASK_ASSAY_ID"),
        F.expr("try_cast(L.ELEMENT_EVENT_CD as bigint)").alias("ELEMENT_EVENT_CD"),
        F.expr("try_cast(L.GRID_NAME_CD as bigint)").alias("GRID_NAME_CD"),
        F.expr("try_cast(L.GRID_EVENT_CD as bigint)").alias("GRID_EVENT_CD"),
        col("L.INPUT_TYPE_FLG").cast(IntegerType()).alias("INPUT_TYPE_FLG"),
        # decision #8 latent edge, now surfaced: a grid row with a NULL PERFORMED_DT_TM
        # resolves GRID_NAME_TXT against current_timestamp(), so its label can drift.
        when(col("L.GRID_NAME_CD").isNotNull() & col("R.PERFORMED_DT_TM").isNull(), 1)
        .otherwise(0).cast(IntegerType()).alias("GRID_NAME_CURRENT_FALLBACK_IND"),
        col("R.SOURCE_ACTIVITY_ADC_UPDT").alias("SOURCE_ACTIVITY_ADC_UPDT"),
        col("R.SOURCE_EVENT_ADC_UPDT").alias("SOURCE_EVENT_ADC_UPDT"),
        col("L.ADC_UPDT").alias("SOURCE_LABEL_ADC_UPDT"),
    )


# COMMAND ----------

import uuid, datetime, json

def ensure_control_tables():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUNS} (
      run_id STRING, run_ts TIMESTAMP, run_type STRING, status STRING,
      watermark_before TIMESTAMP, watermark_after TIMESTAMP,
      label_watermark_before TIMESTAMP, label_watermark_after TIMESTAMP,
      source_fences STRING, candidate_encounters BIGINT, candidate_activities BIGINT,
      batch_nbr INT, batch_total INT,
      rows_inserted BIGINT, rows_updated BIGINT, rows_tombstoned BIGINT,
      target_version_before BIGINT, target_version_after BIGINT,
      raw_max_adc TIMESTAMP, target_max_adc TIMESTAMP, lag_hours DOUBLE, message STRING) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {STATE} (
      source_table STRING, last_committed_version BIGINT, last_committed_adc TIMESTAMP,
      last_run_id STRING, updated_ts TIMESTAMP) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CANDS} (
      run_id STRING, encntr_id BIGINT, dcp_forms_activity_id BIGINT, reason STRING,
      batch_nbr INT, status STRING, updated_ts TIMESTAMP) USING DELTA""")

from pyspark.sql.types import (StructType, StructField, StringType, TimestampType,
                               LongType, IntegerType, DoubleType)

RUNS_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("run_ts", TimestampType()),
    StructField("run_type", StringType()), StructField("status", StringType()),
    StructField("watermark_before", TimestampType()), StructField("watermark_after", TimestampType()),
    StructField("label_watermark_before", TimestampType()), StructField("label_watermark_after", TimestampType()),
    StructField("source_fences", StringType()),
    StructField("candidate_encounters", LongType()), StructField("candidate_activities", LongType()),
    StructField("batch_nbr", IntegerType()), StructField("batch_total", IntegerType()),
    StructField("rows_inserted", LongType()), StructField("rows_updated", LongType()),
    StructField("rows_tombstoned", LongType()),
    StructField("target_version_before", LongType()), StructField("target_version_after", LongType()),
    StructField("raw_max_adc", TimestampType()), StructField("target_max_adc", TimestampType()),
    StructField("lag_hours", DoubleType()), StructField("message", StringType()),
])

def record_run(**kw):
    """Append one immutable audit row (latest non-batch row per run_id is the run state)."""
    def _ts(v):
        return datetime.datetime.fromisoformat(v) if isinstance(v, str) else v
    for k in ("watermark_before", "watermark_after", "label_watermark_before",
              "label_watermark_after", "raw_max_adc", "target_max_adc"):
        kw[k] = _ts(kw.get(k))
    kw["run_ts"] = datetime.datetime.utcnow()
    row = tuple(kw.get(f.name) for f in RUNS_SCHEMA.fields)
    spark.createDataFrame([row], RUNS_SCHEMA).write.mode("append").saveAsTable(RUNS)

def begin_run(mode):
    """Single-writer guard plus opening audit row."""
    ensure_control_tables()
    terminal = (spark.table(RUNS)
                .filter((col("run_type") != "batch") &
                        col("status").isin("success", "failed", "noop", "canary"))
                .select("run_id").distinct())
    live = (spark.table(RUNS)
            .filter((col("run_type") != "batch") & (col("status") == "running") &
                    (col("run_ts") > F.expr("current_timestamp() - INTERVAL 12 HOURS")))
            .join(terminal, "run_id", "left_anti")
            .limit(1).count())
    if live:
        raise RuntimeError("POWERFORMS GUARD: another run is 'running' (<12h old). If it is dead, "
                           f"append a failed row for its run_id to {RUNS} and retry.")
    run_id = str(uuid.uuid4())
    record_run(run_id=run_id, run_type=mode, status="running", message="started")
    return run_id

def source_versions():
    """Fixed per-source Delta version fence captured at run start."""
    return {t: spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").collect()[0]["version"]
            for t in ALL_SOURCES}

def commit_source_state(run_id, fences):
    from pyspark.sql import Row
    rows = [Row(source_table=t, last_committed_version=v,
                last_committed_adc=spark.table(t).agg(F.max("ADC_UPDT")).collect()[0][0],
                last_run_id=run_id, updated_ts=datetime.datetime.utcnow())
            for t, v in fences.items()]
    src = spark.createDataFrame(rows)
    (DeltaTable.forName(spark, STATE).alias("t")
        .merge(src.alias("s"), "t.source_table = s.source_table")
        .whenMatchedUpdate(set={c: f"s.{c}" for c in src.columns})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in src.columns})
        .execute())

def get_watermark():
    """Response ADC watermark from the immutable run log, never target MAX as an ongoing checkpoint."""
    if spark.catalog.tableExists(RUNS):
        latest = (spark.table(RUNS).filter(col("run_type") != "batch")
                  .withColumn("_rn", row_number().over(
                      Window.partitionBy("run_id").orderBy(col("run_ts").desc_nulls_last())))
                  .filter(col("_rn") == 1).drop("_rn"))
        wm = (latest
              .filter((col("status").isin("success", "noop")) &
                      col("run_type").isin("incremental", "backfill", "full_rebuild", "noop"))
              .agg(F.max("watermark_after")).collect()[0][0])
        if wm is not None:
            return wm
    if spark.catalog.tableExists(TARGET):
        wm = spark.table(TARGET).agg(F.max("ADC_UPDT")).collect()[0][0]
        if wm is not None:
            return wm
    return "1900-01-01 00:00:00"

def raw_response_max_adc():
    vals = [spark.table(t).agg(F.max("ADC_UPDT")).collect()[0][0] for t in RESPONSE_SOURCES]
    return max(v for v in vals if v is not None)

def target_version(target):
    return spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").collect()[0]["version"]

def last_merge_metrics(target):
    h = (spark.sql(f"DESCRIBE HISTORY {target}").filter(col("operation") == "MERGE")
         .orderBy(col("version").desc()).limit(1).collect())
    if not h:
        return (0, 0)
    m = h[0]["operationMetrics"]
    return (int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0)))


# COMMAND ----------

REF_PARSE = "try_cast(split(REFERENCE_NBR, '[.]')[0] as bigint)"

def build_candidates(wm, run_id):
    """Build reason-tagged PowerForm candidates for source rows newer than wm."""
    wmL = lit(wm)
    def chg(t):
        return spark.table(t).filter(col("ADC_UPDT") > wmL)
    def shape(df, dfa_col, reason):
        return df.select(
            col("ENCNTR_ID").cast("long").alias("encntr_id"),
            (F.expr(dfa_col) if dfa_col else lit(None)).cast("long").alias("dcp_forms_activity_id"),
            lit(reason).alias("reason"))
    ce_event_map = (spark.table("4_prod.raw.mill_clinical_event")
                    .select(col("EVENT_ID").cast("long").alias("_event_id"),
                            col("ENCNTR_ID").cast("long").alias("_ce_encntr_id"),
                            F.expr(REF_PARSE).cast("long").alias("_ce_dfa"))
                    .filter(col("_event_id").isNotNull() & col("_ce_encntr_id").isNotNull())
                    .dropDuplicates(["_event_id"]))
    def result_shape(t, reason):
        r = (chg(t)
             .select(col("EVENT_ID").cast("long").alias("_event_id"),
                     col("ENCNTR_ID").cast("long").alias("_result_encntr_id")))
        return (r.join(ce_event_map, "_event_id", "left")
                .select(coalesce(col("_result_encntr_id"), col("_ce_encntr_id")).alias("encntr_id"),
                        col("_ce_dfa").alias("dcp_forms_activity_id"),
                        lit(reason).alias("reason")))
    direct = [
        shape(chg("4_prod.raw.mill_dcp_forms_activity"), "DCP_FORMS_ACTIVITY_ID", "dfa"),
        shape(chg("4_prod.raw.mill_dcp_forms_activity_comp"), "DCP_FORMS_ACTIVITY_ID", "dfac"),
    ]
    fallback = [
        shape(chg("4_prod.raw.mill_clinical_event"), REF_PARSE, "clinical_event"),
        shape(chg("4_prod.raw.mill_encounter"), None, "encounter"),
        result_shape("4_prod.raw.mill_ce_date_result", "date_result"),
        result_shape("4_prod.raw.mill_ce_string_result", "string_result"),
        result_shape("4_prod.raw.mill_ce_coded_result", "coded_result"),
    ]
    raw_pf_enc = (spark.table("4_prod.raw.mill_dcp_forms_activity")
                  .select(col("ENCNTR_ID").cast("long").alias("encntr_id")))
    target_pf_enc = (spark.table(TARGET)
                     .select(F.expr("try_cast(ENCNTR_ID as bigint)").alias("encntr_id")))
    pf_enc = (raw_pf_enc.unionByName(target_pf_enc)
              .filter(col("encntr_id").isNotNull()).distinct())
    fb = fallback[0]
    for p in fallback[1:]:
        fb = fb.unionByName(p)
    fb = fb.join(pf_enc, "encntr_id", "left_semi")
    cand = direct[0].unionByName(direct[1]).unionByName(fb)
    return (cand.filter(col("encntr_id").isNotNull())
                .dropDuplicates(["encntr_id", "dcp_forms_activity_id", "reason"])
                .withColumn("run_id", lit(run_id))
                .withColumn("batch_nbr", F.pmod(F.xxhash64(col("encntr_id")), lit(N_BATCH)).cast("int"))
                .withColumn("status", lit("pending"))
                .withColumn("updated_ts", F.current_timestamp())
                .select("run_id", "encntr_id", "dcp_forms_activity_id", "reason",
                        "batch_nbr", "status", "updated_ts"))

def pending_backfill_candidate_run():
    """Return the newest failed/interrupted backfill candidate run with pending work."""
    backfill_ids = (spark.table(RUNS).filter(col("run_type") == "backfill")
                    .select("run_id").distinct())
    rows = (spark.table(CANDS).filter(col("status") == "pending")
            .join(backfill_ids, "run_id", "inner")
            .groupBy("run_id").agg(F.max("updated_ts").alias("updated_ts"))
            .orderBy(col("updated_ts").desc_nulls_last()).limit(1).collect())
    return rows[0]["run_id"] if rows else None

def label_sources_changed(wm):
    wmL = lit(wm)
    return any(spark.table(t).filter(col("ADC_UPDT") > wmL).limit(1).count() > 0
               for t in LABEL_SOURCES)


# COMMAND ----------

def validate_staged(stage_tbl):
    """Hard gates on a materialized staged batch before it can touch the target."""
    s = spark.table(stage_tbl)
    n = s.count()
    if s.groupBy("DOC_RESPONSE_KEY").count().filter("count > 1").limit(1).count():
        raise RuntimeError("POWERFORMS GATE: duplicate DOC_RESPONSE_KEY in staged batch")
    if s.filter(col("DOC_RESPONSE_KEY").isNull()).limit(1).count():
        raise RuntimeError("POWERFORMS GATE: null DOC_RESPONSE_KEY in staged batch")
    bad = s.filter(col("DOC_INPUT_ID").rlike(r"(^~)|(~~)|(~$)") |
                   ~F.size(F.split(col("DOC_INPUT_ID"), "~")).isin(3, 4, 5)).limit(1).count()
    if bad:
        raise RuntimeError("POWERFORMS GATE: malformed DOC_INPUT_ID in staged batch")
    return n

def merge_staged(stage_tbl, target):
    """Explicit governed MERGE; no schema auto-evolution."""
    staged = spark.table(stage_tbl)
    setmap = {c: f"s.{c}" for c in staged.columns}
    (DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY")
        .whenMatchedUpdate(set=setmap)
        .whenNotMatchedInsert(values=setmap)
        .execute())
    return last_merge_metrics(target)

def apply_tombstones(stage_tbl, enc_batch, target, run_id):
    """Deactivate active target rows that vanished from a complete encounter-scoped rebuild."""
    staged_keys = spark.table(stage_tbl).select("DOC_RESPONSE_KEY")
    scoped_active = (spark.table(target)
                     .join(enc_batch.select(col("encntr_id").alias("b_enc")),
                           F.expr("try_cast(ENCNTR_ID as bigint)") == col("b_enc"), "left_semi")
                     .filter(col("ACTIVE_IND") == 1))
    n_scoped = scoped_active.count()
    missing = scoped_active.join(staged_keys, "DOC_RESPONSE_KEY", "left_anti").select("DOC_RESPONSE_KEY")
    n_miss = missing.count()
    if n_miss == 0:
        return 0
    if not ALLOW_MASS_TOMBSTONE and n_miss > 10000 and n_miss > 0.2 * max(n_scoped, 1):
        raise RuntimeError(f"POWERFORMS GATE: {n_miss:,}/{n_scoped:,} scoped active rows would be "
                           "tombstoned — refusing (set allow_mass_tombstone=true only after review)")
    (DeltaTable.forName(spark, target).alias("t")
        .merge(missing.alias("s"), "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY")
        .whenMatchedUpdate(set={
            "ACTIVE_IND": "0",
            "PIPELINE_RUN_ID": f"'{run_id}'",
            "PIPELINE_PROCESSED_DT_TM": "current_timestamp()"})
        .execute())
    return n_miss

def run_batch(run_id, enc_batch, target, batch_nbr=None, batch_total=None, wm=None, fences_json=None):
    """One bounded build -> stage -> validate -> MERGE -> tombstones."""
    tv_before = target_version(target)
    df = (_mill_powerform_table(enc_batch.select(col("encntr_id").alias("ENCNTR_ID")))
          .withColumn("PIPELINE_RUN_ID", lit(run_id))
          .withColumn("PIPELINE_PROCESSED_DT_TM", F.current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGE)
    n_staged = validate_staged(STAGE)
    ins = upd = 0
    if n_staged > 0:
        ins, upd = merge_staged(STAGE, target)
    tombs = apply_tombstones(STAGE, enc_batch, target, run_id)
    tv_after = target_version(target)
    if (n_staged > 0 or tombs > 0) and tv_after == tv_before:
        raise RuntimeError("POWERFORMS GATE: non-empty batch produced no target version change")
    record_run(run_id=run_id, run_type="batch", status="success",
               watermark_before=wm, source_fences=fences_json,
               candidate_encounters=enc_batch.count(), batch_nbr=batch_nbr, batch_total=batch_total,
               rows_inserted=ins, rows_updated=upd, rows_tombstoned=tombs,
               target_version_before=tv_before, target_version_after=tv_after,
               message=f"batch merged: +{ins:,} / ~{upd:,} / -{tombs:,}")
    return ins, upd, tombs


# COMMAND ----------

if RUN_MODE in ("incremental", "backfill"):
    mode = "canary" if CANARY > 0 else RUN_MODE
    run_id = begin_run(mode)
    try:
        wm = dbutils.widgets.get("backfill_floor") if RUN_MODE == "backfill" else get_watermark()
        fences = source_versions()
        fences_json = json.dumps(fences)
        raw_max = raw_response_max_adc()
        tv_start = target_version(TARGET)

        candidate_run_id = pending_backfill_candidate_run() if RUN_MODE == "backfill" else None
        if candidate_run_id:
            cand = spark.table(CANDS).filter(col("run_id") == candidate_run_id)
            print(f"RESUME: reusing candidate set of run {candidate_run_id}")
        else:
            candidate_run_id = run_id
            cand = build_candidates(wm, candidate_run_id)
            if CANARY > 0:
                if CANARY_REPLAY_RUN_ID:
                    enc_cap = (spark.table(CANDS)
                               .filter(col("run_id") == CANARY_REPLAY_RUN_ID)
                               .select("encntr_id").distinct().limit(CANARY))
                    if enc_cap.limit(1).count() == 0:
                        raise RuntimeError(f"POWERFORMS GATE: canary replay run "
                                           f"{CANARY_REPLAY_RUN_ID} has no candidate encounters")
                else:
                    enc_cap = (cand.select("encntr_id").distinct()
                               .orderBy(F.xxhash64(col("encntr_id")), col("encntr_id"))
                               .limit(CANARY))
                cand = cand.join(enc_cap, "encntr_id", "left_semi")
            cand.write.mode("append").saveAsTable(CANDS)
            cand = spark.table(CANDS).filter(col("run_id") == candidate_run_id)

        n_enc = cand.select("encntr_id").distinct().count()
        n_act = (cand.filter(col("dcp_forms_activity_id").isNotNull())
                 .select("dcp_forms_activity_id").distinct().count())
        changed = any(spark.table(t).filter(col("ADC_UPDT") > lit(wm)).limit(1).count() > 0
                      for t in RESPONSE_SOURCES)
        if changed and n_enc == 0:
            raise RuntimeError("POWERFORMS GATE: raw sources changed but candidate set is EMPTY")

        if n_enc == 0:
            if mode == "canary":
                record_run(run_id=run_id, run_type="canary", status="canary",
                           watermark_before=wm, watermark_after=None, raw_max_adc=raw_max,
                           source_fences=fences_json, candidate_encounters=0,
                           target_version_before=tv_start, target_version_after=tv_start,
                           message="canary found no source changes; watermark not advanced")
            else:
                record_run(run_id=run_id, run_type="noop", status="noop", watermark_before=wm,
                           watermark_after=raw_max, raw_max_adc=raw_max, source_fences=fences_json,
                           candidate_encounters=0, target_version_before=tv_start,
                           target_version_after=tv_start, message="no source changes — explained no-op")
                commit_source_state(run_id, fences)
        else:
            t_ins = t_upd = t_tomb = 0
            batches = sorted(r["batch_nbr"] for r in cand.select("batch_nbr").distinct().collect())
            for b in batches:
                pend = cand.filter((col("batch_nbr") == b) & (col("status") == "pending"))
                if pend.limit(1).count() == 0:
                    continue
                enc_b = pend.select("encntr_id").distinct()
                ins, upd, tomb = run_batch(run_id, enc_b, TARGET, batch_nbr=b,
                                           batch_total=len(batches), wm=wm, fences_json=fences_json)
                t_ins, t_upd, t_tomb = t_ins + ins, t_upd + upd, t_tomb + tomb
                spark.sql(f"""UPDATE {CANDS} SET status='done', updated_ts=current_timestamp()
                              WHERE run_id='{candidate_run_id}' AND batch_nbr={b}""")
            tgt_max = spark.table(TARGET).agg(F.max("ADC_UPDT")).collect()[0][0]
            lag_h = abs((raw_max - tgt_max).total_seconds()) / 3600.0
            final_status = "canary" if mode == "canary" else "success"
            tv_end = target_version(TARGET)
            record_run(run_id=run_id, run_type=mode, status=final_status,
                       watermark_before=wm,
                       watermark_after=None if mode == "canary" else raw_max,
                       raw_max_adc=raw_max, target_max_adc=tgt_max, lag_hours=lag_h,
                       source_fences=fences_json,
                       candidate_encounters=n_enc, candidate_activities=n_act,
                       rows_inserted=t_ins, rows_updated=t_upd, rows_tombstoned=t_tomb,
                       target_version_before=tv_start, target_version_after=tv_end,
                       message=f"{mode}: {n_enc:,} enc / {n_act:,} act -> "
                               f"+{t_ins:,} ~{t_upd:,} -{t_tomb:,}")
            if mode != "canary":
                commit_source_state(run_id, fences)
            print(f"POWERFORMS {mode}: {n_enc:,} encounters -> "
                  f"+{t_ins:,} / ~{t_upd:,} / -{t_tomb:,}, lag {lag_h:.1f}h")

        if label_sources_changed(wm):
            print("NOTE: label/code sources changed — existing-row labels refresh via relabel (Phase 4).")
    except Exception as e:
        record_run(run_id=run_id, run_type=mode, status="failed", message=str(e)[:4000])
        raise

elif RUN_MODE == "validate":
    t = spark.table(TARGET)
    raw_max = raw_response_max_adc()
    tgt_max = t.agg(F.max("ADC_UPDT")).collect()[0][0]
    out = {
        "rows": t.count(),
        "distinct_keys": t.select("DOC_RESPONSE_KEY").distinct().count(),
        "malformed_doc_input": t.filter(
            col("DOC_INPUT_ID").rlike(r"(^~)|(~~)|(~$)") |
            ~F.size(F.split(col("DOC_INPUT_ID"), "~")).isin(3, 4, 5)).count(),
        "raw_max_adc": str(raw_max), "target_max_adc": str(tgt_max),
        "lag_hours": abs((raw_max - tgt_max).total_seconds()) / 3600.0,
        "watermark": str(get_watermark()),
    }
    print(json.dumps(out, indent=2))
    assert out["rows"] == out["distinct_keys"], "DOC_RESPONSE_KEY not unique"
    assert out["malformed_doc_input"] == 0, "malformed DOC_INPUT_ID present"
    dbutils.notebook.exit(json.dumps(out))

elif RUN_MODE == "relabel":
    raise NotImplementedError("relabel lands in Phase 4 (Task 10)")

else:
    print(f"run_mode={RUN_MODE}: functions defined, no driver executed.")
