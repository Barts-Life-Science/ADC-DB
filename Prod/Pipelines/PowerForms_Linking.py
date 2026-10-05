# Databricks notebook source
# Drop-in replacement staged 2026-08-15 from the current production source.
# Base production SHA-256: ddc798f4504460de915e0efcc7daa17747ae96adccb5f3f5602bfbbad06da80f
# Validated change: deterministic latest-row selection for duplicate grid clinical_event rows.

dbutils.widgets.text("run_mode", "incremental")
dbutils.widgets.text("target_table", "4_prod.bronze.mill_form_activity")
dbutils.widgets.text("control_schema", "6_mgmt.bronze")   # dev override: 8_dev.powerforms
dbutils.widgets.text("backfill_floor", "2026-06-11 03:07:16.503")
dbutils.widgets.text("n_batches", "auto")  # v3.2: auto -> 1 incremental / 8 backfill
dbutils.widgets.text("canary_limit", "0")
dbutils.widgets.text("canary_replay_run_id", "")
dbutils.widgets.text("allow_mass_tombstone", "false")
dbutils.widgets.text("allow_prod_canary", "false")
dbutils.widgets.text("retain_debug_tables", "false")

# COMMAND ----------




RUN_MODE = dbutils.widgets.get("run_mode")
TARGET = dbutils.widgets.get("target_table")
CTRL = dbutils.widgets.get("control_schema")
RUNS = f"{CTRL}.powerforms_pipeline_runs"
STATE = f"{CTRL}.powerforms_pipeline_state"
CANDS = f"{CTRL}.powerforms_pipeline_candidates"
STAGE_BASE = f"{CTRL}.powerforms_stage"
LABEL_STAGE_BASE = f"{CTRL}.powerforms_label_spine"
TOMBSTONE_STAGE_BASE = f"{CTRL}.powerforms_tombstone_scope"
# v3.2: the 8-way batch split exists for backfill resumability; a ~1M-row daily
# increment paid ~8x the fixed scan cost for nothing. "auto" resolves per mode.
_NB_RAW = dbutils.widgets.get("n_batches").strip().lower()
N_BATCH = (8 if RUN_MODE == "backfill" else 1) if _NB_RAW in ("", "auto") else int(_NB_RAW)
CANARY = int(dbutils.widgets.get("canary_limit"))
CANARY_REPLAY_RUN_ID = dbutils.widgets.get("canary_replay_run_id").strip()
ALLOW_MASS_TOMBSTONE = dbutils.widgets.get("allow_mass_tombstone").lower() == "true"
ALLOW_PROD_CANARY = dbutils.widgets.get("allow_prod_canary").lower() == "true"
RETAIN_DEBUG_TABLES = dbutils.widgets.get("retain_debug_tables").lower() == "true"

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


# v3.2 source-read layer. FENCES: source table -> Delta version captured at run start
# (the fence is now APPLIED, not just logged). SLICES: source table -> run-scoped
# candidate-bounded slice table (set by build_run_slices on incremental/backfill runs).
FENCES = {}
SLICES = {}
SRC_MAX = {}              # v3.5: per-run fenced max(ADC_UPDT) probe, source table -> value
PIPELINE_VERSION = "3.6"  # deterministic grid row; blocks cross-version resume
MAX_RESUME_AGE_H = 72     # response sources keep 8-day deleted-file retention; stay inside it
RUN_AS_OF = None          # business-validity timestamp pinned per candidate run (manifest)

def _as_of_ts():
    """Business-validity timestamp: pinned per candidate run so every batch and any
    resumed attempt agree on VALID_UNTIL/label windows; live clock outside driver runs."""
    return lit(RUN_AS_OF).cast("timestamp") if RUN_AS_OF is not None else F.current_timestamp()

def _src(t):
    """Version-pinned read of a fenced source; plain read before fences are set."""
    v = FENCES.get(t)
    return spark.read.option("versionAsOf", v).table(t) if v is not None else spark.table(t)

def _pf_table(t):
    """Slice-aware read: the run-scoped candidate-bounded slice when built, else _src."""
    s = SLICES.get(t)
    return spark.table(s) if s else _src(t)


def _mill_code_lookup():
    """Mill code_value -> description lookup for the 11 RDE functions below.
    """
    def _clean(c):
        cleaned = trim(regexp_replace(col(c), "[\\x00\\n\\r\\t\\u00A0]", " "))
        return when(cleaned == "", None).otherwise(cleaned)
    cv = _src("3_lookup.mill.mill_code_value").alias("CV")
    return cv.select(
        col("CV.CODE_VALUE").cast(LongType()).cast(StringType()).alias("CODE_VALUE_CD"),
        _clean("CV.DESCRIPTION").alias("CODE_DESC_TXT"),
        _clean("CV.DISPLAY").alias("CODE_DISP_TXT"),
        col("CV.ADC_UPDT").alias("ADC_UPDT"),
    )

def _cv_display_asof(left_df, code_col, asof_col, out_col="CV_DISPLAY"):
    """
    """
    cv = _src("3_lookup.mill.mill_code_value").alias("CVD")
    asof = coalesce(col(asof_col), _as_of_ts())
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
        _src("4_prod.raw.mill_dcp_forms_ref").alias("FREF")
        .join(_src("4_prod.raw.mill_dcp_forms_def").alias("FDEF"),
              col("FREF.DCP_FORM_INSTANCE_ID") == col("FDEF.DCP_FORM_INSTANCE_ID"))
        .join(_src("4_prod.raw.mill_dcp_section_ref").alias("SREF"),
              (col("FDEF.DCP_SECTION_REF_ID") == col("SREF.DCP_SECTION_REF_ID")) &
              (col("SREF.DCP_SECTION_INSTANCE_ID") > 0))
        .join(_src("4_prod.raw.mill_dcp_input_ref").alias("DIREF"),
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
        .join(_src(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME").like("discrete_task_assay%")) &
              (col("NVP.MERGE_ID") > 0))
        .join(_src(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"))
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
        .join(_src(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay"))
        .join(_src(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(_src(nvp).alias("NVP2"),
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
        .join(_src(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay"))
        .join(_src(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(_src(nvp).alias("NVP2"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP2.PARENT_ENTITY_ID")) &
              (col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP2.PVC_NAME") == "grid_event_cd"))
        .join(_src(nvp).alias("NVP3"),
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
        .join(_src(nvp).alias("NVP"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP.PARENT_ENTITY_ID")) &
              (col("NVP.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP.PVC_NAME") == "discrete_task_assay2"))
        .join(_src(dta).alias("DTA"), col("NVP.MERGE_ID") == col("DTA.TASK_ASSAY_CD"), "left")
        .join(_src(nvp).alias("NVP2"),
              (col("B.DCP_INPUT_REF_ID") == col("NVP2.PARENT_ENTITY_ID")) &
              (col("NVP2.PARENT_ENTITY_NAME") == "DCP_INPUT_REF") &
              (col("NVP2.PVC_NAME") == "discrete_task_assay") & (col("NVP2.MERGE_ID") > 0))
        .join(_src(dta).alias("DTA2"), col("NVP2.MERGE_ID") == col("DTA2.TASK_ASSAY_CD"), "left")
        .join(_src(nvp).alias("NVP3"),
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

    cv = _src("3_lookup.mill.mill_code_value")

    # ---- form-level clinical_event (non-error), key (enc, form_event_id, ref_dfa) ----
    formce = (
        _prune(_pf_table("4_prod.raw.mill_clinical_event").alias("FCE"), "FCE.ENCNTR_ID")
        .join(cv.alias("FCV"),
              (col("FCE.RESULT_STATUS_CD") == col("FCV.CODE_VALUE")) & (col("FCV.ACTIVE_IND") == 1),
              "left")
        .filter(col("FCE.VALID_UNTIL_DT_TM") > _as_of_ts())
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
        _prune(_pf_table("4_prod.raw.mill_dcp_forms_activity").alias("DFA"), "DFA.ENCNTR_ID")
        .join(_src("4_prod.raw.mill_dcp_forms_ref").alias("DFR"),
              (col("DFA.DCP_FORMS_REF_ID") == col("DFR.DCP_FORMS_REF_ID")) &
              (col("DFA.VERSION_DT_TM").between(col("DFR.BEG_EFFECTIVE_DT_TM"),
                                                col("DFR.END_EFFECTIVE_DT_TM"))))
        .join(_pf_table("4_prod.raw.mill_dcp_forms_activity_comp").alias("DFAC"),
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
        _prune(_pf_table("4_prod.raw.mill_clinical_event").alias("CE"), "CE.ENCNTR_ID")
        .filter((col("CE.VALID_UNTIL_DT_TM") > _as_of_ts()) &
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
    dres = (_prune_result(_pf_table("4_prod.raw.mill_ce_date_result"))
            .withColumn("eid", col("EVENT_ID").cast(LongType()))
            .withColumn("_r", row_number().over(wdate)).filter(col("_r") == 1)
            .select(col("eid").alias("d_eid"), col("RESULT_DT_TM").alias("RESULT_DT_TM")))
    # F5: stable surrogate tiebreak (VALID_FROM/UPDT), NOT value text
    wstr = Window.partitionBy("eid").orderBy(col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                             col("VALID_FROM_DT_TM").desc_nulls_last(),
                                             col("UPDT_DT_TM").desc_nulls_last())
    sres = (_prune_result(_pf_table("4_prod.raw.mill_ce_string_result"))
            .withColumn("eid", col("EVENT_ID").cast(LongType()))
            .withColumn("_r", row_number().over(wstr)).filter(col("_r") == 1)
            .select(col("eid").alias("s_eid"), col("STRING_RESULT_TEXT").alias("STRING_RESULT_TEXT")))
    # F5: stable surrogate tiebreak (VALID_FROM/UPDT), NOT value text
    wcod = Window.partitionBy("eid", "seq").orderBy(col("VALID_UNTIL_DT_TM").desc_nulls_last(),
                                                    col("VALID_FROM_DT_TM").desc_nulls_last(),
                                                    col("UPDT_DT_TM").desc_nulls_last())
    cres = (_prune_result(_pf_table("4_prod.raw.mill_ce_coded_result"))
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

    # ---- grid frame: latest valid clinical_event for each element encounter ----
    grid_window = Window.partitionBy(
        col("G.ENCNTR_ID"), col("G.EVENT_ID")
    ).orderBy(
        col("G.VALID_FROM_DT_TM").desc_nulls_last(),
        col("G.VALID_UNTIL_DT_TM").desc_nulls_last(),
        col("G.UPDT_DT_TM").desc_nulls_last(),
        col("G.ADC_UPDT").desc_nulls_last(),
        col("G.PARENT_EVENT_ID").desc_nulls_last(),
        col("G.EVENT_TITLE_TEXT").desc_nulls_last(),
        col("G.COLLATING_SEQ").desc_nulls_last(),
        col("G.EVENT_CD").desc_nulls_last(),
        col("G.TASK_ASSAY_CD").desc_nulls_last(),
        col("G.REFERENCE_NBR").desc_nulls_last(),
    )
    grid = (
        _prune(_pf_table("4_prod.raw.mill_clinical_event").alias("G"), "G.ENCNTR_ID")
        .filter(col("G.VALID_UNTIL_DT_TM") > _as_of_ts())
        .withColumn("_grid_row_number", row_number().over(grid_window))
        .filter(col("_grid_row_number") == 1)
        .select(
            col("G.EVENT_ID").alias("g_event_id"),
            col("G.PARENT_EVENT_ID").alias("g_parent_event_id"),
            F.upper(col("G.EVENT_TITLE_TEXT")).alias("g_title"),
            col("G.ENCNTR_ID").cast(LongType()).alias("g_enc"),
            F.expr("try_cast(G.COLLATING_SEQ as bigint)").alias("g_collating_seq"),
            col("G.EVENT_CD").alias("g_event_cd"),
            col("G.TASK_ASSAY_CD").alias("g_task_assay_cd"),
            col("G.UPDT_DT_TM").alias("g_updt_dt_tm"),
        )
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
        .join(_src("4_prod.raw.mill_dcp_section_ref").alias("DSR"),
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
    enc = (_pf_table("4_prod.raw.mill_encounter")
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



def _mill_powerform_table(encntr_ids=None, labels=None):
    """L6 assembly with an optional pre-materialized label spine."""
    resp = _mill_powerform_response(encntr_ids).alias("R")
    # The caller materializes this 3.67M-row reference spine once per resumable run.
    labels = (labels if labels is not None else _mill_powerform_element_ref()).alias("L")
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
    org = (_pf_table("4_prod.raw.v_encntr_to_org_dedup")
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
    '''v3.5: consume the run's single max-ADC probe instead of re-scanning every
    source at commit (the probe is fenced, so the values are identical).'''
    from pyspark.sql import Row
    max_adc = SRC_MAX or source_max_adc(list(fences))
    rows = [Row(source_table=t, last_committed_version=v,
                last_committed_adc=max_adc.get(t),
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

def _wm_ts(value):
    '''Watermark values arrive as str (widget/floor) or datetime (run log/manifest);
    normalize for driver-side comparison against probed ADC maxima.'''
    return datetime.datetime.fromisoformat(value) if isinstance(value, str) else value

def source_max_adc(tables):
    '''v3.5: max(ADC_UPDT) for every listed source in ONE fenced statement (was: one
    sequential Spark job per table here plus a second full sweep at commit).'''
    selects = []
    for t in tables:
        v = FENCES.get(t)
        pin = f" VERSION AS OF {int(v)}" if v is not None else ""
        selects.append(f"SELECT '{t}' AS source_table, max(ADC_UPDT) AS max_adc FROM {t}{pin}")
    return {r["source_table"]: r["max_adc"]
            for r in spark.sql(" UNION ALL ".join(selects)).collect()}

def raw_response_max_adc():
    m = SRC_MAX or source_max_adc(RESPONSE_SOURCES)
    return max(v for t, v in m.items() if t in RESPONSE_SOURCES and v is not None)

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
        return _src(t).filter(col("ADC_UPDT") > wmL)
    def shape(df, dfa_col, reason):
        return df.select(
            col("ENCNTR_ID").cast("long").alias("encntr_id"),
            (F.expr(dfa_col) if dfa_col else lit(None)).cast("long").alias("dcp_forms_activity_id"),
            lit(reason).alias("reason"))
    # v3.2: only EVENT_IDs carried by changed result rows can match the map below, so
    # prune clinical_event to those ids BEFORE deduplicating (was: a full-table
    # dropDuplicates over ~4.4B rows every night).
    _chg_result_ids = None
    for _t in ("4_prod.raw.mill_ce_date_result",
               "4_prod.raw.mill_ce_string_result",
               "4_prod.raw.mill_ce_coded_result"):
        _ids = chg(_t).select(col("EVENT_ID").cast("long").alias("_event_id"))
        _chg_result_ids = _ids if _chg_result_ids is None else _chg_result_ids.unionByName(_ids)
    _chg_result_ids = _chg_result_ids.filter(col("_event_id").isNotNull()).distinct()
    ce_event_map = (_src("4_prod.raw.mill_clinical_event")
                    .select(col("EVENT_ID").cast("long").alias("_event_id"),
                            col("ENCNTR_ID").cast("long").alias("_ce_encntr_id"),
                            F.expr(REF_PARSE).cast("long").alias("_ce_dfa"))
                    .filter(col("_event_id").isNotNull() & col("_ce_encntr_id").isNotNull())
                    .join(_chg_result_ids, "_event_id", "left_semi")
                    .dropDuplicates(["_event_id"]))
    # v3.3: materialize the (small) changed-event map once -- as a lazy frame it was
    # re-expanded by each of the three result lanes, i.e. three more full passes over
    # mill_clinical_event. The driver drops it right after candidates are persisted.
    _cemap_tbl = f"{CTRL}.powerforms_slice_cemap_{_run_token(run_id)}"
    ce_event_map.write.format("delta").mode("overwrite").saveAsTable(_cemap_tbl)
    ce_event_map = spark.table(_cemap_tbl)
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
    raw_pf_enc = (_src("4_prod.raw.mill_dcp_forms_activity")
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

def pending_candidate_run(run_type):
    """Return the newest interrupted candidate set with pending or staged work."""
    candidate_runs = (
        spark.table(RUNS)
        .filter((col("run_type") == run_type) & col("status").isin("running", "failed"))
        .select("run_id")
        .distinct()
    )
    rows = (
        spark.table(CANDS)
        .filter(col("status").isin("pending", "staged"))
        .join(candidate_runs, "run_id", "inner")
        .groupBy("run_id")
        .agg(F.max("updated_ts").alias("updated_ts"))
        .orderBy(col("updated_ts").desc_nulls_last())
        .limit(1)
        .collect()
    )
    return rows[0]["run_id"] if rows else None


def load_manifest(candidate_run_id):
    """v3.3 preparation manifest: the exact watermark/raw-max/fence snapshot the
    candidate set was built under, recorded when the candidates are persisted."""
    rows = (spark.table(RUNS)
            .filter((col("run_id") == candidate_run_id) &
                    (col("run_type") == "manifest") & (col("status") == "prepared"))
            .orderBy(col("run_ts").desc_nulls_last())
            .limit(1).collect())
    return rows[0] if rows else None

def label_sources_changed(wm):
    # v3.5: a label source changed iff its fenced max(ADC_UPDT) exceeds the watermark
    # (max > wm <=> a row > wm exists); no per-table existence scans.
    m = SRC_MAX or source_max_adc(LABEL_SOURCES)
    w = _wm_ts(wm)
    return any(m.get(t) is not None and m[t] > w for t in LABEL_SOURCES)


# COMMAND ----------

def _run_token(value):
    return "".join(ch if ch.isalnum() else "_" for ch in value)


def _run_table(base, run_id):
    return f"{base}_{_run_token(run_id)}"


def _table_exists(name):
    return spark.catalog.tableExists(name)


def build_label_spine(run_id):
    """Build the fixed-cost label spine exactly once for this resumable candidate run."""
    label_tbl = _run_table(LABEL_STAGE_BASE, run_id)
    if _table_exists(label_tbl):
        print(f"REUSE label spine {label_tbl}")
        return label_tbl
    labels = _mill_powerform_element_ref()
    labels.write.format("delta").mode("overwrite").saveAsTable(label_tbl)
    label_rows = int(spark.table(label_tbl).count())
    print(f"BUILT label spine once: {label_rows:,} rows -> {label_tbl}")
    return label_tbl


# v3.2: run-scoped, candidate-bounded slices of the big response sources, built ONCE
# per resumable candidate run. Each batch previously re-scanned the raw tables (no
# file statistics on ENCNTR_ID/EVENT_ID -> zero skipping): mill_clinical_event alone
# was read under 3 aliases x 4 unmaterialized variant re-expansions x n_batches.
# Slices carry only the columns the response assembly reads; every per-frame filter,
# window dedup and per-batch _prune/_prune_result stays downstream, so batch output
# is unchanged.
_ENC_SLICE_SPECS = {
    "4_prod.raw.mill_clinical_event": ("ce", "ENCNTR_ID", [
        "EVENT_ID", "PARENT_EVENT_ID", "ENCNTR_ID", "EVENT_CD", "EVENT_CLASS_CD",
        "TASK_ASSAY_CD", "AUTHENTIC_FLAG", "RESULT_STATUS_CD", "PERFORMED_DT_TM",
        "PERFORMED_PRSNL_ID", "VALID_FROM_DT_TM", "VALID_UNTIL_DT_TM", "UPDT_DT_TM",
        "ADC_UPDT", "REFERENCE_NBR", "EVENT_TITLE_TEXT", "COLLATING_SEQ"]),
    "4_prod.raw.mill_dcp_forms_activity": ("dfa", "ENCNTR_ID", [
        "DCP_FORMS_ACTIVITY_ID", "ENCNTR_ID", "PERSON_ID", "DCP_FORMS_REF_ID",
        "VERSION_DT_TM", "FORM_STATUS_CD", "FORM_DT_TM", "BEG_ACTIVITY_DT_TM",
        "LAST_ACTIVITY_DT_TM", "ADC_UPDT"]),
    "4_prod.raw.mill_encounter": ("enc", "ENCNTR_ID", [
        "ENCNTR_ID", "PERSON_ID", "ACTIVE_IND", "ORGANIZATION_ID"]),
}
_EVENT_SLICE_SPECS = {
    "4_prod.raw.mill_ce_date_result": ("dres", [
        "EVENT_ID", "RESULT_DT_TM", "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM",
        "UPDT_DT_TM"]),
    "4_prod.raw.mill_ce_string_result": ("sres", [
        "EVENT_ID", "STRING_RESULT_TEXT", "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM",
        "UPDT_DT_TM"]),
    "4_prod.raw.mill_ce_coded_result": ("cres", [
        "EVENT_ID", "SEQUENCE_NBR", "NOMENCLATURE_ID", "DESCRIPTOR", "RESULT_CD",
        "VALID_UNTIL_DT_TM", "VALID_FROM_DT_TM", "UPDT_DT_TM"]),
}

def build_run_slices(run_id, cand):
    """Materialize the candidate-bounded source slices once per resumable run.
    REUSE on resume (like the label spine) so partially staged batches from an earlier
    attempt stay mutually consistent; the tables are dropped at run finalize."""
    token = _run_token(run_id)
    started = datetime.datetime.utcnow()
    enc_keys = (cand.select(col("encntr_id").cast(LongType()).alias("_slice_enc"))
                .filter(col("_slice_enc").isNotNull()).distinct())
    # v3.4: only hint broadcast for increment-sized key sets; a floor-to-1900 backfill
    # can carry millions of encounters and must shuffle instead.
    _n_keys = int(enc_keys.count())
    _b = (lambda df: F.broadcast(df)) if _n_keys <= 2_000_000 else (lambda df: df)
    def _mk(short, df):
        tbl = f"{CTRL}.powerforms_slice_{short}_{token}"
        if _table_exists(tbl):
            print(f"REUSE slice {tbl}")
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(tbl)
        return tbl
    for src_tbl, (short, key, cols) in _ENC_SLICE_SPECS.items():
        SLICES[src_tbl] = _mk(short, _src(src_tbl)
                              .join(_b(enc_keys),
                                    col(key).cast(LongType()) == col("_slice_enc"),
                                    "left_semi")
                              .select(*cols))
    # v3.3: TRUST/ORGANIZATION_ID came from view 4_prod.raw.v_encntr_to_org_dedup,
    # which cannot be version-pinned. The view is a pure projection of mill_encounter
    # with a hardcoded org->trust CASE (copied verbatim below), so derive it from the
    # FENCED encounter slice and register it under the view name for _pf_table.
    _TRUST_CASE = """CASE
        WHEN ORGANIZATION_ID IN (873843,8367658,669849,9073614,2681833,4401825,3203824,
            2681830,8061679,669848,8467812,2681824,2619824,2681827,3203825,691988,
            3125827,8061682,8061694,2641824,2641827,669847,8056759,8061685,2641830,
            3201824,691989,669845,669843,8061691,669846,3199824,669850,6333825,669844,
            8397458,8152502,671843,613843,0) THEN 'BARTS'
        WHEN ORGANIZATION_ID IN (9161976,9163579,9161983) THEN 'BHRUT'
        ELSE NULL END"""
    SLICES["4_prod.raw.v_encntr_to_org_dedup"] = _mk(
        "org", spark.table(SLICES["4_prod.raw.mill_encounter"])
        .filter(col("ENCNTR_ID").isNotNull() & col("ORGANIZATION_ID").isNotNull())
        .select("ENCNTR_ID", "ORGANIZATION_ID", F.expr(_TRUST_CASE).alias("TRUST")))
    # DFAC joins on activity id and the result tables on element EVENT_ID: derive both
    # key sets from the encounter-keyed slices just written (supersets of anything a
    # batch can match, so the per-batch semi-joins below them are unchanged).
    dfa_ids = (spark.table(SLICES["4_prod.raw.mill_dcp_forms_activity"])
               .select(col("DCP_FORMS_ACTIVITY_ID").cast(LongType()).alias("_slice_dfa"))
               .filter(col("_slice_dfa").isNotNull()).distinct())
    SLICES["4_prod.raw.mill_dcp_forms_activity_comp"] = _mk(
        "dfac", _src("4_prod.raw.mill_dcp_forms_activity_comp")
        .join(dfa_ids,
              col("DCP_FORMS_ACTIVITY_ID").cast(LongType()) == col("_slice_dfa"),
              "left_semi")
        .select("DCP_FORMS_ACTIVITY_ID", "PARENT_ENTITY_NAME", "PARENT_ENTITY_ID",
                "COMPONENT_CD"))
    event_ids = (spark.table(SLICES["4_prod.raw.mill_clinical_event"])
                 .select(col("EVENT_ID").cast(LongType()).alias("_slice_eid"))
                 .filter(col("_slice_eid").isNotNull()).distinct())
    for src_tbl, (short, cols) in _EVENT_SLICE_SPECS.items():
        SLICES[src_tbl] = _mk(short, _src(src_tbl)
                              .join(event_ids,
                                    col("EVENT_ID").cast(LongType()) == col("_slice_eid"),
                                    "left_semi")
                              .select(*cols))
    secs = (datetime.datetime.utcnow() - started).total_seconds()
    print(f"BUILT {len(SLICES)} run slices once in {secs:.1f}s")


def drop_run_slices():
    for tbl in sorted(set(SLICES.values())):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    SLICES.clear()


def validate_staged(stage_tbl):
    """Hard gates on the complete run-scoped stage before target mutation."""
    # v3.5: all four gates from ONE aggregation pass (was: 4 sequential actions).
    s = spark.table(stage_tbl)
    malformed = (col("DOC_INPUT_ID").rlike(r"(^~)|(~~)|(~$)")
                 | ~F.size(F.split(col("DOC_INPUT_ID"), "~")).isin(3, 4, 5))
    stats = s.agg(
        F.count(lit(1)).alias("n_rows"),
        F.count("DOC_RESPONSE_KEY").alias("n_keys"),          # non-null keys
        F.countDistinct("DOC_RESPONSE_KEY").alias("n_distinct"),
        F.sum(when(malformed, 1).otherwise(0)).alias("n_malformed"),
    ).first()
    n = int(stats["n_rows"] or 0)
    if int(stats["n_keys"] or 0) != int(stats["n_distinct"] or 0):
        raise RuntimeError("POWERFORMS GATE: duplicate DOC_RESPONSE_KEY in complete stage")
    if int(stats["n_keys"] or 0) != n:
        raise RuntimeError("POWERFORMS GATE: null DOC_RESPONSE_KEY in complete stage")
    if int(stats["n_malformed"] or 0):
        raise RuntimeError("POWERFORMS GATE: malformed DOC_INPUT_ID in complete stage")
    return n


def _row_hash(df, columns):
    return F.sha2(
        F.to_json(
            F.struct(*[F.col(column).alias(column) for column in columns]),
            {"ignoreNullFields": "false"},
        ),
        256,
    )


def _target_hash_sql(alias, columns):
    args = ", ".join(
        f"'{column}', {alias}.`{column}`" for column in columns
    )
    return (
        "sha2(to_json(named_struct("
        + args
        + "), map('ignoreNullFields','false')), 256)"
    )


def merge_staged(stage_tbl, target, run_id):
    """One content-guarded MERGE for the complete candidate scope."""
    staged = (
        spark.table(stage_tbl)
        .drop("_batch_nbr")
        .withColumn("PIPELINE_RUN_ID", lit(run_id))
        .withColumn("PIPELINE_PROCESSED_DT_TM", F.current_timestamp())
    )
    # TZ_LOCAL_V1/pfactivity: Europe/London companions; payload_columns keeps them only where the target has them
    # (mill_form_activity), and the content hash then refreshes rows still missing them.
    for _tz_col in ('DOCUMENTATION_DT_TM', 'LAST_DOCUMENTED_DT_TM', 'PERFORMED_DT_TM'):
        if _tz_col in staged.columns:
            staged = staged.withColumn(_tz_col + "_LOCAL", F.from_utc_timestamp(F.col(_tz_col), "Europe/London"))
    target_columns = [field.name for field in spark.table(target).schema]
    payload_columns = [column for column in staged.columns if column in target_columns]
    business_columns = [
        column for column in payload_columns
        if column not in {"PIPELINE_RUN_ID", "PIPELINE_PROCESSED_DT_TM"}
    ]
    payload = staged.select(*payload_columns).withColumn(
        "_content_hash", _row_hash(staged, business_columns)
    )
    setmap = {column: f"s.`{column}`" for column in payload_columns}
    insertmap = dict(setmap)
    target_hash = _target_hash_sql("t", business_columns)
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(payload.alias("s"), "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY")
        .whenMatchedUpdate(
            condition=f"NOT ({target_hash} <=> s._content_hash)",
            set=setmap,
        )
        .whenNotMatchedInsert(values=insertmap)
        .execute()
    )
    return last_merge_metrics(target)


def apply_tombstones(stage_tbl, enc_scope, target, run_id):
    """Apply one cast-free, encounter-scoped tombstone pass for the whole run."""
    staged_keys = (
        spark.table(stage_tbl)
        .select("DOC_RESPONSE_KEY")
        .distinct()
        .withColumn("_is_staged", lit(1))
    )
    target_enc_type = next(
        field.dataType.simpleString()
        for field in spark.table(target).schema.fields
        if field.name.upper() == "ENCNTR_ID"
    )
    typed_scope = (
        enc_scope.select(col("encntr_id").cast(target_enc_type).alias("_scope_enc"))
        .filter(col("_scope_enc").isNotNull())
        .distinct()
    )
    # v3.4: same broadcast guard as the slice builder (mass backfills must shuffle).
    _scope_b = (F.broadcast(typed_scope)
                if int(typed_scope.count()) <= 2_000_000 else typed_scope)
    tombstone_tbl = _run_table(TOMBSTONE_STAGE_BASE, run_id)
    try:
        spark.sql(f"DROP TABLE IF EXISTS {tombstone_tbl}")
        scope_snapshot = (
            spark.table(target)
            .join(
                _scope_b,
                col("ENCNTR_ID") == col("_scope_enc"),
                "left_semi",
            )
            .filter(col("ACTIVE_IND") == 1)
            .select("DOC_RESPONSE_KEY")
            .join(staged_keys, "DOC_RESPONSE_KEY", "left")
            .select(
                "DOC_RESPONSE_KEY",
                col("_is_staged").isNull().alias("_is_missing"),
            )
        )
        (
            scope_snapshot.write.format("delta")
            .mode("overwrite")
            .saveAsTable(tombstone_tbl)
        )
        snapshot = spark.table(tombstone_tbl)
        stats = snapshot.agg(
            F.count(lit(1)).alias("n_scoped"),
            F.sum(when(col("_is_missing"), 1).otherwise(0)).alias("n_miss"),
        ).first()
        n_scoped = int(stats["n_scoped"] or 0)
        n_miss = int(stats["n_miss"] or 0)
        if n_miss == 0:
            return 0
        if (
            not ALLOW_MASS_TOMBSTONE
            and n_miss > 10000
            and n_miss > 0.2 * max(n_scoped, 1)
        ):
            raise RuntimeError(
                f"POWERFORMS GATE: {n_miss:,}/{n_scoped:,} scoped active rows "
                "would be tombstoned — refusing"
            )
        missing = snapshot.filter(col("_is_missing")).select("DOC_RESPONSE_KEY")
        (
            DeltaTable.forName(spark, target)
            .alias("t")
            .merge(missing.alias("s"), "t.DOC_RESPONSE_KEY = s.DOC_RESPONSE_KEY")
            .whenMatchedUpdate(
                set={
                    "ACTIVE_IND": "0",
                    "PIPELINE_RUN_ID": f"'{run_id}'",
                    "PIPELINE_PROCESSED_DT_TM": "current_timestamp()",
                }
            )
            .execute()
        )
        return n_miss
    finally:
        if not RETAIN_DEBUG_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {tombstone_tbl}")


def stage_batch(
    run_id,
    candidate_run_id,
    enc_batch,
    label_tbl,
    stage_tbl,
    batch_nbr,
    batch_total,
    wm,
    fences_json,
):
    """Build one bounded response batch and append it to the run-scoped stage."""
    started = datetime.datetime.utcnow()
    # v3.2 defect fix: count the batch encounters BEFORE the CANDS status flip below --
    # enc_batch is lazy over CANDS, so counting it after the UPDATE always returned 0.
    enc_count = int(enc_batch.count())
    df = (
        _mill_powerform_table(
            enc_batch.select(col("encntr_id").alias("ENCNTR_ID")),
            labels=spark.table(label_tbl),
        )
        .withColumn("_batch_nbr", lit(int(batch_nbr)).cast("int"))
    )
    if _table_exists(stage_tbl):
        DeltaTable.forName(spark, stage_tbl).delete(
            F.col("_batch_nbr") == int(batch_nbr)
        )
        df.write.format("delta").mode("append").saveAsTable(stage_tbl)
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(stage_tbl)

    batch_rows = int(
        spark.table(stage_tbl)
        .filter(col("_batch_nbr") == int(batch_nbr))
        .count()
    )
    spark.sql(
        f"""UPDATE {CANDS}
            SET status='staged', updated_ts=current_timestamp()
            WHERE run_id='{candidate_run_id}' AND batch_nbr={int(batch_nbr)}
              AND status='pending'"""
    )
    record_run(
        run_id=run_id,
        run_type="batch",
        status="staged",
        watermark_before=wm,
        source_fences=fences_json,
        candidate_encounters=enc_count,
        batch_nbr=batch_nbr,
        batch_total=batch_total,
        rows_inserted=0,
        rows_updated=0,
        rows_tombstoned=0,
        message=(
            f"batch staged: {batch_rows:,} rows in "
            f"{(datetime.datetime.utcnow() - started).total_seconds():.1f}s"
        ),
    )
    return batch_rows


# COMMAND ----------

if RUN_MODE in ("incremental", "backfill"):
    mode = "canary" if CANARY > 0 else RUN_MODE
    run_id = begin_run(mode)
    stage_tbl = None
    label_tbl = None
    try:
        wm = dbutils.widgets.get("backfill_floor") if RUN_MODE == "backfill" else get_watermark()

        # v3.3: canaries never resume a real run's candidate set -- a resumed set
        # bypassed the canary cap and mutated the target with the full pending scope.
        if CANARY > 0:
            # v3.4: a canary is a REAL bounded run (it merges and tombstones its capped
            # scope). Against the production target that is a mutation, so it must be
            # explicitly acknowledged or pointed at a non-prod target/control schema.
            if ((TARGET.startswith("4_prod.") or CTRL.startswith("4_prod."))
                    and not ALLOW_PROD_CANARY):
                raise RuntimeError(
                    "POWERFORMS GATE: canary against a 4_prod target/control schema "
                    "requires allow_prod_canary=true (canaries merge + tombstone their "
                    "capped scope). Point target_table/control_schema at dev instead.")
            if pending_candidate_run(RUN_MODE):
                raise RuntimeError(
                    "POWERFORMS GATE: a pending candidate run exists; a canary would "
                    "bypass its scope. Resume or fail the pending run first.")
            candidate_run_id = None
        else:
            candidate_run_id = pending_candidate_run(RUN_MODE)

        manifest = load_manifest(candidate_run_id) if candidate_run_id else None
        meta = None
        if manifest is not None:
            # v3.4: bind resume to the code version, target and control schema, and
            # refuse manifests older than the safe versionAsOf window (deleted-file
            # retention on the response sources is 8 days). Refused manifests fall
            # through to the SUPERSEDE path below and are rebuilt from scratch.
            try:
                meta = json.loads(manifest["message"] or "null")
            except ValueError:
                meta = None
            _age_h = (datetime.datetime.utcnow()
                      - manifest["run_ts"]).total_seconds() / 3600.0
            if not isinstance(meta, dict):
                print(f"REFUSE RESUME: run {candidate_run_id} manifest has no meta")
                manifest = None
            elif meta.get("pipeline_version") != PIPELINE_VERSION:
                print(f"REFUSE RESUME: manifest pipeline_version "
                      f"{meta.get('pipeline_version')} != {PIPELINE_VERSION} "
                      f"(code changed while the run was pending)")
                manifest = None
            elif meta.get("target") != TARGET or meta.get("control_schema") != CTRL:
                print("REFUSE RESUME: manifest target/control schema differs")
                manifest = None
            elif _age_h > MAX_RESUME_AGE_H:
                print(f"REFUSE RESUME: manifest is {_age_h:.0f}h old (> {MAX_RESUME_AGE_H}h)")
                manifest = None
        if candidate_run_id and manifest is None:
            # v3.3: a candidate set without a manifest (pre-v3.3, or lost) cannot be
            # resumed safely -- committing fresh fences/raw_max would advance the
            # watermark past changes its candidates never saw. Supersede it; the fresh
            # build below re-covers its scope (its watermark was never committed).
            print(f"SUPERSEDE: candidate run {candidate_run_id} has no manifest; rebuilding")
            spark.sql(f"""UPDATE {CANDS}
                          SET status='superseded', updated_ts=current_timestamp()
                          WHERE run_id='{candidate_run_id}'
                            AND status IN ('pending','staged')""")
            candidate_run_id = None

        if manifest is not None:
            # v3.3: restore the EXACT preparation snapshot of the interrupted attempt.
            # Watermark, raw max and fences must be the ones the candidate set was
            # built under; anything newer would be committed without being processed.
            wm = manifest["watermark_before"]
            raw_max = manifest["raw_max_adc"]
            fences_json = manifest["source_fences"]
            fences = {t: int(v) for t, v in json.loads(fences_json).items()}
            FENCES.clear()
            FENCES.update(fences)
            RUN_AS_OF = datetime.datetime.fromisoformat(meta["as_of_ts"])
            SRC_MAX.clear()
            SRC_MAX.update(source_max_adc(ALL_SOURCES))  # v3.5: probe at manifest fences
        else:
            fences = source_versions()
            FENCES.clear()
            FENCES.update(fences)  # v3.2: pin every subsequent source read to the fence
            fences_json = json.dumps(fences)
            SRC_MAX.clear()
            SRC_MAX.update(source_max_adc(ALL_SOURCES))  # v3.5: one fenced probe per run
            raw_max = raw_response_max_adc()
            RUN_AS_OF = datetime.datetime.utcnow()
        tv_start = target_version(TARGET)

        if candidate_run_id:
            cand = spark.table(CANDS).filter(col("run_id") == candidate_run_id)
            print(f"RESUME: run {candidate_run_id} restored at its manifest fences")
        else:
            candidate_run_id = run_id
            cand = build_candidates(wm, candidate_run_id)
            if CANARY > 0:
                if CANARY_REPLAY_RUN_ID:
                    enc_cap = (
                        spark.table(CANDS)
                        .filter(col("run_id") == CANARY_REPLAY_RUN_ID)
                        .select("encntr_id")
                        .distinct()
                        .limit(CANARY)
                    )
                    if enc_cap.limit(1).count() == 0:
                        raise RuntimeError(
                            f"POWERFORMS GATE: canary replay run "
                            f"{CANARY_REPLAY_RUN_ID} has no candidate encounters"
                        )
                else:
                    enc_cap = (
                        cand.select("encntr_id")
                        .distinct()
                        .orderBy(F.xxhash64(col("encntr_id")), col("encntr_id"))
                        .limit(CANARY)
                    )
                cand = cand.join(enc_cap, "encntr_id", "left_semi")
            cand.write.mode("append").saveAsTable(CANDS)
            # v3.3: persist the preparation manifest so a resumed attempt restores this
            # exact snapshot instead of re-fencing against a newer one.
            record_run(run_id=candidate_run_id, run_type="manifest", status="prepared",
                       watermark_before=wm, raw_max_adc=raw_max,
                       source_fences=fences_json,
                       message=json.dumps({
                           "kind": "preparation manifest",
                           "pipeline_version": PIPELINE_VERSION,
                           "target": TARGET,
                           "control_schema": CTRL,
                           "as_of_ts": RUN_AS_OF.isoformat(),
                       }))
            spark.sql(f"DROP TABLE IF EXISTS "
                      f"{CTRL}.powerforms_slice_cemap_{_run_token(candidate_run_id)}")
            cand = spark.table(CANDS).filter(col("run_id") == candidate_run_id)

        # v3.5: one aggregation pass (countDistinct ignores NULL activity ids).
        _tally = cand.agg(
            F.countDistinct("encntr_id").alias("n_enc"),
            F.countDistinct("dcp_forms_activity_id").alias("n_act"),
        ).first()
        n_enc = int(_tally["n_enc"] or 0)
        n_act = int(_tally["n_act"] or 0)
        # v3.5: a source changed iff its fenced max(ADC_UPDT) (probed once above)
        # exceeds the watermark -- no per-table existence scans.
        _wm_cmp = _wm_ts(wm)
        changed = any(
            SRC_MAX.get(source) is not None and SRC_MAX[source] > _wm_cmp
            for source in RESPONSE_SOURCES
        )
        if changed and n_enc == 0:
            raise RuntimeError("POWERFORMS GATE: raw sources changed but candidate set is EMPTY")

        if n_enc == 0:
            if mode == "canary":
                record_run(
                    run_id=run_id,
                    run_type="canary",
                    status="canary",
                    watermark_before=wm,
                    watermark_after=None,
                    raw_max_adc=raw_max,
                    source_fences=fences_json,
                    candidate_encounters=0,
                    target_version_before=tv_start,
                    target_version_after=tv_start,
                    message="canary found no source changes; watermark not advanced",
                )
            else:
                record_run(
                    run_id=run_id,
                    run_type="noop",
                    status="noop",
                    watermark_before=wm,
                    watermark_after=raw_max,
                    raw_max_adc=raw_max,
                    source_fences=fences_json,
                    candidate_encounters=0,
                    target_version_before=tv_start,
                    target_version_after=tv_start,
                    message="no source changes — explained no-op",
                )
                commit_source_state(run_id, fences)
        else:
            label_tbl = build_label_spine(candidate_run_id)
            build_run_slices(candidate_run_id, cand)
            stage_tbl = _run_table(STAGE_BASE, candidate_run_id)
            batches = sorted(
                row["batch_nbr"]
                for row in cand.select("batch_nbr").distinct().collect()
            )
            for batch_nbr in batches:
                pending = cand.filter(
                    (col("batch_nbr") == batch_nbr)
                    & (col("status") == "pending")
                )
                if pending.limit(1).count() == 0:
                    continue
                stage_batch(
                    run_id,
                    candidate_run_id,
                    pending.select("encntr_id").distinct(),
                    label_tbl,
                    stage_tbl,
                    batch_nbr,
                    len(batches),
                    wm,
                    fences_json,
                )

            outstanding = int(
                spark.table(CANDS)
                .filter(
                    (col("run_id") == candidate_run_id)
                    & (col("status") == "pending")
                )
                .count()
            )
            if outstanding:
                raise RuntimeError(
                    f"POWERFORMS GATE: {outstanding:,} candidates were not staged"
                )
            if not _table_exists(stage_tbl):
                raise RuntimeError("POWERFORMS GATE: complete stage table is missing")

            n_staged = validate_staged(stage_tbl)
            tv_before = target_version(TARGET)
            inserted = updated = 0
            if n_staged:
                inserted, updated = merge_staged(stage_tbl, TARGET, run_id)
            tombstoned = apply_tombstones(
                stage_tbl,
                cand.select("encntr_id").distinct(),
                TARGET,
                run_id,
            )
            tv_end = target_version(TARGET)
            if (inserted or updated or tombstoned) and tv_end == tv_before:
                raise RuntimeError(
                    "POWERFORMS GATE: reported target changes produced no Delta version change"
                )

            spark.sql(
                f"""UPDATE {CANDS}
                    SET status='done', updated_ts=current_timestamp()
                    WHERE run_id='{candidate_run_id}'
                      AND status IN ('pending','staged')"""
            )
            tgt_max = spark.table(TARGET).agg(F.max("ADC_UPDT")).collect()[0][0]
            lag_h = abs((raw_max - tgt_max).total_seconds()) / 3600.0
            final_status = "canary" if mode == "canary" else "success"
            record_run(
                run_id=run_id,
                run_type=mode,
                status=final_status,
                watermark_before=wm,
                watermark_after=None if mode == "canary" else raw_max,
                raw_max_adc=raw_max,
                target_max_adc=tgt_max,
                lag_hours=lag_h,
                source_fences=fences_json,
                candidate_encounters=n_enc,
                candidate_activities=n_act,
                rows_inserted=inserted,
                rows_updated=updated,
                rows_tombstoned=tombstoned,
                target_version_before=tv_start,
                target_version_after=tv_end,
                message=(
                    f"{mode}: {n_enc:,} enc / {n_act:,} act -> "
                    f"+{inserted:,} ~{updated:,} -{tombstoned:,}; "
                    f"label spine built once"
                ),
            )
            if mode != "canary":
                commit_source_state(run_id, fences)
            print(
                f"POWERFORMS {mode}: {n_enc:,} encounters -> "
                f"+{inserted:,} / ~{updated:,} / -{tombstoned:,}, lag {lag_h:.1f}h"
            )
            if not RETAIN_DEBUG_TABLES:
                spark.sql(f"DROP TABLE IF EXISTS {stage_tbl}")
                spark.sql(f"DROP TABLE IF EXISTS {label_tbl}")
                drop_run_slices()

        if label_sources_changed(wm):
            print("NOTE: label/code sources changed; label spine was rebuilt once for this run.")
    except Exception as exc:
        record_run(run_id=run_id, run_type=mode, status="failed", message=str(exc)[:4000])
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
