# Databricks notebook source
# MAGIC %md
# MAGIC # SLAM + finance_slr → Bronze (slam_finance_pipeline)
# MAGIC | Table | Grain | Refresh |
# MAGIC |---|---|---|
# MAGIC | map_slam_apc_hrg | trimmed CDS_APC_ID | incremental MERGE + relink |
# MAGIC | map_slam_op_hrg | trimmed CDS_OPA_ID | incremental MERGE + relink |
# MAGIC | map_slam_costed_activity | (EXTRACT_CD, ACTIVITY_RECORD_ID) | version-gated rebuild |
# MAGIC | map_slam_cost_line_item | (EXTRACT_CD, ACTIVITY_RECORD_ID, LINE_HASH) | version-gated rebuild |
# MAGIC | map_finance_hcd_expenditure | ROW_HASH | snapshot hash-MERGE, soft delete |
# MAGIC
# MAGIC Direct identifiers are used for linkage then dropped; PERSON_ID +
# MAGIC PERSON_LINK_METHOD are published instead. State/audit committed only
# MAGIC after validation passes.

# COMMAND ----------

# MAGIC %run ./_slam_finance_common

# COMMAND ----------

TARGET_SCHEMA = sf_widget("target_schema", "8_dev.bronze", label="Target schema")
sf_widget("force_full_refresh", "false", ["false", "true"], "Force full refresh")
sf_widget("run_slam_hrg", "true", ["true", "false"], "Run SLAM APC/OP HRG")
sf_widget("run_finance", "true", ["true", "false"], "Run finance HCD")
sf_widget("run_plics", "true", ["true", "false"], "Run PLICS")
sf_widget("pipeline_run_id", "", label="Pipeline run id (optional)")

FORCE_FULL = sf_bool("force_full_refresh", False)
RUN_SLAM_HRG = sf_bool("run_slam_hrg", True)
RUN_FINANCE = sf_bool("run_finance", True)
RUN_PLICS = sf_bool("run_plics", True)
RUN_ID = dbutils.widgets.get("pipeline_run_id") or \
    datetime.utcnow().strftime("slamfin_%Y%m%d_%H%M%S")

assert TARGET_SCHEMA.count(".") == 1, "target_schema must be catalog.schema"
CONTROL_SCHEMA = bronze_control_schema(TARGET_SCHEMA)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}")
ensure_state_tables(CONTROL_SCHEMA)

T_APC = f"{TARGET_SCHEMA}.map_slam_apc_hrg"
T_OP = f"{TARGET_SCHEMA}.map_slam_op_hrg"
T_ACT = f"{TARGET_SCHEMA}.map_slam_costed_activity"
T_COST = f"{TARGET_SCHEMA}.map_slam_cost_line_item"
T_FIN = f"{TARGET_SCHEMA}.map_finance_hcd_expenditure"

_results = []
_started_at = sf_utc_now()
print(f"[SLAM-FIN] run={RUN_ID} target={TARGET_SCHEMA} force_full={FORCE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## map_slam_apc_hrg — FCE-grain HRG grouper output (incremental)

# COMMAND ----------

def _apc_select_sql(watermark=None):
    wm_sql = watermark.strftime("%Y-%m-%d %H:%M:%S.%f") if watermark else None
    inc = f"AND s.ADC_UPDT > timestamp'{wm_sql}'" if wm_sql else ""
    return f"""
    WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
    src AS (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY trim(s.CDS_APC_Id)
                   ORDER BY s.Record_Updated_Dt DESC NULLS LAST,
                            s.ADC_UPDT DESC NULLS LAST,
                            s.Ep_End_Dt DESC NULLS LAST
               ) AS rn
        FROM {SRC_APC} s
        WHERE s.CDS_APC_Id IS NOT NULL AND trim(s.CDS_APC_Id) <> '' {inc}
    )
    SELECT
        p.PERSON_ID,
        CASE WHEN p.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS' END AS PERSON_LINK_METHOD,
        trim(s.CDS_APC_Id)              AS CDS_APC_ID,
        trim(s.Hosp_Prov_Spell_Num)     AS HOSP_PROV_SPELL_NUM,
        s.Org_Code                      AS PROVIDER_ORG_CD,
        s.Adm_Dt                        AS ADMISSION_DT_TM,
        s.Disch_Dt                      AS DISCHARGE_DT_TM,
        s.Ep_St_Dt                      AS EPISODE_START_DT_TM,
        s.Ep_End_Dt                     AS EPISODE_END_DT_TM,
        s.Ep_Order                      AS EPISODE_ORDER,
        s.Ep_Duration                   AS EPISODE_DURATION_DAYS,
        s.Main_Spec_cd                  AS MAIN_SPECIALTY_CD,
        s.Treat_Spec_Cd                 AS TREATMENT_FUNCTION_CD,
        s.Admiss_Method                 AS ADMISSION_METHOD_CD,
        CAST(s.Admiss_Source AS STRING) AS ADMISSION_SOURCE_CD,
        asrc.Admiss_Source_Desc         AS ADMISSION_SOURCE_DESC,
        CAST(s.Disch_Method AS STRING)  AS DISCHARGE_METHOD_CD,
        CAST(s.Disch_Destin AS STRING)  AS DISCHARGE_DEST_CD,
        dd.Disch_Dest_Desc              AS DISCHARGE_DEST_DESC,
        CAST(s.Ptnt_Class AS STRING)    AS PATIENT_CLASS_CD,
        pc.Patient_Class_Desc           AS PATIENT_CLASS_DESC,
        s.Age                           AS SOURCE_AGE,
        s.Sex                           AS SOURCE_SEX_CD,
        s.Neonatal_Care_Level_Cd        AS NEONATAL_CARE_LEVEL_CD,
        s.CC_days                       AS CRITICAL_CARE_DAYS,
        s.RH_days                       AS REHAB_DAYS,
        {compact_code_array("s.ICD_Diag", 50)}  AS ICD_DIAG_CODES,
        {compact_code_array("s.OPCS_Proc", 50)} AS OPCS_PROC_CODES,
        s.FCE_HRG_Cd                    AS FCE_HRG_CD,
        hf.HRG_Desc                     AS FCE_HRG_DESC,
        s.FCE_Grouping_Method_Flag      AS FCE_GROUPING_METHOD_FLAG,
        s.FCE_Dom_proc                  AS FCE_DOMINANT_PROC_CD,
        opf.Proc_Desc                   AS FCE_DOMINANT_PROC_DESC,
        s.FCE_PBC                       AS FCE_PBC_CD,
        s.FCE_Calc_Epi_dur              AS FCE_CALC_EPISODE_DUR,
        s.FCE_Reporting_EPI_DUR         AS FCE_REPORTING_EPISODE_DUR,
        s.Dom_Ep_Flag                   AS DOMINANT_EPISODE_FLAG,
        s.Spell_HRG_Cd                  AS SPELL_HRG_CD,
        hs.HRG_Desc                     AS SPELL_HRG_DESC,
        s.Spell_Group_Method_Flag       AS SPELL_GROUPING_METHOD_FLAG,
        s.Spell_Dom_Proc                AS SPELL_DOMINANT_PROC_CD,
        ops.Proc_Desc                   AS SPELL_DOMINANT_PROC_DESC,
        s.Spell_PDiag                   AS SPELL_PRIMARY_DIAG_CD,
        icdp.ICD_Diag_Desc              AS SPELL_PRIMARY_DIAG_DESC,
        s.Spell_SDiag                   AS SPELL_SECONDARY_DIAG_CD,
        icds.ICD_Diag_Desc              AS SPELL_SECONDARY_DIAG_DESC,
        s.Spell_Ep_Count                AS SPELL_EPISODE_COUNT,
        s.Spell_LOS                     AS SPELL_LOS,
        s.Spell_Reporting_LOS           AS SPELL_REPORTING_LOS,
        s.Spell_CCDays                  AS SPELL_CRITICAL_CARE_DAYS,
        s.Spell_SSC                     AS SPELL_SSC_CD,
        s.Spell_BP                      AS SPELL_BEST_PRACTICE_CD,
        s.Errors                        AS GROUPER_ERRORS,
        s.Record_Updated_Dt             AS SOURCE_RECORD_UPDATED_DT,
        s.ADC_UPDT
    FROM src s
    LEFT JOIN mrn_person p ON trim(s.MRN) = p.ALIAS
    LEFT JOIN {LKP_HRG} hf ON s.FCE_HRG_Cd = hf.HRG_Cd
    LEFT JOIN {LKP_HRG} hs ON s.Spell_HRG_Cd = hs.HRG_Cd
    LEFT JOIN {LKP_OPCS} opf ON replace(trim(s.FCE_Dom_proc), '.', '') = opf.Proc_Cd
    LEFT JOIN {LKP_OPCS} ops ON replace(trim(s.Spell_Dom_Proc), '.', '') = ops.Proc_Cd
    LEFT JOIN {LKP_ICD} icdp ON replace(trim(s.Spell_PDiag), '.', '') = icdp.ICD_Diag_Cd
    LEFT JOIN {LKP_ICD} icds ON replace(trim(s.Spell_SDiag), '.', '') = icds.ICD_Diag_Cd
    LEFT JOIN {LKP_ADMISS_SOURCE} asrc
           ON CAST(s.Admiss_Source AS STRING) = CAST(asrc.Admiss_Source_Cd AS STRING)
    LEFT JOIN {LKP_DISCH_DEST} dd
           ON CAST(s.Disch_Destin AS STRING) = CAST(dd.Disch_Dest_Cd AS STRING)
    LEFT JOIN {LKP_PATIENT_CLASS} pc
           ON CAST(s.Ptnt_Class AS STRING) = CAST(pc.Patient_Class_Cd AS STRING)
    WHERE s.rn = 1
    """

def _relink_unresolved(target_table, key_col, src_table, src_key_expr,
                       src_mrn_expr):
    """Re-attempt MRN linkage for rows that are still unlinked (picks up
    alias-table changes without full CDF plumbing)."""
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING (
            WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
            src AS (
                SELECT {src_key_expr} AS K, {src_mrn_expr} AS M,
                       ROW_NUMBER() OVER (PARTITION BY {src_key_expr}
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {src_table}
                WHERE {src_key_expr} IS NOT NULL
            )
            SELECT s.K, p.PERSON_ID
            FROM src s JOIN mrn_person p ON s.M = p.ALIAS
            WHERE s.rn = 1
        ) s
        ON t.{key_col} = s.K AND t.PERSON_ID IS NULL
        WHEN MATCHED THEN UPDATE SET
            t.PERSON_ID = s.PERSON_ID,
            t.PERSON_LINK_METHOD = 'MRN_ALIAS',
            t.ADC_UPDT = current_timestamp()
    """)

if RUN_SLAM_HRG:
    if FORCE_FULL or not table_exists(T_APC):
        spark.sql(f"CREATE OR REPLACE TABLE {T_APC} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_apc_select_sql()}")
        mode = "FULL"
    else:
        wm = get_max_timestamp(T_APC)
        merge_upsert(spark.sql(_apc_select_sql(watermark=wm)), T_APC, ["CDS_APC_ID"])
        _relink_unresolved(T_APC, "CDS_APC_ID", SRC_APC,
                           "trim(CDS_APC_Id)", "trim(MRN)")
        mode = f"MERGE>{wm}"
    n = spark.table(T_APC).count()
    _results.append({"table": T_APC, "mode": mode, "rows": n})
    print(f"[SLAM-FIN] {T_APC} {mode} rows={n}")
else:
    _results.append({"table": T_APC, "mode": "SKIPPED"})

# COMMAND ----------

if RUN_SLAM_HRG:
    assert_no_direct_identifiers(T_APC)
    apply_table_comment(T_APC, (
        "SLAM HRG4+ grouper output at finished-consultant-episode grain (one "
        "row per trimmed CDS_APC_ID) from 4_prod.raw.slam_apc_hrg_v4. Carries "
        "FCE- and spell-level HRG assignment, grouper metadata and mapped "
        "descriptions; person-resolved via MRN (mill_person_alias type 10) "
        "with the identifier itself not published. ICD/OPCS grouper inputs "
        "kept as ordered arrays (element 1 = primary); coded diagnosis/"
        "procedure of record lives in map_diagnosis/map_procedure."
    ))
    apply_column_comments(T_APC, {
        "PERSON_ID": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
        "CDS_APC_ID": "Trimmed CDS admitted-patient-care episode identifier; unique key; joins back to raw.",
        "HOSP_PROV_SPELL_NUM": "Hospital provider spell number grouping episodes into spells.",
        "SOURCE_AGE": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
        "SOURCE_SEX_CD": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
        "TREATMENT_FUNCTION_CD": "NHS Treatment Function Code (national standard).",
        "ICD_DIAG_CODES": "Ordered ICD-10 codes as submitted to the grouper; element 1 = primary diagnosis.",
        "OPCS_PROC_CODES": "Ordered OPCS-4 codes as submitted to the grouper; element 1 = primary procedure.",
        "FCE_HRG_CD": "HRG4+ code assigned to this finished consultant episode.",
        "SPELL_HRG_CD": "HRG4+ code assigned at hospital-spell level (repeated on every episode of the spell).",
        "GROUPER_ERRORS": "Grouper error/quality messages.",
        "ADC_UPDT": "Load/update timestamp; incremental watermark column.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## map_slam_op_hrg — OP-attendance HRG (incremental)

# COMMAND ----------

def _op_select_sql(watermark=None):
    wm_sql = watermark.strftime("%Y-%m-%d %H:%M:%S.%f") if watermark else None
    inc = f"AND s.ADC_UPDT > timestamp'{wm_sql}'" if wm_sql else ""
    return f"""
    WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
    src AS (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY trim(s.CDS_OPA_Id)
                   ORDER BY s.Record_Updated_Dt DESC NULLS LAST,
                            s.ADC_UPDT DESC NULLS LAST,
                            s.Att_Dt DESC NULLS LAST
               ) AS rn
        FROM {SRC_OP} s
        WHERE s.CDS_OPA_Id IS NOT NULL AND trim(s.CDS_OPA_Id) <> '' {inc}
    )
    SELECT
        p.PERSON_ID,
        CASE WHEN p.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS' END AS PERSON_LINK_METHOD,
        trim(s.CDS_OPA_Id)         AS CDS_OPA_ID,
        s.Att_Dt                   AS ATTENDANCE_DT_TM,
        s.Main_Spec_cd             AS MAIN_SPECIALTY_CD,
        s.Treat_Spec_Cd            AS TREATMENT_FUNCTION_CD,
        CAST(s.First_Attend_Cd AS STRING) AS FIRST_ATTEND_CD,
        fa.First_Attend_Desc       AS FIRST_ATTEND_DESC,
        s.Age                      AS SOURCE_AGE,
        s.Sex                      AS SOURCE_SEX_CD,
        {compact_code_array("s.OPCS_Proc", 12)} AS OPCS_PROC_CODES,
        s.NAC_HRG_Cd               AS HRG_CD,
        h.HRG_Desc                 AS HRG_DESC,
        s.Grouping_Method_Flag     AS GROUPING_METHOD_FLAG,
        s.Dom_proc                 AS DOMINANT_PROC_CD,
        op.Proc_Desc               AS DOMINANT_PROC_DESC,
        s.Errors                   AS GROUPER_ERRORS,
        s.Record_Updated_Dt        AS SOURCE_RECORD_UPDATED_DT,
        s.ADC_UPDT
    FROM src s
    LEFT JOIN mrn_person p ON trim(s.MRN) = p.ALIAS
    LEFT JOIN {LKP_HRG} h ON s.NAC_HRG_Cd = h.HRG_Cd
    LEFT JOIN {LKP_OPCS} op ON replace(trim(s.Dom_proc), '.', '') = op.Proc_Cd
    LEFT JOIN {LKP_FIRST_ATTEND} fa
           ON CAST(s.First_Attend_Cd AS STRING) = CAST(fa.First_Attend_Cd AS STRING)
    WHERE s.rn = 1
    """

if RUN_SLAM_HRG:
    if FORCE_FULL or not table_exists(T_OP):
        spark.sql(f"CREATE OR REPLACE TABLE {T_OP} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_op_select_sql()}")
        mode = "FULL"
    else:
        wm = get_max_timestamp(T_OP)
        merge_upsert(spark.sql(_op_select_sql(watermark=wm)), T_OP, ["CDS_OPA_ID"])
        _relink_unresolved(T_OP, "CDS_OPA_ID", SRC_OP,
                           "trim(CDS_OPA_Id)", "trim(MRN)")
        mode = f"MERGE>{wm}"
    n = spark.table(T_OP).count()
    _results.append({"table": T_OP, "mode": mode, "rows": n})
    print(f"[SLAM-FIN] {T_OP} {mode} rows={n}")
else:
    _results.append({"table": T_OP, "mode": "SKIPPED"})


# COMMAND ----------

if RUN_SLAM_HRG:
    assert_no_direct_identifiers(T_OP)
    apply_table_comment(T_OP, (
        "SLAM HRG4+ non-admitted-consultation grouper output at outpatient-"
        "attendance grain (one row per trimmed CDS_OPA_ID) from "
        "4_prod.raw.slam_op_hrg; repeated source keys deduplicated to latest "
        "Record_Updated_Dt/ADC_UPDT. Person-resolved via MRN alias type 10 "
        "(identifier not published); OPCS inputs retained as an ordered array."
    ))
    apply_column_comments(T_OP, {
        "PERSON_ID": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
        "CDS_OPA_ID": "Trimmed CDS outpatient attendance identifier; unique key; joins back to raw.",
        "SOURCE_AGE": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
        "SOURCE_SEX_CD": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
        "HRG_CD": "HRG4+ non-admitted-consultation HRG assigned to the outpatient attendance.",
        "OPCS_PROC_CODES": "Ordered OPCS-4 codes as submitted to the grouper; element 1 = primary procedure.",
        "ATTENDANCE_DT_TM": "Attendance date/time as supplied; sentinel outliers 1800/2100/2999/5643 are retained losslessly — see validation baselines.",
        "ADC_UPDT": "Load/update timestamp; incremental watermark column.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## map_finance_hcd_expenditure — high-cost drug/device SLR lines
# MAGIC Snapshot hash-gated MERGE with soft delete. HCDR "HRG" codes are local
# MAGIC high-cost-drug categories (0/1.17M match hrg_v4) → HCDR_CATEGORY_CD.
# MAGIC HomeDeliveryCharge omitted (blank on 100% of rows).

# COMMAND ----------

_FIN_BUSINESS_COLS = [
    "TransactionID", "MRN", "NHSNumber", "EffectiveDate", "FinancialYear",
    "FinancialMonth", "ReportingYear", "ReportingMonth", "OrgIDProviderCode",
    "SiteCode", "SpecCode", "ConsultantCode", "PatientType", "POD",
    "ChargeableItem", "AdditionalInfo", "DMD", "DMDTaxonomyCode",
    "RouteOfAdministration", "Strength", "Volume", "PackSize", "Quantity",
    "UnitOfMeasure", "DispensingRoute", "DispensingLocation", "Indication",
    "FundingReference", "HRGCode", "HRGDesc", "CCGResidence", "CCGGP",
    "CommissionerCode", "CommissionerType", "ServiceLine",
    "ServiceCategoryCode", "UnitPrice_Supplier", "UnitPrice_Commissioner",
    "VAT", "VATCode", "Income", "Cost", "Margin", "Lloyds_Dispensing_Fee",
    "Production_Fee", "Fixed_Patient_Income", "CostCentreDesc", "DrugFeed",
    "DataSet", "DrugCategory", "LedgerCode", "ExclusionFlag",
    "ExclusionReason", "Lv3_Code", "Lv3_Description", "Lv6_Code",
    "Lv6_Description", "Lv7_Code", "Lv7_Description", "Lv9_Code",
    "Lv9_Description", "SLR_Code", "Diabetic_Flag", "IMCoE_Flag",
]
_FIN_HASH = "sha2(concat_ws('||', " + ", ".join(
    f"coalesce(CAST(f.{c} AS STRING), '')" for c in _FIN_BUSINESS_COLS
) + "), 256)"

_FIN_SQL = f"""
WITH {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
     {person_alias_cte(ALIAS_TYPE_NHS, "nhs_person")},
dmd_map AS (
    SELECT c.concept_code, c.concept_id, c.concept_name,
           MIN(std.concept_id) AS std_concept_id,
           MIN(std.concept_name) AS std_concept_name,
           COUNT(DISTINCT std.concept_id) AS std_n
    FROM {OMOP_CONCEPT} c
    LEFT JOIN {OMOP_CONCEPT_REL} r
           ON r.concept_id_1 = c.concept_id AND r.relationship_id = 'Maps to'
          AND (r.invalid_reason IS NULL OR r.invalid_reason = '')
    LEFT JOIN {OMOP_CONCEPT} std
           ON std.concept_id = r.concept_id_2 AND std.standard_concept = 'S'
    WHERE c.vocabulary_id = 'dm+d'
    GROUP BY c.concept_code, c.concept_id, c.concept_name
),
src AS (
    SELECT f.*,
        CASE WHEN trim(f.DMD) RLIKE '^[0-9]+$' THEN trim(f.DMD) END AS DMD_CODE_STRICT,
        {_FIN_HASH} AS ROW_HASH
    FROM {SRC_FIN} f
),
collapsed AS (
    SELECT *, COUNT(*) OVER (PARTITION BY ROW_HASH) AS SOURCE_DUPLICATE_COUNT,
           ROW_NUMBER() OVER (PARTITION BY ROW_HASH ORDER BY ROW_HASH) AS rn
    FROM src
)
SELECT
    coalesce(pm.PERSON_ID, pn.PERSON_ID) AS PERSON_ID,
    CASE WHEN pm.PERSON_ID IS NOT NULL THEN 'MRN_ALIAS'
         WHEN pn.PERSON_ID IS NOT NULL THEN 'NHS_ALIAS' END AS PERSON_LINK_METHOD,
    k.ROW_HASH, k.SOURCE_DUPLICATE_COUNT,
    true AS SOURCE_PRESENT_IND,
    nullif(trim(k.TransactionID), '') AS TRANSACTION_ID,
    k.FinancialYear   AS FINANCIAL_YEAR,
    k.FinancialMonth  AS FINANCIAL_MONTH,
    k.ReportingYear   AS REPORTING_YEAR,
    k.ReportingMonth  AS REPORTING_MONTH,
    k.EffectiveDate   AS EFFECTIVE_DT_TM,
    k.OrgIDProviderCode AS PROVIDER_ORG_CD,
    k.SiteCode        AS SITE_CD,
    {finance_site_case("k.SiteCode")} AS SITE_NAME,
    k.SpecCode        AS SPECIALTY_CD,
    k.ConsultantCode  AS CONSULTANT_CD,
    k.PatientType     AS PATIENT_TYPE,
    k.POD             AS POD_CD,
    k.ChargeableItem  AS CHARGEABLE_ITEM,
    k.AdditionalInfo  AS ADDITIONAL_INFO,
    k.DMD             AS DMD_RAW,
    k.DMD_CODE_STRICT AS DMD_CODE,
    d.concept_id      AS DMD_CONCEPT_ID,
    d.concept_name    AS DMD_CONCEPT_NAME,
    CASE WHEN d.std_n = 1 THEN d.std_concept_id END AS DRUG_STANDARD_CONCEPT_ID,
    CASE WHEN d.std_n = 1 THEN d.std_concept_name END AS DRUG_STANDARD_CONCEPT_NAME,
    CASE WHEN k.DMD_CODE_STRICT IS NULL THEN 'NO_CODE'
         WHEN d.concept_id IS NULL THEN 'CODE_UNMAPPED'
         WHEN d.std_n = 1 THEN 'MAPPED_STANDARD'
         ELSE 'MAPPED_DMD_ONLY' END AS DMD_MAPPING_STATUS,
    k.DMDTaxonomyCode AS DMD_TAXONOMY_CD,
    k.RouteOfAdministration AS ROUTE_OF_ADMINISTRATION,
    k.Strength AS STRENGTH, k.Volume AS VOLUME, k.PackSize AS PACK_SIZE,
    k.Quantity AS QUANTITY, k.UnitOfMeasure AS UNIT_OF_MEASURE,
    k.DispensingRoute AS DISPENSING_ROUTE,
    k.DispensingLocation AS DISPENSING_LOCATION,
    k.Indication AS INDICATION, k.FundingReference AS FUNDING_REFERENCE,
    k.HRGCode AS HCDR_CATEGORY_CD, k.HRGDesc AS HCDR_CATEGORY_DESC,
    k.CCGResidence AS CCG_RESIDENCE_CD, k.CCGGP AS CCG_GP_CD,
    k.CommissionerCode AS COMMISSIONER_CD, k.CommissionerType AS COMMISSIONER_TYPE,
    k.ServiceLine AS SERVICE_LINE, k.ServiceCategoryCode AS SERVICE_CATEGORY_CD,
    k.UnitPrice_Supplier AS UNIT_PRICE_SUPPLIER,
    k.UnitPrice_Commissioner AS UNIT_PRICE_COMMISSIONER,
    k.VAT, k.VATCode AS VAT_CD,
    k.Income AS INCOME, k.Cost AS COST, k.Margin AS MARGIN,
    k.Lloyds_Dispensing_Fee AS LLOYDS_DISPENSING_FEE,
    k.Production_Fee AS PRODUCTION_FEE,
    k.Fixed_Patient_Income AS FIXED_PATIENT_INCOME,
    k.CostCentreDesc AS COST_CENTRE_DESC,
    k.DrugFeed AS DRUG_FEED, k.DataSet AS DATA_SET,
    k.DrugCategory AS DRUG_CATEGORY, k.LedgerCode AS LEDGER_CD,
    k.ExclusionFlag AS EXCLUSION_FLAG, k.ExclusionReason AS EXCLUSION_REASON,
    k.Lv3_Code AS LEDGER_LV3_CD, k.Lv3_Description AS LEDGER_LV3_DESC,
    k.Lv6_Code AS LEDGER_LV6_CD, k.Lv6_Description AS LEDGER_LV6_DESC,
    k.Lv7_Code AS LEDGER_LV7_CD, k.Lv7_Description AS LEDGER_LV7_DESC,
    k.Lv9_Code AS LEDGER_LV9_CD, k.Lv9_Description AS LEDGER_LV9_DESC,
    k.SLR_Code AS SLR_CD, k.Diabetic_Flag AS DIABETIC_FLAG,
    k.IMCoE_Flag AS IMCOE_FLAG,
    current_timestamp() AS ADC_UPDT
FROM collapsed k
LEFT JOIN mrn_person pm ON trim(k.MRN) = pm.ALIAS
LEFT JOIN nhs_person pn
       ON regexp_replace(k.NHSNumber, ' ', '') = pn.ALIAS
LEFT JOIN dmd_map d ON k.DMD_CODE_STRICT = d.concept_code
WHERE k.rn = 1
"""

if RUN_FINANCE:
    fin_ver = source_version(SRC_FIN)
    prev_ver = get_state_version(CONTROL_SCHEMA, T_FIN, SRC_FIN)
    if not FORCE_FULL and prev_ver == fin_ver and table_exists(T_FIN):
        mode = "UNCHANGED_SKIP"
    elif FORCE_FULL or not table_exists(T_FIN):
        spark.sql(f"CREATE OR REPLACE TABLE {T_FIN} "
                  f"TBLPROPERTIES (delta.enableChangeDataFeed = true) "
                  f"AS {_FIN_SQL}")
        mode = "FULL"
    else:
        snap = spark.sql(_FIN_SQL)
        merge_upsert(snap, T_FIN, ["ROW_HASH"])
        # soft-delete: rows no longer present in the current snapshot
        spark.sql(f"""
            MERGE INTO {T_FIN} t
            USING (SELECT ROW_HASH FROM ({_FIN_SQL})) s
            ON t.ROW_HASH = s.ROW_HASH
            WHEN NOT MATCHED BY SOURCE AND t.SOURCE_PRESENT_IND THEN UPDATE SET
                t.SOURCE_PRESENT_IND = false, t.ADC_UPDT = current_timestamp()
        """)
        mode = "SNAPSHOT_MERGE"
    n = spark.table(T_FIN).count() if table_exists(T_FIN) else 0
    _results.append({"table": T_FIN, "mode": mode, "rows": n,
                     "source_version": fin_ver})
    print(f"[SLAM-FIN] {T_FIN} {mode} rows={n} src_ver={fin_ver}")
else:
    _results.append({"table": T_FIN, "mode": "SKIPPED"})


# COMMAND ----------

if RUN_FINANCE and table_exists(T_FIN):
    assert_no_direct_identifiers(T_FIN)
    apply_table_comment(T_FIN, (
        "Patient-level SLR high-cost drug/device reimbursement expenditure from "
        "4_prod.raw.finance_slr_hcdr_expenditure_report. No natural source key "
        "(TransactionID is blank on a material share of rows): ROW_HASH is a "
        "content hash over all business columns, with exact duplicates collapsed "
        "using SOURCE_DUPLICATE_COUNT (reconstruct totals as measure × count). "
        "The source is a full snapshot: rows absent from the latest snapshot are "
        "retained with SOURCE_PRESENT_IND=false, never deleted. Person-resolved "
        "via MRN alias type 10 then NHS-number alias type 18; identifiers are not "
        "published. HCDR_CATEGORY_CD is the source HRGCode, a local high-cost-"
        "drug category and not a standard HRG. dm+d is strictly matched using "
        "all-numeric codes only, with a standard hop only for one valid Maps-to "
        "target. HomeDeliveryCharge is omitted because it was blank on all rows "
        "at design time."
    ))
    apply_column_comments(T_FIN, {
        "ROW_HASH": "SHA-256 content hash over every source business column; primary key for snapshot reconciliation.",
        "SOURCE_DUPLICATE_COUNT": "Number of exact-identical source rows represented by this published row; reconstruct additive measures as measure × count.",
        "SOURCE_PRESENT_IND": "True when present in the latest full source snapshot; false marks a restated/disappeared row retained for audit.",
        "PERSON_LINK_METHOD": "How PERSON_ID was resolved: MRN_ALIAS first, NHS_ALIAS fallback, or null when unresolved.",
        "DMD_RAW": "Verbatim source dm+d value; includes junk values such as No SNOMED.",
        "DMD_CODE": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
        "DMD_MAPPING_STATUS": "dm+d mapping state: NO_CODE, CODE_UNMAPPED, MAPPED_STANDARD, or MAPPED_DMD_ONLY.",
        "DRUG_STANDARD_CONCEPT_ID": "OMOP standard concept reached only when exactly one valid Maps-to target exists.",
        "HCDR_CATEGORY_CD": "Local high-cost-drug reimbursement category from source HRGCode; not an HRG4+ code.",
        "SITE_NAME": "Curated mapping for RLH, SBH, WXH, NUH and NGH only; other values are unresolved by design.",
        "EFFECTIVE_DT_TM": "Effective date/time as supplied; two known rows dated 2122 are retained losslessly.",
        "ADC_UPDT": "Pipeline insert/update timestamp.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## map_slam_costed_activity + map_slam_cost_line_item (version-gated)

# COMMAND ----------

def _plics_ep_branch(extract_cd, src_table, has_community):
    community = (
        """e.PartCost AS PARTIAL_COSTING_IND, e.CareDte AS CARE_DT_TM,
           e.CareId AS CARE_ID, e.ClinDuration AS CLINICAL_CONTACT_DURATION,
           e.ChsCcy AS CHS_CURRENCY_CD, CAST(e.TeamType AS INT) AS TEAM_TYPE_CD,
           e.CCSubject AS CONTACT_SUBJECT_CD, e.ConsultType AS CONSULT_TYPE_CD,
           e.CMedium AS CONSULT_MEDIUM_CD, e.LocCode AS LOCATION_CD,
           e.GPTherapyInd AS GP_THERAPY_IND, e.SerReqID AS SERVICE_REQUEST_ID"""
        if has_community else
        """CAST(NULL AS STRING) AS PARTIAL_COSTING_IND, CAST(NULL AS TIMESTAMP) AS CARE_DT_TM,
           CAST(NULL AS STRING) AS CARE_ID, CAST(NULL AS INT) AS CLINICAL_CONTACT_DURATION,
           CAST(NULL AS STRING) AS CHS_CURRENCY_CD, CAST(NULL AS INT) AS TEAM_TYPE_CD,
           CAST(NULL AS STRING) AS CONTACT_SUBJECT_CD, CAST(NULL AS STRING) AS CONSULT_TYPE_CD,
           CAST(NULL AS STRING) AS CONSULT_MEDIUM_CD, CAST(NULL AS STRING) AS LOCATION_CD,
           CAST(NULL AS STRING) AS GP_THERAPY_IND, CAST(NULL AS STRING) AS SERVICE_REQUEST_ID"""
    )
    return f"""
    SELECT '{extract_cd}' AS EXTRACT_CD, e.ActivityRecordID AS ACTIVITY_RECORD_ID,
        e.FeedType AS FEED_TYPE, e.PLEMI AS PLEMI,
        e.NHSNo AS _NHSNO, e.NhsSt AS NHS_NUMBER_STATUS_CD,
        trim(e.CDSID) AS CDS_ID, e.AttID AS ATTENDANCE_ID,
        e.ActivityStartDate AS ACTIVITY_START_DT_TM,
        e.ActivityEndDate AS ACTIVITY_END_DT_TM,
        e.ArrDate AS ARRIVAL_DT, e.ArrTime AS ARRIVAL_TM,
        e.DepDate AS DEPARTURE_DT, e.DepTime AS DEPARTURE_TM,
        e.DepTyp AS DEPARTURE_TYPE_CD,
        e.OrgId AS PROVIDER_ORG_CD, e.PatOrgId AS PATIENT_ORG_CD,
        e.PathID AS PATHWAY_ID, e.Pod AS POD_CD,
        e.Tfc AS TREATMENT_FUNCTION_CD, e.Alos AS SOURCE_LOS,
        e.CFBand AS CF_BAND_CD, e.EpiNo AS EPISODE_NUMBER,
        e.EpStDte AS EPISODE_START_DT_TM, e.EpEnDte AS EPISODE_END_DT_TM,
        e.EpType AS EPISODE_TYPE_CD, e.HSpellNo AS HOSP_SPELL_ID,
        e.HRG AS HRG_CD, e.HrgFce AS FCE_HRG_CD, e.HrgSpl AS SPELL_HRG_CD,
        e.AppDate AS APPOINTMENT_DT, e.AppTime AS APPOINTMENT_TM,
        e.CCUF AS CRITICAL_CARE_UNIT_FUNCTION_CD, e.OrgsSupp AS ORGANS_SUPPORTED,
        e.CCPerType AS CRITICAL_CARE_PERIOD_TYPE_CD, e.CCLI AS CRITICAL_CARE_LEVEL_IND,
        e.UnActDate AS UNBUNDLED_ACTIVITY_DT_TM, e.UnAct AS UNBUNDLED_ACTIVITY_CD,
        e.UnHRG AS UNBUNDLED_HRG_CD,
        {community}
    FROM {src_table} e
    """

def _plics_cost_branch(extract_cd, src_table):
    return f"""
    SELECT '{extract_cd}' AS EXTRACT_CD, c.ActivityRecordID AS ACTIVITY_RECORD_ID,
        c.ActCstID AS ACTIVITY_COST_ITEM_CD, c.ResCstID AS RESOURCE_COST_ITEM_CD,
        c.ActCnt AS ACTIVITY_COUNT, c.CTPCSIU AS UNBUNDLED_SUBTYPE_CD,
        c.UnCur AS UNBUNDLED_CURRENCY_CD, c.CTPUnCurDate AS UNBUNDLED_CURRENCY_DT_TM,
        c.TotCst AS TOTAL_COST, c.TotOCst AS TOTAL_O_COST
    FROM {src_table} c
    """

def _plics_build():
    ep_union = " UNION ALL ".join(
        _plics_ep_branch(x, SRC_PLICS[x][0], x == "PE2122NCC") for x in SRC_PLICS
    )
    cost_union = " UNION ALL ".join(
        _plics_cost_branch(x, SRC_PLICS[x][1]) for x in SRC_PLICS
    )
    # cost lines first: exact duplicates collapsed, deterministic line hash
    spark.sql(f"""
        CREATE OR REPLACE TABLE {T_COST}
        TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
        WITH unioned AS ({cost_union})
        SELECT EXTRACT_CD, ACTIVITY_RECORD_ID,
            sha2(concat_ws('||',
                coalesce(ACTIVITY_COST_ITEM_CD,''), coalesce(RESOURCE_COST_ITEM_CD,''),
                coalesce(ACTIVITY_COUNT,''), coalesce(UNBUNDLED_SUBTYPE_CD,''),
                coalesce(UNBUNDLED_CURRENCY_CD,''),
                coalesce(CAST(UNBUNDLED_CURRENCY_DT_TM AS STRING),''),
                coalesce(CAST(TOTAL_COST AS STRING),''),
                coalesce(CAST(TOTAL_O_COST AS STRING),'')
            ), 256) AS LINE_HASH,
            ACTIVITY_COST_ITEM_CD, RESOURCE_COST_ITEM_CD, ACTIVITY_COUNT,
            UNBUNDLED_SUBTYPE_CD, UNBUNDLED_CURRENCY_CD, UNBUNDLED_CURRENCY_DT_TM,
            TOTAL_COST, TOTAL_O_COST,
            COUNT(*) AS SOURCE_DUPLICATE_COUNT,
            current_timestamp() AS ADC_UPDT
        FROM unioned
        GROUP BY ALL
    """)
    # activity spine with 3-stage linkage + cost aggregates
    spark.sql(f"""
        CREATE OR REPLACE TABLE {T_ACT}
        TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
        WITH {person_alias_cte(ALIAS_TYPE_NHS, "nhs_person")},
        {person_alias_cte(ALIAS_TYPE_MRN, "mrn_person")},
        cds_mrn AS (
            SELECT K, M FROM (
                SELECT trim(CDS_APC_Id) AS K, trim(MRN) AS M,
                       ROW_NUMBER() OVER (PARTITION BY trim(CDS_APC_Id)
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {SRC_APC} WHERE CDS_APC_Id IS NOT NULL
            ) WHERE rn = 1
            UNION ALL
            SELECT K, M FROM (
                SELECT trim(CDS_OPA_Id) AS K, trim(MRN) AS M,
                       ROW_NUMBER() OVER (PARTITION BY trim(CDS_OPA_Id)
                           ORDER BY ADC_UPDT DESC NULLS LAST) rn
                FROM {SRC_OP} WHERE CDS_OPA_Id IS NOT NULL
            ) WHERE rn = 1
        ),
        unioned AS ({ep_union}),
        costs AS (
            SELECT EXTRACT_CD, ACTIVITY_RECORD_ID,
                   SUM(SOURCE_DUPLICATE_COUNT) AS COST_LINE_COUNT,
                   SUM(
                       CAST(TOTAL_COST AS DECIMAL(31,18)) *
                       CAST(SOURCE_DUPLICATE_COUNT AS DECIMAL(6,0))
                   ) AS TOTAL_COST_SUM,
                   SUM(
                       CAST(TOTAL_O_COST AS DECIMAL(31,18)) *
                       CAST(SOURCE_DUPLICATE_COUNT AS DECIMAL(6,0))
                   ) AS TOTAL_O_COST_SUM
            FROM {T_COST} GROUP BY 1, 2
        )
        SELECT
            coalesce(pn.PERSON_ID, pm2.PERSON_ID) AS PERSON_ID,
            CASE WHEN pn.PERSON_ID IS NOT NULL THEN 'NHS_ALIAS'
                 WHEN pm2.PERSON_ID IS NOT NULL THEN 'CDS_MRN' END AS PERSON_LINK_METHOD,
            u.EXTRACT_CD, u.ACTIVITY_RECORD_ID, u.FEED_TYPE, u.PLEMI,
            u.NHS_NUMBER_STATUS_CD, u.CDS_ID, u.ATTENDANCE_ID,
            u.ACTIVITY_START_DT_TM, u.ACTIVITY_END_DT_TM,
            u.ARRIVAL_DT, u.ARRIVAL_TM, u.DEPARTURE_DT, u.DEPARTURE_TM,
            u.DEPARTURE_TYPE_CD, u.PROVIDER_ORG_CD, u.PATIENT_ORG_CD,
            u.PATHWAY_ID, u.POD_CD, u.TREATMENT_FUNCTION_CD, u.SOURCE_LOS,
            u.CF_BAND_CD, u.EPISODE_NUMBER, u.EPISODE_START_DT_TM,
            u.EPISODE_END_DT_TM, u.EPISODE_TYPE_CD, u.HOSP_SPELL_ID,
            u.HRG_CD, h.HRG_Desc AS HRG_DESC,
            u.FCE_HRG_CD, hf.HRG_Desc AS FCE_HRG_DESC,
            u.SPELL_HRG_CD, hs.HRG_Desc AS SPELL_HRG_DESC,
            u.APPOINTMENT_DT, u.APPOINTMENT_TM,
            u.CRITICAL_CARE_UNIT_FUNCTION_CD, u.ORGANS_SUPPORTED,
            u.CRITICAL_CARE_PERIOD_TYPE_CD, u.CRITICAL_CARE_LEVEL_IND,
            u.UNBUNDLED_ACTIVITY_DT_TM, u.UNBUNDLED_ACTIVITY_CD,
            u.UNBUNDLED_HRG_CD, hu.HRG_Desc AS UNBUNDLED_HRG_DESC,
            u.PARTIAL_COSTING_IND, u.CARE_DT_TM, u.CARE_ID,
            u.CLINICAL_CONTACT_DURATION, u.CHS_CURRENCY_CD, u.TEAM_TYPE_CD,
            u.CONTACT_SUBJECT_CD, u.CONSULT_TYPE_CD, u.CONSULT_MEDIUM_CD,
            u.LOCATION_CD, u.GP_THERAPY_IND, u.SERVICE_REQUEST_ID,
            c.COST_LINE_COUNT, c.TOTAL_COST_SUM, c.TOTAL_O_COST_SUM,
            current_timestamp() AS ADC_UPDT
        FROM unioned u
        LEFT JOIN nhs_person pn
               ON regexp_replace(u._NHSNO, ' ', '') = pn.ALIAS
        LEFT JOIN cds_mrn cm ON u.CDS_ID = cm.K
        LEFT JOIN mrn_person pm2 ON cm.M = pm2.ALIAS
        LEFT JOIN costs c ON u.EXTRACT_CD = c.EXTRACT_CD
                         AND u.ACTIVITY_RECORD_ID = c.ACTIVITY_RECORD_ID
        LEFT JOIN {LKP_HRG} h  ON u.HRG_CD = h.HRG_Cd
        LEFT JOIN {LKP_HRG} hf ON u.FCE_HRG_CD = hf.HRG_Cd
        LEFT JOIN {LKP_HRG} hs ON u.SPELL_HRG_CD = hs.HRG_Cd
        LEFT JOIN {LKP_HRG} hu ON u.UNBUNDLED_HRG_CD = hu.HRG_Cd
    """)

if RUN_PLICS:
    plics_srcs = [t for pair in SRC_PLICS.values() for t in pair]
    versions = {t: source_version(t) for t in plics_srcs}
    changed = any(
        get_state_version(CONTROL_SCHEMA, T_ACT, t) != v
        for t, v in versions.items()
    )
    if FORCE_FULL or changed or not table_exists(T_ACT) or not table_exists(T_COST):
        _plics_build()
        mode = "VERSION_REBUILD"
    else:
        mode = "UNCHANGED_SKIP"
    n_act = spark.table(T_ACT).count() if table_exists(T_ACT) else 0
    n_cost = spark.table(T_COST).count() if table_exists(T_COST) else 0
    _results.append({"table": T_ACT, "mode": mode, "rows": n_act})
    _results.append({"table": T_COST, "mode": mode, "rows": n_cost})
    print(f"[SLAM-FIN] PLICS {mode} activity={n_act} cost_lines={n_cost}")
else:
    _results.append({"table": T_ACT, "mode": "SKIPPED"})
    _results.append({"table": T_COST, "mode": "SKIPPED"})

# COMMAND ----------

if RUN_PLICS and table_exists(T_ACT) and table_exists(T_COST):
    assert_no_direct_identifiers(T_ACT)
    assert_no_direct_identifiers(T_COST)
    apply_table_comment(T_ACT, (
        "PLICS patient-level costed activity, combining the frozen PE2103 "
        "(FY2019/20–2020/21) and PE2122 NCC (FY2021/22) R1H extracts from "
        "4_prod.slam. The key is (EXTRACT_CD, ACTIVITY_RECORD_ID); "
        "ACTIVITY_RECORD_ID and PLEMI are reused across extracts and must never "
        "be joined alone. Person linkage uses NHS-number alias first, then a "
        "CDS-to-MRN fallback through the SLAM HRG feeds; direct identifiers are "
        "not published and PERSON_LINK_METHOD records the path. "
        "COST_LINE_COUNT, TOTAL_COST_SUM and TOTAL_O_COST_SUM are duplicate-"
        "weighted aggregates of map_slam_cost_line_item. Frozen sources are "
        "rebuilt only when a source Delta version changes."
    ))
    apply_table_comment(T_COST, (
        "PLICS cost line items; join to costed activity on "
        "(EXTRACT_CD, ACTIVITY_RECORD_ID). Exact-duplicate source lines are "
        "collapsed with SOURCE_DUPLICATE_COUNT and LINE_HASH is the content-hash "
        "line key. Costs retain exact decimal(36,18) values. "
        "ACTIVITY_COST_ITEM_CD and RESOURCE_COST_ITEM_CD are National Cost "
        "Collection dictionary codes preserved without invented mappings. "
        "TOTAL_O_COST is a component of TOTAL_COST."
    ))
    apply_column_comments(T_ACT, {
        "EXTRACT_CD": "Source costing extract discriminator; required as part of every activity join key.",
        "PLEMI": "PLICS extract matching identifier; reused across extracts and must not be joined without EXTRACT_CD.",
        "NHS_NUMBER_STATUS_CD": "Non-identifying source quality/status code for the NHS number used during linkage.",
        "POD_CD": "Source point-of-delivery classification; preserved without an invented mapping.",
        "CF_BAND_CD": "Cystic Fibrosis year-of-care currency band supplied by the source.",
        "ADC_UPDT": "Pipeline build/update timestamp.",
    })
    apply_column_comments(T_COST, {
        "EXTRACT_CD": "Source costing extract discriminator; required as part of every activity join key.",
        "UNBUNDLED_SUBTYPE_CD": "Observed values: 1 high-cost drug, 2 device, 3 diagnostic/other, 0 default; null on ordinary lines.",
        "ADC_UPDT": "Pipeline build/update timestamp.",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation gate → state commit → exit

# COMMAND ----------

def _check(name, sql, ok_fn):
    row = spark.sql(sql).collect()[0].asDict()
    ok = ok_fn(row)
    print(f"[VALIDATE] {'PASS' if ok else 'FAIL'} {name}: {row}")
    return {"check": name, "ok": ok, **row}

_validation = []
if RUN_SLAM_HRG:
    _validation.append(_check("apc_keys_unique",
        f"SELECT COUNT(*) c, COUNT(DISTINCT CDS_APC_ID) k FROM {T_APC}",
        lambda r: r["c"] == r["k"] and r["c"] > 0))
    _validation.append(_check("op_keys_unique",
        f"SELECT COUNT(*) c, COUNT(DISTINCT CDS_OPA_ID) k FROM {T_OP}",
        lambda r: r["c"] == r["k"] and r["c"] > 0))
if RUN_FINANCE and table_exists(T_FIN):
    _validation.append(_check("fin_source_reconcile",
        f"""SELECT (SELECT SUM(SOURCE_DUPLICATE_COUNT) FROM {T_FIN}
                    WHERE SOURCE_PRESENT_IND) a,
                   (SELECT COUNT(*) FROM {SRC_FIN}) b""",
        lambda r: r["a"] == r["b"]))
    _validation.append(_check("fin_cost_conserved",
        f"""SELECT round((SELECT SUM(COST * SOURCE_DUPLICATE_COUNT) FROM {T_FIN}
                    WHERE SOURCE_PRESENT_IND), 2) a,
                   round((SELECT SUM(Cost) FROM {SRC_FIN}), 2) b""",
        lambda r: r["a"] == r["b"]))
if RUN_PLICS and table_exists(T_COST):
    _validation.append(_check("plics_no_orphans",
        f"""SELECT COUNT(*) c FROM {T_COST} c
            LEFT ANTI JOIN {T_ACT} e
              ON c.EXTRACT_CD = e.EXTRACT_CD
             AND c.ACTIVITY_RECORD_ID = e.ACTIVITY_RECORD_ID""",
        lambda r: r["c"] == 0))

_all_ok = all(v["ok"] for v in _validation)
assert _all_ok, f"Validation failed: {[v['check'] for v in _validation if not v['ok']]}"

# state commit — only reached when validation passed
if RUN_FINANCE and table_exists(T_FIN):
    set_state_version(CONTROL_SCHEMA, T_FIN, SRC_FIN, source_version(SRC_FIN))
if RUN_PLICS and table_exists(T_ACT):
    for pair in SRC_PLICS.values():
        for t in pair:
            set_state_version(CONTROL_SCHEMA, T_ACT, t, source_version(t))
for r in _results:
    write_audit(CONTROL_SCHEMA, RUN_ID, r["table"], r.get("mode", "?"),
                r.get("rows", 0), _validation)

_summary = {
    "status": "SUCCESS", "pipeline": "slam_finance_pipeline",
    "run_id": RUN_ID, "target_schema": TARGET_SCHEMA,
    "started_at": _started_at, "completed_at": sf_utc_now(),
    "force_full_refresh": FORCE_FULL,
    "results": _results, "validation": _validation,
}
print(json.dumps(_summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(_summary, default=str))

