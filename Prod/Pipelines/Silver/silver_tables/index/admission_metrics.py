"""Silver index.admission_metrics: 1 dataset(s).

- datasets: reference_admission_metrics
- moved verbatim from: 1_silver_spine_and_reference:3789, 1_silver_spine_and_reference:3854, 1_silver_spine_and_reference:4170

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, _present, _usable_code, read_source, stable_id, subject_key_with_system
from silver_tables._common.journey import (
    SRC_NUMERIC_EVENTS,
    SRC_PATHOLOGY_REPORT_VERSIONS,
    SRC_PHARMACY_ISSUE,
    _allergy_canonical_pregate,
    _baby_delivery_canonical,
    _cancer_treatment_canonical_pregate,
    _cc_daily_score_canonical_pregate,
    _cc_procedure_canonical_pregate,
    _coded_finding_canonical,
    _community_care_activity_canonical_pregate,
    _community_care_contact_canonical,
    _condition_diagnosis_canonical_pregate,
    _condition_problem_canonical_pregate,
    _condition_stage_canonical_pregate,
    _costed_activity_canonical,
    _critical_care_activity_canonical_pregate,
    _critical_care_admission_canonical,
    _critical_care_period_canonical,
    _date_finding_canonical,
    _device_canonical_pregate,
    _drug_expenditure_canonical,
    _elective_access_comment_document_canonical,
    _elective_access_entry_canonical,
    _endobase_document_canonical,
    _endobase_procedure_canonical_pregate,
    _endoscopy_finding_canonical_pregate,
    _family_history_canonical_pregate,
    _form_canonical_pregate,
    _genomic_result_canonical_pregate,
    _genomic_test_canonical_pregate,
    _hrg_grouping_canonical,
    _imaging_exam_canonical_pregate,
    _implant_procedure_canonical_pregate,
    _indication_canonical_pregate,
    _labour_delivery_canonical,
    _maternity_care_contact_canonical,
    _maternity_diagnosis_canonical_pregate,
    _medication_admin_canonical_pregate,
    _medication_dispense_canonical_pregate,
    _medication_order_canonical_pregate,
    _medication_supply_canonical_pregate,
    _micro_isolate_canonical_pregate,
    _mill_radiology_exam_canonical_pregate,
    _neonatal_care_day_canonical_pregate,
    _neonatal_episode_canonical,
    _neonatal_examination_canonical_pregate,
    _neonatal_narrative_document_canonical,
    _nomen_finding_canonical,
    _order_comment_document_canonical,
    _pathology_report_document_base,
    _pathology_report_document_canonical,
    _pathology_report_series_canonical,
    _pathology_requested_test_canonical_pregate,
    _pathology_result_canonical_pregate,
    _pathology_specimen_canonical,
    _pathway_tracking_canonical,
    _procedure_canonical_pregate,
    _promoted_score_from_stage,
    _promoted_vital_from_stage,
    _registry_entry_canonical,
    _susceptibility_canonical_pregate,
    _text_finding_canonical,
    _theatre_procedure_canonical_pregate,
    _transfusion_canonical_pregate,
)
from silver_tables._common.spine_and_reference import _research_enrollment_canonical
from silver_tables.activity.pregnancy_reconciliation import _pregnancy_reconciliation_source
from silver_tables.clinical._shared import _numeric_route_expr
from silver_tables.clinical.numeric_clinical_score import _numeric_score_canonical
from silver_tables.clinical.numeric_vital_sign import _numeric_vital_canonical
from silver_tables.documents.document import _pathology_report_document_history_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / admission metrics
def _pharmacy_issue_canonical():
    # Assemble normalized pharmacy issue rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_PHARMACY_ISSUE)
    event_id = stable_id("pharmacy_issue:jac", s.PHARMACY_ISSUE_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID), ("urn:jac:lnkpid", s.LNKPID)],
        SRC_PHARMACY_ISSUE,
        s.PHARMACY_ISSUE_ID,
    )
    retracted = F.coalesce(~s.SOURCE_PRESENT_IND, F.lit(False))
    promoted = F.upper(F.trim(F.coalesce(s.ISSUE_CATEGORY, F.lit("")))) == F.lit("ISSUE")
    payload = F.parse_json(F.to_json(F.struct(*[s[c] for c in s.columns])))
    return s.select(
        event_id.alias("patient_event_id"),
        event_id.alias("fact_row_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("long").cast("string").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(s.LNKPID.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("string").alias("encounter_id"),
        s.ISSUE_DTTM.alias("event_datetime"),
        F.lit(None).cast("timestamp").alias("event_end_datetime"),
        s.ISSUE_DTTM.alias("event_datetime_local"),
        F.lit(None).cast("timestamp").alias("event_end_datetime_local"),
        F.lit("urn:jac:issue_type").alias("source_coding_system"),
        s.ISSUE_TYPE.alias("source_code"),
        s.ISSUE_CATEGORY.alias("source_display"),
        s.DRUG_FULL.alias("value_text"),
        s.TOTAL_UNITS.cast("decimal(38,4)").alias("value_number"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        s.DRUG_DOSEUNIT.alias("unit"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        s.SOURCE_RECORD_UPDATED_DT.alias("record_status_effective_from"),
        F.when(retracted, s.ADC_UPDT).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.parse_json(F.lit("null")).alias("sensitivity_labels"),
        F.lit("pharmacy_issue").alias("feed_id"),
        payload.alias("payload"),
        F.when(promoted, F.lit("journey_clinical.medication_dispense"))
        .alias("promoted_to_table"),
        F.when(promoted, event_id).alias("promoted_to_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.SOURCE_RECORD_UPDATED_DT.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("jac").alias("_source_system"),
        F.lit(SRC_PHARMACY_ISSUE).alias("_source_table"),
        s.PHARMACY_ISSUE_ID.alias("_source_row_id"),
        s.DMD_VTM_CODE.alias("_dmd_code"),
        s.DMD_VTM_NAME.alias("_dmd_display"),
    )


@materialized_view(
    name=_n("journey_reference.admission_metrics"),
    refresh_policy="auto",
)
def admission_metrics():
    # contract v2: admission metrics remain identifier-free and require no identifier projection change
    # Build the declared dataset: Admission-gate accounting: rows seen per registered route with
    # admitted flag. route_id doubles as the exclusion reason for admitted=false rows. Product-
    # wide usable-code gate: all source_code-bearing fact lanes report excluded_unusable_code;
    # document (narrative route) and the structured admin facts without source_code are exempt;
    # registry_entry is exempt as a code-free class. Exclusions are counted here and retained
    # nowhere.
    def lane(df, feed_id, route_col, admitted_col):
        # Project feed, route, admission decision and load date into the common coverage-metric
        # schema.
        return df.select(
            F.lit(feed_id).alias("feed_id"),
            route_col.cast("string").alias("route_id"),
            admitted_col.alias("admitted"),
            F.to_date(F.col("loaded_at")).alias("load_date"),
            F.col("loaded_at"),
        )

    def typed_route(df):
        # Classify finding rows as admitted, untyped or unusably coded for coverage accounting.
        gated = F.col("_typed_route") & _usable_code(F.col("source_code"))
        route = (
            F.when(gated, F.lit("clinical_finding"))
             .when(F.col("_typed_route"), F.lit("excluded_unusable_code"))
             .otherwise(F.lit("excluded_untyped"))
        )
        return df.withColumn("_metric_route", route), gated

    def fact_lane(df, feed_id, fact_route):
        # Mark fact rows as admitted or excluded for unusable source codes, then project their
        # coverage metrics.
        ok = _usable_code(F.col("source_code"))
        route = F.when(ok, F.lit(fact_route)).otherwise(F.lit("excluded_unusable_code"))
        return lane(df, feed_id, route, ok)

    coded_df, coded_ok = typed_route(_coded_finding_canonical())
    nomen_df, nomen_ok = typed_route(_nomen_finding_canonical())
    date_df, date_ok = typed_route(_date_finding_canonical())
    text_gated = _usable_code(F.col("source_code"))
    text_route = (
        F.when(F.col("_route") == "annex_catch_all", F.lit("excluded_untyped"))
         .when(~text_gated, F.lit("excluded_unusable_code"))
         .otherwise(F.col("_route"))
    )
    rows = (
        lane(coded_df, "coded_events", F.col("_metric_route"), coded_ok)
        .unionByName(lane(nomen_df, "nomen_events", F.col("_metric_route"), nomen_ok))
        .unionByName(lane(date_df, "date_events", F.col("_metric_route"), date_ok))
        .unionByName(lane(
            _text_finding_canonical(), "text_events", text_route,
            (F.col("_route") != "annex_catch_all") & text_gated,
        ))
        .unionByName(lane(
            _numeric_vital_canonical(), "numeric_events",
            F.when(_usable_code(F.col("source_code")), F.lit("vital_sign"))
             .otherwise(F.lit("excluded_unusable_code")),
            _usable_code(F.col("source_code")),
        ))
        .unionByName(lane(
            _numeric_score_canonical(), "numeric_events",
            F.when(_usable_code(F.col("source_code")), F.lit("clinical_score"))
             .otherwise(F.lit("excluded_unusable_code")),
            _usable_code(F.col("source_code")),
        ))
        .unionByName(fact_lane(
            _promoted_vital_from_stage(), "form_promotion", "vital_sign"
        ))
        .unionByName(fact_lane(
            _promoted_score_from_stage(), "form_promotion", "clinical_score"
        ))
        .unionByName(lane(
            _numeric_excluded_canonical(), "numeric_events",
            F.lit("excluded_residual"), F.lit(False),
        ))
        .unionByName(lane(
            _pharmacy_issue_canonical().where(
                F.upper(F.trim(F.coalesce(F.col("source_display"), F.lit("")))) != "ISSUE"
            ),
            "pharmacy_issue", F.lit("excluded_non_issue_audit"), F.lit(False),
        ))
        .unionByName(fact_lane(
            _medication_dispense_canonical_pregate(), "pharmacy_issue", "medication_dispense"
        ))
        .unionByName(fact_lane(
            _family_history_canonical_pregate(), "family_history", "family_history"
        ))
        .unionByName(fact_lane(
            _allergy_canonical_pregate(), "allergy", "allergy_intolerance"
        ))
        .unionByName(fact_lane(
            _transfusion_canonical_pregate(), "bloodtrack", "transfusion"
        ))
        .unionByName(fact_lane(
            _cancer_treatment_canonical_pregate(), "cancer_treatment", "cancer_treatment"
        ))
        .unionByName(fact_lane(
            _condition_stage_canonical_pregate().where(F.col("_source_system") == "aria"), "aria_staging", "condition_stage"
        ))
        .unionByName(fact_lane(  # INSR_W2 W2.20 (W1.14 Millennium TNM arm)
            _condition_stage_canonical_pregate().where(F.col("_source_system") == "millennium"), "cancer_stage_mill",
            "condition_stage"
        ))
        .unionByName(fact_lane(
            _endobase_procedure_canonical_pregate(), "endobase_exam", "procedure"
        ))
        .unionByName(fact_lane(
            _endoscopy_finding_canonical_pregate(), "endobase_exam_term", "endoscopy_finding"
        ))
        .unionByName(lane(
            _endobase_document_canonical(), "endobase_exam",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _order_comment_document_canonical(), "order_comment",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _elective_access_comment_document_canonical(), "elective_access_comment",
            F.lit("document"), F.lit(True),
        ))
        .unionByName(lane(
            _registry_entry_canonical(), "iweb_registry",
            F.col("registry_family"), F.lit(True),
        ))
        .unionByName(fact_lane(
            _device_canonical_pregate(), "mediconnect_device", "device"
        ))
        .unionByName(fact_lane(
            _condition_diagnosis_canonical_pregate(), "diagnosis", "condition"
        ))
        .unionByName(fact_lane(
            _condition_problem_canonical_pregate(), "problem", "condition"
        ))
        .unionByName(fact_lane(
            _procedure_canonical_pregate(), "procedure", "procedure"
        ))
        .unionByName(fact_lane(
            _implant_procedure_canonical_pregate(), "implant_details", "procedure"
        ))
        .unionByName(fact_lane(
            _theatre_procedure_canonical_pregate(), "theatre_case", "procedure"
        ))
        .unionByName(fact_lane(
            _pathology_result_canonical_pregate(), "pathology", "pathology_result"
        ))
        .unionByName(fact_lane(
            _pathology_requested_test_canonical_pregate(),
            "pathology_requested_test", "pathology_order"
        ))
        .unionByName(fact_lane(
            _genomic_test_canonical_pregate(), "pathology_genetic_test", "genomic_test"
        ))
        .unionByName(fact_lane(
            _genomic_result_canonical_pregate(), "pathology_genetic_result", "genomic_result"
        ))
        .unionByName(fact_lane(
            _indication_canonical_pregate(), "pathology_indication", "indication"
        ))
        .unionByName(fact_lane(
            _micro_isolate_canonical_pregate(), "pathology_microbiology_isolate", "microbiology_isolate"
        ))
        .unionByName(fact_lane(
            _susceptibility_canonical_pregate(), "pathology_antimicrobial_susceptibility", "susceptibility_result"
        ))
        .unionByName(lane(
            _pathology_specimen_canonical(), "pathology_accession",
            F.lit("specimen"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_series_canonical(), "pathology_report",
            F.lit("pathology_report"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_canonical(), "pathology_report",
            F.lit("document"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_history_canonical(), "pathology_report",
            F.lit("document_history"), F.lit(True)
        ))
        .unionByName(lane(
            _pathology_report_document_base(
                read_source(SRC_PATHOLOGY_REPORT_VERSIONS).where(
                    F.col("report_text").isNull() | (F.trim(F.col("report_text")) == "")
                )
            ),
            "pathology_report", F.lit("excluded_no_text"), F.lit(False)
        ))
        .unionByName(fact_lane(
            _form_canonical_pregate(), "form_activity", "form"
        ))
        .unionByName(fact_lane(
            _medication_admin_canonical_pregate(), "med_admin", "medication_admin"
        ))
        .unionByName(fact_lane(
            _medication_order_canonical_pregate(), "medication_order", "medication_order"
        ))
        .unionByName(fact_lane(
            _imaging_exam_canonical_pregate(), "pacs_examination", "imaging_exam"
        ))
        .unionByName(fact_lane(
            _community_care_activity_canonical_pregate(),
            "community_care_activity", "community_care_activity"
        ))
        .unionByName(lane(
            _community_care_contact_canonical(), "community_care_contact",
            F.lit("community_care_contact"), F.lit(True)
        ))
        .unionByName(lane(
            _hrg_grouping_canonical(), "slam_apc_hrg", F.col("source_feed"), F.lit(True)
        ).where(F.col("route_id") == "slam_apc_hrg"))
        .unionByName(lane(
            _hrg_grouping_canonical(), "slam_op_hrg", F.col("source_feed"), F.lit(True)
        ).where(F.col("route_id") == "slam_op_hrg"))
        .unionByName(lane(
            _costed_activity_canonical(), "slam_costed_activity",
            F.lit("costed_activity"), F.lit(True)
        ))
        .unionByName(lane(
            _drug_expenditure_canonical(), "finance_hcd_expenditure",
            F.lit("drug_expenditure"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _medication_supply_canonical_pregate(), "homecare_request", "medication_supply"
        ))
        .unionByName(lane(
            _elective_access_entry_canonical(), "elective_access_list",
            F.lit("elective_access_entry"), F.lit(True)
        ))
        .unionByName(lane(
            _pathway_tracking_canonical(), "cancer_ptl",
            F.lit("pathway_tracking"), F.lit(True)
        ))
        .unionByName(lane(
            _critical_care_period_canonical(), "critical_care_period",
            F.lit("critical_care_period"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _critical_care_activity_canonical_pregate(),
            "critical_care_activity", "critical_care_activity"
        ))
        .unionByName(fact_lane(
            _cc_procedure_canonical_pregate(),
            "critical_care_procedure", "procedure"
        ))
        .unionByName(lane(
            _critical_care_admission_canonical(), "critical_care_admission",
            F.lit("critical_care_admission"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _cc_daily_score_canonical_pregate(),
            "critical_care_daily_score", "critical_care_daily_score"
        ))
        .unionByName(lane(
            _neonatal_episode_canonical(), "neonatal_episode",
            F.lit("neonatal_episode"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _neonatal_care_day_canonical_pregate(),
            "neonatal_critical_care", "neonatal_care_day"
        ))
        .unionByName(fact_lane(
            _neonatal_examination_canonical_pregate(),
            "neonatal_examination", "neonatal_examination"
        ))
        .unionByName(lane(
            _neonatal_narrative_document_canonical(), "neonatal_episode_narrative",
            F.lit("document"), F.lit(True)
        ))
        .unionByName(lane(
            _baby_delivery_canonical(), "maternity_baby_delivery",
            F.lit("baby_delivery"), F.lit(True)
        ))
        .unionByName(lane(
            _labour_delivery_canonical(), "maternity_labour_delivery",
            F.lit("labour_delivery"), F.lit(True)
        ))
        .unionByName(lane(
            _maternity_care_contact_canonical(), "maternity_care_contact",
            F.lit("maternity_care_contact"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _maternity_diagnosis_canonical_pregate(),
            "maternity_diagnosis", "condition"
        ))
        .unionByName(lane(
            _research_enrollment_canonical(), "research_subject",
            F.lit("research_enrollment"), F.lit(True)
        ))
        .unionByName(fact_lane(
            _mill_radiology_exam_canonical_pregate(),
            "radiology_event", "imaging_exam"
        ))
        .unionByName(lane(
            _pregnancy_reconciliation_source(), "mat_pregnancy_msds_unmatched",
            F.lit("reconciliation"), F.lit(True)
        ))
    )
    return rows.groupBy("feed_id", "route_id", "admitted", "load_date").agg(
        F.count(F.lit(1)).alias("row_count"),
        F.max("loaded_at").alias("latest_loaded_at"),
    )


def _numeric_excluded_canonical():
    # Assemble normalized numeric excluded rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_NUMERIC_EVENTS)
    s = s.where(_numeric_route_expr(s) == "excluded")
    event_id = stable_id("numeric_event:mill", s.EVENT_ID)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_NUMERIC_EVENTS, s.EVENT_ID
    )
    payload = F.parse_json(F.to_json(F.struct(*[s[c] for c in s.columns])))
    deleted = F.coalesce(s.SOURCE_DELETED_IND, F.lit(False))
    ended = s.CLINICAL_EVENT_VALID_UNTIL_DT_TM.isNotNull() & (
        s.CLINICAL_EVENT_VALID_UNTIL_DT_TM < F.lit("2100-01-01").cast("timestamp")
    )
    return s.select(
        event_id.alias("patient_event_id"), event_id.alias("fact_row_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("string").alias("person_id"),
        F.when(_present(s.PERSON_ID), F.lit("resolved")).otherwise(F.lit("unresolved"))
         .alias("identity_status"),
        F.when(s.ENCNTR_ID.isNotNull(), stable_id("encounter:mill", s.ENCNTR_ID)).alias("encounter_id"),
        F.coalesce(s.PERFORMED_DT_TM, s.EVENT_START_DT_TM).alias("event_datetime"),
        s.EVENT_END_DT_TM.alias("event_end_datetime"),
        F.coalesce(s.PERFORMED_DT_TM_LOCAL, s.EVENT_START_DT_TM_LOCAL).alias("event_datetime_local"),
        s.EVENT_END_DT_TM_LOCAL.alias("event_end_datetime_local"),
        F.lit("urn:cerner:event_cd").alias("source_coding_system"),
        s.EVENT_CD.cast("string").alias("source_code"),
        F.coalesce(s.EVENT_LABEL, s.EVENT_CD_DISPLAY).alias("source_display"),
        s.RESULT_TEXT_EFFECTIVE.alias("value_text"),
        s.NUMERIC_RESULT.cast("decimal(38,10)").alias("value_number"),
        F.lit(None).cast("timestamp").alias("value_datetime"),
        F.coalesce(s.UNIT_OF_MEASURE_DISPLAY, s.RESULT_UNITS_DISPLAY).alias("unit"),
        F.when(deleted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.CLINICAL_EVENT_VALID_FROM_DT_TM.alias("record_status_effective_from"),
        F.when(deleted | ended, s.CLINICAL_EVENT_VALID_UNTIL_DT_TM).alias("record_status_effective_to"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"), F.lit(None).cast("boolean").alias("withheld_identity_ind"),
        F.parse_json(F.lit("null")).alias("sensitivity_labels"),
        F.lit("numeric_event").alias("feed_id"), payload.alias("payload"),
        F.lit(None).cast("string").alias("promoted_to_table"),
        F.lit(None).cast("string").alias("promoted_to_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(s.STRING_RESULT_UPDT_DT_TM, s.CLINICAL_EVENT_UPDT_DT_TM, s.ADC_UPDT)
         .alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("millennium").alias("_source_system"), F.lit(SRC_NUMERIC_EVENTS).alias("_source_table"),
        s.EVENT_ID.cast("string").alias("_source_row_id"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_admission_metrics': {'columns': {'admitted': 'Whether these rows entered silver.',
                                             'feed_id': 'Source-system identifier for the feed associated '
                                                        'with each admission metrics record. It is produced '
                                                        'by the silver transformation and has no direct '
                                                        'bronze-column lineage entry. Its uniqueness is '
                                                        'limited to the originating source namespace; null '
                                                        'means no identifier was supplied.',
                                             'latest_loaded_at': 'Latest bronze load time observed in the '
                                                                 'bucket.',
                                             'load_date': 'Bronze load date bucket for per-load accounting.',
                                             'route_id': 'Route or exclusion reason.',
                                             'row_count': 'Rows in this bucket.'},
                                 'table': 'Admission-gate accounting: rows seen per registered route with '
                                          'admitted flag. route_id doubles as the exclusion reason for '
                                          'admitted=false rows. Product-wide usable-code gate: all '
                                          'source_code-bearing fact lanes report excluded_unusable_code; '
                                          'document (narrative route) and the structured admin facts without '
                                          'source_code are exempt; registry_entry is exempt as a code-free '
                                          'class. Exclusions are counted here and retained nowhere.'}}
