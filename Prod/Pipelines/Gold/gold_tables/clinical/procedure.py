"""Gold clinical.procedure: 2 dataset(s).

- datasets: _qc_clinical_procedure, clinical_procedure
- moved verbatim from: 2_gold_clinical_events:5279, 2_gold_clinical_events:5337, 2_gold_clinical_events:5345, 2_gold_clinical_events:5353, 2_gold_clinical_events:5465, 2_gold_clinical_events:5469, 2_gold_clinical_events:5482

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.procedure ====
# OPCS_QUALIFIER_BLOCK_V1_GOLD: anatomy columns pass through from silver.

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`source_object` AS `source_object`',
    '`procedure_id` AS `procedure_id`',
    '`endobase_exam_id` AS `endobase_exam_id`',
    '`implant_event_id` AS `implant_event_id`',
    '`implant_sequence` AS `implant_sequence`',
    '`surg_case_proc_id` AS `surg_case_proc_id`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    "CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    "CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `event_datetime` END END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`",  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` OR YEAR(CAST(`event_end_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`procedure_code` AS `procedure_code`',
    '`procedure_source_system` AS `procedure_source_system`',  # SRC_LINEAGE_V1
    "CASE WHEN UPPER(TRIM(CAST(`procedure_source_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `procedure_source_code` END AS `procedure_source_code`",  # SRC_LINEAGE_V1
    '`procedure_source_display` AS `procedure_source_display`',  # SRC_LINEAGE_V1
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    "CASE WHEN CAST(`performed_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`performed_start` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `performed_start` END END AS `performed_start`",
    'CASE WHEN `performed_start` IS NOT NULL AND `performed_end` IS NOT NULL AND `performed_start` > `performed_end` OR YEAR(CAST(`performed_end` AS TIMESTAMP)) = 1970 THEN NULL ELSE `performed_end` END AS `performed_end`',
    '`body_site_code` AS `body_site_code`',
    '`body_site_display` AS `body_site_display`',
    "CASE WHEN UPPER(TRIM(CAST(`laterality_code` AS STRING))) = '0' THEN NULL ELSE `laterality_code` END AS `laterality_code`",
    '`laterality_display` AS `laterality_display`',
    '`body_site_method` AS `body_site_method`',
    '`laterality_method` AS `laterality_method`',
    '`opcs_code_role` AS `opcs_code_role`',
    '`opcs_anchor_procedure_id` AS `opcs_anchor_procedure_id`',
    '`performer_practitioner_id` AS `performer_practitioner_id`',
    "CASE WHEN UPPER(TRIM(CAST(`procedure_location_code` AS STRING))) = '0' THEN NULL ELSE `procedure_location_code` END AS `procedure_location_code`",
    '`procedure_location_display` AS `procedure_location_display`',
    '`procedure_note` AS `procedure_note`',
    '`implant_description` AS `implant_description`',
    '`device_code` AS `device_code`',
    '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
    '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
    '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
    '`device_display` AS `device_display`',
    '`manufacturer` AS `manufacturer`',
    '`serial_number` AS `serial_number`',
    '`batch_number` AS `batch_number`',
    '`udi_di` AS `udi_di`',
    '`udi_standard` AS `udi_standard`',
    'CASE WHEN ABS(CAST(`quantity` AS DOUBLE)) > 1e12 THEN NULL ELSE `quantity` END AS `quantity`',
    '`implant_attribute_count` AS `implant_attribute_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_PROCEDURE_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`quantity_parse_rule` AS `quantity_parse_rule`',
    '`procedure_date_basis` AS `procedure_date_basis`',
    '`performed_end_basis` AS `performed_end_basis`',
    '`quantity_parse_status` AS `quantity_parse_status`',
]


# contract v2: QC/batch inputs come from internal _clinical_procedure_metadata; source history stays on the main research table.
CLINICAL_PROCEDURE_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 16,267,993 rows of
    # 16,307,178 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_PROCEDURE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.procedure.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # NDC_DISPLAY_SUFFIX_V1 regression guard: Millennium's retired-term suffix ("... 07-SEP-2022 18:17:27<$>")
    # is stripped upstream in Nomenclature_Pipeline, so this should read zero forever.
    "gold.clinical.procedure.source_display.no_retired_term_suffix":
        "NOT COALESCE(`source_display` LIKE '%<$>', FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 39,185 of 16,307,178 at the profile.
    "gold.clinical.procedure.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 3,038 of
    # 16,307,178 rows (0.0186%) when profiled on 2026-08-24.
    "gold.clinical.procedure.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,009 of 16,307,178 rows (0.00619%) when profiled on 2026-08-24.
    "gold.clinical.procedure.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 41,288 of 16,307,178 rows (0.253%) when profiled on 2026-08-24.
    "gold.clinical.procedure.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # DQ4 G2. Left as a warning because either the code or the person's recorded sex may be
    # the wrong side. Seen on 45 of 16,521,359 rows when profiled on 2026-09-26.
    "gold.clinical.procedure.table.sex_implausible_concept":
        "NOT COALESCE(event_sex_implausible, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PROCEDURE_PERSON_SCOPE_RULES = {
    "gold.clinical.procedure.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PROCEDURE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PROCEDURE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_procedure():
    """Quality-controlled twin of journey_clinical.procedure."""
    df = _qc("clinical_procedure", CLINICAL_PROCEDURE_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_procedure():
    """Contract-v2 public twin of clinical_procedure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_procedure")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`source_object` AS `source_object`',
        '`procedure_id` AS `procedure_id`',
        '`endobase_exam_id` AS `endobase_exam_id`',
        '`implant_event_id` AS `implant_event_id`',
        '`implant_sequence` AS `implant_sequence`',
        '`surg_case_proc_id` AS `surg_case_proc_id`',
        '`source_row_hash` AS `source_row_hash`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_procedure_code` AS `procedure_code`',
        '`procedure_source_system` AS `procedure_source_system`',  # SRC_LINEAGE_V1
        '`procedure_source_code` AS `procedure_source_code`',  # SRC_LINEAGE_V1
        '`procedure_source_display` AS `procedure_source_display`',  # SRC_LINEAGE_V1
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`performed_start` AS `performed_start`',
        '`performed_end` AS `performed_end`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_display` AS `body_site_display`',
        '`laterality_code` AS `laterality_code`',
        '`laterality_display` AS `laterality_display`',
        '`body_site_method` AS `body_site_method`',
        '`laterality_method` AS `laterality_method`',
        '`opcs_code_role` AS `opcs_code_role`',
        '`opcs_anchor_procedure_id` AS `opcs_anchor_procedure_id`',
        '`performer_practitioner_id` AS `performer_practitioner_id`',
        '`procedure_location_code` AS `procedure_location_code`',
        '`procedure_location_display` AS `procedure_location_display`',
        '`procedure_note` AS `procedure_note`',
        '`implant_description` AS `implant_description`',
        '`__gold_json_device_code` AS `device_code`',
        '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
        '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
        '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
        '`device_display` AS `device_display`',
        '`manufacturer` AS `manufacturer`',
        '`serial_number` AS `serial_number`',
        '`batch_number` AS `batch_number`',
        '`udi_di` AS `udi_di`',
        '`udi_standard` AS `udi_standard`',
        '`quantity` AS `quantity`',
        '`implant_attribute_count` AS `implant_attribute_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`quantity_parse_rule` AS `quantity_parse_rule`',  # INSR_W3 W3.12
        '`procedure_date_basis` AS `procedure_date_basis`',  # INSR_W3 W3.12
        '`performed_end_basis` AS `performed_end_basis`',  # INSR_W3 W3.12
        '`quantity_parse_status` AS `quantity_parse_status`',  # INSR_W3 W3.12
    )
    return _s3_flatten_gold_public(df, "clinical_procedure")


def _public_steps_clinical_procedure(df):
    """clinical_procedure: the public body after its projection (GoldRules.public_select), moved from gold_clinical_procedure; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_procedure')


RULES = {
    'clinical_procedure': GoldRules(
        table='clinical_procedure',
        select=CLINICAL_PROCEDURE_SELECT,
        mandatory=CLINICAL_PROCEDURE_MANDATORY_RULES,
        advisory=CLINICAL_PROCEDURE_ADVISORY_RULES,
        person_scope=CLINICAL_PROCEDURE_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'encounter_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        sex_plausibility='procedure_omop_concept_id',
        variant_columns=('procedure_code', 'device_code'),
        s3_axis_specs={'procedure_code': [('procedure', False)], 'device_code': [('device', False)]},
        value_rules={'gold.clinical.procedure.device_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'device_snomed_code',
                                                                              'companions': ['device_snomed_display',
                                                                                             'device_target_code',
                                                                                             'device_target_display'],
                                                                              'match': '`device_snomed_code` '
                                                                                       'IS NOT NULL AND '
                                                                                       '(NOT '
                                                                                       '`device_snomed_code` '
                                                                                       'RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`device_snomed_code`, '
                                                                                       'length(`device_snomed_code`) '
                                                                                       '- 2, 2) NOT IN '
                                                                                       "('00', '10') OR "
                                                                                       '(substring(`device_snomed_code`, '
                                                                                       'length(`device_snomed_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`device_snomed_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_procedure'},
         'gold.clinical.procedure.device_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'device_target_code',
                                                                              'companions': ['device_target_display',
                                                                                             'device_snomed_code',
                                                                                             'device_snomed_display'],
                                                                              'match': '`device_target_system` '
                                                                                       '= '
                                                                                       "'http://snomed.info/sct' "
                                                                                       'AND '
                                                                                       '`device_target_code` '
                                                                                       'IS NOT NULL AND '
                                                                                       '(NOT '
                                                                                       '`device_target_code` '
                                                                                       'RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`device_target_code`, '
                                                                                       'length(`device_target_code`) '
                                                                                       '- 2, 2) NOT IN '
                                                                                       "('00', '10') OR "
                                                                                       '(substring(`device_target_code`, '
                                                                                       'length(`device_target_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`device_target_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_procedure'},
         'gold.clinical.procedure.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                               'column': 'performer_practitioner_id',
                                                                               'companions': [],
                                                                               'match': '`performer_practitioner_id` '
                                                                                        '= 0',
                                                                               'table': 'clinical_procedure'},
         'gold.clinical.procedure.procedure_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'procedure_snomed_code',
                                                                                 'companions': ['procedure_snomed_display',
                                                                                                'procedure_target_code',
                                                                                                'procedure_target_display'],
                                                                                 'match': '`procedure_snomed_code` '
                                                                                          'IS NOT NULL AND '
                                                                                          '(NOT '
                                                                                          '`procedure_snomed_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' "
                                                                                          'OR '
                                                                                          'substring(`procedure_snomed_code`, '
                                                                                          'length(`procedure_snomed_code`) '
                                                                                          '- 2, 2) NOT IN '
                                                                                          "('00', '10') OR "
                                                                                          '(substring(`procedure_snomed_code`, '
                                                                                          'length(`procedure_snomed_code`) '
                                                                                          "- 2, 2) = '10' "
                                                                                          'AND '
                                                                                          'length(`procedure_snomed_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_procedure'},
         'gold.clinical.procedure.procedure_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'procedure_target_code',
                                                                                 'companions': ['procedure_target_display',
                                                                                                'procedure_snomed_code',
                                                                                                'procedure_snomed_display'],
                                                                                 'match': '`procedure_target_system` '
                                                                                          '= '
                                                                                          "'http://snomed.info/sct' "
                                                                                          'AND '
                                                                                          '`procedure_target_code` '
                                                                                          'IS NOT NULL AND '
                                                                                          '(NOT '
                                                                                          '`procedure_target_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' "
                                                                                          'OR '
                                                                                          'substring(`procedure_target_code`, '
                                                                                          'length(`procedure_target_code`) '
                                                                                          '- 2, 2) NOT IN '
                                                                                          "('00', '10') OR "
                                                                                          '(substring(`procedure_target_code`, '
                                                                                          'length(`procedure_target_code`) '
                                                                                          "- 2, 2) = '10' "
                                                                                          'AND '
                                                                                          'length(`procedure_target_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_procedure'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`source_patient_event_key` AS `source_patient_event_key`',
            '`source_object` AS `source_object`',
            '`procedure_id` AS `procedure_id`',
            '`endobase_exam_id` AS `endobase_exam_id`',
            '`implant_event_id` AS `implant_event_id`',
            '`implant_sequence` AS `implant_sequence`',
            '`surg_case_proc_id` AS `surg_case_proc_id`',
            '`source_row_hash` AS `source_row_hash`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_procedure_code` AS `procedure_code`',
            '`procedure_source_system` AS `procedure_source_system`',  # SRC_LINEAGE_V1
            '`procedure_source_code` AS `procedure_source_code`',  # SRC_LINEAGE_V1
            '`procedure_source_display` AS `procedure_source_display`',  # SRC_LINEAGE_V1
            '`status_code` AS `status_code`',
            '`status_display` AS `status_display`',
            '`performed_start` AS `performed_start`',
            '`performed_end` AS `performed_end`',
            '`body_site_code` AS `body_site_code`',
            '`body_site_display` AS `body_site_display`',
            '`laterality_code` AS `laterality_code`',
            '`laterality_display` AS `laterality_display`',
            '`body_site_method` AS `body_site_method`',
            '`laterality_method` AS `laterality_method`',
            '`opcs_code_role` AS `opcs_code_role`',
            '`opcs_anchor_procedure_id` AS `opcs_anchor_procedure_id`',
            '`performer_practitioner_id` AS `performer_practitioner_id`',
            '`procedure_location_code` AS `procedure_location_code`',
            '`procedure_location_display` AS `procedure_location_display`',
            '`procedure_note` AS `procedure_note`',
            '`implant_description` AS `implant_description`',
            '`__gold_json_device_code` AS `device_code`',
            '`device_source_system` AS `device_source_system`',  # SRC_LINEAGE_V1
            '`device_source_code` AS `device_source_code`',  # SRC_LINEAGE_V1
            '`device_source_display` AS `device_source_display`',  # SRC_LINEAGE_V1
            '`device_display` AS `device_display`',
            '`manufacturer` AS `manufacturer`',
            '`serial_number` AS `serial_number`',
            '`batch_number` AS `batch_number`',
            '`udi_di` AS `udi_di`',
            '`udi_standard` AS `udi_standard`',
            '`quantity` AS `quantity`',
            '`implant_attribute_count` AS `implant_attribute_count`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
            '`quantity_parse_rule` AS `quantity_parse_rule`',
            '`procedure_date_basis` AS `procedure_date_basis`',
            '`performed_end_basis` AS `performed_end_basis`',
            '`quantity_parse_status` AS `quantity_parse_status`',
        ],
        public_steps=(_public_steps_clinical_procedure,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_procedure': {'columns': {'batch_number': 'Implant batch number.',
                                        'body_site_code': 'SNOMED body-structure code. Millennium OPCS-4 '
                                                          'rows: the first OPCS site code of the coded '
                                                          "operation, mapped through OMOP 'Maps to'. Implant "
                                                          'rows: the site agreed by the operations the '
                                                          'implant links to. See body_site_method.',
                                        'body_site_display': 'SNOMED body-structure name for body_site_code.',
                                        'body_site_method': 'How body_site_code was derived. '
                                                            'opcs_qualifier_block_v1: first site code of the '
                                                            'OPCS-4 coded operation (same encounter and '
                                                            'coding episode, ordered by PROC_PRIORITY then '
                                                            'PROCEDURE_ID; sites coded after a harvest '
                                                            '(Y54-Y69) code describe the donor site and are '
                                                            'not used); suffix _multiple when the operation '
                                                            'has several site codes, _unmapped when the site '
                                                            'code has no SNOMED map (code NULL). '
                                                            'implant_opcs_link_v1_same_day / _adjacent_day: '
                                                            'site agreed by every candidate operation; '
                                                            '_conflict when they disagree (code NULL). NULL: '
                                                            'no site evidence.',
                                        'confidentiality_code': 'Source confidentiality code.',
                                        'device_code': 'Device concept identifier for implant-placement '
                                                       'facts.',
                                        'device_display': 'Device concept display.',
                                        'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                        'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; primary key for the '
                                                            'endobase_exam arm.',
                                        'event_datetime': 'Primary performed timestamp.',
                                        'event_datetime_local': 'Primary performed timestamp for the '
                                                                'procedure or implant event, expressed in '
                                                                'local time. Gold: NULL wherever Gold nulls '
                                                                'event_datetime.',
                                        'event_end_datetime': 'Procedure end timestamp.',
                                        'event_end_datetime_local': 'Procedure end timestamp expressed in '
                                                                    'local time, null where no end time is '
                                                                    'recorded. Gold: NULL wherever Gold '
                                                                    'nulls event_end_datetime.',
                                        'implant_attribute_count': 'Number of retained implant attribute '
                                                                   'rows.',
                                        'implant_description': 'Implant description for implant-placement '
                                                               'facts.',
                                        'implant_event_id': 'Millennium implant EVENT_ID; first component of '
                                                            'the implant-arm primary key.',
                                        'implant_sequence': 'Millennium IMPLANT_SEQUENCE; second component '
                                                            'of the implant-arm primary key.',
                                        'laterality_code': 'Millennium LATERALITY_CD where the source '
                                                           'records one; otherwise the OPCS-4 Z94.x code of '
                                                           'the coded operation (Z94.1 bilateral, Z94.2 '
                                                           'right, Z94.3 left, Z94.4 unilateral side not '
                                                           'stated). See laterality_method.',
                                        'laterality_display': 'Laterality display: the Millennium laterality '
                                                              'description or the Z94.x meaning.',
                                        'laterality_method': 'How laterality was derived. '
                                                             'source_laterality_cd: Millennium '
                                                             'LATERALITY_CD. opcs_qualifier_block_v1: the '
                                                             'Z94.x code of the OPCS-4 coded operation (a '
                                                             'Z94 coded after a harvest (Y54-Y69) code '
                                                             'describes the donor site and is not used); '
                                                             '_conflict when it carries different Z94 codes '
                                                             '(laterality NULL). '
                                                             'implant_opcs_link_v1_same_day / _adjacent_day: '
                                                             'side agreed by every candidate operation with '
                                                             'a side; _conflict when they disagree. NULL: no '
                                                             'side evidence.',
                                        'loaded_at': 'Millennium, NCCMDS and EndoBase arms use their own '
                                                     'ADC_UPDT. Theatre uses greatest of procedure ADC_UPDT '
                                                     'and joined case ADC_UPDT. Implant first takes '
                                                     'coalesce(SOURCE_MAX_ADC_UPDT, ADC_UPDT, '
                                                     'BASE_EVENT_ADC_UPDT), in that order, then greatest of '
                                                     'this result and maximum ADC_UPDT from matching '
                                                     'map_implant_detail_events grouped by '
                                                     'patient_event_key. Attribute matching uses the base '
                                                     'clinical-event ID or implant-form parent-event ID. The '
                                                     'ordered implant fallback is not itself a maximum; the '
                                                     'five-arm union and QC add no further clocks or Silver '
                                                     'refresh timestamp.',
                                        'manufacturer': 'Implant manufacturer name',
                                        'opcs_anchor_procedure_id': 'PROCEDURE_ID of the OPCS-4 operation '
                                                                    'this row belongs to: the row itself for '
                                                                    'an operation; the preceding operation '
                                                                    'in coding order for a qualifier; NULL '
                                                                    'for a qualifier coded before any '
                                                                    'operation. Implant rows: the linked '
                                                                    'operation when exactly one candidate '
                                                                    'operation exists '
                                                                    '(IMPLANT_OPCS_LINK_V1).',
                                        'opcs_code_role': 'Role of this OPCS-4 code in its coded operation '
                                                          '(OPCS_QUALIFIER_BLOCK_V1): operation, site '
                                                          '(body-structure code), laterality (Z94.x) or '
                                                          'method (Y chapter). NULL for non-OPCS rows. Count '
                                                          "procedures with opcs_code_role = 'operation'; the "
                                                          'other roles qualify the operation named in '
                                                          'opcs_anchor_procedure_id.',
                                        'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                             'cross-feed join key.',
                                        'performed_end': 'Procedure performed end.',
                                        'performed_end_basis': 'source_end, or load_clock_suppressed where '
                                                               "the Endobase end equals the record's update "
                                                               'day and month and follows the start by more '
                                                               'than 30 days (a modify clock, P9_L2786); '
                                                               'performed_end is then NULL. INSR_W2.',
                                        'performed_start': 'Procedure performed start.',
                                        'performer_practitioner_id': 'Millennium personnel PERSON_ID as '
                                                                     'BIGINT when available.',
                                        'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                        'procedure_code': 'Source and mapped procedure codings.',
                                        'procedure_date_basis': 'Basis of PROCEDURE_DT_TM_EFFECTIVE: '
                                                                'proc_dt_tm, proc_start_dt_tm, '
                                                                'proc_end_dt_tm or encounter_arrival. '
                                                                '(bronze PROCEDURE_DATE_BASIS, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                        'procedure_id': 'Millennium PROCEDURE_ID; primary key for the '
                                                        'millennium_procedure arm.',
                                        'procedure_location_code': 'Source procedure-location code retained '
                                                                   'without asserting a location-dimension '
                                                                   'FK.',
                                        'procedure_location_display': 'Source procedure-location display.',
                                        'procedure_note': 'Source procedure note.',
                                        'quantity': 'Parsed implant quantity.',
                                        'quantity_parse_rule': 'Rule set behind '
                                                               'QUANTITY_NUMERIC/QUANTITY_PARSE_STATUS '
                                                               '(IMPLANT_QTY_V2). (bronze '
                                                               'QUANTITY_PARSE_RULE, INSR W1). INSR_W2 '
                                                               'W2.20.',
                                        'quantity_parse_status': 'Status indicator showing whether the '
                                                                 'recorded implant quantity text was '
                                                                 'successfully converted to a numeric value. '
                                                                 '(bronze QUANTITY_PARSE_STATUS, INSR W1). '
                                                                 'INSR_W2 W2.20.',
                                        'record_status': 'Arm-specific label: Millennium procedures are '
                                                         'superseded when lower(coalesce(active_status_desc, '
                                                         'empty string)) contains the substring inactive, '
                                                         'otherwise active. Implants are always active. '
                                                         'Theatre procedures are superseded for source '
                                                         'presence false (null defaults true) or ACTIVE_IND '
                                                         'cast to LONG zero (null defaults one), otherwise '
                                                         'active. EndoBase exams are superseded for source '
                                                         'presence false (null defaults true), otherwise '
                                                         'active. NCCMDS procedures instead map source '
                                                         'absence to retracted, otherwise active. Clinical '
                                                         'completion/cancellation fields remain separate.',
                                        'record_status_effective_from': 'Millennium: ACTIVE_STATUS_DT_TM. '
                                                                        'Implant: VALID_FROM_DT_TM. Theatre: '
                                                                        'first non-null procedure '
                                                                        'PROC_START_DT_TM then parent case '
                                                                        'SCHED_START_DT_TM, not the wider '
                                                                        'performed-time fallback. NCCMDS: '
                                                                        'OPCS_Proc_Dt_CLEAN. EndoBase: first '
                                                                        'non-null PERFORMED_TS_CLEAN, '
                                                                        'EXAM_TS_CLEAN, TRUE_START_TS_CLEAN '
                                                                        'then START_TS_CLEAN. These '
                                                                        'arm-specific values are carried '
                                                                        'unchanged; no ingestion-time or '
                                                                        'cross-arm fallback is added.',
                                        'record_status_effective_to': 'Millennium: ACTIVE_STATUS_DT_TM only '
                                                                      'when inactive. Implant: always null. '
                                                                      'Theatre: first non-null procedure '
                                                                      'SOURCE_ABSENT_DETECTED_TS then '
                                                                      'procedure ADC_UPDT only when '
                                                                      'inactive; the parent case clock does '
                                                                      'not contribute. NCCMDS: own ADC_UPDT '
                                                                      'only when retracted. EndoBase: own '
                                                                      'ADC_UPDT only when '
                                                                      'source-absent/superseded. All other '
                                                                      'rows have null ends; clinical '
                                                                      'procedure/exam end timestamps are not '
                                                                      'substituted.',
                                        'serial_number': 'Implant serial number.',
                                        'source_code': 'Source procedure code or implant label',
                                        'source_coding_system': 'Verbatim source coding system.',
                                        'source_display': 'Verbatim source procedure display.',
                                        'source_feed': 'Registered source feed owning the fact.',
                                        'source_object': 'Source arm: millennium_procedure, implant, '
                                                         'theatre, endobase_exam, or nccmds.',
                                        'source_patient_event_key': 'Stable patient-event identity retained '
                                                                    'from the originating Silver row before '
                                                                    'source splitting.',
                                        'source_row_hash': 'Native ROW_HASH for the nccmds arm; NULL for '
                                                           'other arms.',
                                        'source_update_timestamp': 'Source-arm-specific timestamp; interpret '
                                                                   'with source_object. Cerner procedures '
                                                                   'carry map_procedure.UPDT_DT_TM; implants '
                                                                   'carry '
                                                                   'map_implant_details.CLINSIG_UPDT_DT_TM; '
                                                                   'theatre carries the greatest '
                                                                   'SOURCE_ADC_UPDT from its case and '
                                                                   'case-procedure rows. EndoBase exams and '
                                                                   'NCCMDS procedures carry their bronze '
                                                                   'PIPELINE_UPDT_DT_TM. This mixes source '
                                                                   'and processing clocks and is not a '
                                                                   'uniform clinical event time or '
                                                                   'native-system update time.',
                                        'status_code': 'Source-derived FHIR procedure status.',
                                        'status_display': 'Source status display.',
                                        'subject_id_system': 'Identifier system used for subject_key.',
                                        'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                       'identifier. Not salted, not secret; a stable join '
                                                       'key across feeds where person_id is unresolved.',
                                        'surg_case_proc_id': 'SurgiNet SURG_CASE_PROC_ID; primary key for '
                                                             'the theatre arm.',
                                        'udi_di': 'Unique device identifier device identifier.',
                                        'udi_standard': 'UDI issuing standard.',
                                        'vip_ind': 'Source VIP indicator.',
                                        'withheld_identity_ind': 'Identity-withholding status is not '
                                                                 'populated by the current source '
                                                                 'projections; null does not mean identity '
                                                                 'was not withheld.'},
                            'quantity_override': True,
                            'table': 'Internal quality-controlled twin of clinical_procedure: the Gold '
                                     'repairs, nulls and expectations are applied here and the published '
                                     'Gold MV reads this object. Materialized rather than temporary so the '
                                     'published MV can refresh incrementally.'},
 'clinical_procedure': {'columns': {'batch_number': 'Implant batch number.',
                                    'body_site_code': 'SNOMED body-structure code. Millennium OPCS-4 rows: '
                                                      'the first OPCS site code of the coded operation, '
                                                      "mapped through OMOP 'Maps to'. Implant rows: the site "
                                                      'agreed by the operations the implant links to. See '
                                                      'body_site_method.',
                                    'body_site_display': 'SNOMED body-structure name for body_site_code.',
                                    'body_site_method': 'How body_site_code was derived. '
                                                        'opcs_qualifier_block_v1: first site code of the '
                                                        'OPCS-4 coded operation (same encounter and coding '
                                                        'episode, ordered by PROC_PRIORITY then '
                                                        'PROCEDURE_ID; sites coded after a harvest (Y54-Y69) '
                                                        'code describe the donor site and are not used); '
                                                        'suffix _multiple when the operation has several '
                                                        'site codes, _unmapped when the site code has no '
                                                        'SNOMED map (code NULL). '
                                                        'implant_opcs_link_v1_same_day / _adjacent_day: site '
                                                        'agreed by every candidate operation; _conflict when '
                                                        'they disagree (code NULL). NULL: no site evidence.',
                                    'confidentiality_code': 'Source confidentiality code.',
                                    'device_code': 'Device concept identifier for implant-placement facts.',
                                    'device_display': 'Device concept display.',
                                    'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                    'endobase_exam_id': 'Endobase ENDOBASE_EXAM_ID; primary key for the '
                                                        'endobase_exam arm.',
                                    'event_datetime': 'Primary performed timestamp.',
                                    'event_datetime_local': 'Primary performed timestamp for the procedure '
                                                            'or implant event, expressed in local time. '
                                                            'Gold: NULL wherever Gold nulls event_datetime.',
                                    'event_end_datetime': 'Procedure end timestamp.',
                                    'event_end_datetime_local': 'Procedure end timestamp expressed in local '
                                                                'time, null where no end time is recorded. '
                                                                'Gold: NULL wherever Gold nulls '
                                                                'event_end_datetime.',
                                    'implant_attribute_count': 'Number of retained implant attribute rows.',
                                    'implant_description': 'Implant description for implant-placement facts.',
                                    'implant_event_id': 'Millennium implant EVENT_ID; first component of the '
                                                        'implant-arm primary key.',
                                    'implant_sequence': 'Millennium IMPLANT_SEQUENCE; second component of '
                                                        'the implant-arm primary key.',
                                    'laterality_code': 'Millennium LATERALITY_CD where the source records '
                                                       'one; otherwise the OPCS-4 Z94.x code of the coded '
                                                       'operation (Z94.1 bilateral, Z94.2 right, Z94.3 left, '
                                                       'Z94.4 unilateral side not stated). See '
                                                       'laterality_method.',
                                    'laterality_display': 'Laterality display: the Millennium laterality '
                                                          'description or the Z94.x meaning.',
                                    'laterality_method': 'How laterality was derived. source_laterality_cd: '
                                                         'Millennium LATERALITY_CD. opcs_qualifier_block_v1: '
                                                         'the Z94.x code of the OPCS-4 coded operation (a '
                                                         'Z94 coded after a harvest (Y54-Y69) code describes '
                                                         'the donor site and is not used); _conflict when it '
                                                         'carries different Z94 codes (laterality NULL). '
                                                         'implant_opcs_link_v1_same_day / _adjacent_day: '
                                                         'side agreed by every candidate operation with a '
                                                         'side; _conflict when they disagree. NULL: no side '
                                                         'evidence.',
                                    'loaded_at': 'Millennium, NCCMDS and EndoBase arms use their own '
                                                 'ADC_UPDT. Theatre uses greatest of procedure ADC_UPDT and '
                                                 'joined case ADC_UPDT. Implant first takes '
                                                 'coalesce(SOURCE_MAX_ADC_UPDT, ADC_UPDT, '
                                                 'BASE_EVENT_ADC_UPDT), in that order, then greatest of this '
                                                 'result and maximum ADC_UPDT from matching '
                                                 'map_implant_detail_events grouped by patient_event_key. '
                                                 'Attribute matching uses the base clinical-event ID or '
                                                 'implant-form parent-event ID. The ordered implant fallback '
                                                 'is not itself a maximum; the five-arm union and QC add no '
                                                 'further clocks or Silver refresh timestamp.',
                                    'manufacturer': 'Implant manufacturer name',
                                    'opcs_anchor_procedure_id': 'PROCEDURE_ID of the OPCS-4 operation this '
                                                                'row belongs to: the row itself for an '
                                                                'operation; the preceding operation in '
                                                                'coding order for a qualifier; NULL for a '
                                                                'qualifier coded before any operation. '
                                                                'Implant rows: the linked operation when '
                                                                'exactly one candidate operation exists '
                                                                '(IMPLANT_OPCS_LINK_V1).',
                                    'opcs_code_role': 'Role of this OPCS-4 code in its coded operation '
                                                      '(OPCS_QUALIFIER_BLOCK_V1): operation, site '
                                                      '(body-structure code), laterality (Z94.x) or method '
                                                      '(Y chapter). NULL for non-OPCS rows. Count procedures '
                                                      "with opcs_code_role = 'operation'; the other roles "
                                                      'qualify the operation named in '
                                                      'opcs_anchor_procedure_id.',
                                    'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                         'cross-feed join key.',
                                    'performed_end': 'Procedure performed end.',
                                    'performed_end_basis': 'source_end, or load_clock_suppressed where the '
                                                           "Endobase end equals the record's update day and "
                                                           'month and follows the start by more than 30 days '
                                                           '(a modify clock, P9_L2786); performed_end is '
                                                           'then NULL. INSR_W2.',
                                    'performed_start': 'Procedure performed start.',
                                    'performer_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT '
                                                                 'when available.',
                                    'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                    'procedure_code': 'Source and mapped procedure codings.',
                                    'procedure_date_basis': 'Basis of PROCEDURE_DT_TM_EFFECTIVE: proc_dt_tm, '
                                                            'proc_start_dt_tm, proc_end_dt_tm or '
                                                            'encounter_arrival. (bronze '
                                                            'PROCEDURE_DATE_BASIS, INSR W1). INSR_W2 W2.20.',
                                    'procedure_id': 'Millennium PROCEDURE_ID; primary key for the '
                                                    'millennium_procedure arm.',
                                    'procedure_location_code': 'Source procedure-location code retained '
                                                               'without asserting a location-dimension FK.',
                                    'procedure_location_display': 'Source procedure-location display.',
                                    'procedure_note': 'Source procedure note.',
                                    'quantity': 'Parsed implant quantity.',
                                    'quantity_parse_rule': 'Rule set behind '
                                                           'QUANTITY_NUMERIC/QUANTITY_PARSE_STATUS '
                                                           '(IMPLANT_QTY_V2). (bronze QUANTITY_PARSE_RULE, '
                                                           'INSR W1). INSR_W2 W2.20.',
                                    'quantity_parse_status': 'Status indicator showing whether the recorded '
                                                             'implant quantity text was successfully '
                                                             'converted to a numeric value. (bronze '
                                                             'QUANTITY_PARSE_STATUS, INSR W1). INSR_W2 '
                                                             'W2.20.',
                                    'record_status': 'Arm-specific label: Millennium procedures are '
                                                     'superseded when lower(coalesce(active_status_desc, '
                                                     'empty string)) contains the substring inactive, '
                                                     'otherwise active. Implants are always active. Theatre '
                                                     'procedures are superseded for source presence false '
                                                     '(null defaults true) or ACTIVE_IND cast to LONG zero '
                                                     '(null defaults one), otherwise active. EndoBase exams '
                                                     'are superseded for source presence false (null '
                                                     'defaults true), otherwise active. NCCMDS procedures '
                                                     'instead map source absence to retracted, otherwise '
                                                     'active. Clinical completion/cancellation fields remain '
                                                     'separate.',
                                    'record_status_effective_from': 'Millennium: ACTIVE_STATUS_DT_TM. '
                                                                    'Implant: VALID_FROM_DT_TM. Theatre: '
                                                                    'first non-null procedure '
                                                                    'PROC_START_DT_TM then parent case '
                                                                    'SCHED_START_DT_TM, not the wider '
                                                                    'performed-time fallback. NCCMDS: '
                                                                    'OPCS_Proc_Dt_CLEAN. EndoBase: first '
                                                                    'non-null PERFORMED_TS_CLEAN, '
                                                                    'EXAM_TS_CLEAN, TRUE_START_TS_CLEAN then '
                                                                    'START_TS_CLEAN. These arm-specific '
                                                                    'values are carried unchanged; no '
                                                                    'ingestion-time or cross-arm fallback is '
                                                                    'added.',
                                    'record_status_effective_to': 'Millennium: ACTIVE_STATUS_DT_TM only when '
                                                                  'inactive. Implant: always null. Theatre: '
                                                                  'first non-null procedure '
                                                                  'SOURCE_ABSENT_DETECTED_TS then procedure '
                                                                  'ADC_UPDT only when inactive; the parent '
                                                                  'case clock does not contribute. NCCMDS: '
                                                                  'own ADC_UPDT only when retracted. '
                                                                  'EndoBase: own ADC_UPDT only when '
                                                                  'source-absent/superseded. All other rows '
                                                                  'have null ends; clinical procedure/exam '
                                                                  'end timestamps are not substituted.',
                                    'serial_number': 'Implant serial number.',
                                    'source_code': 'Source procedure code or implant label',
                                    'source_coding_system': 'Verbatim source coding system.',
                                    'source_display': 'Verbatim source procedure display.',
                                    'source_feed': 'Registered source feed owning the fact.',
                                    'source_object': 'Source arm: millennium_procedure, implant, theatre, '
                                                     'endobase_exam, or nccmds.',
                                    'source_patient_event_key': 'Stable patient-event identity retained from '
                                                                'the originating Silver row before source '
                                                                'splitting.',
                                    'source_row_hash': 'Native ROW_HASH for the nccmds arm; NULL for other '
                                                       'arms.',
                                    'source_update_timestamp': 'Source-arm-specific timestamp; interpret '
                                                               'with source_object. Cerner procedures carry '
                                                               'map_procedure.UPDT_DT_TM; implants carry '
                                                               'map_implant_details.CLINSIG_UPDT_DT_TM; '
                                                               'theatre carries the greatest SOURCE_ADC_UPDT '
                                                               'from its case and case-procedure rows. '
                                                               'EndoBase exams and NCCMDS procedures carry '
                                                               'their bronze PIPELINE_UPDT_DT_TM. This mixes '
                                                               'source and processing clocks and is not a '
                                                               'uniform clinical event time or native-system '
                                                               'update time.',
                                    'status_code': 'Source-derived FHIR procedure status.',
                                    'status_display': 'Source status display.',
                                    'subject_id_system': 'Identifier system used for subject_key.',
                                    'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                   'identifier. Not salted, not secret; a stable join key '
                                                   'across feeds where person_id is unresolved.',
                                    'surg_case_proc_id': 'SurgiNet SURG_CASE_PROC_ID; primary key for the '
                                                         'theatre arm.',
                                    'udi_di': 'Unique device identifier device identifier.',
                                    'udi_standard': 'UDI issuing standard.',
                                    'vip_ind': 'Source VIP indicator.',
                                    'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                             'by the current source projections; null does '
                                                             'not mean identity was not withheld.'},
                        'quantity_override': False,
                        'table': 'One performed procedure or implant-placement source fact without '
                                 'cross-feed deduplication. Gold QC twin of the silver product: 13 columns '
                                 'are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each '
                                 'rule states its reason in the pipeline notebook, and Lakeflow expectation '
                                 'metrics report what every rule matched on each update. Coverage: Diagnoses '
                                 'and procedures dated March-April 2026 are about half the January/May '
                                 'volume (gold ratio 0.50 diagnoses, 0.54 procedures); gold holds what raw '
                                 'holds for those months (missing ids 2 of 119,823 and 186 of 92,312), so '
                                 'the shortfall is at source and was not re-landed; The most recent two to '
                                 "three months are incomplete while clinical coding catches up; each table's "
                                 'complete-through month says where the data is complete; complete-through '
                                 'month per release: 6_mgmt.data_quality.release_finding (finding_code '
                                 'F2_complete_through).'}}
