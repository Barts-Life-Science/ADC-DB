"""Gold specialist.cancer_treatment: 2 dataset(s).

- datasets: _qc_clinical_cancer_treatment, clinical_cancer_treatment
- moved verbatim from: 3_gold_specialist_care:256, 3_gold_specialist_care:302, 3_gold_specialist_care:311, 3_gold_specialist_care:416, 3_gold_specialist_care:420, 3_gold_specialist_care:433

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.cancer_treatment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CANCER_TREATMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`treatment_key` AS `treatment_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`drug_code` AS `drug_code`',
    '`drug_source_system` AS `drug_source_system`',  # SRC_LINEAGE_V1
    '`drug_source_code` AS `drug_source_code`',  # SRC_LINEAGE_V1
    '`drug_source_display` AS `drug_source_display`',  # SRC_LINEAGE_V1
    '`treatment_plan` AS `treatment_plan`',
    '`regimen_name` AS `regimen_name`',
    '`indication` AS `indication`',
    '`record_type` AS `record_type`',
    'CASE WHEN `dose_value` < 0 THEN NULL ELSE `dose_value` END AS `dose_value`',
    '`dose_total` AS `dose_total`',
    "CASE WHEN UPPER(TRIM(CAST(`dose_unit_code` AS STRING))) = '0' THEN NULL ELSE `dose_unit_code` END AS `dose_unit_code`",
    '`route_code` AS `route_code`',
    'CASE WHEN CAST(`start_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `start_date` END AS `start_date`',
    'CASE WHEN `start_date` IS NOT NULL AND `end_date` IS NOT NULL AND `start_date` > `end_date` THEN NULL ELSE `end_date` END AS `end_date`',
    '`final_treatment_date` AS `final_treatment_date`',
    '`course_finished` AS `course_finished`',
    '`planned_cycles` AS `planned_cycles`',
    '`default_cycles` AS `default_cycles`',
    '`chemo_radiation` AS `chemo_radiation`',
    '`procurement_opcs_code` AS `procurement_opcs_code`',
    '`delivery_opcs_code` AS `delivery_opcs_code`',
    '`drug_similarity` AS `drug_similarity`',
    '`iqemo_course_id` AS `iqemo_course_id`',
    '`aria_rx_key` AS `aria_rx_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_cancer_treatment_metadata; source history stays on the main research table.
CLINICAL_CANCER_TREATMENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 2,789,249 rows of 2,789,249
    # that are current and attributable; identity_status = 'resolved' keeps the 2,716,669
    # rows of 2,789,249 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_CANCER_TREATMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.cancer_treatment.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 5,115 of 2,789,249
    # rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.end_date.future_owner":
        "NOT COALESCE((CAST(`end_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 5,115 of 2,789,249
    # rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.event_end_datetime.future_owner":
        "NOT COALESCE((CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 6,921 of 2,789,249
    # rows (0.248%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.final_treatment_date.future_owner":
        "NOT COALESCE((CAST(`final_treatment_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 72,580 of 2,789,249 at the profile.
    "gold.clinical.cancer_treatment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 2,789,249 at the profile.
    "gold.clinical.cancer_treatment.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 141 of 2,789,249
    # rows (0.00506%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,565 of 2,789,249 rows (0.235%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 350 of 2,789,249 rows (0.0125%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_CANCER_TREATMENT_PERSON_SCOPE_RULES = {
    "gold.clinical.cancer_treatment.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_cancer_treatment"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_CANCER_TREATMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CANCER_TREATMENT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_CANCER_TREATMENT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_cancer_treatment():
    """Quality-controlled twin of journey_clinical.cancer_treatment."""
    df = _qc("clinical_cancer_treatment", CLINICAL_CANCER_TREATMENT_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.cancer_treatment"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_cancer_treatment():
    """Contract-v2 public twin of clinical_cancer_treatment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_cancer_treatment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`treatment_key` AS `treatment_key`',
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
        '`__gold_json_drug_code` AS `drug_code`',
        '`drug_source_system` AS `drug_source_system`',  # SRC_LINEAGE_V1
        '`drug_source_code` AS `drug_source_code`',  # SRC_LINEAGE_V1
        '`drug_source_display` AS `drug_source_display`',  # SRC_LINEAGE_V1
        '`treatment_plan` AS `treatment_plan`',
        '`regimen_name` AS `regimen_name`',
        '`indication` AS `indication`',
        '`record_type` AS `record_type`',
        '`dose_value` AS `dose_value`',
        '`dose_total` AS `dose_total`',
        '`dose_unit_code` AS `dose_unit_code`',
        '`route_code` AS `route_code`',
        '`start_date` AS `start_date`',
        '`end_date` AS `end_date`',
        '`final_treatment_date` AS `final_treatment_date`',
        '`course_finished` AS `course_finished`',
        '`planned_cycles` AS `planned_cycles`',
        '`default_cycles` AS `default_cycles`',
        '`chemo_radiation` AS `chemo_radiation`',
        '`procurement_opcs_code` AS `procurement_opcs_code`',
        '`delivery_opcs_code` AS `delivery_opcs_code`',
        '`drug_similarity` AS `drug_similarity`',
        '`iqemo_course_id` AS `iqemo_course_id`',
        '`aria_rx_key` AS `aria_rx_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(df, "clinical_cancer_treatment")


def _public_steps_clinical_cancer_treatment(df):
    """clinical_cancer_treatment: the public body after its projection (GoldRules.public_select), moved from gold_clinical_cancer_treatment; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_cancer_treatment')


RULES = {
    'clinical_cancer_treatment': GoldRules(
        table='clinical_cancer_treatment',
        select=CLINICAL_CANCER_TREATMENT_SELECT,
        mandatory=CLINICAL_CANCER_TREATMENT_MANDATORY_RULES,
        advisory=CLINICAL_CANCER_TREATMENT_ADVISORY_RULES,
        person_scope=CLINICAL_CANCER_TREATMENT_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        variant_columns=('drug_code',),
        s3_axis_specs={'drug_code': [('drug', False)]},
        value_rules={'gold.clinical.cancer_treatment.drug_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'drug_snomed_code',
                                                                                   'companions': ['drug_snomed_display',
                                                                                                  'drug_target_code',
                                                                                                  'drug_target_display'],
                                                                                   'match': '`drug_snomed_code` '
                                                                                            'IS NOT NULL '
                                                                                            'AND (NOT '
                                                                                            '`drug_snomed_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' "
                                                                                            'OR '
                                                                                            'substring(`drug_snomed_code`, '
                                                                                            'length(`drug_snomed_code`) '
                                                                                            '- 2, 2) NOT '
                                                                                            "IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`drug_snomed_code`, '
                                                                                            'length(`drug_snomed_code`) '
                                                                                            '- 2, 2) = '
                                                                                            "'10' AND "
                                                                                            'length(`drug_snomed_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_cancer_treatment'},
         'gold.clinical.cancer_treatment.drug_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'drug_target_code',
                                                                                   'companions': ['drug_target_display',
                                                                                                  'drug_snomed_code',
                                                                                                  'drug_snomed_display'],
                                                                                   'match': '`drug_target_system` '
                                                                                            '= '
                                                                                            "'http://snomed.info/sct' "
                                                                                            'AND '
                                                                                            '`drug_target_code` '
                                                                                            'IS NOT NULL '
                                                                                            'AND (NOT '
                                                                                            '`drug_target_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' "
                                                                                            'OR '
                                                                                            'substring(`drug_target_code`, '
                                                                                            'length(`drug_target_code`) '
                                                                                            '- 2, 2) NOT '
                                                                                            "IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`drug_target_code`, '
                                                                                            'length(`drug_target_code`) '
                                                                                            '- 2, 2) = '
                                                                                            "'10' AND "
                                                                                            'length(`drug_target_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_cancer_treatment'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`treatment_key` AS `treatment_key`',
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
            '`__gold_json_drug_code` AS `drug_code`',
            '`drug_source_system` AS `drug_source_system`',  # SRC_LINEAGE_V1
            '`drug_source_code` AS `drug_source_code`',  # SRC_LINEAGE_V1
            '`drug_source_display` AS `drug_source_display`',  # SRC_LINEAGE_V1
            '`treatment_plan` AS `treatment_plan`',
            '`regimen_name` AS `regimen_name`',
            '`indication` AS `indication`',
            '`record_type` AS `record_type`',
            '`dose_value` AS `dose_value`',
            '`dose_total` AS `dose_total`',
            '`dose_unit_code` AS `dose_unit_code`',
            '`route_code` AS `route_code`',
            '`start_date` AS `start_date`',
            '`end_date` AS `end_date`',
            '`final_treatment_date` AS `final_treatment_date`',
            '`course_finished` AS `course_finished`',
            '`planned_cycles` AS `planned_cycles`',
            '`default_cycles` AS `default_cycles`',
            '`chemo_radiation` AS `chemo_radiation`',
            '`procurement_opcs_code` AS `procurement_opcs_code`',
            '`delivery_opcs_code` AS `delivery_opcs_code`',
            '`drug_similarity` AS `drug_similarity`',
            '`iqemo_course_id` AS `iqemo_course_id`',
            '`aria_rx_key` AS `aria_rx_key`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_cancer_treatment,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_cancer_treatment': {'columns': {'aria_rx_key': 'ARIA prescription key.',
                                               'chemo_radiation': 'Concurrent chemo-radiation indicator.',
                                               'confidentiality_code': 'Security classification when '
                                                                       'supplied.',
                                               'course_finished': 'Source indication that the treatment '
                                                                  'course has finished, from '
                                                                  '`CourseFinished` cast to boolean.',
                                               'default_cycles': 'Default regimen cycle count.',
                                               'delivery_opcs_code': 'OPCS delivery code.',
                                               'dose_total': 'Total planned or delivered dose.',
                                               'dose_unit_code': 'ARIA coded value for the dosage unit used '
                                                                 'for administration of the agent.',
                                               'dose_value': 'Constituent dose value.',
                                               'drug_code': 'Source and mapped drug codings.',
                                               'drug_similarity': 'Drug-link similarity score.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'end_date': 'Source end date.',
                                               'event_datetime': 'Treatment start timestamp.',
                                               'event_datetime_local': 'Treatment start timestamp expressed '
                                                                       'in local time, corresponding to '
                                                                       'event_datetime. Gold: NULL wherever '
                                                                       'Gold nulls event_datetime.',
                                               'event_end_datetime': 'Treatment end or final-treatment '
                                                                     'timestamp.',
                                               'event_end_datetime_local': 'Treatment end or final-treatment '
                                                                           'timestamp expressed in local '
                                                                           'time, corresponding to '
                                                                           'event_end_datetime. Gold: NULL '
                                                                           'wherever Gold nulls '
                                                                           'event_end_datetime.',
                                               'final_treatment_date': 'Date of the final treatment '
                                                                       'administered within the iQemo '
                                                                       'chemotherapy course.',
                                               'indication': 'Clinical indication text held against the '
                                                             'iQemo regimen definition for which the regimen '
                                                             'is used.',
                                               'iqemo_course_id': 'iQemo course identifier.',
                                               'loaded_at': 'map_cancer_treatment.ADC_UPDT carried unchanged '
                                                            'through usable-code filtering and QC/public '
                                                            'projections. No joined parent or QC clock is '
                                                            'added. This differs from SRC_ADC_UPDT used for '
                                                            'source_update_timestamp and from the current '
                                                            'Silver refresh time.',
                                               'patient_event_key': 'Deterministic event key derived from '
                                                                    'source identifiers; retained as a '
                                                                    'stable join key.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                            'resolved.',
                                               'planned_cycles': 'Planned cycle count.',
                                               'procurement_opcs_code': 'OPCS procurement code.',
                                               'record_status': 'Superseded when '
                                                                'map_cancer_treatment.SOURCE_PRESENT_IND '
                                                                'cast to BOOLEAN is false, otherwise active; '
                                                                'missing presence defaults true. No '
                                                                'retracted status is emitted. '
                                                                'CourseFinished, record_type and treatment '
                                                                'end dates do not set this row-history '
                                                                'label.',
                                               'record_status_effective_from': 'Start date of the treatment '
                                                                               'record, taken from the ARIA '
                                                                               'administration start date or '
                                                                               'the iQemo chemotherapy '
                                                                               'course start date.',
                                               'record_status_effective_to': 'map_cancer_treatment.ADC_UPDT '
                                                                             'when source presence is false; '
                                                                             'otherwise null. Neither '
                                                                             'EndDate nor FinalTreatmentDate '
                                                                             'is substituted, and a missing '
                                                                             'ADC_UPDT remains null.',
                                               'record_type': 'ARIA/iQemo linkage class.',
                                               'regimen_name': 'Treatment regimen name.',
                                               'route_code': 'Administration route code.',
                                               'source_code': 'Normalized drug token with agent-name display '
                                                              'fallback.',
                                               'source_coding_system': 'SACT drug-token coding system.',
                                               'source_display': 'Source agent name.',
                                               'source_update_timestamp': 'Timestamp of the last update to '
                                                                          'the underlying ARIA or iQemo '
                                                                          'source record as recorded in the '
                                                                          'raw layer.',
                                               'start_date': 'Source start date.',
                                               'subject_id_system': 'Identifier system used for subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'treatment_key': 'SACT TREATMENT_KEY; primary key of this '
                                                                'table.',
                                               'treatment_plan': 'Source treatment plan.',
                                               'vip_ind': 'VIP indicator when supplied.',
                                               'withheld_identity_ind': 'Identity-withholding status is not '
                                                                        'populated by the current source '
                                                                        'projections; null does not mean '
                                                                        'identity was not withheld.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_cancer_treatment: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_cancer_treatment': {'columns': {'aria_rx_key': 'ARIA prescription key.',
                                           'chemo_radiation': 'Concurrent chemo-radiation indicator.',
                                           'confidentiality_code': 'Security classification when supplied.',
                                           'course_finished': 'Source indication that the treatment course '
                                                              'has finished, from `CourseFinished` cast to '
                                                              'boolean.',
                                           'default_cycles': 'Default regimen cycle count.',
                                           'delivery_opcs_code': 'OPCS delivery code.',
                                           'dose_total': 'Total planned or delivered dose.',
                                           'dose_unit_code': 'ARIA coded value for the dosage unit used for '
                                                             'administration of the agent.',
                                           'dose_value': 'Constituent dose value.',
                                           'drug_code': 'Source and mapped drug codings.',
                                           'drug_similarity': 'Drug-link similarity score.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'end_date': 'Source end date.',
                                           'event_datetime': 'Treatment start timestamp.',
                                           'event_datetime_local': 'Treatment start timestamp expressed in '
                                                                   'local time, corresponding to '
                                                                   'event_datetime. Gold: NULL wherever Gold '
                                                                   'nulls event_datetime.',
                                           'event_end_datetime': 'Treatment end or final-treatment '
                                                                 'timestamp.',
                                           'event_end_datetime_local': 'Treatment end or final-treatment '
                                                                       'timestamp expressed in local time, '
                                                                       'corresponding to event_end_datetime. '
                                                                       'Gold: NULL wherever Gold nulls '
                                                                       'event_end_datetime.',
                                           'final_treatment_date': 'Date of the final treatment administered '
                                                                   'within the iQemo chemotherapy course.',
                                           'indication': 'Clinical indication text held against the iQemo '
                                                         'regimen definition for which the regimen is used.',
                                           'iqemo_course_id': 'iQemo course identifier.',
                                           'loaded_at': 'map_cancer_treatment.ADC_UPDT carried unchanged '
                                                        'through usable-code filtering and QC/public '
                                                        'projections. No joined parent or QC clock is added. '
                                                        'This differs from SRC_ADC_UPDT used for '
                                                        'source_update_timestamp and from the current Silver '
                                                        'refresh time.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'planned_cycles': 'Planned cycle count.',
                                           'procurement_opcs_code': 'OPCS procurement code.',
                                           'record_status': 'Superseded when '
                                                            'map_cancer_treatment.SOURCE_PRESENT_IND cast to '
                                                            'BOOLEAN is false, otherwise active; missing '
                                                            'presence defaults true. No retracted status is '
                                                            'emitted. CourseFinished, record_type and '
                                                            'treatment end dates do not set this row-history '
                                                            'label.',
                                           'record_status_effective_from': 'Start date of the treatment '
                                                                           'record, taken from the ARIA '
                                                                           'administration start date or the '
                                                                           'iQemo chemotherapy course start '
                                                                           'date.',
                                           'record_status_effective_to': 'map_cancer_treatment.ADC_UPDT when '
                                                                         'source presence is false; '
                                                                         'otherwise null. Neither EndDate '
                                                                         'nor FinalTreatmentDate is '
                                                                         'substituted, and a missing '
                                                                         'ADC_UPDT remains null.',
                                           'record_type': 'ARIA/iQemo linkage class.',
                                           'regimen_name': 'Treatment regimen name.',
                                           'route_code': 'Administration route code.',
                                           'source_code': 'Normalized drug token with agent-name display '
                                                          'fallback.',
                                           'source_coding_system': 'SACT drug-token coding system.',
                                           'source_display': 'Source agent name.',
                                           'source_update_timestamp': 'Timestamp of the last update to the '
                                                                      'underlying ARIA or iQemo source '
                                                                      'record as recorded in the raw layer.',
                                           'start_date': 'Source start date.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_key': 'SACT TREATMENT_KEY; primary key of this table.',
                                           'treatment_plan': 'Source treatment plan.',
                                           'vip_ind': 'VIP indicator when supplied.',
                                           'withheld_identity_ind': 'Identity-withholding status is not '
                                                                    'populated by the current source '
                                                                    'projections; null does not mean '
                                                                    'identity was not withheld.'},
                               'quantity_override': False,
                               'table': 'One SACT constituent-drug treatment fact, kept separate from '
                                        'encounter-bound Millennium medication administration. Gold QC twin '
                                        'of the silver product: 8 columns are repaired or nulled, 1 rule(s) '
                                        'drop rows, 8 check(s) are advisory. Each rule states its reason in '
                                        'the pipeline notebook, and Lakeflow expectation metrics report what '
                                        'every rule matched on each update. Coverage: iQemo holds regimens, '
                                        'courses and cycles but no per-drug rows at source (its only drug '
                                        'column is a G-CSF regimen drug id), so chemotherapy is published at '
                                        'cycle grain without individual agents; complete-through month per '
                                        'release: 6_mgmt.data_quality.release_finding (finding_code '
                                        'F2_complete_through).'}}
