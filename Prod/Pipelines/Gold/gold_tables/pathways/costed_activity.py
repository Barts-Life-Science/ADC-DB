"""Gold pathways.costed_activity: 2 dataset(s).

- datasets: _qc_clinical_costed_activity, clinical_costed_activity
- moved verbatim from: 4_gold_care_pathways:1004, 4_gold_care_pathways:1009, 4_gold_care_pathways:1013, 4_gold_care_pathways:1035, 4_gold_care_pathways:788, 4_gold_care_pathways:868, 4_gold_care_pathways:874, 4_gold_care_pathways:883

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.costed_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_COSTED_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`extract_cd` AS `extract_cd`',
    '`activity_record_id` AS `activity_record_id`',
    '`feed_type` AS `feed_type`',
    '`plemi` AS `plemi`',
    '`nhs_number_status_cd` AS `nhs_number_status_cd`',
    '`cds_id` AS `cds_id`',
    '`attendance_id` AS `attendance_id`',
    '`arrival_date` AS `arrival_date`',
    '`arrival_time` AS `arrival_time`',
    '`departure_date` AS `departure_date`',
    '`departure_time` AS `departure_time`',
    '`departure_type_cd` AS `departure_type_cd`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`patient_org_cd` AS `patient_org_cd`',
    '`pathway_id` AS `pathway_id`',
    '`pod_cd` AS `pod_cd`',
    '`treatment_function_cd` AS `treatment_function_cd`',
    '`source_los` AS `source_los`',
    '`cf_band_cd` AS `cf_band_cd`',
    '`episode_number` AS `episode_number`',
    '`episode_start_datetime` AS `episode_start_datetime`',
    'CASE WHEN `episode_start_datetime` IS NOT NULL AND `episode_end_datetime` IS NOT NULL AND `episode_start_datetime` > `episode_end_datetime` THEN NULL ELSE `episode_end_datetime` END AS `episode_end_datetime`',
    '`episode_type_cd` AS `episode_type_cd`',
    '`hosp_spell_id` AS `hosp_spell_id`',
    '`hrg_cd` AS `hrg_cd`',
    '`hrg_desc` AS `hrg_desc`',
    '`fce_hrg_cd` AS `fce_hrg_cd`',
    '`fce_hrg_desc` AS `fce_hrg_desc`',
    '`spell_hrg_cd` AS `spell_hrg_cd`',
    '`spell_hrg_desc` AS `spell_hrg_desc`',
    '`appointment_date` AS `appointment_date`',
    '`appointment_time` AS `appointment_time`',
    '`critical_care_unit_function_cd` AS `critical_care_unit_function_cd`',
    '`organs_supported` AS `organs_supported`',
    '`critical_care_period_type_cd` AS `critical_care_period_type_cd`',
    '`critical_care_level_ind` AS `critical_care_level_ind`',
    '`unbundled_activity_datetime` AS `unbundled_activity_datetime`',
    '`unbundled_activity_cd` AS `unbundled_activity_cd`',
    '`unbundled_hrg_cd` AS `unbundled_hrg_cd`',
    '`unbundled_hrg_desc` AS `unbundled_hrg_desc`',
    '`partial_costing_ind` AS `partial_costing_ind`',
    '`care_datetime` AS `care_datetime`',
    '`care_id` AS `care_id`',
    '`clinical_contact_duration` AS `clinical_contact_duration`',
    '`chs_currency_cd` AS `chs_currency_cd`',
    '`team_type_cd` AS `team_type_cd`',
    '`contact_subject_cd` AS `contact_subject_cd`',
    '`consult_type_cd` AS `consult_type_cd`',
    '`consult_medium_cd` AS `consult_medium_cd`',
    '`location_cd` AS `location_cd`',
    '`gp_therapy_ind` AS `gp_therapy_ind`',
    '`service_request_id` AS `service_request_id`',
    '`cost_line_count` AS `cost_line_count`',
    '`total_cost_sum` AS `total_cost_sum`',
    '`total_o_cost_sum` AS `total_o_cost_sum`',
    '`person_link_method` AS `person_link_method`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_COSTED_ACTIVITY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`episode_end_raw` AS `episode_end_raw`',
    '`episode_end_defaulted_ind` AS `episode_end_defaulted_ind`',
]


# contract v2: QC/batch inputs come from internal _clinical_costed_activity_metadata; source history stays on the main research table.
CLINICAL_COSTED_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 5,562,087 rows of 5,562,087
    # that are current and attributable; identity_status = 'resolved' keeps the 5,380,935
    # rows of 5,562,087 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_COSTED_ACTIVITY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.costed_activity.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 181,152 of 5,562,087 at the profile.
    "gold.clinical.costed_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 5,562,087 at the profile.
    "gold.clinical.costed_activity.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 630 of 5,562,087 rows (0.0113%) when profiled on 2026-08-24.
    "gold.clinical.costed_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 5,201 of 5,562,087 rows (0.0935%) when profiled on 2026-08-24.
    "gold.clinical.costed_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_COSTED_ACTIVITY_PERSON_SCOPE_RULES = {
    "gold.clinical.costed_activity.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_COSTED_ACTIVITY_ORPHAN_RULES = {
    "gold.clinical.costed_activity.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_costed_activity"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_COSTED_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COSTED_ACTIVITY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_COSTED_ACTIVITY_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_COSTED_ACTIVITY_ORPHAN_RULES)
def _gold_qc_clinical_costed_activity():
    """Quality-controlled twin of journey_clinical.costed_activity."""
    # 20 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_costed_activity",
        CLINICAL_COSTED_ACTIVITY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.costed_activity"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_costed_activity():
    """Contract-v2 public twin of clinical_costed_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_costed_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
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
        '`extract_cd` AS `extract_cd`',
        '`activity_record_id` AS `activity_record_id`',
        '`feed_type` AS `feed_type`',
        '`plemi` AS `plemi`',
        '`nhs_number_status_cd` AS `nhs_number_status_cd`',
        '`cds_id` AS `cds_id`',
        '`attendance_id` AS `attendance_id`',
        '`arrival_date` AS `arrival_date`',
        '`arrival_time` AS `arrival_time`',
        '`departure_date` AS `departure_date`',
        '`departure_time` AS `departure_time`',
        '`departure_type_cd` AS `departure_type_cd`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`patient_org_cd` AS `patient_org_cd`',
        '`pathway_id` AS `pathway_id`',
        '`pod_cd` AS `pod_cd`',
        '`treatment_function_cd` AS `treatment_function_cd`',
        '`source_los` AS `source_los`',
        '`cf_band_cd` AS `cf_band_cd`',
        '`episode_number` AS `episode_number`',
        '`episode_start_datetime` AS `episode_start_datetime`',
        '`episode_end_datetime` AS `episode_end_datetime`',
        '`episode_type_cd` AS `episode_type_cd`',
        '`hosp_spell_id` AS `hosp_spell_id`',
        '`hrg_cd` AS `hrg_cd`',
        '`hrg_desc` AS `hrg_desc`',
        '`fce_hrg_cd` AS `fce_hrg_cd`',
        '`fce_hrg_desc` AS `fce_hrg_desc`',
        '`spell_hrg_cd` AS `spell_hrg_cd`',
        '`spell_hrg_desc` AS `spell_hrg_desc`',
        '`appointment_date` AS `appointment_date`',
        '`appointment_time` AS `appointment_time`',
        '`critical_care_unit_function_cd` AS `critical_care_unit_function_cd`',
        '`organs_supported` AS `organs_supported`',
        '`critical_care_period_type_cd` AS `critical_care_period_type_cd`',
        '`critical_care_level_ind` AS `critical_care_level_ind`',
        '`unbundled_activity_datetime` AS `unbundled_activity_datetime`',
        '`unbundled_activity_cd` AS `unbundled_activity_cd`',
        '`unbundled_hrg_cd` AS `unbundled_hrg_cd`',
        '`unbundled_hrg_desc` AS `unbundled_hrg_desc`',
        '`partial_costing_ind` AS `partial_costing_ind`',
        '`care_datetime` AS `care_datetime`',
        '`care_id` AS `care_id`',
        '`clinical_contact_duration` AS `clinical_contact_duration`',
        '`chs_currency_cd` AS `chs_currency_cd`',
        '`team_type_cd` AS `team_type_cd`',
        '`contact_subject_cd` AS `contact_subject_cd`',
        '`consult_type_cd` AS `consult_type_cd`',
        '`consult_medium_cd` AS `consult_medium_cd`',
        '`location_cd` AS `location_cd`',
        '`gp_therapy_ind` AS `gp_therapy_ind`',
        '`service_request_id` AS `service_request_id`',
        '`cost_line_count` AS `cost_line_count`',
        '`total_cost_sum` AS `total_cost_sum`',
        '`total_o_cost_sum` AS `total_o_cost_sum`',
        '`person_link_method` AS `person_link_method`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`episode_end_raw` AS `episode_end_raw`',  # INSR_W3 W3.12
        '`episode_end_defaulted_ind` AS `episode_end_defaulted_ind`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_costed_activity': GoldRules(
        table='clinical_costed_activity',
        select=CLINICAL_COSTED_ACTIVITY_SELECT,
        mandatory=CLINICAL_COSTED_ACTIVITY_MANDATORY_RULES,
        advisory=CLINICAL_COSTED_ACTIVITY_ADVISORY_RULES,
        person_scope=CLINICAL_COSTED_ACTIVITY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_costed_activity': {'columns': {'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; '
                                                                    'second component of the primary key.',
                                              'appointment_date': 'Appointment date for the costed '
                                                                  'outpatient or non-admitted activity, as '
                                                                  'supplied by the source extract.',
                                              'appointment_time': 'Appointment time for the costed activity '
                                                                  'as supplied by the source extract, stored '
                                                                  'in a timestamp column.',
                                              'arrival_date': 'Arrival date for the attendance, carried from '
                                                              'the source costing extract.',
                                              'arrival_time': 'Arrival time for the attendance, carried from '
                                                              'the source costing extract and held as a '
                                                              'timestamp.',
                                              'attendance_id': 'Source attendance identifier for the '
                                                               'activity record as supplied by the costing '
                                                               'extract.',
                                              'care_datetime': 'Community care contact date and time '
                                                               'supplied by the PE2122 NCC extract.',
                                              'care_id': 'Source identifier for the community care contact '
                                                         'record, supplied by the PE2122 NCC extract.',
                                              'cds_id': 'Commissioning Data Set record identifier carried '
                                                        'from the source costing extract and used in the '
                                                        'CDS-to-MRN person linkage fallback.',
                                              'cf_band_cd': 'Cystic Fibrosis year-of-care currency band '
                                                            'supplied by the source.',
                                              'chs_currency_cd': 'Community health services currency code '
                                                                 'supplied by the PE2122 NCC costing '
                                                                 'extract.',
                                              'clinical_contact_duration': 'Duration of the clinical contact '
                                                                           'recorded in the PE2122 NCC '
                                                                           'extract, in the units supplied '
                                                                           'by the source.',
                                              'confidentiality_code': 'Source confidentiality classification '
                                                                      'attached to the record for each '
                                                                      'costed activity record. It is '
                                                                      'produced by the silver transformation '
                                                                      'and has no direct bronze-column '
                                                                      'lineage entry. Source code meanings '
                                                                      'and sentinel values are retained '
                                                                      'unless the pipeline explicitly maps '
                                                                      'them; null means no code was '
                                                                      'supplied.',
                                              'consult_medium_cd': 'Consultation medium code recording how '
                                                                   'the community contact was conducted, as '
                                                                   'supplied by the PE2122 NCC costing '
                                                                   'extract (source CMedium).',
                                              'consult_type_cd': 'Consultation type code for the community '
                                                                 'contact as supplied by the PE2122 NCC '
                                                                 'costing extract (source ConsultType).',
                                              'contact_subject_cd': 'Community contact subject code supplied '
                                                                    'by the PE2122 NCC costing extract '
                                                                    '(source CCSubject), preserved without '
                                                                    'an invented mapping.',
                                              'cost_line_count': 'Duplicate-weighted count of '
                                                                 'map_slam_cost_line_item rows aggregated '
                                                                 'for this activity record, using the '
                                                                 'collapsed source duplicate counts.',
                                              'critical_care_level_ind': 'Critical care level indicator '
                                                                         'supplied by the source costing '
                                                                         'extract.',
                                              'critical_care_period_type_cd': 'Critical care period type '
                                                                              'code supplied by the source '
                                                                              'costing extract, preserved '
                                                                              'without an invented mapping.',
                                              'critical_care_unit_function_cd': 'Critical care unit function '
                                                                                'code for the activity '
                                                                                'record, supplied by the '
                                                                                'source costing extract '
                                                                                'without an invented '
                                                                                'mapping.',
                                              'departure_date': 'Departure date for the attendance, carried '
                                                                'from the source costing extract (DepDate).',
                                              'departure_time': 'Departure time for the attendance, carried '
                                                                'from the source costing extract and held as '
                                                                'a timestamp.',
                                              'departure_type_cd': 'Source code describing the type of '
                                                                   'departure recorded for the attendance; '
                                                                   'preserved without an invented mapping.',
                                              'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                              'available.',
                                              'episode_end_datetime': 'Episode end date and time as supplied '
                                                                      'by the source PLICS costing extract '
                                                                      '(EpEnDte).',
                                              'episode_end_defaulted_ind': 'True when the episode end is the '
                                                                           'PLICS financial-year-end default '
                                                                           '(2021-03-31 23:59 with no '
                                                                           'departure date): an extract '
                                                                           'boundary, not a discharge (J23). '
                                                                           'INSR_W2.',
                                              'episode_end_raw': 'episode_end_datetime exactly as supplied '
                                                                 'by the PLICS extract, before the J23 '
                                                                 'FY-end default rule. INSR_W2.',
                                              'episode_number': 'Sequence number of the episode within the '
                                                                'hospital spell, as supplied by the source '
                                                                'costing extract (EpiNo).',
                                              'episode_start_datetime': 'Episode start date and time as '
                                                                        'supplied by the source PLICS '
                                                                        'costing extract (EpStDte).',
                                              'episode_type_cd': 'Source-supplied episode type '
                                                                 'classification code from the PLICS costing '
                                                                 'extract, preserved without an invented '
                                                                 'mapping.',
                                              'event_datetime': 'Start date and time of the costed activity '
                                                                'as supplied by the source PLICS extract '
                                                                '(ActivityStartDate).',
                                              'event_datetime_local': 'Start date and time of the costed '
                                                                      'activity event expressed in local '
                                                                      'time.',
                                              'event_end_datetime': 'End date and time of the costed '
                                                                    'activity as supplied by the source '
                                                                    'PLICS extract (ActivityEndDate).',
                                              'event_end_datetime_local': 'End date and time of the costed '
                                                                          'activity event expressed in local '
                                                                          'time. Gold: NULL wherever Gold '
                                                                          'nulls event_end_datetime.',
                                              'extract_cd': 'SLAM EXTRACT_CD; first component of the primary '
                                                            'key.',
                                              'fact_category': 'Contract category used to group the record '
                                                               'with comparable clinical facts for each '
                                                               'costed activity record. It is produced by '
                                                               'the silver transformation and has no direct '
                                                               'bronze-column lineage entry. Whitespace and '
                                                               'source sentinel text are retained unless the '
                                                               'pipeline explicitly normalizes them; null '
                                                               'means no value was supplied.',
                                              'fce_hrg_cd': 'Healthcare Resource Group code derived at '
                                                            'finished consultant episode level in the source '
                                                            'costing extract.',
                                              'fce_hrg_desc': 'Description of FCE_HRG_CD sourced from the '
                                                              'HRG v4 reference lookup.',
                                              'feed_type': 'Source-supplied code identifying the activity '
                                                           'feed the costed record came from, preserved '
                                                           'without an invented mapping.',
                                              'gp_therapy_ind': 'Source-supplied therapy indicator flag for '
                                                                'the community contact taken from the PE2122 '
                                                                'NCC extract (source GPTherapyInd), '
                                                                'preserved without an invented mapping.',
                                              'hosp_spell_id': 'Source hospital spell identifier for the '
                                                               'activity record, as supplied by the PLICS '
                                                               'costing extract.',
                                              'hrg_cd': 'Healthcare Resource Group (HRG4+) code assigned to '
                                                        'the costed activity record in the source extract.',
                                              'hrg_desc': 'Description of HRG_CD sourced from the HRG v4 '
                                                          'reference lookup.',
                                              'loaded_at': 'map_slam_costed_activity.ADC_UPDT carried '
                                                           'unchanged for the contributing activity row, '
                                                           'identical to source_update_timestamp. No '
                                                           'cost-line child clock is joined or aggregated '
                                                           "here. This is the input row's load provenance, "
                                                           'not the clinical activity date or Silver refresh '
                                                           'time.',
                                              'location_cd': 'Source location code for the site or place '
                                                             'where the costed activity or contact took '
                                                             'place, as supplied by the PE2122 NCC extract '
                                                             '(source LocCode).',
                                              'nhs_number_status_cd': 'Non-identifying source quality/status '
                                                                      'code used during linkage.',
                                              'organs_supported': 'Source-supplied critical care organs '
                                                                  'supported value for the activity record.',
                                              'partial_costing_ind': 'Source indicator flagging partially '
                                                                     'costed activity, available only from '
                                                                     'the PE2122 NCC extract.',
                                              'pathway_id': 'Patient pathway identifier for the activity '
                                                            'record as supplied by the source costing '
                                                            'extract (PathID); use with EXTRACT_CD as '
                                                            'extract-scoped identifiers are not unique '
                                                            'across extracts.',
                                              'patient_event_key': 'Deterministic event key derived from '
                                                                   'source identifiers; retained as a stable '
                                                                   'join key.',
                                              'patient_org_cd': 'Organisation code associated with the '
                                                                "patient's commissioner or responsible "
                                                                'organisation, as supplied by the source '
                                                                'extract.',
                                              'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                           'resolved.',
                                              'person_link_method': 'Code recording the linkage path used to '
                                                                    'derive PERSON_ID, distinguishing the '
                                                                    'NHS-number alias match from the '
                                                                    'CDS-to-MRN fallback.',
                                              'plemi': 'PLICS matching identifier reused across extracts; '
                                                       'never join without EXTRACT_CD.',
                                              'pod_cd': 'Source point-of-delivery classification; preserved '
                                                        'without an invented mapping.',
                                              'provider_org_cd': 'Organisation code of the provider '
                                                                 'submitting the costed activity, as '
                                                                 'supplied by the source extract (OrgId).',
                                              'record_status': 'Retracted when '
                                                               'map_slam_costed_activity.SOURCE_PRESENT_IND '
                                                               'is false, otherwise active; missing presence '
                                                               'defaults true. No superseded status is '
                                                               'emitted. This is source-row presence, not '
                                                               "the clinical activity's completion state or "
                                                               'the partial-costing indicator.',
                                              'record_status_effective_from': 'Start date and time of the '
                                                                              'costed activity as supplied '
                                                                              'by the source PLICS extract '
                                                                              '(ActivityStartDate).',
                                              'record_status_effective_to': 'map_slam_costed_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                            'only when SOURCE_PRESENT_IND is '
                                                                            'false; otherwise null. Missing '
                                                                            'absence time remains null; '
                                                                            'ACTIVITY_END_DT_TM and ADC_UPDT '
                                                                            'are not fallbacks. This is the '
                                                                            'row-absence end proxy, not the '
                                                                            'clinical activity end.',
                                              'service_request_id': 'Source service request identifier for '
                                                                    'the community contact carried from the '
                                                                    'PE2122 NCC costing extract (source '
                                                                    'SerReqID); use with EXTRACT_CD as '
                                                                    'extract-scoped identifiers are not '
                                                                    'unique across extracts.',
                                              'source_code': 'Source point-of-delivery classification; '
                                                             'preserved without an invented mapping.',
                                              'source_coding_system': 'Source point-of-delivery '
                                                                      'classification; preserved without an '
                                                                      'invented mapping.',
                                              'source_display': 'Human-readable label supplied by the source '
                                                                'system for the source code for each costed '
                                                                'activity record. It is produced by the '
                                                                'silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                              'source_feed': 'Originating data feed responsible for the '
                                                             'record for each costed activity record. It is '
                                                             'produced by the silver transformation and has '
                                                             'no direct bronze-column lineage entry. '
                                                             'Whitespace and source sentinel text are '
                                                             'retained unless the pipeline explicitly '
                                                             'normalizes them; null means no value was '
                                                             'supplied.',
                                              'source_los': 'Length-of-stay value carried from the source '
                                                            'costing extract without recalculation.',
                                              'source_update_timestamp': 'map_slam_costed_activity.ADC_UPDT '
                                                                         'carried unchanged, identical to '
                                                                         'loaded_at. The Silver projection '
                                                                         'supplies no independent native '
                                                                         'application-update clock and does '
                                                                         'not substitute Silver refresh '
                                                                         'time.',
                                              'spell_hrg_cd': 'Healthcare Resource Group code derived at '
                                                              'hospital spell level in the source costing '
                                                              'extract.',
                                              'spell_hrg_desc': 'Description of SPELL_HRG_CD sourced from '
                                                                'the HRG v4 reference lookup.',
                                              'subject_id_system': 'Internal Millennium person identifier '
                                                                   'assigned by the person-alias linkage, '
                                                                   'used to group activity for the same '
                                                                   'patient without publishing direct '
                                                                   'identifiers.',
                                              'subject_key': 'Deterministic SHA-256 over the strongest '
                                                             'source identifier. Not salted, not secret; a '
                                                             'stable join key across feeds where person_id '
                                                             'is unresolved.',
                                              'team_type_cd': 'Coded team type for the community contact, '
                                                              'supplied as an integer by the PE2122 NCC '
                                                              'extract.',
                                              'total_cost_sum': 'Duplicate-weighted sum of TOTAL_COST across '
                                                                'the cost line items for this activity '
                                                                'record, retained at exact decimal '
                                                                'precision.',
                                              'total_o_cost_sum': 'Duplicate-weighted sum of TOTAL_O_COST, a '
                                                                  'component of total cost, across the cost '
                                                                  'line items for this activity record, '
                                                                  'retained at exact decimal precision.',
                                              'treatment_function_cd': 'Treatment function code for the '
                                                                       'costed activity as supplied by the '
                                                                       'source costing extract (Tfc).',
                                              'unbundled_activity_cd': 'Code identifying the unbundled '
                                                                       'activity component of the costed '
                                                                       'record, as supplied by the source '
                                                                       'extract.',
                                              'unbundled_activity_datetime': 'Date and time of the unbundled '
                                                                             'activity component associated '
                                                                             'with the costed record.',
                                              'unbundled_hrg_cd': 'Healthcare Resource Group code for the '
                                                                  'unbundled activity component of the '
                                                                  'costed record.',
                                              'unbundled_hrg_desc': 'Description of UNBUNDLED_HRG_CD sourced '
                                                                    'from the HRG v4 reference lookup.',
                                              'vip_ind': 'Indicator that the source record carries VIP '
                                                         'handling status for each costed activity record. '
                                                         'It is produced by the silver transformation and '
                                                         'has no direct bronze-column lineage entry. Null '
                                                         'means the source did not state the indicator and '
                                                         'must not be interpreted as false.',
                                              'withheld_identity_ind': 'Indicator that the source identity '
                                                                       'was deliberately withheld for each '
                                                                       'costed activity record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Null '
                                                                       'means the source did not state the '
                                                                       'indicator and must not be '
                                                                       'interpreted as false.'},
                                  'quantity_override': True,
                                  'table': 'Internal quality-controlled twin of clinical_costed_activity: '
                                           'the Gold repairs, nulls and expectations are applied here and '
                                           'the published Gold MV reads this object. Materialized rather '
                                           'than temporary so the published MV can refresh incrementally.'},
 'clinical_costed_activity': {'columns': {'activity_record_id': 'SLAM ACTIVITY_RECORD_ID as BIGINT; second '
                                                                'component of the primary key.',
                                          'appointment_date': 'Appointment date for the costed outpatient or '
                                                              'non-admitted activity, as supplied by the '
                                                              'source extract.',
                                          'appointment_time': 'Appointment time for the costed activity as '
                                                              'supplied by the source extract, stored in a '
                                                              'timestamp column.',
                                          'arrival_date': 'Arrival date for the attendance, carried from the '
                                                          'source costing extract.',
                                          'arrival_time': 'Arrival time for the attendance, carried from the '
                                                          'source costing extract and held as a timestamp.',
                                          'attendance_id': 'Source attendance identifier for the activity '
                                                           'record as supplied by the costing extract.',
                                          'care_datetime': 'Community care contact date and time supplied by '
                                                           'the PE2122 NCC extract.',
                                          'care_id': 'Source identifier for the community care contact '
                                                     'record, supplied by the PE2122 NCC extract.',
                                          'cds_id': 'Commissioning Data Set record identifier carried from '
                                                    'the source costing extract and used in the CDS-to-MRN '
                                                    'person linkage fallback.',
                                          'cf_band_cd': 'Cystic Fibrosis year-of-care currency band supplied '
                                                        'by the source.',
                                          'chs_currency_cd': 'Community health services currency code '
                                                             'supplied by the PE2122 NCC costing extract.',
                                          'clinical_contact_duration': 'Duration of the clinical contact '
                                                                       'recorded in the PE2122 NCC extract, '
                                                                       'in the units supplied by the source.',
                                          'confidentiality_code': 'Source confidentiality classification '
                                                                  'attached to the record for each costed '
                                                                  'activity record. It is produced by the '
                                                                  'silver transformation and has no direct '
                                                                  'bronze-column lineage entry. Source code '
                                                                  'meanings and sentinel values are retained '
                                                                  'unless the pipeline explicitly maps them; '
                                                                  'null means no code was supplied.',
                                          'consult_medium_cd': 'Consultation medium code recording how the '
                                                               'community contact was conducted, as supplied '
                                                               'by the PE2122 NCC costing extract (source '
                                                               'CMedium).',
                                          'consult_type_cd': 'Consultation type code for the community '
                                                             'contact as supplied by the PE2122 NCC costing '
                                                             'extract (source ConsultType).',
                                          'contact_subject_cd': 'Community contact subject code supplied by '
                                                                'the PE2122 NCC costing extract (source '
                                                                'CCSubject), preserved without an invented '
                                                                'mapping.',
                                          'cost_line_count': 'Duplicate-weighted count of '
                                                             'map_slam_cost_line_item rows aggregated for '
                                                             'this activity record, using the collapsed '
                                                             'source duplicate counts.',
                                          'critical_care_level_ind': 'Critical care level indicator supplied '
                                                                     'by the source costing extract.',
                                          'critical_care_period_type_cd': 'Critical care period type code '
                                                                          'supplied by the source costing '
                                                                          'extract, preserved without an '
                                                                          'invented mapping.',
                                          'critical_care_unit_function_cd': 'Critical care unit function '
                                                                            'code for the activity record, '
                                                                            'supplied by the source costing '
                                                                            'extract without an invented '
                                                                            'mapping.',
                                          'departure_date': 'Departure date for the attendance, carried from '
                                                            'the source costing extract (DepDate).',
                                          'departure_time': 'Departure time for the attendance, carried from '
                                                            'the source costing extract and held as a '
                                                            'timestamp.',
                                          'departure_type_cd': 'Source code describing the type of departure '
                                                               'recorded for the attendance; preserved '
                                                               'without an invented mapping.',
                                          'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                          'available.',
                                          'episode_end_datetime': 'Episode end date and time as supplied by '
                                                                  'the source PLICS costing extract '
                                                                  '(EpEnDte).',
                                          'episode_end_defaulted_ind': 'True when the episode end is the '
                                                                       'PLICS financial-year-end default '
                                                                       '(2021-03-31 23:59 with no departure '
                                                                       'date): an extract boundary, not a '
                                                                       'discharge (J23). INSR_W2.',
                                          'episode_end_raw': 'episode_end_datetime exactly as supplied by '
                                                             'the PLICS extract, before the J23 FY-end '
                                                             'default rule. INSR_W2.',
                                          'episode_number': 'Sequence number of the episode within the '
                                                            'hospital spell, as supplied by the source '
                                                            'costing extract (EpiNo).',
                                          'episode_start_datetime': 'Episode start date and time as supplied '
                                                                    'by the source PLICS costing extract '
                                                                    '(EpStDte).',
                                          'episode_type_cd': 'Source-supplied episode type classification '
                                                             'code from the PLICS costing extract, preserved '
                                                             'without an invented mapping.',
                                          'event_datetime': 'Start date and time of the costed activity as '
                                                            'supplied by the source PLICS extract '
                                                            '(ActivityStartDate).',
                                          'event_datetime_local': 'Start date and time of the costed '
                                                                  'activity event expressed in local time.',
                                          'event_end_datetime': 'End date and time of the costed activity as '
                                                                'supplied by the source PLICS extract '
                                                                '(ActivityEndDate).',
                                          'event_end_datetime_local': 'End date and time of the costed '
                                                                      'activity event expressed in local '
                                                                      'time. Gold: NULL wherever Gold nulls '
                                                                      'event_end_datetime.',
                                          'extract_cd': 'SLAM EXTRACT_CD; first component of the primary '
                                                        'key.',
                                          'fact_category': 'Contract category used to group the record with '
                                                           'comparable clinical facts for each costed '
                                                           'activity record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Whitespace and source sentinel '
                                                           'text are retained unless the pipeline explicitly '
                                                           'normalizes them; null means no value was '
                                                           'supplied.',
                                          'fce_hrg_cd': 'Healthcare Resource Group code derived at finished '
                                                        'consultant episode level in the source costing '
                                                        'extract.',
                                          'fce_hrg_desc': 'Description of FCE_HRG_CD sourced from the HRG v4 '
                                                          'reference lookup.',
                                          'feed_type': 'Source-supplied code identifying the activity feed '
                                                       'the costed record came from, preserved without an '
                                                       'invented mapping.',
                                          'gp_therapy_ind': 'Source-supplied therapy indicator flag for the '
                                                            'community contact taken from the PE2122 NCC '
                                                            'extract (source GPTherapyInd), preserved '
                                                            'without an invented mapping.',
                                          'hosp_spell_id': 'Source hospital spell identifier for the '
                                                           'activity record, as supplied by the PLICS '
                                                           'costing extract.',
                                          'hrg_cd': 'Healthcare Resource Group (HRG4+) code assigned to the '
                                                    'costed activity record in the source extract.',
                                          'hrg_desc': 'Description of HRG_CD sourced from the HRG v4 '
                                                      'reference lookup.',
                                          'loaded_at': 'map_slam_costed_activity.ADC_UPDT carried unchanged '
                                                       'for the contributing activity row, identical to '
                                                       'source_update_timestamp. No cost-line child clock is '
                                                       "joined or aggregated here. This is the input row's "
                                                       'load provenance, not the clinical activity date or '
                                                       'Silver refresh time.',
                                          'location_cd': 'Source location code for the site or place where '
                                                         'the costed activity or contact took place, as '
                                                         'supplied by the PE2122 NCC extract (source '
                                                         'LocCode).',
                                          'nhs_number_status_cd': 'Non-identifying source quality/status '
                                                                  'code used during linkage.',
                                          'organs_supported': 'Source-supplied critical care organs '
                                                              'supported value for the activity record.',
                                          'partial_costing_ind': 'Source indicator flagging partially costed '
                                                                 'activity, available only from the PE2122 '
                                                                 'NCC extract.',
                                          'pathway_id': 'Patient pathway identifier for the activity record '
                                                        'as supplied by the source costing extract (PathID); '
                                                        'use with EXTRACT_CD as extract-scoped identifiers '
                                                        'are not unique across extracts.',
                                          'patient_event_key': 'Deterministic event key derived from source '
                                                               'identifiers; retained as a stable join key.',
                                          'patient_org_cd': "Organisation code associated with the patient's "
                                                            'commissioner or responsible organisation, as '
                                                            'supplied by the source extract.',
                                          'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                          'person_link_method': 'Code recording the linkage path used to '
                                                                'derive PERSON_ID, distinguishing the '
                                                                'NHS-number alias match from the CDS-to-MRN '
                                                                'fallback.',
                                          'plemi': 'PLICS matching identifier reused across extracts; never '
                                                   'join without EXTRACT_CD.',
                                          'pod_cd': 'Source point-of-delivery classification; preserved '
                                                    'without an invented mapping.',
                                          'provider_org_cd': 'Organisation code of the provider submitting '
                                                             'the costed activity, as supplied by the source '
                                                             'extract (OrgId).',
                                          'record_status': 'Retracted when '
                                                           'map_slam_costed_activity.SOURCE_PRESENT_IND is '
                                                           'false, otherwise active; missing presence '
                                                           'defaults true. No superseded status is emitted. '
                                                           'This is source-row presence, not the clinical '
                                                           "activity's completion state or the "
                                                           'partial-costing indicator.',
                                          'record_status_effective_from': 'Start date and time of the costed '
                                                                          'activity as supplied by the '
                                                                          'source PLICS extract '
                                                                          '(ActivityStartDate).',
                                          'record_status_effective_to': 'map_slam_costed_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                        'only when SOURCE_PRESENT_IND is '
                                                                        'false; otherwise null. Missing '
                                                                        'absence time remains null; '
                                                                        'ACTIVITY_END_DT_TM and ADC_UPDT are '
                                                                        'not fallbacks. This is the '
                                                                        'row-absence end proxy, not the '
                                                                        'clinical activity end.',
                                          'service_request_id': 'Source service request identifier for the '
                                                                'community contact carried from the PE2122 '
                                                                'NCC costing extract (source SerReqID); use '
                                                                'with EXTRACT_CD as extract-scoped '
                                                                'identifiers are not unique across extracts.',
                                          'source_code': 'Source point-of-delivery classification; preserved '
                                                         'without an invented mapping.',
                                          'source_coding_system': 'Source point-of-delivery classification; '
                                                                  'preserved without an invented mapping.',
                                          'source_display': 'Human-readable label supplied by the source '
                                                            'system for the source code for each costed '
                                                            'activity record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                          'source_feed': 'Originating data feed responsible for the record '
                                                         'for each costed activity record. It is produced by '
                                                         'the silver transformation and has no direct '
                                                         'bronze-column lineage entry. Whitespace and source '
                                                         'sentinel text are retained unless the pipeline '
                                                         'explicitly normalizes them; null means no value '
                                                         'was supplied.',
                                          'source_los': 'Length-of-stay value carried from the source '
                                                        'costing extract without recalculation.',
                                          'source_update_timestamp': 'map_slam_costed_activity.ADC_UPDT '
                                                                     'carried unchanged, identical to '
                                                                     'loaded_at. The Silver projection '
                                                                     'supplies no independent native '
                                                                     'application-update clock and does not '
                                                                     'substitute Silver refresh time.',
                                          'spell_hrg_cd': 'Healthcare Resource Group code derived at '
                                                          'hospital spell level in the source costing '
                                                          'extract.',
                                          'spell_hrg_desc': 'Description of SPELL_HRG_CD sourced from the '
                                                            'HRG v4 reference lookup.',
                                          'subject_id_system': 'Internal Millennium person identifier '
                                                               'assigned by the person-alias linkage, used '
                                                               'to group activity for the same patient '
                                                               'without publishing direct identifiers.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'team_type_cd': 'Coded team type for the community contact, '
                                                          'supplied as an integer by the PE2122 NCC extract.',
                                          'total_cost_sum': 'Duplicate-weighted sum of TOTAL_COST across the '
                                                            'cost line items for this activity record, '
                                                            'retained at exact decimal precision.',
                                          'total_o_cost_sum': 'Duplicate-weighted sum of TOTAL_O_COST, a '
                                                              'component of total cost, across the cost line '
                                                              'items for this activity record, retained at '
                                                              'exact decimal precision.',
                                          'treatment_function_cd': 'Treatment function code for the costed '
                                                                   'activity as supplied by the source '
                                                                   'costing extract (Tfc).',
                                          'unbundled_activity_cd': 'Code identifying the unbundled activity '
                                                                   'component of the costed record, as '
                                                                   'supplied by the source extract.',
                                          'unbundled_activity_datetime': 'Date and time of the unbundled '
                                                                         'activity component associated with '
                                                                         'the costed record.',
                                          'unbundled_hrg_cd': 'Healthcare Resource Group code for the '
                                                              'unbundled activity component of the costed '
                                                              'record.',
                                          'unbundled_hrg_desc': 'Description of UNBUNDLED_HRG_CD sourced '
                                                                'from the HRG v4 reference lookup.',
                                          'vip_ind': 'Indicator that the source record carries VIP handling '
                                                     'status for each costed activity record. It is produced '
                                                     'by the silver transformation and has no direct '
                                                     'bronze-column lineage entry. Null means the source did '
                                                     'not state the indicator and must not be interpreted as '
                                                     'false.',
                                          'withheld_identity_ind': 'Indicator that the source identity was '
                                                                   'deliberately withheld for each costed '
                                                                   'activity record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Null means '
                                                                   'the source did not state the indicator '
                                                                   'and must not be interpreted as false.'},
                              'quantity_override': False,
                              'table': 'One frozen PLICS costed activity. Gold QC twin of the silver '
                                       'product: 3 columns are repaired or nulled, 1 rule(s) drop rows, 4 '
                                       'check(s) are advisory. Each rule states its reason in the pipeline '
                                       'notebook, and Lakeflow expectation metrics report what every rule '
                                       'matched on each update. Coverage: Frozen feeds with no rows in the '
                                       'last 12 months: condition stage to 2023-10, costed activity and cost '
                                       'line items to 2022-03, device to 2020-01; complete-through month per '
                                       'release: 6_mgmt.data_quality.release_finding (finding_code '
                                       'F2_complete_through).'}}
