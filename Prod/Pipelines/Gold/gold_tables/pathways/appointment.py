"""Gold pathways.appointment: 2 dataset(s).

- datasets: _qc_clinical_appointment, clinical_appointment
- moved verbatim from: 4_gold_care_pathways:165, 4_gold_care_pathways:169, 4_gold_care_pathways:182, 4_gold_care_pathways:29, 4_gold_care_pathways:80, 4_gold_care_pathways:88

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.appointment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_APPOINTMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`sch_event_id` AS `sch_event_id`',
    '`recurrence_parent_sch_event_id` AS `recurrence_parent_sch_event_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    "CASE WHEN UPPER(TRIM(CAST(`appointment_type_code` AS STRING))) = '0' THEN NULL ELSE `appointment_type_code` END AS `appointment_type_code`",
    '`appointment_type_display` AS `appointment_type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`status_meaning` AS `status_meaning`',
    '`referral_identifier` AS `referral_identifier`',
    '`requested_datetime` AS `requested_datetime`',
    '`original_requested_start` AS `original_requested_start`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `original_requested_start` IS NOT NULL AND `original_requested_end` IS NOT NULL AND `original_requested_start` > `original_requested_end` THEN NULL ELSE CASE WHEN (CAST(`original_requested_end` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`original_requested_end` AS DATE)) < 9999) THEN NULL ELSE `original_requested_end` END END AS `original_requested_end`",
    '`first_booked_datetime` AS `first_booked_datetime`',
    '`requested_practitioner_id` AS `requested_practitioner_id`',
    '`allocated_practitioner_id` AS `allocated_practitioner_id`',
    '`location_code` AS `location_code`',
    '`location_display` AS `location_display`',  # SDI_APPT_LOCATION_V1
    '`clinic_resource_code` AS `clinic_resource_code`',  # SDI_APPT_LOCATION_V1
    '`clinic_resource_display` AS `clinic_resource_display`',  # SDI_APPT_LOCATION_V1
    '`service_resource_code` AS `service_resource_code`',  # SDI_APPT_LOCATION_V1
    '`service_resource_display` AS `service_resource_display`',  # SDI_APPT_LOCATION_V1
    '`organization_id` AS `organization_id`',
    '`recurrence_parent_key` AS `recurrence_parent_key`',
    '`recurrence_type_flag` AS `recurrence_type_flag`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    # INSR_W3 W3.05: Gold: NULL where an open-end marker in [2099-12-31, 9999).
    "CASE WHEN (CAST(`record_status_effective_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`record_status_effective_to` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_to` END AS `record_status_effective_to`",
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_appointment_metadata; source history stays on the main research table.
CLINICAL_APPOINTMENT_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 31,446,236 rows of
    # 32,293,081 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_APPOINTMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.appointment.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 846,845 of 32,293,081 at the profile.
    "gold.clinical.appointment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 64,860 of 32,293,081 rows (0.201%) when profiled on 2026-08-24.
    "gold.clinical.appointment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 451 of 32,293,081 rows (0.0014%) when profiled on 2026-08-24.
    "gold.clinical.appointment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_APPOINTMENT_PERSON_SCOPE_RULES = {
    "gold.clinical.appointment.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_appointment"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_APPOINTMENT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_APPOINTMENT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_appointment():
    """Quality-controlled twin of journey_clinical.appointment."""
    df = _qc("clinical_appointment", CLINICAL_APPOINTMENT_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.appointment"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_appointment():
    """Contract-v2 public twin of clinical_appointment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_appointment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`sch_event_id` AS `sch_event_id`',
        '`recurrence_parent_sch_event_id` AS `recurrence_parent_sch_event_id`',
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
        '`appointment_type_code` AS `appointment_type_code`',
        '`appointment_type_display` AS `appointment_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`status_meaning` AS `status_meaning`',
        '`referral_identifier` AS `referral_identifier`',
        '`requested_datetime` AS `requested_datetime`',
        '`original_requested_start` AS `original_requested_start`',
        '`original_requested_end` AS `original_requested_end`',
        '`first_booked_datetime` AS `first_booked_datetime`',
        '`requested_practitioner_id` AS `requested_practitioner_id`',
        '`allocated_practitioner_id` AS `allocated_practitioner_id`',
        '`location_code` AS `location_code`',
        '`location_display` AS `location_display`',  # SDI_APPT_LOCATION_V1
        '`clinic_resource_code` AS `clinic_resource_code`',  # SDI_APPT_LOCATION_V1
        '`clinic_resource_display` AS `clinic_resource_display`',  # SDI_APPT_LOCATION_V1
        '`service_resource_code` AS `service_resource_code`',  # SDI_APPT_LOCATION_V1
        '`service_resource_display` AS `service_resource_display`',  # SDI_APPT_LOCATION_V1
        '`organization_id` AS `organization_id`',
        '`recurrence_parent_key` AS `recurrence_parent_key`',
        '`recurrence_type_flag` AS `recurrence_type_flag`',
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
    )
    return df


RULES = {
    'clinical_appointment': GoldRules(
        table='clinical_appointment',
        select=CLINICAL_APPOINTMENT_SELECT,
        mandatory=CLINICAL_APPOINTMENT_MANDATORY_RULES,
        advisory=CLINICAL_APPOINTMENT_ADVISORY_RULES,
        person_scope=CLINICAL_APPOINTMENT_PERSON_SCOPE_RULES,
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
        value_rules={'gold.clinical.appointment.allocated_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                 'column': 'allocated_practitioner_id',
                                                                                 'companions': [],
                                                                                 'match': '`allocated_practitioner_id` '
                                                                                          '= 0',
                                                                                 'table': 'clinical_appointment'},
         'gold.clinical.appointment.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'organization_id',
                                                                       'companions': [],
                                                                       'match': '`organization_id` = 0',
                                                                       'table': 'clinical_appointment'},
         'gold.clinical.appointment.requested_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                 'column': 'requested_practitioner_id',
                                                                                 'companions': [],
                                                                                 'match': '`requested_practitioner_id` '
                                                                                          '= 0',
                                                                                 'table': 'clinical_appointment'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_appointment': {'columns': {'allocated_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the representative allocated '
                                                                       'practitioner.',
                                          'appointment_type_code': 'Source appointment-type code.',
                                          'appointment_type_display': 'Source appointment-type display.',
                                          'clinic_resource_code': 'Millennium RESOURCE_CD of the booked '
                                                                  'primary non-patient resource '
                                                                  '(PRIMARY_ROLE_IND 1, ROLE_MEANING not '
                                                                  'PATIENT): the clinic/session resource an '
                                                                  'outpatient appointment is booked into, or '
                                                                  'the theatre for a surgical booking. Taken '
                                                                  'from the latest SCHEDULE_SEQ, lowest code '
                                                                  'on ties. SCH_CLINIC_ID is 0 at source, so '
                                                                  'no clinic id is published. Specialty is '
                                                                  "not carried: join the linked encounter's "
                                                                  'responsible_service_display.',
                                          'clinic_resource_display': 'Source RESOURCE_DESCRIPTION for '
                                                                     'clinic_resource_code.',
                                          'confidentiality_code': 'Source confidentiality code when '
                                                                  'supplied.',
                                          'encounter_id': 'Millennium ENCNTR_ID when supplied; native '
                                                          'encounter foreign key.',
                                          'event_datetime': 'Booked slot start where available',
                                          'event_datetime_local': 'Booked appointment slot start expressed '
                                                                  'in local time where available.',
                                          'event_end_datetime': 'Booked slot end where available.',
                                          'event_end_datetime_local': 'Booked appointment slot end expressed '
                                                                      'in local time where available.',
                                          'fact_category': 'Whether this fact is clinical or administrative '
                                                           'in the v2 plane merge.',
                                          'first_booked_datetime': 'First booking timestamp supplied by '
                                                                   'scheduling.',
                                          'loaded_at': 'Greatest of map_appointment.ADC_UPDT and the '
                                                       'per-SCH_EVENT_ID maxima of ADC_UPDT from '
                                                       'map_appointment_schedule and '
                                                       'map_appointment_resource. This combines all '
                                                       'contributing appointment/booking/resource load '
                                                       'clocks, independently of chosen slot times, location '
                                                       'or personnel; it is bronze ingestion provenance, not '
                                                       'Silver refresh time.',
                                          'location_code': 'Millennium scheduling location code: the lowest '
                                                           "APPT_LOCATION_CD across the appointment's "
                                                           'resource rows, else the lowest '
                                                           'map_appointment_schedule.LOCATION_CD. Decodes '
                                                           'through reference_location (location_code, any '
                                                           'level).',
                                          'location_display': 'Display of location_code from the same side '
                                                              'that supplied it: '
                                                              'map_appointment_resource.APPT_LOCATION_DESCRIPTION, '
                                                              'else coalesce(LOCATION_DESCRIPTION, '
                                                              'LOCATION_FREETEXT) from the schedule row with '
                                                              'that code. NULL when location_code is NULL.',
                                          'organization_id': 'Millennium ORGANIZATION_ID for the scheduling '
                                                             'organization.',
                                          'original_requested_end': 'Original requested slot end.',
                                          'original_requested_start': 'Original requested slot start.',
                                          'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                               'cross-feed join key.',
                                          'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                          'record_status': 'Superseded when '
                                                           'map_appointment.SOURCE_PRESENT_IND cast to '
                                                           'Boolean is false, ACTIVE_IND cast to LONG is '
                                                           'zero, or a non-null END_EFFECTIVE_DT_TM is '
                                                           'before 2100-01-01; otherwise active. Missing '
                                                           'presence defaults true and missing activity '
                                                           'defaults 1. Source absence does not emit '
                                                           'retracted; scheduling status and '
                                                           'booking/resource flags are separate.',
                                          'record_status_effective_from': 'map_appointment.BEG_EFFECTIVE_DT_TM '
                                                                          'carried unchanged. No '
                                                                          'scheduled-slot, requested-start, '
                                                                          'first-booked or ingestion-time '
                                                                          'fallback is applied; a missing '
                                                                          'value remains null.',
                                          'record_status_effective_to': 'For superseded appointment rows, '
                                                                        'first non-null '
                                                                        'SOURCE_ABSENT_DETECTED_TS, '
                                                                        'END_EFFECTIVE_DT_TM, then ADC_UPDT; '
                                                                        'otherwise null. The selected end is '
                                                                        'not range-clamped, so an '
                                                                        'inactive/absent row can retain a '
                                                                        'far-future end. This is row-status '
                                                                        'history, not the scheduled slot '
                                                                        'end.',
                                          'recurrence_parent_key': 'Deterministic SHA-256 key of the parent '
                                                                   'appointment.',
                                          'recurrence_parent_sch_event_id': 'Millennium parent SCH_EVENT_ID '
                                                                            'for recurring appointments when '
                                                                            'supplied.',
                                          'recurrence_type_flag': 'Raw source recurrence flag.',
                                          'referral_identifier': 'UBRN or other referral alias retained as '
                                                                 'linkage evidence only.',
                                          'requested_datetime': 'Source referral/request timestamp.',
                                          'requested_practitioner_id': 'Millennium personnel PERSON_ID for '
                                                                       'the requested practitioner.',
                                          'sch_event_id': 'Millennium SCH_EVENT_ID; primary key of this '
                                                          'table.',
                                          'service_resource_code': 'Millennium SERVICE_RESOURCE_CD from the '
                                                                   'latest SCHEDULE_SEQ (lowest code on '
                                                                   'ties); populated for '
                                                                   'theatre/endoscopy/cath-lab bookings '
                                                                   'only, NULL for outpatient clinics.',
                                          'service_resource_display': 'Source SERVICE_RESOURCE_DESCRIPTION '
                                                                      'for service_resource_code.',
                                          'source_code': 'Verbatim appointment-type code.',
                                          'source_coding_system': 'Verbatim appointment-type coding system.',
                                          'source_display': 'Verbatim appointment-type display.',
                                          'source_feed': 'Registered feed owning the fact.',
                                          'source_update_timestamp': 'Greatest of '
                                                                     'map_appointment.SOURCE_ADC_UPDT and '
                                                                     'the per-SCH_EVENT_ID maxima of '
                                                                     'SOURCE_ADC_UPDT from '
                                                                     'map_appointment_schedule and '
                                                                     'map_appointment_resource. All '
                                                                     'contributing booking/resource rows '
                                                                     'participate, not only current rows or '
                                                                     'those supplying a chosen '
                                                                     'slot/location. No ADC_UPDT ingestion '
                                                                     'fallback or Silver refresh timestamp '
                                                                     'is added.',
                                          'status_code': 'Source scheduling-state code.',
                                          'status_display': 'Source scheduling-state display.',
                                          'status_meaning': 'Source scheduling-state meaning.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'vip_ind': 'Source VIP indicator when supplied.',
                                          'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                   'supplied.'},
                              'quantity_override': True,
                              'table': 'Internal quality-controlled twin of clinical_appointment: the Gold '
                                       'repairs, nulls and expectations are applied here and the published '
                                       'Gold MV reads this object. Materialized rather than temporary so the '
                                       'published MV can refresh incrementally.'},
 'clinical_appointment': {'columns': {'allocated_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                   'representative allocated practitioner.',
                                      'appointment_type_code': 'Source appointment-type code.',
                                      'appointment_type_display': 'Source appointment-type display.',
                                      'clinic_resource_code': 'Millennium RESOURCE_CD of the booked primary '
                                                              'non-patient resource (PRIMARY_ROLE_IND 1, '
                                                              'ROLE_MEANING not PATIENT): the clinic/session '
                                                              'resource an outpatient appointment is booked '
                                                              'into, or the theatre for a surgical booking. '
                                                              'Taken from the latest SCHEDULE_SEQ, lowest '
                                                              'code on ties. SCH_CLINIC_ID is 0 at source, '
                                                              'so no clinic id is published. Specialty is '
                                                              "not carried: join the linked encounter's "
                                                              'responsible_service_display.',
                                      'clinic_resource_display': 'Source RESOURCE_DESCRIPTION for '
                                                                 'clinic_resource_code.',
                                      'confidentiality_code': 'Source confidentiality code when supplied.',
                                      'encounter_id': 'Millennium ENCNTR_ID when supplied; native encounter '
                                                      'foreign key.',
                                      'event_datetime': 'Booked slot start where available',
                                      'event_datetime_local': 'Booked appointment slot start expressed in '
                                                              'local time where available.',
                                      'event_end_datetime': 'Booked slot end where available.',
                                      'event_end_datetime_local': 'Booked appointment slot end expressed in '
                                                                  'local time where available.',
                                      'fact_category': 'Whether this fact is clinical or administrative in '
                                                       'the v2 plane merge.',
                                      'first_booked_datetime': 'First booking timestamp supplied by '
                                                               'scheduling.',
                                      'loaded_at': 'Greatest of map_appointment.ADC_UPDT and the '
                                                   'per-SCH_EVENT_ID maxima of ADC_UPDT from '
                                                   'map_appointment_schedule and map_appointment_resource. '
                                                   'This combines all contributing '
                                                   'appointment/booking/resource load clocks, independently '
                                                   'of chosen slot times, location or personnel; it is '
                                                   'bronze ingestion provenance, not Silver refresh time.',
                                      'location_code': 'Millennium scheduling location code: the lowest '
                                                       "APPT_LOCATION_CD across the appointment's resource "
                                                       'rows, else the lowest '
                                                       'map_appointment_schedule.LOCATION_CD. Decodes '
                                                       'through reference_location (location_code, any '
                                                       'level).',
                                      'location_display': 'Display of location_code from the same side that '
                                                          'supplied it: '
                                                          'map_appointment_resource.APPT_LOCATION_DESCRIPTION, '
                                                          'else coalesce(LOCATION_DESCRIPTION, '
                                                          'LOCATION_FREETEXT) from the schedule row with '
                                                          'that code. NULL when location_code is NULL.',
                                      'organization_id': 'Millennium ORGANIZATION_ID for the scheduling '
                                                         'organization.',
                                      'original_requested_end': 'Original requested slot end.',
                                      'original_requested_start': 'Original requested slot start.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                      'record_status': 'Superseded when map_appointment.SOURCE_PRESENT_IND '
                                                       'cast to Boolean is false, ACTIVE_IND cast to LONG is '
                                                       'zero, or a non-null END_EFFECTIVE_DT_TM is before '
                                                       '2100-01-01; otherwise active. Missing presence '
                                                       'defaults true and missing activity defaults 1. '
                                                       'Source absence does not emit retracted; scheduling '
                                                       'status and booking/resource flags are separate.',
                                      'record_status_effective_from': 'map_appointment.BEG_EFFECTIVE_DT_TM '
                                                                      'carried unchanged. No scheduled-slot, '
                                                                      'requested-start, first-booked or '
                                                                      'ingestion-time fallback is applied; a '
                                                                      'missing value remains null.',
                                      'record_status_effective_to': 'For superseded appointment rows, first '
                                                                    'non-null SOURCE_ABSENT_DETECTED_TS, '
                                                                    'END_EFFECTIVE_DT_TM, then ADC_UPDT; '
                                                                    'otherwise null. The selected end is not '
                                                                    'range-clamped, so an inactive/absent '
                                                                    'row can retain a far-future end. This '
                                                                    'is row-status history, not the '
                                                                    'scheduled slot end.',
                                      'recurrence_parent_key': 'Deterministic SHA-256 key of the parent '
                                                               'appointment.',
                                      'recurrence_parent_sch_event_id': 'Millennium parent SCH_EVENT_ID for '
                                                                        'recurring appointments when '
                                                                        'supplied. Parent scheduling event '
                                                                        'of a recurring appointment; 0 means '
                                                                        'the appointment is not part of a '
                                                                        'recurrence (kept as 0 by design).',
                                      'recurrence_type_flag': 'Raw source recurrence flag.',
                                      'referral_identifier': 'UBRN or other referral alias retained as '
                                                             'linkage evidence only.',
                                      'requested_datetime': 'Source referral/request timestamp.',
                                      'requested_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                   'requested practitioner.',
                                      'sch_event_id': 'Millennium SCH_EVENT_ID; primary key of this table.',
                                      'service_resource_code': 'Millennium SERVICE_RESOURCE_CD from the '
                                                               'latest SCHEDULE_SEQ (lowest code on ties); '
                                                               'populated for theatre/endoscopy/cath-lab '
                                                               'bookings only, NULL for outpatient clinics.',
                                      'service_resource_display': 'Source SERVICE_RESOURCE_DESCRIPTION for '
                                                                  'service_resource_code.',
                                      'source_code': 'Verbatim appointment-type code.',
                                      'source_coding_system': 'Verbatim appointment-type coding system.',
                                      'source_display': 'Verbatim appointment-type display.',
                                      'source_feed': 'Registered feed owning the fact.',
                                      'source_update_timestamp': 'Greatest of '
                                                                 'map_appointment.SOURCE_ADC_UPDT and the '
                                                                 'per-SCH_EVENT_ID maxima of SOURCE_ADC_UPDT '
                                                                 'from map_appointment_schedule and '
                                                                 'map_appointment_resource. All contributing '
                                                                 'booking/resource rows participate, not '
                                                                 'only current rows or those supplying a '
                                                                 'chosen slot/location. No ADC_UPDT '
                                                                 'ingestion fallback or Silver refresh '
                                                                 'timestamp is added.',
                                      'status_code': 'Source scheduling-state code.',
                                      'status_display': 'Source scheduling-state display.',
                                      'status_meaning': 'Source scheduling-state meaning.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'vip_ind': 'Source VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                          'quantity_override': False,
                          'table': 'One scheduling appointment with current state and ordered '
                                   'booking/resource history. Gold QC twin of the silver product: 7 columns '
                                   'are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. '
                                   'Each rule states its reason in the pipeline notebook, and Lakeflow '
                                   'expectation metrics report what every rule matched on each update. '
                                   'Coverage: The appointment request time is not available: a scheduling '
                                   'REQUEST action exists for 21% of appointments and precedes the start on '
                                   'only 78% of those; complete-through month per release: '
                                   '6_mgmt.data_quality.release_finding (finding_code F2_complete_through).'}}
