"""Gold pathways.rtt_activity: 2 dataset(s).

- datasets: _qc_clinical_rtt_activity, clinical_rtt_activity
- moved verbatim from: 4_gold_care_pathways:2839, 4_gold_care_pathways:2885, 4_gold_care_pathways:2893, 4_gold_care_pathways:2970, 4_gold_care_pathways:2975, 4_gold_care_pathways:2979, 4_gold_care_pathways:3001

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.rtt_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RTT_ACTIVITY_SELECT = [
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
    '`rtt_activity_oid` AS `rtt_activity_oid`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_key` AS `referral_key`',
    '`appointment_oid` AS `appointment_oid`',
    '`activity_code` AS `activity_code`',
    '`activity_display` AS `activity_display`',
    '`activity_type_code` AS `activity_type_code`',
    '`activity_type_display` AS `activity_type_display`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`status_sequence_asc` AS `status_sequence_asc`',
    '`status_sequence_desc` AS `status_sequence_desc`',
    '`activity_sequence_asc` AS `activity_sequence_asc`',
    '`activity_sequence_desc` AS `activity_sequence_desc`',
    '`activity_datetime_quality` AS `activity_datetime_quality`',
    '`treatment_function_code` AS `treatment_function_code`',
    '`treatment_function_display` AS `treatment_function_display`',
    '`site_code` AS `site_code`',
    '`site_display` AS `site_display`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`source_system_oid` AS `source_system_oid`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_rtt_activity_metadata; source history stays on the main research table.
CLINICAL_RTT_ACTIVITY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 68,818,631 rows of
    # 68,820,429 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_RTT_ACTIVITY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_typo (estate 2026-10-29): RTT events 1990-2006 predate the 18-week standard: kept, warned (L4879).
    'gold.clinical.rtt_activity.event_datetime.insr_f6_rtt_event_early':
        "NOT COALESCE((`event_datetime` >= TIMESTAMP'1990-01-01 00:00:00' AND `event_datetime` < TIMESTAMP'2007-01-01 00:00:00'), FALSE)",
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.rtt_activity.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,798 of 68,820,429 at the profile.
    "gold.clinical.rtt_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 30,737 of 68,820,429 rows (0.0447%) when profiled on 2026-08-24.
    "gold.clinical.rtt_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 2,170 of 68,820,429 rows (0.00315%) when profiled on 2026-08-24.
    "gold.clinical.rtt_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_RTT_ACTIVITY_PERSON_SCOPE_RULES = {
    "gold.clinical.rtt_activity.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_RTT_ACTIVITY_ORPHAN_RULES = {
    "gold.clinical.rtt_activity.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_activity"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_RTT_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RTT_ACTIVITY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_RTT_ACTIVITY_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_RTT_ACTIVITY_ORPHAN_RULES)
def _gold_qc_clinical_rtt_activity():
    """Quality-controlled twin of journey_clinical.rtt_activity."""
    # 6 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_rtt_activity",
        CLINICAL_RTT_ACTIVITY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.rtt_activity"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_rtt_activity():
    """Contract-v2 public twin of clinical_rtt_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_rtt_activity")).selectExpr(
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
        '`rtt_activity_oid` AS `rtt_activity_oid`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_key` AS `referral_key`',
        '`appointment_oid` AS `appointment_oid`',
        '`activity_code` AS `activity_code`',
        '`activity_display` AS `activity_display`',
        '`activity_type_code` AS `activity_type_code`',
        '`activity_type_display` AS `activity_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`status_sequence_asc` AS `status_sequence_asc`',
        '`status_sequence_desc` AS `status_sequence_desc`',
        '`activity_sequence_asc` AS `activity_sequence_asc`',
        '`activity_sequence_desc` AS `activity_sequence_desc`',
        '`activity_datetime_quality` AS `activity_datetime_quality`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`treatment_function_display` AS `treatment_function_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`source_system_oid` AS `source_system_oid`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'clinical_rtt_activity': GoldRules(
        table='clinical_rtt_activity',
        select=CLINICAL_RTT_ACTIVITY_SELECT,
        mandatory=CLINICAL_RTT_ACTIVITY_MANDATORY_RULES,
        advisory=CLINICAL_RTT_ACTIVITY_ADVISORY_RULES,
        person_scope=CLINICAL_RTT_ACTIVITY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'is_illogical',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        value_rules={'gold.clinical.rtt_activity.appointment_oid.insr_f7_zero_id': {'action': 'invalidate',
                                                                        'column': 'appointment_oid',
                                                                        'companions': [],
                                                                        'match': '`appointment_oid` = 0',
                                                                        'table': 'clinical_rtt_activity'},
         'gold.clinical.rtt_activity.event_datetime.insr_f6_rtt_event_floor': {'action': 'invalidate',
                                                                               'column': 'event_datetime',
                                                                               'companions': ['event_datetime_local'],
                                                                               'match': '`event_datetime` '
                                                                                        '< '
                                                                                        "TIMESTAMP'1990-01-01 "
                                                                                        "00:00:00'",
                                                                               'table': 'clinical_rtt_activity'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_rtt_activity': {'columns': {'activity_code': 'Source RTT activity code.',
                                           'activity_datetime_quality': 'Bronze quality classification for '
                                                                        'the activity timestamp.',
                                           'activity_display': 'Source RTT activity display.',
                                           'activity_sequence_asc': 'Ascending RTT-activity sequence.',
                                           'activity_sequence_desc': 'Descending RTT-activity sequence.',
                                           'activity_type_code': 'Source RTT activity-type code.',
                                           'activity_type_display': 'Source RTT activity-type display.',
                                           'appointment_oid': 'Raw appointment linkage evidence; resolution '
                                                              'is gated and no edge is emitted.',
                                           'confidentiality_code': 'Source confidentiality code when '
                                                                   'supplied.',
                                           'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA activity '
                                                           'feed supplies no encounter.',
                                           'event_datetime': 'RTT activity timestamp.',
                                           'event_datetime_local': 'RTT activity timestamp expressed in '
                                                                   'local time.',
                                           'event_end_datetime': 'Event end timestamp; not supplied by LUNA '
                                                                 'RTT activities.',
                                           'event_end_datetime_local': 'Event end timestamp expressed in '
                                                                       'local time; not supplied by LUNA RTT '
                                                                       'activities.',
                                           'fact_category': 'Whether this fact is clinical or administrative '
                                                            'in the v2 plane merge.',
                                           'loaded_at': 'map_rtt_activity.ADC_UPDT carried unchanged through '
                                                        'the source and public projections. No parent or '
                                                        'other source clock is joined into this value; it '
                                                        'differs from SOURCE_ADC_UPDT used for '
                                                        'source_update_timestamp and from the current Silver '
                                                        'refresh time.',
                                           'pathway_oid': 'Raw LUNA pathway object identifier.',
                                           'patient_event_key': 'Deterministic SHA-256 RTT activity '
                                                                'identity.',
                                           'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                        'foreign key.',
                                           'record_status': 'Superseded when '
                                                            'map_rtt_activity.SOURCE_PRESENT_IND or '
                                                            'ACTIVE_IND is false; each missing flag defaults '
                                                            'true. Otherwise active. Source absence and '
                                                            'inactivity both map to superseded, not '
                                                            'retracted; the clinical status code does not '
                                                            'set this row-history label.',
                                           'record_status_effective_from': 'Source system timestamp '
                                                                           'recording when the RTT activity '
                                                                           'record was created.',
                                           'record_status_effective_to': 'For a superseded rtt activity row, '
                                                                         'the first non-null '
                                                                         'map_rtt_activity.SOURCE_ABSENT_DETECTED_TS, '
                                                                         'MODIFIED_DATETIME or ADC_UPDT, in '
                                                                         'that order; otherwise null. The '
                                                                         'same source-presence/inactivity '
                                                                         'predicate sets record_status. No '
                                                                         'event-time clamp or clinical-event '
                                                                         'timestamp is substituted.',
                                           'referral_key': 'Deterministic SHA-256 key of the linked '
                                                           'referral.',
                                           'rtt_activity_oid': 'Raw LUNA RTT activity object identifier.',
                                           'site_code': 'Source site code.',
                                           'site_display': 'Source site display.',
                                           'source_code': 'Verbatim RTT-status code.',
                                           'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                           'source_display': 'Verbatim RTT-status display.',
                                           'source_feed': 'Registered feed owning the fact.',
                                           'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the '
                                                                'native primary key.',
                                           'source_update_timestamp': 'map_rtt_activity.SOURCE_ADC_UPDT '
                                                                      'carried unchanged to the research '
                                                                      'row, distinct from ADC_UPDT used for '
                                                                      'loaded_at. It is not the clinical '
                                                                      'event timestamp or the current Silver '
                                                                      'refresh time.',
                                           'status_code': 'Source RTT-status code.',
                                           'status_display': 'Source RTT-status display.',
                                           'status_sequence_asc': 'Ascending RTT-status sequence.',
                                           'status_sequence_desc': 'Descending RTT-status sequence.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_function_code': 'Source treatment-function code.',
                                           'treatment_function_display': 'Source treatment-function display.',
                                           'vip_ind': 'Source VIP indicator when supplied.',
                                           'withheld_identity_ind': 'Withheld-identity indicator when '
                                                                    'supplied.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of clinical_rtt_activity: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'clinical_rtt_activity': {'columns': {'activity_code': 'Source RTT activity code.',
                                       'activity_datetime_quality': 'Bronze quality classification for the '
                                                                    'activity timestamp.',
                                       'activity_display': 'Source RTT activity display.',
                                       'activity_sequence_asc': 'Ascending RTT-activity sequence.',
                                       'activity_sequence_desc': 'Descending RTT-activity sequence.',
                                       'activity_type_code': 'Source RTT activity-type code.',
                                       'activity_type_display': 'Source RTT activity-type display.',
                                       'appointment_oid': 'Raw appointment linkage evidence; resolution is '
                                                          'gated and no edge is emitted.',
                                       'confidentiality_code': 'Source confidentiality code when supplied.',
                                       'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA activity '
                                                       'feed supplies no encounter.',
                                       'event_datetime': 'RTT activity timestamp.',
                                       'event_datetime_local': 'RTT activity timestamp expressed in local '
                                                               'time.',
                                       'event_end_datetime': 'Event end timestamp; not supplied by LUNA RTT '
                                                             'activities.',
                                       'event_end_datetime_local': 'Event end timestamp expressed in local '
                                                                   'time; not supplied by LUNA RTT '
                                                                   'activities.',
                                       'fact_category': 'Whether this fact is clinical or administrative in '
                                                        'the v2 plane merge.',
                                       'loaded_at': 'map_rtt_activity.ADC_UPDT carried unchanged through the '
                                                    'source and public projections. No parent or other '
                                                    'source clock is joined into this value; it differs from '
                                                    'SOURCE_ADC_UPDT used for source_update_timestamp and '
                                                    'from the current Silver refresh time.',
                                       'pathway_oid': 'Raw LUNA pathway object identifier.',
                                       'patient_event_key': 'Deterministic SHA-256 RTT activity identity.',
                                       'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                    'foreign key.',
                                       'record_status': 'Superseded when map_rtt_activity.SOURCE_PRESENT_IND '
                                                        'or ACTIVE_IND is false; each missing flag defaults '
                                                        'true. Otherwise active. Source absence and '
                                                        'inactivity both map to superseded, not retracted; '
                                                        'the clinical status code does not set this '
                                                        'row-history label.',
                                       'record_status_effective_from': 'Source system timestamp recording '
                                                                       'when the RTT activity record was '
                                                                       'created.',
                                       'record_status_effective_to': 'For a superseded rtt activity row, the '
                                                                     'first non-null '
                                                                     'map_rtt_activity.SOURCE_ABSENT_DETECTED_TS, '
                                                                     'MODIFIED_DATETIME or ADC_UPDT, in that '
                                                                     'order; otherwise null. The same '
                                                                     'source-presence/inactivity predicate '
                                                                     'sets record_status. No event-time '
                                                                     'clamp or clinical-event timestamp is '
                                                                     'substituted.',
                                       'referral_key': 'Deterministic SHA-256 key of the linked referral.',
                                       'rtt_activity_oid': 'Raw LUNA RTT activity object identifier.',
                                       'site_code': 'Source site code.',
                                       'site_display': 'Source site display.',
                                       'source_code': 'Verbatim RTT-status code.',
                                       'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                       'source_display': 'Verbatim RTT-status display.',
                                       'source_feed': 'Registered feed owning the fact.',
                                       'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                            'primary key.',
                                       'source_update_timestamp': 'map_rtt_activity.SOURCE_ADC_UPDT carried '
                                                                  'unchanged to the research row, distinct '
                                                                  'from ADC_UPDT used for loaded_at. It is '
                                                                  'not the clinical event timestamp or the '
                                                                  'current Silver refresh time.',
                                       'status_code': 'Source RTT-status code.',
                                       'status_display': 'Source RTT-status display.',
                                       'status_sequence_asc': 'Ascending RTT-status sequence.',
                                       'status_sequence_desc': 'Descending RTT-status sequence.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'treatment_function_code': 'Source treatment-function code.',
                                       'treatment_function_display': 'Source treatment-function display.',
                                       'vip_ind': 'Source VIP indicator when supplied.',
                                       'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                           'quantity_override': False,
                           'table': 'LUNA clock-affecting RTT activity and status events with quality flags '
                                    'retained. Gold QC twin of the silver product: 1 columns are repaired or '
                                    'nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each rule states '
                                    'its reason in the pipeline notebook, and Lakeflow expectation metrics '
                                    'report what every rule matched on each update.'}}
