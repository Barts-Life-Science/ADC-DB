"""Gold pathways.rtt_pathway: 2 dataset(s).

- datasets: _qc_clinical_rtt_pathway, clinical_rtt_pathway
- moved verbatim from: 4_gold_care_pathways:3065, 4_gold_care_pathways:3117, 4_gold_care_pathways:3122, 4_gold_care_pathways:3130, 4_gold_care_pathways:3228, 4_gold_care_pathways:3232, 4_gold_care_pathways:3245

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.rtt_pathway ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_RTT_PATHWAY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_system_oid` AS `source_system_oid`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`pathway_oid` AS `pathway_oid`',
    '`period_oid` AS `period_oid`',
    '`is_latest_period` AS `is_latest_period`',
    '`start_status_code` AS `start_status_code`',
    '`start_status_display` AS `start_status_display`',
    '`stop_status_code` AS `stop_status_code`',
    '`stop_status_display` AS `stop_status_display`',
    '`current_status_code` AS `current_status_code`',
    '`current_status_display` AS `current_status_display`',
    '`sequence_asc` AS `sequence_asc`',
    '`sequence_desc` AS `sequence_desc`',
    '`clock_discrepant` AS `clock_discrepant`',
    '`core_clock_start` AS `core_clock_start`',
    '`core_clock_stop` AS `core_clock_stop`',
    '`pathway_start_date` AS `pathway_start_date`',
    '`pathway_type_code` AS `pathway_type_code`',
    '`pathway_type_display` AS `pathway_type_display`',
    '`breach_date` AS `breach_date`',
    '`days_waited` AS `days_waited`',
    '`days_waited_active` AS `days_waited_active`',
    '`op_appt_dna_count` AS `op_appt_dna_count`',
    '`treatment_function_code` AS `treatment_function_code`',
    '`treatment_function_display` AS `treatment_function_display`',
    '`site_code` AS `site_code`',
    '`site_display` AS `site_display`',
    '`referring_facility_code` AS `referring_facility_code`',
    '`referring_facility_display` AS `referring_facility_display`',
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


CLINICAL_RTT_PATHWAY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`is_clockless` AS `is_clockless`',
]


# contract v2: QC/batch inputs come from internal _clinical_rtt_pathway_metadata; source history stays on the main research table.
CLINICAL_RTT_PATHWAY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 7,862,177 rows of
    # 7,864,172 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",
}


CLINICAL_RTT_PATHWAY_ADVISORY_RULES = {
    # INSR_W4 W4.05 cross_field (estate 2026-10-29): RTT days waited cannot exceed the clock span (L3281, J27).
    'gold.clinical.rtt_pathway.days_waited.insr_f6_days_waited_overflow':
        'NOT COALESCE((`days_waited` > datediff(coalesce(`core_clock_stop`, `loaded_at`), `core_clock_start`) + 1), FALSE)',
    # INSR_W4 W4.05 date_typo (estate 2026-10-29): RTT events 1990-2006 predate the 18-week standard: kept, warned (L4879).
    'gold.clinical.rtt_pathway.event_datetime.insr_f6_rtt_event_early':
        "NOT COALESCE((`event_datetime` >= TIMESTAMP'1990-01-01 00:00:00' AND `event_datetime` < TIMESTAMP'2007-01-01 00:00:00'), FALSE)",
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.rtt_pathway.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counted rather than nulled because a breach date is a deadline counted forward from
    # the clock start, so it is in the future for every pathway still inside its target.
    # Seen on 70,601 of 7,864,172 rows (0.898%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.breach_date.future_owner":
        "NOT COALESCE((CAST(`breach_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 1,995 of 7,864,172 at the profile.
    "gold.clinical.rtt_pathway.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 277 of 7,864,172 rows (0.00352%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 832 of 7,864,172 rows (0.0106%) when profiled on 2026-08-24.
    "gold.clinical.rtt_pathway.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_RTT_PATHWAY_PERSON_SCOPE_RULES = {
    "gold.clinical.rtt_pathway.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_rtt_pathway"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_RTT_PATHWAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_RTT_PATHWAY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_RTT_PATHWAY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_rtt_pathway():
    """Quality-controlled twin of journey_clinical.rtt_pathway."""
    df = _qc("clinical_rtt_pathway", CLINICAL_RTT_PATHWAY_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.rtt_pathway"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_rtt_pathway():
    """Contract-v2 public twin of clinical_rtt_pathway; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_rtt_pathway")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_system_oid` AS `source_system_oid`',
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
        '`pathway_oid` AS `pathway_oid`',
        '`period_oid` AS `period_oid`',
        '`is_latest_period` AS `is_latest_period`',
        '`start_status_code` AS `start_status_code`',
        '`start_status_display` AS `start_status_display`',
        '`stop_status_code` AS `stop_status_code`',
        '`stop_status_display` AS `stop_status_display`',
        '`current_status_code` AS `current_status_code`',
        '`current_status_display` AS `current_status_display`',
        '`sequence_asc` AS `sequence_asc`',
        '`sequence_desc` AS `sequence_desc`',
        '`clock_discrepant` AS `clock_discrepant`',
        '`core_clock_start` AS `core_clock_start`',
        '`core_clock_stop` AS `core_clock_stop`',
        '`pathway_start_date` AS `pathway_start_date`',
        '`pathway_type_code` AS `pathway_type_code`',
        '`pathway_type_display` AS `pathway_type_display`',
        '`breach_date` AS `breach_date`',
        '`days_waited` AS `days_waited`',
        '`days_waited_active` AS `days_waited_active`',
        '`op_appt_dna_count` AS `op_appt_dna_count`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`treatment_function_display` AS `treatment_function_display`',
        '`site_code` AS `site_code`',
        '`site_display` AS `site_display`',
        '`referring_facility_code` AS `referring_facility_code`',
        '`referring_facility_display` AS `referring_facility_display`',
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
        '`is_clockless` AS `is_clockless`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_rtt_pathway': GoldRules(
        table='clinical_rtt_pathway',
        select=CLINICAL_RTT_PATHWAY_SELECT,
        mandatory=CLINICAL_RTT_PATHWAY_MANDATORY_RULES,
        advisory=CLINICAL_RTT_PATHWAY_ADVISORY_RULES,
        person_scope=CLINICAL_RTT_PATHWAY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'person_id_resolved',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth'),
        value_rules={'gold.clinical.rtt_pathway.event_datetime.insr_f6_rtt_event_floor': {'action': 'invalidate',
                                                                              'column': 'event_datetime',
                                                                              'companions': ['event_datetime_local'],
                                                                              'match': '`event_datetime` < '
                                                                                       "TIMESTAMP'1990-01-01 "
                                                                                       "00:00:00'",
                                                                              'table': 'clinical_rtt_pathway'}}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_rtt_pathway': {'columns': {'breach_date': 'Source breach timestamp.',
                                          'clock_discrepant': 'Bronze flag indicating disagreement with the '
                                                              'core clock timestamps.',
                                          'confidentiality_code': 'Source confidentiality code when '
                                                                  'supplied.',
                                          'core_clock_start': 'Core-system clock start retained as '
                                                              'discrepant sidecar data.',
                                          'core_clock_stop': 'Core-system clock stop retained as discrepant '
                                                             'sidecar data.',
                                          'current_status_code': 'Source current RTT-status code.',
                                          'current_status_display': 'Source current RTT-status display.',
                                          'days_waited': 'Source total days waited.',
                                          'days_waited_active': 'Source active days waited.',
                                          'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA pathway '
                                                          'feed supplies no encounter.',
                                          'event_datetime': 'RTT clock-period start timestamp.',
                                          'event_datetime_local': 'RTT clock-period start timestamp '
                                                                  'expressed in local time. Gold: NULL '
                                                                  'wherever Gold nulls event_datetime.',
                                          'event_end_datetime': 'RTT clock-period stop timestamp.',
                                          'event_end_datetime_local': 'RTT clock-period stop timestamp '
                                                                      'expressed in local time. Gold: NULL '
                                                                      'wherever Gold nulls '
                                                                      'event_end_datetime.',
                                          'fact_category': 'Whether this fact is clinical or administrative '
                                                           'in the v2 plane merge.',
                                          'is_clockless': 'True for a clockless pathway (period_oid 0, '
                                                          "LUNA's no-clock sentinel; kept, not NULLed). "
                                                          'INSR_W2 W2.11.',
                                          'is_latest_period': 'Whether this is the latest period for the '
                                                              'pathway.',
                                          'loaded_at': 'map_rtt_pathway.ADC_UPDT carried unchanged through '
                                                       'the RTT primitive and quality wrappers. This is '
                                                       'bronze ingestion provenance, not a clinical RTT '
                                                       'clock, period creation/modification time or Silver '
                                                       'refresh time; a missing source timestamp remains '
                                                       'null.',
                                          'op_appt_dna_count': 'Source outpatient did-not-attend count.',
                                          'pathway_oid': 'Raw LUNA pathway object identifier.',
                                          'pathway_start_date': 'Source pathway start date.',
                                          'pathway_type_code': 'Source pathway-type code.',
                                          'pathway_type_display': 'Source pathway-type display.',
                                          'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                               'cross-feed join key.',
                                          'period_oid': 'Raw LUNA period object identifier; 0 marks the '
                                                        'clockless-pathway sentinel and the column is never '
                                                        'null.',
                                          'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                       'foreign key.',
                                          'record_status': 'Derived LUNA RTT row status: superseded when '
                                                           'SOURCE_PRESENT_IND cast to Boolean, '
                                                           'PERIOD_ACTIVE_IND or CORE_ACTIVE_IND is false; '
                                                           'otherwise active. Each missing flag defaults '
                                                           'true. Source absence maps to superseded, not '
                                                           'retracted, and this label is separate from the '
                                                           'clinical RTT status or clock-stop state.',
                                          'record_status_effective_from': 'map_rtt_pathway.PERIOD_CREATED_DATETIME '
                                                                          'carried unchanged as the history '
                                                                          'start. No RTT clock-start, '
                                                                          'pathway-start or ingestion-time '
                                                                          'fallback is applied; a missing '
                                                                          'source value remains null.',
                                          'record_status_effective_to': 'For a superseded RTT row, first '
                                                                        'non-null '
                                                                        'map_rtt_pathway.SOURCE_ABSENT_DETECTED_TS, '
                                                                        'PERIOD_MODIFIED_DATETIME, then '
                                                                        'ADC_UPDT. Null for active rows or '
                                                                        'when every fallback is missing. '
                                                                        'This is a derived row-status end '
                                                                        'that may use ingestion time; '
                                                                        'clinical STOP_DATETIME is not used '
                                                                        'as the fallback.',
                                          'referring_facility_code': 'Source referring-facility code.',
                                          'referring_facility_display': 'Source referring-facility display.',
                                          'sequence_asc': 'Ascending pathway-period sequence.',
                                          'sequence_desc': 'Descending pathway-period sequence.',
                                          'site_code': 'Source site code.',
                                          'site_display': 'Source site display.',
                                          'source_code': 'Verbatim current RTT-status code.',
                                          'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                          'source_display': 'Verbatim current RTT-status display.',
                                          'source_feed': 'Registered feed owning the fact.',
                                          'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the '
                                                               'native pathway-period primary key.',
                                          'source_update_timestamp': 'map_rtt_pathway.SOURCE_ADC_UPDT '
                                                                     'carried unchanged for the contributing '
                                                                     'pathway/period row. It is distinct '
                                                                     'from ADC_UPDT ingestion provenance and '
                                                                     'is not PERIOD_MODIFIED_DATETIME, an '
                                                                     'RTT clock start/stop or Silver refresh '
                                                                     'time; a missing source timestamp '
                                                                     'remains null.',
                                          'start_status_code': 'Source clock-start RTT-status code.',
                                          'start_status_display': 'Source clock-start RTT-status display.',
                                          'stop_status_code': 'Source clock-stop RTT-status code.',
                                          'stop_status_display': 'Source clock-stop RTT-status display.',
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
                              'table': 'Internal quality-controlled twin of clinical_rtt_pathway: the Gold '
                                       'repairs, nulls and expectations are applied here and the published '
                                       'Gold MV reads this object. Materialized rather than temporary so the '
                                       'published MV can refresh incrementally.'},
 'clinical_rtt_pathway': {'columns': {'breach_date': 'Source breach timestamp.',
                                      'clock_discrepant': 'Bronze flag indicating disagreement with the core '
                                                          'clock timestamps.',
                                      'confidentiality_code': 'Source confidentiality code when supplied.',
                                      'core_clock_start': 'Core-system clock start retained as discrepant '
                                                          'sidecar data.',
                                      'core_clock_stop': 'Core-system clock stop retained as discrepant '
                                                         'sidecar data.',
                                      'current_status_code': 'Source current RTT-status code.',
                                      'current_status_display': 'Source current RTT-status display.',
                                      'days_waited': 'Source total days waited.',
                                      'days_waited_active': 'Source active days waited.',
                                      'encounter_id': 'Nullable Millennium ENCNTR_ID; the LUNA pathway feed '
                                                      'supplies no encounter.',
                                      'event_datetime': 'RTT clock-period start timestamp.',
                                      'event_datetime_local': 'RTT clock-period start timestamp expressed in '
                                                              'local time. Gold: NULL wherever Gold nulls '
                                                              'event_datetime.',
                                      'event_end_datetime': 'RTT clock-period stop timestamp.',
                                      'event_end_datetime_local': 'RTT clock-period stop timestamp expressed '
                                                                  'in local time. Gold: NULL wherever Gold '
                                                                  'nulls event_end_datetime.',
                                      'fact_category': 'Whether this fact is clinical or administrative in '
                                                       'the v2 plane merge.',
                                      'is_clockless': "True for a clockless pathway (period_oid 0, LUNA's "
                                                      'no-clock sentinel; kept, not NULLed). INSR_W2 W2.11.',
                                      'is_latest_period': 'Whether this is the latest period for the '
                                                          'pathway.',
                                      'loaded_at': 'map_rtt_pathway.ADC_UPDT carried unchanged through the '
                                                   'RTT primitive and quality wrappers. This is bronze '
                                                   'ingestion provenance, not a clinical RTT clock, period '
                                                   'creation/modification time or Silver refresh time; a '
                                                   'missing source timestamp remains null.',
                                      'op_appt_dna_count': 'Source outpatient did-not-attend count.',
                                      'pathway_oid': 'Raw LUNA pathway object identifier.',
                                      'pathway_start_date': 'Source pathway start date.',
                                      'pathway_type_code': 'Source pathway-type code.',
                                      'pathway_type_display': 'Source pathway-type display.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'period_oid': 'Raw LUNA period object identifier; 0 marks the '
                                                    'clockless-pathway sentinel and the column is never '
                                                    'null.',
                                      'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                   'foreign key.',
                                      'record_status': 'Derived LUNA RTT row status: superseded when '
                                                       'SOURCE_PRESENT_IND cast to Boolean, '
                                                       'PERIOD_ACTIVE_IND or CORE_ACTIVE_IND is false; '
                                                       'otherwise active. Each missing flag defaults true. '
                                                       'Source absence maps to superseded, not retracted, '
                                                       'and this label is separate from the clinical RTT '
                                                       'status or clock-stop state.',
                                      'record_status_effective_from': 'map_rtt_pathway.PERIOD_CREATED_DATETIME '
                                                                      'carried unchanged as the history '
                                                                      'start. No RTT clock-start, '
                                                                      'pathway-start or ingestion-time '
                                                                      'fallback is applied; a missing source '
                                                                      'value remains null.',
                                      'record_status_effective_to': 'For a superseded RTT row, first '
                                                                    'non-null '
                                                                    'map_rtt_pathway.SOURCE_ABSENT_DETECTED_TS, '
                                                                    'PERIOD_MODIFIED_DATETIME, then '
                                                                    'ADC_UPDT. Null for active rows or when '
                                                                    'every fallback is missing. This is a '
                                                                    'derived row-status end that may use '
                                                                    'ingestion time; clinical STOP_DATETIME '
                                                                    'is not used as the fallback.',
                                      'referring_facility_code': 'Source referring-facility code.',
                                      'referring_facility_display': 'Source referring-facility display.',
                                      'sequence_asc': 'Ascending pathway-period sequence.',
                                      'sequence_desc': 'Descending pathway-period sequence.',
                                      'site_code': 'Source site code.',
                                      'site_display': 'Source site display.',
                                      'source_code': 'Verbatim current RTT-status code.',
                                      'source_coding_system': 'Verbatim LUNA RTT-status coding system.',
                                      'source_display': 'Verbatim current RTT-status display.',
                                      'source_feed': 'Registered feed owning the fact.',
                                      'source_system_oid': 'LUNA SOURCE_SYSTEM_OID; component of the native '
                                                           'pathway-period primary key.',
                                      'source_update_timestamp': 'map_rtt_pathway.SOURCE_ADC_UPDT carried '
                                                                 'unchanged for the contributing '
                                                                 'pathway/period row. It is distinct from '
                                                                 'ADC_UPDT ingestion provenance and is not '
                                                                 'PERIOD_MODIFIED_DATETIME, an RTT clock '
                                                                 'start/stop or Silver refresh time; a '
                                                                 'missing source timestamp remains null.',
                                      'start_status_code': 'Source clock-start RTT-status code.',
                                      'start_status_display': 'Source clock-start RTT-status display.',
                                      'stop_status_code': 'Source clock-stop RTT-status code.',
                                      'stop_status_display': 'Source clock-stop RTT-status display.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'treatment_function_code': 'Source treatment-function code.',
                                      'treatment_function_display': 'Source treatment-function display.',
                                      'vip_ind': 'Source VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Withheld-identity indicator when supplied.'},
                          'quantity_override': False,
                          'table': 'LUNA RTT pathway and clock-period facts including clockless sentinel '
                                   'rows. Gold QC twin of the silver product: 6 columns are repaired or '
                                   'nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each rule states '
                                   'its reason in the pipeline notebook, and Lakeflow expectation metrics '
                                   'report what every rule matched on each update.'}}
