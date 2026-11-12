"""Gold pathways.community_care_activity: 2 dataset(s).

- datasets: _qc_clinical_community_care_activity, clinical_community_care_activity
- moved verbatim from: 4_gold_care_pathways:252, 4_gold_care_pathways:313, 4_gold_care_pathways:318, 4_gold_care_pathways:327, 4_gold_care_pathways:434, 4_gold_care_pathways:438, 4_gold_care_pathways:455

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.community_care_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`community_care_activity_key` AS `community_care_activity_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    "CASE WHEN CAST(`care_activity_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`care_activity_date` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`care_activity_date` AS DATE)) < 9999 THEN NULL ELSE `care_activity_date` END END AS `care_activity_date`",
    '`care_activity_date_quality_status` AS `care_activity_date_quality_status`',
    '`community_contact_key` AS `community_contact_key`',
    '`contact_match_status` AS `contact_match_status`',
    '`contact_candidate_count` AS `contact_candidate_count`',
    '`same_date_contact_candidate_count` AS `same_date_contact_candidate_count`',
    '`community_database_id` AS `community_database_id`',
    '`care_activity_id` AS `care_activity_id`',
    '`community_patient_key` AS `community_patient_key`',
    '`service_id` AS `service_id`',
    '`service_name` AS `service_name`',
    '`care_professional_local_id` AS `care_professional_local_id`',
    '`duration_minutes` AS `duration_minutes`',
    '`duration_quality_status` AS `duration_quality_status`',
    '`source_clinical_term` AS `source_clinical_term`',
    '`source_clinical_term_key` AS `source_clinical_term_key`',
    '`observation_type_id` AS `observation_type_id`',
    '`code_category_id` AS `code_category_id`',
    '`observation_value_raw` AS `observation_value_raw`',
    '`observation_value_numeric` AS `observation_value_numeric`',
    '`unit_source_value` AS `unit_source_value`',
    '`normalized_ucum_code` AS `normalized_ucum_code`',
    '`unit_concept_id` AS `unit_concept_id`',
    '`unit_concept_name` AS `unit_concept_name`',
    '`unit_mapping_status` AS `unit_mapping_status`',
    '`unit_mapping_method` AS `unit_mapping_method`',
    '`snomed_candidate_count` AS `snomed_candidate_count`',
    '`snomed_candidate_concept_id` AS `snomed_candidate_concept_id`',
    '`snomed_candidate_code` AS `snomed_candidate_code`',
    '`snomed_candidate_name` AS `snomed_candidate_name`',
    '`snomed_candidate_domain` AS `snomed_candidate_domain`',
    '`snomed_candidate_class` AS `snomed_candidate_class`',
    '`snomed_candidate_method` AS `snomed_candidate_method`',
    '`snomed_candidate_status` AS `snomed_candidate_status`',
    '`person_match_status` AS `person_match_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    # INSR_W3 W3.05: Gold: NULL where a post-2100 year below 9999.
    "CASE WHEN (CAST(`record_status_effective_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`record_status_effective_from` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_from` END AS `record_status_effective_from`",
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`january_default_suspected` AS `january_default_suspected`',
]


# contract v2: QC/batch inputs come from internal _clinical_community_care_activity_metadata; source history stays on the main research table.
CLINICAL_COMMUNITY_CARE_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 15,436,295 rows of 15,483,317
    # that are current and attributable; identity_status = 'resolved' keeps the 15,293,141
    # rows of 15,483,317 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_COMMUNITY_CARE_ACTIVITY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.community_care_activity.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 190,176 of 15,483,317 at the profile.
    "gold.clinical.community_care_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 47,022 of 15,483,317 at the profile.
    "gold.clinical.community_care_activity.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 15,483,317
    # rows (1.29e-05%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 7,362 of 15,483,317 rows (0.0475%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 99 of 15,483,317 rows (0.000639%) when profiled on 2026-08-24.
    "gold.clinical.community_care_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_COMMUNITY_CARE_ACTIVITY_PERSON_SCOPE_RULES = {
    "gold.clinical.community_care_activity.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_community_care_activity"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_COMMUNITY_CARE_ACTIVITY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_COMMUNITY_CARE_ACTIVITY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_community_care_activity():
    """Quality-controlled twin of journey_clinical.community_care_activity."""
    df = _qc(
        "clinical_community_care_activity",
        CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.community_care_activity"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_community_care_activity():
    """Contract-v2 public twin of clinical_community_care_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_community_care_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`community_care_activity_key` AS `community_care_activity_key`',
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
        '`care_activity_date` AS `care_activity_date`',
        '`care_activity_date_quality_status` AS `care_activity_date_quality_status`',
        '`community_contact_key` AS `community_contact_key`',
        '`contact_match_status` AS `contact_match_status`',
        '`contact_candidate_count` AS `contact_candidate_count`',
        '`same_date_contact_candidate_count` AS `same_date_contact_candidate_count`',
        '`community_database_id` AS `community_database_id`',
        '`care_activity_id` AS `care_activity_id`',
        '`community_patient_key` AS `community_patient_key`',
        '`service_id` AS `service_id`',
        '`service_name` AS `service_name`',
        '`care_professional_local_id` AS `care_professional_local_id`',
        '`duration_minutes` AS `duration_minutes`',
        '`duration_quality_status` AS `duration_quality_status`',
        '`source_clinical_term` AS `source_clinical_term`',
        '`source_clinical_term_key` AS `source_clinical_term_key`',
        '`observation_type_id` AS `observation_type_id`',
        '`code_category_id` AS `code_category_id`',
        '`observation_value_raw` AS `observation_value_raw`',
        '`observation_value_numeric` AS `observation_value_numeric`',
        '`unit_source_value` AS `unit_source_value`',
        '`normalized_ucum_code` AS `normalized_ucum_code`',
        '`unit_concept_id` AS `unit_concept_id`',
        '`unit_concept_name` AS `unit_concept_name`',
        '`unit_mapping_status` AS `unit_mapping_status`',
        '`unit_mapping_method` AS `unit_mapping_method`',
        '`snomed_candidate_count` AS `snomed_candidate_count`',
        '`snomed_candidate_concept_id` AS `snomed_candidate_concept_id`',
        '`snomed_candidate_code` AS `snomed_candidate_code`',
        '`snomed_candidate_name` AS `snomed_candidate_name`',
        '`snomed_candidate_domain` AS `snomed_candidate_domain`',
        '`snomed_candidate_class` AS `snomed_candidate_class`',
        '`snomed_candidate_method` AS `snomed_candidate_method`',
        '`snomed_candidate_status` AS `snomed_candidate_status`',
        '`person_match_status` AS `person_match_status`',
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
        '`january_default_suspected` AS `january_default_suspected`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_community_care_activity': GoldRules(
        table='clinical_community_care_activity',
        select=CLINICAL_COMMUNITY_CARE_ACTIVITY_SELECT,
        mandatory=CLINICAL_COMMUNITY_CARE_ACTIVITY_MANDATORY_RULES,
        advisory=CLINICAL_COMMUNITY_CARE_ACTIVITY_ADVISORY_RULES,
        person_scope=CLINICAL_COMMUNITY_CARE_ACTIVITY_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_community_care_activity': {'columns': {'care_activity_date': 'Date on which the care activity '
                                                                            'was recorded as taking place.',
                                                      'care_activity_date_quality_status': 'Derived '
                                                                                           'data-quality '
                                                                                           'flag describing '
                                                                                           'the state of the '
                                                                                           'source care '
                                                                                           'activity date, '
                                                                                           'such as whether '
                                                                                           'it is absent or '
                                                                                           'falls within an '
                                                                                           'acceptable '
                                                                                           'range.',
                                                      'care_activity_id': 'Source CSDS care activity '
                                                                          'identifier (CAI) for the activity '
                                                                          'row, unique only within its '
                                                                          'source database.',
                                                      'care_professional_local_id': 'CSDS Care Professional '
                                                                                    'Local Identifier; not a '
                                                                                    'Millennium personnel '
                                                                                    'ID.',
                                                      'code_category_id': 'Numeric code category identifier '
                                                                          'carried unchanged from the source '
                                                                          'CSDS CYP202 care activity '
                                                                          'CodeCategoryId field.',
                                                      'community_care_activity_key': 'Source-preserved '
                                                                                     'community care '
                                                                                     'activity key; primary '
                                                                                     'key of this table.',
                                                      'community_contact_key': 'Deterministic SHA-256 key '
                                                                               'for the parent community '
                                                                               'care contact.',
                                                      'community_database_id': 'Source community system '
                                                                               'database identifier (CDB) '
                                                                               'from the CSDS CYP202 care '
                                                                               'activity extract, used with '
                                                                               'care_activity_id to scope '
                                                                               'source records.',
                                                      'community_patient_key': 'Content-addressed key '
                                                                               'identifying the community '
                                                                               'patient, derived from the '
                                                                               'source patient number and '
                                                                               'registering source database '
                                                                               'identifier in the CSDS '
                                                                               'patient identifier lookup.',
                                                      'confidentiality_code': 'Source confidentiality '
                                                                              'classification attached to '
                                                                              'the record for each community '
                                                                              'care activity record. It is '
                                                                              'produced by the silver '
                                                                              'transformation and has no '
                                                                              'direct bronze-column lineage '
                                                                              'entry. Source code meanings '
                                                                              'and sentinel values are '
                                                                              'retained unless the pipeline '
                                                                              'explicitly maps them; null '
                                                                              'means no code was supplied.',
                                                      'contact_candidate_count': 'Number of candidate care '
                                                                                 'contact rows matched to '
                                                                                 "this activity's care "
                                                                                 'contact identifier during '
                                                                                 'contact linkage.',
                                                      'contact_match_status': 'UNIQUE_ID_DATE_MATCH, '
                                                                              'UNIQUE_ID_DATE_MISMATCH, '
                                                                              'DUPLICATE_ID_RESOLVED_BY_DATE, '
                                                                              'AMBIGUOUS_SAME_DATE, '
                                                                              'DUPLICATE_ID_NO_DATE_MATCH or '
                                                                              'NO_CONTACT.',
                                                      'duration_minutes': 'Recorded duration of the clinical '
                                                                          'contact for this activity, in '
                                                                          'minutes.',
                                                      'duration_quality_status': 'Derived data-quality flag '
                                                                                 'describing the state of '
                                                                                 'the recorded clinical '
                                                                                 'contact duration, such as '
                                                                                 'whether it is absent or '
                                                                                 'within an acceptable '
                                                                                 'range.',
                                                      'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                      'when available.',
                                                      'event_datetime': 'Date on which the care activity was '
                                                                        'recorded as taking place.',
                                                      'event_datetime_local': 'Start date and time of the '
                                                                              'community care activity event '
                                                                              'expressed in local time. '
                                                                              'Gold: NULL wherever Gold '
                                                                              'nulls event_datetime.',
                                                      'event_end_datetime': 'Date and time when the '
                                                                            'represented clinical or '
                                                                            'administrative event ended for '
                                                                            'each community care activity '
                                                                            'record. It is produced by the '
                                                                            'silver transformation and has '
                                                                            'no direct bronze-column lineage '
                                                                            'entry. Source precision and '
                                                                            'timezone handling follow the '
                                                                            'pipeline expression; null means '
                                                                            'the time was unavailable.',
                                                      'event_end_datetime_local': 'End date and time of the '
                                                                                  'community care activity '
                                                                                  'event expressed in local '
                                                                                  'time.',
                                                      'fact_category': 'Contract category used to group the '
                                                                       'record with comparable clinical '
                                                                       'facts for each community care '
                                                                       'activity record. It is produced by '
                                                                       'the silver transformation and has no '
                                                                       'direct bronze-column lineage entry. '
                                                                       'Whitespace and source sentinel text '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly normalizes them; null '
                                                                       'means no value was supplied.',
                                                      'january_default_suspected': 'True for activity dated '
                                                                                   '1 January before '
                                                                                   '2007-07, where the date '
                                                                                   'heaps at more than 5x '
                                                                                   'the other January days: '
                                                                                   'the source recorded the '
                                                                                   'year only. INSR_W2.',
                                                      'loaded_at': 'Always null as a TIMESTAMP in the '
                                                                   'current community-care activity '
                                                                   'projection. No contributing load clock '
                                                                   'is supplied or taken from the parent '
                                                                   'contact; neither care_activity_date nor '
                                                                   'Silver refresh time is substituted.',
                                                      'normalized_ucum_code': 'Exact or approved-alias '
                                                                              'normalized UCUM code.',
                                                      'observation_type_id': 'Numeric observation type '
                                                                             'identifier carried unchanged '
                                                                             'from the source CSDS CYP202 '
                                                                             'care activity ObservationType '
                                                                             'field.',
                                                      'observation_value_numeric': 'Numeric interpretation '
                                                                                   'of the source '
                                                                                   'observation value where '
                                                                                   'the recorded ObsValue '
                                                                                   'could be parsed as a '
                                                                                   'number.',
                                                      'observation_value_raw': 'Observation value as '
                                                                               'recorded in the source CSDS '
                                                                               'CYP202 care activity '
                                                                               'ObsValue field, retained '
                                                                               'unaltered before numeric '
                                                                               'parsing.',
                                                      'patient_event_key': 'Deterministic event key derived '
                                                                           'from source identifiers; '
                                                                           'retained as a stable join key.',
                                                      'person_id': 'Native Millennium PERSON_ID as BIGINT '
                                                                   'when resolved.',
                                                      'person_match_status': 'Outcome status of the linkage '
                                                                             'from the community patient '
                                                                             'identifier to a Millennium '
                                                                             'person record.',
                                                      'record_status': 'Retracted when '
                                                                       'map_community_care_activity.SOURCE_PRESENT_IND '
                                                                       'is false, otherwise active; missing '
                                                                       'presence defaults true. No '
                                                                       'superseded status is emitted. This '
                                                                       'is source-row presence, not the '
                                                                       'clinical finding, contact-match '
                                                                       'status or code-eligibility decision.',
                                                      'record_status_effective_from': 'Date on which the '
                                                                                      'care activity was '
                                                                                      'recorded as taking '
                                                                                      'place.',
                                                      'record_status_effective_to': 'map_community_care_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                                    'only when '
                                                                                    'SOURCE_PRESENT_IND is '
                                                                                    'false; otherwise null. '
                                                                                    'A missing absence '
                                                                                    'timestamp stays null, '
                                                                                    'with no ingestion-time '
                                                                                    'fallback. This is the '
                                                                                    'row-absence end proxy, '
                                                                                    'not a clinical activity '
                                                                                    'or contact end.',
                                                      'same_date_contact_candidate_count': 'Number of '
                                                                                           'candidate care '
                                                                                           'contact rows '
                                                                                           'whose contact '
                                                                                           'date equals the '
                                                                                           'care activity '
                                                                                           'date, used to '
                                                                                           'resolve '
                                                                                           'duplicate '
                                                                                           'contact '
                                                                                           'identifiers.',
                                                      'service_id': 'Identifier of the registering community '
                                                                    'service or organisation from the CSDS '
                                                                    'patient identifier lookup.',
                                                      'service_name': 'Name of the registering community '
                                                                      'service or organisation associated '
                                                                      'with the activity.',
                                                      'snomed_candidate_class': 'OMOP concept class of the '
                                                                                'SNOMED CT candidate '
                                                                                'concept.',
                                                      'snomed_candidate_code': 'SNOMED CT code of the '
                                                                               'candidate concept matched to '
                                                                               'the source clinical term.',
                                                      'snomed_candidate_concept_id': 'Unreviewed '
                                                                                     'deterministic '
                                                                                     'exact-name/synonym '
                                                                                     'candidate; not an '
                                                                                     'approved resolved '
                                                                                     'mapping.',
                                                      'snomed_candidate_count': 'Number of distinct SNOMED '
                                                                                'candidate concepts matched '
                                                                                'for the source clinical '
                                                                                'term on this activity row.',
                                                      'snomed_candidate_domain': 'OMOP domain of the SNOMED '
                                                                                 'CT candidate concept.',
                                                      'snomed_candidate_method': 'Indicator of the '
                                                                                 'deterministic matching '
                                                                                 'technique used to select '
                                                                                 'the SNOMED candidate, such '
                                                                                 'as exact concept name or '
                                                                                 'exact synonym match.',
                                                      'snomed_candidate_name': 'Concept name of the SNOMED '
                                                                               'CT candidate matched to the '
                                                                               'source clinical term.',
                                                      'snomed_candidate_status': 'CANDIDATE_UNREVIEWED, '
                                                                                 'AMBIGUOUS, UNMAPPED, '
                                                                                 'NO_SOURCE_TERM or '
                                                                                 'DISABLED.',
                                                      'source_clinical_term': 'Original clinical term text '
                                                                              'recorded against the activity '
                                                                              'in the source legacy code '
                                                                              'field, used as input to '
                                                                              'candidate SNOMED enrichment.',
                                                      'source_clinical_term_key': 'Normalised form of the '
                                                                                  'source clinical term used '
                                                                                  'as the deterministic join '
                                                                                  'key for SNOMED candidate '
                                                                                  'matching.',
                                                      'source_code': 'CSDS care activity type code recorded '
                                                                     'for the activity in the source '
                                                                     'extract.',
                                                      'source_coding_system': 'CSDS care activity type code '
                                                                              'recorded for the activity in '
                                                                              'the source extract.',
                                                      'source_display': 'Description corresponding to the '
                                                                        'CSDS care activity type code.',
                                                      'source_feed': 'Originating data feed responsible for '
                                                                     'the record for each community care '
                                                                     'activity record. It is produced by the '
                                                                     'silver transformation and has no '
                                                                     'direct bronze-column lineage entry. '
                                                                     'Whitespace and source sentinel text '
                                                                     'are retained unless the pipeline '
                                                                     'explicitly normalizes them; null means '
                                                                     'no value was supplied.',
                                                      'source_update_timestamp': 'Always null as a TIMESTAMP '
                                                                                 'in the current '
                                                                                 'community-care activity '
                                                                                 'projection. No native '
                                                                                 'application-update or '
                                                                                 'ingestion clock is '
                                                                                 'supplied for this field; '
                                                                                 'neither the activity date '
                                                                                 'nor Silver refresh time is '
                                                                                 'substituted.',
                                                      'subject_id_system': 'Content-addressed key '
                                                                           'identifying the community '
                                                                           'patient, derived from the source '
                                                                           'patient number and registering '
                                                                           'source database identifier in '
                                                                           'the CSDS patient identifier '
                                                                           'lookup.',
                                                      'subject_key': 'Deterministic SHA-256 over the '
                                                                     'strongest source identifier. Not '
                                                                     'salted, not secret; a stable join key '
                                                                     'across feeds where person_id is '
                                                                     'unresolved.',
                                                      'unit_concept_id': 'OMOP standard concept identifier '
                                                                         'for the mapped unit of measure.',
                                                      'unit_concept_name': 'OMOP standard concept name for '
                                                                           'the mapped unit of measure.',
                                                      'unit_mapping_method': 'Indicator of the technique '
                                                                             'used to derive the unit '
                                                                             'mapping, such as direct code '
                                                                             'match or alias dictionary '
                                                                             'lookup.',
                                                      'unit_mapping_status': 'Status flag describing the '
                                                                             'outcome of mapping the source '
                                                                             'unit text to an OMOP unit '
                                                                             'concept.',
                                                      'unit_source_value': 'Unit of measure text as recorded '
                                                                           'in the source CSDS CYP202 care '
                                                                           'activity unit field, before '
                                                                           'normalization.',
                                                      'vip_ind': 'Indicator that the source record carries '
                                                                 'VIP handling status for each community '
                                                                 'care activity record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. Null '
                                                                 'means the source did not state the '
                                                                 'indicator and must not be interpreted as '
                                                                 'false.',
                                                      'withheld_identity_ind': 'Indicator that the source '
                                                                               'identity was deliberately '
                                                                               'withheld for each community '
                                                                               'care activity record. It is '
                                                                               'produced by the silver '
                                                                               'transformation and has no '
                                                                               'direct bronze-column lineage '
                                                                               'entry. Null means the source '
                                                                               'did not state the indicator '
                                                                               'and must not be interpreted '
                                                                               'as false.'},
                                          'quantity_override': True,
                                          'table': 'Internal quality-controlled twin of '
                                                   'clinical_community_care_activity: the Gold repairs, '
                                                   'nulls and expectations are applied here and the '
                                                   'published Gold MV reads this object. Materialized rather '
                                                   'than temporary so the published MV can refresh '
                                                   'incrementally.'},
 'clinical_community_care_activity': {'columns': {'care_activity_date': 'Date on which the care activity was '
                                                                        'recorded as taking place.',
                                                  'care_activity_date_quality_status': 'Derived data-quality '
                                                                                       'flag describing the '
                                                                                       'state of the source '
                                                                                       'care activity date, '
                                                                                       'such as whether it '
                                                                                       'is absent or falls '
                                                                                       'within an acceptable '
                                                                                       'range.',
                                                  'care_activity_id': 'Source CSDS care activity identifier '
                                                                      '(CAI) for the activity row, unique '
                                                                      'only within its source database.',
                                                  'care_professional_local_id': 'CSDS Care Professional '
                                                                                'Local Identifier; not a '
                                                                                'Millennium personnel ID.',
                                                  'code_category_id': 'Numeric code category identifier '
                                                                      'carried unchanged from the source '
                                                                      'CSDS CYP202 care activity '
                                                                      'CodeCategoryId field.',
                                                  'community_care_activity_key': 'Source-preserved community '
                                                                                 'care activity key; primary '
                                                                                 'key of this table.',
                                                  'community_contact_key': 'Deterministic SHA-256 key for '
                                                                           'the parent community care '
                                                                           'contact.',
                                                  'community_database_id': 'Source community system database '
                                                                           'identifier (CDB) from the CSDS '
                                                                           'CYP202 care activity extract, '
                                                                           'used with care_activity_id to '
                                                                           'scope source records.',
                                                  'community_patient_key': 'Content-addressed key '
                                                                           'identifying the community '
                                                                           'patient, derived from the source '
                                                                           'patient number and registering '
                                                                           'source database identifier in '
                                                                           'the CSDS patient identifier '
                                                                           'lookup.',
                                                  'confidentiality_code': 'Source confidentiality '
                                                                          'classification attached to the '
                                                                          'record for each community care '
                                                                          'activity record. It is produced '
                                                                          'by the silver transformation and '
                                                                          'has no direct bronze-column '
                                                                          'lineage entry. Source code '
                                                                          'meanings and sentinel values are '
                                                                          'retained unless the pipeline '
                                                                          'explicitly maps them; null means '
                                                                          'no code was supplied.',
                                                  'contact_candidate_count': 'Number of candidate care '
                                                                             'contact rows matched to this '
                                                                             "activity's care contact "
                                                                             'identifier during contact '
                                                                             'linkage.',
                                                  'contact_match_status': 'UNIQUE_ID_DATE_MATCH, '
                                                                          'UNIQUE_ID_DATE_MISMATCH, '
                                                                          'DUPLICATE_ID_RESOLVED_BY_DATE, '
                                                                          'AMBIGUOUS_SAME_DATE, '
                                                                          'DUPLICATE_ID_NO_DATE_MATCH or '
                                                                          'NO_CONTACT.',
                                                  'duration_minutes': 'Recorded duration of the clinical '
                                                                      'contact for this activity, in '
                                                                      'minutes.',
                                                  'duration_quality_status': 'Derived data-quality flag '
                                                                             'describing the state of the '
                                                                             'recorded clinical contact '
                                                                             'duration, such as whether it '
                                                                             'is absent or within an '
                                                                             'acceptable range.',
                                                  'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT '
                                                                  'when available.',
                                                  'event_datetime': 'Date on which the care activity was '
                                                                    'recorded as taking place.',
                                                  'event_datetime_local': 'Start date and time of the '
                                                                          'community care activity event '
                                                                          'expressed in local time. Gold: '
                                                                          'NULL wherever Gold nulls '
                                                                          'event_datetime.',
                                                  'event_end_datetime': 'Date and time when the represented '
                                                                        'clinical or administrative event '
                                                                        'ended for each community care '
                                                                        'activity record. It is produced by '
                                                                        'the silver transformation and has '
                                                                        'no direct bronze-column lineage '
                                                                        'entry. Source precision and '
                                                                        'timezone handling follow the '
                                                                        'pipeline expression; null means the '
                                                                        'time was unavailable.',
                                                  'event_end_datetime_local': 'End date and time of the '
                                                                              'community care activity event '
                                                                              'expressed in local time.',
                                                  'fact_category': 'Contract category used to group the '
                                                                   'record with comparable clinical facts '
                                                                   'for each community care activity record. '
                                                                   'It is produced by the silver '
                                                                   'transformation and has no direct '
                                                                   'bronze-column lineage entry. Whitespace '
                                                                   'and source sentinel text are retained '
                                                                   'unless the pipeline explicitly '
                                                                   'normalizes them; null means no value was '
                                                                   'supplied.',
                                                  'january_default_suspected': 'True for activity dated 1 '
                                                                               'January before 2007-07, '
                                                                               'where the date heaps at more '
                                                                               'than 5x the other January '
                                                                               'days: the source recorded '
                                                                               'the year only. INSR_W2.',
                                                  'loaded_at': 'Always null as a TIMESTAMP in the current '
                                                               'community-care activity projection. No '
                                                               'contributing load clock is supplied or taken '
                                                               'from the parent contact; neither '
                                                               'care_activity_date nor Silver refresh time '
                                                               'is substituted.',
                                                  'normalized_ucum_code': 'Exact or approved-alias '
                                                                          'normalized UCUM code.',
                                                  'observation_type_id': 'Numeric observation type '
                                                                         'identifier carried unchanged from '
                                                                         'the source CSDS CYP202 care '
                                                                         'activity ObservationType field.',
                                                  'observation_value_numeric': 'Numeric interpretation of '
                                                                               'the source observation value '
                                                                               'where the recorded ObsValue '
                                                                               'could be parsed as a number.',
                                                  'observation_value_raw': 'Observation value as recorded in '
                                                                           'the source CSDS CYP202 care '
                                                                           'activity ObsValue field, '
                                                                           'retained unaltered before '
                                                                           'numeric parsing.',
                                                  'patient_event_key': 'Deterministic event key derived from '
                                                                       'source identifiers; retained as a '
                                                                       'stable join key.',
                                                  'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                               'resolved.',
                                                  'person_match_status': 'Outcome status of the linkage from '
                                                                         'the community patient identifier '
                                                                         'to a Millennium person record.',
                                                  'record_status': 'Retracted when '
                                                                   'map_community_care_activity.SOURCE_PRESENT_IND '
                                                                   'is false, otherwise active; missing '
                                                                   'presence defaults true. No superseded '
                                                                   'status is emitted. This is source-row '
                                                                   'presence, not the clinical finding, '
                                                                   'contact-match status or code-eligibility '
                                                                   'decision.',
                                                  'record_status_effective_from': 'Date on which the care '
                                                                                  'activity was recorded as '
                                                                                  'taking place.',
                                                  'record_status_effective_to': 'map_community_care_activity.SOURCE_ABSENT_DETECTED_TS '
                                                                                'only when '
                                                                                'SOURCE_PRESENT_IND is '
                                                                                'false; otherwise null. A '
                                                                                'missing absence timestamp '
                                                                                'stays null, with no '
                                                                                'ingestion-time fallback. '
                                                                                'This is the row-absence end '
                                                                                'proxy, not a clinical '
                                                                                'activity or contact end.',
                                                  'same_date_contact_candidate_count': 'Number of candidate '
                                                                                       'care contact rows '
                                                                                       'whose contact date '
                                                                                       'equals the care '
                                                                                       'activity date, used '
                                                                                       'to resolve duplicate '
                                                                                       'contact identifiers.',
                                                  'service_id': 'Identifier of the registering community '
                                                                'service or organisation from the CSDS '
                                                                'patient identifier lookup.',
                                                  'service_name': 'Name of the registering community service '
                                                                  'or organisation associated with the '
                                                                  'activity.',
                                                  'snomed_candidate_class': 'OMOP concept class of the '
                                                                            'SNOMED CT candidate concept.',
                                                  'snomed_candidate_code': 'SNOMED CT code of the candidate '
                                                                           'concept matched to the source '
                                                                           'clinical term.',
                                                  'snomed_candidate_concept_id': 'Unreviewed deterministic '
                                                                                 'exact-name/synonym '
                                                                                 'candidate; not an approved '
                                                                                 'resolved mapping.',
                                                  'snomed_candidate_count': 'Number of distinct SNOMED '
                                                                            'candidate concepts matched for '
                                                                            'the source clinical term on '
                                                                            'this activity row.',
                                                  'snomed_candidate_domain': 'OMOP domain of the SNOMED CT '
                                                                             'candidate concept.',
                                                  'snomed_candidate_method': 'Indicator of the deterministic '
                                                                             'matching technique used to '
                                                                             'select the SNOMED candidate, '
                                                                             'such as exact concept name or '
                                                                             'exact synonym match.',
                                                  'snomed_candidate_name': 'Concept name of the SNOMED CT '
                                                                           'candidate matched to the source '
                                                                           'clinical term.',
                                                  'snomed_candidate_status': 'CANDIDATE_UNREVIEWED, '
                                                                             'AMBIGUOUS, UNMAPPED, '
                                                                             'NO_SOURCE_TERM or DISABLED.',
                                                  'source_clinical_term': 'Original clinical term text '
                                                                          'recorded against the activity in '
                                                                          'the source legacy code field, '
                                                                          'used as input to candidate SNOMED '
                                                                          'enrichment.',
                                                  'source_clinical_term_key': 'Normalised form of the source '
                                                                              'clinical term used as the '
                                                                              'deterministic join key for '
                                                                              'SNOMED candidate matching.',
                                                  'source_code': 'CSDS care activity type code as recorded '
                                                                 'in the community extract; many activities '
                                                                 'carry a generic type, so the coded '
                                                                 'activity mix is thin (see '
                                                                 'snomed_candidate_code for the mapped '
                                                                 'concept).',
                                                  'source_coding_system': 'CSDS care activity type code '
                                                                          'recorded for the activity in the '
                                                                          'source extract.',
                                                  'source_display': 'Description corresponding to the CSDS '
                                                                    'care activity type code.',
                                                  'source_feed': 'Originating data feed responsible for the '
                                                                 'record for each community care activity '
                                                                 'record. It is produced by the silver '
                                                                 'transformation and has no direct '
                                                                 'bronze-column lineage entry. Whitespace '
                                                                 'and source sentinel text are retained '
                                                                 'unless the pipeline explicitly normalizes '
                                                                 'them; null means no value was supplied.',
                                                  'source_update_timestamp': 'Always null as a TIMESTAMP in '
                                                                             'the current community-care '
                                                                             'activity projection. No native '
                                                                             'application-update or '
                                                                             'ingestion clock is supplied '
                                                                             'for this field; neither the '
                                                                             'activity date nor Silver '
                                                                             'refresh time is substituted.',
                                                  'subject_id_system': 'Content-addressed key identifying '
                                                                       'the community patient, derived from '
                                                                       'the source patient number and '
                                                                       'registering source database '
                                                                       'identifier in the CSDS patient '
                                                                       'identifier lookup.',
                                                  'subject_key': 'Deterministic SHA-256 over the strongest '
                                                                 'source identifier. Not salted, not secret; '
                                                                 'a stable join key across feeds where '
                                                                 'person_id is unresolved.',
                                                  'unit_concept_id': 'OMOP standard concept identifier for '
                                                                     'the mapped unit of measure.',
                                                  'unit_concept_name': 'OMOP standard concept name for the '
                                                                       'mapped unit of measure.',
                                                  'unit_mapping_method': 'Indicator of the technique used to '
                                                                         'derive the unit mapping, such as '
                                                                         'direct code match or alias '
                                                                         'dictionary lookup.',
                                                  'unit_mapping_status': 'Status flag describing the outcome '
                                                                         'of mapping the source unit text to '
                                                                         'an OMOP unit concept.',
                                                  'unit_source_value': 'Unit of measure text as recorded in '
                                                                       'the source CSDS CYP202 care activity '
                                                                       'unit field, before normalization.',
                                                  'vip_ind': 'Indicator that the source record carries VIP '
                                                             'handling status for each community care '
                                                             'activity record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Null means the source did not '
                                                             'state the indicator and must not be '
                                                             'interpreted as false.',
                                                  'withheld_identity_ind': 'Indicator that the source '
                                                                           'identity was deliberately '
                                                                           'withheld for each community care '
                                                                           'activity record. It is produced '
                                                                           'by the silver transformation and '
                                                                           'has no direct bronze-column '
                                                                           'lineage entry. Null means the '
                                                                           'source did not state the '
                                                                           'indicator and must not be '
                                                                           'interpreted as false.'},
                                      'quantity_override': False,
                                      'table': 'One admitted CSDS community-care activity. Gold QC twin of '
                                               'the silver product: 3 columns are repaired or nulled, 1 '
                                               'rule(s) drop rows, 5 check(s) are advisory. Each rule states '
                                               'its reason in the pipeline notebook, and Lakeflow '
                                               'expectation metrics report what every rule matched on each '
                                               'update.'}}
