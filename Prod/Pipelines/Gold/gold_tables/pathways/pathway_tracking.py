"""Gold pathways.pathway_tracking: 2 dataset(s).

- datasets: _qc_clinical_pathway_tracking, clinical_pathway_tracking
- moved verbatim from: 4_gold_care_pathways:2213, 4_gold_care_pathways:2276, 4_gold_care_pathways:2285, 4_gold_care_pathways:2385, 4_gold_care_pathways:2390, 4_gold_care_pathways:2394, 4_gold_care_pathways:2416

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.pathway_tracking ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHWAY_TRACKING_SELECT = [
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
    '`ptl_unique_id` AS `ptl_unique_id`',
    '`ptl_group_id` AS `ptl_group_id`',
    '`archived_pathway` AS `archived_pathway`',
    '`pathway_oid` AS `pathway_oid`',
    '`referral_oid` AS `referral_oid`',
    '`patient_oid` AS `patient_oid`',
    '`ptl_activity_oid` AS `ptl_activity_oid`',
    '`parent_ptl_unique_id` AS `parent_ptl_unique_id`',
    '`parent_ptl_activity_oid` AS `parent_ptl_activity_oid`',
    '`parent_present_ind` AS `parent_present_ind`',
    '`latest_activity_type` AS `latest_activity_type`',
    '`latest_activity_oid` AS `latest_activity_oid`',
    '`latest_activity_date_future_ind` AS `latest_activity_date_future_ind`',
    '`days_waited` AS `days_waited`',
    '`specialty` AS `specialty`',
    '`treatment_function` AS `treatment_function`',
    "CASE WHEN UPPER(TRIM(CAST(`treatment_function_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `treatment_function_code` END AS `treatment_function_code`",
    '`site` AS `site`',
    '`site_group` AS `site_group`',
    '`division` AS `division`',
    '`lead_clinician` AS `lead_clinician`',
    '`lead_clinician_prid` AS `lead_clinician_prid`',
    '`site_rvid` AS `site_rvid`',
    '`source_key_status` AS `source_key_status`',
    '`patient_spine_ind` AS `patient_spine_ind`',
    '`pathway_spine_ind` AS `pathway_spine_ind`',
    '`referral_spine_ind` AS `referral_spine_ind`',
    '`patient_spine_link_status` AS `patient_spine_link_status`',
    '`pathway_spine_link_status` AS `pathway_spine_link_status`',
    '`referral_spine_link_status` AS `referral_spine_link_status`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`person_link_status` AS `person_link_status`',
    '`person_link_method` AS `person_link_method`',
    '`identifier_link_status` AS `identifier_link_status`',
    '`source_system_oid` AS `source_system_oid`',
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


# contract v2: QC/batch inputs come from internal _clinical_pathway_tracking_metadata; source history stays on the main research table.
CLINICAL_PATHWAY_TRACKING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 16,277,672 rows of 16,305,277
    # that are current and attributable; identity_status = 'resolved' keeps the 16,253,290
    # rows of 16,305,277 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_PATHWAY_TRACKING_ADVISORY_RULES = {
    # INSR_W4 W4.06 numeric_plausibility (estate 2026-10-29): A pathway wait outside 0-20 years is implausible (L4642).
    'gold.clinical.pathway_tracking.days_waited.insr_f9_days_waited_range':
        'NOT COALESCE((`days_waited` < 0 OR `days_waited` > 7300), FALSE)',
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.pathway_tracking.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 51,987 of 16,305,277 at the profile.
    "gold.clinical.pathway_tracking.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 27,605 of 16,305,277 at the profile.
    "gold.clinical.pathway_tracking.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 85,762 of 16,305,277 rows (0.526%) when profiled on 2026-08-24.
    "gold.clinical.pathway_tracking.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 917 of 16,305,277 rows (0.00562%) when profiled on 2026-08-24.
    "gold.clinical.pathway_tracking.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PATHWAY_TRACKING_PERSON_SCOPE_RULES = {
    "gold.clinical.pathway_tracking.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_PATHWAY_TRACKING_ORPHAN_RULES = {
    "gold.clinical.pathway_tracking.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_pathway_tracking"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PATHWAY_TRACKING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHWAY_TRACKING_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PATHWAY_TRACKING_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_PATHWAY_TRACKING_ORPHAN_RULES)
def _gold_qc_clinical_pathway_tracking():
    """Quality-controlled twin of journey_clinical.pathway_tracking."""
    # 7 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_pathway_tracking",
        CLINICAL_PATHWAY_TRACKING_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.pathway_tracking"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_pathway_tracking():
    """Contract-v2 public twin of clinical_pathway_tracking; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathway_tracking")).selectExpr(
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
        '`ptl_unique_id` AS `ptl_unique_id`',
        '`ptl_group_id` AS `ptl_group_id`',
        '`archived_pathway` AS `archived_pathway`',
        '`pathway_oid` AS `pathway_oid`',
        '`referral_oid` AS `referral_oid`',
        '`patient_oid` AS `patient_oid`',
        '`ptl_activity_oid` AS `ptl_activity_oid`',
        '`parent_ptl_unique_id` AS `parent_ptl_unique_id`',
        '`parent_ptl_activity_oid` AS `parent_ptl_activity_oid`',
        '`parent_present_ind` AS `parent_present_ind`',
        '`latest_activity_type` AS `latest_activity_type`',
        '`latest_activity_oid` AS `latest_activity_oid`',
        '`latest_activity_date_future_ind` AS `latest_activity_date_future_ind`',
        '`days_waited` AS `days_waited`',
        '`specialty` AS `specialty`',
        '`treatment_function` AS `treatment_function`',
        '`treatment_function_code` AS `treatment_function_code`',
        '`site` AS `site`',
        '`site_group` AS `site_group`',
        '`division` AS `division`',
        '`lead_clinician` AS `lead_clinician`',
        '`lead_clinician_prid` AS `lead_clinician_prid`',
        '`site_rvid` AS `site_rvid`',
        '`source_key_status` AS `source_key_status`',
        '`patient_spine_ind` AS `patient_spine_ind`',
        '`pathway_spine_ind` AS `pathway_spine_ind`',
        '`referral_spine_ind` AS `referral_spine_ind`',
        '`patient_spine_link_status` AS `patient_spine_link_status`',
        '`pathway_spine_link_status` AS `pathway_spine_link_status`',
        '`referral_spine_link_status` AS `referral_spine_link_status`',
        '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
        '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
        '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
        '`person_link_status` AS `person_link_status`',
        '`person_link_method` AS `person_link_method`',
        '`identifier_link_status` AS `identifier_link_status`',
        '`source_system_oid` AS `source_system_oid`',
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
    'clinical_pathway_tracking': GoldRules(
        table='clinical_pathway_tracking',
        select=CLINICAL_PATHWAY_TRACKING_SELECT,
        mandatory=CLINICAL_PATHWAY_TRACKING_MANDATORY_RULES,
        advisory=CLINICAL_PATHWAY_TRACKING_ADVISORY_RULES,
        person_scope=CLINICAL_PATHWAY_TRACKING_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_pathway_tracking': {'columns': {'archived_pathway': 'Source archived-pathway state retained '
                                                                   'verbatim; never used as a publication '
                                                                   'gate.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each pathway tracking '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'days_waited': 'B3 bronze field DAYS_WAITED; source value '
                                                              'retained unless documented as derived.',
                                               'division': 'B3 bronze field DIVISION; source value retained '
                                                           'unless documented as derived.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'B3 bronze field '
                                                                 'LATEST_ACTIVITY_DATE_CLEAN; source value '
                                                                 'retained unless documented as derived.',
                                               'event_datetime_local': 'Local-time timestamp of the PTL '
                                                                       'pathway activity event, '
                                                                       'corresponding to event_datetime.',
                                               'event_end_datetime': 'Date and time when the represented '
                                                                     'clinical or administrative event ended '
                                                                     'for each pathway tracking record. It '
                                                                     'is produced by the silver '
                                                                     'transformation and has no direct '
                                                                     'bronze-column lineage entry. Source '
                                                                     'precision and timezone handling follow '
                                                                     'the pipeline expression; null means '
                                                                     'the time was unavailable.',
                                               'event_end_datetime_local': 'Local-time end timestamp of the '
                                                                           'PTL pathway activity event, '
                                                                           'corresponding to '
                                                                           'event_end_datetime.',
                                               'fact_category': 'Contract category used to group the record '
                                                                'with comparable clinical facts for each '
                                                                'pathway tracking record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                               'identifier_link_status': 'B3 bronze field '
                                                                         'IDENTIFIER_LINK_STATUS; source '
                                                                         'value retained unless documented '
                                                                         'as derived.',
                                               'latest_activity_date_future_ind': 'B3 bronze field '
                                                                                  'LATEST_ACTIVITY_DATE_FUTURE_IND; '
                                                                                  'source value retained '
                                                                                  'unless documented as '
                                                                                  'derived.',
                                               'latest_activity_oid': 'B3 bronze field LATEST_ACTIVITY_OID; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                               'latest_activity_type': 'B3 bronze field '
                                                                       'LATEST_ACTIVITY_TYPE; source value '
                                                                       'retained unless documented as '
                                                                       'derived.',
                                               'lead_clinician': 'B3 bronze field LEAD_CLINICIAN; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                               'lead_clinician_prid': 'B3 bronze field LEAD_CLINICIAN_PRID; '
                                                                      'source value retained unless '
                                                                      'documented as derived.',
                                               'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                                'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                                'source value retained '
                                                                                'unless documented as '
                                                                                'derived.',
                                               'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                                  'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                                  'source value retained '
                                                                                  'unless documented as '
                                                                                  'derived.',
                                               'loaded_at': 'map_cancer_ptl.ADC_UPDT carried unchanged, '
                                                            'distinct from the PIPELINE_UPDT_DT_TM used for '
                                                            'source_update_timestamp. Parent and pathway '
                                                            'keys are derived without adding parent load '
                                                            'clocks; this is not the current Silver refresh '
                                                            'time.',
                                               'nhs_number_valid_ind': 'B3 bronze field '
                                                                       'NHS_NUMBER_VALID_IND; source value '
                                                                       'retained unless documented as '
                                                                       'derived.',
                                               'parent_present_ind': 'Whether a non-null parent '
                                                                     'PTL_UNIQUE_ID exists in the pinned '
                                                                     'source snapshot.',
                                               'parent_ptl_activity_oid': 'B3 bronze field '
                                                                          'PARENT_PTL_ACTIVITY_OID; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                               'parent_ptl_unique_id': 'B3 bronze field '
                                                                       'PARENT_PTL_UNIQUE_ID; source value '
                                                                       'retained unless documented as '
                                                                       'derived.',
                                               'pathway_oid': 'B3 bronze field PATHWAY_OID; source value '
                                                              'retained unless documented as derived.',
                                               'pathway_spine_ind': 'B3 bronze field PATHWAY_SPINE_IND; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                               'pathway_spine_link_status': 'B3 bronze field '
                                                                            'PATHWAY_SPINE_LINK_STATUS; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                               'patient_event_key': 'Deterministic event key derived from '
                                                                    'source identifiers; retained as a '
                                                                    'stable join key.',
                                               'patient_oid': 'B3 bronze field PATIENT_OID; source value '
                                                              'retained unless documented as derived.',
                                               'patient_spine_ind': 'B3 bronze field PATIENT_SPINE_IND; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                               'patient_spine_link_status': 'B3 bronze field '
                                                                            'PATIENT_SPINE_LINK_STATUS; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                            'resolved.',
                                               'person_link_method': 'B3 bronze field PERSON_LINK_METHOD; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                               'person_link_status': 'Consensus-safe published linkage '
                                                                     'status; conflicting arms never publish '
                                                                     'PERSON_ID.',
                                               'ptl_activity_oid': 'B3 bronze field PTL_ACTIVITY_OID; source '
                                                                   'value retained unless documented as '
                                                                   'derived.',
                                               'ptl_group_id': 'B3 bronze field PTL_GROUP_ID; source value '
                                                               'retained unless documented as derived.',
                                               'ptl_unique_id': 'Pathfinder PTL_UNIQUE_ID as BIGINT; primary '
                                                                'key of this table.',
                                               'record_status': 'Retracted when '
                                                                'map_cancer_ptl.SOURCE_PRESENT_IND is false, '
                                                                'otherwise active; missing presence defaults '
                                                                'true. No superseded status is emitted. '
                                                                'ARCHIVED_PATHWAY, parent-presence and '
                                                                'linkage flags do not set this label.',
                                               'record_status_effective_from': 'map_cancer_ptl.LATEST_ACTIVITY_DATE_CLEAN '
                                                                               'carried directly as the '
                                                                               'record-status start. The '
                                                                               'projection does not replace '
                                                                               'it with the pipeline clock '
                                                                               'or a joined parent '
                                                                               'timestamp.',
                                               'record_status_effective_to': 'map_cancer_ptl.SOURCE_ABSENT_DETECTED_TS '
                                                                             'when the source-presence flag '
                                                                             'is false; otherwise null. A '
                                                                             'missing absence-detection '
                                                                             'timestamp stays null, without '
                                                                             'an ADC_UPDT fallback.',
                                               'referral_oid': 'B3 bronze field REFERRAL_OID; source value '
                                                               'retained unless documented as derived.',
                                               'referral_spine_ind': 'B3 bronze field REFERRAL_SPINE_IND; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                               'referral_spine_link_status': 'B3 bronze field '
                                                                             'REFERRAL_SPINE_LINK_STATUS; '
                                                                             'source value retained unless '
                                                                             'documented as derived.',
                                               'site': 'B3 bronze field SITE; source value retained unless '
                                                       'documented as derived.',
                                               'site_group': 'B3 bronze field SITE_GROUP; source value '
                                                             'retained unless documented as derived.',
                                               'site_rvid': 'B3 bronze field SITE_RVID; source value '
                                                            'retained unless documented as derived.',
                                               'source_code': 'Code supplied by the originating source '
                                                              'system for the represented concept for each '
                                                              'pathway tracking record. It is produced by '
                                                              'the silver transformation and has no direct '
                                                              'bronze-column lineage entry. Source code '
                                                              'meanings and sentinel values are retained '
                                                              'unless the pipeline explicitly maps them; '
                                                              'null means no code was supplied.',
                                               'source_coding_system': 'Coding system or source namespace in '
                                                                       'which the source code is defined for '
                                                                       'each pathway tracking record. It is '
                                                                       'produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'source_display': 'Human-readable label supplied by the '
                                                                 'source system for the source code for each '
                                                                 'pathway tracking record. It is produced by '
                                                                 'the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each pathway tracking record. It '
                                                              'is produced by the silver transformation and '
                                                              'has no direct bronze-column lineage entry. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_key_status': 'COMPLETE or NAMESPACE_MISSING; missing '
                                                                    'namespaces publish and are never '
                                                                    'dropped.',
                                               'source_system_oid': 'B3 bronze field SOURCE_SYSTEM_OID; '
                                                                    'source value retained unless documented '
                                                                    'as derived.',
                                               'source_update_timestamp': 'Bronze run-as-of timestamp '
                                                                          'carried from '
                                                                          'map_cancer_ptl.PIPELINE_UPDT_DT_TM '
                                                                          'for the whole-trust Pathfinder '
                                                                          'tracker. The bronze writer stamps '
                                                                          'inserted, changed, revived or '
                                                                          'newly absent rows; unchanged '
                                                                          'present rows retain their prior '
                                                                          'stamp. Run-as-of defaults to '
                                                                          'processing time but can be '
                                                                          'supplied explicitly. This is not '
                                                                          'a clinical event time or '
                                                                          'native-system update timestamp.',
                                               'specialty': 'B3 bronze field SPECIALTY; source value '
                                                            'retained unless documented as derived.',
                                               'subject_id_system': 'B3 bronze field PATIENT_OID; source '
                                                                    'value retained unless documented as '
                                                                    'derived.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'treatment_function': 'B3 bronze field TREATMENT_FUNCTION; '
                                                                     'source value retained unless '
                                                                     'documented as derived.',
                                               'treatment_function_code': 'B3 bronze field '
                                                                          'TREATMENT_FUNCTION_CODE; source '
                                                                          'value retained unless documented '
                                                                          'as derived.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each pathway tracking record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Indicator that the source identity '
                                                                        'was deliberately withheld for each '
                                                                        'pathway tracking record. It is '
                                                                        'produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. Null '
                                                                        'means the source did not state the '
                                                                        'indicator and must not be '
                                                                        'interpreted as false.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_pathway_tracking: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_pathway_tracking': {'columns': {'archived_pathway': 'Source archived-pathway state retained '
                                                               'verbatim; never used as a publication gate.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each pathway '
                                                                   'tracking record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'days_waited': 'B3 bronze field DAYS_WAITED; source value '
                                                          'retained unless documented as derived.',
                                           'division': 'B3 bronze field DIVISION; source value retained '
                                                       'unless documented as derived.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'event_datetime': 'B3 bronze field LATEST_ACTIVITY_DATE_CLEAN; '
                                                             'source value retained unless documented as '
                                                             'derived.',
                                           'event_datetime_local': 'Local-time timestamp of the PTL pathway '
                                                                   'activity event, corresponding to '
                                                                   'event_datetime.',
                                           'event_end_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event ended for '
                                                                 'each pathway tracking record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                           'event_end_datetime_local': 'Local-time end timestamp of the PTL '
                                                                       'pathway activity event, '
                                                                       'corresponding to event_end_datetime.',
                                           'fact_category': 'Contract category used to group the record with '
                                                            'comparable clinical facts for each pathway '
                                                            'tracking record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'identifier_link_status': 'B3 bronze field '
                                                                     'IDENTIFIER_LINK_STATUS; source value '
                                                                     'retained unless documented as derived.',
                                           'latest_activity_date_future_ind': 'B3 bronze field '
                                                                              'LATEST_ACTIVITY_DATE_FUTURE_IND; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                           'latest_activity_oid': 'B3 bronze field LATEST_ACTIVITY_OID; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                           'latest_activity_type': 'B3 bronze field LATEST_ACTIVITY_TYPE; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'lead_clinician': 'B3 bronze field LEAD_CLINICIAN; source value '
                                                             'retained unless documented as derived.',
                                           'lead_clinician_prid': 'B3 bronze field LEAD_CLINICIAN_PRID; '
                                                                  'source value retained unless documented '
                                                                  'as derived.',
                                           'linkage_fallback_conflict_ind': 'B3 bronze field '
                                                                            'LINKAGE_FALLBACK_CONFLICT_IND; '
                                                                            'source value retained unless '
                                                                            'documented as derived.',
                                           'linkage_historical_fallback_ind': 'B3 bronze field '
                                                                              'LINKAGE_HISTORICAL_FALLBACK_IND; '
                                                                              'source value retained unless '
                                                                              'documented as derived.',
                                           'loaded_at': 'map_cancer_ptl.ADC_UPDT carried unchanged, distinct '
                                                        'from the PIPELINE_UPDT_DT_TM used for '
                                                        'source_update_timestamp. Parent and pathway keys '
                                                        'are derived without adding parent load clocks; this '
                                                        'is not the current Silver refresh time.',
                                           'nhs_number_valid_ind': 'B3 bronze field NHS_NUMBER_VALID_IND; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'parent_present_ind': 'Whether a non-null parent PTL_UNIQUE_ID '
                                                                 'exists in the pinned source snapshot.',
                                           'parent_ptl_activity_oid': 'B3 bronze field '
                                                                      'PARENT_PTL_ACTIVITY_OID; source value '
                                                                      'retained unless documented as '
                                                                      'derived.',
                                           'parent_ptl_unique_id': 'B3 bronze field PARENT_PTL_UNIQUE_ID; '
                                                                   'source value retained unless documented '
                                                                   'as derived.',
                                           'pathway_oid': 'B3 bronze field PATHWAY_OID; source value '
                                                          'retained unless documented as derived.',
                                           'pathway_spine_ind': 'B3 bronze field PATHWAY_SPINE_IND; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'pathway_spine_link_status': 'B3 bronze field '
                                                                        'PATHWAY_SPINE_LINK_STATUS; source '
                                                                        'value retained unless documented as '
                                                                        'derived.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'patient_oid': 'B3 bronze field PATIENT_OID; source value '
                                                          'retained unless documented as derived.',
                                           'patient_spine_ind': 'B3 bronze field PATIENT_SPINE_IND; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'patient_spine_link_status': 'B3 bronze field '
                                                                        'PATIENT_SPINE_LINK_STATUS; source '
                                                                        'value retained unless documented as '
                                                                        'derived.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'person_link_method': 'B3 bronze field PERSON_LINK_METHOD; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'person_link_status': 'Consensus-safe published linkage status; '
                                                                 'conflicting arms never publish PERSON_ID.',
                                           'ptl_activity_oid': 'B3 bronze field PTL_ACTIVITY_OID; source '
                                                               'value retained unless documented as derived.',
                                           'ptl_group_id': 'B3 bronze field PTL_GROUP_ID; source value '
                                                           'retained unless documented as derived.',
                                           'ptl_unique_id': 'Pathfinder PTL_UNIQUE_ID as BIGINT; primary key '
                                                            'of this table.',
                                           'record_status': 'Retracted when '
                                                            'map_cancer_ptl.SOURCE_PRESENT_IND is false, '
                                                            'otherwise active; missing presence defaults '
                                                            'true. No superseded status is emitted. '
                                                            'ARCHIVED_PATHWAY, parent-presence and linkage '
                                                            'flags do not set this label.',
                                           'record_status_effective_from': 'map_cancer_ptl.LATEST_ACTIVITY_DATE_CLEAN '
                                                                           'carried directly as the '
                                                                           'record-status start. The '
                                                                           'projection does not replace it '
                                                                           'with the pipeline clock or a '
                                                                           'joined parent timestamp.',
                                           'record_status_effective_to': 'map_cancer_ptl.SOURCE_ABSENT_DETECTED_TS '
                                                                         'when the source-presence flag is '
                                                                         'false; otherwise null. A missing '
                                                                         'absence-detection timestamp stays '
                                                                         'null, without an ADC_UPDT '
                                                                         'fallback.',
                                           'referral_oid': 'B3 bronze field REFERRAL_OID; source value '
                                                           'retained unless documented as derived.',
                                           'referral_spine_ind': 'B3 bronze field REFERRAL_SPINE_IND; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'referral_spine_link_status': 'B3 bronze field '
                                                                         'REFERRAL_SPINE_LINK_STATUS; source '
                                                                         'value retained unless documented '
                                                                         'as derived.',
                                           'site': 'B3 bronze field SITE; source value retained unless '
                                                   'documented as derived.',
                                           'site_group': 'B3 bronze field SITE_GROUP; source value retained '
                                                         'unless documented as derived.',
                                           'site_rvid': 'B3 bronze field SITE_RVID; source value retained '
                                                        'unless documented as derived.',
                                           'source_code': 'Code supplied by the originating source system '
                                                          'for the represented concept for each pathway '
                                                          'tracking record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Source code meanings and sentinel '
                                                          'values are retained unless the pipeline '
                                                          'explicitly maps them; null means no code was '
                                                          'supplied.',
                                           'source_coding_system': 'Coding system or source namespace in '
                                                                   'which the source code is defined for '
                                                                   'each pathway tracking record. It is '
                                                                   'produced by the silver transformation '
                                                                   'and has no direct bronze-column lineage '
                                                                   'entry. Source code meanings and sentinel '
                                                                   'values are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'source_display': 'Human-readable label supplied by the source '
                                                             'system for the source code for each pathway '
                                                             'tracking record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each pathway tracking record. It is produced '
                                                          'by the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_key_status': 'COMPLETE or NAMESPACE_MISSING; missing '
                                                                'namespaces publish and are never dropped.',
                                           'source_system_oid': 'B3 bronze field SOURCE_SYSTEM_OID; source '
                                                                'value retained unless documented as '
                                                                'derived.',
                                           'source_update_timestamp': 'Bronze run-as-of timestamp carried '
                                                                      'from '
                                                                      'map_cancer_ptl.PIPELINE_UPDT_DT_TM '
                                                                      'for the whole-trust Pathfinder '
                                                                      'tracker. The bronze writer stamps '
                                                                      'inserted, changed, revived or newly '
                                                                      'absent rows; unchanged present rows '
                                                                      'retain their prior stamp. Run-as-of '
                                                                      'defaults to processing time but can '
                                                                      'be supplied explicitly. This is not a '
                                                                      'clinical event time or native-system '
                                                                      'update timestamp.',
                                           'specialty': 'B3 bronze field SPECIALTY; source value retained '
                                                        'unless documented as derived.',
                                           'subject_id_system': 'B3 bronze field PATIENT_OID; source value '
                                                                'retained unless documented as derived.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_function': 'B3 bronze field TREATMENT_FUNCTION; source '
                                                                 'value retained unless documented as '
                                                                 'derived.',
                                           'treatment_function_code': 'B3 bronze field '
                                                                      'TREATMENT_FUNCTION_CODE; source value '
                                                                      'retained unless documented as '
                                                                      'derived.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each pathway tracking record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Indicator that the source identity was '
                                                                    'deliberately withheld for each pathway '
                                                                    'tracking record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Null means '
                                                                    'the source did not state the indicator '
                                                                    'and must not be interpreted as false.'},
                               'quantity_override': False,
                               'table': 'One whole-trust Pathfinder PTL activity row. Gold QC twin of the '
                                        'silver product: 2 columns are repaired or nulled, 1 rule(s) drop '
                                        'rows, 4 check(s) are advisory. Each rule states its reason in the '
                                        'pipeline notebook, and Lakeflow expectation metrics report what '
                                        'every rule matched on each update.'}}
