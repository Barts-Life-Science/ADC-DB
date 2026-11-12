"""Gold clinical.specimen: 2 dataset(s).

- datasets: _qc_clinical_specimen, clinical_specimen
- moved verbatim from: 2_gold_clinical_events:5778, 2_gold_clinical_events:5826, 2_gold_clinical_events:5835, 2_gold_clinical_events:5921, 2_gold_clinical_events:5926, 2_gold_clinical_events:5930, 2_gold_clinical_events:5944

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.specimen ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_SPECIMEN_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`specimen_type` AS `specimen_type`',
    '`accession_identifier` AS `accession_identifier`',
    '`resolved_accession_key` AS `resolved_accession_key`',
    '`normalized_lab_no` AS `normalized_lab_no`',
    '`canonical_accession_status` AS `canonical_accession_status`',
    '`person_resolution_status` AS `person_resolution_status`',
    '`lab_series` AS `lab_series`',
    '`discipline` AS `discipline`',
    '`urgent_flag` AS `urgent_flag`',
    '`research_qi_only` AS `research_qi_only`',
    '`clinical_details` AS `clinical_details`',
    '`tlcs_requested` AS `tlcs_requested`',
    '`conditions` AS `conditions`',
    '`reason` AS `reason`',
    "CASE WHEN UPPER(TRIM(CAST(`body_site_code` AS STRING))) = '0' THEN NULL ELSE `body_site_code` END AS `body_site_code`",
    '`body_site_snomed_code` AS `body_site_snomed_code`',
    "CASE WHEN UPPER(TRIM(CAST(`specimen_type_code` AS STRING))) = '0' THEN NULL ELSE `specimen_type_code` END AS `specimen_type_code`",
    '`specimen_type_snomed_code` AS `specimen_type_snomed_code`',
    'CASE WHEN CAST(`sample_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `sample_datetime` END AS `sample_datetime`',
    'CASE WHEN CAST(`request_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `request_datetime` END AS `request_datetime`',
    'CASE WHEN CAST(`report_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`report_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `report_datetime` END AS `report_datetime`',
    '`source_history_row_count` AS `source_history_row_count`',
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


# contract v2: QC/batch inputs come from internal _clinical_specimen_metadata; source history stays on the main research table.
CLINICAL_SPECIMEN_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 172,077,158 rows of
    # 172,077,158 that are current and attributable; identity_status = 'resolved' keeps the
    # 150,927,313 rows of 172,077,158 that are current and attributable. Superseded versions
    # and rows whose identity was never resolved are not research data, and a consumer who
    # wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_SPECIMEN_ADVISORY_RULES = {
    # INSR_W4 W4.06 cross_field (P5f): A specimen with an event time has its local time (L3636; W2.07 fills the 2026-07/08 gap).
    'gold.clinical.specimen.event_datetime_local.insr_f19_local_time_missing':
        'NOT COALESCE((`event_datetime` IS NOT NULL AND `event_datetime_local` IS NULL), FALSE)',
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.specimen.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 21,149,845 of 172,077,158 at the profile.
    "gold.clinical.specimen.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 172,077,158 at the profile.
    "gold.clinical.specimen.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 88,413 of 172,077,158 rows (0.0514%) when profiled on
    # 2026-08-24.
    "gold.clinical.specimen.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,203,382 of 172,077,158 rows (0.699%) when profiled on
    # 2026-08-24.
    "gold.clinical.specimen.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_SPECIMEN_PERSON_SCOPE_RULES = {
    "gold.clinical.specimen.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_SPECIMEN_ORPHAN_RULES = {
    "gold.clinical.specimen.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_specimen"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SPECIMEN_MANDATORY_RULES)
@dp.expect_all(CLINICAL_SPECIMEN_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_SPECIMEN_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_SPECIMEN_ORPHAN_RULES)
def _gold_qc_clinical_specimen():
    """Quality-controlled twin of journey_clinical.specimen."""
    df = _qc("clinical_specimen", CLINICAL_SPECIMEN_SELECT, fk_columns=("encounter_id",))
    return df


@dp.materialized_view(
    name=_n("gold_clinical.specimen"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_specimen():
    """Contract-v2 public twin of clinical_specimen; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_specimen")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_specimen_type` AS `specimen_type`',
        '`accession_identifier` AS `accession_identifier`',
        '`resolved_accession_key` AS `resolved_accession_key`',
        '`normalized_lab_no` AS `normalized_lab_no`',
        '`canonical_accession_status` AS `canonical_accession_status`',
        '`person_resolution_status` AS `person_resolution_status`',
        '`lab_series` AS `lab_series`',
        '`discipline` AS `discipline`',
        '`urgent_flag` AS `urgent_flag`',
        '`research_qi_only` AS `research_qi_only`',
        '`clinical_details` AS `clinical_details`',
        '`tlcs_requested` AS `tlcs_requested`',
        '`conditions` AS `conditions`',
        '`reason` AS `reason`',
        '`body_site_code` AS `body_site_code`',
        '`body_site_snomed_code` AS `body_site_snomed_code`',
        '`specimen_type_code` AS `specimen_type_code`',
        '`specimen_type_snomed_code` AS `specimen_type_snomed_code`',
        '`sample_datetime` AS `sample_datetime`',
        '`request_datetime` AS `request_datetime`',
        '`report_datetime` AS `report_datetime`',
        '`source_history_row_count` AS `source_history_row_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(df, "clinical_specimen")


def _public_steps_clinical_specimen(df):
    """clinical_specimen: the public body after its projection (GoldRules.public_select), moved from gold_clinical_specimen; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_specimen')


RULES = {
    'clinical_specimen': GoldRules(
        table='clinical_specimen',
        select=CLINICAL_SPECIMEN_SELECT,
        mandatory=CLINICAL_SPECIMEN_MANDATORY_RULES,
        advisory=CLINICAL_SPECIMEN_ADVISORY_RULES,
        person_scope=CLINICAL_SPECIMEN_PERSON_SCOPE_RULES,
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
        variant_columns=('specimen_type',),
        s3_axis_specs={'specimen_type': [('specimen_type', False)]},
        value_rules={'gold.clinical.specimen.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                'column': 'body_site_snomed_code',
                                                                                'companions': [],
                                                                                'match': '`body_site_snomed_code` '
                                                                                         'IS NOT NULL AND '
                                                                                         '(NOT '
                                                                                         '`body_site_snomed_code` '
                                                                                         'RLIKE '
                                                                                         "'^[1-9][0-9]{5,17}$' "
                                                                                         'OR '
                                                                                         'substring(`body_site_snomed_code`, '
                                                                                         'length(`body_site_snomed_code`) '
                                                                                         '- 2, 2) NOT IN '
                                                                                         "('00', '10') OR "
                                                                                         '(substring(`body_site_snomed_code`, '
                                                                                         'length(`body_site_snomed_code`) '
                                                                                         "- 2, 2) = '10' "
                                                                                         'AND '
                                                                                         'length(`body_site_snomed_code`) '
                                                                                         '< 11))',
                                                                                'table': 'clinical_specimen'},
         'gold.clinical.specimen.specimen_type_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'specimen_type_snomed_code',
                                                                                    'companions': ['specimen_type_snomed_display',
                                                                                                   'specimen_type_target_code',
                                                                                                   'specimen_type_target_display'],
                                                                                    'match': '`specimen_type_snomed_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`specimen_type_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`specimen_type_snomed_code`, '
                                                                                             'length(`specimen_type_snomed_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`specimen_type_snomed_code`, '
                                                                                             'length(`specimen_type_snomed_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`specimen_type_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_specimen'},
         'gold.clinical.specimen.specimen_type_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'specimen_type_target_code',
                                                                                    'companions': ['specimen_type_target_display',
                                                                                                   'specimen_type_snomed_code',
                                                                                                   'specimen_type_snomed_display'],
                                                                                    'match': '`specimen_type_target_system` '
                                                                                             '= '
                                                                                             "'http://snomed.info/sct' "
                                                                                             'AND '
                                                                                             '`specimen_type_target_code` '
                                                                                             'IS NOT NULL '
                                                                                             'AND (NOT '
                                                                                             '`specimen_type_target_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`specimen_type_target_code`, '
                                                                                             'length(`specimen_type_target_code`) '
                                                                                             '- 2, 2) NOT '
                                                                                             "IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`specimen_type_target_code`, '
                                                                                             'length(`specimen_type_target_code`) '
                                                                                             '- 2, 2) = '
                                                                                             "'10' AND "
                                                                                             'length(`specimen_type_target_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_specimen'}},
        public_select=[
            '`patient_event_key` AS `patient_event_key`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`encounter_id` AS `encounter_id`',
            '`encounter_link_method` AS `encounter_link_method`',
            '`event_datetime` AS `event_datetime`',
            '`event_end_datetime` AS `event_end_datetime`',
            '`event_datetime_local` AS `event_datetime_local`',
            '`event_end_datetime_local` AS `event_end_datetime_local`',
            '`source_coding_system` AS `source_coding_system`',
            '`source_code` AS `source_code`',
            '`source_display` AS `source_display`',
            '`__gold_json_specimen_type` AS `specimen_type`',
            '`accession_identifier` AS `accession_identifier`',
            '`resolved_accession_key` AS `resolved_accession_key`',
            '`normalized_lab_no` AS `normalized_lab_no`',
            '`canonical_accession_status` AS `canonical_accession_status`',
            '`person_resolution_status` AS `person_resolution_status`',
            '`lab_series` AS `lab_series`',
            '`discipline` AS `discipline`',
            '`urgent_flag` AS `urgent_flag`',
            '`research_qi_only` AS `research_qi_only`',
            '`clinical_details` AS `clinical_details`',
            '`tlcs_requested` AS `tlcs_requested`',
            '`conditions` AS `conditions`',
            '`reason` AS `reason`',
            '`body_site_code` AS `body_site_code`',
            '`body_site_snomed_code` AS `body_site_snomed_code`',
            '`specimen_type_code` AS `specimen_type_code`',
            '`specimen_type_snomed_code` AS `specimen_type_snomed_code`',
            '`sample_datetime` AS `sample_datetime`',
            '`request_datetime` AS `request_datetime`',
            '`report_datetime` AS `report_datetime`',
            '`source_history_row_count` AS `source_history_row_count`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`withheld_identity_ind` AS `withheld_identity_ind`',
            '`source_feed` AS `source_feed`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_clinical_specimen,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_specimen': {'columns': {'accession_identifier': 'Canonical accession identifier '
                                                               '(pathology_accession_id); never '
                                                               'LabNo-derived.',
                                       'body_site_code': 'Source collection body-site code.',
                                       'body_site_snomed_code': 'Source-provided SNOMED body-site code.',
                                       'canonical_accession_status': 'Canonical accession-link state.',
                                       'clinical_details': 'Source clinical details. Identifiable free text; '
                                                           'ig_risk 4',
                                       'conditions': 'Source request conditions. Identifiable free text; '
                                                     'ig_risk 4',
                                       'confidentiality_code': 'Source confidentiality code.',
                                       'discipline': 'Source pathology discipline.',
                                       'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                       'encounter_link_method': 'How encounter_id was set '
                                                                '(PATH_ENCOUNTER_V1). source_accession: the '
                                                                "row's own Millennium source names the "
                                                                'encounter. accession_single: every current '
                                                                'Millennium source of the alias-resolved '
                                                                'accession names this one encounter (how TFC '
                                                                'LIMS rows inherit one once their accession '
                                                                'is merged with the Millennium order). '
                                                                'accession_multiple: those sources name '
                                                                'different encounters, so encounter_id is '
                                                                'NULL. none: the accession has no Millennium '
                                                                'source, so encounter_id is NULL. Gold: '
                                                                'encounter_id can still be NULL here when '
                                                                'the encounter is not in the Gold encounter '
                                                                'spine.',
                                       'event_datetime': 'Collection or receipt timestamp.',
                                       'event_datetime_local': 'Local-time representation of the specimen '
                                                               'event (collection or receipt) timestamp. '
                                                               'Gold: NULL wherever Gold nulls '
                                                               'event_datetime.',
                                       'event_end_datetime': 'Event end when supplied.',
                                       'event_end_datetime_local': 'Local-time representation of the '
                                                                   'specimen event end timestamp, when '
                                                                   'supplied.',
                                       'lab_series': 'Source laboratory series.',
                                       'loaded_at': 'ADC_UPDT of the surviving map_pathology_accession row, '
                                                    'carried unchanged through identity and QC/public '
                                                    'projections. The separately aggregated maximum '
                                                    'evidence_loaded_at from alias-resolved '
                                                    'map_pathology_accession_source rows is not used here. '
                                                    "This is the accession row's load clock, not created_at, "
                                                    'specimen collection time or Silver refresh time.',
                                       'normalized_lab_no': 'Matching evidence only — reused lab numbers '
                                                            'reach 3',
                                       'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                            'cross-feed join key.',
                                       'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                       'person_resolution_status': 'Person-projection eligibility state.',
                                       'reason': 'Source request reason. Identifiable free text; ig_risk 4',
                                       'record_status': 'Always active in the current specimen projection of '
                                                        'surviving pathology accessions. The input is '
                                                        'restricted to accessions whose ID equals the '
                                                        'alias-resolved survivor ID; '
                                                        'canonical_accession_status and lifecycle_status do '
                                                        'not change this literal label.',
                                       'record_status_effective_from': 'Time this immutable accession '
                                                                       'identifier was first assigned.',
                                       'record_status_effective_to': 'Always null as a TIMESTAMP in the '
                                                                     'current specimen projection. Sample, '
                                                                     'request and report dates and accession '
                                                                     'lifecycle fields are not used as '
                                                                     'status ends.',
                                       'report_datetime': 'Clamped source report timestamp.',
                                       'request_datetime': 'Clamped source request timestamp.',
                                       'research_qi_only': 'Registry doctrine flag; true on 100% of rows '
                                                           'today — describe-only',
                                       'resolved_accession_key': 'Alias-resolved accession key retained as '
                                                                 'derivation evidence.',
                                       'sample_datetime': 'Clamped source sample timestamp.',
                                       'source_code': 'Source specimen type code.',
                                       'source_coding_system': 'Source specimen type system.',
                                       'source_display': 'Source specimen type display.',
                                       'source_feed': 'Registered owning feed.',
                                       'source_history_row_count': 'accession_source rows behind this '
                                                                   'accession.',
                                       'source_update_timestamp': 'Time this immutable accession identifier '
                                                                  'was first assigned.',
                                       'specimen_type': 'Source specimen type CodeableConcept.',
                                       'specimen_type_code': 'Native specimen-type code.',
                                       'specimen_type_snomed_code': 'Source-provided SNOMED specimen-type '
                                                                    'code.',
                                       'subject_id_system': 'Identifier system used for subject_key.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'tlcs_requested': 'Requested TLC context. Identifiable free text; '
                                                         'ig_risk 4',
                                       'urgent_flag': 'Source urgent-request indicator code (Y',
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each specimen record. It is produced by the '
                                                  'silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Withheld identity indicator.'},
                           'quantity_override': True,
                           'table': 'Internal quality-controlled twin of clinical_specimen: the Gold '
                                    'repairs, nulls and expectations are applied here and the published Gold '
                                    'MV reads this object. Materialized rather than temporary so the '
                                    'published MV can refresh incrementally.'},
 'clinical_specimen': {'columns': {'accession_identifier': 'Canonical accession identifier '
                                                           '(pathology_accession_id); never LabNo-derived.',
                                   'body_site_code': 'Source collection body-site code.',
                                   'body_site_snomed_code': 'Source-provided SNOMED body-site code.',
                                   'canonical_accession_status': 'Canonical accession-link state.',
                                   'clinical_details': 'Source clinical details. Identifiable free text; '
                                                       'ig_risk 4',
                                   'conditions': 'Source request conditions. Identifiable free text; ig_risk '
                                                 '4',
                                   'confidentiality_code': 'Source confidentiality code.',
                                   'discipline': 'Source pathology discipline.',
                                   'encounter_id': 'Millennium ENCNTR_ID when the source or accession names '
                                                   'the encounter (see encounter_link_method); NULL '
                                                   'otherwise.',
                                   'encounter_link_method': 'How encounter_id was set (PATH_ENCOUNTER_V1). '
                                                            "source_accession: the row's own Millennium "
                                                            'source names the encounter. accession_single: '
                                                            'every current Millennium source of the '
                                                            'alias-resolved accession names this one '
                                                            'encounter (how TFC LIMS rows inherit one once '
                                                            'their accession is merged with the Millennium '
                                                            'order). accession_multiple: those sources name '
                                                            'different encounters, so encounter_id is NULL. '
                                                            'none: the accession has no Millennium source, '
                                                            'so encounter_id is NULL. Gold: encounter_id can '
                                                            'still be NULL here when the encounter is not in '
                                                            'the Gold encounter spine.',
                                   'event_datetime': 'Collection or receipt timestamp.',
                                   'event_datetime_local': 'Local-time representation of the specimen event '
                                                           '(collection or receipt) timestamp. Gold: NULL '
                                                           'wherever Gold nulls event_datetime.',
                                   'event_end_datetime': 'Event end when supplied.',
                                   'event_end_datetime_local': 'Local-time representation of the specimen '
                                                               'event end timestamp, when supplied.',
                                   'lab_series': 'Source laboratory series.',
                                   'loaded_at': 'ADC_UPDT of the surviving map_pathology_accession row, '
                                                'carried unchanged through identity and QC/public '
                                                'projections. The separately aggregated maximum '
                                                'evidence_loaded_at from alias-resolved '
                                                'map_pathology_accession_source rows is not used here. This '
                                                "is the accession row's load clock, not created_at, specimen "
                                                'collection time or Silver refresh time.',
                                   'normalized_lab_no': 'Matching evidence only — reused lab numbers reach 3',
                                   'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                        'cross-feed join key.',
                                   'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                   'person_resolution_status': 'Person-projection eligibility state.',
                                   'reason': 'Source request reason. Identifiable free text; ig_risk 4',
                                   'record_status': 'Always active in the current specimen projection of '
                                                    'surviving pathology accessions. The input is restricted '
                                                    'to accessions whose ID equals the alias-resolved '
                                                    'survivor ID; canonical_accession_status and '
                                                    'lifecycle_status do not change this literal label.',
                                   'record_status_effective_from': 'Time this immutable accession identifier '
                                                                   'was first assigned.',
                                   'record_status_effective_to': 'Always null as a TIMESTAMP in the current '
                                                                 'specimen projection. Sample, request and '
                                                                 'report dates and accession lifecycle '
                                                                 'fields are not used as status ends.',
                                   'report_datetime': 'Clamped source report timestamp.',
                                   'request_datetime': 'Clamped source request timestamp.',
                                   'research_qi_only': 'Registry doctrine flag; true on 100% of rows today — '
                                                       'describe-only',
                                   'resolved_accession_key': 'Alias-resolved accession key retained as '
                                                             'derivation evidence.',
                                   'sample_datetime': 'Clamped source sample timestamp.',
                                   'source_code': 'Source specimen type code.',
                                   'source_coding_system': 'Source specimen type system.',
                                   'source_display': 'Source specimen type display.',
                                   'source_feed': 'Registered owning feed.',
                                   'source_history_row_count': 'accession_source rows behind this accession.',
                                   'source_update_timestamp': 'Time this immutable accession identifier was '
                                                              'first assigned.',
                                   'specimen_type': 'Source specimen type CodeableConcept.',
                                   'specimen_type_code': 'Native specimen-type code.',
                                   'specimen_type_snomed_code': 'SNOMED CT specimen type mapped from the '
                                                                'native LIMS or Millennium specimen code '
                                                                '(best-effort map with provenance in '
                                                                'specimen_type_map_*); the source vocabulary '
                                                                'collapses to a small set of concepts.',
                                   'subject_id_system': 'Identifier system used for subject_key.',
                                   'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                  'identifier. Not salted, not secret; a stable join key '
                                                  'across feeds where person_id is unresolved.',
                                   'tlcs_requested': 'Requested TLC context. Identifiable free text; ig_risk '
                                                     '4',
                                   'urgent_flag': 'Source urgent-request indicator code (Y',
                                   'vip_ind': 'Indicator that the source record carries VIP handling status '
                                              'for each specimen record. It is produced by the silver '
                                              'transformation and has no direct bronze-column lineage entry. '
                                              'Null means the source did not state the indicator and must '
                                              'not be interpreted as false.',
                                   'withheld_identity_ind': 'Withheld identity indicator.'},
                       'quantity_override': False,
                       'table': 'One canonical pathology accession with collection and request evidence. '
                                'Gold QC twin of the silver product: 10 columns are repaired or nulled, 1 '
                                'rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in '
                                'the pipeline notebook, and Lakeflow expectation metrics report what every '
                                'rule matched on each update. Coverage: LIMS 2 and 3 sample-level specimens '
                                'start in January 2013, so 2012 specimen volumes are about 40% of 2013; '
                                'complete-through month per release: 6_mgmt.data_quality.release_finding '
                                '(finding_code F2_complete_through).'}}
