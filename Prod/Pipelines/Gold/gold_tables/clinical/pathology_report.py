"""Gold clinical.pathology_report: 2 dataset(s).

- datasets: _qc_clinical_pathology_report, clinical_pathology_report
- moved verbatim from: 2_gold_clinical_events:4788, 2_gold_clinical_events:4833, 2_gold_clinical_events:4842, 2_gold_clinical_events:4925, 2_gold_clinical_events:4929, 2_gold_clinical_events:4942

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.pathology_report ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_PATHOLOGY_REPORT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    'CASE WHEN NOT `person_id_resolved` THEN NULL ELSE `person_id` END AS `person_id`',
    'CASE WHEN NOT `encounter_id_resolved` THEN NULL ELSE `encounter_id` END AS `encounter_id`',
    '`encounter_link_method` AS `encounter_link_method`',  # GOLD_COLUMN_PARITY_V1
    # INSR_W3 W3.06: Gold: NULL where a pinned LIMS placeholder day (P3e).
    "CASE WHEN CAST(`event_datetime` AS DATE) IN (DATE'1999-12-30') THEN NULL ELSE CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END END AS `event_datetime`",
    '`event_end_datetime` AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`report_code` AS `report_code`',
    '`report_role` AS `report_role`',
    '`discipline` AS `discipline`',
    '`report_section` AS `report_section`',
    '`lifecycle_status` AS `lifecycle_status`',
    '`version_ordinal` AS `version_ordinal`',
    '`version_count` AS `version_count`',
    '`report_version_key` AS `report_version_key`',
    '`supersedes_report_version_key` AS `supersedes_report_version_key`',
    '`is_current_present` AS `is_current_present`',
    '`document_key` AS `document_key`',
    'CASE WHEN CAST(`issued_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `issued_datetime` END AS `issued_datetime`',
    '`specimen_key` AS `specimen_key`',
    '`accession_identifier` AS `accession_identifier`',
    '`report_text_hash` AS `report_text_hash`',
    '`research_qi_only` AS `research_qi_only`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    # INSR_W3 W3.05: Gold: NULL where the OLE zero date or a post-2100 year below 9999.
    "CASE WHEN CAST(`record_status_effective_from` AS DATE) = DATE'1899-12-30' OR (CAST(`record_status_effective_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`record_status_effective_from` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_from` END AS `record_status_effective_from`",
    '`record_status_effective_to` AS `record_status_effective_to`',
    # INSR_W3 W3.05: Gold: NULL where the OLE zero date or a post-2100 year below 9999.
    "CASE WHEN CAST(`source_update_timestamp` AS DATE) = DATE'1899-12-30' OR (CAST(`source_update_timestamp` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`source_update_timestamp` AS DATE)) < 9999) THEN NULL ELSE `source_update_timestamp` END AS `source_update_timestamp`",
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_pathology_report_metadata; source history stays on the main research table.
CLINICAL_PATHOLOGY_REPORT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 91,718,730 rows of 91,759,012
    # that are current and attributable; identity_status = 'resolved' keeps the 71,790,751
    # rows of 91,759,012 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_PATHOLOGY_REPORT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.pathology_report.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 19,968,261 of 91,759,012 at the profile.
    "gold.clinical.pathology_report.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 40,282 of 91,759,012 at the profile.
    "gold.clinical.pathology_report.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7 of 91,759,012
    # rows (7.63e-06%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 167,974 of 91,759,012 rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,142 of 91,759,012 rows (0.00669%) when profiled on 2026-08-24.
    "gold.clinical.pathology_report.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PATHOLOGY_REPORT_PERSON_SCOPE_RULES = {
    "gold.clinical.pathology_report.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_pathology_report"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_REPORT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_PATHOLOGY_REPORT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PATHOLOGY_REPORT_PERSON_SCOPE_RULES)
def _gold_qc_clinical_pathology_report():
    """Quality-controlled twin of journey_clinical.pathology_report."""
    df = _qc("clinical_pathology_report", CLINICAL_PATHOLOGY_REPORT_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_clinical.pathology_report"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_pathology_report():
    """Contract-v2 public twin of clinical_pathology_report; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_pathology_report")).selectExpr(
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
        '`__gold_json_report_code` AS `report_code`',
        '`report_role` AS `report_role`',
        '`discipline` AS `discipline`',
        '`report_section` AS `report_section`',
        '`lifecycle_status` AS `lifecycle_status`',
        '`version_ordinal` AS `version_ordinal`',
        '`version_count` AS `version_count`',
        '`report_version_key` AS `report_version_key`',
        '`supersedes_report_version_key` AS `supersedes_report_version_key`',
        '`is_current_present` AS `is_current_present`',
        '`document_key` AS `document_key`',
        '`issued_datetime` AS `issued_datetime`',
        '`specimen_key` AS `specimen_key`',
        '`accession_identifier` AS `accession_identifier`',
        '`report_text_hash` AS `report_text_hash`',
        '`research_qi_only` AS `research_qi_only`',
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
    return _s3_flatten_gold_public(df, "clinical_pathology_report")


def _public_steps_clinical_pathology_report(df):
    """clinical_pathology_report: the public body after its projection (GoldRules.public_select), moved from gold_clinical_pathology_report; df is the projected twin frame."""
    return _s3_flatten_gold_public(df, 'clinical_pathology_report')


RULES = {
    'clinical_pathology_report': GoldRules(
        table='clinical_pathology_report',
        select=CLINICAL_PATHOLOGY_REPORT_SELECT,
        mandatory=CLINICAL_PATHOLOGY_REPORT_MANDATORY_RULES,
        advisory=CLINICAL_PATHOLOGY_REPORT_ADVISORY_RULES,
        person_scope=CLINICAL_PATHOLOGY_REPORT_PERSON_SCOPE_RULES,
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
        variant_columns=('report_code',),
        s3_axis_specs={'report_code': [('report', True)]},
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
            '`__gold_json_report_code` AS `report_code`',
            '`report_role` AS `report_role`',
            '`discipline` AS `discipline`',
            '`report_section` AS `report_section`',
            '`lifecycle_status` AS `lifecycle_status`',
            '`version_ordinal` AS `version_ordinal`',
            '`version_count` AS `version_count`',
            '`report_version_key` AS `report_version_key`',
            '`supersedes_report_version_key` AS `supersedes_report_version_key`',
            '`is_current_present` AS `is_current_present`',
            '`document_key` AS `document_key`',
            '`issued_datetime` AS `issued_datetime`',
            '`specimen_key` AS `specimen_key`',
            '`accession_identifier` AS `accession_identifier`',
            '`report_text_hash` AS `report_text_hash`',
            '`research_qi_only` AS `research_qi_only`',
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
        public_steps=(_public_steps_clinical_pathology_report,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_flatten_gold_public = _rules._s3_flatten_gold_public


finalise()


COMMENTS = {'_qc_clinical_pathology_report': {'columns': {'accession_identifier': 'Alias-resolved canonical pathology '
                                                                       'accession identifier carried by the '
                                                                       'selected report-series version; not '
                                                                       'the source result-record key.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each pathology report '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'discipline': 'Latest-version pathology discipline.',
                                               'document_key': 'Deterministic text-document key.',
                                               'encounter_id': 'Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'encounter_link_method': 'How encounter_id was set '
                                                                        '(PATH_ENCOUNTER_V1). '
                                                                        "source_accession: the row's own "
                                                                        'Millennium source names the '
                                                                        'encounter. accession_single: every '
                                                                        'current Millennium source of the '
                                                                        'alias-resolved accession names this '
                                                                        'one encounter (how TFC LIMS rows '
                                                                        'inherit one once their accession is '
                                                                        'merged with the Millennium order). '
                                                                        'accession_multiple: those sources '
                                                                        'name different encounters, so '
                                                                        'encounter_id is NULL. none: the '
                                                                        'accession has no Millennium source, '
                                                                        'so encounter_id is NULL. Gold: '
                                                                        'encounter_id can still be NULL here '
                                                                        'when the encounter is not in the '
                                                                        'Gold encounter spine.',
                                               'event_datetime': 'Report issue or result timestamp.',
                                               'event_datetime_local': 'Report issue or result timestamp '
                                                                       'expressed in local time. Gold: NULL '
                                                                       'wherever Gold nulls event_datetime.',
                                               'event_end_datetime': 'Report event end.',
                                               'event_end_datetime_local': 'Report event end timestamp '
                                                                           'expressed in local time.',
                                               'is_current_present': 'True when the series has a current '
                                                                     'version row.',
                                               'issued_datetime': 'Report issue timestamp.',
                                               'lifecycle_status': 'Latest-version lifecycle status.',
                                               'loaded_at': 'Maximum map_pathology_report.ADC_UPDT across '
                                                            'all contributing versions in the '
                                                            'report_series_id group, not only the version '
                                                            'selected for display. Accession aliases and '
                                                            'identity/link lookups supply no additional load '
                                                            'clock. This is ingestion provenance, not report '
                                                            'issue time or Silver refresh time; null when '
                                                            'all contributing clocks are null.',
                                               'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                    'stable cross-feed join key.',
                                               'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                               'record_status': 'Derived from lifecycle_status of the '
                                                                'selected map_pathology_report version: '
                                                                'retracted for cancelled or '
                                                                'entered_in_error, otherwise active, '
                                                                'including a null lifecycle status. '
                                                                'Selection is the grouped maximum full '
                                                                'version struct, beginning with '
                                                                'version_ordinal, report_version_id and '
                                                                'source_record_key. This label is not the '
                                                                'is_current_present flag and does not emit '
                                                                'superseded.',
                                               'record_status_effective_from': 'valid_from of the selected '
                                                                               'map_pathology_report '
                                                                               'version, carried unchanged '
                                                                               'as the report-series history '
                                                                               'start. The selected full '
                                                                               'version struct is ranked '
                                                                               'first by version_ordinal, '
                                                                               'report_version_id and '
                                                                               'source_record_key. This is '
                                                                               'not issued_datetime, a '
                                                                               'group-wide maximum validity '
                                                                               'timestamp or Silver refresh '
                                                                               'time; a missing value '
                                                                               'remains null.',
                                               'record_status_effective_to': 'Always NULL as a TIMESTAMP in '
                                                                             'the pathology-report-series '
                                                                             'projection, including '
                                                                             'retracted reports. No '
                                                                             'cancellation, '
                                                                             'successor-version or ingestion '
                                                                             'timestamp is substituted as a '
                                                                             'status end.',
                                               'report_code': 'Source and mapped report codings.',
                                               'report_role': 'Latest-version report role.',
                                               'report_section': 'Latest-version report section.',
                                               'report_text_hash': 'Bronze hash for the latest report '
                                                                   'version',
                                               'report_version_key': 'Deterministic report-version key.',
                                               'research_qi_only': 'Registry doctrine flag; true on 100% of '
                                                                   'rows today — describe-only',
                                               'source_code': 'Source test code.',
                                               'source_coding_system': 'Source test coding system.',
                                               'source_display': 'Source test display.',
                                               'source_feed': 'Registered owning feed.',
                                               'source_update_timestamp': 'valid_from of the selected '
                                                                          'map_pathology_report version, '
                                                                          'carried unchanged. This field is '
                                                                          'a selected-version validity '
                                                                          'timestamp, not an independent '
                                                                          'source modification clock, the '
                                                                          'group-wide maximum ADC_UPDT or '
                                                                          'Silver refresh time; a missing '
                                                                          'value remains null.',
                                               'specimen_key': 'Deterministic specimen key.',
                                               'subject_id_system': 'Identifier system used for subject_key.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'supersedes_report_version_key': 'Deterministic prior '
                                                                                'report-version key.',
                                               'version_count': 'Number of report versions in the series.',
                                               'version_ordinal': 'Latest version ordinal in the report '
                                                                  'series.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each pathology report record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'withheld_identity_ind': 'Withheld identity indicator.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_pathology_report: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_pathology_report': {'columns': {'accession_identifier': 'Alias-resolved canonical pathology '
                                                                   'accession identifier carried by the '
                                                                   'selected report-series version; not the '
                                                                   'source result-record key.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each '
                                                                   'pathology report record. It is produced '
                                                                   'by the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'discipline': 'Latest-version pathology discipline.',
                                           'document_key': 'Deterministic text-document key.',
                                           'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                           'encounter_link_method': 'How encounter_id was set '
                                                                    '(PATH_ENCOUNTER_V1). source_accession: '
                                                                    "the row's own Millennium source names "
                                                                    'the encounter. accession_single: every '
                                                                    'current Millennium source of the '
                                                                    'alias-resolved accession names this one '
                                                                    'encounter (how TFC LIMS rows inherit '
                                                                    'one once their accession is merged with '
                                                                    'the Millennium order). '
                                                                    'accession_multiple: those sources name '
                                                                    'different encounters, so encounter_id '
                                                                    'is NULL. none: the accession has no '
                                                                    'Millennium source, so encounter_id is '
                                                                    'NULL. Gold: encounter_id can still be '
                                                                    'NULL here when the encounter is not in '
                                                                    'the Gold encounter spine.',
                                           'event_datetime': 'Report issue or result timestamp.',
                                           'event_datetime_local': 'Report issue or result timestamp '
                                                                   'expressed in local time. Gold: NULL '
                                                                   'wherever Gold nulls event_datetime.',
                                           'event_end_datetime': 'Report event end.',
                                           'event_end_datetime_local': 'Report event end timestamp expressed '
                                                                       'in local time.',
                                           'is_current_present': 'True when the series has a current version '
                                                                 'row.',
                                           'issued_datetime': 'Report issue timestamp.',
                                           'lifecycle_status': 'Latest-version lifecycle status.',
                                           'loaded_at': 'Maximum map_pathology_report.ADC_UPDT across all '
                                                        'contributing versions in the report_series_id '
                                                        'group, not only the version selected for display. '
                                                        'Accession aliases and identity/link lookups supply '
                                                        'no additional load clock. This is ingestion '
                                                        'provenance, not report issue time or Silver refresh '
                                                        'time; null when all contributing clocks are null.',
                                           'patient_event_key': 'Deterministic SHA-256 event identity; '
                                                                'stable cross-feed join key.',
                                           'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                           'record_status': 'Derived from lifecycle_status of the selected '
                                                            'map_pathology_report version: retracted for '
                                                            'cancelled or entered_in_error, otherwise '
                                                            'active, including a null lifecycle status. '
                                                            'Selection is the grouped maximum full version '
                                                            'struct, beginning with version_ordinal, '
                                                            'report_version_id and source_record_key. This '
                                                            'label is not the is_current_present flag and '
                                                            'does not emit superseded.',
                                           'record_status_effective_from': 'valid_from of the selected '
                                                                           'map_pathology_report version, '
                                                                           'carried unchanged as the '
                                                                           'report-series history start. The '
                                                                           'selected full version struct is '
                                                                           'ranked first by version_ordinal, '
                                                                           'report_version_id and '
                                                                           'source_record_key. This is not '
                                                                           'issued_datetime, a group-wide '
                                                                           'maximum validity timestamp or '
                                                                           'Silver refresh time; a missing '
                                                                           'value remains null.',
                                           'record_status_effective_to': 'Always NULL as a TIMESTAMP in the '
                                                                         'pathology-report-series '
                                                                         'projection, including retracted '
                                                                         'reports. No cancellation, '
                                                                         'successor-version or ingestion '
                                                                         'timestamp is substituted as a '
                                                                         'status end.',
                                           'report_code': 'Source and mapped report codings.',
                                           'report_role': 'Latest-version report role.',
                                           'report_section': 'Latest-version report section.',
                                           'report_text_hash': 'Bronze hash for the latest report version',
                                           'report_version_key': 'Deterministic report-version key.',
                                           'research_qi_only': 'Registry doctrine flag; true on 100% of rows '
                                                               'today — describe-only',
                                           'source_code': 'Source test code.',
                                           'source_coding_system': 'Source test coding system.',
                                           'source_display': 'Source test display.',
                                           'source_feed': 'Registered owning feed.',
                                           'source_update_timestamp': 'valid_from of the selected '
                                                                      'map_pathology_report version, carried '
                                                                      'unchanged. This field is a '
                                                                      'selected-version validity timestamp, '
                                                                      'not an independent source '
                                                                      'modification clock, the group-wide '
                                                                      'maximum ADC_UPDT or Silver refresh '
                                                                      'time; a missing value remains null.',
                                           'specimen_key': 'Deterministic specimen key.',
                                           'subject_id_system': 'Identifier system used for subject_key.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'supersedes_report_version_key': 'Deterministic prior '
                                                                            'report-version key.',
                                           'version_count': 'Number of report versions in the series.',
                                           'version_ordinal': 'Latest version ordinal in the report series.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each pathology report record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Withheld identity indicator.'},
                               'quantity_override': False,
                               'table': 'One pathology report series keyed by report_series_id using the '
                                        'latest-version projection; per-version text lives in '
                                        'journey_text.document. Gold QC twin of the silver product: 8 '
                                        'columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) are '
                                        'advisory. Each rule states its reason in the pipeline notebook, and '
                                        'Lakeflow expectation metrics report what every rule matched on each '
                                        'update.'}}
