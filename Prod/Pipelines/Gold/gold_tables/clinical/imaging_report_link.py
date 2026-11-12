"""Gold clinical.imaging_report_link: 2 dataset(s).

- datasets: _qc_clinical_imaging_report_link, clinical_imaging_report_link
- moved verbatim from: 2_gold_clinical_events:2926, 2_gold_clinical_events:3007, 2_gold_clinical_events:3017, 2_gold_clinical_events:3030

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _apply_person_scope, _n, _src, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


CLINICAL_IMAGING_REPORT_LINK_COLUMNS = ['link_key',
 'link_scope',
 'link_method',
 'accession_key',
 'sectra_accession_number',
 'member_patient_event_key',
 'document_patient_event_key',
 'document_source_feed',
 'pacs_report_id',
 'cerner_document_event_id',
 'document_version_id',
 'document_version_count',
 'document_version_decision',
 'report_resolved_ind',
 'report_datetime',
 'report_status_code',
 'text_source',
 'text_sha256',
 'text_integrity_status',
 'report_text_available_ind',
 'approved_anonymised_text_available_ind',
 'report_text_anonymised',
 'anonymisation_redactor_version',
 'anonymised_at',
 'person_id',
 'record_status',
 'loaded_at']


CLINICAL_IMAGING_REPORT_LINK_ADVISORY_RULES = {
    # Counts PACS report ids with no report row. They are kept as evidence and never make a
    # report available.
    "gold.clinical.imaging_report_link.report_resolved": "report_resolved_ind",
    # Counts Cerner report events whose current blob versions disagree on text; no version
    # is certified for them.
    "gold.clinical.imaging_report_link.document_version_not_ambiguous":
        "COALESCE(document_version_decision, '') <> 'text_version_ambiguous'",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_report_link"),
    refresh_policy="auto",
)
@dp.expect_all(CLINICAL_IMAGING_REPORT_LINK_ADVISORY_RULES)
def _gold_qc_clinical_imaging_report_link():
    """Quality-controlled twin of journey_clinical.imaging_report_link."""
    df = _apply_person_scope(spark.read.table(_src("clinical_imaging_report_link")).select(
        *CLINICAL_IMAGING_REPORT_LINK_COLUMNS
    ))  # INSR_W3 W3.02: gold person scope (C0 ruling 1); direct twin, filter form
    return df


@dp.materialized_view(
    name=_n("gold_clinical.imaging_report_link"),
    table_properties={"quality": "gold"},
    cluster_by=["accession_key"],
    refresh_policy="auto",
)
def gold_clinical_imaging_report_link():
    """Public imaging report links."""
    df = spark.read.table(_n("gold_qc._clinical_imaging_report_link"))
    return df


finalise()


COMMENTS = {'_qc_clinical_imaging_report_link': {'columns': {'accession_key': 'upper(trim(sectra_accession_number)); '
                                                                   'joins clinical_imaging_accession_member. '
                                                                   'Identifier.',
                                                  'anonymisation_redactor_version': 'anon_redactor_version '
                                                                                    'of the lane row that '
                                                                                    'produced '
                                                                                    'report_text_anonymised; '
                                                                                    'NULL when '
                                                                                    'report_text_anonymised '
                                                                                    'is NULL.',
                                                  'anonymised_at': 'anon_processed_at of the lane row that '
                                                                   'produced report_text_anonymised; NULL '
                                                                   'when report_text_anonymised is NULL.',
                                                  'approved_anonymised_text_available_ind': 'The approved '
                                                                                            'anonymisation '
                                                                                            'lane holds '
                                                                                            'output for the '
                                                                                            'current text '
                                                                                            '(anon_status '
                                                                                            'anonymized and '
                                                                                            'matching source '
                                                                                            'hash). False '
                                                                                            'when missing or '
                                                                                            'stale; never '
                                                                                            'inferred from '
                                                                                            'raw text.',
                                                  'cerner_document_event_id': 'Cerner report document '
                                                                              'EVENT_ID: the REF_EXAM_KEY '
                                                                              'sibling for Cerner links, or '
                                                                              'the bridge EVENT_ID supplying '
                                                                              "a PACS report's text.",
                                                  'document_patient_event_key': 'text_document '
                                                                                'patient_event_key of the '
                                                                                'linked document version; '
                                                                                'NULL for a dangling PACS '
                                                                                'report id.',
                                                  'document_source_feed': 'pacs_report or mill_blob_text.',
                                                  'document_version_count': 'Cerner links: current (active) '
                                                                            'silver document versions for '
                                                                            'the event. 1 for PACS links.',
                                                  'document_version_decision': 'single_current, '
                                                                               'identical_text_collapsed '
                                                                               '(several current versions, '
                                                                               'one text), '
                                                                               'text_version_ambiguous '
                                                                               '(conflicting current texts: '
                                                                               'no text is certified) or '
                                                                               'no_current_version. PACS '
                                                                               'native reports: '
                                                                               'native_report.',
                                                  'document_version_id': 'Silver document version_id of the '
                                                                         'linked version.',
                                                  'link_key': 'Deterministic SHA-256 over scope, '
                                                              'member/accession and document identity.',
                                                  'link_method': 'PACS_EXAMINATION_REPORT, '
                                                                 'PACS_REQUEST_REPORT or '
                                                                 'CERNER_REF_EXAM_KEY.',
                                                  'link_scope': 'examination (Sectra report attached to one '
                                                                'examination), direct_cerner_event (Cerner '
                                                                "report document sharing the examination's "
                                                                'REF_EXAM_KEY), or accession (Sectra report '
                                                                'attached to the request only; '
                                                                'member_patient_event_key is NULL).',
                                                  'loaded_at': 'Bronze load clock of the link source row.',
                                                  'member_patient_event_key': 'clinical_imaging_exam / '
                                                                              'accession member '
                                                                              'patient_event_key; NULL for '
                                                                              'accession-scope links.',
                                                  'pacs_report_id': 'Sectra ReportId (PACS links). May be '
                                                                    'dangling (report_resolved_ind false).',
                                                  'person_id': 'Report/document person when resolved.',
                                                  'record_status': 'Link record status: active, or the '
                                                                   'linked document/report record status '
                                                                   'when that is not active.',
                                                  'report_datetime': 'Report time (Sectra REPORT_DT_TM; '
                                                                     'Cerner document event_datetime).',
                                                  'report_resolved_ind': 'True when the report resolves to a '
                                                                         'real report/document row. A '
                                                                         'dangling id never makes a report '
                                                                         'available.',
                                                  'report_status_code': 'Source report status code.',
                                                  'report_text_anonymised': 'Approved-lane anonymised report '
                                                                            'text for this link: '
                                                                            'map_pacs_report.anon_report_text '
                                                                            '(pacs_native), '
                                                                            'map_pacs_report_text_bridge.anon_bridged_text '
                                                                            '(mill_blob_text_bridge) or '
                                                                            'mill_blob_text.anon_text for '
                                                                            'the linked version '
                                                                            '(mill_blob_text). Set exactly '
                                                                            'when '
                                                                            'approved_anonymised_text_available_ind '
                                                                            'is true; never falls back to '
                                                                            'raw text.',
                                                  'report_text_available_ind': 'Non-empty report text is '
                                                                               'available for this link and '
                                                                               'not version-ambiguous.',
                                                  'sectra_accession_number': 'Accession of the linked '
                                                                             'examination or request. '
                                                                             'Identifier.',
                                                  'text_integrity_status': 'Bridge v4 TEXT_INTEGRITY_STATUS '
                                                                           'for bridged PACS text; NULL '
                                                                           'otherwise.',
                                                  'text_sha256': 'SHA-256 of the linked document text '
                                                                 '(silver text_document.text_sha256).',
                                                  'text_source': 'pacs_native, mill_blob_text_bridge (bridge '
                                                                 'v4, version-safe), mill_blob_text, or NULL '
                                                                 'when no text.'},
                                      'quantity_override': True,
                                      'table': 'Internal quality-controlled twin of '
                                               'clinical_imaging_report_link; the published Gold MV reads '
                                               'this object.'},
 'clinical_imaging_report_link': {'columns': {'accession_key': 'upper(trim(sectra_accession_number)); joins '
                                                               'clinical_imaging_accession_member. '
                                                               'Identifier.',
                                              'anonymisation_redactor_version': 'anon_redactor_version of '
                                                                                'the lane row that produced '
                                                                                'report_text_anonymised; '
                                                                                'NULL when '
                                                                                'report_text_anonymised is '
                                                                                'NULL. Redactor version that '
                                                                                'produced '
                                                                                'report_text_anonymised. '
                                                                                'Output of the legacy '
                                                                                'redactor v2 '
                                                                                '(v2-prod-legacy) is '
                                                                                'accepted for TRE delivery '
                                                                                'alongside v3.2 and v3.3 '
                                                                                '(Ben, J16).',
                                              'anonymised_at': 'anon_processed_at of the lane row that '
                                                               'produced report_text_anonymised; NULL when '
                                                               'report_text_anonymised is NULL.',
                                              'approved_anonymised_text_available_ind': 'The approved '
                                                                                        'anonymisation lane '
                                                                                        'holds output for '
                                                                                        'the current text '
                                                                                        '(anon_status '
                                                                                        'anonymized and '
                                                                                        'matching source '
                                                                                        'hash). False when '
                                                                                        'missing or stale; '
                                                                                        'never inferred from '
                                                                                        'raw text.',
                                              'cerner_document_event_id': 'Cerner report document EVENT_ID: '
                                                                          'the REF_EXAM_KEY sibling for '
                                                                          'Cerner links, or the bridge '
                                                                          'EVENT_ID supplying a PACS '
                                                                          "report's text.",
                                              'document_patient_event_key': 'text_document patient_event_key '
                                                                            'of the linked document version; '
                                                                            'NULL for a dangling PACS report '
                                                                            'id.',
                                              'document_source_feed': 'pacs_report or mill_blob_text.',
                                              'document_version_count': 'Cerner links: current (active) '
                                                                        'silver document versions for the '
                                                                        'event. 1 for PACS links.',
                                              'document_version_decision': 'single_current, '
                                                                           'identical_text_collapsed '
                                                                           '(several current versions, one '
                                                                           'text), text_version_ambiguous '
                                                                           '(conflicting current texts: no '
                                                                           'text is certified) or '
                                                                           'no_current_version. PACS native '
                                                                           'reports: native_report.',
                                              'document_version_id': 'Silver document version_id of the '
                                                                     'linked version.',
                                              'link_key': 'Deterministic SHA-256 over scope, '
                                                          'member/accession and document identity.',
                                              'link_method': 'PACS_EXAMINATION_REPORT, PACS_REQUEST_REPORT '
                                                             'or CERNER_REF_EXAM_KEY.',
                                              'link_scope': 'examination (Sectra report attached to one '
                                                            'examination), direct_cerner_event (Cerner '
                                                            "report document sharing the examination's "
                                                            'REF_EXAM_KEY), or accession (Sectra report '
                                                            'attached to the request only; '
                                                            'member_patient_event_key is NULL).',
                                              'loaded_at': 'Bronze load clock of the link source row.',
                                              'member_patient_event_key': 'clinical_imaging_exam / accession '
                                                                          'member patient_event_key; NULL '
                                                                          'for accession-scope links.',
                                              'pacs_report_id': 'Sectra ReportId (PACS links). May be '
                                                                'dangling (report_resolved_ind false).',
                                              'person_id': 'Report/document person when resolved.',
                                              'record_status': 'Link record status: active, or the linked '
                                                               'document/report record status when that is '
                                                               'not active.',
                                              'report_datetime': 'Report time (Sectra REPORT_DT_TM; Cerner '
                                                                 'document event_datetime).',
                                              'report_resolved_ind': 'True when the report resolves to a '
                                                                     'real report/document row. A dangling '
                                                                     'id never makes a report available.',
                                              'report_status_code': 'Source report status code.',
                                              'report_text_anonymised': 'Approved-lane anonymised report '
                                                                        'text for this link: '
                                                                        'map_pacs_report.anon_report_text '
                                                                        '(pacs_native), '
                                                                        'map_pacs_report_text_bridge.anon_bridged_text '
                                                                        '(mill_blob_text_bridge) or '
                                                                        'mill_blob_text.anon_text for the '
                                                                        'linked version (mill_blob_text). '
                                                                        'Set exactly when '
                                                                        'approved_anonymised_text_available_ind '
                                                                        'is true; never falls back to raw '
                                                                        'text. Approved anonymised report '
                                                                        'text; see '
                                                                        'anonymisation_redactor_version for '
                                                                        'the redactor that produced it (v2 '
                                                                        'legacy output accepted, J16).',
                                              'report_text_available_ind': 'Non-empty report text is '
                                                                           'available for this link and not '
                                                                           'version-ambiguous.',
                                              'sectra_accession_number': 'Accession of the linked '
                                                                         'examination or request. '
                                                                         'Identifier.',
                                              'text_integrity_status': 'Bridge v4 TEXT_INTEGRITY_STATUS for '
                                                                       'bridged PACS text; NULL otherwise.',
                                              'text_sha256': 'SHA-256 of the linked document text (silver '
                                                             'text_document.text_sha256).',
                                              'text_source': 'pacs_native, mill_blob_text_bridge (bridge v4, '
                                                             'version-safe), mill_blob_text, or NULL when no '
                                                             'text.'},
                                  'quantity_override': False,
                                  'table': 'Relational imaging report links: examination-, request- and '
                                           'Cerner-event-scoped report documents with version decisions, '
                                           'text availability and approved-anonymisation availability. '
                                           'Multiple reports and addenda stay separate rows.'}}
