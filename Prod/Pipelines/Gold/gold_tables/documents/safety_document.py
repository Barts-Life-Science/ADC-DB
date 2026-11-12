"""Gold documents.safety_document: 2 dataset(s).

- datasets: text_safety_document, _gold_qc_text_safety_document
- moved verbatim from: 5_gold_documents:467, 5_gold_documents:471, 5_gold_documents:481, 5_gold_documents:496

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import INTERNAL_SCHEMA, PUBLIC_SCHEMA, _released
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== text_safety_document ====
TEXT_SAFETY_DOCUMENT_MANDATORY_RULES = {
    # Exclude inactive history, exactly as the previous source filter did.
    "gold.text_safety_document.active_record": "COALESCE(record_status = 'active', FALSE)",
}


TEXT_SAFETY_DOCUMENT_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.text.safety_document.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Report missing source keys without inventing a new exclusion policy.
    "gold.text_safety_document.key_present": "safety_document_key IS NOT NULL",
    # A missing source load time is provenance uncertainty, not grounds to lose the row.
    "gold.text_safety_document.load_time_present": "loaded_at IS NOT NULL",
}


@dp.materialized_view(
    name=f"{PUBLIC_SCHEMA}.text_safety_document",
    refresh_policy="auto",
)
@dp.expect_all_or_drop(TEXT_SAFETY_DOCUMENT_MANDATORY_RULES)
@dp.expect_all(TEXT_SAFETY_DOCUMENT_ADVISORY_RULES)
def gold_text_safety_document():
    """Publish text_safety_document; native rules count exclusions and advisory failures."""
    return _tdx_product("text_safety_document")


@dp.materialized_view(
    name=f"{INTERNAL_SCHEMA}._gold_qc_text_safety_document",
    refresh_policy="auto",
)
def _gold_qc_text_safety_document():
    """Keep the existing admitted-row evidence schema; passed is not an advisory all-clear."""
    return _tdx_qc("text_safety_document", "safety_document_key")


def _tdx_finish_text_safety_document(df):
    """text_safety_document: per-row finish step, moved verbatim from _tdx_product (gold_journey_shared.py:1247)."""
    released = _released(df) & F.col("text_is_anonymised")
    df = df.withColumn("source_text_sha256", F.col("text_sha256"))
    df = df.withColumn("document_text", F.when(released, F.col("document_text_anonymised")))
    # GOLD_COLUMN_PARITY_V1: the source narrative and silver's anonymised field are published too (Ben,
    # 2026-10-17: nothing is withheld from gold except QC failures); document_text stays the released
    # anonymised text, and text_sha256/text_length/text_is_anonymised describe document_text.
    df = df.withColumn("text_sha256", F.sha2("document_text", 256))
    df = df.withColumn("text_length", F.length("document_text"))
    df = df.withColumn("text_is_anonymised", released)
    return df


RULES = {
    'text_safety_document': GoldRules(
        table='text_safety_document',
        mandatory=TEXT_SAFETY_DOCUMENT_MANDATORY_RULES,
        advisory=TEXT_SAFETY_DOCUMENT_ADVISORY_RULES,
        tdx_finish=(_tdx_finish_text_safety_document,)),
}
_rules = QcBinding(RULES)
_tdx_product = _rules._tdx_product
_tdx_qc = _rules._tdx_qc


finalise()


COMMENTS = {'_gold_qc_text_safety_document': {'columns': {'qc_checked_at': 'loaded_at of the admitted row (the QC '
                                                                'evidence time).',
                                               'qc_status': 'passed for every admitted row; advisory '
                                                            'outcomes are in Lakeflow expectation metrics.',
                                               'safety_document_key': 'Key of the admitted '
                                                                      'text_safety_document row.'},
                                   'quantity_override': False,
                                   'table': 'Compatibility evidence of admitted text_safety_document rows; '
                                            'advisory outcomes are in Lakeflow expectation metrics.'},
 'text_safety_document': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                      'affected_patient_count': 'Number of patients affected by the '
                                                                'associated safety incident.',
                                      'anon_context_fingerprint': 'Hash of the contextual inputs the '
                                                                  'redactor used for this row; the '
                                                                  'anonymised text is released only while it '
                                                                  'matches the current context.',
                                      'anon_identity_fingerprint': 'Hash of the patient identity details the '
                                                                   'record-driven redactor used for this '
                                                                   'row.',
                                      'anon_input_digest': 'SHA-256 digest of the fragment text as presented '
                                                           'to the redactor; a changed digest marks the row '
                                                           'for re-anonymisation.',
                                      'anon_processed_at': "When the redactor last processed this row's "
                                                           'text.',
                                      'anon_redaction_count': 'Number of redactions the redactor applied to '
                                                              'the source text.',
                                      'anon_redactor_version': 'Version of the redactor that produced the '
                                                               'anonymised text.',
                                      'anon_source_text_sha': 'SHA-256 digest of the text the redactor '
                                                              'processed; when it no longer matches the '
                                                              'current input digest the anonymised text is '
                                                              'stale.',
                                      'anon_status': "Outcome of the anonymisation run for this row's free "
                                                     'text (anonymized when it succeeded).',
                                      'anon_text': 'Anonymised version of text: identifiers detected in the '
                                                   'free text are replaced by placeholders; published '
                                                   'alongside the source column, which gold releases only '
                                                   'when anonymisation succeeded.',
                                      'context_fingerprint_current': "SHA-256 of the incident's current "
                                                                     'identifying context (names, birth '
                                                                     'date, postcode, reporter, investigator '
                                                                     'and participants); anonymised '
                                                                     'narrative text is released only while '
                                                                     'its anon_context_fingerprint matches '
                                                                     'this value.',
                                      'document_text': 'Released anonymised incident narrative: the '
                                                       'anonymised fragment when redactor v3.3 anonymisation '
                                                       'succeeded and its input digest and incident context '
                                                       'still match; NULL otherwise. The source narrative is '
                                                       'in text.',
                                      'document_text_anonymised': 'Anonymised free-text content of the '
                                                                  'safety incident document assembled from '
                                                                  'text fragments.',
                                      'document_type_code': 'Code identifying the type of safety document, '
                                                            'derived from the fragment role.',
                                      'document_type_display': 'Display label for the type of safety '
                                                               'document, derived from the fragment role.',
                                      'field_name': 'Name of the source field from which the safety text '
                                                    'fragment was taken.',
                                      'fragment_id': 'Identifier of the safety text fragment, carried from '
                                                     'the bronze safety text fragment mapping and derived '
                                                     'from Datix action or incident record identifiers.',
                                      'fragment_role': 'Role or category of the safety text fragment within '
                                                       'the incident documentation.',
                                      'identity_status': 'resolved when person_id is present, unresolved '
                                                         'when absent, not_applicable for tables without a '
                                                         'patient.',
                                      'inc_id': 'Identifier of the Datix incident record that this LFPSE row '
                                                'relates to.',
                                      'inc_ourref': 'Internal organisational reference number of the patient '
                                                    'safety incident to which the text document relates, '
                                                    'carried through from the bronze incident mappings that '
                                                    'link text fragments to incidents.',
                                      'incident_dt_tm': 'Date and time at which the patient safety incident '
                                                        'occurred, carried through from the source incident '
                                                        'record via the bronze incident mappings.',
                                      'incident_id': 'Identifier of the Datix incident to which the safety '
                                                     'text fragment relates, carried from the bronze safety '
                                                     'text fragment mapping and derived from the Datix '
                                                     'incident record identifier.',
                                      'incident_person_id': 'Internal person identifier for the patient '
                                                            'associated with the safety incident.',
                                      'load_batch_id': 'Load batch label derived from loaded_at '
                                                       '(yyyyMMddHHmmss); tdx-v21 when no load time exists.',
                                      'loaded_at': 'When the row was last written to bronze (ADC_UPDT), else '
                                                   'its source update time; the stable load clock.',
                                      'person_id': 'Millennium PERSON_ID of the patient (identifier); NULL '
                                                   'when the source row names no patient.',
                                      'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                      'priority_basis': 'Basis or rationale used to assign priority to the '
                                                        'safety text document.',
                                      'priority_dt_tm': 'Timestamp used to prioritise or order the safety '
                                                        'text fragment, taken from the fragment mapping and '
                                                        'derived from Datix action update date fields.',
                                      'record_status': 'active, or superseded when the row stopped appearing '
                                                       'in the source snapshot (source_present_ind false).',
                                      'record_status_effective_from': 'Start of the record status; not '
                                                                      'supplied by this source (always '
                                                                      'NULL).',
                                      'record_status_effective_to': 'When a superseded row was first '
                                                                    'detected absent from the source '
                                                                    '(source_absent_detected_ts); NULL while '
                                                                    'active.',
                                      'row_hash': "SHA-256 of the row's payload columns, used by the bronze "
                                                  'MERGE to detect changes.',
                                      'safety_document_key': 'Deterministic SHA-256 key of the safety '
                                                             'document row (namespace plus native source '
                                                             'key).',
                                      'source_absent_detected_ts': 'When bronze first found the row absent '
                                                                   'from the source snapshot; NULL while '
                                                                   'present.',
                                      'source_adc_updt': 'When the source row was landed in raw (raw '
                                                         'ADC_UPDT).',
                                      'source_object': 'Source object from which the safety text fragment '
                                                       'was derived, based on the source table name.',
                                      'source_object_table': 'Name of the source table from which the safety '
                                                             'text fragment originated.',
                                      'source_present_ind': 'True while the row is present in the latest '
                                                            'full source snapshot.',
                                      'source_record_id': 'Record identifier of the originating Datix source '
                                                          'record, either an action or an incident, from '
                                                          'which the text fragment was taken.',
                                      'source_row_id': 'Native source row identifier, as text.',
                                      'source_table': 'Bronze or raw source table the row came from.',
                                      'source_text_sha256': 'SHA-256 of the source narrative before '
                                                            'anonymisation (provenance of document_text).',
                                      'source_update_timestamp': 'Time the source row was landed in the raw '
                                                                 'layer (bronze SOURCE_ADC_UPDT), not when '
                                                                 'it changed in the source system; do not '
                                                                 'use it for recency.',
                                      'source_updt_dt_tm': 'Last-update time of the row in the source system '
                                                           '(for Datix, updateddate); NULL when the source '
                                                           'supplies none.',
                                      'subject_id_system': 'System of subject_key: urn:cerner:person_id, or '
                                                           'nosubject when the row names no patient.',
                                      'subject_key': 'Deterministic subject key: hash of the Cerner person '
                                                     'id, or of the source row when no person is recorded.',
                                      'text': 'Free-text content of the safety fragment, drawn from Datix '
                                              'action description and action monitoring narrative fields.',
                                      'text_is_anonymised': 'True when document_text is the released '
                                                            'anonymised text (anonymisation succeeded and '
                                                            'its input digest still matches).',
                                      'text_length': 'Character length of document_text as published.',
                                      'text_sha256': 'SHA-256 of document_text as published (the released '
                                                     'anonymised text).'},
                          'quantity_override': False,
                          'table': 'One row per Datix incident narrative fragment (description, action, '
                                   'outcome and similar free text), with the released anonymised text. Gold '
                                   'QC twin of silver text_safety_document: mandatory rules drop rows, '
                                   'advisory rules are reported in Lakeflow expectation metrics.'}}
