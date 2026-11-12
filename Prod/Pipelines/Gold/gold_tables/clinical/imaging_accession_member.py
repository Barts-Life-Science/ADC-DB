"""Gold clinical.imaging_accession_member: 2 dataset(s).

- datasets: _qc_clinical_imaging_accession_member, clinical_imaging_accession_member
- moved verbatim from: 2_gold_clinical_events:2751, 2_gold_clinical_events:2875, 2_gold_clinical_events:2885, 2_gold_clinical_events:2898

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _apply_person_scope, _n, _s3_axis_columns, _src, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.imaging_accession_member / imaging_report_link (PACS integration v1) ====

# Accession-level selection support. Members keep every accession-bearing examination, so the
# accession's identity status and counts stay honest; default selection is the member's
# default_selection_eligible_ind, decided once in silver. Exam concepts come from Gold
# clinical_imaging_exam, already mapped once in silver and gated by the Gold mapping policy
# here; they are NULL for examinations that table does not publish.
CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS = ['member_key',
 'accession_key',
 'sectra_accession_number',
 'patient_event_key',
 'source_object',
 'event_id',
 'pacs_examination_id',
 'linked_pacs_examination_id',
 'examination_key',
 'study_instance_uid',
 'subject_key',
 'subject_id_system',
 'person_id',
 'identity_status',
 'encounter_id',
 'accession_format',
 'accession_parse_method',
 'accession_rule_version',
 'accession_evidence_level',
 'pacs_link_method',
 'accession_person_count',
 'accession_person_status',
 'performed_datetime',
 'performed_datetime_source',
 'performed_evidence',
 'performed_evidence_verified_ind',
 'eligibility_as_of',
 'default_selection_eligible_ind',
 'eligibility_blocking_reasons',
 'source_coding_system',
 'source_code',
 'source_display',
 'modality_code',
 'body_site_code',
 'status_code',
 'source_status_code',
 'record_status',
 'report_available_ind',
 'report_link_status',
 'report_patient_event_key',
 'linked_report_event_id',
 'native_image_count',
 'native_series_count',
 'measured_series_count',
 'source_feed',
 'source_update_timestamp',
 'loaded_at']


CLINICAL_IMAGING_ACCESSION_MEMBER_ADVISORY_RULES = {
    # Counts members of accessions whose persons conflict. They are kept, never resolved by
    # choice, and blocked from default selection by eligibility_blocking_reasons.
    "gold.clinical.imaging_accession_member.accession_person_status.not_conflicting":
        "accession_person_status <> 'conflicting_persons'",
    # Counts members excluded from default selection for any reason.
    "gold.clinical.imaging_accession_member.default_selection_eligible":
        "default_selection_eligible_ind",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_imaging_accession_member"),
    refresh_policy="auto",
)
@dp.expect_all(CLINICAL_IMAGING_ACCESSION_MEMBER_ADVISORY_RULES)
def _gold_qc_clinical_imaging_accession_member():
    """Quality-controlled twin of journey_clinical.imaging_accession_member."""
    df = _apply_person_scope(spark.read.table(_src("clinical_imaging_accession_member")).select(
        *CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS
    ))  # INSR_W3 W3.02: gold person scope (C0 ruling 1); direct twin, filter form
    return df


@dp.materialized_view(
    name=_n("gold_clinical.imaging_accession_member"),
    table_properties={"quality": "gold"},
    cluster_by=["accession_key"],
    refresh_policy="auto",
)
def gold_clinical_imaging_accession_member():
    """Public accession members with Gold-gated exam concepts from clinical_imaging_exam."""
    axis = _s3_axis_columns("exam", False)
    concepts = spark.read.table(_n("gold_clinical.imaging_exam")).select(
        F.col("patient_event_key").alias("_concept_key"), *axis
    )
    members = spark.read.table(_n("gold_qc._clinical_imaging_accession_member"))
    at = CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS.index("source_display") + 1
    order = (CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[:at] + axis
             + CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[at:])
    df = (
        members.join(concepts, members.patient_event_key == concepts._concept_key, "left")
        .select(*order)
    )
    return df


def _public_steps_clinical_imaging_accession_member(df):
    """clinical_imaging_accession_member: the public body after its projection (GoldRules.public_select), moved from gold_clinical_imaging_accession_member; df is the projected twin frame."""
    axis = _s3_axis_columns('exam', False)
    concepts = spark.read.table(_n('gold_clinical.imaging_exam')).select(F.col('patient_event_key').alias('_concept_key'), *axis)
    members = df
    at = CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS.index('source_display') + 1
    order = CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[:at] + axis + CLINICAL_IMAGING_ACCESSION_MEMBER_COLUMNS[at:]
    df = members.join(concepts, members.patient_event_key == concepts._concept_key, 'left').select(*order)
    return df


RULES = {
    'clinical_imaging_accession_member': GoldRules(
        table='clinical_imaging_accession_member',
        advisory=CLINICAL_IMAGING_ACCESSION_MEMBER_ADVISORY_RULES,
        public_select={'all_but': []},
        public_steps=(_public_steps_clinical_imaging_accession_member,)),
}
_rules = QcBinding(RULES)


finalise()


COMMENTS = {'_qc_clinical_imaging_accession_member': {'columns': {'accession_evidence_level': 'pacs_request, '
                                                                                   'cerner_reference_pacs_linked '
                                                                                   'or '
                                                                                   'cerner_reference_parsed.',
                                                       'accession_format': 'Bronze accession-rule format '
                                                                           'class.',
                                                       'accession_key': 'upper(trim(sectra_accession_number)): '
                                                                        'the grouping key shared by PACS and '
                                                                        'Millennium members of one '
                                                                        'accession. Identifier.',
                                                       'accession_parse_method': 'Provenance of '
                                                                                 'sectra_accession_number.',
                                                       'accession_person_count': 'Distinct non-null '
                                                                                 'person_id across all '
                                                                                 'members of the accession '
                                                                                 '(both arms).',
                                                       'accession_person_status': 'single_person, '
                                                                                  'single_person_partial '
                                                                                  '(some members '
                                                                                  'unresolved), '
                                                                                  'conflicting_persons (more '
                                                                                  'than one person_id, or a '
                                                                                  'refused identity-conflict '
                                                                                  'link) or no_person. '
                                                                                  'Conflicts are never '
                                                                                  'resolved by choice.',
                                                       'accession_rule_version': 'Bronze accession-rule '
                                                                                 'version.',
                                                       'body_site_code': 'Observed body part (PACS '
                                                                         'BODY_PART).',
                                                       'default_selection_eligible_ind': 'True when '
                                                                                         'eligibility_blocking_reasons '
                                                                                         'is NULL. '
                                                                                         'Ineligible members '
                                                                                         'stay queryable '
                                                                                         'here.',
                                                       'eligibility_as_of': 'Selection as-of time: the '
                                                                            "member's own bronze load clock. "
                                                                            'A performed time after it is '
                                                                            'ineligible.',
                                                       'eligibility_blocking_reasons': 'Comma list, NULL '
                                                                                       'when eligible: '
                                                                                       'source_not_current, '
                                                                                       'no_valid_performed_time, '
                                                                                       'performed_after_as_of, '
                                                                                       'performed_evidence_unverified, '
                                                                                       'accession_identity_conflict, '
                                                                                       'unvalidated_cerner_accession. '
                                                                                       'Missing codes never '
                                                                                       'block.',
                                                       'encounter_id': 'Millennium ENCNTR_ID when supplied '
                                                                       '(millennium members).',
                                                       'event_id': 'Millennium EVENT_ID for millennium '
                                                                   'members.',
                                                       'examination_key': 'Physical-examination identity '
                                                                          'used for distinct counts: '
                                                                          'pacs:<PACS examination id> when '
                                                                          'resolved, otherwise '
                                                                          'mill:<EVENT_ID>. A Millennium '
                                                                          'event linked to a PACS '
                                                                          'examination counts once with it.',
                                                       'identity_status': 'Member identity resolution: '
                                                                          'resolved, provisional or '
                                                                          'unresolved.',
                                                       'linked_pacs_examination_id': 'PACS examination the '
                                                                                     'member resolves to '
                                                                                     '(own id for pacs; '
                                                                                     'unique bronze link for '
                                                                                     'millennium).',
                                                       'linked_report_event_id': 'Latest Millennium report '
                                                                                 'document EVENT_ID '
                                                                                 '(convenience pointer '
                                                                                 'only).',
                                                       'loaded_at': 'Bronze load clock inherited from '
                                                                    'clinical_imaging_exam.',
                                                       'measured_series_count': 'Measured series count; raw '
                                                                                'series feed clock ends '
                                                                                '2024-11-02.',
                                                       'member_key': 'Deterministic SHA-256 of the member '
                                                                     '(imaging_accession_member namespace + '
                                                                     'patient_event_key). One row per PACS '
                                                                     'examination or Millennium examination '
                                                                     'event that carries an accession.',
                                                       'modality_code': 'Observed modality: PACS MODALITY or '
                                                                        'Millennium NHSI_MODALITY_CATEGORY. '
                                                                        'Normalised concepts come from '
                                                                        "clinical_imaging_exam's mapped exam "
                                                                        'axis.',
                                                       'native_image_count': 'Native Sectra IMAGE_COUNT; '
                                                                             'PACS feed clock.',
                                                       'native_series_count': 'Native Sectra SERIES_COUNT.',
                                                       'pacs_examination_id': 'Native PACS_EXAMINATION_ID '
                                                                              'for pacs members.',
                                                       'pacs_link_method': 'NATIVE_PACS_EXAMINATION or the '
                                                                           'bronze Millennium PACS link '
                                                                           'method.',
                                                       'patient_event_key': 'patient_event_key of this '
                                                                            'member, minted exactly as '
                                                                            'clinical_imaging_exam mints it. '
                                                                            'Joins clinical_imaging_exam '
                                                                            'when the examination is '
                                                                            'admitted there; examinations '
                                                                            'without a usable code are '
                                                                            'retained here but not admitted '
                                                                            'there, so their exam_* concept '
                                                                            'columns are NULL.',
                                                       'performed_datetime': 'Valid performed time (future '
                                                                             'and sentinel values already '
                                                                             'NULL in bronze).',
                                                       'performed_datetime_source': 'Clean clock that '
                                                                                    'supplied '
                                                                                    'performed_datetime.',
                                                       'performed_evidence': 'Performed evidence set (see '
                                                                             'clinical_imaging_exam).',
                                                       'performed_evidence_verified_ind': 'Verified '
                                                                                          'performed '
                                                                                          'evidence; PACS '
                                                                                          'status 40 and '
                                                                                          'dangling report '
                                                                                          'ids are not '
                                                                                          'evidence.',
                                                       'person_id': 'Millennium PERSON_ID when resolved.',
                                                       'record_status': 'Silver record status inherited from '
                                                                        'clinical_imaging_exam (active, '
                                                                        'retracted or superseded).',
                                                       'report_available_ind': 'At least one resolved report '
                                                                               'linked to this member (see '
                                                                               'clinical_imaging_report_link '
                                                                               'for every link).',
                                                       'report_link_status': 'Source report-link status.',
                                                       'report_patient_event_key': 'Latest resolved PACS '
                                                                                   'report document key '
                                                                                   '(convenience pointer '
                                                                                   'only).',
                                                       'sectra_accession_number': 'Barts/Sectra extraction '
                                                                                  'accession as published by '
                                                                                  "the member's source. "
                                                                                  'Identifier.',
                                                       'source_code': 'Source examination code or display '
                                                                      'fallback; may be NULL (uncoded '
                                                                      'examinations are retained).',
                                                       'source_coding_system': 'Source examination coding '
                                                                               'system.',
                                                       'source_display': 'Source examination description.',
                                                       'source_feed': 'pacs_examination or radiology_event.',
                                                       'source_object': 'pacs (Sectra examination) or '
                                                                        'millennium (Cerner examination '
                                                                        'event, EVENT_CLASS_CD 234).',
                                                       'source_status_code': 'Native status code (PACS '
                                                                             'EXAMINATION_STATUS_CD or '
                                                                             'Millennium RESULT_STATUS_CD).',
                                                       'source_update_timestamp': 'Source update clock '
                                                                                  'inherited from '
                                                                                  'clinical_imaging_exam.',
                                                       'status_code': 'Silver imaging status inherited from '
                                                                      'clinical_imaging_exam.',
                                                       'study_instance_uid': 'DICOM StudyInstanceUID (image '
                                                                             'retrieval key; pacs members '
                                                                             'only). One accession can hold '
                                                                             'several.',
                                                       'subject_id_system': 'Identifier system used for '
                                                                            'subject_key.',
                                                       'subject_key': 'Deterministic subject key inherited '
                                                                      'from clinical_imaging_exam.'},
                                           'quantity_override': True,
                                           'table': 'Internal quality-controlled twin of '
                                                    'clinical_imaging_accession_member; the published Gold '
                                                    'MV reads this object.'},
 'clinical_imaging_accession_member': {'columns': {'accession_evidence_level': 'pacs_request, '
                                                                               'cerner_reference_pacs_linked '
                                                                               'or cerner_reference_parsed.',
                                                   'accession_format': 'Bronze accession-rule format class.',
                                                   'accession_key': 'upper(trim(sectra_accession_number)): '
                                                                    'the grouping key shared by PACS and '
                                                                    'Millennium members of one accession. '
                                                                    'Identifier.',
                                                   'accession_parse_method': 'Provenance of '
                                                                             'sectra_accession_number.',
                                                   'accession_person_count': 'Distinct non-null person_id '
                                                                             'across all members of the '
                                                                             'accession (both arms).',
                                                   'accession_person_status': 'single_person, '
                                                                              'single_person_partial (some '
                                                                              'members unresolved), '
                                                                              'conflicting_persons (more '
                                                                              'than one person_id, or a '
                                                                              'refused identity-conflict '
                                                                              'link) or no_person. Conflicts '
                                                                              'are never resolved by choice.',
                                                   'accession_rule_version': 'Bronze accession-rule version.',
                                                   'body_site_code': 'Observed body part (PACS BODY_PART).',
                                                   'default_selection_eligible_ind': 'True when '
                                                                                     'eligibility_blocking_reasons '
                                                                                     'is NULL. Ineligible '
                                                                                     'members stay queryable '
                                                                                     'here.',
                                                   'eligibility_as_of': "Selection as-of time: the member's "
                                                                        'own bronze load clock. A performed '
                                                                        'time after it is ineligible.',
                                                   'eligibility_blocking_reasons': 'Comma list, NULL when '
                                                                                   'eligible: '
                                                                                   'source_not_current, '
                                                                                   'no_valid_performed_time, '
                                                                                   'performed_after_as_of, '
                                                                                   'performed_evidence_unverified, '
                                                                                   'accession_identity_conflict, '
                                                                                   'unvalidated_cerner_accession. '
                                                                                   'Missing codes never '
                                                                                   'block.',
                                                   'encounter_id': 'Millennium ENCNTR_ID when supplied '
                                                                   '(millennium members).',
                                                   'event_id': 'Millennium EVENT_ID for millennium members.',
                                                   'examination_key': 'Physical-examination identity used '
                                                                      'for distinct counts: pacs:<PACS '
                                                                      'examination id> when resolved, '
                                                                      'otherwise mill:<EVENT_ID>. A '
                                                                      'Millennium event linked to a PACS '
                                                                      'examination counts once with it.',
                                                   'identity_status': 'Member identity resolution: resolved, '
                                                                      'provisional or unresolved.',
                                                   'linked_pacs_examination_id': 'PACS examination the '
                                                                                 'member resolves to (own id '
                                                                                 'for pacs; unique bronze '
                                                                                 'link for millennium).',
                                                   'linked_report_event_id': 'Latest Millennium report '
                                                                             'document EVENT_ID (convenience '
                                                                             'pointer only).',
                                                   'loaded_at': 'Bronze load clock inherited from '
                                                                'clinical_imaging_exam.',
                                                   'measured_series_count': 'Measured series count; raw '
                                                                            'series feed clock ends '
                                                                            '2024-11-02.',
                                                   'member_key': 'Deterministic SHA-256 of the member '
                                                                 '(imaging_accession_member namespace + '
                                                                 'patient_event_key). One row per PACS '
                                                                 'examination or Millennium examination '
                                                                 'event that carries an accession.',
                                                   'modality_code': 'Observed modality: PACS MODALITY or '
                                                                    'Millennium NHSI_MODALITY_CATEGORY. '
                                                                    'Normalised concepts come from '
                                                                    "clinical_imaging_exam's mapped exam "
                                                                    'axis.',
                                                   'native_image_count': 'Native Sectra IMAGE_COUNT; PACS '
                                                                         'feed clock.',
                                                   'native_series_count': 'Native Sectra SERIES_COUNT.',
                                                   'pacs_examination_id': 'Native PACS_EXAMINATION_ID for '
                                                                          'pacs members.',
                                                   'pacs_link_method': 'NATIVE_PACS_EXAMINATION or the '
                                                                       'bronze Millennium PACS link method.',
                                                   'patient_event_key': 'patient_event_key of this member, '
                                                                        'minted exactly as '
                                                                        'clinical_imaging_exam mints it. '
                                                                        'Joins clinical_imaging_exam when '
                                                                        'the examination is admitted there; '
                                                                        'examinations without a usable code '
                                                                        'are retained here but not admitted '
                                                                        'there, so their exam_* concept '
                                                                        'columns are NULL.',
                                                   'performed_datetime': 'Valid performed time (future and '
                                                                         'sentinel values already NULL in '
                                                                         'bronze).',
                                                   'performed_datetime_source': 'Clean clock that supplied '
                                                                                'performed_datetime.',
                                                   'performed_evidence': 'Performed evidence set (see '
                                                                         'clinical_imaging_exam).',
                                                   'performed_evidence_verified_ind': 'Verified performed '
                                                                                      'evidence; PACS status '
                                                                                      '40 and dangling '
                                                                                      'report ids are not '
                                                                                      'evidence.',
                                                   'person_id': 'Millennium PERSON_ID when resolved.',
                                                   'record_status': 'Silver record status inherited from '
                                                                    'clinical_imaging_exam (active, '
                                                                    'retracted or superseded).',
                                                   'report_available_ind': 'At least one resolved report '
                                                                           'linked to this member (see '
                                                                           'clinical_imaging_report_link for '
                                                                           'every link).',
                                                   'report_link_status': 'Source report-link status.',
                                                   'report_patient_event_key': 'Latest resolved PACS report '
                                                                               'document key (convenience '
                                                                               'pointer only).',
                                                   'sectra_accession_number': 'Barts/Sectra extraction '
                                                                              'accession as published by the '
                                                                              "member's source. Identifier.",
                                                   'source_code': 'Source examination code or display '
                                                                  'fallback; may be NULL (uncoded '
                                                                  'examinations are retained).',
                                                   'source_coding_system': 'Source examination coding '
                                                                           'system.',
                                                   'source_display': 'Source examination description.',
                                                   'source_feed': 'pacs_examination or radiology_event.',
                                                   'source_object': 'pacs (Sectra examination) or millennium '
                                                                    '(Cerner examination event, '
                                                                    'EVENT_CLASS_CD 234).',
                                                   'source_status_code': 'Native status code (PACS '
                                                                         'EXAMINATION_STATUS_CD or '
                                                                         'Millennium RESULT_STATUS_CD).',
                                                   'source_update_timestamp': 'Source update clock inherited '
                                                                              'from clinical_imaging_exam.',
                                                   'status_code': 'Silver imaging status inherited from '
                                                                  'clinical_imaging_exam.',
                                                   'study_instance_uid': 'DICOM StudyInstanceUID (image '
                                                                         'retrieval key; pacs members only). '
                                                                         'One accession can hold several.',
                                                   'subject_id_system': 'Identifier system used for '
                                                                        'subject_key.',
                                                   'subject_key': 'Deterministic subject key inherited from '
                                                                  'clinical_imaging_exam.'},
                                       'quantity_override': False,
                                       'table': 'One row per accession-bearing PACS examination or '
                                                'Millennium examination event, with default-selection '
                                                'eligibility and blocking reasons. Select accessions by '
                                                'filtering members, then take distinct accession_key; '
                                                'clinical_v_imaging_accession summarises eligible members '
                                                'per accession.'}}
