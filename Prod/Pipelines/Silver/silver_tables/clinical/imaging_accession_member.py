"""Silver clinical.imaging_accession_member: 1 dataset(s).

- datasets: clinical_imaging_accession_member
- moved verbatim from: 2_silver_clinical_events:6929, 2_silver_clinical_events:6931, 2_silver_clinical_events:6939, silver_journey_shared.py:10013, silver_journey_shared.py:10026, silver_journey_shared.py:9651

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, _present, axis_columns, spark, stable_id
from silver_tables._common.clinical_events import _at
from silver_tables._common.journey import (
    IMAGING_ACCESSION_MEMBER_COLUMNS,
    IMAGING_INTEGRATION_COLUMNS,
    MILL_RADIOLOGY_EXAM_CLASS_CD,
    _imaging_exam_canonical_pregate,
    _mill_radiology_exam_canonical_pregate,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Accession formats that are not a validated Cerner parse (Cerner-only candidates only).
UNVALIDATED_ACCESSION_FORMATS = ("UNRECOGNISED", "OTHER")


def _imaging_member_source(pregate_df):
    # Project one arm of the integrated imaging pregate onto the membership shape.
    keep = [
        "patient_event_key", "source_object", "event_id", "pacs_examination_id",
        "study_instance_uid", "subject_key", "subject_id_system", "person_id",
        "identity_status", "encounter_id", "source_coding_system", "source_code",
        "source_display", "modality_code", "body_site_code", "status_code", "record_status",
        "report_patient_event_key", "source_feed", "source_update_timestamp", "loaded_at",
        *IMAGING_INTEGRATION_COLUMNS,
    ]
    return pregate_df.select(*keep)


def _imaging_accession_member_canonical():
    """One row per accession-bearing imaging examination member, both arms.

    Reads the integrated pregates, so examinations without a usable code are retained.
    Millennium members are examination events only (EVENT_CLASS_CD 234); report documents
    and sections are relational report links, not members.
    """
    pacs = _imaging_member_source(_imaging_exam_canonical_pregate(integration=True))
    mill = _imaging_member_source(
        _mill_radiology_exam_canonical_pregate(integration=True)
        .where(F.col("source_event_class_code") == F.lit(str(MILL_RADIOLOGY_EXAM_CLASS_CD)))
    )
    members = (
        pacs.unionByName(mill)
        .where(_present(F.col("sectra_accession_number")))
        .withColumn("accession_key", F.upper(F.trim(F.col("sectra_accession_number"))))
    )
    # Cross-arm accession identity: an incrementalizable aggregate joined back, not a window.
    accession_identity = members.groupBy("accession_key").agg(
        F.countDistinct("person_id").alias("accession_person_count"),
        F.max(F.col("person_id").isNull().cast("int")).alias("_any_unresolved"),
        F.max((F.col("accession_identity_status") == F.lit("CONFLICTING_PERSONS"))
              .cast("int")).alias("_any_refused_conflict"),
    )
    m = members.join(accession_identity, "accession_key", "left")
    person_status = (
        F.when((F.col("accession_person_count") > 1) | (F.col("_any_refused_conflict") == 1),
               F.lit("conflicting_persons"))
        .when(F.col("accession_person_count") == 0, F.lit("no_person"))
        .when(F.col("_any_unresolved") == 1, F.lit("single_person_partial"))
        .otherwise(F.lit("single_person"))
    )
    reasons = F.concat_ws(
        ",",
        F.when(F.col("record_status") != F.lit("active"), F.lit("source_not_current")),
        F.when(F.col("performed_datetime").isNull(), F.lit("no_valid_performed_time")),
        F.when(F.col("performed_datetime") > F.col("loaded_at"), F.lit("performed_after_as_of")),
        F.when(~F.col("performed_evidence_verified_ind"), F.lit("performed_evidence_unverified")),
        F.when(person_status == F.lit("conflicting_persons"),
               F.lit("accession_identity_conflict")),
        F.when((F.col("accession_evidence_level") == F.lit("cerner_reference_parsed"))
               & F.col("accession_format").isin(*UNVALIDATED_ACCESSION_FORMATS),
               F.lit("unvalidated_cerner_accession")),
    )
    blocking = F.when(reasons != F.lit(""), reasons)
    examination_key = F.coalesce(
        F.concat(F.lit("pacs:"), F.col("linked_pacs_examination_id").cast("string")),
        F.concat(F.lit("mill:"), F.col("event_id").cast("string")),
    )
    return m.select(
        stable_id("imaging_accession_member", F.col("patient_event_key")).alias("member_key"),
        "accession_key", "sectra_accession_number", "patient_event_key", "source_object",
        "event_id", "pacs_examination_id", "linked_pacs_examination_id",
        examination_key.alias("examination_key"), "study_instance_uid", "subject_key",
        "subject_id_system", "person_id", "identity_status", "encounter_id",
        "accession_format", "accession_parse_method", "accession_rule_version",
        "accession_evidence_level", "pacs_link_method",
        F.col("accession_person_count").cast("int").alias("accession_person_count"),
        person_status.alias("accession_person_status"),
        "performed_datetime", "performed_datetime_source", "performed_evidence",
        "performed_evidence_verified_ind",
        F.col("loaded_at").alias("eligibility_as_of"),
        blocking.isNull().alias("default_selection_eligible_ind"),
        blocking.alias("eligibility_blocking_reasons"),
        "source_coding_system", "source_code", "source_display", "modality_code",
        "body_site_code", "status_code", "source_status_code", "record_status",
        "report_available_ind", "report_link_status", "report_patient_event_key",
        "linked_report_event_id", "native_image_count", "native_series_count",
        "measured_series_count", "source_feed", "source_update_timestamp", "loaded_at",
    )


# Clinical / imaging accession member and report link
_EXAM_AXIS = axis_columns("exam", False)


IMAGING_ACCESSION_MEMBER_PUBLIC_COLUMNS = (
    IMAGING_ACCESSION_MEMBER_COLUMNS[:_at] + _EXAM_AXIS + IMAGING_ACCESSION_MEMBER_COLUMNS[_at:]
)


@materialized_view(
    name=_n("journey_clinical.imaging_accession_member"),
    cluster_by=["accession_key"],
    refresh_policy="auto",
)
def imaging_accession_member():
    # Build the declared dataset: accession members with eligibility, carrying the imaging_exam
    # mapped exam axis (NULL for examinations imaging_exam does not admit).
    concepts = spark.read.table(_n("journey_clinical.imaging_exam")).select(
        F.col("patient_event_key").alias("_concept_key"), *_EXAM_AXIS
    )
    members = _imaging_accession_member_canonical()
    return (
        members.join(concepts, members.patient_event_key == concepts._concept_key, "left")
        .select(*IMAGING_ACCESSION_MEMBER_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_imaging_accession_member': {'columns': {'accession_evidence_level': 'pacs_request, '
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
                                                   'exam_map_candidate_count': 'Number of candidate targets '
                                                                               'observed; a mapped parent is '
                                                                               'unambiguous when this is 1.',
                                                   'exam_map_candidate_set_id': 'Stable identifier of the '
                                                                                'retained candidate set.',
                                                   'exam_map_competing_count': 'Candidates within 0.05 '
                                                                               'cosine of the selected '
                                                                               'candidate.',
                                                   'exam_map_component_count': 'Number of source-derived '
                                                                               'components.',
                                                   'exam_map_component_index': 'One-based source-derived '
                                                                               'component index.',
                                                   'exam_map_confidence': 'Mapping confidence in [0,1] where '
                                                                          'the lane measured one.',
                                                   'exam_map_cosine': 'Measured cosine between the '
                                                                      'normalised source and selected '
                                                                      'target; null when scoring did not '
                                                                      'run.',
                                                   'exam_map_method': 'Mapping method: source_native, rule, '
                                                                      'exact_synonym or embedding_proposed.',
                                                   'exam_map_rollup_levels': 'Maximum hierarchy distance to '
                                                                             'a selected common ancestor.',
                                                   'exam_map_rule_id': 'Stable rule or lane identifier that '
                                                                       'produced the mapping.',
                                                   'exam_map_scoring_model': 'Embedding model that produced '
                                                                             'map_cosine.',
                                                   'exam_map_status': 'Explicit mapping outcome from the S3b '
                                                                      'status vocabulary.',
                                                   'exam_map_status_reason': 'Reason for the mapping outcome '
                                                                             'or abstention.',
                                                   'exam_map_version': 'Version of the source, terminology '
                                                                       'release or model used by the '
                                                                       'mapping.',
                                                   'exam_omop_concept_id': 'OMOP concept identifier selected '
                                                                           'with the imaging examination '
                                                                           'mapping.',
                                                   'exam_omop_source_concept_id': 'Non-standard OMOP source '
                                                                                  'concept identifier, kept '
                                                                                  'separate from a standard '
                                                                                  'target.',
                                                   'exam_snomed_code': 'SNOMED CT code selected for the '
                                                                       'imaging examination; null when no '
                                                                       'unambiguous mapping is available.',
                                                   'exam_snomed_display': 'SNOMED CT preferred display '
                                                                          'selected for the imaging '
                                                                          'examination.',
                                                   'exam_source_code': 'Source code for the imaging '
                                                                       'examination, retained without '
                                                                       'de-identification.',
                                                   'exam_source_display': 'Source display for the imaging '
                                                                          'examination.',
                                                   'exam_source_system': 'Canonical coding-system URI for '
                                                                         'the source imaging examination.',
                                                   'exam_target_code': 'Code of the selected target in its '
                                                                       'own vocabulary.',
                                                   'exam_target_display': 'Display of the selected target in '
                                                                          'its own vocabulary.',
                                                   'exam_target_system': 'Coding-system URI of the selected '
                                                                         'target.',
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
                                       'table': 'One row per accession-bearing imaging examination member '
                                                '(PACS examination or Millennium examination event) with '
                                                'default-selection eligibility and blocking reasons. Group '
                                                'by accession_key for accession-level selection.'}}
