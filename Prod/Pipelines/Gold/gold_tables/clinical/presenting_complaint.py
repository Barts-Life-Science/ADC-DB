"""Gold clinical.presenting_complaint: 2 dataset(s).

- datasets: _qc_clinical_presenting_complaint, clinical_presenting_complaint
- moved verbatim from: 2_gold_clinical_events:7948, 2_gold_clinical_events:7959, 2_gold_clinical_events:7960, 2_gold_clinical_events:7961, 2_gold_clinical_events:7974, 2_gold_clinical_events:7979, 2_gold_clinical_events:7984, 2_gold_clinical_events:8002

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _s3_axis_columns, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.presenting_complaint (S3b) ====
PRESENTING_COMPLAINT_COLUMNS = [
    "patient_event_key", "source_patient_event_key", "encounter_key", "encounter_id",
    "subject_key", "subject_id_system", "person_id", "source_object", "source_event_id",
    "statement_sequence", "event_datetime", "encounter_class", "source_coding_system",
    "event_datetime_local",  # GOLD_COLUMN_PARITY_V1
    "source_code", "source_display", "complaint_text_normalised", "complaint_group_display",
    *_s3_axis_columns("complaint"),
    "record_status", "record_status_effective_from", "record_status_effective_to",
    "confidentiality_code", "vip_ind", "withheld_identity_ind",
    "source_update_timestamp", "loaded_at",
]


PRESENTING_COMPLAINT_SELECT = [f"`{name}` AS `{name}`" for name in PRESENTING_COMPLAINT_COLUMNS]


PRESENTING_COMPLAINT_MANDATORY_RULES = {"research_surface": "identity_status = 'resolved'"}


PRESENTING_COMPLAINT_ADVISORY_RULES = {
    "gold.clinical.presenting_complaint.after_death_30d": "NOT COALESCE(event_after_death_30d, FALSE)",
    "gold.clinical.presenting_complaint.before_birth": "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_PRESENTING_COMPLAINT_PERSON_SCOPE_RULES = {
    "gold.clinical.presenting_complaint.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_PRESENTING_COMPLAINT_ORPHAN_RULES = {
    "gold.clinical.presenting_complaint.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
    "gold.clinical.presenting_complaint.encounter_id.orphan_nulled": "NOT COALESCE(`_qc_encounter_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_presenting_complaint"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(PRESENTING_COMPLAINT_MANDATORY_RULES)
@dp.expect_all(PRESENTING_COMPLAINT_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_PRESENTING_COMPLAINT_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_PRESENTING_COMPLAINT_ORPHAN_RULES)
def _gold_qc_clinical_presenting_complaint():
    """Prepare internal QC data for clinical_presenting_complaint; native rules run on the returned rows."""
    df = _qc(
        "clinical_presenting_complaint", PRESENTING_COMPLAINT_SELECT,
        fk_columns=("person_id", "encounter_id"),
        date_flags=("event_after_death_30d", "event_before_birth"),
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.presenting_complaint"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_presenting_complaint():
    """Publish clinical_presenting_complaint with its existing research columns and quality policy."""
    qc = spark.read.table(_n("gold_qc._clinical_presenting_complaint"))
    selected = qc.select(
        *PRESENTING_COMPLAINT_COLUMNS,
        "_complaint_gate_pass", "_complaint_policy_id",
    )
    return (_s3_gate_direct_public(selected, "clinical_presenting_complaint")
            .drop("_complaint_gate_pass", "_complaint_policy_id"))


def _public_steps_clinical_presenting_complaint(df):
    """clinical_presenting_complaint: the public body after its projection (GoldRules.public_select), moved from gold_clinical_presenting_complaint; df is the projected twin frame."""
    selected = df
    return _s3_gate_direct_public(selected, 'clinical_presenting_complaint').drop('_complaint_gate_pass', '_complaint_policy_id')


RULES = {
    'clinical_presenting_complaint': GoldRules(
        table='clinical_presenting_complaint',
        person_scope=CLINICAL_PRESENTING_COMPLAINT_PERSON_SCOPE_RULES,
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
        s3b_direct_axes=(('complaint', False),),
        value_rules={'gold.clinical.presenting_complaint.complaint_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'complaint_snomed_code',
                                                                                            'companions': ['complaint_snomed_display',
                                                                                                           'complaint_target_code',
                                                                                                           'complaint_target_display'],
                                                                                            'match': '`complaint_snomed_code` '
                                                                                                     'IS '
                                                                                                     'NOT '
                                                                                                     'NULL '
                                                                                                     'AND '
                                                                                                     '(NOT '
                                                                                                     '`complaint_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`complaint_snomed_code`, '
                                                                                                     'length(`complaint_snomed_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) '
                                                                                                     'NOT '
                                                                                                     'IN '
                                                                                                     "('00', "
                                                                                                     "'10') "
                                                                                                     'OR '
                                                                                                     '(substring(`complaint_snomed_code`, '
                                                                                                     'length(`complaint_snomed_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) = '
                                                                                                     "'10' "
                                                                                                     'AND '
                                                                                                     'length(`complaint_snomed_code`) '
                                                                                                     '< '
                                                                                                     '11))',
                                                                                            'table': 'clinical_presenting_complaint'},
         'gold.clinical.presenting_complaint.complaint_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'complaint_target_code',
                                                                                            'companions': ['complaint_target_display',
                                                                                                           'complaint_snomed_code',
                                                                                                           'complaint_snomed_display'],
                                                                                            'match': '`complaint_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`complaint_target_code` '
                                                                                                     'IS '
                                                                                                     'NOT '
                                                                                                     'NULL '
                                                                                                     'AND '
                                                                                                     '(NOT '
                                                                                                     '`complaint_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`complaint_target_code`, '
                                                                                                     'length(`complaint_target_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) '
                                                                                                     'NOT '
                                                                                                     'IN '
                                                                                                     "('00', "
                                                                                                     "'10') "
                                                                                                     'OR '
                                                                                                     '(substring(`complaint_target_code`, '
                                                                                                     'length(`complaint_target_code`) '
                                                                                                     '- 2, '
                                                                                                     '2) = '
                                                                                                     "'10' "
                                                                                                     'AND '
                                                                                                     'length(`complaint_target_code`) '
                                                                                                     '< '
                                                                                                     '11))',
                                                                                            'table': 'clinical_presenting_complaint'}},
        public_select=[f'`{c}` AS `{c}`' for c in (*PRESENTING_COMPLAINT_COLUMNS, '_complaint_gate_pass', '_complaint_policy_id')],
        public_steps=(_public_steps_clinical_presenting_complaint,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc
_s3_gate_direct_public = _rules._s3_gate_direct_public


finalise()


COMMENTS = {'_qc_clinical_presenting_complaint': {'columns': {'complaint_group_display': 'S3b presenting-complaint '
                                                                              'field carried from the '
                                                                              'governed Silver product.',
                                                   'complaint_map_candidate_count': 'S3b '
                                                                                    'presenting-complaint '
                                                                                    'field carried from the '
                                                                                    'governed Silver '
                                                                                    'product.',
                                                   'complaint_map_candidate_set_id': 'S3b '
                                                                                     'presenting-complaint '
                                                                                     'field carried from the '
                                                                                     'governed Silver '
                                                                                     'product.',
                                                   'complaint_map_competing_count': 'S3b '
                                                                                    'presenting-complaint '
                                                                                    'field carried from the '
                                                                                    'governed Silver '
                                                                                    'product.',
                                                   'complaint_map_component_count': 'S3b '
                                                                                    'presenting-complaint '
                                                                                    'field carried from the '
                                                                                    'governed Silver '
                                                                                    'product.',
                                                   'complaint_map_component_index': 'S3b '
                                                                                    'presenting-complaint '
                                                                                    'field carried from the '
                                                                                    'governed Silver '
                                                                                    'product.',
                                                   'complaint_map_confidence': 'S3b presenting-complaint '
                                                                               'field carried from the '
                                                                               'governed Silver product.',
                                                   'complaint_map_cosine': 'Cosine similarity between the '
                                                                           'normalised source text and the '
                                                                           'selected target.',
                                                   'complaint_map_method': 'S3b presenting-complaint field '
                                                                           'carried from the governed Silver '
                                                                           'product.',
                                                   'complaint_map_rollup_levels': 'S3b presenting-complaint '
                                                                                  'field carried from the '
                                                                                  'governed Silver product.',
                                                   'complaint_map_rule_id': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.',
                                                   'complaint_map_scoring_model': 'S3b presenting-complaint '
                                                                                  'field carried from the '
                                                                                  'governed Silver product.',
                                                   'complaint_map_status': 'S3b presenting-complaint field '
                                                                           'carried from the governed Silver '
                                                                           'product.',
                                                   'complaint_map_status_reason': 'S3b presenting-complaint '
                                                                                  'field carried from the '
                                                                                  'governed Silver product.',
                                                   'complaint_map_version': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.',
                                                   'complaint_omop_concept_id': 'S3b presenting-complaint '
                                                                                'field carried from the '
                                                                                'governed Silver product.',
                                                   'complaint_omop_source_concept_id': 'S3b '
                                                                                       'presenting-complaint '
                                                                                       'field carried from '
                                                                                       'the governed Silver '
                                                                                       'product.',
                                                   'complaint_snomed_code': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.',
                                                   'complaint_snomed_display': 'S3b presenting-complaint '
                                                                               'field carried from the '
                                                                               'governed Silver product.',
                                                   'complaint_source_code': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.',
                                                   'complaint_source_display': 'S3b presenting-complaint '
                                                                               'field carried from the '
                                                                               'governed Silver product.',
                                                   'complaint_source_system': 'S3b presenting-complaint '
                                                                              'field carried from the '
                                                                              'governed Silver product.',
                                                   'complaint_target_code': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.',
                                                   'complaint_target_display': 'S3b presenting-complaint '
                                                                               'field carried from the '
                                                                               'governed Silver product.',
                                                   'complaint_target_system': 'S3b presenting-complaint '
                                                                              'field carried from the '
                                                                              'governed Silver product.',
                                                   'complaint_text_normalised': 'S3b presenting-complaint '
                                                                                'field carried from the '
                                                                                'governed Silver product.',
                                                   'confidentiality_code': 'S3b presenting-complaint field '
                                                                           'carried from the governed Silver '
                                                                           'product.',
                                                   'encounter_class': 'S3b presenting-complaint field '
                                                                      'carried from the governed Silver '
                                                                      'product.',
                                                   'encounter_id': 'S3b presenting-complaint field carried '
                                                                   'from the governed Silver product.',
                                                   'encounter_key': 'S3b presenting-complaint field carried '
                                                                    'from the governed Silver product.',
                                                   'event_datetime': 'S3b presenting-complaint field carried '
                                                                     'from the governed Silver product.',
                                                   'event_datetime_local': 'S3b presenting-complaint field '
                                                                           'carried from the governed Silver '
                                                                           'product.',
                                                   'loaded_at': 'S3b presenting-complaint field carried from '
                                                                'the governed Silver product.',
                                                   'patient_event_key': 'S3b presenting-complaint field '
                                                                        'carried from the governed Silver '
                                                                        'product.',
                                                   'person_id': 'S3b presenting-complaint field carried from '
                                                                'the governed Silver product.',
                                                   'record_status': 'S3b presenting-complaint field carried '
                                                                    'from the governed Silver product.',
                                                   'record_status_effective_from': 'S3b presenting-complaint '
                                                                                   'field carried from the '
                                                                                   'governed Silver product.',
                                                   'record_status_effective_to': 'S3b presenting-complaint '
                                                                                 'field carried from the '
                                                                                 'governed Silver product.',
                                                   'source_code': 'S3b presenting-complaint field carried '
                                                                  'from the governed Silver product.',
                                                   'source_coding_system': 'S3b presenting-complaint field '
                                                                           'carried from the governed Silver '
                                                                           'product.',
                                                   'source_display': 'S3b presenting-complaint field carried '
                                                                     'from the governed Silver product.',
                                                   'source_event_id': 'S3b presenting-complaint field '
                                                                      'carried from the governed Silver '
                                                                      'product.',
                                                   'source_object': 'S3b presenting-complaint field carried '
                                                                    'from the governed Silver product.',
                                                   'source_patient_event_key': 'S3b presenting-complaint '
                                                                               'field carried from the '
                                                                               'governed Silver product.',
                                                   'source_update_timestamp': 'S3b presenting-complaint '
                                                                              'field carried from the '
                                                                              'governed Silver product.',
                                                   'statement_sequence': 'S3b presenting-complaint field '
                                                                         'carried from the governed Silver '
                                                                         'product.',
                                                   'subject_id_system': 'S3b presenting-complaint field '
                                                                        'carried from the governed Silver '
                                                                        'product.',
                                                   'subject_key': 'S3b presenting-complaint field carried '
                                                                  'from the governed Silver product.',
                                                   'vip_ind': 'S3b presenting-complaint field carried from '
                                                              'the governed Silver product.',
                                                   'withheld_identity_ind': 'S3b presenting-complaint field '
                                                                            'carried from the governed '
                                                                            'Silver product.'},
                                       'quantity_override': True,
                                       'table': 'Internal quality-controlled twin of '
                                                'clinical_presenting_complaint with the S3b admission '
                                                'decision.'},
 'clinical_presenting_complaint': {'columns': {},
                                   'quantity_override': False,
                                   'table': 'Research-facing presenting complaints from encounter RFV, ED '
                                            'pick-list and bounded ED text sources.'}}
