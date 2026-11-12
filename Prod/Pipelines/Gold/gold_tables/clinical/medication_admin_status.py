"""Gold clinical.medication_admin_status: 2 dataset(s).

- datasets: _qc_clinical_medication_admin_status, clinical_medication_admin_status
- moved verbatim from: 2_gold_clinical_events:7360, 2_gold_clinical_events:7386, 2_gold_clinical_events:7392, 2_gold_clinical_events:7408

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_admin_status ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_medication_admin applies the parent Gold drop rule.
CLINICAL_MEDICATION_ADMIN_STATUS_SELECT = [
    '`event_id` AS `event_id`',
    '`sequence` AS `sequence`',
    '`status_kind` AS `status_kind`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`effective_datetime` AS `effective_datetime`',
    '`practitioner_key` AS `practitioner_key`',
    '`source_action_sequence` AS `source_action_sequence`',
    '`source_present_ind` AS `source_present_ind`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_MEDICATION_ADMIN_STATUS_SELECT += [  # GOLD_MV_SPLIT_V1: parent drop-rule inputs silver carries; the R4 view published them
    '`parent_identity_status` AS `parent_identity_status`',
    '`parent_person_id` AS `parent_person_id`',
    '`parent_row_count` AS `parent_row_count`',
]


CLINICAL_MEDICATION_ADMIN_STATUS_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_medication_admin_status.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_admin_status"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_ADMIN_STATUS_MANDATORY_RULES)
def _gold_qc_clinical_medication_admin_status():
    """Task 3 Gold child of clinical_medication_admin; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.medication_admin")).select(
        'event_id',
    ).dropDuplicates(['event_id'])
    child = spark.read.table(_src("clinical_medication_admin_status"))
    df = _with_parent_status(child, parent, ['event_id']).selectExpr(*CLINICAL_MEDICATION_ADMIN_STATUS_SELECT, "__gold_parent_present AS __gold_parent_present")
    df = insr_w4_apply_value_rules('clinical_medication_admin_status', df)  # INSR_W4
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_admin_status"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_admin_status():
    """Publish clinical_medication_admin_status without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_medication_admin_status")).drop("__gold_parent_present")


RULES = {
    'clinical_medication_admin_status': GoldRules(
        table='clinical_medication_admin_status',
        select=CLINICAL_MEDICATION_ADMIN_STATUS_SELECT,
        mandatory=CLINICAL_MEDICATION_ADMIN_STATUS_MANDATORY_RULES,
        value_rules={'gold.clinical.medication_admin_status.status_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                 'column': 'status_code',
                                                                                 'companions': [],
                                                                                 'match': 'TRIM(`status_code`) '
                                                                                          "= '0' AND "
                                                                                          '`status_display` '
                                                                                          'IS NULL',
                                                                                 'table': 'clinical_medication_admin_status'}}),
}
_rules = QcBinding(RULES)
insr_w4_apply_value_rules = _rules.insr_w4_apply_value_rules


finalise()


COMMENTS = {'_qc_clinical_medication_admin_status': {'columns': {'effective_datetime': 'Effective timestamp of the '
                                                                            'status milestone.',
                                                      'event_id': 'Millennium EVENT_ID of the parent '
                                                                  'medication administration.',
                                                      'loaded_at': 'Bronze load timestamp inherited from the '
                                                                   'parent source row.',
                                                      'practitioner_key': 'Deterministic practitioner key '
                                                                          'when the milestone identifies '
                                                                          'personnel.',
                                                      'sequence': 'Deterministic status milestone sequence '
                                                                  'from the parent expression.',
                                                      'source_action_sequence': 'Source action sequence when '
                                                                                'supplied; null for '
                                                                                'synthesized milestones.',
                                                      'source_present_ind': 'True for status rows '
                                                                            'synthesized from a retained '
                                                                            'administration row.',
                                                      'status_code': 'Source status code synthesized by the '
                                                                     'existing parent expression.',
                                                      'status_display': 'Source status display synthesized '
                                                                        'by the existing parent expression.',
                                                      'status_kind': 'Milestone kind such as scheduled, '
                                                                     'result, verified, or discontinued.'},
                                          'quantity_override': True,
                                          'table': 'Internal QC of clinical_medication_admin_status; counts '
                                                   'rows whose Gold parent is absent.'},
 'clinical_medication_admin_status': {'columns': {},
                                      'quantity_override': False,
                                      'table': 'One status milestone per medication administration, '
                                               'retaining source sequence and the parent event key. '
                                               'Coverage: Electronic administration recording starts in '
                                               'August 2021 (raw holds 20 rows from 2016 to July 2021) apart '
                                               'from a 2015 pilot island of about 19,000 rows; '
                                               'complete-through month per release: '
                                               '6_mgmt.data_quality.release_finding (finding_code '
                                               'F2_complete_through).'}}
