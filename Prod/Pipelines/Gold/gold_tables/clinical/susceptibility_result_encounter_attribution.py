"""Gold clinical.susceptibility_result_encounter_attribution: 2 dataset(s).

- datasets: _qc_clinical_susceptibility_result_encounter_attribution, clinical_susceptibility_result_encounter_attribution
- moved verbatim from: 2_gold_clinical_events:8090, 2_gold_clinical_events:8096, 2_gold_clinical_events:8109

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== clinical_susceptibility_result_encounter_attribution (INSR_W3 W3.12: W2.12 encounter-attribution companion, J32) ====
CLINICAL_SUSCEPTIBILITY_RESULT_ENCOUNTER_ATTRIBUTION_MANDATORY_RULES = {
    # The companion row describes a gold susceptibility_result row; it leaves gold with it (person scope and drops carry over).
    "gold.clinical.susceptibility_result_encounter_attribution.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_susceptibility_result_encounter_attribution"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_SUSCEPTIBILITY_RESULT_ENCOUNTER_ATTRIBUTION_MANDATORY_RULES)
def _gold_qc_clinical_susceptibility_result_encounter_attribution():
    """Child twin of journey_clinical.susceptibility_result_encounter_attribution keyed on patient_event_key."""
    parent = spark.read.table(_n("gold_clinical.susceptibility_result")).select("patient_event_key").dropDuplicates(["patient_event_key"])
    child = spark.read.table(_src("clinical_susceptibility_result_encounter_attribution"))
    return _with_parent_status(child, parent, ["patient_event_key"])


@dp.materialized_view(
    name=_n("gold_clinical.susceptibility_result_encounter_attribution"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_susceptibility_result_encounter_attribution():
    """Publish clinical_susceptibility_result_encounter_attribution without the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_susceptibility_result_encounter_attribution")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_susceptibility_result_encounter_attribution': {'columns': {},
                                                              'quantity_override': False,
                                                              'table': 'Internal twin of '
                                                                       'clinical_susceptibility_result_encounter_attribution: '
                                                                       'admitted only with its gold '
                                                                       'clinical_susceptibility_result '
                                                                       'parent row.'},
 'clinical_susceptibility_result_encounter_attribution': {'columns': {},
                                                          'quantity_override': False,
                                                          'table': 'Best-effort encounter for '
                                                                   'susceptibility_result rows that carry no '
                                                                   'encounter_id (J32): derived encounter, '
                                                                   "method and candidate count. The fact's "
                                                                   'own encounter_id is untouched; join on '
                                                                   'patient_event_key.'}}
