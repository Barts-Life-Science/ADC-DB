"""Silver index.person_activity: 55 dataset(s).

- datasets: _reference_person_activity_family_history, _reference_person_activity_presenting_complaint, _reference_person_activity_allergy_intolerance, _reference_person_activity_transfusion, _reference_person_activity_cancer_treatment, _reference_person_activity_condition_stage, _reference_person_activity_endoscopy_finding, _reference_person_activity_registry_entry, _reference_person_activity_device, _reference_person_activity_condition, _reference_person_activity_procedure, _reference_person_activity_pathology_order ...
- moved verbatim from: 1_silver_spine_and_reference:3600, 1_silver_spine_and_reference:3622

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark
from silver_tables.index._shared import EVENT_FEED_NAMES


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# INSR_W2 W2.09 ====================================================================================================
# person_scope: the single scope definition that gold (W3.02) and omop_gold (W5.03) read. Silver removes nobody.
# C0 ruling 1 (2026-10-29): in_scope = has_encounter OR has_clinical_data.
def _declare_person_activity(feed):
    """Per-person activity of one event feed (row count, first/last event, last clinical-category event).
    One small grouped MV per feed, so an edit to one feed re-aggregates only that feed."""
    def person_activity():
        # Group the feed's resolved-person rows by person.
        f = spark.read.table(_n(f"journey_events._feed_{feed}")).where(F.col("person_id").isNotNull())
        clinical = F.col("fact_category") == "clinical"
        return f.groupBy(F.col("person_id").cast("bigint").alias("person_id")).agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            # C0 ruling 1: only clinical-category rows are clinical data (appointments incl. CANCELED/NOSHOW, waiting
            # list, RTT, costing, HRG and drug expenditure are administrative and do not put a person in scope).
            F.sum(clinical.cast("long")).cast("long").alias("n_clinical_rows"),
            F.min("event_datetime").alias("first_event_datetime"),
            F.max("event_datetime").alias("last_event_datetime"),
            F.max(F.when(F.col("fact_category") == "clinical", F.col("event_datetime"))).alias("last_clinical_event_datetime"),
        )
    person_activity.__name__ = f"_person_activity_{feed}"
    materialized_view(name=_n(f"journey_reference._person_activity_{feed}"), private=True,
                      refresh_policy="auto")(person_activity)


for _feed in EVENT_FEED_NAMES:
    _declare_person_activity(_feed)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_person_activity_allergy_intolerance': {'columns': {},
                                                    'table': 'INSR_W2 W2.09: per-person activity of the '
                                                             'allergy_intolerance event feed.'},
 '_reference_person_activity_appointment': {'columns': {},
                                            'table': 'INSR_W2 W2.09: per-person activity of the appointment '
                                                     'event feed.'},
 '_reference_person_activity_artifact_asset': {'columns': {},
                                               'table': 'INSR_W2 W2.09: per-person activity of the '
                                                        'artifact_asset event feed.'},
 '_reference_person_activity_baby_delivery': {'columns': {},
                                              'table': 'INSR_W2 W2.09: per-person activity of the '
                                                       'baby_delivery event feed.'},
 '_reference_person_activity_cancer_treatment': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'cancer_treatment event feed.'},
 '_reference_person_activity_clinical_finding': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'clinical_finding event feed.'},
 '_reference_person_activity_clinical_score': {'columns': {},
                                               'table': 'INSR_W2 W2.09: per-person activity of the '
                                                        'clinical_score event feed.'},
 '_reference_person_activity_community_care_activity': {'columns': {},
                                                        'table': 'INSR_W2 W2.09: per-person activity of the '
                                                                 'community_care_activity event feed.'},
 '_reference_person_activity_community_care_contact': {'columns': {},
                                                       'table': 'INSR_W2 W2.09: per-person activity of the '
                                                                'community_care_contact event feed.'},
 '_reference_person_activity_condition': {'columns': {},
                                          'table': 'INSR_W2 W2.09: per-person activity of the condition '
                                                   'event feed.'},
 '_reference_person_activity_condition_stage': {'columns': {},
                                                'table': 'INSR_W2 W2.09: per-person activity of the '
                                                         'condition_stage event feed.'},
 '_reference_person_activity_costed_activity': {'columns': {},
                                                'table': 'INSR_W2 W2.09: per-person activity of the '
                                                         'costed_activity event feed.'},
 '_reference_person_activity_critical_care_activity': {'columns': {},
                                                       'table': 'INSR_W2 W2.09: per-person activity of the '
                                                                'critical_care_activity event feed.'},
 '_reference_person_activity_critical_care_admission': {'columns': {},
                                                        'table': 'INSR_W2 W2.09: per-person activity of the '
                                                                 'critical_care_admission event feed.'},
 '_reference_person_activity_critical_care_daily_score': {'columns': {},
                                                          'table': 'INSR_W2 W2.09: per-person activity of '
                                                                   'the critical_care_daily_score event '
                                                                   'feed.'},
 '_reference_person_activity_critical_care_period': {'columns': {},
                                                     'table': 'INSR_W2 W2.09: per-person activity of the '
                                                              'critical_care_period event feed.'},
 '_reference_person_activity_device': {'columns': {},
                                       'table': 'INSR_W2 W2.09: per-person activity of the device event '
                                                'feed.'},
 '_reference_person_activity_drug_expenditure': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'drug_expenditure event feed.'},
 '_reference_person_activity_elective_access_entry': {'columns': {},
                                                      'table': 'INSR_W2 W2.09: per-person activity of the '
                                                               'elective_access_entry event feed.'},
 '_reference_person_activity_endoscopy_finding': {'columns': {},
                                                  'table': 'INSR_W2 W2.09: per-person activity of the '
                                                           'endoscopy_finding event feed.'},
 '_reference_person_activity_family_history': {'columns': {},
                                               'table': 'INSR_W2 W2.09: per-person activity of the '
                                                        'family_history event feed.'},
 '_reference_person_activity_form': {'columns': {},
                                     'table': 'INSR_W2 W2.09: per-person activity of the form event feed.'},
 '_reference_person_activity_genomic_result': {'columns': {},
                                               'table': 'INSR_W2 W2.09: per-person activity of the '
                                                        'genomic_result event feed.'},
 '_reference_person_activity_genomic_test': {'columns': {},
                                             'table': 'INSR_W2 W2.09: per-person activity of the '
                                                      'genomic_test event feed.'},
 '_reference_person_activity_hrg_grouping': {'columns': {},
                                             'table': 'INSR_W2 W2.09: per-person activity of the '
                                                      'hrg_grouping event feed.'},
 '_reference_person_activity_imaging_exam': {'columns': {},
                                             'table': 'INSR_W2 W2.09: per-person activity of the '
                                                      'imaging_exam event feed.'},
 '_reference_person_activity_indication': {'columns': {},
                                           'table': 'INSR_W2 W2.09: per-person activity of the indication '
                                                    'event feed.'},
 '_reference_person_activity_labour_delivery': {'columns': {},
                                                'table': 'INSR_W2 W2.09: per-person activity of the '
                                                         'labour_delivery event feed.'},
 '_reference_person_activity_maternity_care_contact': {'columns': {},
                                                       'table': 'INSR_W2 W2.09: per-person activity of the '
                                                                'maternity_care_contact event feed.'},
 '_reference_person_activity_medication_admin': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'medication_admin event feed.'},
 '_reference_person_activity_medication_dispense': {'columns': {},
                                                    'table': 'INSR_W2 W2.09: per-person activity of the '
                                                             'medication_dispense event feed.'},
 '_reference_person_activity_medication_order': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'medication_order event feed.'},
 '_reference_person_activity_medication_supply': {'columns': {},
                                                  'table': 'INSR_W2 W2.09: per-person activity of the '
                                                           'medication_supply event feed.'},
 '_reference_person_activity_microbiology_isolate': {'columns': {},
                                                     'table': 'INSR_W2 W2.09: per-person activity of the '
                                                              'microbiology_isolate event feed.'},
 '_reference_person_activity_neonatal_care_day': {'columns': {},
                                                  'table': 'INSR_W2 W2.09: per-person activity of the '
                                                           'neonatal_care_day event feed.'},
 '_reference_person_activity_neonatal_episode': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'neonatal_episode event feed.'},
 '_reference_person_activity_neonatal_examination': {'columns': {},
                                                     'table': 'INSR_W2 W2.09: per-person activity of the '
                                                              'neonatal_examination event feed.'},
 '_reference_person_activity_pathology_order': {'columns': {},
                                                'table': 'INSR_W2 W2.09: per-person activity of the '
                                                         'pathology_order event feed.'},
 '_reference_person_activity_pathology_report': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'pathology_report event feed.'},
 '_reference_person_activity_pathology_result': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'pathology_result event feed.'},
 '_reference_person_activity_pathway_tracking': {'columns': {},
                                                 'table': 'INSR_W2 W2.09: per-person activity of the '
                                                          'pathway_tracking event feed.'},
 '_reference_person_activity_presenting_complaint': {'columns': {},
                                                     'table': 'INSR_W2 W2.09: per-person activity of the '
                                                              'presenting_complaint event feed.'},
 '_reference_person_activity_procedure': {'columns': {},
                                          'table': 'INSR_W2 W2.09: per-person activity of the procedure '
                                                   'event feed.'},
 '_reference_person_activity_referral': {'columns': {},
                                         'table': 'INSR_W2 W2.09: per-person activity of the referral event '
                                                  'feed.'},
 '_reference_person_activity_registry_entry': {'columns': {},
                                               'table': 'INSR_W2 W2.09: per-person activity of the '
                                                        'registry_entry event feed.'},
 '_reference_person_activity_research_enrollment': {'columns': {},
                                                    'table': 'INSR_W2 W2.09: per-person activity of the '
                                                             'research_enrollment event feed.'},
 '_reference_person_activity_rtt_activity': {'columns': {},
                                             'table': 'INSR_W2 W2.09: per-person activity of the '
                                                      'rtt_activity event feed.'},
 '_reference_person_activity_rtt_pathway': {'columns': {},
                                            'table': 'INSR_W2 W2.09: per-person activity of the rtt_pathway '
                                                     'event feed.'},
 '_reference_person_activity_safety_incident': {'columns': {},
                                                'table': 'INSR_W2 W2.09: per-person activity of the '
                                                         'safety_incident event feed.'},
 '_reference_person_activity_specimen': {'columns': {},
                                         'table': 'INSR_W2 W2.09: per-person activity of the specimen event '
                                                  'feed.'},
 '_reference_person_activity_susceptibility_result': {'columns': {},
                                                      'table': 'INSR_W2 W2.09: per-person activity of the '
                                                               'susceptibility_result event feed.'},
 '_reference_person_activity_text_document': {'columns': {},
                                              'table': 'INSR_W2 W2.09: per-person activity of the '
                                                       'text_document event feed.'},
 '_reference_person_activity_transfusion': {'columns': {},
                                            'table': 'INSR_W2 W2.09: per-person activity of the transfusion '
                                                     'event feed.'},
 '_reference_person_activity_vital_sign': {'columns': {},
                                           'table': 'INSR_W2 W2.09: per-person activity of the vital_sign '
                                                    'event feed.'},
 '_reference_person_activity_waiting_list_entry': {'columns': {},
                                                   'table': 'INSR_W2 W2.09: per-person activity of the '
                                                            'waiting_list_entry event feed.'}}
