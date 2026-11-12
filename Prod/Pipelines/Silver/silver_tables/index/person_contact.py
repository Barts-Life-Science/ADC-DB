"""Silver index.person_contact: 1 dataset(s).

- datasets: _reference_person_contact
- moved verbatim from: 1_silver_spine_and_reference:3627, 1_silver_spine_and_reference:3642

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Contact sources of the ruled encounter definition (P1e, S5; Ben 2026-10-29). Each yields (person_id, source).
PERSON_CONTACT_SOURCES = {'mill_encounter': ('journey_spine.encounter',
                    "attendance_evidence = 'OBSERVED' AND coalesce(type_class_display, '') NOT IN "
                    "('Preadmit', 'Wait List', 'Results Only')"),
 'appointment_attended': ('journey_clinical.appointment',
                          "upper(status_meaning) IN ('CHECKED OUT', 'CHECKED IN')"),
 'theatre_performed': ('journey_reference.theatre_case', "case_status = 'PERFORMED'"),
 'community_contact': ('journey_clinical.community_care_contact', 'true'),
 'maternity_attended': ('journey_clinical.maternity_care_contact',
                        "CAST(attend_code AS STRING) IN ('5', '6')"),
 'ed_checkin': ('journey_clinical.tracking_attendance', 'checkin_dt_tm IS NOT NULL'),
 'critical_care_admission': ('journey_clinical.critical_care_admission', 'true'),
 'neonatal_episode': ('journey_clinical.neonatal_episode', 'true'),
 'slam_hrg_contact': ('journey_clinical.hrg_grouping', "source_object IN ('slam_op_hrg', 'slam_apc_hrg')")}


@materialized_view(name=_n("journey_reference._person_contact"), private=True, refresh_policy="auto")
def _person_contact():
    # Union the qualifying-contact persons of every source, then one row per (person, source).
    frames = [spark.read.table(_n(table)).where(F.expr(predicate))
              .select(F.col("person_id").cast("bigint").alias("person_id"), F.lit(source).alias("contact_source"))
              for source, (table, predicate) in PERSON_CONTACT_SOURCES.items()]
    union = frames[0]
    for frame in frames[1:]:
        union = union.unionByName(frame)
    return union.where(F.col("person_id").isNotNull()).groupBy("person_id", "contact_source").agg(
        F.count(F.lit(1)).cast("long").alias("n_rows"))


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_person_contact': {'columns': {},
                               'table': 'INSR_W2 W2.09: persons with a qualifying encounter, by contact '
                                        'source.'}}
