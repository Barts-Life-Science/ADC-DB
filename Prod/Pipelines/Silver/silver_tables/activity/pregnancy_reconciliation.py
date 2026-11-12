"""Silver activity.pregnancy_reconciliation: 1 dataset(s).

- datasets: reference_pregnancy_reconciliation
- moved verbatim from: 4_silver_care_pathways:4622, 4_silver_care_pathways:4625, 4_silver_care_pathways:4630, 4_silver_care_pathways:4635, silver_journey_shared.py:8833, silver_journey_shared.py:8860

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _pregnancy_reconciliation_source():
    # Publish unmatched maternity records with their original identifiers and reconciliation
    # reasons.
    s = read_source(SRC_MAT_MSDS_UNMATCHED)
    # contract v2: rename the deterministic pregnancy reconciliation identity as a key
    return s.select(
        stable_id("pregnancy_reconciliation:msds", s.UNMATCHED_KEY)
         .alias("pregnancy_reconciliation_key"),
        s.UNMATCHED_REASON.alias("unmatched_reason"),
        s.PREGNANCYID_RAW.alias("pregnancy_id_raw"),
        s.Pregnancy_ID.cast("string").alias("pregnancy_id"),
        s.LPIDMother.alias("lpid_mother"),
        s.AntenatalAppointmentDate.alias("antenatal_appointment_date"),
        s.PregnancyFirstContactDate.alias("pregnancy_first_contact_date"),
        s.ExpectedDeliveryDate.alias("expected_delivery_date"),
        s.LastMensPeriodDate.alias("last_mens_period_date"),
        s.FolicAcidSupplement_CD.alias("folic_acid_supplement_cd"),
        s.PreviousLiveBirths.alias("previous_live_births"),
        s.PreviousStillBirths.alias("previous_still_births"),
        s.PreviousLossesUnder24Weeks.alias("previous_losses_under_24_weeks"),
        s.PreviousCaesareanSections.alias("previous_caesarean_sections"),
        s.SOURCE_SYSTEM.alias("source_system_code"), s.IS_VALID.alias("is_valid"),
        s.MSDS_SOURCE_VERSION.alias("msds_source_version"), F.lit("active").alias("record_status"),
        s.RECORD_UPDATED_DT.alias("source_update_timestamp"), s.ADC_UPDT.alias("loaded_at"),
    )


SRC_MAT_MSDS_UNMATCHED = "4_prod.bronze.map_mat_pregnancy_msds_unmatched"


PREGNANCY_RECONCILIATION_LIFECYCLE_FIELDS = [
]


PREGNANCY_RECONCILIATION_RETIRED_COLUMNS = [
    "is_valid",
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_pregnancy_reconciliation():
    # Assemble pregnancy reconciliation rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _pregnancy_reconciliation_source()


@materialized_view(
    name=_n("journey_reference.pregnancy_reconciliation"),
    refresh_policy="auto",
)
def pregnancy_reconciliation():
    # Build the declared dataset: Unmatched MSDS pregnancy rows documenting the person-spine
    # coverage gap.
    return _lifecycle_source_pregnancy_reconciliation().drop(*PREGNANCY_RECONCILIATION_LIFECYCLE_FIELDS, *PREGNANCY_RECONCILIATION_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_pregnancy_reconciliation': ValuePolicy(table='reference_pregnancy_reconciliation', enabled=True, hub_zero_key='pregnancy_id'),
}


WRAP_POLICY = {'reference_pregnancy_reconciliation'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_pregnancy_reconciliation': {'columns': {'antenatal_appointment_date': 'MSDS booking appointment '
                                                                                  'date.',
                                                    'expected_delivery_date': 'MSDS agreed EDD.',
                                                    'folic_acid_supplement_cd': 'MSDS folic-acid supplement '
                                                                                'code.',
                                                    'last_mens_period_date': 'MSDS last menstrual period '
                                                                             'date.',
                                                    'loaded_at': 'map_mat_pregnancy_msds_unmatched.ADC_UPDT '
                                                                 'carried unchanged for the unmatched '
                                                                 'pregnancy row. No matched pregnancy-spine '
                                                                 'clock is joined or substituted; this is '
                                                                 'bronze ingestion provenance, distinct from '
                                                                 'RECORD_UPDATED_DT and Silver refresh time.',
                                                    'lpid_mother': 'Direct identifier published and '
                                                                   'IG-governed at serve time',
                                                    'msds_source_version': 'MSDS Delta version used for the '
                                                                           'snapshot.',
                                                    'pregnancy_first_contact_date': 'MSDS first '
                                                                                    'pregnancy-contact date.',
                                                    'pregnancy_id': 'MSDS pregnancy identifier after checked '
                                                                    'numeric parsing.',
                                                    'pregnancy_id_raw': 'Unchanged MSDS pregnancy '
                                                                        'identifier.',
                                                    'pregnancy_reconciliation_key': 'Deterministic SHA-256 '
                                                                                    'key for the unmatched '
                                                                                    'MSDS pregnancy row.',
                                                    'previous_caesarean_sections': 'MSDS previous-caesarean '
                                                                                   'count.',
                                                    'previous_live_births': 'MSDS previous live-birth count.',
                                                    'previous_losses_under_24_weeks': 'MSDS '
                                                                                      'terminations/losses '
                                                                                      'under 24 weeks.',
                                                    'previous_still_births': 'MSDS previous stillbirth '
                                                                             'count.',
                                                    'record_status': 'Always active in this projection, '
                                                                     'including unmatched or '
                                                                     'invalid-pregnancy-identifier rows '
                                                                     'retained by the source. The label is '
                                                                     'not derived from IS_VALID, unmatched '
                                                                     'reason, pregnancy outcome or source '
                                                                     'presence.',
                                                    'source_system_code': 'MSDS source system.',
                                                    'source_update_timestamp': 'MSDS record-updated '
                                                                               'timestamp.',
                                                    'unmatched_reason': 'INVALID_PREGNANCY_ID or '
                                                                        'NO_MAT_PREGNANCY_MATCH.'},
                                        'table': 'Unmatched MSDS pregnancy rows documenting the person-spine '
                                                 'coverage gap.'}}
