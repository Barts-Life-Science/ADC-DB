"""Silver activity.pregnancy_history: 1 dataset(s).

- datasets: clinical_pregnancy_history
- moved verbatim from: 4_silver_care_pathways:4896, 4_silver_care_pathways:4968

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, stable_id, subject_key_with_system
from silver_tables._common.care_pathways import _as_int
from silver_tables._common.journey import SRC_MAT_PREGNANCY, _mat_pregnancy_dedup


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


def _pregnancy_history_canonical():
    p = _mat_pregnancy_dedup()
    source_row_id = F.col("Pregnancy_ID").cast("string")
    skey, ssys = subject_key_with_system([("urn:cerner:person_id", F.col("Person_ID"))], SRC_MAT_PREGNANCY, source_row_id)
    deleted = F.coalesce(F.col("SOURCE_DELETED_IND").cast("boolean"), F.lit(False))
    return p.select(
        stable_id("pregnancy_history:mill", F.col("Pregnancy_ID")).alias("pregnancy_history_key"),
        stable_id("journey:pregnancy", F.col("Pregnancy_ID").cast("string")).alias("journey_pregnancy_key"),
        source_row_id.alias("pregnancy_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("Person_ID").cast("bigint").alias("person_id"),
        _as_int("Gravida_NBR").alias("gravida"),
        # MATDQ_SILVER_V1: bronze judges plausibility; silver carries the flag.
        F.coalesce(F.col("GravidaImplausible_IND").cast("boolean"), F.lit(False)).alias("gravida_implausible_ind"),
        _as_int("Parity").alias("parity"),
        _as_int("PrevLiveBirth_NBR").alias("previous_live_births"),
        _as_int("PrevMiscarriages_NBR").alias("previous_miscarriages"),
        _as_int("PrevStillBirth_NBR").alias("previous_stillbirths"),
        _as_int("PreviousCaesareanSections").alias("previous_caesarean_sections"),
        _as_int("PrevElectiveCaesareans").alias("previous_elective_caesareans"),
        _as_int("PrevEmergencyCaesareans").alias("previous_emergency_caesareans"),
        _as_int("PreviousLossesUnder24Weeks").alias("previous_losses_under_24_weeks"),
        F.to_date("FirstAntenatalAPPTDate").alias("first_antenatal_appointment_date"),
        F.to_date("ExpectedDeliveryDate").alias("expected_delivery_date"),
        F.col("ExpectedDeliveryDate_Source").cast("string").alias("expected_delivery_date_source"),
        F.col("LaborOnsetMethod_CD").cast("bigint").cast("string").alias("labour_onset_method_code"),
        F.col("LaborOnsetMethod_DESC").alias("labour_onset_method_display"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("mat-pregnancy-history").alias("construction_rule"),
        F.lit("0.2.0").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_PREGNANCY).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(F.col("ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("ADC_UPDT").alias("source_update_timestamp"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical.pregnancy_history"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def pregnancy_history():
    return _pregnancy_history_canonical()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_pregnancy_history': {'columns': {'construction_rule': 'Rule that built the row.',
                                            'construction_version': 'Version of construction_rule.',
                                            'expected_delivery_date': 'Expected delivery date.',
                                            'expected_delivery_date_source': 'Source of the expected '
                                                                             'delivery date as recorded.',
                                            'first_antenatal_appointment_date': 'Booking (first antenatal '
                                                                                'appointment) date.',
                                            'gravida': 'Gravida_NBR as recorded (integer; implausible values '
                                                       'kept, see gravida_implausible_ind).',
                                            'gravida_implausible_ind': 'True when bronze flags gravida as '
                                                                       'implausible (below 1, i.e. not '
                                                                       'recorded, or above 30).',
                                            'journey_pregnancy_key': 'spine_journey key of the pregnancy '
                                                                     '(journey:pregnancy over Pregnancy_ID).',
                                            'labour_onset_method_code': 'Millennium labour onset method '
                                                                        'code.',
                                            'labour_onset_method_display': 'Millennium labour onset method '
                                                                           'display.',
                                            'load_batch_id': 'Bronze ADC_UPDT as yyyyMMddHHmmss.',
                                            'loaded_at': 'Bronze ADC_UPDT of the winning pregnancy row; '
                                                         'ingestion provenance.',
                                            'parity': 'Parity as recorded (integer; implausible values kept, '
                                                      'judged downstream).',
                                            'person_id': 'Mother Millennium PERSON_ID as BIGINT.',
                                            'pregnancy_history_key': 'Deterministic SHA-256 key of the '
                                                                     'Millennium Pregnancy_ID '
                                                                     '(pregnancy_history:mill).',
                                            'pregnancy_id': 'Millennium Pregnancy_ID; native source key.',
                                            'previous_caesarean_sections': 'PreviousCaesareanSections as '
                                                                           'recorded (integer; implausible '
                                                                           'values kept, judged downstream).',
                                            'previous_elective_caesareans': 'PrevElectiveCaesareans as '
                                                                            'recorded (integer; implausible '
                                                                            'values kept, judged '
                                                                            'downstream).',
                                            'previous_emergency_caesareans': 'PrevEmergencyCaesareans as '
                                                                             'recorded (integer; implausible '
                                                                             'values kept, judged '
                                                                             'downstream).',
                                            'previous_live_births': 'PrevLiveBirth_NBR as recorded (integer; '
                                                                    'implausible values kept, judged '
                                                                    'downstream).',
                                            'previous_losses_under_24_weeks': 'PreviousLossesUnder24Weeks as '
                                                                              'recorded (integer; '
                                                                              'implausible values kept, '
                                                                              'judged downstream).',
                                            'previous_miscarriages': 'PrevMiscarriages_NBR as recorded '
                                                                     '(integer; implausible values kept, '
                                                                     'judged downstream).',
                                            'previous_stillbirths': 'PrevStillBirth_NBR as recorded '
                                                                    '(integer; implausible values kept, '
                                                                    'judged downstream).',
                                            'record_status': 'active, or superseded when the source row is '
                                                             'deleted.',
                                            'source_row_id': 'Pregnancy_ID as string.',
                                            'source_system': 'millennium-maternity.',
                                            'source_table': 'Bronze table read.',
                                            'source_update_timestamp': 'Bronze ADC_UPDT of the winning '
                                                                       'pregnancy row.',
                                            'subject_id_system': 'Identifier system that produced '
                                                                 'subject_key.',
                                            'subject_key': 'Always-populated subject key for the mother.'},
                                'table': 'Obstetric history per pregnancy (gravida, parity, previous '
                                         'births/losses/caesareans, booking and EDD) from Millennium '
                                         'maternity.'}}
