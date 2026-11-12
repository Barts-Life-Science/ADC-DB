"""Silver activity.birth: 1 dataset(s).

- datasets: clinical_birth
- moved verbatim from: 4_silver_care_pathways:5088

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import _n
from silver_tables._common.care_pathways import _birth_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


@materialized_view(
    name=_n("journey_clinical.birth"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def birth():
    return _birth_canonical()


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_birth': {'columns': {'apgar_10_minute': 'Apgar score at 10 minutes as recorded (integer; '
                                                   'out-of-range values kept, judged downstream).',
                                'apgar_1_minute': 'Apgar score at 1 minute as recorded (integer; '
                                                  'out-of-range values kept, judged downstream).',
                                'apgar_5_minute': 'Apgar score at 5 minutes as recorded (integer; '
                                                  'out-of-range values kept, judged downstream).',
                                'baby_identity_status': 'resolved when baby_person_id is set; unresolved '
                                                        'otherwise.',
                                'baby_person_id': 'Baby Millennium PERSON_ID as BIGINT; NULL when the baby '
                                                  'has no Millennium person.',
                                'baby_subject_key': 'Subject key for the baby (person id, else NHS number, '
                                                    'else MRN); the identifiers themselves stay in bronze.',
                                'birth_datetime': 'Birth date/time as recorded (local wall-clock); NULL when '
                                                  'the source held only the year-2100 not-recorded '
                                                  'placeholder.',
                                'birth_datetime_placeholder_ind': 'True when bronze found the year-2100 '
                                                                  'not-recorded placeholder instead of a '
                                                                  'birth time.',
                                'birth_key': 'Deterministic SHA-256 key of BirthRow_ID (birth:mill).',
                                'birth_order': 'Birth order within the delivery.',
                                'birth_row_id': 'Millennium BirthRow_ID; native source key.',
                                'birth_sex': 'Baby sex at birth as recorded.',
                                'birth_weight_grams': 'Birth weight in grams as recorded.',
                                'birth_weight_implausible_ind': 'True when bronze flags the birth weight as '
                                                                'implausible.',
                                'birth_weight_source': 'Source of the birth weight as recorded.',
                                'construction_rule': 'Rule that built the row.',
                                'construction_version': 'Version of construction_rule.',
                                'delivery_method_code': 'Millennium delivery method code.',
                                'delivery_method_display': 'Millennium delivery method display.',
                                'delivery_outcome_code': 'Millennium delivery outcome code (live birth, '
                                                         'stillbirth, ...).',
                                'delivery_outcome_display': 'Millennium delivery outcome display.',
                                'fetus_number': 'Number of fetuses in the pregnancy.',
                                'gestation_days': 'Gestation days beyond the completed weeks as recorded '
                                                  '(NULL when not recorded; never defaulted).',
                                'gestation_implausible_ind': 'True when bronze flags the gestation as '
                                                             'impossible (weeks below 1 or above 45, or days '
                                                             'outside 0-6); weeks under 20 are not flagged.',
                                'gestation_source_value': 'Bronze GestationRaw: the gestation text '
                                                          'weeks/days were parsed from.',
                                'gestation_weeks': 'Completed gestation weeks as recorded (implausible '
                                                   'values kept, see gestation_implausible_ind).',
                                'journey_pregnancy_key': 'spine_journey key of the pregnancy '
                                                         '(journey:pregnancy over Pregnancy_ID).',
                                'load_batch_id': 'Bronze ADC_UPDT as yyyyMMddHHmmss.',
                                'loaded_at': 'Bronze ADC_UPDT; ingestion provenance.',
                                'neonatal_outcome_code': 'Millennium neonatal outcome code.',
                                'neonatal_outcome_display': 'Millennium neonatal outcome display.',
                                'patient_event_key': 'Same value as birth_key; the OMOP registry key stem '
                                                     '(bir:<birth_key>:<item>).',
                                'person_id': 'Mother Millennium PERSON_ID as BIGINT.',
                                'pregnancy_id': 'Millennium Pregnancy_ID.',
                                'pregnancy_outcome_code': 'Millennium pregnancy outcome code.',
                                'pregnancy_outcome_display': 'Millennium pregnancy outcome display.',
                                'presentation_code': 'Millennium presentation at delivery code.',
                                'presentation_display': 'Millennium presentation at delivery display.',
                                'record_status': 'active, or superseded when the source pregnancy row is '
                                                 'deleted.',
                                'source_row_id': 'BirthRow_ID as string.',
                                'source_system': 'millennium-maternity.',
                                'source_table': 'Bronze table read.',
                                'source_update_timestamp': 'Birth source record update time.',
                                'subject_id_system': 'Identifier system that produced subject_key.',
                                'subject_key': 'Always-populated subject key for the mother.'},
                    'table': 'One row per Millennium birth: delivery method/outcome, birth weight, Apgar, '
                             'gestation and baby identity linkage.'}}
