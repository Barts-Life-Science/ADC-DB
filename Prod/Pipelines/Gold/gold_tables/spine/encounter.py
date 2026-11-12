"""Gold spine.encounter: 3 dataset(s).

- datasets: _gold_encounter_keys, _qc_spine_encounter, spine_encounter
- moved verbatim from: 1_gold_spine_and_reference:225, 1_gold_spine_and_reference:353, 1_gold_spine_and_reference:358, 1_gold_spine_and_reference:362, 1_gold_spine_and_reference:38, 1_gold_spine_and_reference:454, 1_gold_spine_and_reference:467

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from gold_core.gold_core import _n, _src, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== The spine keys every cross-table check is measured against ====

# One deduplicated key column per spine table, plus the person's own birth and
# death, declared once for reuse by cross-table checks.
# Temporary views share a definition; they do not guarantee one physical scan.

# INSR_W3 W3.02: replaced by gold_qc._person_scope
# GOLD_MV_SPLIT_V1: an internal MV (was a temporary view): every domain pipeline reads it for orphan nulling,
# and a temporary view cannot be read from another pipeline.
@dp.materialized_view(name=_n("gold_qc._encounter_keys"), refresh_policy="auto")
def _gold_encounter_keys():
    """Read distinct non-null Silver encounter keys for Gold foreign-key checks."""
    return (
        spark.read.table(_src("spine_encounter"))
        .select(F.col("encounter_id").alias("_qc_encounter_id"))
        .where(F.col("_qc_encounter_id").isNotNull())
        .distinct()  # GOLD_MV_SPLIT_V1: dropDuplicates plans First, which blocks incremental refresh
    )


# ==== journey_spine.encounter ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_ENCOUNTER_SELECT = [
    '`encounter_key` AS `encounter_key`',
    '`encounter_id` AS `encounter_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`parent_encounter_id` AS `parent_encounter_id`',
    '`parentage_status` AS `parentage_status`',
    '`encounter_level` AS `encounter_level`',
    "CASE WHEN UPPER(TRIM(CAST(`class_code` AS STRING))) = '0' THEN NULL ELSE `class_code` END AS `class_code`",
    '`class_display` AS `class_display`',
    "CASE WHEN UPPER(TRIM(CAST(`type_code` AS STRING))) = '0' THEN NULL ELSE `type_code` END AS `type_code`",
    '`type_display` AS `type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`type_class_code` AS STRING))) = '0' THEN NULL ELSE `type_class_code` END AS `type_class_code`",
    '`type_class_display` AS `type_class_display`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`period_start` AS `period_start`',
    '`period_end` AS `period_end`',
    '`period_start_local` AS `period_start_local`',  # GOLD_COLUMN_PARITY_V1
    '`period_end_local` AS `period_end_local`',  # GOLD_COLUMN_PARITY_V1
    '`arrival_method` AS `arrival_method`',
    '`arrival_confidence` AS `arrival_confidence`',
    '`departure_method` AS `departure_method`',
    '`departure_confidence` AS `departure_confidence`',
    '`class_actcode` AS `class_actcode`',  # SDI_ENCOUNTER_STATE_V1
    '`class_actcode_display` AS `class_actcode_display`',  # SDI_ENCOUNTER_STATE_V1
    '`class_actcode_method` AS `class_actcode_method`',  # SDI_ENCOUNTER_STATE_V1
    '`departure_rejected_reason` AS `departure_rejected_reason`',  # SDI_ENCOUNTER_STATE_V1
    '`last_activity_datetime` AS `last_activity_datetime`',  # SDI_ENCOUNTER_STATE_V1
    '`encounter_close_state` AS `encounter_close_state`',  # SDI_ENCOUNTER_STATE_V1
    '`length_of_stay_minutes` AS `length_of_stay_minutes`',
    '`scheduled_start` AS `scheduled_start`',
    'CASE WHEN `scheduled_start` IS NOT NULL AND `scheduled_end` IS NOT NULL AND `scheduled_start` > `scheduled_end` THEN NULL ELSE `scheduled_end` END AS `scheduled_end`',
    "CASE WHEN CAST(`registration_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`registration_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`registration_datetime` AS DATE)) < 9999 OR CAST(`registration_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`registration_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`registration_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `registration_datetime` END END AS `registration_datetime`",
    '`inpatient_admit_datetime` AS `inpatient_admit_datetime`',
    "CASE WHEN CAST(`discharge_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`discharge_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`discharge_datetime` AS DATE)) < 9999 THEN NULL ELSE `discharge_datetime` END END AS `discharge_datetime`",
    'CASE WHEN CAST(`workflow_complete_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `workflow_complete_datetime` END AS `workflow_complete_datetime`',
    '`raw_arrival_datetime` AS `raw_arrival_datetime`',
    '`raw_departure_datetime` AS `raw_departure_datetime`',
    "CASE WHEN UPPER(TRIM(CAST(`admission_source_code` AS STRING))) = '0' THEN NULL ELSE `admission_source_code` END AS `admission_source_code`",
    '`admission_source_display` AS `admission_source_display`',
    "CASE WHEN UPPER(TRIM(CAST(`discharge_destination_code` AS STRING))) = '0' THEN NULL ELSE `discharge_destination_code` END AS `discharge_destination_code`",
    '`discharge_destination_display` AS `discharge_destination_display`',
    "CASE WHEN UPPER(TRIM(CAST(`responsible_service_code` AS STRING))) = '0' THEN NULL ELSE `responsible_service_code` END AS `responsible_service_code`",
    '`responsible_service_display` AS `responsible_service_display`',
    "CASE WHEN UPPER(TRIM(CAST(`specialty_code` AS STRING))) = '0' THEN NULL ELSE `specialty_code` END AS `specialty_code`",
    '`specialty_display` AS `specialty_display`',
    '`current_location_key` AS `current_location_key`',
    '`organization_id` AS `organization_id`',
    '`service_provider_organization_key` AS `service_provider_organization_key`',
    '`reason_for_visit` AS `reason_for_visit`',
    '`attendance_evidence` AS `attendance_evidence`',
    '`attendance_witness_count` AS `attendance_witness_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
SPINE_ENCOUNTER_PERSON_SCOPE_RULES = {
    "gold.spine.encounter.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
SPINE_ENCOUNTER_ORPHAN_RULES = {
    "gold.spine.encounter.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._spine_encounter"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(SPINE_ENCOUNTER_PERSON_SCOPE_RULES)
@dp.expect_all(SPINE_ENCOUNTER_ORPHAN_RULES)
def _gold_qc_spine_encounter():
    """Quality-controlled twin of journey_spine.encounter."""
    # 18 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_encounter",
        SPINE_ENCOUNTER_SELECT,
        fk_columns=["person_id"],
    )
    return df


def _with_close_inference(df):
    """Join the inferred close once and place its two columns after encounter_close_state."""
    inferred = spark.read.table(_n("gold_qc._spine_encounter_close_inference")).select(
        F.col("encounter_id").alias("_ci_encounter_id"), "inferred_close_datetime", "inferred_close_method")
    columns = []
    for name in df.columns:
        columns.append(df[name])
        if name == "encounter_close_state":
            columns += [F.col("inferred_close_datetime"), F.col("inferred_close_method")]
    return df.join(inferred, df["encounter_id"] == inferred["_ci_encounter_id"], "left").select(*columns)


@dp.materialized_view(
    name=_n("gold_spine.encounter"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_spine_encounter():
    """Contract-v2 public twin of spine_encounter; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_encounter")).selectExpr(
        '`encounter_key` AS `encounter_key`',
        '`encounter_id` AS `encounter_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`parent_encounter_id` AS `parent_encounter_id`',
        '`parentage_status` AS `parentage_status`',
        '`encounter_level` AS `encounter_level`',
        '`class_code` AS `class_code`',
        '`class_display` AS `class_display`',
        '`type_code` AS `type_code`',
        '`type_display` AS `type_display`',
        '`type_class_code` AS `type_class_code`',
        '`type_class_display` AS `type_class_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`period_start_local` AS `period_start_local`',  # GOLD_COLUMN_PARITY_V1
        '`period_end_local` AS `period_end_local`',  # GOLD_COLUMN_PARITY_V1
        '`arrival_method` AS `arrival_method`',
        '`arrival_confidence` AS `arrival_confidence`',
        '`departure_method` AS `departure_method`',
        '`departure_confidence` AS `departure_confidence`',
        '`class_actcode` AS `class_actcode`',  # SDI_ENCOUNTER_STATE_V1
        '`class_actcode_display` AS `class_actcode_display`',  # SDI_ENCOUNTER_STATE_V1
        '`class_actcode_method` AS `class_actcode_method`',  # SDI_ENCOUNTER_STATE_V1
        '`departure_rejected_reason` AS `departure_rejected_reason`',  # SDI_ENCOUNTER_STATE_V1
        '`last_activity_datetime` AS `last_activity_datetime`',  # SDI_ENCOUNTER_STATE_V1
        '`encounter_close_state` AS `encounter_close_state`',  # SDI_ENCOUNTER_STATE_V1
        '`length_of_stay_minutes` AS `length_of_stay_minutes`',
        '`scheduled_start` AS `scheduled_start`',
        '`scheduled_end` AS `scheduled_end`',
        '`registration_datetime` AS `registration_datetime`',
        '`inpatient_admit_datetime` AS `inpatient_admit_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`workflow_complete_datetime` AS `workflow_complete_datetime`',
        '`raw_arrival_datetime` AS `raw_arrival_datetime`',
        '`raw_departure_datetime` AS `raw_departure_datetime`',
        '`admission_source_code` AS `admission_source_code`',
        '`admission_source_display` AS `admission_source_display`',
        '`discharge_destination_code` AS `discharge_destination_code`',
        '`discharge_destination_display` AS `discharge_destination_display`',
        '`responsible_service_code` AS `responsible_service_code`',
        '`responsible_service_display` AS `responsible_service_display`',
        '`specialty_code` AS `specialty_code`',
        '`specialty_display` AS `specialty_display`',
        '`current_location_key` AS `current_location_key`',
        '`organization_id` AS `organization_id`',
        '`service_provider_organization_key` AS `service_provider_organization_key`',
        '`reason_for_visit` AS `reason_for_visit`',
        '`attendance_evidence` AS `attendance_evidence`',
        '`attendance_witness_count` AS `attendance_witness_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    df = _with_close_inference(df)  # ENCOUNTER_CLOSE_INFERENCE_V1
    return df


def _public_steps_spine_encounter(df):
    """spine_encounter: the public body after its projection (GoldRules.public_select), moved from gold_spine_encounter; df is the projected twin frame."""
    df = _with_close_inference(df)
    return df


RULES = {
    'spine_encounter': GoldRules(
        table='spine_encounter',
        select=SPINE_ENCOUNTER_SELECT,
        person_scope=SPINE_ENCOUNTER_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['encounter_key'],
         'support_columns': ['record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        public_select=[
            '`encounter_key` AS `encounter_key`',
            '`encounter_id` AS `encounter_id`',
            '`subject_key` AS `subject_key`',
            '`subject_id_system` AS `subject_id_system`',
            '`person_id` AS `person_id`',
            '`parent_encounter_id` AS `parent_encounter_id`',
            '`parentage_status` AS `parentage_status`',
            '`encounter_level` AS `encounter_level`',
            '`class_code` AS `class_code`',
            '`class_display` AS `class_display`',
            '`type_code` AS `type_code`',
            '`type_display` AS `type_display`',
            '`type_class_code` AS `type_class_code`',
            '`type_class_display` AS `type_class_display`',
            '`status_code` AS `status_code`',
            '`status_display` AS `status_display`',
            '`period_start` AS `period_start`',
            '`period_end` AS `period_end`',
            '`period_start_local` AS `period_start_local`',
            '`period_end_local` AS `period_end_local`',
            '`arrival_method` AS `arrival_method`',
            '`arrival_confidence` AS `arrival_confidence`',
            '`departure_method` AS `departure_method`',
            '`departure_confidence` AS `departure_confidence`',
            '`class_actcode` AS `class_actcode`',
            '`class_actcode_display` AS `class_actcode_display`',
            '`class_actcode_method` AS `class_actcode_method`',
            '`departure_rejected_reason` AS `departure_rejected_reason`',
            '`last_activity_datetime` AS `last_activity_datetime`',
            '`encounter_close_state` AS `encounter_close_state`',
            '`length_of_stay_minutes` AS `length_of_stay_minutes`',
            '`scheduled_start` AS `scheduled_start`',
            '`scheduled_end` AS `scheduled_end`',
            '`registration_datetime` AS `registration_datetime`',
            '`inpatient_admit_datetime` AS `inpatient_admit_datetime`',
            '`discharge_datetime` AS `discharge_datetime`',
            '`workflow_complete_datetime` AS `workflow_complete_datetime`',
            '`raw_arrival_datetime` AS `raw_arrival_datetime`',
            '`raw_departure_datetime` AS `raw_departure_datetime`',
            '`admission_source_code` AS `admission_source_code`',
            '`admission_source_display` AS `admission_source_display`',
            '`discharge_destination_code` AS `discharge_destination_code`',
            '`discharge_destination_display` AS `discharge_destination_display`',
            '`responsible_service_code` AS `responsible_service_code`',
            '`responsible_service_display` AS `responsible_service_display`',
            '`specialty_code` AS `specialty_code`',
            '`specialty_display` AS `specialty_display`',
            '`current_location_key` AS `current_location_key`',
            '`organization_id` AS `organization_id`',
            '`service_provider_organization_key` AS `service_provider_organization_key`',
            '`reason_for_visit` AS `reason_for_visit`',
            '`attendance_evidence` AS `attendance_evidence`',
            '`attendance_witness_count` AS `attendance_witness_count`',
            '`confidentiality_code` AS `confidentiality_code`',
            '`vip_ind` AS `vip_ind`',
            '`record_status` AS `record_status`',
            '`record_status_effective_from` AS `record_status_effective_from`',
            '`record_status_effective_to` AS `record_status_effective_to`',
            '`source_update_timestamp` AS `source_update_timestamp`',
            '`loaded_at` AS `loaded_at`',
        ],
        public_steps=(_public_steps_spine_encounter,)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_gold_encounter_keys': {'columns': {}, 'quantity_override': False, 'table': None},
 '_qc_spine_encounter': {'columns': {'admission_source_code': 'Source admission source code.',
                                     'admission_source_display': 'Best available label for ADMIT_SRC_CD.',
                                     'arrival_confidence': 'Source-derived confidence for period_start.',
                                     'arrival_method': 'Method selecting period_start.',
                                     'attendance_evidence': 'Productised attendance evidence classification.',
                                     'attendance_witness_count': 'Number of attendance witnesses in the '
                                                                 'source product.',
                                     'class_actcode': 'HL7 v3 ActCode for the encounter class (the '
                                                      'vocabulary FHIR Encounter.class uses): IMP, AMB, EMER '
                                                      'or PRENC. From encounter_level for '
                                                      'spell/outpatient_attendance/emergency_visit/preadmission '
                                                      '(the type class is the stronger signal); from the '
                                                      'source class display for recurring_contact and other; '
                                                      'NULL for results_only, waiting_list_placeholder and '
                                                      'unmapped classes (Wait List has no ActCode). Local '
                                                      'codes are never relabelled; class_code stays the '
                                                      'Cerner code.',
                                     'class_actcode_display': 'HL7 ActCode display for class_actcode.',
                                     'class_actcode_method': 'How class_actcode was assigned: '
                                                             'encounter_level_crosswalk, '
                                                             'source_class_crosswalk or no_class '
                                                             '(class_actcode NULL). Never NULL.',
                                     'class_code': 'Source (local Cerner) encounter class code; see '
                                                   'class_actcode for the HL7 ActCode.',
                                     'class_display': 'Source encounter class display.',
                                     'confidentiality_code': 'Source confidentiality code.',
                                     'current_location_key': "Deterministic key of the encounter's current "
                                                             'nurse unit (joins '
                                                             'reference_location.location_key); null when '
                                                             'map_encounter.LOC_NURSE_UNIT_CD is null or 0 '
                                                             '(no unit recorded).',
                                     'departure_confidence': 'Source-derived confidence for period_end.',
                                     'departure_method': 'Method selecting period_end.',
                                     'departure_rejected_reason': 'Why bronze refused the winning departure '
                                                                  'candidate, leaving period_end NULL: '
                                                                  'BEFORE_ARRIVAL (it preceded the arrival), '
                                                                  'ADMINISTRATIVE_CLOSE (a nightly '
                                                                  'inactivity-timeout close) or '
                                                                  'FIXED_WINDOW_CLOSE (the 60-day outpatient '
                                                                  'close job). NULL when no candidate was '
                                                                  'refused. '
                                                                  'map_encounter.DEPARTURE_REJECTED_REASON.',
                                     'discharge_datetime': 'Source actual discharge timestamp.',
                                     'discharge_destination_code': 'Source discharge destination code.',
                                     'discharge_destination_display': 'Best available label for '
                                                                      'DISCH_TO_LOCTN_CD.',
                                     'encounter_close_state': 'Whether the encounter is known to be closed, '
                                                              'in precedence order: departed (period_end is '
                                                              'set); source_closed_no_observed_departure (an '
                                                              'administrative or fixed-window job closed it, '
                                                              'or the source status is Discharged with no '
                                                              'departure stamp; departure time unknown); '
                                                              'source_cancelled (source status Cancelled, '
                                                              'Cancelled Pending Arrival or Rejected Pending '
                                                              'Arrival: the encounter never ran); '
                                                              'source_not_arrived (source status Pending '
                                                              'Arrival: the patient never arrived); '
                                                              'departure_evidence_invalid (a departure stamp '
                                                              'precedes arrival, closure unknown); '
                                                              'open_ward_interval (the latest nurse-unit '
                                                              'interval has no end); no_departure_recorded '
                                                              '(source status still Active or Preadmit and '
                                                              'no departure evidence; open or abandoned, '
                                                              'judge with last_activity_datetime). Never '
                                                              'NULL; no reference to the current date.',
                                     'encounter_id': 'Deterministic encounter primary key.',
                                     'encounter_key': 'Source encounter primary key.',
                                     'encounter_level': 'Rules-light source-classified encounter level.',
                                     'inferred_close_datetime': 'Gold-inferred time by which a spell or '
                                                                'emergency visit had certainly ended, for '
                                                                'encounters the source still holds Active '
                                                                '(status 854) with encounter_close_state '
                                                                'no_departure_recorded, open_ward_interval '
                                                                'or departure_evidence_invalid. The earliest '
                                                                "of: the start of the person's next spell or "
                                                                "emergency visit after the stay's latest "
                                                                'evidence (the greater of period_start and '
                                                                "last_activity_datetime), and the person's "
                                                                'death at or after that evidence. An upper '
                                                                'bound, not the departure time '
                                                                '(last_activity_datetime is the lower '
                                                                'bound). NULL when there is no such evidence '
                                                                'or the encounter is not eligible. '
                                                                'encounter_close_state is unchanged source '
                                                                'truth.',
                                     'inferred_close_method': 'Evidence behind inferred_close_datetime: '
                                                              'later_spell, later_emergency_visit or death '
                                                              '(the earliest wins; on a tie death, then '
                                                              'later_spell). NULL when no closure was '
                                                              'inferred.',
                                     'inpatient_admit_datetime': 'Inpatient admission timestamp.',
                                     'last_activity_datetime': 'Latest recorded activity on the encounter '
                                                               '(UTC instant): greatest of the last current '
                                                               'clinical event, last active order and last '
                                                               'nurse-unit arrival. NULL when none exists. '
                                                               'Use it to judge whether an encounter with no '
                                                               'departure is still live.',
                                     'length_of_stay_minutes': 'Source-productised encounter duration.',
                                     'loaded_at': 'map_encounter.ADC_UPDT carried unchanged from the '
                                                  'contributing bronze encounter row. This is ingestion '
                                                  'provenance, not the Silver refresh time or encounter '
                                                  'arrival/departure; a missing source timestamp remains '
                                                  'null.',
                                     'organization_id': 'Source encounter organization reference.',
                                     'parent_encounter_id': 'Governed containment parent when supplied by '
                                                            'source evidence.',
                                     'parentage_status': 'Provenance state for encounter containment.',
                                     'period_end': 'Best observed encounter end.',
                                     'period_end_local': 'Best observed encounter end converted from UTC to '
                                                         'Europe/London wall-clock time (GMT/BST).',
                                     'period_start': 'Best observed encounter start.',
                                     'period_start_local': 'Best observed encounter start converted from UTC '
                                                           'to Europe/London wall-clock time (GMT/BST).',
                                     'person_id': 'Resolved Millennium person identifier.',
                                     'raw_arrival_datetime': 'Raw ARRIVE_DT_TM retained without asserting '
                                                             'observability.',
                                     'raw_departure_datetime': 'Raw DEPART_DT_TM retained without asserting '
                                                               'clinical meaning.',
                                     'reason_for_visit': 'Verbatim source reason for visit.',
                                     'record_status': 'Derived encounter-row status: superseded when '
                                                      'map_encounter.END_EFFECTIVE_DT_TM is earlier than '
                                                      '2100-01-01; otherwise active, including a null or '
                                                      'open-ended source timestamp. This classification does '
                                                      'not use the current wall clock or ACTIVE_IND and is '
                                                      'distinct from encounter workflow/discharge status.',
                                     'record_status_effective_from': 'map_encounter.ACTIVE_STATUS_DT_TM '
                                                                     'carried unchanged as the source '
                                                                     'active-status timestamp. No arrival, '
                                                                     'registration or ingestion-clock '
                                                                     'fallback is applied; a missing source '
                                                                     'value remains null.',
                                     'record_status_effective_to': 'map_encounter.END_EFFECTIVE_DT_TM when '
                                                                   'it is earlier than 2100-01-01; otherwise '
                                                                   'null, including missing values and '
                                                                   'open-ended sentinels. This is the '
                                                                   'row-version end boundary, not the '
                                                                   'encounter departure or discharge time.',
                                     'registration_datetime': 'Registration timestamp retained as source '
                                                              'evidence.',
                                     'responsible_service_code': 'Source medical service code.',
                                     'responsible_service_display': 'Best available label for '
                                                                    'MED_SERVICE_CD.',
                                     'scheduled_end': 'Scheduled departure timestamp.',
                                     'scheduled_start': 'Scheduled arrival timestamp.',
                                     'service_provider_organization_key': 'Source organization primarily '
                                                                          'responsible for the encounter.',
                                     'source_update_timestamp': 'map_encounter.UPDT_DT_TM carried unchanged '
                                                                'from the source encounter row. It is '
                                                                'separate from the bronze ADC_UPDT ingestion '
                                                                'clock and is not the Silver refresh time; a '
                                                                'missing source timestamp remains null.',
                                     'specialty_code': 'Source specialty-unit code.',
                                     'specialty_display': 'Source specialty-unit display.',
                                     'status_code': 'Native encounter status code.',
                                     'status_display': 'Native encounter status display.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Always-populated subject join key.',
                                     'type_class_code': 'Source encounter type-class code.',
                                     'type_class_display': 'Source encounter type-class display.',
                                     'type_code': 'Source encounter type code.',
                                     'type_display': 'Source encounter type display.',
                                     'vip_ind': 'Source VIP indicator.',
                                     'workflow_complete_datetime': 'Administrative workflow completion '
                                                                   'timestamp.'},
                         'quantity_override': True,
                         'table': 'Internal quality-controlled twin of spine_encounter: the Gold repairs, '
                                  'nulls and expectations are applied here and the published Gold MV reads '
                                  'this object. Materialized rather than temporary so the published MV can '
                                  'refresh incrementally.'},
 'spine_encounter': {'columns': {'admission_source_code': 'Source admission source code.',
                                 'admission_source_display': 'Best available label for ADMIT_SRC_CD.',
                                 'arrival_confidence': 'Source-derived confidence for period_start.',
                                 'arrival_method': 'Method selecting period_start.',
                                 'attendance_evidence': 'Productised attendance evidence classification.',
                                 'attendance_witness_count': 'Number of attendance witnesses in the source '
                                                             'product.',
                                 'class_actcode': 'HL7 v3 ActCode for the encounter class (the vocabulary '
                                                  'FHIR Encounter.class uses): IMP, AMB, EMER or PRENC. From '
                                                  'encounter_level for '
                                                  'spell/outpatient_attendance/emergency_visit/preadmission '
                                                  '(the type class is the stronger signal); from the source '
                                                  'class display for recurring_contact and other; NULL for '
                                                  'results_only, waiting_list_placeholder and unmapped '
                                                  'classes (Wait List has no ActCode). Local codes are never '
                                                  'relabelled; class_code stays the Cerner code.',
                                 'class_actcode_display': 'HL7 ActCode display for class_actcode.',
                                 'class_actcode_method': 'How class_actcode was assigned: '
                                                         'encounter_level_crosswalk, source_class_crosswalk '
                                                         'or no_class (class_actcode NULL). Never NULL.',
                                 'class_code': 'Source (local Cerner) encounter class code; see '
                                               'class_actcode for the HL7 ActCode.',
                                 'class_display': 'Source encounter class display.',
                                 'confidentiality_code': 'Source confidentiality code.',
                                 'current_location_key': "Deterministic key of the encounter's current nurse "
                                                         'unit (joins reference_location.location_key); null '
                                                         'when map_encounter.LOC_NURSE_UNIT_CD is null or 0 '
                                                         '(no unit recorded).',
                                 'departure_confidence': 'Source-derived confidence for period_end.',
                                 'departure_method': 'Method selecting period_end.',
                                 'departure_rejected_reason': 'Why bronze refused the winning departure '
                                                              'candidate, leaving period_end NULL: '
                                                              'BEFORE_ARRIVAL (it preceded the arrival), '
                                                              'ADMINISTRATIVE_CLOSE (a nightly '
                                                              'inactivity-timeout close) or '
                                                              'FIXED_WINDOW_CLOSE (the 60-day outpatient '
                                                              'close job). NULL when no candidate was '
                                                              'refused. '
                                                              'map_encounter.DEPARTURE_REJECTED_REASON.',
                                 'discharge_datetime': 'Source actual discharge timestamp.',
                                 'discharge_destination_code': 'Source discharge destination code.',
                                 'discharge_destination_display': 'Best available label for '
                                                                  'DISCH_TO_LOCTN_CD.',
                                 'encounter_close_state': 'Whether the encounter is known to be closed, in '
                                                          'precedence order: departed (period_end is set); '
                                                          'source_closed_no_observed_departure (an '
                                                          'administrative or fixed-window job closed it, or '
                                                          'the source status is Discharged with no departure '
                                                          'stamp; departure time unknown); source_cancelled '
                                                          '(source status Cancelled, Cancelled Pending '
                                                          'Arrival or Rejected Pending Arrival: the '
                                                          'encounter never ran); source_not_arrived (source '
                                                          'status Pending Arrival: the patient never '
                                                          'arrived); departure_evidence_invalid (a departure '
                                                          'stamp precedes arrival, closure unknown); '
                                                          'open_ward_interval (the latest nurse-unit '
                                                          'interval has no end); no_departure_recorded '
                                                          '(source status still Active or Preadmit and no '
                                                          'departure evidence; open or abandoned, judge with '
                                                          'last_activity_datetime). Never NULL; no reference '
                                                          'to the current date.',
                                 'encounter_id': 'Deterministic encounter primary key.',
                                 'encounter_key': 'Source encounter primary key.',
                                 'encounter_level': 'Rules-light source-classified encounter level.',
                                 'inferred_close_datetime': 'Gold-inferred time by which a spell or '
                                                            'emergency visit had certainly ended, for '
                                                            'encounters the source still holds Active '
                                                            '(status 854) with encounter_close_state '
                                                            'no_departure_recorded, open_ward_interval or '
                                                            'departure_evidence_invalid. The earliest of: '
                                                            "the start of the person's next spell or "
                                                            "emergency visit after the stay's latest "
                                                            'evidence (the greater of period_start and '
                                                            "last_activity_datetime), and the person's death "
                                                            'at or after that evidence. An upper bound, not '
                                                            'the departure time (last_activity_datetime is '
                                                            'the lower bound). NULL when there is no such '
                                                            'evidence or the encounter is not eligible. '
                                                            'encounter_close_state is unchanged source '
                                                            'truth.',
                                 'inferred_close_method': 'Evidence behind inferred_close_datetime: '
                                                          'later_spell, later_emergency_visit or death (the '
                                                          'earliest wins; on a tie death, then later_spell). '
                                                          'NULL when no closure was inferred.',
                                 'inpatient_admit_datetime': 'Inpatient admission timestamp.',
                                 'last_activity_datetime': 'Latest recorded activity on the encounter (UTC '
                                                           'instant): greatest of the last current clinical '
                                                           'event, last active order and last nurse-unit '
                                                           'arrival. NULL when none exists. Use it to judge '
                                                           'whether an encounter with no departure is still '
                                                           'live.',
                                 'length_of_stay_minutes': 'Source-productised encounter duration.',
                                 'loaded_at': 'map_encounter.ADC_UPDT carried unchanged from the '
                                              'contributing bronze encounter row. This is ingestion '
                                              'provenance, not the Silver refresh time or encounter '
                                              'arrival/departure; a missing source timestamp remains null.',
                                 'organization_id': 'Source encounter organization reference.',
                                 'parent_encounter_id': 'Governed containment parent when supplied by source '
                                                        'evidence.',
                                 'parentage_status': 'Provenance state for encounter containment.',
                                 'period_end': 'Best observed encounter end.',
                                 'period_end_local': 'Best observed encounter end converted from UTC to '
                                                     'Europe/London wall-clock time (GMT/BST).',
                                 'period_start': 'Best observed encounter start.',
                                 'period_start_local': 'Best observed encounter start converted from UTC to '
                                                       'Europe/London wall-clock time (GMT/BST).',
                                 'person_id': 'Resolved Millennium person identifier.',
                                 'raw_arrival_datetime': 'Raw ARRIVE_DT_TM retained without asserting '
                                                         'observability.',
                                 'raw_departure_datetime': 'Raw DEPART_DT_TM retained without asserting '
                                                           'clinical meaning.',
                                 'reason_for_visit': 'Verbatim source reason for visit.',
                                 'record_status': 'Derived encounter-row status: superseded when '
                                                  'map_encounter.END_EFFECTIVE_DT_TM is earlier than '
                                                  '2100-01-01; otherwise active, including a null or '
                                                  'open-ended source timestamp. This classification does not '
                                                  'use the current wall clock or ACTIVE_IND and is distinct '
                                                  'from encounter workflow/discharge status.',
                                 'record_status_effective_from': 'map_encounter.ACTIVE_STATUS_DT_TM carried '
                                                                 'unchanged as the source active-status '
                                                                 'timestamp. No arrival, registration or '
                                                                 'ingestion-clock fallback is applied; a '
                                                                 'missing source value remains null.',
                                 'record_status_effective_to': 'map_encounter.END_EFFECTIVE_DT_TM when it is '
                                                               'earlier than 2100-01-01; otherwise null, '
                                                               'including missing values and open-ended '
                                                               'sentinels. This is the row-version end '
                                                               'boundary, not the encounter departure or '
                                                               'discharge time.',
                                 'registration_datetime': 'Registration timestamp retained as source '
                                                          'evidence.',
                                 'responsible_service_code': 'Source medical service code.',
                                 'responsible_service_display': 'Best available label for MED_SERVICE_CD.',
                                 'scheduled_end': 'Scheduled departure timestamp.',
                                 'scheduled_start': 'Scheduled arrival timestamp.',
                                 'service_provider_organization_key': 'Source organization primarily '
                                                                      'responsible for the encounter.',
                                 'source_update_timestamp': 'map_encounter.UPDT_DT_TM carried unchanged from '
                                                            'the source encounter row. It is separate from '
                                                            'the bronze ADC_UPDT ingestion clock and is not '
                                                            'the Silver refresh time; a missing source '
                                                            'timestamp remains null.',
                                 'specialty_code': 'Source specialty-unit code.',
                                 'specialty_display': 'Source specialty-unit display.',
                                 'status_code': 'Native encounter status code.',
                                 'status_display': 'Native encounter status display.',
                                 'subject_id_system': 'Identifier system used for subject_key.',
                                 'subject_key': 'Always-populated subject join key.',
                                 'type_class_code': 'Source encounter type-class code.',
                                 'type_class_display': 'Source encounter type-class display.',
                                 'type_code': 'Source encounter type code.',
                                 'type_display': 'Source encounter type display.',
                                 'vip_ind': 'Source VIP indicator.',
                                 'workflow_complete_datetime': 'Administrative workflow completion '
                                                               'timestamp.'},
                     'quantity_override': False,
                     'table': 'One source encounter with evidence-based arrival and departure semantics; '
                              'containment remains explicit when unavailable. Gold QC twin of the silver '
                              'product: 13 columns are repaired or nulled, 0 check(s) are advisory. Each '
                              'rule states its reason in the pipeline notebook, and Lakeflow expectation '
                              'metrics report what every rule matched on each update.'}}
