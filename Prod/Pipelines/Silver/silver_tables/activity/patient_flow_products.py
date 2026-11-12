"""Silver activity.patient_flow_products: 22 dataset(s).

- datasets: clinical_tracking_episode, _clinical_tracking_episode_metadata, clinical_tracking_attendance, _clinical_tracking_attendance_metadata, clinical_tracking_location_stay, _clinical_tracking_location_stay_metadata, clinical_tracking_milestone, _clinical_tracking_milestone_metadata, clinical_prearrival, _clinical_prearrival_metadata, clinical_pending_movement, _clinical_pending_movement_metadata ...
- moved verbatim from: 7_silver_patient_flow:153, 7_silver_patient_flow:178, 7_silver_patient_flow:191, 7_silver_patient_flow:35, 7_silver_patient_flow:36, 7_silver_patient_flow:37, 7_silver_patient_flow:39, 7_silver_patient_flow:41

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql import Column
import pyspark.sql.functions as F
from silver_core.active_rows import active_rows
from silver_core.journey_core import spark
from silver_core.value_policy import mill_fk


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")


INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")


BRONZE = spark.conf.get("journey.tdx_bronze_schema")


NULL_TOKEN = "~"


def _enc(value):
    # Encode a key component as trimmed base64, using a distinct token for nulls so separators
    # cannot collide.
    column = value if isinstance(value, Column) else F.lit(value)
    text_value = F.trim(column.cast("string"))
    return F.when(column.isNull(), F.lit(NULL_TOKEN)).otherwise(F.base64(text_value.cast("binary")))


def stable_id(namespace, *columns):
    # Hash the namespace and encoded key components into a deterministic SHA-256 identifier.
    return F.sha2(F.concat_ws(":", _enc(namespace), *[_enc(column) for column in columns]), 256)


def subject_key_with_system(person_id, source_table, source_row_id):
    # Use the Cerner person identifier when present; otherwise create a source-row-specific
    # unresolved subject key.
    present = person_id.isNotNull() & (F.trim(person_id.cast("string")) != "")
    return (
        F.when(present, stable_id("subject", "urn:cerner:person_id", person_id))
         .otherwise(stable_id("subject", "nosubject", source_table, source_row_id)),
        F.when(present, F.lit("urn:cerner:person_id")).otherwise(F.lit("nosubject")),
    )


def canonical(source_name, target_name, keys, key_name, namespace):
    # Normalize a configured patient-flow bronze feed: standardize names, construct stable keys,
    # and carry identity, record status and source-derived load timestamps.
    # ACTIVE_ROWS_V1: bronze rows that are not active never enter silver; the rule lives in silver_active_rows.py.
    source = active_rows(spark.read.table(f"{BRONZE}.{source_name}"), f"{BRONZE}.{source_name}")
    df = source.select(*[F.col(column).alias(column.lower()) for column in source.columns])
    if target_name == "clinical_prearrival":
        df = df.withColumnRenamed("linked_person_id", "person_id").withColumnRenamed("linked_encntr_id", "encounter_id")
    elif "encntr_id" in df.columns and "encounter_id" not in df.columns:
        df = df.withColumnRenamed("encntr_id", "encounter_id")
    if target_name in {"clinical_bed_status_observation", "reference_location_unit"} and "location_cd" in df.columns:
        df = df.withColumnRenamed("location_cd", "location_code")
        keys = ["location_code", "observation_date"] if target_name == "clinical_bed_status_observation" else ["location_code"]

    source_row = F.concat_ws(":", *[F.col(key).cast("string") for key in keys])
    df = df.withColumn(key_name, stable_id(namespace, *[F.col(key) for key in keys]))
    if target_name == "clinical_appointment_action":
        df = df.withColumn("appointment_patient_event_id", stable_id("appointment:mill_scheduling", F.col("sch_event_id")))

    if "source_present_ind" in df.columns:
        absent = ~F.coalesce(F.col("source_present_ind"), F.lit(True))
        df = df.withColumn("record_status", F.when(absent, "superseded").otherwise("active"))
        df = df.withColumn("record_status_effective_to", F.when(absent, F.col("source_absent_detected_ts")))
    else:
        df = df.withColumn("record_status", F.lit("active"))
        df = df.withColumn("record_status_effective_to", F.lit(None).cast("timestamp"))
    df = df.withColumn("record_status_effective_from", F.lit(None).cast("timestamp"))

    if "person_id" in df.columns:
        # INSR_W2 W2.02: subject_key hashes the source text form exactly as before; the published person_id is
        # BIGINT with Millennium's 0 ("no person") as NULL.
        person_text = F.col("person_id").cast("string")
        subject_key, subject_system = subject_key_with_system(person_text, F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
        df = df.withColumn("person_id", mill_fk("person_id"))
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
    else:
        df = df.withColumn("identity_status", F.lit("not_applicable"))
    if "encounter_id" in df.columns:
        # INSR_W2 W2.02: BIGINT, 0 -> NULL (no key hashes encounter_id in this notebook).
        df = df.withColumn("encounter_id", mill_fk("encounter_id"))
    if "source_adc_updt" in df.columns:
        df = df.withColumn("source_update_timestamp", F.col("source_adc_updt"))

    stable_loaded_at = (
        F.col("adc_updt") if "adc_updt" in df.columns
        else F.col("observed_at") if "observed_at" in df.columns
        else F.col("source_update_timestamp") if "source_update_timestamp" in df.columns
        else F.lit(None).cast("timestamp")
    )
    df = df.withColumn("loaded_at", stable_loaded_at)
    df = df.withColumn("load_batch_id", F.coalesce(F.date_format(stable_loaded_at, "yyyyMMddHHmmss"), F.lit("tdx-v21")))

    if target_name == "clinical_tracking_attendance":
        valid = F.col("checkin_dt_tm").isNotNull() & F.col("checkout_dt_tm_clean").isNotNull() & (F.col("checkout_dt_tm_clean") >= F.col("checkin_dt_tm"))
        df = df.withColumn("period_start", F.col("checkin_dt_tm")).withColumn("period_end", F.col("checkout_dt_tm_clean"))
        df = df.withColumn("dwell_minutes", F.when(valid, ((F.unix_timestamp("checkout_dt_tm_clean") - F.unix_timestamp("checkin_dt_tm")) / 60).cast("int")))
    elif target_name == "clinical_tracking_location_stay":
        valid = F.col("arrive_dt_tm").isNotNull() & F.col("depart_dt_tm_clean").isNotNull() & (F.col("depart_dt_tm_clean") >= F.col("arrive_dt_tm"))
        df = df.withColumn("period_start", F.col("arrive_dt_tm")).withColumn("period_end", F.col("depart_dt_tm_clean"))
        df = df.withColumn("stay_minutes", F.when(valid, ((F.unix_timestamp("depart_dt_tm_clean") - F.unix_timestamp("arrive_dt_tm")) / 60).cast("int")))
    elif target_name == "clinical_tracking_milestone":
        df = df.withColumn("milestone_datetime", F.coalesce("complete_dt_tm", "onset_dt_tm", "requested_dt_tm"))
    # INSR_W2 W2.02: zero codes/ids, system user and derived columns (silver_value_policy).
    df = finish_public(df, target_name)
    df = df
    # INSR_W2 W2.02: _with_comments writes a generic comment on every column; put back the policy's own
    # comments (<role>_system_ind, milestone_config_era, ...) so UC carries the real meaning.
    return df


def declare_product(source_name, target_name, keys, key_name, namespace):
    # Register a configured public product and its internal lifecycle and batch-metadata
    # companion.
    def build():
        # Build the configured product using the source, key columns and namespace captured by
        # this declaration.
        return canonical(source_name, target_name, list(keys), key_name, namespace)
    build.__name__ = f"build_{target_name}"
    globals()[build.__name__] = dp.materialized_view(
        name=f"{PUBLIC_SCHEMA}.{target_name}", refresh_policy="auto"
    )(build)

    def metadata():
        # Select the available identity, lifecycle and batch fields from the configured public
        # product.
        frame = spark.read.table(f"{PUBLIC_SCHEMA}.{target_name}")
        columns = [key_name, "identity_status", "linkage_route", "record_status", "record_status_effective_to", "load_batch_id", "loaded_at"]
        return frame.select(*[column for column in columns if column in frame.columns])
    metadata.__name__ = f"build_{target_name}_metadata"
    globals()[metadata.__name__] = dp.materialized_view(
        name=f"{INTERNAL_SCHEMA}._{target_name}_metadata",
        refresh_policy="auto",
    )(metadata)


FLOW_SPECS = [
    ("map_tracking_episode", "clinical_tracking_episode", ["tracking_id"], "tracking_episode_key", "tracking_episode:mill"),
    ("map_tracking_attendance", "clinical_tracking_attendance", ["tracking_checkin_id"], "tracking_attendance_key", "tracking_attendance:mill"),
    ("map_tracking_location_stay", "clinical_tracking_location_stay", ["tracking_locator_id"], "tracking_location_stay_key", "tracking_location_stay:mill"),
    ("map_tracking_milestone", "clinical_tracking_milestone", ["tracking_event_id"], "tracking_milestone_key", "tracking_milestone:mill"),
    ("map_prearrival", "clinical_prearrival", ["tracking_prearrival_id"], "prearrival_key", "prearrival:mill"),
    ("map_pending_movement", "clinical_pending_movement", ["encntr_pending_id"], "pending_movement_key", "pending_movement:mill"),
    ("map_appointment_action", "clinical_appointment_action", ["sch_action_id"], "appointment_action_key", "appointment_action:mill_scheduling"),
    ("map_bed_status_observation", "clinical_bed_status_observation", ["location_cd", "observation_date"], "bed_status_observation_key", "bed_status_observation:mill"),
    ("map_bed_observation_run", "clinical_bed_status_observation_coverage", ["observation_date"], "bed_status_coverage_key", "bed_status_coverage:mill"),
    ("map_location_unit", "reference_location_unit", ["location_cd"], "location_unit_key", "location_unit:mill"),
    ("map_location_attribute_history", "reference_location_attribute_history", ["attribute_row_id"], "location_attribute_key", "location_attribute:mill"),
]


for _spec in FLOW_SPECS:
    declare_product(*_spec)


VALUE_POLICIES = {
    'clinical_tracking_episode': ValuePolicy(table='clinical_tracking_episode', enabled=True, zero_as_null=('cur_tracking_locator_id', 'parent_entity_id', 'base_loc_cd')),
    'clinical_tracking_attendance': ValuePolicy(table='clinical_tracking_attendance', enabled=True, zero_as_null=('checkin_id', 'checkout_id', 'primary_doc_id', 'primary_nurse_id', 'reactivate_user_id', 'registration_status_id', 'secondary_doc_id', 'secondary_nurse_id', 'specialty_id', 'team_id', 'checkout_disposition_cd', 'family_present_cd'), system_user_columns={'reactivate_user_id': 'reactivate'}),
    'clinical_tracking_location_stay': ValuePolicy(table='clinical_tracking_location_stay', enabled=True, zero_as_null=('tracking_acuity_level_id', 'unavail_tracking_event_id', 'loc_bed_cd', 'loc_room_cd', 'tracking_reason_cd')),
    'clinical_tracking_milestone': ValuePolicy(table='clinical_tracking_milestone', enabled=True, zero_as_null=('complete_id', 'onset_id', 'requested_id', 'event_status_cd', 'tracking_event_type_cd')),
    'clinical_prearrival': ValuePolicy(table='clinical_prearrival', enabled=True, zero_as_null=('pcp_provider_id', 'referring_source_id', 'reg_prsnl_id', 'prearrival_type_cd', 'sex_cd')),
    'clinical_pending_movement': ValuePolicy(table='clinical_pending_movement', enabled=True, zero_as_null=('attenddoc_id', 'accommodation_cd', 'disch_disposition_cd', 'disch_to_loctn_cd', 'isolation_cd', 'med_service_cd', 'pend_bed_cd', 'pend_building_cd', 'pend_facility_cd', 'pend_nurse_unit_cd', 'pend_room_cd', 'pending_priority_cd', 'pending_status_cd', 'transaction_reason_cd')),
    'clinical_appointment_action': ValuePolicy(table='clinical_appointment_action', enabled=True, zero_as_null=('organization_id', 'req_action_id', 'schedule_id', 'resource_cd'), system_user_columns={'action_prsnl_id': 'action'}),
    'clinical_bed_status_observation': ValuePolicy(table='clinical_bed_status_observation', enabled=True, zero_as_null=('bed_status_cd',)),
    'reference_location_unit': ValuePolicy(table='reference_location_unit', enabled=True, zero_as_null=('current_bed_status_cd',)),
    'reference_location_attribute_history': ValuePolicy(table='reference_location_attribute_history', enabled=True, zero_as_null=('value_id', 'value_cd')),
}


# ---------------------------------------------------------------------------------------------------------------
# §2 registrations
# ---------------------------------------------------------------------------------------------------------------

# W2.02 (J26): the tracking milestone configuration changed on 2024-04-01; the era lets readers split series.
VALUE_POLICIES["clinical_tracking_milestone"].register("milestone_config_era", ["milestone_datetime"],
         lambda F: F.when(F.col("milestone_datetime") < F.lit("2024-04-01").cast("timestamp"), F.lit("pre_2024_04"))
                    .when(F.col("milestone_datetime").isNotNull(), F.lit("post_2024_04")),
         "Tracking milestone configuration era: pre_2024_04 or post_2024_04 by milestone_datetime (the "
         "configuration changed on 2024-04-01, so step changes at that date are configuration, not activity). "
         "NULL when milestone_datetime is NULL. INSR_W2.")


_value_policy = ValuePolicyBinding(VALUE_POLICIES)
finish_public = _value_policy.finish_public
apply_mill_zero_policy = _value_policy.apply_mill_zero_policy
apply_derived = _value_policy.apply_derived
apply_value_rules = _value_policy.apply_value_rules


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_appointment_action_metadata': {'columns': {'appointment_action_key': 'Deterministic SHA-256 key '
                                                                                 'of the appointment action '
                                                                                 'row (namespace plus native '
                                                                                 'source key).',
                                                       'identity_status': 'resolved when person_id is '
                                                                          'present, unresolved when absent, '
                                                                          'not_applicable for tables without '
                                                                          'a patient.',
                                                       'linkage_route': 'How the row was linked to its '
                                                                        'person or encounter.',
                                                       'load_batch_id': 'Load batch label derived from '
                                                                        'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                        'when no load time exists.',
                                                       'loaded_at': 'When the row was last written to bronze '
                                                                    '(ADC_UPDT), else its source update '
                                                                    'time; the stable load clock.',
                                                       'record_status': 'active, or superseded when the row '
                                                                        'stopped appearing in the source '
                                                                        'snapshot (source_present_ind '
                                                                        'false).',
                                                       'record_status_effective_to': 'When a superseded row '
                                                                                     'was first detected '
                                                                                     'absent from the source '
                                                                                     '(source_absent_detected_ts); '
                                                                                     'NULL while active.'},
                                           'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                    'clinical_appointment_action.'},
 '_clinical_bed_status_observation_coverage_metadata': {'columns': {'bed_status_coverage_key': 'Deterministic '
                                                                                               'SHA-256 key '
                                                                                               'of the bed '
                                                                                               'status '
                                                                                               'coverage row '
                                                                                               '(namespace '
                                                                                               'plus native '
                                                                                               'source key).',
                                                                    'identity_status': 'resolved when '
                                                                                       'person_id is '
                                                                                       'present, unresolved '
                                                                                       'when absent, '
                                                                                       'not_applicable for '
                                                                                       'tables without a '
                                                                                       'patient.',
                                                                    'load_batch_id': 'Load batch label '
                                                                                     'derived from loaded_at '
                                                                                     '(yyyyMMddHHmmss); '
                                                                                     'tdx-v21 when no load '
                                                                                     'time exists.',
                                                                    'loaded_at': 'When the row was last '
                                                                                 'written to bronze '
                                                                                 '(ADC_UPDT), else its '
                                                                                 'source update time; the '
                                                                                 'stable load clock.',
                                                                    'record_status': 'active, or superseded '
                                                                                     'when the row stopped '
                                                                                     'appearing in the '
                                                                                     'source snapshot '
                                                                                     '(source_present_ind '
                                                                                     'false).',
                                                                    'record_status_effective_to': 'When a '
                                                                                                  'superseded '
                                                                                                  'row was '
                                                                                                  'first '
                                                                                                  'detected '
                                                                                                  'absent '
                                                                                                  'from the '
                                                                                                  'source '
                                                                                                  '(source_absent_detected_ts); '
                                                                                                  'NULL '
                                                                                                  'while '
                                                                                                  'active.'},
                                                        'table': 'Internal TDX v2.1 quality and batch '
                                                                 'metadata for '
                                                                 'clinical_bed_status_observation_coverage.'},
 '_clinical_bed_status_observation_metadata': {'columns': {'bed_status_observation_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'bed status '
                                                                                         'observation row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).',
                                                           'identity_status': 'resolved when person_id is '
                                                                              'present, unresolved when '
                                                                              'absent, not_applicable for '
                                                                              'tables without a patient.',
                                                           'load_batch_id': 'Load batch label derived from '
                                                                            'loaded_at (yyyyMMddHHmmss); '
                                                                            'tdx-v21 when no load time '
                                                                            'exists.',
                                                           'loaded_at': 'When the row was last written to '
                                                                        'bronze (ADC_UPDT), else its source '
                                                                        'update time; the stable load clock.',
                                                           'record_status': 'active, or superseded when the '
                                                                            'row stopped appearing in the '
                                                                            'source snapshot '
                                                                            '(source_present_ind false).',
                                                           'record_status_effective_to': 'When a superseded '
                                                                                         'row was first '
                                                                                         'detected absent '
                                                                                         'from the source '
                                                                                         '(source_absent_detected_ts); '
                                                                                         'NULL while '
                                                                                         'active.'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_bed_status_observation.'},
 '_clinical_pending_movement_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                        'unresolved when absent, '
                                                                        'not_applicable for tables without a '
                                                                        'patient.',
                                                     'load_batch_id': 'Load batch label derived from '
                                                                      'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                      'when no load time exists.',
                                                     'loaded_at': 'When the row was last written to bronze '
                                                                  '(ADC_UPDT), else its source update time; '
                                                                  'the stable load clock.',
                                                     'pending_movement_key': 'Deterministic SHA-256 key of '
                                                                             'the pending movement row '
                                                                             '(namespace plus native source '
                                                                             'key).',
                                                     'record_status': 'active, or superseded when the row '
                                                                      'stopped appearing in the source '
                                                                      'snapshot (source_present_ind false).',
                                                     'record_status_effective_to': 'When a superseded row '
                                                                                   'was first detected '
                                                                                   'absent from the source '
                                                                                   '(source_absent_detected_ts); '
                                                                                   'NULL while active.'},
                                         'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                  'clinical_pending_movement.'},
 '_clinical_prearrival_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                  'unresolved when absent, not_applicable '
                                                                  'for tables without a patient.',
                                               'linkage_route': 'How the row was linked to its person or '
                                                                'encounter.',
                                               'load_batch_id': 'Load batch label derived from loaded_at '
                                                                '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                                'exists.',
                                               'loaded_at': 'When the row was last written to bronze '
                                                            '(ADC_UPDT), else its source update time; the '
                                                            'stable load clock.',
                                               'prearrival_key': 'Deterministic SHA-256 key of the '
                                                                 'prearrival row (namespace plus native '
                                                                 'source key).',
                                               'record_status': 'active, or superseded when the row stopped '
                                                                'appearing in the source snapshot '
                                                                '(source_present_ind false).',
                                               'record_status_effective_to': 'When a superseded row was '
                                                                             'first detected absent from the '
                                                                             'source '
                                                                             '(source_absent_detected_ts); '
                                                                             'NULL while active.'},
                                   'table': 'Internal TDX v2.1 quality and batch metadata for '
                                            'clinical_prearrival.'},
 '_clinical_tracking_attendance_metadata': {'columns': {'identity_status': 'resolved when person_id is '
                                                                           'present, unresolved when absent, '
                                                                           'not_applicable for tables '
                                                                           'without a patient.',
                                                        'linkage_route': 'How the row was linked to its '
                                                                         'person or encounter.',
                                                        'load_batch_id': 'Load batch label derived from '
                                                                         'loaded_at (yyyyMMddHHmmss); '
                                                                         'tdx-v21 when no load time exists.',
                                                        'loaded_at': 'When the row was last written to '
                                                                     'bronze (ADC_UPDT), else its source '
                                                                     'update time; the stable load clock.',
                                                        'record_status': 'active, or superseded when the row '
                                                                         'stopped appearing in the source '
                                                                         'snapshot (source_present_ind '
                                                                         'false).',
                                                        'record_status_effective_to': 'When a superseded row '
                                                                                      'was first detected '
                                                                                      'absent from the '
                                                                                      'source '
                                                                                      '(source_absent_detected_ts); '
                                                                                      'NULL while active.',
                                                        'tracking_attendance_key': 'Deterministic SHA-256 '
                                                                                   'key of the tracking '
                                                                                   'attendance row '
                                                                                   '(namespace plus native '
                                                                                   'source key).'},
                                            'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                     'clinical_tracking_attendance.'},
 '_clinical_tracking_episode_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                        'unresolved when absent, '
                                                                        'not_applicable for tables without a '
                                                                        'patient.',
                                                     'linkage_route': 'How the row was linked to its person '
                                                                      'or encounter.',
                                                     'load_batch_id': 'Load batch label derived from '
                                                                      'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                      'when no load time exists.',
                                                     'loaded_at': 'When the row was last written to bronze '
                                                                  '(ADC_UPDT), else its source update time; '
                                                                  'the stable load clock.',
                                                     'record_status': 'active, or superseded when the row '
                                                                      'stopped appearing in the source '
                                                                      'snapshot (source_present_ind false).',
                                                     'record_status_effective_to': 'When a superseded row '
                                                                                   'was first detected '
                                                                                   'absent from the source '
                                                                                   '(source_absent_detected_ts); '
                                                                                   'NULL while active.',
                                                     'tracking_episode_key': 'Deterministic SHA-256 key of '
                                                                             'the tracking episode row '
                                                                             '(namespace plus native source '
                                                                             'key).'},
                                         'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                  'clinical_tracking_episode.'},
 '_clinical_tracking_location_stay_metadata': {'columns': {'identity_status': 'resolved when person_id is '
                                                                              'present, unresolved when '
                                                                              'absent, not_applicable for '
                                                                              'tables without a patient.',
                                                           'linkage_route': 'How the row was linked to its '
                                                                            'person or encounter.',
                                                           'load_batch_id': 'Load batch label derived from '
                                                                            'loaded_at (yyyyMMddHHmmss); '
                                                                            'tdx-v21 when no load time '
                                                                            'exists.',
                                                           'loaded_at': 'When the row was last written to '
                                                                        'bronze (ADC_UPDT), else its source '
                                                                        'update time; the stable load clock.',
                                                           'record_status': 'active, or superseded when the '
                                                                            'row stopped appearing in the '
                                                                            'source snapshot '
                                                                            '(source_present_ind false).',
                                                           'record_status_effective_to': 'When a superseded '
                                                                                         'row was first '
                                                                                         'detected absent '
                                                                                         'from the source '
                                                                                         '(source_absent_detected_ts); '
                                                                                         'NULL while active.',
                                                           'tracking_location_stay_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'tracking location '
                                                                                         'stay row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_tracking_location_stay.'},
 '_clinical_tracking_milestone_metadata': {'columns': {'identity_status': 'resolved when person_id is '
                                                                          'present, unresolved when absent, '
                                                                          'not_applicable for tables without '
                                                                          'a patient.',
                                                       'linkage_route': 'How the row was linked to its '
                                                                        'person or encounter.',
                                                       'load_batch_id': 'Load batch label derived from '
                                                                        'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                        'when no load time exists.',
                                                       'loaded_at': 'When the row was last written to bronze '
                                                                    '(ADC_UPDT), else its source update '
                                                                    'time; the stable load clock.',
                                                       'record_status': 'active, or superseded when the row '
                                                                        'stopped appearing in the source '
                                                                        'snapshot (source_present_ind '
                                                                        'false).',
                                                       'record_status_effective_to': 'When a superseded row '
                                                                                     'was first detected '
                                                                                     'absent from the source '
                                                                                     '(source_absent_detected_ts); '
                                                                                     'NULL while active.',
                                                       'tracking_milestone_key': 'Deterministic SHA-256 key '
                                                                                 'of the tracking milestone '
                                                                                 'row (namespace plus native '
                                                                                 'source key).'},
                                           'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                    'clinical_tracking_milestone.'},
 '_reference_location_attribute_history_metadata': {'columns': {'identity_status': 'resolved when person_id '
                                                                                   'is present, unresolved '
                                                                                   'when absent, '
                                                                                   'not_applicable for '
                                                                                   'tables without a '
                                                                                   'patient.',
                                                                'load_batch_id': 'Load batch label derived '
                                                                                 'from loaded_at '
                                                                                 '(yyyyMMddHHmmss); tdx-v21 '
                                                                                 'when no load time exists.',
                                                                'loaded_at': 'When the row was last written '
                                                                             'to bronze (ADC_UPDT), else its '
                                                                             'source update time; the stable '
                                                                             'load clock.',
                                                                'location_attribute_key': 'Deterministic '
                                                                                          'SHA-256 key of '
                                                                                          'the location '
                                                                                          'attribute row '
                                                                                          '(namespace plus '
                                                                                          'native source '
                                                                                          'key).',
                                                                'record_status': 'active, or superseded when '
                                                                                 'the row stopped appearing '
                                                                                 'in the source snapshot '
                                                                                 '(source_present_ind '
                                                                                 'false).',
                                                                'record_status_effective_to': 'When a '
                                                                                              'superseded '
                                                                                              'row was first '
                                                                                              'detected '
                                                                                              'absent from '
                                                                                              'the source '
                                                                                              '(source_absent_detected_ts); '
                                                                                              'NULL while '
                                                                                              'active.'},
                                                    'table': 'Internal TDX v2.1 quality and batch metadata '
                                                             'for reference_location_attribute_history.'},
 '_reference_location_unit_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                      'unresolved when absent, '
                                                                      'not_applicable for tables without a '
                                                                      'patient.',
                                                   'load_batch_id': 'Load batch label derived from loaded_at '
                                                                    '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                    'time exists.',
                                                   'loaded_at': 'When the row was last written to bronze '
                                                                '(ADC_UPDT), else its source update time; '
                                                                'the stable load clock.',
                                                   'location_unit_key': 'Deterministic SHA-256 key of the '
                                                                        'location unit row (namespace plus '
                                                                        'native source key).',
                                                   'record_status': 'active, or superseded when the row '
                                                                    'stopped appearing in the source '
                                                                    'snapshot (source_present_ind false).',
                                                   'record_status_effective_to': 'When a superseded row was '
                                                                                 'first detected absent from '
                                                                                 'the source '
                                                                                 '(source_absent_detected_ts); '
                                                                                 'NULL while active.'},
                                       'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                'reference_location_unit.'},
 'clinical_appointment_action': {'columns': {'action_dt_tm': 'Date and time the scheduling action was taken.',
                                             'action_meaning': 'Meaning string corresponding to the '
                                                               'scheduling action code.',
                                             'action_prsnl_id': 'Identifier of the personnel member who '
                                                                'performed the scheduling action.',
                                             'action_source_cd': 'Code value identifying the source from '
                                                                 'which the scheduling action originated.',
                                             'action_source_display': 'Display text describing the source of '
                                                                      'the appointment action, resolved from '
                                                                      'a Millennium code value.',
                                             'action_system_ind': 'True when the source action_prsnl_id was '
                                                                  '1, the Cerner system user (J5); the id is '
                                                                  'then published as NULL. NULL when the '
                                                                  'source id was NULL. INSR_W2.',
                                             'active_ind': 'Indicator showing whether the scheduling event '
                                                           'action record is active.',
                                             'adc_updt': 'When bronze last wrote the row.',
                                             'appointment_action_key': 'Deterministic SHA-256 key of the '
                                                                       'appointment action row (namespace '
                                                                       'plus native source key).',
                                             'appointment_patient_event_id': 'Identifier of the scheduling '
                                                                             'appointment event that this '
                                                                             'appointment action relates to, '
                                                                             'sourced from the Millennium '
                                                                             'SCH_EVENT_ID field.',
                                             'appt_synonym_cd': 'Code value identifying the appointment '
                                                                'synonym associated with the scheduling '
                                                                'action.',
                                             'appt_synonym_display': 'Display text of the appointment type '
                                                                     'synonym associated with the '
                                                                     'appointment action, resolved from a '
                                                                     'Millennium code value.',
                                             'breach_offset_days': 'Number of days of breach offset recorded '
                                                                   'for this scheduling event action.',
                                             'contact_follow_up_dt_tm': 'Date and time recorded for a '
                                                                        'follow-up of a patient contact '
                                                                        'associated with the scheduling '
                                                                        'action.',
                                             'contact_outcome_cd': 'Code value identifying the recorded '
                                                                   'outcome of a patient contact associated '
                                                                   'with the scheduling action.',
                                             'contact_outcome_display': 'Display text of the contact outcome '
                                                                        'recorded for the appointment '
                                                                        'action, resolved from a Millennium '
                                                                        'code value.',
                                             'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                             'belongs to (BIGINT); NULL when not recorded.',
                                             'identity_status': 'resolved when person_id is present, '
                                                                'unresolved when absent, not_applicable for '
                                                                'tables without a patient.',
                                             'linkage_route': 'How the row was linked to its person or '
                                                              'encounter.',
                                             'load_batch_id': 'Load batch label derived from loaded_at '
                                                              '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                              'exists.',
                                             'loaded_at': 'When the row was last written to bronze '
                                                          '(ADC_UPDT), else its source update time; the '
                                                          'stable load clock.',
                                             'organization_id': 'Identifier of the organization associated '
                                                                'with this scheduling event action, '
                                                                'populated with a resolved organization '
                                                                'during trust mapping.',
                                             'orig_action_id': 'Identifier of the original scheduling action '
                                                               'from which this action was derived.',
                                             'patient_deceased_ind': 'Indicator flagging whether the patient '
                                                                     'associated with the scheduling event '
                                                                     'was recorded as deceased at the time '
                                                                     'of the action.',
                                             'perform_dt_tm': 'Date and time the scheduling action was '
                                                              'performed.',
                                             'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                          'NULL when the source row names no patient.',
                                             'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                'row.',
                                             'reason_meaning': 'Meaning string corresponding to the '
                                                               'scheduling action reason code.',
                                             'record_status': 'active, or superseded when the row stopped '
                                                              'appearing in the source snapshot '
                                                              '(source_present_ind false).',
                                             'record_status_effective_from': 'Start of the record status; '
                                                                             'not supplied by this source '
                                                                             '(always NULL).',
                                             'record_status_effective_to': 'When a superseded row was first '
                                                                           'detected absent from the source '
                                                                           '(source_absent_detected_ts); '
                                                                           'NULL while active.',
                                             'req_action_id': 'Identifier of the request action linked to '
                                                              'this scheduling event action.',
                                             'req_action_meaning': 'Cerner code meaning text corresponding '
                                                                   'to REQ_ACTION_CD for the requested '
                                                                   'action.',
                                             'resource_cd': 'Code value identifying the scheduling resource '
                                                            'associated with this scheduling event action.',
                                             'resource_display': 'Display text of the scheduling resource '
                                                                 'associated with the appointment action, '
                                                                 'resolved from a Millennium code value.',
                                             'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                         'bronze MERGE to detect changes.',
                                             'sch_action_cd': 'Code value identifying the type of scheduling '
                                                              'action performed on the event.',
                                             'sch_action_display': 'Display text of the scheduling action '
                                                                   'performed on the appointment, resolved '
                                                                   'from a Millennium code value.',
                                             'sch_action_id': 'Internal surrogate identifier uniquely '
                                                              'identifying a scheduling event action record.',
                                             'sch_event_id': 'Identifier of the scheduling event '
                                                             '(appointment) to which this action applies.',
                                             'sch_reason_cd': 'Code value identifying the reason recorded '
                                                              'for the scheduling action.',
                                             'sch_reason_display': 'Display text of the scheduling reason '
                                                                   'recorded for the appointment action, '
                                                                   'resolved from a Millennium code value.',
                                             'schedule_id': 'Identifier of the schedule associated with the '
                                                            'scheduling event action.',
                                             'source_absent_detected_ts': 'When bronze first found the row '
                                                                          'absent from the source snapshot; '
                                                                          'NULL while present.',
                                             'source_adc_updt': 'When the source row was landed in raw (raw '
                                                                'ADC_UPDT).',
                                             'source_present_ind': 'True while the row is present in the '
                                                                   'latest full source snapshot.',
                                             'source_row_id': 'Native source row identifier, as text.',
                                             'source_table': 'Bronze or raw source table the row came from.',
                                             'source_update_timestamp': 'Time the source row was landed in '
                                                                        'the raw layer (bronze '
                                                                        'SOURCE_ADC_UPDT), not when it '
                                                                        'changed in the source system; do '
                                                                        'not use it for recency.',
                                             'subject_id_system': 'System of subject_key: '
                                                                  'urn:cerner:person_id, or nosubject when '
                                                                  'the row names no patient.',
                                             'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                            'person id, or of the source row when no person '
                                                            'is recorded.',
                                             'trust': 'Name of the NHS trust to which the record has been '
                                                      'attributed by the ingestion trust-resolution process.',
                                             'ver_status_meaning': 'Cerner code meaning text corresponding '
                                                                   'to VER_STATUS_CD for the verification '
                                                                   'status.'},
                                 'table': 'One row per Millennium scheduling action on an appointment (book, '
                                          'confirm, check in, cancel, undo), keyed by SCH_ACTION_ID.'},
 'clinical_bed_status_observation': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'bed_active_ind': 'Indicator showing whether the source bed '
                                                                   'record is active, derived from the '
                                                                   'Millennium bed table active indicator.',
                                                 'bed_status_cd': 'Code value representing the status of '
                                                                  'this bed.',
                                                 'bed_status_display': 'Display text describing the status '
                                                                       'of the bed at the time of '
                                                                       'observation, derived from the source '
                                                                       'bed status observation and '
                                                                       'Millennium code value lookup.',
                                                 'bed_status_observation_key': 'Deterministic SHA-256 key of '
                                                                               'the bed status observation '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'bed_updt_dt_tm': 'Date and time the source bed record was '
                                                                   'last updated, taken from the Millennium '
                                                                   'bed table update timestamp.',
                                                 'dup_bed_ind': 'Indicator flagging whether this bed is a '
                                                                'duplicate bed.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'loc_room_cd': 'Code value identifying the parent room '
                                                                'location of this bed.',
                                                 'location_code': 'Code value identifying the location '
                                                                  'associated with the bed, sourced from the '
                                                                  'Millennium bed table location code.',
                                                 'nurse_unit_cd': 'Code identifying the nursing unit (ward) '
                                                                  'associated with the bed status '
                                                                  'observation.',
                                                 'nurse_unit_display': 'Display name of the nursing unit in '
                                                                       'which the observed bed is located, '
                                                                       'derived from source observation, '
                                                                       'location unit mapping and Millennium '
                                                                       'code value lookup.',
                                                 'observation_date': 'Date of the weekly bed-observation run '
                                                                     'that captured this status: the run '
                                                                     'date, or the observation_date job '
                                                                     'parameter when one was given.',
                                                 'observed_at': 'Date and time at which the bed status '
                                                                'observation was recorded, taken from the '
                                                                'observation timestamp in the source bed '
                                                                'status observation.',
                                                 'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                    'row.',
                                                 'record_status': 'active, or superseded when the row '
                                                                  'stopped appearing in the source snapshot '
                                                                  '(source_present_ind false).',
                                                 'record_status_effective_from': 'Start of the record '
                                                                                 'status; not supplied by '
                                                                                 'this source (always NULL).',
                                                 'record_status_effective_to': 'When a superseded row was '
                                                                               'first detected absent from '
                                                                               'the source '
                                                                               '(source_absent_detected_ts); '
                                                                               'NULL while active.',
                                                 'room_location_cd': 'Code identifying the room location '
                                                                     'associated with the bed status '
                                                                     'observation.',
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_delta_version': 'Delta version of the raw bed table '
                                                                         'read by the weekly bed-observation '
                                                                         'run.',
                                                 'source_last_utc_ts': 'Most recent timestamp, in UTC, '
                                                                       'recorded on the source Millennium '
                                                                       'bed record, carried through '
                                                                       'unchanged from the source.',
                                                 'source_present_ind': 'True while the row is present in the '
                                                                       'latest full source snapshot.',
                                                 'source_row_id': 'Native source row identifier, as text.',
                                                 'source_table': 'Bronze or raw source table the row came '
                                                                 'from.',
                                                 'source_update_timestamp': 'Time the source row was landed '
                                                                            'in the raw layer (bronze '
                                                                            'SOURCE_ADC_UPDT), not when it '
                                                                            'changed in the source system; '
                                                                            'do not use it for recency.'},
                                     'table': 'One row per bed location and weekly observation date: the '
                                              "bed's current status as captured by the weekly "
                                              'bed-observation run (snapshot, not within-week history).'},
 'clinical_bed_status_observation_coverage': {'columns': {'bed_status_coverage_key': 'Deterministic SHA-256 '
                                                                                     'key of the bed status '
                                                                                     'coverage row '
                                                                                     '(namespace plus native '
                                                                                     'source key).',
                                                          'beds_active': 'Count of beds considered active '
                                                                         'for the bed status observation '
                                                                         'run, sourced from the bed '
                                                                         'observation run record.',
                                                          'beds_observed': 'Count of beds for which a status '
                                                                           'observation was captured in the '
                                                                           'observation run.',
                                                          'identity_status': 'resolved when person_id is '
                                                                             'present, unresolved when '
                                                                             'absent, not_applicable for '
                                                                             'tables without a patient.',
                                                          'load_batch_id': 'Load batch label derived from '
                                                                           'loaded_at (yyyyMMddHHmmss); '
                                                                           'tdx-v21 when no load time '
                                                                           'exists.',
                                                          'loaded_at': 'When the row was last written to '
                                                                       'bronze (ADC_UPDT), else its source '
                                                                       'update time; the stable load clock.',
                                                          'observation_date': 'Calendar date to which the '
                                                                              'bed status observation run '
                                                                              'applies.',
                                                          'observed_at': 'Timestamp at which the bed status '
                                                                         'observation run was performed.',
                                                          'pipeline_run_id': 'Bronze pipeline run that last '
                                                                             'wrote the row.',
                                                          'reason': 'Explanation recorded with the '
                                                                    'observation run status, sourced from '
                                                                    'the bed observation run record.',
                                                          'record_status': 'active, or superseded when the '
                                                                           'row stopped appearing in the '
                                                                           'source snapshot '
                                                                           '(source_present_ind false).',
                                                          'record_status_effective_from': 'Start of the '
                                                                                          'record status; '
                                                                                          'not supplied by '
                                                                                          'this source '
                                                                                          '(always NULL).',
                                                          'record_status_effective_to': 'When a superseded '
                                                                                        'row was first '
                                                                                        'detected absent '
                                                                                        'from the source '
                                                                                        '(source_absent_detected_ts); '
                                                                                        'NULL while active.',
                                                          'source_delta_version': 'Delta version of the raw '
                                                                                  'bed table read by the '
                                                                                  'weekly bed-observation '
                                                                                  'run.',
                                                          'status': 'Outcome status of the bed status '
                                                                    'observation run, sourced from the bed '
                                                                    'observation run record.'},
                                              'table': 'One row per weekly bed-observation run date, '
                                                       'recording whether the snapshot was taken.'},
 'clinical_pending_movement': {'columns': {'accommodation_cd': 'Code value identifying the accommodation '
                                                               'type associated with the pending encounter '
                                                               'action.',
                                           'accommodation_display': 'Display text of the accommodation code '
                                                                    'for the pending patient movement, '
                                                                    'resolved from the Millennium code value '
                                                                    'lookup.',
                                           'accommodation_reason_cd': 'Code value identifying the reason for '
                                                                      'the accommodation associated with the '
                                                                      'pending encounter action.',
                                           'active_ind': 'Indicator showing whether the pending encounter '
                                                         'record is currently active in Cerner Millennium.',
                                           'adc_updt': 'When bronze last wrote the row.',
                                           'alc_decomp_dt_tm': 'Date and time associated with the alternate '
                                                               'level of care decompensation for the pending '
                                                               'encounter action.',
                                           'alc_reason_cd': 'Code value identifying the reason for the '
                                                            'alternate level of care designation.',
                                           'alt_lvl_care_cd': 'Code value identifying the alternate level of '
                                                              'care associated with the pending encounter '
                                                              'action.',
                                           'alt_lvl_care_dt_tm': 'Date and time associated with the '
                                                                 'alternate level of care designation for '
                                                                 'the pending encounter action.',
                                           'anon_context_fingerprint': 'Hash of the contextual inputs the '
                                                                       'redactor used for this row; the '
                                                                       'anonymised text is released only '
                                                                       'while it matches the current '
                                                                       'context.',
                                           'anon_identity_fingerprint': 'Hash of the patient identity '
                                                                        'details the record-driven redactor '
                                                                        'used for this row.',
                                           'anon_processed_at': "When the redactor last processed this row's "
                                                                'text.',
                                           'anon_redaction_count': 'Number of redactions the redactor '
                                                                   'applied to the source text.',
                                           'anon_redactor_version': 'Version of the redactor that produced '
                                                                    'the anonymised text.',
                                           'anon_source_text_sha': 'SHA-256 digest of the text the redactor '
                                                                   'processed; when it no longer matches the '
                                                                   'current input digest the anonymised text '
                                                                   'is stale.',
                                           'anon_status': "Outcome of the anonymisation run for this row's "
                                                          'free text (anonymized when it succeeded).',
                                           'anon_transaction_reason': 'Anonymised version of '
                                                                      'transaction_reason: identifiers '
                                                                      'detected in the free text are '
                                                                      'replaced by placeholders; published '
                                                                      'alongside the source column, which '
                                                                      'gold releases only when anonymisation '
                                                                      'succeeded.',
                                           'attenddoc_id': 'Internal personnel identifier of the attending '
                                                           'clinician associated with the pending encounter '
                                                           'action.',
                                           'beg_effective_dt_tm': 'Date and time from which the pending '
                                                                  'encounter record is effective.',
                                           'disch_disposition_cd': 'Code value identifying the discharge '
                                                                   'disposition recorded for the pending '
                                                                   'encounter action.',
                                           'disch_disposition_display': 'Display text of the discharge '
                                                                        'disposition code for the pending '
                                                                        'movement, resolved from the '
                                                                        'Millennium code value lookup.',
                                           'disch_to_loctn_cd': 'Code value identifying the location the '
                                                                'patient is to be discharged to for the '
                                                                'pending encounter action.',
                                           'disch_to_loctn_display': 'Display text of the discharge-to '
                                                                     'location code for the pending '
                                                                     'movement, resolved from the Millennium '
                                                                     'code value lookup.',
                                           'encntr_pending_id': 'Unique system-generated identifier for each '
                                                                'pending encounter action record in the '
                                                                'ENCNTR_PENDING table.',
                                           'encntr_type_cd': 'Code value identifying the encounter type '
                                                             'associated with the pending encounter action.',
                                           'encntr_type_display': 'Display text of the encounter type code '
                                                                  'associated with the pending movement, '
                                                                  'resolved from the Millennium code value '
                                                                  'lookup.',
                                           'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                           'belongs to (BIGINT); NULL when not recorded.',
                                           'end_effective_dt_tm': 'Millennium END_EFFECTIVE_DT_TM of the '
                                                                  'pending movement; 2100-12-31 is the '
                                                                  'source marker for a request that is still '
                                                                  'open and is kept on purpose (J14). Use '
                                                                  'end_effective_dt_tm_clean for a '
                                                                  'NULL-for-open version.',
                                           'end_effective_dt_tm_clean': 'Cleaned end-effective date and time '
                                                                        'of the pending encounter movement '
                                                                        'record, derived from the Millennium '
                                                                        'encounter-pending end-effective '
                                                                        'timestamp marking when the record '
                                                                        'ceases to be valid.',
                                           'est_complete_dt_tm': 'Estimated date and time at which the '
                                                                 'pending encounter action is expected to be '
                                                                 'completed.',
                                           'identity_status': 'resolved when person_id is present, '
                                                              'unresolved when absent, not_applicable for '
                                                              'tables without a patient.',
                                           'isolation_cd': 'Code value identifying the isolation requirement '
                                                           'associated with the pending encounter action.',
                                           'isolation_display': 'Display text of the isolation code recorded '
                                                                'for the pending movement, resolved from the '
                                                                'Millennium code value lookup.',
                                           'load_batch_id': 'Load batch label derived from loaded_at '
                                                            '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                            'exists.',
                                           'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                        'else its source update time; the stable load clock.',
                                           'med_service_cd': 'Code value identifying the medical service '
                                                             'associated with the pending encounter action.',
                                           'med_service_display': 'Display text of the medical service code '
                                                                  'for the pending movement, resolved from '
                                                                  'the Millennium code value lookup.',
                                           'organization_id': 'Internal Millennium organisation identifier '
                                                              '(facility or client organisation) attributed '
                                                              'to the pending encounter record during '
                                                              'loading, derived via '
                                                              'encounter-to-organisation mapping from the '
                                                              'linked encounter.',
                                           'pend_bed_cd': 'Location code value identifying the bed '
                                                          'associated with the pending encounter action.',
                                           'pend_bed_display': 'Display text of the bed location code for '
                                                               'the pending movement, resolved from the '
                                                               'Millennium code value lookup.',
                                           'pend_building_cd': 'Location code value identifying the building '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pend_building_display': 'Display text of the building location '
                                                                    'code for the pending movement, resolved '
                                                                    'from the Millennium code value lookup.',
                                           'pend_facility_cd': 'Location code value identifying the facility '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pend_facility_display': 'Display text of the facility location '
                                                                    'code for the pending movement, resolved '
                                                                    'from the Millennium code value lookup.',
                                           'pend_nurse_unit_cd': 'Location code value identifying the nurse '
                                                                 'unit associated with the pending encounter '
                                                                 'action.',
                                           'pend_nurse_unit_display': 'Display text of the nurse unit '
                                                                      'location code for the pending '
                                                                      'movement, resolved from the '
                                                                      'Millennium code value lookup.',
                                           'pend_room_cd': 'Location code value identifying the room '
                                                           'associated with the pending encounter action.',
                                           'pend_room_display': 'Display text of the room location code for '
                                                                'the pending movement, resolved from the '
                                                                'Millennium code value lookup.',
                                           'pending_dt_tm': 'Date and time associated with the pending '
                                                            'encounter action.',
                                           'pending_movement_key': 'Deterministic SHA-256 key of the pending '
                                                                   'movement row (namespace plus native '
                                                                   'source key).',
                                           'pending_priority_cd': 'Code value identifying the priority of '
                                                                  'the pending encounter action.',
                                           'pending_priority_display': 'Display text of the priority code '
                                                                       'assigned to the pending movement, '
                                                                       'resolved from the Millennium code '
                                                                       'value lookup.',
                                           'pending_prsnl_id': 'Identifier of the personnel record '
                                                               'associated with the pending encounter '
                                                               'action.',
                                           'pending_status_cd': 'Code value identifying the status of the '
                                                                'pending encounter action.',
                                           'pending_status_display': 'Display text of the status code of the '
                                                                     'pending movement, resolved from the '
                                                                     'Millennium code value lookup.',
                                           'pending_type_flag': 'Numeric flag indicating the type of pending '
                                                                'encounter action.',
                                           'person_id': 'Person whom this encounter is for.',
                                           'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                           'prev_est_depart_dt_tm': 'Previously recorded estimated departure '
                                                                    'date and time for the encounter.',
                                           'priority_seq': 'Sequence number indicating the priority ordering '
                                                           'of the pending encounter action.',
                                           'process_status_dt_tm': 'Date and time the process status of the '
                                                                   'pending encounter action was last set.',
                                           'process_status_flag': 'Numeric flag indicating the processing '
                                                                  'status of the pending encounter action.',
                                           'record_status': 'active, or superseded when the row stopped '
                                                            'appearing in the source snapshot '
                                                            '(source_present_ind false).',
                                           'record_status_effective_from': 'Start of the record status; not '
                                                                           'supplied by this source (always '
                                                                           'NULL).',
                                           'record_status_effective_to': 'When a superseded row was first '
                                                                         'detected absent from the source '
                                                                         '(source_absent_detected_ts); NULL '
                                                                         'while active.',
                                           'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                       'bronze MERGE to detect changes.',
                                           'source_absent_detected_ts': 'When bronze first found the row '
                                                                        'absent from the source snapshot; '
                                                                        'NULL while present.',
                                           'source_adc_updt': 'When the source row was landed in raw (raw '
                                                              'ADC_UPDT).',
                                           'source_present_ind': 'True while the row is present in the '
                                                                 'latest full source snapshot.',
                                           'source_row_id': 'Native source row identifier, as text.',
                                           'source_table': 'Bronze or raw source table the row came from.',
                                           'source_update_timestamp': 'Time the source row was landed in the '
                                                                      'raw layer (bronze SOURCE_ADC_UPDT), '
                                                                      'not when it changed in the source '
                                                                      'system; do not use it for recency.',
                                           'specialty_unit_cd': 'Code value identifying the specialty unit '
                                                                'associated with the pending encounter '
                                                                'action.',
                                           'subject_id_system': 'System of subject_key: '
                                                                'urn:cerner:person_id, or nosubject when the '
                                                                'row names no patient.',
                                           'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                          'person id, or of the source row when no person is '
                                                          'recorded.',
                                           'transaction_reason': 'Free-text description of the reason for '
                                                                 'the transaction associated with the '
                                                                 'pending encounter action.',
                                           'transaction_reason_cd': 'Code value identifying the reason for '
                                                                    'the transaction associated with the '
                                                                    'pending encounter action.',
                                           'transaction_reason_display': 'Display text of the transaction '
                                                                         'reason code for the pending '
                                                                         'movement, resolved from the '
                                                                         'Millennium code value lookup.',
                                           'trust': 'Name of the NHS Trust to which the record is '
                                                    'attributed, derived during loading via '
                                                    'encounter-to-organisation mapping.'},
                               'table': 'One row per Millennium pending encounter movement (bed request or '
                                        'transfer awaiting completion), keyed by ENCNTR_PENDING_ID.'},
 'clinical_prearrival': {'columns': {'active_ind': 'Indicator showing whether the pre-arrival record is '
                                                   'active.',
                                     'adc_updt': 'When bronze last wrote the row.',
                                     'age_txt': 'Free-text age of the expected patient as recorded at '
                                                'pre-arrival.',
                                     'anon_chief_complaint': 'Anonymised version of chief_complaint: '
                                                             'identifiers detected in the free text are '
                                                             'replaced by placeholders; published alongside '
                                                             'the source column, which gold releases only '
                                                             'when anonymisation succeeded.',
                                     'anon_context_fingerprint': 'Hash of the contextual inputs the redactor '
                                                                 'used for this row; the anonymised text is '
                                                                 'released only while it matches the current '
                                                                 'context.',
                                     'anon_identity_fingerprint': 'Hash of the patient identity details the '
                                                                  'record-driven redactor used for this row.',
                                     'anon_processed_at': "When the redactor last processed this row's text.",
                                     'anon_redaction_count': 'Number of redactions the redactor applied to '
                                                             'the source text.',
                                     'anon_redactor_version': 'Version of the redactor that produced the '
                                                              'anonymised text.',
                                     'anon_source_text_sha': 'SHA-256 digest of the text the redactor '
                                                             'processed; when it no longer matches the '
                                                             'current input digest the anonymised text is '
                                                             'stale.',
                                     'anon_status': "Outcome of the anonymisation run for this row's free "
                                                    'text (anonymized when it succeeded).',
                                     'attached_encntr_id': 'Identifier of the encounter to which the '
                                                           'pre-arrival record was later attached.',
                                     'attached_person_id': 'Identifier of the person to whom the pre-arrival '
                                                           'record was later attached.',
                                     'birth_dt_tm': 'Date and time of birth recorded for the expected '
                                                    'patient.',
                                     'chief_complaint': 'Free-text chief complaint or presenting problem '
                                                        'recorded for the expected patient.',
                                     'create_dt_tm': 'Date and time the pre-arrival record was created.',
                                     'encounter_id': 'Millennium ENCNTR_ID of the encounter the row belongs '
                                                     'to (BIGINT); NULL when not recorded.',
                                     'estimated_arrive_dt_tm': 'Estimated date and time of the expected '
                                                               "patient's arrival.",
                                     'first_name': 'Free-text first name entered for the expected patient on '
                                                   'the pre-arrival record.',
                                     'identity_status': 'resolved when person_id is present, unresolved when '
                                                        'absent, not_applicable for tables without a '
                                                        'patient.',
                                     'last_name': 'Free-text last name entered for the expected patient on '
                                                  'the pre-arrival record.',
                                     'linkage_route': 'How the row was linked to its person or encounter.',
                                     'load_batch_id': 'Load batch label derived from loaded_at '
                                                      '(yyyyMMddHHmmss); tdx-v21 when no load time exists.',
                                     'loaded_at': 'When the row was last written to bronze (ADC_UPDT), else '
                                                  'its source update time; the stable load clock.',
                                     'organization_id': 'Pipeline-added internal organisation identifier '
                                                        '(facility or client organisation of the attached '
                                                        'encounter) used to map the record to a trust.',
                                     'pcp_provider_id': "Identifier of the expected patient's primary care "
                                                        'provider recorded on the pre-arrival record.',
                                     'pcp_provider_name': "Name of the expected patient's primary care "
                                                          'provider recorded on the pre-arrival record.',
                                     'person_id': 'Millennium PERSON_ID of the patient (identifier); NULL '
                                                  'when the source row names no patient.',
                                     'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                     'prearrival_key': 'Deterministic SHA-256 key of the prearrival row '
                                                       '(namespace plus native source key).',
                                     'prearrival_type_cd': 'Code value identifying the type of pre-arrival '
                                                           'record.',
                                     'prearrival_type_display': 'Display text describing the type of '
                                                                'pre-arrival notification, resolved from the '
                                                                'Millennium code value lookup.',
                                     'record_status': 'active, or superseded when the row stopped appearing '
                                                      'in the source snapshot (source_present_ind false).',
                                     'record_status_effective_from': 'Start of the record status; not '
                                                                     'supplied by this source (always NULL).',
                                     'record_status_effective_to': 'When a superseded row was first detected '
                                                                   'absent from the source '
                                                                   '(source_absent_detected_ts); NULL while '
                                                                   'active.',
                                     'referring_source_id': 'Identifier of the referring source associated '
                                                            'with the pre-arrival record.',
                                     'referring_source_name': 'Name of the referring source recorded on the '
                                                              'pre-arrival record.',
                                     'reg_prsnl_id': 'Identifier of the personnel member who registered the '
                                                     'pre-arrival record.',
                                     'reg_prsnl_name': 'Name of the personnel member who registered the '
                                                       'pre-arrival record.',
                                     'row_hash': "SHA-256 of the row's payload columns, used by the bronze "
                                                 'MERGE to detect changes.',
                                     'sex_cd': 'Code value for the recorded sex of the expected patient.',
                                     'sex_display': 'Display text for the sex recorded on the pre-arrival '
                                                    'record, resolved from the Millennium code value lookup.',
                                     'source_absent_detected_ts': 'When bronze first found the row absent '
                                                                  'from the source snapshot; NULL while '
                                                                  'present.',
                                     'source_adc_updt': 'When the source row was landed in raw (raw '
                                                        'ADC_UPDT).',
                                     'source_present_ind': 'True while the row is present in the latest full '
                                                           'source snapshot.',
                                     'source_row_id': 'Native source row identifier, as text.',
                                     'source_table': 'Bronze or raw source table the row came from.',
                                     'source_update_timestamp': 'Time the source row was landed in the raw '
                                                                'layer (bronze SOURCE_ADC_UPDT), not when it '
                                                                'changed in the source system; do not use it '
                                                                'for recency.',
                                     'subject_id_system': 'System of subject_key: urn:cerner:person_id, or '
                                                          'nosubject when the row names no patient.',
                                     'subject_key': 'Deterministic subject key: hash of the Cerner person '
                                                    'id, or of the source row when no person is recorded.',
                                     'tracking_group_cd': 'Code value identifying the tracking group to '
                                                          'which the pre-arrival record is assigned.',
                                     'tracking_group_display': 'Display text for the tracking group '
                                                               'associated with the pre-arrival record, '
                                                               'resolved from the Millennium code value '
                                                               'lookup.',
                                     'tracking_prearrival_id': 'Internal Millennium surrogate identifier '
                                                               'uniquely identifying each pre-arrival '
                                                               'tracking record.',
                                     'trust': 'Pipeline-derived NHS trust name attributed to the pre-arrival '
                                              'record via organisation-to-trust mapping, null where no '
                                              'mapping was resolved.'},
                         'table': 'One row per FirstNet pre-arrival notice (expected patient), keyed by '
                                  'TRACKING_PREARRIVAL_ID.'},
 'clinical_tracking_attendance': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                              'attendance_open_ind': 'Boolean flag showing whether the '
                                                                     'tracking attendance is still open, '
                                                                     'derived from the absence of a '
                                                                     'check-out date and time on the '
                                                                     'underlying Millennium tracking '
                                                                     'check-in record.',
                                              'attendance_sequence': 'Integer ordinal position of the '
                                                                     'tracking attendance, assigned by '
                                                                     'ordering on check-in date and time '
                                                                     'with the tracking check-in identifier '
                                                                     'as tie-breaker.',
                                              'checkin_dt_tm': 'Date and time at which the tracked item was '
                                                               'checked in.',
                                              'checkin_id': 'Identifier of the personnel member who '
                                                            'performed the check-in.',
                                              'checkout_disposition_cd': 'Millennium code value identifying '
                                                                         'the disposition recorded at '
                                                                         'check-out from tracking.',
                                              'checkout_disposition_display': 'Display text for the checkout '
                                                                              'disposition recorded for the '
                                                                              'tracking attendance.',
                                              'checkout_dt_tm': 'Date and time at which the tracked item was '
                                                                'checked out.',
                                              'checkout_dt_tm_clean': 'Cleaned check-out date and time for '
                                                                      'the tracking attendance, derived from '
                                                                      'the Millennium tracking check-in '
                                                                      'check-out timestamp.',
                                              'checkout_id': 'Identifier of the personnel member who '
                                                             'performed the check-out.',
                                              'dwell_minutes': 'Duration of the tracking attendance in '
                                                               'minutes, calculated from check-in date and '
                                                               'time to cleaned check-out date and time.',
                                              'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                              'belongs to (BIGINT); NULL when not recorded.',
                                              'family_present_cd': 'Code value indicating whether family was '
                                                                   'present for the tracked check-in.',
                                              'family_present_display': 'Display text indicating whether '
                                                                        'family was present during the '
                                                                        'tracking attendance.',
                                              'identity_status': 'resolved when person_id is present, '
                                                                 'unresolved when absent, not_applicable for '
                                                                 'tables without a patient.',
                                              'linkage_route': 'How the row was linked to its person or '
                                                               'encounter.',
                                              'load_batch_id': 'Load batch label derived from loaded_at '
                                                               '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                               'exists.',
                                              'loaded_at': 'When the row was last written to bronze '
                                                           '(ADC_UPDT), else its source update time; the '
                                                           'stable load clock.',
                                              'organization_id': 'Millennium organisation identifier added '
                                                                 'by the landing process and used to resolve '
                                                                 'Trust attribution.',
                                              'period_end': 'End timestamp of the attendance period, taken '
                                                            'from the cleaned tracking check-out date and '
                                                            'time.',
                                              'period_start': 'Start timestamp of the attendance period, '
                                                              'taken from the tracking check-in date and '
                                                              'time.',
                                              'person_id': 'Millennium person identifier for the person '
                                                           'associated with the tracking item, where the '
                                                           'tracked item references a person.',
                                              'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                 'row.',
                                              'primary_doc_id': 'Identifier of the primary doctor assigned '
                                                                'to the tracked check-in.',
                                              'primary_nurse_id': 'Identifier of the primary nurse assigned '
                                                                  'to the tracked check-in.',
                                              'reactivate_system_ind': 'True when the source '
                                                                       'reactivate_user_id was 1, the Cerner '
                                                                       'system user (J5); the id is then '
                                                                       'published as NULL. NULL when the '
                                                                       'source id was NULL. INSR_W2.',
                                              'reactivate_user_id': 'Identifier of the Millennium user who '
                                                                    'reactivated the tracking check-in.',
                                              'reactivation_dt_tm': 'Date and time at which the tracking '
                                                                    'check-in was reactivated.',
                                              'record_status': 'active, or superseded when the row stopped '
                                                               'appearing in the source snapshot '
                                                               '(source_present_ind false).',
                                              'record_status_effective_from': 'Start of the record status; '
                                                                              'not supplied by this source '
                                                                              '(always NULL).',
                                              'record_status_effective_to': 'When a superseded row was first '
                                                                            'detected absent from the source '
                                                                            '(source_absent_detected_ts); '
                                                                            'NULL while active.',
                                              'registration_status_id': 'Identifier of the registration '
                                                                        'status associated with the tracked '
                                                                        'check-in.',
                                              'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                          'bronze MERGE to detect changes.',
                                              'secondary_doc_id': 'Identifier of the secondary doctor '
                                                                  'assigned to the tracked check-in.',
                                              'secondary_nurse_id': 'Identifier of the secondary nurse '
                                                                    'assigned to the tracked check-in.',
                                              'source_absent_detected_ts': 'When bronze first found the row '
                                                                           'absent from the source snapshot; '
                                                                           'NULL while present.',
                                              'source_adc_updt': 'When the source row was landed in raw (raw '
                                                                 'ADC_UPDT).',
                                              'source_present_ind': 'True while the row is present in the '
                                                                    'latest full source snapshot.',
                                              'source_row_id': 'Native source row identifier, as text.',
                                              'source_table': 'Bronze or raw source table the row came from.',
                                              'source_update_timestamp': 'Time the source row was landed in '
                                                                         'the raw layer (bronze '
                                                                         'SOURCE_ADC_UPDT), not when it '
                                                                         'changed in the source system; do '
                                                                         'not use it for recency.',
                                              'specialty_id': 'Identifier of the specialty assigned to the '
                                                              'tracked check-in.',
                                              'subject_id_system': 'System of subject_key: '
                                                                   'urn:cerner:person_id, or nosubject when '
                                                                   'the row names no patient.',
                                              'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                             'person id, or of the source row when no person '
                                                             'is recorded.',
                                              'surg_case_id': 'Identifier of the surgical case associated '
                                                              'with the tracking attendance, carried through '
                                                              'from the silver tracking attendance data and '
                                                              'populated only for a minority of rows.',
                                              'team_id': 'Identifier of the care team assigned to the '
                                                         'tracked check-in.',
                                              'tracking_attendance_key': 'Deterministic SHA-256 key of the '
                                                                         'tracking attendance row (namespace '
                                                                         'plus native source key).',
                                              'tracking_checkin_id': 'Unique Millennium identifier for the '
                                                                     'tracking check-in record and primary '
                                                                     'key of the TRACKING_CHECKIN table.',
                                              'tracking_event_type_cd': 'Code value identifying the type of '
                                                                        'tracking event associated with the '
                                                                        'check-in.',
                                              'tracking_group_cd': 'Code value identifying the tracking '
                                                                   'group in which the check-in was '
                                                                   'recorded.',
                                              'tracking_group_display': 'Display text for the tracking group '
                                                                        'associated with the tracking '
                                                                        'attendance.',
                                              'tracking_id': 'Identifier of the parent tracking record to '
                                                             'which this check-in belongs.',
                                              'trauma_ind': 'Indicator flag recording whether the tracked '
                                                            'check-in is flagged as trauma.',
                                              'trust': 'NHS Trust attributed to the record by the landing '
                                                       'process via organisation-to-trust mapping.'},
                                  'table': 'One row per FirstNet tracking check-in, keyed by Millennium '
                                           'TRACKING_CHECKIN_ID.'},
 'clinical_tracking_episode': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                           'base_loc_cd': 'Millennium location code value for the base '
                                                          'location of the tracking item.',
                                           'base_loc_display': 'Display name of the base location associated '
                                                               'with the clinical tracking episode, resolved '
                                                               'from the Millennium code value lookup.',
                                           'base_loc_dt_tm': 'Date and time associated with the base '
                                                             'location assignment of the tracking item.',
                                           'cur_tracking_locator_id': 'Identifier of the current tracking '
                                                                      'locator record for the tracking item.',
                                           'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                           'belongs to (BIGINT); NULL when not recorded.',
                                           'end_tracking_dt_tm': 'Date and time at which tracking of the '
                                                                 'item ended.',
                                           'end_tracking_dt_tm_clean': 'Cleaned date and time at which '
                                                                       'tracking of the item ended, derived '
                                                                       'from the Millennium tracking item '
                                                                       'end tracking timestamp.',
                                           'identity_status': 'resolved when person_id is present, '
                                                              'unresolved when absent, not_applicable for '
                                                              'tables without a patient.',
                                           'item_active_ind': 'Boolean indicator showing whether the '
                                                              'underlying Millennium tracking item record is '
                                                              'active.',
                                           'linkage_route': 'How the row was linked to its person or '
                                                            'encounter.',
                                           'load_batch_id': 'Load batch label derived from loaded_at '
                                                            '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                            'exists.',
                                           'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                        'else its source update time; the stable load clock.',
                                           'organization_id': 'Millennium organisation surrogate identifier '
                                                              'resolved during ingestion, partly via '
                                                              'encounter data, and used to derive the Trust.',
                                           'parent_entity_id': 'Identifier of the parent entity record, in '
                                                               'the table named by PARENT_ENTITY_NAME, '
                                                               'associated with the tracking item.',
                                           'parent_entity_name': 'Name of the parent entity table referenced '
                                                                 'by PARENT_ENTITY_ID for the tracking item.',
                                           'person_id': 'Millennium person identifier for the person '
                                                        'associated with the tracking item, where the '
                                                        'tracked item references a person.',
                                           'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                           'record_status': 'active, or superseded when the row stopped '
                                                            'appearing in the source snapshot '
                                                            '(source_present_ind false).',
                                           'record_status_effective_from': 'Start of the record status; not '
                                                                           'supplied by this source (always '
                                                                           'NULL).',
                                           'record_status_effective_to': 'When a superseded row was first '
                                                                         'detected absent from the source '
                                                                         '(source_absent_detected_ts); NULL '
                                                                         'while active.',
                                           'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                       'bronze MERGE to detect changes.',
                                           'source_absent_detected_ts': 'When bronze first found the row '
                                                                        'absent from the source snapshot; '
                                                                        'NULL while present.',
                                           'source_adc_updt': 'When the source row was landed in raw (raw '
                                                              'ADC_UPDT).',
                                           'source_present_ind': 'True while the row is present in the '
                                                                 'latest full source snapshot.',
                                           'source_row_id': 'Native source row identifier, as text.',
                                           'source_table': 'Bronze or raw source table the row came from.',
                                           'source_update_timestamp': 'Time the source row was landed in the '
                                                                      'raw layer (bronze SOURCE_ADC_UPDT), '
                                                                      'not when it changed in the source '
                                                                      'system; do not use it for recency.',
                                           'start_tracking_dt_tm': 'Date and time at which tracking of the '
                                                                   'item started.',
                                           'subject_id_system': 'System of subject_key: '
                                                                'urn:cerner:person_id, or nosubject when the '
                                                                'row names no patient.',
                                           'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                          'person id, or of the source row when no person is '
                                                          'recorded.',
                                           'surg_case_id': 'Internal surrogate identifier of the surgical '
                                                           'theatre case associated with the tracking '
                                                           'episode, derived from the Millennium tracking '
                                                           'item parent entity reference where it points to '
                                                           'a surgical case.',
                                           'tracking_episode_key': 'Deterministic SHA-256 key of the '
                                                                   'tracking episode row (namespace plus '
                                                                   'native source key).',
                                           'tracking_id': 'Millennium identifier for the tracking item '
                                                          'record, appearing to uniquely identify each row '
                                                          'in TRACKING_ITEM.',
                                           'tracking_open_ind': 'Boolean indicator showing whether the '
                                                                'tracking episode is still open, derived '
                                                                'from the tracking item end tracking date '
                                                                'and time.',
                                           'tracking_prearrival_id': 'Internal surrogate identifier of the '
                                                                     'tracking pre-arrival record linked to '
                                                                     'the episode, derived from the tracking '
                                                                     'item parent entity reference.',
                                           'tracking_status_flag': 'Numeric flag indicating the tracking '
                                                                   'status of the item.',
                                           'tracking_type_flag': 'Numeric flag indicating the type of '
                                                                 'tracking item, such as whether it relates '
                                                                 'to a person, encounter, inventory item or '
                                                                 'personnel member.',
                                           'trust': 'NHS trust name or code assigned at ingestion by mapping '
                                                    "the row's organisation to a trust, partly resolved via "
                                                    'encounter data.'},
                               'table': 'One row per FirstNet (emergency department) tracking episode, keyed '
                                        'by Millennium TRACKING_ID.'},
 'clinical_tracking_location_stay': {'columns': {'active_events_ind': 'Indicator flag denoting whether '
                                                                      'active tracking events exist for this '
                                                                      'locator entry.',
                                                 'adc_updt': 'When bronze last wrote the row.',
                                                 'anon_context_fingerprint': 'Hash of the contextual inputs '
                                                                             'the redactor used for this '
                                                                             'row; the anonymised text is '
                                                                             'released only while it matches '
                                                                             'the current context.',
                                                 'anon_identity_fingerprint': 'Hash of the patient identity '
                                                                              'details the record-driven '
                                                                              'redactor used for this row.',
                                                 'anon_processed_at': 'When the redactor last processed this '
                                                                      "row's text.",
                                                 'anon_redaction_count': 'Number of redactions the redactor '
                                                                         'applied to the source text.',
                                                 'anon_redactor_version': 'Version of the redactor that '
                                                                          'produced the anonymised text.',
                                                 'anon_source_text_sha': 'SHA-256 digest of the text the '
                                                                         'redactor processed; when it no '
                                                                         'longer matches the current input '
                                                                         'digest the anonymised text is '
                                                                         'stale.',
                                                 'anon_status': 'Outcome of the anonymisation run for this '
                                                                "row's free text (anonymized when it "
                                                                'succeeded).',
                                                 'anon_tracking_reason_comment': 'Anonymised version of '
                                                                                 'tracking_reason_comment: '
                                                                                 'identifiers detected in '
                                                                                 'the free text are replaced '
                                                                                 'by placeholders; published '
                                                                                 'alongside the source '
                                                                                 'column, which gold '
                                                                                 'releases only when '
                                                                                 'anonymisation succeeded.',
                                                 'arrive_dt_tm': 'Date and time of arrival at the location '
                                                                 'recorded on this tracking locator entry.',
                                                 'depart_dt_tm': 'Date and time of departure from the '
                                                                 'location recorded on this tracking locator '
                                                                 'entry.',
                                                 'depart_dt_tm_clean': 'Cleaned date and time the tracked '
                                                                       'patient departed the tracking '
                                                                       'location, derived from the '
                                                                       'Millennium tracking locator '
                                                                       'departure timestamp.',
                                                 'encounter_id': 'Millennium ENCNTR_ID of the encounter the '
                                                                 'row belongs to (BIGINT); NULL when not '
                                                                 'recorded.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'linkage_route': 'How the row was linked to its person or '
                                                                  'encounter.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'loc_bed_cd': 'Millennium code value identifying the bed '
                                                               'location for this locator entry.',
                                                 'loc_bed_display': 'Display name of the bed associated with '
                                                                    'the tracked location stay, resolved '
                                                                    'from the Millennium code value lookup.',
                                                 'loc_nurse_unit_cd': 'Millennium code value identifying the '
                                                                      'nurse unit location for this locator '
                                                                      'entry.',
                                                 'loc_nurse_unit_display': 'Display name of the nurse unit '
                                                                           '(ward) associated with the '
                                                                           'tracked location stay, resolved '
                                                                           'from the Millennium code value '
                                                                           'lookup.',
                                                 'loc_room_cd': 'Millennium code value identifying the room '
                                                                'location for this locator entry.',
                                                 'loc_room_display': 'Display name of the room associated '
                                                                     'with the tracked location stay, '
                                                                     'resolved from the Millennium code '
                                                                     'value lookup.',
                                                 'location_cd': 'Millennium code value identifying the '
                                                                'tracked location for this locator entry.',
                                                 'location_display': 'Display name of the tracked location '
                                                                     'for the location stay, resolved from '
                                                                     'the Millennium code value lookup.',
                                                 'organization_id': 'Pipeline-added Millennium organisation '
                                                                    'identifier resolved during landing and '
                                                                    'used to map the row to a Trust.',
                                                 'period_end': 'End timestamp of the tracking location stay '
                                                               'period, taken from the cleaned departure '
                                                               'date and time.',
                                                 'period_start': 'Start timestamp of the tracking location '
                                                                 'stay period, taken from the arrival date '
                                                                 'and time at the tracking location.',
                                                 'person_id': 'Millennium person identifier for the person '
                                                              'associated with the tracking item, where the '
                                                              'tracked item references a person.',
                                                 'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                    'row.',
                                                 'rank_sequence': 'Numeric sequence indicating the rank or '
                                                                  'ordering of this locator entry.',
                                                 'record_status': 'active, or superseded when the row '
                                                                  'stopped appearing in the source snapshot '
                                                                  '(source_present_ind false).',
                                                 'record_status_effective_from': 'Start of the record '
                                                                                 'status; not supplied by '
                                                                                 'this source (always NULL).',
                                                 'record_status_effective_to': 'When a superseded row was '
                                                                               'first detected absent from '
                                                                               'the source '
                                                                               '(source_absent_detected_ts); '
                                                                               'NULL while active.',
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'scheduled_dt_tm': 'Date and time at which the tracked item '
                                                                    'was scheduled to be at this location.',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_present_ind': 'True while the row is present in the '
                                                                       'latest full source snapshot.',
                                                 'source_row_id': 'Native source row identifier, as text.',
                                                 'source_table': 'Bronze or raw source table the row came '
                                                                 'from.',
                                                 'source_update_timestamp': 'Time the source row was landed '
                                                                            'in the raw layer (bronze '
                                                                            'SOURCE_ADC_UPDT), not when it '
                                                                            'changed in the source system; '
                                                                            'do not use it for recency.',
                                                 'stay_minutes': 'Duration of the tracking location stay in '
                                                                 'minutes, calculated from the arrival and '
                                                                 'cleaned departure timestamps.',
                                                 'stay_open_ind': 'Boolean flag indicating whether the '
                                                                  'tracking location stay is still open, '
                                                                  'derived from the absence or state of the '
                                                                  'departure timestamp.',
                                                 'subject_id_system': 'System of subject_key: '
                                                                      'urn:cerner:person_id, or nosubject '
                                                                      'when the row names no patient.',
                                                 'subject_key': 'Deterministic subject key: hash of the '
                                                                'Cerner person id, or of the source row when '
                                                                'no person is recorded.',
                                                 'surg_case_id': 'Identifier of the surgical case associated '
                                                                 'with this tracking location stay, sourced '
                                                                 'from the tracking location stay and '
                                                                 'tracking episode records.',
                                                 'tracking_acuity_level_id': 'Internal Millennium identifier '
                                                                             'of the tracking acuity level '
                                                                             'associated with this locator '
                                                                             'entry.',
                                                 'tracking_id': 'Internal Millennium identifier of the '
                                                                'tracking record to which this locator entry '
                                                                'belongs.',
                                                 'tracking_location_stay_key': 'Deterministic SHA-256 key of '
                                                                               'the tracking location stay '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'tracking_locator_id': 'Internal Millennium identifier that '
                                                                        'uniquely identifies each tracking '
                                                                        'locator entry.',
                                                 'tracking_reason_cd': 'Millennium code value for the '
                                                                       'tracking reason recorded on this '
                                                                       'locator entry.',
                                                 'tracking_reason_comment': 'Free-text comment describing '
                                                                            'the tracking reason for this '
                                                                            'locator entry.',
                                                 'tracking_reason_display': 'Display text of the reason '
                                                                            'recorded for the tracking '
                                                                            'location stay, resolved from '
                                                                            'the Millennium code value '
                                                                            'lookup.',
                                                 'trust': 'Pipeline-added NHS Trust attribution for the row, '
                                                          'derived during landing through organisation and '
                                                          'encounter mapping.',
                                                 'unavail_tracking_event_id': 'Internal Millennium '
                                                                              'identifier of the tracking '
                                                                              'event associated with '
                                                                              'unavailability for this '
                                                                              'locator entry.'},
                                     'table': 'One row per FirstNet tracking location stay (arrival to '
                                              'departure at one tracked location), keyed by '
                                              'TRACKING_LOCATOR_ID.'},
 'clinical_tracking_milestone': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                             'clinical_event_cd': 'Code value identifying the clinical event '
                                                                  'associated with the tracking event.',
                                             'complete_dt_tm': 'Date and time the tracking event was '
                                                               'completed.',
                                             'complete_id': 'Identifier of the personnel associated with '
                                                            'completing the tracking event.',
                                             'critical_interval': 'Interval after which the tracking event '
                                                                  'is treated as critical.',
                                             'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                             'belongs to (BIGINT); NULL when not recorded.',
                                             'event_status_cd': 'Code value representing the status of the '
                                                                'tracking event.',
                                             'event_status_display': 'Display text for the status of the '
                                                                     'tracking milestone event, resolved '
                                                                     'from the Millennium code value lookup.',
                                             'identity_status': 'resolved when person_id is present, '
                                                                'unresolved when absent, not_applicable for '
                                                                'tables without a patient.',
                                             'linkage_route': 'How the row was linked to its person or '
                                                              'encounter.',
                                             'load_batch_id': 'Load batch label derived from loaded_at '
                                                              '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                              'exists.',
                                             'loaded_at': 'When the row was last written to bronze '
                                                          '(ADC_UPDT), else its source update time; the '
                                                          'stable load clock.',
                                             'milestone_class': 'Classification of the clinical tracking '
                                                                'milestone, derived from the tracking '
                                                                'milestone mapping using the Millennium '
                                                                'track event display name.',
                                             'milestone_config_era': 'Tracking milestone configuration era: '
                                                                     'pre_2024_04 or post_2024_04 by '
                                                                     'milestone_datetime (the configuration '
                                                                     'changed on 2024-04-01, so step changes '
                                                                     'at that date are configuration, not '
                                                                     'activity). NULL when '
                                                                     'milestone_datetime is NULL. INSR_W2.',
                                             'milestone_datetime': 'Date and time of the tracking milestone, '
                                                                   "derived from the tracking event's "
                                                                   'complete, onset or requested date-time.',
                                             'milestone_phase': 'Phase of the clinical tracking pathway that '
                                                                'the milestone belongs to, derived from the '
                                                                'tracking milestone mapping using the '
                                                                'Millennium track event display name.',
                                             'milestone_rule_id': 'Identifier of the mapping rule used to '
                                                                  'assign the track event to this milestone, '
                                                                  'derived from the tracking milestone '
                                                                  'mapping.',
                                             'milestone_rule_version': 'Version identifier of the milestone '
                                                                       'rule applied to the tracking event, '
                                                                       'taken directly from the source '
                                                                       'Millennium tracking milestone '
                                                                       'record.',
                                             'onset_dt_tm': 'Date and time of onset (start) of the tracking '
                                                            'event.',
                                             'onset_id': 'Identifier of the personnel associated with the '
                                                         'onset of the tracking event.',
                                             'organization_id': 'Landing-added Millennium organisation '
                                                                'identifier resolved for the tracking event, '
                                                                'used for organisation-to-trust mapping.',
                                             'overdue_interval': 'Interval after which the tracking event is '
                                                                 'treated as overdue.',
                                             'person_id': 'Millennium person identifier for the person '
                                                          'associated with the tracking item, where the '
                                                          'tracked item references a person.',
                                             'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                'row.',
                                             'record_status': 'active, or superseded when the row stopped '
                                                              'appearing in the source snapshot '
                                                              '(source_present_ind false).',
                                             'record_status_effective_from': 'Start of the record status; '
                                                                             'not supplied by this source '
                                                                             '(always NULL).',
                                             'record_status_effective_to': 'When a superseded row was first '
                                                                           'detected absent from the source '
                                                                           '(source_absent_detected_ts); '
                                                                           'NULL while active.',
                                             'requested_dt_tm': 'Date and time the tracking event was '
                                                                'requested.',
                                             'requested_id': 'Identifier of the personnel associated with '
                                                             'requesting the tracking event.',
                                             'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                         'bronze MERGE to detect changes.',
                                             'source_absent_detected_ts': 'When bronze first found the row '
                                                                          'absent from the source snapshot; '
                                                                          'NULL while present.',
                                             'source_adc_updt': 'When the source row was landed in raw (raw '
                                                                'ADC_UPDT).',
                                             'source_present_ind': 'True while the row is present in the '
                                                                   'latest full source snapshot.',
                                             'source_row_id': 'Native source row identifier, as text.',
                                             'source_table': 'Bronze or raw source table the row came from.',
                                             'source_update_timestamp': 'Time the source row was landed in '
                                                                        'the raw layer (bronze '
                                                                        'SOURCE_ADC_UPDT), not when it '
                                                                        'changed in the source system; do '
                                                                        'not use it for recency.',
                                             'subject_id_system': 'System of subject_key: '
                                                                  'urn:cerner:person_id, or nosubject when '
                                                                  'the row names no patient.',
                                             'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                            'person id, or of the source row when no person '
                                                            'is recorded.',
                                             'surg_case_id': 'Internal surrogate identifier of the surgical '
                                                             'case associated with the clinical tracking '
                                                             'milestone, populated only for surgery-related '
                                                             'milestones.',
                                             'track_event_description': 'Description of the Millennium '
                                                                        'tracking event associated with the '
                                                                        'milestone.',
                                             'track_event_display': 'Display name of the Millennium tracking '
                                                                    'event associated with the milestone.',
                                             'track_event_id': 'Identifier of the track event definition '
                                                               'that this tracking event instantiates.',
                                             'tracking_event_id': 'Unique Millennium identifier for the '
                                                                  'tracking event row in the TRACKING_EVENT '
                                                                  'table.',
                                             'tracking_event_type_cd': 'Code value identifying the type of '
                                                                       'tracking event.',
                                             'tracking_event_type_display': 'Display text for the type of '
                                                                            'tracking event associated with '
                                                                            'the milestone, resolved from '
                                                                            'the Millennium code value '
                                                                            'lookup.',
                                             'tracking_group_cd': 'Code value identifying the tracking group '
                                                                  'associated with the tracking event.',
                                             'tracking_group_display': 'Display text for the tracking group '
                                                                       'to which the milestone belongs, '
                                                                       'resolved from the Millennium code '
                                                                       'value lookup.',
                                             'tracking_id': 'Identifier of the tracking record to which this '
                                                            'tracking event belongs.',
                                             'tracking_milestone_key': 'Deterministic SHA-256 key of the '
                                                                       'tracking milestone row (namespace '
                                                                       'plus native source key).',
                                             'trust': 'Landing-added NHS trust name or label resolved for '
                                                      'the row through encounter-to-organisation and '
                                                      'organisation-to-trust mapping.'},
                                 'table': 'One row per FirstNet tracking event (milestone such as triage or '
                                          'seen by clinician), keyed by TRACKING_EVENT_ID.'},
 'reference_location_attribute_history': {'columns': {'active_ind': 'Indicator showing whether the location '
                                                                    'attribute row is active.',
                                                      'adc_updt': 'When bronze last wrote the row.',
                                                      'anon_context_fingerprint': 'Hash of the contextual '
                                                                                  'inputs the redactor used '
                                                                                  'for this row; the '
                                                                                  'anonymised text is '
                                                                                  'released only while it '
                                                                                  'matches the current '
                                                                                  'context.',
                                                      'anon_description': 'Anonymised version of '
                                                                          'description: identifiers detected '
                                                                          'in the free text are replaced by '
                                                                          'placeholders; published alongside '
                                                                          'the source column, which gold '
                                                                          'releases only when anonymisation '
                                                                          'succeeded.',
                                                      'anon_identity_fingerprint': 'Hash of the patient '
                                                                                   'identity details the '
                                                                                   'record-driven redactor '
                                                                                   'used for this row.',
                                                      'anon_processed_at': 'When the redactor last processed '
                                                                           "this row's text.",
                                                      'anon_redaction_count': 'Number of redactions the '
                                                                              'redactor applied to the '
                                                                              'source text.',
                                                      'anon_redactor_version': 'Version of the redactor that '
                                                                               'produced the anonymised '
                                                                               'text.',
                                                      'anon_source_text_sha': 'SHA-256 digest of the text '
                                                                              'the redactor processed; when '
                                                                              'it no longer matches the '
                                                                              'current input digest the '
                                                                              'anonymised text is stale.',
                                                      'anon_status': 'Outcome of the anonymisation run for '
                                                                     "this row's free text (anonymized when "
                                                                     'it succeeded).',
                                                      'anon_value_string': 'Anonymised version of '
                                                                           'value_string: identifiers '
                                                                           'detected in the free text are '
                                                                           'replaced by placeholders; '
                                                                           'published alongside the source '
                                                                           'column, which gold releases only '
                                                                           'when anonymisation succeeded.',
                                                      'attrib_type_cd': 'Millennium code value identifying '
                                                                        'the type of attribute recorded '
                                                                        'against the location.',
                                                      'attrib_type_display': 'Display text of the location '
                                                                             'attribute type, resolved from '
                                                                             'the Millennium code value '
                                                                             'reference.',
                                                      'attribute_row_id': 'Internal surrogate identifier for '
                                                                          'a location attribute record, '
                                                                          'taken from the Millennium '
                                                                          'location attribute or location '
                                                                          'attribute history row identifier.',
                                                      'beg_effective_dt_tm': 'Date and time from which the '
                                                                             'location attribute row is '
                                                                             'effective.',
                                                      'description': 'Free-text description associated with '
                                                                     'the location attribute or its value.',
                                                      'end_effective_dt_tm': 'Date and time until which the '
                                                                             'location attribute row is '
                                                                             'effective, defaulting to a '
                                                                             'far-future date for open '
                                                                             'records.',
                                                      'end_effective_dt_tm_clean': 'Cleaned date and time at '
                                                                                   'which the location '
                                                                                   'attribute record stopped '
                                                                                   'being effective, derived '
                                                                                   'from the Millennium end '
                                                                                   'effective date-time.',
                                                      'hist_action': 'Short code indicating the history '
                                                                     'action recorded for the location '
                                                                     'attribute, such as an addition or '
                                                                     'deletion.',
                                                      'identity_status': 'resolved when person_id is '
                                                                         'present, unresolved when absent, '
                                                                         'not_applicable for tables without '
                                                                         'a patient.',
                                                      'load_batch_id': 'Load batch label derived from '
                                                                       'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                       'when no load time exists.',
                                                      'loaded_at': 'When the row was last written to bronze '
                                                                   '(ADC_UPDT), else its source update time; '
                                                                   'the stable load clock.',
                                                      'location_attribute_key': 'Deterministic SHA-256 key '
                                                                                'of the location attribute '
                                                                                'row (namespace plus native '
                                                                                'source key).',
                                                      'location_cd': 'Millennium code value identifying the '
                                                                     'location to which the attribute '
                                                                     'applies.',
                                                      'location_display': 'Display name of the location to '
                                                                          'which the attribute applies, '
                                                                          'resolved from the Millennium code '
                                                                          'value reference.',
                                                      'pipeline_run_id': 'Bronze pipeline run that last '
                                                                         'wrote the row.',
                                                      'pm_loc_attrib_id': 'Internal surrogate identifier '
                                                                          'uniquely identifying each '
                                                                          'location attribute row in the '
                                                                          'Millennium PM_LOC_ATTRIB table.',
                                                      'record_status': 'active, or superseded when the row '
                                                                       'stopped appearing in the source '
                                                                       'snapshot (source_present_ind false).',
                                                      'record_status_effective_from': 'Start of the record '
                                                                                      'status; not supplied '
                                                                                      'by this source '
                                                                                      '(always NULL).',
                                                      'record_status_effective_to': 'When a superseded row '
                                                                                    'was first detected '
                                                                                    'absent from the source '
                                                                                    '(source_absent_detected_ts); '
                                                                                    'NULL while active.',
                                                      'row_hash': "SHA-256 of the row's payload columns, "
                                                                  'used by the bronze MERGE to detect '
                                                                  'changes.',
                                                      'source_absent_detected_ts': 'When bronze first found '
                                                                                   'the row absent from the '
                                                                                   'source snapshot; NULL '
                                                                                   'while present.',
                                                      'source_adc_updt': 'When the source row was landed in '
                                                                         'raw (raw ADC_UPDT).',
                                                      'source_present_ind': 'True while the row is present '
                                                                            'in the latest full source '
                                                                            'snapshot.',
                                                      'source_row_id': 'Native source row identifier, as '
                                                                       'text.',
                                                      'source_table': 'Bronze or raw source table the row '
                                                                      'came from.',
                                                      'source_update_timestamp': 'Time the attribute row was '
                                                                                 'landed in the raw layer '
                                                                                 '(bronze SOURCE_ADC_UPDT), '
                                                                                 'not when the attribute '
                                                                                 'changed in Millennium; do '
                                                                                 'not use it for recency.',
                                                      'value_cd': 'Millennium code value holding the '
                                                                  'attribute value when the attribute is '
                                                                  'code-typed.',
                                                      'value_display': 'Display text of the location '
                                                                       'attribute value, resolved from the '
                                                                       'Millennium code value reference '
                                                                       'where applicable.',
                                                      'value_dt_tm': 'Date/time attribute value used when '
                                                                     'the attribute is date/time-typed.',
                                                      'value_id': 'Identifier holding the attribute value '
                                                                  'when the attribute is ID-typed.',
                                                      'value_num': 'Numeric attribute value used when the '
                                                                   'attribute is number-typed.',
                                                      'value_string': 'Text holding the attribute value when '
                                                                      'the attribute is string-typed.',
                                                      'value_type': 'Single-character indicator of which '
                                                                    'typed value column (code, ID, string, '
                                                                    'number or date/time) holds the '
                                                                    'attribute value.'},
                                          'table': 'One row per recorded change of a Millennium location '
                                                   'attribute, keyed by the source attribute row id.'},
 'reference_location_unit': {'columns': {'active_ind': 'Indicator of whether this bed record is currently '
                                                       'active.',
                                         'adc_updt': 'When bronze last wrote the row.',
                                         'beg_effective_dt_tm': 'Date and time from which this bed record is '
                                                                'effective.',
                                         'building_cd': 'Millennium code value identifying the building to '
                                                        'which the location unit belongs, as sourced from '
                                                        'the Millennium location hierarchy.',
                                         'class_cd': 'Millennium code value for the class assigned to the '
                                                     'room.',
                                         'current_bed_status_cd': 'Code value indicating the current status '
                                                                  'of the bed location, as sourced from the '
                                                                  'Millennium bed table.',
                                         'current_bed_status_display': 'Display text of the current bed '
                                                                       'status for the location unit, taken '
                                                                       'from the Millennium code value '
                                                                       'lookup.',
                                         'dup_bed_ind': 'Indicator flagging whether this bed is a duplicate '
                                                        'bed.',
                                         'end_effective_dt_tm': 'Date and time at which this bed record '
                                                                'ceases to be effective.',
                                         'facility_cd': 'Millennium code value identifying the facility to '
                                                        'which the location unit belongs, as sourced from '
                                                        'the Millennium location hierarchy.',
                                         'fixed_bed_ind': 'Indicator showing whether the room is flagged as '
                                                          'having fixed beds.',
                                         'identity_status': 'resolved when person_id is present, unresolved '
                                                            'when absent, not_applicable for tables without '
                                                            'a patient.',
                                         'load_batch_id': 'Load batch label derived from loaded_at '
                                                          '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                          'exists.',
                                         'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                      'else its source update time; the stable load clock.',
                                         'location_code': 'Millennium code value uniquely identifying the '
                                                          'location unit.',
                                         'location_display': 'Display name of the location unit, taken from '
                                                             'the Millennium code value lookup.',
                                         'location_level': 'Hierarchy level or type of the location unit, '
                                                           'derived from the Millennium code value CDF '
                                                           'meaning.',
                                         'location_unit_key': 'Deterministic SHA-256 key of the location '
                                                              'unit row (namespace plus native source key).',
                                         'nurse_unit_cd': 'Millennium code value identifying the nurse unit '
                                                          '(ward) associated with the location unit, as '
                                                          'sourced from the Millennium location hierarchy.',
                                         'nurse_unit_display': 'Display name of the nurse unit (ward) '
                                                               'associated with the location, taken from the '
                                                               'Millennium code value lookup.',
                                         'parent_location_cd': 'Millennium code value of the location '
                                                               'directly above this location unit in the '
                                                               'Millennium location hierarchy, such as the '
                                                               'room that contains a bed.',
                                         'parent_location_display': 'Display name of the parent location in '
                                                                    'the location hierarchy, taken from the '
                                                                    'Millennium code value lookup.',
                                         'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                         'record_status': 'active, or superseded when the row stopped '
                                                          'appearing in the source snapshot '
                                                          '(source_present_ind false).',
                                         'record_status_effective_from': 'Start of the record status; not '
                                                                         'supplied by this source (always '
                                                                         'NULL).',
                                         'record_status_effective_to': 'When a superseded row was first '
                                                                       'detected absent from the source '
                                                                       '(source_absent_detected_ts); NULL '
                                                                       'while active.',
                                         'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                     'bronze MERGE to detect changes.',
                                         'source_absent_detected_ts': 'When bronze first found the row '
                                                                      'absent from the source snapshot; NULL '
                                                                      'while present.',
                                         'source_adc_updt': 'When the source row was landed in raw (raw '
                                                            'ADC_UPDT).',
                                         'source_present_ind': 'True while the row is present in the latest '
                                                               'full source snapshot.',
                                         'source_row_id': 'Native source row identifier, as text.',
                                         'source_table': 'Bronze or raw source table the row came from.',
                                         'source_update_timestamp': 'Time the location row was landed in the '
                                                                    'raw layer (bronze SOURCE_ADC_UPDT), not '
                                                                    'when it changed in Millennium; do not '
                                                                    'use it for recency.'},
                             'table': 'One row per Millennium location (bed, room, unit or ward), keyed by '
                                      'LOCATION_CD.'}}
