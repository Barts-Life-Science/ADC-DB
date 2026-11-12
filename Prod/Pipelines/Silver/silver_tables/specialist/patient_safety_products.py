"""Silver specialist.patient_safety_products: 16 dataset(s).

- datasets: clinical_safety_incident, _clinical_safety_incident_metadata, clinical_safety_incident_participant, _clinical_safety_incident_participant_metadata, clinical_safety_incident_injury, _clinical_safety_incident_injury_metadata, clinical_safety_incident_factor, _clinical_safety_incident_factor_metadata, clinical_safety_incident_lfpse, _clinical_safety_incident_lfpse_metadata, clinical_safety_incident_action, _clinical_safety_incident_action_metadata ...
- moved verbatim from: 8_silver_patient_safety:159, 8_silver_patient_safety:184, 8_silver_patient_safety:194, 8_silver_patient_safety:36, 8_silver_patient_safety:38, 8_silver_patient_safety:40, 8_silver_patient_safety:47, 8_silver_patient_safety:51

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql import Column
import pyspark.sql.functions as F
from silver_core.active_rows import active_rows
from silver_core.journey_core import spark
from silver_tables.specialist._shared import INTERNAL_SCHEMA, PUBLIC_SCHEMA


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


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
    # Normalize a configured patient-safety bronze feed: standardize names, construct stable
    # keys, and carry identity, record status and source-derived load timestamps.
    # ACTIVE_ROWS_V1: bronze rows that are not active never enter silver; the rule lives in silver_active_rows.py.
    source = active_rows(spark.read.table(f"{BRONZE}.{source_name}"), f"{BRONZE}.{source_name}")
    df = source.select(*[F.col(column).alias(column.lower()) for column in source.columns])
    native_renames = {
        "clinical_safety_incident": ("recordid", "incident_id"),
        "clinical_safety_incident_injury": ("recordid", "injury_id"),
        "clinical_safety_incident_lfpse": ("recordid", "lfpse_id"),
        "clinical_safety_incident_action": ("recordid", "action_id"),
        "clinical_safety_incident_participant": ("link_recordid", "participant_row_id"),
    }
    if target_name in native_renames:
        old, new = native_renames[target_name]
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
            keys = [new if key == old else key for key in keys]
    if "encntr_id" in df.columns and "encounter_id" not in df.columns:
        df = df.withColumnRenamed("encntr_id", "encounter_id")

    source_row = F.concat_ws(":", *[F.col(key).cast("string") for key in keys])
    df = df.withColumn(key_name, stable_id(namespace, *[F.col(key) for key in keys]))
    if "source_present_ind" in df.columns:
        absent = ~F.coalesce(F.col("source_present_ind"), F.lit(True))
        df = df.withColumn("record_status", F.when(absent, "superseded").otherwise("active"))
        df = df.withColumn("record_status_effective_to", F.when(absent, F.col("source_absent_detected_ts")))
    else:
        df = df.withColumn("record_status", F.lit("active"))
        df = df.withColumn("record_status_effective_to", F.lit(None).cast("timestamp"))
    df = df.withColumn("record_status_effective_from", F.lit(None).cast("timestamp"))

    if "person_id" in df.columns:
        df = df.withColumn("person_id", F.col("person_id").cast("string"))
        subject_key, subject_system = subject_key_with_system(F.col("person_id"), F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
    else:
        df = df.withColumn("identity_status", F.lit("not_applicable"))
    if "encounter_id" in df.columns:
        # SILVER_RELEASE_20261025 item 6: Millennium ENCNTR_ID, published as BIGINT like every other silver table.
        df = df.withColumn("encounter_id", F.col("encounter_id").cast("bigint"))
    if "source_adc_updt" in df.columns:
        df = df.withColumn("source_update_timestamp", F.col("source_adc_updt"))

    stable_loaded_at = (
        F.col("adc_updt") if "adc_updt" in df.columns
        else F.col("source_update_timestamp") if "source_update_timestamp" in df.columns
        else F.lit(None).cast("timestamp")
    )
    df = df.withColumn("loaded_at", stable_loaded_at)
    df = df.withColumn("load_batch_id", F.coalesce(F.date_format(stable_loaded_at, "yyyyMMddHHmmss"), F.lit("tdx-v21")))

    if target_name == "text_safety_document":
        df = df.withColumn("incident_id", F.col("inc_id").cast("string"))
        df = df.withColumn("person_id", F.col("incident_person_id").cast("string"))
        subject_key, subject_system = subject_key_with_system(F.col("person_id"), F.lit(f"{BRONZE}.{source_name}"), source_row)
        df = df.withColumn("identity_status", F.when(F.col("person_id").isNotNull(), "resolved").otherwise("unresolved"))
        df = df.withColumn("subject_key", subject_key).withColumn("subject_id_system", subject_system)
        df = df.withColumn("source_object", F.regexp_replace(F.lower("source_object_table"), "^datix_", ""))
        df = df.withColumn("document_type_code", F.col("fragment_role"))
        df = df.withColumn("document_type_display", F.initcap(F.regexp_replace("fragment_role", "_", " ")))
        df = df.withColumn("document_text", F.col("text"))
        df = df.withColumn("document_text_anonymised", F.col("anon_text"))
        df = df.withColumn(
            "text_is_anonymised",
            (F.col("anon_status") == "anonymized")
            & F.col("anon_redactor_version").isin("v3.3")
            & F.col("anon_source_text_sha").eqNullSafe(F.col("anon_input_digest"))
            & F.col("anon_context_fingerprint").eqNullSafe(F.col("context_fingerprint_current")),
        )
        df = df.withColumn("text_sha256", F.sha2("document_text", 256))
        df = df.withColumn("text_length", F.length("document_text"))
    # SILVER_RELEASE_20261025 item 6: person_id is Millennium PERSON_ID, published as BIGINT like every other silver
    # table. Keys and subject_key above hash its canonical text form, which a full census showed equal to the
    # BIGINT's text for every non-null value, so no key changes.
    if "person_id" in df.columns:
        df = df.withColumn("person_id", F.col("person_id").cast("bigint"))
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


SAFETY_SPECS = [
    ("map_safety_incident", "clinical_safety_incident", ["recordid"], "safety_incident_key", "safety_incident:datix"),
    ("map_safety_incident_participant", "clinical_safety_incident_participant", ["link_recordid"], "safety_incident_participant_key", "safety_incident_participant:datix"),
    ("map_safety_incident_injury", "clinical_safety_incident_injury", ["recordid"], "safety_incident_injury_key", "safety_incident_injury:datix"),
    ("map_safety_incident_factor", "clinical_safety_incident_factor", ["factor_row_id"], "safety_incident_factor_key", "safety_incident_factor:datix"),
    ("map_safety_incident_lfpse", "clinical_safety_incident_lfpse", ["recordid"], "safety_incident_lfpse_key", "safety_incident_lfpse:datix"),
    ("map_safety_incident_action", "clinical_safety_incident_action", ["recordid"], "safety_incident_action_key", "safety_incident_action:datix"),
    ("map_safety_incident_status", "clinical_safety_incident_status", ["status_row_id"], "safety_incident_status_key", "safety_incident_status:datix"),
    ("map_safety_text_fragment", "text_safety_document", ["fragment_id"], "safety_document_key", "safety_document:datix"),
]


for _spec in SAFETY_SPECS:
    declare_product(*_spec)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_safety_incident_action_metadata': {'columns': {'identity_status': 'resolved when person_id is '
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
                                                                                         'NULL while active.',
                                                           'safety_incident_action_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'action row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_safety_incident_action.'},
 '_clinical_safety_incident_factor_metadata': {'columns': {'identity_status': 'resolved when person_id is '
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
                                                                                         'NULL while active.',
                                                           'safety_incident_factor_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'factor row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_safety_incident_factor.'},
 '_clinical_safety_incident_injury_metadata': {'columns': {'identity_status': 'resolved when person_id is '
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
                                                                                         'NULL while active.',
                                                           'safety_incident_injury_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'injury row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_safety_incident_injury.'},
 '_clinical_safety_incident_lfpse_metadata': {'columns': {'identity_status': 'resolved when person_id is '
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
                                                                                        'NULL while active.',
                                                          'safety_incident_lfpse_key': 'Deterministic '
                                                                                       'SHA-256 key of the '
                                                                                       'safety incident '
                                                                                       'lfpse row (namespace '
                                                                                       'plus native source '
                                                                                       'key).'},
                                              'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                       'clinical_safety_incident_lfpse.'},
 '_clinical_safety_incident_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                       'unresolved when absent, '
                                                                       'not_applicable for tables without a '
                                                                       'patient.',
                                                    'load_batch_id': 'Load batch label derived from '
                                                                     'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                     'when no load time exists.',
                                                    'loaded_at': 'When the row was last written to bronze '
                                                                 '(ADC_UPDT), else its source update time; '
                                                                 'the stable load clock.',
                                                    'record_status': 'active, or superseded when the row '
                                                                     'stopped appearing in the source '
                                                                     'snapshot (source_present_ind false).',
                                                    'record_status_effective_to': 'When a superseded row was '
                                                                                  'first detected absent '
                                                                                  'from the source '
                                                                                  '(source_absent_detected_ts); '
                                                                                  'NULL while active.',
                                                    'safety_incident_key': 'Deterministic SHA-256 key of the '
                                                                           'safety incident row (namespace '
                                                                           'plus native source key).'},
                                        'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                 'clinical_safety_incident.'},
 '_clinical_safety_incident_participant_metadata': {'columns': {'identity_status': 'resolved when person_id '
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
                                                                                              'active.',
                                                                'safety_incident_participant_key': 'Deterministic '
                                                                                                   'SHA-256 '
                                                                                                   'key of '
                                                                                                   'the '
                                                                                                   'safety '
                                                                                                   'incident '
                                                                                                   'participant '
                                                                                                   'row '
                                                                                                   '(namespace '
                                                                                                   'plus '
                                                                                                   'native '
                                                                                                   'source '
                                                                                                   'key).'},
                                                    'table': 'Internal TDX v2.1 quality and batch metadata '
                                                             'for clinical_safety_incident_participant.'},
 '_clinical_safety_incident_status_metadata': {'columns': {'identity_status': 'resolved when person_id is '
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
                                                                                         'NULL while active.',
                                                           'safety_incident_status_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'status row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).'},
                                               'table': 'Internal TDX v2.1 quality and batch metadata for '
                                                        'clinical_safety_incident_status.'},
 '_text_safety_document_metadata': {'columns': {'identity_status': 'resolved when person_id is present, '
                                                                   'unresolved when absent, not_applicable '
                                                                   'for tables without a patient.',
                                                'load_batch_id': 'Load batch label derived from loaded_at '
                                                                 '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                 'time exists.',
                                                'loaded_at': 'When the row was last written to bronze '
                                                             '(ADC_UPDT), else its source update time; the '
                                                             'stable load clock.',
                                                'record_status': 'active, or superseded when the row stopped '
                                                                 'appearing in the source snapshot '
                                                                 '(source_present_ind false).',
                                                'record_status_effective_to': 'When a superseded row was '
                                                                              'first detected absent from '
                                                                              'the source '
                                                                              '(source_absent_detected_ts); '
                                                                              'NULL while active.',
                                                'safety_document_key': 'Deterministic SHA-256 key of the '
                                                                       'safety document row (namespace plus '
                                                                       'native source key).'},
                                    'table': 'Internal TDX v2.1 quality and batch metadata for '
                                             'text_safety_document.'},
 'clinical_safety_incident': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                          'affected_patient_count': 'Count of patients recorded as affected '
                                                                    'by the clinical safety incident.',
                                          'context_fingerprint_current': "SHA-256 of the incident's current "
                                                                         'identifying context (names, birth '
                                                                         'date, postcode, reporter, '
                                                                         'investigator and participants); '
                                                                         'anonymised narrative text is '
                                                                         'released only while its '
                                                                         'anon_context_fingerprint matches '
                                                                         'this value.',
                                          'encounter_id': 'Millennium ENCNTR_ID of the encounter the row '
                                                          'belongs to (BIGINT); NULL when not recorded.',
                                          'encounter_link_confidence': 'HIGH when the linked encounter has '
                                                                       'HIGH arrival and departure '
                                                                       'confidence; LOW when an inpatient/ED '
                                                                       'departure was unresolved and bounded '
                                                                       'at arrival + 1 day; else MEDIUM.',
                                          'encounter_link_method': 'Method by which the clinical safety '
                                                                   'incident was linked to an encounter '
                                                                   'record.',
                                          'encounter_link_rule': 'Rule that set encounter_id (TDX-E2 '
                                                                 'incident-time overlap with Millennium '
                                                                 'encounter *_BEST bounds).',
                                          'identity_status': 'resolved when person_id is present, unresolved '
                                                             'when absent, not_applicable for tables without '
                                                             'a patient.',
                                          'inc_acctype': 'Type of accident recorded for the incident.',
                                          'inc_bodypart': 'Part of the body affected or injured in the '
                                                          'incident.',
                                          'inc_carestage': 'Code for the stage of care at which the incident '
                                                           'occurred, as recorded in Datix.',
                                          'inc_category': 'Coded category of the incident.',
                                          'inc_category_display': 'Descriptive label of the Datix incident '
                                                                  'category associated with the code.',
                                          'inc_cause': 'Recorded cause of the incident in Datix.',
                                          'inc_clingroup': 'Code for the clinical group or division '
                                                           'associated with the incident in Datix.',
                                          'inc_clinoutcome': 'Clinical outcome recorded for the incident in '
                                                             'Datix.',
                                          'inc_clinoutcome2': 'Secondary clinical outcome recorded for the '
                                                              'incident in Datix.',
                                          'inc_clintype': 'Code for the clinical incident type recorded in '
                                                          'Datix.',
                                          'inc_consequence': 'Coded consequence score used in the risk '
                                                             'grading of the incident.',
                                          'inc_directorate': 'Code for the directorate associated with the '
                                                             'incident.',
                                          'inc_dob': 'Date of birth of the person involved in the incident.',
                                          'inc_ethnicity': 'Ethnicity of the person affected by the '
                                                           'incident, as recorded in Datix.',
                                          'inc_further_inv': 'Datix incident field recording whether further '
                                                             'investigation of the incident is required.',
                                          'inc_gender': 'Gender of the person involved in the incident as '
                                                        'recorded in Datix.',
                                          'inc_grade': 'Overall risk grade of the incident derived from '
                                                       'consequence and likelihood.',
                                          'inc_grade_display': 'Descriptive label for the Datix incident '
                                                               'grade code.',
                                          'inc_head': 'Code identifying the head or senior manager '
                                                      'associated with the incident record in Datix.',
                                          'inc_impact': 'Impact classification of the incident.',
                                          'inc_injury': 'Type or nature of injury sustained in the incident.',
                                          'inc_investigator': 'Person assigned to investigate the incident, '
                                                              'as recorded in Datix.',
                                          'inc_is_riddor': 'Flag indicating whether the incident is '
                                                           'reportable under RIDDOR (Reporting of Injuries, '
                                                           'Diseases and Dangerous Occurrences Regulations).',
                                          'inc_likelihood': 'Coded likelihood of recurrence used in the risk '
                                                            'grading of the incident.',
                                          'inc_locactual': 'Code for the specific location where the '
                                                           'incident occurred.',
                                          'inc_location': 'Current location of the equipment involved in the '
                                                          'incident.',
                                          'inc_loctype': 'Coded type of location where the incident '
                                                         'occurred.',
                                          'inc_mgr': 'Code identifying the handler or manager responsible '
                                                     'for the incident record in Datix.',
                                          'inc_name': 'Name of the person affected by or associated with the '
                                                      'incident as recorded in Datix.',
                                          'inc_notify': 'Notification details recorded for the incident, '
                                                        'such as who was notified.',
                                          'inc_nspecialty': 'Specialty recorded for the incident, likely an '
                                                            'alternative or national specialty '
                                                            'classification in Datix.',
                                          'inc_organisation': 'Code for the organisation in which the '
                                                              'incident occurred, as recorded in Datix.',
                                          'inc_ourref': 'Local incident reference number assigned by the '
                                                        'organisation in Datix.',
                                          'inc_outcomecode': 'Code recording the outcome of the incident.',
                                          'inc_pat_type': 'Type of patient involved in the incident, as '
                                                          'recorded in Datix.',
                                          'inc_postcode': 'Postcode recorded against the incident.',
                                          'inc_rating': 'Numeric risk rating of the incident derived from '
                                                        'risk grading.',
                                          'inc_rc_required': 'Datix incident field recording whether a root '
                                                             'cause analysis is required for the incident.',
                                          'inc_repname': 'Name of the person who reported the incident.',
                                          'inc_reportedby': 'Role or staff category of the person who '
                                                            'reported the incident, as recorded in Datix.',
                                          'inc_result': 'Recorded result or outcome of the incident in '
                                                        'Datix.',
                                          'inc_riddor': 'RIDDOR-related classification recorded for the '
                                                        'incident.',
                                          'inc_riddorno': 'RIDDOR reference number assigned to the incident '
                                                          'report.',
                                          'inc_ridloc': 'RIDDOR location classification for where the '
                                                        'incident occurred.',
                                          'inc_severity': 'Coded severity or harm level assigned to the '
                                                          'incident.',
                                          'inc_severity_display': 'Descriptive label for the incident '
                                                                  'severity code shown to Datix users.',
                                          'inc_specialty': 'Code for the clinical specialty associated with '
                                                           'the incident.',
                                          'inc_subcategory': 'Coded subcategory of the incident within its '
                                                             'category.',
                                          'inc_subcategory_display': 'Descriptive label for the Datix '
                                                                     'incident subcategory code.',
                                          'inc_treatment': 'Treatment received as a result of the incident.',
                                          'inc_type': 'Coded incident type indicating who or what was '
                                                      'affected, such as patient or staff.',
                                          'inc_type_display': 'Descriptive label for the Datix incident type '
                                                              'code.',
                                          'inc_unit': 'Code for the organisational unit or site where the '
                                                      'incident occurred.',
                                          'inc_unit_type': 'Code recording the type of unit associated with '
                                                           'the Datix incident.',
                                          'incident_dt_tm': 'Date and time the clinical safety incident '
                                                            'occurred, derived from the Datix incident date '
                                                            'and incident time fields.',
                                          'incident_dt_tm_clean': 'Cleaned version of the date and time the '
                                                                  'clinical safety incident occurred, '
                                                                  'derived from the Datix incident date and '
                                                                  'incident time fields.',
                                          'incident_id': 'Datix record identifier for the clinical safety '
                                                         'incident.',
                                          'incident_time_valid_ind': 'Incident time was recorded (an undated '
                                                                     'incident time spans the whole incident '
                                                                     'day in TDX-E2).',
                                          'investigation_complete_dt_tm': 'Date and time the investigation '
                                                                          'of the clinical safety incident '
                                                                          'was completed, sourced from '
                                                                          'Datix.',
                                          'investigation_start_dt_tm': 'Date and time the investigation of '
                                                                       'the clinical safety incident was '
                                                                       'started, sourced from Datix.',
                                          'load_batch_id': 'Load batch label derived from loaded_at '
                                                           '(yyyyMMddHHmmss); tdx-v21 when no load time '
                                                           'exists.',
                                          'loaded_at': 'When the row was last written to bronze (ADC_UPDT), '
                                                       'else its source update time; the stable load clock.',
                                          'notified_dt_tm': 'Date and time of notification recorded against '
                                                            'the clinical safety incident in Datix, '
                                                            'populated for very few records.',
                                          'opened_dt_tm': 'Date and time the clinical safety incident record '
                                                          'was opened in Datix.',
                                          'person_id': 'Millennium PERSON_ID of the patient (identifier); '
                                                       'NULL when the source row names no patient.',
                                          'person_link_method': 'Method by which the clinical safety '
                                                                'incident was linked to a person record.',
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
                                          'rep_approved': 'Approval status code of the incident record '
                                                          'within the Datix review workflow.',
                                          'reported_dt_tm': 'Date and time the clinical safety incident was '
                                                            'reported, sourced from Datix.',
                                          'row_hash': "SHA-256 of the row's payload columns, used by the "
                                                      'bronze MERGE to detect changes.',
                                          'safety_incident_key': 'Deterministic SHA-256 key of the safety '
                                                                 'incident row (namespace plus native source '
                                                                 'key).',
                                          'secgroup': 'Security group associated with the incident record '
                                                      'for Datix access control.',
                                          'secgroup_display': 'Descriptive text for the Datix security group '
                                                              'code.',
                                          'seclevel': 'Security level assigned to the incident record for '
                                                      'Datix access control.',
                                          'source_absent_detected_ts': 'When bronze first found the row '
                                                                       'absent from the source snapshot; '
                                                                       'NULL while present.',
                                          'source_adc_updt': 'When the source row was landed in raw (raw '
                                                             'ADC_UPDT).',
                                          'source_present_ind': 'True while the row is present in the latest '
                                                                'full source snapshot.',
                                          'source_row_id': 'Native source row identifier, as text.',
                                          'source_table': 'Bronze or raw source table the row came from.',
                                          'source_update_timestamp': 'Time the source row was landed in the '
                                                                     'raw layer (bronze SOURCE_ADC_UPDT), '
                                                                     'not when it changed in the source '
                                                                     'system; do not use it for recency.',
                                          'source_updt_dt_tm': 'Last-update time of the row in the source '
                                                               'system (for Datix, updateddate); NULL when '
                                                               'the source supplies none.',
                                          'subject_id_system': 'System of subject_key: urn:cerner:person_id, '
                                                               'or nosubject when the row names no patient.',
                                          'subject_key': 'Deterministic subject key: hash of the Cerner '
                                                         'person id, or of the source row when no person is '
                                                         'recorded.'},
                              'table': 'One row per Datix patient-safety incident (incident reports, not '
                                       'incidence rates or proven causality), keyed by Datix RECORDID.'},
 'clinical_safety_incident_action': {'columns': {'act_by_inits': 'Datix staff initials code of the user who '
                                                                 'completed the action.',
                                                 'act_chain_id': 'Integer identifier of the action chain '
                                                                 'that the action belongs to, where '
                                                                 'applicable.',
                                                 'act_clingroup': 'Code for the clinical group associated '
                                                                  'with the action.',
                                                 'act_cost': 'Cost estimate recorded for the action.',
                                                 'act_cost_max': 'Maximum cost estimate recorded for the '
                                                                 'action.',
                                                 'act_cost_min': 'Minimum cost estimate recorded for the '
                                                                 'action.',
                                                 'act_cost_type': 'Code indicating the type of cost recorded '
                                                                  'for the action.',
                                                 'act_ddone': 'Date on which the action was recorded as '
                                                              'done.',
                                                 'act_ddue': 'Date by which the action is due to be '
                                                             'completed.',
                                                 'act_directorate': 'Code for the directorate associated '
                                                                    'with the action.',
                                                 'act_dstart': 'Start date of the action.',
                                                 'act_from_inits': 'Datix staff initials code of the user '
                                                                   'who assigned the action.',
                                                 'act_locactual': 'Code for the specific location associated '
                                                                  'with the action.',
                                                 'act_loctype': 'Code for the type of location associated '
                                                                'with the action.',
                                                 'act_organisation': 'Code for the organisation associated '
                                                                     'with the action.',
                                                 'act_priority': 'Priority assigned to the action.',
                                                 'act_score': 'Integer score recorded against the action; '
                                                              'its scoring scheme is not documented in the '
                                                              'available metadata.',
                                                 'act_specialty': 'Code for the specialty associated with '
                                                                  'the action.',
                                                 'act_step_no': 'Step number of the action within its action '
                                                                'chain.',
                                                 'act_to_inits': 'Datix staff initials code of the user to '
                                                                 'whom the action is assigned.',
                                                 'act_type': 'Code indicating the type of action.',
                                                 'act_unit': 'Code for the site or unit associated with the '
                                                             'action.',
                                                 'action_id': 'Identifier of the safety incident action '
                                                              'record, sourced from the RECORDID of the '
                                                              'Datix action record.',
                                                 'adc_updt': 'When bronze last wrote the row.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the safety incident to which the '
                                                           'action relates, sourced from the incident '
                                                           'identifier in the incident action mapping and '
                                                           'the case identifier on Datix action records.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
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
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'safety_incident_action_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident action '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'secgroup': 'Datix security group associated with the '
                                                             'action record.',
                                                 'seclevel': 'Datix security level applied to the action '
                                                             'record.',
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
                                                 'source_updt_dt_tm': 'Last-update time of the row in the '
                                                                      'source system (for Datix, '
                                                                      'updateddate); NULL when the source '
                                                                      'supplies none.',
                                                 'updateddate': 'Date and time the action record was last '
                                                                'updated in Datix.'},
                                     'table': 'One row per action recorded against a Datix incident, keyed '
                                              'by RECORDID.'},
 'clinical_safety_incident_factor': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'device_batch_number': 'Batch or lot number of the medical '
                                                                        'device involved in the clinical '
                                                                        'safety incident factor, as recorded '
                                                                        'in the source.',
                                                 'device_equipment': 'Medical device or equipment recorded '
                                                                     'as involved in the safety incident '
                                                                     'factor.',
                                                 'device_involvement_factors': 'Recorded factors describing '
                                                                               'how the medical device was '
                                                                               'involved in the clinical '
                                                                               'safety incident.',
                                                 'device_manufacturer': 'Manufacturer of the medical device '
                                                                        'involved in the safety incident '
                                                                        'factor.',
                                                 'device_model': 'Model of the medical device involved in '
                                                                 'the safety incident factor.',
                                                 'device_serial_number': 'Serial number of the medical '
                                                                         'device involved in the clinical '
                                                                         'safety incident factor, as '
                                                                         'recorded in the source.',
                                                 'device_supplier': 'Supplier of the medical device recorded '
                                                                    'as a factor in the clinical safety '
                                                                    'incident, as captured in the source.',
                                                 'device_type': 'Type or category of the medical device '
                                                                'involved in the safety incident factor.',
                                                 'factor_attributes_json': 'JSON of the raw Datix factor '
                                                                           'attributes (drug, route, dose, '
                                                                           '…) as recorded.',
                                                 'factor_code': 'Code identifying a contributing factor '
                                                                'recorded against a clinical safety '
                                                                'incident, derived from the Datix equipment '
                                                                'or medication drug fields via the incident '
                                                                'factor mapping.',
                                                 'factor_display': 'Display text describing the contributing '
                                                                   'factor recorded against a clinical '
                                                                   'safety incident, derived from the Datix '
                                                                   'equipment or medication drug fields.',
                                                 'factor_map_confidence': 'Numeric confidence score for the '
                                                                          'mapping applied to the clinical '
                                                                          'safety incident factor, populated '
                                                                          'only where a mapping was made.',
                                                 'factor_map_method': 'Method used to map the clinical '
                                                                      'safety incident factor to a target '
                                                                      'concept or code.',
                                                 'factor_map_rule_id': 'Identifier of the mapping rule '
                                                                       'applied to map the clinical safety '
                                                                       'incident factor.',
                                                 'factor_map_version': 'Version of the mapping resource or '
                                                                       'rules used to map the clinical '
                                                                       'safety incident factor.',
                                                 'factor_ordinal': 'Sequence number of the factor within its '
                                                                   'clinical safety incident.',
                                                 'factor_row_id': 'Internal identifier for each incident '
                                                                  'factor row, derived from the Datix '
                                                                  'incident record or LFPSE incident '
                                                                  'identifier.',
                                                 'factor_source_record_id': 'Identifier of the source Datix '
                                                                            'incident or LFPSE record from '
                                                                            'which the incident factor row '
                                                                            'was derived.',
                                                 'factor_source_table': 'Name of the source table from which '
                                                                        'the clinical safety incident factor '
                                                                        'was derived.',
                                                 'factor_target_code': 'SNOMED code the Datix drug text maps '
                                                                       'to via '
                                                                       '3_lookup.omop.drug_name_lexicon; '
                                                                       'null when unmapped.',
                                                 'factor_target_display': 'Display of factor_target_code.',
                                                 'factor_target_system': 'Code system of the mapped '
                                                                         'medication factor (SNOMED CT dm+d '
                                                                         'VTM or Substance).',
                                                 'factor_type': 'Type or category of the contributing factor '
                                                                'recorded for the clinical safety incident.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the Datix incident record that '
                                                           'this LFPSE row relates to.',
                                                 'lfpse_id': 'Numeric identifier of the associated Learn '
                                                             'From Patient Safety Events (LFPSE) record for '
                                                             'the incident factor, where one exists.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
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
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'safety_incident_factor_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident factor '
                                                                               'row (namespace plus native '
                                                                               'source key).',
                                                 'source_absent_detected_ts': 'When bronze first found the '
                                                                              'row absent from the source '
                                                                              'snapshot; NULL while present.',
                                                 'source_adc_updt': 'When the source row was landed in raw '
                                                                    '(raw ADC_UPDT).',
                                                 'source_fields': 'Source field or fields from which the '
                                                                  'clinical safety incident factor was '
                                                                  'extracted.',
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
                                     'table': 'One row per contributory factor (medication, device or '
                                              'process) recorded on a Datix incident.'},
 'clinical_safety_incident_injury': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'con_id': 'Identifier of the Datix contact (person involved '
                                                           'in the incident) who sustained the injury.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_bodypart': 'Datix code identifying the body part '
                                                                 'affected by the recorded injury, which may '
                                                                 'be blank.',
                                                 'inc_bodypart_display': 'Descriptive label of the body part '
                                                                         'represented by the code.',
                                                 'inc_id': 'Identifier of the Datix incident to which the '
                                                           'injury entry relates.',
                                                 'inc_injury': 'Datix code identifying the type of injury '
                                                               'recorded against the incident contact.',
                                                 'inc_injury_display': 'Descriptive label for the injury '
                                                                       'type code.',
                                                 'injury_id': 'Internal surrogate identifier for an injury '
                                                              'record linked to a clinical safety incident, '
                                                              'sourced from the Datix incident injuries '
                                                              'record ID.',
                                                 'listorder': 'Display or sequence order of the injury entry '
                                                              'within the incident-contact combination.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
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
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'safety_incident_injury_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident injury '
                                                                               'row (namespace plus native '
                                                                               'source key).',
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
                                                 'source_updt_dt_tm': 'Last-update time of the row in the '
                                                                      'source system (for Datix, '
                                                                      'updateddate); NULL when the source '
                                                                      'supplies none.'},
                                     'table': 'One row per recorded injury on a Datix incident, keyed by '
                                              'RECORDID.'},
 'clinical_safety_incident_lfpse': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                'con_id': 'Identifier of the Datix contact record that this '
                                                          'LFPSE row relates to, where applicable.',
                                                'identity_status': 'resolved when person_id is present, '
                                                                   'unresolved when absent, not_applicable '
                                                                   'for tables without a patient.',
                                                'inc_id': 'Identifier of the Datix incident record that this '
                                                          'LFPSE row relates to.',
                                                'lfpse_blood_involved': 'LFPSE response recording the blood '
                                                                        'involved in the reported event.',
                                                'lfpse_blood_problem': 'LFPSE response describing the '
                                                                       'problem with blood involved in the '
                                                                       'reported event.',
                                                'lfpse_blood_products_batch': 'LFPSE response recording the '
                                                                              'batch of blood products '
                                                                              'involved in the reported '
                                                                              'event.',
                                                'lfpse_blood_products_brand': 'LFPSE response recording the '
                                                                              'brand of blood products '
                                                                              'involved in the reported '
                                                                              'event.',
                                                'lfpse_blood_products_involved': 'LFPSE response recording '
                                                                                 'the blood products '
                                                                                 'involved in the reported '
                                                                                 'event.',
                                                'lfpse_blood_products_not_used': 'LFPSE response relating to '
                                                                                 'blood products not being '
                                                                                 'used when they should have '
                                                                                 'been in the reported '
                                                                                 'event.',
                                                'lfpse_blood_products_problem': 'LFPSE response describing '
                                                                                'the problem with blood '
                                                                                'products involved in the '
                                                                                'reported event.',
                                                'lfpse_blood_was_not_used': 'LFPSE response relating to '
                                                                            'blood not being used when it '
                                                                            'should have been in the '
                                                                            'reported event.',
                                                'lfpse_buildings_infrastructure': 'LFPSE response recording '
                                                                                  'the buildings or '
                                                                                  'infrastructure involved '
                                                                                  'in the reported event.',
                                                'lfpse_buildings_infrastructure_involved': 'LFPSE response '
                                                                                           'indicating '
                                                                                           'whether '
                                                                                           'buildings or '
                                                                                           'infrastructure '
                                                                                           'were involved in '
                                                                                           'the reported '
                                                                                           'event.',
                                                'lfpse_buildings_infrastructure_not_used': 'LFPSE response '
                                                                                           'relating to '
                                                                                           'buildings or '
                                                                                           'infrastructure '
                                                                                           'not being used '
                                                                                           'when they should '
                                                                                           'have been in the '
                                                                                           'reported event.',
                                                'lfpse_buildings_infrastructure_problem': 'LFPSE response '
                                                                                          'describing the '
                                                                                          'problem with '
                                                                                          'buildings or '
                                                                                          'infrastructure '
                                                                                          'involved in the '
                                                                                          'reported event.',
                                                'lfpse_cqc_criteria': 'LFPSE response recording the Care '
                                                                      'Quality Commission notification '
                                                                      'criteria applicable to the reported '
                                                                      'event.',
                                                'lfpse_cqc_notify': 'LFPSE response indicating whether the '
                                                                    'Care Quality Commission should be '
                                                                    'notified of the reported event.',
                                                'lfpse_damaged_blood': 'LFPSE response relating to damaged '
                                                                       'blood involved in the reported '
                                                                       'event.',
                                                'lfpse_damaged_blood_products': 'LFPSE response relating to '
                                                                                'damaged blood products '
                                                                                'involved in the reported '
                                                                                'event.',
                                                'lfpse_data_sharing_opt_out': 'LFPSE response relating to '
                                                                              'data-sharing opt-out for the '
                                                                              'reported event.',
                                                'lfpse_date': 'Date recorded for the LFPSE event.',
                                                'lfpse_deprivation_of_liberty': 'LFPSE response relating to '
                                                                                'deprivation of liberty for '
                                                                                'the reported event.',
                                                'lfpse_designations_mental_health': 'LFPSE field recording '
                                                                                    'mental health '
                                                                                    'designations relevant '
                                                                                    'to the reported event.',
                                                'lfpse_detection_point': 'LFPSE field recording the point at '
                                                                         'which the event was detected.',
                                                'lfpse_device_involvement_factors': 'LFPSE field recording '
                                                                                    'the factors describing '
                                                                                    'how a device was '
                                                                                    'involved in the event.',
                                                'lfpse_device_type': 'LFPSE field recording the type of '
                                                                     'device involved in the event.',
                                                'lfpse_device_used_unnecessarily': 'LFPSE field giving '
                                                                                   'details where a device '
                                                                                   'was used unnecessarily '
                                                                                   'in the event.',
                                                'lfpse_drug_involvement_factors': 'LFPSE field recording the '
                                                                                  'factors describing how '
                                                                                  'medication was involved '
                                                                                  'in the event.',
                                                'lfpse_drug_reaction': 'LFPSE field recording a drug '
                                                                       'reaction associated with the event.',
                                                'lfpse_drug_used_too_much': 'LFPSE field giving details '
                                                                            'where too much of a medication '
                                                                            'was involved in the event.',
                                                'lfpse_drugs_involved': 'LFPSE field recording the '
                                                                        'medications involved in the event.',
                                                'lfpse_duty_of_candour': 'LFPSE field recording whether duty '
                                                                         'of candour applies to the event.',
                                                'lfpse_estates_services': 'LFPSE field recording estates '
                                                                          'services relevant to the event.',
                                                'lfpse_estates_services_involved': 'LFPSE field recording '
                                                                                   'whether or which estates '
                                                                                   'services were involved '
                                                                                   'in the event.',
                                                'lfpse_estates_services_not_used': 'LFPSE field giving '
                                                                                   'details where estates '
                                                                                   'services were not used '
                                                                                   'in relation to the '
                                                                                   'event.',
                                                'lfpse_estates_services_problem': 'LFPSE field describing '
                                                                                  'the problem with estates '
                                                                                  'services involved in the '
                                                                                  'event.',
                                                'lfpse_estimated_time': 'LFPSE field recording the estimated '
                                                                        'time of the event, stored as a '
                                                                        'string.',
                                                'lfpse_from_online_forms': 'LFPSE field indicating whether '
                                                                           'the record originated from Datix '
                                                                           'online forms.',
                                                'lfpse_good_care_detection_factor': 'LFPSE field recording '
                                                                                    'the factor or factors '
                                                                                    'through which good care '
                                                                                    'was detected in the '
                                                                                    'reported event.',
                                                'lfpse_hsib_notify': 'LFPSE field indicating whether the '
                                                                     'event is notifiable to the Healthcare '
                                                                     'Safety Investigation Branch (HSIB).',
                                                'lfpse_id': 'Record identifier for the safety incident '
                                                            'record from the Learn From Patient Safety '
                                                            'Events (LFPSE) dataset, sourced from the Datix '
                                                            'LFPSE record ID.',
                                                'lfpse_identified_location': 'LFPSE field recording the '
                                                                             'location identified as where '
                                                                             'the reported event occurred.',
                                                'lfpse_incident_occurred_today': 'LFPSE field indicating '
                                                                                 'whether the reported '
                                                                                 'incident occurred on the '
                                                                                 'day of reporting.',
                                                'lfpse_involved_agents': 'LFPSE field recording the agents '
                                                                         'involved in the event, such as '
                                                                         'medication, devices, blood '
                                                                         'products, tissues and organs, '
                                                                         'estates, IT systems or people.',
                                                'lfpse_involved_persons_actions': 'LFPSE field recording the '
                                                                                  'actions of people '
                                                                                  'involved that contributed '
                                                                                  'to the reported event.',
                                                'lfpse_involved_processes': 'LFPSE field recording the care '
                                                                            'processes involved in the '
                                                                            'reported event.',
                                                'lfpse_is_anonymous': 'LFPSE field indicating whether the '
                                                                      'report was submitted anonymously.',
                                                'lfpse_it_systems_involvement_factors': 'LFPSE field '
                                                                                        'recording how IT '
                                                                                        'systems or software '
                                                                                        'were involved in '
                                                                                        'the reported event.',
                                                'lfpse_level_of_concern': 'LFPSE field recording the '
                                                                          "reporter's level of concern about "
                                                                          'the reported event or risk.',
                                                'lfpse_local_authority_safeguarding': 'LFPSE field '
                                                                                      'indicating whether '
                                                                                      'the event involves or '
                                                                                      'was referred for '
                                                                                      'local authority '
                                                                                      'safeguarding.',
                                                'lfpse_location_at_risk': 'LFPSE field recording the '
                                                                          'location affected by an '
                                                                          'identified risk.',
                                                'lfpse_location_known': 'LFPSE field indicating whether the '
                                                                        'location of the reported event is '
                                                                        'known.',
                                                'lfpse_location_within_service': 'LFPSE field recording the '
                                                                                 'location within the '
                                                                                 'service where the reported '
                                                                                 'event occurred.',
                                                'lfpse_manufacturer': 'LFPSE field recording the '
                                                                      'manufacturer of a product, such as a '
                                                                      'device or medication, involved in the '
                                                                      'reported event.',
                                                'lfpse_marvin_reference_number': 'LFPSE field holding an '
                                                                                 'external MARVIN reference '
                                                                                 'number associated with the '
                                                                                 'reported event.',
                                                'lfpse_medication_administered_incorrectly': 'LFPSE field '
                                                                                             'recording '
                                                                                             'details of '
                                                                                             'medication '
                                                                                             'administered '
                                                                                             'incorrectly in '
                                                                                             'the reported '
                                                                                             'event.',
                                                'lfpse_medication_administrator': 'LFPSE field recording who '
                                                                                  'administered the '
                                                                                  'medication involved in '
                                                                                  'the reported event.',
                                                'lfpse_model': 'LFPSE field recording the model of a device '
                                                               'or product involved in the reported event.',
                                                'lfpse_never_event': 'LFPSE field recorded in Datix '
                                                                     'indicating whether the patient safety '
                                                                     'event was considered a Never Event.',
                                                'lfpse_never_event_type': 'LFPSE field recorded in Datix '
                                                                          'giving the type or category of '
                                                                          'Never Event associated with the '
                                                                          'patient safety event.',
                                                'lfpse_nhsbt_report_number': 'External reference number of a '
                                                                             'related report made to NHS '
                                                                             'Blood and Transplant (NHSBT), '
                                                                             'recorded for the LFPSE event.',
                                                'lfpse_organisation': 'LFPSE field recording the '
                                                                      'organisation associated with the '
                                                                      'reported patient safety event.',
                                                'lfpse_outcome_type': 'LFPSE field recording the type of '
                                                                      'outcome of the reported patient '
                                                                      'safety event.',
                                                'lfpse_patient_event': 'LFPSE field indicating or describing '
                                                                       'whether the recorded event is a '
                                                                       'patient event.',
                                                'lfpse_patient_safety_incident_has_occurred': 'LFPSE field '
                                                                                              'indicating '
                                                                                              'whether a '
                                                                                              'patient '
                                                                                              'safety '
                                                                                              'incident has '
                                                                                              'occurred, as '
                                                                                              'opposed to a '
                                                                                              'risk or other '
                                                                                              'event type.',
                                                'lfpse_people_action_factors': 'LFPSE field recording the '
                                                                               "factors relating to people's "
                                                                               'actions that were involved '
                                                                               'in the event.',
                                                'lfpse_people_action_too_much': 'LFPSE field recording '
                                                                                'details where people '
                                                                                'involved in the event took '
                                                                                'too much of an action.',
                                                'lfpse_people_involvement_factor': 'LFPSE field recording '
                                                                                   'how people were involved '
                                                                                   'as a factor in the '
                                                                                   'event.',
                                                'lfpse_precise_time': 'LFPSE field recording the precise '
                                                                      'time at which the event occurred, '
                                                                      'stored as text.',
                                                'lfpse_problem_medication_packaging': 'LFPSE field recording '
                                                                                      'problems with '
                                                                                      'medication packaging '
                                                                                      'involved in the '
                                                                                      'event.',
                                                'lfpse_radiotherapy_incident_code': 'LFPSE field recording '
                                                                                    'the radiotherapy '
                                                                                    'incident classification '
                                                                                    'code for the event.',
                                                'lfpse_record_lvl_one': 'LFPSE level-one record field held '
                                                                        'on the Datix LFPSE record.',
                                                'lfpse_record_lvl_two': 'LFPSE level-two record field held '
                                                                        'on the Datix LFPSE record.',
                                                'lfpse_reference_number': 'LFPSE reference number assigned '
                                                                          'to the reported event.',
                                                'lfpse_reporter_involvement': 'LFPSE field recording how the '
                                                                              'reporter was involved in the '
                                                                              'event.',
                                                'lfpse_reporter_organisation': 'Organisation of the person '
                                                                               'reporting the LFPSE event as '
                                                                               'captured in Datix.',
                                                'lfpse_reporter_role': 'Role of the person reporting the '
                                                                       'LFPSE event as captured in Datix.',
                                                'lfpse_reporter_type': 'Type of reporter submitting the '
                                                                       'LFPSE event record as captured in '
                                                                       'Datix.',
                                                'lfpse_responsible_specialty': 'Clinical specialty '
                                                                               'responsible for the care '
                                                                               'associated with the LFPSE '
                                                                               'event.',
                                                'lfpse_risk_identified_location': 'Location where the '
                                                                                  'patient safety risk was '
                                                                                  'identified for an LFPSE '
                                                                                  'risk record.',
                                                'lfpse_risk_imminent': 'Indicator of whether the recorded '
                                                                       'patient safety risk is imminent.',
                                                'lfpse_risk_population': 'Population or patient group '
                                                                         'potentially affected by the '
                                                                         'recorded patient safety risk.',
                                                'lfpse_risk_service_area': 'Service area associated with the '
                                                                           'recorded patient safety risk.',
                                                'lfpse_risk_theme': 'Theme or category assigned to the '
                                                                    'recorded patient safety risk.',
                                                'lfpse_risk_timeframe': 'Timeframe within which the recorded '
                                                                        'patient safety risk is expected to '
                                                                        'materialise.',
                                                'lfpse_sabre_report_number': 'External SABRE report '
                                                                             'reference number linked to the '
                                                                             'LFPSE event.',
                                                'lfpse_safety_challenges': 'Safety challenges recorded as '
                                                                           'relevant to the LFPSE event.',
                                                'lfpse_serious_incident': 'Indicator of whether the LFPSE '
                                                                          'event is classed as a serious '
                                                                          'incident.',
                                                'lfpse_service_area': 'Service area in which the LFPSE event '
                                                                      'occurred as captured in Datix.',
                                                'lfpse_shot_report_number': 'External SHOT haemovigilance '
                                                                            'report reference number linked '
                                                                            'to the LFPSE event.',
                                                'lfpse_tissue_organs_involvement_factor': 'Factor describing '
                                                                                          'how tissues or '
                                                                                          'organs were '
                                                                                          'involved in the '
                                                                                          'LFPSE event.',
                                                'lfpse_tissue_organs_not_used': 'Details recorded where '
                                                                                'tissues or organs were not '
                                                                                'used when they should have '
                                                                                'been in the LFPSE event.',
                                                'lfpse_tissue_organs_used_too_much': 'Details recorded where '
                                                                                     'too much tissue or '
                                                                                     'organ material was '
                                                                                     'used in the LFPSE '
                                                                                     'event.',
                                                'lfpse_too_little_blood_products': 'LFPSE reporting field '
                                                                                   'capturing information on '
                                                                                   'too little of a blood '
                                                                                   'product being used in '
                                                                                   'the recorded event.',
                                                'lfpse_too_little_blood_used': 'LFPSE reporting field '
                                                                               'capturing information on too '
                                                                               'little blood being used in '
                                                                               'the recorded event.',
                                                'lfpse_too_much_blood_products': 'LFPSE reporting field '
                                                                                 'capturing information on '
                                                                                 'too much of a blood '
                                                                                 'product being used in the '
                                                                                 'recorded event.',
                                                'lfpse_too_much_blood_used': 'LFPSE reporting field '
                                                                             'capturing information on too '
                                                                             'much blood being used in the '
                                                                             'recorded event.',
                                                'lfpse_type': 'LFPSE event type recorded for the Datix '
                                                              'record.',
                                                'lfpse_wrong_blood': 'LFPSE reporting field capturing '
                                                                     'information on the wrong blood being '
                                                                     'involved in the recorded event.',
                                                'lfpse_wrong_blood_products': 'LFPSE reporting field '
                                                                              'capturing information on the '
                                                                              'wrong blood product being '
                                                                              'involved in the recorded '
                                                                              'event.',
                                                'lfpse_wrong_buildings_infrastructure': 'LFPSE reporting '
                                                                                        'field capturing '
                                                                                        'information on '
                                                                                        'wrong buildings or '
                                                                                        'infrastructure '
                                                                                        'involved in the '
                                                                                        'recorded event.',
                                                'lfpse_wrong_estates_services': 'LFPSE reporting field '
                                                                                'capturing information on '
                                                                                'wrong estates services '
                                                                                'involved in the recorded '
                                                                                'event.',
                                                'lfpse_yellow_card_reference': 'External Yellow Card '
                                                                               'reference number associated '
                                                                               'with the LFPSE record.',
                                                'load_batch_id': 'Load batch label derived from loaded_at '
                                                                 '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                 'time exists.',
                                                'loaded_at': 'When the row was last written to bronze '
                                                             '(ADC_UPDT), else its source update time; the '
                                                             'stable load clock.',
                                                'med_id': 'Identifier of the Datix medication record that '
                                                          'this LFPSE row relates to, where applicable.',
                                                'pipeline_run_id': 'Bronze pipeline run that last wrote the '
                                                                   'row.',
                                                'record_status': 'active, or superseded when the row stopped '
                                                                 'appearing in the source snapshot '
                                                                 '(source_present_ind false).',
                                                'record_status_effective_from': 'Start of the record status; '
                                                                                'not supplied by this source '
                                                                                '(always NULL).',
                                                'record_status_effective_to': 'When a superseded row was '
                                                                              'first detected absent from '
                                                                              'the source '
                                                                              '(source_absent_detected_ts); '
                                                                              'NULL while active.',
                                                'row_hash': "SHA-256 of the row's payload columns, used by "
                                                            'the bronze MERGE to detect changes.',
                                                'safety_incident_lfpse_key': 'Deterministic SHA-256 key of '
                                                                             'the safety incident lfpse row '
                                                                             '(namespace plus native source '
                                                                             'key).',
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
                                                                           'changed in the source system; do '
                                                                           'not use it for recency.',
                                                'source_updt_dt_tm': 'Last-update time of the row in the '
                                                                     'source system (for Datix, '
                                                                     'updateddate); NULL when the source '
                                                                     'supplies none.'},
                                    'table': 'One row per LFPSE (Learn from Patient Safety Events) '
                                             'submission linked to a Datix incident, keyed by RECORDID.'},
 'clinical_safety_incident_participant': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                      'con_address': 'Address of the contact.',
                                                      'con_clingroup': 'Clinical group code associated with '
                                                                       "the Datix contact's organisational "
                                                                       'placement.',
                                                      'con_directorate': 'Directorate code associated with '
                                                                         "the Datix contact's organisational "
                                                                         'placement.',
                                                      'con_disability': 'Disability status or details '
                                                                        'recorded for the Datix contact.',
                                                      'con_dob': 'Date of birth of the contact.',
                                                      'con_dod': 'Date of death recorded for the Datix '
                                                                 'contact.',
                                                      'con_email': 'Email address recorded for the Datix '
                                                                   'contact.',
                                                      'con_empl_grade': 'Employment grade recorded for the '
                                                                        'Datix contact.',
                                                      'con_ethnicity': 'Ethnicity of the contact.',
                                                      'con_fax': 'Fax number of the contact.',
                                                      'con_forenames': 'Forename(s) of the contact.',
                                                      'con_gender': 'Gender of the contact.',
                                                      'con_id': 'Identifier of the Datix contact record for '
                                                                'the patient associated with this LFPSE '
                                                                'submission.',
                                                      'con_jobtitle': 'Job title of the contact as free '
                                                                      'text.',
                                                      'con_language': 'Language recorded for the Datix '
                                                                      'contact.',
                                                      'con_locactual': 'Actual location code or codes '
                                                                       'associated with the Datix contact.',
                                                      'con_mrn': 'Medical record number recorded for the '
                                                                 'Datix contact participating in the safety '
                                                                 'incident, sourced from the Datix contact '
                                                                 'number field.',
                                                      'con_nhsno': 'NHS number recorded for the Datix '
                                                                   'contact.',
                                                      'con_orgcode': 'Organisation code associated with the '
                                                                     "Datix contact's organisational "
                                                                     'placement.',
                                                      'con_postcode': "Postcode of the contact's address.",
                                                      'con_religion': 'Religion or belief recorded for the '
                                                                      'contact.',
                                                      'con_sex_orientation': 'Sexual orientation recorded '
                                                                             'for the contact.',
                                                      'con_specialty': 'Specialty code associated with the '
                                                                       "Datix contact's organisational "
                                                                       'placement.',
                                                      'con_subtype': 'Code for the subtype of the Datix '
                                                                     'contact, refining the contact type.',
                                                      'con_surname': 'Surname of the contact.',
                                                      'con_tel1': 'Primary telephone number of the contact.',
                                                      'con_tel2': 'Secondary telephone number of the '
                                                                  'contact.',
                                                      'con_title': 'Title or honorific of the contact.',
                                                      'con_type': 'Code indicating the type of contact, such '
                                                                  'as patient or staff category.',
                                                      'con_unit': 'Unit code associated with the Datix '
                                                                  "contact's organisational placement.",
                                                      'dob_agrees_ind': 'Indicator of whether the date of '
                                                                        'birth recorded on the Datix contact '
                                                                        'agrees with the birth date of the '
                                                                        'resolved person record.',
                                                      'identity_status': 'resolved when person_id is '
                                                                         'present, unresolved when absent, '
                                                                         'not_applicable for tables without '
                                                                         'a patient.',
                                                      'inc_id': 'Identifier of the Datix incident record '
                                                                'associated with this LFPSE patient detail '
                                                                'record.',
                                                      'lfpse_age_at_time_of_incidents_days': "Patient's age "
                                                                                             'at the time of '
                                                                                             'the incident, '
                                                                                             'expressed in '
                                                                                             'days, as '
                                                                                             'recorded for '
                                                                                             'the LFPSE '
                                                                                             'submission.',
                                                      'lfpse_age_bracket': "Raw code for the patient's age "
                                                                           'bracket at the time of the '
                                                                           'incident, as recorded for the '
                                                                           'LFPSE submission.',
                                                      'lfpse_gender': "Raw code for the patient's gender as "
                                                                      'recorded for the LFPSE submission.',
                                                      'lfpse_id': 'Identifier of the Learn from Patient '
                                                                  'Safety Events (LFPSE) submission to which '
                                                                  'this patient detail record belongs.',
                                                      'lfpse_patient_ethnicity': "Raw code for the patient's "
                                                                                 'ethnicity as recorded for '
                                                                                 'the LFPSE submission.',
                                                      'lfpse_patient_recordid': 'Record identifier of the '
                                                                                'linked patient details '
                                                                                'record in the Datix LFPSE '
                                                                                'patient details source, '
                                                                                'where one applies.',
                                                      'lfpse_patient_sequence': 'Sequence number '
                                                                                'distinguishing individual '
                                                                                'patients within an LFPSE '
                                                                                'submission.',
                                                      'lfpse_physical_harm': 'Raw code for the level of '
                                                                             'physical harm to the patient '
                                                                             'recorded for the LFPSE '
                                                                             'submission.',
                                                      'lfpse_psychological_harm': 'Raw code for the level of '
                                                                                  'psychological harm to the '
                                                                                  'patient recorded for the '
                                                                                  'LFPSE submission.',
                                                      'lfpse_strength_of_association': 'Raw code for the '
                                                                                       'strength of '
                                                                                       'association between '
                                                                                       'the incident and the '
                                                                                       'patient harm '
                                                                                       'recorded for the '
                                                                                       'LFPSE submission.',
                                                      'link_abs_end': "End date of the contact's absence "
                                                                      'recorded against this link.',
                                                      'link_abs_start': "Start date of the contact's absence "
                                                                        'recorded against this link.',
                                                      'link_age': 'Age of the linked contact recorded '
                                                                  'against the link to the Datix record.',
                                                      'link_age_band': 'Age band of the linked contact '
                                                                       'recorded against this link.',
                                                      'link_attempted_assault': 'Indicator of whether the '
                                                                                'incident involving the '
                                                                                'linked contact involved an '
                                                                                'attempted assault.',
                                                      'link_become_unconscious': 'Indicator of whether the '
                                                                                 'linked contact became '
                                                                                 'unconscious as a result of '
                                                                                 'the incident.',
                                                      'link_bodypart1': 'Primary body part injured for the '
                                                                        'linked contact in relation to the '
                                                                        'linked Datix record.',
                                                      'link_clin_factors': 'Clinical factors recorded for '
                                                                           'the linked contact in relation '
                                                                           'to the incident.',
                                                      'link_date_admission': 'Date of admission recorded for '
                                                                             'the linked contact on the '
                                                                             'Datix record.',
                                                      'link_daysaway': 'Number of days the linked contact '
                                                                       'was away from work as a result of '
                                                                       'the event.',
                                                      'link_deceased': 'Indicator of whether the linked '
                                                                       'contact is deceased.',
                                                      'link_direct_indirect': 'Indicator of whether the '
                                                                              'linked contact was directly '
                                                                              'or indirectly involved in, or '
                                                                              'affected by, the incident.',
                                                      'link_discomfort_caused': 'Indicator of whether '
                                                                                'discomfort was caused to '
                                                                                'the linked contact in the '
                                                                                'incident.',
                                                      'link_dod': 'Date of death of the linked contact as '
                                                                  'recorded for this link.',
                                                      'link_harassment': 'Indicator of whether the incident '
                                                                         'involving the linked contact '
                                                                         'involved harassment.',
                                                      'link_hospital_24hours': 'Indicator of whether the '
                                                                               'linked contact was admitted '
                                                                               'to hospital for more than 24 '
                                                                               'hours following the '
                                                                               'incident.',
                                                      'link_injuries': 'Injury details recorded for the '
                                                                       'linked contact in relation to the '
                                                                       'linked Datix record.',
                                                      'link_injury1': 'Primary injury recorded for the '
                                                                      'linked contact in relation to the '
                                                                      'linked Datix record.',
                                                      'link_injury_caused': 'Indicator of whether injury was '
                                                                            'caused to the linked contact in '
                                                                            'the incident.',
                                                      'link_is_riddor': 'Indicator of whether the event is '
                                                                        'reportable under RIDDOR for the '
                                                                        'linked contact.',
                                                      'link_mh_observe': 'Mental health observation level '
                                                                         'recorded for the linked contact.',
                                                      'link_mhact': 'Indicator of whether the linked contact '
                                                                    'was detained under the Mental Health '
                                                                    'Act.',
                                                      'link_mhact_section': 'Mental Health Act section '
                                                                            'applicable to the linked '
                                                                            'contact.',
                                                      'link_mhcpa': 'Mental health Care Programme Approach '
                                                                    'status recorded for the linked contact.',
                                                      'link_npsa': 'NPSA-related classification recorded for '
                                                                   'the linked contact on the Datix record.',
                                                      'link_npsa_role': 'NPSA-related role of the linked '
                                                                        'contact in the Datix record.',
                                                      'link_patrelation': 'Relationship of the linked '
                                                                          'contact to the patient involved '
                                                                          'in the Datix record.',
                                                      'link_primary': 'Flag for whether this contact link is '
                                                                      'the primary link for the contact on '
                                                                      'the associated Datix record.',
                                                      'link_req_resuscitation': 'Indicator of whether the '
                                                                                'linked contact required '
                                                                                'resuscitation as a result '
                                                                                'of the incident.',
                                                      'link_riddor': 'RIDDOR reporting detail recorded for '
                                                                     'the contact on the linked Datix '
                                                                     'record.',
                                                      'link_role': 'Code indicating the role the contact '
                                                                   'plays in relation to the linked Datix '
                                                                   'record.',
                                                      'link_role_display': 'Descriptive label for the Datix '
                                                                           'link role code.',
                                                      'link_status': 'Status of the contact in relation to '
                                                                     'the linked Datix record.',
                                                      'link_treatment': 'Treatment received by the linked '
                                                                        'contact in connection with the '
                                                                        'Datix record.',
                                                      'link_type': 'Code indicating the type of link between '
                                                                   'the contact and the Datix record.',
                                                      'link_verbal_abuse': 'Indicator of whether the '
                                                                           'incident involving the linked '
                                                                           'contact involved verbal abuse.',
                                                      'link_worked_alone': 'Indicator of whether the linked '
                                                                           'contact was working alone at the '
                                                                           'time of the incident.',
                                                      'load_batch_id': 'Load batch label derived from '
                                                                       'loaded_at (yyyyMMddHHmmss); tdx-v21 '
                                                                       'when no load time exists.',
                                                      'loaded_at': 'When the row was last written to bronze '
                                                                   '(ADC_UPDT), else its source update time; '
                                                                   'the stable load clock.',
                                                      'login': 'Datix user account login name for the '
                                                               'contact.',
                                                      'participant_class': 'Classification of the incident '
                                                                           'participant derived from the '
                                                                           'Datix contact type and contact '
                                                                           'link role.',
                                                      'participant_row_id': 'Source record identifier for '
                                                                            'the participant link row, taken '
                                                                            'from the Datix contact link or '
                                                                            'LFPSE patient details record.',
                                                      'participant_source': 'Indicates the source from which '
                                                                            'the safety incident participant '
                                                                            'record was derived, carried '
                                                                            'from the bronze safety incident '
                                                                            'participant mapping.',
                                                      'person_id': 'This is the value of the unique primary '
                                                                   'identifier of the person table.  It is '
                                                                   'an internal system assigned number.',
                                                      'person_resolution_rule': 'Rule used to resolve the '
                                                                                'incident participant to a '
                                                                                'person record through the '
                                                                                'patient identifier mapping.',
                                                      'person_resolution_status': 'Outcome status of the '
                                                                                  'attempt to resolve the '
                                                                                  'incident participant to a '
                                                                                  'person record.',
                                                      'pipeline_run_id': 'Bronze pipeline run that last '
                                                                         'wrote the row.',
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
                                                      'safety_incident_participant_key': 'Deterministic '
                                                                                         'SHA-256 key of the '
                                                                                         'safety incident '
                                                                                         'participant row '
                                                                                         '(namespace plus '
                                                                                         'native source '
                                                                                         'key).',
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
                                                      'source_update_timestamp': 'Time the source row was '
                                                                                 'landed in the raw layer '
                                                                                 '(bronze SOURCE_ADC_UPDT), '
                                                                                 'not when it changed in the '
                                                                                 'source system; do not use '
                                                                                 'it for recency.',
                                                      'source_updt_dt_tm': 'Last-update time of the row in '
                                                                           'the source system (for Datix, '
                                                                           'updateddate); NULL when the '
                                                                           'source supplies none.',
                                                      'subject_id_system': 'System of subject_key: '
                                                                           'urn:cerner:person_id, or '
                                                                           'nosubject when the row names no '
                                                                           'patient.',
                                                      'subject_key': 'Deterministic subject key: hash of the '
                                                                     'Cerner person id, or of the source row '
                                                                     'when no person is recorded.',
                                                      'total_absence': 'Total absence recorded for the '
                                                                       'linked contact, likely related to '
                                                                       'the absence start and end dates; '
                                                                       'units are not documented.'},
                                          'table': 'One row per person linked to a Datix incident (patient, '
                                                   'staff or other), keyed by the Datix link record id.'},
 'clinical_safety_incident_status': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                                 'identity_status': 'resolved when person_id is present, '
                                                                    'unresolved when absent, not_applicable '
                                                                    'for tables without a patient.',
                                                 'inc_id': 'Identifier of the clinical safety incident to '
                                                           'which this status audit record relates, sourced '
                                                           'from the Datix incident record ID.',
                                                 'load_batch_id': 'Load batch label derived from loaded_at '
                                                                  '(yyyyMMddHHmmss); tdx-v21 when no load '
                                                                  'time exists.',
                                                 'loaded_at': 'When the row was last written to bronze '
                                                              '(ADC_UPDT), else its source update time; the '
                                                              'stable load clock.',
                                                 'login': 'Login of the user who recorded or changed the '
                                                          'incident status in Datix.',
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
                                                 'row_hash': "SHA-256 of the row's payload columns, used by "
                                                             'the bronze MERGE to detect changes.',
                                                 'safety_incident_status_key': 'Deterministic SHA-256 key of '
                                                                               'the safety incident status '
                                                                               'row (namespace plus native '
                                                                               'source key).',
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
                                                 'status': 'Status value assigned to the safety incident at '
                                                           'this point in its audit history.',
                                                 'status_dt_tm': 'Date and time at which the incident status '
                                                                 'was recorded.',
                                                 'status_row_id': 'Derived identifier for an individual '
                                                                  'incident status audit row, built from the '
                                                                  'status date and user login.',
                                                 'status_sequence': 'Order of this status change within the '
                                                                    "incident's status history, derived from "
                                                                    'the status date and login.'},
                                     'table': 'One row per status change of a Datix incident.'},
 'text_safety_document': {'columns': {'adc_updt': 'When bronze last wrote the row.',
                                      'affected_patient_count': 'Number of patients affected by the '
                                                                'associated safety incident.',
                                      'anon_context_fingerprint': 'Hash of the contextual inputs the '
                                                                  'redactor used for this row; the '
                                                                  'anonymised text is released only while it '
                                                                  'matches the current context.',
                                      'anon_identity_fingerprint': 'Hash of the patient identity details the '
                                                                   'record-driven redactor used for this '
                                                                   'row.',
                                      'anon_input_digest': 'SHA-256 digest of the fragment text as presented '
                                                           'to the redactor; a changed digest marks the row '
                                                           'for re-anonymisation.',
                                      'anon_processed_at': "When the redactor last processed this row's "
                                                           'text.',
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
                                      'anon_text': 'Anonymised version of text: identifiers detected in the '
                                                   'free text are replaced by placeholders; published '
                                                   'alongside the source column, which gold releases only '
                                                   'when anonymisation succeeded.',
                                      'context_fingerprint_current': "SHA-256 of the incident's current "
                                                                     'identifying context (names, birth '
                                                                     'date, postcode, reporter, investigator '
                                                                     'and participants); anonymised '
                                                                     'narrative text is released only while '
                                                                     'its anon_context_fingerprint matches '
                                                                     'this value.',
                                      'document_text': 'Source incident narrative fragment from Datix, not '
                                                       'anonymised (a copy of text); the anonymised fragment '
                                                       'is document_text_anonymised, and gold publishes only '
                                                       'released anonymised text in document_text.',
                                      'document_text_anonymised': 'Anonymised free-text content of the '
                                                                  'safety incident document assembled from '
                                                                  'text fragments.',
                                      'document_type_code': 'Code identifying the type of safety document, '
                                                            'derived from the fragment role.',
                                      'document_type_display': 'Display label for the type of safety '
                                                               'document, derived from the fragment role.',
                                      'field_name': 'Name of the source field from which the safety text '
                                                    'fragment was taken.',
                                      'fragment_id': 'Identifier of the safety text fragment, carried from '
                                                     'the bronze safety text fragment mapping and derived '
                                                     'from Datix action or incident record identifiers.',
                                      'fragment_role': 'Role or category of the safety text fragment within '
                                                       'the incident documentation.',
                                      'identity_status': 'resolved when person_id is present, unresolved '
                                                         'when absent, not_applicable for tables without a '
                                                         'patient.',
                                      'inc_id': 'Identifier of the Datix incident record that this LFPSE row '
                                                'relates to.',
                                      'inc_ourref': 'Local incident reference number assigned by the '
                                                    'organisation in Datix.',
                                      'incident_dt_tm': 'Date and time at which the patient safety incident '
                                                        'occurred, carried through from the source incident '
                                                        'record via the bronze incident mappings.',
                                      'incident_id': 'Identifier of the Datix incident to which the safety '
                                                     'text fragment relates, carried from the bronze safety '
                                                     'text fragment mapping and derived from the Datix '
                                                     'incident record identifier.',
                                      'incident_person_id': 'Internal person identifier for the patient '
                                                            'associated with the safety incident.',
                                      'load_batch_id': 'Load batch label derived from loaded_at '
                                                       '(yyyyMMddHHmmss); tdx-v21 when no load time exists.',
                                      'loaded_at': 'When the row was last written to bronze (ADC_UPDT), else '
                                                   'its source update time; the stable load clock.',
                                      'person_id': 'Millennium PERSON_ID of the patient (identifier); NULL '
                                                   'when the source row names no patient.',
                                      'pipeline_run_id': 'Bronze pipeline run that last wrote the row.',
                                      'priority_basis': 'Basis or rationale used to assign priority to the '
                                                        'safety text document.',
                                      'priority_dt_tm': 'Timestamp used to prioritise or order the safety '
                                                        'text fragment, taken from the fragment mapping and '
                                                        'derived from Datix action update date fields.',
                                      'record_status': 'active, or superseded when the row stopped appearing '
                                                       'in the source snapshot (source_present_ind false).',
                                      'record_status_effective_from': 'Start of the record status; not '
                                                                      'supplied by this source (always '
                                                                      'NULL).',
                                      'record_status_effective_to': 'When a superseded row was first '
                                                                    'detected absent from the source '
                                                                    '(source_absent_detected_ts); NULL while '
                                                                    'active.',
                                      'row_hash': "SHA-256 of the row's payload columns, used by the bronze "
                                                  'MERGE to detect changes.',
                                      'safety_document_key': 'Deterministic SHA-256 key of the safety '
                                                             'document row (namespace plus native source '
                                                             'key).',
                                      'source_absent_detected_ts': 'When bronze first found the row absent '
                                                                   'from the source snapshot; NULL while '
                                                                   'present.',
                                      'source_adc_updt': 'When the source row was landed in raw (raw '
                                                         'ADC_UPDT).',
                                      'source_object': 'Source object from which the safety text fragment '
                                                       'was derived, based on the source table name.',
                                      'source_object_table': 'Name of the source table from which the safety '
                                                             'text fragment originated.',
                                      'source_present_ind': 'True while the row is present in the latest '
                                                            'full source snapshot.',
                                      'source_record_id': 'Record identifier of the originating Datix source '
                                                          'record, either an action or an incident, from '
                                                          'which the text fragment was taken.',
                                      'source_row_id': 'Native source row identifier, as text.',
                                      'source_table': 'Bronze or raw source table the row came from.',
                                      'source_update_timestamp': 'Time the source row was landed in the raw '
                                                                 'layer (bronze SOURCE_ADC_UPDT), not when '
                                                                 'it changed in the source system; do not '
                                                                 'use it for recency.',
                                      'source_updt_dt_tm': 'Last-update time of the row in the source system '
                                                           '(for Datix, updateddate); NULL when the source '
                                                           'supplies none.',
                                      'subject_id_system': 'System of subject_key: urn:cerner:person_id, or '
                                                           'nosubject when the row names no patient.',
                                      'subject_key': 'Deterministic subject key: hash of the Cerner person '
                                                     'id, or of the source row when no person is recorded.',
                                      'text': 'Free-text content of the safety fragment, drawn from Datix '
                                              'action description and action monitoring narrative fields.',
                                      'text_is_anonymised': 'True when document_text_anonymised is '
                                                            'releasable: anonymisation succeeded with '
                                                            'redactor v3.3 and its input digest and incident '
                                                            'context fingerprint still match; gold releases '
                                                            'only these rows.',
                                      'text_length': 'Character length of document_text, the source '
                                                     'narrative fragment before anonymisation.',
                                      'text_sha256': 'SHA-256 of document_text, the source narrative '
                                                     'fragment before anonymisation (bronze TEXT_SHA256).'},
                          'table': 'One row per Datix incident narrative fragment (description, action, '
                                   'outcome and similar free text), with the released anonymised text.'}}
