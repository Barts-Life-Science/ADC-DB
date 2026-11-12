"""Silver spine.practitioner: 2 dataset(s).

- datasets: reference_practitioner, _reference_practitioner_metadata
- moved verbatim from: 1_silver_spine_and_reference:1399, 1_silver_spine_and_reference:1403, 1_silver_spine_and_reference:1407, 1_silver_spine_and_reference:1414, 1_silver_spine_and_reference:1628, 1_silver_spine_and_reference:1639

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import (
    SRC_APPOINTMENT,
    SRC_APPOINTMENT_RESOURCE,
    SRC_CODED_EVENTS,
    SRC_DATE_EVENTS,
    SRC_DIAGNOSIS,
    SRC_ENCOUNTER,
    SRC_FORM_ACTIVITY,
    SRC_IMPLANT_DETAILS,
    SRC_MEDICATION_ORDER,
    SRC_MEDICATION_ORDER_ACTION,
    SRC_MED_ADMIN,
    SRC_MILL_BLOB_TEXT,
    SRC_NOMEN_EVENTS,
    SRC_NUMERIC_EVENTS,
    SRC_PROBLEM,
    SRC_TEXT_EVENTS,
    SRC_THEATRE_CASE,
    SRC_THEATRE_CASE_PROCEDURE,
)
from silver_tables.spine._shared import SRC_PRACTITIONER
from silver_tables.spine.encounter import encounter


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


PRACTITIONER_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


PRACTITIONER_RETIRED_COLUMNS = [

]


PRACTITIONER_LIFECYCLE_COLUMNS = [
    'practitioner_key',
    'practitioner_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_practitioner():
    # Assemble practitioner rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_PRACTITIONER)
    # contract v2: retain practitioner, location, and service hashes as keys while publishing personnel ids as BIGINT
    directory = s.select(
        stable_id("practitioner:mill", s.PERSON_ID).alias("practitioner_key"),
        s.PERSON_ID.cast("bigint").alias("practitioner_id"),
        s.PERSON_ID.cast("bigint").alias("source_practitioner_id"),
        (s.ACTIVE_IND == 1).alias("active"),
        s.NAME_FULL_FORMATTED.alias("name"),
        s.PHYSICIAN_IND.alias("physician_ind"),
        s.POSITION_CD.cast("string").alias("position_code"),
        s.position_name.alias("position_display"),
        s.PRSNL_TYPE_CD.cast("string").alias("practitioner_type_code"),
        s.prsnl_type_name.alias("practitioner_type_display"),
        F.when(s.primary_care_site_cd.isNotNull(),
               stable_id("location:mill:nurse_unit", s.primary_care_site_cd)).alias("primary_location_key"),
        F.when(s.MEDSERVICE_GROUP_ID.isNotNull(),
               stable_id("service:mill:medical", s.MEDSERVICE_GROUP_ID)).alias("medical_service_key"),
        s.NPI.alias("npi"),
        s.DOCNBR.alias("doctor_number"),
        s.GDP_NUMBER.alias("gdp_number"),
        s.EXTERNAL_PROVIDER_ID.alias("external_provider_id"),
        F.when(s.ACTIVE_IND == 1, F.lit("active")).otherwise(F.lit("superseded")).alias("record_status"),
        s.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        s.END_EFFECTIVE_DT_TM.alias("valid_to"),
        F.lit(SRC_PRACTITIONER).alias("source_table"),
        s.PERSON_ID.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )
    encounter = read_source(SRC_ENCOUNTER)
    referenced = (
        encounter.where(encounter.REG_PRSNL_ID.isNotNull())
                 .select(encounter.REG_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                         encounter.ADC_UPDT.alias("loaded_at"))
        .unionByName(
            encounter.where(encounter.CREATE_PRSNL_ID.isNotNull())
                     .select(encounter.CREATE_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                             encounter.ADC_UPDT.alias("loaded_at"))
        )
        .unionByName(
            encounter.where(encounter.DISCH_PRSNL_ID.isNotNull())
                     .select(encounter.DISCH_PRSNL_ID.cast("bigint").alias("source_practitioner_id"),
                             encounter.ADC_UPDT.alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DIAGNOSIS).where(F.col("DIAG_PRSNL_ID").isNotNull())
              .select(F.col("DIAG_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_PROBLEM).where(F.col("ACTIVE_STATUS_PRSNL_ID").isNotNull())
              .select(F.col("ACTIVE_STATUS_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_IMPLANT_DETAILS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.coalesce(F.col("SOURCE_MAX_ADC_UPDT"), F.col("ADC_UPDT")).alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_FORM_ACTIVITY).where(F.col("PERFORMED_PRSNL_ID_LONG").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID_LONG").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NUMERIC_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("PRSNL_ID").isNotNull())
              .select(F.col("PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("CE_VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("CE_VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MED_ADMIN).where(F.col("MAE_VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("MAE_VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER).where(F.col("LAST_UPDATE_PROVIDER_ID").isNotNull())
              .select(F.col("LAST_UPDATE_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("ACTION_PERSONNEL_ID").isNotNull())
              .select(F.col("ACTION_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("ORDER_PROVIDER_ID").isNotNull())
              .select(F.col("ORDER_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MEDICATION_ORDER_ACTION).where(F.col("SUPERVISING_PROVIDER_ID").isNotNull())
              .select(F.col("SUPERVISING_PROVIDER_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_CODED_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_CODED_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NOMEN_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_NOMEN_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DATE_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_DATE_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_TEXT_EVENTS).where(F.col("PERFORMED_PRSNL_ID").isNotNull())
              .select(F.col("PERFORMED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_TEXT_EVENTS).where(F.col("VERIFIED_PRSNL_ID").isNotNull())
              .select(F.col("VERIFIED_PRSNL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_MILL_BLOB_TEXT).where(F.col("UPDT_ID").isNotNull())
              .select(F.col("UPDT_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_APPOINTMENT).where(F.col("REQUESTED_PERSONNEL_ID").isNotNull())
              .select(F.col("REQUESTED_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_APPOINTMENT_RESOURCE).where(F.col("ALLOCATED_PERSONNEL_ID").isNotNull())
              .select(F.col("ALLOCATED_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE).where(F.col("SURGEON_PERSONNEL_ID").isNotNull())
              .select(F.col("SURGEON_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE).where(F.col("ANAESTHETIST_PERSONNEL_ID").isNotNull())
              .select(F.col("ANAESTHETIST_PERSONNEL_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .unionByName(
            read_source(SRC_THEATRE_CASE_PROCEDURE).where(F.col("PRIMARY_SURGEON_ID").isNotNull())
              .select(F.col("PRIMARY_SURGEON_ID").cast("bigint").alias("source_practitioner_id"),
                      F.col("ADC_UPDT").alias("loaded_at"))
        )
        .groupBy("source_practitioner_id")
        .agg(F.max("loaded_at").alias("loaded_at"))
    )
    known = s.select(s.PERSON_ID.cast("bigint").alias("source_practitioner_id"))
    missing = referenced.join(known, "source_practitioner_id", "left_anti")
    placeholders = missing.select(
        stable_id("practitioner:mill", F.col("source_practitioner_id")).alias("practitioner_key"),
        F.col("source_practitioner_id").cast("bigint").alias("practitioner_id"),
        F.col("source_practitioner_id"),
        F.lit(None).cast("boolean").alias("active"),
        F.lit(None).cast("string").alias("name"),
        F.lit(None).cast("boolean").alias("physician_ind"),
        F.lit(None).cast("string").alias("position_code"),
        F.lit(None).cast("string").alias("position_display"),
        F.lit(None).cast("string").alias("practitioner_type_code"),
        F.lit(None).cast("string").alias("practitioner_type_display"),
        F.lit(None).cast("string").alias("primary_location_key"),
        F.lit(None).cast("string").alias("medical_service_key"),
        F.lit(None).cast("string").alias("npi"),
        F.lit(None).cast("string").alias("doctor_number"),
        F.lit(None).cast("string").alias("gdp_number"),
        F.lit(None).cast("string").alias("external_provider_id"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(SRC_ENCOUNTER).alias("source_table"),
        F.col("source_practitioner_id").alias("source_row_id"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("loaded_at"),
    )
    return directory.unionByName(placeholders)


@materialized_view(
    name=_n("journey_reference.practitioner"),
    refresh_policy="auto",
)
def practitioner():
    # Build the declared dataset: One effective-dated Millennium practitioner/personnel row;
    # inactive rows are retained.
    return _lifecycle_source_practitioner().drop(*PRACTITIONER_LIFECYCLE_FIELDS, *PRACTITIONER_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_reference._practitioner_metadata'),
    refresh_policy="auto",
)
def practitioner_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # reference_practitioner; same row grain as the research table. Join keys: practitioner_key,
    # practitioner_id.
    return (_lifecycle_source_practitioner()).select(*PRACTITIONER_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'reference_practitioner': ValuePolicy(table='reference_practitioner', enabled=True, hub_zero_key='practitioner_id'),
}


WRAP_POLICY = {'reference_practitioner'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_practitioner_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                  'activity_record_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'admission_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                  'allergy_instance_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                  'aria_dx_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'aria_pt_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'artifact_link_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'birth_row_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                  'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                  'care_contact_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'care_participation_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                  'cds_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                                  'chemotherapy_course_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                  'community_care_activity_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                  'community_care_contact_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                  'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                  'cost_line_item_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'dcp_forms_activity_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                  'device_mapping_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'diagnosis_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'dicom_path': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                  'doc_response_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                  'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                  'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                  'encounter_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'encounter_id_resolved': 'True when encounter_id is null '
                                                                           'or resolves to the governed '
                                                                           'encounter spine; false records '
                                                                           'unresolved linkage without '
                                                                           'dropping the row.',
                                                  'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                  'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                  'endobase_exam_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'endobase_exam_term_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                  'entity_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                  'entry_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                  'episode_encounter_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                  'episode_encounter_reltn_id': 'Native source identifier '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                  'episode_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'episode_id_resolved': 'True when episode_id is null or '
                                                                         'resolves to the governed episode '
                                                                         'spine; false records unresolved '
                                                                         'linkage without dropping the row.',
                                                  'episode_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                  'event_after_death_30d': 'True when the event timestamp is '
                                                                           'more than 30 days after the '
                                                                           "resolved person's death "
                                                                           'timestamp; silver records the '
                                                                           'doubt and does not filter the '
                                                                           'row.',
                                                  'event_before_birth': 'True when the event timestamp '
                                                                        "precedes the resolved person's "
                                                                        'birth timestamp; silver records the '
                                                                        'doubt and does not filter the row.',
                                                  'event_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                  'extract_cd': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                  'fhx_activity_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                  'genetic_result_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'genetic_test_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'homecare_request_item_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                  'identity_status': 'Subject identity resolution state '
                                                                     'recorded by the silver canonical '
                                                                     'query.',
                                                  'implant_event_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'implant_sequence': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                                  'indication_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                  'is_illogical': 'Source RTT chronology flag indicating an '
                                                                  'illogical activity sequence; retained for '
                                                                  'the gold drop rule.',
                                                  'journey_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                  'journey_link_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'journey_participant_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                  'labour_delivery_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'load_batch_id': 'Deterministic batch token derived from '
                                                                   "the source row's bronze load timestamp.",
                                                  'loaded_at': 'Latest bronze load timestamp contributing to '
                                                               'the public row; never the lifecycle '
                                                               "pipeline's wall-clock time.",
                                                  'location_code': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                  'location_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                  'location_stay_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'mc_device_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'microbiology_isolate_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                  'order_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                  'organization_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'organization_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'pacs_examination_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                  'pacs_report_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                  'pathway_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                  'patient_event_key': 'Deterministic SHA-256 event key '
                                                                       'carried unchanged from the public '
                                                                       'parent; joins one lifecycle row to '
                                                                       'that event row.',
                                                  'patient_event_row_key': 'Deterministic row-level key '
                                                                           'carried unchanged from '
                                                                           'events_patient_event; joins one '
                                                                           'lifecycle row to one event-index '
                                                                           'row.',
                                                  'period_business_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                  'period_oid': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'person_address_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'person_attribute_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                  'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                  'person_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                  'person_id_resolved': 'True when person_id is null or '
                                                                        'resolves to the governed person '
                                                                        'spine; false records unresolved '
                                                                        'linkage without dropping the row.',
                                                  'person_identifier_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                  'person_relationship_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                  'pharmacy_issue_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'pm_wait_list_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'practitioner_group_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                  'practitioner_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                  'practitioner_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                        'key carried '
                                                                                        'unchanged from the '
                                                                                        'public parent; '
                                                                                        'joins one lifecycle '
                                                                                        'row to that '
                                                                                        'research row.',
                                                  'pregnancy_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                  'problem_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'procedure_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'pt_prot_reg_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                  'ptl_unique_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                  'record_status': 'active, superseded or retracted, '
                                                                   "normalised from the source's own status "
                                                                   'fields; superseded rows are earlier '
                                                                   'versions the source kept.',
                                                  'record_status_effective_from': 'Timestamp from which the '
                                                                                  'normalised record status '
                                                                                  'applies, when the source '
                                                                                  'supplies one.',
                                                  'record_status_effective_to': 'Timestamp until which the '
                                                                                'normalised record status '
                                                                                'applies; null means no '
                                                                                'source end is known.',
                                                  'referral_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'report_version_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'request_thread_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'research_study_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'row_source': 'Physical bronze row source, CURRENT or '
                                                                'HIST, carried unchanged from the public '
                                                                'parent; a key component because the '
                                                                'research table is at physical version grain '
                                                                'and patient_event_key is shared across an '
                                                                "entry's versions.",
                                                  'rtt_activity_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'sch_event_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                  'score_type': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                  'sequence_nbr': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                  'service_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                  'service_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                  'snapshot_date': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                  'source_daily_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                  'source_identifier_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                  'source_object': 'Source-arm discriminator carried '
                                                                   'unchanged from the public parent; use it '
                                                                   'with arm-specific native identifiers '
                                                                   'when joining.',
                                                  'source_site': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                  'source_system_oid': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'source_unit': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                  'source_update_timestamp': 'Native source-system update '
                                                                             'timestamp carried through the '
                                                                             'canonical silver query.',
                                                  'source_version_id': 'Native source version identifier '
                                                                       'carried unchanged from the public '
                                                                       'parent; CURRENT rows carry -1. A key '
                                                                       'component alongside row_source.',
                                                  'surg_case_proc_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                  'susceptibility_result_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                  'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                  'theatre_case_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                  'theatre_implant_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                  'transfusion_event_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                  'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                  'treatment_cycle_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                  'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                  'update_count': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                  'valid_from_datetime': 'Primary-key component carried '
                                                                         'unchanged from the public parent; '
                                                                         "combine it with the companion's "
                                                                         'other key columns when joining.',
                                                  'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                  'waiting_list_oid': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                  'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.'},
                                      'table': 'Internal quality and batch metadata for '
                                               'reference_practitioner; same row grain as the research '
                                               'table. Join keys: practitioner_key, practitioner_id.'},
 'reference_practitioner': {'columns': {'active': 'Source active indicator.',
                                        'doctor_number': 'Selected organization doctor number.',
                                        'external_provider_id': 'Selected external provider identifier.',
                                        'gdp_number': 'Selected dental practitioner number.',
                                        'loaded_at': 'Directory rows carry map_medical_personnel.ADC_UPDT '
                                                     'only; clinical-reference clocks are not added to known '
                                                     'personnel. For personnel absent from that directory, '
                                                     'placeholders use the maximum clock across their '
                                                     'contributing encounter, diagnosis, problem, implant, '
                                                     'form, event, medication, text, appointment and theatre '
                                                     'references. Implant references first coalesce '
                                                     'SOURCE_MAX_ADC_UPDT with ADC_UPDT; other references '
                                                     'use ADC_UPDT. This is arm-specific ingestion '
                                                     'provenance, not a uniform implant clock or Silver '
                                                     'refresh time.',
                                        'medical_service_key': 'Deterministic key of the selected medical '
                                                               'service.',
                                        'name': 'Source formatted practitioner name.',
                                        'npi': 'Selected NPI identifier.',
                                        'physician_ind': 'Source physician indicator.',
                                        'position_code': 'Source position code.',
                                        'position_display': 'Source position display.',
                                        'practitioner_id': 'Millennium personnel PERSON_ID; primary key of '
                                                           'this table.',
                                        'practitioner_key': 'Deterministic SHA-256 practitioner key.',
                                        'practitioner_type_code': 'Source personnel type code.',
                                        'practitioner_type_display': 'Source personnel type display.',
                                        'primary_location_key': 'Deterministic key of the primary assigned '
                                                                'or inferred location.',
                                        'record_status': 'Directory rows are active when '
                                                         'map_medical_personnel.ACTIVE_IND equals 1, '
                                                         'otherwise superseded, including when the flag is '
                                                         'null. Personnel referenced by clinical records but '
                                                         'absent from the directory are published as '
                                                         'placeholders with literal active status. Neither '
                                                         'arm emits retracted; placeholder active status is '
                                                         'not evidence of an active personnel-directory '
                                                         'record.',
                                        'source_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT.',
                                        'source_row_id': 'Stable source row identifier.',
                                        'source_table': 'Fully qualified bronze source table.',
                                        'valid_from': 'Source validity start.',
                                        'valid_to': 'Source validity end.'},
                            'table': 'One effective-dated Millennium practitioner/personnel row; inactive '
                                     'rows are retained.'}}
