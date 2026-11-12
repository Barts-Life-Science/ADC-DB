"""Silver spine.care_participation: 2 dataset(s).

- datasets: spine_care_participation, _spine_care_participation_metadata
- moved verbatim from: 1_silver_spine_and_reference:4611, 1_silver_spine_and_reference:4639, 1_silver_spine_and_reference:4645, 1_silver_spine_and_reference:4652, 1_silver_spine_and_reference:4659, 1_silver_spine_and_reference:4690, 1_silver_spine_and_reference:4733, 1_silver_spine_and_reference:4737

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system
from silver_tables._common.journey import SRC_ENCOUNTER
from silver_tables.spine._shared import _encounter_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_ENCOUNTER_PRACTITIONER = "4_prod.bronze.map_encounter_practitioner"  # OGR_PARTICIPATION_V1


SRC_PERSON_GP = "4_prod.bronze.map_person_gp"


# Spine / care participation
def _care_participation_projection(df, practitioner_col, role, start_col):
    # Create encounter-level practitioner participation records for rows with a practitioner in
    # the specified role.
    return (
        # contract v2: rename modelled hashes as keys and publish the native practitioner id
        df.where(F.col(practitioner_col).isNotNull())
          .select(
              stable_id(
                  "care_participation:mill",
                  F.col("_source_encounter_id"), F.col(practitioner_col), F.lit(role),
              ).alias("care_participation_key"),
              "subject_key", "subject_id_system", "person_id",
              F.col("encounter_id"),
              F.lit(None).cast("string").alias("journey_key"),
              F.lit(None).cast("string").alias("service_id"),
              F.col(practitioner_col).cast("bigint").alias("practitioner_id"),
              F.lit(role).alias("role"),
              F.col(start_col).alias("valid_from"),
              F.lit(None).cast("timestamp").alias("valid_to"),
              F.lit("source_encounter_personnel_field").alias("construction_rule"),
              F.lit("0.4.0").alias("construction_version"),
              F.lit(SRC_ENCOUNTER).alias("source_table"),
              F.col("_source_encounter_id").cast("string").alias("source_row_id"),
              "load_batch_id", "loaded_at",
          )
    )


# OGR_PARTICIPATION_V1: ENCNTR_PRSNL_RELTN care roles and registered GP (PERSON_PRSNL_RELTN 1115).
ENCOUNTER_ROLE_BY_CODE = {
    1119: "attending", 1116: "admitting", 1121: "consulting", 1126: "referring",
    673962: "locum_attending", 673961: "locum_admitting", 435702461: "mental_health_attending",
}


def _role_from_code(code_col):
    expr = F.lit(None).cast("string")
    for code, role in sorted(ENCOUNTER_ROLE_BY_CODE.items(), reverse=True):
        expr = F.when(code_col == code, F.lit(role)).otherwise(expr)
    return expr


def _relationship_rows(df, id_col):
    present = F.coalesce(F.col("SOURCE_PRESENT_IND").cast("boolean"), F.lit(True))
    return df.where((F.col("ACTIVE_IND") == 1) & present
                    & F.col("PRSNL_PERSON_ID").isNotNull() & (F.col("PRSNL_PERSON_ID") != 0)
                    & F.col(id_col).isNotNull())


def _encounter_practitioner_participation():
    # Encounter-scoped relationship roles; subject/person follow the canonical encounter row.
    r = _relationship_rows(read_source(SRC_ENCOUNTER_PRACTITIONER), "ENCNTR_PRSNL_RELTN_ID").drop("PERSON_ID")
    e = _encounter_canonical().select(
        F.col("encounter_id").cast("bigint").alias("_cp_encounter_id"),
        "subject_key", "subject_id_system", "person_id",
    )
    role = _role_from_code(F.col("ENCNTR_PRSNL_R_CD").cast("bigint"))
    return (
        r.join(e, F.col("ENCNTR_ID").cast("bigint") == F.col("_cp_encounter_id"), "inner")
        .where(role.isNotNull())
        .select(
            stable_id("care_participation:mill_eprl", F.col("ENCNTR_PRSNL_RELTN_ID")).alias("care_participation_key"),
            "subject_key", "subject_id_system", "person_id",
            F.col("_cp_encounter_id").alias("encounter_id"),
            F.lit(None).cast("string").alias("journey_key"),
            F.lit(None).cast("string").alias("service_id"),
            F.col("PRSNL_PERSON_ID").cast("bigint").alias("practitioner_id"),
            role.alias("role"),
            F.col("BEG_EFFECTIVE_DT_TM").cast("timestamp").alias("valid_from"),
            F.col("END_EFFECTIVE_DT_TM").cast("timestamp").alias("valid_to"),
            F.lit("encounter_prsnl_reltn").alias("construction_rule"),
            F.lit("0.5.0").alias("construction_version"),
            F.lit(SRC_ENCOUNTER_PRACTITIONER).alias("source_table"),
            F.col("ENCNTR_PRSNL_RELTN_ID").cast("string").alias("source_row_id"),
            F.date_format(F.col("ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
            F.col("ADC_UPDT").alias("loaded_at"),
        )
    )


def _registered_gp_participation():
    # Person-scoped registered GP; encounter_id is NULL by design.
    g = _relationship_rows(read_source(SRC_PERSON_GP), "PERSON_PRSNL_RELTN_ID")
    source_row_id = F.col("PERSON_PRSNL_RELTN_ID").cast("string")
    skey, ssys = subject_key_with_system([("urn:cerner:person_id", F.col("PERSON_ID"))], SRC_PERSON_GP, source_row_id)
    return g.select(
        stable_id("care_participation:mill_gp", F.col("PERSON_PRSNL_RELTN_ID")).alias("care_participation_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        F.col("PERSON_ID").cast("bigint").alias("person_id"),
        F.lit(None).cast("bigint").alias("encounter_id"),
        F.lit(None).cast("string").alias("journey_key"),
        F.lit(None).cast("string").alias("service_id"),
        F.col("PRSNL_PERSON_ID").cast("bigint").alias("practitioner_id"),
        F.lit("registered_gp").alias("role"),
        F.col("BEG_EFFECTIVE_DT_TM").cast("timestamp").alias("valid_from"),
        F.col("END_EFFECTIVE_DT_TM").cast("timestamp").alias("valid_to"),
        F.lit("person_prsnl_reltn").alias("construction_rule"),
        F.lit("0.5.0").alias("construction_version"),
        F.lit(SRC_PERSON_GP).alias("source_table"),
        source_row_id.alias("source_row_id"),
        F.date_format(F.col("ADC_UPDT"), "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )


CARE_PARTICIPATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


CARE_PARTICIPATION_RETIRED_COLUMNS = [

]


CARE_PARTICIPATION_LIFECYCLE_COLUMNS = [
    'care_participation_key',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_care_participation():
    # Assemble care participation rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    e = _encounter_canonical()
    return (
        _care_participation_projection(e, "_registration_practitioner_id", "registrar", "_registration_role_start")
        .unionByName(
            _care_participation_projection(e, "_creator_practitioner_id", "recorder", "_creator_role_start")
        )
        .unionByName(
            _care_participation_projection(e, "_discharge_practitioner_id", "discharger", "_discharge_role_start")
        )
        .unionByName(_encounter_practitioner_participation())  # OGR_PARTICIPATION_V1
        .unionByName(_registered_gp_participation())
    )


@materialized_view(
    name=_n("journey_spine.care_participation"),  # OGR_PARTICIPATION_V1
    refresh_policy="auto",
)
def care_participation():
    # Build the declared dataset: Encounter-scoped practitioner participation from registration,
    # creation, and discharge source fields.
    return _lifecycle_source_care_participation().drop(*CARE_PARTICIPATION_LIFECYCLE_FIELDS, *CARE_PARTICIPATION_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._care_participation_metadata'),
    refresh_policy="auto",
)
def care_participation_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_care_participation; same row grain as the research table. Join keys:
    # care_participation_key.
    return (_lifecycle_source_care_participation()).select(*CARE_PARTICIPATION_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'spine_care_participation': ValuePolicy(table='spine_care_participation', enabled=True, zero_as_null=('practitioner_id',)),
}


WRAP_POLICY = {'spine_care_participation'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_care_participation_metadata': {'columns': {'activity_date': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                    'activity_record_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'admission_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'allergy_instance_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'aria_dx_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'aria_pt_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'artifact_link_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                    'birth_row_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'care_contact_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'care_participation_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'cds_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                    'chemotherapy_course_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                    'community_care_activity_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                    'community_care_contact_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'concept_map_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'cost_line_item_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'dcp_forms_activity_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                    'device_mapping_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'diagnosis_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'dicom_path': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'doc_response_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                                    'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                    'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                    'encounter_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'encounter_id_resolved': 'True when encounter_id is null '
                                                                             'or resolves to the governed '
                                                                             'encounter spine; false records '
                                                                             'unresolved linkage without '
                                                                             'dropping the row.',
                                                    'encounter_identifier_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                    'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'endobase_exam_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'endobase_exam_term_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                    'entity_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                    'entry_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                    'episode_encounter_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                    'episode_encounter_reltn_id': 'Native source identifier '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'episode_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'episode_id_resolved': 'True when episode_id is null or '
                                                                           'resolves to the governed episode '
                                                                           'spine; false records unresolved '
                                                                           'linkage without dropping the '
                                                                           'row.',
                                                    'episode_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                    'event_after_death_30d': 'True when the event timestamp '
                                                                             'is more than 30 days after the '
                                                                             "resolved person's death "
                                                                             'timestamp; silver records the '
                                                                             'doubt and does not filter the '
                                                                             'row.',
                                                    'event_before_birth': 'True when the event timestamp '
                                                                          "precedes the resolved person's "
                                                                          'birth timestamp; silver records '
                                                                          'the doubt and does not filter the '
                                                                          'row.',
                                                    'event_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                    'extract_cd': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'fhx_activity_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                        "combine it with the companion's "
                                                                        'other key columns when joining.',
                                                    'indication_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                    'is_illogical': 'Source RTT chronology flag indicating '
                                                                    'an illogical activity sequence; '
                                                                    'retained for the gold drop rule.',
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
                                                                     "the source row's bronze load "
                                                                     'timestamp.',
                                                    'loaded_at': 'Latest bronze load timestamp contributing '
                                                                 'to the public row; never the lifecycle '
                                                                 "pipeline's wall-clock time.",
                                                    'location_code': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
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
                                                                               'lifecycle row to that '
                                                                               'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'pacs_report_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                    'pathway_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                    'patient_event_key': 'Deterministic SHA-256 event key '
                                                                         'carried unchanged from the public '
                                                                         'parent; joins one lifecycle row to '
                                                                         'that event row.',
                                                    'patient_event_row_key': 'Deterministic row-level key '
                                                                             'carried unchanged from '
                                                                             'events_patient_event; joins '
                                                                             'one lifecycle row to one '
                                                                             'event-index row.',
                                                    'period_business_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'period_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'person_address_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                    'person_attribute_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                    'person_death_evidence_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                    'person_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                    'person_id_resolved': 'True when person_id is null or '
                                                                          'resolves to the governed person '
                                                                          'spine; false records unresolved '
                                                                          'linkage without dropping the row.',
                                                    'person_identifier_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
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
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'practitioner_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                    'practitioner_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                          'key carried '
                                                                                          'unchanged from '
                                                                                          'the public '
                                                                                          'parent; joins one '
                                                                                          'lifecycle row to '
                                                                                          'that research '
                                                                                          'row.',
                                                    'pregnancy_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                    'problem_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'procedure_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'pt_prot_reg_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                    'ptl_unique_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                    'record_status': 'active, superseded or retracted, '
                                                                     "normalised from the source's own "
                                                                     'status fields; superseded rows are '
                                                                     'earlier versions the source kept.',
                                                    'record_status_effective_from': 'Timestamp from which '
                                                                                    'the normalised record '
                                                                                    'status applies, when '
                                                                                    'the source supplies '
                                                                                    'one.',
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
                                                                  'research table is at physical version '
                                                                  'grain and patient_event_key is shared '
                                                                  "across an entry's versions.",
                                                    'rtt_activity_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'sch_event_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                    'score_type': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                    'sequence_nbr': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                    'service_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                    'service_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                    'snapshot_date': 'Primary-key component carried '
                                                                     'unchanged from the public parent; '
                                                                     "combine it with the companion's other "
                                                                     'key columns when joining.',
                                                    'source_daily_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                    'source_identifier_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                    'source_object': 'Source-arm discriminator carried '
                                                                     'unchanged from the public parent; use '
                                                                     'it with arm-specific native '
                                                                     'identifiers when joining.',
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
                                                                               'timestamp carried through '
                                                                               'the canonical silver query.',
                                                    'source_version_id': 'Native source version identifier '
                                                                         'carried unchanged from the public '
                                                                         'parent; CURRENT rows carry -1. A '
                                                                         'key component alongside '
                                                                         'row_source.',
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
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                    'theatre_case_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                    'theatre_implant_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                    'transfusion_event_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                    'transfusion_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; combine it with the '
                                                                           "companion's other key columns "
                                                                           'when joining.',
                                                    'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                                    'waiting_list_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                    'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.'},
                                        'table': 'Internal quality and batch metadata for '
                                                 'spine_care_participation; same row grain as the research '
                                                 'table. Join keys: care_participation_key.'},
 'spine_care_participation': {'columns': {'care_participation_key': 'Deterministic SHA-256 participation '
                                                                    'key.',
                                          'construction_rule': 'Governed derivation rule.',
                                          'construction_version': 'Governed derivation-rule version.',
                                          'encounter_id': 'Millennium ENCNTR_ID as BIGINT; NULL for '
                                                          'person-scoped registered_gp rows.',
                                          'journey_key': 'Deterministic journey key when available.',
                                          'loaded_at': 'Bronze ADC_UPDT of the contributing row: '
                                                       'map_encounter for registrar, recorder and '
                                                       'discharger; map_encounter_practitioner or '
                                                       'map_person_gp for relationship rows. Ingestion '
                                                       'provenance, not a role-validity timestamp or Silver '
                                                       'refresh time; a missing source timestamp remains '
                                                       'null.',
                                          'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                          'practitioner_id': 'Millennium personnel PERSON_ID as BIGINT.',
                                          'role': 'Participation role: registrar, recorder, discharger '
                                                  '(encounter source fields); attending, admitting, '
                                                  'consulting, referring, locum_attending, locum_admitting, '
                                                  'mental_health_attending (ENCNTR_PRSNL_RELTN, code set '
                                                  '333); registered_gp (PERSON_PRSNL_RELTN code 1115, '
                                                  'person-scoped).',
                                          'service_id': 'Service-registration context when available.',
                                          'source_row_id': 'Source row identity: encounter id for '
                                                           'source-field rows; ENCNTR_PRSNL_RELTN_ID or '
                                                           'PERSON_PRSNL_RELTN_ID for relationship rows.',
                                          'source_table': 'Fully qualified bronze source table.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'valid_from': 'Participation validity start: role start for '
                                                        'source-field rows; BEG_EFFECTIVE_DT_TM (Millennium '
                                                        'UTC) for relationship rows.',
                                          'valid_to': 'Participation validity end: NULL for source-field '
                                                      'rows; END_EFFECTIVE_DT_TM (Millennium UTC; 2100-12-31 '
                                                      'means open) for relationship rows.'},
                              'table': 'Practitioner participation: encounter source fields, '
                                       'ENCNTR_PRSNL_RELTN care roles and the registered GP '
                                       '(person-scoped).'}}
