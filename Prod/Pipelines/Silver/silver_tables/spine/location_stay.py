"""Silver spine.location_stay: 2 dataset(s).

- datasets: spine_location_stay, _spine_location_stay_metadata
- moved verbatim from: 1_silver_spine_and_reference:4390, 1_silver_spine_and_reference:4392, 1_silver_spine_and_reference:4562, 1_silver_spine_and_reference:4566, 1_silver_spine_and_reference:4570, 1_silver_spine_and_reference:4576, 1_silver_spine_and_reference:4581, 1_silver_spine_and_reference:4593

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system
from silver_tables.spine._shared import _location_projection


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Spine / location stay
SRC_LOCATION_HISTORY = "4_prod.bronze.map_patient_journey"


def _location_stay_canonical():
    # Assemble normalized location stay rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_LOCATION_HISTORY)
    staged = s.withColumn(
        "_stop_discriminator",
        F.when(s.LOCATION_STOP_SEQUENCE.isNotNull(),
               F.concat(F.lit("stop:"), s.LOCATION_STOP_SEQUENCE.cast("string")))
         .otherwise(F.concat(F.lit("event:"), s.ENCNTR_LOC_HIST_ID.cast("string"))),
    )
    grouped = (
        staged.groupBy("ENCNTR_ID", "_stop_discriminator")
        .agg(
            F.max("PERSON_ID").alias("PERSON_ID"),
            F.min("ENCNTR_LOC_HIST_ID").alias("source_row_id_min"),
            F.max("ENCNTR_LOC_HIST_ID").alias("source_row_id_max"),
            F.count(F.lit(1)).alias("source_history_row_count"),
            F.min("HISTORY_EVENT_SEQUENCE").alias("first_history_event_sequence"),
            F.max("HISTORY_EVENT_SEQUENCE").alias("last_history_event_sequence"),
            F.min("LOCATION_STOP_START_DT_TM").alias("stay_start"),
            F.max("LOCATION_STOP_END_DT_TM").alias("stay_end"),
            # VDB_SILVER_OCCUPANCY_V1: map_patient_journey occupancy bounds are stop-level, so min/max return the stop value.
            F.min("LOCATION_OCCUPANCY_START_DT_TM").alias("occupancy_start"),
            F.max("LOCATION_OCCUPANCY_END_DT_TM").alias("occupancy_end"),
            F.max("LOCATION_OCCUPANCY_START_BASIS").alias("occupancy_start_basis"),
            F.max("LOCATION_OCCUPANCY_END_BASIS").alias("occupancy_end_basis"),
            F.min("LOCATION_OCCUPANCY_START_DT_TM_LOCAL").alias("occupancy_start_local"),
            F.max("LOCATION_OCCUPANCY_END_DT_TM_LOCAL").alias("occupancy_end_local"),
            F.max("LOC_NURSE_UNIT_CD").alias("nurse_unit_cd"),
            F.max("NURSE_UNIT_DESC").alias("nurse_unit_display"),
            F.max("LOC_BUILDING_CD").alias("building_cd"),
            F.max("BUILDING_DESC").alias("building_display"),
            F.max("LOC_FACILITY_CD").alias("facility_cd"),
            F.max("FACILITY_DESC").alias("facility_display"),
            F.max("LOC_ROOM_CD").alias("room_cd"),
            F.max("ROOM_DESC").alias("room_display"),
            F.max("LOC_BED_CD").alias("bed_cd"),
            F.max("BED_DESC").alias("bed_display"),
            F.max("MED_SERVICE_CD").alias("service_code"),
            F.max("MED_SERVICE_DESC").alias("service_display"),
            F.max("TRANSFER_REASON_CD").alias("transfer_reason_code"),
            F.max("TRANSFER_REASON_DESC").alias("transfer_reason_display"),
            F.max("CONFID_LEVEL_CD").alias("confidentiality_code"),
            F.max("VIP_CD").alias("vip_code"),
            F.max("ADC_UPDT").alias("loaded_at"),
        )
    )
    source_row_id = F.concat_ws(
        ":",
        F.col("ENCNTR_ID").cast("string"),
        F.col("_stop_discriminator"),
        F.col("source_row_id_min").cast("string"),
        F.col("source_row_id_max").cast("string"),
    )
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", F.col("PERSON_ID"))],
        SRC_LOCATION_HISTORY,
        source_row_id,
    )
    # contract v2: publish native encounter/person/location values while retaining only the modelled stop hash as a key
    projected = grouped.select(
        stable_id("location_stay:mill", F.col("ENCNTR_ID"), F.col("_stop_discriminator"))
          .alias("location_stay_key"),
        F.col("ENCNTR_ID").cast("bigint").alias("encounter_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        F.col("PERSON_ID").cast("bigint").alias("person_id"),
        # SDI_LOCATION_ZERO_V1: code 0 is Millennium's "no location" (12.8M stops carry nurse unit 0); it must fall
        # through to the building or facility instead of hashing a key that can never resolve.
        F.when(F.col("nurse_unit_cd") > 0,
               stable_id("location:mill:nurse_unit", F.col("nurse_unit_cd")))
         .when(F.col("building_cd") > 0,
               stable_id("location:mill:building", F.col("building_cd")))
         .when(F.col("facility_cd") > 0,
               stable_id("location:mill:facility", F.col("facility_cd")))
         .alias("_candidate_location_key"),
        F.coalesce(*[F.when(F.col(c) > 0, F.col(c)) for c in ("nurse_unit_cd", "building_cd", "facility_cd")])
         .cast("string").alias("_candidate_location_code"),
        F.col("stay_start").alias("period_start"),
        F.col("stay_end").alias("period_end"),
        F.col("occupancy_start"),
        F.col("occupancy_end"),
        F.col("occupancy_start_basis"),
        F.col("occupancy_end_basis"),
        F.col("occupancy_start_local"),
        F.col("occupancy_end_local"),
        F.col("nurse_unit_cd").cast("string").alias("nurse_unit_code"),
        F.col("nurse_unit_display"),
        F.col("building_cd").cast("string").alias("building_code"),
        F.col("building_display"),
        F.col("facility_cd").cast("string").alias("facility_code"),
        F.col("facility_display"),
        F.col("room_cd").cast("string").alias("room_code"),
        F.col("room_display"),
        F.col("bed_cd").cast("string").alias("bed_code"),
        F.col("bed_display"),
        F.col("service_code").cast("string").alias("service_code"),
        F.col("service_display"),
        F.col("transfer_reason_code").cast("string").alias("transfer_reason_code"),
        F.col("transfer_reason_display"),
        F.col("source_history_row_count").cast("long").alias("source_history_row_count"),
        F.col("first_history_event_sequence").cast("int").alias("first_history_event_sequence"),
        F.col("last_history_event_sequence").cast("int").alias("last_history_event_sequence"),
        F.col("confidentiality_code").cast("string").alias("confidentiality_code"),
        (F.coalesce(F.col("vip_code"), F.lit(0)) != 0).alias("vip_ind"),
        # SDI_STAY_STATUS_V1: map_patient_journey has no lifecycle flag that retracts or supersedes a stop; an ended
        # stop is still the current record of that stop (its end is period_end), so it is not "superseded".
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit(SRC_LOCATION_HISTORY).alias("source_table"),
        source_row_id.alias("source_row_id"),
        F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("loaded_at"),
    )
    valid_locations = _location_projection().select(
        F.col("location_key").alias("_valid_location_key"),
        F.col("location_code").alias("_valid_location_code"),
    )
    joined = projected.join(
        valid_locations,
        projected._candidate_location_key == valid_locations._valid_location_key,
        "left",
    )
    return joined.select(*[
        F.col("_valid_location_code").alias("location_code")
        if c == "_candidate_location_code" else F.col(c)
        for c in projected.columns if c != "_candidate_location_key"
    ])


LOCATION_STAY_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


LOCATION_STAY_RETIRED_COLUMNS = [

]


LOCATION_STAY_LIFECYCLE_COLUMNS = [
    'location_stay_key',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_location_stay():
    # Assemble location stay rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _location_stay_canonical()


@materialized_view(
    name=_n("journey_spine.location_stay"),
    cluster_by=["person_id", "period_start"],
    refresh_policy="auto",
)
def location_stay():
    # Build the declared dataset: One physical location stop per encounter, anchored on the
    # source encounter and preserving bed/room detail.
    return _lifecycle_source_location_stay().drop(*LOCATION_STAY_LIFECYCLE_FIELDS, *LOCATION_STAY_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._location_stay_metadata'),
    refresh_policy="auto",
)
def location_stay_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_location_stay;
    # same row grain as the research table. Join keys: location_stay_key.
    return (_lifecycle_source_location_stay()).select(*LOCATION_STAY_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_location_stay_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'activity_record_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'admission_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
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
                                               'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                               'birth_row_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'care_contact_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'care_participation_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'cds_id': 'Native source identifier carried unchanged from '
                                                         'the public parent; joins one lifecycle row to that '
                                                         'research row.',
                                               'chemotherapy_course_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'community_care_activity_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                               'community_care_contact_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'concept_map_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                               'cost_line_item_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'dcp_forms_activity_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'device_mapping_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'diagnosis_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'dicom_path': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'doc_response_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                               'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'encounter_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'encounter_id_resolved': 'True when encounter_id is null or '
                                                                        'resolves to the governed encounter '
                                                                        'spine; false records unresolved '
                                                                        'linkage without dropping the row.',
                                               'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'encounter_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'endobase_exam_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'endobase_exam_term_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'entity_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                               'entry_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'episode_encounter_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'episode_encounter_reltn_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                               'episode_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'episode_id_resolved': 'True when episode_id is null or '
                                                                      'resolves to the governed episode '
                                                                      'spine; false records unresolved '
                                                                      'linkage without dropping the row.',
                                               'episode_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'event_after_death_30d': 'True when the event timestamp is '
                                                                        'more than 30 days after the '
                                                                        "resolved person's death timestamp; "
                                                                        'silver records the doubt and does '
                                                                        'not filter the row.',
                                               'event_before_birth': 'True when the event timestamp precedes '
                                                                     "the resolved person's birth timestamp; "
                                                                     'silver records the doubt and does not '
                                                                     'filter the row.',
                                               'event_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'extract_cd': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'fhx_activity_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'genetic_result_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'genetic_test_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'homecare_request_item_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'identity_status': 'Subject identity resolution state '
                                                                  'recorded by the silver canonical query.',
                                               'implant_event_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'implant_sequence': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                               'indication_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                               'is_illogical': 'Source RTT chronology flag indicating an '
                                                               'illogical activity sequence; retained for '
                                                               'the gold drop rule.',
                                               'journey_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'journey_participant_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'labour_delivery_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'load_batch_id': 'Deterministic batch token derived from the '
                                                                "source row's bronze load timestamp.",
                                               'loaded_at': 'Latest bronze load timestamp contributing to '
                                                            "the public row; never the lifecycle pipeline's "
                                                            'wall-clock time.',
                                               'location_code': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'location_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                               'location_stay_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                               'mc_device_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'microbiology_isolate_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'order_id': 'Native source identifier carried unchanged from '
                                                           'the public parent; joins one lifecycle row to '
                                                           'that research row.',
                                               'organization_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'organization_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'pacs_examination_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'pacs_report_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                               'pathway_oid': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                               'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that event row.',
                                               'patient_event_row_key': 'Deterministic row-level key carried '
                                                                        'unchanged from '
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
                                               'person_address_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'person_attribute_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'person_death_evidence_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                               'person_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                               'person_id_resolved': 'True when person_id is null or '
                                                                     'resolves to the governed person spine; '
                                                                     'false records unresolved linkage '
                                                                     'without dropping the row.',
                                               'person_identifier_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'person_relationship_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                               'pharmacy_issue_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'pm_wait_list_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'practitioner_group_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'practitioner_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'practitioner_identifier_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                               'practitioner_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'practitioner_location_evidence_key': 'Deterministic v2 key '
                                                                                     'carried unchanged from '
                                                                                     'the public parent; '
                                                                                     'joins one lifecycle '
                                                                                     'row to that research '
                                                                                     'row.',
                                               'pregnancy_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                               'problem_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'procedure_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                               'ptl_unique_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                               'record_status': 'active, superseded or retracted, normalised '
                                                                "from the source's own status fields; "
                                                                'superseded rows are earlier versions the '
                                                                'source kept.',
                                               'record_status_effective_from': 'Timestamp from which the '
                                                                               'normalised record status '
                                                                               'applies, when the source '
                                                                               'supplies one.',
                                               'record_status_effective_to': 'Timestamp until which the '
                                                                             'normalised record status '
                                                                             'applies; null means no source '
                                                                             'end is known.',
                                               'referral_oid': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'report_version_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'request_thread_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'research_study_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                               'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                             'carried unchanged from the public parent; a '
                                                             'key component because the research table is at '
                                                             'physical version grain and patient_event_key '
                                                             "is shared across an entry's versions.",
                                               'rtt_activity_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'sch_event_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                               'score_type': 'Primary-key component carried unchanged from '
                                                             'the public parent; combine it with the '
                                                             "companion's other key columns when joining.",
                                               'sequence_nbr': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                               'service_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                               'service_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
                                               'snapshot_date': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                               'source_daily_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                               'source_identifier_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                               'source_object': 'Source-arm discriminator carried unchanged '
                                                                'from the public parent; use it with '
                                                                'arm-specific native identifiers when '
                                                                'joining.',
                                               'source_site': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                               'source_system_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'source_unit': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                               'source_update_timestamp': 'Native source-system update '
                                                                          'timestamp carried through the '
                                                                          'canonical silver query.',
                                               'source_version_id': 'Native source version identifier '
                                                                    'carried unchanged from the public '
                                                                    'parent; CURRENT rows carry -1. A key '
                                                                    'component alongside row_source.',
                                               'surg_case_proc_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                               'susceptibility_result_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                               'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                               'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                               'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                               'theatre_implant_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                               'transfusion_event_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                               'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                               'treatment_cycle_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                               'treatment_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'update_count': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                               'valid_from_datetime': 'Primary-key component carried '
                                                                      'unchanged from the public parent; '
                                                                      "combine it with the companion's other "
                                                                      'key columns when joining.',
                                               'value_set_key': 'Deterministic v2 key carried unchanged from '
                                                                'the public parent; joins one lifecycle row '
                                                                'to that research row.',
                                               'waiting_list_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                               'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.'},
                                   'table': 'Internal quality and batch metadata for spine_location_stay; '
                                            'same row grain as the research table. Join keys: '
                                            'location_stay_key.'},
 'spine_location_stay': {'columns': {'bed_code': 'Historical bed code.',
                                     'bed_display': 'Historical bed display.',
                                     'building_code': 'Historical building code.',
                                     'building_display': 'Historical building display.',
                                     'confidentiality_code': 'Source confidentiality code.',
                                     'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                     'spine_encounter.encounter_id.',
                                     'facility_code': 'Historical facility code.',
                                     'facility_display': 'Historical facility display.',
                                     'first_history_event_sequence': 'First source history sequence in the '
                                                                     'stop.',
                                     'last_history_event_sequence': 'Last source history sequence in the '
                                                                    'stop.',
                                     'loaded_at': 'Maximum map_patient_journey.ADC_UPDT within the ENCNTR_ID '
                                                  'and stop-discriminator group. The discriminator uses '
                                                  'LOCATION_STOP_SEQUENCE when present, otherwise '
                                                  'ENCNTR_LOC_HIST_ID. This is contributing ingestion '
                                                  'provenance, not the Silver refresh time; null when all '
                                                  'contributing ADC_UPDT values are null.',
                                     'location_code': 'Native Millennium location code resolved against '
                                                      'reference_location.',
                                     'location_stay_key': 'Deterministic SHA-256 modelled location-stop key.',
                                     'nurse_unit_code': 'Historical nurse-unit code.',
                                     'nurse_unit_display': 'Historical nurse-unit display.',
                                     'occupancy_end': 'Latest `LOCATION_OCCUPANCY_END_DT_TM` across the '
                                                      "stop's source history rows: period_end without the "
                                                      "encounter's administrative or fixed-window close. "
                                                      'Null when the stop is open, its end was that close, '
                                                      'or the end precedes occupancy_start; see '
                                                      'occupancy_end_basis.',
                                     'occupancy_end_basis': '`LOCATION_OCCUPANCY_END_BASIS` for the stop: '
                                                            'NEXT_STOP_START, LOC_DEPART, '
                                                            'FINITE_EFFECTIVE_END, or (occupancy_end null) '
                                                            'REJECTED_ENCOUNTER_CLOSE / '
                                                            'END_BEFORE_OCCUPANCY_START / NO_STOP_END.',
                                     'occupancy_end_local': 'occupancy_end in Europe/London wall-clock time '
                                                            '(`LOCATION_OCCUPANCY_END_DT_TM_LOCAL`). Silver '
                                                            'timestamps are UTC instants; take local '
                                                            'calendar dates and clock times from this '
                                                            'column.',
                                     'occupancy_start': 'Earliest `LOCATION_OCCUPANCY_START_DT_TM` across '
                                                        "the stop's source history rows: when the patient "
                                                        'was physically at the location. Null when the stop '
                                                        'has no attendance evidence or ended before the '
                                                        'encounter arrival; see occupancy_start_basis. '
                                                        'Outpatient/day-case period_start is a booking '
                                                        'transaction, so use this for time at the location.',
                                     'occupancy_start_basis': '`LOCATION_OCCUPANCY_START_BASIS` for the '
                                                              'stop: LOC_ARRIVE, ENCOUNTER_ARRIVAL_BEST, '
                                                              'STOP_START, or (occupancy_start null) '
                                                              'NO_ATTENDANCE_EVIDENCE / PRE_ATTENDANCE.',
                                     'occupancy_start_local': 'occupancy_start in Europe/London wall-clock '
                                                              'time '
                                                              '(`LOCATION_OCCUPANCY_START_DT_TM_LOCAL`). '
                                                              'Silver timestamps are UTC instants; take '
                                                              'local calendar dates and clock times from '
                                                              'this column.',
                                     'period_end': 'Latest `LOCATION_STOP_END_DT_TM` across the source '
                                                   'history rows grouped into this encounter location stop; '
                                                   'the grouped source timestamp is retained without '
                                                   'sentinel removal.',
                                     'period_start': 'Earliest `LOCATION_STOP_START_DT_TM` across the source '
                                                     'history rows grouped into this encounter location '
                                                     'stop.',
                                     'person_id': 'Resolved Millennium PERSON_ID as BIGINT.',
                                     'record_status': 'Literal active: map_patient_journey carries no '
                                                      'lifecycle flag that retracts or supersedes a location '
                                                      'stop, so every published stop is the current record '
                                                      'of that stop. Whether the patient has left is '
                                                      'period_end / occupancy_end, not this status.',
                                     'record_status_effective_to': 'Always null: a location stop has no '
                                                                   "record-lifecycle end. The stop's own end "
                                                                   'is period_end.',
                                     'room_code': 'Historical room code.',
                                     'room_display': 'Historical room display.',
                                     'service_code': 'Service code during the stop.',
                                     'service_display': 'Service display during the stop.',
                                     'source_history_row_count': 'Number of history rows grouped into the '
                                                                 'stop.',
                                     'source_row_id': 'Deterministic grouped source-row identity.',
                                     'source_table': 'Fully qualified bronze source table.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'transfer_reason_code': 'Source transfer-reason code.',
                                     'transfer_reason_display': 'Source transfer-reason display.',
                                     'vip_ind': 'Source VIP indicator.'},
                         'table': 'One physical location stop per encounter, anchored on the source '
                                  'encounter and preserving bed/room detail.'}}
