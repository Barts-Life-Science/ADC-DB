"""Silver pathology.request_thread: 2 dataset(s).

- datasets: spine_request_thread, _spine_request_thread_metadata
- moved verbatim from: 2_silver_clinical_events:3609, 2_silver_clinical_events:3635, 2_silver_clinical_events:3659, 2_silver_clinical_events:3664, 2_silver_clinical_events:3780, 2_silver_clinical_events:3788, 2_silver_clinical_events:3799, 2_silver_clinical_events:3810

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system
from silver_tables._common.journey import (
    SRC_APPOINTMENT,
    SRC_MEDICATION_ORDER,
    SRC_MED_ADMIN,
    SRC_PATHOLOGY_REQUESTED_TEST,
    SRC_WAITING_LIST,
    _pathology_report_series_canonical,
    _pathology_requested_test_canonical,
    _waiting_list_index_representative,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Spine / request thread

# contract v2: retain the full canonical shape for internal reuse.
REQUEST_THREAD_SOURCE_COLUMNS = [
    "request_thread_key",
    "source_object",
    "source_patient_event_key",
    "target_patient_event_key",
    "link_type_code",
    "subject_key",
    "person_id",
    "encounter_id",
    "request_key",
    "response_key",
    "requested_datetime",
    "responded_datetime",
    "source_history_row_count",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
REQUEST_THREAD_PUBLIC_COLUMNS = [
    'request_thread_key',
    'source_object',
    'source_patient_event_key',
    'target_patient_event_key',
    'link_type_code',
    'subject_key',
    'person_id',
    'encounter_id',
    'request_key',
    'response_key',
    'requested_datetime',
    'responded_datetime',
    'source_history_row_count',
    'source_system',
    'source_table',
    'source_row_id',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


REQUEST_THREAD_LIFECYCLE_COLUMNS = [
    'request_thread_key',
    'load_batch_id',
]


def _pathology_request_thread_canonical():
    # contract v2: publish all request-thread arms with key names, source_object, and native person/encounter types
    # Assemble normalized pathology request thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    t = _pathology_requested_test_canonical().select(
        F.col("patient_event_key").alias("source_patient_event_key"),
        F.col("_source_row_id").alias("request_key"),
        "subject_key", "person_id", "specimen_key",
        F.col("event_datetime").alias("requested_datetime"),
        "load_batch_id", "loaded_at",
    )
    r = _pathology_report_series_canonical().select(
        F.col("patient_event_key").alias("target_patient_event_key"),
        F.col("_source_row_id").alias("response_key"),
        F.col("specimen_key").alias("_r_specimen_key"),
        F.col("event_datetime").alias("responded_datetime"),
        F.col("loaded_at").alias("_r_loaded_at"),
    )
    j = t.join(r, t["specimen_key"] == r["_r_specimen_key"], "inner")
    return j.select(
        stable_id(
            "request_thread:pathology", F.col("source_patient_event_key"),
            F.col("target_patient_event_key"), F.lit("order_to_report"),
        ).alias("request_thread_key"),
        "source_patient_event_key", "target_patient_event_key",
        F.lit("order_to_report").alias("link_type_code"),
        F.lit("pathology").alias("source_object"),
        "subject_key", "person_id", F.lit(None).cast("bigint").alias("encounter_id"),
        "request_key", "response_key", "requested_datetime",
        "responded_datetime", F.lit(1).cast("long").alias("source_history_row_count"),
        F.lit("active").alias("record_status"),
        F.lit(None).cast("timestamp").alias("record_status_effective_from"),
        F.lit(None).cast("timestamp").alias("record_status_effective_to"),
        F.lit("laboratory").alias("source_system"),
        F.lit(SRC_PATHOLOGY_REQUESTED_TEST).alias("source_table"),
        F.concat_ws("|", "request_key", "response_key").alias("source_row_id"),
        "load_batch_id", F.lit(None).cast("timestamp").alias("source_update_timestamp"),
        F.greatest("loaded_at", "_r_loaded_at").alias("loaded_at"),
    )


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_request_thread():
    # Assemble request thread rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    pathology = _pathology_request_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    medication = _medication_request_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    waiting_list = _waiting_list_appointment_thread_canonical().select(*REQUEST_THREAD_SOURCE_COLUMNS)
    return pathology.unionByName(medication).unionByName(waiting_list)


@materialized_view(
    name=_n("journey_spine.request_thread"),
    refresh_policy="auto",
)
def request_thread():
    # Build the declared dataset: Directed request-to-response edges: pathology order-to-report,
    # medication order-to-administration, waiting-list entry-to-appointment.
    return _lifecycle_source_request_thread().select(*REQUEST_THREAD_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_spine._request_thread_metadata'),
    refresh_policy="auto",
)
def request_thread_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_request_thread;
    # same row grain as the research table. Join keys: request_thread_key.
    return (_lifecycle_source_request_thread()).select(*REQUEST_THREAD_LIFECYCLE_COLUMNS)


def _medication_request_thread_canonical():
    # Assemble normalized medication request thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    a = read_source(SRC_MED_ADMIN).alias("a")
    o = read_source(SRC_MEDICATION_ORDER).alias("o")
    joined = a.where(F.col("a.ORDER_ID").isNotNull()).join(
        o, F.col("a.ORDER_ID") == F.col("o.ORDER_ID"), "inner"
    )
    source_event_id = stable_id("medication_order:mill", F.col("o.ORDER_ID"))
    target_event_id = stable_id("medication_admin:mill", F.col("a.EVENT_ID"))
    skey, _ = subject_key_with_system(
        [("urn:cerner:person_id", F.coalesce(F.col("a.PERSON_ID"), F.col("o.PERSON_ID")))],
        SRC_MED_ADMIN, F.col("a.EVENT_ID"),
    )
    retracted = F.coalesce(F.col("o.SOURCE_PRESENT_IND"), F.lit(True)) == F.lit(False)
    ended = F.col("a.CE_VALID_UNTIL_DT_TM").isNotNull() & (
        F.col("a.CE_VALID_UNTIL_DT_TM") < F.lit("2100-01-01").cast("timestamp")
    )
    loaded_at = F.greatest(F.col("a.ADC_UPDT"), F.col("o.ADC_UPDT"))
    source_update = F.greatest(F.col("a.ADC_UPDT"), F.col("o.SOURCE_ADC_UPDT"),
                               F.col("o.ADC_UPDT"))
    return joined.select(
        stable_id("request_thread:medication", source_event_id, target_event_id,
                  F.lit("order_to_administration")).alias("request_thread_key"),
        source_event_id.alias("source_patient_event_key"),
        target_event_id.alias("target_patient_event_key"),
        F.lit("order_to_administration").alias("link_type_code"),
        F.lit("medication").alias("source_object"), skey.alias("subject_key"),
        F.coalesce(F.col("a.PERSON_ID"), F.col("o.PERSON_ID")).cast("bigint").alias("person_id"),
        F.coalesce(F.col("a.ENCNTR_ID"), F.col("o.ENCNTR_ID")).cast("bigint").alias("encounter_id"),
        F.col("o.ORDER_ID").cast("string").alias("request_key"),
        F.col("a.EVENT_ID").cast("string").alias("response_key"),
        F.col("o.ORIG_ORDER_DT_TM").alias("requested_datetime"),
        F.coalesce(F.col("a.ADMIN_START_DT_TM"), F.col("a.PERFORMED_DT_TM"))
         .alias("responded_datetime"),
        F.lit(2).cast("long").alias("source_history_row_count"),
        F.when(retracted, F.lit("retracted")).when(ended, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        F.col("o.ORIG_ORDER_DT_TM").alias("record_status_effective_from"),
        F.when(retracted, F.col("o.SOURCE_ABSENT_DETECTED_TS"))
         .when(ended, F.col("a.CE_VALID_UNTIL_DT_TM")).alias("record_status_effective_to"),
        F.lit("millennium").alias("source_system"),
        F.concat_ws("|", F.lit(SRC_MEDICATION_ORDER), F.lit(SRC_MED_ADMIN)).alias("source_table"),
        F.col("a.EVENT_ID").cast("string").alias("source_row_id"),
        F.date_format(loaded_at, "yyyyMMddHHmmss").alias("load_batch_id"),
        source_update.alias("source_update_timestamp"),
        loaded_at.alias("loaded_at"),
    )


def _waiting_list_appointment_thread_canonical():
    # One edge per entry: representative (CURRENT wins) with a non-zero SCH_EVENT_ID,
    # existence-gated against the appointment source so no dangling target id is minted,
    # AND person-gated so an edge never links two different patients (the edge copies the
    # waiting-list subject; an id-only join would silently cross patients).
    # Full-volume 2026-08-12: 14,312,122/14,333,454 distinct ids resolve (99.85%);
    # of 14,324,720 matched pairs 14,271,836 are person-concordant (99.63%),
    # 52,841 appointment-person NULL and 43 discordant are excluded here.
    # Assemble normalized waiting list appointment thread rows for downstream dataset builders,
    # preserving the existing source and identity rules.
    rep = _waiting_list_index_representative().where(
        F.col("sch_event_id").isNotNull() & (F.col("sch_event_id") != 0)
    ).alias("rep")
    appt = (
        read_source(SRC_APPOINTMENT)
        .where(F.col("SCH_EVENT_ID").isNotNull() & F.col("PERSON_ID").isNotNull())
        .select(
            F.col("SCH_EVENT_ID").cast("long").alias("_appt_sch_event_id"),
            F.col("PERSON_ID").cast("bigint").alias("_appt_person_id"),
        )
        .distinct()
        .alias("appt")
    )
    joined = rep.join(
        appt,
        (F.col("rep.sch_event_id") == F.col("_appt_sch_event_id"))
        & (F.col("rep.person_id") == F.col("_appt_person_id")),
        "inner",
    )
    target_id = stable_id("appointment:mill_scheduling", F.col("_appt_sch_event_id"))
    return joined.select(
        stable_id(
            "request_thread:waiting_list", F.col("rep.patient_event_key"), target_id,
            F.lit("waiting_list_to_appointment"),
        ).alias("request_thread_key"),
        F.col("rep.patient_event_key").alias("source_patient_event_key"),
        target_id.alias("target_patient_event_key"),
        F.lit("waiting_list_to_appointment").alias("link_type_code"),
        F.lit("waiting_list").alias("source_object"),
        F.col("rep.subject_key").alias("subject_key"),
        F.col("rep.person_id").alias("person_id"),
        F.col("rep.encounter_id").alias("encounter_id"),
        F.col("rep.pm_wait_list_id").cast("string").alias("request_key"),
        F.col("rep.sch_event_id").cast("string").alias("response_key"),
        F.col("rep.event_datetime").alias("requested_datetime"),
        F.col("rep.scheduled_datetime").alias("responded_datetime"),
        F.lit(1).cast("long").alias("source_history_row_count"),
        F.col("rep.record_status").alias("record_status"),
        F.col("rep.record_status_effective_from").alias("record_status_effective_from"),
        F.col("rep.record_status_effective_to").alias("record_status_effective_to"),
        F.lit("millennium-pm").alias("source_system"),
        F.lit(SRC_WAITING_LIST).alias("source_table"),
        F.concat_ws(
            ":", F.col("rep.pm_wait_list_id").cast("string"), F.col("rep.row_source"),
            F.col("rep.source_version_id").cast("string"),
        ).alias("source_row_id"),
        F.col("rep.load_batch_id").alias("load_batch_id"),
        F.col("rep.source_update_timestamp").alias("source_update_timestamp"),
        F.col("rep.loaded_at").alias("loaded_at"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_request_thread_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                                          'the public parent; joins one lifecycle row to '
                                                          'that research row.',
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
                                                'cost_line_item_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                'dcp_forms_activity_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'device_mapping_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'endobase_exam_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'endobase_exam_term_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'entity_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
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
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                'episode_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'episode_id_resolved': 'True when episode_id is null or '
                                                                       'resolves to the governed episode '
                                                                       'spine; false records unresolved '
                                                                       'linkage without dropping the row.',
                                                'episode_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'event_after_death_30d': 'True when the event timestamp is '
                                                                         'more than 30 days after the '
                                                                         "resolved person's death timestamp; "
                                                                         'silver records the doubt and does '
                                                                         'not filter the row.',
                                                'event_before_birth': 'True when the event timestamp '
                                                                      "precedes the resolved person's birth "
                                                                      'timestamp; silver records the doubt '
                                                                      'and does not filter the row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'genetic_test_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                'homecare_request_item_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'is_illogical': 'Source RTT chronology flag indicating an '
                                                                'illogical activity sequence; retained for '
                                                                'the gold drop rule.',
                                                'journey_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'journey_link_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                'journey_participant_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
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
                                                'pacs_report_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'pathway_oid': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
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
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                'person_relationship_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                'pharmacy_issue_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                                      'carried unchanged '
                                                                                      'from the public '
                                                                                      'parent; joins one '
                                                                                      'lifecycle row to that '
                                                                                      'research row.',
                                                'pregnancy_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                'problem_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'procedure_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'pt_prot_reg_id': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                'ptl_unique_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                              'applies; null means no source '
                                                                              'end is known.',
                                                'referral_oid': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
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
                                                'row_source': 'Physical bronze row source, CURRENT or HIST, '
                                                              'carried unchanged from the public parent; a '
                                                              'key component because the research table is '
                                                              'at physical version grain and '
                                                              "patient_event_key is shared across an entry's "
                                                              'versions.',
                                                'rtt_activity_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'sch_event_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                'score_type': 'Primary-key component carried unchanged from '
                                                              'the public parent; combine it with the '
                                                              "companion's other key columns when joining.",
                                                'sequence_nbr': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'service_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                                'service_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                                'snapshot_date': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
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
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                'susceptibility_result_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
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
                                                'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'update_count': 'Primary-key component carried unchanged '
                                                                'from the public parent; combine it with the '
                                                                "companion's other key columns when joining.",
                                                'valid_from_datetime': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                'waiting_list_oid': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                'waiting_list_snapshot_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.'},
                                    'table': 'Internal quality and batch metadata for spine_request_thread; '
                                             'same row grain as the research table. Join keys: '
                                             'request_thread_key.'},
 'spine_request_thread': {'columns': {'encounter_id': 'Millennium ENCNTR_ID as BIGINT when available.',
                                      'link_type_code': 'Controlled directed relationship type.',
                                      'loaded_at': 'Arm-specific contributing load clock: pathology takes '
                                                   'the greatest requested-test ADC_UPDT and the linked '
                                                   'report-series maximum ADC_UPDT across all its versions; '
                                                   'medication takes the greatest administration ADC_UPDT '
                                                   'and order ADC_UPDT; waiting-list takes ADC_UPDT of the '
                                                   'selected whole representative version. The appointment '
                                                   'join adds no clock, and the union does not aggregate '
                                                   'across arms. Null when that edge has no contributing '
                                                   'load timestamp; not Silver refresh time.',
                                      'person_id': 'Resolved Millennium PERSON_ID as BIGINT when available.',
                                      'record_status': 'Arm-specific edge status: pathology is literal '
                                                       'active. Medication is retracted when the order '
                                                       'SOURCE_PRESENT_IND is false (null defaults to '
                                                       'present), otherwise superseded when administration '
                                                       'CE_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise '
                                                       'active. Waiting-list edges copy the selected '
                                                       "representative version's status: superseded for "
                                                       'source absence or non-current status (null presence '
                                                       'defaults true; null IS_CURRENT defaults false), '
                                                       'otherwise active. This is not a single source '
                                                       'workflow-status code.',
                                      'record_status_effective_from': 'Arm-specific history start: null for '
                                                                      'pathology edges; '
                                                                      'map_medication_order.ORIG_ORDER_DT_TM '
                                                                      'for medication edges; selected '
                                                                      'map_waiting_list representative '
                                                                      'BEG_EFFECTIVE_DT_TM for waiting-list '
                                                                      'edges. These are request/version-time '
                                                                      'proxies, not a uniformly observed '
                                                                      'status transition. Missing timestamps '
                                                                      'remain null.',
                                      'record_status_effective_to': 'Pathology edges always have a null end. '
                                                                    'Medication uses order '
                                                                    'SOURCE_ABSENT_DETECTED_TS when '
                                                                    'retracted, without fallback even if '
                                                                    'null; otherwise finite administration '
                                                                    'CE_VALID_UNTIL_DT_TM when superseded, '
                                                                    'else null. Waiting-list edges copy '
                                                                    "their representative's end: for "
                                                                    'superseded versions, '
                                                                    'coalesce(SOURCE_ABSENT_DETECTED_TS, '
                                                                    'END_EFFECTIVE_DT_TM, ADC_UPDT), with no '
                                                                    'far-future sentinel filter; active '
                                                                    'versions have a null end.',
                                      'request_identifier': 'Verbatim source order identifier.',
                                      'request_key': 'Request-side source identifier retained under the '
                                                     'governed key name.',
                                      'request_thread_key': 'Deterministic request-edge primary key.',
                                      'requested_datetime': 'Source request timestamp.',
                                      'responded_datetime': 'Source report or measurement timestamp.',
                                      'response_identifier': 'Verbatim source report-parent identifier.',
                                      'response_key': 'Response-side source identifier retained under the '
                                                      'governed key name.',
                                      'source_history_row_count': 'Number of source rows supporting the '
                                                                  'edge.',
                                      'source_object': 'Request-thread arm: pathology, medication, or '
                                                       'waiting_list.',
                                      'source_patient_event_key': 'Request-side patient event identifier.',
                                      'source_row_id': 'Representative stable source row identifier.',
                                      'source_system': 'Source system identifier.',
                                      'source_table': 'Fully qualified configured source table.',
                                      'source_update_timestamp': 'Arm-specific provenance: null for '
                                                                 'pathology edges; greatest of '
                                                                 'map_med_admin.ADC_UPDT, '
                                                                 'map_medication_order.SOURCE_ADC_UPDT and '
                                                                 'map_medication_order.ADC_UPDT for '
                                                                 'medication edges; selected '
                                                                 'map_waiting_list representative '
                                                                 'SOURCE_ADC_UPDT for waiting-list edges. '
                                                                 'This is not uniformly a native source '
                                                                 'modification clock. The appointment '
                                                                 'existence/person join contributes no '
                                                                 'timestamp; no Silver refresh timestamp is '
                                                                 'substituted.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'target_patient_event_key': 'Response-side patient event identifier.'},
                          'table': 'Directed request-to-response edges: pathology order-to-report, '
                                   'medication order-to-administration, waiting-list entry-to-appointment.'}}
