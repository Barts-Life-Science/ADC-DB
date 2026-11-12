"""Silver index.journey_link: 2 dataset(s).

- datasets: spine_journey_link, _spine_journey_link_metadata
- moved verbatim from: 4_silver_care_pathways:718, 4_silver_care_pathways:738, 4_silver_care_pathways:756, 4_silver_care_pathways:763, 4_silver_care_pathways:792, 4_silver_care_pathways:822, 4_silver_care_pathways:925, 4_silver_care_pathways:933

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, spark, stable_id
from silver_tables._common.care_pathways import _pregnancy_journey_canonical
from silver_tables.index._shared import EVENT_FEED_NAMES
from silver_tables.spine._shared import _lifecycle_source_encounter
from silver_tables.spine.episode_encounter import SRC_EPISODE_ENCOUNTER, _lifecycle_source_episode_encounter


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Event-side columns of a feed row (everything that came from the old event index).
EVENT_GRAIN_COLUMNS = [
    "patient_event_key", "subject_key", "subject_id_system", "person_id", "encounter_id",
    "event_datetime", "event_end_datetime", "event_datetime_local", "event_end_datetime_local",
    "event_type", "fact_category", "fact_table", "fact_row_key", "source_system", "source_object",
    "source_row_key", "source_coding_system", "source_code", "source_display", "record_status",
    "confidentiality_code", "vip_ind", "withheld_identity_ind", "loaded_at",
]


def _event_feed_union():
    """All event feeds as one frame: the rows the retired events_patient_event held."""
    frames = [spark.read.table(_n(f"journey_events._feed_{feed}")) for feed in EVENT_FEED_NAMES]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


def _event_feed_events():
    """Event grain (one row per admitted event) recovered from the feeds.

    A feed repeats the event columns on every mapping row of the event, and they are identical
    across those rows, so a distinct over them gives back the event row.
    """
    return _event_feed_union().select(*EVENT_GRAIN_COLUMNS).distinct()


# Spine / link

# contract v2: retain the full canonical shape for internal reuse.
JOURNEY_LINK_SOURCE_COLUMNS = [
    "journey_link_key",
    "journey_key",
    "member_type",
    "encounter_id",
    "patient_event_key",
    "role_code",
    "member_start_datetime",
    "member_end_datetime",
    "record_status",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "loaded_at",
]


# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
JOURNEY_LINK_PUBLIC_COLUMNS = [
    'journey_link_key',
    'journey_key',
    'member_type',
    'encounter_id',
    'patient_event_key',
    'role_code',
    'member_start_datetime',
    'member_end_datetime',
    'construction_rule',
    'construction_version',
    'source_system',
    'source_table',
    'source_row_id',
    'record_status',
    'loaded_at',
]


JOURNEY_LINK_LIFECYCLE_COLUMNS = [
    'journey_link_key',
    'patient_event_key',
    'load_batch_id',
]


# contract v2: join encounters by native id while retaining encounter_key in deterministic journey-link hashing
def _journey_encounter_links():
    # Link a pregnancy journey to encounters for the same subject whose periods overlap the
    # journey.
    j = _pregnancy_journey_canonical().alias("j")
    e = _lifecycle_source_encounter().alias("e")
    member_end = F.coalesce(F.col("e.period_end"), F.col("e.period_start"))
    overlap = (
        (F.col("j.subject_key") == F.col("e.subject_key"))
        & F.col("j.period_start").isNotNull()
        & F.col("e.period_start").isNotNull()
        & (F.col("j.period_end").isNull() | (F.col("e.period_start") <= F.col("j.period_end")))
        & (member_end >= F.col("j.period_start"))
    )
    return j.join(e, overlap, "inner").select(
        stable_id("journey_link:temporal_overlap", F.col("j.journey_key"),
                  F.lit("encounter"), F.col("e.encounter_key")).alias("journey_link_key"),
        F.col("j.journey_key"), F.lit("encounter").alias("member_type"),
        F.col("e.encounter_id"), F.lit(None).cast("string").alias("patient_event_key"),
        F.lit("temporal_overlap").alias("role_code"),
        F.col("e.period_start").alias("member_start_datetime"),
        F.col("e.period_end").alias("member_end_datetime"),
        F.lit("active").alias("record_status"),
        F.lit("subject-time-overlap").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.col("j.source_system"), F.col("j.source_table"), F.col("j.source_row_id"),
        F.col("j.load_batch_id"), F.greatest(F.col("j.loaded_at"), F.col("e.loaded_at")).alias("loaded_at"),
    )


# contract v2: join event members through patient_event_key and publish native-typed nullable encounter ids
def _journey_event_links():
    # Link a pregnancy journey to patient events for the same subject whose periods overlap the
    # journey.
    j = _pregnancy_journey_canonical().alias("j")
    # EIR_20261021_V1: event grain from the per-fact-table event feeds (the event index is retired).
    e = _event_feed_events().alias("e")
    member_end = F.coalesce(F.col("e.event_end_datetime"), F.col("e.event_datetime"))
    overlap = (
        (F.col("j.subject_key") == F.col("e.subject_key"))
        & F.col("j.period_start").isNotNull()
        & F.col("e.event_datetime").isNotNull()
        & (F.col("j.period_end").isNull() | (F.col("e.event_datetime") <= F.col("j.period_end")))
        & (member_end >= F.col("j.period_start"))
    )
    return j.join(e, overlap, "inner").select(
        stable_id("journey_link:temporal_overlap", F.col("j.journey_key"),
                  F.lit("patient_event"), F.col("e.patient_event_key")).alias("journey_link_key"),
        F.col("j.journey_key"), F.lit("patient_event").alias("member_type"),
        F.lit(None).cast("bigint").alias("encounter_id"), F.col("e.patient_event_key"),
        F.lit("temporal_overlap").alias("role_code"),
        F.col("e.event_datetime").alias("member_start_datetime"),
        F.col("e.event_end_datetime").alias("member_end_datetime"),
        F.lit("active").alias("record_status"),
        F.lit("subject-time-overlap").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.col("j.source_system"), F.col("j.source_table"), F.col("j.source_row_id"),
        F.col("j.load_batch_id"), F.greatest(F.col("j.loaded_at"), F.col("e.loaded_at")).alias("loaded_at"),
    )


# contract v2: carry renamed journey keys through episode membership and hash native encounters through the canonical encounter key
def _journey_episode_links():
    """Journey members via shared-episode evidence: encounters sharing an active
    episode with a temporally-linked member encounter join the journey with role
    episode_membership. Relations whose episode is absent from the source
    (SOURCE_EPISODE_ABSENT) are retained on the fact but EXCLUDED here — a container bronze
    marked suspect is not linkage evidence. Both sides pre-collapse BEFORE the member join:
    anchors to one (journey_id, episode_id) row, members to one (episode_id, encounter_id)
    row — the intermediate is journeys x their episodes x members, never anchors x members.
    Anchor-relation load times fold into loaded_at so a newly landed anchor relation is
    visible to watermark consumers on the links it creates."""
    base = _journey_encounter_links().select(
        F.col("journey_key"),
        F.col("encounter_id").alias("anchor_encounter_id"),
        F.col("load_batch_id"),
        F.col("loaded_at").alias("j_loaded_at"),
    )
    ee = _lifecycle_source_episode_encounter().where(
        (F.col("record_status") == F.lit("active"))
        & (F.coalesce(F.col("relation_status_code"), F.lit("")) != F.lit("SOURCE_EPISODE_ABSENT"))
    )
    anchor_rel = ee.select(
        F.col("encounter_id").alias("anchor_encounter_id"),
        F.col("episode_id"),
        F.col("loaded_at").alias("anchor_loaded_at"),
    )
    journey_episode = (
        base.join(anchor_rel, "anchor_encounter_id")
            .groupBy("journey_key", "episode_id")
            .agg(
                F.max("load_batch_id").alias("load_batch_id"),
                F.max(F.greatest(F.col("j_loaded_at"), F.col("anchor_loaded_at")))
                 .alias("anchor_loaded_at"),
            )
    )
    member = (
        ee.select(
            F.col("episode_id"),
            F.col("encounter_id").alias("member_encounter_id"),
            F.col("source_row_id"),
            F.col("loaded_at"),
        )
        .groupBy("episode_id", "member_encounter_id")
        .agg(
            F.max("source_row_id").alias("ee_source_row_id"),
            F.max("loaded_at").alias("ee_loaded_at"),
        )
    )
    enc = _lifecycle_source_encounter().select(
        F.col("encounter_id").alias("member_encounter_id"),
        F.col("period_start"),
        F.col("period_end"),
        F.col("loaded_at").alias("enc_loaded_at"),
    )
    paths = (
        journey_episode.join(member, "episode_id")
                       .join(enc, "member_encounter_id")
    )
    grouped = paths.groupBy("journey_key", "member_encounter_id").agg(
        F.min("period_start").alias("member_start_datetime"),
        F.max("period_end").alias("member_end_datetime"),
        F.max("ee_source_row_id").alias("source_row_id"),
        F.max("load_batch_id").alias("load_batch_id"),
        F.greatest(F.max("anchor_loaded_at"), F.max("ee_loaded_at"), F.max("enc_loaded_at")).alias("loaded_at"),
    )
    return grouped.select(
        stable_id("journey_link:episode_membership", F.col("journey_key"),
                  F.lit("encounter"), stable_id("encounter:mill", F.col("member_encounter_id"))).alias("journey_link_key"),
        F.col("journey_key"),
        F.lit("encounter").alias("member_type"),
        F.col("member_encounter_id").alias("encounter_id"),
        F.lit(None).cast("string").alias("patient_event_key"),
        F.lit("episode_membership").alias("role_code"),
        F.col("member_start_datetime"),
        F.col("member_end_datetime"),
        F.lit("active").alias("record_status"),
        F.lit("shared-episode-membership").alias("construction_rule"),
        F.lit("episode-membership-v1").alias("construction_version"),
        F.lit("millennium").alias("source_system"),
        F.lit(SRC_EPISODE_ENCOUNTER).alias("source_table"),
        F.col("source_row_id"),
        F.col("load_batch_id"),
        F.col("loaded_at"),
    )


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_journey_link():
    # Assemble journey link rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    encounters = _journey_encounter_links().select(*JOURNEY_LINK_SOURCE_COLUMNS)
    events = _journey_event_links().select(*JOURNEY_LINK_SOURCE_COLUMNS)
    episodes = _journey_episode_links().select(*JOURNEY_LINK_SOURCE_COLUMNS)
    return encounters.unionByName(events).unionByName(episodes)


@materialized_view(
    name=_n("journey_spine.journey_link"),
    refresh_policy="auto",
)
def journey_link():
    # Build the declared dataset: N:M journey membership links derived from subject and source-
    # supported time overlap.
    return _lifecycle_source_journey_link().select(*JOURNEY_LINK_PUBLIC_COLUMNS)


@materialized_view(
    name=_n('journey_spine._journey_link_metadata'),
    refresh_policy="auto",
)
def journey_link_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_journey_link;
    # same row grain as the research table. Join keys: journey_link_key, patient_event_key.
    return (_lifecycle_source_journey_link()).select(*JOURNEY_LINK_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_journey_link_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                              'activity_record_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                              'admission_key': 'Deterministic v2 key carried unchanged from '
                                                               'the public parent; joins one lifecycle row '
                                                               'to that research row.',
                                              'allergy_instance_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                              'aria_dx_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'aria_pt_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'artifact_link_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                              'birth_row_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                              'bloodtrack_transaction_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                              'cancer_treatment_cycle_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                              'care_contact_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'care_participation_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                              'cds_id': 'Native source identifier carried unchanged from the '
                                                        'public parent; joins one lifecycle row to that '
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
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
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
                                              'elective_access_procedure_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
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
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
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
                                              'episode_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'episode_id_resolved': 'True when episode_id is null or '
                                                                     'resolves to the governed episode '
                                                                     'spine; false records unresolved '
                                                                     'linkage without dropping the row.',
                                              'episode_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                              'event_after_death_30d': 'True when the event timestamp is '
                                                                       'more than 30 days after the resolved '
                                                                       "person's death timestamp; silver "
                                                                       'records the doubt and does not '
                                                                       'filter the row.',
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
                                              'fhx_activity_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'gene_tested_row_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                              'genetic_result_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                              'genetic_test_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'homecare_request_item_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                              'identity_status': 'Subject identity resolution state recorded '
                                                                 'by the silver canonical query.',
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
                                                              'illogical activity sequence; retained for the '
                                                              'gold drop rule.',
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
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                              'load_batch_id': 'Deterministic batch token derived from the '
                                                               "source row's bronze load timestamp.",
                                              'loaded_at': 'Latest bronze load timestamp contributing to the '
                                                           "public row; never the lifecycle pipeline's "
                                                           'wall-clock time.',
                                              'location_code': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                              'location_key': 'Deterministic v2 key carried unchanged from '
                                                              'the public parent; joins one lifecycle row to '
                                                              'that research row.',
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
                                              'organization_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'organization_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                              'pacs_examination_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                              'pacs_report_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                              'pathway_oid': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                              'patient_event_key': 'Deterministic SHA-256 event key carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that event row.',
                                              'patient_event_row_key': 'Deterministic row-level key carried '
                                                                       'unchanged from events_patient_event; '
                                                                       'joins one lifecycle row to one '
                                                                       'event-index row.',
                                              'period_business_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                              'period_oid': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
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
                                              'person_id_resolved': 'True when person_id is null or resolves '
                                                                    'to the governed person spine; false '
                                                                    'records unresolved linkage without '
                                                                    'dropping the row.',
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
                                              'pm_wait_list_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'practitioner_group_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                              'practitioner_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                              'pregnancy_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                              'pregnancy_reconciliation_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                              'problem_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'procedure_id': 'Native source identifier carried unchanged '
                                                              'from the public parent; joins one lifecycle '
                                                              'row to that research row.',
                                              'pt_prot_reg_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
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
                                                            'carried unchanged from the public parent; a key '
                                                            'component because the research table is at '
                                                            'physical version grain and patient_event_key is '
                                                            "shared across an entry's versions.",
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
                                              'service_id': 'Native source identifier carried unchanged from '
                                                            'the public parent; joins one lifecycle row to '
                                                            'that research row.',
                                              'service_key': 'Deterministic v2 key carried unchanged from '
                                                             'the public parent; joins one lifecycle row to '
                                                             'that research row.',
                                              'snapshot_date': 'Primary-key component carried unchanged from '
                                                               'the public parent; combine it with the '
                                                               "companion's other key columns when joining.",
                                              'source_daily_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
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
                                              'source_version_id': 'Native source version identifier carried '
                                                                   'unchanged from the public parent; '
                                                                   'CURRENT rows carry -1. A key component '
                                                                   'alongside row_source.',
                                              'surg_case_proc_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                              'susceptibility_result_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                              'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                              'theatre_case_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                              'theatre_case_milestone_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                              'theatre_implant_key': 'Deterministic v2 key carried unchanged '
                                                                     'from the public parent; joins one '
                                                                     'lifecycle row to that research row.',
                                              'transfusion_event_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                              'transfusion_key': 'Deterministic v2 key carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                              'treatment_cycle_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
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
                                  'table': 'Internal quality and batch metadata for spine_journey_link; same '
                                           'row grain as the research table. Join keys: journey_link_key, '
                                           'patient_event_key.'},
 'spine_journey_link': {'columns': {'construction_rule': 'Governed overlap-construction rule; '
                                                         'episode_membership populated from the governed '
                                                         'episode feeds.',
                                    'construction_version': 'Governed overlap-construction version.',
                                    'encounter_id': 'Millennium ENCNTR_ID for encounter members.',
                                    'journey_key': 'Deterministic SHA-256 journey key.',
                                    'journey_link_key': 'Deterministic SHA-256 journey-membership key.',
                                    'loaded_at': 'Encounter overlap: greatest of constructed journey and '
                                                 'canonical encounter loaded_at. Patient-event overlap: '
                                                 'greatest of journey and internal event-index row '
                                                 'loaded_at, not a maximum over other events. Episode '
                                                 'membership: greatest of grouped anchor, member-relation '
                                                 'and member-encounter clocks across contributing episode '
                                                 'paths. The anchor clock includes the journey/anchor '
                                                 'encounter overlap clock and anchor episode-encounter '
                                                 'relation clock before collapse by journey and episode; '
                                                 'member relations are collapsed by episode and encounter. '
                                                 'Final grouping is by journey and member encounter. No '
                                                 'current Silver refresh time is substituted.',
                                    'member_end_datetime': 'Linked member end timestamp used by the '
                                                           'constructor.',
                                    'member_start_datetime': 'Linked member start timestamp used by the '
                                                             'constructor.',
                                    'member_type': 'Kind of linked member.',
                                    'patient_event_key': 'Deterministic SHA-256 patient-event key for event '
                                                         'members.',
                                    'record_status': 'Always active for all retained encounter-overlap, '
                                                     'patient-event-overlap and episode-membership links, '
                                                     "regardless of the linked journey or event's status. "
                                                     'Episode-membership eligibility separately requires '
                                                     'active episode-encounter relations and excludes '
                                                     'SOURCE_EPISODE_ABSENT; that filter does not create '
                                                     'superseded or retracted link labels.',
                                    'role_code': 'Controlled link role.',
                                    'source_row_id': 'Stable constructor source-row identifier.',
                                    'source_system': 'Constructor source-system identifier.',
                                    'source_table': 'Fully qualified configured journey-constructor source '
                                                    'table.'},
                        'table': 'N:M journey membership links derived from subject and source-supported '
                                 'time overlap.'}}
