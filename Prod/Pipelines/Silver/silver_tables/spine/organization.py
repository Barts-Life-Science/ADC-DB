"""Silver spine.organization: 2 dataset(s).

- datasets: reference_organization, _reference_organization_metadata
- moved verbatim from: 1_silver_spine_and_reference:1777, 1_silver_spine_and_reference:1781, 1_silver_spine_and_reference:1785, 1_silver_spine_and_reference:1792, 1_silver_spine_and_reference:1826, 1_silver_spine_and_reference:1834, 1_silver_spine_and_reference:1857, 1_silver_spine_and_reference:1869

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import SRC_APPOINTMENT, SRC_ENCOUNTER, SRC_MEDICATION_ORDER
from silver_tables.spine._shared import SRC_CARE_SITE


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


ORGANIZATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


ORGANIZATION_RETIRED_COLUMNS = [

]


ORGANIZATION_LIFECYCLE_COLUMNS = [
    'organization_key',
    'organization_id',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_organization():
    # Assemble organization rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_CARE_SITE)
    return (
        s.where(s.ORGANIZATION_ID.isNotNull())
         .groupBy("ORGANIZATION_ID")
         .agg(
             F.max("organization_name").alias("name"),
             F.max("organization_active_ind").alias("active_ind"),
             F.min("organization_beg_effective_dt_tm").alias("valid_from"),
             F.max("organization_end_effective_dt_tm").alias("valid_to"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         # contract v2: retain the organization SHA as organization_key and publish ORGANIZATION_ID as BIGINT
         .select(
             stable_id("organization:mill", F.col("ORGANIZATION_ID")).alias("organization_key"),
             F.col("ORGANIZATION_ID").cast("bigint").alias("organization_id"),
             F.col("ORGANIZATION_ID").cast("string").alias("source_organization_id"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("ORGANIZATION_ID").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )


# INSR_W2 W2.11 (L4164): organisation ids in use with no map_care_site row become member rows, so no FK in use is
# orphaned. Bronze sources and their id column; zero is "no organisation" and never becomes a member.
ORG_IN_USE_SOURCES = {
    SRC_ENCOUNTER: "ORGANIZATION_ID",
    SRC_APPOINTMENT: "ORGANIZATION_ID",
    SRC_MEDICATION_ORDER: "ORGANIZATION_ID",
    "4_prod.bronze.map_tracking_attendance": "ORGANIZATION_ID",
}


def _insr_with_in_use_organizations(published):
    """Care-site organisations plus one referenced_only row per in-use id they lack (GROUP BY + equi-join only)."""
    used = None
    for source, column in ORG_IN_USE_SOURCES.items():
        ids = read_source(source).select(F.expr(f"try_cast(`{column}` AS BIGINT)").alias("organization_id"))
        used = ids if used is None else used.unionByName(ids)
    used = used.where(F.col("organization_id").isNotNull() & (F.col("organization_id") != 0)) \
        .groupBy("organization_id").agg(F.count(F.lit(1)).alias("_refs"))
    known = published.select(F.col("organization_id").alias("_known"))
    missing = used.join(known, F.col("organization_id") == F.col("_known"), "left").where(F.col("_known").isNull())
    referenced = missing.select(
        stable_id("organization:mill", F.col("organization_id")).alias("organization_key"),
        F.col("organization_id"),
        F.col("organization_id").cast("string").alias("source_organization_id"),
        F.lit("referenced_only").alias("status"),
        F.lit(",".join(sorted(ORG_IN_USE_SOURCES))).alias("source_table"),
        F.col("organization_id").cast("string").alias("source_row_id"),
        F.lit("in_use_fk").alias("member_source"),
    )
    return (published.withColumn("member_source", F.lit("map_care_site"))
            .unionByName(referenced, allowMissingColumns=True))


@materialized_view(
    name=_n("journey_reference.organization"),
    refresh_policy="auto",
)
def organization():
    # Build the declared dataset: Thin v1 provider organization dimension derived from bronze
    # care sites.
    return _insr_with_in_use_organizations(  # INSR_W2 W2.11
        _lifecycle_source_organization().drop(*ORGANIZATION_LIFECYCLE_FIELDS, *ORGANIZATION_RETIRED_COLUMNS))


@materialized_view(
    name=_n('journey_reference._organization_metadata'),
    refresh_policy="auto",
)
def organization_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # reference_organization; same row grain as the research table. Join keys: organization_key,
    # organization_id.
    return (_lifecycle_source_organization()).select(*ORGANIZATION_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'reference_organization': ValuePolicy(table='reference_organization', enabled=True),
}


WRAP_POLICY = {'reference_organization'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_organization_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                               'reference_organization; same row grain as the research '
                                               'table. Join keys: organization_key, organization_id.'},
 'reference_organization': {'columns': {'address_city': 'Source organization address city.',
                                        'address_postcode_masked': 'Privacy-aware source postcode.',
                                        'loaded_at': 'Maximum map_care_site.ADC_UPDT over source rows '
                                                     'sharing the non-null ORGANIZATION_ID. The maximum is '
                                                     'not restricted to active source rows or the row '
                                                     'supplying the selected name/address; it records '
                                                     'contributing bronze ingestion, not organization '
                                                     'validity time or Silver refresh time.',
                                        'member_source': 'map_care_site for organisations with a care-site '
                                                         'row; in_use_fk for organisation ids that '
                                                         'encounters, appointments, medication orders or ED '
                                                         'tracking reference but map_care_site lacks (name '
                                                         'and address unknown, status referenced_only). '
                                                         'INSR_W2 W2.11 (L4164).',
                                        'name': 'Source organization name.',
                                        'organization_id': 'Millennium ORGANIZATION_ID; primary key for the '
                                                           'Millennium arm.',
                                        'organization_key': 'Deterministic SHA-256 organization key.',
                                        'source_organization_id': 'Millennium organization identifier.',
                                        'source_row_id': 'Stable source organization identifier.',
                                        'source_table': 'Fully qualified bronze source table.',
                                        'status': 'Source-derived organization status.',
                                        'valid_from': 'Source validity start.',
                                        'valid_to': 'Source validity end.'},
                            'table': 'Thin v1 provider organization dimension derived from bronze care '
                                     'sites.'}}
