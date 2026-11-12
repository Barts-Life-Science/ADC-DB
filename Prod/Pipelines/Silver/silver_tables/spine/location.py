"""Silver spine.location: 2 dataset(s).

- datasets: reference_location, _reference_location_metadata
- moved verbatim from: 1_silver_spine_and_reference:1256, 1_silver_spine_and_reference:1260, 1_silver_spine_and_reference:1264, 1_silver_spine_and_reference:1271, 1_silver_spine_and_reference:1278, 1_silver_spine_and_reference:1281, 1_silver_spine_and_reference:1293, 1_silver_spine_and_reference:1299

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source
from silver_tables.spine._shared import _location_projection


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


LOCATION_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


LOCATION_RETIRED_COLUMNS = [

]


LOCATION_LIFECYCLE_COLUMNS = [
    'location_key',
    'location_code',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_location():
    # contract v2: document location_key, level-qualified location_code, parent key, and native organization id
    # Assemble location rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _location_projection()


SRC_ONSPD = "3_lookup.ons.onspd"  # INSR_W7 W7.08


def _trust_site_areas(locations):
    """Postcode areas of the Trust's own sites: the areas of ACTIVE facility-level care sites (W7-S1 ruling, Ben
    2026-10-31). Derived from the data on every refresh, never a hard-coded list; inactive Cerner template sites (e.g.
    'KINGS HOSP' at TQ2 7AA, 'KINGS TRUST' in PR) do not count. One row per area, for an equi-join."""
    return (locations
            .where((F.col("location_level") == "facility") & (F.col("status") == "active"))
            .select(_postcode_area(F.col("address_postcode_masked")).alias("_ta_area"))
            .where(F.col("_ta_area").isNotNull())
            .distinct()
            .withColumn("_trust_area", F.lit(True)))


def _postcode_area(postcode):
    """The postcode area (the leading letters, e.g. 'E', 'EC', 'TQ') of a postcode, upper-cased; NULL when none."""
    area = F.regexp_extract(F.upper(F.trim(postcode)), r"^([A-Z]{1,2})[0-9]", 1)
    return F.when(area != "", area)


def _with_site_geocode(locations):
    """INSR_W7_SITE_GEOCODE_V1 (W7.08; J15 ruling 2026-10-29, L651): fill a Trust site's missing coordinates.

    Care-site rows carry the site's postcode in address_postcode_masked. Where Millennium gives no latitude, the
    ONS Postcode Directory centroid of that postcode is used, and coordinate_source says which. Only Trust
    locations pass through here (reference_location); person addresses are never touched. ONSPD gives postcodes
    without a grid reference latitude 99.999999, so those never match (lat < 90). One equi-join to a lookup whose
    postcode is unique, so the location grain is unchanged. The centroid choice is a column of its own before it
    is used twice: a predicate built on `latitude` would re-resolve to the new latitude in the next withColumn.

    W7-S1 ruling (Ben 2026-10-31): a location whose postcode area is not one of the Trust's own site areas
    (_trust_site_areas) gets NULL coordinates and coordinate_status 'postcode_outside_trust_areas', whatever the
    coordinates' source (e.g. the 27 'KINGS HOSP' template locations at TQ2 7AA). The area set is joined on the
    area key (one row per area), so the grain is unchanged.
    """
    onspd = (read_source(SRC_ONSPD)
             .where(F.col("lat") < 90)
             .select(F.upper(F.regexp_replace(F.col("pcds"), " ", "")).alias("_pc_key"),
                     F.col("lat").alias("_pc_lat"), F.col("long").alias("_pc_long")))
    keyed = (locations
             .withColumn("_pc_key", F.upper(F.regexp_replace(F.col("address_postcode_masked"), " ", "")))
             .withColumn("_pc_area", _postcode_area(F.col("address_postcode_masked"))))
    joined = (keyed.join(onspd, "_pc_key", "left")
              .join(_trust_site_areas(locations), F.col("_pc_area") == F.col("_ta_area"), "left")
              .withColumn("_outside", F.col("_pc_area").isNotNull() & F.col("_trust_area").isNull())
              .withColumn("_use_centroid", F.col("latitude").isNull() & F.col("_pc_lat").isNotNull()))
    return (joined
            .withColumn("coordinate_source",
                        F.when(F.col("_outside"), F.lit(None).cast("string"))
                         .when(F.col("latitude").isNotNull(), F.lit("source"))
                         .when(F.col("_use_centroid"), F.lit("onspd_postcode_centroid")))
            .withColumn("coordinate_status",
                        F.when(F.col("_outside"), F.lit("postcode_outside_trust_areas")))
            .withColumn("latitude", F.when(F.col("_outside"), F.lit(None).cast("double"))
                        .when(F.col("_use_centroid"), F.col("_pc_lat")).otherwise(F.col("latitude")))
            .withColumn("longitude", F.when(F.col("_outside"), F.lit(None).cast("double"))
                        .when(F.col("_use_centroid"), F.col("_pc_long")).otherwise(F.col("longitude")))
            .drop("_pc_key", "_pc_lat", "_pc_long", "_pc_area", "_ta_area", "_trust_area", "_outside",
                  "_use_centroid"))


@materialized_view(
    name=_n("journey_reference.location"),  # SDI_LOCATION_LANE_V1
    refresh_policy="auto",
)
def location():
    # Build the declared dataset: Effective-dated facility, building, and nurse-unit hierarchy
    # derived from bronze care sites.
    # INSR_W7 W7.08: Trust sites with no source coordinates take their postcode centroid
    return _with_site_geocode(_lifecycle_source_location()).drop(*LOCATION_LIFECYCLE_FIELDS, *LOCATION_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_reference._location_metadata'),
    refresh_policy="auto",
)
def location_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for reference_location;
    # same row grain as the research table. Join keys: location_key, location_code.
    return (_lifecycle_source_location()).select(*LOCATION_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {
    'reference_location': ValuePolicy(table='reference_location', enabled=True),
}


WRAP_POLICY = {'reference_location'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_reference_location_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged from '
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
                                  'table': 'Internal quality and batch metadata for reference_location; same '
                                           'row grain as the research table. Join keys: location_key, '
                                           'location_code.'},
 'reference_location': {'columns': {'address_city': 'Source address city.',
                                    'address_postcode_masked': 'Privacy-aware source postcode.',
                                    'coordinate_source': 'Where latitude/longitude come from: source (the '
                                                         'Millennium address record) or '
                                                         'onspd_postcode_centroid (ONS Postcode Directory '
                                                         'centroid of the site postcode, used when the '
                                                         'source has none); null when the location has no '
                                                         'coordinates (INSR W7.08, J15 ruling 2026-10-29).',
                                    'coordinate_status': "postcode_outside_trust_areas when the location's "
                                                         "postcode area is not one of the Trust's own site "
                                                         'postcode areas (the areas of its active '
                                                         'facility-level care sites); its latitude, '
                                                         'longitude and coordinate_source are then NULL '
                                                         '(e.g. inactive Cerner template sites such as KINGS '
                                                         'HOSP, TQ2 7AA). NULL otherwise (W7-S1 ruling '
                                                         '2026-10-31).',
                                    'latitude': "Latitude of the location: the Millennium address record's "
                                                'where it holds one; for a Trust care site with none, the '
                                                'ONS Postcode Directory centroid of the site postcode (see '
                                                "coordinate_source). Trust sites only, never a person's "
                                                'address (J15).',
                                    'loaded_at': 'From map_care_site: maximum ADC_UPDT per facility_cd for '
                                                 'facility rows, maximum ADC_UPDT per building_cd for '
                                                 "building rows, and the individual source row's ADC_UPDT "
                                                 'for nurse-unit rows. The hierarchy union adds no parent '
                                                 'clock or cross-level aggregation; this is bronze ingestion '
                                                 'provenance, not location validity time or Silver refresh '
                                                 "time. map_location_unit rows carry that row's ADC_UPDT.",
                                    'location_code': 'Native Millennium location code; primary key together '
                                                     'with location_level.',
                                    'location_key': 'Deterministic SHA-256 key across location levels.',
                                    'location_level': 'Hierarchy level. map_care_site rows: facility, '
                                                      'building or nurse_unit. map_location_unit rows: its '
                                                      'LOCATION_LEVEL -- nurse_unit, room, bed, or the '
                                                      'lower-cased Millennium location type (e.g. ancilsurg, '
                                                      'rad, ambulatory, waitroom, facility, building); '
                                                      'unclassified when the type is unknown.',
                                    'longitude': 'Longitude of the location; same source rule as latitude '
                                                 '(see coordinate_source).',
                                    'name': 'Source location name.',
                                    'organization_id': 'Millennium ORGANIZATION_ID as BIGINT when available.',
                                    'organization_id_basis': 'care_site_source when map_care_site carries '
                                                             'the organisation; parent_care_site / '
                                                             'grandparent_care_site / '
                                                             'great_grandparent_care_site when a '
                                                             'map_location_unit row inherits it through '
                                                             'PARENT_LOCATION_CD; NULL when no care-site '
                                                             'ancestor within three hops has one. INSR_W2 '
                                                             'W2.11 (L4164).',
                                    'parent_location_key': 'Deterministic SHA-256 key of the parent '
                                                           'location: the facility or building for care-site '
                                                           'rows; for map_location_unit rows the key of '
                                                           "PARENT_LOCATION_CD (a bed's room, a room's nurse "
                                                           "unit, a nurse unit's building, otherwise the "
                                                           'Millennium location-group parent). Null when the '
                                                           'parent is not published.',
                                    'physical_type_code': 'FHIR physical location type code: si facility, bu '
                                                          'building, wa nurse unit, ro room, bd bed; null '
                                                          'for other Millennium location types.',
                                    'source_location_code': 'Source Millennium location code.',
                                    'source_row_id': 'Stable source row identifier.',
                                    'source_table': 'Fully qualified bronze source table.',
                                    'status': 'Source-derived location status.',
                                    'valid_from': 'Source validity start.',
                                    'valid_to': 'Source validity end.'},
                        'table': 'Effective-dated Millennium location hierarchy: facilities, buildings and '
                                 'nurse units from bronze care sites, and every other location code (rooms, '
                                 'beds, theatres, radiology, ambulatory) from bronze map_location_unit.'}}
