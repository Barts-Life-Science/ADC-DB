"""Silver clinical.vital_sign: 3 dataset(s).

- datasets: _clinical_qc_vital_sign, clinical_vital_sign, _clinical_vital_sign_metadata
- moved verbatim from: 2_silver_clinical_events:4447, 2_silver_clinical_events:4515, 2_silver_clinical_events:4533, silver_journey_shared.py:1192, silver_journey_shared.py:1193, silver_journey_shared.py:1194, silver_journey_shared.py:1197

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, spark
from silver_tables._common.journey import _vital_sign_canonical
from silver_tables.clinical._shared import VITAL_LIFECYCLE_COLUMNS, VITAL_PUBLIC_COLUMNS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# INSR_W2 W2.03: OMOP vocabulary 'Maps to' lanes. 3_lookup.omop.s3_vocab_maps_to holds one ranked row per
# (vocabulary, code) in the governed v2 lookup schema (standard target first, lowest concept_id; method rule;
# rule id omop:maps_to:<vocabulary>; version = vocabulary release). A lane is one join to it, so the S3 stage
# budget is respected. fill='target' maps an otherwise unmapped axis (the _s3_lookup_axis rule); 'omop' fills
# only {axis}_omop_concept_id / _omop_source_concept_id when NULL; 'source_concept' fills only the source concept.

# BBR_FOLDIN_V1 (design BBR-3): anthropometric synonyms on the vital axis. A vital row no lookup mapped whose
# normalised display (lower case, collapsed whitespace, leading 'zzz' stripped) names body weight, height or BMI takes
# that SNOMED observable. The rule id records whether the display says measured or estimated; the OMOP lane
# (f_vital_omop) then fills the OMOP id only where it is NULL. No pattern uses '.', so the Photon/JVM divergence on
# line breaks cannot apply.
# The LOINC (incl. LOINC "Estimated") OMOP ids of each anthropometric SNOMED concept: a governed row on one of these has
# no SNOMED crosswalk, so the synonym gives it the shared SNOMED concept. A row whose governed OMOP id is any other
# concept keeps its own mapping (the SNOMED-standard ids get their SNOMED code from the crosswalk under the governed
# rule; review fix 11-03: the step had relabelled governed rows as synonym-mapped before the crosswalk ran).

# BBR_FOLDIN_V1 (design BBR-11 / BBR-12): pathology test keys on code|display, and QC / non-blood variants.
# 3_lookup.omop.pathology_test_snomed_additions rows of system BBR_PATH_TEST_CODE_DISPLAY_SYSTEM are keyed
# concat_ws('|', test code, normalised display): the code alone is ambiguous (TFC EPI is both 'EPI-estimated GFR' and
# 'Epithelial cells'). Such a row codes a test the bronze map left without a code, or (rule
# BBR_PATH_TEST_DIMENSION_RULE) re-maps a test whose rows carry a rate or per-WBC unit onto its dimension-specific
# concept (or no_candidate). It runs after the tier rescores, which can clear a vetoed target.
# '[\s\S]' instead of '.' (estate rule for regexes that decide a mapping).

# BBR_FOLDIN_V1 (design BBR-4): vital units and a canonical value for the three anthropometric concepts. A NULL
# unit_concept_id with no source unit (or one whose UCUM is that unit) takes the concept's default unit (rule vital:concept_default_unit on unit_map_rule_id); heights
# recorded in metres (<= 3, unit NULL or metres) become cm, weights in grams kg. Every other vital passes its value
# with conversion 'identity' when its unit axis reached a UCUM code.
BBR_VITAL_DEFAULT_UNITS = {'27113001': ('9529', 'kg'), '50373000': ('8582', 'cm'), '60621009': ('9531', 'kg/m2')}


BBR_VITAL_DEFAULT_UNIT_RULE = 'vital:concept_default_unit'


BBR_UCUM_URI = "http://unitsofmeasure.org"


def _bbr_vital_canonical(df):
    """BBR-4: fill the anthropometric default unit concept and add value_canonical / canonical_ucum_code /
    canonical_conversion (the pure-Python twin is bbr_review/build/bbr_rules.vital_canonical_python)."""
    snomed, value = F.col("vital_snomed_code"), F.col("value_number")
    unit = F.lower(F.trim(F.coalesce(F.col("unit_source_value"), F.lit(""))))
    default_concept, default_ucum = F.lit(None).cast("string"), F.lit(None).cast("string")
    for code, (concept, ucum) in BBR_VITAL_DEFAULT_UNITS.items():
        default_concept = F.when(snomed == code, F.lit(concept)).otherwise(default_concept)
        default_ucum = F.when(snomed == code, F.lit(ucum)).otherwise(default_ucum)
    # Inputs are materialised before unit_concept_id / unit_map_rule_id change (a predicate over unit_concept_id
    # would re-resolve to the filled value after its withColumn and lose the concept_default_unit label).
    df = (df.withColumn("_bbr_default_concept", default_concept).withColumn("_bbr_default_ucum", default_ucum)
            .withColumn("_bbr_unit_ucum", F.when(F.col("unit_target_system") == BBR_UCUM_URI, F.col("unit_target_code"))))
    # The default fills only a row whose unit is absent or already means that unit (a weight in lb keeps NULL).
    df = df.withColumn("_bbr_filled", F.col("unit_concept_id").isNull() & F.col("_bbr_default_concept").isNotNull()
                       & ((unit == "") | F.coalesce(F.col("_bbr_unit_ucum") == F.col("_bbr_default_ucum"), F.lit(False))))
    filled = F.col("_bbr_filled")
    # A height with no unit recorded at <= 3 is in metres (it becomes cm below): its filled unit is the metre, so
    # unit_concept_id keeps describing value_number (review fix 11-03).
    metres_no_unit = (snomed == "50373000") & (value <= 3) & (unit == "")
    fill_concept = F.when(metres_no_unit, F.lit('9546')).otherwise(F.col("_bbr_default_concept"))
    conversion = (F.when(value.isNull(), F.lit(None).cast("string"))
                  .when((snomed == "50373000") & (value <= 3) & unit.isin("", "m", "metres", "metre"), F.lit("m->cm"))
                  .when((snomed == "27113001") & (unit == "g"), F.lit("g->kg"))
                  .when(filled & (unit == ""), F.lit("concept_default_unit"))
                  # identity only when the row's unit is the concept's own (or, for other vitals, any UCUM unit);
                  # an anthropometric value in another unit (lb, stone, inches) gets no canonical value.
                  .when(F.col("_bbr_default_concept").isNotNull()
                        & (F.coalesce(F.col("_bbr_unit_ucum") == F.col("_bbr_default_ucum"), F.lit(False))
                           | F.coalesce(F.col("unit_concept_id") == F.col("_bbr_default_concept"), F.lit(False))),
                        F.lit("identity"))
                  .when(F.col("_bbr_default_concept").isNull() & F.col("_bbr_unit_ucum").isNotNull(), F.lit("identity")))
    df = df.withColumn("canonical_conversion", conversion)
    conversion = F.col("canonical_conversion")
    canonical = (F.when(conversion == "m->cm", value * 100).when(conversion == "g->kg", value / 1000)
                 .when(conversion.isNotNull(), value))
    canonical_ucum = (F.when(conversion == "m->cm", F.lit("cm")).when(conversion == "g->kg", F.lit("kg"))
                      .when(conversion.isNotNull(), F.coalesce(F.col("_bbr_default_ucum"), F.col("_bbr_unit_ucum"))))
    return (df.withColumn("value_canonical", canonical.cast("decimal(38,10)"))
              .withColumn("canonical_ucum_code", canonical_ucum)
              .withColumn("unit_map_rule_id",
                          F.when(filled, F.lit(BBR_VITAL_DEFAULT_UNIT_RULE)).otherwise(F.col("unit_map_rule_id")))
              .withColumn("unit_concept_id", F.when(filled, fill_concept).otherwise(F.col("unit_concept_id")))
              .drop("_bbr_default_concept", "_bbr_default_ucum", "_bbr_filled", "_bbr_unit_ucum"))


# Clinical /  qc vital sign
@materialized_view(
    name=_n("journey_clinical._qc_vital_sign"),
    refresh_policy="auto",
)
def _qc_vital_sign():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for vital_sign.
    return _cross_qc_primitive(
        _vital_sign_canonical(),
        "vital_sign",
        {"vital_code": "_qc_vital_code_json"},
    )


@materialized_view(
    name=_n("journey_clinical.vital_sign"),
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="auto",
)
def vital_sign():
    # Build the declared dataset: Native numeric-event measurements and governed form-promoted
    # vital signs, retaining source identifiers, measurement values and units.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_vital_sign")),
        "vital_sign",
        {"vital_code": "_qc_vital_code_json"},
        VITAL_PUBLIC_COLUMNS,
        VITAL_LIFECYCLE_COLUMNS,
    )


@materialized_view(
    name=_n('journey_clinical._vital_sign_metadata'),
    refresh_policy="auto",
)
def vital_sign_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_vital_sign;
    # same row grain as the research table. Join keys: patient_event_key, source_object,
    # doc_response_key, event_id.
    return (spark.read.table(_n("journey_clinical._qc_vital_sign"))).select(*VITAL_LIFECYCLE_COLUMNS)


def _prepare_vital_sign(df):
    """vital_sign: per-row prepare step, moved verbatim from _cross_qc_prepare (silver_journey_shared.py:1104)."""
    for _axis, _value in (
        ("unit", F.col("unit_source_value")),
        ("method", F.col("method_display")),
        ("body_site", F.col("body_site_display")),
        ("interpretation", F.col("interpretation_display")),
    ):
        df = _s3_direct_axis(
            df, _axis, F.lit(f"urn:barts:vital:{_axis}"),
            F.lower(F.trim(F.regexp_replace(_value, r"\s+", " "))), _value,
        )
    return df


def _finish_vital_sign(df):
    """vital_sign: per-row finish step, moved verbatim from _cross_qc_finish (silver_journey_shared.py:1285)."""
    # BBR_FOLDIN_V1 (BBR-4): anthropometric default unit and canonical value
    df = _bbr_vital_canonical(df)
    return df


CROSS_QC = {
    'vital_sign': CrossQc(model='vital_sign', fk=('encounter_id', 'person_id'), dates=('event_after_death_30d', 'event_before_birth'), coded_axes={'vital_code': ('vital', False)}, extra_public_columns=('vital_omop_map_rule_id', 'vital_omop_map_version', 'value_canonical', 'canonical_ucum_code', 'canonical_conversion'), promote_lifecycle=['identity_status', 'person_id_resolved', 'encounter_id_resolved'], finish_steps=(_finish_vital_sign,), prepare_steps=(_prepare_vital_sign,)),
}
_cross_qc = CrossQcBinding(CROSS_QC)
_cross_qc_primitive = _cross_qc.primitive
_cross_qc_prepare = _cross_qc.prepare
_cross_qc_finish = _cross_qc.finish
_cross_qc_public = _cross_qc.public


VALUE_POLICIES = {
    'clinical_vital_sign': ValuePolicy(table='clinical_vital_sign', enabled=True),
}


WRAP_POLICY = {'clinical_vital_sign'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_qc_vital_sign': {'columns': {},
                             'table': 'Private JSON bridge and Gold cross-rule flags for vital_sign.'},
 '_clinical_vital_sign_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
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
                                   'table': 'Internal quality and batch metadata for clinical_vital_sign; '
                                            'same row grain as the research table. Join keys: '
                                            'patient_event_key, source_object, doc_response_key, event_id.'},
 'clinical_vital_sign': {'columns': {'body_site_code': 'Body-site code when supplied.',
                                     'body_site_display': 'Body-site display when supplied.',
                                     'canonical_conversion': 'identity | m->cm (height <= 3 with no unit or '
                                                             'metres, x100) | g->kg (weight in g, /1000) | '
                                                             'concept_default_unit (no source unit; the '
                                                             "concept's unit assumed, unit_map_rule_id "
                                                             'vital:concept_default_unit) | NULL (no numeric '
                                                             'value, or no unit). BBR_FOLDIN_V1 BBR-4.',
                                     'canonical_ucum_code': 'UCUM unit of value_canonical: the '
                                                            "anthropometric concept's unit (kg, cm, kg/m2), "
                                                            "else the unit axis's UCUM target. BBR_FOLDIN_V1 "
                                                            'BBR-4.',
                                     'confidentiality_code': 'Security classification when supplied.',
                                     'doc_response_key': 'PowerForm DOC_RESPONSE_KEY; native key for the '
                                                         'form_promotion arm.',
                                     'encounter_id': 'Millennium ENCNTR_ID; joins to '
                                                     'spine_encounter.encounter_id.',
                                     'event_datetime': 'Clinical start date and time of the event as '
                                                       'recorded on the Millennium CLINICAL_EVENT row.',
                                     'event_datetime_local': 'event_datetime in Europe/London wall-clock '
                                                             'time (GMT/BST). Millennium rows take the '
                                                             'bronze *_LOCAL companions of the same source '
                                                             'columns and fallback order; rows from '
                                                             'local-clock sources (LUNA, PACS/DICOM, TFC '
                                                             'LIMS, CCMDS, MSDS, BadgerNet, EndoBase, SLAM, '
                                                             'iWeb, Datix and similar) carry event_datetime '
                                                             'unchanged. event_datetime is a UTC instant for '
                                                             'Millennium rows, so take local calendar dates '
                                                             'and clock times from this column.',
                                     'event_datetime_status': 'registry_dated or registry_undated for '
                                                              'registry observations; null on native rows.',
                                     'event_end_datetime': 'Measurement end timestamp.',
                                     'event_end_datetime_local': 'event_end_datetime in Europe/London '
                                                                 'wall-clock time (GMT/BST), by the same '
                                                                 'rule as event_datetime_local; null where '
                                                                 'event_end_datetime is null.',
                                     'event_id': 'Millennium numeric EVENT_ID; native key for the '
                                                 'numeric_event arm.',
                                     'interpretation_code': 'Source interpretation code.',
                                     'interpretation_display': 'Source interpretation display.',
                                     'loaded_at': 'map_numeric_events.ADC_UPDT for numeric-event rows, or '
                                                  'map_powerform_assessment_item.ADC_UPDT for form-promoted '
                                                  "rows. Each arm's bronze load clock is carried unchanged "
                                                  'through its staging, union and quality wrappers. No '
                                                  'form-parent or promotion-configuration clock is added; '
                                                  'missing values remain null and this is not Silver refresh '
                                                  'time.',
                                     'method_code': 'Source measurement method code.',
                                     'method_display': 'Source measurement method display.',
                                     'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                          'cross-feed join key.',
                                     'performer_practitioner_id': 'Millennium personnel PERSON_ID for the '
                                                                  'performer.',
                                     'person_id': 'Millennium PERSON_ID; native person foreign key.',
                                     'promotion_rule_id': 'Governed promotion-rule identifier.',
                                     'record_status': 'Arm-specific row status. Numeric events are retracted '
                                                      'when map_numeric_events.SOURCE_DELETED_IND is true '
                                                      '(null defaults false), otherwise superseded when '
                                                      'CLINICAL_EVENT_VALID_UNTIL_DT_TM is before '
                                                      '2100-01-01, otherwise active. Form-promoted rows are '
                                                      'retracted when CANONICAL_SOURCE_DELETED_IND is true '
                                                      'or SOURCE_PRESENT_IND is false or missing; otherwise '
                                                      'superseded when ACTIVE_IND is zero or missing, '
                                                      'otherwise active. Retraction has priority over '
                                                      'supersession; this is not the clinical result-status '
                                                      'code.',
                                     'record_status_effective_from': 'map_numeric_events.CLINICAL_EVENT_VALID_FROM_DT_TM '
                                                                     'on the numeric-event arm; '
                                                                     'map_powerform_assessment_item.FIRST_DOCUMENTED_DT_TM '
                                                                     'on the form-promotion arm. Each is '
                                                                     'carried unchanged, with no '
                                                                     'performed/response/ingestion-time '
                                                                     'fallback; a missing value remains '
                                                                     'null.',
                                     'record_status_effective_to': 'Numeric events return '
                                                                   'CLINICAL_EVENT_VALID_UNTIL_DT_TM when '
                                                                   'deleted or ended, otherwise null; '
                                                                   'deletion can therefore retain a '
                                                                   'far-future end value. Form-promoted rows '
                                                                   'return LAST_DOCUMENTED_DT_TM when '
                                                                   'deleted, absent or inactive, otherwise '
                                                                   'null. No missing-end fallback is '
                                                                   'applied, and the form documentation end '
                                                                   'is not an independently observed '
                                                                   'status-transition time.',
                                     'reference_range_high': 'Reference-range upper bound.',
                                     'reference_range_low': 'Reference-range lower bound.',
                                     'registry_field_id': 'Governed SHA-256 registry product and field '
                                                          'identifier for registry observations; null on '
                                                          'native rows.',
                                     'result_status_code': 'Source result status code.',
                                     'result_status_display': 'Source result status display.',
                                     'source_code': 'Source vital code.',
                                     'source_coding_system': 'Coding system or source namespace in which the '
                                                             'source code is defined for each vital sign '
                                                             'record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Source code meanings and '
                                                             'sentinel values are retained unless the '
                                                             'pipeline explicitly maps them; null means no '
                                                             'code was supplied.',
                                     'source_dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID of '
                                                                     'the source form for promoted rows.',
                                     'source_display': 'Source vital display.',
                                     'source_feed': 'Registered source route.',
                                     'source_form_key': 'Deterministic SHA-256 key of the source PowerForm '
                                                        'instance for promoted rows.',
                                     'source_object': 'Native source arm: numeric_event or form_promotion.',
                                     'source_update_timestamp': 'For numeric events, greatest of '
                                                                'map_numeric_events.STRING_RESULT_UPDT_DT_TM, '
                                                                'CLINICAL_EVENT_UPDT_DT_TM and ADC_UPDT. For '
                                                                'form-promoted rows, '
                                                                'map_powerform_assessment_item.ADC_UPDT '
                                                                'directly. This mixes source and ingestion '
                                                                'provenance rather than uniformly '
                                                                'representing a native modification clock; '
                                                                'the promotion lookup adds no clock and '
                                                                'Silver refresh time is not substituted.',
                                     'subject_id_system': 'Identifier system used for subject_key.',
                                     'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                    'identifier. Not salted, not secret; a stable join key '
                                                    'across feeds where person_id is unresolved.',
                                     'unit_concept_id': 'Mapped unit concept identifier.',
                                     'unit_source_value': 'Source result unit.',
                                     'value_canonical': 'value_number restated in canonical_ucum_code (see '
                                                        'canonical_conversion): heights in cm, weights in '
                                                        'kg; equal to value_number otherwise. BBR_FOLDIN_V1 '
                                                        'BBR-4.',
                                     'value_number': 'Numeric vital value.',
                                     'value_text': 'Verbatim result text.',
                                     'vip_ind': 'VIP indicator when supplied.',
                                     'vital_map_candidate_count': 'Number of candidate targets observed; a '
                                                                  'mapped parent is unambiguous when this is '
                                                                  '1.',
                                     'vital_map_candidate_set_id': 'Stable identifier of the retained '
                                                                   'candidate set.',
                                     'vital_map_competing_count': 'Candidates within 0.05 cosine of the '
                                                                  'selected candidate.',
                                     'vital_map_component_count': 'Number of source-derived components.',
                                     'vital_map_component_index': 'One-based source-derived component index.',
                                     'vital_map_confidence': 'Mapping confidence in [0,1] where the lane '
                                                             'measured one.',
                                     'vital_map_cosine': 'Measured cosine between the normalised source and '
                                                         'selected target; null when scoring did not run.',
                                     'vital_map_method': 'Mapping method: source_native, rule, exact_synonym '
                                                         'or embedding_proposed.',
                                     'vital_map_rollup_levels': 'Maximum hierarchy distance to a selected '
                                                                'common ancestor.',
                                     'vital_map_rule_id': 'Stable rule or lane identifier that produced the '
                                                          'mapping.',
                                     'vital_map_scoring_model': 'Embedding model that produced map_cosine.',
                                     'vital_map_status': 'Explicit mapping outcome from the S3b status '
                                                         'vocabulary.',
                                     'vital_map_status_reason': 'Reason for the mapping outcome or '
                                                                'abstention.',
                                     'vital_map_version': 'Version of the source, terminology release or '
                                                          'model used by the mapping.',
                                     'vital_omop_concept_id': 'OMOP concept identifier selected with the '
                                                              'vital sign mapping.',
                                     'vital_omop_map_rule_id': 'Rule that filled vital_omop_concept_id / '
                                                               'vital_omop_source_concept_id from the OMOP '
                                                               "'Maps to' lookup "
                                                               '(omop:maps_to:<vocabulary>); NULL when those '
                                                               "ids came with the row's own mapping. INSR_W2 "
                                                               'W2.03.',
                                     'vital_omop_map_version': 'Vocabulary release (<vocabulary>|<version>) '
                                                               'of the lookup that filled the vital OMOP '
                                                               'ids; NULL when the lane filled nothing. '
                                                               'INSR_W2 W2.03.',
                                     'vital_omop_source_concept_id': 'Non-standard OMOP source concept '
                                                                     'identifier, kept separate from a '
                                                                     'standard target.',
                                     'vital_snomed_code': 'SNOMED CT code selected for the vital sign; null '
                                                          'when no unambiguous mapping is available.',
                                     'vital_snomed_display': 'SNOMED CT preferred display selected for the '
                                                             'vital sign.',
                                     'vital_source_code': 'Source code for the vital sign, retained without '
                                                          'de-identification.',
                                     'vital_source_display': 'Source display for the vital sign.',
                                     'vital_source_system': 'Canonical coding-system URI for the source '
                                                            'vital sign.',
                                     'vital_target_code': 'Code of the selected target in its own '
                                                          'vocabulary.',
                                     'vital_target_display': 'Display of the selected target in its own '
                                                             'vocabulary.',
                                     'vital_target_system': 'Coding-system URI of the selected target.',
                                     'withheld_identity_ind': 'Withheld identity indicator.'},
                         'table': 'Native numeric-event measurements and governed form-promoted vital signs, '
                                  'retaining source identifiers, measurement values and units.'}}
