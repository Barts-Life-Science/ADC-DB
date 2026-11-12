"""Silver clinical.clinical_score_component: 1 dataset(s).

- datasets: clinical_clinical_score_component
- moved verbatim from: 2_silver_clinical_events:115, 2_silver_clinical_events:4829, 2_silver_clinical_events:4853

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.cross_qc import _s3_direct_axis
from silver_core.journey_core import _n, _usable_code, axis_columns, spark
from silver_tables._common.journey import _s3_lookup_axis


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_S3B_SCORE_COMPONENT_MAP = "3_lookup.omop.score_component_snomed_map"


# Clinical / clinical score component

# ==== journey_clinical.clinical_score_component ====
CLINICAL_SCORE_COMPONENT_PUBLIC_COLUMNS = [
    "patient_event_key", "event_id", "doc_response_key", "source_object",
    "sequence", "component_id", "coding_system", "coding_code",
    "coding_display", "value_number", "value_text", "unit", "loaded_at",
] + axis_columns("component")


@materialized_view(
    name=_n("journey_clinical.clinical_score_component"),
    refresh_policy="auto",
)
def clinical_score_component():
    # Build the declared dataset: One ordered component per native or governed promoted clinical
    # score.
    numeric = spark.read.table(
        _n("journey_clinical._numeric_clinical_score")
    ).select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        F.col("source_code").alias("_score_source_code"), "_components_json", "loaded_at",
    )
    promoted = spark.read.table(
        _n("journey_clinical._promoted_clinical_score_primitive")
    ).select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        F.col("source_code").alias("_score_source_code"), "_components_json", "loaded_at",
    )
    parents = numeric.unionByName(promoted).where(_usable_code(F.col("_score_source_code")))
    component_schema = (
        "array<struct<sequence:bigint,component_id:string,coding_system:string,"
        "coding_code:string,coding_display:string,value_number:decimal(38,10),"
        "value_text:string,unit:string>>"
    )
    exploded = parents.select(
        "patient_event_key", "event_id", "doc_response_key", "source_object", "_score_source_code", "loaded_at",
        F.explode(F.from_json(F.col("_components_json"), component_schema)).alias("_component"),
    )
    result = exploded.select(
        "patient_event_key", "event_id", "doc_response_key", "source_object",
        "_score_source_code",
        F.col("_component.sequence").alias("sequence"),
        F.col("_component.component_id").alias("component_id"),
        F.col("_component.coding_system").alias("coding_system"),
        F.col("_component.coding_code").alias("coding_code"),
        F.col("_component.coding_display").alias("coding_display"),
        F.col("_component.value_number").alias("value_number"),
        F.col("_component.value_text").alias("value_text"),
        F.col("_component.unit").alias("unit"),
        "loaded_at",
    )
    component_key = F.sha2(F.concat_ws("|", F.coalesce("_score_source_code", F.lit("")),
        F.coalesce("coding_system", F.lit("")), F.coalesce("coding_code", F.lit("")),
        F.lower(F.trim(F.regexp_replace(F.coalesce("coding_display", F.lit("")), r"\s+", " "))),
        F.when(F.col("value_number").isNull(),
               F.lower(F.trim(F.regexp_replace(F.coalesce("value_text", F.lit("")), r"\s+", " "))))
         .otherwise(F.lit(""))), 256)
    result = _s3_direct_axis(
        result, "component", F.lit("urn:barts:score-component"), component_key,
        F.coalesce("coding_display", "value_text"),
    )
    result = _s3_lookup_axis(
        result, "component", SRC_S3B_SCORE_COMPONENT_MAP, "urn:barts:score-component",
        component_key, allow_split=False, model_name="clinical_score_component",
        row_key="patient_event_key",
    )
    return result.drop("_score_source_code").select(*CLINICAL_SCORE_COMPONENT_PUBLIC_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_clinical_score_component': {'columns': {'coding_code': 'Source component code.',
                                                   'coding_display': 'Source component display.',
                                                   'coding_system': 'Source coding-system namespace for the '
                                                                    'component.',
                                                   'component_id': 'Native source component identifier.',
                                                   'component_map_candidate_count': 'Number of candidate '
                                                                                    'targets observed; a '
                                                                                    'mapped parent is '
                                                                                    'unambiguous when this '
                                                                                    'is 1.',
                                                   'component_map_candidate_set_id': 'Stable identifier of '
                                                                                     'the retained candidate '
                                                                                     'set.',
                                                   'component_map_competing_count': 'Candidates within 0.05 '
                                                                                    'cosine of the selected '
                                                                                    'candidate.',
                                                   'component_map_component_count': 'Number of '
                                                                                    'source-derived '
                                                                                    'components.',
                                                   'component_map_component_index': 'One-based '
                                                                                    'source-derived '
                                                                                    'component index.',
                                                   'component_map_confidence': 'Mapping confidence in [0,1] '
                                                                               'where the lane measured one.',
                                                   'component_map_cosine': 'Measured cosine between the '
                                                                           'normalised source and selected '
                                                                           'target; null when scoring did '
                                                                           'not run.',
                                                   'component_map_method': 'Mapping method: source_native, '
                                                                           'rule, exact_synonym or '
                                                                           'embedding_proposed.',
                                                   'component_map_rollup_levels': 'Maximum hierarchy '
                                                                                  'distance to a selected '
                                                                                  'common ancestor.',
                                                   'component_map_rule_id': 'Stable rule or lane identifier '
                                                                            'that produced the mapping.',
                                                   'component_map_scoring_model': 'Embedding model that '
                                                                                  'produced map_cosine.',
                                                   'component_map_status': 'Explicit mapping outcome from '
                                                                           'the S3b status vocabulary.',
                                                   'component_map_status_reason': 'Reason for the mapping '
                                                                                  'outcome or abstention.',
                                                   'component_map_version': 'Version of the source, '
                                                                            'terminology release or model '
                                                                            'used by the mapping.',
                                                   'component_omop_concept_id': 'OMOP concept identifier '
                                                                                'selected with the score '
                                                                                'component or categorical '
                                                                                'score response mapping.',
                                                   'component_omop_source_concept_id': 'Non-standard OMOP '
                                                                                       'source concept '
                                                                                       'identifier, kept '
                                                                                       'separate from a '
                                                                                       'standard target.',
                                                   'component_snomed_code': 'SNOMED CT code selected for the '
                                                                            'score component or categorical '
                                                                            'score response; null when no '
                                                                            'unambiguous mapping is '
                                                                            'available.',
                                                   'component_snomed_display': 'SNOMED CT preferred display '
                                                                               'selected for the score '
                                                                               'component or categorical '
                                                                               'score response.',
                                                   'component_source_code': 'Source code for the score '
                                                                            'component or categorical score '
                                                                            'response, retained without '
                                                                            'de-identification.',
                                                   'component_source_display': 'Source display for the score '
                                                                               'component or categorical '
                                                                               'score response.',
                                                   'component_source_system': 'Canonical coding-system URI '
                                                                              'for the source score '
                                                                              'component or categorical '
                                                                              'score response.',
                                                   'component_target_code': 'Code of the selected target in '
                                                                            'its own vocabulary.',
                                                   'component_target_display': 'Display of the selected '
                                                                               'target in its own '
                                                                               'vocabulary.',
                                                   'component_target_system': 'Coding-system URI of the '
                                                                              'selected target.',
                                                   'doc_response_key': 'PowerForm DOC_RESPONSE_KEY for the '
                                                                       'form-promotion parent arm.',
                                                   'event_id': 'Millennium numeric EVENT_ID for the '
                                                               'numeric-event parent arm.',
                                                   'loaded_at': 'Bronze load timestamp inherited from the '
                                                                'parent source row.',
                                                   'patient_event_key': 'Deterministic key of the parent '
                                                                        'clinical_clinical_score row.',
                                                   'sequence': 'Component sequence within the parent score.',
                                                   'source_object': 'Parent source arm: numeric_event or '
                                                                    'form_promotion.',
                                                   'unit': 'Source component unit when supplied.',
                                                   'value_number': 'Numeric component value when supplied.',
                                                   'value_text': 'Verbatim component text when supplied.'},
                                       'table': 'One ordered component per native or governed promoted '
                                                'clinical score.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_clinical_score', dataset='_clinical_clinical_score_parent_inputs', keys=('patient_event_key',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._clinical_score_parent_inputs'), parent_name=_n('journey_clinical.clinical_score'))
SPECS = with_parent_inputs(SPECS, 'clinical_clinical_score_component', R4_PARENT_INPUTS)
