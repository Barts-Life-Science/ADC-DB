"""Silver forms.form_response: 2 dataset(s).

- datasets: _clinical_form_response_primitive, clinical_form_response
- moved verbatim from: 6_silver_forms:119, 6_silver_forms:133, 6_silver_forms:136, 6_silver_forms:13845, 6_silver_forms:146, 6_silver_forms:211, 6_silver_forms:265, 6_silver_forms:266

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from pyspark.sql import Column
import pyspark.sql.functions as F
from silver_core.active_rows import active_rows
from silver_core.journey_core import spark
from silver_core.value_policy import time_only, time_part
from silver_tables.forms._shared import _n


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


FORM_SLUG_LOOKUP_SOURCE = spark.conf.get(
    "journey.form_slug_lookup_source",
    "3_lookup.omop.form_slug_lookup",
)


FORM_ELEMENT_LOOKUP_SOURCE = spark.conf.get(
    "journey.form_element_lookup_source",
    "3_lookup.omop.form_element_lookup",
)


def read_source(name):
    # Read the named source table as a Spark DataFrame.
    # ACTIVE_ROWS_V1: bronze rows that are not active never enter silver; the rule lives in silver_active_rows.py.
    return active_rows(spark.read.table(name), name)


# Deterministic identity helpers

# ==== Deterministic identity helpers ====
NULL_TOKEN = "~"


def _enc(col_or_lit):
    # Encode a key component as trimmed base64, using a distinct token for nulls so separators
    # cannot collide.
    value = col_or_lit if isinstance(col_or_lit, Column) else F.lit(col_or_lit)
    text_value = F.trim(value.cast("string"))
    return F.when(value.isNull(), F.lit(NULL_TOKEN)).otherwise(
        F.base64(text_value.cast("binary"))
    )


def stable_id(namespace, *columns):
    # Hash the namespace and encoded key components into a deterministic SHA-256 identifier.
    return F.sha2(
        F.concat_ws(":", _enc(namespace), *[_enc(column) for column in columns]),
        256,
    )


def _s3b_form_axis_from_omop(df, axis, source_system, source_code, source_display, concept_id, empty_reason):
    """Validate an existing PowerForm concept and expose an explicit outcome."""
    prefix = f"_s3b_{axis}_"
    concept = (spark.read.table("3_lookup.omop.concept")
               .where("invalid_reason IS NULL")
               .select(F.col("concept_id").cast("bigint").alias(prefix + "id"),
                       F.col("concept_code").alias(prefix + "code"),
                       F.col("concept_name").alias(prefix + "display"),
                       F.col("vocabulary_id").alias(prefix + "vocabulary"),
                       F.col("standard_concept").alias(prefix + "standard")))
    df = df.join(concept, concept[prefix + "id"] == concept_id.cast("bigint"), "left")
    mapped = F.col(prefix + "standard") == "S"
    target_system = (F.when(F.col(prefix + "vocabulary") == "SNOMED", F.lit("http://snomed.info/sct"))
                     .when(F.col(prefix + "vocabulary") == "LOINC", F.lit("http://loinc.org"))
                     .when(F.col(prefix + "vocabulary") == "UCUM", F.lit("http://unitsofmeasure.org"))
                     .when(mapped, F.concat(F.lit("urn:omop:vocabulary:"), F.lower(F.col(prefix + "vocabulary")))))
    values = {
        "source_system": F.lit(source_system), "source_code": source_code.cast("string"),
        "source_display": source_display.cast("string"),
        "snomed_code": F.when(mapped & (F.col(prefix + "vocabulary") == "SNOMED"), F.col(prefix + "code")),
        "snomed_display": F.when(mapped & (F.col(prefix + "vocabulary") == "SNOMED"), F.col(prefix + "display")),
        "omop_concept_id": F.when(mapped, F.col(prefix + "id")), "map_method": F.when(mapped, F.lit("rule")),
        "map_version": F.when(mapped, F.lit("POWERFORM_S5")),
        "map_confidence": F.lit(None).cast("double"),
        "map_rule_id": F.when(mapped, F.lit("powerform:S5:validated")),
        "map_candidate_count": F.when(mapped, F.lit(1)).otherwise(F.lit(0)),
        "target_system": target_system, "target_code": F.when(mapped, F.col(prefix + "code")),
        "target_display": F.when(mapped, F.col(prefix + "display")),
        "omop_source_concept_id": F.when(
            concept_id.isNotNull() & ~F.coalesce(mapped, F.lit(False)), concept_id.cast("bigint")),
        # A11: a deterministic translation is not evidence of cosine 1.0.  Keep
        # these null until the pairwise source/target embedding pass measures it.
        "map_cosine": F.lit(None).cast("double"),
        "map_scoring_model": F.lit(None).cast("string"),
        "map_status": F.when(mapped, F.lit("mapped")).otherwise(F.lit("no_candidate")),
        "map_status_reason": F.when(~F.coalesce(mapped, F.lit(False)), F.lit(empty_reason)),
        "map_competing_count": F.when(mapped, F.lit(1)).otherwise(F.lit(0)),
        "map_candidate_set_id": F.lit(None).cast("string"),
        "map_component_index": F.lit(1), "map_component_count": F.lit(1),
        "map_rollup_levels": F.lit(None).cast("int"),
    }
    for suffix, expression in values.items():
        df = df.withColumn(f"{axis}_{suffix}", expression)
    return df.drop(*[name for name in df.columns if name.startswith(prefix)])


# Form sources

# ==== Form sources ====
SRC_FORM_ACTIVITY = "4_prod.bronze.mill_form_activity"


SRC_FORM_ITEM = "4_prod.bronze.map_powerform_assessment_item"


SRC_FORM_ASSESSMENT = "4_prod.bronze.map_powerform_assessment"


SRC_FORM_VTE = "4_prod.bronze.map_mat_vte_assessment"


# Clinical / form response

# ==== journey_clinical.form_response ====
FORM_RESPONSE_BASE_COLUMNS = [
    "dcp_forms_activity_id",
    "patient_event_key",
    "response_id",
    "sequence",
    "section",
    "section_id",
    "element_id",
    "element_label",
    "grid_name",
    "grid_row",
    "response_kind",
    "response_value_text",
    "response_value_number",
    "response_value_datetime",
    "response_value_time",
    "response_coding_system",
    "unit_source",
    "response_coding_code",
    "response_coding_display",
    "question_concept_id",
    "value_concept_id",
    "unit_concept_id",
    "value_mapping_source",
    "powerform_mapping_rule_id",
    "powerform_mapping_version",
    "canonical_value_mapping_rule_id",
    "canonical_match_status",
    "pregnancy_id",
    "pregnancy_match_method",
    "active_ind",
    "source_present_ind",
    "source_deleted_ind",
    "form_slug",
    "configured_column",
    "loaded_at",
]


@materialized_view(
    name=_n("journey_clinical._form_response_primitive"),
    private=True,
    refresh_policy="auto",
)
def _form_response_primitive():
    # Build the declared dataset: Internal plain response-grain projection used by selection,
    # ambiguity evidence and the public clinical_form_response child.
    a = read_source(SRC_FORM_ACTIVITY).alias("a")
    i = read_source(SRC_FORM_ITEM).alias("i")
    g = read_source(SRC_FORM_ASSESSMENT).alias("g")
    v = read_source(SRC_FORM_VTE).select(
        "DOC_RESPONSE_KEY",
        "Pregnancy_ID",
        "PregnancyMatchMethod",
        "Source_ADC_UPDT",
    ).alias("v")
    slug_config = read_source(FORM_SLUG_LOOKUP_SOURCE).alias("slug_config")
    element_config = read_source(FORM_ELEMENT_LOOKUP_SOURCE).alias("element_config")
    joined = (
        a.join(i, a.DOC_RESPONSE_KEY == i.DOC_RESPONSE_KEY, "left")
        .join(g, a.DCP_FORMS_ACTIVITY_ID == g.DCP_FORMS_ACTIVITY_ID, "left")
        .join(v, a.DOC_RESPONSE_KEY == v.DOC_RESPONSE_KEY, "left")
        .join(
            slug_config,
            a.FORM_DESC_TXT == F.col("slug_config.source_display"),
            "left",
        )
        .join(
            element_config,
            (F.col("slug_config.form_slug") == F.col("element_config.form_slug"))
            & (
                F.upper(F.trim(a.ELEMENT_LABEL_TXT))
                == F.col("element_config.label_norm")
            )
            & (
                F.col("element_config.section_norm").isNull()
                | (
                    F.upper(F.trim(a.SECTION_DESC_TXT))
                    == F.col("element_config.section_norm")
                )
            ),
            "left",
        )
    )
    # Contract v2 parent/child conservation. clinical_form publishes one row per
    # form instance, gated on that instance's aggregated max(FORM_REF_ID). But
    # FORM_REF_ID is response-grain in mill_form_activity, so gating individual
    # response rows on it dropped 3,565,199 rows from the 2,638,541 instances
    # where only some rows carry a usable value -- instances the parent still
    # publishes and still counts in response_row_count. Restricting to the ids
    # the parent actually published makes conservation exact in both directions:
    # no orphan child rows, and no published instance short of the response count
    # it advertises. It also cannot drift, because the parent's own output is the
    # key set rather than a second copy of the parent's rule.
    published = spark.read.table(_n("journey_clinical.form")).select(
        F.col("dcp_forms_activity_id").cast("bigint").alias(
            "published_form_activity_id"
        )
    )
    joined = joined.join(
        published,
        a.DCP_FORMS_ACTIVITY_ID.cast("bigint")
        == F.col("published_form_activity_id"),
        "left_semi",
    )
    return joined.select(
        a.DCP_FORMS_ACTIVITY_ID.cast("bigint").alias("dcp_forms_activity_id"),
        stable_id("form:mill:powerform", a.DCP_FORMS_ACTIVITY_ID).alias(
            "patient_event_key"
        ),
        a.DOC_RESPONSE_KEY.cast("string").alias("response_id"),
        F.coalesce(a.RESPONSE_SEQUENCE_NBR, F.lit(0)).cast("long").alias(
            "sequence"
        ),
        a.SECTION_DESC_TXT.alias("section"),
        a.SECTION_REF_ID.cast("string").alias("section_id"),
        a.DCP_INPUT_REF_ID.cast("string").alias("element_id"),
        a.ELEMENT_LABEL_TXT.alias("element_label"),
        a.GRID_NAME_TXT.alias("grid_name"),
        a.GRID_ROW_DESC_TXT.alias("grid_row"),
        i.RESPONSE_KIND.alias("response_kind"),
        F.coalesce(
            i.CANONICAL_TEXT_RESULT,
            i.RESPONSE_TEXT_TXT,
            a.RESPONSE_VALUE_TXT,
            a.STRING_RESPONSE_TXT,
        ).alias("response_value_text"),
        F.coalesce(
            i.CANONICAL_NUMERIC_RESULT,
            a.NUMERIC_RESPONSE_NBR,
        ).cast("decimal(38,10)").alias("response_value_number"),
        # INSR_W2 W2.07 (J4, L2051): a time-only answer keeps its time in response_value_time.
        F.when(~time_only(F.coalesce(i.CANONICAL_DATE_RESULT, i.RESPONSE_DT_TM)),
               F.coalesce(i.CANONICAL_DATE_RESULT, i.RESPONSE_DT_TM)).alias("response_value_datetime"),
        time_part(F.coalesce(i.CANONICAL_DATE_RESULT, i.RESPONSE_DT_TM)).alias("response_value_time"),
        F.when(
            i.RESPONSE_NOMENCLATURE_ID.isNotNull(),
            F.lit("urn:cerner:nomenclature"),
        ).when(
            i.RESPONSE_CODE_VALUE_ID.isNotNull(),
            F.lit("urn:cerner:code_value"),
        ).alias("response_coding_system"),
        F.coalesce(
            i.RESPONSE_NOMENCLATURE_ID,
            i.RESPONSE_CODE_VALUE_ID,
        ).cast("string").alias("response_coding_code"),
        i.RESPONSE_TEXT_TXT.alias("response_coding_display"),
        i.QUESTION_CONCEPT_ID.cast("string").alias("question_concept_id"),
        i.VALUE_CONCEPT_ID.cast("string").alias("value_concept_id"),
        # INSR_W2 W2.04 (L2763): canonical unit, else the PowerForm (DTA) default unit; unit_source says which.
        F.coalesce(i.UNIT_CONCEPT_ID, i.CANONICAL_UNIT_CONCEPT_ID, i.POWERFORM_UNIT_CONCEPT_ID)
        .cast("string").alias("unit_concept_id"),
        F.when(i.UNIT_CONCEPT_ID.isNotNull(), F.coalesce(i.UNIT_MAPPING_SOURCE, F.lit("item_unit")))
         .when(i.CANONICAL_UNIT_CONCEPT_ID.isNotNull(), F.lit("canonical_unit"))
         .when(i.POWERFORM_UNIT_CONCEPT_ID.isNotNull(), F.lit("powerform_dta_default"))
         .alias("unit_source"),
        i.VALUE_MAPPING_SOURCE.alias("value_mapping_source"),
        i.POWERFORM_MAPPING_RULE_ID.cast("string").alias(
            "powerform_mapping_rule_id"
        ),
        i.POWERFORM_MAPPING_VERSION.cast("string").alias(
            "powerform_mapping_version"
        ),
        i.CANONICAL_VALUE_MAPPING_RULE_ID.cast("string").alias(
            "canonical_value_mapping_rule_id"
        ),
        i.CANONICAL_MATCH_STATUS.alias("canonical_match_status"),
        v.Pregnancy_ID.cast("string").alias("pregnancy_id"),
        v.PregnancyMatchMethod.alias("pregnancy_match_method"),
        (a.ACTIVE_IND == 1).alias("active_ind"),
        i.SOURCE_PRESENT_IND.alias("source_present_ind"),
        i.CANONICAL_SOURCE_DELETED_IND.alias("source_deleted_ind"),
        F.col("slug_config.form_slug").alias("form_slug"),
        F.col("element_config.configured_column").alias("configured_column"),
        a.ADC_UPDT.alias("loaded_at"),
    )


@materialized_view(
    name=_n("journey_clinical.form_response"),
    refresh_policy="auto",
)
def form_response():
    # Build the declared dataset: One PowerForm response row per form instance (contract v2
    # child of clinical_form). form_slug and configured_column identify the reviewed instrument
    # field, when any, and selection_rank/is_selected expose the production selection.
    primitive = spark.read.table(
        _n("journey_clinical._form_response_primitive")
    ).alias("p")
    selection = spark.read.table(
        _n("journey_clinical._form_response_selection")
    ).alias("s")
    joined = primitive.join(
        selection,
        (
            (F.col("p.dcp_forms_activity_id") == F.col("s.dcp_forms_activity_id"))
            & F.col("p.response_id").eqNullSafe(F.col("s.response_id"))
            & F.col("p.configured_column").eqNullSafe(
                F.col("s.configured_column")
            )
        ),
        "left",
    )
    df = joined.select(
        *[
            F.col(f"p.{column}").alias(column)
            for column in FORM_RESPONSE_BASE_COLUMNS
            if column != "loaded_at"
        ],
        F.col("s.selection_rank").alias("selection_rank"),
        F.coalesce(F.col("s.is_selected"), F.lit(False)).alias("is_selected"),
        F.col("p.loaded_at").alias("loaded_at"),
    )
    question_text = F.concat_ws("|", F.coalesce("form_slug", F.lit("unconfigured")),
                                F.coalesce("section", F.lit("")), F.coalesce("element_label", F.lit("")))
    answer_text = F.coalesce("response_coding_display", "response_value_text")
    answer_norm = F.lower(F.trim(F.regexp_replace(answer_text, r"\s+", " ")))
    contextual_answer_concept = (
        F.when(answer_norm == "yes", F.col("question_concept_id").cast("bigint"))
         .when(answer_norm == "no", F.lit(None).cast("bigint"))
         .otherwise(F.col("value_concept_id").cast("bigint"))
    )
    df = _s3b_form_axis_from_omop(
        df, "question", "urn:barts:form-question",
        F.sha2(question_text, 256), F.col("element_label"), F.col("question_concept_id"),
        "question has no validated standard concept",
    )
    df = _s3b_form_axis_from_omop(
        df, "answer", "urn:barts:form-answer",
        F.sha2(F.concat_ws("|", question_text, F.coalesce(answer_text, F.lit(""))), 256),
        answer_text, contextual_answer_concept,
        "negative, numeric, datetime, blank or uncoded response retained as data",
    )
    df = _s3b_form_axis_from_omop(
        df, "unit", "urn:barts:form-unit", F.col("unit_concept_id").cast("string"),
        F.col("unit_concept_id").cast("string"), F.col("unit_concept_id"),
        "response has no validated standard unit concept",
    )
    return (df.withColumn("promoted_to_table",
                          F.when(F.col("configured_column").isNotNull(),
                                 F.concat(F.lit("clinical_form_"), F.col("form_slug"))))
              .withColumn("promoted_to_row_id",
                          F.when(F.col("configured_column").isNotNull(), F.col("patient_event_key"))))


# Static contract assertions: these execute while the pipeline source is loaded, without
# triggering Spark actions or imperative writes.
assert len(FORM_RESPONSE_BASE_COLUMNS) == len(set(FORM_RESPONSE_BASE_COLUMNS))


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_form_response_primitive': {'columns': {'active_ind': 'True when the source mill_form_activity '
                                                                 'response is active.',
                                                   'canonical_match_status': 'Canonical mapping match '
                                                                             'status.',
                                                   'canonical_value_mapping_rule_id': 'Rule identifier used '
                                                                                      'for canonical value '
                                                                                      'mapping.',
                                                   'configured_column': 'Instrument column this response can '
                                                                        'populate; NULL means residual or '
                                                                        'unconfigured.',
                                                   'dcp_forms_activity_id': 'Millennium '
                                                                            'DCP_FORMS_ACTIVITY_ID; native '
                                                                            'parent key joining '
                                                                            'clinical_form.',
                                                   'element_id': 'Source PowerForm DCP_INPUT_REF_ID.',
                                                   'element_label': 'Source PowerForm element label used by '
                                                                    'the reviewed form configuration.',
                                                   'form_slug': 'Configured instrument slug; NULL when the '
                                                                'form is outside the 30 reviewed '
                                                                'instruments.',
                                                   'grid_name': 'Source grid name when the response belongs '
                                                                'to a grid.',
                                                   'grid_row': 'Source grid-row description when supplied.',
                                                   'loaded_at': 'Bronze material-change timestamp for the '
                                                                'source response.',
                                                   'patient_event_key': 'Deterministic SHA-256 key of the '
                                                                        'parent PowerForm instance.',
                                                   'powerform_mapping_rule_id': 'Governed PowerForm '
                                                                                'mapping-rule identifier.',
                                                   'powerform_mapping_version': 'Version of the governed '
                                                                                'PowerForm mapping rules.',
                                                   'pregnancy_id': 'Pregnancy episode linked to the source '
                                                                   'response when available.',
                                                   'pregnancy_match_method': 'Method used to link the source '
                                                                             'response to pregnancy.',
                                                   'question_concept_id': 'Mapped question concept '
                                                                          'identifier.',
                                                   'response_coding_code': 'Source nomenclature or '
                                                                           'code-value identifier.',
                                                   'response_coding_display': 'Source display attached to '
                                                                              'the coded response.',
                                                   'response_coding_system': 'Cerner nomenclature or '
                                                                             'code-value namespace for a '
                                                                             'coded response.',
                                                   'response_id': 'Millennium DOC_RESPONSE_KEY; native '
                                                                  'identifier of the response row.',
                                                   'response_kind': 'Canonical response kind from '
                                                                    'map_powerform_assessment_item.',
                                                   'response_value_datetime': 'Canonical or source datetime '
                                                                              'response.',
                                                   'response_value_number': 'Canonical or source numeric '
                                                                            'response as DECIMAL(38,10).',
                                                   'response_value_text': 'Canonical or source textual '
                                                                          'response, preserving source '
                                                                          'fallback order.',
                                                   'response_value_time': 'Time of day (HH:mm:ss) for an '
                                                                          'answer recorded as a time only '
                                                                          '(Millennium placeholder date '
                                                                          '2000-01-01); '
                                                                          'response_value_datetime is then '
                                                                          'NULL. INSR_W2 W2.07.',
                                                   'section': 'Source PowerForm section display.',
                                                   'section_id': 'Source PowerForm SECTION_REF_ID.',
                                                   'sequence': 'Source response sequence, defaulted to zero '
                                                               'when absent.',
                                                   'source_deleted_ind': 'Whether canonical mapping '
                                                                         'identifies the source response as '
                                                                         'deleted.',
                                                   'source_present_ind': 'Whether the mapped response '
                                                                         'remains present in the source.',
                                                   'unit_concept_id': 'Mapped response-unit concept '
                                                                      'identifier.',
                                                   'unit_source': 'Where unit_concept_id came from: the item '
                                                                  'unit mapping source, canonical_unit, or '
                                                                  'powerform_dta_default (the PowerForm DTA '
                                                                  'default unit); NULL when no unit. INSR_W2 '
                                                                  'W2.04.',
                                                   'value_concept_id': 'Mapped response-value concept '
                                                                       'identifier.',
                                                   'value_mapping_source': 'Method or source that produced '
                                                                           'the mapped response value.'},
                                       'table': 'Internal plain response-grain projection used by selection, '
                                                'ambiguity evidence and the public clinical_form_response '
                                                'child.'},
 'clinical_form_response': {'columns': {'active_ind': 'True when the source mill_form_activity response is '
                                                      'active.',
                                        'answer_map_candidate_count': 'Governed S3b form answer mapping '
                                                                      'provenance.',
                                        'answer_map_candidate_set_id': 'Governed S3b form answer mapping '
                                                                       'provenance.',
                                        'answer_map_competing_count': 'Governed S3b form answer mapping '
                                                                      'provenance.',
                                        'answer_map_component_count': 'Governed S3b form answer mapping '
                                                                      'provenance.',
                                        'answer_map_component_index': 'Governed S3b form answer mapping '
                                                                      'provenance.',
                                        'answer_map_confidence': 'Governed S3b form answer mapping '
                                                                 'provenance.',
                                        'answer_map_cosine': 'Governed S3b form answer mapping provenance.',
                                        'answer_map_method': 'Governed S3b form answer mapping provenance.',
                                        'answer_map_rollup_levels': 'Governed S3b form answer mapping '
                                                                    'provenance.',
                                        'answer_map_rule_id': 'Governed S3b form answer mapping provenance.',
                                        'answer_map_scoring_model': 'Governed S3b form answer mapping '
                                                                    'provenance.',
                                        'answer_map_status': 'Governed S3b form answer mapping provenance.',
                                        'answer_map_status_reason': 'Governed S3b form answer mapping '
                                                                    'provenance.',
                                        'answer_map_version': 'Governed S3b form answer mapping provenance.',
                                        'answer_omop_concept_id': 'Governed S3b form answer mapping '
                                                                  'provenance.',
                                        'answer_omop_source_concept_id': 'Governed S3b form answer mapping '
                                                                         'provenance.',
                                        'answer_snomed_code': 'Governed S3b form answer mapping provenance.',
                                        'answer_snomed_display': 'Governed S3b form answer mapping '
                                                                 'provenance.',
                                        'answer_source_code': 'Governed S3b form answer mapping provenance.',
                                        'answer_source_display': 'Governed S3b form answer mapping '
                                                                 'provenance.',
                                        'answer_source_system': 'Governed S3b form answer mapping '
                                                                'provenance.',
                                        'answer_target_code': 'Governed S3b form answer mapping provenance.',
                                        'answer_target_display': 'Governed S3b form answer mapping '
                                                                 'provenance.',
                                        'answer_target_system': 'Governed S3b form answer mapping '
                                                                'provenance.',
                                        'canonical_match_status': 'Canonical mapping match status.',
                                        'canonical_value_mapping_rule_id': 'Rule identifier used for '
                                                                           'canonical value mapping.',
                                        'configured_column': 'Instrument column this response can populate; '
                                                             'NULL means residual or unconfigured.',
                                        'dcp_forms_activity_id': 'Millennium DCP_FORMS_ACTIVITY_ID; native '
                                                                 'parent key joining clinical_form.',
                                        'element_id': 'Source PowerForm DCP_INPUT_REF_ID.',
                                        'element_label': 'Source PowerForm element label used by the '
                                                         'reviewed form configuration.',
                                        'form_slug': 'Configured instrument slug; NULL when the form is '
                                                     'outside the 30 reviewed instruments.',
                                        'grid_name': 'Source grid name when the response belongs to a grid.',
                                        'grid_row': 'Source grid-row description when supplied.',
                                        'is_selected': 'True for rank 1 of a single-valued element or every '
                                                       'active response of a multi-valued element; false for '
                                                       'residuals.',
                                        'loaded_at': "mill_form_activity.ADC_UPDT for this response's "
                                                     'activity row, carried unchanged through the response '
                                                     'primitive and public selection join. Joined '
                                                     'assessment-item, assessment, VTE and '
                                                     'form-configuration clocks do not contribute; the VTE '
                                                     'Source_ADC_UPDT field is selected into the input but '
                                                     "not used here. This is not the parent clinical_form's "
                                                     'grouped greatest-clock expression, a maximum across '
                                                     'responses or the current Silver refresh time.',
                                        'patient_event_key': 'Deterministic SHA-256 key of the parent '
                                                             'PowerForm instance.',
                                        'powerform_mapping_rule_id': 'Governed PowerForm mapping-rule '
                                                                     'identifier.',
                                        'powerform_mapping_version': 'Version of the governed PowerForm '
                                                                     'mapping rules.',
                                        'pregnancy_id': 'Pregnancy episode linked to the source response '
                                                        'when available.',
                                        'pregnancy_match_method': 'Method used to link the source response '
                                                                  'to pregnancy.',
                                        'promoted_to_row_id': 'Patient-event key of the dedicated instrument '
                                                              'row; null for residual responses.',
                                        'promoted_to_table': 'Dedicated instrument product carrying this '
                                                             'configured response, when applicable.',
                                        'question_concept_id': 'Mapped question concept identifier.',
                                        'question_map_candidate_count': 'Governed S3b form question mapping '
                                                                        'provenance.',
                                        'question_map_candidate_set_id': 'Governed S3b form question mapping '
                                                                         'provenance.',
                                        'question_map_competing_count': 'Governed S3b form question mapping '
                                                                        'provenance.',
                                        'question_map_component_count': 'Governed S3b form question mapping '
                                                                        'provenance.',
                                        'question_map_component_index': 'Governed S3b form question mapping '
                                                                        'provenance.',
                                        'question_map_confidence': 'Governed S3b form question mapping '
                                                                   'provenance.',
                                        'question_map_cosine': 'Governed S3b form question mapping '
                                                               'provenance.',
                                        'question_map_method': 'Governed S3b form question mapping '
                                                               'provenance.',
                                        'question_map_rollup_levels': 'Governed S3b form question mapping '
                                                                      'provenance.',
                                        'question_map_rule_id': 'Governed S3b form question mapping '
                                                                'provenance.',
                                        'question_map_scoring_model': 'Governed S3b form question mapping '
                                                                      'provenance.',
                                        'question_map_status': 'Governed S3b form question mapping '
                                                               'provenance.',
                                        'question_map_status_reason': 'Governed S3b form question mapping '
                                                                      'provenance.',
                                        'question_map_version': 'Governed S3b form question mapping '
                                                                'provenance.',
                                        'question_omop_concept_id': 'Governed S3b form question mapping '
                                                                    'provenance.',
                                        'question_omop_source_concept_id': 'Governed S3b form question '
                                                                           'mapping provenance.',
                                        'question_snomed_code': 'Governed S3b form question mapping '
                                                                'provenance.',
                                        'question_snomed_display': 'Governed S3b form question mapping '
                                                                   'provenance.',
                                        'question_source_code': 'Governed S3b form question mapping '
                                                                'provenance.',
                                        'question_source_display': 'Governed S3b form question mapping '
                                                                   'provenance.',
                                        'question_source_system': 'Governed S3b form question mapping '
                                                                  'provenance.',
                                        'question_target_code': 'Governed S3b form question mapping '
                                                                'provenance.',
                                        'question_target_display': 'Governed S3b form question mapping '
                                                                   'provenance.',
                                        'question_target_system': 'Governed S3b form question mapping '
                                                                  'provenance.',
                                        'response_coding_code': 'Source nomenclature or code-value '
                                                                'identifier.',
                                        'response_coding_display': 'Source display attached to the coded '
                                                                   'response.',
                                        'response_coding_system': 'Cerner nomenclature or code-value '
                                                                  'namespace for a coded response.',
                                        'response_id': 'Millennium DOC_RESPONSE_KEY; native identifier of '
                                                       'the response row.',
                                        'response_kind': 'Canonical response kind from '
                                                         'map_powerform_assessment_item.',
                                        'response_value_datetime': 'Canonical or source datetime response.',
                                        'response_value_number': 'Canonical or source numeric response as '
                                                                 'DECIMAL(38,10).',
                                        'response_value_text': 'Canonical or source textual response, '
                                                               'preserving source fallback order.',
                                        'response_value_time': 'Time of day (HH:mm:ss) for an answer '
                                                               'recorded as a time only (Millennium '
                                                               'placeholder date 2000-01-01); '
                                                               'response_value_datetime is then NULL. '
                                                               'INSR_W2 W2.07.',
                                        'section': 'Source PowerForm section display.',
                                        'section_id': 'Source PowerForm SECTION_REF_ID.',
                                        'selection_rank': 'Production ranking within form instance and '
                                                          'configured column: active, valued, latest '
                                                          'sequence, then latest response ID.',
                                        'sequence': 'Source response sequence, defaulted to zero when '
                                                    'absent.',
                                        'source_deleted_ind': 'Whether canonical mapping identifies the '
                                                              'source response as deleted.',
                                        'source_present_ind': 'Whether the mapped response remains present '
                                                              'in the source.',
                                        'unit_concept_id': 'Mapped response-unit concept identifier.',
                                        'unit_map_candidate_count': 'Governed S3b form unit mapping '
                                                                    'provenance.',
                                        'unit_map_candidate_set_id': 'Governed S3b form unit mapping '
                                                                     'provenance.',
                                        'unit_map_competing_count': 'Governed S3b form unit mapping '
                                                                    'provenance.',
                                        'unit_map_component_count': 'Governed S3b form unit mapping '
                                                                    'provenance.',
                                        'unit_map_component_index': 'Governed S3b form unit mapping '
                                                                    'provenance.',
                                        'unit_map_confidence': 'Governed S3b form unit mapping provenance.',
                                        'unit_map_cosine': 'Governed S3b form unit mapping provenance.',
                                        'unit_map_method': 'Governed S3b form unit mapping provenance.',
                                        'unit_map_rollup_levels': 'Governed S3b form unit mapping '
                                                                  'provenance.',
                                        'unit_map_rule_id': 'Governed S3b form unit mapping provenance.',
                                        'unit_map_scoring_model': 'Governed S3b form unit mapping '
                                                                  'provenance.',
                                        'unit_map_status': 'Governed S3b form unit mapping provenance.',
                                        'unit_map_status_reason': 'Governed S3b form unit mapping '
                                                                  'provenance.',
                                        'unit_map_version': 'Governed S3b form unit mapping provenance.',
                                        'unit_omop_concept_id': 'Governed S3b form unit mapping provenance.',
                                        'unit_omop_source_concept_id': 'Governed S3b form unit mapping '
                                                                       'provenance.',
                                        'unit_snomed_code': 'Governed S3b form unit mapping provenance.',
                                        'unit_snomed_display': 'Governed S3b form unit mapping provenance.',
                                        'unit_source': 'Where unit_concept_id came from: the item unit '
                                                       'mapping source, canonical_unit, or '
                                                       'powerform_dta_default (the PowerForm DTA default '
                                                       'unit); NULL when no unit. INSR_W2 W2.04.',
                                        'unit_source_code': 'Governed S3b form unit mapping provenance.',
                                        'unit_source_display': 'Governed S3b form unit mapping provenance.',
                                        'unit_source_system': 'Governed S3b form unit mapping provenance.',
                                        'unit_target_code': 'Governed S3b form unit mapping provenance.',
                                        'unit_target_display': 'Governed S3b form unit mapping provenance.',
                                        'unit_target_system': 'Governed S3b form unit mapping provenance.',
                                        'value_concept_id': 'Mapped response-value concept identifier.',
                                        'value_mapping_source': 'Method or source that produced the mapped '
                                                                'response value.'},
                            'table': 'One PowerForm response row per form instance (contract v2 child of '
                                     'clinical_form). form_slug and configured_column identify the reviewed '
                                     'instrument field, when any, and selection_rank/is_selected expose the '
                                     'production selection.'}}

# INSR_R4_SILVER_V1 (child): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import ParentInputs, with_parent_inputs  # noqa: E402
R4_PARENT_INPUTS = ParentInputs(parent='clinical_form', dataset='_clinical_form_parent_inputs', keys=('patient_event_key',), inputs=('identity_status', 'person_id'), admit_sql="coalesce(((identity_status = 'resolved')), false)", name=_n('journey_clinical._form_parent_inputs'), parent_name=_n('journey_clinical.form'))
SPECS = with_parent_inputs(SPECS, 'clinical_form_response', R4_PARENT_INPUTS)
