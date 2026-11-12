"""Silver activity.theatre_implant: 1 dataset(s).

- datasets: reference_theatre_implant
- moved verbatim from: 4_silver_care_pathways:4802, 4_silver_care_pathways:4832, 4_silver_care_pathways:4835, 4_silver_care_pathways:4840, 4_silver_care_pathways:4877

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import SRC_THEATRE_CASE
from silver_tables.activity._shared import _theatre_parent_keys


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reference / theatre implant
SRC_THEATRE_IMPLANT = "4_prod.bronze.map_theatre_implant_log"


THEATRE_IMPLANT_LIFECYCLE_FIELDS = [
]


THEATRE_IMPLANT_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_implant():
    # Assemble theatre implant rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_THEATRE_IMPLANT).alias("s")
    d = s.join(_theatre_parent_keys(), s.SURG_CASE_ID == F.col("_parent_surg_case_id"), "left")
    # INSR_W2 W2.06 (L5267): the implant log carries no date; the implant happened during its theatre case.
    case_time = read_source(SRC_THEATRE_CASE).groupBy(F.col("SURG_CASE_ID").alias("_it_case")).agg(
        F.max("SURG_START_DT_TM").alias("_it_surg_start"), F.max("CHECKIN_DT_TM").alias("_it_checkin"),
        F.max("SCHED_START_DT_TM").alias("_it_sched"))
    d = d.join(case_time, s.SURG_CASE_ID == F.col("_it_case"), "left")
    # contract v2: rename deterministic theatre identities as keys and publish native practitioner, person, and encounter ids
    return d.select(
        stable_id("theatre_implant:surginet", s.IMPLANT_LOG_ST_ID).alias("theatre_implant_key"),
        s.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        F.when(F.col("_parent_surg_case_id").isNotNull(), F.lit("linked"))
         .otherwise(F.lit("orphan")).alias("case_link_status"),
        s.ITEM_ID.alias("item_id"), s.MANUFACTURER.alias("manufacturer"),
        s.MODEL_NUMBER.alias("model_number"), s.CATALOG_NUMBER.alias("catalog_number"),
        s.SERIAL_NUMBER.alias("serial_number"), s.LOT_NUMBER.alias("lot_number"),
        s.BATCH_NUMBER.alias("batch_number"), s.IMPLANT_SITE.alias("implant_site"),
        s.IMPLANT_SIZE.alias("implant_size"), s.QUANTITY.alias("quantity"),
        s.EXP_DATE.alias("expiry_date"), s.FREE_TEXT_ITEM_DESC.alias("free_text_item_desc"),
        s.IMPLANTED_BY_ID.cast("bigint").alias("implanted_by_practitioner_id"),
        s.MILL_IMPLANT_EVENT_ID.cast("string").alias("mill_implant_event_id"),
        s.IMPLANT_LINK_METHOD.alias("implant_link_method"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        F.coalesce("_it_surg_start", "_it_checkin", "_it_sched").alias("implant_datetime"),  # INSR_W2 W2.06
        F.when(F.col("_it_surg_start").isNotNull(), F.lit("case_surgery_start"))
         .when(F.col("_it_checkin").isNotNull(), F.lit("case_checkin"))
         .when(F.col("_it_sched").isNotNull(), F.lit("case_scheduled_start")).alias("implant_datetime_basis"),
        s.DOC_TYPE_DESCRIPTION.alias("document_type_desc"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"), F.lit(SRC_THEATRE_IMPLANT).alias("_source_table"),
        s.IMPLANT_LOG_ST_ID.cast("string").alias("_source_row_id"),
    )


@materialized_view(
    name=_n("journey_reference.theatre_implant"),
    cluster_by=["person_id"], refresh_policy="auto",
)
def theatre_implant():
    # Build the declared dataset: SurgiNet theatre implant log; serial/lot/batch identifiers are
    # IG-sensitive.
    return _lifecycle_source_theatre_implant().drop(*THEATRE_IMPLANT_LIFECYCLE_FIELDS, *THEATRE_IMPLANT_RETIRED_COLUMNS)


VALUE_POLICIES = {
    'reference_theatre_implant': ValuePolicy(table='reference_theatre_implant', enabled=True, zero_as_null=('implanted_by_practitioner_id', 'item_id')),
}


WRAP_POLICY = {'reference_theatre_implant'}


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'reference_theatre_implant': {'columns': {'_source_row_id': 'S7 procedure implant feeder; retained as a '
                                                             'source-faithful bronze input or Journey '
                                                             'standard-block field.',
                                           '_source_system': 'Value describing source system for the theatre '
                                                             'implant record. It is produced by the silver '
                                                             'transformation and has no direct bronze-column '
                                                             'lineage entry. Whitespace and source sentinel '
                                                             'text are retained unless the pipeline '
                                                             'explicitly normalizes them; null means no '
                                                             'value was supplied.',
                                           '_source_table': 'Value describing source table for the theatre '
                                                            'implant record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'batch_number': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                           'case_link_status': 'S14 theatre feeder and Journey standard '
                                                               'block; retained as a source-faithful bronze '
                                                               'input or Journey standard-block field.',
                                           'catalog_number': 'S7 procedure implant feeder; retained as a '
                                                             'source-faithful bronze input or Journey '
                                                             'standard-block field.',
                                           'document_type_desc': 'S7 procedure implant feeder; retained as a '
                                                                 'source-faithful bronze input or Journey '
                                                                 'standard-block field.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT.',
                                           'expiry_date': 'S7 procedure implant feeder; retained as a '
                                                          'source-faithful bronze input or Journey '
                                                          'standard-block field.',
                                           'free_text_item_desc': 'S7 procedure implant feeder; retained as '
                                                                  'a source-faithful bronze input or Journey '
                                                                  'standard-block field.',
                                           'implant_datetime': 'Time of the theatre case the implant was '
                                                               'logged against (surgery start, else '
                                                               'check-in, else scheduled start); the implant '
                                                               'log has no own date. INSR_W2 W2.06 (L5267).',
                                           'implant_datetime_basis': 'case_surgery_start, case_checkin or '
                                                                     'case_scheduled_start. INSR_W2 W2.06.',
                                           'implant_link_method': 'S7 crosswalk provenance: SERIAL_UNIQUE, '
                                                                  'SERIAL_AMBIGUOUS or NONE.',
                                           'implant_site': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                           'implant_size': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                           'implanted_by_practitioner_id': 'Native Millennium personnel '
                                                                           'PERSON_ID as BIGINT for the '
                                                                           'implanter.',
                                           'item_id': 'S7 procedure implant feeder; retained as a '
                                                      'source-faithful bronze input or Journey '
                                                      'standard-block field.',
                                           'loaded_at': 'map_theatre_implant_log.ADC_UPDT carried unchanged '
                                                        'for the implant-log row. The parent-case existence '
                                                        'join adds no timestamp; this is bronze ingestion '
                                                        'provenance, not implant/expiry time or Silver '
                                                        'refresh time.',
                                           'lot_number': 'S7 implant traceability input retained because '
                                                         'most SurgiNet rows have no Mill counterpart.',
                                           'manufacturer': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                           'mill_implant_event_id': 'S7 identity crosswalk to '
                                                                    'map_implant_details.EVENT_ID when '
                                                                    'person+serial is unique.',
                                           'model_number': 'S7 procedure implant feeder; retained as a '
                                                           'source-faithful bronze input or Journey '
                                                           'standard-block field.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT.',
                                           'quantity': 'S7 procedure implant feeder; retained as a '
                                                       'source-faithful bronze input or Journey '
                                                       'standard-block field.',
                                           'serial_number': 'S7 implant traceability and identity-crosswalk '
                                                            'input.',
                                           'theatre_case_key': 'Source-preserved theatre case key.',
                                           'theatre_implant_key': 'Deterministic SHA-256 key for the theatre '
                                                                  'implant row.'},
                               'table': 'SurgiNet theatre implant log; serial/lot/batch identifiers are '
                                        'IG-sensitive.'}}
