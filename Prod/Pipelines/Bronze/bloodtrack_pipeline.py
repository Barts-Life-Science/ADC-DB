# Databricks notebook source
# MAGIC %md
# MAGIC # BloodTrack Transfusion Bronze Pipeline (source-only validation)
# MAGIC
# MAGIC Two bronze tables from one full-snapshot source:
# MAGIC - map_bloodtrack_transaction: one row per BloodTrack scan, including failed/safety-check attempts.
# MAGIC - map_bloodtrack_transfusion: one row per unit-recipient episode, with begin/end scans merged
# MAGIC   and incomplete or ambiguous episodes retained under an explicit status.
# MAGIC
# MAGIC The pipeline also maintains curated product-group and blood-group maps. It reuses the shared
# MAGIC registry engine for deterministic person linkage, PII removal, row-hash-gated full-snapshot
# MAGIC reconciliation, soft deletion, comments, and audit columns. Validation is self-contained to
# MAGIC raw, lookup, shared-helper, and target tables.
# MAGIC
# MAGIC Widgets: target_schema, pipeline_run_id, expect_idempotent (from _registry_common),
# MAGIC event_time_tolerance_seconds (default 900), publish_operator_username (default false).

# COMMAND ----------

# MAGIC %run ./_registry_common

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

for _n, _d in {
    "event_time_tolerance_seconds": "900",
    "publish_operator_username": "false",
}.items():
    try:
        dbutils.widgets.get(_n)
    except Exception:
        dbutils.widgets.text(_n, _d)

EVENT_TIME_TOLERANCE_SECONDS = int(
    dbutils.widgets.get("event_time_tolerance_seconds") or 900
)
PUBLISH_OPERATOR_USERNAME = (
    str(dbutils.widgets.get("publish_operator_username")).strip().lower()
    in {"1", "true", "yes", "y"}
)

SOURCE_TABLE = f"{RAW}.bloodtrack_transfusion"
MILL_PERSON = f"{RAW}.mill_person"
TXN_TARGET = f"{TARGET_SCHEMA}.map_bloodtrack_transaction"
EPISODE_TARGET = f"{TARGET_SCHEMA}.map_bloodtrack_transfusion"
PRODUCT_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.bloodtrack_product_group_map"
BLOODGROUP_MAP_TABLE = f"{bronze_lookup_schema(TARGET_SCHEMA)}.bloodtrack_blood_group_map"

IDENTIFIER_DROPS = [
    "PatientName",
    "PatientBirthDate",
    "PatientGender",
    "HealthNumber",
    "PatientID",
]
STAFF_DROPS = [
    "FirstName",
    "LastName",
    "SecondaryUserName",
    "SecondaryLastName",
    "SecondaryFirstName",
    "PorterUserName",
]
LOW_VALUE_DROPS = [
    "RowID",
    "MaxRowID",
    "Ctrl_ID",
    "ResponseCodeID",
    "ConfigStatus",
    "RequestDate",
    "StorageTime",
    "RoomTempTransportTime",
    "RefrigeratedTransportTime",
    "TransportType",
    "TransportDevice",
    "Details",
    "Duration",
]
# PatientNumber and UserName are handled specially after linkage/governance checks.
DROP_COLUMNS = IDENTIFIER_DROPS + STAFF_DROPS + LOW_VALUE_DROPS

# All concept IDs were verified as valid standard SNOMED concepts on 2026-07-20.
PRODUCT_GROUP_SEED = [
    (
        "Red Cells",
        4129597,
        "Human red blood cells, blood product",
        4022173,
        "Transfusion of red blood cells",
    ),
    (
        "Platelets",
        4049693,
        "Human platelet product",
        4130829,
        "Platelet transfusion",
    ),
    (
        "Fresh Frozen Plasma",
        4223728,
        "Fresh frozen plasma",
        4022171,
        "Transfusion of fresh frozen plasma",
    ),
    (
        "Cryoprecipitate",
        4106319,
        "Cryoprecipitate",
        4023918,
        "Transfusion of cryoprecipitate",
    ),
    (
        "Leucocytes",
        4054394,
        "Granulocytes, human, blood product",
        4145204,
        "Transfusion of granulocytes",
    ),
]
BLOOD_GROUP_SEED = [
    ("A Pos", 4082948, "Blood group A Rh(D) positive"),
    ("A Neg", 4080397, "Blood group A Rh(D) negative"),
    ("B Pos", 4175555, "Blood group B Rh(D) positive"),
    ("B Neg", 4080398, "Blood group B Rh(D) negative"),
    ("AB Pos", 4080396, "Blood group AB Rh(D) positive"),
    ("AB Neg", 4082949, "Blood group AB Rh(D) negative"),
    ("O Pos", 4080395, "Blood group O Rh(D) positive"),
    ("O Neg", 4082947, "Blood group O Rh(D) negative"),
]
REQ_IRRADIATED_CONCEPT = (
    36675662,
    "Requires irradiated blood component",
)
REQ_CMV_NEG_CONCEPT = (
    42689620,
    "Requires cytomegalovirus negative blood components",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source preflight and schema-drift guard

# COMMAND ----------

ensure_target_schema()
for _table in (SOURCE_TABLE, PERSON_ALIAS, OMOP_CONCEPT, MILL_PERSON):
    assert table_exists(_table), f"Missing source: {_table}"

_source_history = (
    spark.sql(f"DESCRIBE HISTORY {qname(SOURCE_TABLE)}")
    .select("version", "timestamp")
    .orderBy(F.col("version").desc())
    .first()
)
def _bloodtrack_hardening_sql_identifier(table_name: str) -> str:
    return '.'.join('`' + part.replace('`', '``') + '`' for part in table_name.split('.'))

def _bloodtrack_hardening_snapshot_fallback_allowed(exc: BaseException) -> bool:
    messages = []
    current = exc
    while current is not None and len(messages) < 8:
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    combined = ' '.join(messages)
    return any(marker in combined for marker in (
        'delta_unsupported_time_travel_beyond_deleted_file_retention_duration',
        'time travel beyond delta.deletedfileretentionduration',
        'delta_log_file_not_found',
        'unable to retrieve the delta log files to construct table version',
        'delta log file not found',
    ))

def _bloodtrack_hardening_read_pinned_snapshot(table_name: str, version: int):
    requested_version = int(version)
    try:
        pinned = (
            spark.read.format('delta')
            .option('versionAsOf', requested_version)
            .table(table_name)
        )
        # Spark Connect defers Delta-log validation until analysis/action.
        _ = pinned.schema
        pinned.limit(1).collect()
        return pinned
    except Exception as exc:
        if not _bloodtrack_hardening_snapshot_fallback_allowed(exc):
            raise
        row = (
            spark.sql('DESCRIBE HISTORY ' + _bloodtrack_hardening_sql_identifier(table_name) + ' LIMIT 1')
            .select('version')
            .first()
        )
        latest_version = None if row is None else int(row['version'])
        if latest_version != requested_version:
            raise
        print(
            '[WARN] Explicit time travel is outside retained Delta history for '
            + table_name + ' at current version ' + str(requested_version)
            + '; reading the same current snapshot without versionAsOf.'
        )
        current_snapshot = spark.table(table_name)
        _ = current_snapshot.schema
        return current_snapshot

SOURCE_DELTA_VERSION = int(_source_history["version"])
raw = _bloodtrack_hardening_read_pinned_snapshot(
    SOURCE_TABLE,
    SOURCE_DELTA_VERSION,
)

_g = raw.agg(
    F.count("*").alias("rows"),
    F.countDistinct("TransactionID").alias("txn"),
    F.sum(
        F.when(F.col("TransactionID").isNull(), 1).otherwise(0)
    ).alias("null_txn"),
    F.sum(
        F.when(
            ~F.col("RowID").eqNullSafe(F.col("TransactionID")),
            1,
        ).otherwise(0)
    ).alias("rowid_ne"),
).first()
assert _g.rows == _g.txn and int(_g.null_txn or 0) == 0, (
    f"TransactionID not unique/non-null: {_g}"
)
assert int(_g.rowid_ne or 0) == 0, (
    f"RowID != TransactionID on {_g.rowid_ne} rows"
)

_steps = {
    row["Transaction1"]
    for row in raw.select("Transaction1").distinct().collect()
}
assert _steps <= {"Begin Transfusion", "End Transfusion"}, (
    f"Unexpected steps: {_steps}"
)

_required_columns = DROP_COLUMNS + [
    "PatientNumber",
    "UnitID",
    "UnitNumber",
    "Response",
    "RecordUpdatedDt",
    "ADC_UPDT",
]
_missing = [name for name in _required_columns if name not in raw.columns]
assert not _missing, f"Expected source columns absent (schema drift): {_missing}"

EMPTY_EXPECTED = [
    "RequestDate",
    "StorageTime",
    "RoomTempTransportTime",
    "RefrigeratedTransportTime",
    "TransportType",
    "TransportDevice",
    "PorterUserName",
    "SecondaryUserName",
    "SecondaryLastName",
    "SecondaryFirstName",
]
_now_populated = [
    name
    for name in EMPTY_EXPECTED
    if raw.where(
        F.col(name).isNotNull()
        & (F.trim(F.col(name).cast("string")) != "")
    )
    .limit(1)
    .count()
    > 0
]
if _now_populated:
    print(
        "[SCHEMA_DRIFT][WARN] previously-empty columns now populated "
        f"(review before publishing): {_now_populated}"
    )
print(
    f"[PREFLIGHT] source_version={SOURCE_DELTA_VERSION}, "
    f"rows={_g.rows}, txn_ids={_g.txn}, steps={sorted(_steps)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated code maps

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {qname(PRODUCT_MAP_TABLE)} (
  BLOOD_PRODUCT_GROUP STRING COMMENT 'BloodTrack BloodProductGroup value',
  PRODUCT_CONCEPT_ID BIGINT COMMENT 'Standard SNOMED product concept in the Device domain',
  PRODUCT_CONCEPT_NAME STRING,
  PROC_CONCEPT_ID BIGINT COMMENT 'Standard SNOMED Procedure concept for transfusion of this product',
  PROC_CONCEPT_NAME STRING,
  MAPPING_METHOD STRING COMMENT 'CURATED',
  MAPPING_STATUS STRING COMMENT 'APPROVED|PROPOSED|REJECTED; only APPROVED is applied',
  MAPPING_VERSION STRING,
  CURATED_BY STRING,
  CURATED_AT TIMESTAMP
) USING DELTA
COMMENT 'Curated BloodTrack product-group map to both a SNOMED product concept and a SNOMED transfusion procedure concept. ProductCode remains unmapped source data.'
"""
)
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {qname(BLOODGROUP_MAP_TABLE)} (
  BLOOD_GROUP_SOURCE_VALUE STRING COMMENT 'BloodTrack ABO/RhD string',
  SNOMED_CONCEPT_ID BIGINT COMMENT 'Standard SNOMED Condition-domain blood-group concept',
  SNOMED_CONCEPT_NAME STRING,
  MAPPING_METHOD STRING,
  MAPPING_STATUS STRING,
  MAPPING_VERSION STRING,
  CURATED_BY STRING,
  CURATED_AT TIMESTAMP
) USING DELTA
COMMENT 'Curated BloodTrack ABO/RhD map to standard SNOMED blood-group concepts, applied to unit and patient scan-time values.'
"""
)


def _seed(table, rows, business_cols):
    payload = [
        tuple(row) + ("CURATED", "PROPOSED", "1.0.0", None, None)
        for row in rows
    ]
    frame = spark.createDataFrame(payload, schema=spark.table(table).schema)
    condition = " AND ".join(
        f"t.{chr(96)}{name}{chr(96)} <=> s.{chr(96)}{name}{chr(96)}"
        for name in business_cols
    )
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(frame.alias("s"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


_seed(
    PRODUCT_MAP_TABLE,
    PRODUCT_GROUP_SEED,
    ["BLOOD_PRODUCT_GROUP"],
)
_seed(
    BLOODGROUP_MAP_TABLE,
    BLOOD_GROUP_SEED,
    ["BLOOD_GROUP_SOURCE_VALUE"],
)

for _table, _key in (
    (PRODUCT_MAP_TABLE, "BLOOD_PRODUCT_GROUP"),
    (BLOODGROUP_MAP_TABLE, "BLOOD_GROUP_SOURCE_VALUE"),
):
    _counts = spark.table(_table).agg(
        F.count("*").alias("rows"),
        F.countDistinct(_key).alias("keys"),
        F.sum(F.when(F.col(_key).isNull(), 1).otherwise(0)).alias("nulls"),
    ).first()
    assert (
        int(_counts.rows) == int(_counts.keys)
        and int(_counts.nulls or 0) == 0
    ), f"Curated map key violation in {_table}: {_counts}"

_c = spark.table(OMOP_CONCEPT).select(
    F.col("concept_id").cast("long").alias("cid"),
    "domain_id",
    "vocabulary_id",
    "standard_concept",
    "invalid_reason",
)


def _assert_valid(ids, allowed_domains, label):
    valid_ids = [int(value) for value in ids if value is not None]
    bad = (
        spark.createDataFrame([(value,) for value in valid_ids], "cid long")
        .join(_c, "cid", "left")
        .where(
            F.col("standard_concept").isNull()
            | (F.col("standard_concept") != "S")
            | (F.col("vocabulary_id") != "SNOMED")
            | F.col("invalid_reason").isNotNull()
            | ~F.col("domain_id").isin(*allowed_domains)
        )
        .count()
    )
    assert bad == 0, f"Invalid concepts in {label}: {bad}"


_assert_valid(
    [row[1] for row in PRODUCT_GROUP_SEED],
    ["Device"],
    "product_concept",
)
_assert_valid(
    [row[3] for row in PRODUCT_GROUP_SEED],
    ["Procedure"],
    "procedure_concept",
)
_assert_valid(
    [row[1] for row in BLOOD_GROUP_SEED],
    ["Condition"],
    "blood_group_concept",
)
_assert_valid(
    [REQ_IRRADIATED_CONCEPT[0], REQ_CMV_NEG_CONCEPT[0]],
    ["Observation"],
    "requirement_concept",
)
print(
    "[MAPS] seeded and concept-validated "
    f"product_rows={spark.table(PRODUCT_MAP_TABLE).count()}, "
    f"blood_group_rows={spark.table(BLOODGROUP_MAP_TABLE).count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction normalization and linkage

# COMMAND ----------


def parse_flags(df):
    details = F.upper(
        F.coalesce(F.col("BloodUnitDetails").cast("string"), F.lit(""))
    )
    requirements = F.upper(
        F.coalesce(F.col("PatientRequirements").cast("string"), F.lit(""))
    )
    return (
        df.withColumn(
            "UNIT_IS_LEUCOREDUCED",
            details.rlike(r"(^|\s)LR(\s|$)"),
        )
        .withColumn(
            "UNIT_IS_IRRADIATED",
            details.rlike(r"(^|\s)IR(\s|$)"),
        )
        .withColumn(
            "UNIT_IS_CMV_NEG",
            details.rlike(r"CMV-"),
        )
        .withColumn(
            "REQUIRES_IRRADIATED",
            requirements.rlike(r"(^|\s)IR(\s|$)"),
        )
        .withColumn(
            "REQUIRES_CMV_NEG",
            requirements.rlike(r"CMV-"),
        )
    )


def with_effective_time(df):
    lag = F.abs(
        F.col("DeviceDate").cast("long")
        - F.col("TransactionDate").cast("long")
    )
    both_present = (
        F.col("DeviceDate").isNotNull()
        & F.col("TransactionDate").isNotNull()
    )
    use_device = both_present & (
        lag <= F.lit(EVENT_TIME_TOLERANCE_SECONDS)
    )
    return (
        df.withColumn("DEVICE_SERVER_LAG_SECONDS", lag)
        .withColumn(
            "EVENT_TS_EFFECTIVE",
            F.when(use_device, F.col("DeviceDate")).otherwise(
                F.col("TransactionDate")
            ),
        )
        .withColumn(
            "EVENT_TS_SOURCE",
            F.when(use_device, F.lit("DEVICE"))
            .when(F.col("TransactionDate").isNotNull(), F.lit("SERVER"))
            .otherwise(F.lit("MISSING")),
        )
        .withColumn(
            "CLOCK_QUALITY",
            F.when(~both_present, F.lit("MISSING"))
            .when(use_device, F.lit("OK"))
            .otherwise(F.lit("OUT_OF_TOLERANCE")),
        )
    )


def mrn_norm_col(colname):
    digits = F.regexp_replace(
        F.col(colname).cast("string"),
        r"[^0-9]",
        "",
    )
    stripped = F.regexp_replace(digits, r"^0+", "")
    return F.when(
        F.length(digits).between(1, 20) & (stripped != ""),
        stripped,
    )


linked = resolve_persons(raw, mrn_col="PatientNumber")
linked = (
    linked.withColumn("_MRN_NORM", mrn_norm_col("PatientNumber"))
    .withColumn(
        "SOURCE_PATIENT_NUMBER",
        F.col("PatientNumber").cast("string"),
    )
)

pdob = spark.table(MILL_PERSON).select(
    F.col("PERSON_ID").cast("long").alias("PERSON_ID"),
    F.to_date("BIRTH_DT_TM").alias("_MILL_DOB"),
)
linked = (
    linked.join(pdob, "PERSON_ID", "left")
    .withColumn(
        "DOB_CONCORDANCE_IND",
        F.when(
            F.col("PERSON_ID").isNull()
            | F.col("PatientBirthDate").isNull()
            | F.col("_MILL_DOB").isNull(),
            F.lit(None).cast("boolean"),
        ).otherwise(
            F.to_date("PatientBirthDate") == F.col("_MILL_DOB")
        ),
    )
    .drop("_MILL_DOB")
)

linked = (
    linked.withColumn(
        "OPERATOR_PERSONNEL_ID",
        F.lit(None).cast("long"),
    )
    .withColumn(
        "OPERATOR_MATCH_STATUS",
        F.lit("NOT_ATTEMPTED"),
    )
    .withColumn(
        "SOURCE_OPERATOR_USERNAME",
        F.when(
            F.lit(PUBLISH_OPERATOR_USERNAME),
            F.col("UserName").cast("string"),
        ),
    )
)

linked = (
    linked.withColumn(
        "SOURCE_SITE",
        F.regexp_extract(
            F.upper(
                F.coalesce(
                    F.col("LocationName").cast("string"),
                    F.lit(""),
                )
            ),
            r"^(RLH|SBH|NUH|WXH)",
            1,
        ),
    )
    .withColumn(
        "SOURCE_SITE",
        F.when(F.col("SOURCE_SITE") == "", F.lit(None)).otherwise(
            F.col("SOURCE_SITE")
        ),
    )
    .withColumn("CARE_SITE_CD", F.lit(None).cast("double"))
    .withColumn("CARE_SITE_MATCH_STATUS", F.lit("NOT_MAPPED"))
)

explicit = [
    name
    for name in DROP_COLUMNS + ["UserName", "PatientNumber"]
    if name in linked.columns
]
trimmed = linked.drop(*explicit)
trimmed, backstop = drop_pii(trimmed)
assert not backstop, (
    "drop_pii removed columns beyond the explicit policy: "
    f"{backstop}"
)
print("[PII_DROP] transaction " + json.dumps(sorted(explicit)))

txn = (
    parse_flags(with_effective_time(trimmed))
    .withColumn(
        "BLOODTRACK_TRANSACTION_KEY",
        F.sha2(F.col("TransactionID").cast("string"), 256),
    )
    .withColumn(
        "TRANSACTION_SUCCESS_IND",
        F.coalesce(F.col("Response") == "Success", F.lit(False)),
    )
    .withColumn(
        "COMMENT_PRESENT_IND",
        F.coalesce(F.col("Comments").cast("boolean"), F.lit(False)),
    )
    .withColumn(
        "ALERT_PRESENT_IND",
        F.coalesce(F.col("Alerts").cast("boolean"), F.lit(False)),
    )
    .withColumn("QUANTITY_RAW", F.col("Quantity").cast("string"))
    .withColumn(
        "QUANTITY_VALUE",
        F.expr("try_cast(Quantity as double)"),
    )
    .withColumn("QUANTITY_UNIT_STATUS", F.lit("UNKNOWN"))
    .withColumn(
        "PatientBloodGroup",
        blank_to_null(F.col("PatientBloodGroup")),
    )
    .withColumn(
        "PatientRequirements",
        blank_to_null(F.col("PatientRequirements")),
    )
    .withColumn("EXPIRY_TS_QUALITY", date_quality("ExpiryDate"))
    .drop("Quantity", "Comments", "Alerts", "_MRN_NORM")
    .withColumnRenamed(
        "TransactionID",
        "BLOODTRACK_TRANSACTION_ID",
    )
    .withColumnRenamed("UnitID", "BLOODTRACK_UNIT_ID")
    .withColumnRenamed("Transaction1", "WORKFLOW_STEP")
    .withColumnRenamed("Response", "RESPONSE_TEXT")
    .withColumnRenamed("ResponseCodeId2", "RESPONSE_CODE")
    .withColumnRenamed("DeviceDate", "DEVICE_EVENT_TS")
    .withColumnRenamed(
        "TransactionDate",
        "SERVER_TRANSACTION_TS",
    )
    .withColumnRenamed("ReservationDate", "RESERVATION_TS")
    .withColumnRenamed("ExpiryDate", "EXPIRY_TS")
    .withColumnRenamed(
        "LocationName",
        "SOURCE_LOCATION_NAME",
    )
    .withColumnRenamed(
        "RecordUpdatedDt",
        "SOURCE_RECORD_UPDATED_TS",
    )
)

pg = spark.table(PRODUCT_MAP_TABLE)
pg_app = pg.select(
    F.col("BLOOD_PRODUCT_GROUP").alias("_PRODUCT_GROUP_KEY"),
    F.when(
        F.col("MAPPING_STATUS") == "APPROVED",
        F.col("PRODUCT_CONCEPT_ID"),
    ).alias("PRODUCT_CONCEPT_ID"),
    F.when(
        F.col("MAPPING_STATUS") == "APPROVED",
        F.col("PRODUCT_CONCEPT_NAME"),
    ).alias("PRODUCT_CONCEPT_NAME"),
    F.when(
        F.col("MAPPING_STATUS") == "APPROVED",
        F.col("PROC_CONCEPT_ID"),
    ).alias("PRODUCT_PROC_CONCEPT_ID"),
    F.when(
        F.col("MAPPING_STATUS") == "APPROVED",
        F.col("PROC_CONCEPT_NAME"),
    ).alias("PRODUCT_PROC_CONCEPT_NAME"),
    F.col("MAPPING_STATUS").alias("PRODUCT_MAPPING_STATUS"),
)
txn = (
    txn.join(
        pg_app,
        txn["BloodProductGroup"] == pg_app["_PRODUCT_GROUP_KEY"],
        "left",
    )
    .drop("_PRODUCT_GROUP_KEY")
    .withColumn(
        "PRODUCT_MAPPING_STATUS",
        F.coalesce(
            F.col("PRODUCT_MAPPING_STATUS"),
            F.lit("UNMAPPED"),
        ),
    )
)

bg = spark.table(BLOODGROUP_MAP_TABLE)


def join_bg(df, src_col, out_prefix):
    lookup = bg.select(
        F.col("BLOOD_GROUP_SOURCE_VALUE").alias("_BG_KEY"),
        F.when(
            F.col("MAPPING_STATUS") == "APPROVED",
            F.col("SNOMED_CONCEPT_ID"),
        ).alias(f"{out_prefix}_CONCEPT_ID"),
        F.when(
            F.col("MAPPING_STATUS") == "APPROVED",
            F.col("SNOMED_CONCEPT_NAME"),
        ).alias(f"{out_prefix}_CONCEPT_NAME"),
        F.col("MAPPING_STATUS").alias(
            f"{out_prefix}_MAPPING_STATUS"
        ),
    )
    return (
        df.join(
            lookup,
            df[src_col] == lookup["_BG_KEY"],
            "left",
        )
        .drop("_BG_KEY")
        .withColumn(
            f"{out_prefix}_MAPPING_STATUS",
            F.coalesce(
                F.col(f"{out_prefix}_MAPPING_STATUS"),
                F.lit("UNMAPPED"),
            ),
        )
    )


txn = join_bg(txn, "BloodUnitGroup", "UNIT_GROUP")
txn = join_bg(txn, "PatientBloodGroup", "PATIENT_GROUP")
txn = (
    txn.withColumn(
        "REQ_IRRADIATED_CONCEPT_ID",
        F.when(
            F.col("REQUIRES_IRRADIATED"),
            F.lit(REQ_IRRADIATED_CONCEPT[0]),
        ).cast("long"),
    )
    .withColumn(
        "REQ_IRRADIATED_CONCEPT_NAME",
        F.when(
            F.col("REQUIRES_IRRADIATED"),
            F.lit(REQ_IRRADIATED_CONCEPT[1]),
        ),
    )
    .withColumn(
        "REQ_CMV_NEG_CONCEPT_ID",
        F.when(
            F.col("REQUIRES_CMV_NEG"),
            F.lit(REQ_CMV_NEG_CONCEPT[0]),
        ).cast("long"),
    )
    .withColumn(
        "REQ_CMV_NEG_CONCEPT_NAME",
        F.when(
            F.col("REQUIRES_CMV_NEG"),
            F.lit(REQ_CMV_NEG_CONCEPT[1]),
        ),
    )
)

_clock_report = txn.agg(
    F.count("*").alias("rows"),
    F.sum(
        F.when(F.col("EVENT_TS_SOURCE") == "DEVICE", 1).otherwise(0)
    ).alias("device_rows"),
    F.sum(
        F.when(F.col("EVENT_TS_SOURCE") == "SERVER", 1).otherwise(0)
    ).alias("server_rows"),
    F.sum(
        F.when(
            F.col("CLOCK_QUALITY") == "OUT_OF_TOLERANCE",
            1,
        ).otherwise(0)
    ).alias("out_of_tolerance_rows"),
).first()
print(
    "[EVENT_TIME] "
    + json.dumps(
        _clock_report.asDict(),
        sort_keys=True,
        default=str,
    )
)

txn = to_snake(txn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish map_bloodtrack_transaction

# COMMAND ----------

txn_write = registry_update_table(
    txn,
    TXN_TARGET,
    ["BLOODTRACK_TRANSACTION_KEY"],
    "Haemonetics BloodTrack bedside-transfusion scans, one row per source transaction "
    "(WORKFLOW_STEP=Begin/End). Retains failed and safety-check attempts. Adds unit/barcode "
    "traceability, product attributes, unit versus patient ABO/Rh, and safety-check outcomes. "
    "Direct patient demographics and staff names are dropped after linkage; source operator "
    "username is populated only when publish_operator_username=true.",
    {
        "BLOODTRACK_TRANSACTION_KEY": "SHA-256 of the unique source TransactionID; primary key.",
        "BLOODTRACK_TRANSACTION_ID": "Unique source BloodTrack TransactionID.",
        "BLOODTRACK_UNIT_ID": "Source UnitID; the unit key used for episode grouping.",
        "UNIT_NUMBER": "Blood-pack donation number/barcode; not unique enough to be the episode key.",
        "WORKFLOW_STEP": "Begin Transfusion or End Transfusion; a workflow step, not a count.",
        "RESPONSE_TEXT": "Bedside safety-check outcome; TRANSACTION_SUCCESS_IND flags Success.",
        "EVENT_TS_EFFECTIVE": "Device time when within the configured tolerance of server time; otherwise server time.",
        "EVENT_TS_SOURCE": "DEVICE, SERVER, or MISSING for EVENT_TS_EFFECTIVE.",
        "PRODUCT_CONCEPT_ID": "SNOMED product concept when the curated map row is APPROVED.",
        "PRODUCT_PROC_CONCEPT_ID": "SNOMED transfusion Procedure concept when the curated map row is APPROVED.",
        "UNIT_GROUP_CONCEPT_ID": "SNOMED blood-group concept for the unit ABO/Rh when APPROVED.",
        "PATIENT_GROUP_CONCEPT_ID": "SNOMED blood-group concept for scan-time patient ABO/Rh when APPROVED.",
        "REQ_IRRADIATED_CONCEPT_ID": "SNOMED requirement concept when REQUIRES_IRRADIATED is true.",
        "REQ_CMV_NEG_CONCEPT_ID": "SNOMED requirement concept when REQUIRES_CMV_NEG is true.",
        "QUANTITY_UNIT_STATUS": "Always UNKNOWN; the source supplies no reliable quantity unit.",
        "DOB_CONCORDANCE_IND": "Source DOB versus Millennium DOB for the matched PERSON_ID; mismatch never reassigns the patient.",
        "OPERATOR_MATCH_STATUS": "NOT_ATTEMPTED in v1; personnel linkage requires an approved deterministic crosswalk.",
        "CARE_SITE_CD": "Populated only from an approved BloodTrack-location crosswalk; null in v1.",
        "SOURCE_PATIENT_NUMBER": "Source MRN retained for trace-back and episode linkage; not treated as an NHS number.",
        "PERSON_ID": "Cerner PERSON_ID via deterministic MRN alias type 10 linkage.",
        "LINKAGE_STATUS": "MATCHED_MRN, AMBIGUOUS, or UNMATCHED for this MRN-only feed.",
        "SOURCE_RECORD_UPDATED_TS": "Genuine per-row source update timestamp; included in ROW_HASH.",
        "ADC_UPDT": "Whole-source snapshot load timestamp; excluded from ROW_HASH by the shared writer.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and publish map_bloodtrack_transfusion

# COMMAND ----------

episode_events = (
    raw.withColumn("_MRN_NORM", mrn_norm_col("PatientNumber"))
    .withColumn(
        "_MRN_KEY",
        F.coalesce(F.col("_MRN_NORM"), F.lit("<NULL>")),
    )
    .where(F.col("UnitID").isNotNull())
)
episode_events = with_effective_time(episode_events)
success_events = episode_events.where(
    F.coalesce(F.col("Response") == "Success", F.lit(False))
)
keys = ["UnitID", "_MRN_KEY"]
success_groups = success_events.select(*keys).distinct()

gcounts = (
    episode_events.groupBy(*keys)
    .agg(
        F.sum(
            F.when(
                (F.col("Response") == "Success")
                & (F.col("Transaction1") == "Begin Transfusion"),
                1,
            ).otherwise(0)
        ).alias("BEGIN_SUCCESS_COUNT"),
        F.sum(
            F.when(
                (F.col("Response") == "Success")
                & (F.col("Transaction1") == "End Transfusion"),
                1,
            ).otherwise(0)
        ).alias("END_SUCCESS_COUNT"),
        F.sum(
            F.when(
                ~F.coalesce(
                    F.col("Response") == "Success",
                    F.lit(False),
                ),
                1,
            ).otherwise(0)
        ).alias("FAILED_ATTEMPT_COUNT"),
        F.count("*").alias("SOURCE_EVENT_COUNT"),
    )
    .join(success_groups, keys, "inner")
)

begins = success_events.where(
    F.col("Transaction1") == "Begin Transfusion"
)
ends = success_events.where(
    F.col("Transaction1") == "End Transfusion"
)

wb = Window.partitionBy(*keys).orderBy(
    F.col("EVENT_TS_EFFECTIVE").asc_nulls_last(),
    F.col("TransactionID").asc(),
)
begin_sel = (
    begins.withColumn("_RN", F.row_number().over(wb))
    .where(F.col("_RN") == 1)
    .select(
        *keys,
        F.col("TransactionID").alias("BEGIN_TRANSACTION_ID"),
        F.col("EVENT_TS_EFFECTIVE").alias("BEGIN_TS"),
        F.col("UnitNumber").alias("BEGIN_UNIT_NUMBER"),
        F.col("ProductCode").alias("BEGIN_PRODUCT_CODE"),
        F.col("BloodProductGroup").alias(
            "BEGIN_BLOOD_PRODUCT_GROUP"
        ),
        F.col("BloodUnitGroup").alias("BEGIN_BLOOD_UNIT_GROUP"),
        blank_to_null(F.col("PatientBloodGroup")).alias(
            "BEGIN_PATIENT_BLOOD_GROUP"
        ),
        F.col("BloodUnitDetails").alias(
            "BEGIN_BLOOD_UNIT_DETAILS"
        ),
        blank_to_null(F.col("PatientRequirements")).alias(
            "BEGIN_PATIENT_REQUIREMENTS"
        ),
        F.col("DeviceName").alias("BEGIN_DEVICE_NAME"),
        F.col("LocationName").alias("BEGIN_LOCATION_NAME"),
    )
)

end_candidates = ends.select(
    *keys,
    F.col("TransactionID").alias("_END_TRANSACTION_ID"),
    F.col("EVENT_TS_EFFECTIVE").alias("_END_TS"),
    F.col("UnitNumber").alias("_END_UNIT_NUMBER"),
    F.col("ProductCode").alias("_END_PRODUCT_CODE"),
    F.col("BloodProductGroup").alias(
        "_END_BLOOD_PRODUCT_GROUP"
    ),
    F.col("BloodUnitGroup").alias("_END_BLOOD_UNIT_GROUP"),
    blank_to_null(F.col("PatientBloodGroup")).alias(
        "_END_PATIENT_BLOOD_GROUP"
    ),
    F.col("BloodUnitDetails").alias(
        "_END_BLOOD_UNIT_DETAILS"
    ),
    blank_to_null(F.col("PatientRequirements")).alias(
        "_END_PATIENT_REQUIREMENTS"
    ),
    F.col("DeviceName").alias("_END_DEVICE_NAME"),
    F.col("LocationName").alias("_END_LOCATION_NAME"),
    F.col("Quantity").cast("string").alias("_END_QUANTITY_RAW"),
    F.expr("try_cast(Quantity as double)").alias(
        "_END_QUANTITY_VALUE"
    ),
)

end_rank_input = (
    gcounts.join(begin_sel.select(*keys, "BEGIN_TS"), keys, "left")
    .join(end_candidates, keys, "left")
    .withColumn(
        "_END_PRIORITY",
        F.when(F.col("_END_TS").isNull(), F.lit(2))
        .when(F.col("BEGIN_TS").isNull(), F.lit(0))
        .when(F.col("_END_TS") >= F.col("BEGIN_TS"), F.lit(0))
        .otherwise(F.lit(1)),
    )
)
we = Window.partitionBy(*keys).orderBy(
    F.col("_END_PRIORITY").asc(),
    F.col("_END_TS").asc_nulls_last(),
    F.col("_END_TRANSACTION_ID").asc_nulls_last(),
)
end_sel = (
    end_rank_input.withColumn("_RN", F.row_number().over(we))
    .where(F.col("_RN") == 1)
    .select(
        *keys,
        F.col("_END_TRANSACTION_ID").alias(
            "END_TRANSACTION_ID"
        ),
        F.col("_END_TS").alias("END_TS"),
        F.col("_END_UNIT_NUMBER").alias("END_UNIT_NUMBER"),
        F.col("_END_PRODUCT_CODE").alias("END_PRODUCT_CODE"),
        F.col("_END_BLOOD_PRODUCT_GROUP").alias(
            "END_BLOOD_PRODUCT_GROUP"
        ),
        F.col("_END_BLOOD_UNIT_GROUP").alias(
            "END_BLOOD_UNIT_GROUP"
        ),
        F.col("_END_PATIENT_BLOOD_GROUP").alias(
            "END_PATIENT_BLOOD_GROUP"
        ),
        F.col("_END_BLOOD_UNIT_DETAILS").alias(
            "END_BLOOD_UNIT_DETAILS"
        ),
        F.col("_END_PATIENT_REQUIREMENTS").alias(
            "END_PATIENT_REQUIREMENTS"
        ),
        F.col("_END_DEVICE_NAME").alias("END_DEVICE_NAME"),
        F.col("_END_LOCATION_NAME").alias(
            "END_LOCATION_NAME"
        ),
        F.col("_END_QUANTITY_RAW").alias("END_QUANTITY_RAW"),
        F.col("_END_QUANTITY_VALUE").alias(
            "END_QUANTITY_VALUE"
        ),
    )
)

episode = (
    gcounts.join(begin_sel, keys, "left")
    .join(end_sel, keys, "left")
    .withColumn(
        "UNIT_NUMBER",
        F.coalesce(
            F.col("BEGIN_UNIT_NUMBER"),
            F.col("END_UNIT_NUMBER"),
        ),
    )
    .withColumn(
        "PRODUCT_CODE",
        F.coalesce(
            F.col("BEGIN_PRODUCT_CODE"),
            F.col("END_PRODUCT_CODE"),
        ),
    )
    .withColumn(
        "BLOOD_PRODUCT_GROUP",
        F.coalesce(
            F.col("BEGIN_BLOOD_PRODUCT_GROUP"),
            F.col("END_BLOOD_PRODUCT_GROUP"),
        ),
    )
    .withColumn(
        "BLOOD_UNIT_GROUP",
        F.coalesce(
            F.col("BEGIN_BLOOD_UNIT_GROUP"),
            F.col("END_BLOOD_UNIT_GROUP"),
        ),
    )
    .withColumn(
        "PATIENT_BLOOD_GROUP",
        F.coalesce(
            F.col("BEGIN_PATIENT_BLOOD_GROUP"),
            F.col("END_PATIENT_BLOOD_GROUP"),
        ),
    )
    .withColumn(
        "BloodUnitDetails",
        F.coalesce(
            F.col("BEGIN_BLOOD_UNIT_DETAILS"),
            F.col("END_BLOOD_UNIT_DETAILS"),
        ),
    )
    .withColumn(
        "PatientRequirements",
        F.coalesce(
            F.col("BEGIN_PATIENT_REQUIREMENTS"),
            F.col("END_PATIENT_REQUIREMENTS"),
        ),
    )
    .withColumn("END_QUANTITY_UNIT_STATUS", F.lit("UNKNOWN"))
)

multi = (
    (F.col("BEGIN_SUCCESS_COUNT") > 1)
    | (F.col("END_SUCCESS_COUNT") > 1)
)
clock_err = (
    F.col("BEGIN_TS").isNotNull()
    & F.col("END_TS").isNotNull()
    & (F.col("END_TS") < F.col("BEGIN_TS"))
)
episode = (
    parse_flags(episode)
    .withColumn(
        "TRANSFUSION_STATUS",
        F.when(clock_err, F.lit("CLOCK_ORDER_ERROR"))
        .when(multi, F.lit("MULTIPLE_SUCCESS_EVENTS"))
        .when(
            F.col("BEGIN_TS").isNotNull()
            & F.col("END_TS").isNotNull(),
            F.lit("COMPLETED"),
        )
        .when(
            F.col("BEGIN_TS").isNotNull(),
            F.lit("STARTED_NO_END"),
        )
        .otherwise(F.lit("END_WITHOUT_START")),
    )
    .withColumn(
        "ELAPSED_MINUTES",
        F.when(
            F.col("BEGIN_TS").isNotNull()
            & F.col("END_TS").isNotNull()
            & (F.col("END_TS") >= F.col("BEGIN_TS")),
            (
                F.col("END_TS").cast("long")
                - F.col("BEGIN_TS").cast("long")
            )
            / 60.0,
        ),
    )
    .withColumn(
        "AMBIGUITY_IND",
        F.coalesce(multi | clock_err, F.lit(False)),
    )
    .withColumn(
        "SOURCE_MRN_MISSING_IND",
        F.col("_MRN_KEY") == "<NULL>",
    )
    .withColumn(
        "TRANSFUSION_KEY",
        F.sha2(
            F.concat_ws(
                "|",
                F.lit("BLOODTRACK"),
                F.col("UnitID").cast("string"),
                F.col("_MRN_KEY"),
            ),
            256,
        ),
    )
    .withColumnRenamed("UnitID", "BLOODTRACK_UNIT_ID")
)

mrn_person = (
    spark.table(TXN_TARGET)
    .where(
        (F.col("IS_CURRENT_IN_SOURCE") == True)
        & F.col("PERSON_ID").isNotNull()
        & F.col("SOURCE_PATIENT_NUMBER").isNotNull()
    )
    .select("SOURCE_PATIENT_NUMBER", "PERSON_ID")
    .withColumn(
        "_MRN_KEY",
        F.coalesce(
            mrn_norm_col("SOURCE_PATIENT_NUMBER"),
            F.lit("<NULL>"),
        ),
    )
    .groupBy("_MRN_KEY")
    .agg(
        F.max("PERSON_ID").cast("long").alias("PERSON_ID"),
        F.countDistinct("PERSON_ID").alias("_PERSON_COUNT"),
    )
    .where(F.col("_PERSON_COUNT") == 1)
    .select("_MRN_KEY", "PERSON_ID")
)
episode = (
    episode.join(mrn_person, "_MRN_KEY", "left")
    .withColumn(
        "PERSON_LINKAGE_STATUS",
        F.when(
            F.col("SOURCE_MRN_MISSING_IND"),
            F.lit("MISSING_MRN"),
        )
        .when(F.col("PERSON_ID").isNotNull(), F.lit("MATCHED_MRN"))
        .otherwise(F.lit("UNMATCHED")),
    )
)

episode = (
    episode.join(
        pg_app,
        episode["BLOOD_PRODUCT_GROUP"]
        == pg_app["_PRODUCT_GROUP_KEY"],
        "left",
    )
    .drop("_PRODUCT_GROUP_KEY")
    .withColumn(
        "PRODUCT_MAPPING_STATUS",
        F.coalesce(
            F.col("PRODUCT_MAPPING_STATUS"),
            F.lit("UNMAPPED"),
        ),
    )
)
episode = join_bg(episode, "BLOOD_UNIT_GROUP", "UNIT_GROUP")
episode = join_bg(
    episode,
    "PATIENT_BLOOD_GROUP",
    "PATIENT_GROUP",
)
episode = (
    episode.withColumn(
        "REQ_IRRADIATED_CONCEPT_ID",
        F.when(
            F.col("REQUIRES_IRRADIATED"),
            F.lit(REQ_IRRADIATED_CONCEPT[0]),
        ).cast("long"),
    )
    .withColumn(
        "REQ_CMV_NEG_CONCEPT_ID",
        F.when(
            F.col("REQUIRES_CMV_NEG"),
            F.lit(REQ_CMV_NEG_CONCEPT[0]),
        ).cast("long"),
    )
    .drop(
        "_MRN_KEY",
        "BEGIN_UNIT_NUMBER",
        "END_UNIT_NUMBER",
        "BEGIN_PRODUCT_CODE",
        "END_PRODUCT_CODE",
        "BEGIN_BLOOD_PRODUCT_GROUP",
        "END_BLOOD_PRODUCT_GROUP",
        "BEGIN_BLOOD_UNIT_GROUP",
        "END_BLOOD_UNIT_GROUP",
        "BEGIN_PATIENT_BLOOD_GROUP",
        "END_PATIENT_BLOOD_GROUP",
        "BEGIN_BLOOD_UNIT_DETAILS",
        "END_BLOOD_UNIT_DETAILS",
        "BEGIN_PATIENT_REQUIREMENTS",
        "END_PATIENT_REQUIREMENTS",
    )
)
episode = to_snake(episode)

episode_write = registry_update_table(
    episode,
    EPISODE_TARGET,
    ["TRANSFUSION_KEY"],
    "BloodTrack transfusion episodes, one row per unit-recipient group. Successful begin/end "
    "scans are paired deterministically; incomplete, multi-event, and clock-order groups are "
    "retained with an explicit TRANSFUSION_STATUS. Failed and null-UnitID scans remain in "
    "map_bloodtrack_transaction.",
    {
        "TRANSFUSION_KEY": "SHA-256 of BLOODTRACK|UnitID|normalized-MRN; primary key. The normalized MRN is not published.",
        "TRANSFUSION_STATUS": "COMPLETED, STARTED_NO_END, END_WITHOUT_START, MULTIPLE_SUCCESS_EVENTS, or CLOCK_ORDER_ERROR.",
        "ELAPSED_MINUTES": "Selected end minus begin effective time, only when end is on or after begin.",
        "AMBIGUITY_IND": "True for multi-success or clock-order groups.",
        "BEGIN_TRANSACTION_ID": "Selected earliest successful begin transaction ID.",
        "END_TRANSACTION_ID": "Selected first successful end on/after begin, or the earliest end when no valid later end exists.",
        "BEGIN_SUCCESS_COUNT": "Count of successful begin scans in the group.",
        "END_SUCCESS_COUNT": "Count of successful end scans in the group.",
        "FAILED_ATTEMPT_COUNT": "Count of non-success scans sharing this unit-recipient group.",
        "SOURCE_EVENT_COUNT": "All source scans sharing this unit-recipient group.",
        "SOURCE_MRN_MISSING_IND": "True when PatientNumber could not be normalized; the identifier itself is not published.",
        "PERSON_ID": "Cerner PERSON_ID when the normalized MRN maps consistently to one linked person.",
        "PRODUCT_CONCEPT_ID": "SNOMED product concept when the curated map row is APPROVED.",
        "UNIT_GROUP_CONCEPT_ID": "SNOMED unit blood-group concept when the curated map row is APPROVED.",
        "PATIENT_GROUP_CONCEPT_ID": "SNOMED scan-time patient blood-group concept when the curated map row is APPROVED.",
        "END_QUANTITY_UNIT_STATUS": "Always UNKNOWN; the source supplies no reliable quantity unit.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Release validation

# COMMAND ----------

CHECKS = []


def check(name, passed, metric=None, detail=""):
    record = {
        "check": name,
        "passed": bool(passed),
        "metric": metric,
        "detail": str(detail)[:500],
    }
    CHECKS.append(record)
    print(
        ("PASS " if passed else "FAIL ")
        + json.dumps(record, sort_keys=True, default=str)
    )
    return bool(passed)


t = spark.table(TXN_TARGET)
tc = t.where(F.col("IS_CURRENT_IN_SOURCE") == True)
e = spark.table(EPISODE_TARGET)
ec = e.where(F.col("IS_CURRENT_IN_SOURCE") == True)
src = raw
src_rows = int(src.count())
tc_rows = int(tc.count())
ec_rows = int(ec.count())

check("txn_rowcount", tc_rows == src_rows, tc_rows, f"source={src_rows}")
_k = t.agg(
    F.count("*").alias("rows"),
    F.countDistinct("BLOODTRACK_TRANSACTION_KEY").alias("keys"),
    F.sum(
        F.when(
            F.col("BLOODTRACK_TRANSACTION_KEY").isNull(),
            1,
        ).otherwise(0)
    ).alias("nulls"),
).first()
check(
    "txn_key_unique",
    _k.rows == _k.keys and int(_k.nulls or 0) == 0,
    _k.rows,
    f"distinct={_k.keys}, null={_k.nulls}",
)
src_fail = src.where(
    ~F.coalesce(F.col("Response") == "Success", F.lit(False))
).count()
target_fail = tc.where(~F.col("TRANSACTION_SUCCESS_IND")).count()
check(
    "txn_failed_retained",
    target_fail == src_fail,
    target_fail,
    f"source={src_fail}",
)
banned = {
    "PATIENT_NAME",
    "PATIENT_BIRTH_DATE",
    "PATIENT_GENDER",
    "HEALTH_NUMBER",
    "PATIENT_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "USER_NAME",
}
check(
    "txn_pii_absent",
    not (set(t.columns) & banned),
    sorted(set(t.columns) & banned),
)
if not PUBLISH_OPERATOR_USERNAME:
    leaked = tc.where(
        F.col("SOURCE_OPERATOR_USERNAME").isNotNull()
    ).count()
    check("txn_username_suppressed", leaked == 0, leaked)
linkage_rate = (
    tc.where(F.col("PERSON_ID").isNotNull()).count()
    / max(tc_rows, 1)
)
check("txn_linkage_rate", linkage_rate >= 0.97, linkage_rate)

_ek = e.agg(
    F.count("*").alias("rows"),
    F.countDistinct("TRANSFUSION_KEY").alias("keys"),
    F.sum(
        F.when(F.col("TRANSFUSION_KEY").isNull(), 1).otherwise(0)
    ).alias("nulls"),
).first()
check(
    "episode_key_unique",
    _ek.rows == _ek.keys and int(_ek.nulls or 0) == 0,
    _ek.rows,
    f"distinct={_ek.keys}, null={_ek.nulls}",
)
check(
    "episode_no_negative_elapsed",
    ec.where(F.col("ELAPSED_MINUTES") < 0).count() == 0,
)
allowed_statuses = [
    "COMPLETED",
    "STARTED_NO_END",
    "END_WITHOUT_START",
    "MULTIPLE_SUCCESS_EVENTS",
    "CLOCK_ORDER_ERROR",
]
invalid_statuses = ec.where(
    ~F.col("TRANSFUSION_STATUS").isin(*allowed_statuses)
).count()
check(
    "episode_status_domain",
    invalid_statuses == 0,
    invalid_statuses,
)
expected_groups = (
    src.where(
        (F.col("Response") == "Success")
        & F.col("UnitID").isNotNull()
    )
    .select(
        "UnitID",
        mrn_norm_col("PatientNumber").alias("MRN_NORM"),
    )
    .distinct()
    .count()
)
check(
    "episode_group_reconcile",
    ec_rows == expected_groups,
    ec_rows,
    f"expected_groups={expected_groups}",
)
episode_identifier_columns = {
    "SOURCE_MRN_NORM",
    "SOURCE_PATIENT_NUMBER",
}
check(
    "episode_identifiers_absent",
    not (set(e.columns) & episode_identifier_columns),
    sorted(set(e.columns) & episode_identifier_columns),
)

_c = spark.table(OMOP_CONCEPT).select(
    F.col("concept_id").cast("long").alias("cid"),
    "domain_id",
    "vocabulary_id",
    "standard_concept",
    "invalid_reason",
)


def bad_map(table, id_cols):
    ids = spark.table(table).where(
        F.col("MAPPING_STATUS") == "APPROVED"
    )
    count = 0
    for column in id_cols:
        count += (
            ids.where(F.col(column).isNotNull())
            .select(F.col(column).cast("long").alias("cid"))
            .join(_c, "cid", "left")
            .where(
                F.col("standard_concept").isNull()
                | (F.col("standard_concept") != "S")
                | (F.col("vocabulary_id") != "SNOMED")
                | F.col("invalid_reason").isNotNull()
            )
            .count()
        )
    return count


invalid_map_count = bad_map(
    PRODUCT_MAP_TABLE,
    ["PRODUCT_CONCEPT_ID", "PROC_CONCEPT_ID"],
) + bad_map(
    BLOODGROUP_MAP_TABLE,
    ["SNOMED_CONCEPT_ID"],
)
check(
    "map_concepts_valid",
    invalid_map_count == 0,
    invalid_map_count,
)

product_status = {
    row["PRODUCT_MAPPING_STATUS"]: int(row["count"])
    for row in (
        tc.groupBy("PRODUCT_MAPPING_STATUS")
        .count()
        .collect()
    )
}
check(
    "map_coverage_report",
    True,
    product_status.get("UNMAPPED", 0),
    json.dumps(product_status, sort_keys=True),
)

_linkage_report = tc.agg(
    F.count("*").alias("transaction_rows"),
    F.countDistinct("PERSON_ID").alias("linked_people"),
    F.sum(
        F.when(F.col("PERSON_ID").isNull(), F.lit(1)).otherwise(F.lit(0))
    ).alias("unlinked_transaction_rows"),
).first()
check(
    "person_linkage_report",
    True,
    int(_linkage_report["unlinked_transaction_rows"] or 0),
    json.dumps(
        {
            "transaction_rows": int(_linkage_report["transaction_rows"] or 0),
            "linked_people": int(_linkage_report["linked_people"] or 0),
            "unlinked_transaction_rows": int(
                _linkage_report["unlinked_transaction_rows"] or 0
            ),
        },
        sort_keys=True,
    ),
)

if EXPECT_IDEMPOTENT:
    for table in (TXN_TARGET, EPISODE_TARGET):
        metrics = _latest_operation_metrics(table)
        inserted = int(
            metrics.get("numTargetRowsInserted", 0) or 0
        )
        updated = int(
            metrics.get("numTargetRowsUpdated", 0) or 0
        )
        deleted = int(
            metrics.get("numTargetRowsDeleted", 0) or 0
        )
        check(
            f"idempotent:{table.split('.')[-1]}",
            metrics.get("operation") == "MERGE"
            and inserted == 0
            and updated == 0
            and deleted == 0,
            inserted + updated + deleted,
            json.dumps(metrics, sort_keys=True, default=str),
        )

failures = [record for record in CHECKS if not record["passed"]]
if failures:
    raise AssertionError(
        "BLOODTRACK VALIDATION FAILED: "
        + json.dumps(failures, sort_keys=True, default=str)
    )
print(f"[VALIDATION] {len(CHECKS)} checks passed/reported")

# COMMAND ----------

_summary = {
    "status": "SUCCESS",
    "pipeline": "bloodtrack_pipeline_no_rde",
    "run_id": REGISTRY_RUN_ID,
    "source_delta_version": SOURCE_DELTA_VERSION,
    "target_schema": TARGET_SCHEMA,
    "tables": [
        "map_bloodtrack_transaction",
        "map_bloodtrack_transfusion",
        "bloodtrack_product_group_map",
        "bloodtrack_blood_group_map",
    ],
    "validation_checks": len(CHECKS),
}
print(json.dumps(_summary, sort_keys=True))
dbutils.notebook.exit(json.dumps(_summary, sort_keys=True))
