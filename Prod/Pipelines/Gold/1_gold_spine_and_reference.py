# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Spine And Reference
# MAGIC People, encounters, journeys and shared reference data.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    F,
    _n,
    _qc,
    _s3_axis_columns,
    _s3_gate_direct_public,
    _src,
    _with_comments,
    dp,
)

# COMMAND ----------

# ==== The spine keys every cross-table check is measured against ====

# One deduplicated key column per spine table, plus the person's own birth and
# death, declared once for reuse by cross-table checks.
# Temporary views share a definition; they do not guarantee one physical scan.


@dp.temporary_view(name="_gold_person_keys")
def _gold_person_keys():
    """Read distinct non-null Silver person keys for Gold foreign-key checks."""
    return (
        spark.read.table(_src("spine_person"))
        .select(F.col("person_id").alias("_qc_person_id"))
        .where(F.col("_qc_person_id").isNotNull())
        .dropDuplicates(["_qc_person_id"])
    )


@dp.temporary_view(name="_gold_encounter_keys")
def _gold_encounter_keys():
    """Read distinct non-null Silver encounter keys for Gold foreign-key checks."""
    return (
        spark.read.table(_src("spine_encounter"))
        .select(F.col("encounter_id").alias("_qc_encounter_id"))
        .where(F.col("_qc_encounter_id").isNotNull())
        .dropDuplicates(["_qc_encounter_id"])
    )


@dp.temporary_view(name="_gold_episode_keys")
def _gold_episode_keys():
    """Read distinct non-null Silver episode keys for Gold foreign-key checks."""
    return (
        spark.read.table(_src("spine_episode"))
        .select(F.col("episode_id").alias("_qc_episode_id"))
        .where(F.col("_qc_episode_id").isNotNull())
        .dropDuplicates(["_qc_episode_id"])
    )


@dp.temporary_view(name="_gold_person_dates")
def _gold_person_dates():
    """Prepare repaired person dates so event checks agree with Gold's person table."""
    # The two dates are read through the same expressions gold_spine.person
    # publishes them with, rather than raw from silver. A flag measured against a
    # value gold has removed would contradict the person row a researcher joins
    # it to: a death recorded before its own birth is not in the person table, so
    # nothing may be flagged as happening after it, and a birth held at the
    # source's "date of birth unknown" marker is not a birth to compare against.
    return (
        spark.read.table(_src("spine_person"))
        .select(
            F.col("person_id").alias("_qc_date_person_id"),
            F.expr("CASE WHEN CAST(`birth_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`birth_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`birth_datetime` AS DATE) = DATE'2100-12-31' OR CAST(`birth_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`birth_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_datetime` END").alias("_qc_birth_datetime"),
            F.expr("CASE WHEN `deceased_datetime` < `birth_datetime` THEN NULL ELSE `deceased_datetime` END").alias("_qc_deceased_datetime"),
        )
        .where(F.col("_qc_date_person_id").isNotNull())
        .dropDuplicates(["_qc_date_person_id"])
    )

# COMMAND ----------

# ==== journey_spine.care_participation ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_CARE_PARTICIPATION_SELECT = [
    '`care_participation_key` AS `care_participation_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`journey_key` AS `journey_key`',
    '`service_id` AS `service_id`',
    '`practitioner_id` AS `practitioner_id`',
    '`role` AS `role`',
    "CASE WHEN CAST(`valid_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`valid_from` AS DATE)) < 9999 OR CAST(`valid_from` AS DATE) = DATE'1900-01-01' OR CAST(`valid_from` AS DATE) < DATE'1901-01-01' AND CAST(`valid_from` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_from` END AS `valid_from`",
    '`valid_to` AS `valid_to`',
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_care_participation_metadata; source history stays on the main research table.
SPINE_CARE_PARTICIPATION_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 3 of 144,668,217
    # rows (2.07e-06%) when profiled on 2026-08-24.
    "gold.spine.care_participation.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_CARE_PARTICIPATION_COLUMN_COMMENTS = {
    "care_participation_key": "Deterministic SHA-256 participation key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT.",
    "journey_key": "Deterministic journey key when available.",
    "service_id": "Service-registration context when available.",
    "practitioner_id": "Millennium personnel PERSON_ID as BIGINT.",
    "role": "Source-field-derived participation role.",
    "valid_from": "Participation validity start.",
    "valid_to": "Participation validity end.",
    "construction_rule": "Governed derivation rule.",
    "construction_version": "Governed derivation-rule version.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Source encounter row identity.",
    "loaded_at": "map_encounter.ADC_UPDT inherited unchanged through the canonical encounter row for each registrar, recorder or discharger participation. This is encounter ingestion provenance, not a practitioner update, role-validity timestamp or Silver refresh time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_care_participation"),
    comment="Internal quality-controlled twin of spine_care_participation: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_CARE_PARTICIPATION_ADVISORY_RULES)
def _gold_qc_spine_care_participation():
    """Quality-controlled twin of journey_spine.care_participation."""
    # 54 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_care_participation",
        SPINE_CARE_PARTICIPATION_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_CARE_PARTICIPATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.care_participation"),
    comment=(
        "One practitioner role attribution to a subject in encounter context. Gold QC twin of "
        "the silver product: 2 columns are repaired or nulled, 1 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_care_participation():
    """Contract-v2 public twin of spine_care_participation; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_care_participation")).selectExpr(
        '`care_participation_key` AS `care_participation_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`journey_key` AS `journey_key`',
        '`service_id` AS `service_id`',
        '`practitioner_id` AS `practitioner_id`',
        '`role` AS `role`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_CARE_PARTICIPATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.encounter ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_ENCOUNTER_SELECT = [
    '`encounter_key` AS `encounter_key`',
    '`encounter_id` AS `encounter_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`parent_encounter_id` AS `parent_encounter_id`',
    '`parentage_status` AS `parentage_status`',
    '`encounter_level` AS `encounter_level`',
    "CASE WHEN UPPER(TRIM(CAST(`class_code` AS STRING))) = '0' THEN NULL ELSE `class_code` END AS `class_code`",
    '`class_display` AS `class_display`',
    "CASE WHEN UPPER(TRIM(CAST(`type_code` AS STRING))) = '0' THEN NULL ELSE `type_code` END AS `type_code`",
    '`type_display` AS `type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`type_class_code` AS STRING))) = '0' THEN NULL ELSE `type_class_code` END AS `type_class_code`",
    '`type_class_display` AS `type_class_display`',
    '`status_code` AS `status_code`',
    '`status_display` AS `status_display`',
    '`period_start` AS `period_start`',
    '`period_end` AS `period_end`',
    '`arrival_method` AS `arrival_method`',
    '`arrival_confidence` AS `arrival_confidence`',
    '`departure_method` AS `departure_method`',
    '`departure_confidence` AS `departure_confidence`',
    '`length_of_stay_minutes` AS `length_of_stay_minutes`',
    '`scheduled_start` AS `scheduled_start`',
    'CASE WHEN `scheduled_start` IS NOT NULL AND `scheduled_end` IS NOT NULL AND `scheduled_start` > `scheduled_end` THEN NULL ELSE `scheduled_end` END AS `scheduled_end`',
    "CASE WHEN CAST(`registration_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`registration_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`registration_datetime` AS DATE)) < 9999 OR CAST(`registration_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`registration_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`registration_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `registration_datetime` END END AS `registration_datetime`",
    '`inpatient_admit_datetime` AS `inpatient_admit_datetime`',
    "CASE WHEN CAST(`discharge_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`discharge_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`discharge_datetime` AS DATE)) < 9999 THEN NULL ELSE `discharge_datetime` END END AS `discharge_datetime`",
    'CASE WHEN CAST(`workflow_complete_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `workflow_complete_datetime` END AS `workflow_complete_datetime`',
    '`raw_arrival_datetime` AS `raw_arrival_datetime`',
    '`raw_departure_datetime` AS `raw_departure_datetime`',
    "CASE WHEN UPPER(TRIM(CAST(`admission_source_code` AS STRING))) = '0' THEN NULL ELSE `admission_source_code` END AS `admission_source_code`",
    '`admission_source_display` AS `admission_source_display`',
    "CASE WHEN UPPER(TRIM(CAST(`discharge_destination_code` AS STRING))) = '0' THEN NULL ELSE `discharge_destination_code` END AS `discharge_destination_code`",
    '`discharge_destination_display` AS `discharge_destination_display`',
    "CASE WHEN UPPER(TRIM(CAST(`responsible_service_code` AS STRING))) = '0' THEN NULL ELSE `responsible_service_code` END AS `responsible_service_code`",
    '`responsible_service_display` AS `responsible_service_display`',
    "CASE WHEN UPPER(TRIM(CAST(`specialty_code` AS STRING))) = '0' THEN NULL ELSE `specialty_code` END AS `specialty_code`",
    '`specialty_display` AS `specialty_display`',
    '`current_location_key` AS `current_location_key`',
    '`organization_id` AS `organization_id`',
    '`service_provider_organization_key` AS `service_provider_organization_key`',
    '`reason_for_visit` AS `reason_for_visit`',
    '`attendance_evidence` AS `attendance_evidence`',
    '`attendance_witness_count` AS `attendance_witness_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

SPINE_ENCOUNTER_COLUMN_COMMENTS = {
    "encounter_key": "Source encounter primary key.",
    "encounter_id": "Deterministic encounter primary key.",
    "subject_key": "Always-populated subject join key.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium person identifier.",
    "parent_encounter_id": "Governed containment parent when supplied by source evidence.",
    "parentage_status": "Provenance state for encounter containment.",
    "encounter_level": "Rules-light source-classified encounter level.",
    "class_code": "Source encounter class code.",
    "class_display": "Source encounter class display.",
    "type_code": "Source encounter type code.",
    "type_display": "Source encounter type display.",
    "type_class_code": "Source encounter type-class code.",
    "type_class_display": "Source encounter type-class display.",
    "status_code": "Native encounter status code.",
    "status_display": "Native encounter status display.",
    "period_start": "Best observed encounter start.",
    "period_end": "Best observed encounter end.",
    "arrival_method": "Method selecting period_start.",
    "arrival_confidence": "Source-derived confidence for period_start.",
    "departure_method": "Method selecting period_end.",
    "departure_confidence": "Source-derived confidence for period_end.",
    "length_of_stay_minutes": "Source-productised encounter duration.",
    "scheduled_start": "Scheduled arrival timestamp.",
    "scheduled_end": "Scheduled departure timestamp.",
    "registration_datetime": "Registration timestamp retained as source evidence.",
    "inpatient_admit_datetime": "Inpatient admission timestamp.",
    "discharge_datetime": "Source actual discharge timestamp.",
    "workflow_complete_datetime": "Administrative workflow completion timestamp.",
    "raw_arrival_datetime": "Raw ARRIVE_DT_TM retained without asserting observability.",
    "raw_departure_datetime": "Raw DEPART_DT_TM retained without asserting clinical meaning.",
    "admission_source_code": "Source admission source code.",
    "admission_source_display": "Best available label for ADMIT_SRC_CD.",
    "discharge_destination_code": "Source discharge destination code.",
    "discharge_destination_display": "Best available label for DISCH_TO_LOCTN_CD.",
    "responsible_service_code": "Source medical service code.",
    "responsible_service_display": "Best available label for MED_SERVICE_CD.",
    "specialty_code": "Source specialty-unit code.",
    "specialty_display": "Source specialty-unit display.",
    "current_location_key": "Source current nurse-unit code.",
    "organization_id": "Source encounter organization reference.",
    "service_provider_organization_key": "Source organization primarily responsible for the encounter.",
    "reason_for_visit": "Verbatim source reason for visit.",
    "attendance_evidence": "Productised attendance evidence classification.",
    "attendance_witness_count": "Number of attendance witnesses in the source product.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "record_status": "Derived encounter-row status: superseded when map_encounter.END_EFFECTIVE_DT_TM is earlier than 2100-01-01; otherwise active, including a null or open-ended source timestamp. This classification does not use the current wall clock or ACTIVE_IND and is distinct from encounter workflow/discharge status.",
    "record_status_effective_from": "map_encounter.ACTIVE_STATUS_DT_TM carried unchanged as the source active-status timestamp. No arrival, registration or ingestion-clock fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "map_encounter.END_EFFECTIVE_DT_TM when it is earlier than 2100-01-01; otherwise null, including missing values and open-ended sentinels. This is the row-version end boundary, not the encounter departure or discharge time.",
    "source_update_timestamp": "map_encounter.UPDT_DT_TM carried unchanged from the source encounter row. It is separate from the bronze ADC_UPDT ingestion clock and is not the Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_encounter.ADC_UPDT carried unchanged from the contributing bronze encounter row. This is ingestion provenance, not the Silver refresh time or encounter arrival/departure; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_encounter"),
    comment="Internal quality-controlled twin of spine_encounter: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_spine_encounter():
    """Quality-controlled twin of journey_spine.encounter."""
    # 18 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_encounter",
        SPINE_ENCOUNTER_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_ENCOUNTER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.encounter"),
    comment=(
        "One source encounter with evidence-based arrival and departure semantics; "
        "containment remains explicit when unavailable. Gold QC twin of the silver product: "
        "13 columns are repaired or nulled, 0 check(s) are advisory. Each rule states its "
        "reason in the pipeline notebook, and Lakeflow expectation metrics report what every "
        "rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_encounter():
    """Contract-v2 public twin of spine_encounter; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_encounter")).selectExpr(
        '`encounter_key` AS `encounter_key`',
        '`encounter_id` AS `encounter_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`parent_encounter_id` AS `parent_encounter_id`',
        '`parentage_status` AS `parentage_status`',
        '`encounter_level` AS `encounter_level`',
        '`class_code` AS `class_code`',
        '`class_display` AS `class_display`',
        '`type_code` AS `type_code`',
        '`type_display` AS `type_display`',
        '`type_class_code` AS `type_class_code`',
        '`type_class_display` AS `type_class_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`arrival_method` AS `arrival_method`',
        '`arrival_confidence` AS `arrival_confidence`',
        '`departure_method` AS `departure_method`',
        '`departure_confidence` AS `departure_confidence`',
        '`length_of_stay_minutes` AS `length_of_stay_minutes`',
        '`scheduled_start` AS `scheduled_start`',
        '`scheduled_end` AS `scheduled_end`',
        '`registration_datetime` AS `registration_datetime`',
        '`inpatient_admit_datetime` AS `inpatient_admit_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`workflow_complete_datetime` AS `workflow_complete_datetime`',
        '`raw_arrival_datetime` AS `raw_arrival_datetime`',
        '`raw_departure_datetime` AS `raw_departure_datetime`',
        '`admission_source_code` AS `admission_source_code`',
        '`admission_source_display` AS `admission_source_display`',
        '`discharge_destination_code` AS `discharge_destination_code`',
        '`discharge_destination_display` AS `discharge_destination_display`',
        '`responsible_service_code` AS `responsible_service_code`',
        '`responsible_service_display` AS `responsible_service_display`',
        '`specialty_code` AS `specialty_code`',
        '`specialty_display` AS `specialty_display`',
        '`current_location_key` AS `current_location_key`',
        '`organization_id` AS `organization_id`',
        '`service_provider_organization_key` AS `service_provider_organization_key`',
        '`reason_for_visit` AS `reason_for_visit`',
        '`attendance_evidence` AS `attendance_evidence`',
        '`attendance_witness_count` AS `attendance_witness_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_ENCOUNTER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.encounter_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_ENCOUNTER_IDENTIFIER_SELECT = [
    '`encounter_identifier_key` AS `encounter_identifier_key`',
    '`source_identifier_id` AS `source_identifier_id`',
    '`encounter_id` AS `encounter_id`',
    '`identifier_system` AS `identifier_system`',
    '`identifier_type_code` AS `identifier_type_code`',
    '`identifier_value` AS `identifier_value`',
    '`status` AS `status`',
    '`current_ind` AS `current_ind`',
    '`multi_active_ind` AS `multi_active_ind`',
    "CASE WHEN CAST(`valid_from` AS DATE) = DATE'1800-01-01' THEN NULL ELSE `valid_from` END AS `valid_from`",
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_encounter_identifier_metadata; source history stays on the main research table.
SPINE_ENCOUNTER_IDENTIFIER_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because an identifier for an encounter that is not in the
    # spine identifies nothing a researcher can join to. That was 318,195 orphaned rows plus
    # 0 that already had no encounter_id, out of 142,054,710.
    "gold.spine.encounter_identifier.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}

SPINE_ENCOUNTER_IDENTIFIER_COLUMN_COMMENTS = {
    "encounter_identifier_key": "Deterministic SHA-256 key minted from the source alias row.",
    "source_identifier_id": "Native map_encounter_identifier SOURCE_PK rendered as text.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "identifier_system": "Type-scoped identifier system URI (urn:cerner:encntr_alias:<normalized alias type>) — 23.17M values live under multiple alias types",
    "identifier_type_code": "Source alias type verbatim.",
    "identifier_value": "Identifier value; published and IG-governed at serve time",
    "status": "Alias lifecycle status.",
    "current_ind": "Bronze current-alias indicator.",
    "multi_active_ind": "Bronze multiple-active-aliases indicator for this encounter and type.",
    "valid_from": "Alias effective-from timestamp.",
    "valid_to": "Alias effective-to timestamp when supplied.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "loaded_at": "map_encounter_identifier.PIPELINE_UPDT_DT_TM carried unchanged from the contributing bronze alias row. This records bronze processing provenance, not identifier validity or the Silver refresh time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_encounter_identifier"),
    comment="Internal quality-controlled twin of spine_encounter_identifier: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(SPINE_ENCOUNTER_IDENTIFIER_MANDATORY_RULES)
def _gold_qc_spine_encounter_identifier():
    """Quality-controlled twin of journey_spine.encounter_identifier."""
    df = _qc(
        "spine_encounter_identifier",
        SPINE_ENCOUNTER_IDENTIFIER_SELECT,
        fk_columns=["encounter_id"],
    )
    return _with_comments(df, SPINE_ENCOUNTER_IDENTIFIER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.encounter_identifier"),
    comment=(
        "One source encounter identifier assignment (FIN and sibling alias types) from "
        "map_encounter_identifier; reference surface parallel to person_identifier. Gold QC "
        "twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 0 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_encounter_identifier():
    """Contract-v2 public twin of spine_encounter_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_encounter_identifier")).selectExpr(
        '`encounter_identifier_key` AS `encounter_identifier_key`',
        '`source_identifier_id` AS `source_identifier_id`',
        '`encounter_id` AS `encounter_id`',
        '`identifier_system` AS `identifier_system`',
        '`identifier_type_code` AS `identifier_type_code`',
        '`identifier_value` AS `identifier_value`',
        '`status` AS `status`',
        '`current_ind` AS `current_ind`',
        '`multi_active_ind` AS `multi_active_ind`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_ENCOUNTER_IDENTIFIER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.episode ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_EPISODE_SELECT = [
    '`episode_key` AS `episode_key`',
    '`episode_id` AS `episode_id`',
    '`person_id` AS `person_id`',
    '`subject_key` AS `subject_key`',
    '`episode_display` AS `episode_display`',
    "CASE WHEN UPPER(TRIM(CAST(`episode_type_code` AS STRING))) = '0' THEN NULL ELSE `episode_type_code` END AS `episode_type_code`",
    '`episode_type_display` AS `episode_type_display`',
    "CASE WHEN UPPER(TRIM(CAST(`status_code` AS STRING))) = '0' THEN NULL ELSE `status_code` END AS `status_code`",
    '`status_display` AS `status_display`',
    '`period_start` AS `period_start`',
    'CASE WHEN `period_start` IS NOT NULL AND `period_end` IS NOT NULL AND `period_start` > `period_end` THEN NULL ELSE `period_end` END AS `period_end`',
    '`breach_datetime` AS `breach_datetime`',
    '`pause_days` AS `pause_days`',
    "CASE WHEN UPPER(TRIM(CAST(`close_reason_code` AS STRING))) = '0' THEN NULL ELSE `close_reason_code` END AS `close_reason_code`",
    '`close_reason_display` AS `close_reason_display`',
    "CASE WHEN UPPER(TRIM(CAST(`service_category_code` AS STRING))) = '0' THEN NULL ELSE `service_category_code` END AS `service_category_code`",
    '`service_category_display` AS `service_category_display`',
    "CASE WHEN UPPER(TRIM(CAST(`referring_facility_code` AS STRING))) = '0' THEN NULL ELSE `referring_facility_code` END AS `referring_facility_code`",
    '`referring_facility_display` AS `referring_facility_display`',
    "CASE WHEN UPPER(TRIM(CAST(`contributor_system_code` AS STRING))) = '0' THEN NULL ELSE `contributor_system_code` END AS `contributor_system_code`",
    '`contributor_system_display` AS `contributor_system_display`',
    '`direct_encounter_key` AS `direct_encounter_key`',
    '`direct_encounter_id` AS `direct_encounter_id`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_episode_metadata; source history stays on the main research table.
SPINE_EPISODE_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 7,799,236
    # rows (1.28e-05%) when profiled on 2026-08-24.
    "gold.spine.episode.record_status_effective_to.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_EPISODE_COLUMN_COMMENTS = {
    "episode_key": "Deterministic SHA-256 key minted from Millennium EPISODE_ID.",
    "episode_id": "Millennium EPISODE_ID; primary key of this table.",
    "person_id": "Millennium PERSON_ID as BIGINT.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "episode_display": "Source episode display name.",
    "episode_type_code": "Source episode type code.",
    "episode_type_display": "Source episode type display.",
    "status_code": "Source episode status code.",
    "status_display": "Source episode status display.",
    "period_start": "Sentinel-cleaned episode begin timestamp (bronze DQ triplet).",
    "period_end": "Sentinel-cleaned episode end timestamp when supplied.",
    "breach_datetime": "Sentinel-cleaned episode breach timestamp when supplied.",
    "pause_days": "Source pause-day count.",
    "close_reason_code": "Source close-reason code.",
    "close_reason_display": "Source close-reason display.",
    "service_category_code": "Source service-category code.",
    "service_category_display": "Source service-category display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "contributor_system_code": "Source contributor-system code.",
    "contributor_system_display": "Source contributor-system display.",
    "direct_encounter_key": "Deterministic encounter key for the agreement-gated direct reference; evidence only.",
    "direct_encounter_id": "Millennium ENCNTR_ID for the agreement-gated direct reference; evidence only.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "record_status": "Derived map_episode row status: superseded when SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; otherwise active. A null SOURCE_PRESENT_IND defaults to present and a null ACTIVE_IND defaults to 1. Source absence maps to superseded here, not retracted; this is not the clinical episode status code.",
    "record_status_effective_from": "map_episode.ACTIVE_STATUS_DT_TM carried unchanged as the source active-status timestamp. No episode-start or ingestion-clock fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "For a superseded episode, the first available of map_episode.END_EFFECTIVE_DT_TM earlier than 2100-01-01, ACTIVE_STATUS_DT_TM, and ADC_UPDT. Null for an active episode or when every fallback is unavailable. This is a derived end boundary and may use ingestion time; it is not a Boolean active indicator.",
    "loaded_at": "map_episode.ADC_UPDT carried unchanged from the contributing bronze episode row. This is ingestion provenance, not the Silver refresh time or the clinical episode start/end; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_episode"),
    comment="Internal quality-controlled twin of spine_episode: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_EPISODE_ADVISORY_RULES)
def _gold_qc_spine_episode():
    """Quality-controlled twin of journey_spine.episode."""
    # 618 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_episode",
        SPINE_EPISODE_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_EPISODE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.episode"),
    comment=(
        "One Millennium episode container populated from the governed feed map_episode; "
        "source-backed only, never heuristic. Gold QC twin of the silver product: 9 columns "
        "are repaired or nulled, 1 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_episode():
    """Contract-v2 public twin of spine_episode; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_episode")).selectExpr(
        '`episode_key` AS `episode_key`',
        '`episode_id` AS `episode_id`',
        '`person_id` AS `person_id`',
        '`subject_key` AS `subject_key`',
        '`episode_display` AS `episode_display`',
        '`episode_type_code` AS `episode_type_code`',
        '`episode_type_display` AS `episode_type_display`',
        '`status_code` AS `status_code`',
        '`status_display` AS `status_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`breach_datetime` AS `breach_datetime`',
        '`pause_days` AS `pause_days`',
        '`close_reason_code` AS `close_reason_code`',
        '`close_reason_display` AS `close_reason_display`',
        '`service_category_code` AS `service_category_code`',
        '`service_category_display` AS `service_category_display`',
        '`referring_facility_code` AS `referring_facility_code`',
        '`referring_facility_display` AS `referring_facility_display`',
        '`contributor_system_code` AS `contributor_system_code`',
        '`contributor_system_display` AS `contributor_system_display`',
        '`direct_encounter_key` AS `direct_encounter_key`',
        '`direct_encounter_id` AS `direct_encounter_id`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_EPISODE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.episode_encounter ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_EPISODE_ENCOUNTER_SELECT = [
    '`episode_encounter_key` AS `episode_encounter_key`',
    '`episode_encounter_reltn_id` AS `episode_encounter_reltn_id`',
    '`episode_id` AS `episode_id`',
    '`encounter_id` AS `encounter_id`',
    '`relation_status_code` AS `relation_status_code`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_episode_encounter_metadata; source history stays on the main research table.
SPINE_EPISODE_ENCOUNTER_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because this table is nothing but the pair of keys, so a link
    # with an end missing links nothing. That was 71,166 orphaned rows plus 0 that already
    # had no encounter_id, out of 26,812,360.
    "gold.spine.episode_encounter.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",

    # The flow nulls episode_id when it names a parent the spine does not have, and this
    # rule then drops the row, because this table is nothing but the pair of keys, so a link
    # with an end missing links nothing. That was 20,686 orphaned rows plus 0 that already
    # had no episode_id, out of 26,812,360.
    "gold.spine.episode_encounter.episode_id.fk_containment": "`episode_id` IS NOT NULL",
}

SPINE_EPISODE_ENCOUNTER_COLUMN_COMMENTS = {
    "episode_encounter_key": "Deterministic SHA-256 relationship key.",
    "episode_encounter_reltn_id": "Millennium EPISODE_ENCNTR_RELTN_ID; primary key of this table.",
    "episode_id": "Millennium EPISODE_ID; joins to spine_episode.episode_id.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "relation_status_code": "Source relation classification including the orphan marker for episode IDs absent from mill_episode.",
    "source_duplicate_count": "Raw byte-identical duplicate rows collapsed into this canonical row by bronze.",
    "valid_from": "Membership effective-from timestamp.",
    "valid_to": "Membership effective-to timestamp when supplied.",
    "source_table": "Registered bronze source table.",
    "source_row_id": "Native source row identifier.",
    "record_status": "Derived map_episode_encounter relationship-row status: superseded when SOURCE_PRESENT_IND is false or ACTIVE_IND is not 1; otherwise active. Null presence and activity flags default to present and 1 respectively. Source absence maps to superseded, not retracted; this does not describe the encounter's clinical status.",
    "loaded_at": "map_episode_encounter.ADC_UPDT carried unchanged from the contributing bronze relationship row. This is ingestion provenance, not the Silver refresh time or relationship-validity time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_episode_encounter"),
    comment="Internal quality-controlled twin of spine_episode_encounter: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(SPINE_EPISODE_ENCOUNTER_MANDATORY_RULES)
def _gold_qc_spine_episode_encounter():
    """Quality-controlled twin of journey_spine.episode_encounter."""
    df = _qc(
        "spine_episode_encounter",
        SPINE_EPISODE_ENCOUNTER_SELECT,
        fk_columns=["encounter_id", "episode_id"],
    )
    return _with_comments(df, SPINE_EPISODE_ENCOUNTER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.episode_encounter"),
    comment=(
        "N-to-M episode-to-encounter membership from the governed feed map_episode_encounter; "
        "multi-relation pairs preserved. Relations naming an episode or an encounter the "
        "spine does not have are dropped, so both keys resolve. Gold QC twin of the silver "
        "product: 2 columns are repaired or nulled, 2 rule(s) drop rows, 0 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_episode_encounter():
    """Contract-v2 public twin of spine_episode_encounter; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_episode_encounter")).selectExpr(
        '`episode_encounter_key` AS `episode_encounter_key`',
        '`episode_encounter_reltn_id` AS `episode_encounter_reltn_id`',
        '`episode_id` AS `episode_id`',
        '`encounter_id` AS `encounter_id`',
        '`relation_status_code` AS `relation_status_code`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_EPISODE_ENCOUNTER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.journey ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_SELECT = [
    '`journey_key` AS `journey_key`',
    '`parent_journey_key` AS `parent_journey_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`journey_type_code` AS `journey_type_code`',
    '`journey_type_display` AS `journey_type_display`',
    '`period_start` AS `period_start`',
    "CASE WHEN `period_start` IS NOT NULL AND `period_end` IS NOT NULL AND `period_start` > `period_end` THEN NULL ELSE CASE WHEN CAST(`period_end` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `period_end` END END AS `period_end`",
    '`status_code` AS `status_code`',
    '`defining_coding_system` AS `defining_coding_system`',
    '`defining_code` AS `defining_code`',
    '`defining_display` AS `defining_display`',
    '`outcome_code` AS `outcome_code`',
    '`outcome_display` AS `outcome_display`',
    '`source_journey_identifier` AS `source_journey_identifier`',
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_journey_metadata; source history stays on the main research table.
SPINE_JOURNEY_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 25,297 of 824,072
    # rows (3.07%) when profiled on 2026-08-24.
    "gold.spine.journey.period_end.future_owner":
        "NOT COALESCE((CAST(`period_end` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 6,633 of 824,072
    # rows (0.805%) when profiled on 2026-08-24.
    "gold.spine.journey.period_start.future_owner":
        "NOT COALESCE((CAST(`period_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_JOURNEY_COLUMN_COMMENTS = {
    "journey_key": "Deterministic SHA-256 journey key.",
    "parent_journey_key": "Deterministic SHA-256 parent journey key for governed nesting.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native primary-subject foreign key when resolved.",
    "journey_type_code": "Controlled journey type.",
    "journey_type_display": "Human-readable journey type.",
    "period_start": "Earliest source-supported journey boundary.",
    "period_end": "Latest source-supported journey boundary.",
    "status_code": "Normalized journey lifecycle state.",
    "defining_coding_system": "Coding system for the constructor-defining concept.",
    "defining_code": "Constructor-defining concept code.",
    "defining_display": "Constructor-defining concept display.",
    "outcome_code": "Source pregnancy outcome code where supplied.",
    "outcome_display": "Source pregnancy outcome display where supplied.",
    "source_journey_identifier": "Verbatim source pregnancy identifier.",
    "construction_rule": "Governed constructor name.",
    "construction_version": "Governed constructor version.",
    "source_system": "Source-system label for these maternity-derived records: millennium-maternity.",
    "source_table": "Fully qualified configured pregnancy source table.",
    "source_row_id": "Stable constructor source-row identifier.",
    "source_update_timestamp": "Greatest of the selected map_mat_pregnancy row's MAT_RECORD_UPDATED_DT, MSDS_RECORD_UPDATED_DT and BIRTH_ADC_UPDT, and the maximum map_mat_birth.BirthSourceRecordUpdatedDateTime across its Pregnancy_ID group. Pregnancy rows are selected by the existing live/load/person/hash rank before this join. The same value is carried to the pregnancy and all trimester rows; it is not just the birth timestamp or the current Silver refresh time.",
    "loaded_at": "Greatest of ADC_UPDT from the selected map_mat_pregnancy row and maximum map_mat_birth.ADC_UPDT across all rows in the Pregnancy_ID group. The selected pregnancy row, not a maximum over all pregnancy versions, contributes its clock. Pregnancy and trimester rows carry the same value; clinical period boundaries and the current Silver refresh time are not used.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_journey"),
    comment="Internal quality-controlled twin of spine_journey: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_JOURNEY_ADVISORY_RULES)
def _gold_qc_spine_journey():
    """Quality-controlled twin of journey_spine.journey."""
    # 4 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_journey",
        SPINE_JOURNEY_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_JOURNEY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.journey"),
    comment=(
        "One deterministic longitudinal episode of care, including nested structural child "
        "journeys. Gold QC twin of the silver product: 2 columns are repaired or nulled, 2 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_journey():
    """Contract-v2 public twin of spine_journey; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey")).selectExpr(
        '`journey_key` AS `journey_key`',
        '`parent_journey_key` AS `parent_journey_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`journey_type_code` AS `journey_type_code`',
        '`journey_type_display` AS `journey_type_display`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`status_code` AS `status_code`',
        '`defining_coding_system` AS `defining_coding_system`',
        '`defining_code` AS `defining_code`',
        '`defining_display` AS `defining_display`',
        '`outcome_code` AS `outcome_code`',
        '`outcome_display` AS `outcome_display`',
        '`source_journey_identifier` AS `source_journey_identifier`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_JOURNEY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.journey_link ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_LINK_SELECT = [
    '`journey_link_key` AS `journey_link_key`',
    '`journey_key` AS `journey_key`',
    '`member_type` AS `member_type`',
    '`encounter_id` AS `encounter_id`',
    '`patient_event_key` AS `patient_event_key`',
    '`role_code` AS `role_code`',
    "CASE WHEN CAST(`member_start_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`member_start_datetime` AS DATE)) < 9999 OR CAST(`member_start_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`member_start_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`member_start_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `member_start_datetime` END AS `member_start_datetime`",
    "CASE WHEN CAST(`member_end_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`member_end_datetime` AS DATE)) < 9999 THEN NULL ELSE `member_end_datetime` END AS `member_end_datetime`",
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_journey_link_metadata; source history stays on the main research table.
SPINE_JOURNEY_LINK_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 646,884,263 rows of
    # 646,884,263 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(record_status = 'active')",
}

SPINE_JOURNEY_LINK_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 55,712 of
    # 646,884,263 rows (0.00861%) when profiled on 2026-08-24.
    "gold.spine.journey_link.member_end_datetime.future_owner":
        "NOT COALESCE((CAST(`member_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 56,267 of
    # 646,884,263 rows (0.0087%) when profiled on 2026-08-24.
    "gold.spine.journey_link.member_start_datetime.future_owner":
        "NOT COALESCE((CAST(`member_start_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 646,884,263 at the profile.
    "gold.spine.journey_link.record_status.default_view_active": "record_status = 'active'",

    # Left as a warning because it fires on 15,386,217 of 646,884,263 rows (2.38%) when
    # profiled on 2026-08-24 -- at that rate the rule's assumption about what
    # member_start_datetime and member_end_datetime mean is the thing in doubt, not the
    # data. The inverted gaps are mostly minutes, which reads as two clocks rather than two
    # events in the wrong order.
    "gold.spine.journey_link.table.ordering_violation_member_start_datetime_member_end_datetime":
        "NOT COALESCE((`member_start_datetime` IS NOT NULL AND `member_end_datetime` IS NOT NULL AND `member_start_datetime` > `member_end_datetime`), FALSE)",
}

SPINE_JOURNEY_LINK_COLUMN_COMMENTS = {
    "journey_link_key": "Deterministic SHA-256 journey-membership key.",
    "journey_key": "Deterministic SHA-256 journey key.",
    "member_type": "Kind of linked member.",
    "encounter_id": "Millennium ENCNTR_ID for encounter members.",
    "patient_event_key": "Deterministic SHA-256 patient-event key for event members.",
    "role_code": "Controlled link role.",
    "member_start_datetime": "Linked member start timestamp used by the constructor.",
    "member_end_datetime": "Linked member end timestamp used by the constructor.",
    "construction_rule": "Governed overlap-construction rule; episode_membership populated from the governed episode feeds.",
    "construction_version": "Governed overlap-construction version.",
    "source_system": "Constructor source-system identifier.",
    "source_table": "Fully qualified configured journey-constructor source table.",
    "source_row_id": "Stable constructor source-row identifier.",
    "record_status": "Always active for all retained encounter-overlap, patient-event-overlap and episode-membership links, regardless of the linked journey or event's status. Episode-membership eligibility separately requires active episode-encounter relations and excludes SOURCE_EPISODE_ABSENT; that filter does not create superseded or retracted link labels.",
    "loaded_at": "Encounter overlap: greatest of constructed journey and canonical encounter loaded_at. Patient-event overlap: greatest of journey and internal event-index row loaded_at, not a maximum over other events. Episode membership: greatest of grouped anchor, member-relation and member-encounter clocks across contributing episode paths. The anchor clock includes the journey/anchor encounter overlap clock and anchor episode-encounter relation clock before collapse by journey and episode; member relations are collapsed by episode and encounter. Final grouping is by journey and member encounter. No current Silver refresh time is substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_journey_link"),
    comment="Internal quality-controlled twin of spine_journey_link: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(SPINE_JOURNEY_LINK_MANDATORY_RULES)
@dp.expect_all(SPINE_JOURNEY_LINK_ADVISORY_RULES)
def _gold_qc_spine_journey_link():
    """Quality-controlled twin of journey_spine.journey_link."""
    df = _qc("spine_journey_link", SPINE_JOURNEY_LINK_SELECT)
    return _with_comments(df, SPINE_JOURNEY_LINK_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.journey_link"),
    comment=(
        "One deterministic journey-to-encounter or journey-to-event overlap link. Gold QC "
        "twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_journey_link():
    """Contract-v2 public twin of spine_journey_link; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey_link")).selectExpr(
        '`journey_link_key` AS `journey_link_key`',
        '`journey_key` AS `journey_key`',
        '`member_type` AS `member_type`',
        '`encounter_id` AS `encounter_id`',
        '`patient_event_key` AS `patient_event_key`',
        '`role_code` AS `role_code`',
        '`member_start_datetime` AS `member_start_datetime`',
        '`member_end_datetime` AS `member_end_datetime`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_JOURNEY_LINK_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.journey_participant ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_JOURNEY_PARTICIPANT_SELECT = [
    '`journey_participant_key` AS `journey_participant_key`',
    '`journey_key` AS `journey_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`role_code` AS `role_code`',
    '`role_display` AS `role_display`',
    '`valid_from` AS `valid_from`',
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN CAST(`valid_to` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_journey_participant_metadata; source history stays on the main research table.
SPINE_JOURNEY_PARTICIPANT_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 7,368 of 1,010,584
    # rows (0.729%) when profiled on 2026-08-24.
    "gold.spine.journey_participant.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 25,297 of
    # 1,010,584 rows (2.5%) when profiled on 2026-08-24.
    "gold.spine.journey_participant.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_JOURNEY_PARTICIPANT_COLUMN_COMMENTS = {
    "journey_participant_key": "Deterministic SHA-256 journey-participant key.",
    "journey_key": "Deterministic SHA-256 journey key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for participant subject_key.",
    "person_id": "Millennium PERSON_ID; native participant foreign key when resolved.",
    "role_code": "Controlled role in the journey.",
    "role_display": "Human-readable participant role.",
    "valid_from": "Role validity start.",
    "valid_to": "Role validity end.",
    "construction_rule": "Governed participant-construction rule.",
    "construction_version": "Governed participant-construction version.",
    "source_system": "Source-system label for these maternity-derived records: millennium-maternity.",
    "source_table": "Fully qualified configured bronze source table.",
    "source_row_id": "Stable source-row identifier supporting the role.",
    "record_status": "Mother rows are superseded only when their constructed pregnancy/trimester journey status_code is superseded; completed and all other journey statuses map to active. Baby rows are superseded when map_mat_birth.PregnancySource_DELETE_IND cast to LONG is nonzero, otherwise active; null defaults zero. Neither arm emits retracted, and the union does not reconcile statuses between mother and baby.",
    "source_update_timestamp": "Mother participants inherit the pregnancy/trimester journey clock: greatest of the selected pregnancy row's MAT_RECORD_UPDATED_DT, MSDS_RECORD_UPDATED_DT and BIRTH_ADC_UPDT, plus maximum BirthSourceRecordUpdatedDateTime across its map_mat_birth Pregnancy_ID group. Baby participants use greatest of their own map_mat_birth BirthSourceRecordUpdatedDateTime, PregnancySourceRecordUpdatedDateTime and NNUSourceLastUpdate. These clocks are carried per arm without a cross-arm maximum.",
    "loaded_at": "Mother participants inherit the pregnancy/trimester journey load clock: greatest of selected map_mat_pregnancy.ADC_UPDT and maximum map_mat_birth.ADC_UPDT for the Pregnancy_ID group. Baby participants use only their own map_mat_birth.ADC_UPDT. The two arms are unioned without a mother/baby clock maximum or substitution of clinical validity times or the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_journey_participant"),
    comment="Internal quality-controlled twin of spine_journey_participant: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_JOURNEY_PARTICIPANT_ADVISORY_RULES)
def _gold_qc_spine_journey_participant():
    """Quality-controlled twin of journey_spine.journey_participant."""
    # 106 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_journey_participant",
        SPINE_JOURNEY_PARTICIPANT_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_JOURNEY_PARTICIPANT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.journey_participant"),
    comment=(
        "One journey-to-person role interval, retaining unresolved participants through "
        "subject_key. Gold QC twin of the silver product: 2 columns are repaired or nulled, 2 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_journey_participant():
    """Contract-v2 public twin of spine_journey_participant; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_journey_participant")).selectExpr(
        '`journey_participant_key` AS `journey_participant_key`',
        '`journey_key` AS `journey_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`role_code` AS `role_code`',
        '`role_display` AS `role_display`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_JOURNEY_PARTICIPANT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.location_stay ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_LOCATION_STAY_SELECT = [
    '`location_stay_key` AS `location_stay_key`',
    '`encounter_id` AS `encounter_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`location_code` AS `location_code`',
    "CASE WHEN CAST(`period_start` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`period_start` AS DATE)) < 9999 OR CAST(`period_start` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `period_start` END AS `period_start`",
    "CASE WHEN CAST(`period_end` AS DATE) = DATE'2100-12-31' OR CAST(`period_end` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`period_end` AS DATE)) < 9999 THEN NULL ELSE `period_end` END AS `period_end`",
    "CASE WHEN UPPER(TRIM(CAST(`nurse_unit_code` AS STRING))) = '0' THEN NULL ELSE `nurse_unit_code` END AS `nurse_unit_code`",
    '`nurse_unit_display` AS `nurse_unit_display`',
    "CASE WHEN UPPER(TRIM(CAST(`building_code` AS STRING))) = '0' THEN NULL ELSE `building_code` END AS `building_code`",
    '`building_display` AS `building_display`',
    "CASE WHEN UPPER(TRIM(CAST(`facility_code` AS STRING))) = '0' THEN NULL ELSE `facility_code` END AS `facility_code`",
    '`facility_display` AS `facility_display`',
    "CASE WHEN UPPER(TRIM(CAST(`room_code` AS STRING))) = '0' THEN NULL ELSE `room_code` END AS `room_code`",
    '`room_display` AS `room_display`',
    "CASE WHEN UPPER(TRIM(CAST(`bed_code` AS STRING))) = '0' THEN NULL ELSE `bed_code` END AS `bed_code`",
    '`bed_display` AS `bed_display`',
    "CASE WHEN UPPER(TRIM(CAST(`service_code` AS STRING))) = '0' THEN NULL ELSE `service_code` END AS `service_code`",
    '`service_display` AS `service_display`',
    "CASE WHEN UPPER(TRIM(CAST(`transfer_reason_code` AS STRING))) = '0' THEN NULL ELSE `transfer_reason_code` END AS `transfer_reason_code`",
    '`transfer_reason_display` AS `transfer_reason_display`',
    '`source_history_row_count` AS `source_history_row_count`',
    '`first_history_event_sequence` AS `first_history_event_sequence`',
    '`last_history_event_sequence` AS `last_history_event_sequence`',
    "CASE WHEN UPPER(TRIM(CAST(`confidentiality_code` AS STRING))) = '0' THEN NULL ELSE `confidentiality_code` END AS `confidentiality_code`",
    '`vip_ind` AS `vip_ind`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_location_stay_metadata; source history stays on the main research table.
SPINE_LOCATION_STAY_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.period_end.future_owner":
        "NOT COALESCE((CAST(`period_end` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.period_start.future_owner":
        "NOT COALESCE((CAST(`period_start` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 65,583,474
    # rows (1.52e-06%) when profiled on 2026-08-24.
    "gold.spine.location_stay.record_status_effective_to.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_LOCATION_STAY_COLUMN_COMMENTS = {
    "location_stay_key": "Deterministic SHA-256 modelled location-stop key.",
    "encounter_id": "Millennium ENCNTR_ID; joins to spine_encounter.encounter_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT.",
    "location_code": "Native Millennium location code resolved against reference_location.",
    "period_start": "Earliest `LOCATION_STOP_START_DT_TM` across the source history rows grouped into this encounter location stop.",
    "period_end": "Latest `LOCATION_STOP_END_DT_TM` across the source history rows grouped into this encounter location stop; the grouped source timestamp is retained without sentinel removal.",
    "nurse_unit_code": "Historical nurse-unit code.",
    "nurse_unit_display": "Historical nurse-unit display.",
    "building_code": "Historical building code.",
    "building_display": "Historical building display.",
    "facility_code": "Historical facility code.",
    "facility_display": "Historical facility display.",
    "room_code": "Historical room code.",
    "room_display": "Historical room display.",
    "bed_code": "Historical bed code.",
    "bed_display": "Historical bed display.",
    "service_code": "Service code during the stop.",
    "service_display": "Service display during the stop.",
    "transfer_reason_code": "Source transfer-reason code.",
    "transfer_reason_display": "Source transfer-reason display.",
    "source_history_row_count": "Number of history rows grouped into the stop.",
    "first_history_event_sequence": "First source history sequence in the stop.",
    "last_history_event_sequence": "Last source history sequence in the stop.",
    "confidentiality_code": "Source confidentiality code.",
    "vip_ind": "Source VIP indicator.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Deterministic grouped source-row identity.",
    "record_status": "Derived location-stop status: superseded when the grouped maximum map_patient_journey.LOCATION_STOP_END_DT_TM is earlier than 2100-01-01; otherwise active, including an all-null or open-ended maximum. Grouping is by ENCNTR_ID and the stop discriminator. This is a status label, not an end timestamp or a comparison with the current wall clock.",
    "record_status_effective_to": "Maximum map_patient_journey.LOCATION_STOP_END_DT_TM within the encounter/stop-discriminator group, returned only when that maximum is earlier than 2100-01-01. Otherwise null, including all-null or open-ended maxima. This uses the grouped stop end, not the earliest contributing boundary.",
    "loaded_at": "Maximum map_patient_journey.ADC_UPDT within the ENCNTR_ID and stop-discriminator group. The discriminator uses LOCATION_STOP_SEQUENCE when present, otherwise ENCNTR_LOC_HIST_ID. This is contributing ingestion provenance, not the Silver refresh time; null when all contributing ADC_UPDT values are null.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_location_stay"),
    comment="Internal quality-controlled twin of spine_location_stay: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_LOCATION_STAY_ADVISORY_RULES)
def _gold_qc_spine_location_stay():
    """Quality-controlled twin of journey_spine.location_stay."""
    # 9,473 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 24 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_location_stay",
        SPINE_LOCATION_STAY_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return _with_comments(df, SPINE_LOCATION_STAY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.location_stay"),
    comment=(
        "One grouped physical location stop per encounter, retaining raw historical location "
        "evidence even when the current dimension cannot resolve it. Gold QC twin of the "
        "silver product: 12 columns are repaired or nulled, 3 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_location_stay():
    """Contract-v2 public twin of spine_location_stay; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_location_stay")).selectExpr(
        '`location_stay_key` AS `location_stay_key`',
        '`encounter_id` AS `encounter_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`location_code` AS `location_code`',
        '`period_start` AS `period_start`',
        '`period_end` AS `period_end`',
        '`nurse_unit_code` AS `nurse_unit_code`',
        '`nurse_unit_display` AS `nurse_unit_display`',
        '`building_code` AS `building_code`',
        '`building_display` AS `building_display`',
        '`facility_code` AS `facility_code`',
        '`facility_display` AS `facility_display`',
        '`room_code` AS `room_code`',
        '`room_display` AS `room_display`',
        '`bed_code` AS `bed_code`',
        '`bed_display` AS `bed_display`',
        '`service_code` AS `service_code`',
        '`service_display` AS `service_display`',
        '`transfer_reason_code` AS `transfer_reason_code`',
        '`transfer_reason_display` AS `transfer_reason_display`',
        '`source_history_row_count` AS `source_history_row_count`',
        '`first_history_event_sequence` AS `first_history_event_sequence`',
        '`last_history_event_sequence` AS `last_history_event_sequence`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_LOCATION_STAY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.person ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_SELECT = [
    '`person_id` AS `person_id`',
    '`active` AS `active`',
    "CASE WHEN UPPER(TRIM(CAST(`gender_code` AS STRING))) = '0' THEN NULL ELSE `gender_code` END AS `gender_code`",
    '`gender_display` AS `gender_display`',
    "CASE WHEN UPPER(TRIM(CAST(`ethnicity_code` AS STRING))) = '0' THEN NULL ELSE `ethnicity_code` END AS `ethnicity_code`",
    '`ethnicity_display` AS `ethnicity_display`',
    "CASE WHEN CAST(`birth_date` AS DATE) = DATE'1899-12-30' OR CAST(`birth_date` AS DATE) = DATE'1900-01-01' OR CAST(`birth_date` AS DATE) = DATE'2100-12-31' OR CAST(`birth_date` AS DATE) < DATE'1901-01-01' AND CAST(`birth_date` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_date` END AS `birth_date`",
    "CASE WHEN CAST(`birth_datetime` AS DATE) = DATE'1899-12-30' OR CAST(`birth_datetime` AS DATE) = DATE'1900-01-01' OR CAST(`birth_datetime` AS DATE) = DATE'2100-12-31' OR CAST(`birth_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`birth_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `birth_datetime` END AS `birth_datetime`",
    "CASE WHEN UPPER(TRIM(CAST(`birth_precision_code` AS STRING))) = '0' THEN NULL ELSE `birth_precision_code` END AS `birth_precision_code`",
    '`birth_precision_display` AS `birth_precision_display`',
    '`birth_precision_flag` AS `birth_precision_flag`',
    "CASE WHEN UPPER(TRIM(CAST(`language_code` AS STRING))) = '0' THEN NULL ELSE `language_code` END AS `language_code`",
    '`language_display` AS `language_display`',
    "CASE WHEN UPPER(TRIM(CAST(`marital_status_code` AS STRING))) = '0' THEN NULL ELSE `marital_status_code` END AS `marital_status_code`",
    '`marital_status_display` AS `marital_status_display`',
    "CASE WHEN UPPER(TRIM(CAST(`religion_code` AS STRING))) = '0' THEN NULL ELSE `religion_code` END AS `religion_code`",
    '`religion_display` AS `religion_display`',
    '`deceased_ind` AS `deceased_ind`',
    'CASE WHEN `deceased_datetime` < `birth_datetime` THEN NULL ELSE `deceased_datetime` END AS `deceased_datetime`',
    '`deceased_datetime_precision` AS `deceased_datetime_precision`',
    "CASE WHEN UPPER(TRIM(CAST(`confidentiality_code` AS STRING))) = '0' THEN NULL ELSE `confidentiality_code` END AS `confidentiality_code`",
    '`vip_ind` AS `vip_ind`',
    '`current_address_id` AS `current_address_id`',
    '`latest_known_address_id` AS `latest_known_address_id`',
    '`address_selection_status` AS `address_selection_status`',
    '`current_mrn` AS `current_mrn`',
    '`current_mrn_status` AS `current_mrn_status`',
    '`mrn_selection_status` AS `mrn_selection_status`',
    '`nhs_number` AS `nhs_number`',
    '`nhs_number_status` AS `nhs_number_status`',
    '`nhs_number_selection_status` AS `nhs_number_selection_status`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]
SPINE_PERSON_SELECT += [f"`{name}` AS `{name}`" for name in _s3_axis_columns("ethnicity")]

SPINE_PERSON_COLUMN_COMMENTS = {
    "person_id": "Millennium person identifier and table primary key.",
    "active": "Source active indicator.",
    "gender_code": "Source administrative gender code.",
    "gender_display": "Source administrative gender display.",
    "ethnicity_code": "Source ethnicity code.",
    "ethnicity_display": "Source ethnicity display.",
    "birth_date": "Calendar birth date.",
    "birth_datetime": "Source-compatible birth timestamp.",
    "birth_precision_code": "Source code describing birth-date precision.",
    "birth_precision_display": "Display for the source birth precision.",
    "birth_precision_flag": "Raw source birth precision flag.",
    "language_code": "Preferred language source code.",
    "language_display": "Preferred language display.",
    "marital_status_code": "Source marital-status code.",
    "marital_status_display": "Source marital-status display.",
    "religion_code": "Source religion code.",
    "religion_display": "Source religion display.",
    "deceased_ind": "Whether the source indicates the person is deceased.",
    "deceased_datetime": "Source death timestamp.",
    "deceased_datetime_precision": "Raw source death-time precision.",
    "confidentiality_code": "Source confidentiality level carried for downstream access control.",
    "vip_ind": "Source VIP indicator carried as data.",
    "current_address_id": "Current source address reference when available.",
    "latest_known_address_id": "Latest-known source address reference.",
    "address_selection_status": "Provenance for current versus latest-known address selection.",
    "current_mrn": "Current hospital MRN selected from map_patient_identifier: active alias first, then latest valid end-effective, NULLS LAST, deterministic SOURCE_PK tiebreak; NULL when selection is ambiguous or no alias exists.",
    "current_mrn_status": "Lifecycle status of the selected hospital MRN alias; NULL when selection is ambiguous or no alias exists.",
    "mrn_selection_status": "Selection outcome for the governed hospital MRN alias pool; ambiguous = multiple active aliases",
    "nhs_number": "Current NHS number selected from map_patient_identifier: active alias first, then latest valid end-effective, NULLS LAST, deterministic SOURCE_PK tiebreak; NULL when selection is ambiguous or no alias exists.",
    "nhs_number_status": "Lifecycle status of the selected NHS-number alias; NULL when selection is ambiguous or no alias exists.",
    "nhs_number_selection_status": "Selection outcome for the governed NHS-number alias pool; ambiguous = multiple active aliases",
    "record_status": "Derived person-row status: active only when map_person.active_ind is 1 and end_effective_dt_tm is null or not earlier than 2100-01-01; otherwise superseded, including a missing active_ind. This is a normalized row-status label, not the source numeric ACTIVE_IND.",
    "record_status_effective_from": "Source person active-status timestamp carried from `active_status_dt_tm` without a fallback timestamp.",
    "record_status_effective_to": "Source `end_effective_dt_tm` when earlier than 2100-01-01; otherwise null, including missing or open-ended sentinel timestamps.",
    "source_update_timestamp": "Greatest of map_person.source_updt_dt_tm and the maximum map_patient_identifier.SOURCE_ADC_UPDT in each matching MRN and NHS alias pool. The pool maxima include all aliases, even when the displayed identifier is ambiguous; missing alias clocks fall back to the person clock. This is not the Silver refresh time; null only when all contributing clocks are null.",
    "loaded_at": "Greatest of map_person.ADC_UPDT and the maximum map_patient_identifier.PIPELINE_UPDT_DT_TM in each matching MRN and NHS alias pool. These are pool-wide clocks, not just the displayed winning alias; missing alias clocks fall back to the person clock. This is contributing ingestion provenance, not the Silver refresh time; null only when all contributing clocks are null.",
}
SPINE_PERSON_COLUMN_COMMENTS.update({
    name: "Governed S3b ethnic-category mapping provenance."
    for name in _s3_axis_columns("ethnicity")
})

@dp.materialized_view(
    name=_n("gold_qc._spine_person"),
    comment="Internal quality-controlled twin of spine_person: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_spine_person():
    """Quality-controlled twin of journey_spine.person."""
    df = _qc("spine_person", SPINE_PERSON_SELECT)
    return _with_comments(df, SPINE_PERSON_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.person"),
    comment=(
        "One resolved Millennium person with FHIR-aligned demographics and retained source "
        "lifecycle. Gold QC twin of the silver product: 11 columns are repaired or nulled, 0 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_person():
    """Contract-v2 public twin of spine_person; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person")).selectExpr(
        '`person_id` AS `person_id`',
        '`active` AS `active`',
        '`gender_code` AS `gender_code`',
        '`gender_display` AS `gender_display`',
        '`ethnicity_code` AS `ethnicity_code`',
        '`ethnicity_display` AS `ethnicity_display`',
        '`birth_date` AS `birth_date`',
        '`birth_datetime` AS `birth_datetime`',
        '`birth_precision_code` AS `birth_precision_code`',
        '`birth_precision_display` AS `birth_precision_display`',
        '`birth_precision_flag` AS `birth_precision_flag`',
        '`language_code` AS `language_code`',
        '`language_display` AS `language_display`',
        '`marital_status_code` AS `marital_status_code`',
        '`marital_status_display` AS `marital_status_display`',
        '`religion_code` AS `religion_code`',
        '`religion_display` AS `religion_display`',
        '`deceased_ind` AS `deceased_ind`',
        '`deceased_datetime` AS `deceased_datetime`',
        '`deceased_datetime_precision` AS `deceased_datetime_precision`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`current_address_id` AS `current_address_id`',
        '`latest_known_address_id` AS `latest_known_address_id`',
        '`address_selection_status` AS `address_selection_status`',
        '`current_mrn` AS `current_mrn`',
        '`current_mrn_status` AS `current_mrn_status`',
        '`mrn_selection_status` AS `mrn_selection_status`',
        '`nhs_number` AS `nhs_number`',
        '`nhs_number_status` AS `nhs_number_status`',
        '`nhs_number_selection_status` AS `nhs_number_selection_status`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        *[f"`{name}` AS `{name}`" for name in _s3_axis_columns("ethnicity")],
        '`_ethnicity_gate_pass` AS `_ethnicity_gate_pass`',
        '`_ethnicity_policy_id` AS `_ethnicity_policy_id`',
    )
    return (_s3_gate_direct_public(_with_comments(df, SPINE_PERSON_COLUMN_COMMENTS), "spine_person")
            .drop("_ethnicity_gate_pass", "_ethnicity_policy_id"))

# COMMAND ----------

# ==== journey_spine.person_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_IDENTIFIER_SELECT = [
    '`person_identifier_key` AS `person_identifier_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`identifier_system` AS `identifier_system`',
    "CASE WHEN TRIM(CAST(`identifier_value` AS STRING)) = '' THEN NULL ELSE `identifier_value` END AS `identifier_value`",
    '`identifier_type_code` AS `identifier_type_code`',
    '`status` AS `status`',
    "CASE WHEN CAST(`valid_from` AS DATE) = DATE'1900-01-01' OR CAST(`valid_from` AS DATE) < DATE'1901-01-01' AND CAST(`valid_from` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_from` END AS `valid_from`",
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN CAST(`valid_to` AS DATE) = DATE'2100-12-31' OR CAST(`valid_to` AS DATE) = DATE'1900-01-01' OR CAST(`valid_to` AS DATE) < DATE'1901-01-01' AND CAST(`valid_to` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`source_object` AS `source_object`',
    '`source_identifier_id` AS `source_identifier_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_person_identifier_metadata; source history stays on the main research table.
SPINE_PERSON_IDENTIFIER_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 27,018,061 rows of
    # 27,522,731 that are current and attributable. Superseded versions and rows whose
    # identity was never resolved are not research data, and a consumer who wants them has
    # silver.
    "research_surface": "(identity_status = 'resolved')",

    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because an identifier belonging to no person identifies nothing a
    # researcher can join to. That was 1,383 orphaned rows plus 504,671 that already had no
    # person_id, out of 27,522,731.
    "gold.spine.person_identifier.person_id.fk_containment": "`person_id` IS NOT NULL",
}

SPINE_PERSON_IDENTIFIER_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 504,670 of 27,522,731 at the profile.
    "gold.spine.person_identifier.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 27,522,731
    # rows (7.27e-06%) when profiled on 2026-08-24.
    "gold.spine.person_identifier.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 38 of 27,522,731
    # rows (0.000138%) when profiled on 2026-08-24.
    "gold.spine.person_identifier.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_PERSON_IDENTIFIER_COLUMN_COMMENTS = {
    "person_identifier_key": "B3 bronze field ROW_ID; source value retained unless documented as derived.",
    "subject_key": "deterministic join key over the strongest available subject evidence.",
    "subject_id_system": "Identifier system used to derive subject_key.",
    "person_id": "Resolved Millennium person identifier when available.",
    "identifier_system": "Namespace for the source identifier.",
    "identifier_value": "Source identifier value.",
    "identifier_type_code": "Text label of the alias type derived from the alias type code, distinguishing medical record number from NHS number rows.",
    "status": "Source assignment or matching status.",
    "valid_from": "Identifier validity start.",
    "valid_to": "Identifier validity end.",
    "source_system": "Source system name.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "source_object": "Value describing source object for the person identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_identifier_id": "B3 bronze field ROW_ID; source value retained unless documented as derived.",
    "loaded_at": "Arm-specific clock: map_person.ADC_UPDT for Millennium person IDs; always null for both community key and community number assignments; map_pacs_patient_link.ADC_UPDT for PACS; map_patient_identifier.PIPELINE_UPDT_DT_TM for Millennium aliases; map_endobase_patient.ADC_UPDT for EndoBase; map_cancer_ptl_linkage.ADC_UPDT for both Pathfinder MRN and NHS assignments. The eight arms are unioned without a cross-arm maximum or Silver refresh timestamp.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_person_identifier"),
    comment="Internal quality-controlled twin of spine_person_identifier: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(SPINE_PERSON_IDENTIFIER_MANDATORY_RULES)
@dp.expect_all(SPINE_PERSON_IDENTIFIER_ADVISORY_RULES)
def _gold_qc_spine_person_identifier():
    """Quality-controlled twin of journey_spine.person_identifier."""
    df = _qc(
        "spine_person_identifier",
        SPINE_PERSON_IDENTIFIER_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, SPINE_PERSON_IDENTIFIER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.person_identifier"),
    comment=(
        "One source identifier assignment with an always-populated subject key. Gold QC twin "
        "of the silver product: 4 columns are repaired or nulled, 2 rule(s) drop rows, 3 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_person_identifier():
    """Contract-v2 public twin of spine_person_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person_identifier")).selectExpr(
        '`person_identifier_key` AS `person_identifier_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`identifier_system` AS `identifier_system`',
        '`identifier_value` AS `identifier_value`',
        '`identifier_type_code` AS `identifier_type_code`',
        '`status` AS `status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`source_object` AS `source_object`',
        '`source_identifier_id` AS `source_identifier_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_PERSON_IDENTIFIER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.person_relationship ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_PERSON_RELATIONSHIP_SELECT = [
    '`person_relationship_key` AS `person_relationship_key`',
    '`birth_row_id` AS `birth_row_id`',
    '`source_subject_key` AS `source_subject_key`',
    '`source_person_id` AS `source_person_id`',
    '`target_subject_key` AS `target_subject_key`',
    '`target_person_id` AS `target_person_id`',
    '`relationship_type_code` AS `relationship_type_code`',
    '`inverse_relationship_type_code` AS `inverse_relationship_type_code`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`construction_rule` AS `construction_rule`',
    '`construction_version` AS `construction_version`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _spine_person_relationship_metadata; source history stays on the main research table.
SPINE_PERSON_RELATIONSHIP_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 735 of 186,512
    # rows (0.394%) when profiled on 2026-08-24.
    "gold.spine.person_relationship.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

SPINE_PERSON_RELATIONSHIP_COLUMN_COMMENTS = {
    "person_relationship_key": "Deterministic SHA-256 relationship key.",
    "birth_row_id": "MSDS BirthRow_ID; native source primary key.",
    "source_subject_key": "Always-populated subject key for the relationship source person.",
    "source_person_id": "Resolved source-side person identifier when available.",
    "target_subject_key": "Always-populated subject key for the relationship target person.",
    "target_person_id": "Resolved target-side person identifier when available.",
    "relationship_type_code": "Controlled source-to-target relationship type.",
    "inverse_relationship_type_code": "Controlled inverse relationship type.",
    "valid_from": "Relationship validity start from source evidence.",
    "valid_to": "Relationship validity end when source evidence supplies one.",
    "construction_rule": "Governed relationship-construction rule.",
    "construction_version": "Governed relationship-construction version.",
    "source_system": "Source-system label for these maternity-derived records: millennium-maternity.",
    "source_table": "Fully qualified configured bronze source table.",
    "source_row_id": "Stable source birth-row identifier.",
    "record_status": "Superseded when map_mat_birth.PregnancySource_DELETE_IND cast to LONG is nonzero, otherwise active; null defaults to zero. No retracted status is emitted. This is the pregnancy-enrichment deletion flag carried on the birth row, not a Boolean output or an independent test of birth-source absence.",
    "source_update_timestamp": "Greatest of map_mat_birth.BirthSourceRecordUpdatedDateTime, PregnancySourceRecordUpdatedDateTime and NNUSourceLastUpdate for the contributing birth row. Null inputs are ignored; all null gives null. No separate pregnancy or neonatal row is joined to add a clock, and the current Silver refresh time is not substituted.",
    "loaded_at": "map_mat_birth.ADC_UPDT carried unchanged for the mother-to-child relationship. The three contributing source-update clocks are published separately as source_update_timestamp and do not replace this value; no parent-person or separate pregnancy load clock is joined.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_person_relationship"),
    comment="Internal quality-controlled twin of spine_person_relationship: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(SPINE_PERSON_RELATIONSHIP_ADVISORY_RULES)
def _gold_qc_spine_person_relationship():
    """Quality-controlled twin of journey_spine.person_relationship."""
    df = _qc("spine_person_relationship", SPINE_PERSON_RELATIONSHIP_SELECT)
    return _with_comments(df, SPINE_PERSON_RELATIONSHIP_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.person_relationship"),
    comment=(
        "One source-backed relationship between two people; maternity currently supplies "
        "mother-to-baby links. Gold QC twin of the silver product: 0 columns are repaired or "
        "nulled, 1 check(s) are advisory. Each rule states its reason in the pipeline "
        "notebook, and Lakeflow expectation metrics report what every rule matched on each "
        "update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_person_relationship():
    """Contract-v2 public twin of spine_person_relationship; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_person_relationship")).selectExpr(
        '`person_relationship_key` AS `person_relationship_key`',
        '`birth_row_id` AS `birth_row_id`',
        '`source_subject_key` AS `source_subject_key`',
        '`source_person_id` AS `source_person_id`',
        '`target_subject_key` AS `target_subject_key`',
        '`target_person_id` AS `target_person_id`',
        '`relationship_type_code` AS `relationship_type_code`',
        '`inverse_relationship_type_code` AS `inverse_relationship_type_code`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`construction_rule` AS `construction_rule`',
        '`construction_version` AS `construction_version`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_PERSON_RELATIONSHIP_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_spine.request_thread ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
SPINE_REQUEST_THREAD_SELECT = [
    '`request_thread_key` AS `request_thread_key`',
    '`source_object` AS `source_object`',
    '`source_patient_event_key` AS `source_patient_event_key`',
    '`target_patient_event_key` AS `target_patient_event_key`',
    '`link_type_code` AS `link_type_code`',
    '`subject_key` AS `subject_key`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`request_key` AS `request_key`',
    '`response_key` AS `response_key`',
    "CASE WHEN CAST(`requested_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`requested_datetime` AS DATE)) < 9999 THEN NULL ELSE `requested_datetime` END AS `requested_datetime`",
    'CASE WHEN CAST(`responded_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `responded_datetime` END AS `responded_datetime`',
    '`source_history_row_count` AS `source_history_row_count`',
    '`source_system` AS `source_system`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

SPINE_REQUEST_THREAD_COLUMN_COMMENTS = {
    "request_thread_key": "Deterministic request-edge primary key.",
    "source_object": "Request-thread arm: pathology, medication, or waiting_list.",
    "source_patient_event_key": "Request-side patient event identifier.",
    "target_patient_event_key": "Response-side patient event identifier.",
    "link_type_code": "Controlled directed relationship type.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "person_id": "Resolved Millennium PERSON_ID as BIGINT when available.",
    "encounter_id": "Millennium ENCNTR_ID as BIGINT when available.",
    "request_key": "Request-side source identifier retained under the governed key name.",
    "response_key": "Response-side source identifier retained under the governed key name.",
    "requested_datetime": "Source request timestamp.",
    "responded_datetime": "Source report or measurement timestamp.",
    "source_history_row_count": "Number of source rows supporting the edge.",
    "source_system": "Source system identifier.",
    "source_table": "Fully qualified configured source table.",
    "source_row_id": "Representative stable source row identifier.",
    "record_status": "Arm-specific edge status: pathology is literal active. Medication is retracted when the order SOURCE_PRESENT_IND is false (null defaults to present), otherwise superseded when administration CE_VALID_UNTIL_DT_TM is before 2100-01-01, otherwise active. Waiting-list edges copy the selected representative version's status: superseded for source absence or non-current status (null presence defaults true; null IS_CURRENT defaults false), otherwise active. This is not a single source workflow-status code.",
    "record_status_effective_from": "Arm-specific history start: null for pathology edges; map_medication_order.ORIG_ORDER_DT_TM for medication edges; selected map_waiting_list representative BEG_EFFECTIVE_DT_TM for waiting-list edges. These are request/version-time proxies, not a uniformly observed status transition. Missing timestamps remain null.",
    "record_status_effective_to": "Pathology edges always have a null end. Medication uses order SOURCE_ABSENT_DETECTED_TS when retracted, without fallback even if null; otherwise finite administration CE_VALID_UNTIL_DT_TM when superseded, else null. Waiting-list edges copy their representative's end: for superseded versions, coalesce(SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, ADC_UPDT), with no far-future sentinel filter; active versions have a null end.",
    "source_update_timestamp": "Arm-specific provenance: null for pathology edges; greatest of map_med_admin.ADC_UPDT, map_medication_order.SOURCE_ADC_UPDT and map_medication_order.ADC_UPDT for medication edges; selected map_waiting_list representative SOURCE_ADC_UPDT for waiting-list edges. This is not uniformly a native source modification clock. The appointment existence/person join contributes no timestamp; no Silver refresh timestamp is substituted.",
    "loaded_at": "Arm-specific contributing load clock: pathology takes the greatest requested-test ADC_UPDT and the linked report-series maximum ADC_UPDT across all its versions; medication takes the greatest administration ADC_UPDT and order ADC_UPDT; waiting-list takes ADC_UPDT of the selected whole representative version. The appointment join adds no clock, and the union does not aggregate across arms. Null when that edge has no contributing load timestamp; not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._spine_request_thread"),
    comment="Internal quality-controlled twin of spine_request_thread: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_spine_request_thread():
    """Quality-controlled twin of journey_spine.request_thread."""
    # 343,157 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 479 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "spine_request_thread",
        SPINE_REQUEST_THREAD_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return _with_comments(df, SPINE_REQUEST_THREAD_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_spine.request_thread"),
    comment="Directed indexed-event edges for pathology order-to-report, medication order-to-administration, and waiting-list entry-to-appointment routes. Pathology edges are accession-scoped co-membership (requested test and report series share an accession), not result-level attribution. Gold QC twin of the silver product: 4 columns are repaired or nulled, 0 check(s) are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow expectation metrics report what every rule matched on each update.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_spine_request_thread():
    """Contract-v2 public twin of spine_request_thread; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._spine_request_thread")).selectExpr(
        '`request_thread_key` AS `request_thread_key`',
        '`source_object` AS `source_object`',
        '`source_patient_event_key` AS `source_patient_event_key`',
        '`target_patient_event_key` AS `target_patient_event_key`',
        '`link_type_code` AS `link_type_code`',
        '`subject_key` AS `subject_key`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`request_key` AS `request_key`',
        '`response_key` AS `response_key`',
        '`requested_datetime` AS `requested_datetime`',
        '`responded_datetime` AS `responded_datetime`',
        '`source_history_row_count` AS `source_history_row_count`',
        '`source_system` AS `source_system`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, SPINE_REQUEST_THREAD_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.concept_map ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_CONCEPT_MAP_SELECT = [
    '`concept_map_key` AS `concept_map_key`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`target_coding_system` AS `target_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`target_code` AS STRING))) = '0' THEN NULL ELSE `target_code` END AS `target_code`",
    '`target_display` AS `target_display`',
    '`target_domain` AS `target_domain`',
    '`equivalence` AS `equivalence`',
    '`map_source` AS `map_source`',
    '`map_version` AS `map_version`',
    '`mapping_rule_id` AS `mapping_rule_id`',
    '`mapping_rank` AS `mapping_rank`',
    '`map_method` AS `map_method`',
    '`map_confidence` AS `map_confidence`',
    '`map_rule_id` AS `map_rule_id`',
    '`map_candidate_count` AS `map_candidate_count`',
    '`review_status` AS `review_status`',
    '`valid_from` AS `valid_from`',
    '`valid_to` AS `valid_to`',
    '`source_table` AS `source_table`',
    '`source_row_count` AS `source_row_count`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_CONCEPT_MAP_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 33 of 1,080,442
    # rows (0.00305%) when profiled on 2026-08-24.
    "gold.reference.concept_map.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

REFERENCE_CONCEPT_MAP_COLUMN_COMMENTS = {
    "concept_map_key": "Deterministic SHA-256 terminology-map row key.",
    "source_coding_system": "Source code-system namespace.",
    "source_code": "Source code or mapped source value.",
    "source_display": "Source display carried from bronze.",
    "target_coding_system": "Target code-system namespace.",
    "target_code": "Target code or concept identifier.",
    "target_display": "Target display carried from bronze.",
    "target_domain": "Intended downstream semantic domain.",
    "equivalence": "FHIR equivalence when explicitly supplied; null rather than inferred.",
    "map_source": "Governed mapping product and route.",
    "map_version": "Pinned mapping release.",
    "mapping_rule_id": "Source mapping rule or confidence discriminator.",
    "mapping_rank": "Source-supplied candidate rank where available.",
    "map_method": "How the mapping was reached: source_native or rule for legacy reference mappings.",
    "map_confidence": "Mapping confidence in [0,1] where the source supplied one.",
    "map_rule_id": "Stable rule identifier copied from mapping_rule_id for the S3 flat contract.",
    "map_candidate_count": "Number of target candidates for the source code and mapping source.",
    "review_status": "Review state carried verbatim from the mapping source; source_carried rows are bronze-carried without review",
    "valid_from": "Target mapping validity start where supplied.",
    "valid_to": "Target mapping validity end where supplied.",
    "source_table": "Configured bronze source or explicitly authorized development fixture.",
    "source_row_count": "Number of bronze mapping observations represented by the row.",
    "loaded_at": "Maximum contributing clock within each source/target mapping group, with no further clock aggregation across the unioned mapping arms. Diagnosis and the ADC-backed lookup arms use ADC_UPDT; coded-event mappings first coalesce MAPPING_ADC_UPDT with PIPELINE_UPDT_DT_TM; pathology first coalesces mapping_updated_at, ADC_UPDT and source_adc_updt. LUNA and seed maps use updated_at; iWeb, BloodTrack and Mediconnect maps use CURATED_AT. Each chosen clock is cast to TIMESTAMP before the group maximum. These are mixed ingestion/mapping-update clocks, not a uniform native modification time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_concept_map"),
    comment="Internal quality-controlled twin of reference_concept_map: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(REFERENCE_CONCEPT_MAP_ADVISORY_RULES)
def _gold_qc_reference_concept_map():
    """Quality-controlled twin of journey_reference.concept_map."""
    df = _qc("reference_concept_map", REFERENCE_CONCEPT_MAP_SELECT)
    return _with_comments(df, REFERENCE_CONCEPT_MAP_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.concept_map"),
    comment=(
        "One versioned source-to-target concept mapping carried by a governed bronze product; "
        "one-to-many mappings remain separate rows. Gold QC twin of the silver product: 2 "
        "columns are repaired or nulled, 1 check(s) are advisory. Each rule states its reason "
        "in the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_concept_map():
    """Contract-v2 public twin of reference_concept_map; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_concept_map")).selectExpr(
        '`concept_map_key` AS `concept_map_key`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`target_coding_system` AS `target_coding_system`',
        '`target_code` AS `target_code`',
        '`target_display` AS `target_display`',
        '`target_domain` AS `target_domain`',
        '`equivalence` AS `equivalence`',
        '`map_source` AS `map_source`',
        '`map_version` AS `map_version`',
        '`mapping_rule_id` AS `mapping_rule_id`',
        '`mapping_rank` AS `mapping_rank`',
        '`map_method` AS `map_method`',
        '`map_confidence` AS `map_confidence`',
        '`map_rule_id` AS `map_rule_id`',
        '`map_candidate_count` AS `map_candidate_count`',
        '`review_status` AS `review_status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_count` AS `source_row_count`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_CONCEPT_MAP_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.concept_registry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_CONCEPT_REGISTRY_SELECT = [
    '`concept_registry_key` AS `concept_registry_key`',
    '`coding_system` AS `coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`code` AS STRING))) = '0' OR UPPER(TRIM(CAST(`code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `code` END AS `code`",
    '`preferred_display` AS `preferred_display`',
    '`status` AS `status`',
    '`source_use_count` AS `source_use_count`',
    '`target_use_count` AS `target_use_count`',
    '`first_observed_at` AS `first_observed_at`',
    '`last_observed_at` AS `last_observed_at`',
]

REFERENCE_CONCEPT_REGISTRY_COLUMN_COMMENTS = {
    "concept_registry_key": "Deterministic SHA-256 observed-concept key.",
    "coding_system": "Identifier namespace for the observed source or target code, taken from event or mapping evidence; event codes with no source namespace use urn:unknown.",
    "code": "OMOP standard vocabulary concept identifier assigned to the mapped source value.",
    "preferred_display": "Deterministically selected observed display.",
    "status": "Registry observation status.",
    "source_use_count": "Source-side event and mapping observation count.",
    "target_use_count": "Target-side mapping observation count.",
    "first_observed_at": "Earliest represented bronze observation timestamp.",
    "last_observed_at": "Latest represented bronze observation timestamp.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_concept_registry"),
    comment="Internal quality-controlled twin of reference_concept_registry: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_concept_registry():
    """Quality-controlled twin of journey_reference.concept_registry."""
    df = _qc("reference_concept_registry", REFERENCE_CONCEPT_REGISTRY_SELECT)
    return _with_comments(df, REFERENCE_CONCEPT_REGISTRY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.concept_registry"),
    comment=(
        "One observed coding-system and code pair across event sources and mapping targets. "
        "Gold QC twin of the silver product: 1 columns are repaired or nulled, 0 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_concept_registry():
    """Contract-v2 public twin of reference_concept_registry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_concept_registry")).selectExpr(
        '`concept_registry_key` AS `concept_registry_key`',
        '`coding_system` AS `coding_system`',
        '`code` AS `code`',
        '`preferred_display` AS `preferred_display`',
        '`status` AS `status`',
        '`source_use_count` AS `source_use_count`',
        '`target_use_count` AS `target_use_count`',
        '`first_observed_at` AS `first_observed_at`',
        '`last_observed_at` AS `last_observed_at`',
    )
    return _with_comments(df, REFERENCE_CONCEPT_REGISTRY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.device_mapping ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_DEVICE_MAPPING_SELECT = [
    '`device_mapping_key` AS `device_mapping_key`',
    '`source_event_id` AS `source_event_id`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`implant_description` AS `implant_description`',
    '`normalized_description` AS `normalized_description`',
    '`cleaned_description` AS `cleaned_description`',
    '`udi_di` AS `udi_di`',
    '`udi_issuer` AS `udi_issuer`',
    '`gs1_identifier` AS `gs1_identifier`',
    '`hibcc_device_id` AS `hibcc_device_id`',
    '`serial_number` AS `serial_number`',
    '`expiry_date` AS `expiry_date`',
    '`gmdn_code` AS `gmdn_code`',
    '`gmdn_name` AS `gmdn_name`',
    '`snomed_concept_id` AS `snomed_concept_id`',
    '`snomed_name` AS `snomed_name`',
    '`standard_concept_id` AS `standard_concept_id`',
    '`standard_concept_name` AS `standard_concept_name`',
    '`standard_vocabulary_id` AS `standard_vocabulary_id`',
    '`device_type` AS `device_type`',
    '`mapping_layer` AS `mapping_layer`',
    '`mapping_status` AS `mapping_status`',
    '`mapping_confidence` AS `mapping_confidence`',
    '`confidence_tier` AS `confidence_tier`',
    '`mapping_rule_id` AS `mapping_rule_id`',
    '`matched_field` AS `matched_field`',
    '`matched_value` AS `matched_value`',
    '`mapping_candidate_count` AS `mapping_candidate_count`',
    '`mapping_distinct_concept_count` AS `mapping_distinct_concept_count`',
    '`mapping_ambiguous_ind` AS `mapping_ambiguous_ind`',
    '`matched_opcs_code` AS `matched_opcs_code`',
    '`procedure_support_ind` AS `procedure_support_ind`',
    '`mapping_schema_version` AS `mapping_schema_version`',
    '`normalization_version` AS `normalization_version`',
    '`brand_rules_version` AS `brand_rules_version`',
    '`mapped_at` AS `mapped_at`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_DEVICE_MAPPING_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 106,883 of 248,449
    # rows (43%) when profiled on 2026-08-24.
    "gold.reference.device_mapping.expiry_date.future_owner":
        "NOT COALESCE((CAST(`expiry_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

REFERENCE_DEVICE_MAPPING_COLUMN_COMMENTS = {
    "device_mapping_key": "Deterministic SHA-256 key for the Millennium device-mapping row.",
    "source_event_id": "Source implant description event identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT.",
    "implant_description": "Raw source implant description.",
    "normalized_description": "Versioned punctuation/whitespace normalized description.",
    "cleaned_description": "Versioned fallback-matching description.",
    "udi_di": "Coalesced source UDI-DI; issuer is retained separately.",
    "udi_issuer": "GS1, HIBCC or null.",
    "gs1_identifier": "Source GS1 UDI device identifier.",
    "hibcc_device_id": "Source HIBCC UDI device identifier.",
    "serial_number": "GS1 serial number coalesced with raw serial number.",
    "expiry_date": "GS1 expiry date coalesced with raw expiry date.",
    "gmdn_code": "Selected GMDN code.",
    "gmdn_name": "Selected GMDN preferred term name.",
    "snomed_concept_id": "Compatibility field populated only for SNOMED concepts.",
    "snomed_name": "Compatibility field populated only for SNOMED concepts.",
    "standard_concept_id": "Selected standard OMOP device concept identifier.",
    "standard_concept_name": "Selected standard OMOP device concept name.",
    "standard_vocabulary_id": "Vocabulary of STANDARD_CONCEPT_ID.",
    "device_type": "Selected device category/type.",
    "mapping_layer": "Selected mapping evidence layer.",
    "mapping_status": "MAPPED, AMBIGUOUS_SELECTED or UNMAPPED.",
    "mapping_confidence": "Deterministic evidence score between zero and one.",
    "confidence_tier": "HIGH, MEDIUM, LOW or UNMAPPED.",
    "mapping_rule_id": "Rule or algorithm identifier for the selected candidate.",
    "matched_field": "Source field that produced the selected candidate.",
    "matched_value": "Normalized source value used for the selected candidate.",
    "mapping_candidate_count": "Number of distinct mapping candidates retained.",
    "mapping_distinct_concept_count": "Number of distinct selected standard/GMDN tuples.",
    "mapping_ambiguous_ind": "Whether multiple credible candidates existed.",
    "matched_opcs_code": "OPCS code supporting the selected candidate.",
    "procedure_support_ind": "Whether procedure context supports the selected candidate.",
    "mapping_schema_version": "Output schema and mapping implementation version.",
    "normalization_version": "Description normalization version.",
    "brand_rules_version": "Boundary-aware phrase-rule version.",
    "mapped_at": "Timestamp when this event was last evaluated.",
    "_source_system": "Value describing source system for the device mapping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the device mapping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Source implant description event identifier.",
    "loaded_at": "map_device_mapping.PIPELINE_LOADED_AT carried unchanged. This is the contributing bronze row-write clock, distinct from MAPPED_AT evaluation time and from Silver refresh time; a missing value remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_device_mapping"),
    comment="Internal quality-controlled twin of reference_device_mapping: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(REFERENCE_DEVICE_MAPPING_ADVISORY_RULES)
def _gold_qc_reference_device_mapping():
    """Quality-controlled twin of journey_reference.device_mapping."""
    df = _qc("reference_device_mapping", REFERENCE_DEVICE_MAPPING_SELECT)
    return _with_comments(df, REFERENCE_DEVICE_MAPPING_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.device_mapping"),
    comment=(
        "One best-effort implant-device mapping with source and terminology provenance. Gold "
        "QC twin of the silver product: 0 columns are repaired or nulled, 1 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_device_mapping():
    """Contract-v2 public twin of reference_device_mapping; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_device_mapping")).selectExpr(
        '`device_mapping_key` AS `device_mapping_key`',
        '`source_event_id` AS `source_event_id`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`implant_description` AS `implant_description`',
        '`normalized_description` AS `normalized_description`',
        '`cleaned_description` AS `cleaned_description`',
        '`udi_di` AS `udi_di`',
        '`udi_issuer` AS `udi_issuer`',
        '`gs1_identifier` AS `gs1_identifier`',
        '`hibcc_device_id` AS `hibcc_device_id`',
        '`serial_number` AS `serial_number`',
        '`expiry_date` AS `expiry_date`',
        '`gmdn_code` AS `gmdn_code`',
        '`gmdn_name` AS `gmdn_name`',
        '`snomed_concept_id` AS `snomed_concept_id`',
        '`snomed_name` AS `snomed_name`',
        '`standard_concept_id` AS `standard_concept_id`',
        '`standard_concept_name` AS `standard_concept_name`',
        '`standard_vocabulary_id` AS `standard_vocabulary_id`',
        '`device_type` AS `device_type`',
        '`mapping_layer` AS `mapping_layer`',
        '`mapping_status` AS `mapping_status`',
        '`mapping_confidence` AS `mapping_confidence`',
        '`confidence_tier` AS `confidence_tier`',
        '`mapping_rule_id` AS `mapping_rule_id`',
        '`matched_field` AS `matched_field`',
        '`matched_value` AS `matched_value`',
        '`mapping_candidate_count` AS `mapping_candidate_count`',
        '`mapping_distinct_concept_count` AS `mapping_distinct_concept_count`',
        '`mapping_ambiguous_ind` AS `mapping_ambiguous_ind`',
        '`matched_opcs_code` AS `matched_opcs_code`',
        '`procedure_support_ind` AS `procedure_support_ind`',
        '`mapping_schema_version` AS `mapping_schema_version`',
        '`normalization_version` AS `normalization_version`',
        '`brand_rules_version` AS `brand_rules_version`',
        '`mapped_at` AS `mapped_at`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_DEVICE_MAPPING_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.encounter_attribute ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ENCOUNTER_ATTRIBUTE_SELECT = [
    '`encounter_attribute_key` AS `encounter_attribute_key`',
    '`encounter_id` AS `encounter_id`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`attribute_name` AS `attribute_name`',
    '`value_kind` AS `value_kind`',
    "CASE WHEN UPPER(TRIM(CAST(`value_code` AS STRING))) = '0' THEN NULL ELSE `value_code` END AS `value_code`",
    '`value_display` AS `value_display`',
    '`value_code_set` AS `value_code_set`',
    '`value_datetime` AS `value_datetime`',
    '`value_numeric` AS `value_numeric`',
    '`answered_ind` AS `answered_ind`',
    '`active_ind` AS `active_ind`',
    '`current_ind` AS `current_ind`',
    '`beg_effective` AS `beg_effective`',
    '`end_effective` AS `end_effective`',
    '`link_status` AS `link_status`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ENCOUNTER_ATTRIBUTE_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because the row is only an attribute of an encounter, so with
    # no encounter it describes nothing. That was 203,581 orphaned rows plus 0 that already
    # had no encounter_id, out of 87,111,763.
    "gold.reference.encounter_attribute.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}

REFERENCE_ENCOUNTER_ATTRIBUTE_COLUMN_COMMENTS = {
    "encounter_attribute_key": "Deterministic SHA-256 key for the encounter attribute row.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "source_encounter_id": "Internal encounter identifier from the source encounter information row (ENCNTR_ID), linking the attribute to its encounter.",
    "attribute_name": "Standardised curated label for the encounter attribute, derived from the source information sub-type code.",
    "value_kind": "Indicator of which value representation applies to the row, such as a coded, date/time or numeric value.",
    "value_code": "Coded value of the encounter information row, drawn from the code set applicable to the attribute, where zero denotes an unanswered value.",
    "value_display": "The display string for the code_value",
    "value_code_set": "Code set to which VALUE_CD actually belongs, resolved from the code value lookup.",
    "value_datetime": "VALUE_DT_TM with future-dated and sentinel values excluded, leaving only usable date and time values.",
    "value_numeric": "The numeric (non-codified) value of the encounter information.  If the value of the row necessitates storing a numeric (non-codified) value, it is placed in this column.",
    "answered_ind": "Flag indicating whether the row carries an answered coded value rather than the unanswered zero code.",
    "active_ind": "Source indicator of whether the row is active or in an inactive state such as logically deleted or combined away.",
    "current_ind": "Flag marking the latest active row for each encounter and information subtype within the retained history.",
    "beg_effective": "BEG_EFFECTIVE_DT_TM with future-dated and sentinel values excluded, leaving only usable effective-from date and time values.",
    "end_effective": "END_EFFECTIVE_DT_TM with future-dated and sentinel values excluded, leaving only usable effective-to date and time values.",
    "link_status": "Indicator of whether the row's encounter identifier resolves to an encounter in the curated encounter foundation.",
    "_source_system": "Value describing source system for the encounter attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the encounter attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Primary key of the source mill_encntr_info row (ENCNTR_INFO_ID), giving one row per allowlisted encounter information record.",
    "loaded_at": "map_encounter_attribute.PIPELINE_UPDT_DT_TM carried unchanged through the shared attribute projection. This is the contributing bronze write clock, not the attribute's effective dates, clinical value time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_encounter_attribute"),
    comment="Internal quality-controlled twin of reference_encounter_attribute: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_ENCOUNTER_ATTRIBUTE_MANDATORY_RULES)
def _gold_qc_reference_encounter_attribute():
    """Quality-controlled twin of journey_reference.encounter_attribute."""
    df = _qc(
        "reference_encounter_attribute",
        REFERENCE_ENCOUNTER_ATTRIBUTE_SELECT,
        fk_columns=["encounter_id"],
    )
    return _with_comments(df, REFERENCE_ENCOUNTER_ATTRIBUTE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.encounter_attribute"),
    comment=(
        "One allowlisted encounter attribute with current and effective evidence. Gold QC "
        "twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 0 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_encounter_attribute():
    """Contract-v2 public twin of reference_encounter_attribute; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_encounter_attribute")).selectExpr(
        '`encounter_attribute_key` AS `encounter_attribute_key`',
        '`encounter_id` AS `encounter_id`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`attribute_name` AS `attribute_name`',
        '`value_kind` AS `value_kind`',
        '`value_code` AS `value_code`',
        '`value_display` AS `value_display`',
        '`value_code_set` AS `value_code_set`',
        '`value_datetime` AS `value_datetime`',
        '`value_numeric` AS `value_numeric`',
        '`answered_ind` AS `answered_ind`',
        '`active_ind` AS `active_ind`',
        '`current_ind` AS `current_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`link_status` AS `link_status`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_ENCOUNTER_ATTRIBUTE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.encounter_bounds ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ENCOUNTER_BOUNDS_SELECT = [
    '`encounter_bounds_key` AS `encounter_bounds_key`',
    '`encounter_id` AS `encounter_id`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`first_clinical_event_datetime` AS `first_clinical_event_datetime`',
    '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
    '`clinical_event_count` AS `clinical_event_count`',
    '`first_contemporaneous_event_datetime` AS `first_contemporaneous_event_datetime`',
    '`last_contemporaneous_event_datetime` AS `last_contemporaneous_event_datetime`',
    '`contemporaneous_event_count` AS `contemporaneous_event_count`',
    '`first_order_datetime` AS `first_order_datetime`',
    '`last_order_datetime` AS `last_order_datetime`',
    '`order_count` AS `order_count`',
    '`ward_move_count` AS `ward_move_count`',
    '`ward_occupancy_minutes` AS `ward_occupancy_minutes`',
    '`last_ward_in_datetime` AS `last_ward_in_datetime`',
    '`last_ward_out_datetime` AS `last_ward_out_datetime`',
    '`last_ward_still_open_ind` AS `last_ward_still_open_ind`',
    '`event_first_datetime` AS `event_first_datetime`',
    '`event_last_datetime` AS `event_last_datetime`',
    '`in_activity_bounds_ind` AS `in_activity_bounds_ind`',
    '`in_event_bounds_ind` AS `in_event_bounds_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_ENCOUNTER_BOUNDS_MANDATORY_RULES = {
    # The flow nulls encounter_id when it names a parent the spine does not have, and this
    # rule then drops the row, because the row is only the start and end of an encounter, so
    # with no encounter it bounds nothing. That was 413,547 orphaned rows plus 0 that
    # already had no encounter_id, out of 47,674,684.
    "gold.reference.encounter_bounds.encounter_id.fk_containment":
        "`encounter_id` IS NOT NULL",
}

REFERENCE_ENCOUNTER_BOUNDS_COLUMN_COMMENTS = {
    "encounter_bounds_key": "Deterministic SHA-256 key for the encounter-bounds row.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "source_encounter_id": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "first_clinical_event_datetime": "Earliest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter, over current rows only and excluding cancelled, in-error and not-done results. EVENT_END_DT_TM is clinical time and is freely backdated by retrospective documentation, so this can precede a known arrival by years. It is evidence that the encounter happened, not a usable arrival.",
    "last_clinical_event_datetime": "Latest plausible mill_clinical_event.EVENT_END_DT_TM for the encounter under the same filters.",
    "clinical_event_count": "Number of qualifying clinical events, zero when the encounter has none.",
    "first_contemporaneous_event_datetime": "Earliest clinical event whose clinical time is no more than seven days before its documentation time (EVENT_END_DT_TM at or after VALID_FROM_DT_TM minus seven days), which strips most retrospectively backdated entries. This is the only event-derived timestamp permitted to set a best-guess arrival, and then only at low confidence: measured against known arrivals it still precedes them 27.0 per cent of the time, with a 1st percentile of minus 48.9 days.",
    "last_contemporaneous_event_datetime": "Latest clinical event under the same contemporaneity filter.",
    "contemporaneous_event_count": "Number of clinical events passing the contemporaneity filter, zero when the encounter has none.",
    "first_order_datetime": "Earliest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter, excluding order templates and cancelled, voided-without-results and future orders. Orders sit a median 1.4 minutes after a known arrival but precede it 9.9 per cent of the time, because orders can be placed at referral.",
    "last_order_datetime": "Latest plausible mill_orders.ORIG_ORDER_DT_TM for the encounter under the same filters.",
    "order_count": "Number of qualifying orders, zero when the encounter has none.",
    "ward_move_count": "Number of distinct nurse-unit placements recorded in mill_encntr_loc_hist, deduplicated on nurse unit and the placement arrival and departure pair. Zero when the encounter has none.",
    "ward_occupancy_minutes": "Total minutes spent in nurse units, summed over placements that have both an arrival and a later departure. Null when no placement is closed. Overlapping placements are summed as recorded, so read this as evidence of inpatient duration rather than an exact bed-occupancy measure.",
    "last_ward_in_datetime": "Latest plausible nurse-unit arrival recorded in mill_encntr_loc_hist.",
    "last_ward_out_datetime": "Latest plausible nurse-unit departure recorded in mill_encntr_loc_hist. Location history mirrors the encounter DISCH_DT_TM to the minute 99.6 per cent of the time, so this contributes a departure only for the few thousand encounters that have neither DEPART_DT_TM nor DISCH_DT_TM.",
    "last_ward_still_open_ind": "True when the most recent nurse-unit placement has no recorded departure after it, meaning the patient is still in situ. When true, LAST_WARD_OUT_DT_TM must not be read as a departure.",
    "event_first_datetime": "Earliest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.",
    "event_last_datetime": "Latest plausible mill_clinical_event.CLINSIG_UPDT_DT_TM recorded against the encounter.",
    "in_activity_bounds_ind": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "in_event_bounds_ind": "Millennium encounter identifier.",
    "_source_system": "Value describing source system for the encounter bounds record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the encounter bounds record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Millennium encounter identifier. One row per encounter with some recorded clinical activity; encounters with none are absent from this table.",
    "loaded_at": "Always NULL as a TIMESTAMP in the encounter-bounds projection. No load clock is supplied from map_encounter_activity_bounds or map_encounter_event_bounds, and the pipeline does not substitute event-boundary timestamps or the Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_encounter_bounds"),
    comment="Internal quality-controlled twin of reference_encounter_bounds: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_ENCOUNTER_BOUNDS_MANDATORY_RULES)
def _gold_qc_reference_encounter_bounds():
    """Quality-controlled twin of journey_reference.encounter_bounds."""
    df = _qc(
        "reference_encounter_bounds",
        REFERENCE_ENCOUNTER_BOUNDS_SELECT,
        fk_columns=["encounter_id"],
    )
    return _with_comments(df, REFERENCE_ENCOUNTER_BOUNDS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.encounter_bounds"),
    comment=(
        "One full-outer encounter activity and event bounds row. Gold QC twin of the silver "
        "product: 1 columns are repaired or nulled, 1 rule(s) drop rows, 0 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_encounter_bounds():
    """Contract-v2 public twin of reference_encounter_bounds; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_encounter_bounds")).selectExpr(
        '`encounter_bounds_key` AS `encounter_bounds_key`',
        '`encounter_id` AS `encounter_id`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`first_clinical_event_datetime` AS `first_clinical_event_datetime`',
        '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
        '`clinical_event_count` AS `clinical_event_count`',
        '`first_contemporaneous_event_datetime` AS `first_contemporaneous_event_datetime`',
        '`last_contemporaneous_event_datetime` AS `last_contemporaneous_event_datetime`',
        '`contemporaneous_event_count` AS `contemporaneous_event_count`',
        '`first_order_datetime` AS `first_order_datetime`',
        '`last_order_datetime` AS `last_order_datetime`',
        '`order_count` AS `order_count`',
        '`ward_move_count` AS `ward_move_count`',
        '`ward_occupancy_minutes` AS `ward_occupancy_minutes`',
        '`last_ward_in_datetime` AS `last_ward_in_datetime`',
        '`last_ward_out_datetime` AS `last_ward_out_datetime`',
        '`last_ward_still_open_ind` AS `last_ward_still_open_ind`',
        '`event_first_datetime` AS `event_first_datetime`',
        '`event_last_datetime` AS `event_last_datetime`',
        '`in_activity_bounds_ind` AS `in_activity_bounds_ind`',
        '`in_event_bounds_ind` AS `in_event_bounds_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_ENCOUNTER_BOUNDS_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.gene_tested ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_GENE_TESTED_SELECT = [
    '`gene_tested_row_key` AS `gene_tested_row_key`',
    '`source_gene_tested_id` AS `source_gene_tested_id`',
    '`genomic_test_key` AS `genomic_test_key`',
    '`source_genetic_test_id` AS `source_genetic_test_id`',
    '`hgnc_id` AS `hgnc_id`',
    '`reported_gene_symbol` AS `reported_gene_symbol`',
    '`normalized_gene_symbol` AS `normalized_gene_symbol`',
    '`alias_match_type` AS `alias_match_type`',
    '`evidence_type` AS `evidence_type`',
    '`test_scope` AS `test_scope`',
    '`panel_version_inferred` AS `panel_version_inferred`',
    '`confidence` AS `confidence`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_GENE_TESTED_COLUMN_COMMENTS = {
    "gene_tested_row_key": "Deterministic SHA-256 assay-gene row key.",
    "source_gene_tested_id": "Bronze gene-tested identifier.",
    "genomic_test_key": "Deterministic SHA-256 parent genomic-test key.",
    "source_genetic_test_id": "Bronze parent genetic-test identifier.",
    "hgnc_id": "Governed HGNC identifier.",
    "reported_gene_symbol": "Gene symbol as reported or configured.",
    "normalized_gene_symbol": "Current approved HGNC symbol.",
    "alias_match_type": "HGNC symbol-resolution route.",
    "evidence_type": "Source of assay-gene membership.",
    "test_scope": "Reported exon/region/coverage scope when available.",
    "panel_version_inferred": "Whether membership used an inferred panel version.",
    "confidence": "Deterministic normalization confidence.",
    "loaded_at": "map_pathology_gene_tested.ADC_UPDT carried unchanged for the assay-gene membership row. The parent assay key is derived without joining or adding its timestamp; this is bronze load provenance, not a genomic-test event time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_gene_tested"),
    comment="Internal quality-controlled twin of reference_gene_tested: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_gene_tested():
    """Quality-controlled twin of journey_reference.gene_tested."""
    df = _qc("reference_gene_tested", REFERENCE_GENE_TESTED_SELECT)
    return _with_comments(df, REFERENCE_GENE_TESTED_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.gene_tested"),
    comment=(
        "Assay-gene denominator at gene_tested_id grain. The current build covers 42.6% of "
        "assays; BCRL and FLT3 are complete while MNGS is 10.5% and TNGS is 16.6%. Gold QC "
        "twin of the silver product: 0 columns are repaired or nulled, 0 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_gene_tested():
    """Contract-v2 public twin of reference_gene_tested; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_gene_tested")).selectExpr(
        '`gene_tested_row_key` AS `gene_tested_row_key`',
        '`source_gene_tested_id` AS `source_gene_tested_id`',
        '`genomic_test_key` AS `genomic_test_key`',
        '`source_genetic_test_id` AS `source_genetic_test_id`',
        '`hgnc_id` AS `hgnc_id`',
        '`reported_gene_symbol` AS `reported_gene_symbol`',
        '`normalized_gene_symbol` AS `normalized_gene_symbol`',
        '`alias_match_type` AS `alias_match_type`',
        '`evidence_type` AS `evidence_type`',
        '`test_scope` AS `test_scope`',
        '`panel_version_inferred` AS `panel_version_inferred`',
        '`confidence` AS `confidence`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_GENE_TESTED_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.location ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_LOCATION_SELECT = [
    '`location_key` AS `location_key`',
    '`location_code` AS `location_code`',
    '`parent_location_key` AS `parent_location_key`',
    '`location_level` AS `location_level`',
    '`source_location_code` AS `source_location_code`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`organization_id` AS `organization_id`',
    '`physical_type_code` AS `physical_type_code`',
    '`valid_from` AS `valid_from`',
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN CAST(`valid_to` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`latitude` AS `latitude`',
    '`longitude` AS `longitude`',
    '`address_city` AS `address_city`',
    '`address_postcode_masked` AS `address_postcode_masked`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_LOCATION_COLUMN_COMMENTS = {
    "location_key": "Deterministic SHA-256 key across location levels.",
    "location_code": "Native Millennium location code; primary key together with location_level.",
    "parent_location_key": "Deterministic SHA-256 key of the parent facility or building.",
    "location_level": "Derived hierarchy level.",
    "source_location_code": "Source Millennium location code.",
    "name": "Source location name.",
    "status": "Source-derived location status.",
    "organization_id": "Millennium ORGANIZATION_ID as BIGINT when available.",
    "physical_type_code": "FHIR physical location type code.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "latitude": "Source address latitude.",
    "longitude": "Source address longitude.",
    "address_city": "Source address city.",
    "address_postcode_masked": "Privacy-aware source postcode.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "loaded_at": "From map_care_site: maximum ADC_UPDT per facility_cd for facility rows, maximum ADC_UPDT per building_cd for building rows, and the individual source row's ADC_UPDT for nurse-unit rows. The hierarchy union adds no parent clock or cross-level aggregation; this is bronze ingestion provenance, not location validity time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_location"),
    comment="Internal quality-controlled twin of reference_location: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_location():
    """Quality-controlled twin of journey_reference.location."""
    df = _qc("reference_location", REFERENCE_LOCATION_SELECT)
    return _with_comments(df, REFERENCE_LOCATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.location"),
    comment=(
        "Effective-dated facility, building, and nurse-unit hierarchy. Gold QC twin of the "
        "silver product: 1 columns are repaired or nulled, 0 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_location():
    """Contract-v2 public twin of reference_location; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_location")).selectExpr(
        '`location_key` AS `location_key`',
        '`location_code` AS `location_code`',
        '`parent_location_key` AS `parent_location_key`',
        '`location_level` AS `location_level`',
        '`source_location_code` AS `source_location_code`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`organization_id` AS `organization_id`',
        '`physical_type_code` AS `physical_type_code`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`latitude` AS `latitude`',
        '`longitude` AS `longitude`',
        '`address_city` AS `address_city`',
        '`address_postcode_masked` AS `address_postcode_masked`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_LOCATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.organization ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_ORGANIZATION_SELECT = [
    '`organization_key` AS `organization_key`',
    '`organization_id` AS `organization_id`',
    '`source_organization_id` AS `source_organization_id`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`valid_from` AS `valid_from`',
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN CAST(`valid_to` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`address_city` AS `address_city`',
    '`address_postcode_masked` AS `address_postcode_masked`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_ORGANIZATION_COLUMN_COMMENTS = {
    "organization_key": "Deterministic SHA-256 organization key.",
    "organization_id": "Millennium ORGANIZATION_ID; primary key for the Millennium arm.",
    "source_organization_id": "Millennium organization identifier.",
    "name": "Source organization name.",
    "status": "Source-derived organization status.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "address_city": "Source organization address city.",
    "address_postcode_masked": "Privacy-aware source postcode.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source organization identifier.",
    "loaded_at": "Maximum map_care_site.ADC_UPDT over source rows sharing the non-null ORGANIZATION_ID. The maximum is not restricted to active source rows or the row supplying the selected name/address; it records contributing bronze ingestion, not organization validity time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_organization"),
    comment="Internal quality-controlled twin of reference_organization: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_organization():
    """Quality-controlled twin of journey_reference.organization."""
    df = _qc("reference_organization", REFERENCE_ORGANIZATION_SELECT)
    return _with_comments(df, REFERENCE_ORGANIZATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.organization"),
    comment=(
        "Thin v1 provider organization dimension from care-site bronze. Gold QC twin of the "
        "silver product: 1 columns are repaired or nulled, 0 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_organization():
    """Contract-v2 public twin of reference_organization; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_organization")).selectExpr(
        '`organization_key` AS `organization_key`',
        '`organization_id` AS `organization_id`',
        '`source_organization_id` AS `source_organization_id`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`address_city` AS `address_city`',
        '`address_postcode_masked` AS `address_postcode_masked`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_ORGANIZATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.person_address ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_ADDRESS_SELECT = [
    '`person_address_key` AS `person_address_key`',
    '`source_address_id` AS `source_address_id`',
    '`parent_entity` AS `parent_entity`',
    '`person_id` AS `person_id`',
    '`organization_id` AS `organization_id`',
    "CASE WHEN UPPER(TRIM(CAST(`address_type_code` AS STRING))) = '0' THEN NULL ELSE `address_type_code` END AS `address_type_code`",
    '`active_ind` AS `active_ind`',
    "CASE WHEN CAST(`beg_effective` AS DATE) = DATE'1900-01-01' OR CAST(`beg_effective` AS DATE) = DATE'2100-01-01' OR CAST(`beg_effective` AS DATE) = DATE'2100-12-31' OR CAST(`beg_effective` AS DATE) < DATE'1901-01-01' AND CAST(`beg_effective` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `beg_effective` END AS `beg_effective`",
    "CASE WHEN CAST(`end_effective` AS DATE) = DATE'2100-12-31' OR CAST(`end_effective` AS DATE) = DATE'1900-01-01' OR CAST(`end_effective` AS DATE) = DATE'2100-01-01' OR CAST(`end_effective` AS DATE) < DATE'1901-01-01' AND CAST(`end_effective` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `end_effective` END AS `end_effective`",
    '`open_ended_ind` AS `open_ended_ind`',
    '`street_address` AS `street_address`',
    '`city` AS `city`',
    '`postcode` AS `postcode`',
    '`postcode_masked` AS `postcode_masked`',
    '`postcode_outward` AS `postcode_outward`',
    '`uprn` AS `uprn`',
    '`lsoa` AS `lsoa`',
    '`msoa` AS `msoa`',
    '`local_authority_code` AS `local_authority_code`',
    '`imd_decile` AS `imd_decile`',
    '`imd_quintile` AS `imd_quintile`',
    '`latitude` AS `latitude`',
    '`longitude` AS `longitude`',
    '`uprn_match_quality` AS `uprn_match_quality`',
    '`epc_current_energy_rating` AS `epc_current_energy_rating`',
    '`epc_potential_energy_rating` AS `epc_potential_energy_rating`',
    '`epc_property_type` AS `epc_property_type`',
    '`epc_built_form` AS `epc_built_form`',
    '`epc_construction_age_band` AS `epc_construction_age_band`',
    '`epc_tenure` AS `epc_tenure`',
    '`epc_mains_gas_flag` AS `epc_mains_gas_flag`',
    '`epc_total_floor_area` AS `epc_total_floor_area`',
    '`epc_inspection_date` AS `epc_inspection_date`',
    '`epc_lodgement_date` AS `epc_lodgement_date`',
    '`epc_fuel_poverty_risk` AS `epc_fuel_poverty_risk`',
    '`epc_cold_hazard_proxy` AS `epc_cold_hazard_proxy`',
    '`epc_spatial_heating_poverty` AS `epc_spatial_heating_poverty`',
    '`epc_off_gas_grid` AS `epc_off_gas_grid`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`address_id` AS `address_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PERSON_ADDRESS_MANDATORY_RULES = {
    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row unless organization_id names the parent instead, because an address
    # belonging to nobody at all cannot be used, and the orphans are a wider population than
    # the spine covers rather than a join defect. That is the 3,803,862 orphaned rows out of
    # 17,057,866. The 42,490 rows with no person_id at all are not orphans: they belong to
    # something other than a person, and they are kept whenever organization_id says what.
    "gold.reference.person_address.person_id.fk_containment":
        "`person_id` IS NOT NULL OR `organization_id` IS NOT NULL",
}

REFERENCE_PERSON_ADDRESS_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 122 of 17,057,866
    # rows (0.000715%) when profiled on 2026-08-24.
    "gold.reference.person_address.beg_effective.future_owner":
        "NOT COALESCE((CAST(`beg_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1,861 of
    # 17,057,866 rows (0.0109%) when profiled on 2026-08-24.
    "gold.reference.person_address.end_effective.future_owner":
        "NOT COALESCE((CAST(`end_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

REFERENCE_PERSON_ADDRESS_COLUMN_COMMENTS = {
    "person_address_key": "Deterministic SHA-256 key for the Millennium address row.",
    "source_address_id": "The address ID is the primary key of the address table.",
    "parent_entity": "The upper case name of the table to which this address row is related (i.e., PERSON, PRSNL, ORGANIZATION, etc.)",
    "person_id": "Native Millennium PERSON_ID as BIGINT for person-owned addresses.",
    "organization_id": "Native Millennium ORGANIZATION_ID as BIGINT for organization-owned addresses.",
    "address_type_code": "Code value identifying the type of address recorded on the source address row.",
    "active_ind": "The table row is active or inactive. A row is generally active unless it is in an inactive state such as logically deleted, combined away, pending purge, etc.",
    "beg_effective": "The date and time for which this table row becomes effective. Normally, this will be the date and time the row is added, but could be a past or future date and time.",
    "end_effective": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "open_ended_ind": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "street_address": "Concatenated street address.",
    "city": "The city field is the text name of the city associated with the address row.",
    "postcode": "Postcode value as held in the source address record, retained for provenance.",
    "postcode_masked": "Partially masked version of the postcode for privacy protection.",
    "postcode_outward": "Outward (first) part of the postcode derived from the source address postcode for privacy-aware geographic analysis.",
    "uprn": "Unique Property Reference Number - unique identifier for every spatial address in Great Britain (1-999999999999).",
    "lsoa": "LSOA stands for Lower Layer Super Output Area, which is a geographic area used for small area statistics in the UK.",
    "msoa": "2021 Census Middle Layer Super Output Area code derived from the address postcode geography lookup.",
    "local_authority_code": "Local Authority District code for the address, derived from the postcode geography lookup.",
    "imd_decile": "IMD_Decile is used to store the Index of Multiple Deprivation (IMD) decile value.",
    "imd_quintile": "IMD_Quintile is used to store the Index of Multiple Deprivation (IMD) quintile value.",
    "latitude": "Latitude coordinate of the matched address property from the reference address data.",
    "longitude": "Longitude coordinate of the matched address property from the reference address data.",
    "uprn_match_quality": "It provides a descriptive label indicating the type or quality of the address match, based on the matching algorithm used to link the address to the reference data.",
    "epc_current_energy_rating": "EPC band A-G. Core fuel poverty indicator; bands D-G flag risk under LILEE definition.",
    "epc_potential_energy_rating": "Achievable EPC band after recommended improvements.",
    "epc_property_type": "Property type recorded in the linked Energy Performance Certificate, such as house, flat, bungalow, maisonette or park home; carried from `PROPERTY_TYPE`.",
    "epc_built_form": "Detached/Semi/Terrace etc. Affects heat loss via surface-area-to-volume ratio.",
    "epc_construction_age_band": "Building age band. Strongest single predictor of fabric quality. Pre-1919 homes avg SAP ~45.",
    "epc_tenure": "Tenure at time of EPC: Owner-occupied / Rented (private) / Rented (social).",
    "epc_mains_gas_flag": "Whether mains gas is available. Off-gas-grid homes face higher fuel costs.",
    "epc_total_floor_area": "Total floor area in m². Overcrowding proxy when linked to household size.",
    "epc_inspection_date": "Date the EPC assessment was carried out.",
    "epc_lodgement_date": "Date the EPC was lodged on the register.",
    "epc_fuel_poverty_risk": "True if EPC band D-G. Flags fuel poverty risk under LILEE definition.",
    "epc_cold_hazard_proxy": "True if EPC band F or G. Approximates HHSRS Category 1 excessive cold hazard.",
    "epc_spatial_heating_poverty": "True if heated rooms < habitable rooms, indicating partial home heating.",
    "epc_off_gas_grid": "True if no mains gas connection. Off-grid homes face higher fuel costs.",
    "_source_system": "Value describing source system for the person address record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person address record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "The address ID is the primary key of the address table.",
    "address_id": "Millennium ADDRESS_ID as BIGINT; primary key of this table.",
    "loaded_at": "map_address.ADC_UPDT carried unchanged for the address row. The EPC enrichment join contributes no load timestamp, so this field does not track changes to the EPC payload or its inspection/lodgement dates. It is address-source bronze provenance, not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_person_address"),
    comment="Internal quality-controlled twin of reference_person_address: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_ADDRESS_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PERSON_ADDRESS_ADVISORY_RULES)
def _gold_qc_reference_person_address():
    """Quality-controlled twin of journey_reference.person_address."""
    df = _qc(
        "reference_person_address",
        REFERENCE_PERSON_ADDRESS_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, REFERENCE_PERSON_ADDRESS_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.person_address"),
    comment=(
        "One person or organization address assignment; direct address, postcode and UPRN "
        "fields are IG-sensitive. Gold QC twin of the silver product: 4 columns are repaired "
        "or nulled, 1 rule(s) drop rows, 2 check(s) are advisory. Each rule states its reason "
        "in the pipeline notebook, and Lakeflow expectation metrics report what every rule "
        "matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_person_address():
    """Contract-v2 public twin of reference_person_address; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_address")).selectExpr(
        '`person_address_key` AS `person_address_key`',
        '`source_address_id` AS `source_address_id`',
        '`parent_entity` AS `parent_entity`',
        '`person_id` AS `person_id`',
        '`organization_id` AS `organization_id`',
        '`address_type_code` AS `address_type_code`',
        '`active_ind` AS `active_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`open_ended_ind` AS `open_ended_ind`',
        '`street_address` AS `street_address`',
        '`city` AS `city`',
        '`postcode` AS `postcode`',
        '`postcode_masked` AS `postcode_masked`',
        '`postcode_outward` AS `postcode_outward`',
        '`uprn` AS `uprn`',
        '`lsoa` AS `lsoa`',
        '`msoa` AS `msoa`',
        '`local_authority_code` AS `local_authority_code`',
        '`imd_decile` AS `imd_decile`',
        '`imd_quintile` AS `imd_quintile`',
        '`latitude` AS `latitude`',
        '`longitude` AS `longitude`',
        '`uprn_match_quality` AS `uprn_match_quality`',
        '`epc_current_energy_rating` AS `epc_current_energy_rating`',
        '`epc_potential_energy_rating` AS `epc_potential_energy_rating`',
        '`epc_property_type` AS `epc_property_type`',
        '`epc_built_form` AS `epc_built_form`',
        '`epc_construction_age_band` AS `epc_construction_age_band`',
        '`epc_tenure` AS `epc_tenure`',
        '`epc_mains_gas_flag` AS `epc_mains_gas_flag`',
        '`epc_total_floor_area` AS `epc_total_floor_area`',
        '`epc_inspection_date` AS `epc_inspection_date`',
        '`epc_lodgement_date` AS `epc_lodgement_date`',
        '`epc_fuel_poverty_risk` AS `epc_fuel_poverty_risk`',
        '`epc_cold_hazard_proxy` AS `epc_cold_hazard_proxy`',
        '`epc_spatial_heating_poverty` AS `epc_spatial_heating_poverty`',
        '`epc_off_gas_grid` AS `epc_off_gas_grid`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`address_id` AS `address_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PERSON_ADDRESS_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.person_attribute ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_ATTRIBUTE_SELECT = [
    '`person_attribute_key` AS `person_attribute_key`',
    '`person_id` AS `person_id`',
    '`source_person_id` AS `source_person_id`',
    '`attribute_name` AS `attribute_name`',
    '`value_kind` AS `value_kind`',
    "CASE WHEN UPPER(TRIM(CAST(`value_code` AS STRING))) = '0' THEN NULL ELSE `value_code` END AS `value_code`",
    '`value_display` AS `value_display`',
    '`value_code_set` AS `value_code_set`',
    '`value_datetime` AS `value_datetime`',
    '`value_numeric` AS `value_numeric`',
    '`answered_ind` AS `answered_ind`',
    '`active_ind` AS `active_ind`',
    '`current_ind` AS `current_ind`',
    '`beg_effective` AS `beg_effective`',
    '`end_effective` AS `end_effective`',
    '`link_status` AS `link_status`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PERSON_ATTRIBUTE_MANDATORY_RULES = {
    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because the row is only an attribute of a person, so with no
    # person it describes nothing. That was 331 orphaned rows plus 0 that already had no
    # person_id, out of 9,775,723.
    "gold.reference.person_attribute.person_id.fk_containment": "`person_id` IS NOT NULL",
}

REFERENCE_PERSON_ATTRIBUTE_COLUMN_COMMENTS = {
    "person_attribute_key": "Deterministic SHA-256 key for the person attribute row.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "source_person_id": "Internal system-assigned person identifier from the source person table, linking the attribute row to a person.",
    "attribute_name": "Standardised label for the person attribute represented by the row, derived from the source information sub-type code.",
    "value_kind": "Indicator of the value representation expected for the attribute, such as a coded value, date or numeric value.",
    "value_code": "Coded value recorded for the attribute, where zero indicates the attribute was not answered and is never decoded.",
    "value_display": "The display string for the code_value",
    "value_code_set": "Code set of the decoded coded value as held in the reference code value lookup.",
    "value_datetime": "Attribute date/time after removal of sentinel and implausible values, leaving null where no usable value remains.",
    "value_numeric": "If the comment is of a numeric type, it is stored in this attribute",
    "answered_ind": "Flag indicating whether a coded value was actually recorded for the attribute rather than left unanswered.",
    "active_ind": "Indicator of whether the source row is in an active state or has been made inactive, such as logically deleted or combined away.",
    "current_ind": "Flag marking the latest active row for each person and attribute sub-type within the retained history.",
    "beg_effective": "Row begin effective date/time after removal of sentinel and implausible values, leaving null where no usable value remains.",
    "end_effective": "Row end effective date/time after removal of sentinel and open-ended placeholder values, leaving null where no usable value remains.",
    "link_status": "Indicator of whether the attribute row's person identifier matched a person in the curated person foundation table.",
    "_source_system": "Value describing source system for the person attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person attribute record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Unique source primary key of the originating mill_person_info row, carried through as the row grain of this table.",
    "loaded_at": "map_person_attribute.PIPELINE_UPDT_DT_TM carried unchanged through the shared attribute projection. This is the contributing bronze write clock, not the attribute's effective dates, clinical value time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_person_attribute"),
    comment="Internal quality-controlled twin of reference_person_attribute: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_ATTRIBUTE_MANDATORY_RULES)
def _gold_qc_reference_person_attribute():
    """Quality-controlled twin of journey_reference.person_attribute."""
    df = _qc(
        "reference_person_attribute",
        REFERENCE_PERSON_ATTRIBUTE_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, REFERENCE_PERSON_ATTRIBUTE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.person_attribute"),
    comment="One allowlisted person attribute; no-fixed-abode and public-care flags are IG-sensitive. Gold QC twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 0 check(s) are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow expectation metrics report what every rule matched on each update.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_person_attribute():
    """Contract-v2 public twin of reference_person_attribute; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_attribute")).selectExpr(
        '`person_attribute_key` AS `person_attribute_key`',
        '`person_id` AS `person_id`',
        '`source_person_id` AS `source_person_id`',
        '`attribute_name` AS `attribute_name`',
        '`value_kind` AS `value_kind`',
        '`value_code` AS `value_code`',
        '`value_display` AS `value_display`',
        '`value_code_set` AS `value_code_set`',
        '`value_datetime` AS `value_datetime`',
        '`value_numeric` AS `value_numeric`',
        '`answered_ind` AS `answered_ind`',
        '`active_ind` AS `active_ind`',
        '`current_ind` AS `current_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`link_status` AS `link_status`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PERSON_ATTRIBUTE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.person_death_evidence ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PERSON_DEATH_EVIDENCE_SELECT = [
    '`person_death_evidence_key` AS `person_death_evidence_key`',
    '`person_id` AS `person_id`',
    '`deceased_datetime_raw` AS `deceased_datetime_raw`',
    '`deceased_datetime` AS `deceased_datetime`',
    '`calculated_death_date` AS `calculated_death_date`',
    '`precision_flag` AS `precision_flag`',
    '`precision_desc` AS `precision_desc`',
    '`source_desc` AS `source_desc`',
    '`method_desc` AS `method_desc`',
    '`death_date_estimate_source` AS `death_date_estimate_source`',
    '`cause_of_death` AS `cause_of_death`',
    '`autopsy_desc` AS `autopsy_desc`',
    '`age_at_death` AS `age_at_death`',
    "CASE WHEN CAST(`last_encounter_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE CASE WHEN CAST(`last_encounter_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_encounter_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_encounter_datetime` END END AS `last_encounter_datetime`",
    "CASE WHEN CAST(`last_clinical_event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_clinical_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_clinical_event_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_clinical_event_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_clinical_event_datetime` END END AS `last_clinical_event_datetime`",
    "CASE WHEN CAST(`last_known_activity_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_known_activity_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_known_activity_datetime` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_known_activity_datetime` AS DATE)) < 9999 THEN NULL ELSE `last_known_activity_datetime` END END AS `last_known_activity_datetime`",
    '`clinical_event_count` AS `clinical_event_count`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PERSON_DEATH_EVIDENCE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 584,657 rows of 584,657 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",

    # The flow nulls person_id when it names a parent the spine does not have, and this rule
    # then drops the row, because evidence that someone died is unusable without knowing
    # who. That was 226 orphaned rows plus 0 that already had no person_id, out of 584,657.
    "gold.reference.person_death_evidence.person_id.fk_containment":
        "`person_id` IS NOT NULL",
}

REFERENCE_PERSON_DEATH_EVIDENCE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 584,657 at the profile.
    "gold.reference.person_death_evidence.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_PERSON_DEATH_EVIDENCE_COLUMN_COMMENTS = {
    "person_death_evidence_key": "Deterministic SHA-256 key for the person death-evidence row.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "deceased_datetime_raw": "Date and time of death.",
    "deceased_datetime": "Date and time of death.",
    "calculated_death_date": "Calendar date associated with calculated death for the person death evidence record. It is derived from bronze field `CALC_DEATH_DATE` in `4_prod.bronze.map_death`. The value has no time-of-day component; null means the source date was unavailable.",
    "precision_flag": "Source precision flag for DECEASED_DT_TM.",
    "precision_desc": "Decoded death timestamp precision: unknown, full date/time, month, year or day.",
    "source_desc": "Description of the code.",
    "method_desc": "Description of the code.",
    "death_date_estimate_source": "RECORDED_DEATH, LAST_ENCOUNTER, LAST_CLINICAL_EVENT or NONE.",
    "cause_of_death": "Raw free-text cause of death.",
    "autopsy_desc": "Resolved autopsy code description/display.",
    "age_at_death": "Raw reported age at death; source zero values are retained.",
    "last_encounter_datetime": "It represents the timestamp of the last encounter associated with a deceased individual.",
    "last_clinical_event_datetime": "Latest EVENT_END_DT_TM across selected latest source versions, without result-status filtering.",
    "last_known_activity_datetime": "Greatest of LAST_ENCNTR_DT_TM and LAST_NON_ERROR_CLINICAL_EVENT_DT_TM. This is activity, not a confirmed death timestamp.",
    "clinical_event_count": "Count of selected latest logical clinical-event source states for the person.",
    "_source_system": "Value describing source system for the person death evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the person death evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "This is the value of the unique primary identifier of the person table. It is an internal system assigned number.",
    "record_status": "Always active in this projection. The label is a literal, not derived from death evidence, source presence, clinical activity or effective dates; it does not assert that the person is alive.",
    "loaded_at": "map_death.ADC_UPDT carried unchanged for the contributing death-evidence row. This is bronze load provenance, not recorded/calculated death time, last-known activity or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_person_death_evidence"),
    comment="Internal quality-controlled twin of reference_person_death_evidence: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PERSON_DEATH_EVIDENCE_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PERSON_DEATH_EVIDENCE_ADVISORY_RULES)
def _gold_qc_reference_person_death_evidence():
    """Quality-controlled twin of journey_reference.person_death_evidence."""
    df = _qc(
        "reference_person_death_evidence",
        REFERENCE_PERSON_DEATH_EVIDENCE_SELECT,
        fk_columns=["person_id"],
    )
    return _with_comments(df, REFERENCE_PERSON_DEATH_EVIDENCE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.person_death_evidence"),
    comment=(
        "One corroborating death-evidence row; cause of death is IG-sensitive. Gold QC twin "
        "of the silver product: 4 columns are repaired or nulled, 2 rule(s) drop rows, 1 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_person_death_evidence():
    """Contract-v2 public twin of reference_person_death_evidence; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_person_death_evidence")).selectExpr(
        '`person_death_evidence_key` AS `person_death_evidence_key`',
        '`person_id` AS `person_id`',
        '`deceased_datetime_raw` AS `deceased_datetime_raw`',
        '`deceased_datetime` AS `deceased_datetime`',
        '`calculated_death_date` AS `calculated_death_date`',
        '`precision_flag` AS `precision_flag`',
        '`precision_desc` AS `precision_desc`',
        '`source_desc` AS `source_desc`',
        '`method_desc` AS `method_desc`',
        '`death_date_estimate_source` AS `death_date_estimate_source`',
        '`cause_of_death` AS `cause_of_death`',
        '`autopsy_desc` AS `autopsy_desc`',
        '`age_at_death` AS `age_at_death`',
        '`last_encounter_datetime` AS `last_encounter_datetime`',
        '`last_clinical_event_datetime` AS `last_clinical_event_datetime`',
        '`last_known_activity_datetime` AS `last_known_activity_datetime`',
        '`clinical_event_count` AS `clinical_event_count`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PERSON_DEATH_EVIDENCE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.practitioner ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_SELECT = [
    '`practitioner_key` AS `practitioner_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`source_practitioner_id` AS `source_practitioner_id`',
    '`active` AS `active`',
    '`name` AS `name`',
    '`physician_ind` AS `physician_ind`',
    "CASE WHEN UPPER(TRIM(CAST(`position_code` AS STRING))) = '0' THEN NULL ELSE `position_code` END AS `position_code`",
    '`position_display` AS `position_display`',
    "CASE WHEN UPPER(TRIM(CAST(`practitioner_type_code` AS STRING))) = '0' THEN NULL ELSE `practitioner_type_code` END AS `practitioner_type_code`",
    '`practitioner_type_display` AS `practitioner_type_display`',
    '`primary_location_key` AS `primary_location_key`',
    '`medical_service_key` AS `medical_service_key`',
    '`npi` AS `npi`',
    '`doctor_number` AS `doctor_number`',
    '`gdp_number` AS `gdp_number`',
    '`external_provider_id` AS `external_provider_id`',
    '`valid_from` AS `valid_from`',
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN CAST(`valid_to` AS DATE) = DATE'2100-12-31' OR CAST(`valid_to` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999 OR CAST(`valid_to` AS DATE) = DATE'2100-01-01' THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _reference_practitioner_metadata; source history stays on the main research table.
REFERENCE_PRACTITIONER_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 351,201 rows
    # (0.000569%) when profiled on 2026-08-24.
    "gold.reference.practitioner.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 373 of 351,201
    # rows (0.106%) when profiled on 2026-08-24.
    "gold.reference.practitioner.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

REFERENCE_PRACTITIONER_COLUMN_COMMENTS = {
    "practitioner_key": "Deterministic SHA-256 practitioner key.",
    "practitioner_id": "Millennium personnel PERSON_ID; primary key of this table.",
    "source_practitioner_id": "Millennium personnel PERSON_ID as BIGINT.",
    "active": "Source active indicator.",
    "name": "Source formatted practitioner name.",
    "physician_ind": "Source physician indicator.",
    "position_code": "Source position code.",
    "position_display": "Source position display.",
    "practitioner_type_code": "Source personnel type code.",
    "practitioner_type_display": "Source personnel type display.",
    "primary_location_key": "Deterministic key of the primary assigned or inferred location.",
    "medical_service_key": "Deterministic key of the selected medical service.",
    "npi": "Selected NPI identifier.",
    "doctor_number": "Selected organization doctor number.",
    "gdp_number": "Selected dental practitioner number.",
    "external_provider_id": "Selected external provider identifier.",
    "valid_from": "Source validity start.",
    "valid_to": "Source validity end.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source row identifier.",
    "record_status": "Directory rows are active when map_medical_personnel.ACTIVE_IND equals 1, otherwise superseded, including when the flag is null. Personnel referenced by clinical records but absent from the directory are published as placeholders with literal active status. Neither arm emits retracted; placeholder active status is not evidence of an active personnel-directory record.",
    "loaded_at": "Directory rows carry map_medical_personnel.ADC_UPDT only; clinical-reference clocks are not added to known personnel. For personnel absent from that directory, placeholders use the maximum clock across their contributing encounter, diagnosis, problem, implant, form, event, medication, text, appointment and theatre references. Implant references first coalesce SOURCE_MAX_ADC_UPDT with ADC_UPDT; other references use ADC_UPDT. This is arm-specific ingestion provenance, not a uniform implant clock or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner"),
    comment="Internal quality-controlled twin of reference_practitioner: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(REFERENCE_PRACTITIONER_ADVISORY_RULES)
def _gold_qc_reference_practitioner():
    """Quality-controlled twin of journey_reference.practitioner."""
    df = _qc("reference_practitioner", REFERENCE_PRACTITIONER_SELECT)
    return _with_comments(df, REFERENCE_PRACTITIONER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.practitioner"),
    comment=(
        "One effective-dated Millennium practitioner or personnel record. Gold QC twin of the "
        "silver product: 3 columns are repaired or nulled, 2 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_practitioner():
    """Contract-v2 public twin of reference_practitioner; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner")).selectExpr(
        '`practitioner_key` AS `practitioner_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`source_practitioner_id` AS `source_practitioner_id`',
        '`active` AS `active`',
        '`name` AS `name`',
        '`physician_ind` AS `physician_ind`',
        '`position_code` AS `position_code`',
        '`position_display` AS `position_display`',
        '`practitioner_type_code` AS `practitioner_type_code`',
        '`practitioner_type_display` AS `practitioner_type_display`',
        '`primary_location_key` AS `primary_location_key`',
        '`medical_service_key` AS `medical_service_key`',
        '`npi` AS `npi`',
        '`doctor_number` AS `doctor_number`',
        '`gdp_number` AS `gdp_number`',
        '`external_provider_id` AS `external_provider_id`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PRACTITIONER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.practitioner_group ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_GROUP_SELECT = [
    '`practitioner_group_key` AS `practitioner_group_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`in_practitioner_dimension_ind` AS `in_practitioner_dimension_ind`',
    '`source_group_id` AS `source_group_id`',
    '`group_name` AS `group_name`',
    '`group_label` AS `group_label`',
    '`group_type_meaning` AS `group_type_meaning`',
    '`group_type_display` AS `group_type_display`',
    '`primary_ind` AS `primary_ind`',
    '`relation_active_ind` AS `relation_active_ind`',
    '`relation_beg_effective` AS `relation_beg_effective`',
    "CASE WHEN CAST(`relation_end_effective` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `relation_end_effective` END AS `relation_end_effective`",
    '`group_active_ind` AS `group_active_ind`',
    '`group_beg_effective` AS `group_beg_effective`',
    "CASE WHEN CAST(`group_end_effective` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `group_end_effective` END AS `group_end_effective`",
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PRACTITIONER_GROUP_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 251,212 rows of 251,212 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_PRACTITIONER_GROUP_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 251,212 at the profile.
    "gold.reference.practitioner_group.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_PRACTITIONER_GROUP_COLUMN_COMMENTS = {
    "practitioner_group_key": "Deterministic SHA-256 key for the practitioner-group membership.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner group record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_group`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "in_practitioner_dimension_ind": "Exact Millennium personnel identifier.",
    "source_group_id": "Native personnel-group identifier from `PRSNL_GROUP_ID`, retained as a string.",
    "group_name": "Raw personnel-group name.",
    "group_label": "Best label from group name, description and code metadata.",
    "group_type_meaning": "CDF meaning for the group type code.",
    "group_type_display": "Code-value display for the group type.",
    "primary_ind": "Source relationship primary indicator.",
    "relation_active_ind": "Source numeric active indicator for the personnel-to-group relationship, carried from `RELATION_ACTIVE_IND` without boolean conversion.",
    "relation_beg_effective": "Relationship begin-effective timestamp.",
    "relation_end_effective": "Relationship end-effective timestamp.",
    "group_active_ind": "Source numeric active indicator for the personnel group, carried from `GROUP_ACTIVE_IND` without boolean conversion.",
    "group_beg_effective": "Group begin-effective timestamp.",
    "group_end_effective": "Group end-effective timestamp.",
    "_source_system": "Value describing source system for the practitioner group record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner group record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Personnel-group relationship identifier.",
    "record_status": "Retracted when map_medical_personnel_group.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. Relation/group activity, validity dates and personnel-directory membership do not determine this label, and no superseded status is emitted.",
    "loaded_at": "map_medical_personnel_group.ADC_UPDT carried unchanged for the membership row. The personnel-directory existence join supplies only a person identifier and contributes no timestamp; this is bronze load provenance, not relationship validity time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_group"),
    comment="Internal quality-controlled twin of reference_practitioner_group: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PRACTITIONER_GROUP_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PRACTITIONER_GROUP_ADVISORY_RULES)
def _gold_qc_reference_practitioner_group():
    """Quality-controlled twin of journey_reference.practitioner_group."""
    df = _qc("reference_practitioner_group", REFERENCE_PRACTITIONER_GROUP_SELECT)
    return _with_comments(df, REFERENCE_PRACTITIONER_GROUP_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.practitioner_group"),
    comment=(
        "One staff-group membership with practitioner-dimension orphan evidence. Gold QC twin "
        "of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 1 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_practitioner_group():
    """Contract-v2 public twin of reference_practitioner_group; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_group")).selectExpr(
        '`practitioner_group_key` AS `practitioner_group_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`in_practitioner_dimension_ind` AS `in_practitioner_dimension_ind`',
        '`source_group_id` AS `source_group_id`',
        '`group_name` AS `group_name`',
        '`group_label` AS `group_label`',
        '`group_type_meaning` AS `group_type_meaning`',
        '`group_type_display` AS `group_type_display`',
        '`primary_ind` AS `primary_ind`',
        '`relation_active_ind` AS `relation_active_ind`',
        '`relation_beg_effective` AS `relation_beg_effective`',
        '`relation_end_effective` AS `relation_end_effective`',
        '`group_active_ind` AS `group_active_ind`',
        '`group_beg_effective` AS `group_beg_effective`',
        '`group_end_effective` AS `group_end_effective`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PRACTITIONER_GROUP_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.practitioner_identifier ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_IDENTIFIER_SELECT = [
    '`practitioner_identifier_key` AS `practitioner_identifier_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`source_alias_id` AS `source_alias_id`',
    '`alias_type_meaning` AS `alias_type_meaning`',
    '`alias_type_display` AS `alias_type_display`',
    '`alias` AS `alias`',
    '`active_ind` AS `active_ind`',
    '`effective_now_ind` AS `effective_now_ind`',
    '`beg_effective` AS `beg_effective`',
    "CASE WHEN CAST(`end_effective` AS DATE) = DATE'2100-12-31' OR CAST(`end_effective` AS DATE) = DATE'2100-01-01' THEN NULL ELSE `end_effective` END AS `end_effective`",
    "CASE WHEN UPPER(TRIM(CAST(`contributor_system_code` AS STRING))) = '0' THEN NULL ELSE `contributor_system_code` END AS `contributor_system_code`",
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PRACTITIONER_IDENTIFIER_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 519,155 rows of 519,155 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_PRACTITIONER_IDENTIFIER_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 519,155 rows
    # (0.000385%) when profiled on 2026-08-24.
    "gold.reference.practitioner_identifier.beg_effective.future_owner":
        "NOT COALESCE((CAST(`beg_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 519,155 rows
    # (0.000385%) when profiled on 2026-08-24.
    "gold.reference.practitioner_identifier.end_effective.future_owner":
        "NOT COALESCE((CAST(`end_effective` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 519,155 at the profile.
    "gold.reference.practitioner_identifier.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_PRACTITIONER_IDENTIFIER_COLUMN_COMMENTS = {
    "practitioner_identifier_key": "Deterministic SHA-256 key for the practitioner alias row.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner identifier record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_alias`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "source_alias_id": "Native personnel-alias record identifier from `PRSNL_ALIAS_ID`, retained as a string.",
    "alias_type_meaning": "CDF meaning for the alias type.",
    "alias_type_display": "Display for the alias type.",
    "alias": "Raw personnel alias value.",
    "active_ind": "Source numeric active indicator for the personnel alias, carried from `ACTIVE_IND` without boolean conversion.",
    "effective_now_ind": "Whether the alias is effective at pipeline time.",
    "beg_effective": "Alias begin-effective timestamp.",
    "end_effective": "Alias end-effective timestamp.",
    "contributor_system_code": "Alias contributor-system code.",
    "_source_system": "Value describing source system for the practitioner identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner identifier record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Native personnel-alias record identifier from `PRSNL_ALIAS_ID`, retained as a string.",
    "record_status": "Retracted when map_medical_personnel_alias.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. ACTIVE_IND, EFFECTIVE_NOW_IND and alias validity dates do not determine this label, and no superseded status is emitted.",
    "loaded_at": "map_medical_personnel_alias.ADC_UPDT carried unchanged for the alias row. This is bronze load provenance, not the alias begin/end-effective timestamp or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_identifier"),
    comment="Internal quality-controlled twin of reference_practitioner_identifier: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PRACTITIONER_IDENTIFIER_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PRACTITIONER_IDENTIFIER_ADVISORY_RULES)
def _gold_qc_reference_practitioner_identifier():
    """Quality-controlled twin of journey_reference.practitioner_identifier."""
    df = _qc("reference_practitioner_identifier", REFERENCE_PRACTITIONER_IDENTIFIER_SELECT)
    return _with_comments(df, REFERENCE_PRACTITIONER_IDENTIFIER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.practitioner_identifier"),
    comment=(
        "One staff alias assignment; alias values can include IG-sensitive staff postcodes. "
        "Gold QC twin of the silver product: 2 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 3 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_practitioner_identifier():
    """Contract-v2 public twin of reference_practitioner_identifier; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_identifier")).selectExpr(
        '`practitioner_identifier_key` AS `practitioner_identifier_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`source_alias_id` AS `source_alias_id`',
        '`alias_type_meaning` AS `alias_type_meaning`',
        '`alias_type_display` AS `alias_type_display`',
        '`alias` AS `alias`',
        '`active_ind` AS `active_ind`',
        '`effective_now_ind` AS `effective_now_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`contributor_system_code` AS `contributor_system_code`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PRACTITIONER_IDENTIFIER_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.practitioner_location_evidence ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_SELECT = [
    '`practitioner_location_evidence_key` AS `practitioner_location_evidence_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`practitioner_person_id` AS `practitioner_person_id`',
    '`location_code` AS `location_code`',
    '`event_count` AS `event_count`',
    "CASE WHEN CAST(`first_event_datetime_raw` AS DATE) = DATE'1899-12-30' THEN NULL ELSE `first_event_datetime_raw` END AS `first_event_datetime_raw`",
    'CASE WHEN YEAR(CAST(`first_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `first_event_datetime` END AS `first_event_datetime`',
    "CASE WHEN CAST(`last_event_datetime_raw` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_event_datetime_raw` AS TIMESTAMP)) = 1970 THEN NULL ELSE CASE WHEN CAST(`last_event_datetime_raw` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`last_event_datetime_raw` AS DATE)) < 9999 THEN NULL ELSE `last_event_datetime_raw` END END AS `last_event_datetime_raw`",
    'CASE WHEN CAST(`last_event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS OR YEAR(CAST(`last_event_datetime` AS TIMESTAMP)) = 1970 THEN NULL ELSE `last_event_datetime` END AS `last_event_datetime`',
    '`location_rank` AS `location_rank`',
    '`top_count_tie_count` AS `top_count_tie_count`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_COLUMN_COMMENTS = {
    "practitioner_location_evidence_key": "Deterministic SHA-256 key for practitioner-location evidence.",
    "practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT; joins to reference_practitioner.practitioner_id.",
    "practitioner_person_id": "Source-system identifier for the practitioner person associated with each practitioner location evidence record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_medical_personnel_location_evidence`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "location_code": "Observed nurse-unit code.",
    "event_count": "Current canonical logical-event count at this location.",
    "first_event_datetime_raw": "Earliest supporting event timestamp.",
    "first_event_datetime": "Earliest supporting event timestamp.",
    "last_event_datetime_raw": "Latest supporting event timestamp.",
    "last_event_datetime": "Latest supporting event timestamp.",
    "location_rank": "Deterministic personnel location rank.",
    "top_count_tie_count": "Number of locations tied on the maximum event count.",
    "_source_system": "Value describing source system for the practitioner location evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the practitioner location evidence record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Observed nurse-unit code.",
    "loaded_at": "map_medical_personnel_location_evidence.EVIDENCE_ADC_UPDT carried unchanged. It is the supplied supporting-evidence clock, not FIRST_EVENT_DT_TM, LAST_EVENT_DT_TM or Silver refresh time; Silver performs no further timestamp aggregation or clamping on this field.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner_location_evidence"),
    comment="Internal quality-controlled twin of reference_practitioner_location_evidence: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_practitioner_location_evidence():
    """Quality-controlled twin of journey_reference.practitioner_location_evidence."""
    df = _qc("reference_practitioner_location_evidence", REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_SELECT)
    return _with_comments(df, REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.practitioner_location_evidence"),
    comment=(
        "One staff location-evidence summary with sentinel-clamped bounds. Gold QC twin of "
        "the silver product: 4 columns are repaired or nulled, 0 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_practitioner_location_evidence():
    """Contract-v2 public twin of reference_practitioner_location_evidence; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner_location_evidence")).selectExpr(
        '`practitioner_location_evidence_key` AS `practitioner_location_evidence_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`practitioner_person_id` AS `practitioner_person_id`',
        '`location_code` AS `location_code`',
        '`event_count` AS `event_count`',
        '`first_event_datetime_raw` AS `first_event_datetime_raw`',
        '`first_event_datetime` AS `first_event_datetime`',
        '`last_event_datetime_raw` AS `last_event_datetime_raw`',
        '`last_event_datetime` AS `last_event_datetime`',
        '`location_rank` AS `location_rank`',
        '`top_count_tie_count` AS `top_count_tie_count`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PRACTITIONER_LOCATION_EVIDENCE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.research_study ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_RESEARCH_STUDY_SELECT = [
    '`research_study_key` AS `research_study_key`',
    '`source_protocol_id` AS `source_protocol_id`',
    '`study_mnemonic` AS `study_mnemonic`',
    '`study_mnemonic_key` AS `study_mnemonic_key`',
    '`protocol_type_code` AS `protocol_type_code`',
    '`protocol_type_desc` AS `protocol_type_desc`',
    '`protocol_phase_code` AS `protocol_phase_code`',
    '`protocol_phase_desc` AS `protocol_phase_desc`',
    '`protocol_status_code` AS `protocol_status_code`',
    '`protocol_status_desc` AS `protocol_status_desc`',
    '`protocol_purpose_code` AS `protocol_purpose_code`',
    '`protocol_purpose_desc` AS `protocol_purpose_desc`',
    '`parent_protocol_id` AS `parent_protocol_id`',
    '`previous_protocol_id` AS `previous_protocol_id`',
    '`root_protocol_ind` AS `root_protocol_ind`',
    '`beg_effective` AS `beg_effective`',
    "CASE WHEN CAST(`end_effective` AS DATE) = DATE'2100-12-31' THEN NULL ELSE `end_effective` END AS `end_effective`",
    '`open_ended_ind` AS `open_ended_ind`',
    '`display_ind` AS `display_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_RESEARCH_STUDY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 1,055 rows of 1,055 that are
    # current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_RESEARCH_STUDY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 1,055 at the profile.
    "gold.reference.research_study.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_RESEARCH_STUDY_COLUMN_COMMENTS = {
    "research_study_key": "Deterministic SHA-256 key for the Cerner research protocol.",
    "source_protocol_id": "Source-system identifier for the source protocol associated with each research study record. It is derived from bronze field `PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "study_mnemonic": "Mnemonic assigned to the protocol",
    "study_mnemonic_key": "Mnemonic of the protocol stripped of special characters",
    "protocol_type_code": "This field contains a code identifying the type of protocol; therapeutic, non-therapeutic, BCM guidelines etc.",
    "protocol_type_desc": "The description for the code value",
    "protocol_phase_code": "This field contains a code for the phase of the protocol.  Examples of phases would include, but not be limited to, phase 1, phase 1A, phase 1B, phase 2, phase 3, etc.",
    "protocol_phase_desc": "The description for the code value",
    "protocol_status_code": "This field contains a code identifying the status of the protocol/study: approved, open, suspended (temporarily closed to accrual), closed to accrual, closed and terminated.",
    "protocol_status_desc": "The description for the code value",
    "protocol_purpose_code": "This field contains a code identifying the purpose for the protocol/study.  Examples of purposes would include, but not be limited to, cancer control, epidemiology, etc.",
    "protocol_purpose_desc": "The description for the code value",
    "parent_protocol_id": "Source-system identifier for the parent protocol associated with each research study record. It is derived from bronze field `PARENT_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "previous_protocol_id": "Source-system identifier for the previous protocol associated with each research study record. It is derived from bronze field `PREV_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "root_protocol_ind": "Indicator of whether root protocol applies to the research study record. It is derived from bronze field `PARENT_PROT_MASTER_ID` in `4_prod.bronze.map_research_study`, selected as the provenance anchor from 2 recorded bronze source fields. Null means the source did not state the indicator and must not be interpreted as false.",
    "beg_effective": "The date and time for which this table row becomes effective.  Normally, this will be the date and time the row is added, but could be a past or future date and time.",
    "end_effective": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "open_ended_ind": "The date/time after which the row is no longer valid as active current data.  This may be valued with the date that the row became inactive.",
    "display_ind": "This field is an indicator that tells whether the protocol enrollment information for this protocol will be displayed in the electronic medical record.",
    "_source_system": "Value describing source system for the research study record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the research study record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Source-system identifier for the source row associated with each research study record. It is derived from bronze field `PROT_MASTER_ID` in `4_prod.bronze.map_research_study`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "record_status": "Always active in this projection. Protocol status, display flag, begin/end-effective dates and amendment parent/previous links do not determine this literal label; it does not assert that a study is open or recruiting.",
    "loaded_at": "map_research_study.ADC_UPDT carried unchanged for the protocol/amendment row. No parent or previous-protocol timestamp is joined; this is bronze ingestion provenance, not protocol validity time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_research_study"),
    comment="Internal quality-controlled twin of reference_research_study: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_RESEARCH_STUDY_MANDATORY_RULES)
@dp.expect_all(REFERENCE_RESEARCH_STUDY_ADVISORY_RULES)
def _gold_qc_reference_research_study():
    """Quality-controlled twin of journey_reference.research_study."""
    df = _qc("reference_research_study", REFERENCE_RESEARCH_STUDY_SELECT)
    return _with_comments(df, REFERENCE_RESEARCH_STUDY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.research_study"),
    comment=(
        "One Cerner research protocol amendment with parent and previous-protocol lineage. "
        "Gold QC twin of the silver product: 1 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 1 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_research_study():
    """Contract-v2 public twin of reference_research_study; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_research_study")).selectExpr(
        '`research_study_key` AS `research_study_key`',
        '`source_protocol_id` AS `source_protocol_id`',
        '`study_mnemonic` AS `study_mnemonic`',
        '`study_mnemonic_key` AS `study_mnemonic_key`',
        '`protocol_type_code` AS `protocol_type_code`',
        '`protocol_type_desc` AS `protocol_type_desc`',
        '`protocol_phase_code` AS `protocol_phase_code`',
        '`protocol_phase_desc` AS `protocol_phase_desc`',
        '`protocol_status_code` AS `protocol_status_code`',
        '`protocol_status_desc` AS `protocol_status_desc`',
        '`protocol_purpose_code` AS `protocol_purpose_code`',
        '`protocol_purpose_desc` AS `protocol_purpose_desc`',
        '`parent_protocol_id` AS `parent_protocol_id`',
        '`previous_protocol_id` AS `previous_protocol_id`',
        '`root_protocol_ind` AS `root_protocol_ind`',
        '`beg_effective` AS `beg_effective`',
        '`end_effective` AS `end_effective`',
        '`open_ended_ind` AS `open_ended_ind`',
        '`display_ind` AS `display_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_RESEARCH_STUDY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.service ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_SERVICE_SELECT = [
    '`service_key` AS `service_key`',
    '`service_id` AS `service_id`',
    '`source_service_code` AS `source_service_code`',
    '`name` AS `name`',
    '`status` AS `status`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_SERVICE_COLUMN_COMMENTS = {
    "service_key": "Deterministic SHA-256 service key.",
    "service_id": "Millennium MEDSERVICE_GROUP_ID; primary key of this table.",
    "source_service_code": "Source personnel-group service code.",
    "name": "Source service name.",
    "status": "Source-derived service status.",
    "source_table": "Fully qualified bronze source table.",
    "source_row_id": "Stable source service identifier.",
    "loaded_at": "Maximum map_medical_personnel.ADC_UPDT over source rows sharing the non-null MEDSERVICE_GROUP_ID. This includes all contributing group assignments, not only active assignments or the row supplying the selected service name; it is bronze ingestion provenance, not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_service"),
    comment="Internal quality-controlled twin of reference_service: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_service():
    """Quality-controlled twin of journey_reference.service."""
    df = _qc("reference_service", REFERENCE_SERVICE_SELECT)
    return _with_comments(df, REFERENCE_SERVICE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.service"),
    comment=(
        "Thin v1 medical-service dimension from practitioner assignments. Gold QC twin of the "
        "silver product: 0 columns are repaired or nulled, 0 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_service():
    """Contract-v2 public twin of reference_service; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_service")).selectExpr(
        '`service_key` AS `service_key`',
        '`service_id` AS `service_id`',
        '`source_service_code` AS `source_service_code`',
        '`name` AS `name`',
        '`status` AS `status`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_SERVICE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.theatre_attendance ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_ATTENDANCE_SELECT = [
    '`theatre_attendance_key` AS `theatre_attendance_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    '`attendee_practitioner_id` AS `attendee_practitioner_id`',
    '`attendee_personnel_id` AS `attendee_personnel_id`',
    '`role_description` AS `role_description`',
    '`signing_attendee_ind` AS `signing_attendee_ind`',
    '`in_datetime` AS `in_datetime`',
    '`in_datetime_quality` AS `in_datetime_quality`',
    '`out_datetime` AS `out_datetime`',
    '`out_datetime_quality` AS `out_datetime_quality`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`surgical_area_desc` AS `surgical_area_desc`',
    '`active_ind` AS `active_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_THEATRE_ATTENDANCE_COLUMN_COMMENTS = {
    "theatre_attendance_key": "Deterministic SHA-256 key for the theatre attendance row.",
    "theatre_case_key": "Source-preserved theatre case key.",
    "case_link_status": "S14 theatre feeder and Journey standard block; retained as a source-faithful bronze input or Journey standard-block field.",
    "attendee_practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT for the attendee.",
    "attendee_personnel_id": "Journey care_participation practitioner FK; no practitioner attributes are re-landed.",
    "role_description": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "signing_attendee_ind": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "in_datetime": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "in_datetime_quality": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "out_datetime": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "out_datetime_quality": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT.",
    "surgical_area_desc": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "active_ind": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "_source_system": "Value describing source system for the theatre attendance record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the theatre attendance record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "Journey care_participation; retained as a source-faithful bronze input or Journey standard-block field.",
    "loaded_at": "map_theatre_case_attendance.ADC_UPDT carried unchanged for the attendance row. The parent-case existence join adds no timestamp; this is bronze ingestion provenance, not attendee in/out time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_attendance"),
    comment="Internal quality-controlled twin of reference_theatre_attendance: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_theatre_attendance():
    """Quality-controlled twin of journey_reference.theatre_attendance."""
    # 101,593 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 7 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_attendance",
        REFERENCE_THEATRE_ATTENDANCE_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return _with_comments(df, REFERENCE_THEATRE_ATTENDANCE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.theatre_attendance"),
    comment=(
        "One SurgiNet theatre attendance row with parent-case linkage evidence. Gold QC twin "
        "of the silver product: 2 columns are repaired or nulled, 0 check(s) are advisory. "
        "Each rule states its reason in the pipeline notebook, and Lakeflow expectation "
        "metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_theatre_attendance():
    """Contract-v2 public twin of reference_theatre_attendance; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_attendance")).selectExpr(
        '`theatre_attendance_key` AS `theatre_attendance_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`attendee_practitioner_id` AS `attendee_practitioner_id`',
        '`attendee_personnel_id` AS `attendee_personnel_id`',
        '`role_description` AS `role_description`',
        '`signing_attendee_ind` AS `signing_attendee_ind`',
        '`in_datetime` AS `in_datetime`',
        '`in_datetime_quality` AS `in_datetime_quality`',
        '`out_datetime` AS `out_datetime`',
        '`out_datetime_quality` AS `out_datetime_quality`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`surgical_area_desc` AS `surgical_area_desc`',
        '`active_ind` AS `active_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_THEATRE_ATTENDANCE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.theatre_case_milestone ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_CASE_MILESTONE_SELECT = [
    '`theatre_case_milestone_key` AS `theatre_case_milestone_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    "CASE WHEN UPPER(TRIM(CAST(`task_assay_code` AS STRING))) = '0' THEN NULL ELSE `task_assay_code` END AS `task_assay_code`",
    '`task_assay_description` AS `task_assay_description`',
    '`stage_description` AS `stage_description`',
    '`case_time_datetime` AS `case_time_datetime`',
    '`case_time_quality` AS `case_time_quality`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`active_ind` AS `active_ind`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_THEATRE_CASE_MILESTONE_COLUMN_COMMENTS = {
    "theatre_case_milestone_key": "Deterministic SHA-256 key for the theatre milestone row.",
    "theatre_case_key": "Source-preserved theatre case key.",
    "case_link_status": "S14 theatre feeder and Journey standard block; retained as a source-faithful bronze input or Journey standard-block field.",
    "task_assay_code": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "task_assay_description": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "stage_description": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "case_time_datetime": "S14 theatre performed-milestone time after deterministic quality bounds.",
    "case_time_quality": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT.",
    "active_ind": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "_source_system": "Value describing source system for the theatre case milestone record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the theatre case milestone record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "S14 theatre activity; retained as a source-faithful bronze input or Journey standard-block field.",
    "loaded_at": "map_theatre_case_times.ADC_UPDT carried unchanged for the milestone row. The parent-case existence join adds no timestamp; this is bronze ingestion provenance, not CASE_TIME_DT_TM or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_case_milestone"),
    comment="Internal quality-controlled twin of reference_theatre_case_milestone: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_theatre_case_milestone():
    """Quality-controlled twin of journey_reference.theatre_case_milestone."""
    # 122,160 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    # 8 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_case_milestone",
        REFERENCE_THEATRE_CASE_MILESTONE_SELECT,
        fk_columns=["encounter_id", "person_id"],
    )
    return _with_comments(df, REFERENCE_THEATRE_CASE_MILESTONE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.theatre_case_milestone"),
    comment=(
        "One SurgiNet theatre case-time milestone with parent-case linkage evidence. Gold QC "
        "twin of the silver product: 3 columns are repaired or nulled, 0 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_theatre_case_milestone():
    """Contract-v2 public twin of reference_theatre_case_milestone; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_case_milestone")).selectExpr(
        '`theatre_case_milestone_key` AS `theatre_case_milestone_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`task_assay_code` AS `task_assay_code`',
        '`task_assay_description` AS `task_assay_description`',
        '`stage_description` AS `stage_description`',
        '`case_time_datetime` AS `case_time_datetime`',
        '`case_time_quality` AS `case_time_quality`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`active_ind` AS `active_ind`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_THEATRE_CASE_MILESTONE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.theatre_implant ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_THEATRE_IMPLANT_SELECT = [
    '`theatre_implant_key` AS `theatre_implant_key`',
    '`theatre_case_key` AS `theatre_case_key`',
    '`case_link_status` AS `case_link_status`',
    '`item_id` AS `item_id`',
    '`manufacturer` AS `manufacturer`',
    '`model_number` AS `model_number`',
    '`catalog_number` AS `catalog_number`',
    '`serial_number` AS `serial_number`',
    '`lot_number` AS `lot_number`',
    '`batch_number` AS `batch_number`',
    '`implant_site` AS `implant_site`',
    '`implant_size` AS `implant_size`',
    '`quantity` AS `quantity`',
    '`expiry_date` AS `expiry_date`',
    '`free_text_item_desc` AS `free_text_item_desc`',
    '`implanted_by_practitioner_id` AS `implanted_by_practitioner_id`',
    '`mill_implant_event_id` AS `mill_implant_event_id`',
    '`implant_link_method` AS `implant_link_method`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`document_type_desc` AS `document_type_desc`',
    '`_source_system` AS `_source_system`',
    '`_source_table` AS `_source_table`',
    '`_source_row_id` AS `_source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_THEATRE_IMPLANT_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 55,523 of 205,632
    # rows (27%) when profiled on 2026-08-24.
    "gold.reference.theatre_implant.expiry_date.future_owner":
        "NOT COALESCE((CAST(`expiry_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}

REFERENCE_THEATRE_IMPLANT_COLUMN_COMMENTS = {
    "theatre_implant_key": "Deterministic SHA-256 key for the theatre implant row.",
    "theatre_case_key": "Source-preserved theatre case key.",
    "case_link_status": "S14 theatre feeder and Journey standard block; retained as a source-faithful bronze input or Journey standard-block field.",
    "item_id": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "manufacturer": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "model_number": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "catalog_number": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "serial_number": "S7 implant traceability and identity-crosswalk input.",
    "lot_number": "S7 implant traceability input retained because most SurgiNet rows have no Mill counterpart.",
    "batch_number": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "implant_site": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "implant_size": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "quantity": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "expiry_date": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "free_text_item_desc": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "implanted_by_practitioner_id": "Native Millennium personnel PERSON_ID as BIGINT for the implanter.",
    "mill_implant_event_id": "S7 identity crosswalk to map_implant_details.EVENT_ID when person+serial is unique.",
    "implant_link_method": "S7 crosswalk provenance: SERIAL_UNIQUE, SERIAL_AMBIGUOUS or NONE.",
    "person_id": "Native Millennium PERSON_ID as BIGINT.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT.",
    "document_type_desc": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "_source_system": "Value describing source system for the theatre implant record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_table": "Value describing source table for the theatre implant record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "_source_row_id": "S7 procedure implant feeder; retained as a source-faithful bronze input or Journey standard-block field.",
    "loaded_at": "map_theatre_implant_log.ADC_UPDT carried unchanged for the implant-log row. The parent-case existence join adds no timestamp; this is bronze ingestion provenance, not implant/expiry time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_theatre_implant"),
    comment="Internal quality-controlled twin of reference_theatre_implant: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all(REFERENCE_THEATRE_IMPLANT_ADVISORY_RULES)
def _gold_qc_reference_theatre_implant():
    """Quality-controlled twin of journey_reference.theatre_implant."""
    # 3,332 rows point at a encounter_id the spine does not have. The pointer is nulled so
    # it cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "reference_theatre_implant",
        REFERENCE_THEATRE_IMPLANT_SELECT,
        fk_columns=["encounter_id"],
    )
    return _with_comments(df, REFERENCE_THEATRE_IMPLANT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.theatre_implant"),
    comment=(
        "One SurgiNet theatre implant row; serial, lot and batch fields are IG-sensitive. "
        "Gold QC twin of the silver product: 1 columns are repaired or nulled, 1 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_theatre_implant():
    """Contract-v2 public twin of reference_theatre_implant; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_theatre_implant")).selectExpr(
        '`theatre_implant_key` AS `theatre_implant_key`',
        '`theatre_case_key` AS `theatre_case_key`',
        '`case_link_status` AS `case_link_status`',
        '`item_id` AS `item_id`',
        '`manufacturer` AS `manufacturer`',
        '`model_number` AS `model_number`',
        '`catalog_number` AS `catalog_number`',
        '`serial_number` AS `serial_number`',
        '`lot_number` AS `lot_number`',
        '`batch_number` AS `batch_number`',
        '`implant_site` AS `implant_site`',
        '`implant_size` AS `implant_size`',
        '`quantity` AS `quantity`',
        '`expiry_date` AS `expiry_date`',
        '`free_text_item_desc` AS `free_text_item_desc`',
        '`implanted_by_practitioner_id` AS `implanted_by_practitioner_id`',
        '`mill_implant_event_id` AS `mill_implant_event_id`',
        '`implant_link_method` AS `implant_link_method`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`document_type_desc` AS `document_type_desc`',
        '`_source_system` AS `_source_system`',
        '`_source_table` AS `_source_table`',
        '`_source_row_id` AS `_source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_THEATRE_IMPLANT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.value_set ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_VALUE_SET_SELECT = [
    '`value_set_key` AS `value_set_key`',
    '`canonical_url` AS `canonical_url`',
    '`version` AS `version`',
    '`member_system` AS `member_system`',
    "CASE WHEN UPPER(TRIM(CAST(`member_code` AS STRING))) = 'UNKNOWN' THEN NULL ELSE `member_code` END AS `member_code`",
    '`member_display` AS `member_display`',
    '`status` AS `status`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`loaded_at` AS `loaded_at`',
]

REFERENCE_VALUE_SET_COLUMN_COMMENTS = {
    "value_set_key": "Deterministic SHA-256 value-set membership key.",
    "canonical_url": "Canonical value-set URL.",
    "version": "Pinned value-set version.",
    "member_system": "Member coding system.",
    "member_code": "Source-system code representing member for the value set record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "member_display": "Human-readable member label for the value set record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "status": "Value-set membership status.",
    "source_table": "Governed bronze value-set source.",
    "source_row_id": "Stable source membership identity.",
    "loaded_at": "3_lookup.omop.value_set_release.updated_at carried unchanged for the value-set membership row. This is the lookup's update timestamp, not a bronze ADC_UPDT clock, release identifier or Silver refresh time; a missing value remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._reference_value_set"),
    comment="Internal quality-controlled twin of reference_value_set: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
def _gold_qc_reference_value_set():
    """Quality-controlled twin of journey_reference.value_set."""
    df = _qc("reference_value_set", REFERENCE_VALUE_SET_SELECT)
    return _with_comments(df, REFERENCE_VALUE_SET_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.value_set"),
    comment=(
        "Pinned value-set membership materialized from the governed value_set_release lookup "
        "asset (FHIR R4 4.0.1 core sets, UK Core ethnic category, NHS RTT period status); "
        "v3-ServiceDeliveryLocationRoleType is a recorded v1 exclusion. Gold QC twin of the "
        "silver product: 1 columns are repaired or nulled, 0 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_value_set():
    """Contract-v2 public twin of reference_value_set; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_value_set")).selectExpr(
        '`value_set_key` AS `value_set_key`',
        '`canonical_url` AS `canonical_url`',
        '`version` AS `version`',
        '`member_system` AS `member_system`',
        '`member_code` AS `member_code`',
        '`member_display` AS `member_display`',
        '`status` AS `status`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_VALUE_SET_COLUMN_COMMENTS)

# COMMAND ----------

