# Databricks notebook source
# MAGIC %md
# MAGIC # Care pathways
# MAGIC Journeys, scheduling, referrals, waiting lists, RTT, community care, costing and operational activity.

# MAGIC
# MAGIC Reading order: 4 of 8. Numbers guide navigation; Lakeflow schedules datasets by their dependencies.
# MAGIC Shared helpers live in `silver_journey_shared.py`, an importable Python file.

# COMMAND ----------

# Shared projections also serve the patient event index.
from silver_journey_shared import (
    APPOINTMENT_PRIMITIVE_COLUMNS,
    F,
    LIFECYCLE_COLUMN_COMMENTS,
    SRC_APPOINTMENT_RESOURCE,
    SRC_EAL_PROCEDURE,
    SRC_EPISODE_ENCOUNTER,
    SRC_MAT_PREGNANCY,
    SRC_THEATRE_CASE,
    _appointment_canonical,
    _community_care_activity_canonical,
    _community_care_contact_canonical,
    _costed_activity_canonical,
    _cross_qc_primitive,
    _cross_qc_public,
    _drug_expenditure_canonical,
    _elective_access_entry_canonical,
    _hrg_grouping_canonical,
    _lifecycle_source_encounter,
    _lifecycle_source_episode_encounter,
    _mat_pregnancy_dedup,
    _medication_supply_canonical,
    _n,
    _pathway_tracking_canonical,
    _pregnancy_reconciliation_source,
    _referral_canonical,
    _rtt_activity_canonical,
    _rtt_pathway_canonical,
    _waiting_list_entry_canonical,
    materialized_view,
    read_source,
    stable_id,
    subject_key_with_system,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / person relationship

# COMMAND ----------

# ==== Journeys, maternity relationships and temporal membership links ====

SRC_MAT_BIRTH = "4_prod.bronze.map_mat_birth"

PERSON_RELATIONSHIP_PUBLIC_COLUMNS = [
    "person_relationship_key",
    "birth_row_id",
    "source_subject_key",
    "source_person_id",
    "target_subject_key",
    "target_person_id",
    "relationship_type_code",
    "inverse_relationship_type_code",
    "valid_from",
    "valid_to",
    "record_status",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

def _person_relationship_canonical():
    # Assemble normalized person relationship rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    b = read_source(SRC_MAT_BIRTH)
    source_row_id = b.BirthRow_ID.cast("string")
    mother_key, _ = subject_key_with_system(
        [("urn:cerner:person_id", b.MotherPerson_ID)], SRC_MAT_BIRTH, source_row_id
    )
    baby_key, _ = subject_key_with_system(
        [
            ("urn:cerner:person_id", b.BabyPerson_ID),
            ("https://fhir.nhs.uk/Id/nhs-number", b.Baby_NHS),
            ("urn:barts:mrn", b.Baby_MRN),
        ],
        SRC_MAT_BIRTH,
        source_row_id,
    )
    deleted = F.coalesce(b.PregnancySource_DELETE_IND.cast("long"), F.lit(0)) != 0
    # contract v2: retain the relationship SHA as person_relationship_key and publish native BirthRow_ID
    return b.select(
        stable_id("person_relationship:mother_to_child", b.BirthRow_ID)
        .alias("person_relationship_key"),
        b.BirthRow_ID.cast("string").alias("birth_row_id"),
        mother_key.alias("source_subject_key"),
        b.MotherPerson_ID.cast("string").alias("source_person_id"),
        baby_key.alias("target_subject_key"),
        b.BabyPerson_ID.cast("string").alias("target_person_id"),
        F.lit("mother_to_child").alias("relationship_type_code"),
        F.lit("child_of_mother").alias("inverse_relationship_type_code"),
        b.BirthDateTime.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("mat-birth-mother-baby").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_BIRTH).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(b.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(b.BirthSourceRecordUpdatedDateTime, b.PregnancySourceRecordUpdatedDateTime,
                   b.NNUSourceLastUpdate).alias("source_update_timestamp"),
        b.ADC_UPDT.alias("loaded_at"),
    ).select(*PERSON_RELATIONSHIP_PUBLIC_COLUMNS)

PERSON_RELATIONSHIP_COLUMN_COMMENTS = {
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

PERSON_RELATIONSHIP_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

PERSON_RELATIONSHIP_RETIRED_COLUMNS = [

]

PERSON_RELATIONSHIP_LIFECYCLE_COLUMNS = [
    'person_relationship_key',
    'birth_row_id',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_relationship():
    # Assemble person relationship rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _person_relationship_canonical()

@materialized_view(
    name=_n("journey_spine.person_relationship"),
    comment="Source-backed relationships between people, including mother-to-baby links supplied by maternity records.",
    refresh_policy="incremental",
    column_comments=PERSON_RELATIONSHIP_COLUMN_COMMENTS,
)
def person_relationship():
    # Build the declared dataset: Source-backed relationships between people, including mother-
    # to-baby links supplied by maternity records.
    return _lifecycle_source_person_relationship().drop(*PERSON_RELATIONSHIP_LIFECYCLE_FIELDS, *PERSON_RELATIONSHIP_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._person_relationship_metadata'),
    comment='Internal quality and batch metadata for spine_person_relationship; same row grain as the research table. Join keys: person_relationship_key, birth_row_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def person_relationship_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_person_relationship; same row grain as the research table. Join keys:
    # person_relationship_key, birth_row_id.
    return (_lifecycle_source_person_relationship()).select(*PERSON_RELATIONSHIP_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregnancy journey canonical

# COMMAND ----------

def _pregnancy_journey_canonical():
    # Assemble normalized pregnancy journey rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    base = _pregnancy_constructor_base()
    parent_id = stable_id("journey:pregnancy", F.col("_pregnancy_id"))
    # contract v2: rename journey SHA identities to keys in parent and trimester rows
    parent = base.select(
        parent_id.alias("journey_key"),
        F.lit(None).cast("string").alias("parent_journey_key"),
        F.col("_subject_key").alias("subject_key"),
        F.col("_subject_id_system").alias("subject_id_system"),
        F.col("_person_id").alias("person_id"),
        F.lit("pregnancy").alias("journey_type_code"),
        F.lit("Pregnancy").alias("journey_type_display"),
        F.col("_period_start").alias("period_start"),
        F.col("_period_end").alias("period_end"),
        F.col("_status_code").alias("status_code"),
        F.lit("http://snomed.info/sct").alias("defining_coding_system"),
        F.lit("77386006").alias("defining_code"),
        F.lit("Pregnancy").alias("defining_display"),
        F.col("_pregnancy_outcome_code").alias("outcome_code"),
        F.col("_pregnancy_outcome_display").alias("outcome_display"),
        F.col("_pregnancy_id").alias("source_journey_identifier"),
        F.lit("pregnancy").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_PREGNANCY).alias("source_table"),
        F.col("_pregnancy_id").alias("source_row_id"),
        F.date_format("_loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("_source_update_timestamp").alias("source_update_timestamp"),
        F.col("_loaded_at").alias("loaded_at"),
    )

    expanded = base.withColumn("_trimester", F.explode(F.array(F.lit(1), F.lit(2), F.lit(3))))
    raw_start = (
        F.when(F.col("_trimester") == 1, F.col("_period_start"))
        .when(F.col("_trimester") == 2, F.date_add("_period_start", 98).cast("timestamp"))
        .otherwise(F.date_add("_period_start", 196).cast("timestamp"))
    )
    nominal_end = (
        F.when(F.col("_trimester") == 1, F.date_add("_period_start", 97).cast("timestamp"))
        .when(F.col("_trimester") == 2, F.date_add("_period_start", 195).cast("timestamp"))
        .otherwise(F.col("_period_end"))
    )
    child_start = F.when(
        F.col("_period_end").isNull() | (raw_start <= F.col("_period_end")), raw_start
    )
    child_end = F.when(
        child_start.isNotNull(),
        F.when(F.col("_period_end").isNull(), nominal_end)
        .otherwise(F.least(nominal_end, F.col("_period_end"))),
    )
    child = expanded.select(
        stable_id(
            "journey:pregnancy:trimester", F.col("_pregnancy_id"), F.col("_trimester")
        ).alias("journey_key"),
        parent_id.alias("parent_journey_key"),
        F.col("_subject_key").alias("subject_key"),
        F.col("_subject_id_system").alias("subject_id_system"),
        F.col("_person_id").alias("person_id"),
        F.lit("pregnancy_trimester").alias("journey_type_code"),
        F.concat(F.lit("Pregnancy trimester "), F.col("_trimester")).alias("journey_type_display"),
        child_start.alias("period_start"),
        child_end.alias("period_end"),
        F.col("_status_code").alias("status_code"),
        F.lit("urn:journey:pregnancy-structure").alias("defining_coding_system"),
        F.concat(F.lit("trimester-"), F.col("_trimester")).alias("defining_code"),
        F.concat(F.lit("Pregnancy trimester "), F.col("_trimester")).alias("defining_display"),
        F.lit(None).cast("string").alias("outcome_code"),
        F.lit(None).cast("string").alias("outcome_display"),
        F.col("_pregnancy_id").alias("source_journey_identifier"),
        F.lit("pregnancy-trimester").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_PREGNANCY).alias("source_table"),
        F.concat_ws(":", F.col("_pregnancy_id"), F.lit("trimester"), F.col("_trimester"))
        .alias("source_row_id"),
        F.date_format("_loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("_source_update_timestamp").alias("source_update_timestamp"),
        F.col("_loaded_at").alias("loaded_at"),
    )
    return parent.unionByName(child).select(*JOURNEY_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / journey

# COMMAND ----------

JOURNEY_PUBLIC_COLUMNS = [
    "journey_key",
    "parent_journey_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "journey_type_code",
    "journey_type_display",
    "period_start",
    "period_end",
    "status_code",
    "defining_coding_system",
    "defining_code",
    "defining_display",
    "outcome_code",
    "outcome_display",
    "source_journey_identifier",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

def _pregnancy_constructor_base():
    # Combine deduplicated pregnancy records with birth dates and outcomes used to construct
    # maternity journeys.
    p = _mat_pregnancy_dedup().alias("p")
    b = (
        read_source(SRC_MAT_BIRTH)
        .groupBy("Pregnancy_ID")
        .agg(
            F.min("BirthDateTime").alias("_first_birth_datetime"),
            F.max("BirthDateTime").alias("_last_birth_datetime"),
            F.max("PregOutcome_CD").cast("string").alias("_pregnancy_outcome_code"),
            F.max("PregOutcome_DESC").alias("_pregnancy_outcome_display"),
            F.max("ADC_UPDT").alias("_birth_loaded_at"),
            F.max("BirthSourceRecordUpdatedDateTime").alias("_birth_source_update"),
        )
        .alias("b")
    )
    joined = p.join(b, p.Pregnancy_ID == b.Pregnancy_ID, "left")
    source_row_id = p.Pregnancy_ID.cast("string")
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", p.Person_ID),
            ("urn:barts:mrn", p.MRN),
            ("https://fhir.nhs.uk/Id/nhs-number", p.NHS_Number),
        ],
        SRC_MAT_PREGNANCY,
        source_row_id,
    )
    start = F.coalesce(
        p.LastMensPeriodDate,
        F.date_sub(p.ExpectedDeliveryDate, 280).cast("timestamp"),
        p.PregnancyFirstContactDate,
        p.FirstAntenatalAPPTDate,
    )
    end = F.coalesce(
        p.MaternityServiceDischargeDate,
        F.col("b._last_birth_datetime"),
        p.ExpectedDeliveryDate,
    )
    deleted = F.coalesce(p.SOURCE_DELETED_IND.cast("boolean"), F.lit(False))
    completed = p.MaternityServiceDischargeDate.isNotNull() | F.col("b._last_birth_datetime").isNotNull()
    status = (
        F.when(deleted, F.lit("superseded"))
        .when(completed, F.lit("completed"))
        .otherwise(F.lit("active"))
    )
    source_update = F.greatest(
        p.MAT_RECORD_UPDATED_DT,
        p.MSDS_RECORD_UPDATED_DT,
        p.BIRTH_ADC_UPDT,
        F.col("b._birth_source_update"),
    )
    loaded_at = F.greatest(p.ADC_UPDT, F.col("b._birth_loaded_at"))
    # contract v2: publish the resolved pregnancy subject as native Millennium PERSON_ID
    return joined.select(
        p.Pregnancy_ID.cast("string").alias("_pregnancy_id"),
        skey.alias("_subject_key"),
        ssys.alias("_subject_id_system"),
        p.Person_ID.cast("bigint").alias("_person_id"),
        start.alias("_period_start"),
        end.alias("_period_end"),
        status.alias("_status_code"),
        F.col("b._pregnancy_outcome_code"),
        F.col("b._pregnancy_outcome_display"),
        source_update.alias("_source_update_timestamp"),
        loaded_at.alias("_loaded_at"),
    )

JOURNEY_COLUMN_COMMENTS = {
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

JOURNEY_LIFECYCLE_FIELDS = [
    'load_batch_id',
]

JOURNEY_RETIRED_COLUMNS = [

]

JOURNEY_LIFECYCLE_COLUMNS = [
    'journey_key',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_journey():
    # Assemble journey rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _pregnancy_journey_canonical()

@materialized_view(
    name=_n("journey_spine.journey"),
    comment="Deterministic longitudinal journeys; pregnancy-v1 supplies parent and trimester rows.",
    cluster_by=["person_id", "period_start"],
    refresh_policy="incremental",
    column_comments=JOURNEY_COLUMN_COMMENTS,
)
def journey():
    # Build the declared dataset: Deterministic longitudinal journeys; pregnancy-v1 supplies
    # parent and trimester rows.
    return _lifecycle_source_journey().drop(*JOURNEY_LIFECYCLE_FIELDS, *JOURNEY_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_spine._journey_metadata'),
    comment='Internal quality and batch metadata for spine_journey; same row grain as the research table. Join keys: journey_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def journey_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_journey; same
    # row grain as the research table. Join keys: journey_key.
    return (_lifecycle_source_journey()).select(*JOURNEY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / participant

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
JOURNEY_PARTICIPANT_SOURCE_COLUMNS = [
    "journey_participant_key",
    "journey_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "role_code",
    "role_display",
    "valid_from",
    "valid_to",
    "record_status",
    "construction_rule",
    "construction_version",
    "source_system",
    "source_table",
    "source_row_id",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
JOURNEY_PARTICIPANT_PUBLIC_COLUMNS = [
    'journey_participant_key',
    'journey_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'role_code',
    'role_display',
    'valid_from',
    'valid_to',
    'construction_rule',
    'construction_version',
    'source_system',
    'source_table',
    'source_row_id',
    'record_status',
    'source_update_timestamp',
    'loaded_at',
]

JOURNEY_PARTICIPANT_LIFECYCLE_COLUMNS = [
    'journey_participant_key',
    'load_batch_id',
]

def _mother_journey_participants():
    # Create the mother's participant record for each pregnancy journey, carrying the journey's
    # effective period.
    j = _pregnancy_journey_canonical()
    # contract v2: use the renamed journey key and publish an explicit participant key for mother rows
    return j.select(
        stable_id("journey_participant:mother", F.col("journey_key"), F.col("subject_key"))
        .alias("journey_participant_key"),
        "journey_key", "subject_key", "subject_id_system", "person_id",
        F.lit("mother").alias("role_code"),
        F.lit("Mother / primary subject").alias("role_display"),
        F.col("period_start").alias("valid_from"),
        F.col("period_end").alias("valid_to"),
        F.when(F.col("status_code") == "superseded", F.lit("superseded"))
        .otherwise(F.lit("active")).alias("record_status"),
        F.lit("pregnancy-primary-subject").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        "source_system", "source_table", "source_row_id", "load_batch_id", "source_update_timestamp", "loaded_at",
    )

def _baby_journey_participants():
    # Create baby participant records from birth data using the existing person, NHS-number and
    # MRN evidence hierarchy.
    b = read_source(SRC_MAT_BIRTH)
    source_row_id = b.BirthRow_ID.cast("string")
    skey, ssys = subject_key_with_system(
        [
            ("urn:cerner:person_id", b.BabyPerson_ID),
            ("https://fhir.nhs.uk/Id/nhs-number", b.Baby_NHS),
            ("urn:barts:mrn", b.Baby_MRN),
        ],
        SRC_MAT_BIRTH,
        source_row_id,
    )
    deleted = F.coalesce(b.PregnancySource_DELETE_IND.cast("long"), F.lit(0)) != 0
    # contract v2: use explicit journey keys and native baby PERSON_ID for baby participant rows
    return b.select(
        stable_id("journey_participant:baby", b.Pregnancy_ID, b.BirthRow_ID)
        .alias("journey_participant_key"),
        stable_id("journey:pregnancy", b.Pregnancy_ID).alias("journey_key"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        b.BabyPerson_ID.cast("bigint").alias("person_id"),
        F.lit("baby").alias("role_code"), F.lit("Baby").alias("role_display"),
        b.BirthDateTime.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"),
        F.when(deleted, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        F.lit("pregnancy-birth-participant").alias("construction_rule"),
        F.lit("pregnancy-v1").alias("construction_version"),
        F.lit("millennium-maternity").alias("source_system"),
        F.lit(SRC_MAT_BIRTH).alias("source_table"), source_row_id.alias("source_row_id"),
        F.date_format(b.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(b.BirthSourceRecordUpdatedDateTime, b.PregnancySourceRecordUpdatedDateTime,
                   b.NNUSourceLastUpdate).alias("source_update_timestamp"),
        b.ADC_UPDT.alias("loaded_at"),
    )

JOURNEY_PARTICIPANT_COLUMN_COMMENTS = {
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

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_journey_participant():
    # Assemble journey participant rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    mother = _mother_journey_participants().select(*JOURNEY_PARTICIPANT_SOURCE_COLUMNS)
    baby = _baby_journey_participants().select(*JOURNEY_PARTICIPANT_SOURCE_COLUMNS)
    return mother.unionByName(baby)

@materialized_view(
    name=_n("journey_spine.journey_participant"),
    comment="Journey participant roles with subject-key support for unresolved babies.",
    refresh_policy="incremental",
    column_comments=JOURNEY_PARTICIPANT_COLUMN_COMMENTS,
)
def journey_participant():
    # Build the declared dataset: Journey participant roles with subject-key support for
    # unresolved babies.
    return _lifecycle_source_journey_participant().select(*JOURNEY_PARTICIPANT_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_spine._journey_participant_metadata'),
    comment='Internal quality and batch metadata for spine_journey_participant; same row grain as the research table. Join keys: journey_participant_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def journey_participant_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_journey_participant; same row grain as the research table. Join keys:
    # journey_participant_key.
    return (_lifecycle_source_journey_participant()).select(*JOURNEY_PARTICIPANT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spine / link

# COMMAND ----------

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
    e = spark.read.table(_n("journey_events._event_index")).alias("e")
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

JOURNEY_LINK_COLUMN_COMMENTS = {
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
    comment="N:M journey membership links derived from subject and source-supported time overlap.",
    refresh_policy="incremental",
    column_comments=JOURNEY_LINK_COLUMN_COMMENTS,
)
def journey_link():
    # Build the declared dataset: N:M journey membership links derived from subject and source-
    # supported time overlap.
    return _lifecycle_source_journey_link().select(*JOURNEY_LINK_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_spine._journey_link_metadata'),
    comment='Internal quality and batch metadata for spine_journey_link; same row grain as the research table. Join keys: journey_link_key, patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def journey_link_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for spine_journey_link;
    # same row grain as the research table. Join keys: journey_link_key, patient_event_key.
    return (_lifecycle_source_journey_link()).select(*JOURNEY_LINK_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  appointment schedule grouped

# COMMAND ----------

SRC_APPOINTMENT_SCHEDULE = "4_prod.bronze.map_appointment_schedule"

def _appointment_schedule_grouped_query():
    # Group ordered schedule-state evidence per appointment for the appointment's history
    # payload.
    s = read_source(SRC_APPOINTMENT_SCHEDULE)
    entry = F.struct(
        F.coalesce(s.SCHEDULE_SEQ, s.SCHEDULE_ID).cast("long").alias("sequence"),
        s.SCHEDULE_ID.cast("string").alias("schedule_id"),
        s.SCHEDULE_SEQ.cast("long").alias("schedule_sequence"),
        s.SCH_STATE_CD.cast("string").alias("status_code"),
        s.SCH_STATE_DESCRIPTION.alias("status_display"),
        s.SOURCE_STATE_MEANING.alias("status_meaning"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        s.LOCATION_CD.cast("string").alias("location_code"),
        F.coalesce(s.LOCATION_DESCRIPTION, s.LOCATION_FREETEXT).alias("location_display"),
        s.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
    )
    return (
        s.groupBy("SCH_EVENT_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(entry))).alias("_booking_json"),
            F.min("LOCATION_CD").alias("_schedule_location_cd"),
            F.max("SOURCE_ADC_UPDT").alias("_schedule_source_update"),
            F.max("ADC_UPDT").alias("_schedule_loaded_at"),
        )
    )

@materialized_view(
    name=_n("journey_clinical._appointment_schedule_grouped"),
    private=True,
    comment="Internal incremental appointment booking/location history grouped as deterministic JSON.",
    refresh_policy="incremental",
)
def _appointment_schedule_grouped():
    # Build the declared dataset: Internal incremental appointment booking/location history
    # grouped as deterministic JSON.
    return _appointment_schedule_grouped_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  appointment resource grouped

# COMMAND ----------

def _appointment_resource_grouped_query():
    # Group appointment resource and practitioner-role evidence for the appointment's resource
    # payload.
    r = read_source(SRC_APPOINTMENT_RESOURCE)
    entry = F.struct(
        F.coalesce(r.SCHEDULE_SEQ, r.SCH_APPT_ID).cast("long").alias("sequence"),
        r.SCH_APPT_ID.cast("string").alias("appointment_role_id"),
        r.SCHEDULE_ID.cast("string").alias("schedule_id"),
        r.SCH_ROLE_CD.cast("string").alias("role_code"),
        F.coalesce(r.SCH_ROLE_DESCRIPTION, r.ROLE_MEANING).alias("role_display"),
        F.when(r.ALLOCATED_PERSONNEL_ID.isNotNull(),
               stable_id("practitioner:mill", r.ALLOCATED_PERSONNEL_ID))
        .alias("practitioner_id"),
        r.APPT_LOCATION_CD.cast("string").alias("location_code"),
        r.BEG_DT_TM.alias("slot_start"), r.END_DT_TM.alias("slot_end"),
        r.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
    )
    return (
        r.groupBy("SCH_EVENT_ID")
        .agg(
            F.to_json(F.sort_array(F.collect_list(entry))).alias("_resource_json"),
            F.max("BEG_DT_TM").alias("_slot_start"),
            F.max("END_DT_TM").alias("_slot_end"),
            F.min("ALLOCATED_PERSONNEL_ID").alias("_allocated_personnel_id"),
            F.min("APPT_LOCATION_CD").alias("_resource_location_cd"),
            F.max("SOURCE_ADC_UPDT").alias("_resource_source_update"),
            F.max("ADC_UPDT").alias("_resource_loaded_at"),
        )
    )

@materialized_view(
    name=_n("journey_clinical._appointment_resource_grouped"),
    private=True,
    comment="Internal incremental appointment resource/slot history grouped as deterministic JSON.",
    refresh_policy="incremental",
)
def _appointment_resource_grouped():
    # Build the declared dataset: Internal incremental appointment resource/slot history grouped
    # as deterministic JSON.
    return _appointment_resource_grouped_query()

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
APPOINTMENT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'sch_event_id',
    'recurrence_parent_sch_event_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'appointment_type_code',
    'appointment_type_display',
    'status_code',
    'status_display',
    'status_meaning',
    'referral_identifier',
    'requested_datetime',
    'original_requested_start',
    'original_requested_end',
    'first_booked_datetime',
    'requested_practitioner_id',
    'allocated_practitioner_id',
    'location_code',
    'organization_id',
    'recurrence_parent_key',
    'recurrence_type_flag',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

APPOINTMENT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'sch_event_id',
    'identity_status',
    'person_id_resolved',
    'encounter_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  appointment primitive

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._appointment_primitive"),
    private=True,
    comment="Internal orderable appointment join; booking/resource VARIANT arrays cross as JSON.",
    refresh_policy="incremental",
)
def _appointment_primitive():
    # Build the declared dataset: Internal orderable appointment join; booking/resource VARIANT
    # arrays cross as JSON.
    return _appointment_canonical().select(*APPOINTMENT_PRIMITIVE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc appointment

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_appointment"),
    comment="Private JSON bridge and Gold cross-rule flags for appointment.",
    refresh_policy="incremental",
)
def _qc_appointment():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for appointment.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._appointment_primitive")),
        "appointment",
        {},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / appointment

# COMMAND ----------

APPOINTMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "sch_event_id": "Millennium SCH_EVENT_ID; primary key of this table.",
    "recurrence_parent_sch_event_id": "Millennium parent SCH_EVENT_ID for recurring appointments when supplied.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Booked slot start where available",
    "event_end_datetime": "Booked slot end where available.",
    "source_coding_system": "Verbatim appointment-type coding system.",
    "source_code": "Verbatim appointment-type code.",
    "source_display": "Verbatim appointment-type display.",
    "appointment_type_code": "Source appointment-type code.",
    "appointment_type_display": "Source appointment-type display.",
    "status_code": "Source scheduling-state code.",
    "status_display": "Source scheduling-state display.",
    "status_meaning": "Source scheduling-state meaning.",
    "referral_identifier": "UBRN or other referral alias retained as linkage evidence only.",
    "requested_datetime": "Source referral/request timestamp.",
    "original_requested_start": "Original requested slot start.",
    "original_requested_end": "Original requested slot end.",
    "first_booked_datetime": "First booking timestamp supplied by scheduling.",
    "requested_practitioner_id": "Millennium personnel PERSON_ID for the requested practitioner.",
    "allocated_practitioner_id": "Millennium personnel PERSON_ID for the representative allocated practitioner.",
    "location_code": "Millennium scheduling location code.",
    "organization_id": "Millennium ORGANIZATION_ID for the scheduling organization.",
    "recurrence_parent_key": "Deterministic SHA-256 key of the parent appointment.",
    "recurrence_type_flag": "Raw source recurrence flag.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Superseded when map_appointment.SOURCE_PRESENT_IND cast to Boolean is false, ACTIVE_IND cast to LONG is zero, or a non-null END_EFFECTIVE_DT_TM is before 2100-01-01; otherwise active. Missing presence defaults true and missing activity defaults 1. Source absence does not emit retracted; scheduling status and booking/resource flags are separate.",
    "record_status_effective_from": "map_appointment.BEG_EFFECTIVE_DT_TM carried unchanged. No scheduled-slot, requested-start, first-booked or ingestion-time fallback is applied; a missing value remains null.",
    "record_status_effective_to": "For superseded appointment rows, first non-null SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, then ADC_UPDT; otherwise null. The selected end is not range-clamped, so an inactive/absent row can retain a far-future end. This is row-status history, not the scheduled slot end.",
    "source_update_timestamp": "Greatest of map_appointment.SOURCE_ADC_UPDT and the per-SCH_EVENT_ID maxima of SOURCE_ADC_UPDT from map_appointment_schedule and map_appointment_resource. All contributing booking/resource rows participate, not only current rows or those supplying a chosen slot/location. No ADC_UPDT ingestion fallback or Silver refresh timestamp is added.",
    "loaded_at": "Greatest of map_appointment.ADC_UPDT and the per-SCH_EVENT_ID maxima of ADC_UPDT from map_appointment_schedule and map_appointment_resource. This combines all contributing appointment/booking/resource load clocks, independently of chosen slot times, location or personnel; it is bronze ingestion provenance, not Silver refresh time.",
}

@materialized_view(
    name=_n("journey_clinical.appointment"),
    comment="Scheduling appointments with ordered booking and resource history; SCH_EVENT_ID is never treated as a clinical EVENT_ID.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=APPOINTMENT_COLUMN_COMMENTS,
)
def appointment():
    # Build the declared dataset: Scheduling appointments with ordered booking and resource
    # history; SCH_EVENT_ID is never treated as a clinical EVENT_ID.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_appointment")),
        "appointment",
        {
        },
        APPOINTMENT_PUBLIC_COLUMNS,
        APPOINTMENT_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._appointment_metadata'),
    comment='Internal quality and batch metadata for clinical_appointment; same row grain as the research table. Join keys: patient_event_key, sch_event_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def appointment_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_appointment;
    # same row grain as the research table. Join keys: patient_event_key, sch_event_id.
    return (spark.read.table(_n("journey_clinical._qc_appointment"))).select(*APPOINTMENT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / appointment booking

# COMMAND ----------

# ==== journey_clinical.appointment_booking ====

APPOINTMENT_BOOKING_PUBLIC_COLUMNS = [
    "sch_event_id", "sequence", "schedule_id", "schedule_sequence",
    "status_code", "status_display", "status_meaning", "effective_start",
    "effective_end", "location_code", "location_display",
    "source_present_ind", "loaded_at",
]

APPOINTMENT_BOOKING_COLUMN_COMMENTS = {
    "sch_event_id": "Millennium SCH_EVENT_ID of the parent clinical_appointment row.",
    "sequence": "Booking sequence, falling back to SCHEDULE_ID.",
    "schedule_id": "Source scheduling iteration identifier.",
    "schedule_sequence": "Source scheduling sequence.",
    "status_code": "Source scheduling-state code for this iteration.",
    "status_display": "Source scheduling-state display for this iteration.",
    "status_meaning": "Source scheduling-state meaning for this iteration.",
    "effective_start": "Source effective-start timestamp.",
    "effective_end": "Source effective-end timestamp.",
    "location_code": "Source scheduling location code.",
    "location_display": "Source location description, falling back to free text.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the scheduling row.",
}

@materialized_view(
    name=_n("journey_clinical.appointment_booking"),
    comment="One booking or location iteration per scheduling appointment.",
    column_comments=APPOINTMENT_BOOKING_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def appointment_booking():
    # Contract v2 parent/child conservation: publish only bookings whose
    # appointment the parent published. map_appointment_schedule carries 4,563
    # SCH_EVENT_IDs that map_appointment does not hold at all, which left 5,630
    # booking rows unreachable through clinical_appointment. Taking the key set
    # from the parent's own output rather than re-deriving the parent's rule
    # means this cannot drift if that rule changes.
    # Build the declared dataset: One booking or location iteration per scheduling appointment.
    published = spark.read.table(_n("journey_clinical.appointment")).select(
        F.col("sch_event_id").cast("bigint").alias("published_sch_event_id")
    )
    s = read_source(SRC_APPOINTMENT_SCHEDULE)
    s = s.join(
        published,
        s.SCH_EVENT_ID.cast("bigint") == F.col("published_sch_event_id"),
        "left_semi",
    )
    return s.select(
        s.SCH_EVENT_ID.cast("bigint").alias("sch_event_id"),
        F.coalesce(s.SCHEDULE_SEQ, s.SCHEDULE_ID).cast("long").alias("sequence"),
        s.SCHEDULE_ID.cast("string").alias("schedule_id"),
        s.SCHEDULE_SEQ.cast("long").alias("schedule_sequence"),
        s.SCH_STATE_CD.cast("string").alias("status_code"),
        s.SCH_STATE_DESCRIPTION.alias("status_display"),
        s.SOURCE_STATE_MEANING.alias("status_meaning"),
        s.BEG_EFFECTIVE_DT_TM.alias("effective_start"),
        s.END_EFFECTIVE_DT_TM.alias("effective_end"),
        s.LOCATION_CD.cast("string").alias("location_code"),
        F.coalesce(s.LOCATION_DESCRIPTION, s.LOCATION_FREETEXT)
         .alias("location_display"),
        s.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
        s.ADC_UPDT.alias("loaded_at"),
    ).select(*APPOINTMENT_BOOKING_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / appointment resource

# COMMAND ----------

# ==== journey_clinical.appointment_resource ====

APPOINTMENT_RESOURCE_PUBLIC_COLUMNS = [
    "sch_event_id", "sequence", "appointment_role_id", "schedule_id",
    "role_code", "role_display", "practitioner_key", "location_code",
    "slot_start", "slot_end", "source_present_ind", "loaded_at",
]

APPOINTMENT_RESOURCE_COLUMN_COMMENTS = {
    "sch_event_id": "Millennium SCH_EVENT_ID of the parent clinical_appointment row.",
    "sequence": "Resource sequence, falling back to SCH_APPT_ID.",
    "appointment_role_id": "Source scheduling appointment-role identifier.",
    "schedule_id": "Source schedule identifier.",
    "role_code": "Source scheduling role code.",
    "role_display": "Source scheduling role display, falling back to role meaning.",
    "practitioner_key": "Deterministic practitioner key derived from ALLOCATED_PERSONNEL_ID.",
    "location_code": "Source appointment location code.",
    "slot_start": "Source allocated slot start.",
    "slot_end": "Source allocated slot end.",
    "source_present_ind": "Whether the source row remains present.",
    "loaded_at": "Bronze load timestamp of the resource row.",
}

@materialized_view(
    name=_n("journey_clinical.appointment_resource"),
    comment="One scheduling resource or slot row per appointment.",
    column_comments=APPOINTMENT_RESOURCE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def appointment_resource():
    # Contract v2 parent/child conservation, as for appointment_booking above.
    # map_appointment_resource carries 40,219 SCH_EVENT_IDs absent from
    # map_appointment, which left 53,626 resource rows unreachable through
    # clinical_appointment.
    # Build the declared dataset: One scheduling resource or slot row per appointment.
    published = spark.read.table(_n("journey_clinical.appointment")).select(
        F.col("sch_event_id").cast("bigint").alias("published_sch_event_id")
    )
    r = read_source(SRC_APPOINTMENT_RESOURCE)
    r = r.join(
        published,
        r.SCH_EVENT_ID.cast("bigint") == F.col("published_sch_event_id"),
        "left_semi",
    )
    return r.select(
        r.SCH_EVENT_ID.cast("bigint").alias("sch_event_id"),
        F.coalesce(r.SCHEDULE_SEQ, r.SCH_APPT_ID).cast("long").alias("sequence"),
        r.SCH_APPT_ID.cast("string").alias("appointment_role_id"),
        r.SCHEDULE_ID.cast("string").alias("schedule_id"),
        r.SCH_ROLE_CD.cast("string").alias("role_code"),
        F.coalesce(r.SCH_ROLE_DESCRIPTION, r.ROLE_MEANING).alias("role_display"),
        F.when(
            r.ALLOCATED_PERSONNEL_ID.isNotNull(),
            stable_id("practitioner:mill", r.ALLOCATED_PERSONNEL_ID),
        ).alias("practitioner_key"),
        r.APPT_LOCATION_CD.cast("string").alias("location_code"),
        r.BEG_DT_TM.alias("slot_start"),
        r.END_DT_TM.alias("slot_end"),
        r.SOURCE_PRESENT_IND.cast("boolean").alias("source_present_ind"),
        r.ADC_UPDT.alias("loaded_at"),
    ).select(*APPOINTMENT_RESOURCE_PUBLIC_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / referral

# COMMAND ----------

# contract v2: rename direct-publication comment keys and remove the dropped storage-row comment
REFERRAL_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 referral identity.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA referral feed supplies no encounter.",
    "event_datetime": "Referral received timestamp.",
    "event_end_datetime": "Event end timestamp; not supplied by LUNA referrals.",
    "source_coding_system": "Verbatim LUNA treatment-function coding system.",
    "source_code": "Verbatim treatment-function code.",
    "source_display": "Verbatim treatment-function display.",
    "ubrn": "Referral linkage evidence only; never a join key.",
    "waiting_list_oid": "Raw LUNA waiting-list object identifier.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "referral_priority_code": "Source referral-priority code.",
    "referral_priority_display": "Source referral-priority display.",
    "referral_source_code": "Source referral-source code.",
    "referral_source_display": "Source referral-source display.",
    "status_code": "Source referral-status code.",
    "status_display": "Source referral-status display.",
    "status_change_reason_code": "Source referral status-change reason code.",
    "status_change_reason_display": "Source referral status-change reason display.",
    "status_change_datetime": "Source referral status-change timestamp.",
    "encounter_type_code": "Source encounter-type code.",
    "encounter_type_display": "Source encounter-type display.",
    "suspected_cancer_site_code": "Source suspected-cancer-site code.",
    "suspected_cancer_site_display": "Source suspected-cancer-site display.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "service_type_requested_code": "Source requested-service-type code.",
    "service_type_requested_display": "Source requested-service-type display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "referred_by_org_id": "Raw referring organization identifier.",
    "booking_type_code": "Source booking-type code.",
    "booking_type_display": "Source booking-type display.",
    "admin_category_code": "Source administrative-category code.",
    "admin_category_display": "Source administrative-category display.",
    "business_unit": "Source business unit.",
    "division": "Name of the clinical division responsible for the referral within the organisational hierarchy.",
    "original_received_datetime": "Original referral received timestamp.",
    "ers_ubrn_received": "e-Referral UBRN received date.",
    "ers_pathway_start": "e-Referral pathway start date.",
    "ers_service_name": "e-Referral service name.",
    "ers_specialty": "e-Referral specialty recorded in the source referral field `ERS_SPECIALTY`.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native primary key.",
    "referral_oid": "LUNA REFERRAL_OID; component of the native primary key.",
    "record_status": "Superseded when map_referral.SOURCE_PRESENT_IND or ACTIVE_IND is false; each missing flag defaults true. Otherwise active. Source absence and inactivity both map to superseded, not retracted; the clinical status code does not set this row-history label.",
    "record_status_effective_from": "Date and time the referral record was created in the source system.",
    "record_status_effective_to": "For a superseded referral row, the first non-null map_referral.SOURCE_ABSENT_DETECTED_TS, MODIFIED_DATETIME or ADC_UPDT, in that order; otherwise null. The same source-presence/inactivity predicate sets record_status. No event-time clamp or clinical-event timestamp is substituted.",
    "source_update_timestamp": "map_referral.SOURCE_ADC_UPDT carried unchanged to the research row, distinct from ADC_UPDT used for loaded_at. It is not the clinical event timestamp or the current Silver refresh time.",
    "loaded_at": "map_referral.ADC_UPDT carried unchanged through the source and public projections. No parent or other source clock is joined into this value; it differs from SOURCE_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

REFERRAL_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

REFERRAL_RETIRED_COLUMNS = [

]

REFERRAL_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'referral_oid',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_referral():
    # Assemble referral rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _referral_canonical().drop("_source_system", "_source_table", "_source_row_id")

@materialized_view(
    name=_n("journey_clinical.referral"),
    comment="LUNA referrals (request side of threads). UBRN retained as linkage evidence only.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=REFERRAL_COLUMN_COMMENTS,
)
def referral():
    # Build the declared dataset: LUNA referrals (request side of threads). UBRN retained as
    # linkage evidence only.
    return _lifecycle_source_referral().drop(*REFERRAL_LIFECYCLE_FIELDS, *REFERRAL_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._referral_metadata'),
    comment='Internal quality and batch metadata for clinical_referral; same row grain as the research table. Join keys: patient_event_key, source_system_oid, referral_oid.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def referral_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_referral;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # referral_oid.
    return (_lifecycle_source_referral()).select(*REFERRAL_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  rtt pathway primitive

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._rtt_pathway_primitive"),
    private=True,
    comment="Internal RTT period rows; encounter-type tags cross as JSON.",
    refresh_policy="incremental",
)
def _rtt_pathway_primitive():
    # Build the declared dataset: Internal RTT period rows; encounter-type tags cross as JSON.
    return _rtt_pathway_canonical().drop("_source_system", "_source_table", "_source_row_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  qc rtt pathway

# COMMAND ----------

@materialized_view(
    name=_n("journey_clinical._qc_rtt_pathway"),
    comment="Private JSON bridge and Gold cross-rule flags for rtt_pathway.",
    refresh_policy="incremental",
)
def _qc_rtt_pathway():
    # Build the declared dataset: Private JSON bridge and Gold cross-rule flags for rtt_pathway.
    return _cross_qc_primitive(
        spark.read.table(_n("journey_clinical._rtt_pathway_primitive")),
        "rtt_pathway",
        {"encounter_types": "_encounter_types_json"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / rtt pathway

# COMMAND ----------

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
RTT_PATHWAY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'pathway_oid',
    'period_oid',
    'is_latest_period',
    'start_status_code',
    'start_status_display',
    'stop_status_code',
    'stop_status_display',
    'current_status_code',
    'current_status_display',
    'sequence_asc',
    'sequence_desc',
    'clock_discrepant',
    'core_clock_start',
    'core_clock_stop',
    'pathway_start_date',
    'pathway_type_code',
    'pathway_type_display',
    'breach_date',
    'days_waited',
    'days_waited_active',
    'op_appt_dna_count',
    'treatment_function_code',
    'treatment_function_display',
    'site_code',
    'site_display',
    'referring_facility_code',
    'referring_facility_display',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

RTT_PATHWAY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'pathway_oid',
    'period_oid',
    'identity_status',
    'person_id_resolved',
    'event_before_birth',
    'event_after_death_30d',
    'load_batch_id',
]

RTT_PATHWAY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 event identity; stable cross-feed join key.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native pathway-period primary key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA pathway feed supplies no encounter.",
    "event_datetime": "RTT clock-period start timestamp.",
    "event_end_datetime": "RTT clock-period stop timestamp.",
    "source_coding_system": "Verbatim LUNA RTT-status coding system.",
    "source_code": "Verbatim current RTT-status code.",
    "source_display": "Verbatim current RTT-status display.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "period_oid": "Raw LUNA period object identifier; 0 marks the clockless-pathway sentinel and the column is never null.",
    "is_latest_period": "Whether this is the latest period for the pathway.",
    "start_status_code": "Source clock-start RTT-status code.",
    "start_status_display": "Source clock-start RTT-status display.",
    "stop_status_code": "Source clock-stop RTT-status code.",
    "stop_status_display": "Source clock-stop RTT-status display.",
    "current_status_code": "Source current RTT-status code.",
    "current_status_display": "Source current RTT-status display.",
    "sequence_asc": "Ascending pathway-period sequence.",
    "sequence_desc": "Descending pathway-period sequence.",
    "clock_discrepant": "Bronze flag indicating disagreement with the core clock timestamps.",
    "core_clock_start": "Core-system clock start retained as discrepant sidecar data.",
    "core_clock_stop": "Core-system clock stop retained as discrepant sidecar data.",
    "pathway_start_date": "Source pathway start date.",
    "pathway_type_code": "Source pathway-type code.",
    "pathway_type_display": "Source pathway-type display.",
    "breach_date": "Source breach timestamp.",
    "days_waited": "Source total days waited.",
    "days_waited_active": "Source active days waited.",
    "op_appt_dna_count": "Source outpatient did-not-attend count.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "referring_facility_code": "Source referring-facility code.",
    "referring_facility_display": "Source referring-facility display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Derived LUNA RTT row status: superseded when SOURCE_PRESENT_IND cast to Boolean, PERIOD_ACTIVE_IND or CORE_ACTIVE_IND is false; otherwise active. Each missing flag defaults true. Source absence maps to superseded, not retracted, and this label is separate from the clinical RTT status or clock-stop state.",
    "record_status_effective_from": "map_rtt_pathway.PERIOD_CREATED_DATETIME carried unchanged as the history start. No RTT clock-start, pathway-start or ingestion-time fallback is applied; a missing source value remains null.",
    "record_status_effective_to": "For a superseded RTT row, first non-null map_rtt_pathway.SOURCE_ABSENT_DETECTED_TS, PERIOD_MODIFIED_DATETIME, then ADC_UPDT. Null for active rows or when every fallback is missing. This is a derived row-status end that may use ingestion time; clinical STOP_DATETIME is not used as the fallback.",
    "source_update_timestamp": "map_rtt_pathway.SOURCE_ADC_UPDT carried unchanged for the contributing pathway/period row. It is distinct from ADC_UPDT ingestion provenance and is not PERIOD_MODIFIED_DATETIME, an RTT clock start/stop or Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_rtt_pathway.ADC_UPDT carried unchanged through the RTT primitive and quality wrappers. This is bronze ingestion provenance, not a clinical RTT clock, period creation/modification time or Silver refresh time; a missing source timestamp remains null.",
}

@materialized_view(
    name=_n("journey_clinical.rtt_pathway"),
    comment="LUNA RTT pathway/clock periods (hybrid grain: one row per period, plus clockless-pathway sentinel rows). Clock discrepancies are carried, never filtered.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=RTT_PATHWAY_COLUMN_COMMENTS,
)
def rtt_pathway():
    # Build the declared dataset: LUNA RTT pathway/clock periods (hybrid grain: one row per
    # period, plus clockless-pathway sentinel rows). Clock discrepancies are carried, never
    # filtered.
    return _cross_qc_public(
        spark.read.table(_n("journey_clinical._qc_rtt_pathway")),
        "rtt_pathway",
        {},
        RTT_PATHWAY_PUBLIC_COLUMNS,
        RTT_PATHWAY_LIFECYCLE_COLUMNS,
    )

@materialized_view(
    name=_n('journey_clinical._rtt_pathway_metadata'),
    comment='Internal quality and batch metadata for clinical_rtt_pathway; same row grain as the research table. Join keys: patient_event_key, source_system_oid, pathway_oid, period_oid.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def rtt_pathway_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_rtt_pathway;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # pathway_oid, period_oid.
    return (spark.read.table(_n("journey_clinical._qc_rtt_pathway"))).select(*RTT_PATHWAY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / rtt pathway encounter type

# COMMAND ----------

# ==== journey_clinical.rtt_pathway_encounter_type ====

RTT_PATHWAY_ENCOUNTER_TYPE_PUBLIC_COLUMNS = [
    "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
    "sequence", "encounter_type", "loaded_at",
]

RTT_PATHWAY_ENCOUNTER_TYPE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_rtt_pathway row.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; first native pathway-period key component.",
    "pathway_oid": "LUNA PATHWAY_OID; second native pathway-period key component.",
    "period_oid": "LUNA PERIOD_OID; third native pathway-period key component.",
    "sequence": "One-based order of the encounter-type tag in the source array.",
    "encounter_type": "Verbatim LUNA pathway encounter-type tag.",
    "loaded_at": "Bronze load timestamp inherited from the parent pathway row.",
}

@materialized_view(
    name=_n("journey_clinical.rtt_pathway_encounter_type"),
    comment="One ordered encounter-type tag per LUNA RTT pathway period.",
    column_comments=RTT_PATHWAY_ENCOUNTER_TYPE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def rtt_pathway_encounter_type():
    # Build the declared dataset: One ordered encounter-type tag per LUNA RTT pathway period.
    source = spark.read.table(
        _n("journey_clinical._rtt_pathway_primitive")
    ).select(
        "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
        "_encounter_types_json", "loaded_at",
    )
    return (
        source.select(
            "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
            "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_encounter_types_json"), "array<string>")
            ).alias("_position", "encounter_type"),
        )
        .where(F.col("encounter_type").isNotNull())
        .select(
            "patient_event_key", "source_system_oid", "pathway_oid", "period_oid",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("encounter_type").cast("string").alias("encounter_type"),
            "loaded_at",
        )
        .select(*RTT_PATHWAY_ENCOUNTER_TYPE_PUBLIC_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / rtt activity

# COMMAND ----------

# contract v2: rename direct-publication event and referral comment keys and remove fact_row_id
RTT_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 RTT activity identity.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID when resolved; native person foreign key.",
    "encounter_id": "Nullable Millennium ENCNTR_ID; the LUNA activity feed supplies no encounter.",
    "event_datetime": "RTT activity timestamp.",
    "event_end_datetime": "Event end timestamp; not supplied by LUNA RTT activities.",
    "source_coding_system": "Verbatim LUNA RTT-status coding system.",
    "source_code": "Verbatim RTT-status code.",
    "source_display": "Verbatim RTT-status display.",
    "rtt_activity_oid": "Raw LUNA RTT activity object identifier.",
    "pathway_oid": "Raw LUNA pathway object identifier.",
    "referral_key": "Deterministic SHA-256 key of the linked referral.",
    "appointment_oid": "Raw appointment linkage evidence; resolution is gated and no edge is emitted.",
    "activity_code": "Source RTT activity code.",
    "activity_display": "Source RTT activity display.",
    "activity_type_code": "Source RTT activity-type code.",
    "activity_type_display": "Source RTT activity-type display.",
    "status_code": "Source RTT-status code.",
    "status_display": "Source RTT-status display.",
    "status_sequence_asc": "Ascending RTT-status sequence.",
    "status_sequence_desc": "Descending RTT-status sequence.",
    "activity_sequence_asc": "Ascending RTT-activity sequence.",
    "activity_sequence_desc": "Descending RTT-activity sequence.",
    "activity_datetime_quality": "Bronze quality classification for the activity timestamp.",
    "treatment_function_code": "Source treatment-function code.",
    "treatment_function_display": "Source treatment-function display.",
    "site_code": "Source site code.",
    "site_display": "Source site display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID; component of the native primary key.",
    "record_status": "Superseded when map_rtt_activity.SOURCE_PRESENT_IND or ACTIVE_IND is false; each missing flag defaults true. Otherwise active. Source absence and inactivity both map to superseded, not retracted; the clinical status code does not set this row-history label.",
    "record_status_effective_from": "Source system timestamp recording when the RTT activity record was created.",
    "record_status_effective_to": "For a superseded rtt activity row, the first non-null map_rtt_activity.SOURCE_ABSENT_DETECTED_TS, MODIFIED_DATETIME or ADC_UPDT, in that order; otherwise null. The same source-presence/inactivity predicate sets record_status. No event-time clamp or clinical-event timestamp is substituted.",
    "source_update_timestamp": "map_rtt_activity.SOURCE_ADC_UPDT carried unchanged to the research row, distinct from ADC_UPDT used for loaded_at. It is not the clinical event timestamp or the current Silver refresh time.",
    "loaded_at": "map_rtt_activity.ADC_UPDT carried unchanged through the source and public projections. No parent or other source clock is joined into this value; it differs from SOURCE_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

RTT_ACTIVITY_LIFECYCLE_FIELDS = [
    'identity_status',
    'is_illogical',
    'load_batch_id',
]

RTT_ACTIVITY_RETIRED_COLUMNS = [

]

RTT_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'rtt_activity_oid',
    'identity_status',
    'is_illogical',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_rtt_activity():
    # Assemble rtt activity rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _rtt_activity_canonical().drop("_source_system", "_source_table", "_source_row_id")

@materialized_view(
    name=_n("journey_clinical.rtt_activity"),
    comment="LUNA clock-affecting RTT activity/status events. IS_ILLOGICAL is carried as data, never filtered.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=RTT_ACTIVITY_COLUMN_COMMENTS,
)
def rtt_activity():
    # Build the declared dataset: LUNA clock-affecting RTT activity/status events. IS_ILLOGICAL
    # is carried as data, never filtered.
    return _lifecycle_source_rtt_activity().drop(*RTT_ACTIVITY_LIFECYCLE_FIELDS, *RTT_ACTIVITY_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._rtt_activity_metadata'),
    comment='Internal quality and batch metadata for clinical_rtt_activity; same row grain as the research table. Join keys: patient_event_key, source_system_oid, rtt_activity_oid.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def rtt_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_rtt_activity;
    # same row grain as the research table. Join keys: patient_event_key, source_system_oid,
    # rtt_activity_oid.
    return (_lifecycle_source_rtt_activity()).select(*RTT_ACTIVITY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / waiting list entry

# COMMAND ----------

# contract v2: rename direct-publication event and location comments and remove fact_row_id
WAITING_LIST_ENTRY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 waiting-list entry identity shared by physical versions.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "event_datetime": "Recorded waiting-period start timestamp carried from `WAITING_START_DT_TM`.",
    "event_end_datetime": "Recorded waiting-period end timestamp carried from `WAITING_END_DT_TM`.",
    "source_coding_system": "Verbatim Millennium planned-procedure coding system.",
    "source_code": "Verbatim planned-procedure code.",
    "source_display": "Verbatim planned-procedure display.",
    "pm_wait_list_id": "Native waiting-list entry identifier.",
    "row_source": "Physical bronze row source.",
    "source_version_id": "Native source version identifier; CURRENT rows carry -1.",
    "hist_action": "Source history action when the row is historical.",
    "version_datetime": "Descriptive fallback version timestamp only; it defines no effective interval.",
    "is_current": "Whether this is the current physical version.",
    "updt_cnt": "Native source update counter.",
    "sch_event_id": "Raw scheduling event linkage evidence.",
    "status_code": "Source waiting-list status code.",
    "status_display": "Source waiting-list status display.",
    "sub_status_display": "Source waiting-list sub-status display.",
    "active_status_display": "Source active-status display.",
    "urgency_display": "Source urgency display.",
    "stand_by_display": "Source stand-by display.",
    "admit_category_display": "Source admission-category display.",
    "admit_booking_display": "Source admission-booking display.",
    "admit_type_display": "Source admission-type display.",
    "admit_offer_outcome_display": "Source admission-offer outcome display.",
    "management_display": "Source management display.",
    "attendance_display": "Source attendance display.",
    "reason_for_change_display": "Source reason-for-change display.",
    "reason_for_removal_display": "Source reason-for-removal display.",
    "anesthetic_display": "Source anesthetic display.",
    "planned_procedure_code": "Source planned-procedure code.",
    "planned_procedure_display": "Source planned-procedure display.",
    "referral_source_display": "Source referral-source display.",
    "referral_type_display": "Source referral-type display.",
    "service_type_requested_display": "Source requested-service-type display.",
    "from_ed_ind": "Raw source indicator for origin in the emergency department.",
    "suspended_days": "Source count of suspended days.",
    "recommend_datetime": "Source recommendation timestamp.",
    "referral_datetime": "Source referral timestamp.",
    "original_request_received_datetime": "Original request-received timestamp.",
    "admit_decision_datetime": "Source decision-to-admit timestamp.",
    "admit_guaranteed_datetime": "Source guaranteed-admission timestamp.",
    "provisional_admit_datetime": "Source provisional-admission timestamp.",
    "previous_provisional_admit_datetime": "Previous provisional-admission timestamp.",
    "adjusted_waiting_start_datetime": "Adjusted waiting-start timestamp.",
    "scheduled_datetime": "Source scheduled timestamp.",
    "requested_datetime": "Source requested timestamp.",
    "removal_datetime": "Source removal timestamp.",
    "last_dna_datetime": "Source last did-not-attend timestamp.",
    "status_datetime": "Source status timestamp.",
    "status_end_datetime": "Source status-end timestamp.",
    "location_code": "Millennium nurse-unit code.",
    "location_display": "Source nurse-unit display.",
    "facility_display": "Source facility display.",
    "confidentiality_code": "Source confidentiality code when supplied.",
    "vip_ind": "Source VIP indicator when supplied.",
    "withheld_identity_ind": "Withheld-identity indicator when supplied.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the fact.",
    "record_status": "Derived map_waiting_list version status: superseded when SOURCE_PRESENT_IND cast to Boolean is false or IS_CURRENT is false; otherwise active. Null presence defaults to true and null IS_CURRENT defaults to false. Source absence therefore maps to superseded, not retracted; this is distinct from the clinical waiting-list status code.",
    "record_status_effective_from": "map_waiting_list.BEG_EFFECTIVE_DT_TM carried unchanged for the physical CURRENT or HIST version. VERSION_DT_TM and waiting/request dates are not used as fallbacks; a missing value remains null.",
    "record_status_effective_to": "For a superseded waiting-list version, first non-null map_waiting_list.SOURCE_ABSENT_DETECTED_TS, END_EFFECTIVE_DT_TM, then ADC_UPDT. Null for active versions or when all three are missing. This expression does not filter far-future END_EFFECTIVE_DT_TM sentinels and can use ingestion time as its last fallback.",
    "source_update_timestamp": "map_waiting_list.SOURCE_ADC_UPDT carried unchanged for the physical CURRENT or HIST version. It is separate from ADC_UPDT ingestion provenance and is not VERSION_DT_TM, the clinical status date or Silver refresh time; a missing timestamp remains null.",
    "loaded_at": "map_waiting_list.ADC_UPDT carried unchanged for the physical CURRENT or HIST version. This is ingestion provenance, not waiting-start/end, descriptive VERSION_DT_TM or Silver refresh time; a missing source timestamp remains null.",
}

WAITING_LIST_ENTRY_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]

WAITING_LIST_ENTRY_RETIRED_COLUMNS = [

]

WAITING_LIST_ENTRY_LIFECYCLE_COLUMNS = [
    # The companion must join one-to-one onto the research table. This table is
    # at physical version grain (CURRENT + HIST) and patient_event_key is
    # entry-grain, deliberately shared across an entry's versions, so it is not
    # a row key. pm_wait_list_id does not separate versions either: the pair
    # leaves 36,464,339 duplicate key groups, and joining on it fans gold out
    # to 5,531,116,187 rows. row_source with source_version_id is the physical
    # version grain -- 233,876,687 groups over 233,876,687 rows.
    'patient_event_key',
    'pm_wait_list_id',
    'row_source',
    'source_version_id',
    'identity_status',
    'load_batch_id',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_waiting_list_entry():
    # Assemble waiting list entry rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _waiting_list_entry_canonical().drop(
        "_source_system", "_source_table", "_source_row_id"
    )

@materialized_view(
    name=_n("journey_clinical.waiting_list_entry"),
    comment="Waiting-list entries at physical version grain (CURRENT + HIST). patient_event_id is entry-grain and shared across versions; VERSION_DT_TM is descriptive fallback only, never an effective interval.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=WAITING_LIST_ENTRY_COLUMN_COMMENTS,
)
def waiting_list_entry():
    # Build the declared dataset: Waiting-list entries at physical version grain (CURRENT +
    # HIST). patient_event_id is entry-grain and shared across versions; VERSION_DT_TM is
    # descriptive fallback only, never an effective interval.
    return _lifecycle_source_waiting_list_entry().drop(*WAITING_LIST_ENTRY_LIFECYCLE_FIELDS, *WAITING_LIST_ENTRY_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._waiting_list_entry_metadata'),
    comment='Internal quality and batch metadata for clinical_waiting_list_entry; same row grain as the research table. Join keys: patient_event_key, row_source, source_version_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def waiting_list_entry_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_waiting_list_entry; same row grain as the research table. Join keys:
    # patient_event_key, row_source, source_version_id.
    return (_lifecycle_source_waiting_list_entry()).select(*WAITING_LIST_ENTRY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / waiting list snapshot

# COMMAND ----------

SRC_WAITING_LIST_SNAPSHOT = "4_prod.bronze.map_waiting_list_snapshot"

def _waiting_list_snapshot_canonical():
    # Assemble normalized waiting list snapshot rows for downstream dataset builders, preserving
    # the existing source and identity rules.
    s = read_source(SRC_WAITING_LIST_SNAPSHOT).alias("s")
    snap_id = stable_id("wl_census:mill_pm", s.SNAPSHOT_DATE, s.PM_WAIT_LIST_ID)
    row_ref = F.concat_ws(":", s.SNAPSHOT_DATE.cast("string"), s.PM_WAIT_LIST_ID.cast("string"))
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)], SRC_WAITING_LIST_SNAPSHOT, row_ref
    )
    # contract v2: rename the census SHA key and publish native person, encounter, and location identifiers
    return s.select(
        snap_id.alias("waiting_list_snapshot_key"),
        s.SNAPSHOT_DATE.alias("snapshot_date"),
        s.SNAPSHOT_CUTOFF_TS.alias("snapshot_cutoff_ts"),
        s.PM_WAIT_LIST_ID.cast("long").alias("pm_wait_list_id"),
        skey.alias("subject_key"), ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(s.PERSON_ID.isNotNull(), F.lit("resolved")).otherwise(F.lit("unresolved"))
        .alias("identity_status"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.SCH_EVENT_ID.cast("long").alias("sch_event_id"),
        s.STATUS_CD.cast("string").alias("status_code"), s.STATUS_DESC.alias("status_display"),
        s.SUB_STATUS_DESC.alias("sub_status_display"),
        s.ACTIVE_STATUS_DESC.alias("active_status_display"),
        s.URGENCY_DESC.alias("urgency_display"), s.STAND_BY_DESC.alias("stand_by_display"),
        s.ADMIT_CATEGORY_DESC.alias("admit_category_display"),
        s.ADMIT_BOOKING_DESC.alias("admit_booking_display"),
        s.ADMIT_TYPE_DESC.alias("admit_type_display"),
        s.ADMIT_OFFER_OUTCOME_DESC.alias("admit_offer_outcome_display"),
        s.MANAGEMENT_DESC.alias("management_display"),
        s.ATTENDANCE_DESC.alias("attendance_display"),
        s.REASON_FOR_CHANGE_DESC.alias("reason_for_change_display"),
        s.REASON_FOR_REMOVAL_DESC.alias("reason_for_removal_display"),
        s.ANESTHETIC_DESC.alias("anesthetic_display"),
        s.PLANNED_PROCEDURE_CD.cast("string").alias("planned_procedure_code"),
        s.PLANNED_PROCEDURE_DESC.alias("planned_procedure_display"),
        s.REFERRAL_SOURCE_DESC.alias("referral_source_display"),
        s.REFERRAL_TYPE_DESC.alias("referral_type_display"),
        s.SERVICE_TYPE_REQUESTED_DESC.alias("service_type_requested_display"),
        s.FROM_ED_IND.cast("long").alias("from_ed_ind"),
        s.SUSPENDED_DAYS.alias("suspended_days"),
        s.RECOMMEND_DT_TM.alias("recommend_datetime"),
        s.REFERRAL_DT_TM.alias("referral_datetime"),
        s.ORIG_REQUEST_RECEIVED_DT_TM.alias("original_request_received_datetime"),
        s.ADMIT_DECISION_DT_TM.alias("admit_decision_datetime"),
        s.ADMIT_GUARANTEED_DT_TM.alias("admit_guaranteed_datetime"),
        s.PROVISIONAL_ADMIT_DT_TM.alias("provisional_admit_datetime"),
        s.PREV_PROV_ADMIT_DT_TM.alias("previous_provisional_admit_datetime"),
        s.WAITING_START_DT_TM.alias("waiting_start_datetime"),
        s.WAITING_END_DT_TM.alias("waiting_end_datetime"),
        s.ADJ_WAITING_START_DT_TM.alias("adjusted_waiting_start_datetime"),
        s.SCHEDULE_DT_TM.alias("scheduled_datetime"),
        s.REQUESTED_DT_TM.alias("requested_datetime"),
        s.REMOVAL_DT_TM.alias("removal_datetime"),
        s.LAST_DNA_DT_TM.alias("last_dna_datetime"),
        s.STATUS_DT_TM.alias("status_datetime"),
        s.STATUS_END_DT_TM.alias("status_end_datetime"),
        s.LOC_NURSE_UNIT_CD.cast("string").alias("location_code"),
        s.LOC_NURSE_UNIT_DESC.alias("location_display"),
        s.LOC_FACILITY_DESC.alias("facility_display"),
        F.lit("administrative").alias("fact_category"),
        F.lit("waiting_list_census").alias("source_feed"),
        s.SNAPSHOT_CUTOFF_TS.alias("loaded_at"),
    )

# contract v2: rename direct-publication census and location comment keys
WAITING_LIST_SNAPSHOT_COLUMN_COMMENTS = {
    "waiting_list_snapshot_key": "Deterministic SHA-256 census-row key.",
    "snapshot_date": "Census snapshot date.",
    "snapshot_cutoff_ts": "Immutable census cutoff timestamp.",
    "pm_wait_list_id": "Native waiting-list entry identifier.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Millennium PERSON_ID; native person foreign key.",
    "encounter_id": "Millennium ENCNTR_ID when supplied; native encounter foreign key.",
    "sch_event_id": "Raw scheduling event linkage evidence.",
    "status_code": "Source waiting-list status code.",
    "status_display": "Source waiting-list status display.",
    "sub_status_display": "Source waiting-list sub-status display.",
    "active_status_display": "Source active-status display.",
    "urgency_display": "Source urgency display.",
    "stand_by_display": "Source stand-by display.",
    "admit_category_display": "Source admission-category display.",
    "admit_booking_display": "Source admission-booking display.",
    "admit_type_display": "Source admission-type display.",
    "admit_offer_outcome_display": "Source admission-offer outcome display.",
    "management_display": "Source management display.",
    "attendance_display": "Source attendance display.",
    "reason_for_change_display": "Source reason-for-change display.",
    "reason_for_removal_display": "Source reason-for-removal display.",
    "anesthetic_display": "Source anesthetic display.",
    "planned_procedure_code": "Source planned-procedure code.",
    "planned_procedure_display": "Source planned-procedure display.",
    "referral_source_display": "Source referral-source display.",
    "referral_type_display": "Source referral-type display.",
    "service_type_requested_display": "Source requested-service-type display.",
    "from_ed_ind": "Raw source indicator for origin in the emergency department.",
    "suspended_days": "Source count of suspended days.",
    "recommend_datetime": "Source recommendation timestamp.",
    "referral_datetime": "Source referral timestamp.",
    "original_request_received_datetime": "Original request-received timestamp.",
    "admit_decision_datetime": "Source decision-to-admit timestamp.",
    "admit_guaranteed_datetime": "Source guaranteed-admission timestamp.",
    "provisional_admit_datetime": "Source provisional-admission timestamp.",
    "previous_provisional_admit_datetime": "Previous provisional-admission timestamp.",
    "waiting_start_datetime": "Source waiting-start timestamp.",
    "waiting_end_datetime": "Source waiting-end timestamp.",
    "adjusted_waiting_start_datetime": "Adjusted waiting-start timestamp.",
    "scheduled_datetime": "Source scheduled timestamp.",
    "requested_datetime": "Source requested timestamp.",
    "removal_datetime": "Source removal timestamp.",
    "last_dna_datetime": "Source last did-not-attend timestamp.",
    "status_datetime": "Source status timestamp.",
    "status_end_datetime": "Source status-end timestamp.",
    "location_code": "Millennium nurse-unit code.",
    "location_display": "Source nurse-unit display.",
    "facility_display": "Source facility display.",
    "fact_category": "Whether this fact is clinical or administrative in the v2 plane merge.",
    "source_feed": "Registered feed owning the census row.",
    "loaded_at": "Single immutable UTC cutoff timestamp shared by every row for SNAPSHOT_DATE.",
}

WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS = [
    'identity_status',
]

WAITING_LIST_SNAPSHOT_RETIRED_COLUMNS = [

]

WAITING_LIST_SNAPSHOT_LIFECYCLE_COLUMNS = [
    'waiting_list_snapshot_key',
    'snapshot_date',
    'pm_wait_list_id',
    'identity_status',
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_waiting_list_snapshot():
    # Assemble waiting list snapshot rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _waiting_list_snapshot_canonical()

@materialized_view(
    name=_n("journey_clinical.waiting_list_snapshot"),
    comment="Immutable weekly waiting-list census (one row per SNAPSHOT_DATE x PM_WAIT_LIST_ID). Snapshots never enter patient_event.",
    refresh_policy="incremental",
    column_comments=WAITING_LIST_SNAPSHOT_COLUMN_COMMENTS,
)
def waiting_list_snapshot():
    # Build the declared dataset: Immutable weekly waiting-list census (one row per
    # SNAPSHOT_DATE x PM_WAIT_LIST_ID). Snapshots never enter patient_event.
    return _lifecycle_source_waiting_list_snapshot().drop(*WAITING_LIST_SNAPSHOT_LIFECYCLE_FIELDS, *WAITING_LIST_SNAPSHOT_RETIRED_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._waiting_list_snapshot_metadata'),
    comment='Internal quality and batch metadata for clinical_waiting_list_snapshot; same row grain as the research table. Join keys: waiting_list_snapshot_key, snapshot_date, pm_wait_list_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def waiting_list_snapshot_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_waiting_list_snapshot; same row grain as the research table. Join keys:
    # waiting_list_snapshot_key, snapshot_date, pm_wait_list_id.
    return (_lifecycle_source_waiting_list_snapshot()).select(*WAITING_LIST_SNAPSHOT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / community care contact

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
COMMUNITY_CONTACT_SOURCE_COLUMNS = [
    "patient_event_key",
    "community_care_contact_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "care_contact_date",
    "community_database_id",
    "care_contact_id",
    "community_patient_key",
    "service_request_id",
    "service_id",
    "service_name",
    "team_id",
    "care_contact_id_variant_count",
    "person_match_status",
    "duration_minutes",
    "duration_quality_status",
    "earliest_reasonable_offer_date",
    "earliest_clinically_appropriate_date",
    "commissioner_ods_code",
    "commissioner_organization_id",
    "commissioner_organization_name",
    "consultation_mechanism_code",
    "consultation_mechanism_display",
    "location_type_code",
    "location_type_display",
    "service_team_type_code",
    "service_team_type_display",
    "service_team_type_mapping_status",
    "source_consultation_term",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
COMMUNITY_CONTACT_PUBLIC_COLUMNS = [
    'patient_event_key',
    'community_care_contact_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'care_contact_date',
    'community_database_id',
    'care_contact_id',
    'community_patient_key',
    'service_request_id',
    'service_id',
    'service_name',
    'team_id',
    'care_contact_id_variant_count',
    'person_match_status',
    'duration_minutes',
    'duration_quality_status',
    'earliest_reasonable_offer_date',
    'earliest_clinically_appropriate_date',
    'commissioner_ods_code',
    'commissioner_organization_id',
    'commissioner_organization_name',
    'consultation_mechanism_code',
    'consultation_mechanism_display',
    'location_type_code',
    'location_type_display',
    'service_team_type_code',
    'service_team_type_display',
    'service_team_type_mapping_status',
    'source_consultation_term',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

COMMUNITY_CONTACT_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'community_care_contact_key',
    'identity_status',
    'load_batch_id',
]

COMMUNITY_CARE_CONTACT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "community_care_contact_key": "Source-preserved community care contact key; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Pseudonymised patient key derived from the source community system patient number and its registering database identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date of the care contact as submitted in the CSDS CYP201 record.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coded consultation type submitted with the CSDS CYP201 care contact record.",
    "source_code": "Coded consultation type submitted with the CSDS CYP201 care contact record.",
    "source_display": "Description corresponding to the coded consultation type submitted with the care contact record.",
    "care_contact_date": "Date of the care contact as submitted in the CSDS CYP201 record.",
    "community_database_id": "Source community system database identifier (CDB) from the CSDS CYP201 care contact extract, forming part of the record grain.",
    "care_contact_id": "Source CYP201 Care Contact Identifier; not unique by itself.",
    "community_patient_key": "Pseudonymised patient key derived from the source community system patient number and its registering database identifier.",
    "service_request_id": "Source CYP201 Service Request Identifier.",
    "service_id": "Identifier of the registering service or organisation recorded in the source community system patient lookup.",
    "service_name": "Name of the registering service or organisation recorded in the source community system patient lookup.",
    "team_id": "Local team identifier submitted with the CSDS CYP201 care contact record.",
    "care_contact_id_variant_count": "Rows sharing source_database_id and care_contact_id.",
    "person_match_status": "Status flag indicating the outcome of matching the community patient record to a curated person record.",
    "duration_minutes": "Clinical contact duration in minutes as submitted in the CSDS CYP201 record.",
    "duration_quality_status": "Derived data-quality status describing the completeness or plausibility of the submitted clinical contact duration.",
    "earliest_reasonable_offer_date": "Earliest reasonable offer date submitted with the CSDS CYP201 care contact record.",
    "earliest_clinically_appropriate_date": "Earliest clinically appropriate date submitted with the CSDS CYP201 care contact record.",
    "commissioner_ods_code": "Submitted commissioner ODS code.",
    "commissioner_organization_id": "Internal organisation identifier for the commissioner resolved by matching the submitted ODS code to organisation alias records.",
    "commissioner_organization_name": "Name of the commissioner organisation resolved from the organisation reference data for the submitted commissioner ODS code.",
    "consultation_mechanism_code": "Consultation mechanism (medium of contact) code standardised by the pipeline from the submitted source value.",
    "consultation_mechanism_display": "Description corresponding to the standardised consultation mechanism code.",
    "location_type_code": "CSDS activity location type (site type) code submitted on the CYP201 care contact, derived from the source location code field.",
    "location_type_display": "Descriptive label for the CSDS activity location type code recorded on the care contact.",
    "service_team_type_code": "CSDS team type code identifying the type of service team delivering the care contact.",
    "service_team_type_display": "Descriptive label for the CSDS service team type code recorded on the care contact.",
    "service_team_type_mapping_status": "Derived status flag indicating whether the submitted CSDS team type value was successfully resolved to a recognised service team type reference code.",
    "source_consultation_term": "Original consultation mechanism term as recorded in the source system before normalisation.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each community care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_community_care_contact.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This describes source-row presence, not attendance, consultation outcome or identity-match status.",
    "record_status_effective_from": "First non-null care_contact_datetime_local cast to TIMESTAMP then care_contact_date cast to TIMESTAMP from map_community_care_contact. This contact-time proxy is not an independent status transition. Unlike event_datetime, this history start is not clamped to the [1950,2100) range, and has no ingestion-time fallback.",
    "record_status_effective_to": "map_community_care_contact.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. A missing absence timestamp stays null, with no ingestion-time fallback. This is the row-absence end proxy, not the clinical contact end.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current community-care contact projection. No native application-update or ingestion clock is supplied for this field; neither the contact date nor Silver refresh time is substituted.",
    "loaded_at": "Always null as a TIMESTAMP in the current community-care contact projection. No contributing load timestamp is supplied; neither the contact date nor Silver refresh time is substituted.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_community_care_contact():
    # Assemble community care contact rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _community_care_contact_canonical().select(*COMMUNITY_CONTACT_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.community_care_contact"),
    comment="One CSDS community-care contact with sentinel-safe event time.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=COMMUNITY_CARE_CONTACT_COLUMN_COMMENTS,
)
def community_care_contact():
    # Build the declared dataset: One CSDS community-care contact with sentinel-safe event time.
    return _lifecycle_source_community_care_contact().select(*COMMUNITY_CONTACT_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._community_care_contact_metadata'),
    comment='Internal quality and batch metadata for clinical_community_care_contact; same row grain as the research table. Join keys: patient_event_key, community_care_contact_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def community_care_contact_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_community_care_contact; same row grain as the research table. Join keys:
    # patient_event_key, community_care_contact_key.
    return (_lifecycle_source_community_care_contact()).select(*COMMUNITY_CONTACT_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / community care activity

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
COMMUNITY_ACTIVITY_SOURCE_COLUMNS = [
    "patient_event_key",
    "community_care_activity_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "care_activity_date",
    "care_activity_date_quality_status",
    "community_contact_key",
    "contact_match_status",
    "contact_candidate_count",
    "same_date_contact_candidate_count",
    "community_database_id",
    "care_activity_id",
    "community_patient_key",
    "service_id",
    "service_name",
    "care_professional_local_id",
    "duration_minutes",
    "duration_quality_status",
    "source_clinical_term",
    "source_clinical_term_key",
    "observation_type_id",
    "code_category_id",
    "observation_value_raw",
    "observation_value_numeric",
    "unit_source_value",
    "normalized_ucum_code",
    "unit_concept_id",
    "unit_concept_name",
    "unit_mapping_status",
    "unit_mapping_method",
    "snomed_candidate_count",
    "snomed_candidate_concept_id",
    "snomed_candidate_code",
    "snomed_candidate_name",
    "snomed_candidate_domain",
    "snomed_candidate_class",
    "snomed_candidate_method",
    "snomed_candidate_status",
    "person_match_status",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
COMMUNITY_ACTIVITY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'community_care_activity_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'care_activity_date',
    'care_activity_date_quality_status',
    'community_contact_key',
    'contact_match_status',
    'contact_candidate_count',
    'same_date_contact_candidate_count',
    'community_database_id',
    'care_activity_id',
    'community_patient_key',
    'service_id',
    'service_name',
    'care_professional_local_id',
    'duration_minutes',
    'duration_quality_status',
    'source_clinical_term',
    'source_clinical_term_key',
    'observation_type_id',
    'code_category_id',
    'observation_value_raw',
    'observation_value_numeric',
    'unit_source_value',
    'normalized_ucum_code',
    'unit_concept_id',
    'unit_concept_name',
    'unit_mapping_status',
    'unit_mapping_method',
    'snomed_candidate_count',
    'snomed_candidate_concept_id',
    'snomed_candidate_code',
    'snomed_candidate_name',
    'snomed_candidate_domain',
    'snomed_candidate_class',
    'snomed_candidate_method',
    'snomed_candidate_status',
    'person_match_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

COMMUNITY_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'community_care_activity_key',
    'identity_status',
    'load_batch_id',
]

COMMUNITY_CARE_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "community_care_activity_key": "Source-preserved community care activity key; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Content-addressed key identifying the community patient, derived from the source patient number and registering source database identifier in the CSDS patient identifier lookup.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date on which the care activity was recorded as taking place.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "CSDS care activity type code recorded for the activity in the source extract.",
    "source_code": "CSDS care activity type code recorded for the activity in the source extract.",
    "source_display": "Description corresponding to the CSDS care activity type code.",
    "care_activity_date": "Date on which the care activity was recorded as taking place.",
    "care_activity_date_quality_status": "Derived data-quality flag describing the state of the source care activity date, such as whether it is absent or falls within an acceptable range.",
    "community_contact_key": "Deterministic SHA-256 key for the parent community care contact.",
    "contact_match_status": "UNIQUE_ID_DATE_MATCH, UNIQUE_ID_DATE_MISMATCH, DUPLICATE_ID_RESOLVED_BY_DATE, AMBIGUOUS_SAME_DATE, DUPLICATE_ID_NO_DATE_MATCH or NO_CONTACT.",
    "contact_candidate_count": "Number of candidate care contact rows matched to this activity's care contact identifier during contact linkage.",
    "same_date_contact_candidate_count": "Number of candidate care contact rows whose contact date equals the care activity date, used to resolve duplicate contact identifiers.",
    "community_database_id": "Source community system database identifier (CDB) from the CSDS CYP202 care activity extract, used with care_activity_id to scope source records.",
    "care_activity_id": "Source CSDS care activity identifier (CAI) for the activity row, unique only within its source database.",
    "community_patient_key": "Content-addressed key identifying the community patient, derived from the source patient number and registering source database identifier in the CSDS patient identifier lookup.",
    "service_id": "Identifier of the registering community service or organisation from the CSDS patient identifier lookup.",
    "service_name": "Name of the registering community service or organisation associated with the activity.",
    "care_professional_local_id": "CSDS Care Professional Local Identifier; not a Millennium personnel ID.",
    "duration_minutes": "Recorded duration of the clinical contact for this activity, in minutes.",
    "duration_quality_status": "Derived data-quality flag describing the state of the recorded clinical contact duration, such as whether it is absent or within an acceptable range.",
    "source_clinical_term": "Original clinical term text recorded against the activity in the source legacy code field, used as input to candidate SNOMED enrichment.",
    "source_clinical_term_key": "Normalised form of the source clinical term used as the deterministic join key for SNOMED candidate matching.",
    "observation_type_id": "Numeric observation type identifier carried unchanged from the source CSDS CYP202 care activity ObservationType field.",
    "code_category_id": "Numeric code category identifier carried unchanged from the source CSDS CYP202 care activity CodeCategoryId field.",
    "observation_value_raw": "Observation value as recorded in the source CSDS CYP202 care activity ObsValue field, retained unaltered before numeric parsing.",
    "observation_value_numeric": "Numeric interpretation of the source observation value where the recorded ObsValue could be parsed as a number.",
    "unit_source_value": "Unit of measure text as recorded in the source CSDS CYP202 care activity unit field, before normalization.",
    "normalized_ucum_code": "Exact or approved-alias normalized UCUM code.",
    "unit_concept_id": "OMOP standard concept identifier for the mapped unit of measure.",
    "unit_concept_name": "OMOP standard concept name for the mapped unit of measure.",
    "unit_mapping_status": "Status flag describing the outcome of mapping the source unit text to an OMOP unit concept.",
    "unit_mapping_method": "Indicator of the technique used to derive the unit mapping, such as direct code match or alias dictionary lookup.",
    "snomed_candidate_count": "Number of distinct SNOMED candidate concepts matched for the source clinical term on this activity row.",
    "snomed_candidate_concept_id": "Unreviewed deterministic exact-name/synonym candidate; not an approved resolved mapping.",
    "snomed_candidate_code": "SNOMED CT code of the candidate concept matched to the source clinical term.",
    "snomed_candidate_name": "Concept name of the SNOMED CT candidate matched to the source clinical term.",
    "snomed_candidate_domain": "OMOP domain of the SNOMED CT candidate concept.",
    "snomed_candidate_class": "OMOP concept class of the SNOMED CT candidate concept.",
    "snomed_candidate_method": "Indicator of the deterministic matching technique used to select the SNOMED candidate, such as exact concept name or exact synonym match.",
    "snomed_candidate_status": "CANDIDATE_UNREVIEWED, AMBIGUOUS, UNMAPPED, NO_SOURCE_TERM or DISABLED.",
    "person_match_status": "Outcome status of the linkage from the community patient identifier to a Millennium person record.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each community care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_community_care_activity.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This is source-row presence, not the clinical finding, contact-match status or code-eligibility decision.",
    "record_status_effective_from": "Date on which the care activity was recorded as taking place.",
    "record_status_effective_to": "map_community_care_activity.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. A missing absence timestamp stays null, with no ingestion-time fallback. This is the row-absence end proxy, not a clinical activity or contact end.",
    "source_update_timestamp": "Always null as a TIMESTAMP in the current community-care activity projection. No native application-update or ingestion clock is supplied for this field; neither the activity date nor Silver refresh time is substituted.",
    "loaded_at": "Always null as a TIMESTAMP in the current community-care activity projection. No contributing load clock is supplied or taken from the parent contact; neither care_activity_date nor Silver refresh time is substituted.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_community_care_activity():
    # Assemble community care activity rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _community_care_activity_canonical().select(*COMMUNITY_ACTIVITY_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.community_care_activity"),
    comment="One admitted CSDS community-care activity with candidate terminology provenance.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=COMMUNITY_CARE_ACTIVITY_COLUMN_COMMENTS,
)
def community_care_activity():
    # Build the declared dataset: One admitted CSDS community-care activity with candidate
    # terminology provenance.
    return _lifecycle_source_community_care_activity().select(*COMMUNITY_ACTIVITY_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._community_care_activity_metadata'),
    comment='Internal quality and batch metadata for clinical_community_care_activity; same row grain as the research table. Join keys: patient_event_key, community_care_activity_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def community_care_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_community_care_activity; same row grain as the research table. Join keys:
    # patient_event_key, community_care_activity_key.
    return (_lifecycle_source_community_care_activity()).select(*COMMUNITY_ACTIVITY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
HRG_GROUPING_SOURCE_COLUMNS = [
    "patient_event_key",
    "cds_id",
    "source_object",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "cds_record_id",
    "person_link_method",
    "admission_datetime",
    "discharge_datetime",
    "episode_start_datetime",
    "episode_end_datetime",
    "hosp_prov_spell_num",
    "provider_org_cd",
    "episode_order",
    "episode_duration_days",
    "main_specialty_cd",
    "treatment_function_cd",
    "admission_method_cd",
    "admission_source_cd",
    "admission_source_desc",
    "discharge_method_cd",
    "discharge_dest_cd",
    "discharge_dest_desc",
    "patient_class_cd",
    "patient_class_desc",
    "source_age",
    "source_sex_cd",
    "neonatal_care_level_cd",
    "critical_care_days",
    "rehab_days",
    "fce_hrg_cd",
    "fce_hrg_desc",
    "fce_grouping_method_flag",
    "fce_dominant_proc_cd",
    "fce_dominant_proc_desc",
    "fce_pbc_cd",
    "fce_calc_episode_duration",
    "fce_reporting_episode_duration",
    "dominant_episode_flag",
    "spell_hrg_cd",
    "spell_hrg_desc",
    "spell_grouping_method_flag",
    "spell_dominant_proc_cd",
    "spell_dominant_proc_desc",
    "spell_primary_diag_cd",
    "spell_primary_diag_desc",
    "spell_secondary_diag_cd",
    "spell_secondary_diag_desc",
    "spell_episode_count",
    "spell_los",
    "spell_reporting_los",
    "spell_critical_care_days",
    "spell_ssc_cd",
    "spell_best_practice_cd",
    "first_attend_cd",
    "first_attend_desc",
    "grouping_method_flag",
    "dominant_proc_cd",
    "dominant_proc_desc",
    "grouper_errors",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
HRG_GROUPING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'cds_id',
    'source_object',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'cds_record_id',
    'person_link_method',
    'admission_datetime',
    'discharge_datetime',
    'episode_start_datetime',
    'episode_end_datetime',
    'hosp_prov_spell_num',
    'provider_org_cd',
    'episode_order',
    'episode_duration_days',
    'main_specialty_cd',
    'treatment_function_cd',
    'admission_method_cd',
    'admission_source_cd',
    'admission_source_desc',
    'discharge_method_cd',
    'discharge_dest_cd',
    'discharge_dest_desc',
    'patient_class_cd',
    'patient_class_desc',
    'source_age',
    'source_sex_cd',
    'neonatal_care_level_cd',
    'critical_care_days',
    'rehab_days',
    'fce_hrg_cd',
    'fce_hrg_desc',
    'fce_grouping_method_flag',
    'fce_dominant_proc_cd',
    'fce_dominant_proc_desc',
    'fce_pbc_cd',
    'fce_calc_episode_duration',
    'fce_reporting_episode_duration',
    'dominant_episode_flag',
    'spell_hrg_cd',
    'spell_hrg_desc',
    'spell_grouping_method_flag',
    'spell_dominant_proc_cd',
    'spell_dominant_proc_desc',
    'spell_primary_diag_cd',
    'spell_primary_diag_desc',
    'spell_secondary_diag_cd',
    'spell_secondary_diag_desc',
    'spell_episode_count',
    'spell_los',
    'spell_reporting_los',
    'spell_critical_care_days',
    'spell_ssc_cd',
    'spell_best_practice_cd',
    'first_attend_cd',
    'first_attend_desc',
    'grouping_method_flag',
    'dominant_proc_cd',
    'dominant_proc_desc',
    'grouper_errors',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

HRG_GROUPING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_object',
    'cds_id',
    'identity_status',
    'load_batch_id',
]

HRG_GROUPING_STAGE_COLUMNS = HRG_GROUPING_SOURCE_COLUMNS + [
    "icd_diagnosis_codes_json", "opcs_procedure_codes_json",
]

HRG_GROUPING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID; primary key within source_object.",
    "source_object": "SLAM source arm (slam_apc_hrg or slam_op_hrg); part of the primary key with cds_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Millennium PERSON_ID resolved from source MRN via mill_person_alias type 10 (deterministic tiebreak); null when unresolved — source MRN is retained only in raw.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time the finished consultant episode started.",
    "event_end_datetime": "Date and time the finished consultant episode ended.",
    "source_coding_system": "HRG4+ code assigned to this finished consultant episode.",
    "source_code": "HRG4+ code assigned to this finished consultant episode.",
    "source_display": "Description of the episode-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "cds_record_id": "Trimmed CDS admitted-patient-care episode identifier; unique key; joins back to raw.",
    "person_link_method": "How PERSON_ID was resolved: MRN_ALIAS, or null when unlinked.",
    "admission_datetime": "Date and time the patient was admitted for the hospital provider spell.",
    "discharge_datetime": "Date and time the patient was discharged from the hospital provider spell.",
    "episode_start_datetime": "Date and time the finished consultant episode started.",
    "episode_end_datetime": "Date and time the finished consultant episode ended.",
    "hosp_prov_spell_num": "Hospital provider spell number grouping episodes into spells.",
    "provider_org_cd": "NHS organisation (provider) code identifying the submitting healthcare provider for the episode.",
    "episode_order": "Sequence number of this episode within the hospital provider spell.",
    "episode_duration_days": "Length of the episode in days as submitted to the grouper.",
    "main_specialty_cd": "NHS main specialty code recorded for the episode.",
    "treatment_function_cd": "NHS Treatment Function Code (national standard).",
    "admission_method_cd": "NHS admission method code for the hospital provider spell.",
    "admission_source_cd": "NHS source of admission code for the hospital provider spell.",
    "admission_source_desc": "Description of the source of admission code, mapped from the CDS admission source lookup.",
    "discharge_method_cd": "NHS discharge method code for the hospital provider spell.",
    "discharge_dest_cd": "NHS discharge destination code for the hospital provider spell.",
    "discharge_dest_desc": "Description of the discharge destination code, mapped from the CDS discharge destination lookup.",
    "patient_class_cd": "CDS patient classification code for the episode, such as ordinary or day case admission.",
    "patient_class_desc": "Description of the patient classification code, mapped from the CDS patient class lookup.",
    "source_age": "NON-CANONICAL grouper input age as submitted to SLAM; demographics of record are in map_person.",
    "source_sex_cd": "NON-CANONICAL grouper input sex code as submitted to SLAM; demographics of record are in map_person.",
    "neonatal_care_level_cd": "Coded level of neonatal care recorded for the episode as submitted to the grouper.",
    "critical_care_days": "Number of critical care days for the episode as submitted to the grouper.",
    "rehab_days": "Number of rehabilitation days for the episode as submitted to the grouper.",
    "fce_hrg_cd": "HRG4+ code assigned to this finished consultant episode.",
    "fce_hrg_desc": "Description of the episode-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "fce_grouping_method_flag": "Grouper-returned flag indicating the method by which the HRG4+ code was derived for this finished consultant episode.",
    "fce_dominant_proc_cd": "OPCS-4 procedure code identified by the HRG4+ grouper as the dominant procedure for this episode; null where no procedure drove the grouping.",
    "fce_dominant_proc_desc": "Description of the episode dominant procedure code, mapped from the OPCS-4.10 reference lookup.",
    "fce_pbc_cd": "Programme Budgeting Category code assigned by the HRG4+ grouper to this episode.",
    "fce_calc_episode_duration": "Grouper-calculated episode duration in days for this finished consultant episode.",
    "fce_reporting_episode_duration": "Episode duration in days as used for HRG reporting purposes by the grouper.",
    "dominant_episode_flag": "Grouper flag indicating whether this episode is the dominant episode used to derive the spell HRG.",
    "spell_hrg_cd": "HRG4+ code assigned at hospital-spell level (repeated on every episode of the spell).",
    "spell_hrg_desc": "Description of the spell-level HRG4+ code, mapped from the HRG v4 reference lookup.",
    "spell_grouping_method_flag": "Grouper-returned flag indicating the method by which the spell-level HRG4+ code was derived.",
    "spell_dominant_proc_cd": "OPCS-4 procedure code identified by the HRG4+ grouper as the dominant procedure for the hospital provider spell.",
    "spell_dominant_proc_desc": "Description of the spell dominant procedure code, mapped from the OPCS-4.10 reference lookup.",
    "spell_primary_diag_cd": "ICD-10 primary diagnosis code used by the grouper at hospital provider spell level.",
    "spell_primary_diag_desc": "Description of the spell primary diagnosis code, mapped from the ICD diagnosis reference lookup.",
    "spell_secondary_diag_cd": "ICD-10 secondary diagnosis code used by the grouper at hospital provider spell level.",
    "spell_secondary_diag_desc": "Description of the spell secondary diagnosis code, mapped from the ICD diagnosis reference lookup.",
    "spell_episode_count": "Number of finished consultant episodes counted by the grouper within the hospital provider spell.",
    "spell_los": "Grouper-calculated length of stay in days for the hospital provider spell.",
    "spell_reporting_los": "Spell length of stay in days as used for HRG reporting purposes by the grouper.",
    "spell_critical_care_days": "Number of critical care days recorded for the hospital provider spell as submitted to the grouper.",
    "spell_ssc_cd": "Specialised service code assigned by the HRG4+ grouper at hospital provider spell level.",
    "spell_best_practice_cd": "Best practice tariff code assigned by the HRG4+ grouper at hospital provider spell level, null where no best practice classification applies.",
    "first_attend_cd": "CDS first attendance code indicating whether the attendance was a first or follow-up contact and its modality.",
    "first_attend_desc": "Description of FIRST_ATTEND_CD obtained from the lookup 3_lookup.dwh.cds_first_attend.",
    "grouping_method_flag": "HRG4+ grouper flag indicating the method by which the HRG was derived for the attendance.",
    "dominant_proc_cd": "OPCS-4 code of the dominant procedure determined by the grouper for the attendance.",
    "dominant_proc_desc": "Description of DOMINANT_PROC_CD obtained from the lookup 3_lookup.dwh.opcs_410.",
    "grouper_errors": "Grouper error/quality messages.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each HRG grouping record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "For both SLAM APC and outpatient HRG arms, retracted when the arm's SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. Neither arm emits superseded. This is a source-row presence status, not an HRG grouper result or clinical episode state.",
    "record_status_effective_from": "map_slam_apc_hrg.EPISODE_START_DT_TM for the APC arm, or map_slam_op_hrg.ATTENDANCE_DT_TM for the outpatient arm, cast to TIMESTAMP. Unlike event_datetime, this history field is not range-clamped to [1950,2100). It is an event-time proxy rather than an independently observed row-status transition; missing values remain null.",
    "record_status_effective_to": "The relevant SLAM arm's SOURCE_ABSENT_DETECTED_TS only when that row is retracted; otherwise null. A missing absence-detection time remains null: episode end, discharge and ADC_UPDT are not substituted.",
    "source_update_timestamp": "SOURCE_RECORD_UPDATED_DT carried unchanged from the contributing map_slam_apc_hrg or map_slam_op_hrg row. It is separate from ADC_UPDT ingestion provenance; the union does not aggregate clocks across arms and Silver refresh time is not substituted.",
    "loaded_at": "ADC_UPDT carried unchanged from the contributing map_slam_apc_hrg or map_slam_op_hrg row through the common HRG stage. This is per-row bronze ingestion provenance, not episode/attendance time or Silver refresh time; missing values remain null.",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical /  hrg grouping stage

# COMMAND ----------

# ==== journey_clinical._hrg_grouping_stage (internal) ====

@materialized_view(
    name=_n("journey_clinical._hrg_grouping_stage"),
    private=True,
    comment="Internal HRG rows carrying JSON code arrays for the public parent and code children.",
    refresh_policy="incremental",
)
def _hrg_grouping_stage():
    # Build the declared dataset: Internal HRG rows carrying JSON code arrays for the public
    # parent and code children.
    return _hrg_grouping_canonical().select(*HRG_GROUPING_STAGE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / hrg grouping

# COMMAND ----------

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_hrg_grouping():
    # Assemble hrg grouping rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return spark.read.table(_n("journey_clinical._hrg_grouping_stage")).select(*HRG_GROUPING_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.hrg_grouping"),
    comment="SLAM APC and outpatient HRG grouping results with sentinel-safe event time.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=HRG_GROUPING_COLUMN_COMMENTS,
)
def hrg_grouping():
    # Build the declared dataset: SLAM APC and outpatient HRG grouping results with sentinel-
    # safe event time.
    return _lifecycle_source_hrg_grouping().select(*HRG_GROUPING_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._hrg_grouping_metadata'),
    comment='Internal quality and batch metadata for clinical_hrg_grouping; same row grain as the research table. Join keys: patient_event_key, source_object, cds_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def hrg_grouping_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for clinical_hrg_grouping;
    # same row grain as the research table. Join keys: patient_event_key, source_object, cds_id.
    return (_lifecycle_source_hrg_grouping()).select(*HRG_GROUPING_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / hrg grouping diagnosis

# COMMAND ----------

# ==== journey_clinical.hrg_grouping_diagnosis ====

HRG_GROUPING_DIAGNOSIS_PUBLIC_COLUMNS = [
    "patient_event_key", "source_object", "cds_id", "sequence",
    "icd10_code", "display", "loaded_at",
]

HRG_GROUPING_DIAGNOSIS_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_hrg_grouping row.",
    "source_object": "SLAM source arm; part of the native parent key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID within source_object.",
    "sequence": "One-based source order of the ICD-10 diagnosis code.",
    "icd10_code": "Verbatim ICD-10 diagnosis code from the HRG source array.",
    "display": "Reserved display text; populated in Session 3.",
    "loaded_at": "Bronze load timestamp inherited from the HRG grouping row.",
}

@materialized_view(
    name=_n("journey_clinical.hrg_grouping_diagnosis"),
    comment="One ICD-10 diagnosis code per HRG grouping row in source order.",
    column_comments=HRG_GROUPING_DIAGNOSIS_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def hrg_grouping_diagnosis():
    # Build the declared dataset: One ICD-10 diagnosis code per HRG grouping row in source
    # order.
    stage = spark.read.table(
        _n("journey_clinical._hrg_grouping_stage")
    ).select(
        "patient_event_key", "source_object", "cds_id",
        "icd_diagnosis_codes_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "source_object", "cds_id", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("icd_diagnosis_codes_json"), "array<string>")
            ).alias("_position", "icd10_code"),
        )
        .where(F.col("icd10_code").isNotNull())
        .select(
            "patient_event_key", "source_object", "cds_id",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("icd10_code")).alias("icd10_code"),
            F.lit(None).cast("string").alias("display"),
            "loaded_at",
        )
        .select(*HRG_GROUPING_DIAGNOSIS_PUBLIC_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / hrg grouping procedure

# COMMAND ----------

# ==== journey_clinical.hrg_grouping_procedure ====

HRG_GROUPING_PROCEDURE_PUBLIC_COLUMNS = [
    "patient_event_key", "source_object", "cds_id", "sequence",
    "opcs4_code", "display", "loaded_at",
]

HRG_GROUPING_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_hrg_grouping row.",
    "source_object": "SLAM source arm; part of the native parent key.",
    "cds_id": "Native CDS_APC_ID or CDS_OPA_ID within source_object.",
    "sequence": "One-based source order of the OPCS-4 procedure code.",
    "opcs4_code": "Verbatim OPCS-4 procedure code from the HRG source array.",
    "display": "Reserved display text; populated in Session 3.",
    "loaded_at": "Bronze load timestamp inherited from the HRG grouping row.",
}

@materialized_view(
    name=_n("journey_clinical.hrg_grouping_procedure"),
    comment="One OPCS-4 procedure code per HRG grouping row in source order.",
    column_comments=HRG_GROUPING_PROCEDURE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def hrg_grouping_procedure():
    # Build the declared dataset: One OPCS-4 procedure code per HRG grouping row in source
    # order.
    stage = spark.read.table(
        _n("journey_clinical._hrg_grouping_stage")
    ).select(
        "patient_event_key", "source_object", "cds_id",
        "opcs_procedure_codes_json", "loaded_at",
    )
    return (
        stage.select(
            "patient_event_key", "source_object", "cds_id", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("opcs_procedure_codes_json"), "array<string>")
            ).alias("_position", "opcs4_code"),
        )
        .where(F.col("opcs4_code").isNotNull())
        .select(
            "patient_event_key", "source_object", "cds_id",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.trim(F.col("opcs4_code")).alias("opcs4_code"),
            F.lit(None).cast("string").alias("display"),
            "loaded_at",
        )
        .select(*HRG_GROUPING_PROCEDURE_PUBLIC_COLUMNS)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / costed activity

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
COSTED_ACTIVITY_SOURCE_COLUMNS = [
    "patient_event_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "extract_cd",
    "activity_record_id",
    "feed_type",
    "plemi",
    "nhs_number_status_cd",
    "cds_id",
    "attendance_id",
    "arrival_date",
    "arrival_time",
    "departure_date",
    "departure_time",
    "departure_type_cd",
    "provider_org_cd",
    "patient_org_cd",
    "pathway_id",
    "pod_cd",
    "treatment_function_cd",
    "source_los",
    "cf_band_cd",
    "episode_number",
    "episode_start_datetime",
    "episode_end_datetime",
    "episode_type_cd",
    "hosp_spell_id",
    "hrg_cd",
    "hrg_desc",
    "fce_hrg_cd",
    "fce_hrg_desc",
    "spell_hrg_cd",
    "spell_hrg_desc",
    "appointment_date",
    "appointment_time",
    "critical_care_unit_function_cd",
    "organs_supported",
    "critical_care_period_type_cd",
    "critical_care_level_ind",
    "unbundled_activity_datetime",
    "unbundled_activity_cd",
    "unbundled_hrg_cd",
    "unbundled_hrg_desc",
    "partial_costing_ind",
    "care_datetime",
    "care_id",
    "clinical_contact_duration",
    "chs_currency_cd",
    "team_type_cd",
    "contact_subject_cd",
    "consult_type_cd",
    "consult_medium_cd",
    "location_cd",
    "gp_therapy_ind",
    "service_request_id",
    "cost_line_count",
    "total_cost_sum",
    "total_o_cost_sum",
    "person_link_method",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
COSTED_ACTIVITY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'extract_cd',
    'activity_record_id',
    'feed_type',
    'plemi',
    'nhs_number_status_cd',
    'cds_id',
    'attendance_id',
    'arrival_date',
    'arrival_time',
    'departure_date',
    'departure_time',
    'departure_type_cd',
    'provider_org_cd',
    'patient_org_cd',
    'pathway_id',
    'pod_cd',
    'treatment_function_cd',
    'source_los',
    'cf_band_cd',
    'episode_number',
    'episode_start_datetime',
    'episode_end_datetime',
    'episode_type_cd',
    'hosp_spell_id',
    'hrg_cd',
    'hrg_desc',
    'fce_hrg_cd',
    'fce_hrg_desc',
    'spell_hrg_cd',
    'spell_hrg_desc',
    'appointment_date',
    'appointment_time',
    'critical_care_unit_function_cd',
    'organs_supported',
    'critical_care_period_type_cd',
    'critical_care_level_ind',
    'unbundled_activity_datetime',
    'unbundled_activity_cd',
    'unbundled_hrg_cd',
    'unbundled_hrg_desc',
    'partial_costing_ind',
    'care_datetime',
    'care_id',
    'clinical_contact_duration',
    'chs_currency_cd',
    'team_type_cd',
    'contact_subject_cd',
    'consult_type_cd',
    'consult_medium_cd',
    'location_cd',
    'gp_therapy_ind',
    'service_request_id',
    'cost_line_count',
    'total_cost_sum',
    'total_o_cost_sum',
    'person_link_method',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

COSTED_ACTIVITY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'extract_cd',
    'activity_record_id',
    'identity_status',
    'load_batch_id',
]

COSTED_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Internal Millennium person identifier assigned by the person-alias linkage, used to group activity for the same patient without publishing direct identifiers.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Start date and time of the costed activity as supplied by the source PLICS extract (ActivityStartDate).",
    "event_end_datetime": "End date and time of the costed activity as supplied by the source PLICS extract (ActivityEndDate).",
    "source_coding_system": "Source point-of-delivery classification; preserved without an invented mapping.",
    "source_code": "Source point-of-delivery classification; preserved without an invented mapping.",
    "source_display": "Human-readable label supplied by the source system for the source code for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "extract_cd": "SLAM EXTRACT_CD; first component of the primary key.",
    "activity_record_id": "SLAM ACTIVITY_RECORD_ID as BIGINT; second component of the primary key.",
    "feed_type": "Source-supplied code identifying the activity feed the costed record came from, preserved without an invented mapping.",
    "plemi": "PLICS matching identifier reused across extracts; never join without EXTRACT_CD.",
    "nhs_number_status_cd": "Non-identifying source quality/status code used during linkage.",
    "cds_id": "Commissioning Data Set record identifier carried from the source costing extract and used in the CDS-to-MRN person linkage fallback.",
    "attendance_id": "Source attendance identifier for the activity record as supplied by the costing extract.",
    "arrival_date": "Arrival date for the attendance, carried from the source costing extract.",
    "arrival_time": "Arrival time for the attendance, carried from the source costing extract and held as a timestamp.",
    "departure_date": "Departure date for the attendance, carried from the source costing extract (DepDate).",
    "departure_time": "Departure time for the attendance, carried from the source costing extract and held as a timestamp.",
    "departure_type_cd": "Source code describing the type of departure recorded for the attendance; preserved without an invented mapping.",
    "provider_org_cd": "Organisation code of the provider submitting the costed activity, as supplied by the source extract (OrgId).",
    "patient_org_cd": "Organisation code associated with the patient's commissioner or responsible organisation, as supplied by the source extract.",
    "pathway_id": "Patient pathway identifier for the activity record as supplied by the source costing extract (PathID); use with EXTRACT_CD as extract-scoped identifiers are not unique across extracts.",
    "pod_cd": "Source point-of-delivery classification; preserved without an invented mapping.",
    "treatment_function_cd": "Treatment function code for the costed activity as supplied by the source costing extract (Tfc).",
    "source_los": "Length-of-stay value carried from the source costing extract without recalculation.",
    "cf_band_cd": "Cystic Fibrosis year-of-care currency band supplied by the source.",
    "episode_number": "Sequence number of the episode within the hospital spell, as supplied by the source costing extract (EpiNo).",
    "episode_start_datetime": "Episode start date and time as supplied by the source PLICS costing extract (EpStDte).",
    "episode_end_datetime": "Episode end date and time as supplied by the source PLICS costing extract (EpEnDte).",
    "episode_type_cd": "Source-supplied episode type classification code from the PLICS costing extract, preserved without an invented mapping.",
    "hosp_spell_id": "Source hospital spell identifier for the activity record, as supplied by the PLICS costing extract.",
    "hrg_cd": "Healthcare Resource Group (HRG4+) code assigned to the costed activity record in the source extract.",
    "hrg_desc": "Description of HRG_CD sourced from the HRG v4 reference lookup.",
    "fce_hrg_cd": "Healthcare Resource Group code derived at finished consultant episode level in the source costing extract.",
    "fce_hrg_desc": "Description of FCE_HRG_CD sourced from the HRG v4 reference lookup.",
    "spell_hrg_cd": "Healthcare Resource Group code derived at hospital spell level in the source costing extract.",
    "spell_hrg_desc": "Description of SPELL_HRG_CD sourced from the HRG v4 reference lookup.",
    "appointment_date": "Appointment date for the costed outpatient or non-admitted activity, as supplied by the source extract.",
    "appointment_time": "Appointment time for the costed activity as supplied by the source extract, stored in a timestamp column.",
    "critical_care_unit_function_cd": "Critical care unit function code for the activity record, supplied by the source costing extract without an invented mapping.",
    "organs_supported": "Source-supplied critical care organs supported value for the activity record.",
    "critical_care_period_type_cd": "Critical care period type code supplied by the source costing extract, preserved without an invented mapping.",
    "critical_care_level_ind": "Critical care level indicator supplied by the source costing extract.",
    "unbundled_activity_datetime": "Date and time of the unbundled activity component associated with the costed record.",
    "unbundled_activity_cd": "Code identifying the unbundled activity component of the costed record, as supplied by the source extract.",
    "unbundled_hrg_cd": "Healthcare Resource Group code for the unbundled activity component of the costed record.",
    "unbundled_hrg_desc": "Description of UNBUNDLED_HRG_CD sourced from the HRG v4 reference lookup.",
    "partial_costing_ind": "Source indicator flagging partially costed activity, available only from the PE2122 NCC extract.",
    "care_datetime": "Community care contact date and time supplied by the PE2122 NCC extract.",
    "care_id": "Source identifier for the community care contact record, supplied by the PE2122 NCC extract.",
    "clinical_contact_duration": "Duration of the clinical contact recorded in the PE2122 NCC extract, in the units supplied by the source.",
    "chs_currency_cd": "Community health services currency code supplied by the PE2122 NCC costing extract.",
    "team_type_cd": "Coded team type for the community contact, supplied as an integer by the PE2122 NCC extract.",
    "contact_subject_cd": "Community contact subject code supplied by the PE2122 NCC costing extract (source CCSubject), preserved without an invented mapping.",
    "consult_type_cd": "Consultation type code for the community contact as supplied by the PE2122 NCC costing extract (source ConsultType).",
    "consult_medium_cd": "Consultation medium code recording how the community contact was conducted, as supplied by the PE2122 NCC costing extract (source CMedium).",
    "location_cd": "Source location code for the site or place where the costed activity or contact took place, as supplied by the PE2122 NCC extract (source LocCode).",
    "gp_therapy_ind": "Source-supplied therapy indicator flag for the community contact taken from the PE2122 NCC extract (source GPTherapyInd), preserved without an invented mapping.",
    "service_request_id": "Source service request identifier for the community contact carried from the PE2122 NCC costing extract (source SerReqID); use with EXTRACT_CD as extract-scoped identifiers are not unique across extracts.",
    "cost_line_count": "Duplicate-weighted count of map_slam_cost_line_item rows aggregated for this activity record, using the collapsed source duplicate counts.",
    "total_cost_sum": "Duplicate-weighted sum of TOTAL_COST across the cost line items for this activity record, retained at exact decimal precision.",
    "total_o_cost_sum": "Duplicate-weighted sum of TOTAL_O_COST, a component of total cost, across the cost line items for this activity record, retained at exact decimal precision.",
    "person_link_method": "Code recording the linkage path used to derive PERSON_ID, distinguishing the NHS-number alias match from the CDS-to-MRN fallback.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each costed activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_slam_costed_activity.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. This is source-row presence, not the clinical activity's completion state or the partial-costing indicator.",
    "record_status_effective_from": "Start date and time of the costed activity as supplied by the source PLICS extract (ActivityStartDate).",
    "record_status_effective_to": "map_slam_costed_activity.SOURCE_ABSENT_DETECTED_TS only when SOURCE_PRESENT_IND is false; otherwise null. Missing absence time remains null; ACTIVITY_END_DT_TM and ADC_UPDT are not fallbacks. This is the row-absence end proxy, not the clinical activity end.",
    "source_update_timestamp": "map_slam_costed_activity.ADC_UPDT carried unchanged, identical to loaded_at. The Silver projection supplies no independent native application-update clock and does not substitute Silver refresh time.",
    "loaded_at": "map_slam_costed_activity.ADC_UPDT carried unchanged for the contributing activity row, identical to source_update_timestamp. No cost-line child clock is joined or aggregated here. This is the input row's load provenance, not the clinical activity date or Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_costed_activity():
    # Assemble costed activity rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _costed_activity_canonical().select(*COSTED_ACTIVITY_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.costed_activity"),
    comment="One frozen PLICS costed activity from FY19/20 through FY21/22 extracts.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=COSTED_ACTIVITY_COLUMN_COMMENTS,
)
def costed_activity():
    # Build the declared dataset: One frozen PLICS costed activity from FY19/20 through FY21/22
    # extracts.
    return _lifecycle_source_costed_activity().select(*COSTED_ACTIVITY_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._costed_activity_metadata'),
    comment='Internal quality and batch metadata for clinical_costed_activity; same row grain as the research table. Join keys: patient_event_key, extract_cd, activity_record_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def costed_activity_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_costed_activity; same row grain as the research table. Join keys:
    # patient_event_key, extract_cd, activity_record_id.
    return (_lifecycle_source_costed_activity()).select(*COSTED_ACTIVITY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / cost line item

# COMMAND ----------

SRC_SLAM_COST_LINE_ITEM = "4_prod.bronze.map_slam_cost_line_item"

COST_LINE_ITEM_COLUMN_COMMENTS = {
    "cost_line_item_key": "Deterministic SHA-256 key for this SLAM cost line.",
    "costed_activity_key": "Deterministic SHA-256 key for the parent costed activity.",
    "extract_cd": "Source costing extract discriminator; required in every cost-line join.",
    "activity_record_id": "SLAM ACTIVITY_RECORD_ID as BIGINT; joins with extract_cd to the parent native key.",
    "line_hash": "Content-hash key derived from the source line attributes, used to identify a distinct cost line after collapsing exact duplicates.",
    "activity_cost_item_cd": "National Cost Collection activity cost item code for the cost line, preserved as supplied by the source extract without mapping.",
    "resource_cost_item_cd": "National Cost Collection resource cost item code for the cost line, preserved as supplied by the source extract without mapping.",
    "activity_count": "Source-reported activity count for the cost line, retained as text as supplied by the extract.",
    "unbundled_subtype_cd": "Observed values: 1 high-cost drug, 2 device, 3 diagnostic/other, 0 default; null on ordinary lines.",
    "unbundled_currency_cd": "Unbundled currency code recorded against the cost line by the source extract, null on ordinary bundled lines.",
    "unbundled_currency_datetime": "Date and time recorded against the unbundled currency for the cost line, as reported by the source extract.",
    "total_cost": "Total cost reported by the source extract for the cost line, retained at exact decimal(36,18) precision.",
    "total_o_cost": "Component of the line total cost reported by the source extract, retained at exact decimal(36,18) precision.",
    "source_duplicate_count": "Exact source rows represented by this collapsed line.",
    "record_status": "Retracted when map_slam_cost_line_item.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted, and the value is a derived STRING label rather than the original Boolean presence flag.",
    "loaded_at": "map_slam_cost_line_item.ADC_UPDT carried unchanged for the cost-line row. The parent activity key is derived without a parent join or added parent clock; this is bronze load provenance, not the currency date/time or Silver refresh time.",
}

COST_LINE_ITEM_LIFECYCLE_FIELDS = [
]

COST_LINE_ITEM_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_cost_line_item():
    # Assemble cost line item rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    s = read_source(SRC_SLAM_COST_LINE_ITEM)
    # contract v2: rename deterministic row and parent identities as keys and align the native activity component with its parent type
    return s.select(
        stable_id("cost_line:slam", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID, s.LINE_HASH)
         .alias("cost_line_item_key"),
        stable_id("costed_activity:slam", s.EXTRACT_CD, s.ACTIVITY_RECORD_ID)
         .alias("costed_activity_key"),
        s.EXTRACT_CD.alias("extract_cd"),
        s.ACTIVITY_RECORD_ID.cast("bigint").alias("activity_record_id"),
        s.LINE_HASH.alias("line_hash"), s.ACTIVITY_COST_ITEM_CD.alias("activity_cost_item_cd"),
        s.RESOURCE_COST_ITEM_CD.alias("resource_cost_item_cd"),
        s.ACTIVITY_COUNT.alias("activity_count"), s.UNBUNDLED_SUBTYPE_CD.alias("unbundled_subtype_cd"),
        s.UNBUNDLED_CURRENCY_CD.alias("unbundled_currency_cd"),
        s.UNBUNDLED_CURRENCY_DT_TM.alias("unbundled_currency_datetime"),
        s.TOTAL_COST.cast("decimal(38,6)").alias("total_cost"),
        s.TOTAL_O_COST.cast("decimal(38,6)").alias("total_o_cost"),
        s.SOURCE_DUPLICATE_COUNT.alias("source_duplicate_count"),
        F.when(~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True)), F.lit("retracted"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_reference.cost_line_item"),
    comment="PLICS cost lines (frozen FY19/20-FY21/22 extracts); event plane excluded by design.",
    refresh_policy="incremental",
    column_comments=COST_LINE_ITEM_COLUMN_COMMENTS,
)
def cost_line_item():
    # Build the declared dataset: PLICS cost lines (frozen FY19/20-FY21/22 extracts); event
    # plane excluded by design.
    return _lifecycle_source_cost_line_item().drop(*COST_LINE_ITEM_LIFECYCLE_FIELDS, *COST_LINE_ITEM_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / drug expenditure

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
DRUG_EXPENDITURE_SOURCE_COLUMNS = [
    "patient_event_key",
    "source_row_hash",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "transaction_id",
    "financial_year",
    "financial_month",
    "reporting_year",
    "reporting_month",
    "provider_org_cd",
    "site_cd",
    "site_name",
    "specialty_cd",
    "consultant_cd",
    "patient_type",
    "pod_cd",
    "chargeable_item",
    "additional_info",
    "dmd_raw",
    "dmd_code",
    "dmd_concept_id",
    "dmd_concept_name",
    "drug_standard_concept_id",
    "drug_standard_concept_name",
    "dmd_mapping_status",
    "dmd_taxonomy_cd",
    "route_of_administration",
    "strength",
    "volume",
    "pack_size",
    "quantity",
    "unit_of_measure",
    "dispensing_route",
    "dispensing_location",
    "indication",
    "funding_reference",
    "hcdr_category_cd",
    "hcdr_category_desc",
    "ccg_residence_cd",
    "ccg_gp_cd",
    "commissioner_cd",
    "commissioner_type",
    "service_line",
    "service_category_cd",
    "unit_price_supplier",
    "unit_price_commissioner",
    "vat",
    "vat_cd",
    "income",
    "cost",
    "margin",
    "lloyds_dispensing_fee",
    "production_fee",
    "fixed_patient_income",
    "cost_centre_desc",
    "drug_feed",
    "data_set",
    "drug_category",
    "ledger_cd",
    "exclusion_flag",
    "exclusion_reason",
    "ledger_lv3_cd",
    "ledger_lv3_desc",
    "ledger_lv6_cd",
    "ledger_lv6_desc",
    "ledger_lv7_cd",
    "ledger_lv7_desc",
    "ledger_lv9_cd",
    "ledger_lv9_desc",
    "slr_cd",
    "diabetic_flag",
    "imcoe_flag",
    "source_duplicate_count",
    "person_link_method",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
DRUG_EXPENDITURE_PUBLIC_COLUMNS = [
    'patient_event_key',
    'source_row_hash',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'transaction_id',
    'financial_year',
    'financial_month',
    'reporting_year',
    'reporting_month',
    'provider_org_cd',
    'site_cd',
    'site_name',
    'specialty_cd',
    'consultant_cd',
    'patient_type',
    'pod_cd',
    'chargeable_item',
    'additional_info',
    'dmd_raw',
    'dmd_code',
    'dmd_concept_id',
    'dmd_concept_name',
    'drug_standard_concept_id',
    'drug_standard_concept_name',
    'dmd_mapping_status',
    'dmd_taxonomy_cd',
    'route_of_administration',
    'strength',
    'volume',
    'pack_size',
    'quantity',
    'unit_of_measure',
    'dispensing_route',
    'dispensing_location',
    'indication',
    'funding_reference',
    'hcdr_category_cd',
    'hcdr_category_desc',
    'ccg_residence_cd',
    'ccg_gp_cd',
    'commissioner_cd',
    'commissioner_type',
    'service_line',
    'service_category_cd',
    'unit_price_supplier',
    'unit_price_commissioner',
    'vat',
    'vat_cd',
    'income',
    'cost',
    'margin',
    'lloyds_dispensing_fee',
    'production_fee',
    'fixed_patient_income',
    'cost_centre_desc',
    'drug_feed',
    'data_set',
    'drug_category',
    'ledger_cd',
    'exclusion_flag',
    'exclusion_reason',
    'ledger_lv3_cd',
    'ledger_lv3_desc',
    'ledger_lv6_cd',
    'ledger_lv6_desc',
    'ledger_lv7_cd',
    'ledger_lv7_desc',
    'ledger_lv9_cd',
    'ledger_lv9_desc',
    'slr_cd',
    'diabetic_flag',
    'imcoe_flag',
    'source_duplicate_count',
    'person_link_method',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

DRUG_EXPENDITURE_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'identity_status',
    'load_batch_id',
]

DRUG_EXPENDITURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_row_hash": "HCD ROW_HASH supplied by the source; evidence for the exception where no durable native row id exists.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Internal Millennium person identifier resolved from source patient identifiers via person alias (MRN first, then NHS number); null when unresolved.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Effective date/time as supplied; two known rows dated 2122 are retained losslessly.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
    "source_code": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "source_display": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "transaction_id": "Source SLR transaction reference as supplied, blank or null on a material share of rows and therefore not usable as a key.",
    "financial_year": "Financial year to which the high-cost drug or device expenditure is attributed, as supplied by the source.",
    "financial_month": "Financial period (month) within the financial year to which the expenditure is attributed, as supplied by the source.",
    "reporting_year": "Year in which the expenditure was reported, as supplied by the source.",
    "reporting_month": "Month number within the reporting year in which the expenditure was reported, as supplied by the source.",
    "provider_org_cd": "Organisation code of the provider recording the expenditure, as supplied by the source.",
    "site_cd": "Source site code identifying the hospital site associated with the charge.",
    "site_name": "Curated mapping for RLH, SBH, WXH, NUH and NGH only; other values are unresolved by design.",
    "specialty_cd": "Treatment specialty code associated with the charge, as supplied by the source.",
    "consultant_cd": "Local code identifying the responsible consultant for the charge; may be blank or null.",
    "patient_type": "Source-supplied categorisation of the care setting under which the chargeable item was supplied.",
    "pod_cd": "Point of delivery code classifying the activity under which the item is reimbursed, as supplied by the source.",
    "chargeable_item": "Name of the chargeable high-cost drug or device as recorded in the source.",
    "additional_info": "Free-text supplementary description of the supplied item, including presentation and administration details, as recorded in the source.",
    "dmd_raw": "Verbatim source dm+d value; includes junk values such as No SNOMED.",
    "dmd_code": "Strict trimmed all-numeric dm+d code; null for blank or non-numeric source values.",
    "dmd_concept_id": "OMOP concept identifier for the dm+d source concept matched from DMD_CODE; null when no match is made.",
    "dmd_concept_name": "OMOP concept name of the matched dm+d source concept; null when no match is made.",
    "drug_standard_concept_id": "OMOP standard concept reached only when exactly one valid Maps-to target exists.",
    "drug_standard_concept_name": "OMOP standard concept name for the drug, populated from the OMOP concept table only where the dm+d code resolved to exactly one Maps-to standard concept.",
    "dmd_mapping_status": "dm+d mapping state: NO_CODE, CODE_UNMAPPED, MAPPED_STANDARD, or MAPPED_DMD_ONLY.",
    "dmd_taxonomy_cd": "dm+d taxonomy level code as supplied in the source DMDTaxonomyCode field, indicating the dm+d concept type of the coded item.",
    "route_of_administration": "Route of administration for the dispensed item as supplied by the source, typically a SNOMED CT route code but occasionally free-text route wording.",
    "strength": "Strength of the supplied drug or device as recorded in the source, held as free text and sometimes blank or non-numeric placeholder wording.",
    "volume": "Product volume as supplied by the source, held as free text and frequently blank.",
    "pack_size": "Pack or dispensed unit description for the item as supplied by the source, held as free text.",
    "quantity": "Quantity of the item attributed to this expenditure row, which may be fractional; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "unit_of_measure": "Unit of measure for the recorded quantity as supplied by the source, generally a SNOMED CT unit code and sometimes blank.",
    "dispensing_route": "Source-supplied code indicating the dispensing route or supply channel used for the item.",
    "dispensing_location": "Description of the dispensing location or supply point recorded in the source, such as a hospital pharmacy, clinic or homecare service.",
    "indication": "Clinical indication for the high-cost drug or device as recorded in the source, held as free text.",
    "funding_reference": "Source-supplied funding or approval reference for the item, frequently null.",
    "hcdr_category_cd": "Local high-cost-drug reimbursement category from source HRGCode; not an HRG4+ code.",
    "hcdr_category_desc": "Description of the local high-cost-drug reimbursement category, taken from the source HRGDesc field and paired with HCDR_CATEGORY_CD.",
    "ccg_residence_cd": "Code of the CCG or successor commissioning organisation associated with the patient's area of residence, as supplied by the source.",
    "ccg_gp_cd": "Code of the CCG or successor commissioning organisation associated with the patient's registered GP practice, as supplied by the source.",
    "commissioner_cd": "Code of the commissioning organisation responsible for funding the item, as supplied by the source.",
    "commissioner_type": "Type or category of the responsible commissioner as supplied by the source, and blank where not recorded.",
    "service_line": "Service line identifier assigned to the expenditure row in Service Line Reporting, often null.",
    "service_category_cd": "Source-supplied service category code classifying the expenditure row within the finance hierarchy.",
    "unit_price_supplier": "Supplier unit price for the item as supplied by the source finance report.",
    "unit_price_commissioner": "Commissioner unit price for the item as supplied by the source finance report.",
    "vat": "Source-supplied indicator of whether VAT applies to the item.",
    "vat_cd": "Source-supplied VAT code applied to the item for finance reporting.",
    "income": "Reimbursement income value for the drug or device line as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "cost": "Expenditure cost value for the drug or device line as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "margin": "Margin value for the line, the difference between income and cost as supplied by the SLR source; multiply by SOURCE_DUPLICATE_COUNT to reconstruct totals.",
    "lloyds_dispensing_fee": "Dispensing fee element charged by the homecare/dispensing provider for the line as supplied by the source finance report, null where no such fee applies.",
    "production_fee": "Production (compounding/preparation) fee element for the line as supplied by the source, null where no such fee applies.",
    "fixed_patient_income": "Fixed per-patient income element for the line as supplied by the source, null where income is not charged on a fixed-patient basis.",
    "cost_centre_desc": "Finance cost centre description for the line, which commonly embeds the site abbreviation, the responsible consultant's name and the specialty.",
    "drug_feed": "Source-supplied identifier of the data feed or supply system from which the drug expenditure line was obtained.",
    "data_set": "Submission dataset grouping identifying the site or sites and feed combination the line was reported under.",
    "drug_category": "Source-supplied categorisation of the drug expenditure line by supply or treatment category for finance reporting.",
    "ledger_cd": "General ledger account code to which the expenditure or income line is posted.",
    "exclusion_flag": "Source indicator showing whether the line is excluded from the reported SLR expenditure position.",
    "exclusion_reason": "Source-supplied reason accompanying EXCLUSION_FLAG explaining why the line was excluded, null when not excluded.",
    "ledger_lv3_cd": "Level 3 code of the finance ledger hierarchy, representing the hospital site or division to which the line is attributed.",
    "ledger_lv3_desc": "Description for LEDGER_LV3_CD, naming the hospital site or division level of the finance ledger hierarchy.",
    "ledger_lv6_cd": "Level 6 code of the finance ledger hierarchy, representing the directorate or service grouping for the line.",
    "ledger_lv6_desc": "Description for LEDGER_LV6_CD, naming the directorate or service grouping in the finance ledger hierarchy.",
    "ledger_lv7_cd": "Level 7 code of the finance ledger hierarchy, representing the site-level specialty or department for the line.",
    "ledger_lv7_desc": "Description for LEDGER_LV7_CD, naming the site-level specialty or department in the finance ledger hierarchy.",
    "ledger_lv9_cd": "Level 9 code of the finance ledger hierarchy, the lowest-level cost centre or account for the line.",
    "ledger_lv9_desc": "Description for LEDGER_LV9_CD, naming the lowest-level cost centre or account in the finance ledger hierarchy.",
    "slr_cd": "Service Line Reporting service line code assigned to the expenditure line.",
    "diabetic_flag": "Source-supplied indicator marking lines classified as diabetes-related expenditure; null where not flagged.",
    "imcoe_flag": "Source-supplied indicator marking lines attributed to the IMCoE service classification; null where not flagged.",
    "source_duplicate_count": "Number of exact-identical source rows represented by this published row; reconstruct additive measures as measure × count.",
    "person_link_method": "How PERSON_ID was resolved: MRN_ALIAS first, NHS_ALIAS fallback, or null when unresolved.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each drug expenditure record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false on 4_prod.tmp.journey_finance_hcd_expenditure_s42, otherwise active; missing presence defaults true. No superseded status is emitted. This is staged source-row presence, not the financial EXCLUSION_FLAG or drug administration status.",
    "record_status_effective_from": "EFFECTIVE_DT_TM carried unchanged from 4_prod.tmp.journey_finance_hcd_expenditure_s42. This expenditure-effective-time proxy is not an independently observed status transition; the Silver projection applies no [1950,2100) clamp or ingestion-time fallback.",
    "record_status_effective_to": "ADC_UPDT from 4_prod.tmp.journey_finance_hcd_expenditure_s42 only when its SOURCE_PRESENT_IND is false; otherwise null. This is the staged row's load-time end proxy, not the expenditure date or an independently observed source-status transition.",
    "source_update_timestamp": "ADC_UPDT carried unchanged from 4_prod.tmp.journey_finance_hcd_expenditure_s42, identical to loaded_at. The Silver projection reads this staging table directly, not map_finance_hcd_expenditure, and supplies no independent native application-update clock.",
    "loaded_at": "ADC_UPDT carried unchanged from the contributing row of 4_prod.tmp.journey_finance_hcd_expenditure_s42, identical to source_update_timestamp. No extra source clock is joined or aggregated here; the value is not the current Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_drug_expenditure():
    # Assemble drug expenditure rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _drug_expenditure_canonical().select(*DRUG_EXPENDITURE_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.drug_expenditure"),
    comment="One HCD drug-expenditure row keyed by immutable ROW_HASH.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=DRUG_EXPENDITURE_COLUMN_COMMENTS,
)
def drug_expenditure():
    # Build the declared dataset: One HCD drug-expenditure row keyed by immutable ROW_HASH.
    return _lifecycle_source_drug_expenditure().select(*DRUG_EXPENDITURE_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._drug_expenditure_metadata'),
    comment='Internal quality and batch metadata for clinical_drug_expenditure; same row grain as the research table. Join keys: patient_event_key.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def drug_expenditure_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_drug_expenditure; same row grain as the research table. Join keys:
    # patient_event_key.
    return (_lifecycle_source_drug_expenditure()).select(*DRUG_EXPENDITURE_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / medication supply

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
MEDICATION_SUPPLY_SOURCE_COLUMNS = [
    "patient_event_key",
    "homecare_request_item_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "request_key",
    "item_seq",
    "request_status_code",
    "request_status_display",
    "item_status_code",
    "item_status_display",
    "item_type",
    "item_description",
    "pack_description",
    "quantity_requested",
    "quantity_original",
    "quantity_delivered",
    "order_unit",
    "label_directions",
    "nfd_reason",
    "supply_start_date",
    "supply_interval",
    "supply_period",
    "request_item_count",
    "request_complete_item_count",
    "request_released_date",
    "location_name",
    "cost_centre_name",
    "indication",
    "clinic",
    "dmd_vtm_concept_id",
    "dmd_vtm_code",
    "dmd_vtm_name",
    "drug_mapping_method",
    "care_site_cd",
    "care_site_match_method",
    "lnkpid",
    "name_key",
    "mrn_candidates",
    "nhs_candidates",
    "person_match_method",
    "person_match_status",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
MEDICATION_SUPPLY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'homecare_request_item_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'request_key',
    'item_seq',
    'request_status_code',
    'request_status_display',
    'item_status_code',
    'item_status_display',
    'item_type',
    'item_description',
    'pack_description',
    'quantity_requested',
    'quantity_original',
    'quantity_delivered',
    'order_unit',
    'label_directions',
    'nfd_reason',
    'supply_start_date',
    'supply_interval',
    'supply_period',
    'request_item_count',
    'request_complete_item_count',
    'request_released_date',
    'location_name',
    'cost_centre_name',
    'indication',
    'clinic',
    'dmd_vtm_concept_id',
    'dmd_vtm_code',
    'dmd_vtm_name',
    'drug_mapping_method',
    'care_site_cd',
    'care_site_match_method',
    'lnkpid',
    'name_key',
    'mrn_candidates',
    'nhs_candidates',
    'person_match_method',
    'person_match_status',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

MEDICATION_SUPPLY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'homecare_request_item_id',
    'identity_status',
    'load_batch_id',
]

MEDICATION_SUPPLY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "homecare_request_item_id": "JAC HOMECARE_REQUEST_ITEM_ID; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "JAC internal patient link key; not a direct patient identifier.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Source request-created date.",
    "event_end_datetime": "Source request-completed date.",
    "source_coding_system": "Matched dm+d VTM SNOMED CT identifier.",
    "source_code": "Matched dm+d VTM SNOMED CT identifier.",
    "source_display": "Matched dm+d VTM preferred name.",
    "request_key": "JAC homecare request identifier from lnkrb.",
    "item_seq": "Item sequence within the request; NULL for an itemless header.",
    "request_status_code": "Raw JAC header status code.",
    "request_status_display": "COMPLETE, RELEASED, IN_PROGRESS, or provisional CANCELLED_OR_ERROR.",
    "item_status_code": "Raw JAC item status code.",
    "item_status_display": "COMPLETE, INCOMPLETE, or provisional CANCELLED_OR_ERROR.",
    "item_type": "Raw JAC homecare item type.",
    "item_description": "Source medicine or supply description.",
    "pack_description": "Source pack description.",
    "quantity_requested": "Requested quantity from the item.",
    "quantity_original": "Original requested quantity from the item.",
    "quantity_delivered": "Delivered quantity from the item.",
    "order_unit": "Source order unit.",
    "label_directions": "Source dispensing label directions.",
    "nfd_reason": "Source reason the item was not fulfilled.",
    "supply_start_date": "Requested supply start date.",
    "supply_interval": "Source supply interval.",
    "supply_period": "Source supply period.",
    "request_item_count": "Header-reported total item count.",
    "request_complete_item_count": "Header-reported completed item count.",
    "request_released_date": "Source request-released date.",
    "location_name": "Raw JAC homecare location name.",
    "cost_centre_name": "Raw JAC homecare cost-centre name.",
    "indication": "Clinical indication recorded on the request.",
    "clinic": "Clinic recorded on the request.",
    "dmd_vtm_concept_id": "Valid dm+d VTM OMOP concept_id from exact item-description matching.",
    "dmd_vtm_code": "Matched dm+d VTM SNOMED CT identifier.",
    "dmd_vtm_name": "Matched dm+d VTM preferred name.",
    "drug_mapping_method": "VTM_NAME, VTM_SYNONYM, UNMAPPED, DISABLED, or NULL for itemless headers.",
    "care_site_cd": "map_care_site key from a unique exact location-name match.",
    "care_site_match_method": "NAME_EXACT, UNMAPPED, or UNAVAILABLE.",
    "lnkpid": "Direct identifier published and IG-governed at serve time",
    "name_key": "Direct identifier published and IG-governed at serve time",
    "mrn_candidates": "Distinct active Cerner PERSON_ID candidates for the normalised MRN.",
    "nhs_candidates": "Distinct active Cerner PERSON_ID candidates for the checksum-valid NHS number.",
    "person_match_method": "MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, CONFLICT, AMBIGUOUS, or NONE.",
    "person_match_status": "MATCHED or the reason no PERSON_ID was assigned.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each medication supply record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_homecare_request.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. Request/item status, request completion and delivered quantity do not determine this label; usable-code filtering separately controls publication eligibility.",
    "record_status_effective_from": "Source request-created date.",
    "record_status_effective_to": "map_homecare_request.ADC_UPDT only when SOURCE_PRESENT_IND is false; otherwise null. This is the input-load-time end proxy, not REQUEST_COMPLETED or a clinical supply end; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "First non-null map_homecare_request.ITEM_SOURCE_RECORD_UPDATED_DT then SOURCE_RECORD_UPDATED_DT. These are upstream mirror timestamps, with the item clock preferred even when the request clock is later; the expression is coalesce, not greatest, and has no ADC_UPDT or Silver refresh-time fallback.",
    "loaded_at": "map_homecare_request.ADC_UPDT carried unchanged through usable-code filtering and source/public projections. No parent clock is joined or aggregated. This input load clock is distinct from the preferred item/request mirror clock in source_update_timestamp, supply dates and Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_medication_supply():
    # Assemble medication supply rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _medication_supply_canonical().select(*MEDICATION_SUPPLY_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.medication_supply"),
    comment="One JAC homecare supply-request item; supply, never administration.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=MEDICATION_SUPPLY_COLUMN_COMMENTS,
)
def medication_supply():
    # Build the declared dataset: One JAC homecare supply-request item; supply, never
    # administration.
    return _lifecycle_source_medication_supply().select(*MEDICATION_SUPPLY_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._medication_supply_metadata'),
    comment='Internal quality and batch metadata for clinical_medication_supply; same row grain as the research table. Join keys: patient_event_key, homecare_request_item_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def medication_supply_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_medication_supply; same row grain as the research table. Join keys:
    # patient_event_key, homecare_request_item_id.
    return (_lifecycle_source_medication_supply()).select(*MEDICATION_SUPPLY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / elective access entry

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
ELECTIVE_ACCESS_ENTRY_SOURCE_COLUMNS = [
    "patient_event_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "waiting_list_oid",
    "source_system_oid",
    "pathway_oid",
    "referral_oid",
    "patient_oid",
    "waiting_list_id",
    "legacy_waiting_list_id",
    "waiting_list_name",
    "waiting_list_code",
    "department",
    "division",
    "business_unit",
    "clinical_priority_code",
    "clinical_priority_recorded_datetime",
    "site_rvid",
    "treatment_function_rvid",
    "admin_category_rvid",
    "intended_management_rvid",
    "admit_method_rvid",
    "priority_rvid",
    "status_rvid",
    "elective_admission_type_rvid",
    "encounter_type_rvid",
    "removal_reason_rvid",
    "division_rvid",
    "tci_location_rvid",
    "admit_offer_outcome_rvid",
    "lead_clinician_prid",
    "status_reason",
    "status_change_datetime",
    "decided_to_admit_datetime",
    "tci_datetime",
    "tci_future_ind",
    "tci_created_datetime",
    "guaranteed_activity_datetime",
    "actual_guaranteed_activity_datetime",
    "planned_datetime",
    "earliest_reasonable_offer_datetime",
    "admit_datetime",
    "comments",
    "active_ind",
    "created_datetime",
    "created_by_prid",
    "modified_datetime",
    "modified_by_prid",
    "person_link_status",
    "person_link_method",
    "identifier_link_status",
    "linkage_historical_fallback_ind",
    "linkage_fallback_conflict_ind",
    "nhs_number_valid_ind",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'waiting_list_oid',
    'source_system_oid',
    'pathway_oid',
    'referral_oid',
    'patient_oid',
    'waiting_list_id',
    'legacy_waiting_list_id',
    'waiting_list_name',
    'waiting_list_code',
    'department',
    'division',
    'business_unit',
    'clinical_priority_code',
    'clinical_priority_recorded_datetime',
    'site_rvid',
    'treatment_function_rvid',
    'admin_category_rvid',
    'intended_management_rvid',
    'admit_method_rvid',
    'priority_rvid',
    'status_rvid',
    'elective_admission_type_rvid',
    'encounter_type_rvid',
    'removal_reason_rvid',
    'division_rvid',
    'tci_location_rvid',
    'admit_offer_outcome_rvid',
    'lead_clinician_prid',
    'status_reason',
    'status_change_datetime',
    'decided_to_admit_datetime',
    'tci_datetime',
    'tci_future_ind',
    'tci_created_datetime',
    'guaranteed_activity_datetime',
    'actual_guaranteed_activity_datetime',
    'planned_datetime',
    'earliest_reasonable_offer_datetime',
    'admit_datetime',
    'comments',
    'active_ind',
    'created_datetime',
    'created_by_prid',
    'modified_datetime',
    'modified_by_prid',
    'person_link_status',
    'person_link_method',
    'identifier_link_status',
    'linkage_historical_fallback_ind',
    'linkage_fallback_conflict_ind',
    'nhs_number_valid_ind',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

ELECTIVE_ACCESS_ENTRY_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'source_system_oid',
    'waiting_list_oid',
    'identity_status',
    'load_batch_id',
]

ELECTIVE_ACCESS_ENTRY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "B3 bronze field CREATED_DT_TM; source value retained unless documented as derived.",
    "event_end_datetime": "B3 bronze field ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "source_coding_system": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "source_code": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "source_display": "B3 bronze field PROCEDURE_DESC; source value retained unless documented as derived.",
    "waiting_list_oid": "LUNA WAITING_LIST_OID as BIGINT; second component of the primary key.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID as BIGINT; first component of the primary key.",
    "pathway_oid": "B3 bronze field PATHWAY_OID; source value retained unless documented as derived.",
    "referral_oid": "B3 bronze field REFERRAL_OID; source value retained unless documented as derived.",
    "patient_oid": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "waiting_list_id": "B3 bronze field WAITING_LIST_ID; source value retained unless documented as derived.",
    "legacy_waiting_list_id": "B3 bronze field LEGACY_WAITING_LIST_ID; source value retained unless documented as derived.",
    "waiting_list_name": "B3 bronze field WAITING_LIST_NAME; source value retained unless documented as derived.",
    "waiting_list_code": "B3 bronze field WAITING_LIST_CODE; source value retained unless documented as derived.",
    "department": "B3 bronze field DEPARTMENT; source value retained unless documented as derived.",
    "division": "B3 bronze field DIVISION; source value retained unless documented as derived.",
    "business_unit": "B3 bronze field BUSINESS_UNIT; source value retained unless documented as derived.",
    "clinical_priority_code": "B3 bronze field FIELD_VALUE_VAR; source value retained unless documented as derived.",
    "clinical_priority_recorded_datetime": "B3 bronze field FIELD_VALUE_DATE_CLEAN; source value retained unless documented as derived.",
    "site_rvid": "B3 bronze field SITE_RVID; source value retained unless documented as derived.",
    "treatment_function_rvid": "B3 bronze field TREATMENT_FUNCTION_RVID; source value retained unless documented as derived.",
    "admin_category_rvid": "B3 bronze field ADMIN_CATEGORY_RVID; source value retained unless documented as derived.",
    "intended_management_rvid": "B3 bronze field INTENDED_MANAGEMENT_RVID; source value retained unless documented as derived.",
    "admit_method_rvid": "B3 bronze field ADMIT_METHOD_RVID; source value retained unless documented as derived.",
    "priority_rvid": "B3 bronze field WAITING_LIST_PRIORITY_RVID; source value retained unless documented as derived.",
    "status_rvid": "B3 bronze field WAITING_LIST_STATUS_RVID; source value retained unless documented as derived.",
    "elective_admission_type_rvid": "B3 bronze field ELECTIVE_ADMISSION_TYPE_RVID; source value retained unless documented as derived.",
    "encounter_type_rvid": "B3 bronze field ENCOUNTER_TYPE_RVID; source value retained unless documented as derived.",
    "removal_reason_rvid": "B3 bronze field REMOVAL_REASON_RVID; source value retained unless documented as derived.",
    "division_rvid": "B3 bronze field DIVISION_RVID; source value retained unless documented as derived.",
    "tci_location_rvid": "B3 bronze field TCI_LOCATION_RVID; source value retained unless documented as derived.",
    "admit_offer_outcome_rvid": "B3 bronze field ADMIT_OFFER_OUTCOME_RVID; source value retained unless documented as derived.",
    "lead_clinician_prid": "B3 bronze field LEAD_CLINICIAN_PRID; source value retained unless documented as derived.",
    "status_reason": "B3 bronze field WAITING_LIST_STATUS_REASON; source value retained unless documented as derived.",
    "status_change_datetime": "B3 bronze field WAITING_LIST_STATUS_CHANGE_DT_TM_CLEAN; source value retained unless documented as derived.",
    "decided_to_admit_datetime": "B3 bronze field DECIDED_TO_ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "tci_datetime": "B3 bronze field TCI_DT_TM_CLEAN; source value retained unless documented as derived.",
    "tci_future_ind": "B3 bronze field TCI_DT_TM_FUTURE_IND; source value retained unless documented as derived.",
    "tci_created_datetime": "B3 bronze field TCI_CREATED_DT_TM; source value retained unless documented as derived.",
    "guaranteed_activity_datetime": "B3 bronze field GUARANTEED_ACTIVITY_DT_TM_CLEAN; source value retained unless documented as derived.",
    "actual_guaranteed_activity_datetime": "B3 bronze field ACTUAL_GUARANTEED_ACTIVITY_DT_TM_CLEAN; source value retained unless documented as derived.",
    "planned_datetime": "B3 bronze field PLANNED_DT_TM_CLEAN; source value retained unless documented as derived.",
    "earliest_reasonable_offer_datetime": "B3 bronze field EARLIEST_REASONABLE_OFFER_DT_TM_CLEAN; source value retained unless documented as derived.",
    "admit_datetime": "B3 bronze field ADMIT_DT_TM_CLEAN; source value retained unless documented as derived.",
    "comments": "Direct identifier published and IG-governed at serve time",
    "active_ind": "Source closure state retained verbatim; false does not mean source absence.",
    "created_datetime": "B3 bronze field CREATED_DT_TM; source value retained unless documented as derived.",
    "created_by_prid": "B3 bronze field CREATED_BY_PRID; source value retained unless documented as derived.",
    "modified_datetime": "B3 bronze field MODIFIED_DT_TM; source value retained unless documented as derived.",
    "modified_by_prid": "B3 bronze field MODIFIED_BY_PRID; source value retained unless documented as derived.",
    "person_link_status": "Cross-arm consensus status; conflicts never publish PERSON_ID.",
    "person_link_method": "B3 bronze field PERSON_LINK_METHOD; source value retained unless documented as derived.",
    "identifier_link_status": "B3 bronze field IDENTIFIER_LINK_STATUS; source value retained unless documented as derived.",
    "linkage_historical_fallback_ind": "B3 bronze field LINKAGE_HISTORICAL_FALLBACK_IND; source value retained unless documented as derived.",
    "linkage_fallback_conflict_ind": "B3 bronze field LINKAGE_FALLBACK_CONFLICT_IND; source value retained unless documented as derived.",
    "nhs_number_valid_ind": "B3 bronze field NHS_NUMBER_VALID_IND; source value retained unless documented as derived.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each elective access entry record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_elective_access_list.SOURCE_PRESENT_IND is false; otherwise superseded when ACTIVE_IND is false; otherwise active. Each missing flag defaults true, and retraction takes priority over supersession. This is a derived label, not the source closure flag.",
    "record_status_effective_from": "map_elective_access_list.CREATED_DT_TM carried directly as the record-status start, without event-time clamping. No admission, procedure or child-attribute date replaces it.",
    "record_status_effective_to": "map_elective_access_list.SOURCE_ABSENT_DETECTED_TS for a retracted row; otherwise MODIFIED_DT_TM for a superseded row; otherwise null. Retraction takes priority. A missing timestamp in the selected branch remains null: there is no fallback to the other branch or ADC_UPDT.",
    "source_update_timestamp": "map_elective_access_list.MODIFIED_DT_TM carried unchanged, not a maximum over the joined procedure or attribute children. No ingestion or Silver refresh timestamp is substituted.",
    "loaded_at": "map_elective_access_list.ADC_UPDT carried unchanged. Procedure and attribute children contribute clinical fields but their clocks are not selected or aggregated here; this is not the latest child load time or the current Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_elective_access_entry():
    # Assemble elective access entry rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _elective_access_entry_canonical().select(*ELECTIVE_ACCESS_ENTRY_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.elective_access_entry"),
    comment="One LUNA elective-access entry with primary OPCS and clinical-priority folds.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=ELECTIVE_ACCESS_ENTRY_COLUMN_COMMENTS,
)
def elective_access_entry():
    # Build the declared dataset: One LUNA elective-access entry with primary OPCS and clinical-
    # priority folds.
    return _lifecycle_source_elective_access_entry().select(*ELECTIVE_ACCESS_ENTRY_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._elective_access_entry_metadata'),
    comment='Internal quality and batch metadata for clinical_elective_access_entry; same row grain as the research table. Join keys: patient_event_key, source_system_oid, waiting_list_oid.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def elective_access_entry_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_elective_access_entry; same row grain as the research table. Join keys:
    # patient_event_key, source_system_oid, waiting_list_oid.
    return (_lifecycle_source_elective_access_entry()).select(*ELECTIVE_ACCESS_ENTRY_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / elective access procedure

# COMMAND ----------

ELECTIVE_ACCESS_PROCEDURE_COLUMN_COMMENTS = {
    "elective_access_procedure_key": "Deterministic SHA-256 key for this LUNA procedure row.",
    "elective_access_entry_key": "Deterministic SHA-256 key for the parent elective access entry.",
    "waiting_list_oid": "LUNA WAITING_LIST_OID as BIGINT.",
    "procedure_code": "B3 bronze field PROCEDURE_CODE; source value retained unless documented as derived.",
    "procedure_desc": "B3 bronze field PROCEDURE_DESC; source value retained unless documented as derived.",
    "procedure_catalog": "B3 bronze field PROCEDURE_CATALOG; source value retained unless documented as derived.",
    "procedure_rvid": "B3 bronze field PROCEDURE_RVID; source value retained unless documented as derived.",
    "procedure_type_seq": "B3 bronze field PROCEDURE_TYPE_SEQ; source value retained unless documented as derived.",
    "procedure_seq": "B3 bronze field PROCEDURE_SEQ; source value retained unless documented as derived.",
    "active_ind": "B3 bronze field ACTIVE_IND; source value retained unless documented as derived.",
    "parent_present_ind": "B3 bronze field PARENT_PRESENT_IND; source value retained unless documented as derived.",
    "source_system_oid": "LUNA SOURCE_SYSTEM_OID as BIGINT.",
    "source_system_oid_inherited_ind": "Always true on present rows; proves namespace provenance is the parent.",
    "record_status": "Retracted when SOURCE_PRESENT_IND is false; otherwise superseded when ACTIVE_IND is false; otherwise active. Both missing flags default true, and retraction has priority. PARENT_PRESENT_IND is published separately and does not determine this row status.",
    "loaded_at": "map_elective_access_list_procedure.ADC_UPDT carried unchanged for the planned-procedure slot. No parent waiting-list load clock is joined or added; this is bronze load provenance, not procedure time or Silver refresh time.",
}

ELECTIVE_ACCESS_PROCEDURE_LIFECYCLE_FIELDS = [
]

ELECTIVE_ACCESS_PROCEDURE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_elective_access_procedure():
    # Assemble elective access procedure rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_EAL_PROCEDURE)
    retracted = ~F.coalesce(s.SOURCE_PRESENT_IND, F.lit(True))
    superseded = ~F.coalesce(s.ACTIVE_IND, F.lit(True))
    # contract v2: rename deterministic row and parent identities as keys and publish parent native identifiers with consistent types
    return s.select(
        stable_id("elective_access_procedure:luna", s.WAITING_LIST_OID,
                  s.PROCEDURE_TYPE_SEQ, s.PROCEDURE_SEQ)
         .alias("elective_access_procedure_key"),
        stable_id("elective_access:luna", s.SOURCE_SYSTEM_OID, s.WAITING_LIST_OID)
         .alias("elective_access_entry_key"),
        s.WAITING_LIST_OID.cast("bigint").alias("waiting_list_oid"),
        s.PROCEDURE_CODE.alias("procedure_code"), s.PROCEDURE_DESC.alias("procedure_desc"),
        s.PROCEDURE_CATALOG.alias("procedure_catalog"), s.PROCEDURE_RVID.alias("procedure_rvid"),
        s.PROCEDURE_TYPE_SEQ.alias("procedure_type_seq"), s.PROCEDURE_SEQ.alias("procedure_seq"),
        s.ACTIVE_IND.alias("active_ind"), s.PARENT_PRESENT_IND.alias("parent_present_ind"),
        s.SOURCE_SYSTEM_OID.cast("bigint").alias("source_system_oid"),
        s.SOURCE_SYSTEM_OID_INHERITED_IND.alias("source_system_oid_inherited_ind"),
        F.when(retracted, F.lit("retracted"))
         .when(superseded, F.lit("superseded"))
         .otherwise(F.lit("active")).alias("record_status"),
        s.ADC_UPDT.alias("loaded_at"),
    )

@materialized_view(
    name=_n("journey_reference.elective_access_procedure"),
    comment="All LUNA elective-access planned-procedure slots, including bare and tombstoned rows.",
    refresh_policy="incremental",
    column_comments=ELECTIVE_ACCESS_PROCEDURE_COLUMN_COMMENTS,
)
def elective_access_procedure():
    # Build the declared dataset: All LUNA elective-access planned-procedure slots, including
    # bare and tombstoned rows.
    return _lifecycle_source_elective_access_procedure().drop(*ELECTIVE_ACCESS_PROCEDURE_LIFECYCLE_FIELDS, *ELECTIVE_ACCESS_PROCEDURE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clinical / pathway tracking

# COMMAND ----------

# contract v2: retain the full canonical shape for internal reuse.
PATHWAY_TRACKING_SOURCE_COLUMNS = [
    "patient_event_key",
    "subject_key",
    "subject_id_system",
    "person_id",
    "identity_status",
    "encounter_id",
    "event_datetime",
    "event_end_datetime",
    "source_coding_system",
    "source_code",
    "source_display",
    "ptl_unique_id",
    "ptl_group_id",
    "archived_pathway",
    "pathway_oid",
    "referral_oid",
    "patient_oid",
    "ptl_activity_oid",
    "parent_ptl_unique_id",
    "parent_ptl_activity_oid",
    "parent_present_ind",
    "latest_activity_type",
    "latest_activity_oid",
    "latest_activity_date_future_ind",
    "days_waited",
    "specialty",
    "treatment_function",
    "treatment_function_code",
    "site",
    "site_group",
    "division",
    "lead_clinician",
    "lead_clinician_prid",
    "site_rvid",
    "source_key_status",
    "patient_spine_ind",
    "pathway_spine_ind",
    "referral_spine_ind",
    "patient_spine_link_status",
    "pathway_spine_link_status",
    "referral_spine_link_status",
    "nhs_number_valid_ind",
    "linkage_historical_fallback_ind",
    "linkage_fallback_conflict_ind",
    "person_link_status",
    "person_link_method",
    "identifier_link_status",
    "source_system_oid",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "confidentiality_code",
    "vip_ind",
    "withheld_identity_ind",
    "fact_category",
    "source_feed",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
PATHWAY_TRACKING_PUBLIC_COLUMNS = [
    'patient_event_key',
    'subject_key',
    'subject_id_system',
    'person_id',
    'encounter_id',
    'event_datetime',
    'event_end_datetime',
    'source_coding_system',
    'source_code',
    'source_display',
    'ptl_unique_id',
    'ptl_group_id',
    'archived_pathway',
    'pathway_oid',
    'referral_oid',
    'patient_oid',
    'ptl_activity_oid',
    'parent_ptl_unique_id',
    'parent_ptl_activity_oid',
    'parent_present_ind',
    'latest_activity_type',
    'latest_activity_oid',
    'latest_activity_date_future_ind',
    'days_waited',
    'specialty',
    'treatment_function',
    'treatment_function_code',
    'site',
    'site_group',
    'division',
    'lead_clinician',
    'lead_clinician_prid',
    'site_rvid',
    'source_key_status',
    'patient_spine_ind',
    'pathway_spine_ind',
    'referral_spine_ind',
    'patient_spine_link_status',
    'pathway_spine_link_status',
    'referral_spine_link_status',
    'nhs_number_valid_ind',
    'linkage_historical_fallback_ind',
    'linkage_fallback_conflict_ind',
    'person_link_status',
    'person_link_method',
    'identifier_link_status',
    'source_system_oid',
    'confidentiality_code',
    'vip_ind',
    'withheld_identity_ind',
    'fact_category',
    'source_feed',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]

PATHWAY_TRACKING_LIFECYCLE_COLUMNS = [
    'patient_event_key',
    'ptl_unique_id',
    'identity_status',
    'load_batch_id',
]

PATHWAY_TRACKING_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "B3 bronze field LATEST_ACTIVITY_DATE_CLEAN; source value retained unless documented as derived.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "ptl_unique_id": "Pathfinder PTL_UNIQUE_ID as BIGINT; primary key of this table.",
    "ptl_group_id": "B3 bronze field PTL_GROUP_ID; source value retained unless documented as derived.",
    "archived_pathway": "Source archived-pathway state retained verbatim; never used as a publication gate.",
    "pathway_oid": "B3 bronze field PATHWAY_OID; source value retained unless documented as derived.",
    "referral_oid": "B3 bronze field REFERRAL_OID; source value retained unless documented as derived.",
    "patient_oid": "B3 bronze field PATIENT_OID; source value retained unless documented as derived.",
    "ptl_activity_oid": "B3 bronze field PTL_ACTIVITY_OID; source value retained unless documented as derived.",
    "parent_ptl_unique_id": "B3 bronze field PARENT_PTL_UNIQUE_ID; source value retained unless documented as derived.",
    "parent_ptl_activity_oid": "B3 bronze field PARENT_PTL_ACTIVITY_OID; source value retained unless documented as derived.",
    "parent_present_ind": "Whether a non-null parent PTL_UNIQUE_ID exists in the pinned source snapshot.",
    "latest_activity_type": "B3 bronze field LATEST_ACTIVITY_TYPE; source value retained unless documented as derived.",
    "latest_activity_oid": "B3 bronze field LATEST_ACTIVITY_OID; source value retained unless documented as derived.",
    "latest_activity_date_future_ind": "B3 bronze field LATEST_ACTIVITY_DATE_FUTURE_IND; source value retained unless documented as derived.",
    "days_waited": "B3 bronze field DAYS_WAITED; source value retained unless documented as derived.",
    "specialty": "B3 bronze field SPECIALTY; source value retained unless documented as derived.",
    "treatment_function": "B3 bronze field TREATMENT_FUNCTION; source value retained unless documented as derived.",
    "treatment_function_code": "B3 bronze field TREATMENT_FUNCTION_CODE; source value retained unless documented as derived.",
    "site": "B3 bronze field SITE; source value retained unless documented as derived.",
    "site_group": "B3 bronze field SITE_GROUP; source value retained unless documented as derived.",
    "division": "B3 bronze field DIVISION; source value retained unless documented as derived.",
    "lead_clinician": "B3 bronze field LEAD_CLINICIAN; source value retained unless documented as derived.",
    "lead_clinician_prid": "B3 bronze field LEAD_CLINICIAN_PRID; source value retained unless documented as derived.",
    "site_rvid": "B3 bronze field SITE_RVID; source value retained unless documented as derived.",
    "source_key_status": "COMPLETE or NAMESPACE_MISSING; missing namespaces publish and are never dropped.",
    "patient_spine_ind": "B3 bronze field PATIENT_SPINE_IND; source value retained unless documented as derived.",
    "pathway_spine_ind": "B3 bronze field PATHWAY_SPINE_IND; source value retained unless documented as derived.",
    "referral_spine_ind": "B3 bronze field REFERRAL_SPINE_IND; source value retained unless documented as derived.",
    "patient_spine_link_status": "B3 bronze field PATIENT_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "pathway_spine_link_status": "B3 bronze field PATHWAY_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "referral_spine_link_status": "B3 bronze field REFERRAL_SPINE_LINK_STATUS; source value retained unless documented as derived.",
    "nhs_number_valid_ind": "B3 bronze field NHS_NUMBER_VALID_IND; source value retained unless documented as derived.",
    "linkage_historical_fallback_ind": "B3 bronze field LINKAGE_HISTORICAL_FALLBACK_IND; source value retained unless documented as derived.",
    "linkage_fallback_conflict_ind": "B3 bronze field LINKAGE_FALLBACK_CONFLICT_IND; source value retained unless documented as derived.",
    "person_link_status": "Consensus-safe published linkage status; conflicting arms never publish PERSON_ID.",
    "person_link_method": "B3 bronze field PERSON_LINK_METHOD; source value retained unless documented as derived.",
    "identifier_link_status": "B3 bronze field IDENTIFIER_LINK_STATUS; source value retained unless documented as derived.",
    "source_system_oid": "B3 bronze field SOURCE_SYSTEM_OID; source value retained unless documented as derived.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each pathway tracking record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_cancer_ptl.SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. No superseded status is emitted. ARCHIVED_PATHWAY, parent-presence and linkage flags do not set this label.",
    "record_status_effective_from": "map_cancer_ptl.LATEST_ACTIVITY_DATE_CLEAN carried directly as the record-status start. The projection does not replace it with the pipeline clock or a joined parent timestamp.",
    "record_status_effective_to": "map_cancer_ptl.SOURCE_ABSENT_DETECTED_TS when the source-presence flag is false; otherwise null. A missing absence-detection timestamp stays null, without an ADC_UPDT fallback.",
    "source_update_timestamp": "Bronze run-as-of timestamp carried from map_cancer_ptl.PIPELINE_UPDT_DT_TM for the whole-trust Pathfinder tracker. The bronze writer stamps inserted, changed, revived or newly absent rows; unchanged present rows retain their prior stamp. Run-as-of defaults to processing time but can be supplied explicitly. This is not a clinical event time or native-system update timestamp.",
    "loaded_at": "map_cancer_ptl.ADC_UPDT carried unchanged, distinct from the PIPELINE_UPDT_DT_TM used for source_update_timestamp. Parent and pathway keys are derived without adding parent load clocks; this is not the current Silver refresh time.",
}

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_pathway_tracking():
    # Assemble pathway tracking rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    return _pathway_tracking_canonical().select(*PATHWAY_TRACKING_SOURCE_COLUMNS)

@materialized_view(
    name=_n("journey_clinical.pathway_tracking"),
    comment="Whole-trust Pathfinder PTL activity rows; scope is PTL_GROUP_ID, not cancer.",
    cluster_by=["person_id", "event_datetime"],
    refresh_policy="incremental",
    column_comments=PATHWAY_TRACKING_COLUMN_COMMENTS,
)
def pathway_tracking():
    # Build the declared dataset: Whole-trust Pathfinder PTL activity rows; scope is
    # PTL_GROUP_ID, not cancer.
    return _lifecycle_source_pathway_tracking().select(*PATHWAY_TRACKING_PUBLIC_COLUMNS)

@materialized_view(
    name=_n('journey_clinical._pathway_tracking_metadata'),
    comment='Internal quality and batch metadata for clinical_pathway_tracking; same row grain as the research table. Join keys: patient_event_key, ptl_unique_id.',
    column_comments=LIFECYCLE_COLUMN_COMMENTS,
    refresh_policy="incremental",
)
def pathway_tracking_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # clinical_pathway_tracking; same row grain as the research table. Join keys:
    # patient_event_key, ptl_unique_id.
    return (_lifecycle_source_pathway_tracking()).select(*PATHWAY_TRACKING_LIFECYCLE_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / pregnancy reconciliation

# COMMAND ----------

# contract v2: align the published comment contract with pregnancy_reconciliation_key
PREGNANCY_RECONCILIATION_COLUMN_COMMENTS = {
    "pregnancy_reconciliation_key": "Deterministic SHA-256 key for the unmatched MSDS pregnancy row.",
    "unmatched_reason": "INVALID_PREGNANCY_ID or NO_MAT_PREGNANCY_MATCH.",
    "pregnancy_id_raw": "Unchanged MSDS pregnancy identifier.",
    "pregnancy_id": "MSDS pregnancy identifier after checked numeric parsing.",
    "lpid_mother": "Direct identifier published and IG-governed at serve time",
    "antenatal_appointment_date": "MSDS booking appointment date.",
    "pregnancy_first_contact_date": "MSDS first pregnancy-contact date.",
    "expected_delivery_date": "MSDS agreed EDD.",
    "last_mens_period_date": "MSDS last menstrual period date.",
    "folic_acid_supplement_cd": "MSDS folic-acid supplement code.",
    "previous_live_births": "MSDS previous live-birth count.",
    "previous_still_births": "MSDS previous stillbirth count.",
    "previous_losses_under_24_weeks": "MSDS terminations/losses under 24 weeks.",
    "previous_caesarean_sections": "MSDS previous-caesarean count.",
    "source_system_code": "MSDS source system.",
    "msds_source_version": "MSDS Delta version used for the snapshot.",
    "record_status": "Always active in this projection, including unmatched or invalid-pregnancy-identifier rows retained by the source. The label is not derived from IS_VALID, unmatched reason, pregnancy outcome or source presence.",
    "source_update_timestamp": "MSDS record-updated timestamp.",
    "loaded_at": "map_mat_pregnancy_msds_unmatched.ADC_UPDT carried unchanged for the unmatched pregnancy row. No matched pregnancy-spine clock is joined or substituted; this is bronze ingestion provenance, distinct from RECORD_UPDATED_DT and Silver refresh time.",
}

PREGNANCY_RECONCILIATION_LIFECYCLE_FIELDS = [
]

PREGNANCY_RECONCILIATION_RETIRED_COLUMNS = [
    "is_valid",
]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_pregnancy_reconciliation():
    # Assemble pregnancy reconciliation rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    return _pregnancy_reconciliation_source()

@materialized_view(
    name=_n("journey_reference.pregnancy_reconciliation"),
    comment="Unmatched MSDS pregnancy rows documenting the person-spine coverage gap.",
    refresh_policy="incremental",
    column_comments=PREGNANCY_RECONCILIATION_COLUMN_COMMENTS,
)
def pregnancy_reconciliation():
    # Build the declared dataset: Unmatched MSDS pregnancy rows documenting the person-spine
    # coverage gap.
    return _lifecycle_source_pregnancy_reconciliation().drop(*PREGNANCY_RECONCILIATION_LIFECYCLE_FIELDS, *PREGNANCY_RECONCILIATION_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / theatre attendance

# COMMAND ----------

def _theatre_parent_keys():
    # Read distinct surgical-case identifiers used to validate links from theatre child records.
    return read_source(SRC_THEATRE_CASE).select(
        F.col("SURG_CASE_ID").alias("_parent_surg_case_id")
    ).distinct()

SRC_THEATRE_ATTENDANCE = "4_prod.bronze.map_theatre_case_attendance"

THEATRE_ATTENDANCE_COLUMN_COMMENTS = {
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

THEATRE_ATTENDANCE_LIFECYCLE_FIELDS = [
]

THEATRE_ATTENDANCE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_attendance():
    # Assemble theatre attendance rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    s = read_source(SRC_THEATRE_ATTENDANCE).alias("s")
    d = s.join(_theatre_parent_keys(), s.SURG_CASE_ID == F.col("_parent_surg_case_id"), "left")
    # contract v2: rename deterministic theatre identities as keys and publish native practitioner, person, and encounter ids
    return d.select(
        stable_id("theatre_attendance:surginet", s.CASE_ATTENDANCE_ID).alias("theatre_attendance_key"),
        s.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        F.when(F.col("_parent_surg_case_id").isNotNull(), F.lit("linked"))
         .otherwise(F.lit("orphan")).alias("case_link_status"),
        s.CASE_ATTENDEE_ID.cast("bigint").alias("attendee_practitioner_id"),
        s.CASE_ATTENDEE_ID.cast("string").alias("attendee_personnel_id"),
        s.ROLE_PERF_DESCRIPTION.alias("role_description"),
        s.SIGNING_ATTENDEE_IND.alias("signing_attendee_ind"),
        s.IN_DT_TM.alias("in_datetime"), s.IN_DT_TM_QUALITY.alias("in_datetime_quality"),
        s.OUT_DT_TM.alias("out_datetime"), s.OUT_DT_TM_QUALITY.alias("out_datetime_quality"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.SURG_AREA_DESCRIPTION.alias("surgical_area_desc"), s.ACTIVE_IND.alias("active_ind"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"), F.lit(SRC_THEATRE_ATTENDANCE).alias("_source_table"),
        s.CASE_ATTENDANCE_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.theatre_attendance"),
    comment="SurgiNet case attendance; parent-orphan rows remain visible and flagged.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=THEATRE_ATTENDANCE_COLUMN_COMMENTS,
)
def theatre_attendance():
    # Build the declared dataset: SurgiNet case attendance; parent-orphan rows remain visible
    # and flagged.
    return _lifecycle_source_theatre_attendance().drop(*THEATRE_ATTENDANCE_LIFECYCLE_FIELDS, *THEATRE_ATTENDANCE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / theatre case milestone

# COMMAND ----------

SRC_THEATRE_TIMES = "4_prod.bronze.map_theatre_case_times"

THEATRE_CASE_MILESTONE_COLUMN_COMMENTS = {
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

THEATRE_CASE_MILESTONE_LIFECYCLE_FIELDS = [
]

THEATRE_CASE_MILESTONE_RETIRED_COLUMNS = [

]

# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_theatre_case_milestone():
    # Assemble theatre case milestone rows with lifecycle and source evidence for the public
    # product and its internal metadata.
    s = read_source(SRC_THEATRE_TIMES).alias("s")
    d = s.join(_theatre_parent_keys(), s.SURG_CASE_ID == F.col("_parent_surg_case_id"), "left")
    # contract v2: rename deterministic theatre identities as keys and publish native person and encounter ids
    return d.select(
        stable_id("theatre_milestone:surginet", s.CASE_TIMES_ID).alias("theatre_case_milestone_key"),
        s.SURG_CASE_ID.cast("string").alias("theatre_case_key"),
        F.when(F.col("_parent_surg_case_id").isNotNull(), F.lit("linked"))
         .otherwise(F.lit("orphan")).alias("case_link_status"),
        s.TASK_ASSAY_CD.cast("string").alias("task_assay_code"),
        s.TASK_ASSAY_DESCRIPTION.alias("task_assay_description"),
        s.STAGE_DESCRIPTION.alias("stage_description"), s.CASE_TIME_DT_TM.alias("case_time_datetime"),
        s.CASE_TIME_DT_TM_QUALITY.alias("case_time_quality"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        s.ACTIVE_IND.alias("active_ind"), s.ADC_UPDT.alias("loaded_at"), F.lit("surginet").alias("_source_system"),
        F.lit(SRC_THEATRE_TIMES).alias("_source_table"),
        s.CASE_TIMES_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.theatre_case_milestone"),
    comment="SurgiNet case-time milestones; source-case-time meaning is dead upstream.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=THEATRE_CASE_MILESTONE_COLUMN_COMMENTS,
)
def theatre_case_milestone():
    # Build the declared dataset: SurgiNet case-time milestones; source-case-time meaning is
    # dead upstream.
    return _lifecycle_source_theatre_case_milestone().drop(*THEATRE_CASE_MILESTONE_LIFECYCLE_FIELDS, *THEATRE_CASE_MILESTONE_RETIRED_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference / theatre implant

# COMMAND ----------

SRC_THEATRE_IMPLANT = "4_prod.bronze.map_theatre_implant_log"

THEATRE_IMPLANT_COLUMN_COMMENTS = {
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
        s.DOC_TYPE_DESCRIPTION.alias("document_type_desc"),
        s.ADC_UPDT.alias("loaded_at"),
        F.lit("surginet").alias("_source_system"), F.lit(SRC_THEATRE_IMPLANT).alias("_source_table"),
        s.IMPLANT_LOG_ST_ID.cast("string").alias("_source_row_id"),
    )

@materialized_view(
    name=_n("journey_reference.theatre_implant"),
    comment="SurgiNet theatre implant log; serial/lot/batch identifiers are IG-sensitive.",
    cluster_by=["person_id"], refresh_policy="incremental",
    column_comments=THEATRE_IMPLANT_COLUMN_COMMENTS,
)
def theatre_implant():
    # Build the declared dataset: SurgiNet theatre implant log; serial/lot/batch identifiers are
    # IG-sensitive.
    return _lifecycle_source_theatre_implant().drop(*THEATRE_IMPLANT_LIFECYCLE_FIELDS, *THEATRE_IMPLANT_RETIRED_COLUMNS)

