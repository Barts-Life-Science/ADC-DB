# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Specialist Care
# MAGIC Cancer, critical care, maternity, neonatal care and registry children.
# MAGIC
# MAGIC Numbers indicate reading order; Lakeflow uses dataset dependencies to schedule work.
# MAGIC Mandatory rules use `dp.expect_all_or_drop`; advisory rules use `dp.expect_all`.
# MAGIC Repairs and nulling stay in the projection. Public names and identifiers are preserved.
# MAGIC Helpers are in `gold_journey_shared.py`, an ordinary Python file.

# COMMAND ----------

from gold_journey_shared import (
    _n,
    _qc,
    _s3_flatten_gold_public,
    _src,
    _with_comments,
    _with_parent_status,
    dp,
)

# COMMAND ----------

# ==== journey_clinical.baby_delivery ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_BABY_DELIVERY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    "CASE WHEN UPPER(TRIM(CAST(`source_code` AS STRING))) = '0' THEN NULL ELSE `source_code` END AS `source_code`",
    '`source_display` AS `source_display`',
    '`baby_person_id` AS `baby_person_id`',
    '`birth_order` AS `birth_order`',
    '`pregnancy_outcome` AS `pregnancy_outcome`',
    "CASE WHEN UPPER(TRIM(CAST(`delivery_method_code` AS STRING))) = '0' THEN NULL ELSE `delivery_method_code` END AS `delivery_method_code`",
    '`phenotypic_sex` AS `phenotypic_sex`',
    '`gestation_length_birth` AS `gestation_length_birth`',
    '`birthweight` AS `birthweight`',
    '`apgar_5` AS `apgar_5`',
    '`baby_death_datetime` AS `baby_death_datetime`',
    '`first_feed_datetime` AS `first_feed_datetime`',
    '`first_feed_code` AS `first_feed_code`',
    '`first_feed_breast_milk_status` AS `first_feed_breast_milk_status`',
    '`breast_milk_status_discharge` AS `breast_milk_status_discharge`',
    '`skin_to_skin_ind` AS `skin_to_skin_ind`',
    '`baby_discharge_datetime` AS `baby_discharge_datetime`',
    '`delivery_org_site` AS `delivery_org_site`',
    '`birth_setting` AS `birth_setting`',
    '`birth_place_type` AS `birth_place_type`',
    '`midwifery_place_type` AS `midwifery_place_type`',
    '`labour_delivery_id` AS `labour_delivery_id`',
    '`labour_delivery_key` AS `labour_delivery_key`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`journey_pregnancy_key` AS `journey_pregnancy_key`',
    '`source_link_status` AS `source_link_status`',
    '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
    '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_baby_delivery_metadata; source history stays on the main research table.
CLINICAL_BABY_DELIVERY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 99,299 rows of 99,299 that
    # are current and attributable; identity_status = 'resolved' keeps the 98,912 rows of
    # 99,299 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_BABY_DELIVERY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 387 of 99,299 at the profile.
    "gold.clinical.baby_delivery.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 99,299 at the profile.
    "gold.clinical.baby_delivery.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_BABY_DELIVERY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_row_hash": "MSDS ROW_HASH supplied by the source; evidence for the exception where no durable baby-delivery row id exists.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Unique MAT person identifier for the mother.",
    "person_id": "Maternal Millennium person identifier, exposed as BIGINT. Silver prefers Person_ID from the linked pregnancy spine, falling back to the MSDS adapter PERSON_ID only when its source-row group has one distinct person identifier. Null if neither yields an identifier. This is not the baby's identifier; baby_person_id is separate.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each baby delivery record. It is derived from bronze field `PERSONBIRTHDATETIMEBABY_CLEAN` in `4_prod.bronze.map_maternity_baby_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each baby delivery record. It is derived from bronze field `DISCHARGEDATETIMEBABYHSP_CLEAN` in `4_prod.bronze.map_maternity_baby_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each baby delivery record. It is derived from bronze field `DELIVERYMETHODCODE` in `4_prod.bronze.map_maternity_baby_delivery`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each baby delivery record. It is derived from bronze field `DELIVERYMETHODCODE` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "baby_person_id": "Millennium identifier of the baby, carried as STRING from bronze BABY_PERSON_ID. Bronze requires one normalized baby NHS value matching exactly one active alias-type-18 person. Null when that unique linkage is unavailable; distinct from maternal person_id.",
    "birth_order": "Value describing birth order for the baby delivery record. It is derived from bronze field `BIRTHORDERMATERNITYSUS` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "pregnancy_outcome": "Value describing pregnancy outcome for the baby delivery record. It is derived from bronze field `PREGOUTCOME` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "delivery_method_code": "Source-system code representing delivery method for the baby delivery record. It is derived from bronze field `DELIVERYMETHODCODE` in `4_prod.bronze.map_maternity_baby_delivery`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "phenotypic_sex": "Value describing phenotypic sex for the baby delivery record. It is derived from bronze field `PERSONPHENSEX` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "gestation_length_birth": "Value describing gestation length birth for the baby delivery record. It is derived from bronze field `GESTATIONLENGTHBIRTH` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "birthweight": "Value describing birthweight for the baby delivery record. It is carried from bronze field `BIRTHWEIGHT` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "apgar_5": "Value describing apgar 5 for the baby delivery record. It is derived from bronze field `APGARSCORE5` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "baby_death_datetime": "Date and time associated with baby death for the baby delivery record. It is derived from bronze field `PERSONDEATHDATETIMEBABY_CLEAN` in `4_prod.bronze.map_maternity_baby_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "first_feed_datetime": "Date and time associated with first feed for the baby delivery record. It is derived from bronze field `BABYFIRSTFEEDDATETIME_CLEAN` in `4_prod.bronze.map_maternity_baby_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "first_feed_code": "Source-system code representing first feed for the baby delivery record. It is derived from bronze field `BABYFIRSTFEEDINDCODE` in `4_prod.bronze.map_maternity_baby_delivery`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "first_feed_breast_milk_status": "Processing or clinical status of first feed breast milk for the baby delivery record. It is derived from bronze field `BABYFIRSTFEEDBREASTMILKSTATUS` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "breast_milk_status_discharge": "Value describing breast milk status discharge for the baby delivery record. It is derived from bronze field `BABYBREASTMILKSTATUSDISCHARGE` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "skin_to_skin_ind": "Indicator of whether skin to skin applies to the baby delivery record. It is derived from bronze field `SKINTOSKINCONTACT1HOURIND` in `4_prod.bronze.map_maternity_baby_delivery`. Null means the source did not state the indicator and must not be interpreted as false.",
    "baby_discharge_datetime": "Date and time associated with baby discharge for the baby delivery record. It is derived from bronze field `DISCHARGEDATETIMEBABYHSP_CLEAN` in `4_prod.bronze.map_maternity_baby_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "delivery_org_site": "Value describing delivery org site for the baby delivery record. It is derived from bronze field `ORGSITEIDACTUALDELIVERY` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "birth_setting": "Value describing birth setting for the baby delivery record. It is derived from bronze field `SETTINGPLACEBIRTH` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "birth_place_type": "Value describing birth place type for the baby delivery record. It is derived from bronze field `PLACETYPEACTUALDELIVERY` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "midwifery_place_type": "Value describing midwifery place type for the baby delivery record. It is derived from bronze field `PLACETYPEACTUALMIDWIFERY` in `4_prod.bronze.map_maternity_baby_delivery`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "labour_delivery_id": "MSDS LABOURDELIVERYID; native linkage to the labour-delivery record.",
    "labour_delivery_key": "Deterministic SHA-256 key for the linked labour delivery.",
    "pregnancy_id": "Source-system identifier for the pregnancy associated with each baby delivery record. It is derived from bronze field `PREGNANCY_ID_PARSED` in `4_prod.bronze.map_maternity_baby_delivery`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "journey_pregnancy_key": "Deterministic SHA-256 key for the pregnancy journey.",
    "source_link_status": "Processing or clinical status of source link for the baby delivery record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_maternity_baby_delivery`, selected as the provenance anchor from 2 recorded bronze source fields. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "pregnancy_orphan_ind": "Unique pregnancy episode identifier.",
    "spine_person_mismatch_ind": "Unique MAT person identifier for the mother.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each baby delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Literal active for every retained MSDS baby-delivery row. This is not derived from source presence, pregnancy outcome or infant survival; it does not establish that a source record has never been superseded or deleted.",
    "record_status_effective_from": "map_maternity_baby_delivery.PERSONBIRTHDATETIMEBABY_CLEAN carried unchanged as the history start. This is the baby's recorded birth time, not an independently observed row-status change; a missing source value remains null.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the baby-delivery projection. The pipeline supplies no row-status end boundary and does not substitute discharge, death or ingestion timestamps.",
    "source_update_timestamp": "map_maternity_baby_delivery.RECORD_UPDATED_DT carried unchanged from the deduplicated delivery source row. This is not the pregnancy-spine update clock or Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_maternity_baby_delivery.ADC_UPDT carried unchanged from the deduplicated delivery source row. Identity fan-out is collapsed without aggregating this clock; the joined pregnancy spine does not contribute a load timestamp. This is not the Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_baby_delivery"),
    comment="Internal quality-controlled twin of clinical_baby_delivery: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_BABY_DELIVERY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_BABY_DELIVERY_ADVISORY_RULES)
def _gold_qc_clinical_baby_delivery():
    """Quality-controlled twin of journey_clinical.baby_delivery."""
    df = _qc("clinical_baby_delivery", CLINICAL_BABY_DELIVERY_SELECT)
    return _with_comments(df, CLINICAL_BABY_DELIVERY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.baby_delivery"),
    comment=(
        "One MSDS baby-delivery row with mother identity recovered through the pregnancy "
        "spine. Gold QC twin of the silver product: 3 columns are repaired or nulled, 1 "
        "rule(s) drop rows, 2 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_baby_delivery():
    """Contract-v2 public twin of clinical_baby_delivery; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_baby_delivery")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_row_hash` AS `source_row_hash`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`baby_person_id` AS `baby_person_id`',
        '`birth_order` AS `birth_order`',
        '`pregnancy_outcome` AS `pregnancy_outcome`',
        '`delivery_method_code` AS `delivery_method_code`',
        '`phenotypic_sex` AS `phenotypic_sex`',
        '`gestation_length_birth` AS `gestation_length_birth`',
        '`birthweight` AS `birthweight`',
        '`apgar_5` AS `apgar_5`',
        '`baby_death_datetime` AS `baby_death_datetime`',
        '`first_feed_datetime` AS `first_feed_datetime`',
        '`first_feed_code` AS `first_feed_code`',
        '`first_feed_breast_milk_status` AS `first_feed_breast_milk_status`',
        '`breast_milk_status_discharge` AS `breast_milk_status_discharge`',
        '`skin_to_skin_ind` AS `skin_to_skin_ind`',
        '`baby_discharge_datetime` AS `baby_discharge_datetime`',
        '`delivery_org_site` AS `delivery_org_site`',
        '`birth_setting` AS `birth_setting`',
        '`birth_place_type` AS `birth_place_type`',
        '`midwifery_place_type` AS `midwifery_place_type`',
        '`labour_delivery_id` AS `labour_delivery_id`',
        '`labour_delivery_key` AS `labour_delivery_key`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`journey_pregnancy_key` AS `journey_pregnancy_key`',
        '`source_link_status` AS `source_link_status`',
        '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
        '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_BABY_DELIVERY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.cancer_treatment ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CANCER_TREATMENT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`treatment_key` AS `treatment_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`drug_code` AS `drug_code`',
    '`treatment_plan` AS `treatment_plan`',
    '`regimen_name` AS `regimen_name`',
    '`indication` AS `indication`',
    '`record_type` AS `record_type`',
    'CASE WHEN `dose_value` < 0 THEN NULL ELSE `dose_value` END AS `dose_value`',
    '`dose_total` AS `dose_total`',
    "CASE WHEN UPPER(TRIM(CAST(`dose_unit_code` AS STRING))) = '0' THEN NULL ELSE `dose_unit_code` END AS `dose_unit_code`",
    '`route_code` AS `route_code`',
    'CASE WHEN CAST(`start_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `start_date` END AS `start_date`',
    'CASE WHEN `start_date` IS NOT NULL AND `end_date` IS NOT NULL AND `start_date` > `end_date` THEN NULL ELSE `end_date` END AS `end_date`',
    '`final_treatment_date` AS `final_treatment_date`',
    '`course_finished` AS `course_finished`',
    '`planned_cycles` AS `planned_cycles`',
    '`default_cycles` AS `default_cycles`',
    '`chemo_radiation` AS `chemo_radiation`',
    '`procurement_opcs_code` AS `procurement_opcs_code`',
    '`delivery_opcs_code` AS `delivery_opcs_code`',
    '`drug_similarity` AS `drug_similarity`',
    '`iqemo_course_id` AS `iqemo_course_id`',
    '`aria_rx_key` AS `aria_rx_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_cancer_treatment_metadata; source history stays on the main research table.
CLINICAL_CANCER_TREATMENT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 2,789,249 rows of 2,789,249
    # that are current and attributable; identity_status = 'resolved' keeps the 2,716,669
    # rows of 2,789,249 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_CANCER_TREATMENT_ADVISORY_RULES = {
    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 5,115 of 2,789,249
    # rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.end_date.future_owner":
        "NOT COALESCE((CAST(`end_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 5,115 of 2,789,249
    # rows (0.183%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.event_end_datetime.future_owner":
        "NOT COALESCE((CAST(`event_end_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counted rather than nulled because a treatment course that is still running has a
    # planned end date, which is in the future by construction. Seen on 6,921 of 2,789,249
    # rows (0.248%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.final_treatment_date.future_owner":
        "NOT COALESCE((CAST(`final_treatment_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 72,580 of 2,789,249 at the profile.
    "gold.clinical.cancer_treatment.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 2,789,249 at the profile.
    "gold.clinical.cancer_treatment.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 141 of 2,789,249
    # rows (0.00506%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 6,565 of 2,789,249 rows (0.235%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 350 of 2,789,249 rows (0.0125%) when profiled on 2026-08-24.
    "gold.clinical.cancer_treatment.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CANCER_TREATMENT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "treatment_key": "SACT TREATMENT_KEY; primary key of this table.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Treatment start timestamp.",
    "event_end_datetime": "Treatment end or final-treatment timestamp.",
    "source_coding_system": "SACT drug-token coding system.",
    "source_code": "Normalized drug token with agent-name display fallback.",
    "source_display": "Source agent name.",
    "drug_code": "Source and mapped drug codings.",
    "treatment_plan": "Source treatment plan.",
    "regimen_name": "Treatment regimen name.",
    "indication": "Clinical indication text held against the iQemo regimen definition for which the regimen is used.",
    "record_type": "ARIA/iQemo linkage class.",
    "dose_value": "Constituent dose value.",
    "dose_total": "Total planned or delivered dose.",
    "dose_unit_code": "ARIA coded value for the dosage unit used for administration of the agent.",
    "route_code": "Administration route code.",
    "start_date": "Source start date.",
    "end_date": "Source end date.",
    "final_treatment_date": "Date of the final treatment administered within the iQemo chemotherapy course.",
    "course_finished": "Source indication that the treatment course has finished, from `CourseFinished` cast to boolean.",
    "planned_cycles": "Planned cycle count.",
    "default_cycles": "Default regimen cycle count.",
    "chemo_radiation": "Concurrent chemo-radiation indicator.",
    "procurement_opcs_code": "OPCS procurement code.",
    "delivery_opcs_code": "OPCS delivery code.",
    "drug_similarity": "Drug-link similarity score.",
    "iqemo_course_id": "iQemo course identifier.",
    "aria_rx_key": "ARIA prescription key.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "record_status": "Superseded when map_cancer_treatment.SOURCE_PRESENT_IND cast to BOOLEAN is false, otherwise active; missing presence defaults true. No retracted status is emitted. CourseFinished, record_type and treatment end dates do not set this row-history label.",
    "record_status_effective_from": "Start date of the treatment record, taken from the ARIA administration start date or the iQemo chemotherapy course start date.",
    "record_status_effective_to": "map_cancer_treatment.ADC_UPDT when source presence is false; otherwise null. Neither EndDate nor FinalTreatmentDate is substituted, and a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Timestamp of the last update to the underlying ARIA or iQemo source record as recorded in the raw layer.",
    "loaded_at": "map_cancer_treatment.ADC_UPDT carried unchanged through usable-code filtering and QC/public projections. No joined parent or QC clock is added. This differs from SRC_ADC_UPDT used for source_update_timestamp and from the current Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_cancer_treatment"),
    comment="Internal quality-controlled twin of clinical_cancer_treatment: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CANCER_TREATMENT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CANCER_TREATMENT_ADVISORY_RULES)
def _gold_qc_clinical_cancer_treatment():
    """Quality-controlled twin of journey_clinical.cancer_treatment."""
    df = _qc("clinical_cancer_treatment", CLINICAL_CANCER_TREATMENT_SELECT)
    return _with_comments(df, CLINICAL_CANCER_TREATMENT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.cancer_treatment"),
    comment=(
        "One SACT constituent-drug treatment fact, kept separate from encounter-bound "
        "Millennium medication administration. Gold QC twin of the silver product: 8 columns "
        "are repaired or nulled, 1 rule(s) drop rows, 8 check(s) are advisory. Each rule "
        "states its reason in the pipeline notebook, and Lakeflow expectation metrics report "
        "what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_cancer_treatment():
    """Contract-v2 public twin of clinical_cancer_treatment; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_cancer_treatment")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`treatment_key` AS `treatment_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`__gold_json_drug_code` AS `drug_code`',
        '`treatment_plan` AS `treatment_plan`',
        '`regimen_name` AS `regimen_name`',
        '`indication` AS `indication`',
        '`record_type` AS `record_type`',
        '`dose_value` AS `dose_value`',
        '`dose_total` AS `dose_total`',
        '`dose_unit_code` AS `dose_unit_code`',
        '`route_code` AS `route_code`',
        '`start_date` AS `start_date`',
        '`end_date` AS `end_date`',
        '`final_treatment_date` AS `final_treatment_date`',
        '`course_finished` AS `course_finished`',
        '`planned_cycles` AS `planned_cycles`',
        '`default_cycles` AS `default_cycles`',
        '`chemo_radiation` AS `chemo_radiation`',
        '`procurement_opcs_code` AS `procurement_opcs_code`',
        '`delivery_opcs_code` AS `delivery_opcs_code`',
        '`drug_similarity` AS `drug_similarity`',
        '`iqemo_course_id` AS `iqemo_course_id`',
        '`aria_rx_key` AS `aria_rx_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _s3_flatten_gold_public(_with_comments(df, CLINICAL_CANCER_TREATMENT_COLUMN_COMMENTS), "clinical_cancer_treatment")

# COMMAND ----------

# ==== journey_clinical.cancer_treatment_cycle ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CANCER_TREATMENT_CYCLE_SELECT = [
    '`cancer_treatment_cycle_key` AS `cancer_treatment_cycle_key`',
    '`iqemo_course_id` AS `iqemo_course_id`',
    '`cycle_sequence_id` AS `cycle_sequence_id`',
    '`regimen_cycle_id` AS `regimen_cycle_id`',
    '`cycle_code` AS `cycle_code`',
    '`prescribed_datetime` AS `prescribed_datetime`',
    '`pharmacy_confirmed_datetime` AS `pharmacy_confirmed_datetime`',
    '`cycle_start_datetime` AS `cycle_start_datetime`',
    '`cancellation_datetime` AS `cancellation_datetime`',
    "CASE WHEN UPPER(TRIM(CAST(`cycle_status_code` AS STRING))) = '0' THEN NULL ELSE `cycle_status_code` END AS `cycle_status_code`",
    '`treatment_response_id` AS `treatment_response_id`',
    '`line_of_treatment` AS `line_of_treatment`',
    '`regimen_number` AS `regimen_number`',
    '`course_link_status` AS `course_link_status`',
    '`outcome_comments` AS `outcome_comments`',
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`chemotherapy_course_id` AS `chemotherapy_course_id`',
    '`treatment_cycle_id` AS `treatment_cycle_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_cancer_treatment_cycle_metadata; source history stays on the main research table.
CLINICAL_CANCER_TREATMENT_CYCLE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 294,917 rows of 294,917 that
    # are current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

CLINICAL_CANCER_TREATMENT_CYCLE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 294,917 at the profile.
    "gold.clinical.cancer_treatment_cycle.record_status.default_view_active":
        "record_status = 'active'",
}

CLINICAL_CANCER_TREATMENT_CYCLE_COLUMN_COMMENTS = {
    "cancer_treatment_cycle_key": "Deterministic SHA-256 key for the iQemo course-cycle pair.",
    "iqemo_course_id": "iQemo chemotherapy course identifier.",
    "cycle_sequence_id": "Verbatim treatment-cycle sequence token.",
    "regimen_cycle_id": "Regimen cycle identifier.",
    "cycle_code": "Source cycle code.",
    "prescribed_datetime": "Sentinel-cleaned prescribed timestamp.",
    "pharmacy_confirmed_datetime": "Sentinel-cleaned pharmacy-confirmed timestamp.",
    "cycle_start_datetime": "Sentinel-cleaned cycle start timestamp.",
    "cancellation_datetime": "Sentinel-cleaned cancellation timestamp.",
    "cycle_status_code": "Source cycle status code.",
    "treatment_response_id": "Source treatment-response identifier from `TreatmentResponseID`, retained as a string on the treatment cycle.",
    "line_of_treatment": "Coded lookup identifier for the line of treatment represented by the parent chemotherapy course.",
    "regimen_number": "Sequential number identifying the regimen within the parent chemotherapy course.",
    "course_link_status": "Parent course-link status.",
    "outcome_comments": "Source outcome comments.",
    "source_table": "Registered source table.",
    "source_row_id": "Composite source row identifier.",
    "chemotherapy_course_id": "iQemo CHEMOTHERAPY_COURSE_ID as BIGINT; first component of the primary key.",
    "treatment_cycle_id": "iQemo TREATMENT_CYCLE_ID as BIGINT; second component of the primary key.",
    "record_status": "Always active in the current map_cancer_treatment_cycle projection. CycleStatus and CANCELLATION_DATE_CLEAN remain separate clinical fields and do not change this literal label; no superseded or retracted status is emitted.",
    "loaded_at": "map_cancer_treatment_cycle.PIPELINE_UPDT_DT_TM carried unchanged. No treatment-parent clock is joined, and ADC_UPDT, prescription dates, cancellation time and the current Silver refresh time are not substituted.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_cancer_treatment_cycle"),
    comment="Internal quality-controlled twin of clinical_cancer_treatment_cycle: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CANCER_TREATMENT_CYCLE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CANCER_TREATMENT_CYCLE_ADVISORY_RULES)
def _gold_qc_clinical_cancer_treatment_cycle():
    """Quality-controlled twin of journey_clinical.cancer_treatment_cycle."""
    df = _qc("clinical_cancer_treatment_cycle", CLINICAL_CANCER_TREATMENT_CYCLE_SELECT)
    return _with_comments(df, CLINICAL_CANCER_TREATMENT_CYCLE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.cancer_treatment_cycle"),
    comment=(
        "One iQemo chemotherapy cycle child reference at composite course/cycle grain. Gold "
        "QC twin of the silver product: 1 columns are repaired or nulled, 1 rule(s) drop "
        "rows, 1 check(s) are advisory. Each rule states its reason in the pipeline notebook, "
        "and Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_cancer_treatment_cycle():
    """Contract-v2 public twin of clinical_cancer_treatment_cycle; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_cancer_treatment_cycle")).selectExpr(
        '`cancer_treatment_cycle_key` AS `cancer_treatment_cycle_key`',
        '`iqemo_course_id` AS `iqemo_course_id`',
        '`cycle_sequence_id` AS `cycle_sequence_id`',
        '`regimen_cycle_id` AS `regimen_cycle_id`',
        '`cycle_code` AS `cycle_code`',
        '`prescribed_datetime` AS `prescribed_datetime`',
        '`pharmacy_confirmed_datetime` AS `pharmacy_confirmed_datetime`',
        '`cycle_start_datetime` AS `cycle_start_datetime`',
        '`cancellation_datetime` AS `cancellation_datetime`',
        '`cycle_status_code` AS `cycle_status_code`',
        '`treatment_response_id` AS `treatment_response_id`',
        '`line_of_treatment` AS `line_of_treatment`',
        '`regimen_number` AS `regimen_number`',
        '`course_link_status` AS `course_link_status`',
        '`outcome_comments` AS `outcome_comments`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`chemotherapy_course_id` AS `chemotherapy_course_id`',
        '`treatment_cycle_id` AS `treatment_cycle_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_CANCER_TREATMENT_CYCLE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.critical_care_activity ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`period_link_status` AS `period_link_status`',
    '`period_business_key` AS `period_business_key`',
    '`parent_period_id` AS `parent_period_id`',
    '`cds_apc_id` AS `cds_apc_id`',
    '`cc_type` AS `cc_type`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_critical_care_activity_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_ACTIVITY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 336,263 rows of 341,038 that
    # are current and attributable; identity_status = 'resolved' keeps the 331,191 rows of
    # 341,038 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_CRITICAL_CARE_ACTIVITY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 9,847 of 341,038 at the profile.
    "gold.clinical.critical_care_activity.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 4,775 of 341,038 at the profile.
    "gold.clinical.critical_care_activity.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 7,892 of 341,038 rows (2.31%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_activity.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CRITICAL_CARE_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_row_hash": "CCMDS ROW_HASH supplied by the source; evidence for the exception where no durable native activity id exists.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each critical care activity record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_critical_care_activity`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each critical care activity record. It is derived from bronze field `Activity_Date_CLEAN` in `4_prod.bronze.map_critical_care_activity`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each critical care activity record. It is derived from bronze field `Activity_Code` in `4_prod.bronze.map_critical_care_activity`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "period_link_status": "Processing or clinical status of period link for the critical care activity record. It is carried from bronze field `PERIOD_LINK_STATUS` in `4_prod.bronze.map_critical_care_activity`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "period_business_key": "CCMDS parent period business key.",
    "parent_period_id": "Internal surrogate identifier uniquely identifying each critical care period record in this table.",
    "cds_apc_id": "Source-system identifier for the CDS apc associated with each critical care activity record. It is carried from bronze field `CDS_APC_ID` in `4_prod.bronze.map_critical_care_activity`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "cc_type": "Value describing cc type for the critical care activity record. It is carried from bronze field `CC_Type` in `4_prod.bronze.map_critical_care_activity`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_duplicate_count": "Number of source duplicate items associated with the critical care activity record. It is carried from bronze field `SOURCE_DUPLICATE_COUNT` in `4_prod.bronze.map_critical_care_activity`. The unit is stated in the description; null means the measure could not be derived and is not equivalent to zero.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each critical care activity record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_critical_care_activity.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted. Parent-period link status, care type and activity code do not determine this row-status label.",
    "record_status_effective_from": "map_critical_care_activity.Activity_Date_CLEAN carried unchanged. This is an activity-date proxy, not an independently observed source-status transition; no parent-period start or ingestion-time fallback is applied.",
    "record_status_effective_to": "map_critical_care_activity.ADC_UPDT only when the activity row is retracted; otherwise null. This is an ingestion-time end proxy, not a parent-period discharge or clinical activity end; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_critical_care_activity`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "map_critical_care_activity.ADC_UPDT carried unchanged through code filtering and source/public projections. No parent-period timestamp is joined or added. It is the contributing bronze load clock, distinct from PIPELINE_UPDT_DT_TM published as source_update_timestamp, not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_activity"),
    comment="Internal quality-controlled twin of clinical_critical_care_activity: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ACTIVITY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_ACTIVITY_ADVISORY_RULES)
def _gold_qc_clinical_critical_care_activity():
    """Quality-controlled twin of journey_clinical.critical_care_activity."""
    df = _qc(
        "clinical_critical_care_activity",
        CLINICAL_CRITICAL_CARE_ACTIVITY_SELECT,
        date_flags=["event_before_birth"],
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_ACTIVITY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.critical_care_activity"),
    comment=(
        "One admitted CCMDS critical-care activity with parent-link evidence. Gold QC twin of "
        "the silver product: 1 columns are repaired or nulled, 1 rule(s) drop rows, 3 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_critical_care_activity():
    """Contract-v2 public twin of clinical_critical_care_activity; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_activity")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_row_hash` AS `source_row_hash`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`period_link_status` AS `period_link_status`',
        '`period_business_key` AS `period_business_key`',
        '`parent_period_id` AS `parent_period_id`',
        '`cds_apc_id` AS `cds_apc_id`',
        '`cc_type` AS `cc_type`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_ACTIVITY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.critical_care_admission ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_ADMISSION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_pat_id` AS `source_pat_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`admission_key` AS `admission_key`',
    '`source_unit` AS `source_unit`',
    '`unit_raw` AS `unit_raw`',
    '`source_site` AS `source_site`',
    '`hospital_admission_datetime` AS `hospital_admission_datetime`',
    '`unit_admission_datetime` AS `unit_admission_datetime`',
    '`unit_discharge_datetime` AS `unit_discharge_datetime`',
    '`hospital_discharge_datetime` AS `hospital_discharge_datetime`',
    '`admission_diagnosis_code` AS `admission_diagnosis_code`',
    '`max_organ_support` AS `max_organ_support`',
    '`cause_of_death` AS `cause_of_death`',
    '`date_of_death` AS `date_of_death`',
    '`source_updated_at` AS `source_updated_at`',
    '`person_link_status` AS `person_link_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_critical_care_admission_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_ADMISSION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 141,391 rows of 141,391 that
    # are current and attributable; identity_status = 'resolved' keeps the 135,937 rows of
    # 141,391 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_CRITICAL_CARE_ADMISSION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 5,454 of 141,391 at the profile.
    "gold.clinical.critical_care_admission.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 141,391 at the profile.
    "gold.clinical.critical_care_admission.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12 of 141,391 rows (0.00849%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_admission.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 141,391 rows (0.000707%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_admission.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CRITICAL_CARE_ADMISSION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_pat_id": "Native Medicus patient/admission-row id from SOURCE_PAT_ID, exposed as BIGINT. Together with source_unit it identifies the source admission row; not a Millennium person identifier.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each critical care admission record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_critical_care_admission`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each critical care admission record. It is derived from bronze field `date_unit_adm_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each critical care admission record. It is derived from bronze field `date_unit_discharge_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "admission_key": "Composite admission linkage string built in bronze by joining SOURCE_UNIT and SOURCE_PAT_ID with a colon. The native components are published separately; the existing linkage value is retained.",
    "source_unit": "Value describing source unit for the critical care admission record. It is carried from bronze field `SOURCE_UNIT` in `4_prod.bronze.map_critical_care_admission`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "unit_raw": "Value describing unit raw for the critical care admission record. It is carried from bronze field `UNIT_RAW` in `4_prod.bronze.map_critical_care_admission`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_site": "Value describing source site for the critical care admission record. It is carried from bronze field `SOURCE_SITE` in `4_prod.bronze.map_critical_care_admission`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "hospital_admission_datetime": "Date and time associated with hospital admission for the critical care admission record. It is derived from bronze field `date_hospital_adm_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "unit_admission_datetime": "Date and time associated with unit admission for the critical care admission record. It is derived from bronze field `date_unit_adm_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "unit_discharge_datetime": "Date and time associated with unit discharge for the critical care admission record. It is derived from bronze field `date_unit_discharge_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "hospital_discharge_datetime": "Date and time associated with hospital discharge for the critical care admission record. It is derived from bronze field `date_hospital_discharge_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "admission_diagnosis_code": "Source-system code representing admission diagnosis for the critical care admission record. It is derived from bronze field `dgn_adm1_code` in `4_prod.bronze.map_critical_care_admission`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "max_organ_support": "Numeric measure of max organ support for the critical care admission record. It is derived from bronze field `maxorgansupp` in `4_prod.bronze.map_critical_care_admission`. Units and source sentinel values follow the named source field unless the pipeline explicitly converts them; null is not equivalent to zero.",
    "cause_of_death": "Value describing cause of death for the critical care admission record. It is carried from bronze field `cause_of_death` in `4_prod.bronze.map_critical_care_admission`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "date_of_death": "Date or time value for date of death on the critical care admission record. It is derived from bronze field `date_of_death_CLEAN` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_updated_at": "Date or time value for source updated at on the critical care admission record. It is derived from bronze field `updated_at` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "person_link_status": "Processing or clinical status of person link for the critical care admission record. It is carried from bronze field `PERSON_LINK_STATUS` in `4_prod.bronze.map_critical_care_admission`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each critical care admission record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_critical_care_admission.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted, and hospital/unit discharge or death dates do not determine this label.",
    "record_status_effective_from": "map_critical_care_admission.date_unit_adm_CLEAN carried unchanged as the history start. This unit-admission time is a proxy, not an independently observed status transition; no hospital-admission or ingestion fallback is applied.",
    "record_status_effective_to": "map_critical_care_admission.ADC_UPDT only when the admission row is retracted; otherwise null. This is an ingestion-time end proxy, not unit/hospital discharge or death time; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "Native source-system update timestamp carried through the research transformation for each critical care admission record. It is derived from bronze field `updated_at` in `4_prod.bronze.map_critical_care_admission`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "loaded_at": "map_critical_care_admission.ADC_UPDT carried unchanged for the contributing Medicus admission row. This is bronze ingestion provenance, distinct from updated_at/source_update_timestamp and the admission/discharge dates; it is not Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_admission"),
    comment="Internal quality-controlled twin of clinical_critical_care_admission: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_ADMISSION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_ADMISSION_ADVISORY_RULES)
def _gold_qc_clinical_critical_care_admission():
    """Quality-controlled twin of journey_clinical.critical_care_admission."""
    df = _qc(
        "clinical_critical_care_admission",
        CLINICAL_CRITICAL_CARE_ADMISSION_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_ADMISSION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.critical_care_admission"),
    comment=(
        "One Medicus critical-care admission per source admission key. Gold QC twin of the "
        "silver product: 1 columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_critical_care_admission():
    """Contract-v2 public twin of clinical_critical_care_admission; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_admission")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_pat_id` AS `source_pat_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`admission_key` AS `admission_key`',
        '`source_unit` AS `source_unit`',
        '`unit_raw` AS `unit_raw`',
        '`source_site` AS `source_site`',
        '`hospital_admission_datetime` AS `hospital_admission_datetime`',
        '`unit_admission_datetime` AS `unit_admission_datetime`',
        '`unit_discharge_datetime` AS `unit_discharge_datetime`',
        '`hospital_discharge_datetime` AS `hospital_discharge_datetime`',
        '`admission_diagnosis_code` AS `admission_diagnosis_code`',
        '`max_organ_support` AS `max_organ_support`',
        '`cause_of_death` AS `cause_of_death`',
        '`date_of_death` AS `date_of_death`',
        '`source_updated_at` AS `source_updated_at`',
        '`person_link_status` AS `person_link_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_ADMISSION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.critical_care_daily_score ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_DAILY_SCORE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_unit` AS `source_unit`',
    '`source_site` AS `source_site`',
    '`source_daily_id` AS `source_daily_id`',
    '`score_type` AS `score_type`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    'CASE WHEN `score_value` < 0 THEN NULL ELSE `score_value` END AS `score_value`',
    '`score_value_raw` AS `score_value_raw`',
    '`score_date` AS `score_date`',
    '`score_calc_date` AS `score_calc_date`',
    '`admission_key` AS `admission_key`',
    '`critical_care_admission_key` AS `critical_care_admission_key`',
    '`day_latest_ind` AS `day_latest_ind`',
    '`row_class` AS `row_class`',
    '`person_link_status` AS `person_link_status`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_critical_care_daily_score_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_DAILY_SCORE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 3,989,576 rows of 3,989,576
    # that are current and attributable; identity_status = 'resolved' keeps the 3,798,086
    # rows of 3,989,576 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_CRITICAL_CARE_DAILY_SCORE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 191,490 of 3,989,576 at the profile.
    "gold.clinical.critical_care_daily_score.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 3,989,576 at the profile.
    "gold.clinical.critical_care_daily_score.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 156 of 3,989,576 rows (0.00391%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_daily_score.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_CRITICAL_CARE_DAILY_SCORE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_unit": "Medicus SOURCE_UNIT; primary-key component.",
    "source_site": "Medicus SOURCE_SITE; primary-key component.",
    "source_daily_id": "Medicus SOURCE_DAILY_ID as BIGINT; primary-key component.",
    "score_type": "Medicus SCORE_TYPE; primary-key component.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each critical care daily score record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_critical_care_daily_score`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each critical care daily score record. It is derived from bronze field `DATE_DAILY_CLEAN` in `4_prod.bronze.map_critical_care_daily_score`, selected as the provenance anchor from 2 recorded bronze source fields. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each critical care daily score record. It is derived from bronze field `SCORE_TYPE` in `4_prod.bronze.map_critical_care_daily_score`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each critical care daily score record. It is derived from bronze field `SCORE_TYPE` in `4_prod.bronze.map_critical_care_daily_score`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "score_value": "Numeric value recorded for score on the critical care daily score record. It is carried from bronze field `SCORE_VALUE` in `4_prod.bronze.map_critical_care_daily_score`. Units and source sentinel values follow the named source field unless the pipeline explicitly converts them; null is not equivalent to zero.",
    "score_value_raw": "Value describing score value raw for the critical care daily score record. It is carried from bronze field `SCORE_VALUE_RAW` in `4_prod.bronze.map_critical_care_daily_score`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "score_date": "Calendar date associated with score for the critical care daily score record. It is derived from bronze field `DATE_DAILY_CLEAN` in `4_prod.bronze.map_critical_care_daily_score`. The value has no time-of-day component; null means the source date was unavailable.",
    "score_calc_date": "Calendar date associated with score calc for the critical care daily score record. It is derived from bronze field `DATE_SCORE_CALC_CLEAN` in `4_prod.bronze.map_critical_care_daily_score`. The value has no time-of-day component; null means the source date was unavailable.",
    "admission_key": "Stable silver-v2 key identifying the admission associated with each critical care daily score record. It is carried from bronze field `ADMISSION_KEY` in `4_prod.bronze.map_critical_care_daily_score`. This is a contract identifier rather than a display value; null means the source identifiers were insufficient to derive the key.",
    "critical_care_admission_key": "Deterministic SHA-256 key for the parent critical-care admission.",
    "day_latest_ind": "Indicator of whether day latest applies to the critical care daily score record. It is carried from bronze field `DAY_LATEST_IND` in `4_prod.bronze.map_critical_care_daily_score`. Null means the source did not state the indicator and must not be interpreted as false.",
    "row_class": "Value describing row class for the critical care daily score record. It is carried from bronze field `ROW_CLASS` in `4_prod.bronze.map_critical_care_daily_score`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "person_link_status": "Processing or clinical status of person link for the critical care daily score record. It is carried from bronze field `PERSON_LINK_STATUS` in `4_prod.bronze.map_critical_care_daily_score`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each critical care daily score record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_critical_care_daily_score.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted. DAY_LATEST_IND, ROW_CLASS, score type and parent-admission state do not determine this label.",
    "record_status_effective_from": "First non-null map_critical_care_daily_score.DATE_DAILY_CLEAN then DATE_SCORE_CALC_CLEAN, matching the event-time expression. This date/calculation-time proxy is not an independently observed row-status transition; no UPDATED_AT or ingestion fallback is applied.",
    "record_status_effective_to": "map_critical_care_daily_score.ADC_UPDT only when the score row is retracted; otherwise null. This is an ingestion-time end proxy, not the score date, calculation date or admission discharge time.",
    "source_update_timestamp": "Native source-system update timestamp carried through the research transformation for each critical care daily score record. It is derived from bronze field `UPDATED_AT` in `4_prod.bronze.map_critical_care_daily_score`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "loaded_at": "map_critical_care_daily_score.ADC_UPDT carried unchanged through code filtering and source/public projections. The parent admission key is derived without joining a parent clock; this is row-level bronze ingestion provenance, distinct from UPDATED_AT and Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_daily_score"),
    comment="Internal quality-controlled twin of clinical_critical_care_daily_score: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_DAILY_SCORE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_DAILY_SCORE_ADVISORY_RULES)
def _gold_qc_clinical_critical_care_daily_score():
    """Quality-controlled twin of journey_clinical.critical_care_daily_score."""
    df = _qc(
        "clinical_critical_care_daily_score",
        CLINICAL_CRITICAL_CARE_DAILY_SCORE_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_DAILY_SCORE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.critical_care_daily_score"),
    comment=(
        "One admitted Medicus daily critical-care score row. Gold QC twin of the silver "
        "product: 1 columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_critical_care_daily_score():
    """Contract-v2 public twin of clinical_critical_care_daily_score; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_daily_score")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_unit` AS `source_unit`',
        '`source_site` AS `source_site`',
        '`source_daily_id` AS `source_daily_id`',
        '`score_type` AS `score_type`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`score_value` AS `score_value`',
        '`score_value_raw` AS `score_value_raw`',
        '`score_date` AS `score_date`',
        '`score_calc_date` AS `score_calc_date`',
        '`admission_key` AS `admission_key`',
        '`critical_care_admission_key` AS `critical_care_admission_key`',
        '`day_latest_ind` AS `day_latest_ind`',
        '`row_class` AS `row_class`',
        '`person_link_status` AS `person_link_status`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_DAILY_SCORE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.critical_care_period ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_CRITICAL_CARE_PERIOD_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`crit_care_period_id` AS `crit_care_period_id`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`period_end_datetime` AS `period_end_datetime`',
    '`period_business_key` AS `period_business_key`',
    '`no_current_version_ind` AS `no_current_version_ind`',
    '`business_key_status` AS `business_key_status`',
    '`source_valid_ind` AS `source_valid_ind`',
    '`care_type` AS `care_type`',
    '`unit_function` AS `unit_function`',
    '`unit_id` AS `unit_id`',
    '`cds_source_system` AS `cds_source_system`',
    '`level2_days` AS `level2_days`',
    '`level3_days` AS `level3_days`',
    '`organ_systems_supported` AS `organ_systems_supported`',
    '`gestation_length` AS `gestation_length`',
    '`discharge_status` AS `discharge_status`',
    '`discharge_destination` AS `discharge_destination`',
    '`source_encounter_id` AS `source_encounter_id`',
    '`cc_encounter_key` AS `cc_encounter_key`',
    '`cc_encounter_id` AS `cc_encounter_id`',
    '`cds_apc_id` AS `cds_apc_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_critical_care_period_metadata; source history stays on the main research table.
CLINICAL_CRITICAL_CARE_PERIOD_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 176,886 rows of 176,886 that
    # are current and attributable; identity_status = 'resolved' keeps the 169,705 rows of
    # 176,886 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_CRITICAL_CARE_PERIOD_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 7,181 of 176,886 at the profile.
    "gold.clinical.critical_care_period.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 176,886 at the profile.
    "gold.clinical.critical_care_period.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 308 of 176,886 rows (0.174%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_period.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 64 of 176,886 rows (0.0362%) when profiled on 2026-08-24.
    "gold.clinical.critical_care_period.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_CRITICAL_CARE_PERIOD_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "crit_care_period_id": "Source Crit_Care_Period_Id of the selected CCMDS revision, exposed as BIGINT. Identifies the representative source row chosen for this business-period record; period_business_key and patient_event_key retain the existing group and event linkage.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each critical care period record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_critical_care_period`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each critical care period record. It is derived from bronze field `CC_Period_Start_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each critical care period record. It is derived from bronze field `CC_Period_Disch_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "period_end_datetime": "Date and time associated with period end for the critical care period record. It is derived from bronze field `CC_Period_Disch_Dt_Tm_CLEAN` in `4_prod.bronze.map_critical_care_period`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "period_business_key": "Bronze-generated 64-bit xxhash64 of CC_Period_Local_Id and normalized MRN. Identifies the business-key group, not a native source revision; consult business_key_status for ambiguous or missing-MRN groups.",
    "no_current_version_ind": "Indicator of whether no current version applies to the critical care period record. It is derived from bronze field `CURRENT_IND` in `4_prod.bronze.map_critical_care_period`. Null means the source did not state the indicator and must not be interpreted as false.",
    "business_key_status": "Processing or clinical status of business key for the critical care period record. It is carried from bronze field `BUSINESS_KEY_STATUS` in `4_prod.bronze.map_critical_care_period`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_valid_ind": "Flag indicating whether the critical care period record passed source validation checks.",
    "care_type": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "unit_function": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "unit_id": "Identifier of the critical care unit to which the period of care relates.",
    "cds_source_system": "Code identifying the source system or organisation that supplied the critical care period record.",
    "level2_days": "Number of calendar days of level 2 critical care delivered during the critical care period.",
    "level3_days": "Number of days within the critical care period during which the patient received level 3 (intensive care) support.",
    "organ_systems_supported": "Maximum number of organ systems supported at any one time during the critical care period.",
    "gestation_length": "Recorded gestation length in completed weeks for the patient where relevant to the critical care period.",
    "discharge_status": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "discharge_destination": "Textual description of the meaning of the NHS Data Dictionary code value for the element.",
    "source_encounter_id": "Source-system identifier for the source encounter associated with each critical care period record. It is derived from bronze field `ENCNTR_ID` in `4_prod.bronze.map_critical_care_period`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "cc_encounter_key": "Deterministic SHA-256 key for the linked critical-care encounter.",
    "cc_encounter_id": "Native Millennium CC_ENCNTR_ID as BIGINT; joins to spine_encounter.encounter_id.",
    "cds_apc_id": "Identifier linking the critical care period to the associated Commissioning Data Set admitted patient care record.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each critical care period record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "For the selected CCMDS period revision, retracted when SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted. CURRENT_IND influences representative-row selection but does not itself determine this status; no_current_version_ind and source_valid_ind are separate.",
    "record_status_effective_from": "CC_Period_Start_Dt_Tm_CLEAN of the selected map_critical_care_period revision, carried unchanged. This care-period start is a history proxy, not an independently observed status change; no Record_Updated_Dt or ingestion-time fallback is applied.",
    "record_status_effective_to": "ADC_UPDT of the selected map_critical_care_period revision only when it is retracted; otherwise null. This is an ingestion-time end proxy, not CC_Period_Disch_Dt_Tm_CLEAN or an independent status-transition timestamp.",
    "source_update_timestamp": "Bronze pipeline processing timestamp carried from `PIPELINE_UPDT_DT_TM` in `4_prod.bronze.map_critical_care_period`. It records when bronze processed the source row, not a native clinical-system update timestamp, clinical event time, or the current Silver refresh time.",
    "loaded_at": "ADC_UPDT of the selected map_critical_care_period revision, not the maximum load time across the business-period group. Selection maximizes the struct of CURRENT_IND cast to INT (null 0), Record_Updated_Dt cast to TIMESTAMP (null 1900-01-01), then Crit_Care_Period_Id (null -1), within PERIOD_BUSINESS_KEY. The chosen row's clock is carried unchanged; it is distinct from PIPELINE_UPDT_DT_TM and Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_critical_care_period"),
    comment="Internal quality-controlled twin of clinical_critical_care_period: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_CRITICAL_CARE_PERIOD_MANDATORY_RULES)
@dp.expect_all(CLINICAL_CRITICAL_CARE_PERIOD_ADVISORY_RULES)
def _gold_qc_clinical_critical_care_period():
    """Quality-controlled twin of journey_clinical.critical_care_period."""
    df = _qc(
        "clinical_critical_care_period",
        CLINICAL_CRITICAL_CARE_PERIOD_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_PERIOD_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.critical_care_period"),
    comment=(
        "One CCMDS critical-care period per business key with deterministic version "
        "selection. Gold QC twin of the silver product: 1 columns are repaired or nulled, 1 "
        "rule(s) drop rows, 4 check(s) are advisory. Each rule states its reason in the "
        "pipeline notebook, and Lakeflow expectation metrics report what every rule matched "
        "on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_critical_care_period():
    """Contract-v2 public twin of clinical_critical_care_period; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_critical_care_period")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`crit_care_period_id` AS `crit_care_period_id`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`period_end_datetime` AS `period_end_datetime`',
        '`period_business_key` AS `period_business_key`',
        '`no_current_version_ind` AS `no_current_version_ind`',
        '`business_key_status` AS `business_key_status`',
        '`source_valid_ind` AS `source_valid_ind`',
        '`care_type` AS `care_type`',
        '`unit_function` AS `unit_function`',
        '`unit_id` AS `unit_id`',
        '`cds_source_system` AS `cds_source_system`',
        '`level2_days` AS `level2_days`',
        '`level3_days` AS `level3_days`',
        '`organ_systems_supported` AS `organ_systems_supported`',
        '`gestation_length` AS `gestation_length`',
        '`discharge_status` AS `discharge_status`',
        '`discharge_destination` AS `discharge_destination`',
        '`source_encounter_id` AS `source_encounter_id`',
        '`cc_encounter_key` AS `cc_encounter_key`',
        '`cc_encounter_id` AS `cc_encounter_id`',
        '`cds_apc_id` AS `cds_apc_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_CRITICAL_CARE_PERIOD_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.labour_delivery ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_LABOUR_DELIVERY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`labour_delivery_id` AS `labour_delivery_id`',
    '`labour_onset_method` AS `labour_onset_method`',
    '`labour_onset_presentation` AS `labour_onset_presentation`',
    '`caesarean_datetime` AS `caesarean_datetime`',
    '`decision_to_deliver_datetime` AS `decision_to_deliver_datetime`',
    '`rom_datetime` AS `rom_datetime`',
    '`rom_method` AS `rom_method`',
    '`rom_reason` AS `rom_reason`',
    '`second_stage_datetime` AS `second_stage_datetime`',
    '`third_stage_end_datetime` AS `third_stage_end_datetime`',
    '`episiotomy_reason` AS `episiotomy_reason`',
    '`placenta_delivery_method` AS `placenta_delivery_method`',
    '`mother_admission_method` AS `mother_admission_method`',
    '`mother_discharge_datetime` AS `mother_discharge_datetime`',
    '`mother_discharge_method` AS `mother_discharge_method`',
    '`mother_discharge_destination` AS `mother_discharge_destination`',
    '`intrapartum_org_site` AS `intrapartum_org_site`',
    '`intrapartum_setting` AS `intrapartum_setting`',
    '`postnatal_lead_provider` AS `postnatal_lead_provider`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`journey_pregnancy_key` AS `journey_pregnancy_key`',
    '`source_link_status` AS `source_link_status`',
    '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
    '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_labour_delivery_metadata; source history stays on the main research table.
CLINICAL_LABOUR_DELIVERY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 101,238 rows of 101,238 that
    # are current and attributable; identity_status = 'resolved' keeps the 100,859 rows of
    # 101,238 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_LABOUR_DELIVERY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 379 of 101,238 at the profile.
    "gold.clinical.labour_delivery.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 101,238 at the profile.
    "gold.clinical.labour_delivery.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 101,238 rows (0.000988%) when profiled on 2026-08-24.
    "gold.clinical.labour_delivery.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}

CLINICAL_LABOUR_DELIVERY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Unique MAT person identifier for the mother.",
    "person_id": "Maternal Millennium person identifier, exposed as BIGINT. Silver prefers Person_ID from the linked pregnancy spine, falling back to the MSDS adapter PERSON_ID only when its source-row group has one distinct person identifier. Null if neither yields an identifier.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each labour delivery record. It is derived from bronze field `CAESAREANDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`, selected as the provenance anchor from 3 recorded bronze source fields. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each labour delivery record. It is derived from bronze field `DISCHARGEDATETIMEMOTHERHSP_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "The method by which the process of labour began, or delivery by caesarean section occurred.",
    "source_display": "The method by which the process of labour began, or delivery by caesarean section occurred.",
    "labour_delivery_id": "MSDS LABOURDELIVERYID; primary key of this table.",
    "labour_onset_method": "The method by which the process of labour began, or delivery by caesarean section occurred.",
    "labour_onset_presentation": "The coded fetal presentation recorded at the onset of labour.",
    "caesarean_datetime": "Date and time associated with caesarean for the labour delivery record. It is derived from bronze field `CAESAREANDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "decision_to_deliver_datetime": "Date and time associated with decision to deliver for the labour delivery record. It is derived from bronze field `DECISIONTODELIVERDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "rom_datetime": "Date and time associated with rom for the labour delivery record. It is derived from bronze field `ROMDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "rom_method": "The coded method by which rupture of the membranes occurred.",
    "rom_reason": "The coded reason for the rupture of the membranes being performed.",
    "second_stage_datetime": "Date and time associated with second stage for the labour delivery record. It is derived from bronze field `LABOURONSETSECONDSTAGEDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "third_stage_end_datetime": "Date and time associated with third stage end for the labour delivery record. It is derived from bronze field `LABOURTHIRDSTAGEENDDATETIME_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "episiotomy_reason": "The coded reason an episiotomy was performed.",
    "placenta_delivery_method": "The coded method by which the placenta was delivered.",
    "mother_admission_method": "The coded method of admission for the mother's hospital provider spell in which delivery occurred.",
    "mother_discharge_datetime": "Date and time associated with mother discharge for the labour delivery record. It is derived from bronze field `DISCHARGEDATETIMEMOTHERHSP_CLEAN` in `4_prod.bronze.map_maternity_labour_delivery`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "mother_discharge_method": "The coded method of discharge for the mother from the post-delivery hospital provider spell.",
    "mother_discharge_destination": "The coded destination of the mother on discharge from the post-delivery hospital provider spell.",
    "intrapartum_org_site": "The NHS organisation site identifier where the mother started intrapartum care.",
    "intrapartum_setting": "The mother's actual location (type) at the start of intrapartum care.",
    "postnatal_lead_provider": "NHS organisation identifier of post natal lead provider organisation.",
    "pregnancy_id": "Source-system identifier for the pregnancy associated with each labour delivery record. It is derived from bronze field `PREGNANCY_ID_PARSED` in `4_prod.bronze.map_maternity_labour_delivery`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "journey_pregnancy_key": "Deterministic SHA-256 key for the pregnancy journey.",
    "source_link_status": "Processing or clinical status of source link for the labour delivery record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_maternity_labour_delivery`, selected as the provenance anchor from 2 recorded bronze source fields. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "pregnancy_orphan_ind": "Unique pregnancy episode identifier.",
    "spine_person_mismatch_ind": "Unique MAT person identifier for the mother.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each labour delivery record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Literal active for every retained MSDS labour-delivery row. This is not derived from source presence or clinical delivery outcome and does not establish that a source record has never been superseded or deleted.",
    "record_status_effective_from": "First non-null map_maternity_labour_delivery timestamp in the order LABOURONSETDATETIME_CLEAN, CAESAREANDATETIME_CLEAN, STARTDATETIMEMOTHERDELIVERYHPS_CLEAN, retained only within [1950-01-01, 2100-01-01). Selection precedes range validation: an out-of-range first value becomes null rather than falling through to a later value. This event-time proxy is not an independently observed status change.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the labour-delivery projection. The pipeline supplies no row-status end boundary and does not substitute discharge, delivery-end or ingestion timestamps.",
    "source_update_timestamp": "map_maternity_labour_delivery.RECORD_UPDATED_DT carried unchanged from the deduplicated labour source row. This is not the pregnancy-spine update clock or Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_maternity_labour_delivery.ADC_UPDT carried unchanged from the deduplicated labour source row. Identity fan-out is collapsed without aggregating this clock; the joined pregnancy spine does not contribute a load timestamp. This is not the Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_labour_delivery"),
    comment="Internal quality-controlled twin of clinical_labour_delivery: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_LABOUR_DELIVERY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_LABOUR_DELIVERY_ADVISORY_RULES)
def _gold_qc_clinical_labour_delivery():
    """Quality-controlled twin of journey_clinical.labour_delivery."""
    # 1 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_labour_delivery",
        CLINICAL_LABOUR_DELIVERY_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d"],
    )
    return _with_comments(df, CLINICAL_LABOUR_DELIVERY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.labour_delivery"),
    comment=(
        "One MSDS labour-delivery row with pregnancy-spine identity evidence. Gold QC twin of "
        "the silver product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 3 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_labour_delivery():
    """Contract-v2 public twin of clinical_labour_delivery; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_labour_delivery")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`labour_delivery_id` AS `labour_delivery_id`',
        '`labour_onset_method` AS `labour_onset_method`',
        '`labour_onset_presentation` AS `labour_onset_presentation`',
        '`caesarean_datetime` AS `caesarean_datetime`',
        '`decision_to_deliver_datetime` AS `decision_to_deliver_datetime`',
        '`rom_datetime` AS `rom_datetime`',
        '`rom_method` AS `rom_method`',
        '`rom_reason` AS `rom_reason`',
        '`second_stage_datetime` AS `second_stage_datetime`',
        '`third_stage_end_datetime` AS `third_stage_end_datetime`',
        '`episiotomy_reason` AS `episiotomy_reason`',
        '`placenta_delivery_method` AS `placenta_delivery_method`',
        '`mother_admission_method` AS `mother_admission_method`',
        '`mother_discharge_datetime` AS `mother_discharge_datetime`',
        '`mother_discharge_method` AS `mother_discharge_method`',
        '`mother_discharge_destination` AS `mother_discharge_destination`',
        '`intrapartum_org_site` AS `intrapartum_org_site`',
        '`intrapartum_setting` AS `intrapartum_setting`',
        '`postnatal_lead_provider` AS `postnatal_lead_provider`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`journey_pregnancy_key` AS `journey_pregnancy_key`',
        '`source_link_status` AS `source_link_status`',
        '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
        '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_LABOUR_DELIVERY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.maternity_care_contact ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MATERNITY_CARE_CONTACT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`care_contact_id` AS `care_contact_id`',
    '`attend_code` AS `attend_code`',
    '`consult_type` AS `consult_type`',
    '`contact_subject` AS `contact_subject`',
    '`medium` AS `medium`',
    '`duration` AS `duration`',
    '`admin_category` AS `admin_category`',
    '`gp_therapy_ind` AS `gp_therapy_ind`',
    '`cancel_datetime` AS `cancel_datetime`',
    '`cancel_reason` AS `cancel_reason`',
    '`replacement_offer_datetime` AS `replacement_offer_datetime`',
    '`replacement_appointment_datetime` AS `replacement_appointment_datetime`',
    '`organization_id` AS `organization_id`',
    '`site_id` AS `site_id`',
    '`location_code` AS `location_code`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`journey_pregnancy_key` AS `journey_pregnancy_key`',
    '`source_link_status` AS `source_link_status`',
    '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
    '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_maternity_care_contact_metadata; source history stays on the main research table.
CLINICAL_MATERNITY_CARE_CONTACT_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 1,052,832 rows of 1,052,832
    # that are current and attributable; identity_status = 'resolved' keeps the 1,052,592
    # rows of 1,052,832 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_MATERNITY_CARE_CONTACT_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 240 of 1,052,832 at the profile.
    "gold.clinical.maternity_care_contact.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 1,052,832 at the profile.
    "gold.clinical.maternity_care_contact.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 1,052,832 rows (9.5e-05%) when profiled on 2026-08-24.
    "gold.clinical.maternity_care_contact.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 7 of 1,052,832 rows (0.000665%) when profiled on 2026-08-24.
    "gold.clinical.maternity_care_contact.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_MATERNITY_CARE_CONTACT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Unique MAT person identifier for the mother.",
    "person_id": "Maternal Millennium person identifier, exposed as BIGINT. Silver prefers Person_ID from the linked pregnancy spine, falling back to the MSDS adapter PERSON_ID only when its source-row group has one distinct person identifier. Null if neither yields an identifier.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each maternity care contact record. It is derived from bronze field `CCONTACTDATETIME_CLEAN` in `4_prod.bronze.map_maternity_care_contact`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "care_contact_id": "MSDS CARECONID; primary key of this table.",
    "attend_code": "Indicates whether an APPOINTMENT for a CARE CONTACT took place and if the APPOINTMENT did not take place it also indicates if advance warning was given.",
    "consult_type": "This indicates the type of consultation for a SERVICE.",
    "contact_subject": "The person who was the subject of the Care Contact.",
    "medium": "Identifies the communication mechanism used to relay information between the CARE PROFESSIONAL and the PERSON who is the subject of the consultation, during a CARE ACTIVITY.",
    "duration": "The total duration of the direct clinical contact at CARE CONTACT in minutes, excluding any administration time and travel time.",
    "admin_category": "The ADMINISTRATIVE CATEGORY CODE recorded for PATIENT ACTIVITY, indicating whether the patient qualifies for free NHS healthcare or is paying for treatment.",
    "gp_therapy_ind": "An indicator of whether a Care Activity was delivered as Group Therapy.",
    "cancel_datetime": "Date and time associated with cancel for the maternity care contact record. It is derived from bronze field `CANCELDATE_CLEAN` in `4_prod.bronze.map_maternity_care_contact`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "cancel_reason": "The reason that a Care Contact was cancelled.",
    "replacement_offer_datetime": "Date and time associated with replacement offer for the maternity care contact record. It is derived from bronze field `REPLAPPTOFFDATE_CLEAN` in `4_prod.bronze.map_maternity_care_contact`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "replacement_appointment_datetime": "Date and time associated with replacement appointment for the maternity care contact record. It is derived from bronze field `REPLAPPTDATE_CLEAN` in `4_prod.bronze.map_maternity_care_contact`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "organization_id": "Native Millennium organization identifier as BIGINT when available.",
    "site_id": "ORGANISATION SITE IDENTIFIER (OF TREATMENT) is the ORGANISATION IDENTIFIER of the Organisation Site where the PATIENT was treated.",
    "location_code": "The type of physical LOCATION where PATIENTS are seen or where SERVICES are provided.",
    "pregnancy_id": "Source-system identifier for the pregnancy associated with each maternity care contact record. It is derived from bronze field `PREGNANCY_ID_PARSED` in `4_prod.bronze.map_maternity_care_contact`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "journey_pregnancy_key": "Deterministic SHA-256 key for the pregnancy journey.",
    "source_link_status": "Processing or clinical status of source link for the maternity care contact record. It is derived from bronze field `PERSON_ID` in `4_prod.bronze.map_maternity_care_contact`, selected as the provenance anchor from 2 recorded bronze source fields. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "pregnancy_orphan_ind": "Unique pregnancy episode identifier.",
    "spine_person_mismatch_ind": "Unique MAT person identifier for the mother.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each maternity care contact record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Literal active for every retained MSDS maternity-contact row. This is not derived from attendance, cancellation or source-presence fields; a cancelled contact can still carry this normalized row-status label.",
    "record_status_effective_from": "map_maternity_care_contact.CCONTACTDATETIME_CLEAN carried unchanged as the history start. This is the recorded contact time, not an independently observed row-status change; a missing source value remains null.",
    "record_status_effective_to": "Always NULL as a TIMESTAMP in the maternity-contact projection. The pipeline supplies no row-status end boundary and does not substitute cancellation, replacement-appointment or ingestion timestamps.",
    "source_update_timestamp": "map_maternity_care_contact.RECORD_UPDATED_DT carried unchanged from the deduplicated contact source row. This is not the pregnancy-spine update clock or Silver refresh time; a missing source timestamp remains null.",
    "loaded_at": "map_maternity_care_contact.ADC_UPDT carried unchanged from the deduplicated contact source row. Identity fan-out is collapsed without aggregating this clock; the joined pregnancy spine does not contribute a load timestamp. This is not the Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_maternity_care_contact"),
    comment="Internal quality-controlled twin of clinical_maternity_care_contact: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_MATERNITY_CARE_CONTACT_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MATERNITY_CARE_CONTACT_ADVISORY_RULES)
def _gold_qc_clinical_maternity_care_contact():
    """Quality-controlled twin of journey_clinical.maternity_care_contact."""
    df = _qc(
        "clinical_maternity_care_contact",
        CLINICAL_MATERNITY_CARE_CONTACT_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_MATERNITY_CARE_CONTACT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.maternity_care_contact"),
    comment=(
        "One MSDS maternity care contact with pregnancy-spine identity evidence. Gold QC twin "
        "of the silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 4 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_maternity_care_contact():
    """Contract-v2 public twin of clinical_maternity_care_contact; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_maternity_care_contact")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`care_contact_id` AS `care_contact_id`',
        '`attend_code` AS `attend_code`',
        '`consult_type` AS `consult_type`',
        '`contact_subject` AS `contact_subject`',
        '`medium` AS `medium`',
        '`duration` AS `duration`',
        '`admin_category` AS `admin_category`',
        '`gp_therapy_ind` AS `gp_therapy_ind`',
        '`cancel_datetime` AS `cancel_datetime`',
        '`cancel_reason` AS `cancel_reason`',
        '`replacement_offer_datetime` AS `replacement_offer_datetime`',
        '`replacement_appointment_datetime` AS `replacement_appointment_datetime`',
        '`organization_id` AS `organization_id`',
        '`site_id` AS `site_id`',
        '`location_code` AS `location_code`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`journey_pregnancy_key` AS `journey_pregnancy_key`',
        '`source_link_status` AS `source_link_status`',
        '`pregnancy_orphan_ind` AS `pregnancy_orphan_ind`',
        '`spine_person_mismatch_ind` AS `spine_person_mismatch_ind`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_MATERNITY_CARE_CONTACT_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.neonatal_care_day ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_NEONATAL_CARE_DAY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`entity_id` AS `entity_id`',
    'CASE WHEN CAST(`activity_date` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `activity_date` END AS `activity_date`',
    '`ward_location` AS `ward_location`',
    '`unit_function` AS `unit_function`',
    '`critical_care_start` AS `critical_care_start`',
    '`critical_care_discharge` AS `critical_care_discharge`',
    '`episode_link_status` AS `episode_link_status`',
    '`neonatal_episode_key` AS `neonatal_episode_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_neonatal_care_day_metadata; source history stays on the main research table.
CLINICAL_NEONATAL_CARE_DAY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 507,781 rows of 507,781 that
    # are current and attributable; identity_status = 'resolved' keeps the 501,933 rows of
    # 507,781 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_NEONATAL_CARE_DAY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 5,848 of 507,781 at the profile.
    "gold.clinical.neonatal_care_day.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 507,781 at the profile.
    "gold.clinical.neonatal_care_day.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 1 of 507,781 rows
    # (0.000197%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 48 of 507,781 rows (0.00945%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 17,443 of 507,781 rows (3.44%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_care_day.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_NEONATAL_CARE_DAY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each neonatal care day record. It is derived from bronze field `BABY_PERSON_ID` in `4_prod.bronze.map_neonatal_critical_care`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date of the critical care activity record.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Neonatal Critical Care Activity Code 1 - coded activity performed on this day.",
    "source_display": "Neonatal Critical Care Activity Code 1 - coded activity performed on this day.",
    "entity_id": "BadgerNet EntityID; first component of the primary key.",
    "activity_date": "BadgerNet ActivityDate; second component of the primary key.",
    "ward_location": "Ward location code (e.g. nnu = neonatal unit, tc = transitional care).",
    "unit_function": "Functional type of the critical care unit (e.g. 13 = NICU, 14 = LNU).",
    "critical_care_start": "Date or time value for critical care start on the neonatal care day record. It is derived from bronze field `CriticalCareStartDate_CLEAN` in `4_prod.bronze.map_neonatal_critical_care`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "critical_care_discharge": "Date or time value for critical care discharge on the neonatal care day record. It is derived from bronze field `CriticalCareDischargeDate_CLEAN` in `4_prod.bronze.map_neonatal_critical_care`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "episode_link_status": "Processing or clinical status of episode link for the neonatal care day record. It is carried from bronze field `EPISODE_LINK_STATUS` in `4_prod.bronze.map_neonatal_critical_care`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "neonatal_episode_key": "Deterministic SHA-256 key for the parent neonatal episode.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each neonatal care day record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Derived BadgerNet care-day row status: retracted when map_neonatal_critical_care.SOURCE_PRESENT_IND is false, otherwise active; a missing flag defaults true. The projection does not emit superseded and this status does not describe the baby's clinical condition or care level.",
    "record_status_effective_from": "map_neonatal_critical_care.ActivityDate carried unchanged as the history start. This is the care-day date/time used as a proxy, not an independently observed status-change timestamp; no clinical-care-start or ingestion-time fallback is applied.",
    "record_status_effective_to": "map_neonatal_critical_care.ADC_UPDT only when the care-day row is retracted; otherwise null. This is an ingestion-time end proxy, not ActivityDate or the critical-care discharge timestamp; a missing ADC_UPDT remains null.",
    "source_update_timestamp": "map_neonatal_critical_care.ADC_UPDT carried unchanged. It is the same contributing bronze load clock published as loaded_at, not an independent native clinical-system modification timestamp or Silver refresh time.",
    "loaded_at": "map_neonatal_critical_care.ADC_UPDT carried unchanged through the care-day stage and public projection. This is bronze ingestion provenance, not ActivityDate, critical-care start/discharge or Silver refresh time; a missing source timestamp remains null.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_care_day"),
    comment="Internal quality-controlled twin of clinical_neonatal_care_day: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_NEONATAL_CARE_DAY_ADVISORY_RULES)
def _gold_qc_clinical_neonatal_care_day():
    """Quality-controlled twin of journey_clinical.neonatal_care_day."""
    df = _qc(
        "clinical_neonatal_care_day",
        CLINICAL_NEONATAL_CARE_DAY_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_NEONATAL_CARE_DAY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.neonatal_care_day"),
    comment=(
        "One admitted BadgerNet neonatal critical-care day. Gold QC twin of the silver "
        "product: 2 columns are repaired or nulled, 1 rule(s) drop rows, 5 check(s) are "
        "advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_neonatal_care_day():
    """Contract-v2 public twin of clinical_neonatal_care_day; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_neonatal_care_day")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`entity_id` AS `entity_id`',
        '`activity_date` AS `activity_date`',
        '`ward_location` AS `ward_location`',
        '`unit_function` AS `unit_function`',
        '`critical_care_start` AS `critical_care_start`',
        '`critical_care_discharge` AS `critical_care_discharge`',
        '`episode_link_status` AS `episode_link_status`',
        '`neonatal_episode_key` AS `neonatal_episode_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_NEONATAL_CARE_DAY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.neonatal_episode ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_NEONATAL_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`entity_id` AS `entity_id`',
    '`badger_unique_id` AS `badger_unique_id`',
    '`mother_person_id` AS `mother_person_id`',
    '`care_location_id` AS `care_location_id`',
    '`care_location_name` AS `care_location_name`',
    '`birth_datetime` AS `birth_datetime`',
    '`birth_datetime_raw` AS `birth_datetime_raw`',
    '`admit_datetime` AS `admit_datetime`',
    '`discharge_datetime` AS `discharge_datetime`',
    '`gestation_weeks` AS `gestation_weeks`',
    '`gestation_days` AS `gestation_days`',
    '`birthweight` AS `birthweight`',
    '`sex` AS `sex`',
    '`final_nnu_outcome` AS `final_nnu_outcome`',
    '`unit_level` AS `unit_level`',
    '`person_link_status` AS `person_link_status`',
    '`record_timestamp` AS `record_timestamp`',
    '`last_update` AS `last_update`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_neonatal_episode_metadata; source history stays on the main research table.
CLINICAL_NEONATAL_EPISODE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 36,718 rows of 36,718 that
    # are current and attributable; identity_status = 'resolved' keeps the 36,161 rows of
    # 36,718 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_NEONATAL_EPISODE_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 557 of 36,718 at the profile.
    "gold.clinical.neonatal_episode.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 36,718 at the profile.
    "gold.clinical.neonatal_episode.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 834 of 36,718 rows (2.27%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_episode.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_NEONATAL_EPISODE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each neonatal episode record. It is derived from bronze field `BABY_PERSON_ID` in `4_prod.bronze.map_neonatal_episode`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each neonatal episode record. It is derived from bronze field `AdmitTime_CLEAN` in `4_prod.bronze.map_neonatal_episode`, selected as the provenance anchor from 2 recorded bronze source fields. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each neonatal episode record. It is derived from bronze field `DischTime_CLEAN` in `4_prod.bronze.map_neonatal_episode`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "entity_id": "BadgerNet EntityID; primary key of this table.",
    "badger_unique_id": "Unique identifier assigned by the Badger neonatal system (legacy).",
    "mother_person_id": "Source-system identifier for the mother person associated with each neonatal episode record. It is carried from bronze field `MOTHER_PERSON_ID` in `4_prod.bronze.map_neonatal_episode`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "care_location_id": "Identifier for the neonatal care location.",
    "care_location_name": "Name of the neonatal care location.",
    "birth_datetime": "Date and time associated with birth for the neonatal episode record. It is derived from bronze field `BirthTimeBaby_CLEAN` in `4_prod.bronze.map_neonatal_episode`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "birth_datetime_raw": "Date and time of birth of the baby.",
    "admit_datetime": "Date and time associated with admit for the neonatal episode record. It is derived from bronze field `AdmitTime_CLEAN` in `4_prod.bronze.map_neonatal_episode`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "discharge_datetime": "Date and time associated with discharge for the neonatal episode record. It is derived from bronze field `DischTime_CLEAN` in `4_prod.bronze.map_neonatal_episode`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "gestation_weeks": "Gestational age at birth in completed weeks.",
    "gestation_days": "Remaining days of gestational age at birth beyond completed weeks.",
    "birthweight": "Birth weight of the baby in grams.",
    "sex": "Sex of the baby.",
    "final_nnu_outcome": "Final outcome of the neonatal unit admission (e.g. Home, Transfer, Death).",
    "unit_level": "Designation level of the neonatal unit.",
    "person_link_status": "Processing or clinical status of person link for the neonatal episode record. It is carried from bronze field `PERSON_LINK_STATUS` in `4_prod.bronze.map_neonatal_episode`. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_timestamp": "Technical timestamp recording when this record was created or last written in the source extract.",
    "last_update": "Timestamp of the last update to this record.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each neonatal episode record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_neonatal_episode.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted, and the baby's outcome, discharge date or identity-link status do not determine this label.",
    "record_status_effective_from": "First non-null map_neonatal_episode.AdmitTime_CLEAN then BirthTimeBaby_CLEAN, matching the event-time expression. This admission/birth-time proxy is not an independently observed row-status transition; no LastUpdate, RecordTimestamp or ingestion fallback is applied.",
    "record_status_effective_to": "map_neonatal_episode.ADC_UPDT only when the episode row is retracted; otherwise null. This is an ingestion-time end proxy, not DischTime_CLEAN, LastUpdate or an independent status-transition timestamp.",
    "source_update_timestamp": "Timestamp of the last update to this record.",
    "loaded_at": "map_neonatal_episode.ADC_UPDT carried unchanged for the contributing BadgerNet episode row. It is bronze ingestion provenance, distinct from LastUpdate published as source_update_timestamp and from RecordTimestamp; it is not admission/birth/discharge time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_episode"),
    comment="Internal quality-controlled twin of clinical_neonatal_episode: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_EPISODE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_NEONATAL_EPISODE_ADVISORY_RULES)
def _gold_qc_clinical_neonatal_episode():
    """Quality-controlled twin of journey_clinical.neonatal_episode."""
    df = _qc(
        "clinical_neonatal_episode",
        CLINICAL_NEONATAL_EPISODE_SELECT,
        date_flags=["event_before_birth"],
    )
    return _with_comments(df, CLINICAL_NEONATAL_EPISODE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.neonatal_episode"),
    comment=(
        "One curated BadgerNet neonatal episode for the baby subject. Gold QC twin of the "
        "silver product: 1 columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) "
        "are advisory. Each rule states its reason in the pipeline notebook, and Lakeflow "
        "expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_neonatal_episode():
    """Contract-v2 public twin of clinical_neonatal_episode; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_neonatal_episode")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`entity_id` AS `entity_id`',
        '`badger_unique_id` AS `badger_unique_id`',
        '`mother_person_id` AS `mother_person_id`',
        '`care_location_id` AS `care_location_id`',
        '`care_location_name` AS `care_location_name`',
        '`birth_datetime` AS `birth_datetime`',
        '`birth_datetime_raw` AS `birth_datetime_raw`',
        '`admit_datetime` AS `admit_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`gestation_weeks` AS `gestation_weeks`',
        '`gestation_days` AS `gestation_days`',
        '`birthweight` AS `birthweight`',
        '`sex` AS `sex`',
        '`final_nnu_outcome` AS `final_nnu_outcome`',
        '`unit_level` AS `unit_level`',
        '`person_link_status` AS `person_link_status`',
        '`record_timestamp` AS `record_timestamp`',
        '`last_update` AS `last_update`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_NEONATAL_EPISODE_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.neonatal_examination ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_NEONATAL_EXAMINATION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    '`event_end_datetime` AS `event_end_datetime`',
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`entity_id` AS `entity_id`',
    '`examination_datetime` AS `examination_datetime`',
    '`exam_date_derived` AS `exam_date_derived`',
    '`include_in_discharge_letter` AS `include_in_discharge_letter`',
    '`head_circumference` AS `head_circumference`',
    '`spine_finding` AS `spine_finding`',
    '`heart_finding` AS `heart_finding`',
    '`genitalia_finding` AS `genitalia_finding`',
    '`hips_finding` AS `hips_finding`',
    '`right_hip_finding` AS `right_hip_finding`',
    '`eyes_finding` AS `eyes_finding`',
    '`spine_comments` AS `spine_comments`',
    '`heart_comments` AS `heart_comments`',
    '`genitalia_comments` AS `genitalia_comments`',
    '`hips_comments` AS `hips_comments`',
    '`eyes_comments` AS `eyes_comments`',
    '`neonatal_episode_key` AS `neonatal_episode_key`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_neonatal_examination_metadata; source history stays on the main research table.
CLINICAL_NEONATAL_EXAMINATION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 7,528 rows of 7,528 that are
    # current and attributable; identity_status = 'resolved' keeps the 7,416 rows of 7,528
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}

CLINICAL_NEONATAL_EXAMINATION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 112 of 7,528 at the profile.
    "gold.clinical.neonatal_examination.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 7,528 at the profile.
    "gold.clinical.neonatal_examination.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 7,528 rows (0.0133%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_examination.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 213 of 7,528 rows (2.83%) when profiled on 2026-08-24.
    "gold.clinical.neonatal_examination.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_NEONATAL_EXAMINATION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier namespace used to interpret the subject identifier on this record for each neonatal examination record. It is derived from bronze field `BABY_PERSON_ID` in `4_prod.bronze.map_neonatal_examination`. Its uniqueness is limited to the originating source namespace; null means no identifier was supplied.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Date and time when the represented clinical or administrative event occurred for each neonatal examination record. It is derived from bronze field `DateOfExamination_CLEAN` in `4_prod.bronze.map_neonatal_examination`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "event_end_datetime": "Date and time when the represented clinical or administrative event ended for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "source_coding_system": "Coding system or source namespace in which the source code is defined for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_code": "Code supplied by the originating source system for the represented concept for each neonatal examination record. It is derived from bronze field `EXAM_POPULATED_IND` in `4_prod.bronze.map_neonatal_examination`. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "source_display": "Human-readable label supplied by the source system for the source code for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "entity_id": "BadgerNet EntityID; primary key of this table.",
    "examination_datetime": "Date and time associated with examination for the neonatal examination record. It is derived from bronze field `DateOfExamination_CLEAN` in `4_prod.bronze.map_neonatal_examination`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "exam_date_derived": "Date or time value for exam date derived on the neonatal examination record. It is carried from bronze field `EXAM_DATE_DERIVED` in `4_prod.bronze.map_neonatal_examination`. Source precision and timezone handling follow the pipeline expression; null means the time was unavailable.",
    "include_in_discharge_letter": "Whether the examination findings should be included in the discharge letter.",
    "head_circumference": "Head circumference measurement in centimetres at examination.",
    "spine_finding": "Examination finding for spine.",
    "heart_finding": "Examination finding for heart/cardiac.",
    "genitalia_finding": "Examination finding for genitalia.",
    "hips_finding": "Examination finding for left hip.",
    "right_hip_finding": "Examination finding for right hip.",
    "eyes_finding": "Examination finding for eyes.",
    "spine_comments": "Free-text comments on spine examination finding.",
    "heart_comments": "Free-text comments on heart/cardiac examination finding.",
    "genitalia_comments": "Free-text comments on genitalia examination finding.",
    "hips_comments": "Free-text comments on left hip examination finding.",
    "eyes_comments": "Free-text comments on eyes examination finding.",
    "neonatal_episode_key": "Deterministic SHA-256 key for the parent neonatal episode.",
    "confidentiality_code": "Source confidentiality classification attached to the record for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Source code meanings and sentinel values are retained unless the pipeline explicitly maps them; null means no code was supplied.",
    "vip_ind": "Indicator that the source record carries VIP handling status for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "withheld_identity_ind": "Indicator that the source identity was deliberately withheld for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Null means the source did not state the indicator and must not be interpreted as false.",
    "fact_category": "Contract category used to group the record with comparable clinical facts for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "source_feed": "Originating data feed responsible for the record for each neonatal examination record. It is produced by the silver transformation and has no direct bronze-column lineage entry. Whitespace and source sentinel text are retained unless the pipeline explicitly normalizes them; null means no value was supplied.",
    "record_status": "Retracted when map_neonatal_examination.SOURCE_PRESENT_IND is false, otherwise active; a missing presence flag defaults true. No superseded status is emitted. This label does not describe examination findings or the baby's clinical condition; the separate usable-code filter controls publication eligibility.",
    "record_status_effective_from": "map_neonatal_examination.DateOfExamination_CLEAN carried unchanged. This examination-time proxy is not an independently observed row-status transition; no episode admission/birth or ingestion-time fallback is applied.",
    "record_status_effective_to": "map_neonatal_examination.ADC_UPDT only when the examination row is retracted; otherwise null. This is an ingestion-time end proxy, not the examination date or episode discharge time.",
    "source_update_timestamp": "map_neonatal_examination.ADC_UPDT carried unchanged, identical to loaded_at. The projection supplies no independent native application-update clock here and does not substitute Silver refresh time.",
    "loaded_at": "map_neonatal_examination.ADC_UPDT carried unchanged through code filtering and source/public projections. The neonatal-episode key is derived without joining an episode clock. This is bronze ingestion provenance, identical to source_update_timestamp, not examination time or Silver refresh time.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_examination"),
    comment="Internal quality-controlled twin of clinical_neonatal_examination: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_EXAMINATION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_NEONATAL_EXAMINATION_ADVISORY_RULES)
def _gold_qc_clinical_neonatal_examination():
    """Quality-controlled twin of journey_clinical.neonatal_examination."""
    df = _qc(
        "clinical_neonatal_examination",
        CLINICAL_NEONATAL_EXAMINATION_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return _with_comments(df, CLINICAL_NEONATAL_EXAMINATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.neonatal_examination"),
    comment=(
        "One populated BadgerNet neonatal examination. Gold QC twin of the silver product: 0 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 4 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_neonatal_examination():
    """Contract-v2 public twin of clinical_neonatal_examination; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_neonatal_examination")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`entity_id` AS `entity_id`',
        '`examination_datetime` AS `examination_datetime`',
        '`exam_date_derived` AS `exam_date_derived`',
        '`include_in_discharge_letter` AS `include_in_discharge_letter`',
        '`head_circumference` AS `head_circumference`',
        '`spine_finding` AS `spine_finding`',
        '`heart_finding` AS `heart_finding`',
        '`genitalia_finding` AS `genitalia_finding`',
        '`hips_finding` AS `hips_finding`',
        '`right_hip_finding` AS `right_hip_finding`',
        '`eyes_finding` AS `eyes_finding`',
        '`spine_comments` AS `spine_comments`',
        '`heart_comments` AS `heart_comments`',
        '`genitalia_comments` AS `genitalia_comments`',
        '`hips_comments` AS `hips_comments`',
        '`eyes_comments` AS `eyes_comments`',
        '`neonatal_episode_key` AS `neonatal_episode_key`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`fact_category` AS `fact_category`',
        '`source_feed` AS `source_feed`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_NEONATAL_EXAMINATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.registry_entry ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_REGISTRY_ENTRY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`registry_family` AS `registry_family`',
    '`registry_type` AS `registry_type`',
    '`entry_id` AS `entry_id`',
    '`parent_registry_entry_key` AS `parent_registry_entry_key`',
    '`linkage_status` AS `linkage_status`',
    '`mrn` AS `mrn`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_death` AS `date_of_death`',
    '`source_edit_datetime` AS `source_edit_datetime`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: QC/batch inputs come from internal _clinical_registry_entry_metadata; source history stays on the main research table.
CLINICAL_REGISTRY_ENTRY_MANDATORY_RULES = {
    # The research surface. identity_status = 'resolved' keeps the 236,653 rows of 236,690
    # that are current and attributable. Superseded versions and rows whose identity was
    # never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved')",
}

CLINICAL_REGISTRY_ENTRY_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 37 of 236,690 at the profile.
    "gold.clinical.registry_entry.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 18 of 236,690 rows (0.0076%) when profiled on 2026-08-24.
    "gold.clinical.registry_entry.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1 of 236,690 rows (0.000422%) when profiled on 2026-08-24.
    "gold.clinical.registry_entry.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}

CLINICAL_REGISTRY_ENTRY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic event key derived from source identifiers; retained as a stable join key.",
    "source_object": "iWeb registry family identifying the source object; part of the primary key with entry_id.",
    "subject_key": "Deterministic SHA-256 over the strongest source identifier. Not salted, not secret; a stable join key across feeds where person_id is unresolved.",
    "subject_id_system": "Identifier system used for subject_key.",
    "person_id": "Native Millennium PERSON_ID as BIGINT when resolved.",
    "encounter_id": "Native Millennium ENCNTR_ID as BIGINT when available.",
    "event_datetime": "Family-specific clinical timestamp; child rows inherit the parent timestamp.",
    "event_end_datetime": "Family-specific clinical end timestamp.",
    "registry_family": "Parameterized registry family.",
    "registry_type": "Registry subtype for composite-key MDT rows.",
    "entry_id": "Native iWeb ENTRY_ID as BIGINT; primary key within source_object.",
    "parent_registry_entry_key": "Deterministic SHA-256 key for the parent registry entry.",
    "linkage_status": "Bronze person-link status.",
    "mrn": "Source MRN where supplied.",
    "nhs_number": "Source NHS number where supplied.",
    "date_of_death": "Family-specific demographic date of death.",
    "source_edit_datetime": "Source edit timestamp; never treated as clinical time.",
    "confidentiality_code": "Security classification when supplied.",
    "vip_ind": "VIP indicator when supplied.",
    "withheld_identity_ind": "Identity-withholding status is not populated by the current source projections; null does not mean identity was not withheld.",
    "registry_payload": "Full verbatim source row as key/value pairs.",
    "record_status": "Superseded when the contributing iWeb row's SOURCE_PRESENT_IND is false, otherwise active; missing presence defaults true. This rule applies to all ten configured registry families and emits no retracted status. Device rows in the shared internal stage are filtered out before registry publication.",
    "record_status_effective_from": "Always null as a TIMESTAMP for all ten configured iWeb registry families. Neither the row's clinical event date, a joined parent procedure/episode event date, DATE_LAST_CHANGED nor a MediConnect implant date fills this history start.",
    "record_status_effective_to": "For nine iWeb families, SOURCE_ABSENT_DETECTED_TS only when source presence is false, otherwise null. Coronary-procedure rows always have a null history end, including source-absent rows, because that family's configured absence-time expression is null. No ADC_UPDT or parent-date fallback is applied.",
    "source_update_timestamp": "Timestamp of the last change made to the record in the source registry system.",
    "loaded_at": "ADC_UPDT of the contributing iWeb row, carried unchanged per family. Surgery-procedure and coronary-lesion parent joins select parent identity and clinical event time, not a parent load clock. The family union takes no cross-family maximum; device rows are excluded before registry QC/public projections.",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_entry"),
    comment="Internal quality-controlled twin of clinical_registry_entry: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ENTRY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_REGISTRY_ENTRY_ADVISORY_RULES)
def _gold_qc_clinical_registry_entry():
    """Quality-controlled twin of journey_clinical.registry_entry."""
    df = _qc("clinical_registry_entry", CLINICAL_REGISTRY_ENTRY_SELECT)
    return _with_comments(df, CLINICAL_REGISTRY_ENTRY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_entry"),
    comment=(
        "One iWeb cardiac-registry entry from one of ten registered families, retaining the "
        "full source row as a governed VARIANT payload. Gold QC twin of the silver product: 1 "
        "columns are repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. Each "
        "rule states its reason in the pipeline notebook, and Lakeflow expectation metrics "
        "report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_entry():
    """Contract-v2 public twin of clinical_registry_entry; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_registry_entry")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_object` AS `source_object`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`registry_family` AS `registry_family`',
        '`registry_type` AS `registry_type`',
        '`entry_id` AS `entry_id`',
        '`parent_registry_entry_key` AS `parent_registry_entry_key`',
        '`linkage_status` AS `linkage_status`',
        '`mrn` AS `mrn`',
        '`nhs_number` AS `nhs_number`',
        '`date_of_death` AS `date_of_death`',
        '`source_edit_datetime` AS `source_edit_datetime`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, CLINICAL_REGISTRY_ENTRY_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_reference.pregnancy_reconciliation ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PREGNANCY_RECONCILIATION_SELECT = [
    '`pregnancy_reconciliation_key` AS `pregnancy_reconciliation_key`',
    '`unmatched_reason` AS `unmatched_reason`',
    '`pregnancy_id_raw` AS `pregnancy_id_raw`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`lpid_mother` AS `lpid_mother`',
    '`antenatal_appointment_date` AS `antenatal_appointment_date`',
    '`pregnancy_first_contact_date` AS `pregnancy_first_contact_date`',
    '`expected_delivery_date` AS `expected_delivery_date`',
    '`last_mens_period_date` AS `last_mens_period_date`',
    '`folic_acid_supplement_cd` AS `folic_acid_supplement_cd`',
    '`previous_live_births` AS `previous_live_births`',
    '`previous_still_births` AS `previous_still_births`',
    '`previous_losses_under_24_weeks` AS `previous_losses_under_24_weeks`',
    '`previous_caesarean_sections` AS `previous_caesarean_sections`',
    '`source_system_code` AS `source_system_code`',
    '`msds_source_version` AS `msds_source_version`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]

# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PREGNANCY_RECONCILIATION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 59 rows of 59 that are
    # current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}

REFERENCE_PREGNANCY_RECONCILIATION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 59 at the profile.
    "gold.reference.pregnancy_reconciliation.record_status.default_view_active":
        "record_status = 'active'",
}

REFERENCE_PREGNANCY_RECONCILIATION_COLUMN_COMMENTS = {
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

@dp.materialized_view(
    name=_n("gold_qc._reference_pregnancy_reconciliation"),
    comment="Internal quality-controlled twin of reference_pregnancy_reconciliation: the Gold repairs, nulls and expectations are applied here and the published Gold MV reads this object. Materialized rather than temporary so the published MV can refresh incrementally.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(REFERENCE_PREGNANCY_RECONCILIATION_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PREGNANCY_RECONCILIATION_ADVISORY_RULES)
def _gold_qc_reference_pregnancy_reconciliation():
    """Quality-controlled twin of journey_reference.pregnancy_reconciliation."""
    df = _qc("reference_pregnancy_reconciliation", REFERENCE_PREGNANCY_RECONCILIATION_SELECT)
    return _with_comments(df, REFERENCE_PREGNANCY_RECONCILIATION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_reference.pregnancy_reconciliation"),
    comment=(
        "Unmatched MSDS pregnancy evidence documenting a person-spine coverage gap. Gold QC "
        "twin of the silver product: 0 columns are repaired or nulled, 1 rule(s) drop rows, 1 "
        "check(s) are advisory. Each rule states its reason in the pipeline notebook, and "
        "Lakeflow expectation metrics report what every rule matched on each update."
    ),
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_reference_pregnancy_reconciliation():
    """Contract-v2 public twin of reference_pregnancy_reconciliation; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_pregnancy_reconciliation")).selectExpr(
        '`pregnancy_reconciliation_key` AS `pregnancy_reconciliation_key`',
        '`unmatched_reason` AS `unmatched_reason`',
        '`pregnancy_id_raw` AS `pregnancy_id_raw`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`lpid_mother` AS `lpid_mother`',
        '`antenatal_appointment_date` AS `antenatal_appointment_date`',
        '`pregnancy_first_contact_date` AS `pregnancy_first_contact_date`',
        '`expected_delivery_date` AS `expected_delivery_date`',
        '`last_mens_period_date` AS `last_mens_period_date`',
        '`folic_acid_supplement_cd` AS `folic_acid_supplement_cd`',
        '`previous_live_births` AS `previous_live_births`',
        '`previous_still_births` AS `previous_still_births`',
        '`previous_losses_under_24_weeks` AS `previous_losses_under_24_weeks`',
        '`previous_caesarean_sections` AS `previous_caesarean_sections`',
        '`source_system_code` AS `source_system_code`',
        '`msds_source_version` AS `msds_source_version`',
        '`record_status` AS `record_status`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return _with_comments(df, REFERENCE_PREGNANCY_RECONCILIATION_COLUMN_COMMENTS)

# COMMAND ----------

# ==== journey_clinical.neonatal_care_day_activity ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_neonatal_care_day applies the parent Gold drop rule.
CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entity_id` AS `entity_id`',
    '`activity_date` AS `activity_date`',
    '`sequence` AS `sequence`',
    '`activity_code` AS `activity_code`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_neonatal_care_day row.",
    "entity_id": "BadgerNet EntityID; first native parent-key component.",
    "activity_date": "BadgerNet ActivityDate; second native parent-key component.",
    "sequence": "One-based source position from CCAC1 through CCAC20.",
    "activity_code": "Verbatim neonatal critical-care activity code.",
    "loaded_at": "Bronze load timestamp inherited from the care-day row.",
}

CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_neonatal_care_day_activity.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_care_day_activity"),
    comment="Internal QC of clinical_neonatal_care_day_activity; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_MANDATORY_RULES)
def _gold_qc_clinical_neonatal_care_day_activity():
    """Task 3 Gold child of clinical_neonatal_care_day; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.neonatal_care_day")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_neonatal_care_day_activity"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_NEONATAL_CARE_DAY_ACTIVITY_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.neonatal_care_day_activity"),
    comment='One BadgerNet critical-care activity code per neonatal care day.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_neonatal_care_day_activity():
    """Publish clinical_neonatal_care_day_activity without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_neonatal_care_day_activity")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.neonatal_care_day_high_cost_drug ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_neonatal_care_day applies the parent Gold drop rule.
CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entity_id` AS `entity_id`',
    '`activity_date` AS `activity_date`',
    '`sequence` AS `sequence`',
    '`high_cost_drug_code` AS `high_cost_drug_code`',
    '`loaded_at` AS `loaded_at`',
]

CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic key of the parent clinical_neonatal_care_day row.",
    "entity_id": "BadgerNet EntityID; first native parent-key component.",
    "activity_date": "BadgerNet ActivityDate; second native parent-key component.",
    "sequence": "One-based source position from HCDRUG1 through HCDRUG20.",
    "high_cost_drug_code": "Verbatim neonatal high-cost-drug code.",
    "loaded_at": "Bronze load timestamp inherited from the care-day row.",
}

CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_neonatal_care_day_high_cost_drug.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_neonatal_care_day_high_cost_drug"),
    comment="Internal QC of clinical_neonatal_care_day_high_cost_drug; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_MANDATORY_RULES)
def _gold_qc_clinical_neonatal_care_day_high_cost_drug():
    """Task 3 Gold child of clinical_neonatal_care_day; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.neonatal_care_day")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_neonatal_care_day_high_cost_drug"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_NEONATAL_CARE_DAY_HIGH_COST_DRUG_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.neonatal_care_day_high_cost_drug"),
    comment='One BadgerNet high-cost-drug code per neonatal care day.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_neonatal_care_day_high_cost_drug():
    """Publish clinical_neonatal_care_day_high_cost_drug without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_neonatal_care_day_high_cost_drug")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_acs_transfer ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_ACS_TRANSFER_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`admissiondate` AS `admissiondate`',
    '`patient_admin_status` AS `patient_admin_status`',
    '`admission_method` AS `admission_method`',
    '`referring_hospitaltxt` AS `referring_hospitaltxt`',
    '`admission_ward` AS `admission_ward`',
    '`admitting_consultant` AS `admitting_consultant`',
    '`pathway` AS `pathway`',
    '`symptom_onset` AS `symptom_onset`',
    '`call_for_help` AS `call_for_help`',
    '`first_responder` AS `first_responder`',
    '`arrival_of_ambulance` AS `arrival_of_ambulance`',
    '`arrive_dgh` AS `arrive_dgh`',
    '`arrive_here` AS `arrive_here`',
    '`time_of_diagnostic_ecg` AS `time_of_diagnostic_ecg`',
    '`angio_table_time` AS `angio_table_time`',
    '`reperfusion_date_time` AS `reperfusion_date_time`',
    '`local_intervention_date` AS `local_intervention_date`',
    '`referral_for_investigationintervention` AS `referral_for_investigationintervention`',
    '`critical_transfer_call` AS `critical_transfer_call`',
    '`time_left_dgh` AS `time_left_dgh`',
    '`ambulance_trust_codetxt` AS `ambulance_trust_codetxt`',
    '`cad` AS `cad`',
    '`transfer_cad` AS `transfer_cad`',
    '`previous_ami` AS `previous_ami`',
    '`previous_angina` AS `previous_angina`',
    '`hypertension` AS `hypertension`',
    '`hypercholesterolaemia` AS `hypercholesterolaemia`',
    '`peripheral_vascular_disease` AS `peripheral_vascular_disease`',
    '`cerebrovascular_disease` AS `cerebrovascular_disease`',
    '`asthma_copd` AS `asthma_copd`',
    '`chronic_renal_failure` AS `chronic_renal_failure`',
    '`heart_failure` AS `heart_failure`',
    '`previous_pci` AS `previous_pci`',
    '`previous_cabg` AS `previous_cabg`',
    '`family_history_of_chd` AS `family_history_of_chd`',
    '`smoking_status` AS `smoking_status`',
    '`diabetes` AS `diabetes`',
    '`beta_blocker_use` AS `beta_blocker_use`',
    '`ac_eor_ar_buse` AS `ac_eor_ar_buse`',
    '`statin_use` AS `statin_use`',
    '`thienopyridine_inhibitor_use` AS `thienopyridine_inhibitor_use`',
    '`initial_diagnosis` AS `initial_diagnosis`',
    '`high_risk_nstemi` AS `high_risk_nstemi`',
    '`killip_class` AS `killip_class`',
    '`place_first12_lead_ec_gperformed` AS `place_first12_lead_ec_gperformed`',
    '`ec_gdetermining_treatment` AS `ec_gdetermining_treatment`',
    '`ec_gsubcategoriestxt` AS `ec_gsubcategoriestxt`',
    '`qr_sduration` AS `qr_sduration`',
    '`where_was_aspirin_given` AS `where_was_aspirin_given`',
    '`patient_location_at_time_of_stemi` AS `patient_location_at_time_of_stemi`',
    '`patient_ventilated` AS `patient_ventilated`',
    '`cardiogenic_shock` AS `cardiogenic_shock`',
    '`grac_escore` AS `grac_escore`',
    '`serum_glucose` AS `serum_glucose`',
    '`serum_cholesterol` AS `serum_cholesterol`',
    '`creatinine` AS `creatinine`',
    '`haemoglobin` AS `haemoglobin`',
    '`peak_ck` AS `peak_ck`',
    '`peak_troponin` AS `peak_troponin`',
    '`troponin_assay` AS `troponin_assay`',
    '`cardiac_enzymes_raised` AS `cardiac_enzymes_raised`',
    '`systolic_bp` AS `systolic_bp`',
    '`heart_rate` AS `heart_rate`',
    '`lvef` AS `lvef`',
    '`height` AS `height`',
    '`weight` AS `weight`',
    '`assessment_at_non_interventional_hospital` AS `assessment_at_non_interventional_hospital`',
    '`cardiological_care` AS `cardiological_care`',
    '`initial_reperfusion` AS `initial_reperfusion`',
    '`where_was_initial_reperfusion_treatment_given` AS `where_was_initial_reperfusion_treatment_given`',
    '`reason_reperfusion_treatment_not_given` AS `reason_reperfusion_treatment_not_given`',
    '`delay_before_treatment` AS `delay_before_treatment`',
    '`additional_reperfusion` AS `additional_reperfusion`',
    '`assessment_at_interventional_centre` AS `assessment_at_interventional_centre`',
    '`intended_reperfusion_procedure` AS `intended_reperfusion_procedure`',
    '`procedure_performed` AS `procedure_performed`',
    '`why_was_no_angiogram_performed` AS `why_was_no_angiogram_performed`',
    '`why_was_no_intervention_performed` AS `why_was_no_intervention_performed`',
    '`bleeding_complications` AS `bleeding_complications`',
    '`site_of_infarction` AS `site_of_infarction`',
    '`coronary_angiography` AS `coronary_angiography`',
    '`coronary_intervention` AS `coronary_intervention`',
    '`delay_to_performance_of_angiogram` AS `delay_to_performance_of_angiogram`',
    '`exercise_test` AS `exercise_test`',
    '`echocardiography` AS `echocardiography`',
    '`radionuclide_study` AS `radionuclide_study`',
    '`stress_echo` AS `stress_echo`',
    '`cardiac_arrest` AS `cardiac_arrest`',
    '`cardiac_arrest_location` AS `cardiac_arrest_location`',
    '`arrest_presenting_rhythm` AS `arrest_presenting_rhythm`',
    '`outcome_of_arrest` AS `outcome_of_arrest`',
    '`unfractionated_heparin` AS `unfractionated_heparin`',
    '`low_molecular_weight_heparin` AS `low_molecular_weight_heparin`',
    '`thienopyridine_platelet_inhibitor` AS `thienopyridine_platelet_inhibitor`',
    '`iv2b3a_agent` AS `iv2b3a_agent`',
    '`iv_beta_blocker` AS `iv_beta_blocker`',
    '`calcium_channel_blocker` AS `calcium_channel_blocker`',
    '`i_vnitrate` AS `i_vnitrate`',
    '`oral_nitrate` AS `oral_nitrate`',
    '`potassium_channel_modulator` AS `potassium_channel_modulator`',
    '`warfarin` AS `warfarin`',
    '`ac_ei_arb` AS `ac_ei_arb`',
    '`thiazide_diuretic` AS `thiazide_diuretic`',
    '`loop_diuretic` AS `loop_diuretic`',
    '`oral_beta_blocker` AS `oral_beta_blocker`',
    '`aldosterone_antagonist` AS `aldosterone_antagonist`',
    '`fondaparinux` AS `fondaparinux`',
    '`bivalirudin` AS `bivalirudin`',
    '`thrombolytic_drug` AS `thrombolytic_drug`',
    '`inpatient_management_of_hyperglycaemia_diabetes` AS `inpatient_management_of_hyperglycaemia_diabetes`',
    '`start_of_insulin_infusion` AS `start_of_insulin_infusion`',
    '`beta_blockerdischarge` AS `beta_blockerdischarge`',
    '`ace_ior_ar_bdischarge` AS `ace_ior_ar_bdischarge`',
    '`statin_discharge` AS `statin_discharge`',
    '`aspirin_discharge` AS `aspirin_discharge`',
    '`thienopyridine_inhibitor_discharge` AS `thienopyridine_inhibitor_discharge`',
    '`aldosterone_antagonist_discharge` AS `aldosterone_antagonist_discharge`',
    '`ticagrelor_discharge` AS `ticagrelor_discharge`',
    '`diabetic_therapy_at_discharge` AS `diabetic_therapy_at_discharge`',
    '`discharge_diagnosis` AS `discharge_diagnosis`',
    '`other_discharge_diagnosistxt` AS `other_discharge_diagnosistxt`',
    '`discharge_destination` AS `discharge_destination`',
    '`reinfarction` AS `reinfarction`',
    '`death_in_hospital` AS `death_in_hospital`',
    '`followed_up_by` AS `followed_up_by`',
    '`cardiac_rehabilitation` AS `cardiac_rehabilitation`',
    '`smoking_cessation_advice_given` AS `smoking_cessation_advice_given`',
    '`dietary_advice_given` AS `dietary_advice_given`',
    '`date_of_return_to_referring_hospital` AS `date_of_return_to_referring_hospital`',
    '`what_procedure_was_performed_at_the_interventional_hospit` AS `what_procedure_was_performed_at_the_interventional_hospit`',
    '`date_of_discharge` AS `date_of_discharge`',
    '`stress_precipitanttxt` AS `stress_precipitanttxt`',
    '`m_stress_precipitant_other` AS `m_stress_precipitant_other`',
    '`datetime_of_stressful_precipitant` AS `datetime_of_stressful_precipitant`',
    '`regional_lv_dysfunction_distribution` AS `regional_lv_dysfunction_distribution`',
    '`m_regional_lv_dysfunction_distribution_other` AS `m_regional_lv_dysfunction_distribution_other`',
    '`previous_history_of_takotsubo` AS `previous_history_of_takotsubo`',
    '`family_history_of_takotsubo` AS `family_history_of_takotsubo`',
    '`thyroid_disease` AS `thyroid_disease`',
    '`psychiatric_disease` AS `psychiatric_disease`',
    '`hormonal_status` AS `hormonal_status`',
    '`hr_tor_oral_contraceptive` AS `hr_tor_oral_contraceptive`',
    '`androgen_modifying_therapy` AS `androgen_modifying_therapy`',
    '`beta_adrenergic_receptor_agonists` AS `beta_adrenergic_receptor_agonists`',
    '`qtc_interval` AS `qtc_interval`',
    '`atrial_tachycardia_or_fibrillation` AS `atrial_tachycardia_or_fibrillation`',
    '`ventricular_tachycardia_or_fibrillation` AS `ventricular_tachycardia_or_fibrillation`',
    '`left_ventricular_apical_thrombus` AS `left_ventricular_apical_thrombus`',
    '`left_ventricular_outflow_tract_obstruction` AS `left_ventricular_outflow_tract_obstruction`',
    '`bystander_coronary_artery_disease` AS `bystander_coronary_artery_disease`',
    '`lvedp` AS `lvedp`',
    '`cm_rimaging` AS `cm_rimaging`',
    '`positive_inotropic_supporttxt` AS `positive_inotropic_supporttxt`',
    '`intraaortic_balloon_pump_support` AS `intraaortic_balloon_pump_support`',
    '`reminder` AS `reminder`',
    '`lved_pvalue` AS `lved_pvalue`',
    '`dataset_version` AS `dataset_version`',
    '`ambulance_job_number` AS `ambulance_job_number`',
    '`daycase_transfer_dt` AS `daycase_transfer_dt`',
    '`referral_dt_tm` AS `referral_dt_tm`',
    '`interventional_hospitaltxt` AS `interventional_hospitaltxt`',
    '`m_hac_team_audit_notes` AS `m_hac_team_audit_notes`',
    '`data_complete` AS `data_complete`',
    '`cm_rimaging_performed` AS `cm_rimaging_performed`',
    '`ticagrelor_use` AS `ticagrelor_use`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`admission_date_quality` AS `admission_date_quality`',
    '`admission_date_clean` AS `admission_date_clean`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`hospital_identifier` AS `hospital_identifier`',
]

CLINICAL_REGISTRY_ACS_TRANSFER_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the acs_transfer source family and native entry_id.",
    "entry_id": "Internal source registry record identifier for the ACS transfer entry, carried through from the feeder registry table.",
    "mrn": "Local hospital medical record number identifying the patient for this registry record.",
    "gender": "Patient gender as recorded in the source ACS registry record.",
    "date_of_death": "Recorded date of patient death as held in the source registry record.",
    "ethnic_origin": "Patient ethnic origin code as recorded in the source ACS registry.",
    "admissiondate": "Raw hospital admission date/time from the source record, which may contain sentinel or future values flagged by ADMISSION_DATE_QUALITY.",
    "patient_admin_status": "Administrative status of the patient for the admission, such as NHS or private funding category, as recorded in the source registry.",
    "admission_method": "Coded method by which the patient was admitted for this ACS episode, as recorded in the source registry.",
    "referring_hospitaltxt": "Text label of the hospital that referred or transferred the patient, as held in the source registry.",
    "admission_ward": "Ward or clinical area to which the patient was admitted, as recorded in the source registry.",
    "admitting_consultant": "Category or identity of the consultant responsible for the admission, as recorded in the source registry.",
    "pathway": "Care pathway classification for the acute coronary syndrome episode, as recorded in the source registry.",
    "symptom_onset": "Date and time of onset of the patient's cardiac symptoms, as recorded in the source registry.",
    "call_for_help": "Date and time the patient or bystander called for medical help, as recorded in the source registry.",
    "first_responder": "Date and time the first responder attended the patient, as recorded in the source registry.",
    "arrival_of_ambulance": "Date and time of ambulance arrival at the patient, as recorded in the source registry.",
    "arrive_dgh": "Date and time the patient arrived at the district general hospital, as recorded in the source registry.",
    "arrive_here": "Date and time the patient arrived at the reporting hospital, as recorded in the source registry.",
    "time_of_diagnostic_ecg": "Date and time of the ECG that determined the diagnosis, as recorded in the source registry.",
    "angio_table_time": "Date and time the patient was on the angiography table, as recorded in the source registry.",
    "reperfusion_date_time": "Date and time reperfusion treatment was delivered, as recorded in the source registry.",
    "local_intervention_date": "Date and time of coronary intervention performed at the local hospital, as recorded in the source registry.",
    "referral_for_investigationintervention": "Date and time of referral for further investigation or intervention, as recorded in the source registry.",
    "critical_transfer_call": "Date and time of the critical transfer call for the patient, as recorded in the source registry.",
    "time_left_dgh": "Date and time the patient left the district general hospital for transfer, as recorded in the source registry.",
    "ambulance_trust_codetxt": "Decoded text label of the ambulance trust involved in the patient's conveyance, following the txt suffix convention for lookup descriptions in this extract.",
    "cad": "Source-recorded indicator of known coronary artery disease for the patient.",
    "transfer_cad": "Source-recorded coronary artery disease status captured in the transfer section of the record.",
    "previous_ami": "Source-coded indicator of whether the patient has a history of previous acute myocardial infarction.",
    "previous_angina": "Source-coded indicator of whether the patient has a history of previous angina.",
    "hypertension": "Indicates whether the patient has a recorded history of hypertension.",
    "hypercholesterolaemia": "Indicates whether the patient has a recorded history of hypercholesterolaemia.",
    "peripheral_vascular_disease": "Indicates whether the patient has a recorded history of peripheral vascular disease.",
    "cerebrovascular_disease": "Indicates whether the patient has a recorded history of cerebrovascular disease.",
    "asthma_copd": "Indicates whether the patient has a recorded history of asthma or chronic obstructive pulmonary disease.",
    "chronic_renal_failure": "Indicates whether the patient has a recorded history of chronic renal failure.",
    "heart_failure": "Indicates whether the patient has a recorded history of heart failure.",
    "previous_pci": "Indicates whether the patient has undergone previous percutaneous coronary intervention.",
    "previous_cabg": "Indicates whether the patient has undergone previous coronary artery bypass graft surgery.",
    "family_history_of_chd": "Indicates whether the patient has a family history of coronary heart disease.",
    "smoking_status": "Recorded smoking status of the patient at the time of the ACS episode.",
    "diabetes": "Recorded diabetes status of the patient, including non-diabetic and treatment-based categories.",
    "beta_blocker_use": "Indicates whether the patient was taking a beta blocker prior to the ACS admission.",
    "ac_eor_ar_buse": "Indicates whether the patient was taking an ACE inhibitor or angiotensin receptor blocker prior to the ACS admission.",
    "statin_use": "Indicates whether the patient was taking a statin prior to the ACS admission.",
    "thienopyridine_inhibitor_use": "Indicates whether the patient was taking a thienopyridine platelet inhibitor prior to the ACS admission.",
    "initial_diagnosis": "Initial working diagnosis recorded at presentation, retained as the source label.",
    "high_risk_nstemi": "Indicates whether the patient was assessed as having high-risk non-ST-elevation myocardial infarction.",
    "killip_class": "Killip classification of heart failure severity recorded at presentation.",
    "place_first12_lead_ec_gperformed": "Location where the first 12-lead ECG was performed for the patient.",
    "ec_gdetermining_treatment": "Coded ECG finding that determined the treatment given for the acute coronary syndrome episode.",
    "ec_gsubcategoriestxt": "Text label of the ECG subcategory classification recorded alongside the determining ECG finding, blank where not recorded.",
    "qr_sduration": "Recorded QRS duration category from the ECG assessment, held as source-formatted text.",
    "where_was_aspirin_given": "Categorical record of the setting in which aspirin was first administered during the acute episode.",
    "patient_location_at_time_of_stemi": "Categorical record of the patient's care location when the STEMI occurred, including a not-applicable option for non-STEMI presentations.",
    "patient_ventilated": "Indicator of whether the patient required mechanical ventilation during the acute coronary syndrome episode.",
    "cardiogenic_shock": "Indicator of whether cardiogenic shock was present during the acute coronary syndrome episode.",
    "grac_escore": "GRACE risk score recorded for the acute coronary syndrome episode, stored as a string and frequently unpopulated.",
    "serum_glucose": "Recorded serum glucose laboratory result for the acute episode; source units are not documented.",
    "serum_cholesterol": "Recorded serum cholesterol laboratory result for the acute episode; source units are not documented.",
    "creatinine": "Recorded serum creatinine laboratory result for the acute episode; source units are not documented.",
    "haemoglobin": "Recorded haemoglobin laboratory result for the acute episode; source units are not documented.",
    "peak_ck": "Peak creatine kinase laboratory result recorded for the episode; source units are not documented.",
    "peak_troponin": "Peak troponin laboratory result recorded for the episode, interpreted with reference to the assay recorded in TROPONIN_ASSAY; source units are not documented.",
    "troponin_assay": "Categorical record of the troponin assay type used for the recorded troponin result.",
    "cardiac_enzymes_raised": "Indicator of whether cardiac enzyme or troponin levels were raised during the episode.",
    "systolic_bp": "Systolic blood pressure recorded at initial assessment for the episode; source units are not documented.",
    "heart_rate": "Heart rate recorded at initial assessment for the episode; source units are not documented.",
    "lvef": "Categorical banding of left ventricular ejection fraction assessed during the episode, including a not-assessed option.",
    "height": "Patient height recorded for the episode; source units are not documented.",
    "weight": "Patient weight recorded for the episode; source units are not documented.",
    "assessment_at_non_interventional_hospital": "Categorical record of the assessment or care received at the referring non-interventional hospital before transfer.",
    "cardiological_care": "Categorical record of whether the patient received cardiological care during the admission.",
    "initial_reperfusion": "Categorical record of the initial reperfusion treatment given for the acute coronary syndrome episode.",
    "where_was_initial_reperfusion_treatment_given": "Categorical record of the setting in which initial reperfusion treatment was delivered.",
    "reason_reperfusion_treatment_not_given": "Coded MINAP reason why reperfusion treatment was not given for the acute coronary syndrome episode.",
    "delay_before_treatment": "Coded MINAP reason for any delay before reperfusion treatment was given.",
    "additional_reperfusion": "Coded record of any additional reperfusion treatment given after the initial reperfusion attempt.",
    "assessment_at_interventional_centre": "Coded outcome of clinical assessment of the patient at the interventional (PCI) centre.",
    "intended_reperfusion_procedure": "Coded reperfusion procedure that was intended for the patient at the interventional centre.",
    "procedure_performed": "Coded procedure actually performed at the interventional centre for this ACS episode.",
    "why_was_no_angiogram_performed": "Coded reason recorded when no coronary angiogram was performed.",
    "why_was_no_intervention_performed": "Coded reason recorded when no coronary intervention was performed.",
    "bleeding_complications": "Coded bleeding complications recorded during the admission or after intervention.",
    "site_of_infarction": "Coded anatomical site of the myocardial infarction.",
    "coronary_angiography": "Coded status of coronary angiography for the episode, including whether and where it was performed or arranged.",
    "coronary_intervention": "Coded status of coronary intervention for the episode, including whether it was performed or arranged.",
    "delay_to_performance_of_angiogram": "Coded reason for any delay to performance of the coronary angiogram.",
    "exercise_test": "Coded source record of whether an exercise tolerance test was carried out, planned or considered unnecessary during the episode.",
    "echocardiography": "Coded source record of whether echocardiography was carried out, arranged for later or judged unnecessary during the episode.",
    "radionuclide_study": "Coded source record of whether a radionuclide myocardial study was carried out, arranged or judged unnecessary during the episode.",
    "stress_echo": "Coded source record of whether stress echocardiography was carried out, arranged or judged unnecessary during the episode.",
    "cardiac_arrest": "Date and time of cardiac arrest recorded for the episode.",
    "cardiac_arrest_location": "Coded location at which the cardiac arrest occurred, including a no-arrest category, as recorded in the source registry.",
    "arrest_presenting_rhythm": "Coded presenting cardiac rhythm at the time of the cardiac arrest.",
    "outcome_of_arrest": "Coded outcome of the cardiac arrest.",
    "unfractionated_heparin": "Coded indicator of whether unfractionated heparin was given during the acute admission episode.",
    "low_molecular_weight_heparin": "Coded indication of whether low molecular weight heparin was given during the admission.",
    "thienopyridine_platelet_inhibitor": "Coded indication of whether a thienopyridine platelet inhibitor was given during the admission.",
    "iv2b3a_agent": "Coded indication of whether an intravenous glycoprotein IIb/IIIa agent was given during the admission.",
    "iv_beta_blocker": "Coded indicator of whether an intravenous beta blocker was given during the acute admission episode.",
    "calcium_channel_blocker": "Coded indicator of whether a calcium channel blocker was given during the acute admission episode.",
    "i_vnitrate": "Coded indicator of whether an intravenous nitrate was given during the acute admission episode.",
    "oral_nitrate": "Coded indicator of whether an oral nitrate was given during the acute admission episode.",
    "potassium_channel_modulator": "Coded indicator of whether a potassium channel modulator (for example nicorandil) was given during the acute admission episode.",
    "warfarin": "Coded indicator of whether warfarin was given during the acute admission episode.",
    "ac_ei_arb": "Coded indicator of whether an ACE inhibitor or angiotensin receptor blocker was given during the acute admission episode.",
    "thiazide_diuretic": "Coded indicator of whether a thiazide diuretic was given during the acute admission episode.",
    "loop_diuretic": "Coded indicator of whether a loop diuretic was given during the acute admission episode.",
    "oral_beta_blocker": "Coded indicator of whether an oral beta blocker was given during the acute admission episode.",
    "aldosterone_antagonist": "Coded indicator of whether an aldosterone antagonist was given during the acute admission episode.",
    "fondaparinux": "Coded indicator of whether fondaparinux was given during the acute admission episode.",
    "bivalirudin": "Coded indicator of whether bivalirudin was given during the acute admission episode.",
    "thrombolytic_drug": "Coded record of the thrombolytic drug used, where thrombolysis was given for the acute coronary event.",
    "inpatient_management_of_hyperglycaemia_diabetes": "Coded description of how hyperglycaemia or diabetes was managed during the inpatient stay.",
    "start_of_insulin_infusion": "Date and time that an insulin infusion was started during the inpatient stay.",
    "beta_blockerdischarge": "Coded indicator of whether a beta blocker was prescribed at discharge.",
    "ace_ior_ar_bdischarge": "Coded indicator of whether an ACE inhibitor or angiotensin receptor blocker was prescribed at discharge.",
    "statin_discharge": "Coded indicator of whether a statin was prescribed at discharge.",
    "aspirin_discharge": "Coded indicator of whether aspirin was prescribed at discharge.",
    "thienopyridine_inhibitor_discharge": "Coded indicator of whether a thienopyridine platelet inhibitor was prescribed at discharge.",
    "aldosterone_antagonist_discharge": "Coded indicator of whether an aldosterone antagonist was prescribed at discharge.",
    "ticagrelor_discharge": "Coded indicator of whether ticagrelor was prescribed at discharge.",
    "diabetic_therapy_at_discharge": "Coded description of the diabetes therapy in place at discharge.",
    "discharge_diagnosis": "Source-coded discharge diagnosis label for the acute coronary syndrome episode, retained as recorded in the source registry.",
    "other_discharge_diagnosistxt": "Decoded text label of an additional or other discharge diagnosis category, retained as recorded in the source registry.",
    "discharge_destination": "Coded destination category to which the patient was discharged at the end of the acute coronary syndrome admission.",
    "reinfarction": "Source-coded indicator of whether reinfarction occurred during the admission.",
    "death_in_hospital": "Source-coded indicator of whether the patient died during the hospital admission.",
    "followed_up_by": "Coded category describing the clinician or service responsible for follow-up after discharge.",
    "cardiac_rehabilitation": "Source-coded record of whether cardiac rehabilitation was arranged or indicated following the admission.",
    "smoking_cessation_advice_given": "Source-coded record of whether smoking cessation advice was given before discharge.",
    "dietary_advice_given": "Source-coded record of whether dietary advice was given before discharge.",
    "date_of_return_to_referring_hospital": "Date and time the patient was repatriated to the referring district general hospital after intervention.",
    "what_procedure_was_performed_at_the_interventional_hospit": "Source-coded description of the procedure performed at the interventional (receiving) hospital, with the name truncated in the source system.",
    "date_of_discharge": "Date and time of discharge from the admission recorded in this ACS transfer record.",
    "stress_precipitanttxt": "Source text label for the stressful precipitant category recorded in the Takotsubo assessment.",
    "m_stress_precipitant_other": "Free-text description of a stressful precipitant not covered by the coded Takotsubo precipitant categories.",
    "datetime_of_stressful_precipitant": "Date and time of the stressful precipitant event recorded in the Takotsubo assessment.",
    "regional_lv_dysfunction_distribution": "Source-coded distribution pattern of regional left ventricular dysfunction recorded in the Takotsubo assessment.",
    "m_regional_lv_dysfunction_distribution_other": "Free-text description of a regional left ventricular dysfunction distribution not covered by the coded categories.",
    "previous_history_of_takotsubo": "Source-coded indicator of whether the patient has a previous history of Takotsubo syndrome.",
    "family_history_of_takotsubo": "Source-coded indicator of whether there is a family history of Takotsubo syndrome.",
    "thyroid_disease": "Source-coded indicator of thyroid disease recorded as part of the Takotsubo assessment comorbidities.",
    "psychiatric_disease": "Source-coded indicator of psychiatric disease recorded as part of the Takotsubo assessment comorbidities.",
    "hormonal_status": "Source-coded hormonal status recorded as part of the Takotsubo assessment.",
    "hr_tor_oral_contraceptive": "Source-coded indicator of hormone replacement therapy or oral contraceptive use, recorded in the Takotsubo assessment.",
    "androgen_modifying_therapy": "Source-coded indicator of androgen-modifying therapy use recorded in the Takotsubo assessment.",
    "beta_adrenergic_receptor_agonists": "Source-coded indicator of beta-adrenergic receptor agonist use recorded in the Takotsubo assessment.",
    "qtc_interval": "Corrected QT interval recorded from the ECG in the Takotsubo assessment, held as source-formatted text.",
    "atrial_tachycardia_or_fibrillation": "Coded source indicator of whether atrial tachycardia or atrial fibrillation was recorded for the episode.",
    "ventricular_tachycardia_or_fibrillation": "Coded source indicator of whether ventricular tachycardia or ventricular fibrillation was recorded for the episode.",
    "left_ventricular_apical_thrombus": "Coded source indicator of whether left ventricular apical thrombus was identified.",
    "left_ventricular_outflow_tract_obstruction": "Coded source indicator of whether left ventricular outflow tract obstruction was identified.",
    "bystander_coronary_artery_disease": "Coded source indicator of whether incidental (bystander) coronary artery disease was present alongside the presenting condition.",
    "lvedp": "Coded source field recording left ventricular end-diastolic pressure assessment status, with any measured figure held in LVED_PVALUE.",
    "cm_rimaging": "Coded source field describing cardiac magnetic resonance imaging assessment recorded for the episode.",
    "positive_inotropic_supporttxt": "Decoded source text describing positive inotropic support given, following the txt suffix convention for lookup descriptions in this extract.",
    "intraaortic_balloon_pump_support": "Coded source indicator of whether intra-aortic balloon pump support was used during the episode.",
    "reminder": "Free-text reminder or follow-up note entered by data collectors against the registry record.",
    "lved_pvalue": "Recorded numeric left ventricular end-diastolic pressure value as held in the source registry, with no unit asserted in source metadata.",
    "dataset_version": "Version label of the registry dataset specification under which the record was collected.",
    "ambulance_job_number": "Ambulance service job or incident reference recorded for the patient's conveyance.",
    "daycase_transfer_dt": "Date and time of daycase transfer recorded in the source registry record.",
    "referral_dt_tm": "Date and time the referral was made within the acute coronary syndrome transfer pathway.",
    "interventional_hospitaltxt": "Decoded name of the interventional hospital involved in the transfer pathway.",
    "m_hac_team_audit_notes": "Free-text audit notes entered by the heart attack centre team about the registry record.",
    "data_complete": "Source flag indicating whether the registry record has been marked as complete.",
    "cm_rimaging_performed": "Source flag indicating whether cardiac magnetic resonance imaging was performed.",
    "ticagrelor_use": "Source field recording ticagrelor use for the patient as captured in the registry medication section.",
    "date_last_changed": "Timestamp of the last change made to the record in the source registry system.",
    "adc_updt": "Source system extract timestamp indicating when the row was last updated in the upstream feed.",
    "nhs_number_valid_ind": "Derived boolean flag indicating whether the source NHS number passed validity checking; the NHS number itself is not retained in this column.",
    "linkage_status": "Derived code describing the outcome of linking the registry record to a master person record.",
    "person_id": "Internal surrogate person identifier from the source patient administration system used to link the record to the master person record.",
    "linkage_method": "Code describing which identifier combination was used to link this record to the master person identifier during pipeline processing.",
    "linkage_historical_fallback_ind": "Boolean flag indicating that the person linkage was resolved using a historical fallback identifier rather than a current match.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating that the fallback linkage produced a conflicting person match during processing.",
    "admission_date_quality": "VALID | SENTINEL (year <=2000) | FUTURE | MISSING.",
    "admission_date_clean": "Admission date with sentinel/future values nulled; raw ADMISSIONDATE retained.",
    "source_table": "Name of the source system table from which this record was ingested.",
    "source_record_key": "Source system record identifier for this row, taken from the feeder registry entry key.",
    "row_hash": "Hash of the source record's field values used to detect row-level changes between pipeline loads.",
    "pipeline_loaded_at": "Timestamp recording when the pipeline loaded this row into the bronze table.",
    "is_current_in_source": "Boolean flag indicating whether the record was still present as current in the source system at the last load.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the pipeline first detected that the record was no longer present in the source system.",
    "surname": "Patient surname as recorded in the source ACS transfer registry record.",
    "forename": "Patient forename as recorded in the source ACS transfer registry record.",
    "nhs_number": "Patient NHS number as recorded in the source registry, with validity indicated by NHS_NUMBER_VALID_IND.",
    "date_of_birth": "Patient date of birth as recorded in the source ACS transfer registry record.",
    "hospital_identifier": "Hospital identifier recorded against the patient in the source registry record.",
}

CLINICAL_REGISTRY_ACS_TRANSFER_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_acs_transfer.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_acs_transfer"),
    comment="Internal QC of clinical_registry_acs_transfer; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ACS_TRANSFER_MANDATORY_RULES)
def _gold_qc_clinical_registry_acs_transfer():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_acs_transfer"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_ACS_TRANSFER_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_ACS_TRANSFER_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_acs_transfer"),
    comment='Typed child of clinical_registry_entry for the acs_transfer family. MINAP-style ACS transfer pathway: symptom, transfer and reperfusion timestamps, GRACE/Killip, drugs, laboratory results and Takotsubo fields. Diagnoses remain source labels. ADMISSION_DATE_QUALITY identifies the known 2000-01-01 placeholder range. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_acs_transfer():
    """Publish clinical_registry_acs_transfer without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_acs_transfer")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_cardiac_mdt ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_CARDIAC_MDT_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`md_tdate` AS `md_tdate`',
    '`consultant_responsible_for_proceduretxt` AS `consultant_responsible_for_proceduretxt`',
    '`presenting_cliniciantxt` AS `presenting_cliniciantxt`',
    '`other_presenting_clinician` AS `other_presenting_clinician`',
    '`referring_hospital` AS `referring_hospital`',
    '`m_presenting_history` AS `m_presenting_history`',
    '`cardiac_risk_factorstxt` AS `cardiac_risk_factorstxt`',
    '`significant_non_coronary_cardiac_historytxt` AS `significant_non_coronary_cardiac_historytxt`',
    '`mguc_hdetail` AS `mguc_hdetail`',
    '`m_cardiomyopathy_detail` AS `m_cardiomyopathy_detail`',
    '`other_referring_hospitaltxt` AS `other_referring_hospitaltxt`',
    '`patient_preference` AS `patient_preference`',
    '`euroscore` AS `euroscore`',
    '`anginal_medicationtxt` AS `anginal_medicationtxt`',
    '`troponin_ng_l` AS `troponin_ng_l`',
    '`e_gfr_ml_min` AS `e_gfr_ml_min`',
    '`significant_valve_diseasetxt` AS `significant_valve_diseasetxt`',
    '`m_aortic_valve_disease` AS `m_aortic_valve_disease`',
    '`m_mitral_valve_disease` AS `m_mitral_valve_disease`',
    '`m_tricuspid_valve_disease` AS `m_tricuspid_valve_disease`',
    '`m_pulmonary_valve_disease` AS `m_pulmonary_valve_disease`',
    '`viability_assessmenttxt` AS `viability_assessmenttxt`',
    '`mcm_rdetail` AS `mcm_rdetail`',
    '`m_stress_echo_detail` AS `m_stress_echo_detail`',
    '`mmps_detail` AS `mmps_detail`',
    '`syntax_score1` AS `syntax_score1`',
    '`clinical_presentation` AS `clinical_presentation`',
    '`m_clinical_presentation_other` AS `m_clinical_presentation_other`',
    '`smoking_status` AS `smoking_status`',
    '`lvef` AS `lvef`',
    '`cardiology_attendeestxt` AS `cardiology_attendeestxt`',
    '`m_discussion` AS `m_discussion`',
    '`treatment_planned` AS `treatment_planned`',
    '`planned_timing` AS `planned_timing`',
    '`responsible_consultant_speciality` AS `responsible_consultant_speciality`',
    '`responsible_consultant_coronarytxt` AS `responsible_consultant_coronarytxt`',
    '`responsible_consultant_cardiothoracictxt` AS `responsible_consultant_cardiothoracictxt`',
    '`pci` AS `pci`',
    '`cabg` AS `cabg`',
    '`medical_therapy_only` AS `medical_therapy_only`',
    '`m_other_treatment` AS `m_other_treatment`',
    '`other_consultant_and_speciality` AS `other_consultant_and_speciality`',
    '`cardiothoracic_attendeestxt` AS `cardiothoracic_attendeestxt`',
    '`imaging_attendee` AS `imaging_attendee`',
    '`md_tchair` AS `md_tchair`',
    '`current_location` AS `current_location`',
    '`haemoglobin` AS `haemoglobin`',
    '`other_coronary_consultant` AS `other_coronary_consultant`',
    '`mff_rdetail` AS `mff_rdetail`',
    '`primary_operator` AS `primary_operator`',
    '`creatinine_umol_l` AS `creatinine_umol_l`',
    '`treatment_deviationtxt` AS `treatment_deviationtxt`',
    '`m_details_of_deviation` AS `m_details_of_deviation`',
    '`treatment_pathway` AS `treatment_pathway`',
    '`pathway_date` AS `pathway_date`',
    '`cause_of_deviationtxt` AS `cause_of_deviationtxt`',
    '`consultant_informed_dt` AS `consultant_informed_dt`',
    '`audit_entered_bytxt` AS `audit_entered_bytxt`',
    '`audit_dt` AS `audit_dt`',
    '`md_tguide` AS `md_tguide`',
    '`complex_mdt` AS `complex_mdt`',
    '`other_therapy_optionstxt` AS `other_therapy_optionstxt`',
    '`cardiology_consultant_of_the_weektxt` AS `cardiology_consultant_of_the_weektxt`',
    '`cardiology_consultant_otw_present` AS `cardiology_consultant_otw_present`',
    '`cardiothoracic_consultant_of_the_weektxt` AS `cardiothoracic_consultant_of_the_weektxt`',
    '`cardiothoracic_consultant_otw_present` AS `cardiothoracic_consultant_otw_present`',
    '`patient_removed_without_discussion` AS `patient_removed_without_discussion`',
    '`removed_without_discussion_other` AS `removed_without_discussion_other`',
    '`removed_dt` AS `removed_dt`',
    '`e_pattendeestxt` AS `e_pattendeestxt`',
    '`ep_consultant_of_the_weektxt` AS `ep_consultant_of_the_weektxt`',
    '`ep_consultant_of_the_week_present` AS `ep_consultant_of_the_week_present`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`registry_type` AS `registry_type`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`relevant_medicationtxt` AS `relevant_medicationtxt`',
    '`hybrid_surgery` AS `hybrid_surgery`',
    '`open_surgery` AS `open_surgery`',
    '`other_speciality` AS `other_speciality`',
    '`other_consultants_name` AS `other_consultants_name`',
    '`endovascular` AS `endovascular`',
    '`surveillance` AS `surveillance`',
    '`discharge` AS `discharge`',
    '`aetiologytxt` AS `aetiologytxt`',
    '`pathway_dt` AS `pathway_dt`',
    '`presenting_surgeontxt` AS `presenting_surgeontxt`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`m_other_relevant_medication` AS `m_other_relevant_medication`',
    '`other_patient_preference` AS `other_patient_preference`',
    '`palliative_care_only` AS `palliative_care_only`',
    '`outcome_completed` AS `outcome_completed`',
    '`m_other_attendees` AS `m_other_attendees`',
    '`m_other_aetiology` AS `m_other_aetiology`',
    '`imaging_attendee2nd` AS `imaging_attendee2nd`',
    '`surveillance_pathway_patient` AS `surveillance_pathway_patient`',
    '`use_guidelines_threshold` AS `use_guidelines_threshold`',
    '`new_operative_threshold` AS `new_operative_threshold`',
    '`new_surveillance_interval` AS `new_surveillance_interval`',
    '`operative_threshold_met` AS `operative_threshold_met`',
    '`rationale_for_contradictingtxt` AS `rationale_for_contradictingtxt`',
    '`m_other_high_risk` AS `m_other_high_risk`',
    '`m_other_comorbidities` AS `m_other_comorbidities`',
    '`m_other_reason` AS `m_other_reason`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`referring_consultant_email` AS `referring_consultant_email`',
    '`cardiothoracic_consultant_emailtxt` AS `cardiothoracic_consultant_emailtxt`',
    '`cardiologist_consultant_emailtxt` AS `cardiologist_consultant_emailtxt`',
]

CLINICAL_REGISTRY_CARDIAC_MDT_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the cardiac_mdt source family and native entry_id.",
    "entry_id": "Source EntryId, unique only within REGISTRY_TYPE.",
    "mrn": "Local hospital medical record number of the patient the MDT record relates to, carried through from the source iWeb MDT registries.",
    "gender": "Recorded gender of the patient as held in the source iWeb MDT registry.",
    "date_of_death": "Recorded date of death of the patient, where known, as held in the source iWeb MDT registry.",
    "ethnic_origin": "Coded ethnic origin of the patient as recorded in the source iWeb MDT registry.",
    "md_tdate": "Date of the multidisciplinary team meeting at which the case was discussed.",
    "consultant_responsible_for_proceduretxt": "Name of the consultant recorded as responsible for the planned procedure, populated for the coronary revascularisation workflow.",
    "presenting_cliniciantxt": "Name of the clinician who presented the case at the MDT meeting.",
    "other_presenting_clinician": "Free-text name of the presenting clinician where the individual is not available in the standard clinician picklist.",
    "referring_hospital": "Hospital or organisation that referred the patient for MDT discussion, selected from the source picklist.",
    "m_presenting_history": "Free-text narrative of the patient's presenting history and relevant background recorded for the MDT discussion.",
    "cardiac_risk_factorstxt": "Coded list of cardiac risk factors recorded for the patient at MDT, expressed as resolved picklist labels.",
    "significant_non_coronary_cardiac_historytxt": "Coded list of significant non-coronary cardiac history items recorded at MDT, expressed as resolved picklist labels.",
    "mguc_hdetail": "Free-text detail supporting the grown-up congenital heart disease item recorded within the cardiac history.",
    "m_cardiomyopathy_detail": "Free-text detail describing the patient's cardiomyopathy where recorded in the cardiac history.",
    "other_referring_hospitaltxt": "Free-text or picklist-derived name of the referring hospital where the organisation is not available in the standard referral list.",
    "patient_preference": "Recorded treatment preference expressed by the patient to inform the MDT decision.",
    "euroscore": "EuroSCORE operative risk score recorded for the patient at MDT.",
    "anginal_medicationtxt": "Coded list of anti-anginal medications the patient is taking, expressed as resolved picklist labels.",
    "troponin_ng_l": "Recorded troponin result for the patient in nanograms per litre.",
    "e_gfr_ml_min": "Recorded estimated glomerular filtration rate for the patient in millilitres per minute.",
    "significant_valve_diseasetxt": "Coded list of valves affected by significant valve disease, expressed as resolved picklist labels.",
    "m_aortic_valve_disease": "Free-text detail describing the patient's aortic valve disease where recorded.",
    "m_mitral_valve_disease": "Free-text detail describing the patient's mitral valve disease where recorded.",
    "m_tricuspid_valve_disease": "Free-text detail describing the patient's tricuspid valve disease where recorded.",
    "m_pulmonary_valve_disease": "Recorded assessment or description of pulmonary valve disease for the patient at the cardiac MDT.",
    "viability_assessmenttxt": "Coded text describing the myocardial viability assessment method recorded at the MDT.",
    "mcm_rdetail": "Free-text detail of the cardiac magnetic resonance findings considered at the MDT.",
    "m_stress_echo_detail": "Free-text detail of stress echocardiography findings recorded for the MDT assessment.",
    "mmps_detail": "Free-text detail of the myocardial perfusion scan findings recorded for the MDT assessment.",
    "syntax_score1": "SYNTAX score recorded for the patient's coronary anatomy at the MDT.",
    "clinical_presentation": "Coded category describing the patient's clinical presentation at referral to the MDT.",
    "m_clinical_presentation_other": "Free-text description of the clinical presentation when the coded presentation category is recorded as other.",
    "smoking_status": "Coded smoking status recorded as part of the cardiac risk factor assessment for the MDT.",
    "lvef": "Left ventricular ejection fraction value recorded for the patient at the MDT assessment.",
    "cardiology_attendeestxt": "Coded text list of the cardiology clinicians recorded as attending the MDT meeting.",
    "m_discussion": "Free-text narrative of the MDT discussion and clinical reasoning for the patient.",
    "treatment_planned": "Coded category describing the treatment setting or pathway planned at the MDT.",
    "planned_timing": "Coded urgency or timing category for the treatment planned at the MDT.",
    "responsible_consultant_speciality": "Coded speciality of the consultant designated as responsible for the patient's planned treatment.",
    "responsible_consultant_coronarytxt": "Name of the coronary (cardiology) consultant recorded as responsible for the patient's planned treatment following the MDT decision, held as decoded picklist text.",
    "responsible_consultant_cardiothoracictxt": "Name of the cardiothoracic surgical consultant recorded as responsible for the patient's planned treatment following the MDT decision, held as decoded picklist text.",
    "pci": "MDT recommendation status for percutaneous coronary intervention as a treatment option.",
    "cabg": "MDT recommendation status for coronary artery bypass grafting as a treatment option.",
    "medical_therapy_only": "MDT recommendation status for medical therapy alone as the treatment option.",
    "m_other_treatment": "Free-text description of treatment agreed at the MDT that is not covered by the coded treatment options.",
    "other_consultant_and_speciality": "Free-text record of an additional consultant and their speciality involved in the MDT decision.",
    "cardiothoracic_attendeestxt": "Decoded list of the cardiothoracic surgical clinicians recorded as attending the MDT meeting.",
    "imaging_attendee": "Imaging clinician recorded as attending the MDT meeting, entered as free text.",
    "md_tchair": "Clinician recorded as chairing the MDT meeting.",
    "current_location": "Coded description of the patient's care setting or location at the time of the MDT discussion.",
    "haemoglobin": "Haemoglobin result recorded for the MDT assessment; units are not stated in the source metadata.",
    "other_coronary_consultant": "Free-text name of an additional coronary consultant recorded against the coronary revascularisation MDT record.",
    "mff_rdetail": "Free-text detail of the fractional flow reserve assessment recorded in the coronary MDT record.",
    "primary_operator": "Clinician recorded as the primary operator for the planned procedure, held as free text and sometimes containing placeholder entries.",
    "creatinine_umol_l": "Serum creatinine result in micromoles per litre recorded for the MDT assessment.",
    "treatment_deviationtxt": "Lookup label indicating whether the treatment delivered deviated from the MDT recommendation.",
    "m_details_of_deviation": "Free-text description of how the delivered treatment deviated from the MDT recommendation.",
    "treatment_pathway": "Coded description of the treatment pathway followed relative to the MDT recommendation.",
    "pathway_date": "Date recorded against the treatment pathway for the MDT record.",
    "cause_of_deviationtxt": "Lookup label giving the recorded cause of deviation from the MDT-recommended treatment.",
    "consultant_informed_dt": "Date and time the responsible consultant was informed in relation to the MDT record.",
    "audit_entered_bytxt": "Name of the member of staff who entered the audit record for this MDT entry.",
    "audit_dt": "Date and time the audit entry was recorded for this MDT record.",
    "md_tguide": "Indicator recorded in the coronary MDT record referencing the MDT guidance applied to the discussion.",
    "complex_mdt": "Categorical indicator of the type of MDT meeting in which the case was discussed.",
    "other_therapy_optionstxt": "Recorded label describing other therapy options considered by the MDT.",
    "cardiology_consultant_of_the_weektxt": "Name of the cardiology consultant of the week associated with the MDT meeting.",
    "cardiology_consultant_otw_present": "Indicator of whether the cardiology consultant of the week was present at the MDT meeting.",
    "cardiothoracic_consultant_of_the_weektxt": "Name of the cardiothoracic consultant of the week associated with the MDT meeting.",
    "cardiothoracic_consultant_otw_present": "Indicator of whether the cardiothoracic consultant of the week was present at the MDT meeting.",
    "patient_removed_without_discussion": "Indicator or reason recorded when the patient was removed from the MDT list without being discussed.",
    "removed_without_discussion_other": "Free-text detail of the other reason the patient was removed from the MDT list without discussion.",
    "removed_dt": "Date and time the patient was removed from the MDT list.",
    "e_pattendeestxt": "Recorded electrophysiology attendees at the MDT meeting.",
    "ep_consultant_of_the_weektxt": "Name of the electrophysiology consultant of the week associated with the MDT meeting, held as decoded text.",
    "ep_consultant_of_the_week_present": "Indicator of whether the electrophysiology consultant of the week was present at the MDT meeting.",
    "date_last_changed": "Date and time the record was last changed in the source iWeb MDT registry.",
    "adc_updt": "Timestamp of the last update applied to the record by the upstream data ingestion process.",
    "nhs_number_valid_ind": "Derived boolean indicating whether the NHS number held on the source MDT record passed validation, without exposing the number itself.",
    "linkage_status": "Coded outcome of linking the MDT record to a master patient record, indicating which identifiers matched.",
    "person_id": "Internal Millennium person identifier linked to this MDT record via the person alias table.",
    "linkage_method": "Code describing which identifier combination was used to link the MDT record to the master patient record.",
    "linkage_historical_fallback_ind": "Indicator that patient linkage was achieved using a historical identifier fallback rather than a current identifier match.",
    "linkage_fallback_conflict_ind": "Indicator that the fallback linkage produced a conflicting patient match during processing.",
    "registry_type": "Source workflow discriminator and part of the composite key.",
    "source_table": "Name of the source iWeb registry table from which the harmonised record was derived.",
    "source_record_key": "Identifier of the originating source registry record, derived from the source EntryId and used with REGISTRY_TYPE to identify the record uniquely.",
    "relevant_medicationtxt": "Relevant medication recorded at the MDT for the CT/aortic surveillance workflow, held as decoded text.",
    "hybrid_surgery": "Indicator that hybrid surgery was the treatment decided at the CT/aortic MDT.",
    "open_surgery": "Indicator that open surgery was the treatment decided at the CT/aortic MDT.",
    "other_speciality": "Free-text specialty of another clinician involved in or attending the CT/aortic MDT discussion.",
    "other_consultants_name": "Name of another consultant involved in the CT/aortic MDT discussion, entered as free text.",
    "endovascular": "Indicator that endovascular treatment was the option decided at the CT/aortic MDT.",
    "surveillance": "Indicator that continued surveillance was the outcome decided at the CT/aortic MDT.",
    "discharge": "Indicator that discharge from the CT/aortic MDT pathway was the outcome decided.",
    "aetiologytxt": "Recorded aetiology of the condition discussed at the CT/aortic MDT, held as decoded selection text.",
    "pathway_dt": "Date and time recorded for the CT/aortic MDT treatment pathway milestone.",
    "presenting_surgeontxt": "Named surgeon who presented the case at the CT/aortic MDT, held as decoded text.",
    "consultant_responsible_for_procedure": "Consultant identified as responsible for the planned procedure following the CT/aortic MDT decision.",
    "m_other_relevant_medication": "Free-text description of other relevant medication recorded at the CT/aortic MDT, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "other_patient_preference": "Free-text detail of the patient's stated treatment preference where not covered by the coded PATIENT_PREFERENCE options, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "palliative_care_only": "Indicates whether the MDT treatment decision was palliative care only, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "outcome_completed": "Indicates whether the MDT outcome record has been completed, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "m_other_attendees": "Free-text record of additional MDT attendees not captured in the specialty attendee fields, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "m_other_aetiology": "Free-text description of aetiology where not covered by the coded AETIOLOGYTXT options, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "imaging_attendee2nd": "Second imaging clinician recorded as attending the MDT meeting, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "surveillance_pathway_patient": "Indicates whether the patient is managed on the aortic surveillance pathway, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "use_guidelines_threshold": "Indicates whether the standard guideline operative threshold was applied rather than a locally agreed threshold, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "new_operative_threshold": "Revised operative threshold agreed at the MDT for this patient, populated only for REGISTRY_TYPE=CT_AORTIC; no unit is stated in source metadata.",
    "new_surveillance_interval": "Revised surveillance imaging interval agreed at the MDT, populated only for REGISTRY_TYPE=CT_AORTIC; no unit is stated in source metadata.",
    "operative_threshold_met": "Indicates whether the applicable operative threshold was met at the time of MDT review, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "rationale_for_contradictingtxt": "Free-text rationale for a decision that contradicts the guideline threshold or recommendation, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "m_other_high_risk": "Free-text description of other high-risk features considered at the MDT, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "m_other_comorbidities": "Free-text description of other comorbidities not captured in the coded history fields, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "m_other_reason": "Free-text description of another reason recorded for the MDT decision or pathway deviation, populated only for REGISTRY_TYPE=CT_AORTIC.",
    "row_hash": "Hash of the source record's business columns used to detect changes between pipeline loads.",
    "pipeline_loaded_at": "Timestamp when the record was loaded into this table by the ingestion pipeline.",
    "is_current_in_source": "Boolean flag indicating whether the record is still present as the current version in the source system.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recorded by the ingestion pipeline when the record was first detected as no longer present in the source iWeb registry.",
    "surname": "Surname of the patient the MDT record relates to, carried through from the source iWeb MDT registries.",
    "forename": "Forename of the patient the MDT record relates to, carried through from the source iWeb MDT registries.",
    "nhs_number": "NHS number of the patient the MDT record relates to, carried through from the source iWeb MDT registries.",
    "date_of_birth": "Date of birth of the patient the MDT record relates to, carried through from the source iWeb MDT registries.",
    "referring_consultant_email": "Email address recorded for the consultant who referred the patient for MDT discussion, as held in the source iWeb MDT registries.",
    "cardiothoracic_consultant_emailtxt": "Email contact recorded for the cardiothoracic consultant associated with the MDT record, held as decoded picklist text in the CT/aortic workflow.",
    "cardiologist_consultant_emailtxt": "Email contact recorded for the cardiology consultant associated with the MDT record, held as decoded picklist text in the CT/aortic workflow.",
}

CLINICAL_REGISTRY_CARDIAC_MDT_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_cardiac_mdt.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_cardiac_mdt"),
    comment="Internal QC of clinical_registry_cardiac_mdt; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CARDIAC_MDT_MANDATORY_RULES)
def _gold_qc_clinical_registry_cardiac_mdt():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_cardiac_mdt"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CARDIAC_MDT_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_CARDIAC_MDT_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_cardiac_mdt"),
    comment='Typed child of clinical_registry_entry for the cardiac_mdt family. Harmonised iWeb cardiac MDT record. REGISTRY_TYPE=CORONARY_REVASC identifies the coronary revascularisation workflow; REGISTRY_TYPE=CT_AORTIC identifies CT/aortic surveillance. Workflow-specific columns are null for the other registry. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_cardiac_mdt():
    """Publish clinical_registry_cardiac_mdt without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_cardiac_mdt")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_coronary_lesion ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_CORONARY_LESION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`parent_entry_id` AS `parent_entry_id`',
    '`lesion` AS `lesion`',
    '`chronic_lesion` AS `chronic_lesion`',
    '`restenosis` AS `restenosis`',
    '`instent_stenoses` AS `instent_stenoses`',
    '`drugs_eluted_by_stentstxt` AS `drugs_eluted_by_stentstxt`',
    '`stenosispre_pci` AS `stenosispre_pci`',
    '`stenosis_post_pci` AS `stenosis_post_pci`',
    '`in_graft_stenosis` AS `in_graft_stenosis`',
    '`successful` AS `successful`',
    '`number_of_stents_used` AS `number_of_stents_used`',
    '`largest_ballon_diameter` AS `largest_ballon_diameter`',
    '`largest_stent_diameter` AS `largest_stent_diameter`',
    '`length_of_segment_treated` AS `length_of_segment_treated`',
    '`indication_for_stent` AS `indication_for_stent`',
    '`stents_usedtxt` AS `stents_usedtxt`',
    '`lms_protected` AS `lms_protected`',
    '`number_of_drug_eluting_stents_used` AS `number_of_drug_eluting_stents_used`',
    '`diagnostic_devices_used_during_proceduretxt` AS `diagnostic_devices_used_during_proceduretxt`',
    '`minimal_reference_diameter` AS `minimal_reference_diameter`',
    '`minimal_lumen_diameter` AS `minimal_lumen_diameter`',
    '`distal_vessel_diameter` AS `distal_vessel_diameter`',
    '`proximal_vessel_diameter` AS `proximal_vessel_diameter`',
    '`fractional_flow_reserve` AS `fractional_flow_reserve`',
    '`cath_proceduretxt` AS `cath_proceduretxt`',
    '`coronary_dominance` AS `coronary_dominance`',
    '`brachytherapy_devices_usedtxt` AS `brachytherapy_devices_usedtxt`',
    '`emboli_protection_devices_usedtxt` AS `emboli_protection_devices_usedtxt`',
    '`graft_lesion_position` AS `graft_lesion_position`',
    '`athero_thrombus_removal_devices_usedtxt` AS `athero_thrombus_removal_devices_usedtxt`',
    '`procedural_devices_usedtxt` AS `procedural_devices_usedtxt`',
    '`is_there_a_graft_to_this_vessel` AS `is_there_a_graft_to_this_vessel`',
    '`graft_typetxt` AS `graft_typetxt`',
    '`distal_anastomosis` AS `distal_anastomosis`',
    '`graft_conduit` AS `graft_conduit`',
    '`proceed_to_pci` AS `proceed_to_pci`',
    '`bifurcation` AS `bifurcation`',
    '`medina_classification` AS `medina_classification`',
    '`wire_usedtxt` AS `wire_usedtxt`',
    '`pre_dilation_balloon_diameter` AS `pre_dilation_balloon_diameter`',
    '`pre_dilation_balloon_length` AS `pre_dilation_balloon_length`',
    '`post_dilation_balloon_length` AS `post_dilation_balloon_length`',
    '`post_dilation_balloon_dilation` AS `post_dilation_balloon_dilation`',
    '`n_cpre_dilation` AS `n_cpre_dilation`',
    '`n_cpost_dilation` AS `n_cpost_dilation`',
    '`import_child_id` AS `import_child_id`',
    '`m_comment_on_lesion` AS `m_comment_on_lesion`',
    '`guide_cathetertxt` AS `guide_cathetertxt`',
    '`catheter_size` AS `catheter_size`',
    '`guide_extension_used` AS `guide_extension_used`',
    '`guide_extensiontxt` AS `guide_extensiontxt`',
    '`other_guide_extension` AS `other_guide_extension`',
    '`guide_extension_size` AS `guide_extension_size`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
]

CLINICAL_REGISTRY_CORONARY_LESION_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the coronary_lesion source family and native entry_id.",
    "entry_id": "Source registry identifier for the individual lesion or vessel sub-procedure record, carried through from the iWeb coronary sub-procedure table.",
    "mrn": "Local medical record number of the patient whose coronary lesion record this is, as held in the iWeb registry and used for linkage to the master patient index.",
    "gender": "Patient gender as recorded in the iWeb coronary registry.",
    "date_of_death": "Recorded date of death for the patient as held in the iWeb coronary registry.",
    "ethnic_origin": "Coded patient ethnic origin as recorded in the iWeb coronary registry.",
    "parent_entry_id": "Foreign key to the currently un-landed iWeb coronary procedure registry.",
    "lesion": "Coronary vessel segment or lesion location treated or assessed in this sub-procedure record.",
    "chronic_lesion": "Indicator of whether the lesion is a chronic total or long-standing occlusion.",
    "restenosis": "Indicator of whether the lesion represents restenosis of a previously treated site.",
    "instent_stenoses": "Indicator of whether the stenosis is located within a previously implanted stent.",
    "drugs_eluted_by_stentstxt": "Coded text description of the drug-eluting stent product or eluted drug recorded for the lesion.",
    "stenosispre_pci": "Banded percentage stenosis of the lesion assessed before percutaneous coronary intervention.",
    "stenosis_post_pci": "Banded percentage residual stenosis of the lesion assessed after percutaneous coronary intervention.",
    "in_graft_stenosis": "Indicator of whether the stenosis is located within a bypass graft.",
    "successful": "Indicator of whether the intervention on this lesion was recorded as successful.",
    "number_of_stents_used": "Number of stents deployed in this lesion, held as text in the source registry.",
    "largest_ballon_diameter": "Largest balloon diameter used when treating this lesion, as recorded in the registry (source spelling of 'ballon' retained).",
    "largest_stent_diameter": "Largest stent diameter deployed in this lesion, as recorded in the registry.",
    "length_of_segment_treated": "Recorded length of the coronary segment treated for this lesion.",
    "indication_for_stent": "Recorded clinical indication for stent use in this lesion.",
    "stents_usedtxt": "Coded text description of the stent products used in this lesion.",
    "lms_protected": "Indicator of whether the left main stem lesion was protected by a patent bypass graft.",
    "number_of_drug_eluting_stents_used": "Number of drug-eluting stents deployed in this lesion, held as text in the source registry.",
    "diagnostic_devices_used_during_proceduretxt": "Coded text description of diagnostic devices used during the procedure for this lesion.",
    "minimal_reference_diameter": "Minimal reference vessel diameter measured for this lesion, as recorded in the registry.",
    "minimal_lumen_diameter": "Angiographic minimal lumen diameter measured at the treated lesion, as recorded in the iWeb coronary registry.",
    "distal_vessel_diameter": "Angiographic diameter of the vessel distal to the lesion, as recorded in the iWeb coronary registry.",
    "proximal_vessel_diameter": "Angiographic diameter of the vessel proximal to the lesion, as recorded in the iWeb coronary registry.",
    "fractional_flow_reserve": "Fractional flow reserve physiology measurement recorded for the lesion or vessel.",
    "cath_proceduretxt": "Coded text description of the catheter laboratory procedure performed for this lesion or vessel record.",
    "coronary_dominance": "Coded classification of coronary circulation dominance recorded at angiography.",
    "brachytherapy_devices_usedtxt": "Coded text listing brachytherapy devices used during the lesion treatment, including a none option.",
    "emboli_protection_devices_usedtxt": "Coded text listing embolic protection devices used during the lesion treatment, including a none option.",
    "graft_lesion_position": "Coded position of the lesion within the bypass graft.",
    "athero_thrombus_removal_devices_usedtxt": "Coded text listing atherectomy or thrombus removal devices used during the lesion treatment, including a none option.",
    "procedural_devices_usedtxt": "Coded text listing other procedural devices used during the lesion treatment, including a none option.",
    "is_there_a_graft_to_this_vessel": "Indicator of whether a bypass graft supplies the vessel being described.",
    "graft_typetxt": "Coded text describing the type of bypass graft associated with this vessel.",
    "distal_anastomosis": "Coded description of the distal anastomosis site of the graft to the coronary vessel.",
    "graft_conduit": "Coded description of the conduit used for the bypass graft to this vessel.",
    "proceed_to_pci": "Indicator of whether the lesion proceeded to percutaneous coronary intervention.",
    "bifurcation": "Indicator of whether the treated lesion involves a coronary bifurcation.",
    "medina_classification": "Medina classification of the bifurcation lesion.",
    "wire_usedtxt": "Coded text describing the guidewire used for the lesion.",
    "pre_dilation_balloon_diameter": "Diameter of the balloon used for pre-dilation of the lesion, as recorded in the registry.",
    "pre_dilation_balloon_length": "Length of the balloon used for pre-dilation of the lesion, as recorded in the registry.",
    "post_dilation_balloon_length": "Length of the balloon used for post-dilation of the lesion, as recorded in the registry.",
    "post_dilation_balloon_dilation": "Diameter of the balloon used for post-dilation of the lesion, as recorded in the registry.",
    "n_cpre_dilation": "Indicator of whether a non-compliant balloon was used for pre-dilation of the lesion.",
    "n_cpost_dilation": "Indicator of whether a non-compliant balloon was used for post-dilation of the lesion.",
    "import_child_id": "Identifier of the child record carried over from a data import into the iWeb coronary registry.",
    "m_comment_on_lesion": "Free-text clinical comment recorded about the lesion or its treatment in the iWeb coronary registry.",
    "guide_cathetertxt": "Coded text describing the guide catheter used for the lesion, as recorded in the iWeb coronary registry.",
    "catheter_size": "Recorded size of the guide catheter used for the lesion, held as text in the source registry.",
    "guide_extension_used": "Indicator of whether a guide extension catheter was used during treatment of the lesion.",
    "guide_extensiontxt": "Coded text describing the guide extension catheter used for the lesion, as recorded in the iWeb coronary registry.",
    "other_guide_extension": "Free-text description of a guide extension device not available in the coded guide extension list.",
    "guide_extension_size": "Recorded size of the guide extension catheter used for the lesion, held as text in the source registry.",
    "date_last_changed": "Source edit timestamp; it must not be interpreted as the procedure date.",
    "adc_updt": "Timestamp of the last update applied to the record in the upstream iWeb extract.",
    "nhs_number_valid_ind": "Flag indicating whether the source NHS number passed validation checks during linkage processing.",
    "linkage_status": "Outcome status of the patient linkage process matching this registry record to a Millennium person record.",
    "person_id": "Internal Millennium person identifier linked to this registry record.",
    "linkage_method": "Identifier combination used to link this registry record to the Millennium person record.",
    "linkage_historical_fallback_ind": "Flag indicating that linkage was achieved using a historical rather than current person identifier.",
    "linkage_fallback_conflict_ind": "Flag indicating that fallback linkage produced conflicting person matches for this record.",
    "source_table": "Name of the upstream source table from which the record was ingested.",
    "source_record_key": "Business key identifying the source registry record, derived from the iWeb sub-procedure entry identifier.",
    "row_hash": "Hash of the source record's field values used to detect changes between loads.",
    "pipeline_loaded_at": "Timestamp recording when the pipeline loaded this record into the bronze table.",
    "is_current_in_source": "Flag indicating whether the record is still present as a current row in the source system.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the pipeline first detected that this record was no longer present in the upstream iWeb source.",
    "surname": "Patient surname as recorded in the iWeb coronary registry and used for patient linkage.",
    "forename": "Patient forename as recorded in the iWeb coronary registry and used for patient linkage.",
    "nhs_number": "Patient NHS number as held in the iWeb coronary registry, used with the validation flag for linkage to the Millennium person record.",
    "date_of_birth": "Patient date of birth as recorded in the iWeb coronary registry and used for patient linkage.",
}

CLINICAL_REGISTRY_CORONARY_LESION_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_coronary_lesion.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_coronary_lesion"),
    comment="Internal QC of clinical_registry_coronary_lesion; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CORONARY_LESION_MANDATORY_RULES)
def _gold_qc_clinical_registry_coronary_lesion():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_coronary_lesion"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CORONARY_LESION_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_CORONARY_LESION_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_coronary_lesion"),
    comment='Typed child of clinical_registry_entry for the coronary_lesion family. Per-lesion/per-vessel PCI and angiography detail from the iWeb coronary registry. The parent coronary procedure registry is not landed, so PARENT_ENTRY_ID is retained for future linkage but currently has no bronze parent and there is no reliable procedure date. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_coronary_lesion():
    """Publish clinical_registry_coronary_lesion without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_coronary_lesion")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_coronary_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_CORONARY_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`pre_date_last_changed` AS `pre_date_last_changed`',
    '`pre_adc_updt` AS `pre_adc_updt`',
    '`pre_gender` AS `pre_gender`',
    '`pre_date_of_death` AS `pre_date_of_death`',
    '`pre_ethnic_origin` AS `pre_ethnic_origin`',
    '`pre_dateandtimeofoperation` AS `pre_dateandtimeofoperation`',
    '`pre_age` AS `pre_age`',
    '`pre_sex` AS `pre_sex`',
    '`pre_patient_ethnic_group` AS `pre_patient_ethnic_group`',
    '`pre_administrative_category` AS `pre_administrative_category`',
    '`pre_clinical_syndromepci` AS `pre_clinical_syndromepci`',
    '`pre_indication_for_intervention` AS `pre_indication_for_intervention`',
    '`pre_procedure_urgency` AS `pre_procedure_urgency`',
    '`pre_cardiogenic_shockpre_procedure` AS `pre_cardiogenic_shockpre_procedure`',
    '`pre_ccs_angina_statuspre_procedure_stable_only` AS `pre_ccs_angina_statuspre_procedure_stable_only`',
    '`pre_nyha_dyspnoea_statuspre_procedure_stable_only` AS `pre_nyha_dyspnoea_statuspre_procedure_stable_only`',
    '`pre_datetime_arrival_at_first_hospitalacs_only` AS `pre_datetime_arrival_at_first_hospitalacs_only`',
    '`pre_admission_routeacs_only` AS `pre_admission_routeacs_only`',
    '`pre_presenting_ecgacs_only` AS `pre_presenting_ecgacs_only`',
    '`pre_recent_lysisacs_only` AS `pre_recent_lysisacs_only`',
    '`pre_cardiac_enzymesmarkers_raised` AS `pre_cardiac_enzymesmarkers_raised`',
    '`pre_previous_mi` AS `pre_previous_mi`',
    '`pre_previous_cabg` AS `pre_previous_cabg`',
    '`pre_previous_pci` AS `pre_previous_pci`',
    '`pre_diabetes` AS `pre_diabetes`',
    '`pre_height` AS `pre_height`',
    '`pre_weight` AS `pre_weight`',
    '`pre_body_surface_area` AS `pre_body_surface_area`',
    '`pre_body_mass_index` AS `pre_body_mass_index`',
    '`pre_lv_ejection_fraction_category` AS `pre_lv_ejection_fraction_category`',
    '`pre_lv_ejection_fraction` AS `pre_lv_ejection_fraction`',
    '`pre_consultant_responsible_for_proceduretxt` AS `pre_consultant_responsible_for_proceduretxt`',
    '`pre_primary_operatortxt` AS `pre_primary_operatortxt`',
    '`pre_primary_operator_status` AS `pre_primary_operator_status`',
    '`pre_second_operatortxt` AS `pre_second_operatortxt`',
    '`pre_second_operator_status` AS `pre_second_operator_status`',
    '`pre_third_operatortxt` AS `pre_third_operatortxt`',
    '`pre_third_operator_status` AS `pre_third_operator_status`',
    '`pre_gp_iibiiia_drugs_used_during_proceduretxt` AS `pre_gp_iibiiia_drugs_used_during_proceduretxt`',
    '`pre_emboli_protection_devices_usedtxt` AS `pre_emboli_protection_devices_usedtxt`',
    '`pre_circulatory_supporttxt` AS `pre_circulatory_supporttxt`',
    '`pre_local_procedure_identifier` AS `pre_local_procedure_identifier`',
    '`pre_cholesterol` AS `pre_cholesterol`',
    '`pre_smoking_history` AS `pre_smoking_history`',
    '`pre_family_history_of_cad` AS `pre_family_history_of_cad`',
    '`pre_medical_historytxt` AS `pre_medical_historytxt`',
    '`pre_ventilated_preop` AS `pre_ventilated_preop`',
    '`pre_q_wave_on_ecg` AS `pre_q_wave_on_ecg`',
    '`pre_ecg_ischaemia` AS `pre_ecg_ischaemia`',
    '`pre_follow_onad_hoc_procedure` AS `pre_follow_onad_hoc_procedure`',
    '`pre_training_procedure` AS `pre_training_procedure`',
    '`pre_research_procedure` AS `pre_research_procedure`',
    '`pre_research_title` AS `pre_research_title`',
    '`pre_arterial_accesstxt` AS `pre_arterial_accesstxt`',
    '`pre_why_no_iibiiia_during_procedure` AS `pre_why_no_iibiiia_during_procedure`',
    '`pre_surgical_cover` AS `pre_surgical_cover`',
    '`pre_date_time_arrival_at_pci_hospital` AS `pre_date_time_arrival_at_pci_hospital`',
    '`pre_date_time_of_call_for_helpstemi` AS `pre_date_time_of_call_for_helpstemi`',
    '`pre_referring_hospitaltxt` AS `pre_referring_hospitaltxt`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway` AS `pre_datetime_of_ecg_triggering_ppci_pathway`',
    '`pre_patient_location_at_time_of_stemi_onset_raw` AS `pre_patient_location_at_time_of_stemi_onset_raw`',
    '`pre_patient_location_at_time_of_stemi_onset_ts` AS `pre_patient_location_at_time_of_stemi_onset_ts`',
    '`pre_serum_creatinine` AS `pre_serum_creatinine`',
    '`pre_datetime_of_symptom_onsetpci_acs_only` AS `pre_datetime_of_symptom_onsetpci_acs_only`',
    '`pre_echo_report` AS `pre_echo_report`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_ph` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_ph`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate`',
    '`pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess` AS `pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess`',
    '`pre_glasgow_coma_scale_on_arrival_in_cath_lab` AS `pre_glasgow_coma_scale_on_arrival_in_cath_lab`',
    '`pre_other_therapeutic_hypothermia` AS `pre_other_therapeutic_hypothermia`',
    '`pre_therapeutic_hypothermiatxt` AS `pre_therapeutic_hypothermiatxt`',
    '`pre_cath_proceduretxt` AS `pre_cath_proceduretxt`',
    '`pre_out_of_hospital_cardiac_arrest` AS `pre_out_of_hospital_cardiac_arrest`',
    '`pre_presumed_date_time_of_arrest` AS `pre_presumed_date_time_of_arrest`',
    '`pre_coronary_dominance` AS `pre_coronary_dominance`',
    '`pre_m_procedure_findings` AS `pre_m_procedure_findings`',
    '`pre_arterial_managementtxt` AS `pre_arterial_managementtxt`',
    '`pre_drug_therapy_preproceduretxt` AS `pre_drug_therapy_preproceduretxt`',
    '`pre_referring_cardiologisttxt` AS `pre_referring_cardiologisttxt`',
    '`pre_history_of_renal_disease1` AS `pre_history_of_renal_disease1`',
    '`pre_temporary_pacing_wire` AS `pre_temporary_pacing_wire`',
    '`pre_cath_devicestxt` AS `pre_cath_devicestxt`',
    '`pre_cath_lab_number` AS `pre_cath_lab_number`',
    '`pre_date_and_time_on_table` AS `pre_date_and_time_on_table`',
    '`pre_ventilation` AS `pre_ventilation`',
    '`pre_hospital_identifier` AS `pre_hospital_identifier`',
    '`pre_date_time_shortcuts_raw` AS `pre_date_time_shortcuts_raw`',
    '`pre_date_time_shortcuts_ts` AS `pre_date_time_shortcuts_ts`',
    '`pre_m_admission_history` AS `pre_m_admission_history`',
    '`pre_diabetes_type` AS `pre_diabetes_type`',
    '`pre_interventions` AS `pre_interventions`',
    '`pre_convalescent_stem_ior_nstemi` AS `pre_convalescent_stem_ior_nstemi`',
    '`pre_us_sguided` AS `pre_us_sguided`',
    '`pre_screen_of_left_femoral_head` AS `pre_screen_of_left_femoral_head`',
    '`pre_left_femoral_micropuncture` AS `pre_left_femoral_micropuncture`',
    '`pre_left_height_of_puncture` AS `pre_left_height_of_puncture`',
    '`pre_height_of_puncture_diagram` AS `pre_height_of_puncture_diagram`',
    '`pre_right_femoral_micropuncture` AS `pre_right_femoral_micropuncture`',
    '`pre_right_height_of_puncture` AS `pre_right_height_of_puncture`',
    '`pre_left_femoral_us_sguided` AS `pre_left_femoral_us_sguided`',
    '`pre_right_femoral_us_sguided` AS `pre_right_femoral_us_sguided`',
    '`pre_screen_of_right_femoral_head` AS `pre_screen_of_right_femoral_head`',
    '`pre_clinical_frailty_scale` AS `pre_clinical_frailty_scale`',
    '`pre_clinical_frailty_scale_image` AS `pre_clinical_frailty_scale_image`',
    '`post_date_last_changed` AS `post_date_last_changed`',
    '`post_adc_updt` AS `post_adc_updt`',
    '`post_date_of_discharge_raw` AS `post_date_of_discharge_raw`',
    '`post_date_of_discharge_ts` AS `post_date_of_discharge_ts`',
    '`post_gender` AS `post_gender`',
    '`post_date_of_death` AS `post_date_of_death`',
    '`post_ethnic_origin` AS `post_ethnic_origin`',
    '`post_vessels_attemptedtxt` AS `post_vessels_attemptedtxt`',
    '`post_number_of_vessels_attemptednot_epicardial_territories` AS `post_number_of_vessels_attemptednot_epicardial_territories`',
    '`post_number_of_lesions_attempted` AS `post_number_of_lesions_attempted`',
    '`post_number_of_chronic_occlusions_attempted` AS `post_number_of_chronic_occlusions_attempted`',
    '`post_number_restenoses_attempted` AS `post_number_restenoses_attempted`',
    '`post_number_instent_stenoses_attempted` AS `post_number_instent_stenoses_attempted`',
    '`post_number_stents_used` AS `post_number_stents_used`',
    '`post_number_of_drug_eluting_stents_used` AS `post_number_of_drug_eluting_stents_used`',
    '`post_drugs_eluted_by_stentstxt` AS `post_drugs_eluted_by_stentstxt`',
    '`post_left_main_stem_stenosispost_pci` AS `post_left_main_stem_stenosispost_pci`',
    '`post_lad_proximal_stenosispost_pci` AS `post_lad_proximal_stenosispost_pci`',
    '`post_lad_other_stenosispost_pci` AS `post_lad_other_stenosispost_pci`',
    '`post_rca_stenosispost_pci` AS `post_rca_stenosispost_pci`',
    '`post_cx_stenosispost_pci` AS `post_cx_stenosispost_pci`',
    '`post_number_coronary_grafts_patent_postop` AS `post_number_coronary_grafts_patent_postop`',
    '`post_flow_in_ira_postopacs` AS `post_flow_in_ira_postopacs`',
    '`post_m_operation_reportcomment` AS `post_m_operation_reportcomment`',
    '`post_device_failure` AS `post_device_failure`',
    '`post_pci_hospital_outcometxt` AS `post_pci_hospital_outcometxt`',
    '`post_enzymes_postop` AS `post_enzymes_postop`',
    '`post_status_at_discharge` AS `post_status_at_discharge`',
    '`post_largest_ballon_stent_diameter` AS `post_largest_ballon_stent_diameter`',
    '`post_longest_stented_treated_segment` AS `post_longest_stented_treated_segment`',
    '`post_procedural_complicationtxt` AS `post_procedural_complicationtxt`',
    '`post_arterial_complicationstxt` AS `post_arterial_complicationstxt`',
    '`post_time_to_bypass` AS `post_time_to_bypass`',
    '`post_left_main_stem_protected` AS `post_left_main_stem_protected`',
    '`post_patient_status_during_transfer_to_theatre` AS `post_patient_status_during_transfer_to_theatre`',
    '`post_indication_for_stent` AS `post_indication_for_stent`',
    '`post_number_of_lesions_successful` AS `post_number_of_lesions_successful`',
    '`post_bleeding_up_to_discharge` AS `post_bleeding_up_to_discharge`',
    '`post_ventilation` AS `post_ventilation`',
    '`post_external_cooling_pads_blankets_wrapstxt` AS `post_external_cooling_pads_blankets_wrapstxt`',
    '`post_consultant_responsible_for_procedure_gmc_numbertxt` AS `post_consultant_responsible_for_procedure_gmc_numbertxt`',
    '`post_primary_operator_gmc_numbertxt` AS `post_primary_operator_gmc_numbertxt`',
    '`post_second_operator_gmc_numbertxt` AS `post_second_operator_gmc_numbertxt`',
    '`post_third_operator_gmc_numbertxt` AS `post_third_operator_gmc_numbertxt`',
    '`post_left_main_stem_stenosispre_pci` AS `post_left_main_stem_stenosispre_pci`',
    '`post_lad_proximal_stenosis_pre_pci` AS `post_lad_proximal_stenosis_pre_pci`',
    '`post_lad_other_stenosis_pre_pci` AS `post_lad_other_stenosis_pre_pci`',
    '`post_rca_stenosis_pre_pci` AS `post_rca_stenosis_pre_pci`',
    '`post_cx_stenosis_pre_pci` AS `post_cx_stenosis_pre_pci`',
    '`post_management_plan` AS `post_management_plan`',
    '`post_other_management_plan` AS `post_other_management_plan`',
    '`post_cath_proceduretxt` AS `post_cath_proceduretxt`',
    '`post_m_angiography_findings` AS `post_m_angiography_findings`',
    '`post_m_procedure_findings_and_report` AS `post_m_procedure_findings_and_report`',
    '`post_left_main_stem_stenosis_procedure` AS `post_left_main_stem_stenosis_procedure`',
    '`post_lad_proximal_stenosis_procedure` AS `post_lad_proximal_stenosis_procedure`',
    '`post_lad_other_stenosis_procedure` AS `post_lad_other_stenosis_procedure`',
    '`post_cx_stenosis_procedure` AS `post_cx_stenosis_procedure`',
    '`post_flow_in_ira_at_angio` AS `post_flow_in_ira_at_angio`',
    '`post_rca_stenosis_procedure` AS `post_rca_stenosis_procedure`',
    '`post_number_grafts_presentangio` AS `post_number_grafts_presentangio`',
    '`post_number_grafts_patent_pre_pci` AS `post_number_grafts_patent_pre_pci`',
    '`post_procedural_devices_used_detailtxt` AS `post_procedural_devices_used_detailtxt`',
    '`post_athero_thrombus_removal_devices_usedtxt` AS `post_athero_thrombus_removal_devices_usedtxt`',
    '`post_left_main_stem_stenosis_comment` AS `post_left_main_stem_stenosis_comment`',
    '`post_lad_proximal_stenosis_comment` AS `post_lad_proximal_stenosis_comment`',
    '`post_lad_other_stenosis_comment` AS `post_lad_other_stenosis_comment`',
    '`post_rca_stenosis_comment` AS `post_rca_stenosis_comment`',
    '`post_cx_stenosis_comment` AS `post_cx_stenosis_comment`',
    '`post_diagnostic_devices_used_during_proceduretxt` AS `post_diagnostic_devices_used_during_proceduretxt`',
    '`post_procedural_devices_usedtxt` AS `post_procedural_devices_usedtxt`',
    '`post_brachytherapy_devices_usedtxt` AS `post_brachytherapy_devices_usedtxt`',
    '`post_emboli_protection_devices_usedtxt` AS `post_emboli_protection_devices_usedtxt`',
    '`post_datetime_of_first_balloon_inflationpci` AS `post_datetime_of_first_balloon_inflationpci`',
    '`post_gp_iibiiia_drugs_used_during_proceduretxt` AS `post_gp_iibiiia_drugs_used_during_proceduretxt`',
    '`post_why_no_iibiiia_during_procedure` AS `post_why_no_iibiiia_during_procedure`',
    '`post_dataset_complete` AS `post_dataset_complete`',
    '`post_pci_for_stent_thrombosis` AS `post_pci_for_stent_thrombosis`',
    '`post_m_pre_discharge_instructions` AS `post_m_pre_discharge_instructions`',
    '`post_m_discharge_management_plan` AS `post_m_discharge_management_plan`',
    '`post_m_information_for_the_patient` AS `post_m_information_for_the_patient`',
    '`post_mg_pinfo` AS `post_mg_pinfo`',
    '`post_nurse_led_discharge` AS `post_nurse_led_discharge`',
    '`post_m_details_of_all_devices_implanted` AS `post_m_details_of_all_devices_implanted`',
    '`post_diagnostic_interventions_undertakentxt` AS `post_diagnostic_interventions_undertakentxt`',
    '`post_therapeutic_interventions_undertakentxt` AS `post_therapeutic_interventions_undertakentxt`',
    '`post_contrast_volume` AS `post_contrast_volume`',
    '`post_interesting_case` AS `post_interesting_case`',
    '`post_m_case_details` AS `post_m_case_details`',
    '`post_sca_istage_pre_pci` AS `post_sca_istage_pre_pci`',
    '`post_sca_istage_post_cci` AS `post_sca_istage_post_cci`',
    '`post_echo_physiology_pre_pci` AS `post_echo_physiology_pre_pci`',
    '`post_peak_lactate` AS `post_peak_lactate`',
    '`post_troughp_h` AS `post_troughp_h`',
    '`post_trough_sys_bp` AS `post_trough_sys_bp`',
    '`post_inopressor_use_pre_pci` AS `post_inopressor_use_pre_pci`',
    '`post_peak_noradrenaline_dose_during_cath_lab` AS `post_peak_noradrenaline_dose_during_cath_lab`',
    '`post_ic_uadmission_post_pci` AS `post_ic_uadmission_post_pci`',
    '`post_csa_iimage` AS `post_csa_iimage`',
    '`post_l_vgram` AS `post_l_vgram`',
    '`source_join_status` AS `source_join_status`',
    '`nhs_number` AS `nhs_number`',
    '`mrn` AS `mrn`',
    '`identifier_concordance_ind` AS `identifier_concordance_ind`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`lesion_child_count` AS `lesion_child_count`',
    '`pre_date_of_death_future_ind` AS `pre_date_of_death_future_ind`',
    '`pre_date_of_death_sentinel_ind` AS `pre_date_of_death_sentinel_ind`',
    '`pre_date_of_death_clean` AS `pre_date_of_death_clean`',
    '`pre_dateandtimeofoperation_future_ind` AS `pre_dateandtimeofoperation_future_ind`',
    '`pre_dateandtimeofoperation_sentinel_ind` AS `pre_dateandtimeofoperation_sentinel_ind`',
    '`pre_dateandtimeofoperation_clean` AS `pre_dateandtimeofoperation_clean`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_future_ind` AS `pre_datetime_arrival_at_first_hospitalacs_only_future_ind`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind` AS `pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind`',
    '`pre_datetime_arrival_at_first_hospitalacs_only_clean` AS `pre_datetime_arrival_at_first_hospitalacs_only_clean`',
    '`pre_date_time_arrival_at_pci_hospital_future_ind` AS `pre_date_time_arrival_at_pci_hospital_future_ind`',
    '`pre_date_time_arrival_at_pci_hospital_sentinel_ind` AS `pre_date_time_arrival_at_pci_hospital_sentinel_ind`',
    '`pre_date_time_arrival_at_pci_hospital_clean` AS `pre_date_time_arrival_at_pci_hospital_clean`',
    '`pre_date_time_of_call_for_helpstemi_future_ind` AS `pre_date_time_of_call_for_helpstemi_future_ind`',
    '`pre_date_time_of_call_for_helpstemi_sentinel_ind` AS `pre_date_time_of_call_for_helpstemi_sentinel_ind`',
    '`pre_date_time_of_call_for_helpstemi_clean` AS `pre_date_time_of_call_for_helpstemi_clean`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_future_ind` AS `pre_datetime_of_ecg_triggering_ppci_pathway_future_ind`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind` AS `pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind`',
    '`pre_datetime_of_ecg_triggering_ppci_pathway_clean` AS `pre_datetime_of_ecg_triggering_ppci_pathway_clean`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_future_ind` AS `pre_patient_location_at_time_of_stemi_onset_ts_future_ind`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind` AS `pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind`',
    '`pre_patient_location_at_time_of_stemi_onset_ts_clean` AS `pre_patient_location_at_time_of_stemi_onset_ts_clean`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_future_ind` AS `pre_datetime_of_symptom_onsetpci_acs_only_future_ind`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind` AS `pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind`',
    '`pre_datetime_of_symptom_onsetpci_acs_only_clean` AS `pre_datetime_of_symptom_onsetpci_acs_only_clean`',
    '`pre_presumed_date_time_of_arrest_future_ind` AS `pre_presumed_date_time_of_arrest_future_ind`',
    '`pre_presumed_date_time_of_arrest_sentinel_ind` AS `pre_presumed_date_time_of_arrest_sentinel_ind`',
    '`pre_presumed_date_time_of_arrest_clean` AS `pre_presumed_date_time_of_arrest_clean`',
    '`pre_date_and_time_on_table_future_ind` AS `pre_date_and_time_on_table_future_ind`',
    '`pre_date_and_time_on_table_sentinel_ind` AS `pre_date_and_time_on_table_sentinel_ind`',
    '`pre_date_and_time_on_table_clean` AS `pre_date_and_time_on_table_clean`',
    '`pre_date_time_shortcuts_ts_future_ind` AS `pre_date_time_shortcuts_ts_future_ind`',
    '`pre_date_time_shortcuts_ts_sentinel_ind` AS `pre_date_time_shortcuts_ts_sentinel_ind`',
    '`pre_date_time_shortcuts_ts_clean` AS `pre_date_time_shortcuts_ts_clean`',
    '`post_date_of_discharge_ts_future_ind` AS `post_date_of_discharge_ts_future_ind`',
    '`post_date_of_discharge_ts_sentinel_ind` AS `post_date_of_discharge_ts_sentinel_ind`',
    '`post_date_of_discharge_ts_clean` AS `post_date_of_discharge_ts_clean`',
    '`post_date_of_death_future_ind` AS `post_date_of_death_future_ind`',
    '`post_date_of_death_sentinel_ind` AS `post_date_of_death_sentinel_ind`',
    '`post_date_of_death_clean` AS `post_date_of_death_clean`',
    '`post_datetime_of_first_balloon_inflationpci_future_ind` AS `post_datetime_of_first_balloon_inflationpci_future_ind`',
    '`post_datetime_of_first_balloon_inflationpci_sentinel_ind` AS `post_datetime_of_first_balloon_inflationpci_sentinel_ind`',
    '`post_datetime_of_first_balloon_inflationpci_clean` AS `post_datetime_of_first_balloon_inflationpci_clean`',
    '`pipeline_updt_dt_tm` AS `pipeline_updt_dt_tm`',
    '`row_hash` AS `row_hash`',
    '`source_present_ind` AS `source_present_ind`',
]

CLINICAL_REGISTRY_CORONARY_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the coronary_procedure source family and native entry_id.",
    "entry_id": "Reg_CORONARY_Pre/PostProcedure EntryId; parent key for lesion PARENT_ENTRY_ID.",
    "pre_date_last_changed": "Source edit timestamp from the pre-procedure iWeb coronary record; it is not a clinical event time.",
    "pre_adc_updt": "Landing timestamp carried by the pre-procedure iWeb coronary source record.",
    "pre_gender": "Gender recorded in the pre-procedure iWeb coronary assessment.",
    "pre_date_of_death": "Date of death recorded in the pre-procedure iWeb coronary assessment.",
    "pre_ethnic_origin": "Ethnic origin recorded in the pre-procedure iWeb coronary assessment.",
    "pre_dateandtimeofoperation": "Date and timeofoperation recorded in the pre-procedure iWeb coronary assessment.",
    "pre_age": "Age recorded in the pre-procedure iWeb coronary assessment.",
    "pre_sex": "Sex recorded in the pre-procedure iWeb coronary assessment.",
    "pre_patient_ethnic_group": "Patient ethnic group recorded in the pre-procedure iWeb coronary assessment.",
    "pre_administrative_category": "Administrative category recorded in the pre-procedure iWeb coronary assessment.",
    "pre_clinical_syndromepci": "Clinical syndrome percutaneous coronary intervention (PCI) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_indication_for_intervention": "Indication for intervention recorded in the pre-procedure iWeb coronary assessment.",
    "pre_procedure_urgency": "Procedure urgency recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cardiogenic_shockpre_procedure": "Cardiogenic shockpre procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_ccs_angina_statuspre_procedure_stable_only": "Canadian Cardiovascular Society angina status before the procedure for stable presentations.",
    "pre_nyha_dyspnoea_statuspre_procedure_stable_only": "New York Heart Association dyspnoea status before the procedure for stable presentations.",
    "pre_datetime_arrival_at_first_hospitalacs_only": "Date and time arrival at first hospital acute coronary syndrome (ACS) only recorded in the pre-procedure iWeb coronary assessment.",
    "pre_admission_routeacs_only": "Admission routeacs only recorded in the pre-procedure iWeb coronary assessment.",
    "pre_presenting_ecgacs_only": "Presenting ecgacs only recorded in the pre-procedure iWeb coronary assessment.",
    "pre_recent_lysisacs_only": "Recent lysisacs only recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cardiac_enzymesmarkers_raised": "Cardiac enzymes markers raised recorded in the pre-procedure iWeb coronary assessment.",
    "pre_previous_mi": "Previous mi recorded in the pre-procedure iWeb coronary assessment.",
    "pre_previous_cabg": "Previous coronary artery bypass grafting (CABG) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_previous_pci": "Previous percutaneous coronary intervention (PCI) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_diabetes": "Diabetes recorded in the pre-procedure iWeb coronary assessment.",
    "pre_height": "Height recorded in the pre-procedure iWeb coronary assessment.",
    "pre_weight": "Weight recorded in the pre-procedure iWeb coronary assessment.",
    "pre_body_surface_area": "Body surface area recorded in the pre-procedure iWeb coronary assessment.",
    "pre_body_mass_index": "Body mass index recorded in the pre-procedure iWeb coronary assessment.",
    "pre_lv_ejection_fraction_category": "Left-ventricular ejection fraction category recorded in the pre-procedure iWeb coronary assessment.",
    "pre_lv_ejection_fraction": "Left-ventricular ejection fraction recorded in the pre-procedure iWeb coronary assessment.",
    "pre_consultant_responsible_for_proceduretxt": "Free-text consultant responsible for procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_primary_operatortxt": "Free-text primary operator recorded in the pre-procedure iWeb coronary assessment.",
    "pre_primary_operator_status": "Primary operator status recorded in the pre-procedure iWeb coronary assessment.",
    "pre_second_operatortxt": "Free-text second operator recorded in the pre-procedure iWeb coronary assessment.",
    "pre_second_operator_status": "Second operator status recorded in the pre-procedure iWeb coronary assessment.",
    "pre_third_operatortxt": "Free-text third operator recorded in the pre-procedure iWeb coronary assessment.",
    "pre_third_operator_status": "Third operator status recorded in the pre-procedure iWeb coronary assessment.",
    "pre_gp_iibiiia_drugs_used_during_proceduretxt": "Glycoprotein IIb/IIIa drugs recorded as used during the coronary procedure.",
    "pre_emboli_protection_devices_usedtxt": "Free-text emboli protection devices used recorded in the pre-procedure iWeb coronary assessment.",
    "pre_circulatory_supporttxt": "Free-text circulatory support recorded in the pre-procedure iWeb coronary assessment.",
    "pre_local_procedure_identifier": "Local procedure identifier recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cholesterol": "Cholesterol recorded in the pre-procedure iWeb coronary assessment.",
    "pre_smoking_history": "Smoking history recorded in the pre-procedure iWeb coronary assessment.",
    "pre_family_history_of_cad": "Family history of coronary artery disease (CAD) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_medical_historytxt": "Free-text medical history recorded in the pre-procedure iWeb coronary assessment.",
    "pre_ventilated_preop": "Ventilated pre-operative recorded in the pre-procedure iWeb coronary assessment.",
    "pre_q_wave_on_ecg": "Q wave on electrocardiogram (ECG) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_ecg_ischaemia": "Electrocardiogram (ECG) ischaemia recorded in the pre-procedure iWeb coronary assessment.",
    "pre_follow_onad_hoc_procedure": "Follow onad hoc procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_training_procedure": "Training procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_research_procedure": "Research procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_research_title": "Research title recorded in the pre-procedure iWeb coronary assessment.",
    "pre_arterial_accesstxt": "Free-text arterial access recorded in the pre-procedure iWeb coronary assessment.",
    "pre_why_no_iibiiia_during_procedure": "Why no IIb/IIIa during procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_surgical_cover": "Surgical cover recorded in the pre-procedure iWeb coronary assessment.",
    "pre_date_time_arrival_at_pci_hospital": "Date and time arrival at percutaneous coronary intervention (PCI) hospital recorded in the pre-procedure iWeb coronary assessment.",
    "pre_date_time_of_call_for_helpstemi": "Date and time of call for help ST-elevation myocardial infarction (STEMI) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_referring_hospitaltxt": "Free-text referring hospital recorded in the pre-procedure iWeb coronary assessment.",
    "pre_datetime_of_ecg_triggering_ppci_pathway": "Date and time of electrocardiogram (ECG) triggering primary percutaneous coronary intervention (PPCI) pathway recorded in the pre-procedure iWeb coronary assessment.",
    "pre_patient_location_at_time_of_stemi_onset_raw": "Verbatim pre-procedure patient-location-at-STEMI-onset value before timestamp parsing.",
    "pre_patient_location_at_time_of_stemi_onset_ts": "Timestamp parsed from the pre-procedure patient-location-at-STEMI-onset source value.",
    "pre_serum_creatinine": "Serum creatinine recorded in the pre-procedure iWeb coronary assessment.",
    "pre_datetime_of_symptom_onsetpci_acs_only": "Date and time of symptom onset percutaneous coronary intervention (PCI) acute coronary syndrome (ACS) only recorded in the pre-procedure iWeb coronary assessment.",
    "pre_echo_report": "Echo report recorded in the pre-procedure iWeb coronary assessment.",
    "pre_arterial_blood_gas_on_arrival_in_cath_lab_ph": "Arterial blood gas on arrival in cath lab pH recorded in the pre-procedure iWeb coronary assessment.",
    "pre_arterial_blood_gas_on_arrival_in_cath_lab_lactate": "Arterial blood gas on arrival in cath lab lactate recorded in the pre-procedure iWeb coronary assessment.",
    "pre_arterial_blood_gas_on_arrival_in_cath_lab_base_excess": "Arterial blood gas on arrival in cath lab base excess recorded in the pre-procedure iWeb coronary assessment.",
    "pre_glasgow_coma_scale_on_arrival_in_cath_lab": "Glasgow coma scale on arrival in cath lab recorded in the pre-procedure iWeb coronary assessment.",
    "pre_other_therapeutic_hypothermia": "Other therapeutic hypothermia recorded in the pre-procedure iWeb coronary assessment.",
    "pre_therapeutic_hypothermiatxt": "Free-text therapeutic hypothermia recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cath_proceduretxt": "Free-text cath procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_out_of_hospital_cardiac_arrest": "Out of hospital cardiac arrest recorded in the pre-procedure iWeb coronary assessment.",
    "pre_presumed_date_time_of_arrest": "Presumed date and time of arrest recorded in the pre-procedure iWeb coronary assessment.",
    "pre_coronary_dominance": "Coronary dominance recorded in the pre-procedure iWeb coronary assessment.",
    "pre_m_procedure_findings": "Free-text procedure findings recorded in the pre-procedure iWeb coronary assessment.",
    "pre_arterial_managementtxt": "Free-text arterial management recorded in the pre-procedure iWeb coronary assessment.",
    "pre_drug_therapy_preproceduretxt": "Free-text drug therapy before the procedure recorded in the pre-procedure iWeb coronary assessment.",
    "pre_referring_cardiologisttxt": "Free-text referring cardiologist recorded in the pre-procedure iWeb coronary assessment.",
    "pre_history_of_renal_disease1": "History of renal disease1 recorded in the pre-procedure iWeb coronary assessment.",
    "pre_temporary_pacing_wire": "Temporary pacing wire recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cath_devicestxt": "Free-text cath devices recorded in the pre-procedure iWeb coronary assessment.",
    "pre_cath_lab_number": "Cath lab number recorded in the pre-procedure iWeb coronary assessment.",
    "pre_date_and_time_on_table": "Date and time on table recorded in the pre-procedure iWeb coronary assessment.",
    "pre_ventilation": "Ventilation recorded in the pre-procedure iWeb coronary assessment.",
    "pre_hospital_identifier": "Hospital identifier recorded in the pre-procedure iWeb coronary assessment.",
    "pre_date_time_shortcuts_raw": "Verbatim pre-procedure date/time shortcut value before timestamp parsing.",
    "pre_date_time_shortcuts_ts": "Timestamp parsed from the pre-procedure date/time shortcut source value.",
    "pre_m_admission_history": "Free-text admission history recorded in the pre-procedure iWeb coronary assessment.",
    "pre_diabetes_type": "Diabetes type recorded in the pre-procedure iWeb coronary assessment.",
    "pre_interventions": "Interventions recorded in the pre-procedure iWeb coronary assessment.",
    "pre_convalescent_stem_ior_nstemi": "Convalescent stem or non-ST-elevation myocardial infarction (NSTEMI) recorded in the pre-procedure iWeb coronary assessment.",
    "pre_us_sguided": "Indicates whether ultrasound guidance was recorded for vascular access before the procedure.",
    "pre_screen_of_left_femoral_head": "Screen of left femoral head recorded in the pre-procedure iWeb coronary assessment.",
    "pre_left_femoral_micropuncture": "Left femoral micropuncture recorded in the pre-procedure iWeb coronary assessment.",
    "pre_left_height_of_puncture": "Left height of puncture recorded in the pre-procedure iWeb coronary assessment.",
    "pre_height_of_puncture_diagram": "Height of puncture diagram recorded in the pre-procedure iWeb coronary assessment.",
    "pre_right_femoral_micropuncture": "Right femoral micropuncture recorded in the pre-procedure iWeb coronary assessment.",
    "pre_right_height_of_puncture": "Right height of puncture recorded in the pre-procedure iWeb coronary assessment.",
    "pre_left_femoral_us_sguided": "Indicates whether left femoral access was ultrasound-guided.",
    "pre_right_femoral_us_sguided": "Indicates whether right femoral access was ultrasound-guided.",
    "pre_screen_of_right_femoral_head": "Screen of right femoral head recorded in the pre-procedure iWeb coronary assessment.",
    "pre_clinical_frailty_scale": "Clinical frailty scale recorded in the pre-procedure iWeb coronary assessment.",
    "pre_clinical_frailty_scale_image": "Clinical frailty scale image recorded in the pre-procedure iWeb coronary assessment.",
    "post_date_last_changed": "Source edit timestamp from the post-procedure iWeb coronary record; it is not a clinical event time.",
    "post_adc_updt": "Landing timestamp carried by the post-procedure iWeb coronary source record.",
    "post_date_of_discharge_raw": "Verbatim source discharge value; retained even when future-dated.",
    "post_date_of_discharge_ts": "Parsed discharge timestamp; use its DQ triplet and CLEAN value.",
    "post_gender": "Gender recorded in the post-procedure iWeb coronary record.",
    "post_date_of_death": "Date of death recorded in the post-procedure iWeb coronary record.",
    "post_ethnic_origin": "Ethnic origin recorded in the post-procedure iWeb coronary record.",
    "post_vessels_attemptedtxt": "Free-text vessels attempted recorded in the post-procedure iWeb coronary record.",
    "post_number_of_vessels_attemptednot_epicardial_territories": "Number of vessels attempted not epicardial territories recorded in the post-procedure iWeb coronary record.",
    "post_number_of_lesions_attempted": "Number of lesions attempted recorded in the post-procedure iWeb coronary record.",
    "post_number_of_chronic_occlusions_attempted": "Number of chronic occlusions attempted recorded in the post-procedure iWeb coronary record.",
    "post_number_restenoses_attempted": "Number of restenoses attempted recorded in the post-procedure iWeb coronary record.",
    "post_number_instent_stenoses_attempted": "Number of in-stent stenoses attempted recorded in the post-procedure iWeb coronary record.",
    "post_number_stents_used": "Number stents used recorded in the post-procedure iWeb coronary record.",
    "post_number_of_drug_eluting_stents_used": "Number of drug eluting stents used recorded in the post-procedure iWeb coronary record.",
    "post_drugs_eluted_by_stentstxt": "Free-text drugs eluted by stents recorded in the post-procedure iWeb coronary record.",
    "post_left_main_stem_stenosispost_pci": "Left main stem stenosis post percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_lad_proximal_stenosispost_pci": "Left anterior descending artery proximal stenosis post percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_lad_other_stenosispost_pci": "Left anterior descending artery other stenosis post percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_rca_stenosispost_pci": "Right coronary artery stenosis post percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_cx_stenosispost_pci": "Circumflex coronary artery stenosis post percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_number_coronary_grafts_patent_postop": "Number coronary grafts patent post-operative recorded in the post-procedure iWeb coronary record.",
    "post_flow_in_ira_postopacs": "Flow in infarct-related artery post operativeacs recorded in the post-procedure iWeb coronary record.",
    "post_m_operation_reportcomment": "Free-text operation reportcomment recorded in the post-procedure iWeb coronary record.",
    "post_device_failure": "Device failure recorded in the post-procedure iWeb coronary record.",
    "post_pci_hospital_outcometxt": "Free-text percutaneous coronary intervention (PCI) hospital outcome recorded in the post-procedure iWeb coronary record.",
    "post_enzymes_postop": "Enzymes post-operative recorded in the post-procedure iWeb coronary record.",
    "post_status_at_discharge": "Status at discharge recorded in the post-procedure iWeb coronary record.",
    "post_largest_ballon_stent_diameter": "Largest balloon stent diameter recorded in the post-procedure iWeb coronary record.",
    "post_longest_stented_treated_segment": "Longest stented treated segment recorded in the post-procedure iWeb coronary record.",
    "post_procedural_complicationtxt": "Free-text procedural complication recorded in the post-procedure iWeb coronary record.",
    "post_arterial_complicationstxt": "Free-text arterial complications recorded in the post-procedure iWeb coronary record.",
    "post_time_to_bypass": "Time to bypass recorded in the post-procedure iWeb coronary record.",
    "post_left_main_stem_protected": "Left main stem protected recorded in the post-procedure iWeb coronary record.",
    "post_patient_status_during_transfer_to_theatre": "Patient status during transfer to theatre recorded in the post-procedure iWeb coronary record.",
    "post_indication_for_stent": "Indication for stent recorded in the post-procedure iWeb coronary record.",
    "post_number_of_lesions_successful": "Number of lesions successful recorded in the post-procedure iWeb coronary record.",
    "post_bleeding_up_to_discharge": "Bleeding up to discharge recorded in the post-procedure iWeb coronary record.",
    "post_ventilation": "Ventilation recorded in the post-procedure iWeb coronary record.",
    "post_external_cooling_pads_blankets_wrapstxt": "Free-text external cooling pads blankets wraps recorded in the post-procedure iWeb coronary record.",
    "post_consultant_responsible_for_procedure_gmc_numbertxt": "Free-text consultant responsible for procedure General Medical Council number recorded in the post-procedure iWeb coronary record.",
    "post_primary_operator_gmc_numbertxt": "Free-text primary operator General Medical Council number recorded in the post-procedure iWeb coronary record.",
    "post_second_operator_gmc_numbertxt": "Free-text second operator General Medical Council number recorded in the post-procedure iWeb coronary record.",
    "post_third_operator_gmc_numbertxt": "Free-text third operator General Medical Council number recorded in the post-procedure iWeb coronary record.",
    "post_left_main_stem_stenosispre_pci": "Left main stem stenosis pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_lad_proximal_stenosis_pre_pci": "Left anterior descending artery proximal stenosis pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_lad_other_stenosis_pre_pci": "Left anterior descending artery other stenosis pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_rca_stenosis_pre_pci": "Right coronary artery stenosis pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_cx_stenosis_pre_pci": "Circumflex coronary artery stenosis pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_management_plan": "Management plan recorded in the post-procedure iWeb coronary record.",
    "post_other_management_plan": "Other management plan recorded in the post-procedure iWeb coronary record.",
    "post_cath_proceduretxt": "Free-text cath procedure recorded in the post-procedure iWeb coronary record.",
    "post_m_angiography_findings": "Free-text angiography findings recorded in the post-procedure iWeb coronary record.",
    "post_m_procedure_findings_and_report": "Free-text procedure findings and report recorded in the post-procedure iWeb coronary record.",
    "post_left_main_stem_stenosis_procedure": "Left main stem stenosis procedure recorded in the post-procedure iWeb coronary record.",
    "post_lad_proximal_stenosis_procedure": "Left anterior descending artery proximal stenosis procedure recorded in the post-procedure iWeb coronary record.",
    "post_lad_other_stenosis_procedure": "Left anterior descending artery other stenosis procedure recorded in the post-procedure iWeb coronary record.",
    "post_cx_stenosis_procedure": "Circumflex coronary artery stenosis procedure recorded in the post-procedure iWeb coronary record.",
    "post_flow_in_ira_at_angio": "Flow in infarct-related artery at angio recorded in the post-procedure iWeb coronary record.",
    "post_rca_stenosis_procedure": "Right coronary artery stenosis procedure recorded in the post-procedure iWeb coronary record.",
    "post_number_grafts_presentangio": "Number grafts present angio recorded in the post-procedure iWeb coronary record.",
    "post_number_grafts_patent_pre_pci": "Number grafts patent pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_procedural_devices_used_detailtxt": "Free-text procedural devices used detail recorded in the post-procedure iWeb coronary record.",
    "post_athero_thrombus_removal_devices_usedtxt": "Free-text athero thrombus removal devices used recorded in the post-procedure iWeb coronary record.",
    "post_left_main_stem_stenosis_comment": "Left main stem stenosis comment recorded in the post-procedure iWeb coronary record.",
    "post_lad_proximal_stenosis_comment": "Left anterior descending artery proximal stenosis comment recorded in the post-procedure iWeb coronary record.",
    "post_lad_other_stenosis_comment": "Left anterior descending artery other stenosis comment recorded in the post-procedure iWeb coronary record.",
    "post_rca_stenosis_comment": "Right coronary artery stenosis comment recorded in the post-procedure iWeb coronary record.",
    "post_cx_stenosis_comment": "Circumflex coronary artery stenosis comment recorded in the post-procedure iWeb coronary record.",
    "post_diagnostic_devices_used_during_proceduretxt": "Free-text diagnostic devices used during procedure recorded in the post-procedure iWeb coronary record.",
    "post_procedural_devices_usedtxt": "Free-text procedural devices used recorded in the post-procedure iWeb coronary record.",
    "post_brachytherapy_devices_usedtxt": "Free-text brachytherapy devices used recorded in the post-procedure iWeb coronary record.",
    "post_emboli_protection_devices_usedtxt": "Free-text emboli protection devices used recorded in the post-procedure iWeb coronary record.",
    "post_datetime_of_first_balloon_inflationpci": "Date and time of first balloon inflation percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_gp_iibiiia_drugs_used_during_proceduretxt": "Glycoprotein IIb/IIIa drugs recorded as used during the coronary procedure.",
    "post_why_no_iibiiia_during_procedure": "Why no IIb/IIIa during procedure recorded in the post-procedure iWeb coronary record.",
    "post_dataset_complete": "Dataset complete recorded in the post-procedure iWeb coronary record.",
    "post_pci_for_stent_thrombosis": "Percutaneous coronary intervention (PCI) for stent thrombosis recorded in the post-procedure iWeb coronary record.",
    "post_m_pre_discharge_instructions": "Free-text pre discharge instructions recorded in the post-procedure iWeb coronary record.",
    "post_m_discharge_management_plan": "Free-text discharge management plan recorded in the post-procedure iWeb coronary record.",
    "post_m_information_for_the_patient": "Free-text information for the patient recorded in the post-procedure iWeb coronary record.",
    "post_mg_pinfo": "Mg pinfo recorded in the post-procedure iWeb coronary record.",
    "post_nurse_led_discharge": "Nurse led discharge recorded in the post-procedure iWeb coronary record.",
    "post_m_details_of_all_devices_implanted": "Free-text details of all devices implanted recorded in the post-procedure iWeb coronary record.",
    "post_diagnostic_interventions_undertakentxt": "Free-text diagnostic interventions undertaken recorded in the post-procedure iWeb coronary record.",
    "post_therapeutic_interventions_undertakentxt": "Free-text therapeutic interventions undertaken recorded in the post-procedure iWeb coronary record.",
    "post_contrast_volume": "Contrast volume recorded in the post-procedure iWeb coronary record.",
    "post_interesting_case": "Interesting case recorded in the post-procedure iWeb coronary record.",
    "post_m_case_details": "Free-text case details recorded in the post-procedure iWeb coronary record.",
    "post_sca_istage_pre_pci": "SCAI cardiogenic-shock stage recorded before percutaneous coronary intervention.",
    "post_sca_istage_post_cci": "SCAI cardiogenic-shock stage recorded after coronary intervention; the source field retains its CCI spelling.",
    "post_echo_physiology_pre_pci": "Echocardiographic or physiological assessment recorded before percutaneous coronary intervention.",
    "post_peak_lactate": "Peak lactate recorded in the post-procedure iWeb coronary record.",
    "post_troughp_h": "Lowest recorded pH associated with the coronary procedure.",
    "post_trough_sys_bp": "Trough sys bp recorded in the post-procedure iWeb coronary record.",
    "post_inopressor_use_pre_pci": "Inopressor use pre percutaneous coronary intervention (PCI) recorded in the post-procedure iWeb coronary record.",
    "post_peak_noradrenaline_dose_during_cath_lab": "Peak noradrenaline dose during cath lab recorded in the post-procedure iWeb coronary record.",
    "post_ic_uadmission_post_pci": "Intensive-care admission status after percutaneous coronary intervention; the source field retains its ICU spelling.",
    "post_csa_iimage": "SCAI image or image reference retained from the post-procedure iWeb module.",
    "post_l_vgram": "Left ventriculogram finding or status retained from the post-procedure iWeb module.",
    "source_join_status": "Indicates whether the coronary procedure was present in both source modules or only the pre-procedure or post-procedure module.",
    "nhs_number": "Source NHS number published under identifier doctrine; ig_risk=4 and ig_severity=2.",
    "mrn": "Source local medical record number published under identifier doctrine; ig_risk=4 and ig_severity=2.",
    "identifier_concordance_ind": "True when the pre-procedure and post-procedure NHS number and MRN agree; null when either module lacks a comparison identifier.",
    "date_last_changed": "Greatest Pre/Post source edit timestamp; not the clinical procedure date.",
    "adc_updt": "Greatest landing timestamp across the pre-procedure and post-procedure source records.",
    "nhs_number_valid_ind": "True when the published NHS number passes the bronze NHS-number validity check.",
    "linkage_status": "MATCHED_BOTH, MATCHED_MRN, MATCHED_NHS, CONFLICT, AMBIGUOUS, or UNMATCHED.",
    "person_id": "Millennium PERSON_ID from the live iWeb consensus-safe MRN/NHS resolver.",
    "linkage_method": "Identifier route used to resolve PERSON_ID, using MRN, NHS number, or both.",
    "linkage_historical_fallback_ind": "True when person resolution required the consensus-safe historical alias fallback.",
    "linkage_fallback_conflict_ind": "True when historical alias evidence conflicted and therefore could not safely resolve a person.",
    "lesion_child_count": "Count of current iweb_coronary_lesion rows whose PARENT_ENTRY_ID equals this EntryId.",
    "pre_date_of_death_future_ind": "True when the pre-procedure date of death is later than the pipeline evaluation time.",
    "pre_date_of_death_sentinel_ind": "True when the pre-procedure date of death matches a configured sentinel date or timestamp.",
    "pre_date_of_death_clean": "Pre-procedure date of death after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_dateandtimeofoperation_future_ind": "True when the pre-procedure date and timeofoperation is later than the pipeline evaluation time.",
    "pre_dateandtimeofoperation_sentinel_ind": "True when the pre-procedure date and timeofoperation matches a configured sentinel date or timestamp.",
    "pre_dateandtimeofoperation_clean": "Pre-procedure date and timeofoperation after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_datetime_arrival_at_first_hospitalacs_only_future_ind": "True when the pre-procedure date and time arrival at first hospital acute coronary syndrome (ACS) only is later than the pipeline evaluation time.",
    "pre_datetime_arrival_at_first_hospitalacs_only_sentinel_ind": "True when the pre-procedure date and time arrival at first hospital acute coronary syndrome (ACS) only matches a configured sentinel date or timestamp.",
    "pre_datetime_arrival_at_first_hospitalacs_only_clean": "Pre-procedure date and time arrival at first hospital acute coronary syndrome (ACS) only after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_date_time_arrival_at_pci_hospital_future_ind": "True when the pre-procedure date and time arrival at percutaneous coronary intervention (PCI) hospital is later than the pipeline evaluation time.",
    "pre_date_time_arrival_at_pci_hospital_sentinel_ind": "True when the pre-procedure date and time arrival at percutaneous coronary intervention (PCI) hospital matches a configured sentinel date or timestamp.",
    "pre_date_time_arrival_at_pci_hospital_clean": "Pre-procedure date and time arrival at percutaneous coronary intervention (PCI) hospital after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_date_time_of_call_for_helpstemi_future_ind": "True when the pre-procedure date and time of call for help ST-elevation myocardial infarction (STEMI) is later than the pipeline evaluation time.",
    "pre_date_time_of_call_for_helpstemi_sentinel_ind": "True when the pre-procedure date and time of call for help ST-elevation myocardial infarction (STEMI) matches a configured sentinel date or timestamp.",
    "pre_date_time_of_call_for_helpstemi_clean": "Pre-procedure date and time of call for help ST-elevation myocardial infarction (STEMI) after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_datetime_of_ecg_triggering_ppci_pathway_future_ind": "True when the pre-procedure date and time of electrocardiogram (ECG) triggering primary percutaneous coronary intervention (PPCI) pathway is later than the pipeline evaluation time.",
    "pre_datetime_of_ecg_triggering_ppci_pathway_sentinel_ind": "True when the pre-procedure date and time of electrocardiogram (ECG) triggering primary percutaneous coronary intervention (PPCI) pathway matches a configured sentinel date or timestamp.",
    "pre_datetime_of_ecg_triggering_ppci_pathway_clean": "Pre-procedure date and time of electrocardiogram (ECG) triggering primary percutaneous coronary intervention (PPCI) pathway after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_patient_location_at_time_of_stemi_onset_ts_future_ind": "True when the pre-procedure patient location at time of ST-elevation myocardial infarction (STEMI) onset ts is later than the pipeline evaluation time.",
    "pre_patient_location_at_time_of_stemi_onset_ts_sentinel_ind": "True when the pre-procedure patient location at time of ST-elevation myocardial infarction (STEMI) onset ts matches a configured sentinel date or timestamp.",
    "pre_patient_location_at_time_of_stemi_onset_ts_clean": "Pre-procedure patient location at time of ST-elevation myocardial infarction (STEMI) onset ts after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_datetime_of_symptom_onsetpci_acs_only_future_ind": "True when the pre-procedure date and time of symptom onset percutaneous coronary intervention (PCI) acute coronary syndrome (ACS) only is later than the pipeline evaluation time.",
    "pre_datetime_of_symptom_onsetpci_acs_only_sentinel_ind": "True when the pre-procedure date and time of symptom onset percutaneous coronary intervention (PCI) acute coronary syndrome (ACS) only matches a configured sentinel date or timestamp.",
    "pre_datetime_of_symptom_onsetpci_acs_only_clean": "Pre-procedure date and time of symptom onset percutaneous coronary intervention (PCI) acute coronary syndrome (ACS) only after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_presumed_date_time_of_arrest_future_ind": "True when the pre-procedure presumed date and time of arrest is later than the pipeline evaluation time.",
    "pre_presumed_date_time_of_arrest_sentinel_ind": "True when the pre-procedure presumed date and time of arrest matches a configured sentinel date or timestamp.",
    "pre_presumed_date_time_of_arrest_clean": "Pre-procedure presumed date and time of arrest after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_date_and_time_on_table_future_ind": "True when the pre-procedure date and time on table is later than the pipeline evaluation time.",
    "pre_date_and_time_on_table_sentinel_ind": "True when the pre-procedure date and time on table matches a configured sentinel date or timestamp.",
    "pre_date_and_time_on_table_clean": "Pre-procedure date and time on table after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pre_date_time_shortcuts_ts_future_ind": "True when the pre-procedure date and time shortcuts ts is later than the pipeline evaluation time.",
    "pre_date_time_shortcuts_ts_sentinel_ind": "True when the pre-procedure date and time shortcuts ts matches a configured sentinel date or timestamp.",
    "pre_date_time_shortcuts_ts_clean": "Pre-procedure date and time shortcuts ts after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "post_date_of_discharge_ts_future_ind": "True when the post-procedure date of discharge ts is later than the pipeline evaluation time.",
    "post_date_of_discharge_ts_sentinel_ind": "True when the post-procedure date of discharge ts matches a configured sentinel date or timestamp.",
    "post_date_of_discharge_ts_clean": "Post-procedure date of discharge ts after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "post_date_of_death_future_ind": "True when the post-procedure date of death is later than the pipeline evaluation time.",
    "post_date_of_death_sentinel_ind": "True when the post-procedure date of death matches a configured sentinel date or timestamp.",
    "post_date_of_death_clean": "Post-procedure date of death after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "post_datetime_of_first_balloon_inflationpci_future_ind": "True when the post-procedure date and time of first balloon inflation percutaneous coronary intervention (PCI) is later than the pipeline evaluation time.",
    "post_datetime_of_first_balloon_inflationpci_sentinel_ind": "True when the post-procedure date and time of first balloon inflation percutaneous coronary intervention (PCI) matches a configured sentinel date or timestamp.",
    "post_datetime_of_first_balloon_inflationpci_clean": "Post-procedure date and time of first balloon inflation percutaneous coronary intervention (PCI) after date-quality cleaning; null when missing, future-dated or a configured sentinel.",
    "pipeline_updt_dt_tm": "Timestamp when the bronze coronary-procedure pipeline last processed this row.",
    "row_hash": "SHA-256 content fingerprint over the row's business columns, excluding volatile pipeline timestamps.",
    "source_present_ind": "True while the procedure remains present in the latest source snapshot; false identifies a retained source retraction.",
}

CLINICAL_REGISTRY_CORONARY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_coronary_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_coronary_procedure"),
    comment="Internal QC of clinical_registry_coronary_procedure; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CORONARY_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_registry_coronary_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_coronary_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CORONARY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_CORONARY_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_coronary_procedure"),
    comment='Typed child of clinical_registry_entry for the coronary_procedure family. One row per unioned Reg_CORONARY_Pre/Reg_CORONARY_PostProcedure EntryId, with PRE_ and POST_ source fields kept separate. NHS number and MRN are deliberately published and column-tagged ig_risk=4/ig_severity=2 under the 2026-08-13 identifier doctrine. Person linkage copies the live iWeb current-alias plus consensus-safe historical fallback. DateOfDischarge raw text is retained beside parsed future/sentinel/clean fields; known source maximum 2026-12-10 is a future typo, never silently accepted. DATE_LAST_CHANGED is an edit timestamp, not the procedure date. LESION_CHILD_COUNT counts current iweb_coronary_lesion children; parent procedures with zero lesions are legitimate and retained. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_coronary_procedure():
    """Publish clinical_registry_coronary_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_coronary_procedure")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_eracs_episode ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_ERACS_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`date_of_entry` AS `date_of_entry`',
    '`erac_spatient` AS `erac_spatient`',
    '`consultant_cardiothoracictxt` AS `consultant_cardiothoracictxt`',
    '`primary_operatortxt` AS `primary_operatortxt`',
    '`cardiopulmonary_bypass_time` AS `cardiopulmonary_bypass_time`',
    '`cross_clamp_time` AS `cross_clamp_time`',
    '`complicationstxt` AS `complicationstxt`',
    '`m_other_complications` AS `m_other_complications`',
    '`blood_products_administeredtxt` AS `blood_products_administeredtxt`',
    '`medical_historytxt` AS `medical_historytxt`',
    '`m_other_medical_history` AS `m_other_medical_history`',
    '`echo_post_cb_preport_on_pacs` AS `echo_post_cb_preport_on_pacs`',
    '`l_vfunction_pre` AS `l_vfunction_pre`',
    '`r_vfunction_pre` AS `r_vfunction_pre`',
    '`chest_drainstxt` AS `chest_drainstxt`',
    '`pacing_wires` AS `pacing_wires`',
    '`m_underlying_rhythm_details` AS `m_underlying_rhythm_details`',
    '`underlying_rhythmtxt` AS `underlying_rhythmtxt`',
    '`setup_pacing_mode` AS `setup_pacing_mode`',
    '`setup_pacing_rate` AS `setup_pacing_rate`',
    '`vcap_threshold` AS `vcap_threshold`',
    '`acap_threshold` AS `acap_threshold`',
    '`dripstxt` AS `dripstxt`',
    '`pca` AS `pca`',
    '`vein_harvest_sitetxt` AS `vein_harvest_sitetxt`',
    '`antibioticstxt` AS `antibioticstxt`',
    '`timing_of2nd` AS `timing_of2nd`',
    '`timing_of1st` AS `timing_of1st`',
    '`admission_dt_tm` AS `admission_dt_tm`',
    '`patient_medically_ready_dt_tm` AS `patient_medically_ready_dt_tm`',
    '`actual_discharge_dt_tm` AS `actual_discharge_dt_tm`',
    '`discharge_to` AS `discharge_to`',
    '`first_time_sitting_dt_tm` AS `first_time_sitting_dt_tm`',
    '`first_mobilisation_dt_tm` AS `first_mobilisation_dt_tm`',
    '`drains_out_on_icu` AS `drains_out_on_icu`',
    '`drains_removed_dt` AS `drains_removed_dt`',
    '`total_volume_drained` AS `total_volume_drained`',
    '`cv_couton_icu` AS `cv_couton_icu`',
    '`two_couton_icu` AS `two_couton_icu`',
    '`temperature_at_arrival_on_icu` AS `temperature_at_arrival_on_icu`',
    '`p_wouton_icu` AS `p_wouton_icu`',
    '`blood_products_ic_utxt` AS `blood_products_ic_utxt`',
    '`patient_paced6hrs` AS `patient_paced6hrs`',
    '`pacing_mode` AS `pacing_mode`',
    '`arrhythmia_on_ic_utxt` AS `arrhythmia_on_ic_utxt`',
    '`pon_vscore` AS `pon_vscore`',
    '`pain_score` AS `pain_score`',
    '`pc_ausage12_hrs` AS `pc_ausage12_hrs`',
    '`total_amount_pca` AS `total_amount_pca`',
    '`m_other_medical_reason_for_delay` AS `m_other_medical_reason_for_delay`',
    '`m_other_non_medical_reason_for_delay` AS `m_other_non_medical_reason_for_delay`',
    '`non_medical_reasonstxt` AS `non_medical_reasonstxt`',
    '`filtertxt` AS `filtertxt`',
    '`m_other_arrhythmia_icu` AS `m_other_arrhythmia_icu`',
    '`l_vfunction_post_cpb` AS `l_vfunction_post_cpb`',
    '`r_vfunction_post_cpb` AS `r_vfunction_post_cpb`',
    '`eracs_medication_pathway` AS `eracs_medication_pathway`',
    '`m_other_drip_details` AS `m_other_drip_details`',
    '`other_regional_block` AS `other_regional_block`',
    '`discharged_hdu_dt_tm` AS `discharged_hdu_dt_tm`',
    '`procedure_performedtxt` AS `procedure_performedtxt`',
    '`aortic_valve` AS `aortic_valve`',
    '`mitral_valve` AS `mitral_valve`',
    '`tricuspid_valve` AS `tricuspid_valve`',
    '`aorta` AS `aorta`',
    '`achd` AS `achd`',
    '`m_other_procedure_performed` AS `m_other_procedure_performed`',
    '`other_aortic_procedure` AS `other_aortic_procedure`',
    '`other_ach_dprocedure` AS `other_ach_dprocedure`',
    '`heart_rate` AS `heart_rate`',
    '`crossclamp_time120_min` AS `crossclamp_time120_min`',
    '`bypasstime180_min` AS `bypasstime180_min`',
    '`patient_on_first_floor` AS `patient_on_first_floor`',
    '`allergiestxt` AS `allergiestxt`',
    '`m_other_allergies` AS `m_other_allergies`',
    '`medical_reasons_for_delaytxt` AS `medical_reasons_for_delaytxt`',
    '`antiemetictxt` AS `antiemetictxt`',
    '`ulcer_prophylaxistxt` AS `ulcer_prophylaxistxt`',
    '`i_vopiate_loading_at_end` AS `i_vopiate_loading_at_end`',
    '`anaesthetic_registrar_fellow` AS `anaesthetic_registrar_fellow`',
    '`complete_revascularisation` AS `complete_revascularisation`',
    '`manadatory` AS `manadatory`',
    '`medication_prescribed_on_powerchart` AS `medication_prescribed_on_powerchart`',
    '`post_op_plantxt` AS `post_op_plantxt`',
    '`m_other_post_op_plan_details` AS `m_other_post_op_plan_details`',
    '`p_min_backup` AS `p_min_backup`',
    '`backup_rate` AS `backup_rate`',
    '`cabg` AS `cabg`',
    '`baseline_act` AS `baseline_act`',
    '`post_protamine_act` AS `post_protamine_act`',
    '`platelet_mapping_performed` AS `platelet_mapping_performed`',
    '`teg_post_protamine_performed` AS `teg_post_protamine_performed`',
    '`surgical_site_checked` AS `surgical_site_checked`',
    '`m_volume_local_anaesthetic` AS `m_volume_local_anaesthetic`',
    '`adpma35mm` AS `adpma35mm`',
    '`aama35mm` AS `aama35mm`',
    '`rbc_number_of_units` AS `rbc_number_of_units`',
    '`ffp_number_of_units` AS `ffp_number_of_units`',
    '`cryo_number_of_units_transfused` AS `cryo_number_of_units_transfused`',
    '`octoplex_units_transfused` AS `octoplex_units_transfused`',
    '`cell_salvage_transfused` AS `cell_salvage_transfused`',
    '`fibrinogen_transfused` AS `fibrinogen_transfused`',
    '`areas_of_concern` AS `areas_of_concern`',
    '`m_teg_result_summary` AS `m_teg_result_summary`',
    '`timing_of_ondansetron` AS `timing_of_ondansetron`',
    '`paracetamol_administered` AS `paracetamol_administered`',
    '`timing_of_paracetamol_administered` AS `timing_of_paracetamol_administered`',
    '`suggamedex_administered` AS `suggamedex_administered`',
    '`post_op_analgesiatxt` AS `post_op_analgesiatxt`',
    '`platelet_number_of_units` AS `platelet_number_of_units`',
    '`grade_of_intubation` AS `grade_of_intubation`',
    '`antiplateletstxt` AS `antiplateletstxt`',
    '`other_antiplatelet` AS `other_antiplatelet`',
    '`spinal_drain_inserted` AS `spinal_drain_inserted`',
    '`spinal_drainage_above` AS `spinal_drainage_above`',
    '`issues_with_spinal_drain` AS `issues_with_spinal_drain`',
    '`m_details_of_spinal_drain_issues` AS `m_details_of_spinal_drain_issues`',
    '`m_thoracic_proceduredetails` AS `m_thoracic_proceduredetails`',
    '`cardiac_or_thoracic` AS `cardiac_or_thoracic`',
    '`thoracic_proceduretxt` AS `thoracic_proceduretxt`',
    '`bronchoscopy` AS `bronchoscopy`',
    '`lvrs` AS `lvrs`',
    '`lobes_operated_ontxt` AS `lobes_operated_ontxt`',
    '`lymph_node_resection` AS `lymph_node_resection`',
    '`consultant_thoracictxt` AS `consultant_thoracictxt`',
    '`intraoperative_pain_controltxt` AS `intraoperative_pain_controltxt`',
    '`postoperative_pain_controltxt` AS `postoperative_pain_controltxt`',
    '`tof` AS `tof`',
    '`instruction` AS `instruction`',
    '`post_op_planthoracic_patientstxt` AS `post_op_planthoracic_patientstxt`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`belmont_volume` AS `belmont_volume`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
]

CLINICAL_REGISTRY_ERACS_EPISODE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the eracs_episode source family and native entry_id.",
    "entry_id": "Internal surrogate identifier for the ERACS registry entry, carried through from the source registry record.",
    "mrn": "Local hospital medical record number identifying the patient for this ERACS registry entry.",
    "gender": "Recorded gender of the patient associated with the ERACS episode.",
    "date_of_death": "Recorded date of death of the patient, populated only where a death has been registered against the record.",
    "ethnic_origin": "Coded ethnic category recorded for the patient in the ERACS registry.",
    "date_of_entry": "Date and time the ERACS registry entry was recorded for the patient.",
    "erac_spatient": "Indicator of whether the patient was managed on the Enhanced Recovery After Cardiac Surgery pathway.",
    "consultant_cardiothoracictxt": "Name of the responsible cardiothoracic consultant recorded for the episode.",
    "primary_operatortxt": "Name of the clinician recorded as the primary operator for the procedure.",
    "cardiopulmonary_bypass_time": "Recorded cardiopulmonary bypass duration in minutes for the operation.",
    "cross_clamp_time": "Recorded aortic cross-clamp duration in minutes for the operation.",
    "complicationstxt": "Coded list of peri-operative complications recorded for the episode, with any additional detail held in the other complications field.",
    "m_other_complications": "Free-text description of complications not covered by the coded complications list.",
    "blood_products_administeredtxt": "Coded list of blood and salvage products administered during the episode.",
    "medical_historytxt": "Coded list of relevant pre-existing medical conditions recorded for the patient, with additional detail held in the other medical history field.",
    "m_other_medical_history": "Free-text narrative of additional medical history not captured by the coded medical history list.",
    "echo_post_cb_preport_on_pacs": "Indicator of whether the post-cardiopulmonary bypass echocardiogram report was available on PACS.",
    "l_vfunction_pre": "Categorical assessment of left ventricular function recorded before the procedure.",
    "r_vfunction_pre": "Categorical assessment of right ventricular function recorded before the procedure.",
    "chest_drainstxt": "Coded list of chest drain sites in place following the procedure.",
    "pacing_wires": "Categorical record of which temporary epicardial pacing wires were placed.",
    "m_underlying_rhythm_details": "Free-text detail describing the patient's underlying cardiac rhythm where the coded value requires clarification.",
    "underlying_rhythmtxt": "Coded underlying cardiac rhythm recorded for the patient.",
    "setup_pacing_mode": "Pacing mode configured when temporary pacing was set up for the patient.",
    "setup_pacing_rate": "Pacing rate configured when temporary pacing was set up for the patient.",
    "vcap_threshold": "Ventricular capture threshold recorded at the epicardial pacing wire check, held as a free-text value.",
    "acap_threshold": "Atrial capture threshold recorded at the epicardial pacing wire check, held as a free-text value.",
    "dripstxt": "Coded list of intravenous infusions running for the episode, stored as concatenated code and description pairs.",
    "pca": "Indicator of whether patient-controlled analgesia was used for the episode.",
    "vein_harvest_sitetxt": "Coded conduit vein harvest site or sites recorded for the operation, stored as concatenated code and description pairs.",
    "antibioticstxt": "Coded prophylactic antibiotic agents administered, stored as concatenated code and description pairs.",
    "timing_of2nd": "Recorded timing of the second prophylactic antibiotic dose, held as free text.",
    "timing_of1st": "Recorded timing of the first prophylactic antibiotic dose, held as free text.",
    "admission_dt_tm": "Date and time of the patient's admission for the ERACS pathway episode.",
    "patient_medically_ready_dt_tm": "Date and time the patient was recorded as medically ready for discharge.",
    "actual_discharge_dt_tm": "Date and time the patient was actually discharged.",
    "discharge_to": "Recorded destination the patient was discharged to at the end of the episode.",
    "first_time_sitting_dt_tm": "Date and time the patient first sat out as part of the enhanced recovery mobilisation pathway.",
    "first_mobilisation_dt_tm": "Date and time of the patient's first mobilisation after surgery.",
    "drains_out_on_icu": "Indicator of whether the chest drains were removed while the patient was on ICU.",
    "drains_removed_dt": "Date and time the chest drains were removed.",
    "total_volume_drained": "Total volume drained via the chest drains as recorded by the clinical team, held as free text.",
    "cv_couton_icu": "Indicator of whether the central venous catheter was removed while the patient was on ICU.",
    "two_couton_icu": "Indicator of whether a trial without urinary catheter took place while the patient was on ICU.",
    "temperature_at_arrival_on_icu": "Patient temperature recorded on arrival on ICU, held as free text.",
    "p_wouton_icu": "Indicator of whether the temporary pacing wires were removed while the patient was on ICU.",
    "blood_products_ic_utxt": "Coded blood products administered on ICU, stored as concatenated code and description pairs.",
    "patient_paced6hrs": "Indicator of whether the patient was paced during the first six hours on ICU.",
    "pacing_mode": "Pacing mode in use on ICU for the episode.",
    "arrhythmia_on_ic_utxt": "Coded arrhythmias observed on ICU, stored as concatenated code and description pairs.",
    "pon_vscore": "Recorded post-operative nausea and vomiting (PONV) score for the patient following cardiac or thoracic surgery, held as a coded text value.",
    "pain_score": "Recorded post-operative pain score for the episode, held as a coded text value.",
    "pc_ausage12_hrs": "Patient-controlled analgesia (PCA) usage recorded over the first 12 hours post-operatively.",
    "total_amount_pca": "Total patient-controlled analgesia amount recorded for the episode, stored as text without a documented unit.",
    "m_other_medical_reason_for_delay": "Free-text description of an other medical reason for delayed discharge or pathway progression, captured when the coded medical reason list does not apply.",
    "m_other_non_medical_reason_for_delay": "Free-text description of an other non-medical reason for delayed discharge or pathway progression, captured when the coded non-medical reason list does not apply.",
    "non_medical_reasonstxt": "Decoded description of the non-medical reason(s) recorded for delay in discharge or pathway progression.",
    "filtertxt": "Decoded description of the filter option selected on the ERACS peri-operative form.",
    "m_other_arrhythmia_icu": "Free-text description of an other arrhythmia observed on the intensive care unit when not covered by the coded arrhythmia list.",
    "l_vfunction_post_cpb": "Assessed left ventricular function following cardiopulmonary bypass, recorded as a graded category.",
    "r_vfunction_post_cpb": "Assessed right ventricular function following cardiopulmonary bypass, recorded as a graded category.",
    "eracs_medication_pathway": "Indicator of whether the ERACS medication pathway was followed for the episode.",
    "m_other_drip_details": "Free-text details of other infusions (drips) administered that are not covered by the coded drips list.",
    "other_regional_block": "Record of an other regional anaesthetic block used, where not covered by the standard block options.",
    "discharged_hdu_dt_tm": "Date and time the patient was discharged from the high dependency unit.",
    "procedure_performedtxt": "Decoded description of the cardiac surgical procedure performed for the episode.",
    "aortic_valve": "Aortic valve procedure or intervention type recorded for the episode.",
    "mitral_valve": "Mitral valve procedure or intervention type recorded for the episode.",
    "tricuspid_valve": "Tricuspid valve procedure or intervention type recorded for the episode.",
    "aorta": "Aortic procedure or intervention type recorded for the episode.",
    "achd": "Adult congenital heart disease procedure or intervention type recorded for the episode.",
    "m_other_procedure_performed": "Free-text description of the procedure performed when the coded procedure list records an other procedure.",
    "other_aortic_procedure": "Record of an other aortic procedure performed that is not covered by the standard aortic procedure options.",
    "other_ach_dprocedure": "Record of an other adult congenital heart disease procedure performed that is not covered by the standard options.",
    "heart_rate": "Recorded heart rate for the patient at the relevant peri-operative assessment point.",
    "crossclamp_time120_min": "Yes/No indicator recording whether the aortic cross-clamp time exceeded 120 minutes for this ERACS episode.",
    "bypasstime180_min": "Yes/No indicator recording whether the cardiopulmonary bypass time exceeded 180 minutes for this ERACS episode.",
    "patient_on_first_floor": "Indicator recording whether the patient was located on the first floor (step-down ward area) as part of the ERACS pathway.",
    "allergiestxt": "Coded picklist label(s) describing the patient's recorded allergies captured on the ERACS form.",
    "m_other_allergies": "Free-text description of an allergy selected as Other on the ERACS allergies question.",
    "medical_reasons_for_delaytxt": "Coded picklist label(s) recording medical reasons for delay in the patient's discharge or pathway progression.",
    "antiemetictxt": "Coded picklist label(s) recording the antiemetic agent administered during the ERACS pathway.",
    "ulcer_prophylaxistxt": "Coded picklist label(s) recording the ulcer prophylaxis medication given during the ERACS pathway.",
    "i_vopiate_loading_at_end": "Yes/No indicator of whether intravenous opiate loading was given at the end of the operation.",
    "anaesthetic_registrar_fellow": "Name of the anaesthetic registrar or fellow involved in the patient's procedure, entered as free text.",
    "complete_revascularisation": "Yes/No indicator of whether complete surgical revascularisation was achieved for the coronary procedure.",
    "manadatory": "Form control field (name misspelled from 'mandatory') used by the ERACS data-entry form, with no populated values observed.",
    "medication_prescribed_on_powerchart": "Yes/No indicator of whether the ERACS pathway medication was prescribed in the PowerChart electronic record.",
    "post_op_plantxt": "Coded picklist label(s) describing the agreed post-operative management plan for the patient.",
    "m_other_post_op_plan_details": "Free-text narrative detailing additional post-operative plan instructions where 'Other' was selected on the post-operative plan question.",
    "p_min_backup": "Yes/No indicator of whether backup (P-min) pacing was set for the patient post-operatively.",
    "backup_rate": "Numeric backup pacing rate set for the patient post-operatively.",
    "cabg": "Recorded detail of coronary artery bypass grafting performed, including the number of grafts.",
    "baseline_act": "Baseline activated clotting time (ACT) result recorded before heparinisation, stored as text.",
    "post_protamine_act": "Activated clotting time (ACT) result recorded after protamine administration, stored as text.",
    "platelet_mapping_performed": "Yes/No indicator of whether platelet mapping testing was performed for this episode.",
    "teg_post_protamine_performed": "Yes/No indicator of whether a thromboelastography (TEG) test was performed after protamine administration.",
    "surgical_site_checked": "Yes/No indicator of whether the surgical site was checked as part of the ERACS peri-operative compliance checks.",
    "m_volume_local_anaesthetic": "Free-text entry recording the volume of local anaesthetic administered.",
    "adpma35mm": "Yes/No indicator of whether the ADP maximum amplitude (MA) on platelet mapping was above 35 mm.",
    "aama35mm": "Indicator of whether the platelet mapping arachidonic acid maximum amplitude reached the 35 mm threshold, recorded as a yes/no response on the ERACS form.",
    "rbc_number_of_units": "Number of red blood cell units transfused for the ERACS episode.",
    "ffp_number_of_units": "Number of fresh frozen plasma units transfused for the ERACS episode.",
    "cryo_number_of_units_transfused": "Number of cryoprecipitate units transfused for the ERACS episode.",
    "octoplex_units_transfused": "Amount of Octaplex prothrombin complex concentrate transfused for the ERACS episode.",
    "cell_salvage_transfused": "Volume of cell-salvaged blood returned to the patient during the ERACS episode.",
    "fibrinogen_transfused": "Amount of fibrinogen concentrate transfused for the ERACS episode.",
    "areas_of_concern": "Free-text note describing clinical areas of concern identified during the ERACS pathway episode.",
    "m_teg_result_summary": "Free-text summary of thromboelastography (TEG) results recorded for the episode.",
    "timing_of_ondansetron": "Recorded time at which ondansetron was administered during the episode.",
    "paracetamol_administered": "Indicator of whether paracetamol was administered as part of the ERACS analgesia pathway.",
    "timing_of_paracetamol_administered": "Recorded time at which paracetamol was administered during the episode.",
    "suggamedex_administered": "Indicator of whether sugammadex was administered for neuromuscular blockade reversal during the episode.",
    "post_op_analgesiatxt": "Post-operative analgesia option(s) recorded for the episode from the ERACS coded picklist.",
    "platelet_number_of_units": "Number of platelet units transfused for the ERACS episode.",
    "grade_of_intubation": "Recorded laryngoscopy grade at intubation for the episode's airway management.",
    "antiplateletstxt": "Antiplatelet agent(s) recorded for the episode, selected from the ERACS coded picklist.",
    "other_antiplatelet": "Free-text detail of an antiplatelet agent not covered by the coded antiplatelet picklist.",
    "spinal_drain_inserted": "Indicator of whether a spinal (cerebrospinal fluid) drain was inserted during the episode.",
    "spinal_drainage_above": "Numeric spinal drainage threshold value recorded in the ERACS spinal drain section.",
    "issues_with_spinal_drain": "Indicator of whether any issues occurred with the spinal drain during the episode.",
    "m_details_of_spinal_drain_issues": "Free-text description of issues encountered with the spinal drain during the episode.",
    "m_thoracic_proceduredetails": "Free-text detail of the thoracic procedure performed where not fully captured by the coded procedure picklist.",
    "cardiac_or_thoracic": "Categorisation of the episode as a cardiac or thoracic surgical pathway.",
    "thoracic_proceduretxt": "Thoracic procedure recorded for the episode, selected from the ERACS coded picklist.",
    "bronchoscopy": "Indicator of whether bronchoscopy was performed as part of the thoracic surgical episode.",
    "lvrs": "Indicator of whether lung volume reduction surgery (LVRS) was performed for the thoracic episode.",
    "lobes_operated_ontxt": "Decoded description of the lung lobe or lobes operated on during the thoracic procedure.",
    "lymph_node_resection": "Indicator of whether lymph node resection was carried out during the thoracic procedure.",
    "consultant_thoracictxt": "Decoded name of the responsible thoracic surgery consultant recorded for the episode.",
    "intraoperative_pain_controltxt": "Decoded description of the intraoperative pain control method or methods used for the episode.",
    "postoperative_pain_controltxt": "Decoded description of the post-operative pain control method or methods used for the episode.",
    "tof": "Recorded train-of-four (TOF) count from neuromuscular blockade monitoring for the episode.",
    "instruction": "Form instruction or guidance text field on the ERACS data-entry form.",
    "post_op_planthoracic_patientstxt": "Decoded description of the agreed post-operative management plan for thoracic surgery patients.",
    "consultant_responsible_for_procedure": "Free-text entry recording the consultant responsible for the procedure.",
    "belmont_volume": "Volume recorded as delivered through the Belmont rapid infuser during the episode.",
    "date_last_changed": "Date and time the registry record was last changed in the source ERACS system.",
    "adc_updt": "Audit timestamp recording when the record was last updated during ingestion from the source registry.",
    "nhs_number_valid_ind": "Boolean flag indicating whether the NHS number held on the source registry record passed validation.",
    "linkage_status": "Coded status describing the outcome of linking the registry record to a master person record.",
    "person_id": "Internal surrogate identifier of the linked person record in the source patient administration system.",
    "linkage_method": "Code indicating which identifiers were used to link the registry record to the person record.",
    "linkage_historical_fallback_ind": "Boolean flag indicating whether the person linkage relied on a historical identifier fallback.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating whether a conflict was detected when a fallback identifier was used for person linkage.",
    "source_table": "Name of the source system table from which the record was ingested.",
    "source_record_key": "Key of the originating source registry record, derived from the source entry identifier.",
    "row_hash": "Hash value computed across the source record's field values to support change detection during ingestion.",
    "pipeline_loaded_at": "Timestamp when the ingestion pipeline loaded the record into this table.",
    "is_current_in_source": "Boolean flag indicating whether the record is still present as current in the source system.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the ingestion process first detected that the record was no longer present in the source registry.",
    "surname": "Patient's surname as recorded on the source ERACS registry record.",
    "forename": "Patient's forename as recorded on the source ERACS registry record.",
    "nhs_number": "NHS number recorded for the patient on the source ERACS registry record, used for national person identification and linkage.",
    "date_of_birth": "Patient's date of birth as recorded on the source ERACS registry record.",
}

CLINICAL_REGISTRY_ERACS_EPISODE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_eracs_episode.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_eracs_episode"),
    comment="Internal QC of clinical_registry_eracs_episode; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_ERACS_EPISODE_MANDATORY_RULES)
def _gold_qc_clinical_registry_eracs_episode():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_eracs_episode"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_ERACS_EPISODE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_ERACS_EPISODE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_eracs_episode"),
    comment='Typed child of clinical_registry_entry for the eracs_episode family. Enhanced Recovery After Cardiac Surgery pathway registry, live since 2025: pathway timestamps, pacing, TEG/platelet mapping, transfusion, mobilisation and discharge milestones. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_eracs_episode():
    """Publish clinical_registry_eracs_episode without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_eracs_episode")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_mortality_review ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_MORTALITY_REVIEW_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death_demog` AS `date_of_death_demog`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`dateof_death` AS `dateof_death`',
    '`date_and_time_of_death` AS `date_and_time_of_death`',
    '`consultant_in_charge_at_time_of_deathtxt` AS `consultant_in_charge_at_time_of_deathtxt`',
    '`date_and_time_of_admission` AS `date_and_time_of_admission`',
    '`date_and_time_of_last_medical_review` AS `date_and_time_of_last_medical_review`',
    '`critical_care_unit_episode_during_admission` AS `critical_care_unit_episode_during_admission`',
    '`causeofdeath_1a` AS `causeofdeath_1a`',
    '`causeofdeath_1b` AS `causeofdeath_1b`',
    '`causeofdeath_1c` AS `causeofdeath_1c`',
    '`causeofdeath_2` AS `causeofdeath_2`',
    '`significant_co_existing_factorstxt` AS `significant_co_existing_factorstxt`',
    '`was_the_patient_covered_by_dols_or_mc_a_order` AS `was_the_patient_covered_by_dols_or_mc_a_order`',
    '`coronial_case` AS `coronial_case`',
    '`outcome_of_coronial_referral` AS `outcome_of_coronial_referral`',
    '`serious_incident_or_complaint_raised_regarding_any_aspect` AS `serious_incident_or_complaint_raised_regarding_any_aspect`',
    '`discharge_summary_completed` AS `discharge_summary_completed`',
    '`timeto1st_review_hrs` AS `timeto1st_review_hrs`',
    '`patient_admitted_to_appropriate_ward` AS `patient_admitted_to_appropriate_ward`',
    '`any_period_when_pt_not_reviewed_by_a_consultant_for72_hours` AS `any_period_when_pt_not_reviewed_by_a_consultant_for72_hours`',
    '`did_the_pt_fall_whilst_on_the_wards` AS `did_the_pt_fall_whilst_on_the_wards`',
    '`did_the_pt_develop_a_pressure_ulcer_since_admission` AS `did_the_pt_develop_a_pressure_ulcer_since_admission`',
    '`was_there_a_clear_treatment_escalation_plan_documented` AS `was_there_a_clear_treatment_escalation_plan_documented`',
    '`was_there_evidence_of_a_resuscitation_decision_documented` AS `was_there_evidence_of_a_resuscitation_decision_documented`',
    '`was_the_pt_cared_for_using_the_compassionate_care_plan` AS `was_the_pt_cared_for_using_the_compassionate_care_plan`',
    '`ncepod_classification` AS `ncepod_classification`',
    '`m_why_is_ncepod_bcde` AS `m_why_is_ncepod_bcde`',
    '`m_anything_else_of_note` AS `m_anything_else_of_note`',
    '`date_of_completion_of_form` AS `date_of_completion_of_form`',
    '`date_of_mortality_review` AS `date_of_mortality_review`',
    '`ward_where_patient_died` AS `ward_where_patient_died`',
    '`consultant_responsible_for_procedure` AS `consultant_responsible_for_procedure`',
    '`primary_operator` AS `primary_operator`',
    '`specialitytxt` AS `specialitytxt`',
    '`m_note` AS `m_note`',
    '`m_clinical_historyevents` AS `m_clinical_historyevents`',
    '`entered_into_database` AS `entered_into_database`',
    '`presented_bytxt` AS `presented_bytxt`',
    '`peereview` AS `peereview`',
    '`cardiology_attendeestxt` AS `cardiology_attendeestxt`',
    '`surgical_attendeestxt` AS `surgical_attendeestxt`',
    '`chairman` AS `chairman`',
    '`medical_examiner` AS `medical_examiner`',
    '`cremated` AS `cremated`',
    '`me_discussed` AS `me_discussed`',
    '`survey_off` AS `survey_off`',
    '`me_classification` AS `me_classification`',
    '`me_review_dt` AS `me_review_dt`',
    '`death_ccp` AS `death_ccp`',
    '`ccp_initiated` AS `ccp_initiated`',
    '`m_me_notes` AS `m_me_notes`',
    '`dsummarycomp` AS `dsummarycomp`',
    '`further_action_to_be_taken` AS `further_action_to_be_taken`',
    '`m_action_required` AS `m_action_required`',
    '`lfd_concern` AS `lfd_concern`',
    '`lfd_resus` AS `lfd_resus`',
    '`lfd_recog` AS `lfd_recog`',
    '`lfd_commun` AS `lfd_commun`',
    '`lf_documentation` AS `lf_documentation`',
    '`lfd_procedural` AS `lfd_procedural`',
    '`lfd_other` AS `lfd_other`',
    '`mlfd_details` AS `mlfd_details`',
    '`mlfd_action` AS `mlfd_action`',
    '`cr_sinstruction` AS `cr_sinstruction`',
    '`at_the_end_of_the_admission` AS `at_the_end_of_the_admission`',
    '`during_admission` AS `during_admission`',
    '`was_this_an_elective_death` AS `was_this_an_elective_death`',
    '`problem_in_assessment` AS `problem_in_assessment`',
    '`problem_with_medication` AS `problem_with_medication`',
    '`problem_related_to_treatment_and_management_plan` AS `problem_related_to_treatment_and_management_plan`',
    '`problem_with_infection_management` AS `problem_with_infection_management`',
    '`problem_related_to_operative_invasive_procedure` AS `problem_related_to_operative_invasive_procedure`',
    '`problem_in_clinical_monitoring` AS `problem_in_clinical_monitoring`',
    '`problem_in_resuscitation` AS `problem_in_resuscitation`',
    '`problem_of_any_other_type` AS `problem_of_any_other_type`',
    '`m_sharing_education` AS `m_sharing_education`',
    '`m_quality_improvement_project` AS `m_quality_improvement_project`',
    '`m_other_specific_action` AS `m_other_specific_action`',
    '`were_there_any_problems_with_the_care` AS `were_there_any_problems_with_the_care`',
    '`did_the_problem_lead_to_harm1` AS `did_the_problem_lead_to_harm1`',
    '`did_the_problem_lead_to_harm2` AS `did_the_problem_lead_to_harm2`',
    '`did_the_problem_lead_to_harm3` AS `did_the_problem_lead_to_harm3`',
    '`did_the_problem_lead_to_harm4` AS `did_the_problem_lead_to_harm4`',
    '`did_the_problem_lead_to_harm5` AS `did_the_problem_lead_to_harm5`',
    '`did_the_problem_lead_to_harm6` AS `did_the_problem_lead_to_harm6`',
    '`did_the_problem_lead_to_harm7` AS `did_the_problem_lead_to_harm7`',
    '`did_the_problem_lead_to_harm8` AS `did_the_problem_lead_to_harm8`',
    '`m_other_problem_details` AS `m_other_problem_details`',
    '`presented_by_other` AS `presented_by_other`',
    '`form_completed_bytxt` AS `form_completed_bytxt`',
    '`form_completed_by_other` AS `form_completed_by_other`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`date_of_death_quality` AS `date_of_death_quality`',
    '`date_of_death_clean` AS `date_of_death_clean`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`nameofkin` AS `nameofkin`',
    '`phone_of_kin` AS `phone_of_kin`',
    '`m_commentskin` AS `m_commentskin`',
]

CLINICAL_REGISTRY_MORTALITY_REVIEW_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the mortality_review source family and native entry_id.",
    "entry_id": "Internal surrogate identifier for the mortality review record, carried from the source registry entry key.",
    "mrn": "Local hospital medical record number identifying the deceased patient in the source registry.",
    "gender": "Recorded gender of the patient as held in the source registry demographics.",
    "date_of_death_demog": "Date of death taken from the patient demographic record, as distinct from the review form death date.",
    "ethnic_origin": "Coded ethnic origin of the patient as recorded in the source registry demographics.",
    "dateof_death": "Date of death as recorded on the mortality review form, retained in raw form alongside the cleaned derivation.",
    "date_and_time_of_death": "Date and time of death recorded on the mortality review form.",
    "consultant_in_charge_at_time_of_deathtxt": "Free-text name of the consultant responsible for the patient at the time of death.",
    "date_and_time_of_admission": "Date and time the patient was admitted for the admission during which the death occurred.",
    "date_and_time_of_last_medical_review": "Date and time of the last documented medical review of the patient before death.",
    "critical_care_unit_episode_during_admission": "Indicator of whether the patient had a critical care unit episode during the admission.",
    "causeofdeath_1a": "Source-text cause of death recorded in part 1a of the death certificate chain, not asserted to be coded.",
    "causeofdeath_1b": "Source-text cause of death recorded in part 1b of the death certificate chain, not asserted to be coded.",
    "causeofdeath_1c": "Source-text cause of death recorded in part 1c of the death certificate chain, not asserted to be coded.",
    "causeofdeath_2": "Source-text contributory conditions recorded in part 2 of the death certificate, not asserted to be coded.",
    "significant_co_existing_factorstxt": "Text field listing significant co-existing factors or comorbidities recorded for the death review.",
    "was_the_patient_covered_by_dols_or_mc_a_order": "Indicator of whether the patient was subject to a Deprivation of Liberty Safeguards or Mental Capacity Act order.",
    "coronial_case": "Indicator of whether the death was referred to or treated as a coronial case.",
    "outcome_of_coronial_referral": "Recorded outcome of the referral of the death to the coroner.",
    "serious_incident_or_complaint_raised_regarding_any_aspect": "Indicator of whether a serious incident or complaint was raised regarding any aspect of the patient's care.",
    "discharge_summary_completed": "Indicator of whether a discharge summary was completed for the admission.",
    "timeto1st_review_hrs": "Time from admission to the first medical review, recorded in whole hours.",
    "patient_admitted_to_appropriate_ward": "Reviewer assessment of whether the patient was admitted to an appropriate ward.",
    "any_period_when_pt_not_reviewed_by_a_consultant_for72_hours": "Indicator of whether there was any period during which the patient was not reviewed by a consultant for 72 hours.",
    "did_the_pt_fall_whilst_on_the_wards": "Indicator of whether the patient sustained a fall while on the wards during the admission.",
    "did_the_pt_develop_a_pressure_ulcer_since_admission": "Reviewer-recorded categorical response indicating whether the patient developed a pressure ulcer after admission.",
    "was_there_a_clear_treatment_escalation_plan_documented": "Reviewer-recorded categorical response indicating whether a clear treatment escalation plan was documented.",
    "was_there_evidence_of_a_resuscitation_decision_documented": "Reviewer-recorded categorical response indicating whether a documented resuscitation decision was evident in the record.",
    "was_the_pt_cared_for_using_the_compassionate_care_plan": "Reviewer-recorded categorical response indicating whether the patient was cared for under the compassionate care plan.",
    "ncepod_classification": "NCEPOD care-quality grading assigned to the death at mortality review, held as a coded category label.",
    "m_why_is_ncepod_bcde": "Free-text reviewer explanation of why the case was graded in the lower NCEPOD categories.",
    "m_anything_else_of_note": "Free-text narrative capturing any additional points of note from the mortality or M&M review.",
    "date_of_completion_of_form": "Date on which the mortality review form was completed.",
    "date_of_mortality_review": "Date on which the mortality or M&M review of the case took place.",
    "ward_where_patient_died": "Ward or clinical area where the patient died, recorded as a local location label.",
    "consultant_responsible_for_procedure": "Consultant with overall responsibility for the procedure, held as a source-system staff reference.",
    "primary_operator": "Primary operator performing the procedure, held as a source-system staff reference.",
    "specialitytxt": "Specialty or specialties associated with the case, recorded as coded label text from the source picklist.",
    "m_note": "Free-text note recorded against the mortality review record.",
    "m_clinical_historyevents": "Free-text clinical history and sequence of events leading to the death, as summarised for review.",
    "entered_into_database": "Source-system indicator recording whether the case has been entered into the review database.",
    "presented_bytxt": "Clinician who presented the case at the mortality or M&M meeting, recorded as name text.",
    "peereview": "Indicator of whether the case underwent peer review as part of the mortality review process.",
    "cardiology_attendeestxt": "Cardiology staff recorded as attending the mortality or M&M meeting, held as name text.",
    "surgical_attendeestxt": "Surgical staff recorded as attending the mortality or M&M meeting, held as name text.",
    "chairman": "Chair of the mortality or M&M review meeting, recorded as a short staff reference or initials.",
    "medical_examiner": "Medical examiner associated with scrutiny of the death, recorded as a staff reference.",
    "cremated": "Indicator of whether the death proceeded to cremation, relevant to certification and medical examiner processes.",
    "me_discussed": "Indicator of whether the death was discussed with the medical examiner.",
    "survey_off": "Source-system flag indicating whether the review survey or questionnaire section is switched off for this record.",
    "me_classification": "Medical examiner's classification outcome recorded for the death at scrutiny.",
    "me_review_dt": "Date and time the medical examiner review of the death was carried out.",
    "death_ccp": "Indicator of whether the patient died while on the compassionate care plan pathway.",
    "ccp_initiated": "Indicator of whether a compassionate care plan was initiated for the patient.",
    "m_me_notes": "Free-text notes recorded by or about the medical examiner review.",
    "dsummarycomp": "Date and time the discharge summary was completed.",
    "further_action_to_be_taken": "Indicator of whether further action is required following the mortality review.",
    "m_action_required": "Free-text description of the actions required following the mortality review.",
    "lfd_concern": "Learning-from-deaths flag indicating whether a general concern about care was identified.",
    "lfd_resus": "Learning-from-deaths flag indicating whether a concern relating to resuscitation was identified.",
    "lfd_recog": "Learning-from-deaths flag indicating whether a concern relating to recognition of deterioration or illness was identified.",
    "lfd_commun": "Learning-from-deaths flag indicating whether a concern relating to communication was identified.",
    "lf_documentation": "Learning-from-deaths flag indicating whether a concern relating to documentation was identified.",
    "lfd_procedural": "Learning-from-deaths flag indicating whether a concern relating to a procedure was identified.",
    "lfd_other": "Learning-from-deaths flag indicating whether a concern of any other type was identified.",
    "mlfd_details": "Free-text details of the learning-from-deaths concerns identified at review.",
    "mlfd_action": "Free-text description of actions arising from the learning-from-deaths concerns.",
    "cr_sinstruction": "Field recording the coroner-related instruction captured during the mortality review.",
    "at_the_end_of_the_admission": "Review response describing the care position at the end of the admission.",
    "during_admission": "Review response describing the care position during the admission.",
    "was_this_an_elective_death": "Indicator of whether the death followed an elective admission or procedure.",
    "problem_in_assessment": "Indicator of whether a problem in patient assessment was identified during the mortality review.",
    "problem_with_medication": "Indicator of whether a medication-related problem was identified during the mortality review.",
    "problem_related_to_treatment_and_management_plan": "Indicator of whether a problem with the treatment and management plan was identified during the mortality review.",
    "problem_with_infection_management": "Indicator of whether a problem with infection management was identified during the mortality review.",
    "problem_related_to_operative_invasive_procedure": "Structured review response indicating whether a problem related to an operative or invasive procedure was identified during the mortality review.",
    "problem_in_clinical_monitoring": "Structured review response indicating whether a problem in clinical monitoring of the patient was identified during the mortality review.",
    "problem_in_resuscitation": "Structured review response indicating whether a problem in resuscitation was identified during the mortality review.",
    "problem_of_any_other_type": "Structured review response indicating whether a care problem of any other type was identified during the mortality review.",
    "m_sharing_education": "Free-text note describing sharing or educational actions agreed as a result of the mortality review.",
    "m_quality_improvement_project": "Free-text note describing any quality improvement project arising from the mortality review.",
    "m_other_specific_action": "Free-text note describing any other specific action agreed following the mortality review.",
    "were_there_any_problems_with_the_care": "Structured reviewer response stating whether any problems with the patient's care were identified, including branching guidance options.",
    "did_the_problem_lead_to_harm1": "Structured assessment of whether the first identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm2": "Structured assessment of whether the second identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm3": "Structured assessment of whether the third identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm4": "Structured assessment of whether the fourth identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm5": "Structured assessment of whether the fifth identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm6": "Structured assessment of whether the sixth identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm7": "Structured assessment of whether the seventh identified care problem led to patient harm.",
    "did_the_problem_lead_to_harm8": "Structured assessment of whether the eighth identified care problem led to patient harm.",
    "m_other_problem_details": "Free-text narrative describing the other care problem identified during the mortality review.",
    "presented_by_other": "Free-text entry naming the presenter of the case at the review meeting when not available in the standard selection list.",
    "form_completed_bytxt": "Text value of the staff member recorded as having completed the mortality review form.",
    "form_completed_by_other": "Free-text entry naming the person who completed the form when not available in the standard selection list.",
    "date_last_changed": "Timestamp recording when the mortality review record was last modified in the source registry.",
    "adc_updt": "Timestamp of the last extract or update of the record by the ingestion process.",
    "nhs_number_valid_ind": "Derived boolean indicator showing whether the source NHS number passed validation during processing.",
    "linkage_status": "Coded status describing the outcome of linking the review record to a master patient identity.",
    "person_id": "Internal system-assigned identifier of the linked patient record in the master person table.",
    "linkage_method": "Code describing the identifier combination used to link the mortality review record to the master person record.",
    "linkage_historical_fallback_ind": "Boolean flag indicating that patient linkage was resolved using a historical (non-current) alias fallback rather than a current identifier match.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating that the fallback linkage attempt produced conflicting person matches.",
    "date_of_death_quality": "VALID | FUTURE | MISSING; raw DATEOF_DEATH is retained.",
    "date_of_death_clean": "Date of death with future values nulled.",
    "source_table": "Name of the upstream source table from which the record was ingested.",
    "source_record_key": "Primary key value of the originating source record, held as text for provenance and deduplication.",
    "row_hash": "Hash of the source record's field values used to detect changes between pipeline loads.",
    "pipeline_loaded_at": "Timestamp recording when the record was loaded into this table by the ingestion pipeline.",
    "is_current_in_source": "Boolean flag indicating whether the record was still present in the source system at the latest pipeline load.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the pipeline first detected that the record was no longer present in the source system.",
    "surname": "Family name of the deceased patient as recorded in the source mortality review registry.",
    "forename": "Given name of the deceased patient as recorded in the source mortality review registry.",
    "nhs_number": "NHS number of the patient as held in the source mortality review registry, retained in its source text formatting.",
    "date_of_birth": "Date of birth of the patient as recorded in the source mortality review registry.",
    "nameofkin": "Free-text name of the patient's next of kin as recorded on the mortality review form.",
    "phone_of_kin": "Contact telephone number for the patient's next of kin recorded on the mortality review form.",
    "m_commentskin": "Free-text comments relating to the patient's next of kin, such as contact or communication notes recorded at review.",
}

CLINICAL_REGISTRY_MORTALITY_REVIEW_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_mortality_review.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_mortality_review"),
    comment="Internal QC of clinical_registry_mortality_review; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_MORTALITY_REVIEW_MANDATORY_RULES)
def _gold_qc_clinical_registry_mortality_review():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_mortality_review"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_MORTALITY_REVIEW_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_MORTALITY_REVIEW_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_mortality_review"),
    comment='Typed child of clinical_registry_entry for the mortality_review family. Cardiac mortality/M&M review registry: source-text cause-of-death chain (not asserted to be ICD-coded), NCEPOD classification, coronial/medical-examiner fields and structured care-problem flags. Patient names, contact details and next-of-kin content are retained and governed with IG tags. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_mortality_review():
    """Publish clinical_registry_mortality_review without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_mortality_review")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_noncoronary_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`dateofprocedure` AS `dateofprocedure`',
    '`sex` AS `sex`',
    '`patient_ethnic_group` AS `patient_ethnic_group`',
    '`administrative_category` AS `administrative_category`',
    '`procedure_urgency` AS `procedure_urgency`',
    '`height` AS `height`',
    '`weight` AS `weight`',
    '`body_surface_area` AS `body_surface_area`',
    '`body_mass_index` AS `body_mass_index`',
    '`consultant_responsible_for_proceduretxt` AS `consultant_responsible_for_proceduretxt`',
    '`primary_operatortxt` AS `primary_operatortxt`',
    '`second_operatortxt` AS `second_operatortxt`',
    '`third_operatortxt` AS `third_operatortxt`',
    '`primary_operator_status` AS `primary_operator_status`',
    '`second_operator_status` AS `second_operator_status`',
    '`third_operator_status` AS `third_operator_status`',
    '`local_procedure_identifier` AS `local_procedure_identifier`',
    '`training_procedure` AS `training_procedure`',
    '`research_procedure` AS `research_procedure`',
    '`research_title` AS `research_title`',
    '`referring_hospitaltxt` AS `referring_hospitaltxt`',
    '`m_procedure_findings` AS `m_procedure_findings`',
    '`arterial_managementtxt` AS `arterial_managementtxt`',
    '`arterial_complicationstxt` AS `arterial_complicationstxt`',
    '`device_failure` AS `device_failure`',
    '`procedural_complicationtxt` AS `procedural_complicationtxt`',
    '`management_plan` AS `management_plan`',
    '`status_at_discharge` AS `status_at_discharge`',
    '`data_complete` AS `data_complete`',
    '`date_of_discharge_death` AS `date_of_discharge_death`',
    '`hospital_outcometxt` AS `hospital_outcometxt`',
    '`m_procedure_findings_and_report` AS `m_procedure_findings_and_report`',
    '`investigationstxt` AS `investigationstxt`',
    '`interventionstxt` AS `interventionstxt`',
    '`click_here` AS `click_here`',
    '`m_pre_discharge_instructions` AS `m_pre_discharge_instructions`',
    '`m_discharge_management_plan` AS `m_discharge_management_plan`',
    '`m_patient_info` AS `m_patient_info`',
    '`mg_pinfo` AS `mg_pinfo`',
    '`age_at_intervention` AS `age_at_intervention`',
    '`other_intervention_description` AS `other_intervention_description`',
    '`referring_consultant_cardiologisttxt` AS `referring_consultant_cardiologisttxt`',
    '`nurse_led_discharge` AS `nurse_led_discharge`',
    '`m_indications_and_background_history` AS `m_indications_and_background_history`',
    '`m_details_of_all_devices_implanted` AS `m_details_of_all_devices_implanted`',
    '`m_outcome_of_the_procedure` AS `m_outcome_of_the_procedure`',
    '`other_investigation` AS `other_investigation`',
    '`tunnel_length` AS `tunnel_length`',
    '`contrast_volume` AS `contrast_volume`',
    '`date_last_changed` AS `date_last_changed`',
    '`m_complication_details` AS `m_complication_details`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
    '`postcode_of_usual_address` AS `postcode_of_usual_address`',
    '`hospital_identifier` AS `hospital_identifier`',
]

CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the noncoronary_procedure source family and native entry_id.",
    "entry_id": "Internal iWeb registry entry identifier for the non-coronary procedure record, carried through from the source registry table.",
    "mrn": "Local hospital medical record number identifying the patient for this non-coronary procedure record.",
    "gender": "Recorded gender of the patient at the time of the non-coronary procedure.",
    "date_of_death": "Recorded date of death of the patient, where known.",
    "ethnic_origin": "Coded ethnic origin of the patient as recorded in the iWeb registry.",
    "dateofprocedure": "Date on which the non-coronary catheter-laboratory procedure was performed.",
    "sex": "Recorded sex of the patient at the time of the non-coronary procedure.",
    "patient_ethnic_group": "Free-text or locally recorded patient ethnic group description captured in the registry.",
    "administrative_category": "Administrative funding category under which the patient was treated for the procedure.",
    "procedure_urgency": "Categorical record of the urgency with which the non-coronary catheter-laboratory procedure was undertaken.",
    "height": "Recorded patient height used for body-size calculations in the registry record.",
    "weight": "Recorded patient weight used for body-size calculations in the registry record.",
    "body_surface_area": "Patient body surface area derived from recorded height and weight.",
    "body_mass_index": "Patient body mass index derived from recorded height and weight.",
    "consultant_responsible_for_proceduretxt": "Name of the consultant with overall responsibility for the non-coronary procedure, selected from the iWeb clinician lookup.",
    "primary_operatortxt": "Name of the primary operator performing the non-coronary procedure, as free text.",
    "second_operatortxt": "Name of the second operator assisting with the non-coronary procedure, as free text.",
    "third_operatortxt": "Name of the third operator assisting with the non-coronary procedure, as free text.",
    "primary_operator_status": "Grade or role status of the primary operator for the procedure.",
    "second_operator_status": "Grade or role status of the second operator for the procedure.",
    "third_operator_status": "Grade or role status of the third operator for the procedure.",
    "local_procedure_identifier": "Locally assigned identifier for the procedure as recorded in the iWeb registry.",
    "training_procedure": "Indicator of whether the procedure was performed as a training case.",
    "research_procedure": "Indicator of whether the procedure was performed as part of a research study.",
    "research_title": "Title of the research study associated with the procedure, where applicable.",
    "referring_hospitaltxt": "Text description of the hospital that referred the patient for the non-coronary procedure, taken from the iWeb referring-hospital lookup.",
    "m_procedure_findings": "Free-text memo recording the findings of the non-coronary catheter-laboratory procedure.",
    "arterial_managementtxt": "Decoded label describing how the arterial access site was managed at the end of the procedure.",
    "arterial_complicationstxt": "Decoded label recording arterial access-site complications associated with the procedure.",
    "device_failure": "Indicator recording whether a device failure occurred during the procedure.",
    "procedural_complicationtxt": "Decoded label recording procedural complications occurring during the non-coronary intervention.",
    "management_plan": "Recorded management plan for the patient following the non-coronary procedure.",
    "status_at_discharge": "Categorical record of the patient's status at discharge from the admission in which the procedure took place.",
    "data_complete": "Data-quality flag indicating whether the registry record has been marked as complete.",
    "date_of_discharge_death": "Date of discharge or of in-hospital death following the non-coronary procedure.",
    "hospital_outcometxt": "Decoded label describing the patient's hospital outcome for the procedural admission.",
    "m_procedure_findings_and_report": "Free-text procedural report describing access, technique and findings of the non-coronary intervention.",
    "investigationstxt": "Decoded label or labels for investigations performed during the non-coronary procedure, with local N-Label values also exploded in iweb_multiselect_value.",
    "interventionstxt": "Decoded label or labels for the interventions performed, with local N-Label values also exploded in iweb_multiselect_value.",
    "click_here": "iWeb form control field with no documented clinical meaning, retained as received from the source registry.",
    "m_pre_discharge_instructions": "Free-text memo holding instructions given to the patient before discharge after the procedure.",
    "m_discharge_management_plan": "Free-text memo describing the discharge management plan following the non-coronary procedure.",
    "m_patient_info": "Free-text memo recording information given to the patient about the procedure and follow-up.",
    "mg_pinfo": "Free-text memo holding information for the general practitioner regarding follow-up after the procedure.",
    "age_at_intervention": "Patient's age in whole years at the time of the non-coronary intervention.",
    "other_intervention_description": "Free-text description of an intervention performed when it is not covered by the coded intervention list.",
    "referring_consultant_cardiologisttxt": "Name of the consultant cardiologist who referred the patient for the non-coronary procedure, selected from the iWeb clinician lookup.",
    "nurse_led_discharge": "Indicator of whether the patient's discharge after the procedure was nurse-led.",
    "m_indications_and_background_history": "Free-text memo describing the indication for the procedure and the patient's relevant background medical history.",
    "m_details_of_all_devices_implanted": "Free-text memo listing the devices implanted during the procedure, including model and serial-number details.",
    "m_outcome_of_the_procedure": "Free-text memo describing the outcome of the non-coronary catheter-laboratory procedure.",
    "other_investigation": "Free-text description of an investigation performed during the procedure when it is not covered by the coded investigation list.",
    "tunnel_length": "Recorded tunnel length measurement captured for the non-coronary procedure in the iWeb registry.",
    "contrast_volume": "Volume of contrast medium used during the non-coronary procedure as recorded in the registry.",
    "date_last_changed": "Timestamp recording when the registry record was last amended in the source iWeb system.",
    "m_complication_details": "Free-text memo giving details of complications occurring in relation to the non-coronary procedure.",
    "adc_updt": "Technical timestamp from the source extract indicating when the record was last updated for downstream loading.",
    "nhs_number_valid_ind": "Boolean flag indicating whether the NHS number held on the source registry record passed validation, derived without retaining the number itself.",
    "linkage_status": "Coded status describing the outcome of linking this registry record to a master patient index person record.",
    "person_id": "Internal system-assigned unique identifier for the patient in the Millennium person table, used to link this record to the master patient index.",
    "linkage_method": "Code indicating which identifiers were used to match this registry record to a person record during linkage processing.",
    "linkage_historical_fallback_ind": "Boolean flag indicating that the person match was achieved using a historical identifier rather than a current one.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating that the fallback linkage produced a conflicting person match requiring review.",
    "source_table": "Name of the source system table from which this record was ingested.",
    "source_record_key": "Key identifying the originating source-system record, populated from the iWeb registry entry identifier.",
    "row_hash": "Hash value computed across the source record's fields to support change detection during ingestion.",
    "pipeline_loaded_at": "Timestamp recording when the ingestion pipeline loaded this row into the bronze table.",
    "is_current_in_source": "Boolean flag indicating whether the record is still present as a current record in the source system.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the ingestion process first detected that this record was no longer present in the source registry extract.",
    "surname": "Patient's surname as recorded on the iWeb non-coronary procedure registry record.",
    "forename": "Patient's forename as recorded on the iWeb non-coronary procedure registry record.",
    "nhs_number": "Patient's NHS number as held on the source iWeb registry record, with a separate flag indicating whether it passed validation.",
    "date_of_birth": "Patient's date of birth as recorded on the iWeb non-coronary procedure registry record.",
    "postcode_of_usual_address": "Postcode of the patient's usual address as recorded on the iWeb non-coronary procedure registry record.",
    "hospital_identifier": "Identifier of the hospital or site recorded against the non-coronary procedure in the iWeb registry.",
}

CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_noncoronary_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_noncoronary_procedure"),
    comment="Internal QC of clinical_registry_noncoronary_procedure; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_registry_noncoronary_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_noncoronary_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_noncoronary_procedure"),
    comment='Typed child of clinical_registry_entry for the noncoronary_procedure family. iWeb non-coronary/structural cath-lab registry covering interventions such as TAVI, TMVI, ASD/VSD closure and septal ablation. Local N-Label values are retained here and exploded separately in iweb_multiselect_value. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_noncoronary_procedure():
    """Publish clinical_registry_noncoronary_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_noncoronary_procedure")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_surgery_episode ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_SURGERY_EPISODE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`gender` AS `gender`',
    '`date_of_birth` AS `date_of_birth`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`date_of_operation` AS `date_of_operation`',
    '`age_at_operation` AS `age_at_operation`',
    '`date_of_admission` AS `date_of_admission`',
    '`administrative_category` AS `administrative_category`',
    '`angina_status_pre_surgery` AS `angina_status_pre_surgery`',
    '`dyspnoea_status_pre_surgery` AS `dyspnoea_status_pre_surgery`',
    '`number_of_previous_mis` AS `number_of_previous_mis`',
    '`interval_between_surgery_and_last_mi` AS `interval_between_surgery_and_last_mi`',
    '`previous_pci` AS `previous_pci`',
    '`previous_cardiac_surgerytxt` AS `previous_cardiac_surgerytxt`',
    '`date_of_last_cardiac_operation` AS `date_of_last_cardiac_operation`',
    '`diabetes_management` AS `diabetes_management`',
    '`cigarette_smoking_history` AS `cigarette_smoking_history`',
    '`history_of_hypertension` AS `history_of_hypertension`',
    '`dialysis` AS `dialysis`',
    '`history_of_pulmonary_disease` AS `history_of_pulmonary_disease`',
    '`history_of_neurological_diseasetxt` AS `history_of_neurological_diseasetxt`',
    '`history_of_neurological_dysfunction` AS `history_of_neurological_dysfunction`',
    '`extracardiac_arteriopathy` AS `extracardiac_arteriopathy`',
    '`pre_operative_heart_rhythm` AS `pre_operative_heart_rhythm`',
    '`left_heart_catheterisation` AS `left_heart_catheterisation`',
    '`date_of_last_catheterisation` AS `date_of_last_catheterisation`',
    '`extent_of_coronary_vessel_disease` AS `extent_of_coronary_vessel_disease`',
    '`ejection_fraction_category` AS `ejection_fraction_category`',
    '`pa_systolic` AS `pa_systolic`',
    '`severity_of_aortic_valve_stenosis_eoa` AS `severity_of_aortic_valve_stenosis_eoa`',
    '`intravenous_nitrates_or_any_heparin` AS `intravenous_nitrates_or_any_heparin`',
    '`intravenous_inotropes_prior_to_anaesthesia` AS `intravenous_inotropes_prior_to_anaesthesia`',
    '`ventilated_pre_operation` AS `ventilated_pre_operation`',
    '`cardiogenic_shock_pre_operation` AS `cardiogenic_shock_pre_operation`',
    '`date_and_time_of_operation` AS `date_and_time_of_operation`',
    '`operative_urgency` AS `operative_urgency`',
    '`number_of_previous_heart_operations` AS `number_of_previous_heart_operations`',
    '`responsible_consultant_anaesthetisttxt` AS `responsible_consultant_anaesthetisttxt`',
    '`first_operatortxt` AS `first_operatortxt`',
    '`first_operator_grade` AS `first_operator_grade`',
    '`first_operator_calman_year_of_trainee` AS `first_operator_calman_year_of_trainee`',
    '`first_assistanttxt` AS `first_assistanttxt`',
    '`first_assistant_grade` AS `first_assistant_grade`',
    '`first_assistant_calman_year_of_trainee` AS `first_assistant_calman_year_of_trainee`',
    '`left_main_stem_disease` AS `left_main_stem_disease`',
    '`left_ventricular_function` AS `left_ventricular_function`',
    '`cardiac_procedures` AS `cardiac_procedures`',
    '`other_actual_cardiac_procedurestxt` AS `other_actual_cardiac_procedurestxt`',
    '`actual_creatinine_at_time_of_surgery` AS `actual_creatinine_at_time_of_surgery`',
    '`severity_of_aortic_valve_stenosis_gradient` AS `severity_of_aortic_valve_stenosis_gradient`',
    '`category_of_aortic_valve_stenosis` AS `category_of_aortic_valve_stenosis`',
    '`cabg` AS `cabg`',
    '`valve` AS `valve`',
    '`major_aortic` AS `major_aortic`',
    '`other_cardiac_procedures` AS `other_cardiac_procedures`',
    '`presentationtxt` AS `presentationtxt`',
    '`other_thoracic_and_vascular_procedurestxt` AS `other_thoracic_and_vascular_procedurestxt`',
    '`date_and_time_of_operation_finished` AS `date_and_time_of_operation_finished`',
    '`reason_pre_operative_intra_aortic_balloon_pump_used` AS `reason_pre_operative_intra_aortic_balloon_pump_used`',
    '`pre_op_support_devices_usedtxt` AS `pre_op_support_devices_usedtxt`',
    '`reason_for_pre_operative_impeller_device_use` AS `reason_for_pre_operative_impeller_device_use`',
    '`reason_for_pre_operative_ventricular_assist_device_use` AS `reason_for_pre_operative_ventricular_assist_device_use`',
    '`reason_for_use_of_other_support_device_pre_operation` AS `reason_for_use_of_other_support_device_pre_operation`',
    '`postcode_of_usual_address` AS `postcode_of_usual_address`',
    '`pulmonary_hypertension` AS `pulmonary_hypertension`',
    '`aetiologytxt` AS `aetiologytxt`',
    '`height_in_cm` AS `height_in_cm`',
    '`poor_mobility_non_cardiac_reason` AS `poor_mobility_non_cardiac_reason`',
    '`creatinine_clearance` AS `creatinine_clearance`',
    '`renal_impairment` AS `renal_impairment`',
    '`hospital_identifier_old` AS `hospital_identifier_old`',
    '`diabetes_type` AS `diabetes_type`',
    '`cardiology_consultanttxt` AS `cardiology_consultanttxt`',
    '`hypercholesterolaemia` AS `hypercholesterolaemia`',
    '`mean_pawpla` AS `mean_pawpla`',
    '`antibiotic_treatment_for_endocarditis` AS `antibiotic_treatment_for_endocarditis`',
    '`second_assistanttxt` AS `second_assistanttxt`',
    '`second_assistant_grade` AS `second_assistant_grade`',
    '`second_assistant_calman_year_of_trainee` AS `second_assistant_calman_year_of_trainee`',
    '`incision` AS `incision`',
    '`arterial_cannulation_version5pt2` AS `arterial_cannulation_version5pt2`',
    '`venous_cannulation` AS `venous_cannulation`',
    '`perfusionisttxt` AS `perfusionisttxt`',
    '`other_cardiology_consultant` AS `other_cardiology_consultant`',
    '`other_cardiology_consultant_gm_cnumber` AS `other_cardiology_consultant_gm_cnumber`',
    '`weight_in_kg` AS `weight_in_kg`',
    '`other_cardiac_procedure_details` AS `other_cardiac_procedure_details`',
    '`m_pre_operation_history` AS `m_pre_operation_history`',
    '`history_of_poor_mobility` AS `history_of_poor_mobility`',
    '`surgical_incisiontxt` AS `surgical_incisiontxt`',
    '`consultant_responsible_for_proceduretxt` AS `consultant_responsible_for_proceduretxt`',
    '`responsible_consultant_surgeontxt` AS `responsible_consultant_surgeontxt`',
    '`primary_operator` AS `primary_operator`',
    '`hospital_identifiertxt` AS `hospital_identifiertxt`',
    '`myectomy_optionstxt` AS `myectomy_optionstxt`',
    '`conversion_to_sternotomy` AS `conversion_to_sternotomy`',
    '`referring_hospitaltxt` AS `referring_hospitaltxt`',
    '`unit_collective_responsibility_case` AS `unit_collective_responsibility_case`',
    '`secondary_assisting_consultant_surgeontxt` AS `secondary_assisting_consultant_surgeontxt`',
    '`covid19_diagnosis` AS `covid19_diagnosis`',
    '`covid19_pt` AS `covid19_pt`',
    '`harley_street_clinic` AS `harley_street_clinic`',
    '`discussed_at_documented_quorate_preop_mdt` AS `discussed_at_documented_quorate_preop_mdt`',
    '`haemoglobin_at_time_of_surgery` AS `haemoglobin_at_time_of_surgery`',
    '`clinical_presentationtxt` AS `clinical_presentationtxt`',
    '`clinical_presentation_malperfusiontxt` AS `clinical_presentation_malperfusiontxt`',
    '`ct_aorta_dt` AS `ct_aorta_dt`',
    '`ct_aorta_diagnosistxt` AS `ct_aorta_diagnosistxt`',
    '`ct_aorta_other_dx` AS `ct_aorta_other_dx`',
    '`ct_aorta_extent_of_dissectiontxt` AS `ct_aorta_extent_of_dissectiontxt`',
    '`ct_aorta_primary_entry_teartxt` AS `ct_aorta_primary_entry_teartxt`',
    '`ct_aorta_secondary_entry_tear_arch` AS `ct_aorta_secondary_entry_tear_arch`',
    '`ct_aorta_root` AS `ct_aorta_root`',
    '`ct_aorta_stj` AS `ct_aorta_stj`',
    '`ct_aorta_mid_aa` AS `ct_aorta_mid_aa`',
    '`ct_aorta_distal_aa` AS `ct_aorta_distal_aa`',
    '`ct_aorta_arch_lcca` AS `ct_aorta_arch_lcca`',
    '`ct_aorta_arch_las` AS `ct_aorta_arch_las`',
    '`ct_aorta_proximal_dta` AS `ct_aorta_proximal_dta`',
    '`ct_aorta_mid_dta` AS `ct_aorta_mid_dta`',
    '`ct_aorta_distal_dt_adiaphragm` AS `ct_aorta_distal_dt_adiaphragm`',
    '`ct_aorta_visceral_level` AS `ct_aorta_visceral_level`',
    '`ct_aorta_infrarenal` AS `ct_aorta_infrarenal`',
    '`fev1` AS `fev1`',
    '`fev1_percent_predicted` AS `fev1_percent_predicted`',
    '`fev1_fv_cratio` AS `fev1_fv_cratio`',
    '`fev1_fv_cpercent` AS `fev1_fv_cpercent`',
    '`diffusion_capacitymlminkpa` AS `diffusion_capacitymlminkpa`',
    '`diffusion_capacity_percent_predicted` AS `diffusion_capacity_percent_predicted`',
    '`cpet_anaerobic_threshold` AS `cpet_anaerobic_threshold`',
    '`maximal_oxygen_uptake` AS `maximal_oxygen_uptake`',
    '`surgical_incisionnon_nicor_optionstxt` AS `surgical_incisionnon_nicor_optionstxt`',
    '`previous_abdominal_descending_aortic_surgtxt` AS `previous_abdominal_descending_aortic_surgtxt`',
    '`previous_endovascular_surgerytxt` AS `previous_endovascular_surgerytxt`',
    '`prior_right_chest_surgery_or_radiation` AS `prior_right_chest_surgery_or_radiation`',
    '`significant_chest_wall_deformity` AS `significant_chest_wall_deformity`',
    '`preoperative_transcatheter_interventionstxt` AS `preoperative_transcatheter_interventionstxt`',
    '`rv_fractional_area_change` AS `rv_fractional_area_change`',
    '`tapse` AS `tapse`',
    '`coronary_ct` AS `coronary_ct`',
    '`abdominal_or_descending` AS `abdominal_or_descending`',
    '`rockwood_frailty_score` AS `rockwood_frailty_score`',
    '`abdominal_or_descending_aortic` AS `abdominal_or_descending_aortic`',
    '`m_other_previous_endovascular_surgery` AS `m_other_previous_endovascular_surgery`',
    '`aortic_valve_anatomy` AS `aortic_valve_anatomy`',
    '`other_aortic_valve_anatomy` AS `other_aortic_valve_anatomy`',
    '`aortic_valve_diseasetxt` AS `aortic_valve_diseasetxt`',
    '`aortic_valve_regurgitation` AS `aortic_valve_regurgitation`',
    '`ero_aaortic` AS `ero_aaortic`',
    '`vena_contracta_width_aortic` AS `vena_contracta_width_aortic`',
    '`ar_jet_direction` AS `ar_jet_direction`',
    '`lv_end_systolic_diameter_aortic` AS `lv_end_systolic_diameter_aortic`',
    '`lv_end_diastolic_diameter_aortic` AS `lv_end_diastolic_diameter_aortic`',
    '`severity_of_aortic_valve_stenosis_ava` AS `severity_of_aortic_valve_stenosis_ava`',
    '`mitral_valve_diseasetxt` AS `mitral_valve_diseasetxt`',
    '`mitral_valve_regurgitation` AS `mitral_valve_regurgitation`',
    '`mechanism_of_mitral_regurgitationtxt` AS `mechanism_of_mitral_regurgitationtxt`',
    '`flail` AS `flail`',
    '`prolapse` AS `prolapse`',
    '`restriction` AS `restriction`',
    '`degree_of_myxomatous_change` AS `degree_of_myxomatous_change`',
    '`ero_amitral` AS `ero_amitral`',
    '`vena_contracta_width_mitral` AS `vena_contracta_width_mitral`',
    '`mr_jet_direction` AS `mr_jet_direction`',
    '`lv_end_systolic_diameter_mitral` AS `lv_end_systolic_diameter_mitral`',
    '`lv_end_diastolic_diameter_mitral` AS `lv_end_diastolic_diameter_mitral`',
    '`l_adiameter` AS `l_adiameter`',
    '`r_vimpairment` AS `r_vimpairment`',
    '`pericardial_effusion` AS `pericardial_effusion`',
    '`mitral_valve_stenosis` AS `mitral_valve_stenosis`',
    '`mitral_annulus_calcification` AS `mitral_annulus_calcification`',
    '`severity_of_mitral_valve_stenosis_eoa` AS `severity_of_mitral_valve_stenosis_eoa`',
    '`severity_of_mitral_valve_stenosis_gradient` AS `severity_of_mitral_valve_stenosis_gradient`',
    '`date_last_changed` AS `date_last_changed`',
    '`pre1_adc_updt` AS `pre1_adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`pre2_date_last_changed` AS `pre2_date_last_changed`',
    '`pre2_adc_updt` AS `pre2_adc_updt`',
    '`post1_date_last_changed` AS `post1_date_last_changed`',
    '`post1_adc_updt` AS `post1_adc_updt`',
    '`post2_date_last_changed` AS `post2_date_last_changed`',
    '`post2_adc_updt` AS `post2_adc_updt`',
    '`episode_last_changed` AS `episode_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`pre2_module` AS `pre2_module`',
    '`post1_module` AS `post1_module`',
    '`post2_module` AS `post2_module`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
]

CLINICAL_REGISTRY_SURGERY_EPISODE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the surgery_episode source family and native entry_id.",
    "entry_id": "Stable iWeb episode key and parent of iweb_cardiac_surgery_procedure.",
    "mrn": "Native source hospital number retained for trace-back.",
    "surname": "Patient surname as recorded in the iWeb CS2010G pre-operative registry module.",
    "forename": "Patient forename as recorded in the iWeb CS2010G pre-operative registry module.",
    "nhs_number": "Native source NHS number retained for governed trace-back.",
    "gender": "Patient gender recorded on the pre-operative CS2010G module for this surgical episode.",
    "date_of_birth": "Patient date of birth as recorded in the iWeb CS2010G pre-operative registry module.",
    "date_of_death": "Recorded date of patient death, null where no death has been recorded on the registry.",
    "ethnic_origin": "Coded ethnic origin of the patient as recorded in the pre-operative registry module.",
    "date_of_operation": "Date of the cardiac surgical operation for this episode.",
    "age_at_operation": "Patient age in completed years at the date of the operation.",
    "date_of_admission": "Date the patient was admitted for the episode of care leading to this operation.",
    "administrative_category": "Administrative funding category of the patient for this episode, such as NHS or private care.",
    "angina_status_pre_surgery": "Pre-operative angina severity classification recorded before surgery.",
    "dyspnoea_status_pre_surgery": "Pre-operative dyspnoea severity classification recorded before surgery.",
    "number_of_previous_mis": "Categorised count of previous myocardial infarctions recorded before surgery.",
    "interval_between_surgery_and_last_mi": "Categorised time interval between the most recent myocardial infarction and the operation.",
    "previous_pci": "Indicates whether the patient had previous percutaneous coronary intervention and its timing category.",
    "previous_cardiac_surgerytxt": "Coded description of any previous cardiac surgery undertaken before this episode.",
    "date_of_last_cardiac_operation": "Date of the patient's most recent previous cardiac operation, null where there was no prior surgery.",
    "diabetes_management": "Method of diabetes management recorded before surgery, including a not-diabetic option.",
    "cigarette_smoking_history": "Recorded cigarette smoking status history of the patient before surgery.",
    "history_of_hypertension": "Indicates whether the patient has a recorded history of hypertension prior to admission.",
    "dialysis": "Pre-operative dialysis status, including type and timing of dialysis relative to cardiac surgery.",
    "history_of_pulmonary_disease": "Recorded history of chronic pulmonary disease before surgery.",
    "history_of_neurological_diseasetxt": "Coded description of any history of neurological disease recorded before surgery.",
    "history_of_neurological_dysfunction": "Indicates whether pre-operative neurological dysfunction severely affecting ambulation or day-to-day functioning was recorded.",
    "extracardiac_arteriopathy": "Indicates whether extracardiac arteriopathy was recorded as a pre-operative risk factor.",
    "pre_operative_heart_rhythm": "Coded cardiac rhythm recorded at pre-operative assessment in the CS2010G pre-operative module.",
    "left_heart_catheterisation": "Indicates whether left heart catheterisation was performed before surgery.",
    "date_of_last_catheterisation": "Date of the patient's most recent cardiac catheterisation prior to the surgical episode, as recorded in the CS2010G pre-operative module.",
    "extent_of_coronary_vessel_disease": "Coded pre-operative assessment of the number of coronary vessels with significant stenosis.",
    "ejection_fraction_category": "Coded category of pre-operative left ventricular ejection fraction used in cardiac surgery risk assessment.",
    "pa_systolic": "Pre-operative pulmonary artery systolic pressure as recorded in the registry.",
    "severity_of_aortic_valve_stenosis_eoa": "Aortic valve effective orifice area recorded as a measure of aortic stenosis severity before surgery.",
    "intravenous_nitrates_or_any_heparin": "Indicator of whether the patient received intravenous nitrates or any heparin before operation.",
    "intravenous_inotropes_prior_to_anaesthesia": "Indicator of whether intravenous inotropes were administered prior to induction of anaesthesia.",
    "ventilated_pre_operation": "Indicator of whether the patient was mechanically ventilated immediately before the operation.",
    "cardiogenic_shock_pre_operation": "Indicator of whether the patient was in cardiogenic shock before the operation.",
    "date_and_time_of_operation": "Date and time at which the surgical procedure started for this episode.",
    "operative_urgency": "Coded urgency category of the cardiac surgical operation as recorded pre-operatively in the CS2010G registry.",
    "number_of_previous_heart_operations": "Count of previous cardiac operations undergone by the patient before this episode.",
    "responsible_consultant_anaesthetisttxt": "Text label naming the consultant anaesthetist responsible for the case, selected from the iWeb staff picklist.",
    "first_operatortxt": "Free-text name of the first operator performing the procedure.",
    "first_operator_grade": "Coded staff grade of the first operator performing the procedure, as recorded in the CS2010G pre-operative module.",
    "first_operator_calman_year_of_trainee": "Calman training year of the first operator when that operator is a trainee.",
    "first_assistanttxt": "Free-text name of the first assistant in the operating team.",
    "first_assistant_grade": "Coded staff grade of the first assistant in the operating team.",
    "first_assistant_calman_year_of_trainee": "Calman training year of the first assistant when that assistant is a trainee.",
    "left_main_stem_disease": "Coded assessment of left main stem coronary artery disease severity before surgery.",
    "left_ventricular_function": "Recorded pre-operative left ventricular function value from cardiac imaging or catheterisation.",
    "cardiac_procedures": "Coded cardiac procedure category recorded in the pre-operative module for this episode.",
    "other_actual_cardiac_procedurestxt": "Text field listing additional cardiac procedures performed, generally holding coded picklist descriptions.",
    "actual_creatinine_at_time_of_surgery": "Serum creatinine result recorded at the time of surgery, as captured in the registry.",
    "severity_of_aortic_valve_stenosis_gradient": "Aortic valve pressure gradient recorded as a measure of aortic stenosis severity before surgery.",
    "category_of_aortic_valve_stenosis": "Coded category of aortic valve stenosis recorded at pre-operative assessment in the CS2010G pre1 module.",
    "cabg": "Indicator of whether the episode included coronary artery bypass grafting.",
    "valve": "Indicator of whether the episode included a valve procedure.",
    "major_aortic": "Indicator of whether the episode included a major aortic procedure.",
    "other_cardiac_procedures": "Indicator of whether other cardiac procedures were performed in addition to the main procedure categories.",
    "presentationtxt": "Coded presentation option or options recorded for the valve or aortic pathology, stored as concatenated code-label text.",
    "other_thoracic_and_vascular_procedurestxt": "Coded selection of other thoracic and vascular procedures associated with the episode, stored as concatenated code-label text.",
    "date_and_time_of_operation_finished": "Date and time at which the surgical operation finished.",
    "reason_pre_operative_intra_aortic_balloon_pump_used": "Coded reason for use of an intra-aortic balloon pump before the operation.",
    "pre_op_support_devices_usedtxt": "Coded selection of mechanical support devices used before the operation, stored as concatenated code-label text.",
    "reason_for_pre_operative_impeller_device_use": "Coded reason for use of an impeller device before the operation.",
    "reason_for_pre_operative_ventricular_assist_device_use": "Coded reason for use of a ventricular assist device before the operation.",
    "reason_for_use_of_other_support_device_pre_operation": "Coded reason for use of another type of circulatory support device before the operation.",
    "postcode_of_usual_address": "Postcode of the patient's usual address as recorded in the iWeb CS2010G pre-operative registry module.",
    "pulmonary_hypertension": "Coded pre-operative pulmonary hypertension status.",
    "aetiologytxt": "Coded aetiology option or options recorded for the valve or aortic pathology, stored as concatenated code-label text.",
    "height_in_cm": "Patient height in centimetres recorded pre-operatively.",
    "poor_mobility_non_cardiac_reason": "Indicator of whether poor mobility is attributable to a non-cardiac reason.",
    "creatinine_clearance": "Pre-operative creatinine clearance value recorded in the registry.",
    "renal_impairment": "Coded category of pre-operative renal impairment.",
    "hospital_identifier_old": "Legacy hospital or unit identifier code retained from the iWeb CS2010G pre-operative module.",
    "diabetes_type": "Coded type of diabetes recorded pre-operatively.",
    "cardiology_consultanttxt": "Recorded name of the cardiology consultant associated with the episode.",
    "hypercholesterolaemia": "Indicator of a recorded history of hypercholesterolaemia before surgery.",
    "mean_pawpla": "Mean pulmonary artery wedge or left atrial pressure recorded at pre-operative cardiac investigation.",
    "antibiotic_treatment_for_endocarditis": "Indicator of whether the patient was receiving antibiotic treatment for endocarditis before surgery.",
    "second_assistanttxt": "Recorded name of the second assistant present at the operation.",
    "second_assistant_grade": "Coded staff grade of the second assistant at the operation, as recorded in the iWeb CS2010G pre-operative module.",
    "second_assistant_calman_year_of_trainee": "Calman training year recorded for the second assistant when that assistant is a trainee.",
    "incision": "Coded surgical incision type recorded for the episode in the CS2010G pre-operative module.",
    "arterial_cannulation_version5pt2": "Arterial cannulation site or method recorded using the version 5.2 registry option list.",
    "venous_cannulation": "Coded venous cannulation technique or cannula type used for cardiopulmonary bypass.",
    "perfusionisttxt": "Text label identifying the perfusionist selected for the case from the iWeb staff picklist.",
    "other_cardiology_consultant": "Additional cardiology consultant recorded for the patient where different from the main cardiology consultant.",
    "other_cardiology_consultant_gm_cnumber": "GMC registration number of the other cardiology consultant recorded for the episode.",
    "weight_in_kg": "Patient weight in kilograms recorded pre-operatively for the episode.",
    "other_cardiac_procedure_details": "Free-text detail describing other cardiac procedures performed that are not covered by the coded procedure fields.",
    "m_pre_operation_history": "Free-text pre-operative clinical history narrative for the episode.",
    "history_of_poor_mobility": "Coded indicator of pre-operative poor mobility used in cardiac surgical risk assessment.",
    "surgical_incisiontxt": "Text label of the surgical incision option selected for the procedure.",
    "consultant_responsible_for_proceduretxt": "Text label naming the consultant responsible for the procedure, as selected from the iWeb staff picklist.",
    "responsible_consultant_surgeontxt": "Text label naming the consultant surgeon responsible for the patient's care for this episode.",
    "primary_operator": "Primary operator recorded for the procedure in the CS2010G pre-operative module.",
    "hospital_identifiertxt": "Text label of the hospital or trust identifier recorded for the episode in the iWeb CS2010G pre-operative module.",
    "myectomy_optionstxt": "Text label of the myectomy option recorded for the procedure.",
    "conversion_to_sternotomy": "Coded indicator of whether the procedure was converted to a sternotomy.",
    "referring_hospitaltxt": "Text label of the hospital that referred the patient for the cardiac surgical episode.",
    "unit_collective_responsibility_case": "Coded indicator of whether the case was designated as a unit collective responsibility case.",
    "secondary_assisting_consultant_surgeontxt": "Text label naming the secondary assisting consultant surgeon for the procedure.",
    "covid19_diagnosis": "Coded record of COVID-19 diagnosis status relating to the surgical episode.",
    "covid19_pt": "Coded indicator of whether the patient was a COVID-19 patient at the time of the episode.",
    "harley_street_clinic": "Coded flag indicating whether the episode was associated with the Harley Street Clinic.",
    "discussed_at_documented_quorate_preop_mdt": "Coded indicator of whether the case was discussed at a documented quorate pre-operative multidisciplinary team meeting.",
    "haemoglobin_at_time_of_surgery": "Pre-operative haemoglobin concentration recorded at the time of the surgical episode, as entered in the iWeb CS2010G pre-operative module.",
    "clinical_presentationtxt": "Text label of the recorded clinical presentation category for the episode, as captured in the iWeb CS2010G pre-operative module.",
    "clinical_presentation_malperfusiontxt": "Text label of the recorded malperfusion component of the clinical presentation in the iWeb CS2010G pre-operative module.",
    "ct_aorta_dt": "Date and time of the pre-operative CT aorta study recorded in the iWeb CS2010G pre-operative module.",
    "ct_aorta_diagnosistxt": "Text label of the diagnosis recorded from the pre-operative CT aorta study in the iWeb CS2010G pre-operative module.",
    "ct_aorta_other_dx": "Free-text description of any other diagnosis identified on the pre-operative CT aorta study.",
    "ct_aorta_extent_of_dissectiontxt": "Text label describing the extent of aortic dissection seen on the pre-operative CT aorta study.",
    "ct_aorta_primary_entry_teartxt": "Text label describing the location of the primary entry tear identified on the pre-operative CT aorta study.",
    "ct_aorta_secondary_entry_tear_arch": "Recorded indication of a secondary entry tear at the aortic arch on the pre-operative CT aorta study.",
    "ct_aorta_root": "Aortic root measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_stj": "Sinotubular junction measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_mid_aa": "Mid ascending aorta measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_distal_aa": "Distal ascending aorta measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_arch_lcca": "Aortic arch measurement at the left common carotid artery recorded from the pre-operative CT aorta study.",
    "ct_aorta_arch_las": "Aortic arch measurement at the left subclavian artery level recorded from the pre-operative CT aorta study.",
    "ct_aorta_proximal_dta": "Proximal descending thoracic aorta measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_mid_dta": "Mid descending thoracic aorta measurement recorded from the pre-operative CT aorta study.",
    "ct_aorta_distal_dt_adiaphragm": "Distal descending thoracic aorta measurement at diaphragm level recorded from the pre-operative CT aorta study.",
    "ct_aorta_visceral_level": "Aortic measurement at the visceral segment level recorded from the pre-operative CT aorta study.",
    "ct_aorta_infrarenal": "Infrarenal aortic measurement recorded from the pre-operative CT aorta study.",
    "fev1": "Pre-operative forced expiratory volume in one second recorded from lung function testing.",
    "fev1_percent_predicted": "Pre-operative FEV1 expressed as a percentage of the predicted value.",
    "fev1_fv_cratio": "Pre-operative ratio of FEV1 to forced vital capacity from lung function testing.",
    "fev1_fv_cpercent": "Pre-operative FEV1/FVC expressed as a percentage from lung function testing.",
    "diffusion_capacitymlminkpa": "Pre-operative lung diffusion capacity measured in ml/min/kPa.",
    "diffusion_capacity_percent_predicted": "Pre-operative lung diffusion capacity expressed as a percentage of the predicted value, from the CS2010G pre-operative module.",
    "cpet_anaerobic_threshold": "Anaerobic threshold measured at pre-operative cardiopulmonary exercise testing, recorded in the CS2010G pre-operative module.",
    "maximal_oxygen_uptake": "Maximal oxygen uptake recorded at pre-operative cardiopulmonary exercise testing in the CS2010G pre-operative module.",
    "surgical_incisionnon_nicor_optionstxt": "Text label of the surgical incision selected from local non-NICOR option list in the pre-operative module.",
    "previous_abdominal_descending_aortic_surgtxt": "Text label recording previous abdominal or descending aortic surgery selected from the pre-operative option list.",
    "previous_endovascular_surgerytxt": "Text label recording previous endovascular surgery selected from the pre-operative option list.",
    "prior_right_chest_surgery_or_radiation": "Coded indicator of prior right chest surgery or radiotherapy recorded pre-operatively.",
    "significant_chest_wall_deformity": "Coded indicator of significant chest wall deformity identified pre-operatively.",
    "preoperative_transcatheter_interventionstxt": "Text label of pre-operative transcatheter interventions selected from the registry option list.",
    "rv_fractional_area_change": "Right ventricular fractional area change measured on pre-operative echocardiography.",
    "tapse": "Tricuspid annular plane systolic excursion measured on pre-operative echocardiography.",
    "coronary_ct": "Coded indicator of whether pre-operative coronary CT imaging was performed.",
    "abdominal_or_descending": "Coded pre-operative field relating to abdominal or descending aortic disease or prior surgery.",
    "rockwood_frailty_score": "Rockwood clinical frailty scale category recorded at pre-operative assessment.",
    "abdominal_or_descending_aortic": "Coded pre-operative field relating to abdominal or descending aortic pathology or intervention, held alongside the related abdominal or descending field.",
    "m_other_previous_endovascular_surgery": "Free-text detail describing other previous endovascular surgery not covered by the coded options.",
    "aortic_valve_anatomy": "Coded description of aortic valve anatomy recorded at pre-operative assessment.",
    "other_aortic_valve_anatomy": "Text detail of aortic valve anatomy when not covered by the coded anatomy options.",
    "aortic_valve_diseasetxt": "Text label of aortic valve disease selected from the pre-operative valve assessment option list.",
    "aortic_valve_regurgitation": "Coded severity grade of aortic valve regurgitation recorded pre-operatively.",
    "ero_aaortic": "Effective regurgitant orifice area for aortic regurgitation measured on pre-operative echocardiography.",
    "vena_contracta_width_aortic": "Vena contracta width of the aortic regurgitant jet measured on pre-operative echocardiography.",
    "ar_jet_direction": "Coded direction of the aortic regurgitant jet observed on pre-operative echocardiography.",
    "lv_end_systolic_diameter_aortic": "Left ventricular end-systolic diameter recorded within the pre-operative aortic valve assessment.",
    "lv_end_diastolic_diameter_aortic": "Left ventricular end-diastolic diameter recorded within the pre-operative aortic valve assessment.",
    "severity_of_aortic_valve_stenosis_ava": "Pre-operative aortic valve area measured on echocardiography, recorded as a marker of aortic stenosis severity.",
    "mitral_valve_diseasetxt": "Coded description of pre-operative mitral valve disease, held as the registry's decoded text label.",
    "mitral_valve_regurgitation": "Pre-operative grade of mitral valve regurgitation recorded as a coded category.",
    "mechanism_of_mitral_regurgitationtxt": "Coded mechanism of mitral regurgitation identified pre-operatively, held as the registry's decoded text label.",
    "flail": "Indicator of mitral leaflet flail identified on pre-operative valve assessment.",
    "prolapse": "Indicator of mitral leaflet prolapse identified on pre-operative valve assessment.",
    "restriction": "Indicator of mitral leaflet restriction identified on pre-operative valve assessment.",
    "degree_of_myxomatous_change": "Recorded degree of myxomatous change of the mitral valve on pre-operative assessment.",
    "ero_amitral": "Effective regurgitant orifice area of the mitral valve measured on pre-operative echocardiography.",
    "vena_contracta_width_mitral": "Vena contracta width of the mitral regurgitant jet measured on pre-operative echocardiography.",
    "mr_jet_direction": "Direction of the mitral regurgitant jet recorded on pre-operative echocardiography.",
    "lv_end_systolic_diameter_mitral": "Left ventricular end-systolic diameter recorded as part of the pre-operative mitral valve assessment.",
    "lv_end_diastolic_diameter_mitral": "Left ventricular end-diastolic diameter recorded as part of the pre-operative mitral valve assessment.",
    "l_adiameter": "Left atrial diameter measured on pre-operative echocardiography.",
    "r_vimpairment": "Recorded degree of right ventricular impairment on pre-operative assessment.",
    "pericardial_effusion": "Indicator of pericardial effusion identified on pre-operative assessment.",
    "mitral_valve_stenosis": "Pre-operative grade or presence of mitral valve stenosis recorded as a coded category.",
    "mitral_annulus_calcification": "Recorded presence or degree of mitral annular calcification on pre-operative assessment.",
    "severity_of_mitral_valve_stenosis_eoa": "Effective orifice area of the mitral valve recorded as a measure of mitral stenosis severity.",
    "severity_of_mitral_valve_stenosis_gradient": "Transmitral pressure gradient recorded as a measure of mitral stenosis severity.",
    "date_last_changed": "Timestamp of the most recent change to the pre1 module record in the source iWeb registry.",
    "pre1_adc_updt": "Technical ingestion/update timestamp for the pre1 source row within the data platform.",
    "nhs_number_valid_ind": "Derived flag indicating whether the source NHS number passed validation, retained after the NHS number itself was dropped.",
    "linkage_status": "Derived status describing the outcome of linking the registry episode to the master patient index.",
    "person_id": "Resolved independently through NHS number and MRN; NULL on conflict or ambiguity.",
    "linkage_method": "Derived indicator of which identifier combination was used to resolve the patient linkage.",
    "linkage_historical_fallback_ind": "Boolean flag indicating that the PERSON_ID linkage was resolved using a historical alias fallback rather than a current identifier match.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating that the fallback linkage step returned conflicting candidate person matches.",
    "pre2_date_last_changed": "Timestamp of the last edit to the CS2010G pre2 record in the iWeb source system.",
    "pre2_adc_updt": "Technical timestamp of the last ingestion or update of the CS2010G pre2 row in the data platform.",
    "post1_date_last_changed": "Timestamp of the last change to the post1 module record in the source iWeb registry.",
    "post1_adc_updt": "Technical timestamp of the last extract or load update of the post1 module row.",
    "post2_date_last_changed": "Timestamp of the last recorded change to the post2 module record in the iWeb source system.",
    "post2_adc_updt": "Ingestion timestamp indicating when the post2 module row was last updated in the data platform.",
    "episode_last_changed": "Greatest DateLastChanged timestamp across the four source modules.",
    "adc_updt": "Greatest landing timestamp across the four source modules; excluded from ROW_HASH.",
    "source_table": "Label listing the iWeb CS2010G source modules contributing to the merged episode row.",
    "source_record_key": "Source system EntryId for the episode carried through as a string provenance key.",
    "pre2_module": "Typed lossless payload of reg_cs2010g_pre2 fields except promoted module timestamps.",
    "post1_module": "Typed lossless payload of reg_cs2010g_post1 fields except promoted module timestamps.",
    "post2_module": "Typed lossless payload of reg_cs2010g_post2 fields except promoted module timestamps.",
    "row_hash": "Deterministic hash of the merged row's source column values used for change detection.",
    "pipeline_loaded_at": "Timestamp recording when the pipeline loaded this row into the table.",
    "is_current_in_source": "Boolean flag indicating whether the episode is still present as a current record in the iWeb source.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Technical timestamp recording when the episode was first detected as no longer present in the iWeb source.",
}

CLINICAL_REGISTRY_SURGERY_EPISODE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_episode.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_episode"),
    comment="Internal QC of clinical_registry_surgery_episode; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_EPISODE_MANDATORY_RULES)
def _gold_qc_clinical_registry_surgery_episode():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_episode"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_EPISODE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_SURGERY_EPISODE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_episode"),
    comment="Typed child of clinical_registry_entry for the surgery_episode family. iWeb CS2010G adult cardiac surgery registry. Complete PRE1 fields remain top-level; PRE2, POST1 and POST2 source fields are retained losslessly in typed module structs. This preserves identifiers and clinical content while remaining within Unity Catalog's 1,000-tag-per-table quota. Every governed bronze column is retained under its lower-case name.",
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_surgery_episode():
    """Publish clinical_registry_surgery_episode without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_episode")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_surgery_followup ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_SURGERY_FOLLOWUP_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`flw_id` AS `flw_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`discharge_dt` AS `discharge_dt`',
    '`time_of_followup` AS `time_of_followup`',
    '`procedure_status` AS `procedure_status`',
    '`patient_status_at_follow_up` AS `patient_status_at_follow_up`',
    '`post_operative_complicationtxt` AS `post_operative_complicationtxt`',
    '`atrial_fibrillation` AS `atrial_fibrillation`',
    '`anticoagulation_commenced` AS `anticoagulation_commenced`',
    '`lrt_itxt` AS `lrt_itxt`',
    '`post_operative_pyrexia_treatmenttxt` AS `post_operative_pyrexia_treatmenttxt`',
    '`sternal_wound_infection_treatmenttxt` AS `sternal_wound_infection_treatmenttxt`',
    '`m_sternal_wound_infection_reoperation_type` AS `m_sternal_wound_infection_reoperation_type`',
    '`harvest_wound_infection_treatmenttxt` AS `harvest_wound_infection_treatmenttxt`',
    '`m_harvest_wound_infection_reoperation_type` AS `m_harvest_wound_infection_reoperation_type`',
    '`post_operative_ileus` AS `post_operative_ileus`',
    '`pericardial_effusion` AS `pericardial_effusion`',
    '`resternotomy_for_bleeding_on_day` AS `resternotomy_for_bleeding_on_day`',
    '`investigations_satisfactory_prior_to_discharge` AS `investigations_satisfactory_prior_to_discharge`',
    '`m_other_investigations_satisfactory` AS `m_other_investigations_satisfactory`',
    '`m_local_clinic_details` AS `m_local_clinic_details`',
    '`excess_fluid_overload` AS `excess_fluid_overload`',
    '`pleural_effusion` AS `pleural_effusion`',
    '`pleural_effusion_management` AS `pleural_effusion_management`',
    '`pneumothorax` AS `pneumothorax`',
    '`pneumothorax_management` AS `pneumothorax_management`',
    '`maximal_depthmm` AS `maximal_depthmm`',
    '`indication_for_ppm` AS `indication_for_ppm`',
    '`new_cva` AS `new_cva`',
    '`follow_up_arrangements` AS `follow_up_arrangements`',
    '`it_udischarge_dt` AS `it_udischarge_dt`',
    '`hd_udischarge_dt` AS `hd_udischarge_dt`',
    '`peak_creatinine` AS `peak_creatinine`',
    '`ix_satisfactory_prior_to_dischargetxt` AS `ix_satisfactory_prior_to_dischargetxt`',
    '`m_post_operative_echo_data` AS `m_post_operative_echo_data`',
    '`is_dialysis_needed_post_discharge` AS `is_dialysis_needed_post_discharge`',
    '`pain_delaying_discharge` AS `pain_delaying_discharge`',
    '`reintubation_dt` AS `reintubation_dt`',
    '`extubation_dt` AS `extubation_dt`',
    '`length_of_intubation` AS `length_of_intubation`',
    '`import_child_id` AS `import_child_id`',
    '`date_last_changed` AS `date_last_changed`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
]

CLINICAL_REGISTRY_SURGERY_FOLLOWUP_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the surgery_followup source family and native entry_id.",
    "entry_id": "Stable iWeb follow-up key after deterministic latest-row deduplication.",
    "flw_id": "Internal iWeb identifier for the follow-up entry within the CS2010G registry record.",
    "mrn": "Local hospital medical record number identifying the patient for the follow-up record.",
    "gender": "Recorded gender of the patient as captured in the CS2010G follow-up registry.",
    "date_of_death": "Date of the patient's death as recorded in the CS2010G follow-up registry.",
    "ethnic_origin": "Coded ethnic origin of the patient recorded in the CS2010G follow-up registry.",
    "discharge_dt": "Date and time the patient was discharged following the cardiac surgery episode.",
    "time_of_followup": "Time recorded for the follow-up assessment, stored as text.",
    "procedure_status": "Coded status of the cardiac surgical procedure recorded at follow-up.",
    "patient_status_at_follow_up": "Coded status of the patient (such as clinical/vital status) recorded at the follow-up assessment.",
    "post_operative_complicationtxt": "Text description of post-operative complications recorded at follow-up, typically a coded value with its descriptive label.",
    "atrial_fibrillation": "Indicator of whether post-operative atrial fibrillation was recorded at follow-up.",
    "anticoagulation_commenced": "Indicator of whether anticoagulation therapy was commenced following surgery.",
    "lrt_itxt": "Text description of lower respiratory tract infection details recorded at follow-up.",
    "post_operative_pyrexia_treatmenttxt": "Text description of treatment given for post-operative pyrexia.",
    "sternal_wound_infection_treatmenttxt": "Text description of treatment given for sternal wound infection.",
    "m_sternal_wound_infection_reoperation_type": "Type of reoperation performed for sternal wound infection.",
    "harvest_wound_infection_treatmenttxt": "Text description of treatment given for conduit harvest site wound infection.",
    "m_harvest_wound_infection_reoperation_type": "Type of reoperation performed for harvest site wound infection.",
    "post_operative_ileus": "Indicator of whether post-operative ileus was recorded at follow-up.",
    "pericardial_effusion": "Indicator of whether pericardial effusion was recorded at follow-up.",
    "resternotomy_for_bleeding_on_day": "Post-operative day number on which resternotomy for bleeding was performed.",
    "investigations_satisfactory_prior_to_discharge": "Record of which investigations were satisfactory before the patient was discharged.",
    "m_other_investigations_satisfactory": "Free-text detail of other investigations found satisfactory prior to discharge.",
    "m_local_clinic_details": "Free-text details of the local clinic arranged for the patient's follow-up.",
    "excess_fluid_overload": "Recorded presence or management of excess fluid overload during post-operative cardiac surgery follow-up, as entered in the iWeb CS2010G follow-up form.",
    "pleural_effusion": "Indicates whether a pleural effusion was recorded during post-operative cardiac surgery follow-up.",
    "pleural_effusion_management": "Describes the management or treatment applied for a recorded post-operative pleural effusion.",
    "pneumothorax": "Indicates whether a pneumothorax was recorded during post-operative cardiac surgery follow-up.",
    "pneumothorax_management": "Describes the management or treatment applied for a recorded post-operative pneumothorax.",
    "maximal_depthmm": "Maximal depth measurement in millimetres recorded for the associated follow-up finding, as captured in the iWeb CS2010G follow-up form.",
    "indication_for_ppm": "Records the clinical indication for permanent pacemaker implantation noted at post-operative follow-up.",
    "new_cva": "Indicates whether a new cerebrovascular accident was recorded during post-operative cardiac surgery follow-up.",
    "follow_up_arrangements": "Free-text description of the arranged post-discharge follow-up, such as the service or centre responsible for review.",
    "it_udischarge_dt": "Date and time the patient was discharged from the intensive therapy unit following cardiac surgery.",
    "hd_udischarge_dt": "Date and time the patient was discharged from the high dependency unit following cardiac surgery.",
    "peak_creatinine": "Highest recorded post-operative serum creatinine value for the patient during the follow-up period, in the units captured by the source registry.",
    "ix_satisfactory_prior_to_dischargetxt": "Text list of investigations recorded as satisfactory prior to discharge in the iWeb CS2010G follow-up form.",
    "m_post_operative_echo_data": "Free-text notes capturing post-operative echocardiography findings recorded at follow-up.",
    "is_dialysis_needed_post_discharge": "Indicates whether the patient required dialysis after discharge, as recorded at cardiac surgery follow-up.",
    "pain_delaying_discharge": "Indicates whether pain was recorded as a factor delaying the patient's discharge after cardiac surgery.",
    "reintubation_dt": "Date and time the patient was reintubated following cardiac surgery.",
    "extubation_dt": "Date and time the patient was extubated following cardiac surgery.",
    "length_of_intubation": "Recorded duration of intubation following cardiac surgery, in the units captured by the source registry.",
    "import_child_id": "Source system import identifier for the child follow-up record, carried through from the iWeb registry load process.",
    "date_last_changed": "Timestamp of the last change to the record in the iWeb source system, used with ADC_UPDT to select the latest row per EntryId.",
    "adc_updt": "Pipeline update timestamp for the record, used with DATE_LAST_CHANGED to deterministically select the latest row per EntryId.",
    "nhs_number_valid_ind": "Derived flag indicating whether the source NHS number passed validation; the NHS number itself is not retained in this table.",
    "linkage_status": "Derived status describing the outcome of linking this follow-up record to a Millennium person record.",
    "person_id": "Internal Millennium person identifier linked to this follow-up record, used to join to patient-level tables.",
    "linkage_method": "Code describing which identifier combination was used to link this follow-up record to the master person index during pipeline processing.",
    "linkage_historical_fallback_ind": "Boolean flag indicating that person linkage relied on a historical (previous) identifier rather than a current one.",
    "linkage_fallback_conflict_ind": "Boolean flag indicating that the fallback linkage attempt produced conflicting person matches.",
    "source_table": "Name of the upstream iWeb source table from which the record was ingested.",
    "source_record_key": "String representation of the upstream iWeb EntryId identifying the source follow-up record for traceability back to the feeder table.",
    "row_hash": "Deterministic hash of the source record's field values used to detect changes between pipeline loads.",
    "pipeline_loaded_at": "Timestamp recording when the row was loaded into this bronze table by the ingestion pipeline.",
    "is_current_in_source": "Boolean flag indicating whether the record is still present as the current version in the upstream iWeb source.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when the pipeline first detected that the record was no longer present in the upstream iWeb source.",
    "surname": "Patient's surname as recorded in the iWeb CS2010G cardiac surgery follow-up record.",
    "forename": "Patient's forename as recorded in the iWeb CS2010G cardiac surgery follow-up record.",
    "nhs_number": "Patient's NHS number as recorded in the iWeb CS2010G cardiac surgery follow-up record, stored as text.",
    "date_of_birth": "Patient's date of birth as recorded in the iWeb CS2010G cardiac surgery follow-up record.",
}

CLINICAL_REGISTRY_SURGERY_FOLLOWUP_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_followup.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_followup"),
    comment="Internal QC of clinical_registry_surgery_followup; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_FOLLOWUP_MANDATORY_RULES)
def _gold_qc_clinical_registry_surgery_followup():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_followup"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_FOLLOWUP_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_SURGERY_FOLLOWUP_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_followup"),
    comment='Typed child of clinical_registry_entry for the surgery_followup family. CS2010G follow-up module. The source is small and dormant but semantically distinct. Duplicate EntryId rows are reduced deterministically to the latest DateLastChanged/ADC_UPDT row. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_surgery_followup():
    """Publish clinical_registry_surgery_followup without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_followup")).drop("__gold_parent_present")

# COMMAND ----------

# ==== journey_clinical.registry_surgery_procedure ====

# contract v2: Task 3 child; mandatory parent-admission check against clinical_registry_entry applies the parent Gold drop rule.
CLINICAL_REGISTRY_SURGERY_PROCEDURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`entry_id` AS `entry_id`',
    '`mrn` AS `mrn`',
    '`gender` AS `gender`',
    '`date_of_death` AS `date_of_death`',
    '`ethnic_origin` AS `ethnic_origin`',
    '`parent_entry_id` AS `parent_entry_id`',
    '`procedure_site` AS `procedure_site`',
    '`graft_conduit` AS `graft_conduit`',
    '`graft_anastomosis` AS `graft_anastomosis`',
    '`graft_site` AS `graft_site`',
    '`valve_implant_name` AS `valve_implant_name`',
    '`valve_haemodynamic_pathology` AS `valve_haemodynamic_pathology`',
    '`native_valve_pathology` AS `native_valve_pathology`',
    '`native_valve_other_pathology` AS `native_valve_other_pathology`',
    '`reason_for_repeat_valve_replacementtxt` AS `reason_for_repeat_valve_replacementtxt`',
    '`other_reason_for_repeat_valve_replacement` AS `other_reason_for_repeat_valve_replacement`',
    '`valve_procedure` AS `valve_procedure`',
    '`valve_implant_type` AS `valve_implant_type`',
    '`implant_prosthesis_nametxt` AS `implant_prosthesis_nametxt`',
    '`valve_or_ring_model_number` AS `valve_or_ring_model_number`',
    '`valve_or_ring_serial_number` AS `valve_or_ring_serial_number`',
    '`valve_or_ring_size` AS `valve_or_ring_size`',
    '`valve_explant_type` AS `valve_explant_type`',
    '`aortic_pathologytxt` AS `aortic_pathologytxt`',
    '`aortic_procedure` AS `aortic_procedure`',
    '`aortic_arch_type` AS `aortic_arch_type`',
    '`distal_anastamosis_constructed_bytxt` AS `distal_anastamosis_constructed_bytxt`',
    '`proximal_anastamosis_constructed_bytxt` AS `proximal_anastamosis_constructed_bytxt`',
    '`aortotomytxt` AS `aortotomytxt`',
    '`excision_of_valvetxt` AS `excision_of_valvetxt`',
    '`decalcification_of_annulustxt` AS `decalcification_of_annulustxt`',
    '`implantation_of_valvetxt` AS `implantation_of_valvetxt`',
    '`closure_of_aortotomytxt` AS `closure_of_aortotomytxt`',
    '`de_airing_of_hearttxt` AS `de_airing_of_hearttxt`',
    '`bi_caval_cannulationtxt` AS `bi_caval_cannulationtxt`',
    '`access_to_mitral_valvetxt` AS `access_to_mitral_valvetxt`',
    '`assessment_and_repairtxt` AS `assessment_and_repairtxt`',
    '`excision_of_valve_and_annular_debridementtxt` AS `excision_of_valve_and_annular_debridementtxt`',
    '`repair_of_valvetxt` AS `repair_of_valvetxt`',
    '`ringtxt` AS `ringtxt`',
    '`implantation_of_mitral_valvetxt` AS `implantation_of_mitral_valvetxt`',
    '`atrial_closuretxt` AS `atrial_closuretxt`',
    '`de_airing_of_heart_mitraltxt` AS `de_airing_of_heart_mitraltxt`',
    '`limatxt` AS `limatxt`',
    '`rimatxt` AS `rimatxt`',
    '`lsvtxt` AS `lsvtxt`',
    '`ssvtxt` AS `ssvtxt`',
    '`radialtxt` AS `radialtxt`',
    '`other_veintxt` AS `other_veintxt`',
    '`other_arterytxt` AS `other_arterytxt`',
    '`conduit_quality` AS `conduit_quality`',
    '`grafted_using` AS `grafted_using`',
    '`closed_using` AS `closed_using`',
    '`valve_implanted_using` AS `valve_implanted_using`',
    '`size_of_coronary_artery` AS `size_of_coronary_artery`',
    '`coronary_artery_condition` AS `coronary_artery_condition`',
    '`primary_repair_techniquetxt` AS `primary_repair_techniquetxt`',
    '`neochord` AS `neochord`',
    '`minimally_invasive_mitral_valve_surgery` AS `minimally_invasive_mitral_valve_surgery`',
    '`m_aortic_graft_detail` AS `m_aortic_graft_detail`',
    '`aortic_graft_serial_number` AS `aortic_graft_serial_number`',
    '`reimplantation_of_intercostals` AS `reimplantation_of_intercostals`',
    '`implantation_technique` AS `implantation_technique`',
    '`m_level_of_implantation` AS `m_level_of_implantation`',
    '`aortic_occlusion_bypass` AS `aortic_occlusion_bypass`',
    '`s_vcharacteristicstxt` AS `s_vcharacteristicstxt`',
    '`harvesting_characteristicstxt` AS `harvesting_characteristicstxt`',
    '`holes` AS `holes`',
    '`a_gscore` AS `a_gscore`',
    '`msv_gcomments` AS `msv_gcomments`',
    '`harvesting_technique` AS `harvesting_technique`',
    '`amount_of_heparin_before_evh` AS `amount_of_heparin_before_evh`',
    '`import_child_id` AS `import_child_id`',
    '`native_aortic_valve_morphology` AS `native_aortic_valve_morphology`',
    '`aortic_procedure2txt` AS `aortic_procedure2txt`',
    '`aortic_implant_locationtxt` AS `aortic_implant_locationtxt`',
    '`date_last_changed` AS `date_last_changed`',
    '`wall_quality` AS `wall_quality`',
    '`accessibility` AS `accessibility`',
    '`distal_run_off` AS `distal_run_off`',
    '`tvc_sinterpretation` AS `tvc_sinterpretation`',
    '`adc_updt` AS `adc_updt`',
    '`nhs_number_valid_ind` AS `nhs_number_valid_ind`',
    '`linkage_status` AS `linkage_status`',
    '`person_id` AS `person_id`',
    '`linkage_method` AS `linkage_method`',
    '`linkage_historical_fallback_ind` AS `linkage_historical_fallback_ind`',
    '`linkage_fallback_conflict_ind` AS `linkage_fallback_conflict_ind`',
    '`source_table` AS `source_table`',
    '`source_record_key` AS `source_record_key`',
    '`row_hash` AS `row_hash`',
    '`pipeline_loaded_at` AS `pipeline_loaded_at`',
    '`is_current_in_source` AS `is_current_in_source`',
    '`source_present_ind` AS `source_present_ind`',
    '`source_absent_detected_ts` AS `source_absent_detected_ts`',
    '`surname` AS `surname`',
    '`forename` AS `forename`',
    '`nhs_number` AS `nhs_number`',
    '`date_of_birth` AS `date_of_birth`',
]

CLINICAL_REGISTRY_SURGERY_PROCEDURE_COLUMN_COMMENTS = {
    "patient_event_key": "Deterministic SHA-256 key matching clinical_registry_entry.patient_event_key for the surgery_procedure source family and native entry_id.",
    "entry_id": "Stable iWeb child-record key.",
    "mrn": "Local hospital medical record number identifying the patient for this cardiac surgery component record.",
    "gender": "Patient gender recorded on the iWeb cardiac surgery record, held as a descriptive text value.",
    "date_of_death": "Recorded date of death for the patient associated with this surgery record, null where no death is recorded.",
    "ethnic_origin": "Coded ethnic origin of the patient as recorded in iWeb.",
    "parent_entry_id": "Foreign key to iweb_cardiac_surgery_episode.ENTRY_ID.",
    "procedure_site": "Anatomical site of the heart or coronary target at which this surgery component was performed, as recorded in the iWeb CS2010G subprocedure form.",
    "graft_conduit": "Type of conduit used for this coronary graft, from the iWeb picklist.",
    "graft_anastomosis": "Anastomosis technique used for this graft, from the iWeb picklist.",
    "graft_site": "Target coronary anatomical site to which this graft was anastomosed, from the iWeb picklist.",
    "valve_implant_name": "Valve position or implant designation recorded for this valve component in iWeb.",
    "valve_haemodynamic_pathology": "Haemodynamic pathology of the valve, such as the recorded functional lesion category, from the iWeb picklist.",
    "native_valve_pathology": "Coded pathology of the patient's native valve recorded for this valve component in iWeb.",
    "native_valve_other_pathology": "Free-text description of other native valve pathology where the standard picklist category does not apply.",
    "reason_for_repeat_valve_replacementtxt": "Text value recording the reason a repeat valve replacement was required for this valve component, as captured in iWeb.",
    "other_reason_for_repeat_valve_replacement": "Free-text description of another reason for repeat valve replacement where the standard picklist category does not apply.",
    "valve_procedure": "Type of valve procedure performed for this component, such as the recorded operative category from the iWeb picklist.",
    "valve_implant_type": "Category of valve implant used for this component, as recorded in the iWeb picklist.",
    "implant_prosthesis_nametxt": "Text label of the implanted prosthesis selected in iWeb; known source defect means some values are drug-picklist bleed-through rather than prosthesis names.",
    "valve_or_ring_model_number": "Manufacturer model number of the implanted valve or annuloplasty ring.",
    "valve_or_ring_serial_number": "Manufacturer serial number of the implanted valve or annuloplasty ring, with some entries containing additional free-text device description.",
    "valve_or_ring_size": "Recorded size of the implanted valve or annuloplasty ring as an integer, as captured in iWeb.",
    "valve_explant_type": "Type of valve or prior repair removed at this procedure, from the iWeb picklist.",
    "aortic_pathologytxt": "Text description of the aortic pathology recorded for the aortic component of this CS2010G surgery in iWeb.",
    "aortic_procedure": "Type of aortic procedure performed for this component, as recorded in the iWeb CS2010G subprocedure form.",
    "aortic_arch_type": "Coded classification of the aortic arch type recorded for the aortic component of this CS2010G surgery.",
    "distal_anastamosis_constructed_bytxt": "Free-text record of the operator who constructed the distal anastomosis during this CS2010G surgical component.",
    "proximal_anastamosis_constructed_bytxt": "Free-text record of the operator who constructed the proximal anastomosis during this CS2010G surgical component.",
    "aortotomytxt": "Free-text record of the operator who performed the aortotomy step of this CS2010G aortic valve procedure.",
    "excision_of_valvetxt": "Free-text record of the operator who performed the valve excision step of this CS2010G aortic valve procedure.",
    "decalcification_of_annulustxt": "Free-text record of the operator who performed the annulus decalcification step of this CS2010G aortic valve procedure.",
    "implantation_of_valvetxt": "Free-text record of the operator who performed the valve implantation step of this CS2010G aortic valve procedure.",
    "closure_of_aortotomytxt": "Free-text record of the operator who performed the aortotomy closure step of this CS2010G aortic valve procedure.",
    "de_airing_of_hearttxt": "Free-text record of the operator who performed the de-airing of the heart step of this CS2010G aortic valve procedure.",
    "bi_caval_cannulationtxt": "Free-text record of the operator who performed the bi-caval cannulation step of this CS2010G mitral valve procedure.",
    "access_to_mitral_valvetxt": "Free-text record of the operator who performed the access to the mitral valve step of this CS2010G mitral valve procedure.",
    "assessment_and_repairtxt": "Free-text record of the operator who performed the mitral valve assessment and repair step of this CS2010G procedure.",
    "excision_of_valve_and_annular_debridementtxt": "Free-text record of the operator who performed the valve excision and annular debridement step of this CS2010G mitral valve procedure.",
    "repair_of_valvetxt": "Free-text record of the operator who performed the mitral valve repair step of this CS2010G procedure.",
    "ringtxt": "Free-text record of the operator who performed the annuloplasty ring step of this CS2010G mitral valve procedure.",
    "implantation_of_mitral_valvetxt": "Free-text record of the operator who performed the mitral valve implantation step of this CS2010G procedure.",
    "atrial_closuretxt": "Free-text record of the operator who performed the atrial closure step of this CS2010G mitral valve procedure.",
    "de_airing_of_heart_mitraltxt": "Free-text record of the operator who performed the de-airing of the heart step of this CS2010G mitral valve procedure.",
    "limatxt": "Free-text record of the operator who harvested or used the left internal mammary artery conduit in this CS2010G graft component.",
    "rimatxt": "Free-text record of the operator who harvested or used the right internal mammary artery conduit in this CS2010G graft component.",
    "lsvtxt": "Free-text record of the operator who harvested or used the long saphenous vein conduit in this CS2010G graft component.",
    "ssvtxt": "Free-text record of the operator who harvested or used the short saphenous vein conduit in this CS2010G graft component.",
    "radialtxt": "Free-text record of the operator who harvested or used the radial artery conduit in this CS2010G graft component.",
    "other_veintxt": "Free-text record of the operator who harvested or used another venous conduit in this CS2010G graft component.",
    "other_arterytxt": "Free-text record of the operator who harvested or used another arterial conduit in this CS2010G graft component.",
    "conduit_quality": "Recorded quality assessment of the graft conduit for this CS2010G subprocedure component, as captured in iWeb.",
    "grafted_using": "Suture or material used to construct the graft anastomosis for this component, as recorded in iWeb.",
    "closed_using": "Suture or material used for closure at this stage of the CS2010G surgery component, as recorded in iWeb.",
    "valve_implanted_using": "Suture technique or material used to implant the valve prosthesis or ring for this component, as recorded in iWeb.",
    "size_of_coronary_artery": "Recorded size of the target coronary artery for this graft component, held as text including the unit as entered in iWeb.",
    "coronary_artery_condition": "Recorded condition of the target coronary artery for this graft component in the iWeb CS2010G form.",
    "primary_repair_techniquetxt": "Text description of the primary valve repair technique used for this component, as recorded in iWeb.",
    "neochord": "Indicates whether neochordae were used or describes the neochord detail for this mitral valve repair component.",
    "minimally_invasive_mitral_valve_surgery": "Indicates whether a minimally invasive approach was used for the mitral valve component of this surgery.",
    "m_aortic_graft_detail": "Descriptive detail of the aortic graft used for this implant component, as recorded in iWeb.",
    "aortic_graft_serial_number": "Manufacturer serial number of the aortic graft implanted for this component.",
    "reimplantation_of_intercostals": "Records whether intercostal arteries were reimplanted during this aortic implant component.",
    "implantation_technique": "Technique used to implant the prosthesis or graft for this component, as recorded in iWeb.",
    "m_level_of_implantation": "Anatomical level at which the aortic graft or prosthesis was implanted for this component.",
    "aortic_occlusion_bypass": "Records the aortic occlusion or bypass approach used during this aortic component of the surgery.",
    "s_vcharacteristicstxt": "Text description of the saphenous vein conduit characteristics recorded for this graft component.",
    "harvesting_characteristicstxt": "Text description of conduit harvesting characteristics recorded for this graft component.",
    "holes": "Records holes identified in the harvested conduit for this graft component, as entered in iWeb.",
    "a_gscore": "Numeric assessment score recorded for the harvested conduit on the iWeb CS2010G subprocedure form.",
    "msv_gcomments": "Free-text comments recorded about the saphenous vein graft harvesting for this component.",
    "harvesting_technique": "Technique used to harvest the conduit for this graft component, as recorded in iWeb.",
    "amount_of_heparin_before_evh": "Amount of heparin given before endoscopic vein harvesting, recorded as a number without a stated unit in the source.",
    "import_child_id": "Legacy child-record identifier carried over from data imported into iWeb for this component row.",
    "native_aortic_valve_morphology": "Recorded morphology of the patient's native aortic valve for this valve component.",
    "aortic_procedure2txt": "Text description of an additional aortic procedure recorded for this component alongside AORTIC_PROCEDURE.",
    "aortic_implant_locationtxt": "Text value recording the anatomical location of the aortic implant for this surgery component, as captured in iWeb.",
    "date_last_changed": "Timestamp of the last change made to this subprocedure record in the iWeb source system.",
    "wall_quality": "Recorded quality of the target vessel wall for this graft component, as captured in iWeb.",
    "accessibility": "Recorded accessibility of the target vessel for this graft component, as captured in iWeb.",
    "distal_run_off": "Recorded distal run-off assessment of the target coronary artery for this graft component.",
    "tvc_sinterpretation": "Recorded interpretation of the transit time flow or vessel check assessment for this graft component, as entered in iWeb.",
    "adc_updt": "Timestamp of the last update applied to this record by the upstream data collection process.",
    "nhs_number_valid_ind": "Boolean indicator showing whether the NHS number held on the source record passed validation, derived from the source NHS number.",
    "linkage_status": "Coded outcome of the patient linkage process between this registry record and the master person index.",
    "person_id": "Internal surrogate person identifier from the Millennium person index, used to link this record to the patient master.",
    "linkage_method": "Code describing which identifier combination was used to link this record to the master person index.",
    "linkage_historical_fallback_ind": "Boolean indicator showing whether the person linkage was resolved using a historical identifier fallback rule.",
    "linkage_fallback_conflict_ind": "Boolean indicator flagging that the fallback linkage rule produced conflicting person matches for this record.",
    "source_table": "Name of the upstream source table from which this row was ingested.",
    "source_record_key": "Key identifying this row in the upstream source table, derived from the source record identifier.",
    "row_hash": "Hash of the source column values for this row, used for change detection during ingestion.",
    "pipeline_loaded_at": "Timestamp recording when this row was loaded into the bronze table by the ingestion pipeline.",
    "is_current_in_source": "Boolean flag indicating whether the record is still present as a current row in the upstream source system.",
    "source_present_ind": "Canonical boolean mirror of IS_CURRENT_IN_SOURCE; false rows are retained source tombstones.",
    "source_absent_detected_ts": "Timestamp recording when this record was first detected as no longer present in the upstream iWeb source system, null while the record remains present.",
    "surname": "Patient surname as recorded on the iWeb CS2010G subprocedure record.",
    "forename": "Patient forename as recorded on the iWeb CS2010G subprocedure record.",
    "nhs_number": "Patient NHS number as recorded on the iWeb CS2010G subprocedure record.",
    "date_of_birth": "Patient date of birth as recorded on the iWeb CS2010G subprocedure record.",
}

CLINICAL_REGISTRY_SURGERY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}

@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_procedure"),
    comment="Internal QC of clinical_registry_surgery_procedure; counts rows whose Gold parent is absent.",
    refresh_policy="incremental",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_registry_surgery_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return _with_comments(df, CLINICAL_REGISTRY_SURGERY_PROCEDURE_COLUMN_COMMENTS)

@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_procedure"),
    comment='Typed child of clinical_registry_entry for the surgery_procedure family. One row per graft, valve or aortic-implant component of a CS2010G surgery. PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is hard-gated to zero orphans. Known source defect: IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist bleed-through. Every governed bronze column is retained under its lower-case name.',
    table_properties={"quality": "gold"},
    refresh_policy="incremental",
)
def gold_clinical_registry_surgery_procedure():
    """Publish clinical_registry_surgery_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_procedure")).drop("__gold_parent_present")

# COMMAND ----------

