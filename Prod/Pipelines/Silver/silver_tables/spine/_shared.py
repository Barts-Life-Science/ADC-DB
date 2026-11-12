"""Silver spine._shared: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 1_silver_spine_and_reference:1060, 1_silver_spine_and_reference:1061, 1_silver_spine_and_reference:1063, 1_silver_spine_and_reference:1365, 1_silver_spine_and_reference:218, 1_silver_spine_and_reference:230, 1_silver_spine_and_reference:232, 1_silver_spine_and_reference:235

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from functools import reduce
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from silver_core.journey_core import canonical_coding_system, read_source, stable_id, subject_key_with_system
from silver_tables._common.journey import SRC_ENCOUNTER, SRC_MEDICONNECT_TYPE_MAP
from silver_tables._common.spine_and_reference import (
    SRC_IWEB_VALUE_MAP,
    SRC_LUNA_CODE_MAPPING,
    SRC_SEED_CODE_MAPPING,
    _iweb_target_system,
)


def _encounter_canonical():
    # Assemble normalized encounter rows for downstream dataset builders, preserving the
    # existing source and identity rules.
    s = read_source(SRC_ENCOUNTER)
    skey, ssys = subject_key_with_system(
        [("urn:cerner:person_id", s.PERSON_ID)],
        SRC_ENCOUNTER,
        s.ENCNTR_ID,
    )
    type_class = F.lower(F.coalesce(s.encntr_type_class_desc, s.encntr_class_desc, F.lit("")))
    ended = F.coalesce(
        s.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    # SDI_ENCOUNTER_STATE_V1: encounter_level is named so the ActCode crosswalk can read it. Level first for the four
    # classed levels (type class is the stronger signal); the source class display only for recurring_contact/other.
    encounter_level = (
        F.when(type_class.contains("inpatient"), F.lit("spell"))
         .when(type_class.contains("emergency"), F.lit("emergency_visit"))
         .when(type_class.contains("outpatient"), F.lit("outpatient_attendance"))
         .when(type_class.contains("recurring"), F.lit("recurring_contact"))
         .when(type_class.contains("preadmit"), F.lit("preadmission"))
         .when(type_class.contains("wait list"), F.lit("waiting_list_placeholder"))
         .when(type_class.contains("results only"), F.lit("results_only"))
         .otherwise(F.lit("other"))
    )
    lvl_cls = F.element_at(F.create_map(F.lit("spell"), F.lit("IMP"), F.lit("outpatient_attendance"), F.lit("AMB"), F.lit("emergency_visit"), F.lit("EMER"), F.lit("preadmission"), F.lit("PRENC")), encounter_level)
    src_cls = F.when(encounter_level.isin("recurring_contact", "other"),
                     F.element_at(F.create_map(F.lit("Inpatient"), F.lit("IMP"), F.lit("Outpatient"), F.lit("AMB"), F.lit("Emergency"), F.lit("EMER"), F.lit("Preadmit"), F.lit("PRENC")), s.encntr_class_desc))
    class_actcode = F.coalesce(lvl_cls, src_cls)
    # Bronze declares LAST_WARD_STILL_OPEN_IND but does not publish it; same rule from the two published columns.
    ward_open = s.LAST_WARD_IN_DT_TM.isNotNull() & (
        s.LAST_WARD_OUT_DT_TM.isNull() | (s.LAST_WARD_OUT_DT_TM < s.LAST_WARD_IN_DT_TM))
    # ENCOUNTER_SOURCE_STATUS_CLOSE_V1: the source encounter status (code set 261) closes encounters that never ran or were discharged
    # without a departure stamp: 855 Cancelled, 666807 Cancelled Pending Arrival, 666809 Rejected Pending Arrival,
    # 666808 Pending Arrival, 856 Discharged.
    status_cd = s.ENCNTR_STATUS_CD.cast("bigint")
    close_state = (
        F.when(s.DEPARTURE_DT_TM_BEST.isNotNull(), F.lit("departed"))
         .when(s.DEPARTURE_REJECTED_REASON.isin("ADMINISTRATIVE_CLOSE", "FIXED_WINDOW_CLOSE"),
               F.lit("source_closed_no_observed_departure"))
         .when(status_cd.isin(855, 666807, 666809), F.lit("source_cancelled"))
         .when(status_cd == 666808, F.lit("source_not_arrived"))
         .when(s.DEPARTURE_REJECTED_REASON == "BEFORE_ARRIVAL", F.lit("departure_evidence_invalid"))
         .when(status_cd == 856, F.lit("source_closed_no_observed_departure"))
         .when(ward_open, F.lit("open_ward_interval"))
         .otherwise(F.lit("no_departure_recorded"))
    )
    # contract v2: retain the encounter SHA as encounter_key and publish native encounter, person, and organization ids
    return s.select(
        stable_id("encounter:mill", s.ENCNTR_ID).alias("encounter_key"),
        s.ENCNTR_ID.cast("bigint").alias("encounter_id"),
        skey.alias("subject_key"),
        ssys.alias("subject_id_system"),
        s.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit(None).cast("string").alias("parent_encounter_id"),
        F.lit("source_parent_unavailable").alias("parentage_status"),
        encounter_level.alias("encounter_level"),  # SDI_ENCOUNTER_STATE_V1
        s.ENCNTR_CLASS_CD.cast("string").alias("class_code"),
        s.encntr_class_desc.alias("class_display"),
        s.ENCNTR_TYPE_CD.cast("string").alias("type_code"),
        s.encntr_type_desc.alias("type_display"),
        s.ENCNTR_TYPE_CLASS_CD.cast("string").alias("type_class_code"),
        s.encntr_type_class_desc.alias("type_class_display"),
        s.ENCNTR_STATUS_CD.cast("string").alias("status_code"),
        s.encntr_status_desc.alias("status_display"),
        s.ARRIVAL_DT_TM_BEST.alias("period_start"),
        s.DEPARTURE_DT_TM_BEST.alias("period_end"),
        # VDB_SILVER_LOCAL_V1: bronze publishes the Europe/London companions; silver only carries them.
        s.ARRIVAL_DT_TM_BEST_LOCAL.alias("period_start_local"),
        s.DEPARTURE_DT_TM_BEST_LOCAL.alias("period_end_local"),
        s.ARRIVAL_METHOD.alias("arrival_method"),
        s.ARRIVAL_CONFIDENCE.alias("arrival_confidence"),
        s.DEPARTURE_METHOD.alias("departure_method"),
        s.DEPARTURE_CONFIDENCE.alias("departure_confidence"),
        class_actcode.alias("class_actcode"),  # SDI_ENCOUNTER_STATE_V1
        F.element_at(F.create_map(F.lit("IMP"), F.lit("inpatient encounter"), F.lit("AMB"), F.lit("ambulatory"), F.lit("EMER"), F.lit("emergency"), F.lit("PRENC"), F.lit("pre-admission")), class_actcode).alias("class_actcode_display"),
        F.when(lvl_cls.isNotNull(), F.lit("encounter_level_crosswalk"))
         .when(src_cls.isNotNull(), F.lit("source_class_crosswalk"))
         .otherwise(F.lit("no_class")).alias("class_actcode_method"),
        s.DEPARTURE_REJECTED_REASON.alias("departure_rejected_reason"),
        F.greatest(s.LAST_CLINICAL_EVENT_DT_TM, s.LAST_ORDER_DT_TM, s.LAST_WARD_IN_DT_TM).alias("last_activity_datetime"),
        close_state.alias("encounter_close_state"),
        s.LENGTH_OF_STAY_MINUTES.cast("long").alias("length_of_stay_minutes"),
        s.SCHEDULED_ARRIVAL_DT_TM.alias("scheduled_start"),
        s.SCHEDULED_DEPARTURE_DT_TM.alias("scheduled_end"),
        s.REG_DT_TM.alias("registration_datetime"),
        s.INPATIENT_ADMIT_DT_TM.alias("inpatient_admit_datetime"),
        s.DISCH_DT_TM.alias("discharge_datetime"),
        s.ENCOUNTER_COMPLETE_DT_TM_EFFECTIVE.alias("workflow_complete_datetime"),
        s.ARRIVE_DT_TM.alias("raw_arrival_datetime"),
        s.DEPART_DT_TM.alias("raw_departure_datetime"),
        s.ADMIT_SRC_CD.cast("string").alias("admission_source_code"),
        s.admit_src_desc.alias("admission_source_display"),
        s.DISCH_TO_LOCTN_CD.cast("string").alias("discharge_destination_code"),
        s.disch_loctn_desc.alias("discharge_destination_display"),
        s.MED_SERVICE_CD.cast("string").alias("responsible_service_code"),
        s.med_service_desc.alias("responsible_service_display"),
        s.SPECIALTY_UNIT_CD.cast("string").alias("specialty_code"),
        s.specialty_unit_desc.alias("specialty_display"),
        F.when(s.LOC_NURSE_UNIT_CD > 0,  # SDI_LOCATION_ZERO_V1: 0 is "no unit" (4.4M encounters), not a location
               stable_id("location:mill:nurse_unit", s.LOC_NURSE_UNIT_CD)).alias("current_location_key"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        F.when(s.SERVICE_PROVIDER_ORG_ID.isNotNull(),
               stable_id("organization:mill", s.SERVICE_PROVIDER_ORG_ID))
         .alias("service_provider_organization_key"),
        s.REASON_FOR_VISIT.alias("reason_for_visit"),
        s.ATTENDANCE_EVIDENCE.alias("attendance_evidence"),
        s.ATTENDANCE_WITNESS_COUNT.cast("int").alias("attendance_witness_count"),
        F.lit(None).cast("string").alias("confidentiality_code"),
        F.lit(None).cast("boolean").alias("vip_ind"),
        F.when(ended, F.lit("superseded")).otherwise(F.lit("active")).alias("record_status"),
        s.ACTIVE_STATUS_DT_TM.alias("record_status_effective_from"),
        F.when(ended, s.END_EFFECTIVE_DT_TM).alias("record_status_effective_to"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.UPDT_DT_TM.alias("source_update_timestamp"),
        s.ADC_UPDT.alias("loaded_at"),
        s.ENCNTR_ID.alias("_source_encounter_id"),
        s.REG_PRSNL_ID.alias("_registration_practitioner_id"),
        s.DISCH_PRSNL_ID.alias("_discharge_practitioner_id"),
        s.CREATE_PRSNL_ID.alias("_creator_practitioner_id"),
        s.REG_DT_TM.alias("_registration_role_start"),
        s.DISCH_DT_TM.alias("_discharge_role_start"),
        s.CREATE_DT_TM.alias("_creator_role_start"),
    )


# ==== Encounter spine, location stays, relationships and care participation ====

# contract v2: retain the full canonical shape for internal reuse.
ENCOUNTER_SOURCE_COLUMNS = [
    "encounter_key",
    "encounter_id",
    "subject_key",
    "subject_id_system",
    "person_id",
    "parent_encounter_id",
    "parentage_status",
    "encounter_level",
    "class_code",
    "class_display",
    "type_code",
    "type_display",
    "type_class_code",
    "type_class_display",
    "status_code",
    "status_display",
    "period_start",
    "period_end",
    "period_start_local",
    "period_end_local",
    "arrival_method",
    "arrival_confidence",
    "departure_method",
    "departure_confidence",
    "class_actcode",  # SDI_ENCOUNTER_STATE_V1
    "class_actcode_display",  # SDI_ENCOUNTER_STATE_V1
    "class_actcode_method",  # SDI_ENCOUNTER_STATE_V1
    "departure_rejected_reason",  # SDI_ENCOUNTER_STATE_V1
    "last_activity_datetime",  # SDI_ENCOUNTER_STATE_V1
    "encounter_close_state",  # SDI_ENCOUNTER_STATE_V1
    "length_of_stay_minutes",
    "scheduled_start",
    "scheduled_end",
    "registration_datetime",
    "inpatient_admit_datetime",
    "discharge_datetime",
    "workflow_complete_datetime",
    "raw_arrival_datetime",
    "raw_departure_datetime",
    "admission_source_code",
    "admission_source_display",
    "discharge_destination_code",
    "discharge_destination_display",
    "responsible_service_code",
    "responsible_service_display",
    "specialty_code",
    "specialty_display",
    "current_location_key",
    "organization_id",
    "service_provider_organization_key",
    "reason_for_visit",
    "attendance_evidence",
    "attendance_witness_count",
    "confidentiality_code",
    "vip_ind",
    "record_status",
    "record_status_effective_from",
    "record_status_effective_to",
    "load_batch_id",
    "source_update_timestamp",
    "loaded_at",
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_encounter():
    # Assemble encounter rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    return _encounter_canonical().select(*ENCOUNTER_SOURCE_COLUMNS)


# Reference /  snomed concept crosswalk
SRC_S3B_ETHNICITY_MAP = "3_lookup.omop.ethnicity_snomed_map"


# Spine / person

# ==== Person identity and reference dimensions ====
SRC_PERSON         = "4_prod.bronze.map_person"


SRC_PATIENT_IDENTIFIER = "4_prod.bronze.map_patient_identifier"


# MRN_BARTS_ONLY_V1: current_mrn is the Barts MRN only; BHRUT (RF4 MRN, 1115132483) and other pools are never selected.
MRN_POOL_PREFERENCE = {6200990: ("barts", 1)}  # RNJ 5C4 MRN


def _person_alias_selection(alias_type, pool_preference=None):
    """Pick the current MRN or NHS number from the governed alias feed.
    With pool_preference, only aliases in those pools are candidates.
    Ranking: active alias first (CURRENT_IND); among active aliases the preferred pool when
    pool_preference is given; then latest valid end-effective (open-ended sorts last via the
    2100 sentinel), NULLS LAST via coalesce floors, deterministic SOURCE_PK tiebreak.
    Ambiguity is never tiebroken: when the winning pool (the whole alias type without
    pool_preference) holds more than one distinct active value, it resolves to 'ambiguous'
    with NULL value. The bronze MULTI_ACTIVE_IND is per (person, type), so it would call one
    Barts plus one BHRUT MRN ambiguous; it is not used."""
    a = read_source(SRC_PATIENT_IDENTIFIER).where(F.col("ALIAS_TYPE") == alias_type)
    if pool_preference:  # MRN_BARTS_ONLY_V1: candidates come only from the listed pools
        a = a.where(F.col("ALIAS_POOL_CD").isin(list(pool_preference)))
    current = F.coalesce(a.CURRENT_IND, F.lit(False))
    pool_rank, pool_label = F.lit(0), F.lit("other")
    for pool_cd, (label, rank) in (pool_preference or {}).items():
        pool_rank = F.when(a.ALIAS_POOL_CD == F.lit(pool_cd), F.lit(rank)).otherwise(pool_rank)
        pool_label = F.when(a.ALIAS_POOL_CD == F.lit(pool_cd), F.lit(label)).otherwise(pool_label)
    per_pool = a.select(
        a.PERSON_ID.cast("string").alias("_pid"),
        (a.ALIAS_POOL_CD if pool_preference else F.lit(0)).alias("_pool"),
        F.when(current, a.ALIAS_VALUE).alias("_cur_value"),
        a.PIPELINE_UPDT_DT_TM.alias("_alias_loaded_at"),
        a.SOURCE_ADC_UPDT.alias("_alias_source_updt"),
        F.struct(
            current.cast("int").alias("o_current"),
            F.when(current, pool_rank).otherwise(F.lit(0)).alias("o_pool"),
            F.coalesce(a.END_EFFECTIVE_DT_TM_CLEAN,
                       F.lit("2100-01-01").cast("timestamp")).alias("o_valid_to"),
            F.coalesce(a.BEG_EFFECTIVE_DT_TM_CLEAN,
                       F.lit("1900-01-01").cast("timestamp")).alias("o_valid_from"),
            F.coalesce(a.SOURCE_PK, F.lit(-1)).alias("o_tiebreak"),
            a.ALIAS_VALUE.alias("v_value"),
            current.alias("v_current"),
            pool_label.alias("v_pool"),
        ).alias("ranked"),
    ).groupBy("_pid", "_pool").agg(
        F.max("ranked").alias("w"),
        F.countDistinct("_cur_value").alias("_n_current"),
        F.max("_alias_loaded_at").alias("_alias_loaded_at"),
        F.max("_alias_source_updt").alias("_alias_source_updt"),
    )
    won = per_pool.groupBy("_pid").agg(
        F.max(F.struct(F.col("w"), F.col("_n_current"))).alias("p"),
        F.max("_alias_loaded_at").alias("_alias_loaded_at"),
        F.max("_alias_source_updt").alias("_alias_source_updt"),
    )
    ambiguous = F.col("p._n_current") > 1
    return won.select(
        F.col("_pid"),
        F.when(ambiguous, F.lit(None).cast("string")).otherwise(F.col("p.w.v_value")).alias("_value"),
        F.when(ambiguous, F.lit(None).cast("string"))
         .when(F.col("p.w.v_current"), F.lit("active")).otherwise(F.lit("inactive")).alias("_value_status"),
        F.when(ambiguous, F.lit("ambiguous"))
         .when(F.col("p.w.v_current"), F.lit("active_alias"))
         .otherwise(F.lit("latest_valid_alias")).alias("_selection_status"),
        F.col("p.w.v_pool").alias("_pool_label"),
        F.col("_alias_loaded_at"),
        F.col("_alias_source_updt"),
    )


PERSON_LIFECYCLE_FIELDS = [
    'load_batch_id',
]


PERSON_RETIRED_COLUMNS = [

]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person():
    # Assemble person rows with lifecycle and source evidence for the public product and its
    # internal metadata.
    s = read_source(SRC_PERSON)
    mrn = _person_alias_selection("MRN", MRN_POOL_PREFERENCE).alias("mrn")  # SDI_MRN_POOL_V1/silver:
    nhs = _person_alias_selection("NHS").alias("nhs")
    # BBR_FOLDIN_V1 (BBR-16): religion SNOMED crosswalk, one row per Cerner RELIGION_CD (built by INSR P1.BBR.lookups).
    religion = read_source("3_lookup.omop.religion_snomed_map").select(
        F.col("source_code").cast("string").alias("_rel_code"), "religion_snomed_code", "religion_snomed_display",
        "religion_group", F.col("mapping_rule_id").alias("religion_map_rule_id")).alias("rel")
    ended = F.coalesce(
        s.end_effective_dt_tm < F.lit("2100-01-01").cast("timestamp"),
        F.lit(False),
    )
    joined = (
        s.join(mrn, s.person_id.cast("string") == F.col("mrn._pid"), "left")
         .join(nhs, s.person_id.cast("string") == F.col("nhs._pid"), "left")
         .join(religion, s.religion_cd.cast("string") == F.col("rel._rel_code"), "left")  # BBR_FOLDIN_V1
    )
    # contract v2: publish Millennium PERSON_ID as the BIGINT person primary key
    return joined.select(
        s.person_id.cast("bigint").alias("person_id"),
        (s.active_ind == F.lit(1)).alias("active"),
        s.gender_cd.cast("string").alias("gender_code"),
        s.gender_display.alias("gender_display"),
        s.ethnicity_cd.cast("string").alias("ethnicity_code"),
        s.ethnicity_display.alias("ethnicity_display"),
        s.birth_date.alias("birth_date"),
        s.birth_datetime.alias("birth_datetime"),
        s.birth_dt_cd.cast("string").alias("birth_precision_code"),
        s.birth_dt_display.alias("birth_precision_display"),
        s.birth_precision_flag.cast("string").alias("birth_precision_flag"),
        s.language_cd.cast("string").alias("language_code"),
        s.language_display.alias("language_display"),
        s.marital_type_cd.cast("string").alias("marital_status_code"),
        s.marital_type_display.alias("marital_status_display"),
        s.religion_cd.cast("string").alias("religion_code"),
        s.religion_display.alias("religion_display"),
        F.col("rel.religion_snomed_code"), F.col("rel.religion_snomed_display"),  # BBR_FOLDIN_V1 BBR-16
        F.col("rel.religion_group"), F.col("rel.religion_map_rule_id"),  # BBR_FOLDIN_V1 BBR-16
        # SDI_DECEASED_V1/silver: DECEASED_CD 684730 displays 'No'; only a date or the 'Deceased' code counts.
        (s.deceased_dt_tm.isNotNull()
         | F.coalesce(F.upper(F.trim(s.deceased_display)) == F.lit("DECEASED"), F.lit(False))).alias("deceased_ind"),
        F.when(s.deceased_dt_tm.isNotNull(), F.lit("death_date"))
         .when(F.upper(F.trim(s.deceased_display)) == F.lit("DECEASED"), F.lit("deceased_code"))
         .alias("deceased_ind_source"),
        s.deceased_dt_tm.alias("deceased_datetime"),
        s.deceased_dt_tm_precision_flag.cast("string").alias("deceased_datetime_precision"),
        s.confid_level_cd.cast("string").alias("confidentiality_code"),
        (F.coalesce(s.vip_cd, F.lit(0)) != 0).alias("vip_ind"),
        s.current_address_id.cast("string").alias("current_address_id"),
        s.latest_known_address_id.cast("string").alias("latest_known_address_id"),
        s.address_selection_status.alias("address_selection_status"),
        F.col("mrn._value").alias("current_mrn"),
        F.col("mrn._value_status").alias("current_mrn_status"),
        F.coalesce(F.col("mrn._selection_status"), F.lit("no_alias")).alias("mrn_selection_status"),
        F.col("mrn._pool_label").alias("current_mrn_pool"),  # SDI_MRN_POOL_V1/silver:
        F.col("nhs._value").alias("nhs_number"),
        F.col("nhs._value_status").alias("nhs_number_status"),
        F.coalesce(F.col("nhs._selection_status"), F.lit("no_alias")).alias("nhs_number_selection_status"),
        F.when((s.active_ind == 1) & ~ended, F.lit("active"))
         .otherwise(F.lit("superseded")).alias("record_status"),
        s.active_status_dt_tm.alias("record_status_effective_from"),
        F.when(ended, s.end_effective_dt_tm).alias("record_status_effective_to"),
        F.greatest(
            s.source_updt_dt_tm,
            F.coalesce(F.col("mrn._alias_source_updt"), s.source_updt_dt_tm),
            F.coalesce(F.col("nhs._alias_source_updt"), s.source_updt_dt_tm),
        ).alias("source_update_timestamp"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        F.greatest(
            s.ADC_UPDT,
            F.coalesce(F.col("mrn._alias_loaded_at"), s.ADC_UPDT),
            F.coalesce(F.col("nhs._alias_loaded_at"), s.ADC_UPDT),
        ).alias("loaded_at"),
    )


# Location projection
SRC_CARE_SITE      = "4_prod.bronze.map_care_site"


SRC_LOCATION_UNIT  = "4_prod.bronze.map_location_unit"  # SDI_LOCATION_LANE_V1


def _location_projection():
    # Derive the facility, building, unit and room location hierarchy from care-site evidence.
    s = read_source(SRC_CARE_SITE)

    # contract v2: retain SHA hierarchy keys while publishing level-qualified source location codes and native organization ids
    facilities = (
        s.where(s.facility_cd.isNotNull())
         .groupBy("facility_cd")
         .agg(
             F.max("facility_name").alias("name"),
             F.max("facility_code_active_ind").alias("active_ind"),
             F.min("facility_relation_beg_effective_dt_tm").alias("valid_from"),
             F.max("facility_relation_end_effective_dt_tm").alias("valid_to"),
             F.max("ORGANIZATION_ID").alias("organization_id"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("latitude").alias("latitude"),
             F.max("longitude").alias("longitude"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         .select(
             stable_id("location:mill:facility", F.col("facility_cd")).alias("location_key"),
             F.col("facility_cd").cast("string").alias("location_code"),
             F.lit(None).cast("string").alias("parent_location_key"),
             F.lit("facility").alias("location_level"),
             F.col("facility_cd").cast("string").alias("source_location_code"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("organization_id").cast("bigint").alias("organization_id"),
             F.lit("si").alias("physical_type_code"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("latitude"), F.col("longitude"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("facility_cd").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

    buildings = (
        s.where(s.building_cd.isNotNull())
         .groupBy("building_cd")
         .agg(
             F.max("building_name").alias("name"),
             F.max("building_code_active_ind").alias("active_ind"),
             F.min("building_relation_beg_effective_dt_tm").alias("valid_from"),
             F.max("building_relation_end_effective_dt_tm").alias("valid_to"),
             F.min("facility_cd").alias("facility_cd"),
             F.max("ORGANIZATION_ID").alias("organization_id"),
             F.max("masked_zipcode").alias("address_postcode_masked"),
             F.max("city").alias("address_city"),
             F.max("latitude").alias("latitude"),
             F.max("longitude").alias("longitude"),
             F.max("ADC_UPDT").alias("loaded_at"),
         )
         .select(
             stable_id("location:mill:building", F.col("building_cd")).alias("location_key"),
             F.col("building_cd").cast("string").alias("location_code"),
             F.when(F.col("facility_cd").isNotNull(),
                    stable_id("location:mill:facility", F.col("facility_cd"))).alias("parent_location_key"),
             F.lit("building").alias("location_level"),
             F.col("building_cd").cast("string").alias("source_location_code"),
             F.col("name"),
             F.when(F.col("active_ind") == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
             F.col("organization_id").cast("bigint").alias("organization_id"),
             F.lit("bu").alias("physical_type_code"),
             F.col("valid_from"), F.col("valid_to"),
             F.col("latitude"), F.col("longitude"),
             F.col("address_city"), F.col("address_postcode_masked"),
             F.lit(SRC_CARE_SITE).alias("source_table"),
             F.col("building_cd").cast("string").alias("source_row_id"),
             F.date_format("loaded_at", "yyyyMMddHHmmss").alias("load_batch_id"),
             F.col("loaded_at"),
         )
    )

    units = s.select(
        stable_id("location:mill:nurse_unit", s.care_site_cd).alias("location_key"),
        s.care_site_cd.cast("string").alias("location_code"),
        F.when(s.building_cd.isNotNull(), stable_id("location:mill:building", s.building_cd))
         .when(s.facility_cd.isNotNull(), stable_id("location:mill:facility", s.facility_cd))
         .alias("parent_location_key"),
        F.lit("nurse_unit").alias("location_level"),
        s.care_site_cd.cast("string").alias("source_location_code"),
        s.care_site_name.alias("name"),
        F.when(s.location_active_ind == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        s.ORGANIZATION_ID.cast("bigint").alias("organization_id"),
        F.lit("wa").alias("physical_type_code"),
        s.location_beg_effective_dt_tm.alias("valid_from"),
        s.location_end_effective_dt_tm.alias("valid_to"),
        s.latitude, s.longitude,
        s.city.alias("address_city"),
        s.masked_zipcode.alias("address_postcode_masked"),
        F.lit(SRC_CARE_SITE).alias("source_table"),
        s.care_site_cd.cast("string").alias("source_row_id"),
        F.date_format(s.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        s.ADC_UPDT.alias("loaded_at"),
    )
    care = facilities.unionByName(buildings).unionByName(units)

    # SDI_LOCATION_LANE_V1: map_care_site holds 438 of the 977 nurse-unit codes encounters reference, so 38M of 44M
    # encounter locations and most stays and appointments could not decode. Every other Millennium location (nurse
    # units without a care-site row, rooms, beds, theatres/ancillary, radiology, ambulatory, facilities and buildings)
    # comes from map_location_unit; a code already published from map_care_site keeps that row. Keys hash in the
    # namespace consumers use: facility and building levels their own, every other level nurse_unit.
    def _key_namespace(level):
        return (F.when(level == "facility", F.lit("location:mill:facility"))
                 .when(level == "building", F.lit("location:mill:building"))
                 .otherwise(F.lit("location:mill:nurse_unit")))

    u = read_source(SRC_LOCATION_UNIT).where(F.col("LOCATION_CD") > 0)
    care_codes = care.select(F.col("location_code").alias("_code"), F.col("location_level").alias("_level"))
    other = u.join(care_codes.select("_code").distinct(), F.col("LOCATION_CD").cast("string") == F.col("_code"), "left_anti")
    parents = (care_codes.unionByName(other.select(F.col("LOCATION_CD").cast("string").alias("_code"),
                                                   F.col("LOCATION_LEVEL").alias("_level")))
               .groupBy(F.col("_code").alias("_parent_code")).agg(F.min("_level").alias("_parent_level")))
    level = F.col("LOCATION_LEVEL")
    lane = other.join(parents, F.col("PARENT_LOCATION_CD").cast("string") == F.col("_parent_code"), "left").select(
        stable_id(_key_namespace(level), F.col("LOCATION_CD")).alias("location_key"),
        F.col("LOCATION_CD").cast("string").alias("location_code"),
        F.when(F.col("_parent_level").isNotNull(),
               stable_id(_key_namespace(F.col("_parent_level")), F.col("PARENT_LOCATION_CD"))).alias("parent_location_key"),
        level.alias("location_level"),
        F.col("LOCATION_CD").cast("string").alias("source_location_code"),
        F.col("LOCATION_DISPLAY").alias("name"),
        F.when(F.col("ACTIVE_IND") & F.col("SOURCE_PRESENT_IND"), F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        F.lit(None).cast("bigint").alias("organization_id"),
        F.when(level == "nurse_unit", F.lit("wa")).when(level == "room", F.lit("ro")).when(level == "bed", F.lit("bd"))
         .when(level == "facility", F.lit("si")).when(level == "building", F.lit("bu")).alias("physical_type_code"),
        F.col("BEG_EFFECTIVE_DT_TM").alias("valid_from"),
        F.col("END_EFFECTIVE_DT_TM").alias("valid_to"),
        F.lit(None).cast("double").alias("latitude"), F.lit(None).cast("double").alias("longitude"),
        F.lit(None).cast("string").alias("address_city"), F.lit(None).cast("string").alias("address_postcode_masked"),
        F.lit(SRC_LOCATION_UNIT).alias("source_table"),
        F.col("LOCATION_CD").cast("string").alias("source_row_id"),
        F.date_format("ADC_UPDT", "yyyyMMddHHmmss").alias("load_batch_id"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )
    # INSR_W2 W2.11 (L4164): map_location_unit rows inherit the organisation of their nearest care-site ancestor
    # (parent, grandparent, great-grandparent via PARENT_LOCATION_CD); care-site rows keep their own. Equi-joins only.
    org_of = (care.where(F.col("organization_id").isNotNull())
              .groupBy(F.col("location_code").alias("_oc"))
              .agg(F.min("organization_id").alias("_olo"), F.max("organization_id").alias("_ohi"))
              .where(F.col("_olo") == F.col("_ohi")).select("_oc", F.col("_olo").alias("_org")))
    hop = u.groupBy(F.col("LOCATION_CD").cast("string").alias("_c")).agg(
        F.min(F.col("PARENT_LOCATION_CD").cast("string")).alias("_p1"))
    chain = (hop.join(hop.select(F.col("_c").alias("_c2"), F.col("_p1").alias("_p2")), F.col("_p1") == F.col("_c2"), "left")
                .join(hop.select(F.col("_c").alias("_c3"), F.col("_p1").alias("_p3")), F.col("_p2") == F.col("_c3"), "left"))
    for i in (1, 2, 3):
        chain = chain.join(org_of.select(F.col("_oc").alias(f"_oc{i}"), F.col("_org").alias(f"_org{i}")),
                           F.col(f"_p{i}") == F.col(f"_oc{i}"), "left")
    chain = chain.select(
        F.col("_c").alias("_lane_code"), F.coalesce("_org1", "_org2", "_org3").alias("_lane_org"),
        F.when(F.col("_org1").isNotNull(), F.lit("parent_care_site")).when(F.col("_org2").isNotNull(), F.lit("grandparent_care_site"))
         .when(F.col("_org3").isNotNull(), F.lit("great_grandparent_care_site")).alias("_lane_org_basis"))
    lane = (lane.join(chain, F.col("location_code") == F.col("_lane_code"), "left")
                .withColumn("organization_id", F.col("_lane_org"))
                .withColumn("organization_id_basis", F.col("_lane_org_basis"))
                .drop("_lane_code", "_lane_org", "_lane_org_basis"))
    care = care.withColumn("organization_id_basis", F.when(F.col("organization_id").isNotNull(), F.lit("care_site_source")))
    return care.unionByName(lane)


SRC_PRACTITIONER   = "4_prod.bronze.map_medical_personnel"


# Spine / encounter

# contract v2: the researcher-facing parent excludes lifecycle, QC and retired fields.
ENCOUNTER_PUBLIC_COLUMNS = [
    'encounter_key',
    'encounter_id',
    'subject_key',
    'subject_id_system',
    'person_id',
    'parent_encounter_id',
    'parentage_status',
    'encounter_level',
    'class_code',
    'class_display',
    'type_code',
    'type_display',
    'type_class_code',
    'type_class_display',
    'status_code',
    'status_display',
    'period_start',
    'period_end',
    'period_start_local',
    'period_end_local',
    'arrival_method',
    'arrival_confidence',
    'departure_method',
    'departure_confidence',
    'class_actcode',  # SDI_ENCOUNTER_STATE_V1
    'class_actcode_display',  # SDI_ENCOUNTER_STATE_V1
    'class_actcode_method',  # SDI_ENCOUNTER_STATE_V1
    'departure_rejected_reason',  # SDI_ENCOUNTER_STATE_V1
    'last_activity_datetime',  # SDI_ENCOUNTER_STATE_V1
    'encounter_close_state',  # SDI_ENCOUNTER_STATE_V1
    'length_of_stay_minutes',
    'scheduled_start',
    'scheduled_end',
    'registration_datetime',
    'inpatient_admit_datetime',
    'discharge_datetime',
    'workflow_complete_datetime',
    'raw_arrival_datetime',
    'raw_departure_datetime',
    'admission_source_code',
    'admission_source_display',
    'discharge_destination_code',
    'discharge_destination_display',
    'responsible_service_code',
    'responsible_service_display',
    'specialty_code',
    'specialty_display',
    'current_location_key',
    'organization_id',
    'service_provider_organization_key',
    'reason_for_visit',
    'attendance_evidence',
    'attendance_witness_count',
    'confidentiality_code',
    'vip_ind',
    'record_status',
    'record_status_effective_from',
    'record_status_effective_to',
    'source_update_timestamp',
    'loaded_at',
]


SRC_DIAGNOSIS_MAPPING = "4_prod.bronze.map_diagnosis"


SRC_CODED_EVENT_MAPPING = "4_prod.bronze.map_coded_events_omop_bridge"


SRC_PATHOLOGY_MAPPING = "4_prod.bronze.map_pathology"


SRC_PATHOLOGY_CROSS_ARM_TEST_MAP = "3_lookup.omop.pathology_cross_arm_test_map"


SRC_PATHOLOGY_HGNC_GENE = "3_lookup.omop.pathology_hgnc_gene"


SRC_PATHOLOGY_HGNC_ALIAS = "3_lookup.omop.pathology_hgnc_alias"


SRC_PATHOLOGY_ANTIMICROBIAL_MAP = "3_lookup.omop.pathology_antimicrobial_map"


SRC_PATHOLOGY_PANEL_DEFINITION = "3_lookup.omop.pathology_panel_definition"


SRC_PATHOLOGY_PANEL_GENE = "3_lookup.omop.pathology_panel_gene"


SRC_CANCER_DRUG_MAP = "3_lookup.omop.cancer_treatment_term_map"


SRC_BLOODTRACK_BLOOD_GROUP_MAP = "3_lookup.omop.bloodtrack_blood_group_map"


SRC_BLOODTRACK_PRODUCT_GROUP_MAP = "3_lookup.omop.bloodtrack_product_group_map"


def _concept_map_projection(
    df,
    source_system,
    source_code,
    source_display,
    target_system,
    target_code,
    target_display,
    target_domain,
    map_source,
    source_table,
    mapping_rule_id,
    mapping_rank,
    valid_from,
    valid_to,
    loaded_at,
    *,
    review_status_col=None,
):
    # Normalize source and target coding systems and project mapping evidence into the shared
    # concept-map schema.
    staged = df.select(
        canonical_coding_system(source_system).alias("source_coding_system"),
        F.trim(source_code.cast("string")).alias("source_code"),  # INSR_W2 W2.08 (S08.1 ruling): key + grouping
        source_display.cast("string").alias("source_display"),
        canonical_coding_system(target_system).alias("target_coding_system"),
        target_code.cast("string").alias("target_code"),
        target_display.cast("string").alias("target_display"),
        target_domain.cast("string").alias("target_domain"),
        map_source.cast("string").alias("map_source"),
        source_table.cast("string").alias("source_table"),
        F.lit(None).cast("string").alias("map_version"),
        (F.lit("source_carried") if review_status_col is None else review_status_col)
        .cast("string").alias("review_status"),
        mapping_rule_id.cast("string").alias("mapping_rule_id"),
        mapping_rank.cast("int").alias("mapping_rank"),
        valid_from.cast("date").alias("valid_from"),
        valid_to.cast("date").alias("valid_to"),
        loaded_at.cast("timestamp").alias("loaded_at"),
    ).where(
        F.col("source_coding_system").isNotNull()
        & F.col("source_code").isNotNull()
        & F.col("target_coding_system").isNotNull()
        & F.col("target_code").isNotNull()
    )
    grouped = (
        staged.groupBy(
            "source_coding_system", "source_code",
            "target_coding_system", "target_code", "target_domain",
            "map_source", "source_table", "map_version", "mapping_rule_id", "mapping_rank",
            "review_status", "valid_from", "valid_to",
        )
        .agg(
            F.max("source_display").alias("source_display"),
            F.max("target_display").alias("target_display"),
            F.count(F.lit(1)).cast("long").alias("source_row_count"),
            F.max("loaded_at").alias("loaded_at"),
        )
    )
    grouped = (grouped
        .withColumn("map_method",
            F.when((F.col("source_coding_system") == F.col("target_coding_system")) &
                   (F.col("source_code") == F.col("target_code")), F.lit("source_native"))
             .otherwise(F.lit("rule")))
        .withColumn("map_confidence", F.lit(None).cast("double"))
        .withColumn("map_rule_id", F.col("mapping_rule_id"))
        .withColumn("map_candidate_count", F.count(F.lit(1)).over(Window.partitionBy(
            "source_coding_system", "source_code", "map_source")).cast("int")))
    # contract v2: name the deterministic terminology-map identity concept_map_key
    # INSR_W2 W2.08 (L5072, P5b_key): 906 keys covered rows differing only in source_table / review_status. Only
    # those groups add the two parts to their key, so every other key is unchanged.
    key_parts = [F.col(c) for c in ("source_coding_system", "source_code", "target_coding_system", "target_code",
                                    "target_domain", "map_source", "map_version", "mapping_rule_id", "mapping_rank",
                                    "valid_from", "valid_to")]
    shared_key = F.count(F.lit(1)).over(Window.partitionBy(*key_parts)) > 1
    return grouped.select(
        F.when(shared_key, stable_id("terminology-map", *key_parts, F.col("source_table"), F.col("review_status")))
         .otherwise(stable_id("terminology-map", *key_parts)).alias("concept_map_key"),
        "source_coding_system", "source_code", "source_display",
        "target_coding_system", "target_code", "target_display", "target_domain",
        F.lit(None).cast("string").alias("equivalence"),
        "map_source", "map_version", "mapping_rule_id", "mapping_rank",
        "map_method", "map_confidence", "map_rule_id", "map_candidate_count",
        "review_status",
        "valid_from", "valid_to",
        "source_table",
        "source_row_count", "loaded_at",
        )


def _union_frames(frames):
    # Stack DataFrames by column name, preserving all rows from every supplied frame.
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def _concept_map_projection_all():
    # Combine the supported diagnosis, procedure and other terminology feeds into the common
    # concept-map projection.
    d = read_source(SRC_DIAGNOSIS_MAPPING)
    diagnosis_source_system = F.coalesce(
        d.source_vocabulary_desc,
        d.CONCEPT_CKI_SOURCE,
        F.lit("urn:cerner:nomenclature"),
    )
    diagnosis_source_code = F.coalesce(
        d.SOURCE_IDENTIFIER,
        d.CONCEPT_CKI_IDENTIFIER,
        d.NOMENCLATURE_ID.cast("string"),
    )
    diagnosis_source_display = F.coalesce(
        d.SOURCE_STRING, d.DIAGNOSIS_DISPLAY, d.DIAGNOSIS_TEXT
    )
    no_date = F.lit(None).cast("date")
    no_rank = F.lit(None).cast("int")
    mappings = [
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("http://snomed.info/sct"), d.SNOMED_CODE, d.SNOMED_TERM,
            F.lit("condition"), F.lit("bronze.map_diagnosis:snomed"),
            F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_snomed"), d.SNOMED_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("http://hl7.org/fhir/sid/icd-10"), d.ICD10_CODE, d.ICD10_TERM,
            F.lit("condition"), F.lit("bronze.map_diagnosis:icd10"),
            F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_icd10"), d.ICD10_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
        _concept_map_projection(
            d, diagnosis_source_system, diagnosis_source_code, diagnosis_source_display,
            F.lit("urn:omop:concept_id"), d.OMOP_CONCEPT_ID, d.OMOP_CONCEPT_NAME,
            F.coalesce(F.lower(d.OMOP_CONCEPT_DOMAIN), F.lit("condition")),
            F.lit("bronze.map_diagnosis:omop"), F.lit(SRC_DIAGNOSIS_MAPPING),
            F.lit("diagnosis_omop"),
            d.OMOP_MATCH_NUMBER, no_date, no_date, d.ADC_UPDT,
        ),
    ]

    b = read_source(SRC_CODED_EVENT_MAPPING)
    mappings.append(
        _concept_map_projection(
            b,
            F.concat(F.lit("urn:cerner:coded-event:"),
                     F.lower(F.coalesce(b.SOURCE_FIELD, F.lit("unknown")))),
            b.SOURCE_VALUE,
            b.MATCHED_SOURCE_VARIANT,
            F.lit("urn:omop:concept_id"),
            b.OMOP_CONCEPT_ID,
            b.OMOP_CONCEPT_NAME,
            F.coalesce(F.lower(b.OMOP_CONCEPT_DOMAIN), F.lower(b.OMOP_TABLE), F.lit("unknown")),
            F.lit("bronze.map_coded_events_omop_bridge:omop"),
            F.lit(SRC_CODED_EVENT_MAPPING),
            b.MAPPING_RULE_ID,
            b.MAPPING_RANK,
            b.OMOP_VALID_START_DATE,
            b.OMOP_VALID_END_DATE,
            F.coalesce(b.MAPPING_ADC_UPDT, b.PIPELINE_UPDT_DT_TM),
        )
    )

    p = read_source(SRC_PATHOLOGY_MAPPING)
    pathology_source_system = F.coalesce(
        p.code_system, F.lit("urn:barts:pathology:event-code")
    )
    pathology_source_code = F.coalesce(p.code, p.EVENT_CD.cast("string"))
    pathology_source_display = F.coalesce(p.description, p.EVENT_CD_DISPLAY)
    result_source_system = F.concat(
        F.lit("urn:barts:pathology:result-value:"), pathology_source_code
    )
    pathology_loaded_at = F.coalesce(p.mapping_updated_at, p.ADC_UPDT, p.source_adc_updt)
    mappings.extend([
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("http://snomed.info/sct"), p.test_snomed_code, p.description,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-snomed"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("http://loinc.org"), p.test_loinc_code, p.description,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-loinc"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, pathology_source_system, pathology_source_code, pathology_source_display,
            F.lit("urn:omop:concept_id"), p.test_omop_concept_id, p.measurement_concept_name,
            F.lit("measurement"), F.lit("bronze.map_pathology:test-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.test_confidence_tier, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("http://snomed.info/sct"), p.result_snomed_code, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-snomed"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("http://loinc.org"), p.result_loinc_code, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-loinc"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, result_source_system, p.value_source_value, p.value_source_value,
            F.lit("urn:omop:concept_id"), p.result_omop_concept_id, p.result_concept_name,
            F.lit("measurement_value"), F.lit("bronze.map_pathology:result-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.result_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, F.lit("urn:barts:pathology:unit"), p.unit_source_value, p.unit_source_value,
            F.lit("http://unitsofmeasure.org"), p.ucum_code, p.ucum_code,
            F.lit("unit"), F.lit("bronze.map_pathology:unit-ucum"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.unit_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
        _concept_map_projection(
            p, F.lit("urn:barts:pathology:unit"), p.unit_source_value, p.unit_source_value,
            F.lit("urn:omop:concept_id"), p.unit_concept_id, p.ucum_code,
            F.lit("unit"), F.lit("bronze.map_pathology:unit-omop"),
            F.lit(SRC_PATHOLOGY_MAPPING),
            p.unit_mapping_match_type, no_rank, no_date, no_date, pathology_loaded_at,
        ),
    ])
    xa = read_source(SRC_PATHOLOGY_CROSS_ARM_TEST_MAP)
    mappings.append(
        _concept_map_projection(
            xa, F.lit("urn:barts:pathology:wkg-tlc"), xa.wkg_code, xa.wkg_code,
            F.lit("urn:cerner:order-mnemonic"), xa.order_mnemonic, xa.order_mnemonic,
            F.lit("measurement"), F.lit("lookup.pathology_cross_arm_test_map"),
            F.lit(SRC_PATHOLOGY_CROSS_ARM_TEST_MAP),
            F.concat(F.lit("tfc:"), F.coalesce(xa.tfc_code.cast("string"), F.lit("~"))),
            no_rank, xa.valid_from, xa.valid_to, xa.ADC_UPDT,
            review_status_col=xa.status,
        )
    )
    hg = read_source(SRC_PATHOLOGY_HGNC_GENE)
    mappings.append(
        _concept_map_projection(
            hg, F.lit("urn:barts:pathology:gene-symbol"), hg.approved_symbol,
            hg.approved_name, F.lit("urn:hgnc"), hg.hgnc_id, hg.approved_symbol,
            F.lit("gene"), F.lit("lookup.pathology_hgnc_gene"),
            F.lit(SRC_PATHOLOGY_HGNC_GENE),
            hg.reference_release, no_rank, no_date, no_date, hg.ADC_UPDT,
        )
    )
    ha = read_source(SRC_PATHOLOGY_HGNC_ALIAS)
    mappings.append(
        _concept_map_projection(
            ha, F.lit("urn:barts:pathology:gene-symbol"), ha.alias_symbol,
            ha.alias_symbol, F.lit("urn:hgnc"), ha.hgnc_id, ha.approved_symbol,
            F.lit("gene"), F.lit("lookup.pathology_hgnc_alias"),
            F.lit(SRC_PATHOLOGY_HGNC_ALIAS),
            ha.alias_type, no_rank, no_date, no_date, ha.ADC_UPDT,
            review_status_col=F.when(F.coalesce(ha.ambiguous_ind, F.lit(False)),
                                     F.lit("ambiguous")).otherwise(F.lit("source_carried")),
        )
    )
    am = read_source(SRC_PATHOLOGY_ANTIMICROBIAL_MAP)
    mappings.append(
        _concept_map_projection(
            am, am.code_system, am.code, am.antimicrobial_text,
            F.lit("urn:omop:concept_id"), am.antimicrobial_omop_concept_id,
            am.antimicrobial_text, F.lit("drug"),
            F.lit("lookup.pathology_antimicrobial_map"),
            F.lit(SRC_PATHOLOGY_ANTIMICROBIAL_MAP),
            am.method, no_rank, no_date, no_date, am.ADC_UPDT,
            review_status_col=am.status,
        )
    )
    pdef = read_source(SRC_PATHOLOGY_PANEL_DEFINITION)
    mappings.append(
        _concept_map_projection(
            pdef, F.lit("urn:barts:pathology:panel-code"),
            F.concat_ws("@", pdef.panel_code, pdef.panel_version), pdef.panel_name,
            F.lit("urn:barts:pathology:assay-code"), pdef.source_assay_code,
            pdef.panel_name, F.lit("measurement"),
            F.lit("lookup.pathology_panel_definition"),
            F.lit(SRC_PATHOLOGY_PANEL_DEFINITION),
            pdef.analysis_context, no_rank, pdef.effective_from, pdef.effective_to, pdef.ADC_UPDT,
        )
    )
    pgn = read_source(SRC_PATHOLOGY_PANEL_GENE)
    mappings.append(
        _concept_map_projection(
            pgn, F.lit("urn:barts:pathology:panel-code"),
            F.concat_ws("@", pgn.panel_code, pgn.panel_version), pgn.panel_code,
            F.lit("urn:hgnc"), pgn.hgnc_id, pgn.gene_symbol,
            F.lit("gene"), F.lit("lookup.pathology_panel_gene"),
            F.lit(SRC_PATHOLOGY_PANEL_GENE),
            pgn.test_scope, no_rank, no_date, no_date, pgn.ADC_UPDT,
        )
    )
    l = read_source(SRC_LUNA_CODE_MAPPING)
    mappings.append(
        _concept_map_projection(
            l, l.source_coding_system, l.source_code, l.source_display,
            l.target_coding_system, l.target_code, l.target_display, l.target_domain,
            F.concat(F.lit("lookup.luna_code_map:"), l.mapping_rule_id),
            F.lit(SRC_LUNA_CODE_MAPPING),
            l.mapping_rule_id, l.mapping_rank, no_date, no_date, l.updated_at,
        )
    )
    sm = read_source(SRC_SEED_CODE_MAPPING)
    mappings.append(
        _concept_map_projection(
            sm, sm.source_coding_system, sm.source_code, sm.source_display,
            sm.target_coding_system, sm.target_code, sm.target_display, sm.target_domain,
            F.concat(F.lit("lookup.seed_code_map:"), sm.mapping_rule_id),
            F.lit(SRC_SEED_CODE_MAPPING),
            sm.mapping_rule_id, sm.mapping_rank, no_date, no_date, sm.updated_at,
            review_status_col=sm.review_status,
        )
    )
    iw = read_source(SRC_IWEB_VALUE_MAP)
    mappings.append(
        _concept_map_projection(
            iw,
            F.concat(F.lit("urn:iweb:"), F.lower(iw.SOURCE_TABLE), F.lit(":"),
                     F.lower(iw.FIELD_NAME)),
            iw.CODE, iw.LABEL,
            _iweb_target_system(iw.TARGET_VOCABULARY),
            F.coalesce(iw.TARGET_CODE, iw.TARGET_CONCEPT_ID.cast("string")),
            iw.TARGET_CONCEPT_NAME, F.lit("registry"),
            F.concat(F.lit("lookup.iweb_value_to_concept:"), iw.MAPPING_STATUS),
            F.lit(SRC_IWEB_VALUE_MAP), iw.MAPPING_METHOD, no_rank,
            no_date, no_date, iw.CURATED_AT,
            review_status_col=iw.MAPPING_STATUS,
        )
    )
    ct = read_source(SRC_CANCER_DRUG_MAP)
    mappings.append(
        _concept_map_projection(
            ct, F.lit("urn:barts:sact:drug-token"), ct.drug_token, ct.drug_token,
            F.lit("urn:omop:concept_id"), ct.drug_concept_id, ct.drug_concept_name,
            F.lit("drug"),
            F.concat(F.lit("lookup.cancer_treatment_term_map:"), ct.mapping_status),
            F.lit(SRC_CANCER_DRUG_MAP), ct.mapping_status, no_rank,
            no_date, no_date, ct.ADC_UPDT,
            review_status_col=ct.mapping_status,
        )
    )
    bg = read_source(SRC_BLOODTRACK_BLOOD_GROUP_MAP)
    mappings.append(
        _concept_map_projection(
            bg, F.lit("urn:bloodtrack:blood-group"), bg.BLOOD_GROUP_SOURCE_VALUE,
            bg.BLOOD_GROUP_SOURCE_VALUE, F.lit("http://snomed.info/sct"),
            bg.SNOMED_CONCEPT_ID, bg.SNOMED_CONCEPT_NAME, F.lit("observation"),
            F.concat(F.lit("lookup.bloodtrack_blood_group_map:"), bg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_BLOOD_GROUP_MAP), bg.MAPPING_METHOD, no_rank,
            no_date, no_date, bg.CURATED_AT,
            review_status_col=bg.MAPPING_STATUS,
        )
    )
    pg = read_source(SRC_BLOODTRACK_PRODUCT_GROUP_MAP)
    mappings.extend([
        _concept_map_projection(
            pg, F.lit("urn:bloodtrack:product-group"), pg.BLOOD_PRODUCT_GROUP,
            pg.BLOOD_PRODUCT_GROUP, F.lit("http://snomed.info/sct"),
            pg.PRODUCT_CONCEPT_ID, pg.PRODUCT_CONCEPT_NAME, F.lit("device"),
            F.concat(F.lit("lookup.bloodtrack_product_group_map:"), pg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_PRODUCT_GROUP_MAP), pg.MAPPING_METHOD, no_rank,
            no_date, no_date, pg.CURATED_AT,
            review_status_col=pg.MAPPING_STATUS,
        ),
        _concept_map_projection(
            pg, F.lit("urn:bloodtrack:product-group"), pg.BLOOD_PRODUCT_GROUP,
            pg.BLOOD_PRODUCT_GROUP, F.lit("http://snomed.info/sct"),
            pg.PROC_CONCEPT_ID, pg.PROC_CONCEPT_NAME, F.lit("procedure"),
            F.concat(F.lit("lookup.bloodtrack_product_group_map:"), pg.MAPPING_STATUS),
            F.lit(SRC_BLOODTRACK_PRODUCT_GROUP_MAP), pg.MAPPING_METHOD, no_rank,
            no_date, no_date, pg.CURATED_AT,
            review_status_col=pg.MAPPING_STATUS,
        ),
    ])
    md = read_source(SRC_MEDICONNECT_TYPE_MAP)
    mappings.append(
        _concept_map_projection(
            md, F.lit("urn:barts:mediconnect:device-type"), md.DEVICE_TYPE,
            md.SOURCE_LABEL, F.lit("http://snomed.info/sct"),
            md.SNOMED_CONCEPT_ID, md.SNOMED_CONCEPT_NAME, F.lit("device"),
            F.concat(F.lit("lookup.mediconnect_device_type_map:"), md.MAPPING_STATUS),
            F.lit(SRC_MEDICONNECT_TYPE_MAP), md.MAPPING_METHOD, no_rank,
            no_date, no_date, md.CURATED_AT,
            review_status_col=md.MAPPING_STATUS,
        )
    )
    return _union_frames(mappings)


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_concept_map():
    # Assemble concept map rows with lifecycle and source evidence for the public product and
    # its internal metadata.
    return _concept_map_projection_all()


SRC_AIR_QUALITY_SITE_MONTH = "4_prod.bronze.map_air_quality_site_month"


AQ_LOCATION = ["address_latitude", "address_longitude", "address_lat_cell", "address_lon_cell"]


AQ_SITE_LOCATION = ["site_id", "site_latitude", "site_longitude", "site_lat_cell", "site_lon_cell"]


def aq_observations(source):
    # Normalize monthly air-quality observations and monitoring-site coordinates to the expected
    # types.
    return source.select(
        F.col("site_id").cast("string").alias("site_id"),
        F.to_date("month_start").alias("month_start"),
        F.col("pollutant").cast("string").alias("pollutant"),
        F.col("measurement_value").cast("double").alias("measurement_value"),
        F.col("data_capture_pct").cast("double").alias("data_capture_pct"),
        F.col("site_latitude").cast("double").alias("site_latitude"),
        F.col("site_longitude").cast("double").alias("site_longitude"),
        "site_lat_cell", "site_lon_cell",
    ).where(reduce(lambda x, y: x & y, [F.col(c).isNotNull() for c in AQ_SITE_LOCATION + ["month_start", "pollutant", "measurement_value"]]))


# Attribute reference
def _attribute_reference(slot, namespace, id_name, entity_kind):
    # Project a configured attribute feed using its entity identifier and source attribute
    # evidence.
    s = read_source(slot)
    entity_id = s.ENTITY_ID
    # contract v2: publish native BIGINT entity ids from the shared attribute helper
    entity_ref = entity_id.cast("bigint")
    cols = [
        stable_id(namespace, s.SOURCE_PK).alias(id_name),
        entity_ref.alias(f"{entity_kind}_id"),
        entity_id.cast("string").alias(f"source_{entity_kind}_id"),
        s.ATTRIBUTE_NAME.alias("attribute_name"), s.VALUE_KIND.alias("value_kind"),
        s.VALUE_CD.cast("string").alias("value_code"), s.VALUE_DISPLAY.alias("value_display"),
        s.VALUE_CODE_SET.cast("string").alias("value_code_set"),
        s.VALUE_DT_TM_CLEAN.alias("value_datetime"), s.VALUE_NUMERIC.alias("value_numeric"),
        s.VALUE_ANSWERED_IND.alias("answered_ind"), s.ACTIVE_IND.alias("active_ind"),
        s.CURRENT_IND.alias("current_ind"),
        s.BEG_EFFECTIVE_DT_TM_CLEAN.alias("beg_effective"),
        s.END_EFFECTIVE_DT_TM_CLEAN.alias("end_effective"), s.LINK_STATUS.alias("link_status"),
        s.PIPELINE_UPDT_DT_TM.alias("loaded_at"), F.lit("millennium").alias("_source_system"),
        F.lit(slot).alias("_source_table"), s.SOURCE_PK.cast("string").alias("_source_row_id"),
    ]
    return s.select(*cols)


# Pathology, reports and specimens

# ==== Pathology, reports and specimens ====
SRC_PATHOLOGY_ACCESSION_SOURCE = "4_prod.bronze.map_pathology_accession_source"
