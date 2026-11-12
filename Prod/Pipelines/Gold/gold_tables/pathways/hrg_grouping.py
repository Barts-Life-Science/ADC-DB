"""Gold pathways.hrg_grouping: 2 dataset(s).

- datasets: _qc_clinical_hrg_grouping, clinical_hrg_grouping
- moved verbatim from: 4_gold_care_pathways:1854, 4_gold_care_pathways:1938, 4_gold_care_pathways:1947, 4_gold_care_pathways:2078, 4_gold_care_pathways:2083, 4_gold_care_pathways:2087, 4_gold_care_pathways:2109

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.hrg_grouping ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_HRG_GROUPING_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`cds_id` AS `cds_id`',
    '`source_object` AS `source_object`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    'CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND (CASE WHEN CAST(`event_datetime` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS THEN NULL ELSE `event_datetime` END) IS NULL THEN NULL ELSE `event_datetime_local` END AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`cds_record_id` AS `cds_record_id`',
    '`person_link_method` AS `person_link_method`',
    "CASE WHEN CAST(`admission_datetime` AS DATE) = DATE'1800-01-01' OR CAST(`admission_datetime` AS DATE) < DATE'1901-01-01' AND CAST(`admission_datetime` AS DATE) NOT IN (DATE'1800-01-01', DATE'1899-12-30', DATE'1900-01-01') THEN NULL ELSE `admission_datetime` END AS `admission_datetime`",
    '`discharge_datetime` AS `discharge_datetime`',
    '`episode_start_datetime` AS `episode_start_datetime`',
    'CASE WHEN `episode_start_datetime` IS NOT NULL AND `episode_end_datetime` IS NOT NULL AND `episode_start_datetime` > `episode_end_datetime` THEN NULL ELSE `episode_end_datetime` END AS `episode_end_datetime`',
    '`hosp_prov_spell_num` AS `hosp_prov_spell_num`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`episode_order` AS `episode_order`',
    '`episode_duration_days` AS `episode_duration_days`',
    '`main_specialty_cd` AS `main_specialty_cd`',
    '`treatment_function_cd` AS `treatment_function_cd`',
    '`admission_method_cd` AS `admission_method_cd`',
    '`admission_source_cd` AS `admission_source_cd`',
    '`admission_source_desc` AS `admission_source_desc`',
    '`discharge_method_cd` AS `discharge_method_cd`',
    '`discharge_dest_cd` AS `discharge_dest_cd`',
    '`discharge_dest_desc` AS `discharge_dest_desc`',
    '`patient_class_cd` AS `patient_class_cd`',
    '`patient_class_desc` AS `patient_class_desc`',
    '`source_age` AS `source_age`',
    '`source_sex_cd` AS `source_sex_cd`',
    '`neonatal_care_level_cd` AS `neonatal_care_level_cd`',
    '`critical_care_days` AS `critical_care_days`',
    '`rehab_days` AS `rehab_days`',
    '`fce_hrg_cd` AS `fce_hrg_cd`',
    '`fce_hrg_desc` AS `fce_hrg_desc`',
    '`fce_grouping_method_flag` AS `fce_grouping_method_flag`',
    '`fce_dominant_proc_cd` AS `fce_dominant_proc_cd`',
    '`fce_dominant_proc_desc` AS `fce_dominant_proc_desc`',
    '`fce_pbc_cd` AS `fce_pbc_cd`',
    '`fce_calc_episode_duration` AS `fce_calc_episode_duration`',
    '`fce_reporting_episode_duration` AS `fce_reporting_episode_duration`',
    '`dominant_episode_flag` AS `dominant_episode_flag`',
    '`spell_hrg_cd` AS `spell_hrg_cd`',
    '`spell_hrg_desc` AS `spell_hrg_desc`',
    '`spell_grouping_method_flag` AS `spell_grouping_method_flag`',
    '`spell_dominant_proc_cd` AS `spell_dominant_proc_cd`',
    '`spell_dominant_proc_desc` AS `spell_dominant_proc_desc`',
    '`spell_primary_diag_cd` AS `spell_primary_diag_cd`',
    '`spell_primary_diag_desc` AS `spell_primary_diag_desc`',
    '`spell_secondary_diag_cd` AS `spell_secondary_diag_cd`',
    '`spell_secondary_diag_desc` AS `spell_secondary_diag_desc`',
    '`spell_episode_count` AS `spell_episode_count`',
    '`spell_los` AS `spell_los`',
    '`spell_reporting_los` AS `spell_reporting_los`',
    '`spell_critical_care_days` AS `spell_critical_care_days`',
    '`spell_ssc_cd` AS `spell_ssc_cd`',
    '`spell_best_practice_cd` AS `spell_best_practice_cd`',
    '`first_attend_cd` AS `first_attend_cd`',
    '`first_attend_desc` AS `first_attend_desc`',
    '`grouping_method_flag` AS `grouping_method_flag`',
    '`dominant_proc_cd` AS `dominant_proc_cd`',
    '`dominant_proc_desc` AS `dominant_proc_desc`',
    '`grouper_errors` AS `grouper_errors`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`fact_category` AS `fact_category`',
    '`source_feed` AS `source_feed`',
    '`record_status` AS `record_status`',
    # INSR_W3 W3.05: Gold: NULL where the 1800-01-01 / 2100-12-31 sentinels or a post-2100 year below 9999 (L2282: deployed code passed it through).
    "CASE WHEN CAST(`record_status_effective_from` AS DATE) IN (DATE'1800-01-01', DATE'2100-12-31') OR (CAST(`record_status_effective_from` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`record_status_effective_from` AS DATE)) < 9999) THEN NULL ELSE `record_status_effective_from` END AS `record_status_effective_from`",
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _clinical_hrg_grouping_metadata; source history stays on the main research table.
CLINICAL_HRG_GROUPING_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 47,137,282 rows of 47,137,282
    # that are current and attributable; identity_status = 'resolved' keeps the 43,616,250
    # rows of 47,137,282 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_HRG_GROUPING_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.hrg_grouping.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 3,521,032 of 47,137,282 at the profile.
    "gold.clinical.hrg_grouping.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 47,137,282 at the profile.
    "gold.clinical.hrg_grouping.record_status.default_view_active":
        "record_status = 'active'",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 13 of 47,137,282
    # rows (2.76e-05%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.record_status_effective_from.future_owner":
        "NOT COALESCE((CAST(`record_status_effective_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 147,553 of 47,137,282 rows (0.313%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 43,005 of 47,137,282 rows (0.0912%) when profiled on 2026-08-24.
    "gold.clinical.hrg_grouping.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",

    # DQ4_G_GOLD_RULES_V1: G3. UZ01Z is the grouper's "data invalid for grouping" HRG: 101,858 of these rows
    # carry DIAG_01 "is blank", i.e. no primary diagnosis reached the grouper. The HRG is what
    # the source published, so it is flagged, not replaced. Seen on 353,777 of 47,440,327 rows
    # when profiled on 2026-09-26.
    "gold.clinical.hrg_grouping.fce_hrg_cd.uz01z_ungroupable":
        "NOT COALESCE((fce_hrg_cd = 'UZ01Z' OR spell_hrg_cd = 'UZ01Z'), FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_HRG_GROUPING_PERSON_SCOPE_RULES = {
    "gold.clinical.hrg_grouping.person_id.person_in_scope": "`_qc_person_in_scope`",
}


# INSR_W3 W3.08 (L5240, F6): orphan references counted before _qc nulls them.
CLINICAL_HRG_GROUPING_ORPHAN_RULES = {
    "gold.clinical.hrg_grouping.person_id.orphan_nulled": "NOT COALESCE(`_qc_person_id_orphan`, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_hrg_grouping"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_MANDATORY_RULES)
@dp.expect_all(CLINICAL_HRG_GROUPING_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_HRG_GROUPING_PERSON_SCOPE_RULES)
@dp.expect_all(CLINICAL_HRG_GROUPING_ORPHAN_RULES)
def _gold_qc_clinical_hrg_grouping():
    """Quality-controlled twin of journey_clinical.hrg_grouping."""
    # 5 rows point at a person_id the spine does not have. The pointer is nulled so it
    # cannot be followed to nothing, and the row is kept because it still describes
    # something in its own right.
    df = _qc(
        "clinical_hrg_grouping",
        CLINICAL_HRG_GROUPING_SELECT,
        fk_columns=["person_id"],
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.hrg_grouping"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_hrg_grouping():
    """Contract-v2 public twin of clinical_hrg_grouping; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_hrg_grouping")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`cds_id` AS `cds_id`',
        '`source_object` AS `source_object`',
        '`subject_key` AS `subject_key`',
        '`subject_id_system` AS `subject_id_system`',
        '`person_id` AS `person_id`',
        '`encounter_id` AS `encounter_id`',
        '`event_datetime` AS `event_datetime`',
        '`event_end_datetime` AS `event_end_datetime`',
        '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`event_end_datetime_local` AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
        '`source_coding_system` AS `source_coding_system`',
        '`source_code` AS `source_code`',
        '`source_display` AS `source_display`',
        '`cds_record_id` AS `cds_record_id`',
        '`person_link_method` AS `person_link_method`',
        '`admission_datetime` AS `admission_datetime`',
        '`discharge_datetime` AS `discharge_datetime`',
        '`episode_start_datetime` AS `episode_start_datetime`',
        '`episode_end_datetime` AS `episode_end_datetime`',
        '`hosp_prov_spell_num` AS `hosp_prov_spell_num`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`episode_order` AS `episode_order`',
        '`episode_duration_days` AS `episode_duration_days`',
        '`main_specialty_cd` AS `main_specialty_cd`',
        '`treatment_function_cd` AS `treatment_function_cd`',
        '`admission_method_cd` AS `admission_method_cd`',
        '`admission_source_cd` AS `admission_source_cd`',
        '`admission_source_desc` AS `admission_source_desc`',
        '`discharge_method_cd` AS `discharge_method_cd`',
        '`discharge_dest_cd` AS `discharge_dest_cd`',
        '`discharge_dest_desc` AS `discharge_dest_desc`',
        '`patient_class_cd` AS `patient_class_cd`',
        '`patient_class_desc` AS `patient_class_desc`',
        '`source_age` AS `source_age`',
        '`source_sex_cd` AS `source_sex_cd`',
        '`neonatal_care_level_cd` AS `neonatal_care_level_cd`',
        '`critical_care_days` AS `critical_care_days`',
        '`rehab_days` AS `rehab_days`',
        '`fce_hrg_cd` AS `fce_hrg_cd`',
        '`fce_hrg_desc` AS `fce_hrg_desc`',
        '`fce_grouping_method_flag` AS `fce_grouping_method_flag`',
        '`fce_dominant_proc_cd` AS `fce_dominant_proc_cd`',
        '`fce_dominant_proc_desc` AS `fce_dominant_proc_desc`',
        '`fce_pbc_cd` AS `fce_pbc_cd`',
        '`fce_calc_episode_duration` AS `fce_calc_episode_duration`',
        '`fce_reporting_episode_duration` AS `fce_reporting_episode_duration`',
        '`dominant_episode_flag` AS `dominant_episode_flag`',
        '`spell_hrg_cd` AS `spell_hrg_cd`',
        '`spell_hrg_desc` AS `spell_hrg_desc`',
        '`spell_grouping_method_flag` AS `spell_grouping_method_flag`',
        '`spell_dominant_proc_cd` AS `spell_dominant_proc_cd`',
        '`spell_dominant_proc_desc` AS `spell_dominant_proc_desc`',
        '`spell_primary_diag_cd` AS `spell_primary_diag_cd`',
        '`spell_primary_diag_desc` AS `spell_primary_diag_desc`',
        '`spell_secondary_diag_cd` AS `spell_secondary_diag_cd`',
        '`spell_secondary_diag_desc` AS `spell_secondary_diag_desc`',
        '`spell_episode_count` AS `spell_episode_count`',
        '`spell_los` AS `spell_los`',
        '`spell_reporting_los` AS `spell_reporting_los`',
        '`spell_critical_care_days` AS `spell_critical_care_days`',
        '`spell_ssc_cd` AS `spell_ssc_cd`',
        '`spell_best_practice_cd` AS `spell_best_practice_cd`',
        '`first_attend_cd` AS `first_attend_cd`',
        '`first_attend_desc` AS `first_attend_desc`',
        '`grouping_method_flag` AS `grouping_method_flag`',
        '`dominant_proc_cd` AS `dominant_proc_cd`',
        '`dominant_proc_desc` AS `dominant_proc_desc`',
        '`grouper_errors` AS `grouper_errors`',
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
    return df


RULES = {
    'clinical_hrg_grouping': GoldRules(
        table='clinical_hrg_grouping',
        select=CLINICAL_HRG_GROUPING_SELECT,
        mandatory=CLINICAL_HRG_GROUPING_MANDATORY_RULES,
        advisory=CLINICAL_HRG_GROUPING_ADVISORY_RULES,
        person_scope=CLINICAL_HRG_GROUPING_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d', 'event_before_birth')),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_hrg_grouping': {'columns': {'admission_datetime': 'Date and time the patient was admitted for '
                                                                 'the hospital provider spell.',
                                           'admission_method_cd': 'NHS admission method code for the '
                                                                  'hospital provider spell.',
                                           'admission_source_cd': 'NHS source of admission code for the '
                                                                  'hospital provider spell.',
                                           'admission_source_desc': 'Description of the source of admission '
                                                                    'code, mapped from the CDS admission '
                                                                    'source lookup.',
                                           'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID; primary key within '
                                                     'source_object.',
                                           'cds_record_id': 'Trimmed CDS admitted-patient-care episode '
                                                            'identifier; unique key; joins back to raw.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each HRG '
                                                                   'grouping record. It is produced by the '
                                                                   'silver transformation and has no direct '
                                                                   'bronze-column lineage entry. Source code '
                                                                   'meanings and sentinel values are '
                                                                   'retained unless the pipeline explicitly '
                                                                   'maps them; null means no code was '
                                                                   'supplied.',
                                           'critical_care_days': 'Number of critical care days for the '
                                                                 'episode as submitted to the grouper.',
                                           'discharge_datetime': 'Date and time the patient was discharged '
                                                                 'from the hospital provider spell.',
                                           'discharge_dest_cd': 'NHS discharge destination code for the '
                                                                'hospital provider spell.',
                                           'discharge_dest_desc': 'Description of the discharge destination '
                                                                  'code, mapped from the CDS discharge '
                                                                  'destination lookup.',
                                           'discharge_method_cd': 'NHS discharge method code for the '
                                                                  'hospital provider spell.',
                                           'dominant_episode_flag': 'Grouper flag indicating whether this '
                                                                    'episode is the dominant episode used to '
                                                                    'derive the spell HRG.',
                                           'dominant_proc_cd': 'OPCS-4 code of the dominant procedure '
                                                               'determined by the grouper for the '
                                                               'attendance.',
                                           'dominant_proc_desc': 'Description of DOMINANT_PROC_CD obtained '
                                                                 'from the lookup 3_lookup.dwh.opcs_410.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'episode_duration_days': 'Length of the episode in days as '
                                                                    'submitted to the grouper.',
                                           'episode_end_datetime': 'Date and time the finished consultant '
                                                                   'episode ended.',
                                           'episode_order': 'Sequence number of this episode within the '
                                                            'hospital provider spell.',
                                           'episode_start_datetime': 'Date and time the finished consultant '
                                                                     'episode started.',
                                           'event_datetime': 'Date and time the finished consultant episode '
                                                             'started.',
                                           'event_datetime_local': 'Local-time representation of the start '
                                                                   'date and time of the HRG-grouped '
                                                                   'activity event. Gold: NULL wherever Gold '
                                                                   'nulls event_datetime.',
                                           'event_end_datetime': 'Date and time the finished consultant '
                                                                 'episode ended.',
                                           'event_end_datetime_local': 'Local-time representation of the end '
                                                                       'date and time of the HRG-grouped '
                                                                       'activity event, where available. '
                                                                       'Gold: NULL wherever Gold nulls '
                                                                       'event_end_datetime.',
                                           'fact_category': 'Contract category used to group the record with '
                                                            'comparable clinical facts for each HRG grouping '
                                                            'record. It is produced by the silver '
                                                            'transformation and has no direct bronze-column '
                                                            'lineage entry. Whitespace and source sentinel '
                                                            'text are retained unless the pipeline '
                                                            'explicitly normalizes them; null means no value '
                                                            'was supplied.',
                                           'fce_calc_episode_duration': 'Grouper-calculated episode duration '
                                                                        'in days for this finished '
                                                                        'consultant episode.',
                                           'fce_dominant_proc_cd': 'OPCS-4 procedure code identified by the '
                                                                   'HRG4+ grouper as the dominant procedure '
                                                                   'for this episode; null where no '
                                                                   'procedure drove the grouping.',
                                           'fce_dominant_proc_desc': 'Description of the episode dominant '
                                                                     'procedure code, mapped from the '
                                                                     'OPCS-4.10 reference lookup.',
                                           'fce_grouping_method_flag': 'Grouper-returned flag indicating the '
                                                                       'method by which the HRG4+ code was '
                                                                       'derived for this finished consultant '
                                                                       'episode.',
                                           'fce_hrg_cd': 'HRG4+ code assigned to this finished consultant '
                                                         'episode.',
                                           'fce_hrg_desc': 'Description of the episode-level HRG4+ code, '
                                                           'mapped from the HRG v4 reference lookup.',
                                           'fce_pbc_cd': 'Programme Budgeting Category code assigned by the '
                                                         'HRG4+ grouper to this episode.',
                                           'fce_reporting_episode_duration': 'Episode duration in days as '
                                                                             'used for HRG reporting '
                                                                             'purposes by the grouper.',
                                           'first_attend_cd': 'CDS first attendance code indicating whether '
                                                              'the attendance was a first or follow-up '
                                                              'contact and its modality.',
                                           'first_attend_desc': 'Description of FIRST_ATTEND_CD obtained '
                                                                'from the lookup '
                                                                '3_lookup.dwh.cds_first_attend.',
                                           'grouper_errors': 'Grouper error/quality messages.',
                                           'grouping_method_flag': 'HRG4+ grouper flag indicating the method '
                                                                   'by which the HRG was derived for the '
                                                                   'attendance.',
                                           'hosp_prov_spell_num': 'Hospital provider spell number grouping '
                                                                  'episodes into spells.',
                                           'loaded_at': 'ADC_UPDT carried unchanged from the contributing '
                                                        'map_slam_apc_hrg or map_slam_op_hrg row through the '
                                                        'common HRG stage. This is per-row bronze ingestion '
                                                        'provenance, not episode/attendance time or Silver '
                                                        'refresh time; missing values remain null.',
                                           'main_specialty_cd': 'NHS main specialty code recorded for the '
                                                                'episode.',
                                           'neonatal_care_level_cd': 'Coded level of neonatal care recorded '
                                                                     'for the episode as submitted to the '
                                                                     'grouper.',
                                           'patient_class_cd': 'CDS patient classification code for the '
                                                               'episode, such as ordinary or day case '
                                                               'admission.',
                                           'patient_class_desc': 'Description of the patient classification '
                                                                 'code, mapped from the CDS patient class '
                                                                 'lookup.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'person_link_method': 'How PERSON_ID was resolved: MRN_ALIAS, or '
                                                                 'null when unlinked.',
                                           'provider_org_cd': 'NHS organisation (provider) code identifying '
                                                              'the submitting healthcare provider for the '
                                                              'episode.',
                                           'record_status': 'For both SLAM APC and outpatient HRG arms, '
                                                            "retracted when the arm's SOURCE_PRESENT_IND is "
                                                            'false, otherwise active; a missing presence '
                                                            'flag defaults true. Neither arm emits '
                                                            'superseded. This is a source-row presence '
                                                            'status, not an HRG grouper result or clinical '
                                                            'episode state.',
                                           'record_status_effective_from': 'map_slam_apc_hrg.EPISODE_START_DT_TM '
                                                                           'for the APC arm, or '
                                                                           'map_slam_op_hrg.ATTENDANCE_DT_TM '
                                                                           'for the outpatient arm, cast to '
                                                                           'TIMESTAMP. Unlike '
                                                                           'event_datetime, this history '
                                                                           'field is not range-clamped to '
                                                                           '[1950,2100). It is an event-time '
                                                                           'proxy rather than an '
                                                                           'independently observed '
                                                                           'row-status transition; missing '
                                                                           'values remain null.',
                                           'record_status_effective_to': "The relevant SLAM arm's "
                                                                         'SOURCE_ABSENT_DETECTED_TS only '
                                                                         'when that row is retracted; '
                                                                         'otherwise null. A missing '
                                                                         'absence-detection time remains '
                                                                         'null: episode end, discharge and '
                                                                         'ADC_UPDT are not substituted.',
                                           'rehab_days': 'Number of rehabilitation days for the episode as '
                                                         'submitted to the grouper.',
                                           'source_age': 'NON-CANONICAL grouper input age as submitted to '
                                                         'SLAM; demographics of record are in map_person.',
                                           'source_code': 'HRG4+ code assigned to this finished consultant '
                                                          'episode.',
                                           'source_coding_system': 'HRG4+ code assigned to this finished '
                                                                   'consultant episode.',
                                           'source_display': 'Description of the episode-level HRG4+ code, '
                                                             'mapped from the HRG v4 reference lookup.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each HRG grouping record. It is produced by '
                                                          'the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_object': 'SLAM source arm (slam_apc_hrg or slam_op_hrg); '
                                                            'part of the primary key with cds_id.',
                                           'source_sex_cd': 'NON-CANONICAL grouper input sex code as '
                                                            'submitted to SLAM; demographics of record are '
                                                            'in map_person.',
                                           'source_update_timestamp': 'SOURCE_RECORD_UPDATED_DT carried '
                                                                      'unchanged from the contributing '
                                                                      'map_slam_apc_hrg or map_slam_op_hrg '
                                                                      'row. It is separate from ADC_UPDT '
                                                                      'ingestion provenance; the union does '
                                                                      'not aggregate clocks across arms and '
                                                                      'Silver refresh time is not '
                                                                      'substituted.',
                                           'spell_best_practice_cd': 'Best practice tariff code assigned by '
                                                                     'the HRG4+ grouper at hospital provider '
                                                                     'spell level, null where no best '
                                                                     'practice classification applies.',
                                           'spell_critical_care_days': 'Number of critical care days '
                                                                       'recorded for the hospital provider '
                                                                       'spell as submitted to the grouper.',
                                           'spell_dominant_proc_cd': 'OPCS-4 procedure code identified by '
                                                                     'the HRG4+ grouper as the dominant '
                                                                     'procedure for the hospital provider '
                                                                     'spell.',
                                           'spell_dominant_proc_desc': 'Description of the spell dominant '
                                                                       'procedure code, mapped from the '
                                                                       'OPCS-4.10 reference lookup.',
                                           'spell_episode_count': 'Number of finished consultant episodes '
                                                                  'counted by the grouper within the '
                                                                  'hospital provider spell.',
                                           'spell_grouping_method_flag': 'Grouper-returned flag indicating '
                                                                         'the method by which the '
                                                                         'spell-level HRG4+ code was '
                                                                         'derived.',
                                           'spell_hrg_cd': 'HRG4+ code assigned at hospital-spell level '
                                                           '(repeated on every episode of the spell).',
                                           'spell_hrg_desc': 'Description of the spell-level HRG4+ code, '
                                                             'mapped from the HRG v4 reference lookup.',
                                           'spell_los': 'Grouper-calculated length of stay in days for the '
                                                        'hospital provider spell.',
                                           'spell_primary_diag_cd': 'ICD-10 primary diagnosis code used by '
                                                                    'the grouper at hospital provider spell '
                                                                    'level.',
                                           'spell_primary_diag_desc': 'Description of the spell primary '
                                                                      'diagnosis code, mapped from the ICD '
                                                                      'diagnosis reference lookup.',
                                           'spell_reporting_los': 'Spell length of stay in days as used for '
                                                                  'HRG reporting purposes by the grouper.',
                                           'spell_secondary_diag_cd': 'ICD-10 secondary diagnosis code used '
                                                                      'by the grouper at hospital provider '
                                                                      'spell level.',
                                           'spell_secondary_diag_desc': 'Description of the spell secondary '
                                                                        'diagnosis code, mapped from the ICD '
                                                                        'diagnosis reference lookup.',
                                           'spell_ssc_cd': 'Specialised service code assigned by the HRG4+ '
                                                           'grouper at hospital provider spell level.',
                                           'subject_id_system': 'Millennium PERSON_ID resolved from source '
                                                                'MRN via mill_person_alias type 10 '
                                                                '(deterministic tiebreak); null when '
                                                                'unresolved — source MRN is retained only in '
                                                                'raw.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'treatment_function_cd': 'NHS Treatment Function Code (national '
                                                                    'standard).',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each HRG grouping record. It is produced '
                                                      'by the silver transformation and has no direct '
                                                      'bronze-column lineage entry. Null means the source '
                                                      'did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'withheld_identity_ind': 'Indicator that the source identity was '
                                                                    'deliberately withheld for each HRG '
                                                                    'grouping record. It is produced by the '
                                                                    'silver transformation and has no direct '
                                                                    'bronze-column lineage entry. Null means '
                                                                    'the source did not state the indicator '
                                                                    'and must not be interpreted as false.'},
                               'quantity_override': True,
                               'table': 'Internal quality-controlled twin of clinical_hrg_grouping: the Gold '
                                        'repairs, nulls and expectations are applied here and the published '
                                        'Gold MV reads this object. Materialized rather than temporary so '
                                        'the published MV can refresh incrementally.'},
 'clinical_hrg_grouping': {'columns': {'admission_datetime': 'Date and time the patient was admitted for the '
                                                             'hospital provider spell.',
                                       'admission_method_cd': 'NHS admission method code for the hospital '
                                                              'provider spell.',
                                       'admission_source_cd': 'NHS source of admission code for the hospital '
                                                              'provider spell.',
                                       'admission_source_desc': 'Description of the source of admission '
                                                                'code, mapped from the CDS admission source '
                                                                'lookup.',
                                       'cds_id': 'Native CDS_APC_ID or CDS_OPA_ID; primary key within '
                                                 'source_object.',
                                       'cds_record_id': 'Trimmed CDS admitted-patient-care episode '
                                                        'identifier; unique key; joins back to raw.',
                                       'confidentiality_code': 'Source confidentiality classification '
                                                               'attached to the record for each HRG grouping '
                                                               'record. It is produced by the silver '
                                                               'transformation and has no direct '
                                                               'bronze-column lineage entry. Source code '
                                                               'meanings and sentinel values are retained '
                                                               'unless the pipeline explicitly maps them; '
                                                               'null means no code was supplied.',
                                       'critical_care_days': 'Number of critical care days for the episode '
                                                             'as submitted to the grouper.',
                                       'discharge_datetime': 'Date and time the patient was discharged from '
                                                             'the hospital provider spell.',
                                       'discharge_dest_cd': 'NHS discharge destination code for the hospital '
                                                            'provider spell.',
                                       'discharge_dest_desc': 'Description of the discharge destination '
                                                              'code, mapped from the CDS discharge '
                                                              'destination lookup.',
                                       'discharge_method_cd': 'NHS discharge method code for the hospital '
                                                              'provider spell.',
                                       'dominant_episode_flag': 'Grouper flag indicating whether this '
                                                                'episode is the dominant episode used to '
                                                                'derive the spell HRG.',
                                       'dominant_proc_cd': 'OPCS-4 code of the dominant procedure determined '
                                                           'by the grouper for the attendance.',
                                       'dominant_proc_desc': 'Description of DOMINANT_PROC_CD obtained from '
                                                             'the lookup 3_lookup.dwh.opcs_410.',
                                       'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                       'available.',
                                       'episode_duration_days': 'Length of the episode in days as submitted '
                                                                'to the grouper.',
                                       'episode_end_datetime': 'Date and time the finished consultant '
                                                               'episode ended.',
                                       'episode_order': 'Sequence number of this episode within the hospital '
                                                        'provider spell.',
                                       'episode_start_datetime': 'Date and time the finished consultant '
                                                                 'episode started.',
                                       'event_datetime': 'Date and time the finished consultant episode '
                                                         'started.',
                                       'event_datetime_local': 'Local-time representation of the start date '
                                                               'and time of the HRG-grouped activity event. '
                                                               'Gold: NULL wherever Gold nulls '
                                                               'event_datetime.',
                                       'event_end_datetime': 'Date and time the finished consultant episode '
                                                             'ended.',
                                       'event_end_datetime_local': 'Local-time representation of the end '
                                                                   'date and time of the HRG-grouped '
                                                                   'activity event, where available. Gold: '
                                                                   'NULL wherever Gold nulls '
                                                                   'event_end_datetime.',
                                       'fact_category': 'Contract category used to group the record with '
                                                        'comparable clinical facts for each HRG grouping '
                                                        'record. It is produced by the silver transformation '
                                                        'and has no direct bronze-column lineage entry. '
                                                        'Whitespace and source sentinel text are retained '
                                                        'unless the pipeline explicitly normalizes them; '
                                                        'null means no value was supplied.',
                                       'fce_calc_episode_duration': 'Grouper-calculated episode duration in '
                                                                    'days for this finished consultant '
                                                                    'episode.',
                                       'fce_dominant_proc_cd': 'OPCS-4 procedure code identified by the '
                                                               'HRG4+ grouper as the dominant procedure for '
                                                               'this episode; null where no procedure drove '
                                                               'the grouping.',
                                       'fce_dominant_proc_desc': 'Description of the episode dominant '
                                                                 'procedure code, mapped from the OPCS-4.10 '
                                                                 'reference lookup.',
                                       'fce_grouping_method_flag': 'Grouper-returned flag indicating the '
                                                                   'method by which the HRG4+ code was '
                                                                   'derived for this finished consultant '
                                                                   'episode.',
                                       'fce_hrg_cd': 'HRG4+ code assigned to this finished consultant '
                                                     'episode.',
                                       'fce_hrg_desc': 'Description of the episode-level HRG4+ code, mapped '
                                                       'from the HRG v4 reference lookup.',
                                       'fce_pbc_cd': 'Programme Budgeting Category code assigned by the '
                                                     'HRG4+ grouper to this episode.',
                                       'fce_reporting_episode_duration': 'Episode duration in days as used '
                                                                         'for HRG reporting purposes by the '
                                                                         'grouper.',
                                       'first_attend_cd': 'CDS first attendance code indicating whether the '
                                                          'attendance was a first or follow-up contact and '
                                                          'its modality.',
                                       'first_attend_desc': 'Description of FIRST_ATTEND_CD obtained from '
                                                            'the lookup 3_lookup.dwh.cds_first_attend.',
                                       'grouper_errors': 'Grouper error/quality messages.',
                                       'grouping_method_flag': 'HRG4+ grouper flag indicating the method by '
                                                               'which the HRG was derived for the '
                                                               'attendance.',
                                       'hosp_prov_spell_num': 'Hospital provider spell number grouping '
                                                              'episodes into spells.',
                                       'loaded_at': 'ADC_UPDT carried unchanged from the contributing '
                                                    'map_slam_apc_hrg or map_slam_op_hrg row through the '
                                                    'common HRG stage. This is per-row bronze ingestion '
                                                    'provenance, not episode/attendance time or Silver '
                                                    'refresh time; missing values remain null.',
                                       'main_specialty_cd': 'NHS main specialty code recorded for the '
                                                            'episode.',
                                       'neonatal_care_level_cd': 'Coded level of neonatal care recorded for '
                                                                 'the episode as submitted to the grouper.',
                                       'patient_class_cd': 'CDS patient classification code for the episode, '
                                                           'such as ordinary or day case admission.',
                                       'patient_class_desc': 'Description of the patient classification '
                                                             'code, mapped from the CDS patient class '
                                                             'lookup.',
                                       'patient_event_key': 'Deterministic event key derived from source '
                                                            'identifiers; retained as a stable join key.',
                                       'person_id': 'Native Millennium PERSON_ID as BIGINT when resolved.',
                                       'person_link_method': 'How PERSON_ID was resolved: MRN_ALIAS, or null '
                                                             'when unlinked.',
                                       'provider_org_cd': 'NHS organisation (provider) code identifying the '
                                                          'submitting healthcare provider for the episode.',
                                       'record_status': 'For both SLAM APC and outpatient HRG arms, '
                                                        "retracted when the arm's SOURCE_PRESENT_IND is "
                                                        'false, otherwise active; a missing presence flag '
                                                        'defaults true. Neither arm emits superseded. This '
                                                        'is a source-row presence status, not an HRG grouper '
                                                        'result or clinical episode state.',
                                       'record_status_effective_from': 'map_slam_apc_hrg.EPISODE_START_DT_TM '
                                                                       'for the APC arm, or '
                                                                       'map_slam_op_hrg.ATTENDANCE_DT_TM for '
                                                                       'the outpatient arm, cast to '
                                                                       'TIMESTAMP. Unlike event_datetime, '
                                                                       'this history field is not '
                                                                       'range-clamped to [1950,2100). It is '
                                                                       'an event-time proxy rather than an '
                                                                       'independently observed row-status '
                                                                       'transition; missing values remain '
                                                                       'null.',
                                       'record_status_effective_to': "The relevant SLAM arm's "
                                                                     'SOURCE_ABSENT_DETECTED_TS only when '
                                                                     'that row is retracted; otherwise null. '
                                                                     'A missing absence-detection time '
                                                                     'remains null: episode end, discharge '
                                                                     'and ADC_UPDT are not substituted.',
                                       'rehab_days': 'Number of rehabilitation days for the episode as '
                                                     'submitted to the grouper.',
                                       'source_age': 'NON-CANONICAL grouper input age as submitted to SLAM; '
                                                     'demographics of record are in map_person.',
                                       'source_code': 'HRG4+ code assigned to this finished consultant '
                                                      'episode.',
                                       'source_coding_system': 'HRG4+ code assigned to this finished '
                                                               'consultant episode.',
                                       'source_display': 'Description of the episode-level HRG4+ code, '
                                                         'mapped from the HRG v4 reference lookup.',
                                       'source_feed': 'Originating data feed responsible for the record for '
                                                      'each HRG grouping record. It is produced by the '
                                                      'silver transformation and has no direct bronze-column '
                                                      'lineage entry. Whitespace and source sentinel text '
                                                      'are retained unless the pipeline explicitly '
                                                      'normalizes them; null means no value was supplied.',
                                       'source_object': 'SLAM source arm (slam_apc_hrg or slam_op_hrg); part '
                                                        'of the primary key with cds_id.',
                                       'source_sex_cd': 'NON-CANONICAL grouper input sex code as submitted '
                                                        'to SLAM; demographics of record are in map_person.',
                                       'source_update_timestamp': 'SOURCE_RECORD_UPDATED_DT carried '
                                                                  'unchanged from the contributing '
                                                                  'map_slam_apc_hrg or map_slam_op_hrg row. '
                                                                  'It is separate from ADC_UPDT ingestion '
                                                                  'provenance; the union does not aggregate '
                                                                  'clocks across arms and Silver refresh '
                                                                  'time is not substituted.',
                                       'spell_best_practice_cd': 'Best practice tariff code assigned by the '
                                                                 'HRG4+ grouper at hospital provider spell '
                                                                 'level, null where no best practice '
                                                                 'classification applies.',
                                       'spell_critical_care_days': 'Number of critical care days recorded '
                                                                   'for the hospital provider spell as '
                                                                   'submitted to the grouper.',
                                       'spell_dominant_proc_cd': 'OPCS-4 procedure code identified by the '
                                                                 'HRG4+ grouper as the dominant procedure '
                                                                 'for the hospital provider spell.',
                                       'spell_dominant_proc_desc': 'Description of the spell dominant '
                                                                   'procedure code, mapped from the '
                                                                   'OPCS-4.10 reference lookup.',
                                       'spell_episode_count': 'Number of finished consultant episodes '
                                                              'counted by the grouper within the hospital '
                                                              'provider spell.',
                                       'spell_grouping_method_flag': 'Grouper-returned flag indicating the '
                                                                     'method by which the spell-level HRG4+ '
                                                                     'code was derived.',
                                       'spell_hrg_cd': 'HRG4+ code assigned at hospital-spell level '
                                                       '(repeated on every episode of the spell).',
                                       'spell_hrg_desc': 'Description of the spell-level HRG4+ code, mapped '
                                                         'from the HRG v4 reference lookup.',
                                       'spell_los': 'Grouper-calculated length of stay in days for the '
                                                    'hospital provider spell.',
                                       'spell_primary_diag_cd': 'ICD-10 primary diagnosis code used by the '
                                                                'grouper at hospital provider spell level.',
                                       'spell_primary_diag_desc': 'Description of the spell primary '
                                                                  'diagnosis code, mapped from the ICD '
                                                                  'diagnosis reference lookup.',
                                       'spell_reporting_los': 'Spell length of stay in days as used for HRG '
                                                              'reporting purposes by the grouper.',
                                       'spell_secondary_diag_cd': 'ICD-10 secondary diagnosis code used by '
                                                                  'the grouper at hospital provider spell '
                                                                  'level.',
                                       'spell_secondary_diag_desc': 'Description of the spell secondary '
                                                                    'diagnosis code, mapped from the ICD '
                                                                    'diagnosis reference lookup.',
                                       'spell_ssc_cd': 'Specialised service code assigned by the HRG4+ '
                                                       'grouper at hospital provider spell level.',
                                       'subject_id_system': 'Millennium PERSON_ID resolved from source MRN '
                                                            'via mill_person_alias type 10 (deterministic '
                                                            'tiebreak); null when unresolved — source MRN is '
                                                            'retained only in raw.',
                                       'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                      'identifier. Not salted, not secret; a stable join key '
                                                      'across feeds where person_id is unresolved.',
                                       'treatment_function_cd': 'NHS Treatment Function Code (national '
                                                                'standard).',
                                       'vip_ind': 'Indicator that the source record carries VIP handling '
                                                  'status for each HRG grouping record. It is produced by '
                                                  'the silver transformation and has no direct bronze-column '
                                                  'lineage entry. Null means the source did not state the '
                                                  'indicator and must not be interpreted as false.',
                                       'withheld_identity_ind': 'Indicator that the source identity was '
                                                                'deliberately withheld for each HRG grouping '
                                                                'record. It is produced by the silver '
                                                                'transformation and has no direct '
                                                                'bronze-column lineage entry. Null means the '
                                                                'source did not state the indicator and must '
                                                                'not be interpreted as false.'},
                           'quantity_override': False,
                           'table': 'One SLAM APC or outpatient HRG grouping row. Gold QC twin of the silver '
                                    'product: 6 columns are repaired or nulled, 1 rule(s) drop rows, 5 '
                                    'check(s) are advisory. Each rule states its reason in the pipeline '
                                    'notebook, and Lakeflow expectation metrics report what every rule '
                                    'matched on each update. Coverage: The most recent two to three months '
                                    "are incomplete while clinical coding catches up; each table's "
                                    'complete-through month says where the data is complete; '
                                    'complete-through month per release: 6_mgmt.data_quality.release_finding '
                                    '(finding_code F2_complete_through).'}}
