"""Gold pathways.drug_expenditure: 2 dataset(s).

- datasets: _qc_clinical_drug_expenditure, clinical_drug_expenditure
- moved verbatim from: 4_gold_care_pathways:1139, 4_gold_care_pathways:1236, 4_gold_care_pathways:1245, 4_gold_care_pathways:1379, 4_gold_care_pathways:1383, 4_gold_care_pathways:1400

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.drug_expenditure ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_DRUG_EXPENDITURE_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`source_row_hash` AS `source_row_hash`',
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
    '`transaction_id` AS `transaction_id`',
    '`financial_year` AS `financial_year`',
    '`financial_month` AS `financial_month`',
    '`reporting_year` AS `reporting_year`',
    '`reporting_month` AS `reporting_month`',
    '`provider_org_cd` AS `provider_org_cd`',
    '`site_cd` AS `site_cd`',
    '`site_name` AS `site_name`',
    '`specialty_cd` AS `specialty_cd`',
    '`consultant_cd` AS `consultant_cd`',
    '`patient_type` AS `patient_type`',
    '`pod_cd` AS `pod_cd`',
    '`chargeable_item` AS `chargeable_item`',
    '`additional_info` AS `additional_info`',
    '`dmd_raw` AS `dmd_raw`',
    '`dmd_code` AS `dmd_code`',
    '`dmd_concept_id` AS `dmd_concept_id`',
    '`dmd_concept_name` AS `dmd_concept_name`',
    '`drug_standard_concept_id` AS `drug_standard_concept_id`',
    '`drug_standard_concept_name` AS `drug_standard_concept_name`',
    '`dmd_mapping_status` AS `dmd_mapping_status`',
    '`dmd_taxonomy_cd` AS `dmd_taxonomy_cd`',
    '`route_of_administration` AS `route_of_administration`',
    '`strength` AS `strength`',
    '`volume` AS `volume`',
    '`pack_size` AS `pack_size`',
    'CASE WHEN `quantity` < 0 THEN NULL ELSE `quantity` END AS `quantity`',
    '`unit_of_measure` AS `unit_of_measure`',
    '`dispensing_route` AS `dispensing_route`',
    '`dispensing_location` AS `dispensing_location`',
    '`indication` AS `indication`',
    '`funding_reference` AS `funding_reference`',
    '`hcdr_category_cd` AS `hcdr_category_cd`',
    '`hcdr_category_desc` AS `hcdr_category_desc`',
    '`ccg_residence_cd` AS `ccg_residence_cd`',
    '`ccg_gp_cd` AS `ccg_gp_cd`',
    '`commissioner_cd` AS `commissioner_cd`',
    '`commissioner_type` AS `commissioner_type`',
    '`service_line` AS `service_line`',
    '`service_category_cd` AS `service_category_cd`',
    '`unit_price_supplier` AS `unit_price_supplier`',
    '`unit_price_commissioner` AS `unit_price_commissioner`',
    '`vat` AS `vat`',
    '`vat_cd` AS `vat_cd`',
    '`income` AS `income`',
    '`cost` AS `cost`',
    '`margin` AS `margin`',
    '`lloyds_dispensing_fee` AS `lloyds_dispensing_fee`',
    '`production_fee` AS `production_fee`',
    '`fixed_patient_income` AS `fixed_patient_income`',
    '`cost_centre_desc` AS `cost_centre_desc`',
    '`drug_feed` AS `drug_feed`',
    '`data_set` AS `data_set`',
    '`drug_category` AS `drug_category`',
    '`ledger_cd` AS `ledger_cd`',
    '`exclusion_flag` AS `exclusion_flag`',
    '`exclusion_reason` AS `exclusion_reason`',
    '`ledger_lv3_cd` AS `ledger_lv3_cd`',
    '`ledger_lv3_desc` AS `ledger_lv3_desc`',
    '`ledger_lv6_cd` AS `ledger_lv6_cd`',
    '`ledger_lv6_desc` AS `ledger_lv6_desc`',
    '`ledger_lv7_cd` AS `ledger_lv7_cd`',
    '`ledger_lv7_desc` AS `ledger_lv7_desc`',
    '`ledger_lv9_cd` AS `ledger_lv9_cd`',
    '`ledger_lv9_desc` AS `ledger_lv9_desc`',
    '`slr_cd` AS `slr_cd`',
    '`diabetic_flag` AS `diabetic_flag`',
    '`imcoe_flag` AS `imcoe_flag`',
    '`source_duplicate_count` AS `source_duplicate_count`',
    '`person_link_method` AS `person_link_method`',
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


# contract v2: QC/batch inputs come from internal _clinical_drug_expenditure_metadata; source history stays on the main research table.
CLINICAL_DRUG_EXPENDITURE_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 1,167,749 rows of 1,167,749
    # that are current and attributable; identity_status = 'resolved' keeps the 1,167,731
    # rows of 1,167,749 that are current and attributable. Superseded versions and rows
    # whose identity was never resolved are not research data, and a consumer who wants them
    # has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_DRUG_EXPENDITURE_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.drug_expenditure.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 W4.06 cross_field (estate 2026-10-29): An excluded HCD line states why (L2565).
    'gold.clinical.drug_expenditure.table.insr_f19_exclusion_without_reason':
        "NOT COALESCE((`exclusion_flag` = 1 AND NULLIF(TRIM(`exclusion_reason`), '') IS NULL), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 18 of 1,167,749 at the profile.
    "gold.clinical.drug_expenditure.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 1,167,749 at the profile.
    "gold.clinical.drug_expenditure.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 3,486 of 1,167,749 rows (0.299%) when profiled on 2026-08-24.
    "gold.clinical.drug_expenditure.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 12 of 1,167,749 rows (0.00103%) when profiled on 2026-08-24.
    "gold.clinical.drug_expenditure.table.before_birth_event_datetime_person_birth_datetime":
        "NOT COALESCE(event_before_birth, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_DRUG_EXPENDITURE_PERSON_SCOPE_RULES = {
    "gold.clinical.drug_expenditure.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_drug_expenditure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_DRUG_EXPENDITURE_MANDATORY_RULES)
@dp.expect_all(CLINICAL_DRUG_EXPENDITURE_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_DRUG_EXPENDITURE_PERSON_SCOPE_RULES)
def _gold_qc_clinical_drug_expenditure():
    """Quality-controlled twin of journey_clinical.drug_expenditure."""
    df = _qc(
        "clinical_drug_expenditure",
        CLINICAL_DRUG_EXPENDITURE_SELECT,
        date_flags=["event_after_death_30d", "event_before_birth"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.drug_expenditure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_drug_expenditure():
    """Contract-v2 public twin of clinical_drug_expenditure; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_drug_expenditure")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`source_row_hash` AS `source_row_hash`',
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
        '`transaction_id` AS `transaction_id`',
        '`financial_year` AS `financial_year`',
        '`financial_month` AS `financial_month`',
        '`reporting_year` AS `reporting_year`',
        '`reporting_month` AS `reporting_month`',
        '`provider_org_cd` AS `provider_org_cd`',
        '`site_cd` AS `site_cd`',
        '`site_name` AS `site_name`',
        '`specialty_cd` AS `specialty_cd`',
        '`consultant_cd` AS `consultant_cd`',
        '`patient_type` AS `patient_type`',
        '`pod_cd` AS `pod_cd`',
        '`chargeable_item` AS `chargeable_item`',
        '`additional_info` AS `additional_info`',
        '`dmd_raw` AS `dmd_raw`',
        '`dmd_code` AS `dmd_code`',
        '`dmd_concept_id` AS `dmd_concept_id`',
        '`dmd_concept_name` AS `dmd_concept_name`',
        '`drug_standard_concept_id` AS `drug_standard_concept_id`',
        '`drug_standard_concept_name` AS `drug_standard_concept_name`',
        '`dmd_mapping_status` AS `dmd_mapping_status`',
        '`dmd_taxonomy_cd` AS `dmd_taxonomy_cd`',
        '`route_of_administration` AS `route_of_administration`',
        '`strength` AS `strength`',
        '`volume` AS `volume`',
        '`pack_size` AS `pack_size`',
        '`quantity` AS `quantity`',
        '`unit_of_measure` AS `unit_of_measure`',
        '`dispensing_route` AS `dispensing_route`',
        '`dispensing_location` AS `dispensing_location`',
        '`indication` AS `indication`',
        '`funding_reference` AS `funding_reference`',
        '`hcdr_category_cd` AS `hcdr_category_cd`',
        '`hcdr_category_desc` AS `hcdr_category_desc`',
        '`ccg_residence_cd` AS `ccg_residence_cd`',
        '`ccg_gp_cd` AS `ccg_gp_cd`',
        '`commissioner_cd` AS `commissioner_cd`',
        '`commissioner_type` AS `commissioner_type`',
        '`service_line` AS `service_line`',
        '`service_category_cd` AS `service_category_cd`',
        '`unit_price_supplier` AS `unit_price_supplier`',
        '`unit_price_commissioner` AS `unit_price_commissioner`',
        '`vat` AS `vat`',
        '`vat_cd` AS `vat_cd`',
        '`income` AS `income`',
        '`cost` AS `cost`',
        '`margin` AS `margin`',
        '`lloyds_dispensing_fee` AS `lloyds_dispensing_fee`',
        '`production_fee` AS `production_fee`',
        '`fixed_patient_income` AS `fixed_patient_income`',
        '`cost_centre_desc` AS `cost_centre_desc`',
        '`drug_feed` AS `drug_feed`',
        '`data_set` AS `data_set`',
        '`drug_category` AS `drug_category`',
        '`ledger_cd` AS `ledger_cd`',
        '`exclusion_flag` AS `exclusion_flag`',
        '`exclusion_reason` AS `exclusion_reason`',
        '`ledger_lv3_cd` AS `ledger_lv3_cd`',
        '`ledger_lv3_desc` AS `ledger_lv3_desc`',
        '`ledger_lv6_cd` AS `ledger_lv6_cd`',
        '`ledger_lv6_desc` AS `ledger_lv6_desc`',
        '`ledger_lv7_cd` AS `ledger_lv7_cd`',
        '`ledger_lv7_desc` AS `ledger_lv7_desc`',
        '`ledger_lv9_cd` AS `ledger_lv9_cd`',
        '`ledger_lv9_desc` AS `ledger_lv9_desc`',
        '`slr_cd` AS `slr_cd`',
        '`diabetic_flag` AS `diabetic_flag`',
        '`imcoe_flag` AS `imcoe_flag`',
        '`source_duplicate_count` AS `source_duplicate_count`',
        '`person_link_method` AS `person_link_method`',
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
    'clinical_drug_expenditure': GoldRules(
        table='clinical_drug_expenditure',
        select=CLINICAL_DRUG_EXPENDITURE_SELECT,
        mandatory=CLINICAL_DRUG_EXPENDITURE_MANDATORY_RULES,
        advisory=CLINICAL_DRUG_EXPENDITURE_ADVISORY_RULES,
        person_scope=CLINICAL_DRUG_EXPENDITURE_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_drug_expenditure': {'columns': {'additional_info': 'Free-text supplementary description of '
                                                                  'the supplied item, including presentation '
                                                                  'and administration details, as recorded '
                                                                  'in the source.',
                                               'ccg_gp_cd': 'Code of the CCG or successor commissioning '
                                                            "organisation associated with the patient's "
                                                            'registered GP practice, as supplied by the '
                                                            'source.',
                                               'ccg_residence_cd': 'Code of the CCG or successor '
                                                                   'commissioning organisation associated '
                                                                   "with the patient's area of residence, as "
                                                                   'supplied by the source.',
                                               'chargeable_item': 'Name of the chargeable high-cost drug or '
                                                                  'device as recorded in the source.',
                                               'commissioner_cd': 'Code of the commissioning organisation '
                                                                  'responsible for funding the item, as '
                                                                  'supplied by the source.',
                                               'commissioner_type': 'Type or category of the responsible '
                                                                    'commissioner as supplied by the source, '
                                                                    'and blank where not recorded.',
                                               'confidentiality_code': 'Source confidentiality '
                                                                       'classification attached to the '
                                                                       'record for each drug expenditure '
                                                                       'record. It is produced by the silver '
                                                                       'transformation and has no direct '
                                                                       'bronze-column lineage entry. Source '
                                                                       'code meanings and sentinel values '
                                                                       'are retained unless the pipeline '
                                                                       'explicitly maps them; null means no '
                                                                       'code was supplied.',
                                               'consultant_cd': 'Local code identifying the responsible '
                                                                'consultant for the charge; may be blank or '
                                                                'null.',
                                               'cost': 'Expenditure cost value for the drug or device line '
                                                       'as supplied by the SLR source; multiply by '
                                                       'SOURCE_DUPLICATE_COUNT to reconstruct totals.',
                                               'cost_centre_desc': 'Finance cost centre description for the '
                                                                   'line, which commonly embeds the site '
                                                                   'abbreviation, the responsible '
                                                                   "consultant's name and the specialty.",
                                               'data_set': 'Submission dataset grouping identifying the site '
                                                           'or sites and feed combination the line was '
                                                           'reported under.',
                                               'diabetic_flag': 'Source-supplied indicator marking lines '
                                                                'classified as diabetes-related expenditure; '
                                                                'null where not flagged.',
                                               'dispensing_location': 'Description of the dispensing '
                                                                      'location or supply point recorded in '
                                                                      'the source, such as a hospital '
                                                                      'pharmacy, clinic or homecare service.',
                                               'dispensing_route': 'Source-supplied code indicating the '
                                                                   'dispensing route or supply channel used '
                                                                   'for the item.',
                                               'dmd_code': 'Strict trimmed all-numeric dm+d code; null for '
                                                           'blank or non-numeric source values.',
                                               'dmd_concept_id': 'OMOP concept identifier for the dm+d '
                                                                 'source concept matched from DMD_CODE; null '
                                                                 'when no match is made.',
                                               'dmd_concept_name': 'OMOP concept name of the matched dm+d '
                                                                   'source concept; null when no match is '
                                                                   'made.',
                                               'dmd_mapping_status': 'dm+d mapping state: NO_CODE, '
                                                                     'CODE_UNMAPPED, MAPPED_STANDARD, or '
                                                                     'MAPPED_DMD_ONLY.',
                                               'dmd_raw': 'Verbatim source dm+d value; includes junk values '
                                                          'such as No SNOMED.',
                                               'dmd_taxonomy_cd': 'dm+d taxonomy level code as supplied in '
                                                                  'the source DMDTaxonomyCode field, '
                                                                  'indicating the dm+d concept type of the '
                                                                  'coded item.',
                                               'drug_category': 'Source-supplied categorisation of the drug '
                                                                'expenditure line by supply or treatment '
                                                                'category for finance reporting.',
                                               'drug_feed': 'Source-supplied identifier of the data feed or '
                                                            'supply system from which the drug expenditure '
                                                            'line was obtained.',
                                               'drug_standard_concept_id': 'OMOP standard concept reached '
                                                                           'only when exactly one valid '
                                                                           'Maps-to target exists.',
                                               'drug_standard_concept_name': 'OMOP standard concept name for '
                                                                             'the drug, populated from the '
                                                                             'OMOP concept table only where '
                                                                             'the dm+d code resolved to '
                                                                             'exactly one Maps-to standard '
                                                                             'concept.',
                                               'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                               'available.',
                                               'event_datetime': 'Effective date/time as supplied; two known '
                                                                 'rows dated 2122 are retained losslessly.',
                                               'event_datetime_local': 'Local-time representation of the '
                                                                       'drug-expenditure event start date '
                                                                       'and time.',
                                               'event_end_datetime': 'Date and time when the represented '
                                                                     'clinical or administrative event ended '
                                                                     'for each drug expenditure record. It '
                                                                     'is produced by the silver '
                                                                     'transformation and has no direct '
                                                                     'bronze-column lineage entry. Source '
                                                                     'precision and timezone handling follow '
                                                                     'the pipeline expression; null means '
                                                                     'the time was unavailable.',
                                               'event_end_datetime_local': 'Local-time representation of the '
                                                                           'drug-expenditure event end date '
                                                                           'and time.',
                                               'exclusion_flag': 'Source indicator showing whether the line '
                                                                 'is excluded from the reported SLR '
                                                                 'expenditure position.',
                                               'exclusion_reason': 'Source-supplied reason accompanying '
                                                                   'EXCLUSION_FLAG explaining why the line '
                                                                   'was excluded, null when not excluded.',
                                               'fact_category': 'Contract category used to group the record '
                                                                'with comparable clinical facts for each '
                                                                'drug expenditure record. It is produced by '
                                                                'the silver transformation and has no direct '
                                                                'bronze-column lineage entry. Whitespace and '
                                                                'source sentinel text are retained unless '
                                                                'the pipeline explicitly normalizes them; '
                                                                'null means no value was supplied.',
                                               'financial_month': 'Financial period (month) within the '
                                                                  'financial year to which the expenditure '
                                                                  'is attributed, as supplied by the source.',
                                               'financial_year': 'Financial year to which the high-cost drug '
                                                                 'or device expenditure is attributed, as '
                                                                 'supplied by the source.',
                                               'fixed_patient_income': 'Fixed per-patient income element for '
                                                                       'the line as supplied by the source, '
                                                                       'null where income is not charged on '
                                                                       'a fixed-patient basis.',
                                               'funding_reference': 'Source-supplied funding or approval '
                                                                    'reference for the item, frequently '
                                                                    'null.',
                                               'hcdr_category_cd': 'Local high-cost-drug reimbursement '
                                                                   'category from source HRGCode; not an '
                                                                   'HRG4+ code.',
                                               'hcdr_category_desc': 'Description of the local '
                                                                     'high-cost-drug reimbursement category, '
                                                                     'taken from the source HRGDesc field '
                                                                     'and paired with HCDR_CATEGORY_CD.',
                                               'imcoe_flag': 'Source-supplied indicator marking lines '
                                                             'attributed to the IMCoE service '
                                                             'classification; null where not flagged.',
                                               'income': 'Reimbursement income value for the drug or device '
                                                         'line as supplied by the SLR source; multiply by '
                                                         'SOURCE_DUPLICATE_COUNT to reconstruct totals.',
                                               'indication': 'Clinical indication for the high-cost drug or '
                                                             'device as recorded in the source, held as free '
                                                             'text.',
                                               'ledger_cd': 'General ledger account code to which the '
                                                            'expenditure or income line is posted.',
                                               'ledger_lv3_cd': 'Level 3 code of the finance ledger '
                                                                'hierarchy, representing the hospital site '
                                                                'or division to which the line is '
                                                                'attributed.',
                                               'ledger_lv3_desc': 'Description for LEDGER_LV3_CD, naming the '
                                                                  'hospital site or division level of the '
                                                                  'finance ledger hierarchy.',
                                               'ledger_lv6_cd': 'Level 6 code of the finance ledger '
                                                                'hierarchy, representing the directorate or '
                                                                'service grouping for the line.',
                                               'ledger_lv6_desc': 'Description for LEDGER_LV6_CD, naming the '
                                                                  'directorate or service grouping in the '
                                                                  'finance ledger hierarchy.',
                                               'ledger_lv7_cd': 'Level 7 code of the finance ledger '
                                                                'hierarchy, representing the site-level '
                                                                'specialty or department for the line.',
                                               'ledger_lv7_desc': 'Description for LEDGER_LV7_CD, naming the '
                                                                  'site-level specialty or department in the '
                                                                  'finance ledger hierarchy.',
                                               'ledger_lv9_cd': 'Level 9 code of the finance ledger '
                                                                'hierarchy, the lowest-level cost centre or '
                                                                'account for the line.',
                                               'ledger_lv9_desc': 'Description for LEDGER_LV9_CD, naming the '
                                                                  'lowest-level cost centre or account in '
                                                                  'the finance ledger hierarchy.',
                                               'lloyds_dispensing_fee': 'Dispensing fee element charged by '
                                                                        'the homecare/dispensing provider '
                                                                        'for the line as supplied by the '
                                                                        'source finance report, null where '
                                                                        'no such fee applies.',
                                               'loaded_at': 'ADC_UPDT carried unchanged from the '
                                                            'contributing row of '
                                                            '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                            'identical to source_update_timestamp. No extra '
                                                            'source clock is joined or aggregated here; the '
                                                            'value is not the current Silver refresh time.',
                                               'margin': 'Margin value for the line, the difference between '
                                                         'income and cost as supplied by the SLR source; '
                                                         'multiply by SOURCE_DUPLICATE_COUNT to reconstruct '
                                                         'totals.',
                                               'pack_size': 'Pack or dispensed unit description for the item '
                                                            'as supplied by the source, held as free text.',
                                               'patient_event_key': 'Deterministic event key derived from '
                                                                    'source identifiers; retained as a '
                                                                    'stable join key.',
                                               'patient_type': 'Source-supplied categorisation of the care '
                                                               'setting under which the chargeable item was '
                                                               'supplied.',
                                               'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                            'resolved.',
                                               'person_link_method': 'How PERSON_ID was resolved: MRN_ALIAS '
                                                                     'first, NHS_ALIAS fallback, or null '
                                                                     'when unresolved.',
                                               'pod_cd': 'Point of delivery code classifying the activity '
                                                         'under which the item is reimbursed, as supplied by '
                                                         'the source.',
                                               'production_fee': 'Production (compounding/preparation) fee '
                                                                 'element for the line as supplied by the '
                                                                 'source, null where no such fee applies.',
                                               'provider_org_cd': 'Organisation code of the provider '
                                                                  'recording the expenditure, as supplied by '
                                                                  'the source.',
                                               'quantity': 'Quantity of the item attributed to this '
                                                           'expenditure row, which may be fractional; '
                                                           'multiply by SOURCE_DUPLICATE_COUNT to '
                                                           'reconstruct totals.',
                                               'record_status': 'Retracted when SOURCE_PRESENT_IND is false '
                                                                'on '
                                                                '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                                'otherwise active; missing presence defaults '
                                                                'true. No superseded status is emitted. This '
                                                                'is staged source-row presence, not the '
                                                                'financial EXCLUSION_FLAG or drug '
                                                                'administration status.',
                                               'record_status_effective_from': 'EFFECTIVE_DT_TM carried '
                                                                               'unchanged from '
                                                                               '4_prod.tmp.journey_finance_hcd_expenditure_s42. '
                                                                               'This '
                                                                               'expenditure-effective-time '
                                                                               'proxy is not an '
                                                                               'independently observed '
                                                                               'status transition; the '
                                                                               'Silver projection applies no '
                                                                               '[1950,2100) clamp or '
                                                                               'ingestion-time fallback.',
                                               'record_status_effective_to': 'ADC_UPDT from '
                                                                             '4_prod.tmp.journey_finance_hcd_expenditure_s42 '
                                                                             'only when its '
                                                                             'SOURCE_PRESENT_IND is false; '
                                                                             'otherwise null. This is the '
                                                                             "staged row's load-time end "
                                                                             'proxy, not the expenditure '
                                                                             'date or an independently '
                                                                             'observed source-status '
                                                                             'transition.',
                                               'reporting_month': 'Month number within the reporting year in '
                                                                  'which the expenditure was reported, as '
                                                                  'supplied by the source.',
                                               'reporting_year': 'Year in which the expenditure was '
                                                                 'reported, as supplied by the source.',
                                               'route_of_administration': 'Route of administration for the '
                                                                          'dispensed item as supplied by the '
                                                                          'source, typically a SNOMED CT '
                                                                          'route code but occasionally '
                                                                          'free-text route wording.',
                                               'service_category_cd': 'Source-supplied service category code '
                                                                      'classifying the expenditure row '
                                                                      'within the finance hierarchy.',
                                               'service_line': 'Service line identifier assigned to the '
                                                               'expenditure row in Service Line Reporting, '
                                                               'often null.',
                                               'site_cd': 'Source site code identifying the hospital site '
                                                          'associated with the charge.',
                                               'site_name': 'Curated mapping for RLH, SBH, WXH, NUH and NGH '
                                                            'only; other values are unresolved by design.',
                                               'slr_cd': 'Service Line Reporting service line code assigned '
                                                         'to the expenditure line.',
                                               'source_code': 'Name of the chargeable high-cost drug or '
                                                              'device as recorded in the source.',
                                               'source_coding_system': 'Strict trimmed all-numeric dm+d '
                                                                       'code; null for blank or non-numeric '
                                                                       'source values.',
                                               'source_display': 'Name of the chargeable high-cost drug or '
                                                                 'device as recorded in the source.',
                                               'source_duplicate_count': 'Number of exact-identical source '
                                                                         'rows represented by this published '
                                                                         'row; reconstruct additive measures '
                                                                         'as measure × count.',
                                               'source_feed': 'Originating data feed responsible for the '
                                                              'record for each drug expenditure record. It '
                                                              'is produced by the silver transformation and '
                                                              'has no direct bronze-column lineage entry. '
                                                              'Whitespace and source sentinel text are '
                                                              'retained unless the pipeline explicitly '
                                                              'normalizes them; null means no value was '
                                                              'supplied.',
                                               'source_row_hash': 'HCD ROW_HASH supplied by the source; '
                                                                  'evidence for the exception where no '
                                                                  'durable native row id exists.',
                                               'source_update_timestamp': 'ADC_UPDT carried unchanged from '
                                                                          '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                                          'identical to loaded_at. The '
                                                                          'Silver projection reads this '
                                                                          'staging table directly, not '
                                                                          'map_finance_hcd_expenditure, and '
                                                                          'supplies no independent native '
                                                                          'application-update clock.',
                                               'specialty_cd': 'Treatment specialty code associated with the '
                                                               'charge, as supplied by the source.',
                                               'strength': 'Strength of the supplied drug or device as '
                                                           'recorded in the source, held as free text and '
                                                           'sometimes blank or non-numeric placeholder '
                                                           'wording.',
                                               'subject_id_system': 'Internal Millennium person identifier '
                                                                    'resolved from source patient '
                                                                    'identifiers via person alias (MRN '
                                                                    'first, then NHS number); null when '
                                                                    'unresolved.',
                                               'subject_key': 'Deterministic SHA-256 over the strongest '
                                                              'source identifier. Not salted, not secret; a '
                                                              'stable join key across feeds where person_id '
                                                              'is unresolved.',
                                               'transaction_id': 'Source SLR transaction reference as '
                                                                 'supplied, blank or null on a material '
                                                                 'share of rows and therefore not usable as '
                                                                 'a key.',
                                               'unit_of_measure': 'Unit of measure for the recorded quantity '
                                                                  'as supplied by the source, generally a '
                                                                  'SNOMED CT unit code and sometimes blank.',
                                               'unit_price_commissioner': 'Commissioner unit price for the '
                                                                          'item as supplied by the source '
                                                                          'finance report.',
                                               'unit_price_supplier': 'Supplier unit price for the item as '
                                                                      'supplied by the source finance '
                                                                      'report.',
                                               'vat': 'Source-supplied indicator of whether VAT applies to '
                                                      'the item.',
                                               'vat_cd': 'Source-supplied VAT code applied to the item for '
                                                         'finance reporting.',
                                               'vip_ind': 'Indicator that the source record carries VIP '
                                                          'handling status for each drug expenditure record. '
                                                          'It is produced by the silver transformation and '
                                                          'has no direct bronze-column lineage entry. Null '
                                                          'means the source did not state the indicator and '
                                                          'must not be interpreted as false.',
                                               'volume': 'Product volume as supplied by the source, held as '
                                                         'free text and frequently blank.',
                                               'withheld_identity_ind': 'Indicator that the source identity '
                                                                        'was deliberately withheld for each '
                                                                        'drug expenditure record. It is '
                                                                        'produced by the silver '
                                                                        'transformation and has no direct '
                                                                        'bronze-column lineage entry. Null '
                                                                        'means the source did not state the '
                                                                        'indicator and must not be '
                                                                        'interpreted as false.'},
                                   'quantity_override': True,
                                   'table': 'Internal quality-controlled twin of clinical_drug_expenditure: '
                                            'the Gold repairs, nulls and expectations are applied here and '
                                            'the published Gold MV reads this object. Materialized rather '
                                            'than temporary so the published MV can refresh incrementally.'},
 'clinical_drug_expenditure': {'columns': {'additional_info': 'Free-text supplementary description of the '
                                                              'supplied item, including presentation and '
                                                              'administration details, as recorded in the '
                                                              'source.',
                                           'ccg_gp_cd': 'Code of the CCG or successor commissioning '
                                                        "organisation associated with the patient's "
                                                        'registered GP practice, as supplied by the source.',
                                           'ccg_residence_cd': 'Code of the CCG or successor commissioning '
                                                               "organisation associated with the patient's "
                                                               'area of residence, as supplied by the '
                                                               'source.',
                                           'chargeable_item': 'Name of the chargeable high-cost drug or '
                                                              'device as recorded in the source.',
                                           'commissioner_cd': 'Code of the commissioning organisation '
                                                              'responsible for funding the item, as supplied '
                                                              'by the source.',
                                           'commissioner_type': 'Type or category of the responsible '
                                                                'commissioner as supplied by the source, and '
                                                                'blank where not recorded.',
                                           'confidentiality_code': 'Source confidentiality classification '
                                                                   'attached to the record for each drug '
                                                                   'expenditure record. It is produced by '
                                                                   'the silver transformation and has no '
                                                                   'direct bronze-column lineage entry. '
                                                                   'Source code meanings and sentinel values '
                                                                   'are retained unless the pipeline '
                                                                   'explicitly maps them; null means no code '
                                                                   'was supplied.',
                                           'consultant_cd': 'Local code identifying the responsible '
                                                            'consultant for the charge; may be blank or '
                                                            'null.',
                                           'cost': 'Expenditure cost value for the drug or device line as '
                                                   'supplied by the SLR source; multiply by '
                                                   'SOURCE_DUPLICATE_COUNT to reconstruct totals.',
                                           'cost_centre_desc': 'Finance cost centre description for the '
                                                               'line, which commonly embeds the site '
                                                               "abbreviation, the responsible consultant's "
                                                               'name and the specialty.',
                                           'data_set': 'Submission dataset grouping identifying the site or '
                                                       'sites and feed combination the line was reported '
                                                       'under.',
                                           'diabetic_flag': 'Source-supplied indicator marking lines '
                                                            'classified as diabetes-related expenditure; '
                                                            'null where not flagged.',
                                           'dispensing_location': 'Description of the dispensing location or '
                                                                  'supply point recorded in the source, such '
                                                                  'as a hospital pharmacy, clinic or '
                                                                  'homecare service.',
                                           'dispensing_route': 'Source-supplied code indicating the '
                                                               'dispensing route or supply channel used for '
                                                               'the item.',
                                           'dmd_code': 'Strict trimmed all-numeric dm+d code; null for blank '
                                                       'or non-numeric source values.',
                                           'dmd_concept_id': 'OMOP concept identifier for the dm+d source '
                                                             'concept matched from DMD_CODE; null when no '
                                                             'match is made.',
                                           'dmd_concept_name': 'OMOP concept name of the matched dm+d source '
                                                               'concept; null when no match is made.',
                                           'dmd_mapping_status': 'dm+d mapping state: NO_CODE, '
                                                                 'CODE_UNMAPPED, MAPPED_STANDARD, or '
                                                                 'MAPPED_DMD_ONLY.',
                                           'dmd_raw': 'Verbatim source dm+d value; includes junk values such '
                                                      'as No SNOMED.',
                                           'dmd_taxonomy_cd': 'dm+d taxonomy level code as supplied in the '
                                                              'source DMDTaxonomyCode field, indicating the '
                                                              'dm+d concept type of the coded item.',
                                           'drug_category': 'Source-supplied categorisation of the drug '
                                                            'expenditure line by supply or treatment '
                                                            'category for finance reporting.',
                                           'drug_feed': 'Source-supplied identifier of the data feed or '
                                                        'supply system from which the drug expenditure line '
                                                        'was obtained.',
                                           'drug_standard_concept_id': 'OMOP standard concept reached only '
                                                                       'when exactly one valid Maps-to '
                                                                       'target exists.',
                                           'drug_standard_concept_name': 'OMOP standard concept name for the '
                                                                         'drug, populated from the OMOP '
                                                                         'concept table only where the dm+d '
                                                                         'code resolved to exactly one '
                                                                         'Maps-to standard concept.',
                                           'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                           'available.',
                                           'event_datetime': 'Effective date/time as supplied; two known '
                                                             'rows dated 2122 are retained losslessly.',
                                           'event_datetime_local': 'Local-time representation of the '
                                                                   'drug-expenditure event start date and '
                                                                   'time.',
                                           'event_end_datetime': 'Date and time when the represented '
                                                                 'clinical or administrative event ended for '
                                                                 'each drug expenditure record. It is '
                                                                 'produced by the silver transformation and '
                                                                 'has no direct bronze-column lineage entry. '
                                                                 'Source precision and timezone handling '
                                                                 'follow the pipeline expression; null means '
                                                                 'the time was unavailable.',
                                           'event_end_datetime_local': 'Local-time representation of the '
                                                                       'drug-expenditure event end date and '
                                                                       'time.',
                                           'exclusion_flag': 'Source indicator showing whether the line is '
                                                             'excluded from the reported SLR expenditure '
                                                             'position.',
                                           'exclusion_reason': 'Source-supplied reason accompanying '
                                                               'EXCLUSION_FLAG explaining why the line was '
                                                               'excluded, null when not excluded.',
                                           'fact_category': 'Contract category used to group the record with '
                                                            'comparable clinical facts for each drug '
                                                            'expenditure record. It is produced by the '
                                                            'silver transformation and has no direct '
                                                            'bronze-column lineage entry. Whitespace and '
                                                            'source sentinel text are retained unless the '
                                                            'pipeline explicitly normalizes them; null means '
                                                            'no value was supplied.',
                                           'financial_month': 'Financial period (month) within the financial '
                                                              'year to which the expenditure is attributed, '
                                                              'as supplied by the source.',
                                           'financial_year': 'Financial year to which the high-cost drug or '
                                                             'device expenditure is attributed, as supplied '
                                                             'by the source.',
                                           'fixed_patient_income': 'Fixed per-patient income element for the '
                                                                   'line as supplied by the source, null '
                                                                   'where income is not charged on a '
                                                                   'fixed-patient basis.',
                                           'funding_reference': 'Source-supplied funding or approval '
                                                                'reference for the item, frequently null.',
                                           'hcdr_category_cd': 'Local high-cost-drug reimbursement category '
                                                               'from source HRGCode; not an HRG4+ code.',
                                           'hcdr_category_desc': 'Description of the local high-cost-drug '
                                                                 'reimbursement category, taken from the '
                                                                 'source HRGDesc field and paired with '
                                                                 'HCDR_CATEGORY_CD.',
                                           'imcoe_flag': 'Source-supplied indicator marking lines attributed '
                                                         'to the IMCoE service classification; null where '
                                                         'not flagged.',
                                           'income': 'Reimbursement income value for the drug or device line '
                                                     'as supplied by the SLR source; multiply by '
                                                     'SOURCE_DUPLICATE_COUNT to reconstruct totals.',
                                           'indication': 'Clinical indication for the high-cost drug or '
                                                         'device as recorded in the source, held as free '
                                                         'text.',
                                           'ledger_cd': 'General ledger account code to which the '
                                                        'expenditure or income line is posted.',
                                           'ledger_lv3_cd': 'Level 3 code of the finance ledger hierarchy, '
                                                            'representing the hospital site or division to '
                                                            'which the line is attributed.',
                                           'ledger_lv3_desc': 'Description for LEDGER_LV3_CD, naming the '
                                                              'hospital site or division level of the '
                                                              'finance ledger hierarchy.',
                                           'ledger_lv6_cd': 'Level 6 code of the finance ledger hierarchy, '
                                                            'representing the directorate or service '
                                                            'grouping for the line.',
                                           'ledger_lv6_desc': 'Description for LEDGER_LV6_CD, naming the '
                                                              'directorate or service grouping in the '
                                                              'finance ledger hierarchy.',
                                           'ledger_lv7_cd': 'Level 7 code of the finance ledger hierarchy, '
                                                            'representing the site-level specialty or '
                                                            'department for the line.',
                                           'ledger_lv7_desc': 'Description for LEDGER_LV7_CD, naming the '
                                                              'site-level specialty or department in the '
                                                              'finance ledger hierarchy.',
                                           'ledger_lv9_cd': 'Level 9 code of the finance ledger hierarchy, '
                                                            'the lowest-level cost centre or account for the '
                                                            'line.',
                                           'ledger_lv9_desc': 'Description for LEDGER_LV9_CD, naming the '
                                                              'lowest-level cost centre or account in the '
                                                              'finance ledger hierarchy.',
                                           'lloyds_dispensing_fee': 'Dispensing fee element charged by the '
                                                                    'homecare/dispensing provider for the '
                                                                    'line as supplied by the source finance '
                                                                    'report, null where no such fee applies.',
                                           'loaded_at': 'ADC_UPDT carried unchanged from the contributing '
                                                        'row of '
                                                        '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                        'identical to source_update_timestamp. No extra '
                                                        'source clock is joined or aggregated here; the '
                                                        'value is not the current Silver refresh time.',
                                           'margin': 'Margin value for the line, the difference between '
                                                     'income and cost as supplied by the SLR source; '
                                                     'multiply by SOURCE_DUPLICATE_COUNT to reconstruct '
                                                     'totals.',
                                           'pack_size': 'Pack or dispensed unit description for the item as '
                                                        'supplied by the source, held as free text.',
                                           'patient_event_key': 'Deterministic event key derived from source '
                                                                'identifiers; retained as a stable join key.',
                                           'patient_type': 'Source-supplied categorisation of the care '
                                                           'setting under which the chargeable item was '
                                                           'supplied.',
                                           'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                        'resolved.',
                                           'person_link_method': 'How PERSON_ID was resolved: MRN_ALIAS '
                                                                 'first, NHS_ALIAS fallback, or null when '
                                                                 'unresolved.',
                                           'pod_cd': 'Point of delivery code classifying the activity under '
                                                     'which the item is reimbursed, as supplied by the '
                                                     'source.',
                                           'production_fee': 'Production (compounding/preparation) fee '
                                                             'element for the line as supplied by the '
                                                             'source, null where no such fee applies.',
                                           'provider_org_cd': 'Organisation code of the provider recording '
                                                              'the expenditure, as supplied by the source.',
                                           'quantity': 'Quantity of the item attributed to this expenditure '
                                                       'row, which may be fractional; multiply by '
                                                       'SOURCE_DUPLICATE_COUNT to reconstruct totals.',
                                           'record_status': 'Retracted when SOURCE_PRESENT_IND is false on '
                                                            '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                            'otherwise active; missing presence defaults '
                                                            'true. No superseded status is emitted. This is '
                                                            'staged source-row presence, not the financial '
                                                            'EXCLUSION_FLAG or drug administration status.',
                                           'record_status_effective_from': 'EFFECTIVE_DT_TM carried '
                                                                           'unchanged from '
                                                                           '4_prod.tmp.journey_finance_hcd_expenditure_s42. '
                                                                           'This expenditure-effective-time '
                                                                           'proxy is not an independently '
                                                                           'observed status transition; the '
                                                                           'Silver projection applies no '
                                                                           '[1950,2100) clamp or '
                                                                           'ingestion-time fallback.',
                                           'record_status_effective_to': 'ADC_UPDT from '
                                                                         '4_prod.tmp.journey_finance_hcd_expenditure_s42 '
                                                                         'only when its SOURCE_PRESENT_IND '
                                                                         'is false; otherwise null. This is '
                                                                         "the staged row's load-time end "
                                                                         'proxy, not the expenditure date or '
                                                                         'an independently observed '
                                                                         'source-status transition.',
                                           'reporting_month': 'Month number within the reporting year in '
                                                              'which the expenditure was reported, as '
                                                              'supplied by the source.',
                                           'reporting_year': 'Year in which the expenditure was reported, as '
                                                             'supplied by the source.',
                                           'route_of_administration': 'Route of administration for the '
                                                                      'dispensed item as supplied by the '
                                                                      'source, typically a SNOMED CT route '
                                                                      'code but occasionally free-text route '
                                                                      'wording.',
                                           'service_category_cd': 'Source-supplied service category code '
                                                                  'classifying the expenditure row within '
                                                                  'the finance hierarchy.',
                                           'service_line': 'Service line identifier assigned to the '
                                                           'expenditure row in Service Line Reporting, often '
                                                           'null.',
                                           'site_cd': 'Source site code identifying the hospital site '
                                                      'associated with the charge.',
                                           'site_name': 'Curated mapping for RLH, SBH, WXH, NUH and NGH '
                                                        'only; other values are unresolved by design.',
                                           'slr_cd': 'Service Line Reporting service line code assigned to '
                                                     'the expenditure line.',
                                           'source_code': 'Name of the chargeable high-cost drug or device '
                                                          'as recorded in the source.',
                                           'source_coding_system': 'Strict trimmed all-numeric dm+d code; '
                                                                   'null for blank or non-numeric source '
                                                                   'values.',
                                           'source_display': 'Name of the chargeable high-cost drug or '
                                                             'device as recorded in the source.',
                                           'source_duplicate_count': 'Number of exact-identical source rows '
                                                                     'represented by this published row; '
                                                                     'reconstruct additive measures as '
                                                                     'measure × count.',
                                           'source_feed': 'Originating data feed responsible for the record '
                                                          'for each drug expenditure record. It is produced '
                                                          'by the silver transformation and has no direct '
                                                          'bronze-column lineage entry. Whitespace and '
                                                          'source sentinel text are retained unless the '
                                                          'pipeline explicitly normalizes them; null means '
                                                          'no value was supplied.',
                                           'source_row_hash': 'HCD ROW_HASH supplied by the source; evidence '
                                                              'for the exception where no durable native row '
                                                              'id exists.',
                                           'source_update_timestamp': 'ADC_UPDT carried unchanged from '
                                                                      '4_prod.tmp.journey_finance_hcd_expenditure_s42, '
                                                                      'identical to loaded_at. The Silver '
                                                                      'projection reads this staging table '
                                                                      'directly, not '
                                                                      'map_finance_hcd_expenditure, and '
                                                                      'supplies no independent native '
                                                                      'application-update clock.',
                                           'specialty_cd': 'Treatment specialty code associated with the '
                                                           'charge, as supplied by the source.',
                                           'strength': 'Strength of the supplied drug or device as recorded '
                                                       'in the source, held as free text and sometimes blank '
                                                       'or non-numeric placeholder wording.',
                                           'subject_id_system': 'Internal Millennium person identifier '
                                                                'resolved from source patient identifiers '
                                                                'via person alias (MRN first, then NHS '
                                                                'number); null when unresolved.',
                                           'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                          'identifier. Not salted, not secret; a stable join '
                                                          'key across feeds where person_id is unresolved.',
                                           'transaction_id': 'Source SLR transaction reference as supplied, '
                                                             'blank or null on a material share of rows and '
                                                             'therefore not usable as a key.',
                                           'unit_of_measure': 'Unit of measure for the recorded quantity as '
                                                              'supplied by the source, generally a SNOMED CT '
                                                              'unit code and sometimes blank.',
                                           'unit_price_commissioner': 'Commissioner unit price for the item '
                                                                      'as supplied by the source finance '
                                                                      'report.',
                                           'unit_price_supplier': 'Supplier unit price for the item as '
                                                                  'supplied by the source finance report.',
                                           'vat': 'Source-supplied indicator of whether VAT applies to the '
                                                  'item.',
                                           'vat_cd': 'Source-supplied VAT code applied to the item for '
                                                     'finance reporting.',
                                           'vip_ind': 'Indicator that the source record carries VIP handling '
                                                      'status for each drug expenditure record. It is '
                                                      'produced by the silver transformation and has no '
                                                      'direct bronze-column lineage entry. Null means the '
                                                      'source did not state the indicator and must not be '
                                                      'interpreted as false.',
                                           'volume': 'Product volume as supplied by the source, held as free '
                                                     'text and frequently blank.',
                                           'withheld_identity_ind': 'Indicator that the source identity was '
                                                                    'deliberately withheld for each drug '
                                                                    'expenditure record. It is produced by '
                                                                    'the silver transformation and has no '
                                                                    'direct bronze-column lineage entry. '
                                                                    'Null means the source did not state the '
                                                                    'indicator and must not be interpreted '
                                                                    'as false.'},
                               'quantity_override': False,
                               'table': 'One HCD drug-expenditure row keyed by ROW_HASH. Gold QC twin of the '
                                        'silver product: 1 columns are repaired or nulled, 1 rule(s) drop '
                                        'rows, 4 check(s) are advisory. Each rule states its reason in the '
                                        'pipeline notebook, and Lakeflow expectation metrics report what '
                                        'every rule matched on each update.'}}
