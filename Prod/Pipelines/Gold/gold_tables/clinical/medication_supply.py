"""Gold clinical.medication_supply: 2 dataset(s).

- datasets: _qc_clinical_medication_supply, clinical_medication_supply
- moved verbatim from: 2_gold_clinical_events:4062, 2_gold_clinical_events:4124, 2_gold_clinical_events:4138, 2_gold_clinical_events:4146, 2_gold_clinical_events:4249, 2_gold_clinical_events:4253, 2_gold_clinical_events:4270

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.medication_supply ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_MEDICATION_SUPPLY_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`homecare_request_item_id` AS `homecare_request_item_id`',
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
    '`request_key` AS `request_key`',
    '`item_seq` AS `item_seq`',
    '`request_status_code` AS `request_status_code`',
    '`request_status_display` AS `request_status_display`',
    '`item_status_code` AS `item_status_code`',
    '`item_status_display` AS `item_status_display`',
    '`item_type` AS `item_type`',
    '`item_description` AS `item_description`',
    '`pack_description` AS `pack_description`',
    '`quantity_requested` AS `quantity_requested`',
    '`quantity_original` AS `quantity_original`',
    '`quantity_delivered` AS `quantity_delivered`',
    '`order_unit` AS `order_unit`',
    '`label_directions` AS `label_directions`',
    '`nfd_reason` AS `nfd_reason`',
    '`supply_start_date` AS `supply_start_date`',
    '`supply_interval` AS `supply_interval`',
    '`supply_period` AS `supply_period`',
    '`request_item_count` AS `request_item_count`',
    '`request_complete_item_count` AS `request_complete_item_count`',
    '`request_released_date` AS `request_released_date`',
    '`location_name` AS `location_name`',
    '`cost_centre_name` AS `cost_centre_name`',
    '`indication` AS `indication`',
    '`clinic` AS `clinic`',
    '`dmd_vtm_concept_id` AS `dmd_vtm_concept_id`',
    '`dmd_vtm_code` AS `dmd_vtm_code`',
    '`dmd_vtm_name` AS `dmd_vtm_name`',
    '`drug_mapping_method` AS `drug_mapping_method`',
    '`care_site_cd` AS `care_site_cd`',
    '`care_site_match_method` AS `care_site_match_method`',
    '`lnkpid` AS `lnkpid`',
    '`name_key` AS `name_key`',
    '`mrn_candidates` AS `mrn_candidates`',
    '`nhs_candidates` AS `nhs_candidates`',
    '`person_match_method` AS `person_match_method`',
    '`person_match_status` AS `person_match_status`',
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


CLINICAL_MEDICATION_SUPPLY_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`drug_mapping_rule_id` AS `drug_mapping_rule_id`',
    '`drug_mapping_model` AS `drug_mapping_model`',
    '`drug_mapping_score` AS `drug_mapping_score`',
    '`homecare_contact_name` AS `homecare_contact_name`',
    '`homecare_contact_addr1` AS `homecare_contact_addr1`',
    '`homecare_contact_addr2` AS `homecare_contact_addr2`',
    '`homecare_contact_addr3` AS `homecare_contact_addr3`',
    '`homecare_contact_addr4` AS `homecare_contact_addr4`',
    '`homecare_contact_postcode` AS `homecare_contact_postcode`',
    '`homecare_contact_telephone` AS `homecare_contact_telephone`',
]


# contract v2: QC/batch inputs come from internal _clinical_medication_supply_metadata; source history stays on the main research table.
CLINICAL_MEDICATION_SUPPLY_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 271,725 rows of 271,752 that
    # are current and attributable; identity_status = 'resolved' keeps the 269,250 rows of
    # 271,752 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_MEDICATION_SUPPLY_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.medication_supply.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 2,502 of 271,752 at the profile.
    "gold.clinical.medication_supply.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 27 of 271,752 at the profile.
    "gold.clinical.medication_supply.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 489 of 271,752 rows (0.18%) when profiled on 2026-08-24.
    "gold.clinical.medication_supply.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_MEDICATION_SUPPLY_PERSON_SCOPE_RULES = {
    "gold.clinical.medication_supply.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_medication_supply"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_SUPPLY_MANDATORY_RULES)
@dp.expect_all(CLINICAL_MEDICATION_SUPPLY_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_MEDICATION_SUPPLY_PERSON_SCOPE_RULES)
def _gold_qc_clinical_medication_supply():
    """Quality-controlled twin of journey_clinical.medication_supply."""
    df = _qc(
        "clinical_medication_supply",
        CLINICAL_MEDICATION_SUPPLY_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.medication_supply"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_medication_supply():
    """Contract-v2 public twin of clinical_medication_supply; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_medication_supply")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`homecare_request_item_id` AS `homecare_request_item_id`',
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
        '`request_key` AS `request_key`',
        '`item_seq` AS `item_seq`',
        '`request_status_code` AS `request_status_code`',
        '`request_status_display` AS `request_status_display`',
        '`item_status_code` AS `item_status_code`',
        '`item_status_display` AS `item_status_display`',
        '`item_type` AS `item_type`',
        '`item_description` AS `item_description`',
        '`pack_description` AS `pack_description`',
        '`quantity_requested` AS `quantity_requested`',
        '`quantity_original` AS `quantity_original`',
        '`quantity_delivered` AS `quantity_delivered`',
        '`order_unit` AS `order_unit`',
        '`label_directions` AS `label_directions`',
        '`nfd_reason` AS `nfd_reason`',
        '`supply_start_date` AS `supply_start_date`',
        '`supply_interval` AS `supply_interval`',
        '`supply_period` AS `supply_period`',
        '`request_item_count` AS `request_item_count`',
        '`request_complete_item_count` AS `request_complete_item_count`',
        '`request_released_date` AS `request_released_date`',
        '`location_name` AS `location_name`',
        '`cost_centre_name` AS `cost_centre_name`',
        '`indication` AS `indication`',
        '`clinic` AS `clinic`',
        '`dmd_vtm_concept_id` AS `dmd_vtm_concept_id`',
        '`dmd_vtm_code` AS `dmd_vtm_code`',
        '`dmd_vtm_name` AS `dmd_vtm_name`',
        '`drug_mapping_method` AS `drug_mapping_method`',
        '`care_site_cd` AS `care_site_cd`',
        '`care_site_match_method` AS `care_site_match_method`',
        '`lnkpid` AS `lnkpid`',
        '`name_key` AS `name_key`',
        '`mrn_candidates` AS `mrn_candidates`',
        '`nhs_candidates` AS `nhs_candidates`',
        '`person_match_method` AS `person_match_method`',
        '`person_match_status` AS `person_match_status`',
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
        '`drug_mapping_rule_id` AS `drug_mapping_rule_id`',  # INSR_W3 W3.12
        '`drug_mapping_model` AS `drug_mapping_model`',  # INSR_W3 W3.12
        '`drug_mapping_score` AS `drug_mapping_score`',  # INSR_W3 W3.12
        '`homecare_contact_name` AS `homecare_contact_name`',  # INSR_W3 W3.12
        '`homecare_contact_addr1` AS `homecare_contact_addr1`',  # INSR_W3 W3.12
        '`homecare_contact_addr2` AS `homecare_contact_addr2`',  # INSR_W3 W3.12
        '`homecare_contact_addr3` AS `homecare_contact_addr3`',  # INSR_W3 W3.12
        '`homecare_contact_addr4` AS `homecare_contact_addr4`',  # INSR_W3 W3.12
        '`homecare_contact_postcode` AS `homecare_contact_postcode`',  # INSR_W3 W3.12
        '`homecare_contact_telephone` AS `homecare_contact_telephone`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_medication_supply': GoldRules(
        table='clinical_medication_supply',
        select=CLINICAL_MEDICATION_SUPPLY_SELECT,
        mandatory=CLINICAL_MEDICATION_SUPPLY_MANDATORY_RULES,
        advisory=CLINICAL_MEDICATION_SUPPLY_ADVISORY_RULES,
        person_scope=CLINICAL_MEDICATION_SUPPLY_PERSON_SCOPE_RULES,
        lifecycle={'join_keys': ['patient_event_key'],
         'support_columns': ['identity_status',
                             'record_status',
                             'record_status_effective_from',
                             'record_status_effective_to',
                             'load_batch_id',
                             'source_update_timestamp',
                             'loaded_at']},
        date_flags=('event_after_death_30d',)),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_clinical_medication_supply': {'columns': {'care_site_cd': 'map_care_site key from a unique exact '
                                                                'location-name match.',
                                                'care_site_match_method': 'NAME_EXACT, UNMAPPED, or '
                                                                          'UNAVAILABLE.',
                                                'clinic': 'Clinic recorded on the request.',
                                                'confidentiality_code': 'Source confidentiality '
                                                                        'classification attached to the '
                                                                        'record for each medication supply '
                                                                        'record. It is produced by the '
                                                                        'silver transformation and has no '
                                                                        'direct bronze-column lineage entry. '
                                                                        'Source code meanings and sentinel '
                                                                        'values are retained unless the '
                                                                        'pipeline explicitly maps them; null '
                                                                        'means no code was supplied.',
                                                'cost_centre_name': 'Raw JAC homecare cost-centre name.',
                                                'dmd_vtm_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                                'dmd_vtm_concept_id': 'Valid dm+d VTM OMOP concept_id from '
                                                                      'exact item-description matching.',
                                                'dmd_vtm_name': 'Matched dm+d VTM preferred name.',
                                                'drug_mapping_method': 'VTM_NAME, VTM_SYNONYM, UNMAPPED, '
                                                                       'DISABLED, or NULL for itemless '
                                                                       'headers.',
                                                'drug_mapping_model': 'Verifier model of an '
                                                                      'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                      'DRUG_MAPPING_MODEL, INSR W1). INSR_W2 '
                                                                      'W2.20.',
                                                'drug_mapping_rule_id': 'Rule id of an '
                                                                        'EMBEDDING_LLM_VERIFIED match. '
                                                                        '(bronze DRUG_MAPPING_RULE_ID, INSR '
                                                                        'W1). INSR_W2 W2.20.',
                                                'drug_mapping_score': 'Cosine similarity of an '
                                                                      'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                      'DRUG_MAPPING_SCORE, INSR W1). INSR_W2 '
                                                                      'W2.20.',
                                                'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                                'available.',
                                                'event_datetime': 'Source request-created date.',
                                                'event_datetime_local': 'Local-time equivalent of the event '
                                                                        'start datetime for the homecare '
                                                                        'supply-request item.',
                                                'event_end_datetime': 'Source request-completed date.',
                                                'event_end_datetime_local': 'Local-time equivalent of the '
                                                                            'event end datetime for the '
                                                                            'homecare supply-request item.',
                                                'fact_category': 'Contract category used to group the record '
                                                                 'with comparable clinical facts for each '
                                                                 'medication supply record. It is produced '
                                                                 'by the silver transformation and has no '
                                                                 'direct bronze-column lineage entry. '
                                                                 'Whitespace and source sentinel text are '
                                                                 'retained unless the pipeline explicitly '
                                                                 'normalizes them; null means no value was '
                                                                 'supplied.',
                                                'homecare_contact_addr1': 'Homecare delivery address line 1. '
                                                                          'Identifier, published and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze HOMECARE_CONTACT_ADDR1, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_addr2': 'Homecare delivery address line 2. '
                                                                          'Identifier, published and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze HOMECARE_CONTACT_ADDR2, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_addr3': 'Homecare delivery address line 3. '
                                                                          'Identifier, published and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze HOMECARE_CONTACT_ADDR3, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_addr4': 'Homecare delivery address line 4. '
                                                                          'Identifier, published and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze HOMECARE_CONTACT_ADDR4, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_name': 'Homecare delivery contact name. '
                                                                         'Identifier, published and tagged. '
                                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                         '(bronze HOMECARE_CONTACT_NAME, '
                                                                         'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_postcode': 'Homecare delivery postcode. '
                                                                             'Identifier, published and '
                                                                             'tagged. '
                                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                             '(bronze '
                                                                             'HOMECARE_CONTACT_POSTCODE, '
                                                                             'INSR W1). INSR_W2 W2.20.',
                                                'homecare_contact_telephone': 'Homecare delivery contact '
                                                                              'telephone. Identifier, '
                                                                              'published and tagged. '
                                                                              'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                              '(bronze '
                                                                              'HOMECARE_CONTACT_TELEPHONE, '
                                                                              'INSR W1). INSR_W2 W2.20.',
                                                'homecare_request_item_id': 'JAC HOMECARE_REQUEST_ITEM_ID; '
                                                                            'primary key of this table.',
                                                'indication': 'Clinical indication recorded on the request.',
                                                'item_description': 'Source medicine or supply description.',
                                                'item_seq': 'Item sequence within the request; NULL for an '
                                                            'itemless header.',
                                                'item_status_code': 'Raw JAC item status code.',
                                                'item_status_display': 'COMPLETE, INCOMPLETE, or provisional '
                                                                       'CANCELLED_OR_ERROR.',
                                                'item_type': 'Raw JAC homecare item type.',
                                                'label_directions': 'Source dispensing label directions.',
                                                'lnkpid': 'Direct identifier published and IG-governed at '
                                                          'serve time',
                                                'loaded_at': 'map_homecare_request.ADC_UPDT carried '
                                                             'unchanged through usable-code filtering and '
                                                             'source/public projections. No parent clock is '
                                                             'joined or aggregated. This input load clock is '
                                                             'distinct from the preferred item/request '
                                                             'mirror clock in source_update_timestamp, '
                                                             'supply dates and Silver refresh time.',
                                                'location_name': 'Raw JAC homecare location name.',
                                                'mrn_candidates': 'Distinct active Cerner PERSON_ID '
                                                                  'candidates for the normalised MRN.',
                                                'name_key': 'Direct identifier published and IG-governed at '
                                                            'serve time',
                                                'nfd_reason': 'Source reason the item was not fulfilled.',
                                                'nhs_candidates': 'Distinct active Cerner PERSON_ID '
                                                                  'candidates for the checksum-valid NHS '
                                                                  'number.',
                                                'order_unit': 'Source order unit.',
                                                'pack_description': 'Source pack description.',
                                                'patient_event_key': 'Deterministic event key derived from '
                                                                     'source identifiers; retained as a '
                                                                     'stable join key.',
                                                'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                             'resolved.',
                                                'person_match_method': 'MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, '
                                                                       'CONFLICT, AMBIGUOUS, or NONE.',
                                                'person_match_status': 'MATCHED or the reason no PERSON_ID '
                                                                       'was assigned.',
                                                'quantity_delivered': 'Delivered quantity from the item.',
                                                'quantity_original': 'Original requested quantity from the '
                                                                     'item.',
                                                'quantity_requested': 'Requested quantity from the item.',
                                                'record_status': 'Retracted when '
                                                                 'map_homecare_request.SOURCE_PRESENT_IND is '
                                                                 'false, otherwise active; missing presence '
                                                                 'defaults true. No superseded status is '
                                                                 'emitted. Request/item status, request '
                                                                 'completion and delivered quantity do not '
                                                                 'determine this label; usable-code '
                                                                 'filtering separately controls publication '
                                                                 'eligibility.',
                                                'record_status_effective_from': 'Source request-created '
                                                                                'date.',
                                                'record_status_effective_to': 'map_homecare_request.ADC_UPDT '
                                                                              'only when SOURCE_PRESENT_IND '
                                                                              'is false; otherwise null. '
                                                                              'This is the input-load-time '
                                                                              'end proxy, not '
                                                                              'REQUEST_COMPLETED or a '
                                                                              'clinical supply end; a '
                                                                              'missing ADC_UPDT remains '
                                                                              'null.',
                                                'request_complete_item_count': 'Header-reported completed '
                                                                               'item count.',
                                                'request_item_count': 'Header-reported total item count.',
                                                'request_key': 'JAC homecare request identifier from lnkrb.',
                                                'request_released_date': 'Source request-released date.',
                                                'request_status_code': 'Raw JAC header status code.',
                                                'request_status_display': 'COMPLETE, RELEASED, IN_PROGRESS, '
                                                                          'or provisional '
                                                                          'CANCELLED_OR_ERROR.',
                                                'source_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                                'source_coding_system': 'Matched dm+d VTM SNOMED CT '
                                                                        'identifier.',
                                                'source_display': 'Matched dm+d VTM preferred name.',
                                                'source_feed': 'Originating data feed responsible for the '
                                                               'record for each medication supply record. It '
                                                               'is produced by the silver transformation and '
                                                               'has no direct bronze-column lineage entry. '
                                                               'Whitespace and source sentinel text are '
                                                               'retained unless the pipeline explicitly '
                                                               'normalizes them; null means no value was '
                                                               'supplied.',
                                                'source_update_timestamp': 'First non-null '
                                                                           'map_homecare_request.ITEM_SOURCE_RECORD_UPDATED_DT '
                                                                           'then SOURCE_RECORD_UPDATED_DT. '
                                                                           'These are upstream mirror '
                                                                           'timestamps, with the item clock '
                                                                           'preferred even when the request '
                                                                           'clock is later; the expression '
                                                                           'is coalesce, not greatest, and '
                                                                           'has no ADC_UPDT or Silver '
                                                                           'refresh-time fallback.',
                                                'subject_id_system': 'JAC internal patient link key; not a '
                                                                     'direct patient identifier.',
                                                'subject_key': 'Deterministic SHA-256 over the strongest '
                                                               'source identifier. Not salted, not secret; a '
                                                               'stable join key across feeds where person_id '
                                                               'is unresolved.',
                                                'supply_interval': 'Source supply interval.',
                                                'supply_period': 'Source supply period.',
                                                'supply_start_date': 'Requested supply start date.',
                                                'vip_ind': 'Indicator that the source record carries VIP '
                                                           'handling status for each medication supply '
                                                           'record. It is produced by the silver '
                                                           'transformation and has no direct bronze-column '
                                                           'lineage entry. Null means the source did not '
                                                           'state the indicator and must not be interpreted '
                                                           'as false.',
                                                'withheld_identity_ind': 'Indicator that the source identity '
                                                                         'was deliberately withheld for each '
                                                                         'medication supply record. It is '
                                                                         'produced by the silver '
                                                                         'transformation and has no direct '
                                                                         'bronze-column lineage entry. Null '
                                                                         'means the source did not state the '
                                                                         'indicator and must not be '
                                                                         'interpreted as false.'},
                                    'quantity_override': True,
                                    'table': 'Internal quality-controlled twin of '
                                             'clinical_medication_supply: the Gold repairs, nulls and '
                                             'expectations are applied here and the published Gold MV reads '
                                             'this object. Materialized rather than temporary so the '
                                             'published MV can refresh incrementally.'},
 'clinical_medication_supply': {'columns': {'care_site_cd': 'map_care_site key from a unique exact '
                                                            'location-name match.',
                                            'care_site_match_method': 'NAME_EXACT, UNMAPPED, or UNAVAILABLE.',
                                            'clinic': 'Clinic recorded on the request.',
                                            'confidentiality_code': 'Source confidentiality classification '
                                                                    'attached to the record for each '
                                                                    'medication supply record. It is '
                                                                    'produced by the silver transformation '
                                                                    'and has no direct bronze-column lineage '
                                                                    'entry. Source code meanings and '
                                                                    'sentinel values are retained unless the '
                                                                    'pipeline explicitly maps them; null '
                                                                    'means no code was supplied.',
                                            'cost_centre_name': 'Raw JAC homecare cost-centre name.',
                                            'dmd_vtm_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'dmd_vtm_concept_id': 'Valid dm+d VTM OMOP concept_id from exact '
                                                                  'item-description matching.',
                                            'dmd_vtm_name': 'Matched dm+d VTM preferred name.',
                                            'drug_mapping_method': 'VTM_NAME, VTM_SYNONYM, UNMAPPED, '
                                                                   'DISABLED, or NULL for itemless headers.',
                                            'drug_mapping_model': 'Verifier model of an '
                                                                  'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                  'DRUG_MAPPING_MODEL, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                            'drug_mapping_rule_id': 'Rule id of an EMBEDDING_LLM_VERIFIED '
                                                                    'match. (bronze DRUG_MAPPING_RULE_ID, '
                                                                    'INSR W1). INSR_W2 W2.20.',
                                            'drug_mapping_score': 'Cosine similarity of an '
                                                                  'EMBEDDING_LLM_VERIFIED match. (bronze '
                                                                  'DRUG_MAPPING_SCORE, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                            'encounter_id': 'Native Millennium ENCNTR_ID as BIGINT when '
                                                            'available.',
                                            'event_datetime': 'Source request-created date.',
                                            'event_datetime_local': 'Local-time equivalent of the event '
                                                                    'start datetime for the homecare '
                                                                    'supply-request item.',
                                            'event_end_datetime': 'Source request-completed date.',
                                            'event_end_datetime_local': 'Local-time equivalent of the event '
                                                                        'end datetime for the homecare '
                                                                        'supply-request item.',
                                            'fact_category': 'Contract category used to group the record '
                                                             'with comparable clinical facts for each '
                                                             'medication supply record. It is produced by '
                                                             'the silver transformation and has no direct '
                                                             'bronze-column lineage entry. Whitespace and '
                                                             'source sentinel text are retained unless the '
                                                             'pipeline explicitly normalizes them; null '
                                                             'means no value was supplied.',
                                            'homecare_contact_addr1': 'Homecare delivery address line 1. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR1, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr2': 'Homecare delivery address line 2. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR2, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr3': 'Homecare delivery address line 3. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR3, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_addr4': 'Homecare delivery address line 4. '
                                                                      'Identifier, published and tagged. '
                                                                      'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                      'HOMECARE_CONTACT_ADDR4, INSR W1). '
                                                                      'INSR_W2 W2.20.',
                                            'homecare_contact_name': 'Homecare delivery contact name. '
                                                                     'Identifier, published and tagged. '
                                                                     'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                                     'HOMECARE_CONTACT_NAME, INSR W1). '
                                                                     'INSR_W2 W2.20.',
                                            'homecare_contact_postcode': 'Homecare delivery postcode. '
                                                                         'Identifier, published and tagged. '
                                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                         '(bronze HOMECARE_CONTACT_POSTCODE, '
                                                                         'INSR W1). INSR_W2 W2.20.',
                                            'homecare_contact_telephone': 'Homecare delivery contact '
                                                                          'telephone. Identifier, published '
                                                                          'and tagged. '
                                                                          'INSR_W1_IDENTIFIER_RETAIN_V1 '
                                                                          '(bronze '
                                                                          'HOMECARE_CONTACT_TELEPHONE, INSR '
                                                                          'W1). INSR_W2 W2.20.',
                                            'homecare_request_item_id': 'JAC HOMECARE_REQUEST_ITEM_ID; '
                                                                        'primary key of this table.',
                                            'indication': 'Clinical indication recorded on the request.',
                                            'item_description': 'Source medicine or supply description.',
                                            'item_seq': 'Item sequence within the request; NULL for an '
                                                        'itemless header.',
                                            'item_status_code': 'Raw JAC item status code.',
                                            'item_status_display': 'COMPLETE, INCOMPLETE, or provisional '
                                                                   'CANCELLED_OR_ERROR.',
                                            'item_type': 'Raw JAC homecare item type.',
                                            'label_directions': 'Source dispensing label directions.',
                                            'lnkpid': 'Direct identifier published and IG-governed at serve '
                                                      'time',
                                            'loaded_at': 'map_homecare_request.ADC_UPDT carried unchanged '
                                                         'through usable-code filtering and source/public '
                                                         'projections. No parent clock is joined or '
                                                         'aggregated. This input load clock is distinct from '
                                                         'the preferred item/request mirror clock in '
                                                         'source_update_timestamp, supply dates and Silver '
                                                         'refresh time.',
                                            'location_name': 'Raw JAC homecare location name.',
                                            'mrn_candidates': 'Distinct active Cerner PERSON_ID candidates '
                                                              'for the normalised MRN.',
                                            'name_key': 'Direct identifier published and IG-governed at '
                                                        'serve time',
                                            'nfd_reason': 'Source reason the item was not fulfilled.',
                                            'nhs_candidates': 'Distinct active Cerner PERSON_ID candidates '
                                                              'for the checksum-valid NHS number.',
                                            'order_unit': 'Source order unit.',
                                            'pack_description': 'Source pack description.',
                                            'patient_event_key': 'Deterministic event key derived from '
                                                                 'source identifiers; retained as a stable '
                                                                 'join key.',
                                            'person_id': 'Native Millennium PERSON_ID as BIGINT when '
                                                         'resolved.',
                                            'person_match_method': 'MRN_NHS_AGREE, MRN_ONLY, NHS_ONLY, '
                                                                   'CONFLICT, AMBIGUOUS, or NONE.',
                                            'person_match_status': 'MATCHED or the reason no PERSON_ID was '
                                                                   'assigned.',
                                            'quantity_delivered': 'Delivered quantity from the item.',
                                            'quantity_original': 'Original requested quantity from the item.',
                                            'quantity_requested': 'Requested quantity from the item.',
                                            'record_status': 'Retracted when '
                                                             'map_homecare_request.SOURCE_PRESENT_IND is '
                                                             'false, otherwise active; missing presence '
                                                             'defaults true. No superseded status is '
                                                             'emitted. Request/item status, request '
                                                             'completion and delivered quantity do not '
                                                             'determine this label; usable-code filtering '
                                                             'separately controls publication eligibility.',
                                            'record_status_effective_from': 'Source request-created date.',
                                            'record_status_effective_to': 'map_homecare_request.ADC_UPDT '
                                                                          'only when SOURCE_PRESENT_IND is '
                                                                          'false; otherwise null. This is '
                                                                          'the input-load-time end proxy, '
                                                                          'not REQUEST_COMPLETED or a '
                                                                          'clinical supply end; a missing '
                                                                          'ADC_UPDT remains null.',
                                            'request_complete_item_count': 'Header-reported completed item '
                                                                           'count.',
                                            'request_item_count': 'Header-reported total item count.',
                                            'request_key': 'JAC homecare request identifier from lnkrb.',
                                            'request_released_date': 'Source request-released date.',
                                            'request_status_code': 'Raw JAC header status code.',
                                            'request_status_display': 'COMPLETE, RELEASED, IN_PROGRESS, or '
                                                                      'provisional CANCELLED_OR_ERROR.',
                                            'source_code': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'source_coding_system': 'Matched dm+d VTM SNOMED CT identifier.',
                                            'source_display': 'Matched dm+d VTM preferred name.',
                                            'source_feed': 'Originating data feed responsible for the record '
                                                           'for each medication supply record. It is '
                                                           'produced by the silver transformation and has no '
                                                           'direct bronze-column lineage entry. Whitespace '
                                                           'and source sentinel text are retained unless the '
                                                           'pipeline explicitly normalizes them; null means '
                                                           'no value was supplied.',
                                            'source_update_timestamp': 'First non-null '
                                                                       'map_homecare_request.ITEM_SOURCE_RECORD_UPDATED_DT '
                                                                       'then SOURCE_RECORD_UPDATED_DT. These '
                                                                       'are upstream mirror timestamps, with '
                                                                       'the item clock preferred even when '
                                                                       'the request clock is later; the '
                                                                       'expression is coalesce, not '
                                                                       'greatest, and has no ADC_UPDT or '
                                                                       'Silver refresh-time fallback.',
                                            'subject_id_system': 'JAC internal patient link key; not a '
                                                                 'direct patient identifier.',
                                            'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                           'identifier. Not salted, not secret; a stable '
                                                           'join key across feeds where person_id is '
                                                           'unresolved.',
                                            'supply_interval': 'Source supply interval.',
                                            'supply_period': 'Source supply period.',
                                            'supply_start_date': 'Requested supply start date.',
                                            'vip_ind': 'Indicator that the source record carries VIP '
                                                       'handling status for each medication supply record. '
                                                       'It is produced by the silver transformation and has '
                                                       'no direct bronze-column lineage entry. Null means '
                                                       'the source did not state the indicator and must not '
                                                       'be interpreted as false.',
                                            'withheld_identity_ind': 'Indicator that the source identity was '
                                                                     'deliberately withheld for each '
                                                                     'medication supply record. It is '
                                                                     'produced by the silver transformation '
                                                                     'and has no direct bronze-column '
                                                                     'lineage entry. Null means the source '
                                                                     'did not state the indicator and must '
                                                                     'not be interpreted as false.'},
                                'quantity_override': False,
                                'table': 'One JAC homecare supply-request item; supply, never '
                                         'administration. Gold QC twin of the silver product: 0 columns are '
                                         'repaired or nulled, 1 rule(s) drop rows, 3 check(s) are advisory. '
                                         'Each rule states its reason in the pipeline notebook, and Lakeflow '
                                         'expectation metrics report what every rule matched on each '
                                         'update.'}}
