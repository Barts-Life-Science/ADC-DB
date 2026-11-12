"""Gold clinical.transfusion: 2 dataset(s).

- datasets: _qc_clinical_transfusion, clinical_transfusion
- moved verbatim from: 2_gold_clinical_events:6223, 2_gold_clinical_events:6267, 2_gold_clinical_events:6288, 2_gold_clinical_events:6296, 2_gold_clinical_events:6394, 2_gold_clinical_events:6398, 2_gold_clinical_events:6415

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_clinical.transfusion ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
CLINICAL_TRANSFUSION_SELECT = [
    '`patient_event_key` AS `patient_event_key`',
    '`transfusion_key` AS `transfusion_key`',
    '`subject_key` AS `subject_key`',
    '`subject_id_system` AS `subject_id_system`',
    '`person_id` AS `person_id`',
    '`encounter_id` AS `encounter_id`',
    '`event_datetime` AS `event_datetime`',
    'CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END AS `event_end_datetime`',
    '`event_datetime_local` AS `event_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    'CASE WHEN `event_end_datetime` IS NOT NULL AND (CASE WHEN `event_datetime` IS NOT NULL AND `event_end_datetime` IS NOT NULL AND `event_datetime` > `event_end_datetime` THEN NULL ELSE `event_end_datetime` END) IS NULL THEN NULL ELSE `event_end_datetime_local` END AS `event_end_datetime_local`',  # GOLD_COLUMN_PARITY_V1
    '`source_coding_system` AS `source_coding_system`',
    '`source_code` AS `source_code`',
    '`source_display` AS `source_display`',
    '`begin_datetime` AS `begin_datetime`',
    'CASE WHEN `begin_datetime` IS NOT NULL AND `end_datetime` IS NOT NULL AND `begin_datetime` > `end_datetime` THEN NULL ELSE `end_datetime` END AS `end_datetime`',
    '`transfusion_status` AS `transfusion_status`',
    '`elapsed_minutes` AS `elapsed_minutes`',
    '`unit_number` AS `unit_number`',
    '`blood_product_group` AS `blood_product_group`',
    '`blood_unit_group` AS `blood_unit_group`',
    '`patient_blood_group` AS `patient_blood_group`',
    '`quantity_value` AS `quantity_value`',
    '`quantity_raw` AS `quantity_raw`',
    '`begin_location` AS `begin_location`',
    '`end_location` AS `end_location`',
    '`unit_is_irradiated` AS `unit_is_irradiated`',
    '`unit_is_cmv_neg` AS `unit_is_cmv_neg`',
    '`requires_irradiated` AS `requires_irradiated`',
    '`requires_cmv_neg` AS `requires_cmv_neg`',
    '`ambiguity_ind` AS `ambiguity_ind`',
    '`product_concept_id` AS `product_concept_id`',
    '`product_concept_name` AS `product_concept_name`',
    '`unit_group_concept_id` AS `unit_group_concept_id`',
    '`patient_group_concept_id` AS `patient_group_concept_id`',
    '`confidentiality_code` AS `confidentiality_code`',
    '`vip_ind` AS `vip_ind`',
    '`withheld_identity_ind` AS `withheld_identity_ind`',
    '`record_status` AS `record_status`',
    '`record_status_effective_from` AS `record_status_effective_from`',
    '`record_status_effective_to` AS `record_status_effective_to`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


CLINICAL_TRANSFUSION_SELECT += [  # INSR_W3 W3.12: silver columns new in this release (W2)
    '`end_quantity_unit_inferred` AS `end_quantity_unit_inferred`',
    '`end_quantity_ml` AS `end_quantity_ml`',
    '`quantity_unit_rule` AS `quantity_unit_rule`',
    '`isbt_product_concept_id` AS `isbt_product_concept_id`',
    '`isbt_product_concept_name` AS `isbt_product_concept_name`',
    '`isbt_mapping_method` AS `isbt_mapping_method`',
    '`isbt_mapping_rule_id` AS `isbt_mapping_rule_id`',
    '`isbt_mapping_model` AS `isbt_mapping_model`',
    '`isbt_mapping_score` AS `isbt_mapping_score`',
    '`product_mapping_method` AS `product_mapping_method`',
    '`product_mapping_rule_id` AS `product_mapping_rule_id`',
    '`unit_group_mapping_method` AS `unit_group_mapping_method`',
    '`unit_group_mapping_rule_id` AS `unit_group_mapping_rule_id`',
    '`patient_group_mapping_method` AS `patient_group_mapping_method`',
    '`patient_group_mapping_rule_id` AS `patient_group_mapping_rule_id`',
    '`product_mapping_source` AS `product_mapping_source`',
    '`source_mrn_norm` AS `source_mrn_norm`',
]


# contract v2: QC/batch inputs come from internal _clinical_transfusion_metadata; source history stays on the main research table.
CLINICAL_TRANSFUSION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 127,986 rows of 127,986 that
    # are current and attributable; identity_status = 'resolved' keeps the 127,955 rows of
    # 127,986 that are current and attributable. Superseded versions and rows whose identity
    # was never resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(identity_status = 'resolved') AND (record_status = 'active')",
}


CLINICAL_TRANSFUSION_ADVISORY_RULES = {
    # INSR_W4 W4.05 date_future (measured in W4.05 Step 8): A record cannot become effective after it was loaded (L2282, F5(h)).
    'gold.clinical.transfusion.record_status_effective_from.insr_f5h_effective_after_load':
        'NOT COALESCE((`record_status_effective_from` > `loaded_at`), FALSE)',
    # INSR_W4 W4.06 cross_field (estate 2026-10-29): STARTED_NO_END rows cannot carry an end (L3775 status vs end).
    'gold.clinical.transfusion.table.insr_f19_started_with_end':
        "NOT COALESCE((`transfusion_status` = 'STARTED_NO_END' AND `end_datetime` IS NOT NULL), FALSE)",
    # INSR_W4 W4.05 cross_field (estate 2026-10-29): A COMPLETED transfusion has an end (L3775).
    'gold.clinical.transfusion.table.insr_f6_completed_without_end':
        "NOT COALESCE((`transfusion_status` = 'COMPLETED' AND `end_datetime` IS NULL), FALSE)",
    # Counts what the research surface removed: rows failing identity_status = 'resolved'.
    # The mandatory rule above has already dropped them, so this is how many went, not how
    # many survive. 31 of 127,986 at the profile.
    "gold.clinical.transfusion.identity_status.default_view_resolved":
        "identity_status = 'resolved'",

    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 127,986 at the profile.
    "gold.clinical.transfusion.record_status.default_view_active":
        "record_status = 'active'",

    # Left as a warning because the comparison has no trustworthy side: 2,051 people in the
    # spine carry a death recorded before their own birth, 14,720 died before 1990 and
    # 188,119 were born before 1920. When an event disagrees with the anchor, the data does
    # not say which of the two is wrong, so nulling either would destroy sound values at an
    # unknown rate. Seen on 1,523 of 127,986 rows (1.19%) when profiled on 2026-08-24.
    "gold.clinical.transfusion.table.after_death_30d_event_datetime_person_deceased_datetime":
        "NOT COALESCE(event_after_death_30d, FALSE)",
}


# INSR_W3 W3.02 (C0 ruling 1): rows of a spine person with no encounter and no clinical data leave gold;
# silver keeps them. NULL-person and orphan rows pass (_qc_person_in_scope is TRUE).
CLINICAL_TRANSFUSION_PERSON_SCOPE_RULES = {
    "gold.clinical.transfusion.person_id.person_in_scope": "`_qc_person_in_scope`",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_transfusion"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_MANDATORY_RULES)
@dp.expect_all(CLINICAL_TRANSFUSION_ADVISORY_RULES)
@dp.expect_all_or_drop(CLINICAL_TRANSFUSION_PERSON_SCOPE_RULES)
def _gold_qc_clinical_transfusion():
    """Quality-controlled twin of journey_clinical.transfusion."""
    df = _qc(
        "clinical_transfusion",
        CLINICAL_TRANSFUSION_SELECT,
        date_flags=["event_after_death_30d"],
    )
    return df


@dp.materialized_view(
    name=_n("gold_clinical.transfusion"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_transfusion():
    """Contract-v2 public twin of clinical_transfusion; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._clinical_transfusion")).selectExpr(
        '`patient_event_key` AS `patient_event_key`',
        '`transfusion_key` AS `transfusion_key`',
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
        '`begin_datetime` AS `begin_datetime`',
        '`end_datetime` AS `end_datetime`',
        '`transfusion_status` AS `transfusion_status`',
        '`elapsed_minutes` AS `elapsed_minutes`',
        '`unit_number` AS `unit_number`',
        '`blood_product_group` AS `blood_product_group`',
        '`blood_unit_group` AS `blood_unit_group`',
        '`patient_blood_group` AS `patient_blood_group`',
        '`quantity_value` AS `quantity_value`',
        '`quantity_raw` AS `quantity_raw`',
        '`begin_location` AS `begin_location`',
        '`end_location` AS `end_location`',
        '`unit_is_irradiated` AS `unit_is_irradiated`',
        '`unit_is_cmv_neg` AS `unit_is_cmv_neg`',
        '`requires_irradiated` AS `requires_irradiated`',
        '`requires_cmv_neg` AS `requires_cmv_neg`',
        '`ambiguity_ind` AS `ambiguity_ind`',
        '`product_concept_id` AS `product_concept_id`',
        '`product_concept_name` AS `product_concept_name`',
        '`unit_group_concept_id` AS `unit_group_concept_id`',
        '`patient_group_concept_id` AS `patient_group_concept_id`',
        '`confidentiality_code` AS `confidentiality_code`',
        '`vip_ind` AS `vip_ind`',
        '`withheld_identity_ind` AS `withheld_identity_ind`',
        '`record_status` AS `record_status`',
        '`record_status_effective_from` AS `record_status_effective_from`',
        '`record_status_effective_to` AS `record_status_effective_to`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
        '`end_quantity_unit_inferred` AS `end_quantity_unit_inferred`',  # INSR_W3 W3.12
        '`end_quantity_ml` AS `end_quantity_ml`',  # INSR_W3 W3.12
        '`quantity_unit_rule` AS `quantity_unit_rule`',  # INSR_W3 W3.12
        '`isbt_product_concept_id` AS `isbt_product_concept_id`',  # INSR_W3 W3.12
        '`isbt_product_concept_name` AS `isbt_product_concept_name`',  # INSR_W3 W3.12
        '`isbt_mapping_method` AS `isbt_mapping_method`',  # INSR_W3 W3.12
        '`isbt_mapping_rule_id` AS `isbt_mapping_rule_id`',  # INSR_W3 W3.12
        '`isbt_mapping_model` AS `isbt_mapping_model`',  # INSR_W3 W3.12
        '`isbt_mapping_score` AS `isbt_mapping_score`',  # INSR_W3 W3.12
        '`product_mapping_method` AS `product_mapping_method`',  # INSR_W3 W3.12
        '`product_mapping_rule_id` AS `product_mapping_rule_id`',  # INSR_W3 W3.12
        '`unit_group_mapping_method` AS `unit_group_mapping_method`',  # INSR_W3 W3.12
        '`unit_group_mapping_rule_id` AS `unit_group_mapping_rule_id`',  # INSR_W3 W3.12
        '`patient_group_mapping_method` AS `patient_group_mapping_method`',  # INSR_W3 W3.12
        '`patient_group_mapping_rule_id` AS `patient_group_mapping_rule_id`',  # INSR_W3 W3.12
        '`product_mapping_source` AS `product_mapping_source`',  # INSR_W3 W3.12
        '`source_mrn_norm` AS `source_mrn_norm`',  # INSR_W3 W3.12
    )
    return df


RULES = {
    'clinical_transfusion': GoldRules(
        table='clinical_transfusion',
        select=CLINICAL_TRANSFUSION_SELECT,
        mandatory=CLINICAL_TRANSFUSION_MANDATORY_RULES,
        advisory=CLINICAL_TRANSFUSION_ADVISORY_RULES,
        person_scope=CLINICAL_TRANSFUSION_PERSON_SCOPE_RULES,
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


COMMENTS = {'_qc_clinical_transfusion': {'columns': {'ambiguity_ind': 'Pairing ambiguity indicator.',
                                          'begin_datetime': 'Paired transfusion begin timestamp.',
                                          'begin_location': 'Begin workflow location.',
                                          'blood_product_group': 'Normalized blood product group.',
                                          'blood_unit_group': 'Blood group recorded on the unit.',
                                          'confidentiality_code': 'Security classification when supplied.',
                                          'elapsed_minutes': 'Elapsed transfusion minutes when calculable.',
                                          'encounter_id': 'Nullable Millennium ENCNTR_ID; BloodTrack '
                                                          'transfusion rows do not supply encounter context.',
                                          'end_datetime': 'Paired transfusion end timestamp.',
                                          'end_location': 'End workflow location.',
                                          'end_quantity_ml': 'END_QUANTITY_VALUE when the inferred unit is '
                                                             'ML. (bronze END_QUANTITY_ML, INSR W1). INSR_W2 '
                                                             'W2.20.',
                                          'end_quantity_unit_inferred': 'BT_QTY_UNIT_V1 inferred unit of '
                                                                        'END_QUANTITY_VALUE. (bronze '
                                                                        'END_QUANTITY_UNIT_INFERRED, INSR '
                                                                        'W1). INSR_W2 W2.20.',
                                          'event_datetime': 'Transfusion begin timestamp falling back to '
                                                            'end.',
                                          'event_datetime_local': 'Transfusion begin timestamp, falling back '
                                                                  'to end, expressed in local time.',
                                          'event_end_datetime': 'Transfusion end timestamp.',
                                          'event_end_datetime_local': 'Transfusion end timestamp expressed '
                                                                      'in local time. Gold: NULL wherever '
                                                                      'Gold nulls event_end_datetime.',
                                          'isbt_mapping_method': 'EXACT_NAME, EMBEDDING_LLM_VERIFIED or '
                                                                 'GROUP_RULE. (bronze ISBT_MAPPING_METHOD, '
                                                                 'INSR W1). INSR_W2 W2.20.',
                                          'isbt_mapping_model': 'Verifier model of the ISBT mapping. (bronze '
                                                                'ISBT_MAPPING_MODEL, INSR W1). INSR_W2 '
                                                                'W2.20.',
                                          'isbt_mapping_rule_id': 'Rule id of the ISBT mapping. (bronze '
                                                                  'ISBT_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                                  'W2.20.',
                                          'isbt_mapping_score': 'Similarity score of the ISBT mapping. '
                                                                '(bronze ISBT_MAPPING_SCORE, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                          'isbt_product_concept_id': 'SNOMED blood-product concept for the '
                                                                     'ISBT 128 PRODUCT_CODE. (bronze '
                                                                     'ISBT_PRODUCT_CONCEPT_ID, INSR W1). '
                                                                     'INSR_W2 W2.20.',
                                          'isbt_product_concept_name': 'Name of ISBT_PRODUCT_CONCEPT_ID. '
                                                                       '(bronze ISBT_PRODUCT_CONCEPT_NAME, '
                                                                       'INSR W1). INSR_W2 W2.20.',
                                          'loaded_at': 'Timestamp when the pipeline wrote this row to the '
                                                       'bronze table.',
                                          'patient_blood_group': 'Patient blood group at transfusion.',
                                          'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                               'cross-feed join key.',
                                          'patient_group_concept_id': 'Mapped patient blood-group concept '
                                                                      'identifier.',
                                          'patient_group_mapping_method': 'PATIENT_GROUP map provenance '
                                                                          '(method). (bronze '
                                                                          'PATIENT_GROUP_MAPPING_METHOD, '
                                                                          'INSR W1). INSR_W2 W2.20.',
                                          'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance '
                                                                           '(rule_id). (bronze '
                                                                           'PATIENT_GROUP_MAPPING_RULE_ID, '
                                                                           'INSR W1). INSR_W2 W2.20.',
                                          'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                       'foreign key.',
                                          'product_concept_id': 'Mapped SNOMED device concept identifier.',
                                          'product_concept_name': 'Mapped product concept display.',
                                          'product_mapping_method': 'PRODUCT map provenance (method). '
                                                                    '(bronze PRODUCT_MAPPING_METHOD, INSR '
                                                                    'W1). INSR_W2 W2.20.',
                                          'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). '
                                                                     '(bronze PRODUCT_MAPPING_RULE_ID, INSR '
                                                                     'W1). INSR_W2 W2.20.',
                                          'product_mapping_source': 'Map table the product concept came '
                                                                    'from. (bronze PRODUCT_MAPPING_SOURCE, '
                                                                    'INSR W1). INSR_W2 W2.20.',
                                          'quantity_raw': 'Verbatim quantity text.',
                                          'quantity_unit_rule': 'Rule id of the unit inference '
                                                                '(BT_QTY_UNIT_V1). (bronze '
                                                                'QUANTITY_UNIT_RULE, INSR W1). INSR_W2 '
                                                                'W2.20.',
                                          'quantity_value': 'Parsed transfused quantity.',
                                          'record_status': 'Superseded when '
                                                           'map_bloodtrack_transfusion.IS_CURRENT_IN_SOURCE '
                                                           'is false, otherwise active; a missing flag '
                                                           'defaults true. No retracted status is emitted. '
                                                           'TRANSFUSION_STATUS and ambiguity indicators are '
                                                           'separate, while usable-code filtering controls '
                                                           'publication eligibility.',
                                          'record_status_effective_from': 'First non-null '
                                                                          'map_bloodtrack_transfusion.BEGIN_TS '
                                                                          'then END_TS, also used as this '
                                                                          "row's clinical event time. This "
                                                                          'is a begin/end-time proxy, not an '
                                                                          'independently observed status '
                                                                          'transition; no event-time clamp '
                                                                          'is applied.',
                                          'record_status_effective_to': 'map_bloodtrack_transfusion.PIPELINE_LOADED_AT '
                                                                        'when IS_CURRENT_IN_SOURCE is false; '
                                                                        'otherwise null. A missing '
                                                                        'processing timestamp remains null; '
                                                                        'neither BEGIN_TS nor END_TS is used '
                                                                        'as a fallback.',
                                          'requires_cmv_neg': 'Patient requires CMV-negative product.',
                                          'requires_irradiated': 'Patient requires irradiated product.',
                                          'source_code': 'ISBT product code with product display fallback.',
                                          'source_coding_system': 'BloodTrack product coding system.',
                                          'source_display': 'Blood product description.',
                                          'source_mrn_norm': 'Digits-only normalisation of '
                                                             'SOURCE_PATIENT_NUMBER (the BloodTrack MRN) '
                                                             'used for MRN linkage and TRANSFUSION_KEY. '
                                                             'Identifier, published and tagged. '
                                                             'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                             'SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.',
                                          'source_update_timestamp': 'Always null as a TIMESTAMP in the '
                                                                     'current transfusion projection. '
                                                                     'PIPELINE_LOADED_AT is published '
                                                                     'separately as loaded_at and is not '
                                                                     'copied into this field; no native '
                                                                     'update, clinical-event or Silver '
                                                                     'refresh timestamp is substituted.',
                                          'subject_id_system': 'Identifier system used for subject_key.',
                                          'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                         'identifier. Not salted, not secret; a stable join '
                                                         'key across feeds where person_id is unresolved.',
                                          'transfusion_key': 'BloodTrack TRANSFUSION_KEY; native primary key '
                                                             'of this table.',
                                          'transfusion_status': 'Pairing and clock-quality outcome.',
                                          'unit_group_concept_id': 'Mapped unit blood-group concept '
                                                                   'identifier.',
                                          'unit_group_mapping_method': 'UNIT_GROUP map provenance (method). '
                                                                       '(bronze UNIT_GROUP_MAPPING_METHOD, '
                                                                       'INSR W1). INSR_W2 W2.20.',
                                          'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance '
                                                                        '(rule_id). (bronze '
                                                                        'UNIT_GROUP_MAPPING_RULE_ID, INSR '
                                                                        'W1). INSR_W2 W2.20.',
                                          'unit_is_cmv_neg': 'Unit CMV-negative flag.',
                                          'unit_is_irradiated': 'Unit irradiation flag.',
                                          'unit_number': 'Blood unit number.',
                                          'vip_ind': 'VIP indicator when supplied.',
                                          'withheld_identity_ind': 'Identity-withholding status is not '
                                                                   'populated by the current source '
                                                                   'projections; null does not mean identity '
                                                                   'was not withheld.'},
                              'quantity_override': True,
                              'table': 'Internal quality-controlled twin of clinical_transfusion: the Gold '
                                       'repairs, nulls and expectations are applied here and the published '
                                       'Gold MV reads this object. Materialized rather than temporary so the '
                                       'published MV can refresh incrementally.'},
 'clinical_transfusion': {'columns': {'ambiguity_ind': 'Pairing ambiguity indicator.',
                                      'begin_datetime': 'Paired transfusion begin timestamp.',
                                      'begin_location': 'Begin workflow location.',
                                      'blood_product_group': 'Normalized blood product group.',
                                      'blood_unit_group': 'Blood group recorded on the unit.',
                                      'confidentiality_code': 'Security classification when supplied.',
                                      'elapsed_minutes': 'Elapsed transfusion minutes when calculable.',
                                      'encounter_id': 'Nullable Millennium ENCNTR_ID; BloodTrack transfusion '
                                                      'rows do not supply encounter context.',
                                      'end_datetime': 'Paired transfusion end timestamp.',
                                      'end_location': 'End workflow location.',
                                      'end_quantity_ml': 'END_QUANTITY_VALUE when the inferred unit is ML. '
                                                         '(bronze END_QUANTITY_ML, INSR W1). INSR_W2 W2.20.',
                                      'end_quantity_unit_inferred': 'BT_QTY_UNIT_V1 inferred unit of '
                                                                    'END_QUANTITY_VALUE. (bronze '
                                                                    'END_QUANTITY_UNIT_INFERRED, INSR W1). '
                                                                    'INSR_W2 W2.20.',
                                      'event_datetime': 'Transfusion begin timestamp falling back to end.',
                                      'event_datetime_local': 'Transfusion begin timestamp, falling back to '
                                                              'end, expressed in local time.',
                                      'event_end_datetime': 'Transfusion end timestamp.',
                                      'event_end_datetime_local': 'Transfusion end timestamp expressed in '
                                                                  'local time. Gold: NULL wherever Gold '
                                                                  'nulls event_end_datetime.',
                                      'isbt_mapping_method': 'EXACT_NAME, EMBEDDING_LLM_VERIFIED or '
                                                             'GROUP_RULE. (bronze ISBT_MAPPING_METHOD, INSR '
                                                             'W1). INSR_W2 W2.20.',
                                      'isbt_mapping_model': 'Verifier model of the ISBT mapping. (bronze '
                                                            'ISBT_MAPPING_MODEL, INSR W1). INSR_W2 W2.20.',
                                      'isbt_mapping_rule_id': 'Rule id of the ISBT mapping. (bronze '
                                                              'ISBT_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                              'W2.20.',
                                      'isbt_mapping_score': 'Similarity score of the ISBT mapping. (bronze '
                                                            'ISBT_MAPPING_SCORE, INSR W1). INSR_W2 W2.20.',
                                      'isbt_product_concept_id': 'SNOMED blood-product concept for the ISBT '
                                                                 '128 PRODUCT_CODE. (bronze '
                                                                 'ISBT_PRODUCT_CONCEPT_ID, INSR W1). INSR_W2 '
                                                                 'W2.20.',
                                      'isbt_product_concept_name': 'Name of ISBT_PRODUCT_CONCEPT_ID. (bronze '
                                                                   'ISBT_PRODUCT_CONCEPT_NAME, INSR W1). '
                                                                   'INSR_W2 W2.20.',
                                      'loaded_at': 'Timestamp when the pipeline wrote this row to the bronze '
                                                   'table.',
                                      'patient_blood_group': 'Patient blood group at transfusion.',
                                      'patient_event_key': 'Deterministic SHA-256 event identity; stable '
                                                           'cross-feed join key.',
                                      'patient_group_concept_id': 'Mapped patient blood-group concept '
                                                                  'identifier.',
                                      'patient_group_mapping_method': 'PATIENT_GROUP map provenance '
                                                                      '(method). (bronze '
                                                                      'PATIENT_GROUP_MAPPING_METHOD, INSR '
                                                                      'W1). INSR_W2 W2.20.',
                                      'patient_group_mapping_rule_id': 'PATIENT_GROUP map provenance '
                                                                       '(rule_id). (bronze '
                                                                       'PATIENT_GROUP_MAPPING_RULE_ID, INSR '
                                                                       'W1). INSR_W2 W2.20.',
                                      'person_id': 'Millennium PERSON_ID when resolved; native person '
                                                   'foreign key.',
                                      'product_concept_id': 'Mapped SNOMED device concept identifier.',
                                      'product_concept_name': 'Mapped product concept display.',
                                      'product_mapping_method': 'PRODUCT map provenance (method). (bronze '
                                                                'PRODUCT_MAPPING_METHOD, INSR W1). INSR_W2 '
                                                                'W2.20.',
                                      'product_mapping_rule_id': 'PRODUCT map provenance (rule_id). (bronze '
                                                                 'PRODUCT_MAPPING_RULE_ID, INSR W1). INSR_W2 '
                                                                 'W2.20.',
                                      'product_mapping_source': 'Map table the product concept came from. '
                                                                '(bronze PRODUCT_MAPPING_SOURCE, INSR W1). '
                                                                'INSR_W2 W2.20.',
                                      'quantity_raw': 'Verbatim quantity text.',
                                      'quantity_unit_rule': 'Rule id of the unit inference (BT_QTY_UNIT_V1). '
                                                            '(bronze QUANTITY_UNIT_RULE, INSR W1). INSR_W2 '
                                                            'W2.20.',
                                      'quantity_value': 'Parsed transfused quantity.',
                                      'record_status': 'Superseded when '
                                                       'map_bloodtrack_transfusion.IS_CURRENT_IN_SOURCE is '
                                                       'false, otherwise active; a missing flag defaults '
                                                       'true. No retracted status is emitted. '
                                                       'TRANSFUSION_STATUS and ambiguity indicators are '
                                                       'separate, while usable-code filtering controls '
                                                       'publication eligibility.',
                                      'record_status_effective_from': 'First non-null '
                                                                      'map_bloodtrack_transfusion.BEGIN_TS '
                                                                      "then END_TS, also used as this row's "
                                                                      'clinical event time. This is a '
                                                                      'begin/end-time proxy, not an '
                                                                      'independently observed status '
                                                                      'transition; no event-time clamp is '
                                                                      'applied.',
                                      'record_status_effective_to': 'map_bloodtrack_transfusion.PIPELINE_LOADED_AT '
                                                                    'when IS_CURRENT_IN_SOURCE is false; '
                                                                    'otherwise null. A missing processing '
                                                                    'timestamp remains null; neither '
                                                                    'BEGIN_TS nor END_TS is used as a '
                                                                    'fallback.',
                                      'requires_cmv_neg': 'Patient requires CMV-negative product.',
                                      'requires_irradiated': 'Patient requires irradiated product.',
                                      'source_code': 'ISBT product code with product display fallback.',
                                      'source_coding_system': 'BloodTrack product coding system.',
                                      'source_display': 'Blood product description.',
                                      'source_mrn_norm': 'Digits-only normalisation of SOURCE_PATIENT_NUMBER '
                                                         '(the BloodTrack MRN) used for MRN linkage and '
                                                         'TRANSFUSION_KEY. Identifier, published and tagged. '
                                                         'INSR_W1_IDENTIFIER_RETAIN_V1 (bronze '
                                                         'SOURCE_MRN_NORM, INSR W1). INSR_W2 W2.20.',
                                      'source_update_timestamp': 'Always null as a TIMESTAMP in the current '
                                                                 'transfusion projection. PIPELINE_LOADED_AT '
                                                                 'is published separately as loaded_at and '
                                                                 'is not copied into this field; no native '
                                                                 'update, clinical-event or Silver refresh '
                                                                 'timestamp is substituted.',
                                      'subject_id_system': 'Identifier system used for subject_key.',
                                      'subject_key': 'Deterministic SHA-256 over the strongest source '
                                                     'identifier. Not salted, not secret; a stable join key '
                                                     'across feeds where person_id is unresolved.',
                                      'transfusion_key': 'BloodTrack TRANSFUSION_KEY; native primary key of '
                                                         'this table.',
                                      'transfusion_status': 'Pairing and clock-quality outcome.',
                                      'unit_group_concept_id': 'Mapped unit blood-group concept identifier.',
                                      'unit_group_mapping_method': 'UNIT_GROUP map provenance (method). '
                                                                   '(bronze UNIT_GROUP_MAPPING_METHOD, INSR '
                                                                   'W1). INSR_W2 W2.20.',
                                      'unit_group_mapping_rule_id': 'UNIT_GROUP map provenance (rule_id). '
                                                                    '(bronze UNIT_GROUP_MAPPING_RULE_ID, '
                                                                    'INSR W1). INSR_W2 W2.20.',
                                      'unit_is_cmv_neg': 'Unit CMV-negative flag.',
                                      'unit_is_irradiated': 'Unit irradiation flag.',
                                      'unit_number': 'Blood unit number.',
                                      'vip_ind': 'VIP indicator when supplied.',
                                      'withheld_identity_ind': 'Identity-withholding status is not populated '
                                                               'by the current source projections; null does '
                                                               'not mean identity was not withheld.'},
                          'quantity_override': False,
                          'table': 'One BloodTrack unit-recipient transfusion episode with paired begin/end '
                                   'evidence and mapped product and blood-group concepts. Gold QC twin of '
                                   'the silver product: 2 columns are repaired or nulled, 1 rule(s) drop '
                                   'rows, 3 check(s) are advisory. Each rule states its reason in the '
                                   'pipeline notebook, and Lakeflow expectation metrics report what every '
                                   'rule matched on each update. Coverage: BloodTrack transfusion records '
                                   'start on 2023-05-02 (go-live); there are no earlier rows at source; '
                                   'complete-through month per release: 6_mgmt.data_quality.release_finding '
                                   '(finding_code F2_complete_through).'}}
