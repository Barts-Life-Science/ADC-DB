"""Gold specialist.registry_coronary_lesion: 2 dataset(s).

- datasets: _qc_clinical_registry_coronary_lesion, clinical_registry_coronary_lesion
- moved verbatim from: 3_gold_specialist_care:3631, 3_gold_specialist_care:3793, 3_gold_specialist_care:3799, 3_gold_specialist_care:3814

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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


CLINICAL_REGISTRY_CORONARY_LESION_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_coronary_lesion.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_coronary_lesion"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_CORONARY_LESION_MANDATORY_RULES)
def _gold_qc_clinical_registry_coronary_lesion():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_coronary_lesion"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_CORONARY_LESION_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_coronary_lesion"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_coronary_lesion():
    """Publish clinical_registry_coronary_lesion without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_coronary_lesion")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_coronary_lesion': {'columns': {'adc_updt': 'Timestamp of the last update applied to '
                                                                   'the record in the upstream iWeb extract.',
                                                       'athero_thrombus_removal_devices_usedtxt': 'Coded '
                                                                                                  'text '
                                                                                                  'listing '
                                                                                                  'atherectomy '
                                                                                                  'or '
                                                                                                  'thrombus '
                                                                                                  'removal '
                                                                                                  'devices '
                                                                                                  'used '
                                                                                                  'during '
                                                                                                  'the '
                                                                                                  'lesion '
                                                                                                  'treatment, '
                                                                                                  'including '
                                                                                                  'a none '
                                                                                                  'option.',
                                                       'bifurcation': 'Indicator of whether the treated '
                                                                      'lesion involves a coronary '
                                                                      'bifurcation.',
                                                       'brachytherapy_devices_usedtxt': 'Coded text listing '
                                                                                        'brachytherapy '
                                                                                        'devices used during '
                                                                                        'the lesion '
                                                                                        'treatment, '
                                                                                        'including a none '
                                                                                        'option.',
                                                       'cath_proceduretxt': 'Coded text description of the '
                                                                            'catheter laboratory procedure '
                                                                            'performed for this lesion or '
                                                                            'vessel record.',
                                                       'catheter_size': 'Recorded size of the guide catheter '
                                                                        'used for the lesion, held as text '
                                                                        'in the source registry.',
                                                       'chronic_lesion': 'Indicator of whether the lesion is '
                                                                         'a chronic total or long-standing '
                                                                         'occlusion.',
                                                       'coronary_dominance': 'Coded classification of '
                                                                             'coronary circulation dominance '
                                                                             'recorded at angiography.',
                                                       'date_last_changed': 'Source edit timestamp; it must '
                                                                            'not be interpreted as the '
                                                                            'procedure date.',
                                                       'date_of_birth': 'Patient date of birth as recorded '
                                                                        'in the iWeb coronary registry and '
                                                                        'used for patient linkage.',
                                                       'date_of_death': 'Recorded date of death for the '
                                                                        'patient as held in the iWeb '
                                                                        'coronary registry.',
                                                       'diagnostic_devices_used_during_proceduretxt': 'Coded '
                                                                                                      'text '
                                                                                                      'description '
                                                                                                      'of '
                                                                                                      'diagnostic '
                                                                                                      'devices '
                                                                                                      'used '
                                                                                                      'during '
                                                                                                      'the '
                                                                                                      'procedure '
                                                                                                      'for '
                                                                                                      'this '
                                                                                                      'lesion.',
                                                       'distal_anastomosis': 'Coded description of the '
                                                                             'distal anastomosis site of the '
                                                                             'graft to the coronary vessel.',
                                                       'distal_vessel_diameter': 'Angiographic diameter of '
                                                                                 'the vessel distal to the '
                                                                                 'lesion, as recorded in the '
                                                                                 'iWeb coronary registry.',
                                                       'drugs_eluted_by_stentstxt': 'Coded text description '
                                                                                    'of the drug-eluting '
                                                                                    'stent product or eluted '
                                                                                    'drug recorded for the '
                                                                                    'lesion.',
                                                       'emboli_protection_devices_usedtxt': 'Coded text '
                                                                                            'listing embolic '
                                                                                            'protection '
                                                                                            'devices used '
                                                                                            'during the '
                                                                                            'lesion '
                                                                                            'treatment, '
                                                                                            'including a '
                                                                                            'none option.',
                                                       'entry_id': 'Source registry identifier for the '
                                                                   'individual lesion or vessel '
                                                                   'sub-procedure record, carried through '
                                                                   'from the iWeb coronary sub-procedure '
                                                                   'table.',
                                                       'ethnic_origin': 'Coded patient ethnic origin as '
                                                                        'recorded in the iWeb coronary '
                                                                        'registry.',
                                                       'forename': 'Patient forename as recorded in the iWeb '
                                                                   'coronary registry and used for patient '
                                                                   'linkage.',
                                                       'fractional_flow_reserve': 'Fractional flow reserve '
                                                                                  'physiology measurement '
                                                                                  'recorded for the lesion '
                                                                                  'or vessel.',
                                                       'gender': 'Patient gender as recorded in the iWeb '
                                                                 'coronary registry.',
                                                       'graft_conduit': 'Coded description of the conduit '
                                                                        'used for the bypass graft to this '
                                                                        'vessel.',
                                                       'graft_lesion_position': 'Coded position of the '
                                                                                'lesion within the bypass '
                                                                                'graft.',
                                                       'graft_typetxt': 'Coded text describing the type of '
                                                                        'bypass graft associated with this '
                                                                        'vessel.',
                                                       'guide_cathetertxt': 'Coded text describing the guide '
                                                                            'catheter used for the lesion, '
                                                                            'as recorded in the iWeb '
                                                                            'coronary registry.',
                                                       'guide_extension_size': 'Recorded size of the guide '
                                                                               'extension catheter used for '
                                                                               'the lesion, held as text in '
                                                                               'the source registry.',
                                                       'guide_extension_used': 'Indicator of whether a guide '
                                                                               'extension catheter was used '
                                                                               'during treatment of the '
                                                                               'lesion.',
                                                       'guide_extensiontxt': 'Coded text describing the '
                                                                             'guide extension catheter used '
                                                                             'for the lesion, as recorded in '
                                                                             'the iWeb coronary registry.',
                                                       'import_child_id': 'Identifier of the child record '
                                                                          'carried over from a data import '
                                                                          'into the iWeb coronary registry.',
                                                       'in_graft_stenosis': 'Indicator of whether the '
                                                                            'stenosis is located within a '
                                                                            'bypass graft.',
                                                       'indication_for_stent': 'Recorded clinical indication '
                                                                               'for stent use in this '
                                                                               'lesion.',
                                                       'instent_stenoses': 'Indicator of whether the '
                                                                           'stenosis is located within a '
                                                                           'previously implanted stent.',
                                                       'is_current_in_source': 'Flag indicating whether the '
                                                                               'record is still present as a '
                                                                               'current row in the source '
                                                                               'system.',
                                                       'is_there_a_graft_to_this_vessel': 'Indicator of '
                                                                                          'whether a bypass '
                                                                                          'graft supplies '
                                                                                          'the vessel being '
                                                                                          'described.',
                                                       'largest_ballon_diameter': 'Largest balloon diameter '
                                                                                  'used when treating this '
                                                                                  'lesion, as recorded in '
                                                                                  'the registry (source '
                                                                                  "spelling of 'ballon' "
                                                                                  'retained).',
                                                       'largest_stent_diameter': 'Largest stent diameter '
                                                                                 'deployed in this lesion, '
                                                                                 'as recorded in the '
                                                                                 'registry.',
                                                       'length_of_segment_treated': 'Recorded length of the '
                                                                                    'coronary segment '
                                                                                    'treated for this '
                                                                                    'lesion.',
                                                       'lesion': 'Coronary vessel segment or lesion location '
                                                                 'treated or assessed in this sub-procedure '
                                                                 'record.',
                                                       'linkage_fallback_conflict_ind': 'Flag indicating '
                                                                                        'that fallback '
                                                                                        'linkage produced '
                                                                                        'conflicting person '
                                                                                        'matches for this '
                                                                                        'record.',
                                                       'linkage_historical_fallback_ind': 'Flag indicating '
                                                                                          'that linkage was '
                                                                                          'achieved using a '
                                                                                          'historical rather '
                                                                                          'than current '
                                                                                          'person '
                                                                                          'identifier.',
                                                       'linkage_method': 'Identifier combination used to '
                                                                         'link this registry record to the '
                                                                         'Millennium person record.',
                                                       'linkage_status': 'Outcome status of the patient '
                                                                         'linkage process matching this '
                                                                         'registry record to a Millennium '
                                                                         'person record.',
                                                       'lms_protected': 'Indicator of whether the left main '
                                                                        'stem lesion was protected by a '
                                                                        'patent bypass graft.',
                                                       'm_comment_on_lesion': 'Free-text clinical comment '
                                                                              'recorded about the lesion or '
                                                                              'its treatment in the iWeb '
                                                                              'coronary registry.',
                                                       'medina_classification': 'Medina classification of '
                                                                                'the bifurcation lesion.',
                                                       'minimal_lumen_diameter': 'Angiographic minimal lumen '
                                                                                 'diameter measured at the '
                                                                                 'treated lesion, as '
                                                                                 'recorded in the iWeb '
                                                                                 'coronary registry.',
                                                       'minimal_reference_diameter': 'Minimal reference '
                                                                                     'vessel diameter '
                                                                                     'measured for this '
                                                                                     'lesion, as recorded in '
                                                                                     'the registry.',
                                                       'mrn': 'Local medical record number of the patient '
                                                              'whose coronary lesion record this is, as held '
                                                              'in the iWeb registry and used for linkage to '
                                                              'the master patient index.',
                                                       'n_cpost_dilation': 'Indicator of whether a '
                                                                           'non-compliant balloon was used '
                                                                           'for post-dilation of the lesion.',
                                                       'n_cpre_dilation': 'Indicator of whether a '
                                                                          'non-compliant balloon was used '
                                                                          'for pre-dilation of the lesion.',
                                                       'nhs_number': 'Patient NHS number as held in the iWeb '
                                                                     'coronary registry, used with the '
                                                                     'validation flag for linkage to the '
                                                                     'Millennium person record.',
                                                       'nhs_number_valid_ind': 'Flag indicating whether the '
                                                                               'source NHS number passed '
                                                                               'validation checks during '
                                                                               'linkage processing.',
                                                       'number_of_drug_eluting_stents_used': 'Number of '
                                                                                             'drug-eluting '
                                                                                             'stents '
                                                                                             'deployed in '
                                                                                             'this lesion, '
                                                                                             'held as text '
                                                                                             'in the source '
                                                                                             'registry.',
                                                       'number_of_stents_used': 'Number of stents deployed '
                                                                                'in this lesion, held as '
                                                                                'text in the source '
                                                                                'registry.',
                                                       'other_guide_extension': 'Free-text description of a '
                                                                                'guide extension device not '
                                                                                'available in the coded '
                                                                                'guide extension list.',
                                                       'parent_entry_id': 'Foreign key to the currently '
                                                                          'un-landed iWeb coronary procedure '
                                                                          'registry.',
                                                       'patient_event_key': 'Deterministic SHA-256 key '
                                                                            'matching '
                                                                            'clinical_registry_entry.patient_event_key '
                                                                            'for the coronary_lesion source '
                                                                            'family and native entry_id.',
                                                       'person_id': 'Internal Millennium person identifier '
                                                                    'linked to this registry record.',
                                                       'pipeline_loaded_at': 'Timestamp recording when the '
                                                                             'pipeline loaded this record '
                                                                             'into the bronze table.',
                                                       'post_dilation_balloon_dilation': 'Diameter of the '
                                                                                         'balloon used for '
                                                                                         'post-dilation of '
                                                                                         'the lesion, as '
                                                                                         'recorded in the '
                                                                                         'registry.',
                                                       'post_dilation_balloon_length': 'Length of the '
                                                                                       'balloon used for '
                                                                                       'post-dilation of the '
                                                                                       'lesion, as recorded '
                                                                                       'in the registry.',
                                                       'pre_dilation_balloon_diameter': 'Diameter of the '
                                                                                        'balloon used for '
                                                                                        'pre-dilation of the '
                                                                                        'lesion, as recorded '
                                                                                        'in the registry.',
                                                       'pre_dilation_balloon_length': 'Length of the balloon '
                                                                                      'used for pre-dilation '
                                                                                      'of the lesion, as '
                                                                                      'recorded in the '
                                                                                      'registry.',
                                                       'procedural_devices_usedtxt': 'Coded text listing '
                                                                                     'other procedural '
                                                                                     'devices used during '
                                                                                     'the lesion treatment, '
                                                                                     'including a none '
                                                                                     'option.',
                                                       'proceed_to_pci': 'Indicator of whether the lesion '
                                                                         'proceeded to percutaneous coronary '
                                                                         'intervention.',
                                                       'proximal_vessel_diameter': 'Angiographic diameter of '
                                                                                   'the vessel proximal to '
                                                                                   'the lesion, as recorded '
                                                                                   'in the iWeb coronary '
                                                                                   'registry.',
                                                       'restenosis': 'Indicator of whether the lesion '
                                                                     'represents restenosis of a previously '
                                                                     'treated site.',
                                                       'row_hash': "Hash of the source record's field values "
                                                                   'used to detect changes between loads.',
                                                       'source_absent_detected_ts': 'Timestamp recording '
                                                                                    'when the pipeline first '
                                                                                    'detected that this '
                                                                                    'record was no longer '
                                                                                    'present in the upstream '
                                                                                    'iWeb source.',
                                                       'source_present_ind': 'Canonical boolean mirror of '
                                                                             'IS_CURRENT_IN_SOURCE; false '
                                                                             'rows are retained source '
                                                                             'tombstones.',
                                                       'source_record_key': 'Business key identifying the '
                                                                            'source registry record, derived '
                                                                            'from the iWeb sub-procedure '
                                                                            'entry identifier.',
                                                       'source_table': 'Name of the upstream source table '
                                                                       'from which the record was ingested.',
                                                       'stenosis_post_pci': 'Banded percentage residual '
                                                                            'stenosis of the lesion assessed '
                                                                            'after percutaneous coronary '
                                                                            'intervention.',
                                                       'stenosispre_pci': 'Banded percentage stenosis of the '
                                                                          'lesion assessed before '
                                                                          'percutaneous coronary '
                                                                          'intervention.',
                                                       'stents_usedtxt': 'Coded text description of the '
                                                                         'stent products used in this '
                                                                         'lesion.',
                                                       'successful': 'Indicator of whether the intervention '
                                                                     'on this lesion was recorded as '
                                                                     'successful.',
                                                       'surname': 'Patient surname as recorded in the iWeb '
                                                                  'coronary registry and used for patient '
                                                                  'linkage.',
                                                       'wire_usedtxt': 'Coded text describing the guidewire '
                                                                       'used for the lesion.'},
                                           'quantity_override': True,
                                           'table': 'Internal QC of clinical_registry_coronary_lesion; '
                                                    'counts rows whose Gold parent is absent.'},
 'clinical_registry_coronary_lesion': {'columns': {'parent_entry_id': 'entry_id of the parent coronary '
                                                                      'procedure in '
                                                                      'clinical_registry_coronary_procedure, '
                                                                      'which carries the procedure date.'},
                                       'quantity_override': False,
                                       'table': 'One row per coronary lesion recorded in the iWeb coronary '
                                                'registry; it links to its procedure through the registry '
                                                'entry, not to an encounter. Coverage: iWeb registries start '
                                                'at different years: coronary 2013, non-coronary 2016, '
                                                'surgery 2017, cardiac MDT 2018 (CT aortic 2021), mortality '
                                                'review 2018, ERACS 2025, ACS transfer 2000; '
                                                'complete-through month per release: '
                                                '6_mgmt.data_quality.release_finding (finding_code '
                                                'F2_complete_through).'}}
