"""Gold specialist.registry_surgery_followup: 2 dataset(s).

- datasets: _qc_clinical_registry_surgery_followup, clinical_registry_surgery_followup
- moved verbatim from: 3_gold_specialist_care:5783, 3_gold_specialist_care:5919, 3_gold_specialist_care:5925, 3_gold_specialist_care:5940

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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


CLINICAL_REGISTRY_SURGERY_FOLLOWUP_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_surgery_followup.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_surgery_followup"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_SURGERY_FOLLOWUP_MANDATORY_RULES)
def _gold_qc_clinical_registry_surgery_followup():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_surgery_followup"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_SURGERY_FOLLOWUP_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_surgery_followup"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_surgery_followup():
    """Publish clinical_registry_surgery_followup without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_surgery_followup")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_surgery_followup': {'columns': {'adc_updt': 'Pipeline update timestamp for the '
                                                                    'record, used with DATE_LAST_CHANGED to '
                                                                    'deterministically select the latest row '
                                                                    'per EntryId.',
                                                        'anticoagulation_commenced': 'Indicator of whether '
                                                                                     'anticoagulation '
                                                                                     'therapy was commenced '
                                                                                     'following surgery.',
                                                        'atrial_fibrillation': 'Indicator of whether '
                                                                               'post-operative atrial '
                                                                               'fibrillation was recorded at '
                                                                               'follow-up.',
                                                        'date_last_changed': 'Timestamp of the last change '
                                                                             'to the record in the iWeb '
                                                                             'source system, used with '
                                                                             'ADC_UPDT to select the latest '
                                                                             'row per EntryId.',
                                                        'date_of_birth': "Patient's date of birth as "
                                                                         'recorded in the iWeb CS2010G '
                                                                         'cardiac surgery follow-up record.',
                                                        'date_of_death': "Date of the patient's death as "
                                                                         'recorded in the CS2010G follow-up '
                                                                         'registry.',
                                                        'discharge_dt': 'Date and time the patient was '
                                                                        'discharged following the cardiac '
                                                                        'surgery episode.',
                                                        'entry_id': 'Stable iWeb follow-up key after '
                                                                    'deterministic latest-row deduplication.',
                                                        'ethnic_origin': 'Coded ethnic origin of the patient '
                                                                         'recorded in the CS2010G follow-up '
                                                                         'registry.',
                                                        'excess_fluid_overload': 'Recorded presence or '
                                                                                 'management of excess fluid '
                                                                                 'overload during '
                                                                                 'post-operative cardiac '
                                                                                 'surgery follow-up, as '
                                                                                 'entered in the iWeb '
                                                                                 'CS2010G follow-up form.',
                                                        'extubation_dt': 'Date and time the patient was '
                                                                         'extubated following cardiac '
                                                                         'surgery.',
                                                        'flw_id': 'Internal iWeb identifier for the '
                                                                  'follow-up entry within the CS2010G '
                                                                  'registry record.',
                                                        'follow_up_arrangements': 'Free-text description of '
                                                                                  'the arranged '
                                                                                  'post-discharge follow-up, '
                                                                                  'such as the service or '
                                                                                  'centre responsible for '
                                                                                  'review.',
                                                        'forename': "Patient's forename as recorded in the "
                                                                    'iWeb CS2010G cardiac surgery follow-up '
                                                                    'record.',
                                                        'gender': 'Recorded gender of the patient as '
                                                                  'captured in the CS2010G follow-up '
                                                                  'registry.',
                                                        'harvest_wound_infection_treatmenttxt': 'Text '
                                                                                                'description '
                                                                                                'of '
                                                                                                'treatment '
                                                                                                'given for '
                                                                                                'conduit '
                                                                                                'harvest '
                                                                                                'site wound '
                                                                                                'infection.',
                                                        'hd_udischarge_dt': 'Date and time the patient was '
                                                                            'discharged from the high '
                                                                            'dependency unit following '
                                                                            'cardiac surgery.',
                                                        'import_child_id': 'Source system import identifier '
                                                                           'for the child follow-up record, '
                                                                           'carried through from the iWeb '
                                                                           'registry load process.',
                                                        'indication_for_ppm': 'Records the clinical '
                                                                              'indication for permanent '
                                                                              'pacemaker implantation noted '
                                                                              'at post-operative follow-up.',
                                                        'investigations_satisfactory_prior_to_discharge': 'Record '
                                                                                                          'of '
                                                                                                          'which '
                                                                                                          'investigations '
                                                                                                          'were '
                                                                                                          'satisfactory '
                                                                                                          'before '
                                                                                                          'the '
                                                                                                          'patient '
                                                                                                          'was '
                                                                                                          'discharged.',
                                                        'is_current_in_source': 'Boolean flag indicating '
                                                                                'whether the record is still '
                                                                                'present as the current '
                                                                                'version in the upstream '
                                                                                'iWeb source.',
                                                        'is_dialysis_needed_post_discharge': 'Indicates '
                                                                                             'whether the '
                                                                                             'patient '
                                                                                             'required '
                                                                                             'dialysis after '
                                                                                             'discharge, as '
                                                                                             'recorded at '
                                                                                             'cardiac '
                                                                                             'surgery '
                                                                                             'follow-up.',
                                                        'it_udischarge_dt': 'Date and time the patient was '
                                                                            'discharged from the intensive '
                                                                            'therapy unit following cardiac '
                                                                            'surgery.',
                                                        'ix_satisfactory_prior_to_dischargetxt': 'Text list '
                                                                                                 'of '
                                                                                                 'investigations '
                                                                                                 'recorded '
                                                                                                 'as '
                                                                                                 'satisfactory '
                                                                                                 'prior to '
                                                                                                 'discharge '
                                                                                                 'in the '
                                                                                                 'iWeb '
                                                                                                 'CS2010G '
                                                                                                 'follow-up '
                                                                                                 'form.',
                                                        'length_of_intubation': 'Recorded duration of '
                                                                                'intubation following '
                                                                                'cardiac surgery, in the '
                                                                                'units captured by the '
                                                                                'source registry.',
                                                        'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                         'indicating that '
                                                                                         'the fallback '
                                                                                         'linkage attempt '
                                                                                         'produced '
                                                                                         'conflicting person '
                                                                                         'matches.',
                                                        'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                           'indicating that '
                                                                                           'person linkage '
                                                                                           'relied on a '
                                                                                           'historical '
                                                                                           '(previous) '
                                                                                           'identifier '
                                                                                           'rather than a '
                                                                                           'current one.',
                                                        'linkage_method': 'Code describing which identifier '
                                                                          'combination was used to link this '
                                                                          'follow-up record to the master '
                                                                          'person index during pipeline '
                                                                          'processing.',
                                                        'linkage_status': 'Derived status describing the '
                                                                          'outcome of linking this follow-up '
                                                                          'record to a Millennium person '
                                                                          'record.',
                                                        'lrt_itxt': 'Text description of lower respiratory '
                                                                    'tract infection details recorded at '
                                                                    'follow-up.',
                                                        'm_harvest_wound_infection_reoperation_type': 'Type '
                                                                                                      'of '
                                                                                                      'reoperation '
                                                                                                      'performed '
                                                                                                      'for '
                                                                                                      'harvest '
                                                                                                      'site '
                                                                                                      'wound '
                                                                                                      'infection.',
                                                        'm_local_clinic_details': 'Free-text details of the '
                                                                                  'local clinic arranged for '
                                                                                  "the patient's follow-up.",
                                                        'm_other_investigations_satisfactory': 'Free-text '
                                                                                               'detail of '
                                                                                               'other '
                                                                                               'investigations '
                                                                                               'found '
                                                                                               'satisfactory '
                                                                                               'prior to '
                                                                                               'discharge.',
                                                        'm_post_operative_echo_data': 'Free-text notes '
                                                                                      'capturing '
                                                                                      'post-operative '
                                                                                      'echocardiography '
                                                                                      'findings recorded at '
                                                                                      'follow-up.',
                                                        'm_sternal_wound_infection_reoperation_type': 'Type '
                                                                                                      'of '
                                                                                                      'reoperation '
                                                                                                      'performed '
                                                                                                      'for '
                                                                                                      'sternal '
                                                                                                      'wound '
                                                                                                      'infection.',
                                                        'maximal_depthmm': 'Maximal depth measurement in '
                                                                           'millimetres recorded for the '
                                                                           'associated follow-up finding, as '
                                                                           'captured in the iWeb CS2010G '
                                                                           'follow-up form.',
                                                        'mrn': 'Local hospital medical record number '
                                                               'identifying the patient for the follow-up '
                                                               'record.',
                                                        'new_cva': 'Indicates whether a new cerebrovascular '
                                                                   'accident was recorded during '
                                                                   'post-operative cardiac surgery '
                                                                   'follow-up.',
                                                        'nhs_number': "Patient's NHS number as recorded in "
                                                                      'the iWeb CS2010G cardiac surgery '
                                                                      'follow-up record, stored as text.',
                                                        'nhs_number_valid_ind': 'Derived flag indicating '
                                                                                'whether the source NHS '
                                                                                'number passed validation; '
                                                                                'the NHS number itself is '
                                                                                'not retained in this table.',
                                                        'pain_delaying_discharge': 'Indicates whether pain '
                                                                                   'was recorded as a factor '
                                                                                   "delaying the patient's "
                                                                                   'discharge after cardiac '
                                                                                   'surgery.',
                                                        'patient_event_key': 'Deterministic SHA-256 key '
                                                                             'matching '
                                                                             'clinical_registry_entry.patient_event_key '
                                                                             'for the surgery_followup '
                                                                             'source family and native '
                                                                             'entry_id.',
                                                        'patient_status_at_follow_up': 'Coded status of the '
                                                                                       'patient (such as '
                                                                                       'clinical/vital '
                                                                                       'status) recorded at '
                                                                                       'the follow-up '
                                                                                       'assessment.',
                                                        'peak_creatinine': 'Highest recorded post-operative '
                                                                           'serum creatinine value for the '
                                                                           'patient during the follow-up '
                                                                           'period, in the units captured by '
                                                                           'the source registry.',
                                                        'pericardial_effusion': 'Indicator of whether '
                                                                                'pericardial effusion was '
                                                                                'recorded at follow-up.',
                                                        'person_id': 'Internal Millennium person identifier '
                                                                     'linked to this follow-up record, used '
                                                                     'to join to patient-level tables.',
                                                        'pipeline_loaded_at': 'Timestamp recording when the '
                                                                              'row was loaded into this '
                                                                              'bronze table by the ingestion '
                                                                              'pipeline.',
                                                        'pleural_effusion': 'Indicates whether a pleural '
                                                                            'effusion was recorded during '
                                                                            'post-operative cardiac surgery '
                                                                            'follow-up.',
                                                        'pleural_effusion_management': 'Describes the '
                                                                                       'management or '
                                                                                       'treatment applied '
                                                                                       'for a recorded '
                                                                                       'post-operative '
                                                                                       'pleural effusion.',
                                                        'pneumothorax': 'Indicates whether a pneumothorax '
                                                                        'was recorded during post-operative '
                                                                        'cardiac surgery follow-up.',
                                                        'pneumothorax_management': 'Describes the management '
                                                                                   'or treatment applied for '
                                                                                   'a recorded '
                                                                                   'post-operative '
                                                                                   'pneumothorax.',
                                                        'post_operative_complicationtxt': 'Text description '
                                                                                          'of post-operative '
                                                                                          'complications '
                                                                                          'recorded at '
                                                                                          'follow-up, '
                                                                                          'typically a coded '
                                                                                          'value with its '
                                                                                          'descriptive '
                                                                                          'label.',
                                                        'post_operative_ileus': 'Indicator of whether '
                                                                                'post-operative ileus was '
                                                                                'recorded at follow-up.',
                                                        'post_operative_pyrexia_treatmenttxt': 'Text '
                                                                                               'description '
                                                                                               'of treatment '
                                                                                               'given for '
                                                                                               'post-operative '
                                                                                               'pyrexia.',
                                                        'procedure_status': 'Coded status of the cardiac '
                                                                            'surgical procedure recorded at '
                                                                            'follow-up.',
                                                        'reintubation_dt': 'Date and time the patient was '
                                                                           'reintubated following cardiac '
                                                                           'surgery.',
                                                        'resternotomy_for_bleeding_on_day': 'Post-operative '
                                                                                            'day number on '
                                                                                            'which '
                                                                                            'resternotomy '
                                                                                            'for bleeding '
                                                                                            'was performed.',
                                                        'row_hash': 'Deterministic hash of the source '
                                                                    "record's field values used to detect "
                                                                    'changes between pipeline loads.',
                                                        'source_absent_detected_ts': 'Timestamp recording '
                                                                                     'when the pipeline '
                                                                                     'first detected that '
                                                                                     'the record was no '
                                                                                     'longer present in the '
                                                                                     'upstream iWeb source.',
                                                        'source_present_ind': 'Canonical boolean mirror of '
                                                                              'IS_CURRENT_IN_SOURCE; false '
                                                                              'rows are retained source '
                                                                              'tombstones.',
                                                        'source_record_key': 'String representation of the '
                                                                             'upstream iWeb EntryId '
                                                                             'identifying the source '
                                                                             'follow-up record for '
                                                                             'traceability back to the '
                                                                             'feeder table.',
                                                        'source_table': 'Name of the upstream iWeb source '
                                                                        'table from which the record was '
                                                                        'ingested.',
                                                        'sternal_wound_infection_treatmenttxt': 'Text '
                                                                                                'description '
                                                                                                'of '
                                                                                                'treatment '
                                                                                                'given for '
                                                                                                'sternal '
                                                                                                'wound '
                                                                                                'infection.',
                                                        'surname': "Patient's surname as recorded in the "
                                                                   'iWeb CS2010G cardiac surgery follow-up '
                                                                   'record.',
                                                        'time_of_followup': 'Time recorded for the follow-up '
                                                                            'assessment, stored as text.'},
                                            'quantity_override': True,
                                            'table': 'Internal QC of clinical_registry_surgery_followup; '
                                                     'counts rows whose Gold parent is absent.'},
 'clinical_registry_surgery_followup': {'columns': {},
                                        'quantity_override': False,
                                        'table': 'Typed child of clinical_registry_entry for the '
                                                 'surgery_followup family. CS2010G follow-up module. The '
                                                 'source is small and dormant but semantically distinct. '
                                                 'Duplicate EntryId rows are reduced deterministically to '
                                                 'the latest DateLastChanged/ADC_UPDT row. Every governed '
                                                 'bronze column is retained under its lower-case name. '
                                                 'Coverage: iWeb registries start at different years: '
                                                 'coronary 2013, non-coronary 2016, surgery 2017, cardiac '
                                                 'MDT 2018 (CT aortic 2021), mortality review 2018, ERACS '
                                                 '2025, ACS transfer 2000; complete-through month per '
                                                 'release: 6_mgmt.data_quality.release_finding (finding_code '
                                                 'F2_complete_through).'}}
