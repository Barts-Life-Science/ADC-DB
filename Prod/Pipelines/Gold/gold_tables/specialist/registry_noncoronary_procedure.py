"""Gold specialist.registry_noncoronary_procedure: 2 dataset(s).

- datasets: _qc_clinical_registry_noncoronary_procedure, clinical_registry_noncoronary_procedure
- moved verbatim from: 3_gold_specialist_care:5114, 3_gold_specialist_care:5274, 3_gold_specialist_care:5280, 3_gold_specialist_care:5295

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, _src, _with_parent_status, spark
from gold_core.gold_spec import DatasetCollector, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


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


CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_MANDATORY_RULES = {
    # The child belongs in Gold only when its parent was admitted to Gold.
    # The left join retains rejected rows long enough for Lakeflow to count them.
    "gold.clinical_registry_noncoronary_procedure.parent_admitted": "COALESCE(__gold_parent_present, FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._clinical_registry_noncoronary_procedure"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_MANDATORY_RULES)
def _gold_qc_clinical_registry_noncoronary_procedure():
    """Task 3 Gold child of clinical_registry_entry; parent-rejected rows are excluded."""
    parent = spark.read.table(_n("gold_clinical.registry_entry")).select(
        'patient_event_key',
    ).dropDuplicates(['patient_event_key'])
    child = spark.read.table(_src("clinical_registry_noncoronary_procedure"))
    df = _with_parent_status(child, parent, ['patient_event_key']).selectExpr(*CLINICAL_REGISTRY_NONCORONARY_PROCEDURE_SELECT, "__gold_parent_present AS __gold_parent_present")
    return df


@dp.materialized_view(
    name=_n("gold_clinical.registry_noncoronary_procedure"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_clinical_registry_noncoronary_procedure():
    """Publish clinical_registry_noncoronary_procedure without exposing the internal parent-admission marker."""
    return spark.read.table(_n("gold_qc._clinical_registry_noncoronary_procedure")).drop("__gold_parent_present")


finalise()


COMMENTS = {'_qc_clinical_registry_noncoronary_procedure': {'columns': {'adc_updt': 'Technical timestamp from the '
                                                                         'source extract indicating when the '
                                                                         'record was last updated for '
                                                                         'downstream loading.',
                                                             'administrative_category': 'Administrative '
                                                                                        'funding category '
                                                                                        'under which the '
                                                                                        'patient was treated '
                                                                                        'for the procedure.',
                                                             'age_at_intervention': "Patient's age in whole "
                                                                                    'years at the time of '
                                                                                    'the non-coronary '
                                                                                    'intervention.',
                                                             'arterial_complicationstxt': 'Decoded label '
                                                                                          'recording '
                                                                                          'arterial '
                                                                                          'access-site '
                                                                                          'complications '
                                                                                          'associated with '
                                                                                          'the procedure.',
                                                             'arterial_managementtxt': 'Decoded label '
                                                                                       'describing how the '
                                                                                       'arterial access site '
                                                                                       'was managed at the '
                                                                                       'end of the '
                                                                                       'procedure.',
                                                             'body_mass_index': 'Patient body mass index '
                                                                                'derived from recorded '
                                                                                'height and weight.',
                                                             'body_surface_area': 'Patient body surface area '
                                                                                  'derived from recorded '
                                                                                  'height and weight.',
                                                             'click_here': 'iWeb form control field with no '
                                                                           'documented clinical meaning, '
                                                                           'retained as received from the '
                                                                           'source registry.',
                                                             'consultant_responsible_for_proceduretxt': 'Name '
                                                                                                        'of '
                                                                                                        'the '
                                                                                                        'consultant '
                                                                                                        'with '
                                                                                                        'overall '
                                                                                                        'responsibility '
                                                                                                        'for '
                                                                                                        'the '
                                                                                                        'non-coronary '
                                                                                                        'procedure, '
                                                                                                        'selected '
                                                                                                        'from '
                                                                                                        'the '
                                                                                                        'iWeb '
                                                                                                        'clinician '
                                                                                                        'lookup.',
                                                             'contrast_volume': 'Volume of contrast medium '
                                                                                'used during the '
                                                                                'non-coronary procedure as '
                                                                                'recorded in the registry.',
                                                             'data_complete': 'Data-quality flag indicating '
                                                                              'whether the registry record '
                                                                              'has been marked as complete.',
                                                             'date_last_changed': 'Timestamp recording when '
                                                                                  'the registry record was '
                                                                                  'last amended in the '
                                                                                  'source iWeb system.',
                                                             'date_of_birth': "Patient's date of birth as "
                                                                              'recorded on the iWeb '
                                                                              'non-coronary procedure '
                                                                              'registry record.',
                                                             'date_of_death': 'Recorded date of death of the '
                                                                              'patient, where known.',
                                                             'date_of_discharge_death': 'Date of discharge '
                                                                                        'or of in-hospital '
                                                                                        'death following the '
                                                                                        'non-coronary '
                                                                                        'procedure.',
                                                             'dateofprocedure': 'Date on which the '
                                                                                'non-coronary '
                                                                                'catheter-laboratory '
                                                                                'procedure was performed.',
                                                             'device_failure': 'Indicator recording whether '
                                                                               'a device failure occurred '
                                                                               'during the procedure.',
                                                             'entry_id': 'Internal iWeb registry entry '
                                                                         'identifier for the non-coronary '
                                                                         'procedure record, carried through '
                                                                         'from the source registry table.',
                                                             'ethnic_origin': 'Coded ethnic origin of the '
                                                                              'patient as recorded in the '
                                                                              'iWeb registry.',
                                                             'forename': "Patient's forename as recorded on "
                                                                         'the iWeb non-coronary procedure '
                                                                         'registry record.',
                                                             'gender': 'Recorded gender of the patient at '
                                                                       'the time of the non-coronary '
                                                                       'procedure.',
                                                             'height': 'Recorded patient height used for '
                                                                       'body-size calculations in the '
                                                                       'registry record.',
                                                             'hospital_identifier': 'Identifier of the '
                                                                                    'hospital or site '
                                                                                    'recorded against the '
                                                                                    'non-coronary procedure '
                                                                                    'in the iWeb registry.',
                                                             'hospital_outcometxt': 'Decoded label '
                                                                                    'describing the '
                                                                                    "patient's hospital "
                                                                                    'outcome for the '
                                                                                    'procedural admission.',
                                                             'interventionstxt': 'Decoded label or labels '
                                                                                 'for the interventions '
                                                                                 'performed, with local '
                                                                                 'N-Label values also '
                                                                                 'exploded in '
                                                                                 'iweb_multiselect_value.',
                                                             'investigationstxt': 'Decoded label or labels '
                                                                                  'for investigations '
                                                                                  'performed during the '
                                                                                  'non-coronary procedure, '
                                                                                  'with local N-Label values '
                                                                                  'also exploded in '
                                                                                  'iweb_multiselect_value.',
                                                             'is_current_in_source': 'Boolean flag '
                                                                                     'indicating whether the '
                                                                                     'record is still '
                                                                                     'present as a current '
                                                                                     'record in the source '
                                                                                     'system.',
                                                             'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                              'indicating '
                                                                                              'that the '
                                                                                              'fallback '
                                                                                              'linkage '
                                                                                              'produced a '
                                                                                              'conflicting '
                                                                                              'person match '
                                                                                              'requiring '
                                                                                              'review.',
                                                             'linkage_historical_fallback_ind': 'Boolean '
                                                                                                'flag '
                                                                                                'indicating '
                                                                                                'that the '
                                                                                                'person '
                                                                                                'match was '
                                                                                                'achieved '
                                                                                                'using a '
                                                                                                'historical '
                                                                                                'identifier '
                                                                                                'rather than '
                                                                                                'a current '
                                                                                                'one.',
                                                             'linkage_method': 'Code indicating which '
                                                                               'identifiers were used to '
                                                                               'match this registry record '
                                                                               'to a person record during '
                                                                               'linkage processing.',
                                                             'linkage_status': 'Coded status describing the '
                                                                               'outcome of linking this '
                                                                               'registry record to a master '
                                                                               'patient index person record.',
                                                             'local_procedure_identifier': 'Locally assigned '
                                                                                           'identifier for '
                                                                                           'the procedure as '
                                                                                           'recorded in the '
                                                                                           'iWeb registry.',
                                                             'm_complication_details': 'Free-text memo '
                                                                                       'giving details of '
                                                                                       'complications '
                                                                                       'occurring in '
                                                                                       'relation to the '
                                                                                       'non-coronary '
                                                                                       'procedure.',
                                                             'm_details_of_all_devices_implanted': 'Free-text '
                                                                                                   'memo '
                                                                                                   'listing '
                                                                                                   'the '
                                                                                                   'devices '
                                                                                                   'implanted '
                                                                                                   'during '
                                                                                                   'the '
                                                                                                   'procedure, '
                                                                                                   'including '
                                                                                                   'model '
                                                                                                   'and '
                                                                                                   'serial-number '
                                                                                                   'details.',
                                                             'm_discharge_management_plan': 'Free-text memo '
                                                                                            'describing the '
                                                                                            'discharge '
                                                                                            'management plan '
                                                                                            'following the '
                                                                                            'non-coronary '
                                                                                            'procedure.',
                                                             'm_indications_and_background_history': 'Free-text '
                                                                                                     'memo '
                                                                                                     'describing '
                                                                                                     'the '
                                                                                                     'indication '
                                                                                                     'for '
                                                                                                     'the '
                                                                                                     'procedure '
                                                                                                     'and '
                                                                                                     'the '
                                                                                                     "patient's "
                                                                                                     'relevant '
                                                                                                     'background '
                                                                                                     'medical '
                                                                                                     'history.',
                                                             'm_outcome_of_the_procedure': 'Free-text memo '
                                                                                           'describing the '
                                                                                           'outcome of the '
                                                                                           'non-coronary '
                                                                                           'catheter-laboratory '
                                                                                           'procedure.',
                                                             'm_patient_info': 'Free-text memo recording '
                                                                               'information given to the '
                                                                               'patient about the procedure '
                                                                               'and follow-up.',
                                                             'm_pre_discharge_instructions': 'Free-text memo '
                                                                                             'holding '
                                                                                             'instructions '
                                                                                             'given to the '
                                                                                             'patient before '
                                                                                             'discharge '
                                                                                             'after the '
                                                                                             'procedure.',
                                                             'm_procedure_findings': 'Free-text memo '
                                                                                     'recording the findings '
                                                                                     'of the non-coronary '
                                                                                     'catheter-laboratory '
                                                                                     'procedure.',
                                                             'm_procedure_findings_and_report': 'Free-text '
                                                                                                'procedural '
                                                                                                'report '
                                                                                                'describing '
                                                                                                'access, '
                                                                                                'technique '
                                                                                                'and '
                                                                                                'findings of '
                                                                                                'the '
                                                                                                'non-coronary '
                                                                                                'intervention.',
                                                             'management_plan': 'Recorded management plan '
                                                                                'for the patient following '
                                                                                'the non-coronary procedure.',
                                                             'mg_pinfo': 'Free-text memo holding information '
                                                                         'for the general practitioner '
                                                                         'regarding follow-up after the '
                                                                         'procedure.',
                                                             'mrn': 'Local hospital medical record number '
                                                                    'identifying the patient for this '
                                                                    'non-coronary procedure record.',
                                                             'nhs_number': "Patient's NHS number as held on "
                                                                           'the source iWeb registry record, '
                                                                           'with a separate flag indicating '
                                                                           'whether it passed validation.',
                                                             'nhs_number_valid_ind': 'Boolean flag '
                                                                                     'indicating whether the '
                                                                                     'NHS number held on the '
                                                                                     'source registry record '
                                                                                     'passed validation, '
                                                                                     'derived without '
                                                                                     'retaining the number '
                                                                                     'itself.',
                                                             'nurse_led_discharge': 'Indicator of whether '
                                                                                    "the patient's discharge "
                                                                                    'after the procedure was '
                                                                                    'nurse-led.',
                                                             'other_intervention_description': 'Free-text '
                                                                                               'description '
                                                                                               'of an '
                                                                                               'intervention '
                                                                                               'performed '
                                                                                               'when it is '
                                                                                               'not covered '
                                                                                               'by the coded '
                                                                                               'intervention '
                                                                                               'list.',
                                                             'other_investigation': 'Free-text description '
                                                                                    'of an investigation '
                                                                                    'performed during the '
                                                                                    'procedure when it is '
                                                                                    'not covered by the '
                                                                                    'coded investigation '
                                                                                    'list.',
                                                             'patient_ethnic_group': 'Free-text or locally '
                                                                                     'recorded patient '
                                                                                     'ethnic group '
                                                                                     'description captured '
                                                                                     'in the registry.',
                                                             'patient_event_key': 'Deterministic SHA-256 key '
                                                                                  'matching '
                                                                                  'clinical_registry_entry.patient_event_key '
                                                                                  'for the '
                                                                                  'noncoronary_procedure '
                                                                                  'source family and native '
                                                                                  'entry_id.',
                                                             'person_id': 'Internal system-assigned unique '
                                                                          'identifier for the patient in the '
                                                                          'Millennium person table, used to '
                                                                          'link this record to the master '
                                                                          'patient index.',
                                                             'pipeline_loaded_at': 'Timestamp recording when '
                                                                                   'the ingestion pipeline '
                                                                                   'loaded this row into the '
                                                                                   'bronze table.',
                                                             'postcode_of_usual_address': 'Postcode of the '
                                                                                          "patient's usual "
                                                                                          'address as '
                                                                                          'recorded on the '
                                                                                          'iWeb non-coronary '
                                                                                          'procedure '
                                                                                          'registry record.',
                                                             'primary_operator_status': 'Grade or role '
                                                                                        'status of the '
                                                                                        'primary operator '
                                                                                        'for the procedure.',
                                                             'primary_operatortxt': 'Name of the primary '
                                                                                    'operator performing the '
                                                                                    'non-coronary procedure, '
                                                                                    'as free text.',
                                                             'procedural_complicationtxt': 'Decoded label '
                                                                                           'recording '
                                                                                           'procedural '
                                                                                           'complications '
                                                                                           'occurring during '
                                                                                           'the non-coronary '
                                                                                           'intervention.',
                                                             'procedure_urgency': 'Categorical record of the '
                                                                                  'urgency with which the '
                                                                                  'non-coronary '
                                                                                  'catheter-laboratory '
                                                                                  'procedure was undertaken.',
                                                             'referring_consultant_cardiologisttxt': 'Name '
                                                                                                     'of the '
                                                                                                     'consultant '
                                                                                                     'cardiologist '
                                                                                                     'who '
                                                                                                     'referred '
                                                                                                     'the '
                                                                                                     'patient '
                                                                                                     'for '
                                                                                                     'the '
                                                                                                     'non-coronary '
                                                                                                     'procedure, '
                                                                                                     'selected '
                                                                                                     'from '
                                                                                                     'the '
                                                                                                     'iWeb '
                                                                                                     'clinician '
                                                                                                     'lookup.',
                                                             'referring_hospitaltxt': 'Text description of '
                                                                                      'the hospital that '
                                                                                      'referred the patient '
                                                                                      'for the non-coronary '
                                                                                      'procedure, taken from '
                                                                                      'the iWeb '
                                                                                      'referring-hospital '
                                                                                      'lookup.',
                                                             'research_procedure': 'Indicator of whether the '
                                                                                   'procedure was performed '
                                                                                   'as part of a research '
                                                                                   'study.',
                                                             'research_title': 'Title of the research study '
                                                                               'associated with the '
                                                                               'procedure, where applicable.',
                                                             'row_hash': 'Hash value computed across the '
                                                                         "source record's fields to support "
                                                                         'change detection during ingestion.',
                                                             'second_operator_status': 'Grade or role status '
                                                                                       'of the second '
                                                                                       'operator for the '
                                                                                       'procedure.',
                                                             'second_operatortxt': 'Name of the second '
                                                                                   'operator assisting with '
                                                                                   'the non-coronary '
                                                                                   'procedure, as free text.',
                                                             'sex': 'Recorded sex of the patient at the time '
                                                                    'of the non-coronary procedure.',
                                                             'source_absent_detected_ts': 'Timestamp '
                                                                                          'recording when '
                                                                                          'the ingestion '
                                                                                          'process first '
                                                                                          'detected that '
                                                                                          'this record was '
                                                                                          'no longer present '
                                                                                          'in the source '
                                                                                          'registry extract.',
                                                             'source_present_ind': 'Canonical boolean mirror '
                                                                                   'of IS_CURRENT_IN_SOURCE; '
                                                                                   'false rows are retained '
                                                                                   'source tombstones.',
                                                             'source_record_key': 'Key identifying the '
                                                                                  'originating source-system '
                                                                                  'record, populated from '
                                                                                  'the iWeb registry entry '
                                                                                  'identifier.',
                                                             'source_table': 'Name of the source system '
                                                                             'table from which this record '
                                                                             'was ingested.',
                                                             'status_at_discharge': 'Categorical record of '
                                                                                    "the patient's status at "
                                                                                    'discharge from the '
                                                                                    'admission in which the '
                                                                                    'procedure took place.',
                                                             'surname': "Patient's surname as recorded on "
                                                                        'the iWeb non-coronary procedure '
                                                                        'registry record.',
                                                             'third_operator_status': 'Grade or role status '
                                                                                      'of the third operator '
                                                                                      'for the procedure.',
                                                             'third_operatortxt': 'Name of the third '
                                                                                  'operator assisting with '
                                                                                  'the non-coronary '
                                                                                  'procedure, as free text.',
                                                             'training_procedure': 'Indicator of whether the '
                                                                                   'procedure was performed '
                                                                                   'as a training case.',
                                                             'tunnel_length': 'Recorded tunnel length '
                                                                              'measurement captured for the '
                                                                              'non-coronary procedure in the '
                                                                              'iWeb registry.',
                                                             'weight': 'Recorded patient weight used for '
                                                                       'body-size calculations in the '
                                                                       'registry record.'},
                                                 'quantity_override': True,
                                                 'table': 'Internal QC of '
                                                          'clinical_registry_noncoronary_procedure; counts '
                                                          'rows whose Gold parent is absent.'},
 'clinical_registry_noncoronary_procedure': {'columns': {},
                                             'quantity_override': False,
                                             'table': 'Typed child of clinical_registry_entry for the '
                                                      'noncoronary_procedure family. iWeb '
                                                      'non-coronary/structural cath-lab registry covering '
                                                      'interventions such as TAVI, TMVI, ASD/VSD closure and '
                                                      'septal ablation. Local N-Label values are retained '
                                                      'here and exploded separately in '
                                                      'iweb_multiselect_value. Every governed bronze column '
                                                      'is retained under its lower-case name. Coverage: iWeb '
                                                      'registries start at different years: coronary 2013, '
                                                      'non-coronary 2016, surgery 2017, cardiac MDT 2018 (CT '
                                                      'aortic 2021), mortality review 2018, ERACS 2025, ACS '
                                                      'transfer 2000; Non-coronary iWeb procedures from '
                                                      'April 2025 run at 0.37 of the previous two years at '
                                                      'source (coronary is unchanged); The discharge date is '
                                                      'empty on 3,510 of 7,667 non-coronary procedures at '
                                                      'source; the table has no encounter id and a '
                                                      'containing hospital stay would fill only 2.5% more, '
                                                      'so it is not derived; complete-through month per '
                                                      'release: 6_mgmt.data_quality.release_finding '
                                                      '(finding_code F2_complete_through).'}}
