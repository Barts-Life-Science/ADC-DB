"""Silver specialist.registry_noncoronary_procedure: 1 dataset(s).

- datasets: clinical_registry_noncoronary_procedure
- moved verbatim from: 3_silver_specialist_care:5883, 3_silver_specialist_care:6128

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id
from silver_tables._common.journey import IWEB_REGISTRY_SLOTS


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Clinical / registry noncoronary procedure

# ==== journey_clinical.registry_noncoronary_procedure ====
REGISTRY_NONCORONARY_PROCEDURE_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("DATEOFPROCEDURE", "timestamp"),
    ("SEX", "string"),
    ("PATIENT_ETHNIC_GROUP", "string"),
    ("ADMINISTRATIVE_CATEGORY", "string"),
    ("PROCEDURE_URGENCY", "string"),
    ("HEIGHT", "int"),
    ("WEIGHT", "double"),
    ("BODY_SURFACE_AREA", "double"),
    ("BODY_MASS_INDEX", "double"),
    ("CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT", "string"),
    ("PRIMARY_OPERATORTXT", "string"),
    ("SECOND_OPERATORTXT", "string"),
    ("THIRD_OPERATORTXT", "string"),
    ("PRIMARY_OPERATOR_STATUS", "string"),
    ("SECOND_OPERATOR_STATUS", "string"),
    ("THIRD_OPERATOR_STATUS", "string"),
    ("LOCAL_PROCEDURE_IDENTIFIER", "string"),
    ("TRAINING_PROCEDURE", "string"),
    ("RESEARCH_PROCEDURE", "string"),
    ("RESEARCH_TITLE", "string"),
    ("REFERRING_HOSPITALTXT", "string"),
    ("M_PROCEDURE_FINDINGS", "string"),
    ("ARTERIAL_MANAGEMENTTXT", "string"),
    ("ARTERIAL_COMPLICATIONSTXT", "string"),
    ("DEVICE_FAILURE", "string"),
    ("PROCEDURAL_COMPLICATIONTXT", "string"),
    ("MANAGEMENT_PLAN", "string"),
    ("STATUS_AT_DISCHARGE", "string"),
    ("DATA_COMPLETE", "string"),
    ("DATE_OF_DISCHARGE_DEATH", "timestamp"),
    ("HOSPITAL_OUTCOMETXT", "string"),
    ("M_PROCEDURE_FINDINGS_AND_REPORT", "string"),
    ("INVESTIGATIONSTXT", "string"),
    ("INTERVENTIONSTXT", "string"),
    ("CLICK_HERE", "string"),
    ("M_PRE_DISCHARGE_INSTRUCTIONS", "string"),
    ("M_DISCHARGE_MANAGEMENT_PLAN", "string"),
    ("M_PATIENT_INFO", "string"),
    ("MG_PINFO", "string"),
    ("AGE_AT_INTERVENTION", "int"),
    ("OTHER_INTERVENTION_DESCRIPTION", "string"),
    ("REFERRING_CONSULTANT_CARDIOLOGISTTXT", "string"),
    ("NURSE_LED_DISCHARGE", "string"),
    ("M_INDICATIONS_AND_BACKGROUND_HISTORY", "string"),
    ("M_DETAILS_OF_ALL_DEVICES_IMPLANTED", "string"),
    ("M_OUTCOME_OF_THE_PROCEDURE", "string"),
    ("OTHER_INVESTIGATION", "string"),
    ("TUNNEL_LENGTH", "int"),
    ("CONTRAST_VOLUME", "int"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("M_COMPLICATION_DETAILS", "string"),
    ("ADC_UPDT", "timestamp"),
    ("NHS_NUMBER_VALID_IND", "boolean"),
    ("LINKAGE_STATUS", "string"),
    ("PERSON_ID", "bigint"),
    ("LINKAGE_METHOD", "string"),
    ("LINKAGE_HISTORICAL_FALLBACK_IND", "boolean"),
    ("LINKAGE_FALLBACK_CONFLICT_IND", "boolean"),
    ("SOURCE_TABLE", "string"),
    ("SOURCE_RECORD_KEY", "string"),
    ("ROW_HASH", "string"),
    ("PIPELINE_LOADED_AT", "timestamp"),
    ("IS_CURRENT_IN_SOURCE", "boolean"),
    ("SOURCE_PRESENT_IND", "boolean"),
    ("SOURCE_ABSENT_DETECTED_TS", "timestamp"),
    ("SURNAME", "string"),
    ("FORENAME", "string"),
    ("NHS_NUMBER", "string"),
    ("DATE_OF_BIRTH", "date"),
    ("POSTCODE_OF_USUAL_ADDRESS", "string"),
    ("HOSPITAL_IDENTIFIER", "string"),
]


@materialized_view(
    name=_n("journey_clinical.registry_noncoronary_procedure"),
    refresh_policy="auto",
)
def registry_noncoronary_procedure():
    # Build the declared dataset: Typed child of clinical_registry_entry for the
    # noncoronary_procedure family. iWeb non-coronary/structural cath-lab registry covering
    # interventions such as TAVI, TMVI, ASD/VSD closure and septal ablation. Local N-Label
    # values are retained here and exploded separately in iweb_multiselect_value. Every governed
    # bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["noncoronary_procedure"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_NONCORONARY_PROCEDURE_EXPECTED_SCHEMA:
        raise RuntimeError("noncoronary_procedure: bronze schema drift" + f"; expected={REGISTRY_NONCORONARY_PROCEDURE_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("noncoronary_procedure"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["DATEOFPROCEDURE"].alias("dateofprocedure"),
        s["SEX"].alias("sex"),
        s["PATIENT_ETHNIC_GROUP"].alias("patient_ethnic_group"),
        s["ADMINISTRATIVE_CATEGORY"].alias("administrative_category"),
        s["PROCEDURE_URGENCY"].alias("procedure_urgency"),
        s["HEIGHT"].alias("height"),
        s["WEIGHT"].alias("weight"),
        s["BODY_SURFACE_AREA"].alias("body_surface_area"),
        s["BODY_MASS_INDEX"].alias("body_mass_index"),
        s["CONSULTANT_RESPONSIBLE_FOR_PROCEDURETXT"].alias("consultant_responsible_for_proceduretxt"),
        s["PRIMARY_OPERATORTXT"].alias("primary_operatortxt"),
        s["SECOND_OPERATORTXT"].alias("second_operatortxt"),
        s["THIRD_OPERATORTXT"].alias("third_operatortxt"),
        s["PRIMARY_OPERATOR_STATUS"].alias("primary_operator_status"),
        s["SECOND_OPERATOR_STATUS"].alias("second_operator_status"),
        s["THIRD_OPERATOR_STATUS"].alias("third_operator_status"),
        s["LOCAL_PROCEDURE_IDENTIFIER"].alias("local_procedure_identifier"),
        s["TRAINING_PROCEDURE"].alias("training_procedure"),
        s["RESEARCH_PROCEDURE"].alias("research_procedure"),
        s["RESEARCH_TITLE"].alias("research_title"),
        s["REFERRING_HOSPITALTXT"].alias("referring_hospitaltxt"),
        s["M_PROCEDURE_FINDINGS"].alias("m_procedure_findings"),
        s["ARTERIAL_MANAGEMENTTXT"].alias("arterial_managementtxt"),
        s["ARTERIAL_COMPLICATIONSTXT"].alias("arterial_complicationstxt"),
        s["DEVICE_FAILURE"].alias("device_failure"),
        s["PROCEDURAL_COMPLICATIONTXT"].alias("procedural_complicationtxt"),
        s["MANAGEMENT_PLAN"].alias("management_plan"),
        s["STATUS_AT_DISCHARGE"].alias("status_at_discharge"),
        s["DATA_COMPLETE"].alias("data_complete"),
        s["DATE_OF_DISCHARGE_DEATH"].alias("date_of_discharge_death"),
        s["HOSPITAL_OUTCOMETXT"].alias("hospital_outcometxt"),
        s["M_PROCEDURE_FINDINGS_AND_REPORT"].alias("m_procedure_findings_and_report"),
        s["INVESTIGATIONSTXT"].alias("investigationstxt"),
        s["INTERVENTIONSTXT"].alias("interventionstxt"),
        s["CLICK_HERE"].alias("click_here"),
        s["M_PRE_DISCHARGE_INSTRUCTIONS"].alias("m_pre_discharge_instructions"),
        s["M_DISCHARGE_MANAGEMENT_PLAN"].alias("m_discharge_management_plan"),
        s["M_PATIENT_INFO"].alias("m_patient_info"),
        s["MG_PINFO"].alias("mg_pinfo"),
        s["AGE_AT_INTERVENTION"].alias("age_at_intervention"),
        s["OTHER_INTERVENTION_DESCRIPTION"].alias("other_intervention_description"),
        s["REFERRING_CONSULTANT_CARDIOLOGISTTXT"].alias("referring_consultant_cardiologisttxt"),
        s["NURSE_LED_DISCHARGE"].alias("nurse_led_discharge"),
        s["M_INDICATIONS_AND_BACKGROUND_HISTORY"].alias("m_indications_and_background_history"),
        s["M_DETAILS_OF_ALL_DEVICES_IMPLANTED"].alias("m_details_of_all_devices_implanted"),
        s["M_OUTCOME_OF_THE_PROCEDURE"].alias("m_outcome_of_the_procedure"),
        s["OTHER_INVESTIGATION"].alias("other_investigation"),
        s["TUNNEL_LENGTH"].alias("tunnel_length"),
        s["CONTRAST_VOLUME"].alias("contrast_volume"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["M_COMPLICATION_DETAILS"].alias("m_complication_details"),
        s["ADC_UPDT"].alias("adc_updt"),
        s["NHS_NUMBER_VALID_IND"].alias("nhs_number_valid_ind"),
        s["LINKAGE_STATUS"].alias("linkage_status"),
        s["PERSON_ID"].alias("person_id"),
        s["LINKAGE_METHOD"].alias("linkage_method"),
        s["LINKAGE_HISTORICAL_FALLBACK_IND"].alias("linkage_historical_fallback_ind"),
        s["LINKAGE_FALLBACK_CONFLICT_IND"].alias("linkage_fallback_conflict_ind"),
        s["SOURCE_TABLE"].alias("source_table"),
        s["SOURCE_RECORD_KEY"].alias("source_record_key"),
        s["ROW_HASH"].alias("row_hash"),
        s["PIPELINE_LOADED_AT"].alias("pipeline_loaded_at"),
        s["IS_CURRENT_IN_SOURCE"].alias("is_current_in_source"),
        s["SOURCE_PRESENT_IND"].alias("source_present_ind"),
        s["SOURCE_ABSENT_DETECTED_TS"].alias("source_absent_detected_ts"),
        s["SURNAME"].alias("surname"),
        s["FORENAME"].alias("forename"),
        s["NHS_NUMBER"].alias("nhs_number"),
        s["DATE_OF_BIRTH"].alias("date_of_birth"),
        s["POSTCODE_OF_USUAL_ADDRESS"].alias("postcode_of_usual_address"),
        s["HOSPITAL_IDENTIFIER"].alias("hospital_identifier"),
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_noncoronary_procedure': {'columns': {'adc_updt': 'Technical timestamp from the source '
                                                                     'extract indicating when the record was '
                                                                     'last updated for downstream loading.',
                                                         'administrative_category': 'Administrative funding '
                                                                                    'category under which '
                                                                                    'the patient was treated '
                                                                                    'for the procedure.',
                                                         'age_at_intervention': "Patient's age in whole "
                                                                                'years at the time of the '
                                                                                'non-coronary intervention.',
                                                         'arterial_complicationstxt': 'Decoded label '
                                                                                      'recording arterial '
                                                                                      'access-site '
                                                                                      'complications '
                                                                                      'associated with the '
                                                                                      'procedure.',
                                                         'arterial_managementtxt': 'Decoded label describing '
                                                                                   'how the arterial access '
                                                                                   'site was managed at the '
                                                                                   'end of the procedure.',
                                                         'body_mass_index': 'Patient body mass index derived '
                                                                            'from recorded height and '
                                                                            'weight.',
                                                         'body_surface_area': 'Patient body surface area '
                                                                              'derived from recorded height '
                                                                              'and weight.',
                                                         'click_here': 'iWeb form control field with no '
                                                                       'documented clinical meaning, '
                                                                       'retained as received from the source '
                                                                       'registry.',
                                                         'consultant_responsible_for_proceduretxt': 'Name of '
                                                                                                    'the '
                                                                                                    'consultant '
                                                                                                    'with '
                                                                                                    'overall '
                                                                                                    'responsibility '
                                                                                                    'for the '
                                                                                                    'non-coronary '
                                                                                                    'procedure, '
                                                                                                    'selected '
                                                                                                    'from '
                                                                                                    'the '
                                                                                                    'iWeb '
                                                                                                    'clinician '
                                                                                                    'lookup.',
                                                         'contrast_volume': 'Volume of contrast medium used '
                                                                            'during the non-coronary '
                                                                            'procedure as recorded in the '
                                                                            'registry.',
                                                         'data_complete': 'Data-quality flag indicating '
                                                                          'whether the registry record has '
                                                                          'been marked as complete.',
                                                         'date_last_changed': 'Timestamp recording when the '
                                                                              'registry record was last '
                                                                              'amended in the source iWeb '
                                                                              'system.',
                                                         'date_of_birth': "Patient's date of birth as "
                                                                          'recorded on the iWeb non-coronary '
                                                                          'procedure registry record.',
                                                         'date_of_death': 'Recorded date of death of the '
                                                                          'patient, where known.',
                                                         'date_of_discharge_death': 'Date of discharge or of '
                                                                                    'in-hospital death '
                                                                                    'following the '
                                                                                    'non-coronary procedure.',
                                                         'dateofprocedure': 'Date on which the non-coronary '
                                                                            'catheter-laboratory procedure '
                                                                            'was performed.',
                                                         'device_failure': 'Indicator recording whether a '
                                                                           'device failure occurred during '
                                                                           'the procedure.',
                                                         'entry_id': 'Internal iWeb registry entry '
                                                                     'identifier for the non-coronary '
                                                                     'procedure record, carried through from '
                                                                     'the source registry table.',
                                                         'ethnic_origin': 'Coded ethnic origin of the '
                                                                          'patient as recorded in the iWeb '
                                                                          'registry.',
                                                         'forename': "Patient's forename as recorded on the "
                                                                     'iWeb non-coronary procedure registry '
                                                                     'record.',
                                                         'gender': 'Recorded gender of the patient at the '
                                                                   'time of the non-coronary procedure.',
                                                         'height': 'Recorded patient height used for '
                                                                   'body-size calculations in the registry '
                                                                   'record.',
                                                         'hospital_identifier': 'Identifier of the hospital '
                                                                                'or site recorded against '
                                                                                'the non-coronary procedure '
                                                                                'in the iWeb registry.',
                                                         'hospital_outcometxt': 'Decoded label describing '
                                                                                "the patient's hospital "
                                                                                'outcome for the procedural '
                                                                                'admission.',
                                                         'interventionstxt': 'Decoded label or labels for '
                                                                             'the interventions performed, '
                                                                             'with local N-Label values also '
                                                                             'exploded in '
                                                                             'iweb_multiselect_value.',
                                                         'investigationstxt': 'Decoded label or labels for '
                                                                              'investigations performed '
                                                                              'during the non-coronary '
                                                                              'procedure, with local N-Label '
                                                                              'values also exploded in '
                                                                              'iweb_multiselect_value.',
                                                         'is_current_in_source': 'Boolean flag indicating '
                                                                                 'whether the record is '
                                                                                 'still present as a current '
                                                                                 'record in the source '
                                                                                 'system.',
                                                         'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                          'indicating that '
                                                                                          'the fallback '
                                                                                          'linkage produced '
                                                                                          'a conflicting '
                                                                                          'person match '
                                                                                          'requiring review.',
                                                         'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                            'indicating that '
                                                                                            'the person '
                                                                                            'match was '
                                                                                            'achieved using '
                                                                                            'a historical '
                                                                                            'identifier '
                                                                                            'rather than a '
                                                                                            'current one.',
                                                         'linkage_method': 'Code indicating which '
                                                                           'identifiers were used to match '
                                                                           'this registry record to a person '
                                                                           'record during linkage '
                                                                           'processing.',
                                                         'linkage_status': 'Coded status describing the '
                                                                           'outcome of linking this registry '
                                                                           'record to a master patient index '
                                                                           'person record.',
                                                         'local_procedure_identifier': 'Locally assigned '
                                                                                       'identifier for the '
                                                                                       'procedure as '
                                                                                       'recorded in the iWeb '
                                                                                       'registry.',
                                                         'm_complication_details': 'Free-text memo giving '
                                                                                   'details of complications '
                                                                                   'occurring in relation to '
                                                                                   'the non-coronary '
                                                                                   'procedure.',
                                                         'm_details_of_all_devices_implanted': 'Free-text '
                                                                                               'memo listing '
                                                                                               'the devices '
                                                                                               'implanted '
                                                                                               'during the '
                                                                                               'procedure, '
                                                                                               'including '
                                                                                               'model and '
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
                                                                                                 'for the '
                                                                                                 'procedure '
                                                                                                 'and the '
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
                                                                           'information given to the patient '
                                                                           'about the procedure and '
                                                                           'follow-up.',
                                                         'm_pre_discharge_instructions': 'Free-text memo '
                                                                                         'holding '
                                                                                         'instructions given '
                                                                                         'to the patient '
                                                                                         'before discharge '
                                                                                         'after the '
                                                                                         'procedure.',
                                                         'm_procedure_findings': 'Free-text memo recording '
                                                                                 'the findings of the '
                                                                                 'non-coronary '
                                                                                 'catheter-laboratory '
                                                                                 'procedure.',
                                                         'm_procedure_findings_and_report': 'Free-text '
                                                                                            'procedural '
                                                                                            'report '
                                                                                            'describing '
                                                                                            'access, '
                                                                                            'technique and '
                                                                                            'findings of the '
                                                                                            'non-coronary '
                                                                                            'intervention.',
                                                         'management_plan': 'Recorded management plan for '
                                                                            'the patient following the '
                                                                            'non-coronary procedure.',
                                                         'mg_pinfo': 'Free-text memo holding information for '
                                                                     'the general practitioner regarding '
                                                                     'follow-up after the procedure.',
                                                         'mrn': 'Local hospital medical record number '
                                                                'identifying the patient for this '
                                                                'non-coronary procedure record.',
                                                         'nhs_number': "Patient's NHS number as held on the "
                                                                       'source iWeb registry record, with a '
                                                                       'separate flag indicating whether it '
                                                                       'passed validation.',
                                                         'nhs_number_valid_ind': 'Boolean flag indicating '
                                                                                 'whether the NHS number '
                                                                                 'held on the source '
                                                                                 'registry record passed '
                                                                                 'validation, derived '
                                                                                 'without retaining the '
                                                                                 'number itself.',
                                                         'nurse_led_discharge': 'Indicator of whether the '
                                                                                "patient's discharge after "
                                                                                'the procedure was '
                                                                                'nurse-led.',
                                                         'other_intervention_description': 'Free-text '
                                                                                           'description of '
                                                                                           'an intervention '
                                                                                           'performed when '
                                                                                           'it is not '
                                                                                           'covered by the '
                                                                                           'coded '
                                                                                           'intervention '
                                                                                           'list.',
                                                         'other_investigation': 'Free-text description of an '
                                                                                'investigation performed '
                                                                                'during the procedure when '
                                                                                'it is not covered by the '
                                                                                'coded investigation list.',
                                                         'patient_ethnic_group': 'Free-text or locally '
                                                                                 'recorded patient ethnic '
                                                                                 'group description captured '
                                                                                 'in the registry.',
                                                         'patient_event_key': 'Deterministic SHA-256 key '
                                                                              'matching '
                                                                              'clinical_registry_entry.patient_event_key '
                                                                              'for the noncoronary_procedure '
                                                                              'source family and native '
                                                                              'entry_id.',
                                                         'person_id': 'Internal system-assigned unique '
                                                                      'identifier for the patient in the '
                                                                      'Millennium person table, used to link '
                                                                      'this record to the master patient '
                                                                      'index.',
                                                         'pipeline_loaded_at': 'Timestamp recording when the '
                                                                               'ingestion pipeline loaded '
                                                                               'this row into the bronze '
                                                                               'table.',
                                                         'postcode_of_usual_address': 'Postcode of the '
                                                                                      "patient's usual "
                                                                                      'address as recorded '
                                                                                      'on the iWeb '
                                                                                      'non-coronary '
                                                                                      'procedure registry '
                                                                                      'record.',
                                                         'primary_operator_status': 'Grade or role status of '
                                                                                    'the primary operator '
                                                                                    'for the procedure.',
                                                         'primary_operatortxt': 'Name of the primary '
                                                                                'operator performing the '
                                                                                'non-coronary procedure, as '
                                                                                'free text.',
                                                         'procedural_complicationtxt': 'Decoded label '
                                                                                       'recording procedural '
                                                                                       'complications '
                                                                                       'occurring during the '
                                                                                       'non-coronary '
                                                                                       'intervention.',
                                                         'procedure_urgency': 'Categorical record of the '
                                                                              'urgency with which the '
                                                                              'non-coronary '
                                                                              'catheter-laboratory procedure '
                                                                              'was undertaken.',
                                                         'referring_consultant_cardiologisttxt': 'Name of '
                                                                                                 'the '
                                                                                                 'consultant '
                                                                                                 'cardiologist '
                                                                                                 'who '
                                                                                                 'referred '
                                                                                                 'the '
                                                                                                 'patient '
                                                                                                 'for the '
                                                                                                 'non-coronary '
                                                                                                 'procedure, '
                                                                                                 'selected '
                                                                                                 'from the '
                                                                                                 'iWeb '
                                                                                                 'clinician '
                                                                                                 'lookup.',
                                                         'referring_hospitaltxt': 'Text description of the '
                                                                                  'hospital that referred '
                                                                                  'the patient for the '
                                                                                  'non-coronary procedure, '
                                                                                  'taken from the iWeb '
                                                                                  'referring-hospital '
                                                                                  'lookup.',
                                                         'research_procedure': 'Indicator of whether the '
                                                                               'procedure was performed as '
                                                                               'part of a research study.',
                                                         'research_title': 'Title of the research study '
                                                                           'associated with the procedure, '
                                                                           'where applicable.',
                                                         'row_hash': 'Hash value computed across the source '
                                                                     "record's fields to support change "
                                                                     'detection during ingestion.',
                                                         'second_operator_status': 'Grade or role status of '
                                                                                   'the second operator for '
                                                                                   'the procedure.',
                                                         'second_operatortxt': 'Name of the second operator '
                                                                               'assisting with the '
                                                                               'non-coronary procedure, as '
                                                                               'free text.',
                                                         'sex': 'Recorded sex of the patient at the time of '
                                                                'the non-coronary procedure.',
                                                         'source_absent_detected_ts': 'Timestamp recording '
                                                                                      'when the ingestion '
                                                                                      'process first '
                                                                                      'detected that this '
                                                                                      'record was no longer '
                                                                                      'present in the source '
                                                                                      'registry extract.',
                                                         'source_present_ind': 'Canonical boolean mirror of '
                                                                               'IS_CURRENT_IN_SOURCE; false '
                                                                               'rows are retained source '
                                                                               'tombstones.',
                                                         'source_record_key': 'Key identifying the '
                                                                              'originating source-system '
                                                                              'record, populated from the '
                                                                              'iWeb registry entry '
                                                                              'identifier.',
                                                         'source_table': 'Name of the source system table '
                                                                         'from which this record was '
                                                                         'ingested.',
                                                         'status_at_discharge': 'Categorical record of the '
                                                                                "patient's status at "
                                                                                'discharge from the '
                                                                                'admission in which the '
                                                                                'procedure took place.',
                                                         'surname': "Patient's surname as recorded on the "
                                                                    'iWeb non-coronary procedure registry '
                                                                    'record.',
                                                         'third_operator_status': 'Grade or role status of '
                                                                                  'the third operator for '
                                                                                  'the procedure.',
                                                         'third_operatortxt': 'Name of the third operator '
                                                                              'assisting with the '
                                                                              'non-coronary procedure, as '
                                                                              'free text.',
                                                         'training_procedure': 'Indicator of whether the '
                                                                               'procedure was performed as a '
                                                                               'training case.',
                                                         'tunnel_length': 'Recorded tunnel length '
                                                                          'measurement captured for the '
                                                                          'non-coronary procedure in the '
                                                                          'iWeb registry.',
                                                         'weight': 'Recorded patient weight used for '
                                                                   'body-size calculations in the registry '
                                                                   'record.'},
                                             'table': 'Typed child of clinical_registry_entry for the '
                                                      'noncoronary_procedure family. iWeb '
                                                      'non-coronary/structural cath-lab registry covering '
                                                      'interventions such as TAVI, TMVI, ASD/VSD closure and '
                                                      'septal ablation. Local N-Label values are retained '
                                                      'here and exploded separately in '
                                                      'iweb_multiselect_value. Every governed bronze column '
                                                      'is retained under its lower-case name.'}}
