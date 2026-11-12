"""Silver specialist.registry_surgery_followup: 1 dataset(s).

- datasets: clinical_registry_surgery_followup
- moved verbatim from: 3_silver_specialist_care:2960, 3_silver_specialist_care:3169

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


# Clinical / registry surgery followup

# ==== journey_clinical.registry_surgery_followup ====
REGISTRY_SURGERY_FOLLOWUP_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("FLW_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("DISCHARGE_DT", "timestamp"),
    ("TIME_OF_FOLLOWUP", "string"),
    ("PROCEDURE_STATUS", "int"),
    ("PATIENT_STATUS_AT_FOLLOW_UP", "string"),
    ("POST_OPERATIVE_COMPLICATIONTXT", "string"),
    ("ATRIAL_FIBRILLATION", "string"),
    ("ANTICOAGULATION_COMMENCED", "string"),
    ("LRT_ITXT", "string"),
    ("POST_OPERATIVE_PYREXIA_TREATMENTTXT", "string"),
    ("STERNAL_WOUND_INFECTION_TREATMENTTXT", "string"),
    ("M_STERNAL_WOUND_INFECTION_REOPERATION_TYPE", "string"),
    ("HARVEST_WOUND_INFECTION_TREATMENTTXT", "string"),
    ("M_HARVEST_WOUND_INFECTION_REOPERATION_TYPE", "string"),
    ("POST_OPERATIVE_ILEUS", "string"),
    ("PERICARDIAL_EFFUSION", "string"),
    ("RESTERNOTOMY_FOR_BLEEDING_ON_DAY", "int"),
    ("INVESTIGATIONS_SATISFACTORY_PRIOR_TO_DISCHARGE", "string"),
    ("M_OTHER_INVESTIGATIONS_SATISFACTORY", "string"),
    ("M_LOCAL_CLINIC_DETAILS", "string"),
    ("EXCESS_FLUID_OVERLOAD", "string"),
    ("PLEURAL_EFFUSION", "string"),
    ("PLEURAL_EFFUSION_MANAGEMENT", "string"),
    ("PNEUMOTHORAX", "string"),
    ("PNEUMOTHORAX_MANAGEMENT", "string"),
    ("MAXIMAL_DEPTHMM", "double"),
    ("INDICATION_FOR_PPM", "string"),
    ("NEW_CVA", "string"),
    ("FOLLOW_UP_ARRANGEMENTS", "string"),
    ("IT_UDISCHARGE_DT", "timestamp"),
    ("HD_UDISCHARGE_DT", "timestamp"),
    ("PEAK_CREATININE", "int"),
    ("IX_SATISFACTORY_PRIOR_TO_DISCHARGETXT", "string"),
    ("M_POST_OPERATIVE_ECHO_DATA", "string"),
    ("IS_DIALYSIS_NEEDED_POST_DISCHARGE", "string"),
    ("PAIN_DELAYING_DISCHARGE", "string"),
    ("REINTUBATION_DT", "timestamp"),
    ("EXTUBATION_DT", "timestamp"),
    ("LENGTH_OF_INTUBATION", "int"),
    ("IMPORT_CHILD_ID", "string"),
    ("DATE_LAST_CHANGED", "timestamp"),
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
]


@materialized_view(
    name=_n("journey_clinical.registry_surgery_followup"),
    refresh_policy="auto",
)
def registry_surgery_followup():
    # Build the declared dataset: Typed child of clinical_registry_entry for the
    # surgery_followup family. CS2010G follow-up module. The source is small and dormant but
    # semantically distinct. Duplicate EntryId rows are reduced deterministically to the latest
    # DateLastChanged/ADC_UPDT row. Every governed bronze column is retained under its lower-
    # case name.
    s = read_source(IWEB_REGISTRY_SLOTS["surgery_followup"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_SURGERY_FOLLOWUP_EXPECTED_SCHEMA:
        raise RuntimeError("surgery_followup: bronze schema drift" + f"; expected={REGISTRY_SURGERY_FOLLOWUP_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("surgery_followup"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["FLW_ID"].alias("flw_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["DISCHARGE_DT"].alias("discharge_dt"),
        s["TIME_OF_FOLLOWUP"].alias("time_of_followup"),
        s["PROCEDURE_STATUS"].alias("procedure_status"),
        s["PATIENT_STATUS_AT_FOLLOW_UP"].alias("patient_status_at_follow_up"),
        s["POST_OPERATIVE_COMPLICATIONTXT"].alias("post_operative_complicationtxt"),
        s["ATRIAL_FIBRILLATION"].alias("atrial_fibrillation"),
        s["ANTICOAGULATION_COMMENCED"].alias("anticoagulation_commenced"),
        s["LRT_ITXT"].alias("lrt_itxt"),
        s["POST_OPERATIVE_PYREXIA_TREATMENTTXT"].alias("post_operative_pyrexia_treatmenttxt"),
        s["STERNAL_WOUND_INFECTION_TREATMENTTXT"].alias("sternal_wound_infection_treatmenttxt"),
        s["M_STERNAL_WOUND_INFECTION_REOPERATION_TYPE"].alias("m_sternal_wound_infection_reoperation_type"),
        s["HARVEST_WOUND_INFECTION_TREATMENTTXT"].alias("harvest_wound_infection_treatmenttxt"),
        s["M_HARVEST_WOUND_INFECTION_REOPERATION_TYPE"].alias("m_harvest_wound_infection_reoperation_type"),
        s["POST_OPERATIVE_ILEUS"].alias("post_operative_ileus"),
        s["PERICARDIAL_EFFUSION"].alias("pericardial_effusion"),
        s["RESTERNOTOMY_FOR_BLEEDING_ON_DAY"].alias("resternotomy_for_bleeding_on_day"),
        s["INVESTIGATIONS_SATISFACTORY_PRIOR_TO_DISCHARGE"].alias("investigations_satisfactory_prior_to_discharge"),
        s["M_OTHER_INVESTIGATIONS_SATISFACTORY"].alias("m_other_investigations_satisfactory"),
        s["M_LOCAL_CLINIC_DETAILS"].alias("m_local_clinic_details"),
        s["EXCESS_FLUID_OVERLOAD"].alias("excess_fluid_overload"),
        s["PLEURAL_EFFUSION"].alias("pleural_effusion"),
        s["PLEURAL_EFFUSION_MANAGEMENT"].alias("pleural_effusion_management"),
        s["PNEUMOTHORAX"].alias("pneumothorax"),
        s["PNEUMOTHORAX_MANAGEMENT"].alias("pneumothorax_management"),
        s["MAXIMAL_DEPTHMM"].alias("maximal_depthmm"),
        s["INDICATION_FOR_PPM"].alias("indication_for_ppm"),
        s["NEW_CVA"].alias("new_cva"),
        s["FOLLOW_UP_ARRANGEMENTS"].alias("follow_up_arrangements"),
        s["IT_UDISCHARGE_DT"].alias("it_udischarge_dt"),
        s["HD_UDISCHARGE_DT"].alias("hd_udischarge_dt"),
        s["PEAK_CREATININE"].alias("peak_creatinine"),
        s["IX_SATISFACTORY_PRIOR_TO_DISCHARGETXT"].alias("ix_satisfactory_prior_to_dischargetxt"),
        s["M_POST_OPERATIVE_ECHO_DATA"].alias("m_post_operative_echo_data"),
        s["IS_DIALYSIS_NEEDED_POST_DISCHARGE"].alias("is_dialysis_needed_post_discharge"),
        s["PAIN_DELAYING_DISCHARGE"].alias("pain_delaying_discharge"),
        s["REINTUBATION_DT"].alias("reintubation_dt"),
        s["EXTUBATION_DT"].alias("extubation_dt"),
        s["LENGTH_OF_INTUBATION"].alias("length_of_intubation"),
        s["IMPORT_CHILD_ID"].alias("import_child_id"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
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
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_registry_surgery_followup': {'columns': {'adc_updt': 'Pipeline update timestamp for the record, '
                                                                'used with DATE_LAST_CHANGED to '
                                                                'deterministically select the latest row per '
                                                                'EntryId.',
                                                    'anticoagulation_commenced': 'Indicator of whether '
                                                                                 'anticoagulation therapy '
                                                                                 'was commenced following '
                                                                                 'surgery.',
                                                    'atrial_fibrillation': 'Indicator of whether '
                                                                           'post-operative atrial '
                                                                           'fibrillation was recorded at '
                                                                           'follow-up.',
                                                    'date_last_changed': 'Timestamp of the last change to '
                                                                         'the record in the iWeb source '
                                                                         'system, used with ADC_UPDT to '
                                                                         'select the latest row per EntryId.',
                                                    'date_of_birth': "Patient's date of birth as recorded in "
                                                                     'the iWeb CS2010G cardiac surgery '
                                                                     'follow-up record.',
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
                                                                             'overload during post-operative '
                                                                             'cardiac surgery follow-up, as '
                                                                             'entered in the iWeb CS2010G '
                                                                             'follow-up form.',
                                                    'extubation_dt': 'Date and time the patient was '
                                                                     'extubated following cardiac surgery.',
                                                    'flw_id': 'Internal iWeb identifier for the follow-up '
                                                              'entry within the CS2010G registry record.',
                                                    'follow_up_arrangements': 'Free-text description of the '
                                                                              'arranged post-discharge '
                                                                              'follow-up, such as the '
                                                                              'service or centre responsible '
                                                                              'for review.',
                                                    'forename': "Patient's forename as recorded in the iWeb "
                                                                'CS2010G cardiac surgery follow-up record.',
                                                    'gender': 'Recorded gender of the patient as captured in '
                                                              'the CS2010G follow-up registry.',
                                                    'harvest_wound_infection_treatmenttxt': 'Text '
                                                                                            'description of '
                                                                                            'treatment given '
                                                                                            'for conduit '
                                                                                            'harvest site '
                                                                                            'wound '
                                                                                            'infection.',
                                                    'hd_udischarge_dt': 'Date and time the patient was '
                                                                        'discharged from the high dependency '
                                                                        'unit following cardiac surgery.',
                                                    'import_child_id': 'Source system import identifier for '
                                                                       'the child follow-up record, carried '
                                                                       'through from the iWeb registry load '
                                                                       'process.',
                                                    'indication_for_ppm': 'Records the clinical indication '
                                                                          'for permanent pacemaker '
                                                                          'implantation noted at '
                                                                          'post-operative follow-up.',
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
                                                    'is_current_in_source': 'Boolean flag indicating whether '
                                                                            'the record is still present as '
                                                                            'the current version in the '
                                                                            'upstream iWeb source.',
                                                    'is_dialysis_needed_post_discharge': 'Indicates whether '
                                                                                         'the patient '
                                                                                         'required dialysis '
                                                                                         'after discharge, '
                                                                                         'as recorded at '
                                                                                         'cardiac surgery '
                                                                                         'follow-up.',
                                                    'it_udischarge_dt': 'Date and time the patient was '
                                                                        'discharged from the intensive '
                                                                        'therapy unit following cardiac '
                                                                        'surgery.',
                                                    'ix_satisfactory_prior_to_dischargetxt': 'Text list of '
                                                                                             'investigations '
                                                                                             'recorded as '
                                                                                             'satisfactory '
                                                                                             'prior to '
                                                                                             'discharge in '
                                                                                             'the iWeb '
                                                                                             'CS2010G '
                                                                                             'follow-up '
                                                                                             'form.',
                                                    'length_of_intubation': 'Recorded duration of intubation '
                                                                            'following cardiac surgery, in '
                                                                            'the units captured by the '
                                                                            'source registry.',
                                                    'linkage_fallback_conflict_ind': 'Boolean flag '
                                                                                     'indicating that the '
                                                                                     'fallback linkage '
                                                                                     'attempt produced '
                                                                                     'conflicting person '
                                                                                     'matches.',
                                                    'linkage_historical_fallback_ind': 'Boolean flag '
                                                                                       'indicating that '
                                                                                       'person linkage '
                                                                                       'relied on a '
                                                                                       'historical '
                                                                                       '(previous) '
                                                                                       'identifier rather '
                                                                                       'than a current one.',
                                                    'linkage_method': 'Code describing which identifier '
                                                                      'combination was used to link this '
                                                                      'follow-up record to the master person '
                                                                      'index during pipeline processing.',
                                                    'linkage_status': 'Derived status describing the outcome '
                                                                      'of linking this follow-up record to a '
                                                                      'Millennium person record.',
                                                    'lrt_itxt': 'Text description of lower respiratory tract '
                                                                'infection details recorded at follow-up.',
                                                    'm_harvest_wound_infection_reoperation_type': 'Type of '
                                                                                                  'reoperation '
                                                                                                  'performed '
                                                                                                  'for '
                                                                                                  'harvest '
                                                                                                  'site '
                                                                                                  'wound '
                                                                                                  'infection.',
                                                    'm_local_clinic_details': 'Free-text details of the '
                                                                              'local clinic arranged for the '
                                                                              "patient's follow-up.",
                                                    'm_other_investigations_satisfactory': 'Free-text detail '
                                                                                           'of other '
                                                                                           'investigations '
                                                                                           'found '
                                                                                           'satisfactory '
                                                                                           'prior to '
                                                                                           'discharge.',
                                                    'm_post_operative_echo_data': 'Free-text notes capturing '
                                                                                  'post-operative '
                                                                                  'echocardiography findings '
                                                                                  'recorded at follow-up.',
                                                    'm_sternal_wound_infection_reoperation_type': 'Type of '
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
                                                    'mrn': 'Local hospital medical record number identifying '
                                                           'the patient for the follow-up record.',
                                                    'new_cva': 'Indicates whether a new cerebrovascular '
                                                               'accident was recorded during post-operative '
                                                               'cardiac surgery follow-up.',
                                                    'nhs_number': "Patient's NHS number as recorded in the "
                                                                  'iWeb CS2010G cardiac surgery follow-up '
                                                                  'record, stored as text.',
                                                    'nhs_number_valid_ind': 'Derived flag indicating whether '
                                                                            'the source NHS number passed '
                                                                            'validation; the NHS number '
                                                                            'itself is not retained in this '
                                                                            'table.',
                                                    'pain_delaying_discharge': 'Indicates whether pain was '
                                                                               'recorded as a factor '
                                                                               "delaying the patient's "
                                                                               'discharge after cardiac '
                                                                               'surgery.',
                                                    'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                         'clinical_registry_entry.patient_event_key '
                                                                         'for the surgery_followup source '
                                                                         'family and native entry_id.',
                                                    'patient_status_at_follow_up': 'Coded status of the '
                                                                                   'patient (such as '
                                                                                   'clinical/vital status) '
                                                                                   'recorded at the '
                                                                                   'follow-up assessment.',
                                                    'peak_creatinine': 'Highest recorded post-operative '
                                                                       'serum creatinine value for the '
                                                                       'patient during the follow-up period, '
                                                                       'in the units captured by the source '
                                                                       'registry.',
                                                    'pericardial_effusion': 'Indicator of whether '
                                                                            'pericardial effusion was '
                                                                            'recorded at follow-up.',
                                                    'person_id': 'Internal Millennium person identifier '
                                                                 'linked to this follow-up record, used to '
                                                                 'join to patient-level tables.',
                                                    'pipeline_loaded_at': 'Timestamp recording when the row '
                                                                          'was loaded into this bronze table '
                                                                          'by the ingestion pipeline.',
                                                    'pleural_effusion': 'Indicates whether a pleural '
                                                                        'effusion was recorded during '
                                                                        'post-operative cardiac surgery '
                                                                        'follow-up.',
                                                    'pleural_effusion_management': 'Describes the management '
                                                                                   'or treatment applied for '
                                                                                   'a recorded '
                                                                                   'post-operative pleural '
                                                                                   'effusion.',
                                                    'pneumothorax': 'Indicates whether a pneumothorax was '
                                                                    'recorded during post-operative cardiac '
                                                                    'surgery follow-up.',
                                                    'pneumothorax_management': 'Describes the management or '
                                                                               'treatment applied for a '
                                                                               'recorded post-operative '
                                                                               'pneumothorax.',
                                                    'post_operative_complicationtxt': 'Text description of '
                                                                                      'post-operative '
                                                                                      'complications '
                                                                                      'recorded at '
                                                                                      'follow-up, typically '
                                                                                      'a coded value with '
                                                                                      'its descriptive '
                                                                                      'label.',
                                                    'post_operative_ileus': 'Indicator of whether '
                                                                            'post-operative ileus was '
                                                                            'recorded at follow-up.',
                                                    'post_operative_pyrexia_treatmenttxt': 'Text description '
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
                                                    'resternotomy_for_bleeding_on_day': 'Post-operative day '
                                                                                        'number on which '
                                                                                        'resternotomy for '
                                                                                        'bleeding was '
                                                                                        'performed.',
                                                    'row_hash': "Deterministic hash of the source record's "
                                                                'field values used to detect changes between '
                                                                'pipeline loads.',
                                                    'source_absent_detected_ts': 'Timestamp recording when '
                                                                                 'the pipeline first '
                                                                                 'detected that the record '
                                                                                 'was no longer present in '
                                                                                 'the upstream iWeb source.',
                                                    'source_present_ind': 'Canonical boolean mirror of '
                                                                          'IS_CURRENT_IN_SOURCE; false rows '
                                                                          'are retained source tombstones.',
                                                    'source_record_key': 'String representation of the '
                                                                         'upstream iWeb EntryId identifying '
                                                                         'the source follow-up record for '
                                                                         'traceability back to the feeder '
                                                                         'table.',
                                                    'source_table': 'Name of the upstream iWeb source table '
                                                                    'from which the record was ingested.',
                                                    'sternal_wound_infection_treatmenttxt': 'Text '
                                                                                            'description of '
                                                                                            'treatment given '
                                                                                            'for sternal '
                                                                                            'wound '
                                                                                            'infection.',
                                                    'surname': "Patient's surname as recorded in the iWeb "
                                                               'CS2010G cardiac surgery follow-up record.',
                                                    'time_of_followup': 'Time recorded for the follow-up '
                                                                        'assessment, stored as text.'},
                                        'table': 'Typed child of clinical_registry_entry for the '
                                                 'surgery_followup family. CS2010G follow-up module. The '
                                                 'source is small and dormant but semantically distinct. '
                                                 'Duplicate EntryId rows are reduced deterministically to '
                                                 'the latest DateLastChanged/ADC_UPDT row. Every governed '
                                                 'bronze column is retained under its lower-case name.'}}
