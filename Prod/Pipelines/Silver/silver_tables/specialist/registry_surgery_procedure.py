"""Silver specialist.registry_surgery_procedure: 1 dataset(s).

- datasets: clinical_registry_surgery_procedure
- moved verbatim from: 3_silver_specialist_care:3262, 3_silver_specialist_care:3576

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


# Clinical / registry surgery procedure

# ==== journey_clinical.registry_surgery_procedure ====
REGISTRY_SURGERY_PROCEDURE_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("PARENT_ENTRY_ID", "int"),
    ("PROCEDURE_SITE", "string"),
    ("GRAFT_CONDUIT", "string"),
    ("GRAFT_ANASTOMOSIS", "string"),
    ("GRAFT_SITE", "string"),
    ("VALVE_IMPLANT_NAME", "string"),
    ("VALVE_HAEMODYNAMIC_PATHOLOGY", "string"),
    ("NATIVE_VALVE_PATHOLOGY", "string"),
    ("NATIVE_VALVE_OTHER_PATHOLOGY", "string"),
    ("REASON_FOR_REPEAT_VALVE_REPLACEMENTTXT", "string"),
    ("OTHER_REASON_FOR_REPEAT_VALVE_REPLACEMENT", "string"),
    ("VALVE_PROCEDURE", "string"),
    ("VALVE_IMPLANT_TYPE", "string"),
    ("IMPLANT_PROSTHESIS_NAMETXT", "string"),
    ("VALVE_OR_RING_MODEL_NUMBER", "string"),
    ("VALVE_OR_RING_SERIAL_NUMBER", "string"),
    ("VALVE_OR_RING_SIZE", "int"),
    ("VALVE_EXPLANT_TYPE", "string"),
    ("AORTIC_PATHOLOGYTXT", "string"),
    ("AORTIC_PROCEDURE", "string"),
    ("AORTIC_ARCH_TYPE", "string"),
    ("DISTAL_ANASTAMOSIS_CONSTRUCTED_BYTXT", "string"),
    ("PROXIMAL_ANASTAMOSIS_CONSTRUCTED_BYTXT", "string"),
    ("AORTOTOMYTXT", "string"),
    ("EXCISION_OF_VALVETXT", "string"),
    ("DECALCIFICATION_OF_ANNULUSTXT", "string"),
    ("IMPLANTATION_OF_VALVETXT", "string"),
    ("CLOSURE_OF_AORTOTOMYTXT", "string"),
    ("DE_AIRING_OF_HEARTTXT", "string"),
    ("BI_CAVAL_CANNULATIONTXT", "string"),
    ("ACCESS_TO_MITRAL_VALVETXT", "string"),
    ("ASSESSMENT_AND_REPAIRTXT", "string"),
    ("EXCISION_OF_VALVE_AND_ANNULAR_DEBRIDEMENTTXT", "string"),
    ("REPAIR_OF_VALVETXT", "string"),
    ("RINGTXT", "string"),
    ("IMPLANTATION_OF_MITRAL_VALVETXT", "string"),
    ("ATRIAL_CLOSURETXT", "string"),
    ("DE_AIRING_OF_HEART_MITRALTXT", "string"),
    ("LIMATXT", "string"),
    ("RIMATXT", "string"),
    ("LSVTXT", "string"),
    ("SSVTXT", "string"),
    ("RADIALTXT", "string"),
    ("OTHER_VEINTXT", "string"),
    ("OTHER_ARTERYTXT", "string"),
    ("CONDUIT_QUALITY", "string"),
    ("GRAFTED_USING", "string"),
    ("CLOSED_USING", "string"),
    ("VALVE_IMPLANTED_USING", "string"),
    ("SIZE_OF_CORONARY_ARTERY", "string"),
    ("CORONARY_ARTERY_CONDITION", "string"),
    ("PRIMARY_REPAIR_TECHNIQUETXT", "string"),
    ("NEOCHORD", "string"),
    ("MINIMALLY_INVASIVE_MITRAL_VALVE_SURGERY", "string"),
    ("M_AORTIC_GRAFT_DETAIL", "string"),
    ("AORTIC_GRAFT_SERIAL_NUMBER", "string"),
    ("REIMPLANTATION_OF_INTERCOSTALS", "string"),
    ("IMPLANTATION_TECHNIQUE", "string"),
    ("M_LEVEL_OF_IMPLANTATION", "string"),
    ("AORTIC_OCCLUSION_BYPASS", "string"),
    ("S_VCHARACTERISTICSTXT", "string"),
    ("HARVESTING_CHARACTERISTICSTXT", "string"),
    ("HOLES", "string"),
    ("A_GSCORE", "double"),
    ("MSV_GCOMMENTS", "string"),
    ("HARVESTING_TECHNIQUE", "string"),
    ("AMOUNT_OF_HEPARIN_BEFORE_EVH", "double"),
    ("IMPORT_CHILD_ID", "string"),
    ("NATIVE_AORTIC_VALVE_MORPHOLOGY", "string"),
    ("AORTIC_PROCEDURE2TXT", "string"),
    ("AORTIC_IMPLANT_LOCATIONTXT", "string"),
    ("DATE_LAST_CHANGED", "timestamp"),
    ("WALL_QUALITY", "string"),
    ("ACCESSIBILITY", "string"),
    ("DISTAL_RUN_OFF", "string"),
    ("TVC_SINTERPRETATION", "string"),
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
    name=_n("journey_clinical.registry_surgery_procedure"),
    refresh_policy="auto",
)
def registry_surgery_procedure():
    # Build the declared dataset: Typed child of clinical_registry_entry for the
    # surgery_procedure family. One row per graft, valve or aortic-implant component of a
    # CS2010G surgery. PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is hard-gated to
    # zero orphans. Known source defect: IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist
    # bleed-through. Every governed bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["surgery_procedure"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_SURGERY_PROCEDURE_EXPECTED_SCHEMA:
        raise RuntimeError("surgery_procedure: bronze schema drift" + f"; expected={REGISTRY_SURGERY_PROCEDURE_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("surgery_procedure"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["PARENT_ENTRY_ID"].alias("parent_entry_id"),
        s["PROCEDURE_SITE"].alias("procedure_site"),
        s["GRAFT_CONDUIT"].alias("graft_conduit"),
        s["GRAFT_ANASTOMOSIS"].alias("graft_anastomosis"),
        s["GRAFT_SITE"].alias("graft_site"),
        s["VALVE_IMPLANT_NAME"].alias("valve_implant_name"),
        s["VALVE_HAEMODYNAMIC_PATHOLOGY"].alias("valve_haemodynamic_pathology"),
        s["NATIVE_VALVE_PATHOLOGY"].alias("native_valve_pathology"),
        s["NATIVE_VALVE_OTHER_PATHOLOGY"].alias("native_valve_other_pathology"),
        s["REASON_FOR_REPEAT_VALVE_REPLACEMENTTXT"].alias("reason_for_repeat_valve_replacementtxt"),
        s["OTHER_REASON_FOR_REPEAT_VALVE_REPLACEMENT"].alias("other_reason_for_repeat_valve_replacement"),
        s["VALVE_PROCEDURE"].alias("valve_procedure"),
        s["VALVE_IMPLANT_TYPE"].alias("valve_implant_type"),
        s["IMPLANT_PROSTHESIS_NAMETXT"].alias("implant_prosthesis_nametxt"),
        s["VALVE_OR_RING_MODEL_NUMBER"].alias("valve_or_ring_model_number"),
        s["VALVE_OR_RING_SERIAL_NUMBER"].alias("valve_or_ring_serial_number"),
        s["VALVE_OR_RING_SIZE"].alias("valve_or_ring_size"),
        s["VALVE_EXPLANT_TYPE"].alias("valve_explant_type"),
        s["AORTIC_PATHOLOGYTXT"].alias("aortic_pathologytxt"),
        s["AORTIC_PROCEDURE"].alias("aortic_procedure"),
        s["AORTIC_ARCH_TYPE"].alias("aortic_arch_type"),
        s["DISTAL_ANASTAMOSIS_CONSTRUCTED_BYTXT"].alias("distal_anastamosis_constructed_bytxt"),
        s["PROXIMAL_ANASTAMOSIS_CONSTRUCTED_BYTXT"].alias("proximal_anastamosis_constructed_bytxt"),
        s["AORTOTOMYTXT"].alias("aortotomytxt"),
        s["EXCISION_OF_VALVETXT"].alias("excision_of_valvetxt"),
        s["DECALCIFICATION_OF_ANNULUSTXT"].alias("decalcification_of_annulustxt"),
        s["IMPLANTATION_OF_VALVETXT"].alias("implantation_of_valvetxt"),
        s["CLOSURE_OF_AORTOTOMYTXT"].alias("closure_of_aortotomytxt"),
        s["DE_AIRING_OF_HEARTTXT"].alias("de_airing_of_hearttxt"),
        s["BI_CAVAL_CANNULATIONTXT"].alias("bi_caval_cannulationtxt"),
        s["ACCESS_TO_MITRAL_VALVETXT"].alias("access_to_mitral_valvetxt"),
        s["ASSESSMENT_AND_REPAIRTXT"].alias("assessment_and_repairtxt"),
        s["EXCISION_OF_VALVE_AND_ANNULAR_DEBRIDEMENTTXT"].alias("excision_of_valve_and_annular_debridementtxt"),
        s["REPAIR_OF_VALVETXT"].alias("repair_of_valvetxt"),
        s["RINGTXT"].alias("ringtxt"),
        s["IMPLANTATION_OF_MITRAL_VALVETXT"].alias("implantation_of_mitral_valvetxt"),
        s["ATRIAL_CLOSURETXT"].alias("atrial_closuretxt"),
        s["DE_AIRING_OF_HEART_MITRALTXT"].alias("de_airing_of_heart_mitraltxt"),
        s["LIMATXT"].alias("limatxt"),
        s["RIMATXT"].alias("rimatxt"),
        s["LSVTXT"].alias("lsvtxt"),
        s["SSVTXT"].alias("ssvtxt"),
        s["RADIALTXT"].alias("radialtxt"),
        s["OTHER_VEINTXT"].alias("other_veintxt"),
        s["OTHER_ARTERYTXT"].alias("other_arterytxt"),
        s["CONDUIT_QUALITY"].alias("conduit_quality"),
        s["GRAFTED_USING"].alias("grafted_using"),
        s["CLOSED_USING"].alias("closed_using"),
        s["VALVE_IMPLANTED_USING"].alias("valve_implanted_using"),
        s["SIZE_OF_CORONARY_ARTERY"].alias("size_of_coronary_artery"),
        s["CORONARY_ARTERY_CONDITION"].alias("coronary_artery_condition"),
        s["PRIMARY_REPAIR_TECHNIQUETXT"].alias("primary_repair_techniquetxt"),
        s["NEOCHORD"].alias("neochord"),
        s["MINIMALLY_INVASIVE_MITRAL_VALVE_SURGERY"].alias("minimally_invasive_mitral_valve_surgery"),
        s["M_AORTIC_GRAFT_DETAIL"].alias("m_aortic_graft_detail"),
        s["AORTIC_GRAFT_SERIAL_NUMBER"].alias("aortic_graft_serial_number"),
        s["REIMPLANTATION_OF_INTERCOSTALS"].alias("reimplantation_of_intercostals"),
        s["IMPLANTATION_TECHNIQUE"].alias("implantation_technique"),
        s["M_LEVEL_OF_IMPLANTATION"].alias("m_level_of_implantation"),
        s["AORTIC_OCCLUSION_BYPASS"].alias("aortic_occlusion_bypass"),
        s["S_VCHARACTERISTICSTXT"].alias("s_vcharacteristicstxt"),
        s["HARVESTING_CHARACTERISTICSTXT"].alias("harvesting_characteristicstxt"),
        s["HOLES"].alias("holes"),
        s["A_GSCORE"].alias("a_gscore"),
        s["MSV_GCOMMENTS"].alias("msv_gcomments"),
        s["HARVESTING_TECHNIQUE"].alias("harvesting_technique"),
        s["AMOUNT_OF_HEPARIN_BEFORE_EVH"].alias("amount_of_heparin_before_evh"),
        s["IMPORT_CHILD_ID"].alias("import_child_id"),
        s["NATIVE_AORTIC_VALVE_MORPHOLOGY"].alias("native_aortic_valve_morphology"),
        s["AORTIC_PROCEDURE2TXT"].alias("aortic_procedure2txt"),
        s["AORTIC_IMPLANT_LOCATIONTXT"].alias("aortic_implant_locationtxt"),
        s["DATE_LAST_CHANGED"].alias("date_last_changed"),
        s["WALL_QUALITY"].alias("wall_quality"),
        s["ACCESSIBILITY"].alias("accessibility"),
        s["DISTAL_RUN_OFF"].alias("distal_run_off"),
        s["TVC_SINTERPRETATION"].alias("tvc_sinterpretation"),
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


COMMENTS = {'clinical_registry_surgery_procedure': {'columns': {'a_gscore': 'Numeric assessment score recorded for the '
                                                                 'harvested conduit on the iWeb CS2010G '
                                                                 'subprocedure form.',
                                                     'access_to_mitral_valvetxt': 'Free-text record of the '
                                                                                  'operator who performed '
                                                                                  'the access to the mitral '
                                                                                  'valve step of this '
                                                                                  'CS2010G mitral valve '
                                                                                  'procedure.',
                                                     'accessibility': 'Recorded accessibility of the target '
                                                                      'vessel for this graft component, as '
                                                                      'captured in iWeb.',
                                                     'adc_updt': 'Timestamp of the last update applied to '
                                                                 'this record by the upstream data '
                                                                 'collection process.',
                                                     'amount_of_heparin_before_evh': 'Amount of heparin '
                                                                                     'given before '
                                                                                     'endoscopic vein '
                                                                                     'harvesting, recorded '
                                                                                     'as a number without a '
                                                                                     'stated unit in the '
                                                                                     'source.',
                                                     'aortic_arch_type': 'Coded classification of the aortic '
                                                                         'arch type recorded for the aortic '
                                                                         'component of this CS2010G surgery.',
                                                     'aortic_graft_serial_number': 'Manufacturer serial '
                                                                                   'number of the aortic '
                                                                                   'graft implanted for this '
                                                                                   'component.',
                                                     'aortic_implant_locationtxt': 'Text value recording the '
                                                                                   'anatomical location of '
                                                                                   'the aortic implant for '
                                                                                   'this surgery component, '
                                                                                   'as captured in iWeb.',
                                                     'aortic_occlusion_bypass': 'Records the aortic '
                                                                                'occlusion or bypass '
                                                                                'approach used during this '
                                                                                'aortic component of the '
                                                                                'surgery.',
                                                     'aortic_pathologytxt': 'Text description of the aortic '
                                                                            'pathology recorded for the '
                                                                            'aortic component of this '
                                                                            'CS2010G surgery in iWeb.',
                                                     'aortic_procedure': 'Type of aortic procedure performed '
                                                                         'for this component, as recorded in '
                                                                         'the iWeb CS2010G subprocedure '
                                                                         'form.',
                                                     'aortic_procedure2txt': 'Text description of an '
                                                                             'additional aortic procedure '
                                                                             'recorded for this component '
                                                                             'alongside AORTIC_PROCEDURE.',
                                                     'aortotomytxt': 'Free-text record of the operator who '
                                                                     'performed the aortotomy step of this '
                                                                     'CS2010G aortic valve procedure.',
                                                     'assessment_and_repairtxt': 'Free-text record of the '
                                                                                 'operator who performed the '
                                                                                 'mitral valve assessment '
                                                                                 'and repair step of this '
                                                                                 'CS2010G procedure.',
                                                     'atrial_closuretxt': 'Free-text record of the operator '
                                                                          'who performed the atrial closure '
                                                                          'step of this CS2010G mitral valve '
                                                                          'procedure.',
                                                     'bi_caval_cannulationtxt': 'Free-text record of the '
                                                                                'operator who performed the '
                                                                                'bi-caval cannulation step '
                                                                                'of this CS2010G mitral '
                                                                                'valve procedure.',
                                                     'closed_using': 'Suture or material used for closure at '
                                                                     'this stage of the CS2010G surgery '
                                                                     'component, as recorded in iWeb.',
                                                     'closure_of_aortotomytxt': 'Free-text record of the '
                                                                                'operator who performed the '
                                                                                'aortotomy closure step of '
                                                                                'this CS2010G aortic valve '
                                                                                'procedure.',
                                                     'conduit_quality': 'Recorded quality assessment of the '
                                                                        'graft conduit for this CS2010G '
                                                                        'subprocedure component, as captured '
                                                                        'in iWeb.',
                                                     'coronary_artery_condition': 'Recorded condition of the '
                                                                                  'target coronary artery '
                                                                                  'for this graft component '
                                                                                  'in the iWeb CS2010G form.',
                                                     'date_last_changed': 'Timestamp of the last change made '
                                                                          'to this subprocedure record in '
                                                                          'the iWeb source system.',
                                                     'date_of_birth': 'Patient date of birth as recorded on '
                                                                      'the iWeb CS2010G subprocedure record.',
                                                     'date_of_death': 'Recorded date of death for the '
                                                                      'patient associated with this surgery '
                                                                      'record, null where no death is '
                                                                      'recorded.',
                                                     'de_airing_of_heart_mitraltxt': 'Free-text record of '
                                                                                     'the operator who '
                                                                                     'performed the '
                                                                                     'de-airing of the heart '
                                                                                     'step of this CS2010G '
                                                                                     'mitral valve '
                                                                                     'procedure.',
                                                     'de_airing_of_hearttxt': 'Free-text record of the '
                                                                              'operator who performed the '
                                                                              'de-airing of the heart step '
                                                                              'of this CS2010G aortic valve '
                                                                              'procedure.',
                                                     'decalcification_of_annulustxt': 'Free-text record of '
                                                                                      'the operator who '
                                                                                      'performed the annulus '
                                                                                      'decalcification step '
                                                                                      'of this CS2010G '
                                                                                      'aortic valve '
                                                                                      'procedure.',
                                                     'distal_anastamosis_constructed_bytxt': 'Free-text '
                                                                                             'record of the '
                                                                                             'operator who '
                                                                                             'constructed '
                                                                                             'the distal '
                                                                                             'anastomosis '
                                                                                             'during this '
                                                                                             'CS2010G '
                                                                                             'surgical '
                                                                                             'component.',
                                                     'distal_run_off': 'Recorded distal run-off assessment '
                                                                       'of the target coronary artery for '
                                                                       'this graft component.',
                                                     'entry_id': 'Stable iWeb child-record key.',
                                                     'ethnic_origin': 'Coded ethnic origin of the patient as '
                                                                      'recorded in iWeb.',
                                                     'excision_of_valve_and_annular_debridementtxt': 'Free-text '
                                                                                                     'record '
                                                                                                     'of the '
                                                                                                     'operator '
                                                                                                     'who '
                                                                                                     'performed '
                                                                                                     'the '
                                                                                                     'valve '
                                                                                                     'excision '
                                                                                                     'and '
                                                                                                     'annular '
                                                                                                     'debridement '
                                                                                                     'step '
                                                                                                     'of '
                                                                                                     'this '
                                                                                                     'CS2010G '
                                                                                                     'mitral '
                                                                                                     'valve '
                                                                                                     'procedure.',
                                                     'excision_of_valvetxt': 'Free-text record of the '
                                                                             'operator who performed the '
                                                                             'valve excision step of this '
                                                                             'CS2010G aortic valve '
                                                                             'procedure.',
                                                     'forename': 'Patient forename as recorded on the iWeb '
                                                                 'CS2010G subprocedure record.',
                                                     'gender': 'Patient gender recorded on the iWeb cardiac '
                                                               'surgery record, held as a descriptive text '
                                                               'value.',
                                                     'graft_anastomosis': 'Anastomosis technique used for '
                                                                          'this graft, from the iWeb '
                                                                          'picklist.',
                                                     'graft_conduit': 'Type of conduit used for this '
                                                                      'coronary graft, from the iWeb '
                                                                      'picklist.',
                                                     'graft_site': 'Target coronary anatomical site to which '
                                                                   'this graft was anastomosed, from the '
                                                                   'iWeb picklist.',
                                                     'grafted_using': 'Suture or material used to construct '
                                                                      'the graft anastomosis for this '
                                                                      'component, as recorded in iWeb.',
                                                     'harvesting_characteristicstxt': 'Text description of '
                                                                                      'conduit harvesting '
                                                                                      'characteristics '
                                                                                      'recorded for this '
                                                                                      'graft component.',
                                                     'harvesting_technique': 'Technique used to harvest the '
                                                                             'conduit for this graft '
                                                                             'component, as recorded in '
                                                                             'iWeb.',
                                                     'holes': 'Records holes identified in the harvested '
                                                              'conduit for this graft component, as entered '
                                                              'in iWeb.',
                                                     'implant_prosthesis_nametxt': 'Text label of the '
                                                                                   'implanted prosthesis '
                                                                                   'selected in iWeb; known '
                                                                                   'source defect means some '
                                                                                   'values are drug-picklist '
                                                                                   'bleed-through rather '
                                                                                   'than prosthesis names.',
                                                     'implantation_of_mitral_valvetxt': 'Free-text record of '
                                                                                        'the operator who '
                                                                                        'performed the '
                                                                                        'mitral valve '
                                                                                        'implantation step '
                                                                                        'of this CS2010G '
                                                                                        'procedure.',
                                                     'implantation_of_valvetxt': 'Free-text record of the '
                                                                                 'operator who performed the '
                                                                                 'valve implantation step of '
                                                                                 'this CS2010G aortic valve '
                                                                                 'procedure.',
                                                     'implantation_technique': 'Technique used to implant '
                                                                               'the prosthesis or graft for '
                                                                               'this component, as recorded '
                                                                               'in iWeb.',
                                                     'import_child_id': 'Legacy child-record identifier '
                                                                        'carried over from data imported '
                                                                        'into iWeb for this component row.',
                                                     'is_current_in_source': 'Boolean flag indicating '
                                                                             'whether the record is still '
                                                                             'present as a current row in '
                                                                             'the upstream source system.',
                                                     'limatxt': 'Free-text record of the operator who '
                                                                'harvested or used the left internal mammary '
                                                                'artery conduit in this CS2010G graft '
                                                                'component.',
                                                     'linkage_fallback_conflict_ind': 'Boolean indicator '
                                                                                      'flagging that the '
                                                                                      'fallback linkage rule '
                                                                                      'produced conflicting '
                                                                                      'person matches for '
                                                                                      'this record.',
                                                     'linkage_historical_fallback_ind': 'Boolean indicator '
                                                                                        'showing whether the '
                                                                                        'person linkage was '
                                                                                        'resolved using a '
                                                                                        'historical '
                                                                                        'identifier fallback '
                                                                                        'rule.',
                                                     'linkage_method': 'Code describing which identifier '
                                                                       'combination was used to link this '
                                                                       'record to the master person index.',
                                                     'linkage_status': 'Coded outcome of the patient linkage '
                                                                       'process between this registry record '
                                                                       'and the master person index.',
                                                     'lsvtxt': 'Free-text record of the operator who '
                                                               'harvested or used the long saphenous vein '
                                                               'conduit in this CS2010G graft component.',
                                                     'm_aortic_graft_detail': 'Descriptive detail of the '
                                                                              'aortic graft used for this '
                                                                              'implant component, as '
                                                                              'recorded in iWeb.',
                                                     'm_level_of_implantation': 'Anatomical level at which '
                                                                                'the aortic graft or '
                                                                                'prosthesis was implanted '
                                                                                'for this component.',
                                                     'minimally_invasive_mitral_valve_surgery': 'Indicates '
                                                                                                'whether a '
                                                                                                'minimally '
                                                                                                'invasive '
                                                                                                'approach '
                                                                                                'was used '
                                                                                                'for the '
                                                                                                'mitral '
                                                                                                'valve '
                                                                                                'component '
                                                                                                'of this '
                                                                                                'surgery.',
                                                     'mrn': 'Local hospital medical record number '
                                                            'identifying the patient for this cardiac '
                                                            'surgery component record.',
                                                     'msv_gcomments': 'Free-text comments recorded about the '
                                                                      'saphenous vein graft harvesting for '
                                                                      'this component.',
                                                     'native_aortic_valve_morphology': 'Recorded morphology '
                                                                                       "of the patient's "
                                                                                       'native aortic valve '
                                                                                       'for this valve '
                                                                                       'component.',
                                                     'native_valve_other_pathology': 'Free-text description '
                                                                                     'of other native valve '
                                                                                     'pathology where the '
                                                                                     'standard picklist '
                                                                                     'category does not '
                                                                                     'apply.',
                                                     'native_valve_pathology': 'Coded pathology of the '
                                                                               "patient's native valve "
                                                                               'recorded for this valve '
                                                                               'component in iWeb.',
                                                     'neochord': 'Indicates whether neochordae were used or '
                                                                 'describes the neochord detail for this '
                                                                 'mitral valve repair component.',
                                                     'nhs_number': 'Patient NHS number as recorded on the '
                                                                   'iWeb CS2010G subprocedure record.',
                                                     'nhs_number_valid_ind': 'Boolean indicator showing '
                                                                             'whether the NHS number held on '
                                                                             'the source record passed '
                                                                             'validation, derived from the '
                                                                             'source NHS number.',
                                                     'other_arterytxt': 'Free-text record of the operator '
                                                                        'who harvested or used another '
                                                                        'arterial conduit in this CS2010G '
                                                                        'graft component.',
                                                     'other_reason_for_repeat_valve_replacement': 'Free-text '
                                                                                                  'description '
                                                                                                  'of '
                                                                                                  'another '
                                                                                                  'reason '
                                                                                                  'for '
                                                                                                  'repeat '
                                                                                                  'valve '
                                                                                                  'replacement '
                                                                                                  'where the '
                                                                                                  'standard '
                                                                                                  'picklist '
                                                                                                  'category '
                                                                                                  'does not '
                                                                                                  'apply.',
                                                     'other_veintxt': 'Free-text record of the operator who '
                                                                      'harvested or used another venous '
                                                                      'conduit in this CS2010G graft '
                                                                      'component.',
                                                     'parent_entry_id': 'Foreign key to '
                                                                        'iweb_cardiac_surgery_episode.ENTRY_ID.',
                                                     'patient_event_key': 'Deterministic SHA-256 key '
                                                                          'matching '
                                                                          'clinical_registry_entry.patient_event_key '
                                                                          'for the surgery_procedure source '
                                                                          'family and native entry_id.',
                                                     'person_id': 'Internal surrogate person identifier from '
                                                                  'the Millennium person index, used to link '
                                                                  'this record to the patient master.',
                                                     'pipeline_loaded_at': 'Timestamp recording when this '
                                                                           'row was loaded into the bronze '
                                                                           'table by the ingestion pipeline.',
                                                     'primary_repair_techniquetxt': 'Text description of the '
                                                                                    'primary valve repair '
                                                                                    'technique used for this '
                                                                                    'component, as recorded '
                                                                                    'in iWeb.',
                                                     'procedure_site': 'Anatomical site of the heart or '
                                                                       'coronary target at which this '
                                                                       'surgery component was performed, as '
                                                                       'recorded in the iWeb CS2010G '
                                                                       'subprocedure form.',
                                                     'proximal_anastamosis_constructed_bytxt': 'Free-text '
                                                                                               'record of '
                                                                                               'the operator '
                                                                                               'who '
                                                                                               'constructed '
                                                                                               'the proximal '
                                                                                               'anastomosis '
                                                                                               'during this '
                                                                                               'CS2010G '
                                                                                               'surgical '
                                                                                               'component.',
                                                     'radialtxt': 'Free-text record of the operator who '
                                                                  'harvested or used the radial artery '
                                                                  'conduit in this CS2010G graft component.',
                                                     'reason_for_repeat_valve_replacementtxt': 'Text value '
                                                                                               'recording '
                                                                                               'the reason a '
                                                                                               'repeat valve '
                                                                                               'replacement '
                                                                                               'was required '
                                                                                               'for this '
                                                                                               'valve '
                                                                                               'component, '
                                                                                               'as captured '
                                                                                               'in iWeb.',
                                                     'reimplantation_of_intercostals': 'Records whether '
                                                                                       'intercostal arteries '
                                                                                       'were reimplanted '
                                                                                       'during this aortic '
                                                                                       'implant component.',
                                                     'repair_of_valvetxt': 'Free-text record of the operator '
                                                                           'who performed the mitral valve '
                                                                           'repair step of this CS2010G '
                                                                           'procedure.',
                                                     'rimatxt': 'Free-text record of the operator who '
                                                                'harvested or used the right internal '
                                                                'mammary artery conduit in this CS2010G '
                                                                'graft component.',
                                                     'ringtxt': 'Free-text record of the operator who '
                                                                'performed the annuloplasty ring step of '
                                                                'this CS2010G mitral valve procedure.',
                                                     'row_hash': 'Hash of the source column values for this '
                                                                 'row, used for change detection during '
                                                                 'ingestion.',
                                                     's_vcharacteristicstxt': 'Text description of the '
                                                                              'saphenous vein conduit '
                                                                              'characteristics recorded for '
                                                                              'this graft component.',
                                                     'size_of_coronary_artery': 'Recorded size of the target '
                                                                                'coronary artery for this '
                                                                                'graft component, held as '
                                                                                'text including the unit as '
                                                                                'entered in iWeb.',
                                                     'source_absent_detected_ts': 'Timestamp recording when '
                                                                                  'this record was first '
                                                                                  'detected as no longer '
                                                                                  'present in the upstream '
                                                                                  'iWeb source system, null '
                                                                                  'while the record remains '
                                                                                  'present.',
                                                     'source_present_ind': 'Canonical boolean mirror of '
                                                                           'IS_CURRENT_IN_SOURCE; false rows '
                                                                           'are retained source tombstones.',
                                                     'source_record_key': 'Key identifying this row in the '
                                                                          'upstream source table, derived '
                                                                          'from the source record '
                                                                          'identifier.',
                                                     'source_table': 'Name of the upstream source table from '
                                                                     'which this row was ingested.',
                                                     'ssvtxt': 'Free-text record of the operator who '
                                                               'harvested or used the short saphenous vein '
                                                               'conduit in this CS2010G graft component.',
                                                     'surname': 'Patient surname as recorded on the iWeb '
                                                                'CS2010G subprocedure record.',
                                                     'tvc_sinterpretation': 'Recorded interpretation of the '
                                                                            'transit time flow or vessel '
                                                                            'check assessment for this graft '
                                                                            'component, as entered in iWeb.',
                                                     'valve_explant_type': 'Type of valve or prior repair '
                                                                           'removed at this procedure, from '
                                                                           'the iWeb picklist.',
                                                     'valve_haemodynamic_pathology': 'Haemodynamic pathology '
                                                                                     'of the valve, such as '
                                                                                     'the recorded '
                                                                                     'functional lesion '
                                                                                     'category, from the '
                                                                                     'iWeb picklist.',
                                                     'valve_implant_name': 'Valve position or implant '
                                                                           'designation recorded for this '
                                                                           'valve component in iWeb.',
                                                     'valve_implant_type': 'Category of valve implant used '
                                                                           'for this component, as recorded '
                                                                           'in the iWeb picklist.',
                                                     'valve_implanted_using': 'Suture technique or material '
                                                                              'used to implant the valve '
                                                                              'prosthesis or ring for this '
                                                                              'component, as recorded in '
                                                                              'iWeb.',
                                                     'valve_or_ring_model_number': 'Manufacturer model '
                                                                                   'number of the implanted '
                                                                                   'valve or annuloplasty '
                                                                                   'ring.',
                                                     'valve_or_ring_serial_number': 'Manufacturer serial '
                                                                                    'number of the implanted '
                                                                                    'valve or annuloplasty '
                                                                                    'ring, with some entries '
                                                                                    'containing additional '
                                                                                    'free-text device '
                                                                                    'description.',
                                                     'valve_or_ring_size': 'Recorded size of the implanted '
                                                                           'valve or annuloplasty ring as an '
                                                                           'integer, as captured in iWeb.',
                                                     'valve_procedure': 'Type of valve procedure performed '
                                                                        'for this component, such as the '
                                                                        'recorded operative category from '
                                                                        'the iWeb picklist.',
                                                     'wall_quality': 'Recorded quality of the target vessel '
                                                                     'wall for this graft component, as '
                                                                     'captured in iWeb.'},
                                         'table': 'Typed child of clinical_registry_entry for the '
                                                  'surgery_procedure family. One row per graft, valve or '
                                                  'aortic-implant component of a CS2010G surgery. '
                                                  'PARENT_ENTRY_ID joins iweb_cardiac_surgery_episode and is '
                                                  'hard-gated to zero orphans. Known source defect: '
                                                  'IMPLANT_PROSTHESIS_NAMETXT contains some drug-picklist '
                                                  'bleed-through. Every governed bronze column is retained '
                                                  'under its lower-case name.'}}
