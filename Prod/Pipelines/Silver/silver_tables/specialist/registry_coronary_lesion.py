"""Silver specialist.registry_coronary_lesion: 1 dataset(s).

- datasets: clinical_registry_coronary_lesion
- moved verbatim from: 3_silver_specialist_care:4808, 3_silver_specialist_care:5056

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


# Clinical / registry coronary lesion

# ==== journey_clinical.registry_coronary_lesion ====
REGISTRY_CORONARY_LESION_EXPECTED_SCHEMA = [
    ("ENTRY_ID", "int"),
    ("MRN", "string"),
    ("GENDER", "string"),
    ("DATE_OF_DEATH", "date"),
    ("ETHNIC_ORIGIN", "string"),
    ("PARENT_ENTRY_ID", "int"),
    ("LESION", "string"),
    ("CHRONIC_LESION", "string"),
    ("RESTENOSIS", "string"),
    ("INSTENT_STENOSES", "string"),
    ("DRUGS_ELUTED_BY_STENTSTXT", "string"),
    ("STENOSISPRE_PCI", "string"),
    ("STENOSIS_POST_PCI", "string"),
    ("IN_GRAFT_STENOSIS", "string"),
    ("SUCCESSFUL", "string"),
    ("NUMBER_OF_STENTS_USED", "string"),
    ("LARGEST_BALLON_DIAMETER", "double"),
    ("LARGEST_STENT_DIAMETER", "double"),
    ("LENGTH_OF_SEGMENT_TREATED", "int"),
    ("INDICATION_FOR_STENT", "string"),
    ("STENTS_USEDTXT", "string"),
    ("LMS_PROTECTED", "string"),
    ("NUMBER_OF_DRUG_ELUTING_STENTS_USED", "string"),
    ("DIAGNOSTIC_DEVICES_USED_DURING_PROCEDURETXT", "string"),
    ("MINIMAL_REFERENCE_DIAMETER", "double"),
    ("MINIMAL_LUMEN_DIAMETER", "double"),
    ("DISTAL_VESSEL_DIAMETER", "double"),
    ("PROXIMAL_VESSEL_DIAMETER", "double"),
    ("FRACTIONAL_FLOW_RESERVE", "double"),
    ("CATH_PROCEDURETXT", "string"),
    ("CORONARY_DOMINANCE", "string"),
    ("BRACHYTHERAPY_DEVICES_USEDTXT", "string"),
    ("EMBOLI_PROTECTION_DEVICES_USEDTXT", "string"),
    ("GRAFT_LESION_POSITION", "string"),
    ("ATHERO_THROMBUS_REMOVAL_DEVICES_USEDTXT", "string"),
    ("PROCEDURAL_DEVICES_USEDTXT", "string"),
    ("IS_THERE_A_GRAFT_TO_THIS_VESSEL", "string"),
    ("GRAFT_TYPETXT", "string"),
    ("DISTAL_ANASTOMOSIS", "string"),
    ("GRAFT_CONDUIT", "string"),
    ("PROCEED_TO_PCI", "string"),
    ("BIFURCATION", "string"),
    ("MEDINA_CLASSIFICATION", "string"),
    ("WIRE_USEDTXT", "string"),
    ("PRE_DILATION_BALLOON_DIAMETER", "double"),
    ("PRE_DILATION_BALLOON_LENGTH", "int"),
    ("POST_DILATION_BALLOON_LENGTH", "int"),
    ("POST_DILATION_BALLOON_DILATION", "double"),
    ("N_CPRE_DILATION", "string"),
    ("N_CPOST_DILATION", "string"),
    ("IMPORT_CHILD_ID", "string"),
    ("M_COMMENT_ON_LESION", "string"),
    ("GUIDE_CATHETERTXT", "string"),
    ("CATHETER_SIZE", "string"),
    ("GUIDE_EXTENSION_USED", "string"),
    ("GUIDE_EXTENSIONTXT", "string"),
    ("OTHER_GUIDE_EXTENSION", "string"),
    ("GUIDE_EXTENSION_SIZE", "string"),
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
    name=_n("journey_clinical.registry_coronary_lesion"),
    refresh_policy="auto",
)
def registry_coronary_lesion():
    # Build the declared dataset: Typed child of clinical_registry_entry for the coronary_lesion
    # family. Per-lesion/per-vessel PCI and angiography detail from the iWeb coronary registry.
    # The parent coronary procedure registry is not landed, so PARENT_ENTRY_ID is retained for
    # future linkage but currently has no bronze parent and there is no reliable procedure date.
    # Every governed bronze column is retained under its lower-case name.
    s = read_source(IWEB_REGISTRY_SLOTS["coronary_lesion"])
    actual_schema = [(field.name, field.dataType.simpleString()) for field in s.schema.fields]
    if actual_schema != REGISTRY_CORONARY_LESION_EXPECTED_SCHEMA:
        raise RuntimeError("coronary_lesion: bronze schema drift" + f"; expected={REGISTRY_CORONARY_LESION_EXPECTED_SCHEMA!r} actual={actual_schema!r}")
    return s.select(
        stable_id("registry_entry:iweb", F.lit("coronary_lesion"), F.lit("~"), s["ENTRY_ID"]).alias("patient_event_key"),
        s["ENTRY_ID"].alias("entry_id"),
        s["MRN"].alias("mrn"),
        s["GENDER"].alias("gender"),
        s["DATE_OF_DEATH"].alias("date_of_death"),
        s["ETHNIC_ORIGIN"].alias("ethnic_origin"),
        s["PARENT_ENTRY_ID"].alias("parent_entry_id"),
        s["LESION"].alias("lesion"),
        s["CHRONIC_LESION"].alias("chronic_lesion"),
        s["RESTENOSIS"].alias("restenosis"),
        s["INSTENT_STENOSES"].alias("instent_stenoses"),
        s["DRUGS_ELUTED_BY_STENTSTXT"].alias("drugs_eluted_by_stentstxt"),
        s["STENOSISPRE_PCI"].alias("stenosispre_pci"),
        s["STENOSIS_POST_PCI"].alias("stenosis_post_pci"),
        s["IN_GRAFT_STENOSIS"].alias("in_graft_stenosis"),
        s["SUCCESSFUL"].alias("successful"),
        s["NUMBER_OF_STENTS_USED"].alias("number_of_stents_used"),
        s["LARGEST_BALLON_DIAMETER"].alias("largest_ballon_diameter"),
        s["LARGEST_STENT_DIAMETER"].alias("largest_stent_diameter"),
        s["LENGTH_OF_SEGMENT_TREATED"].alias("length_of_segment_treated"),
        s["INDICATION_FOR_STENT"].alias("indication_for_stent"),
        s["STENTS_USEDTXT"].alias("stents_usedtxt"),
        s["LMS_PROTECTED"].alias("lms_protected"),
        s["NUMBER_OF_DRUG_ELUTING_STENTS_USED"].alias("number_of_drug_eluting_stents_used"),
        s["DIAGNOSTIC_DEVICES_USED_DURING_PROCEDURETXT"].alias("diagnostic_devices_used_during_proceduretxt"),
        s["MINIMAL_REFERENCE_DIAMETER"].alias("minimal_reference_diameter"),
        s["MINIMAL_LUMEN_DIAMETER"].alias("minimal_lumen_diameter"),
        s["DISTAL_VESSEL_DIAMETER"].alias("distal_vessel_diameter"),
        s["PROXIMAL_VESSEL_DIAMETER"].alias("proximal_vessel_diameter"),
        s["FRACTIONAL_FLOW_RESERVE"].alias("fractional_flow_reserve"),
        s["CATH_PROCEDURETXT"].alias("cath_proceduretxt"),
        s["CORONARY_DOMINANCE"].alias("coronary_dominance"),
        s["BRACHYTHERAPY_DEVICES_USEDTXT"].alias("brachytherapy_devices_usedtxt"),
        s["EMBOLI_PROTECTION_DEVICES_USEDTXT"].alias("emboli_protection_devices_usedtxt"),
        s["GRAFT_LESION_POSITION"].alias("graft_lesion_position"),
        s["ATHERO_THROMBUS_REMOVAL_DEVICES_USEDTXT"].alias("athero_thrombus_removal_devices_usedtxt"),
        s["PROCEDURAL_DEVICES_USEDTXT"].alias("procedural_devices_usedtxt"),
        s["IS_THERE_A_GRAFT_TO_THIS_VESSEL"].alias("is_there_a_graft_to_this_vessel"),
        s["GRAFT_TYPETXT"].alias("graft_typetxt"),
        s["DISTAL_ANASTOMOSIS"].alias("distal_anastomosis"),
        s["GRAFT_CONDUIT"].alias("graft_conduit"),
        s["PROCEED_TO_PCI"].alias("proceed_to_pci"),
        s["BIFURCATION"].alias("bifurcation"),
        s["MEDINA_CLASSIFICATION"].alias("medina_classification"),
        s["WIRE_USEDTXT"].alias("wire_usedtxt"),
        s["PRE_DILATION_BALLOON_DIAMETER"].alias("pre_dilation_balloon_diameter"),
        s["PRE_DILATION_BALLOON_LENGTH"].alias("pre_dilation_balloon_length"),
        s["POST_DILATION_BALLOON_LENGTH"].alias("post_dilation_balloon_length"),
        s["POST_DILATION_BALLOON_DILATION"].alias("post_dilation_balloon_dilation"),
        s["N_CPRE_DILATION"].alias("n_cpre_dilation"),
        s["N_CPOST_DILATION"].alias("n_cpost_dilation"),
        s["IMPORT_CHILD_ID"].alias("import_child_id"),
        s["M_COMMENT_ON_LESION"].alias("m_comment_on_lesion"),
        s["GUIDE_CATHETERTXT"].alias("guide_cathetertxt"),
        s["CATHETER_SIZE"].alias("catheter_size"),
        s["GUIDE_EXTENSION_USED"].alias("guide_extension_used"),
        s["GUIDE_EXTENSIONTXT"].alias("guide_extensiontxt"),
        s["OTHER_GUIDE_EXTENSION"].alias("other_guide_extension"),
        s["GUIDE_EXTENSION_SIZE"].alias("guide_extension_size"),
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


COMMENTS = {'clinical_registry_coronary_lesion': {'columns': {'adc_updt': 'Timestamp of the last update applied to the '
                                                               'record in the upstream iWeb extract.',
                                                   'athero_thrombus_removal_devices_usedtxt': 'Coded text '
                                                                                              'listing '
                                                                                              'atherectomy '
                                                                                              'or thrombus '
                                                                                              'removal '
                                                                                              'devices used '
                                                                                              'during the '
                                                                                              'lesion '
                                                                                              'treatment, '
                                                                                              'including a '
                                                                                              'none option.',
                                                   'bifurcation': 'Indicator of whether the treated lesion '
                                                                  'involves a coronary bifurcation.',
                                                   'brachytherapy_devices_usedtxt': 'Coded text listing '
                                                                                    'brachytherapy devices '
                                                                                    'used during the lesion '
                                                                                    'treatment, including a '
                                                                                    'none option.',
                                                   'cath_proceduretxt': 'Coded text description of the '
                                                                        'catheter laboratory procedure '
                                                                        'performed for this lesion or vessel '
                                                                        'record.',
                                                   'catheter_size': 'Recorded size of the guide catheter '
                                                                    'used for the lesion, held as text in '
                                                                    'the source registry.',
                                                   'chronic_lesion': 'Indicator of whether the lesion is a '
                                                                     'chronic total or long-standing '
                                                                     'occlusion.',
                                                   'coronary_dominance': 'Coded classification of coronary '
                                                                         'circulation dominance recorded at '
                                                                         'angiography.',
                                                   'date_last_changed': 'Source edit timestamp; it must not '
                                                                        'be interpreted as the procedure '
                                                                        'date.',
                                                   'date_of_birth': 'Patient date of birth as recorded in '
                                                                    'the iWeb coronary registry and used for '
                                                                    'patient linkage.',
                                                   'date_of_death': 'Recorded date of death for the patient '
                                                                    'as held in the iWeb coronary registry.',
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
                                                                                                  'for this '
                                                                                                  'lesion.',
                                                   'distal_anastomosis': 'Coded description of the distal '
                                                                         'anastomosis site of the graft to '
                                                                         'the coronary vessel.',
                                                   'distal_vessel_diameter': 'Angiographic diameter of the '
                                                                             'vessel distal to the lesion, '
                                                                             'as recorded in the iWeb '
                                                                             'coronary registry.',
                                                   'drugs_eluted_by_stentstxt': 'Coded text description of '
                                                                                'the drug-eluting stent '
                                                                                'product or eluted drug '
                                                                                'recorded for the lesion.',
                                                   'emboli_protection_devices_usedtxt': 'Coded text listing '
                                                                                        'embolic protection '
                                                                                        'devices used during '
                                                                                        'the lesion '
                                                                                        'treatment, '
                                                                                        'including a none '
                                                                                        'option.',
                                                   'entry_id': 'Source registry identifier for the '
                                                               'individual lesion or vessel sub-procedure '
                                                               'record, carried through from the iWeb '
                                                               'coronary sub-procedure table.',
                                                   'ethnic_origin': 'Coded patient ethnic origin as recorded '
                                                                    'in the iWeb coronary registry.',
                                                   'forename': 'Patient forename as recorded in the iWeb '
                                                               'coronary registry and used for patient '
                                                               'linkage.',
                                                   'fractional_flow_reserve': 'Fractional flow reserve '
                                                                              'physiology measurement '
                                                                              'recorded for the lesion or '
                                                                              'vessel.',
                                                   'gender': 'Patient gender as recorded in the iWeb '
                                                             'coronary registry.',
                                                   'graft_conduit': 'Coded description of the conduit used '
                                                                    'for the bypass graft to this vessel.',
                                                   'graft_lesion_position': 'Coded position of the lesion '
                                                                            'within the bypass graft.',
                                                   'graft_typetxt': 'Coded text describing the type of '
                                                                    'bypass graft associated with this '
                                                                    'vessel.',
                                                   'guide_cathetertxt': 'Coded text describing the guide '
                                                                        'catheter used for the lesion, as '
                                                                        'recorded in the iWeb coronary '
                                                                        'registry.',
                                                   'guide_extension_size': 'Recorded size of the guide '
                                                                           'extension catheter used for the '
                                                                           'lesion, held as text in the '
                                                                           'source registry.',
                                                   'guide_extension_used': 'Indicator of whether a guide '
                                                                           'extension catheter was used '
                                                                           'during treatment of the lesion.',
                                                   'guide_extensiontxt': 'Coded text describing the guide '
                                                                         'extension catheter used for the '
                                                                         'lesion, as recorded in the iWeb '
                                                                         'coronary registry.',
                                                   'import_child_id': 'Identifier of the child record '
                                                                      'carried over from a data import into '
                                                                      'the iWeb coronary registry.',
                                                   'in_graft_stenosis': 'Indicator of whether the stenosis '
                                                                        'is located within a bypass graft.',
                                                   'indication_for_stent': 'Recorded clinical indication for '
                                                                           'stent use in this lesion.',
                                                   'instent_stenoses': 'Indicator of whether the stenosis is '
                                                                       'located within a previously '
                                                                       'implanted stent.',
                                                   'is_current_in_source': 'Flag indicating whether the '
                                                                           'record is still present as a '
                                                                           'current row in the source '
                                                                           'system.',
                                                   'is_there_a_graft_to_this_vessel': 'Indicator of whether '
                                                                                      'a bypass graft '
                                                                                      'supplies the vessel '
                                                                                      'being described.',
                                                   'largest_ballon_diameter': 'Largest balloon diameter used '
                                                                              'when treating this lesion, as '
                                                                              'recorded in the registry '
                                                                              "(source spelling of 'ballon' "
                                                                              'retained).',
                                                   'largest_stent_diameter': 'Largest stent diameter '
                                                                             'deployed in this lesion, as '
                                                                             'recorded in the registry.',
                                                   'length_of_segment_treated': 'Recorded length of the '
                                                                                'coronary segment treated '
                                                                                'for this lesion.',
                                                   'lesion': 'Coronary vessel segment or lesion location '
                                                             'treated or assessed in this sub-procedure '
                                                             'record.',
                                                   'linkage_fallback_conflict_ind': 'Flag indicating that '
                                                                                    'fallback linkage '
                                                                                    'produced conflicting '
                                                                                    'person matches for this '
                                                                                    'record.',
                                                   'linkage_historical_fallback_ind': 'Flag indicating that '
                                                                                      'linkage was achieved '
                                                                                      'using a historical '
                                                                                      'rather than current '
                                                                                      'person identifier.',
                                                   'linkage_method': 'Identifier combination used to link '
                                                                     'this registry record to the Millennium '
                                                                     'person record.',
                                                   'linkage_status': 'Outcome status of the patient linkage '
                                                                     'process matching this registry record '
                                                                     'to a Millennium person record.',
                                                   'lms_protected': 'Indicator of whether the left main stem '
                                                                    'lesion was protected by a patent bypass '
                                                                    'graft.',
                                                   'm_comment_on_lesion': 'Free-text clinical comment '
                                                                          'recorded about the lesion or its '
                                                                          'treatment in the iWeb coronary '
                                                                          'registry.',
                                                   'medina_classification': 'Medina classification of the '
                                                                            'bifurcation lesion.',
                                                   'minimal_lumen_diameter': 'Angiographic minimal lumen '
                                                                             'diameter measured at the '
                                                                             'treated lesion, as recorded in '
                                                                             'the iWeb coronary registry.',
                                                   'minimal_reference_diameter': 'Minimal reference vessel '
                                                                                 'diameter measured for this '
                                                                                 'lesion, as recorded in the '
                                                                                 'registry.',
                                                   'mrn': 'Local medical record number of the patient whose '
                                                          'coronary lesion record this is, as held in the '
                                                          'iWeb registry and used for linkage to the master '
                                                          'patient index.',
                                                   'n_cpost_dilation': 'Indicator of whether a non-compliant '
                                                                       'balloon was used for post-dilation '
                                                                       'of the lesion.',
                                                   'n_cpre_dilation': 'Indicator of whether a non-compliant '
                                                                      'balloon was used for pre-dilation of '
                                                                      'the lesion.',
                                                   'nhs_number': 'Patient NHS number as held in the iWeb '
                                                                 'coronary registry, used with the '
                                                                 'validation flag for linkage to the '
                                                                 'Millennium person record.',
                                                   'nhs_number_valid_ind': 'Flag indicating whether the '
                                                                           'source NHS number passed '
                                                                           'validation checks during linkage '
                                                                           'processing.',
                                                   'number_of_drug_eluting_stents_used': 'Number of '
                                                                                         'drug-eluting '
                                                                                         'stents deployed in '
                                                                                         'this lesion, held '
                                                                                         'as text in the '
                                                                                         'source registry.',
                                                   'number_of_stents_used': 'Number of stents deployed in '
                                                                            'this lesion, held as text in '
                                                                            'the source registry.',
                                                   'other_guide_extension': 'Free-text description of a '
                                                                            'guide extension device not '
                                                                            'available in the coded guide '
                                                                            'extension list.',
                                                   'parent_entry_id': 'entry_id of the parent coronary '
                                                                      'procedure in '
                                                                      'clinical_registry_coronary_procedure, '
                                                                      'which carries the procedure date.',
                                                   'patient_event_key': 'Deterministic SHA-256 key matching '
                                                                        'clinical_registry_entry.patient_event_key '
                                                                        'for the coronary_lesion source '
                                                                        'family and native entry_id.',
                                                   'person_id': 'Internal Millennium person identifier '
                                                                'linked to this registry record.',
                                                   'pipeline_loaded_at': 'Timestamp recording when the '
                                                                         'pipeline loaded this record into '
                                                                         'the bronze table.',
                                                   'post_dilation_balloon_dilation': 'Diameter of the '
                                                                                     'balloon used for '
                                                                                     'post-dilation of the '
                                                                                     'lesion, as recorded in '
                                                                                     'the registry.',
                                                   'post_dilation_balloon_length': 'Length of the balloon '
                                                                                   'used for post-dilation '
                                                                                   'of the lesion, as '
                                                                                   'recorded in the '
                                                                                   'registry.',
                                                   'pre_dilation_balloon_diameter': 'Diameter of the balloon '
                                                                                    'used for pre-dilation '
                                                                                    'of the lesion, as '
                                                                                    'recorded in the '
                                                                                    'registry.',
                                                   'pre_dilation_balloon_length': 'Length of the balloon '
                                                                                  'used for pre-dilation of '
                                                                                  'the lesion, as recorded '
                                                                                  'in the registry.',
                                                   'procedural_devices_usedtxt': 'Coded text listing other '
                                                                                 'procedural devices used '
                                                                                 'during the lesion '
                                                                                 'treatment, including a '
                                                                                 'none option.',
                                                   'proceed_to_pci': 'Indicator of whether the lesion '
                                                                     'proceeded to percutaneous coronary '
                                                                     'intervention.',
                                                   'proximal_vessel_diameter': 'Angiographic diameter of the '
                                                                               'vessel proximal to the '
                                                                               'lesion, as recorded in the '
                                                                               'iWeb coronary registry.',
                                                   'restenosis': 'Indicator of whether the lesion represents '
                                                                 'restenosis of a previously treated site.',
                                                   'row_hash': "Hash of the source record's field values "
                                                               'used to detect changes between loads.',
                                                   'source_absent_detected_ts': 'Timestamp recording when '
                                                                                'the pipeline first detected '
                                                                                'that this record was no '
                                                                                'longer present in the '
                                                                                'upstream iWeb source.',
                                                   'source_present_ind': 'Canonical boolean mirror of '
                                                                         'IS_CURRENT_IN_SOURCE; false rows '
                                                                         'are retained source tombstones.',
                                                   'source_record_key': 'Business key identifying the source '
                                                                        'registry record, derived from the '
                                                                        'iWeb sub-procedure entry '
                                                                        'identifier.',
                                                   'source_table': 'Name of the upstream source table from '
                                                                   'which the record was ingested.',
                                                   'stenosis_post_pci': 'Banded percentage residual stenosis '
                                                                        'of the lesion assessed after '
                                                                        'percutaneous coronary intervention.',
                                                   'stenosispre_pci': 'Banded percentage stenosis of the '
                                                                      'lesion assessed before percutaneous '
                                                                      'coronary intervention.',
                                                   'stents_usedtxt': 'Coded text description of the stent '
                                                                     'products used in this lesion.',
                                                   'successful': 'Indicator of whether the intervention on '
                                                                 'this lesion was recorded as successful.',
                                                   'surname': 'Patient surname as recorded in the iWeb '
                                                              'coronary registry and used for patient '
                                                              'linkage.',
                                                   'wire_usedtxt': 'Coded text describing the guidewire used '
                                                                   'for the lesion.'},
                                       'table': 'Typed child of clinical_registry_entry for the '
                                                'coronary_lesion family: per-lesion and per-vessel PCI and '
                                                'angiography detail from the iWeb coronary registry. Each '
                                                'lesion belongs to one coronary procedure '
                                                '(parent_entry_id).'}}
