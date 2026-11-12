"""Silver spine.person_identifier: 2 dataset(s).

- datasets: spine_person_identifier, _spine_person_identifier_metadata
- moved verbatim from: 1_silver_spine_and_reference:474, 1_silver_spine_and_reference:476, 1_silver_spine_and_reference:478, 1_silver_spine_and_reference:480, 1_silver_spine_and_reference:501, 1_silver_spine_and_reference:506, 1_silver_spine_and_reference:510, 1_silver_spine_and_reference:518

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, read_source, stable_id, subject_key_with_system
from silver_tables.spine._shared import SRC_PATIENT_IDENTIFIER, SRC_PERSON


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Spine / person identifier
SRC_COMMUNITY_LINK = "4_prod.bronze.map_community_patient_link"


SRC_PACS_PATIENT_LINK = "4_prod.bronze.map_pacs_patient_link"


SRC_ENDOBASE_PATIENT = "4_prod.bronze.map_endobase_patient"


SRC_CANCER_PTL_LINKAGE = "4_prod.bronze.map_cancer_ptl_linkage"


PERSON_IDENTIFIER_LIFECYCLE_FIELDS = [
    'identity_status',
    'load_batch_id',
]


PERSON_IDENTIFIER_RETIRED_COLUMNS = [

]


PERSON_IDENTIFIER_LIFECYCLE_COLUMNS = [
    'person_identifier_key',
    'source_identifier_id',
    'identity_status',
    'load_batch_id',
]


# contract v2: one canonical DataFrame feeds the research table and any internal metadata projection.
def _lifecycle_source_person_identifier():
    # Assemble person identifier rows with lifecycle and source evidence for the public product
    # and its internal metadata.
    p = read_source(SRC_PERSON)
    p_skey, p_ssys = subject_key_with_system(
        [("urn:cerner:person_id", p.person_id)],
        SRC_PERSON,
        p.person_id,
    )
    # contract v2: rename the deterministic assignment hash, publish source arm and native mill_alias row id, and type person_id as BIGINT
    p_rows = p.select(
        stable_id("person_identifier:mill:person_id", p.person_id).alias("person_identifier_key"),
        F.lit("mill:person_id").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        p_skey.alias("subject_key"),
        p_ssys.alias("subject_id_system"),
        p.person_id.cast("bigint").alias("person_id"),
        F.lit("urn:cerner:person_id").alias("identifier_system"),
        p.person_id.cast("string").alias("identifier_value"),
        F.lit("PI").alias("identifier_type_code"),
        F.when(p.active_ind == 1, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        p.beg_effective_dt_tm.alias("valid_from"),
        p.end_effective_dt_tm.alias("valid_to"),
        F.lit("millennium").alias("source_system"),
        F.lit(SRC_PERSON).alias("source_table"),
        p.person_id.cast("string").alias("source_row_id"),
        F.lit("resolved").alias("identity_status"),
        F.date_format(p.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        p.ADC_UPDT.alias("loaded_at"),
    )

    c = read_source(SRC_COMMUNITY_LINK)
    c_skey, c_ssys = subject_key_with_system(
        [("urn:cerner:person_id", c.person_id),
         ("urn:barts:community_patient_key", c.community_patient_key)],
        SRC_COMMUNITY_LINK,
        c.community_patient_key,
    )
    common = [
        c_skey.alias("subject_key"),
        c_ssys.alias("subject_id_system"),
        c.person_id.cast("bigint").alias("person_id"),
        c.person_match_status.alias("status"),
        c.community_registration_date.cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit("bh-community").alias("source_system"),
        F.lit(SRC_COMMUNITY_LINK).alias("source_table"),
        c.community_patient_key.alias("source_row_id"),
        F.when(c.person_id.isNotNull(), F.lit("resolved"))
         .when(c.community_patient_key.isNotNull(), F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.lit(None).cast("string").alias("load_batch_id"),
        F.lit(None).cast("timestamp").alias("loaded_at"),
    ]
    c_key_rows = c.select(
        stable_id("person_identifier:community:key", c.community_patient_key).alias("person_identifier_key"),
        F.lit("community:key").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        *common[:3],
        F.lit("urn:barts:community_patient_key").alias("identifier_system"),
        c.community_patient_key.alias("identifier_value"),
        F.lit("COMMUNITY_REGISTRATION").alias("identifier_type_code"),
        *common[3:],
    )
    c_number_rows = c.where(c.source_patient_number.isNotNull()).select(
        stable_id("person_identifier:community:number", c.source_database_id, c.source_patient_number)
          .alias("person_identifier_key"),
        F.lit("community:number").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        *common[:3],
        F.concat(F.lit("urn:barts:community_patient_number:"), c.source_database_id.cast("string"))
          .alias("identifier_system"),
        c.source_patient_number.cast("string").alias("identifier_value"),
        F.lit("MR").alias("identifier_type_code"),
        *common[3:],
    )
    pacs = read_source(SRC_PACS_PATIENT_LINK)
    pacs_skey, pacs_ssys = subject_key_with_system(
        [("urn:cerner:person_id", pacs.PERSON_ID),
         ("urn:sectra:pacs-patient-id", pacs.PACS_PATIENT_ID)],
        SRC_PACS_PATIENT_LINK,
        pacs.PACS_PATIENT_ID,
    )
    pacs_rows = pacs.select(
        stable_id("person_identifier:pacs:patient", pacs.PACS_PATIENT_ID)
        .alias("person_identifier_key"),
        F.lit("pacs:patient").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        pacs_skey.alias("subject_key"), pacs_ssys.alias("subject_id_system"),
        pacs.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("urn:sectra:pacs-patient-id").alias("identifier_system"),
        pacs.PACS_PATIENT_ID.cast("string").alias("identifier_value"),
        F.lit("PACS_INTERNAL").alias("identifier_type_code"),
        pacs.PERSON_MATCH_STATUS.alias("status"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.when(~F.coalesce(pacs.SOURCE_PRESENT_IND, F.lit(True)), pacs.ADC_UPDT)
        .alias("valid_to"),
        F.lit("sectra-pacs").alias("source_system"),
        F.lit(SRC_PACS_PATIENT_LINK).alias("source_table"),
        pacs.PACS_PATIENT_ID.cast("string").alias("source_row_id"),
        F.when(pacs.PERSON_ID.isNotNull(), F.lit("resolved"))
        .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.date_format(pacs.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        pacs.ADC_UPDT.alias("loaded_at"),
    )

    ali = read_source(SRC_PATIENT_IDENTIFIER)
    ali_skey, ali_ssys = subject_key_with_system(
        [("urn:cerner:person_id", ali.PERSON_ID)], SRC_PATIENT_IDENTIFIER, ali.SOURCE_PK
    )
    ali_rows = ali.select(
        stable_id("person_identifier:mill_alias", ali.SOURCE_PK).alias("person_identifier_key"),
        F.lit("mill_alias").alias("source_object"),
        ali.SOURCE_PK.cast("string").alias("source_identifier_id"),
        ali_skey.alias("subject_key"),
        ali_ssys.alias("subject_id_system"),
        ali.PERSON_ID.cast("bigint").alias("person_id"),
        F.when(ali.ALIAS_TYPE == F.lit("NHS"), F.lit("https://fhir.nhs.uk/Id/nhs-number"))
         .otherwise(F.lit("urn:barts:mrn")).alias("identifier_system"),
        ali.ALIAS_VALUE.alias("identifier_value"),
        F.when(ali.ALIAS_TYPE == F.lit("NHS"), F.lit("NH")).otherwise(F.lit("MR")).alias("identifier_type_code"),
        F.when(F.coalesce(ali.CURRENT_IND, F.lit(False)), F.lit("active"))
         .otherwise(F.lit("inactive")).alias("status"),
        ali.BEG_EFFECTIVE_DT_TM.alias("valid_from"),
        F.when(ali.END_EFFECTIVE_DT_TM < F.lit("2100-01-01").cast("timestamp"),
               ali.END_EFFECTIVE_DT_TM).alias("valid_to"),
        F.lit("millennium").alias("source_system"),
        F.lit(SRC_PATIENT_IDENTIFIER).alias("source_table"),
        ali.SOURCE_PK.cast("string").alias("source_row_id"),
        F.lit("resolved").alias("identity_status"),
        F.date_format(ali.PIPELINE_UPDT_DT_TM, "yyyyMMddHHmmss").alias("load_batch_id"),
        ali.PIPELINE_UPDT_DT_TM.alias("loaded_at"),
    )

    e = read_source(SRC_ENDOBASE_PATIENT)
    e_skey, e_ssys = subject_key_with_system(
        [("urn:cerner:person_id", e.PERSON_ID),
         ("urn:barts:endobase:patient-id", e.ENDOBASE_PATIENT_ID)],
        SRC_ENDOBASE_PATIENT,
        e.ENDOBASE_PATIENT_ID,
    )
    e_present = F.coalesce(e.SOURCE_PRESENT_IND, F.lit(True))
    e_rows = e.select(
        stable_id("person_identifier:endobase", e.ENDOBASE_PATIENT_ID)
        .alias("person_identifier_key"),
        F.lit("endobase").alias("source_object"),
        F.lit(None).cast("string").alias("source_identifier_id"),
        e_skey.alias("subject_key"),
        e_ssys.alias("subject_id_system"),
        e.PERSON_ID.cast("bigint").alias("person_id"),
        F.lit("urn:barts:endobase:patient-id").alias("identifier_system"),
        e.ENDOBASE_PATIENT_ID.cast("string").alias("identifier_value"),
        F.lit("ENDOBASE_INTERNAL").alias("identifier_type_code"),
        F.when(e_present, F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        e.SOURCE_CREATE_DT.cast("timestamp").alias("valid_from"),
        F.when(~e_present, e.ADC_UPDT).alias("valid_to"),
        F.lit("endobase").alias("source_system"),
        F.lit(SRC_ENDOBASE_PATIENT).alias("source_table"),
        e.ENDOBASE_PATIENT_ID.cast("string").alias("source_row_id"),
        F.when(e.PERSON_ID.isNotNull(), F.lit("resolved"))
         .when(e.PERSON_LINK_STATUS.isin("CONSENSUS", "MRN_ONLY", "NHS_ONLY"),
               F.lit("provisional"))
         .otherwise(F.lit("unresolved")).alias("identity_status"),
        F.date_format(e.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
        e.ADC_UPDT.alias("loaded_at"),
    )

    lk = read_source(SRC_CANCER_PTL_LINKAGE)
    lk_skey, lk_ssys = subject_key_with_system(
        [("urn:cerner:person_id", lk.PERSON_ID)],
        SRC_CANCER_PTL_LINKAGE,
        lk.ROW_ID.cast("string"),
    )

    def _ptl_identifier(namespace, system, value_col, type_code):
        # Publish nonblank PTL patient identifiers with stable keys and the supplied subject-
        # linkage evidence.
        return lk.where(F.trim(F.coalesce(value_col.cast("string"), F.lit(""))) != "").select(
            stable_id(namespace, lk.ROW_ID).alias("person_identifier_key"),
            F.regexp_extract(F.lit(namespace), r"^person_identifier:([\s\S]*)$", 1).alias("source_object"),
            F.lit(None).cast("string").alias("source_identifier_id"),
            lk_skey.alias("subject_key"),
            lk_ssys.alias("subject_id_system"),
            lk.PERSON_ID.cast("bigint").alias("person_id"),
            F.lit(system).alias("identifier_system"),
            value_col.cast("string").alias("identifier_value"),
            F.lit(type_code).alias("identifier_type_code"),
            F.when(F.coalesce(lk.PERSON_VALID_IND, F.lit(False)), F.lit("MATCHED"))
             .otherwise(F.lit("UNVALIDATED")).alias("status"),
            lk.REFERRAL_DATE_CLEAN.cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit("luna-pathfinder").alias("source_system"),
            F.lit(SRC_CANCER_PTL_LINKAGE).alias("source_table"),
            lk.ROW_ID.cast("string").alias("source_row_id"),
            F.lit("resolved").alias("identity_status"),
            F.date_format(lk.ADC_UPDT, "yyyyMMddHHmmss").alias("load_batch_id"),
            lk.ADC_UPDT.alias("loaded_at"),
        )

    ptl_mrn_rows = _ptl_identifier(
        "person_identifier:luna_ptl:mrn", "urn:barts:mrn", lk.HOSPITAL_NUMBER, "MR"
    )
    ptl_nhs_rows = _ptl_identifier(
        "person_identifier:luna_ptl:nhs",
        "https://fhir.nhs.uk/Id/nhs-number",
        lk.NHS_NUMBER,
        "NH",
    )
    return (
        p_rows.unionByName(c_key_rows).unionByName(c_number_rows)
        .unionByName(pacs_rows).unionByName(ali_rows).unionByName(e_rows)
        .unionByName(ptl_mrn_rows).unionByName(ptl_nhs_rows)
    )


@materialized_view(
    name=_n("journey_spine.person_identifier"),
    refresh_policy="auto",
)
def person_identifier():
    # Build the declared dataset: One source identifier assignment. subject_key is person-keyed
    # where resolution exists and source-keyed otherwise.
    return _lifecycle_source_person_identifier().drop(*PERSON_IDENTIFIER_LIFECYCLE_FIELDS, *PERSON_IDENTIFIER_RETIRED_COLUMNS)


@materialized_view(
    name=_n('journey_spine._person_identifier_metadata'),
    refresh_policy="auto",
)
def person_identifier_lifecycle():
    # Build the declared dataset: Internal quality and batch metadata for
    # spine_person_identifier; same row grain as the research table. Join keys:
    # person_identifier_key, source_identifier_id.
    return (_lifecycle_source_person_identifier()).select(*PERSON_IDENTIFIER_LIFECYCLE_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_spine_person_identifier_metadata': {'columns': {'activity_date': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                   'activity_record_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'admission_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                   'allergy_instance_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'aria_dx_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'aria_pt_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'artifact_link_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'birth_row_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'bloodtrack_transaction_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'cancer_treatment_cycle_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'care_contact_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'care_participation_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                   'cds_id': 'Native source identifier carried unchanged '
                                                             'from the public parent; joins one lifecycle '
                                                             'row to that research row.',
                                                   'chemotherapy_course_id': 'Native source identifier '
                                                                             'carried unchanged from the '
                                                                             'public parent; joins one '
                                                                             'lifecycle row to that research '
                                                                             'row.',
                                                   'community_care_activity_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'community_care_contact_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'concept_map_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'cost_line_item_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'dcp_forms_activity_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                   'device_mapping_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'diagnosis_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'dicom_path': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'doc_response_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'elective_access_procedure_key': 'Deterministic v2 key '
                                                                                    'carried unchanged from '
                                                                                    'the public parent; '
                                                                                    'joins one lifecycle row '
                                                                                    'to that research row.',
                                                   'encounter_attribute_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                   'encounter_bounds_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'encounter_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'encounter_id_resolved': 'True when encounter_id is null '
                                                                            'or resolves to the governed '
                                                                            'encounter spine; false records '
                                                                            'unresolved linkage without '
                                                                            'dropping the row.',
                                                   'encounter_identifier_key': 'Deterministic v2 key carried '
                                                                               'unchanged from the public '
                                                                               'parent; joins one lifecycle '
                                                                               'row to that research row.',
                                                   'encounter_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                   'endobase_exam_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'endobase_exam_term_id': 'Native source identifier '
                                                                            'carried unchanged from the '
                                                                            'public parent; joins one '
                                                                            'lifecycle row to that research '
                                                                            'row.',
                                                   'entity_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'entry_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'episode_encounter_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'episode_encounter_reltn_id': 'Native source identifier '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'episode_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'episode_id_resolved': 'True when episode_id is null or '
                                                                          'resolves to the governed episode '
                                                                          'spine; false records unresolved '
                                                                          'linkage without dropping the row.',
                                                   'episode_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'event_after_death_30d': 'True when the event timestamp '
                                                                            'is more than 30 days after the '
                                                                            "resolved person's death "
                                                                            'timestamp; silver records the '
                                                                            'doubt and does not filter the '
                                                                            'row.',
                                                   'event_before_birth': 'True when the event timestamp '
                                                                         "precedes the resolved person's "
                                                                         'birth timestamp; silver records '
                                                                         'the doubt and does not filter the '
                                                                         'row.',
                                                   'event_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'extract_cd': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'fhx_activity_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'gene_tested_row_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'genetic_result_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'genetic_test_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'homecare_request_item_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                   'identity_status': 'Subject identity resolution state '
                                                                      'recorded by the silver canonical '
                                                                      'query.',
                                                   'implant_event_id': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'implant_sequence': 'Primary-key component carried '
                                                                       'unchanged from the public parent; '
                                                                       "combine it with the companion's "
                                                                       'other key columns when joining.',
                                                   'indication_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                   'is_illogical': 'Source RTT chronology flag indicating an '
                                                                   'illogical activity sequence; retained '
                                                                   'for the gold drop rule.',
                                                   'journey_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'journey_link_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'journey_participant_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                   'labour_delivery_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'load_batch_id': 'Deterministic batch token derived from '
                                                                    "the source row's bronze load timestamp.",
                                                   'loaded_at': 'Latest bronze load timestamp contributing '
                                                                'to the public row; never the lifecycle '
                                                                "pipeline's wall-clock time.",
                                                   'location_code': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                   'location_key': 'Deterministic v2 key carried unchanged '
                                                                   'from the public parent; joins one '
                                                                   'lifecycle row to that research row.',
                                                   'location_stay_key': 'Deterministic v2 key carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'mc_device_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'microbiology_isolate_id': 'Native source identifier '
                                                                              'carried unchanged from the '
                                                                              'public parent; joins one '
                                                                              'lifecycle row to that '
                                                                              'research row.',
                                                   'order_id': 'Native source identifier carried unchanged '
                                                               'from the public parent; joins one lifecycle '
                                                               'row to that research row.',
                                                   'organization_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'organization_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'pacs_examination_id': 'Native source identifier carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'pacs_report_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                   'pathway_oid': 'Native source identifier carried '
                                                                  'unchanged from the public parent; joins '
                                                                  'one lifecycle row to that research row.',
                                                   'patient_event_key': 'Deterministic SHA-256 event key '
                                                                        'carried unchanged from the public '
                                                                        'parent; joins one lifecycle row to '
                                                                        'that event row.',
                                                   'patient_event_row_key': 'Deterministic row-level key '
                                                                            'carried unchanged from '
                                                                            'events_patient_event; joins one '
                                                                            'lifecycle row to one '
                                                                            'event-index row.',
                                                   'period_business_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'period_oid': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'person_address_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'person_attribute_key': 'Deterministic v2 key carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'person_death_evidence_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.',
                                                   'person_id': 'Native source identifier carried unchanged '
                                                                'from the public parent; joins one lifecycle '
                                                                'row to that research row.',
                                                   'person_id_resolved': 'True when person_id is null or '
                                                                         'resolves to the governed person '
                                                                         'spine; false records unresolved '
                                                                         'linkage without dropping the row.',
                                                   'person_identifier_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'person_relationship_key': 'Deterministic v2 key carried '
                                                                              'unchanged from the public '
                                                                              'parent; joins one lifecycle '
                                                                              'row to that research row.',
                                                   'pharmacy_issue_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'pm_wait_list_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'practitioner_group_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                   'practitioner_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'practitioner_identifier_key': 'Deterministic v2 key '
                                                                                  'carried unchanged from '
                                                                                  'the public parent; joins '
                                                                                  'one lifecycle row to that '
                                                                                  'research row.',
                                                   'practitioner_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'practitioner_location_evidence_key': 'Deterministic v2 '
                                                                                         'key carried '
                                                                                         'unchanged from the '
                                                                                         'public parent; '
                                                                                         'joins one '
                                                                                         'lifecycle row to '
                                                                                         'that research row.',
                                                   'pregnancy_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pregnancy_reconciliation_key': 'Deterministic v2 key '
                                                                                   'carried unchanged from '
                                                                                   'the public parent; joins '
                                                                                   'one lifecycle row to '
                                                                                   'that research row.',
                                                   'problem_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'procedure_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'pt_prot_reg_id': 'Native source identifier carried '
                                                                     'unchanged from the public parent; '
                                                                     'joins one lifecycle row to that '
                                                                     'research row.',
                                                   'ptl_unique_id': 'Native source identifier carried '
                                                                    'unchanged from the public parent; joins '
                                                                    'one lifecycle row to that research row.',
                                                   'record_status': 'active, superseded or retracted, '
                                                                    "normalised from the source's own status "
                                                                    'fields; superseded rows are earlier '
                                                                    'versions the source kept.',
                                                   'record_status_effective_from': 'Timestamp from which the '
                                                                                   'normalised record status '
                                                                                   'applies, when the source '
                                                                                   'supplies one.',
                                                   'record_status_effective_to': 'Timestamp until which the '
                                                                                 'normalised record status '
                                                                                 'applies; null means no '
                                                                                 'source end is known.',
                                                   'referral_oid': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'report_version_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'request_thread_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'research_study_key': 'Deterministic v2 key carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'row_source': 'Physical bronze row source, CURRENT or '
                                                                 'HIST, carried unchanged from the public '
                                                                 'parent; a key component because the '
                                                                 'research table is at physical version '
                                                                 'grain and patient_event_key is shared '
                                                                 "across an entry's versions.",
                                                   'rtt_activity_oid': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'sch_event_id': 'Native source identifier carried '
                                                                   'unchanged from the public parent; joins '
                                                                   'one lifecycle row to that research row.',
                                                   'score_type': 'Primary-key component carried unchanged '
                                                                 'from the public parent; combine it with '
                                                                 "the companion's other key columns when "
                                                                 'joining.',
                                                   'sequence_nbr': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                   'service_id': 'Native source identifier carried unchanged '
                                                                 'from the public parent; joins one '
                                                                 'lifecycle row to that research row.',
                                                   'service_key': 'Deterministic v2 key carried unchanged '
                                                                  'from the public parent; joins one '
                                                                  'lifecycle row to that research row.',
                                                   'snapshot_date': 'Primary-key component carried unchanged '
                                                                    'from the public parent; combine it with '
                                                                    "the companion's other key columns when "
                                                                    'joining.',
                                                   'source_daily_id': 'Native source identifier carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'source_identifier_id': 'Native source identifier carried '
                                                                           'unchanged from the public '
                                                                           'parent; joins one lifecycle row '
                                                                           'to that research row.',
                                                   'source_object': 'Source-arm discriminator carried '
                                                                    'unchanged from the public parent; use '
                                                                    'it with arm-specific native identifiers '
                                                                    'when joining.',
                                                   'source_site': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                   'source_system_oid': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'source_unit': 'Primary-key component carried unchanged '
                                                                  'from the public parent; combine it with '
                                                                  "the companion's other key columns when "
                                                                  'joining.',
                                                   'source_update_timestamp': 'Native source-system update '
                                                                              'timestamp carried through the '
                                                                              'canonical silver query.',
                                                   'source_version_id': 'Native source version identifier '
                                                                        'carried unchanged from the public '
                                                                        'parent; CURRENT rows carry -1. A '
                                                                        'key component alongside row_source.',
                                                   'surg_case_proc_id': 'Native source identifier carried '
                                                                        'unchanged from the public parent; '
                                                                        'joins one lifecycle row to that '
                                                                        'research row.',
                                                   'susceptibility_result_id': 'Native source identifier '
                                                                               'carried unchanged from the '
                                                                               'public parent; joins one '
                                                                               'lifecycle row to that '
                                                                               'research row.',
                                                   'theatre_attendance_key': 'Deterministic v2 key carried '
                                                                             'unchanged from the public '
                                                                             'parent; joins one lifecycle '
                                                                             'row to that research row.',
                                                   'theatre_case_key': 'Deterministic v2 key carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'theatre_case_milestone_key': 'Deterministic v2 key '
                                                                                 'carried unchanged from the '
                                                                                 'public parent; joins one '
                                                                                 'lifecycle row to that '
                                                                                 'research row.',
                                                   'theatre_implant_key': 'Deterministic v2 key carried '
                                                                          'unchanged from the public parent; '
                                                                          'joins one lifecycle row to that '
                                                                          'research row.',
                                                   'transfusion_event_key': 'Deterministic v2 key carried '
                                                                            'unchanged from the public '
                                                                            'parent; joins one lifecycle row '
                                                                            'to that research row.',
                                                   'transfusion_key': 'Deterministic v2 key carried '
                                                                      'unchanged from the public parent; '
                                                                      'joins one lifecycle row to that '
                                                                      'research row.',
                                                   'treatment_cycle_id': 'Native source identifier carried '
                                                                         'unchanged from the public parent; '
                                                                         'joins one lifecycle row to that '
                                                                         'research row.',
                                                   'treatment_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                   'update_count': 'Primary-key component carried unchanged '
                                                                   'from the public parent; combine it with '
                                                                   "the companion's other key columns when "
                                                                   'joining.',
                                                   'valid_from_datetime': 'Primary-key component carried '
                                                                          'unchanged from the public parent; '
                                                                          "combine it with the companion's "
                                                                          'other key columns when joining.',
                                                   'value_set_key': 'Deterministic v2 key carried unchanged '
                                                                    'from the public parent; joins one '
                                                                    'lifecycle row to that research row.',
                                                   'waiting_list_oid': 'Native source identifier carried '
                                                                       'unchanged from the public parent; '
                                                                       'joins one lifecycle row to that '
                                                                       'research row.',
                                                   'waiting_list_snapshot_key': 'Deterministic v2 key '
                                                                                'carried unchanged from the '
                                                                                'public parent; joins one '
                                                                                'lifecycle row to that '
                                                                                'research row.'},
                                       'table': 'Internal quality and batch metadata for '
                                                'spine_person_identifier; same row grain as the research '
                                                'table. Join keys: person_identifier_key, '
                                                'source_identifier_id.'},
 'spine_person_identifier': {'columns': {'identifier_system': 'Namespace for the source identifier.',
                                         'identifier_type_code': 'Text label of the alias type derived from '
                                                                 'the alias type code, distinguishing '
                                                                 'medical record number from NHS number '
                                                                 'rows.',
                                         'identifier_value': 'Source identifier value.',
                                         'loaded_at': 'Arm-specific clock: map_person.ADC_UPDT for '
                                                      'Millennium person IDs; always null for both community '
                                                      'key and community number assignments; '
                                                      'map_pacs_patient_link.ADC_UPDT for PACS; '
                                                      'map_patient_identifier.PIPELINE_UPDT_DT_TM for '
                                                      'Millennium aliases; map_endobase_patient.ADC_UPDT for '
                                                      'EndoBase; map_cancer_ptl_linkage.ADC_UPDT for both '
                                                      'Pathfinder MRN and NHS assignments. The eight arms '
                                                      'are unioned without a cross-arm maximum or Silver '
                                                      'refresh timestamp.',
                                         'person_id': 'Resolved Millennium person identifier when available.',
                                         'person_identifier_key': 'B3 bronze field ROW_ID; source value '
                                                                  'retained unless documented as derived.',
                                         'source_identifier_id': 'B3 bronze field ROW_ID; source value '
                                                                 'retained unless documented as derived.',
                                         'source_object': 'Value describing source object for the person '
                                                          'identifier record. It is produced by the silver '
                                                          'transformation and has no direct bronze-column '
                                                          'lineage entry. Whitespace and source sentinel '
                                                          'text are retained unless the pipeline explicitly '
                                                          'normalizes them; null means no value was '
                                                          'supplied.',
                                         'source_row_id': 'Stable source row identifier.',
                                         'source_system': 'Source system name.',
                                         'source_table': 'Fully qualified bronze source table.',
                                         'status': 'Source assignment or matching status.',
                                         'subject_id_system': 'Identifier system used to derive subject_key.',
                                         'subject_key': 'deterministic join key over the strongest available '
                                                        'subject evidence.',
                                         'valid_from': 'Identifier validity start.',
                                         'valid_to': 'Identifier validity end.'},
                             'table': 'One source identifier assignment. subject_key is person-keyed where '
                                      'resolution exists and source-keyed otherwise.'}}

# INSR_R4_SILVER_V1 (module_drop): generated by r4/release/gen_silver_components.py; edit the generator, not this.
from silver_core.parent_inputs import with_columns_last  # noqa: E402
PERSON_IDENTIFIER_LIFECYCLE_FIELDS = [c for c in PERSON_IDENTIFIER_LIFECYCLE_FIELDS if c not in ['identity_status']]  # decision 1: published
SPECS = with_columns_last(SPECS, 'spine_person_identifier', ('identity_status',))  # additions last
