"""Shared Gold transformations. Dataset declarations and their rules live in the numbered notebooks.

Mandatory expectations drop rows; advisory expectations only report. Value repairs,
mapping-target withholding and anonymised-text substitution remain transformations.
This is a Python FILE, not a notebook. It registers no datasets when imported.
"""

import json
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import pipelines as dp

spark = SparkSession.builder.getOrCreate()

SILVER_CATALOG_SCHEMA = spark.conf.get("journey.silver_catalog_schema")
PUBLIC_SCHEMA = spark.conf.get("journey.public_schema")
# Underscore-prefixed datasets are implementation objects, not research products, and live
# in the internal schema exactly as they do in the silver pipeline (dev gold_v2_internal;
# 4_prod.tmp at the Session 4 cutover). The 94 quality-controlled twins moved here on
# 2026-09-09 when they became materialized views: a twin held as a temporary view carries
# no row tracking, so Enzyme could only rebuild every published Gold MV from scratch on
# each update (decision_register: gold_qc_twin_incremental_refresh; proof in
# 8_dev.s2_evidence.gold_incremental_probe).
INTERNAL_SCHEMA = spark.conf.get("journey.internal_schema")

def _n(name):
    """Flatten a logical Gold plane into a configured two-part dataset name."""
    logical_schema, table = name.split(".", 1)
    if not logical_schema.startswith("gold_"):
        raise ValueError(f"unexpected flow schema in {name!r}")
    plane = logical_schema[len("gold_") :]
    if table.startswith("_"):
        return INTERNAL_SCHEMA + "._" + plane + table
    return PUBLIC_SCHEMA + "." + plane + "_" + table

def _src(flat):
    """The configured Silver table a Gold product reads."""
    return SILVER_CATALOG_SCHEMA + "." + flat

def _assert_not_boilerplate(text, where):
    """INSR_W8_COMMENTS_V1 (W8.04): a comment set by gold may never be a generator template (F17)."""
    from gold_tdx_comments import assert_not_boilerplate
    assert_not_boilerplate(text, where)


def _with_comments(df, comments):
    """Attach each column's comment to the schema the flow returns."""
    shared_quantity_columns = {
        "dose_value", "dose_unit", "volume_value", "volume_unit", "rate_value", "rate_unit",
        "duration_value", "duration_unit", "quantity_value", "quantity_unit", "cost_value", "cost_unit",
    }
    for column_name in df.columns:
        comment = comments.get(column_name)
        if column_name in shared_quantity_columns and "medication_map_component_count" in df.columns:
            comment = (
                "Carried only on component 1 of a split compound medication statement; "
                "aggregate over medication_map_component_index = 1 or count distinct "
                "source_patient_event_key."
            )
        if comment:
            _assert_not_boilerplate(comment, column_name)  # INSR_W8_COMMENTS_V1
            df = df.withMetadata(column_name, {"comment": comment})
    return df


GOLD_V2_LIFECYCLE = {'clinical_allergy_intolerance': {'join_keys': ['patient_event_key'],
                                  'support_columns': ['identity_status',
                                                      'record_status',
                                                      'record_status_effective_from',
                                                      'record_status_effective_to',
                                                      'person_id_resolved',
                                                      'encounter_id_resolved',
                                                      'load_batch_id',
                                                      'source_update_timestamp',
                                                      'loaded_at']},
 'clinical_appointment': {'join_keys': ['patient_event_key'],
                          'support_columns': ['identity_status',
                                              'record_status',
                                              'record_status_effective_from',
                                              'record_status_effective_to',
                                              'person_id_resolved',
                                              'encounter_id_resolved',
                                              'load_batch_id',
                                              'source_update_timestamp',
                                              'loaded_at']},
 'clinical_baby_delivery': {'join_keys': ['patient_event_key'],
                            'support_columns': ['identity_status',
                                                'record_status',
                                                'record_status_effective_from',
                                                'record_status_effective_to',
                                                'load_batch_id',
                                                'source_update_timestamp',
                                                'loaded_at']},
 'clinical_cancer_treatment': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_cancer_treatment_cycle': {'join_keys': ['cancer_treatment_cycle_key'],
                                     'support_columns': ['record_status', 'load_batch_id', 'loaded_at']},
 'clinical_clinical_finding': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'person_id_resolved',
                                                   'encounter_id_resolved',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_clinical_score': {'join_keys': ['patient_event_key'],
                             'support_columns': ['identity_status',
                                                 'record_status',
                                                 'record_status_effective_from',
                                                 'record_status_effective_to',
                                                 'person_id_resolved',
                                                 'encounter_id_resolved',
                                                 'load_batch_id',
                                                 'source_update_timestamp',
                                                 'loaded_at']},
 'clinical_community_care_activity': {'join_keys': ['patient_event_key'],
                                      'support_columns': ['identity_status',
                                                          'record_status',
                                                          'record_status_effective_from',
                                                          'record_status_effective_to',
                                                          'load_batch_id',
                                                          'source_update_timestamp',
                                                          'loaded_at']},
 'clinical_community_care_contact': {'join_keys': ['patient_event_key'],
                                     'support_columns': ['identity_status',
                                                         'record_status',
                                                         'record_status_effective_from',
                                                         'record_status_effective_to',
                                                         'load_batch_id',
                                                         'source_update_timestamp',
                                                         'loaded_at']},
 'clinical_condition': {'join_keys': ['patient_event_key'],
                        'support_columns': ['identity_status',
                                            'record_status',
                                            'record_status_effective_from',
                                            'record_status_effective_to',
                                            'person_id_resolved',
                                            'encounter_id_resolved',
                                            'load_batch_id',
                                            'source_update_timestamp',
                                            'loaded_at']},
 'clinical_condition_stage': {'join_keys': ['patient_event_key'],
                              'support_columns': ['identity_status',
                                                  'record_status',
                                                  'record_status_effective_from',
                                                  'record_status_effective_to',
                                                  'load_batch_id',
                                                  'source_update_timestamp',
                                                  'loaded_at']},
 'clinical_costed_activity': {'join_keys': ['patient_event_key'],
                              'support_columns': ['identity_status',
                                                  'record_status',
                                                  'record_status_effective_from',
                                                  'record_status_effective_to',
                                                  'load_batch_id',
                                                  'source_update_timestamp',
                                                  'loaded_at']},
 'clinical_critical_care_activity': {'join_keys': ['patient_event_key'],
                                     'support_columns': ['identity_status',
                                                         'record_status',
                                                         'record_status_effective_from',
                                                         'record_status_effective_to',
                                                         'load_batch_id',
                                                         'source_update_timestamp',
                                                         'loaded_at']},
 'clinical_critical_care_admission': {'join_keys': ['patient_event_key'],
                                      'support_columns': ['identity_status',
                                                          'record_status',
                                                          'record_status_effective_from',
                                                          'record_status_effective_to',
                                                          'load_batch_id',
                                                          'source_update_timestamp',
                                                          'loaded_at']},
 'clinical_critical_care_daily_score': {'join_keys': ['patient_event_key'],
                                        'support_columns': ['identity_status',
                                                            'record_status',
                                                            'record_status_effective_from',
                                                            'record_status_effective_to',
                                                            'load_batch_id',
                                                            'source_update_timestamp',
                                                            'loaded_at']},
 'clinical_critical_care_period': {'join_keys': ['patient_event_key'],
                                   'support_columns': ['identity_status',
                                                       'record_status',
                                                       'record_status_effective_from',
                                                       'record_status_effective_to',
                                                       'load_batch_id',
                                                       'source_update_timestamp',
                                                       'loaded_at']},
 'clinical_device': {'join_keys': ['patient_event_key'],
                     'support_columns': ['identity_status',
                                         'record_status',
                                         'record_status_effective_from',
                                         'record_status_effective_to',
                                         'load_batch_id',
                                         'source_update_timestamp',
                                         'loaded_at']},
 'clinical_drug_expenditure': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_elective_access_entry': {'join_keys': ['patient_event_key'],
                                    'support_columns': ['identity_status',
                                                        'record_status',
                                                        'record_status_effective_from',
                                                        'record_status_effective_to',
                                                        'load_batch_id',
                                                        'source_update_timestamp',
                                                        'loaded_at']},
 'clinical_endoscopy_finding': {'join_keys': ['patient_event_key'],
                                'support_columns': ['identity_status',
                                                    'record_status',
                                                    'record_status_effective_from',
                                                    'record_status_effective_to',
                                                    'load_batch_id',
                                                    'source_update_timestamp',
                                                    'loaded_at']},
 'clinical_family_history': {'join_keys': ['patient_event_key'],
                             'support_columns': ['identity_status',
                                                 'record_status',
                                                 'record_status_effective_from',
                                                 'record_status_effective_to',
                                                 'load_batch_id',
                                                 'source_update_timestamp',
                                                 'loaded_at']},
 'clinical_form': {'join_keys': ['patient_event_key'],
                   'support_columns': ['identity_status',
                                       'record_status',
                                       'record_status_effective_from',
                                       'record_status_effective_to',
                                       'person_id_resolved',
                                       'encounter_id_resolved',
                                       'load_batch_id',
                                       'source_update_timestamp',
                                       'loaded_at']},
 'clinical_genomic_result': {'join_keys': ['patient_event_key'],
                             'support_columns': ['identity_status',
                                                 'record_status',
                                                 'record_status_effective_from',
                                                 'record_status_effective_to',
                                                 'load_batch_id',
                                                 'source_update_timestamp',
                                                 'loaded_at']},
 'clinical_genomic_test': {'join_keys': ['patient_event_key'],
                           'support_columns': ['identity_status',
                                               'record_status',
                                               'record_status_effective_from',
                                               'record_status_effective_to',
                                               'load_batch_id',
                                               'source_update_timestamp',
                                               'loaded_at']},
 'clinical_hrg_grouping': {'join_keys': ['patient_event_key'],
                           'support_columns': ['identity_status',
                                               'record_status',
                                               'record_status_effective_from',
                                               'record_status_effective_to',
                                               'load_batch_id',
                                               'source_update_timestamp',
                                               'loaded_at']},
 'clinical_imaging_exam': {'join_keys': ['patient_event_key'],
                           'support_columns': ['identity_status',
                                               'record_status',
                                               'record_status_effective_from',
                                               'record_status_effective_to',
                                               'person_id_resolved',
                                               'encounter_id_resolved',
                                               'load_batch_id',
                                               'source_update_timestamp',
                                               'loaded_at']},
 'clinical_indication': {'join_keys': ['patient_event_key'],
                         'support_columns': ['identity_status',
                                             'record_status',
                                             'record_status_effective_from',
                                             'record_status_effective_to',
                                             'load_batch_id',
                                             'source_update_timestamp',
                                             'loaded_at']},
 'clinical_labour_delivery': {'join_keys': ['patient_event_key'],
                              'support_columns': ['identity_status',
                                                  'record_status',
                                                  'record_status_effective_from',
                                                  'record_status_effective_to',
                                                  'load_batch_id',
                                                  'source_update_timestamp',
                                                  'loaded_at']},
 'clinical_maternity_care_contact': {'join_keys': ['patient_event_key'],
                                     'support_columns': ['identity_status',
                                                         'record_status',
                                                         'record_status_effective_from',
                                                         'record_status_effective_to',
                                                         'load_batch_id',
                                                         'source_update_timestamp',
                                                         'loaded_at']},
 'clinical_medication_admin': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'person_id_resolved',
                                                   'encounter_id_resolved',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_medication_dispense': {'join_keys': ['patient_event_key'],
                                  'support_columns': ['identity_status',
                                                      'record_status',
                                                      'record_status_effective_from',
                                                      'record_status_effective_to',
                                                      'load_batch_id',
                                                      'source_update_timestamp',
                                                      'loaded_at']},
 'clinical_medication_order': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'person_id_resolved',
                                                   'encounter_id_resolved',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_medication_supply': {'join_keys': ['patient_event_key'],
                                'support_columns': ['identity_status',
                                                    'record_status',
                                                    'record_status_effective_from',
                                                    'record_status_effective_to',
                                                    'load_batch_id',
                                                    'source_update_timestamp',
                                                    'loaded_at']},
 'clinical_microbiology_isolate': {'join_keys': ['patient_event_key'],
                                   'support_columns': ['identity_status',
                                                       'record_status',
                                                       'record_status_effective_from',
                                                       'record_status_effective_to',
                                                       'load_batch_id',
                                                       'source_update_timestamp',
                                                       'loaded_at']},
 'clinical_neonatal_care_day': {'join_keys': ['patient_event_key'],
                                'support_columns': ['identity_status',
                                                    'record_status',
                                                    'record_status_effective_from',
                                                    'record_status_effective_to',
                                                    'load_batch_id',
                                                    'source_update_timestamp',
                                                    'loaded_at']},
 'clinical_neonatal_episode': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_neonatal_examination': {'join_keys': ['patient_event_key'],
                                   'support_columns': ['identity_status',
                                                       'record_status',
                                                       'record_status_effective_from',
                                                       'record_status_effective_to',
                                                       'load_batch_id',
                                                       'source_update_timestamp',
                                                       'loaded_at']},
 'clinical_pathology_order': {'join_keys': ['patient_event_key'],
                              'support_columns': ['identity_status',
                                                  'record_status',
                                                  'record_status_effective_from',
                                                  'record_status_effective_to',
                                                  'load_batch_id',
                                                  'source_update_timestamp',
                                                  'loaded_at']},
 'clinical_pathology_report': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'person_id_resolved',
                                                   'encounter_id_resolved',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_pathology_result': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'person_id_resolved',
                                                   'encounter_id_resolved',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_pathway_tracking': {'join_keys': ['patient_event_key'],
                               'support_columns': ['identity_status',
                                                   'record_status',
                                                   'record_status_effective_from',
                                                   'record_status_effective_to',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'clinical_procedure': {'join_keys': ['patient_event_key'],
                        'support_columns': ['identity_status',
                                            'record_status',
                                            'record_status_effective_from',
                                            'record_status_effective_to',
                                            'person_id_resolved',
                                            'encounter_id_resolved',
                                            'load_batch_id',
                                            'source_update_timestamp',
                                            'loaded_at']},
 'clinical_referral': {'join_keys': ['patient_event_key'],
                       'support_columns': ['identity_status',
                                           'record_status',
                                           'record_status_effective_from',
                                           'record_status_effective_to',
                                           'load_batch_id',
                                           'source_update_timestamp',
                                           'loaded_at']},
 'clinical_registry_entry': {'join_keys': ['patient_event_key'],
                             'support_columns': ['identity_status',
                                                 'record_status',
                                                 'record_status_effective_from',
                                                 'record_status_effective_to',
                                                 'load_batch_id',
                                                 'source_update_timestamp',
                                                 'loaded_at']},
 'clinical_research_enrollment': {'join_keys': ['patient_event_key'],
                                  'support_columns': ['identity_status',
                                                      'record_status',
                                                      'record_status_effective_from',
                                                      'record_status_effective_to',
                                                      'load_batch_id',
                                                      'source_update_timestamp',
                                                      'loaded_at']},
 'clinical_rtt_activity': {'join_keys': ['patient_event_key'],
                           'support_columns': ['identity_status',
                                               'record_status',
                                               'record_status_effective_from',
                                               'record_status_effective_to',
                                               'is_illogical',
                                               'load_batch_id',
                                               'source_update_timestamp',
                                               'loaded_at']},
 'clinical_rtt_pathway': {'join_keys': ['patient_event_key'],
                          'support_columns': ['identity_status',
                                              'record_status',
                                              'record_status_effective_from',
                                              'record_status_effective_to',
                                              'person_id_resolved',
                                              'load_batch_id',
                                              'source_update_timestamp',
                                              'loaded_at']},
 'clinical_specimen': {'join_keys': ['patient_event_key'],
                       'support_columns': ['identity_status',
                                           'record_status',
                                           'record_status_effective_from',
                                           'record_status_effective_to',
                                           'person_id_resolved',
                                           'load_batch_id',
                                           'source_update_timestamp',
                                           'loaded_at']},
 'clinical_susceptibility_result': {'join_keys': ['patient_event_key'],
                                    'support_columns': ['identity_status',
                                                        'record_status',
                                                        'record_status_effective_from',
                                                        'record_status_effective_to',
                                                        'load_batch_id',
                                                        'source_update_timestamp',
                                                        'loaded_at']},
 'clinical_transfusion': {'join_keys': ['patient_event_key'],
                          'support_columns': ['identity_status',
                                              'record_status',
                                              'record_status_effective_from',
                                              'record_status_effective_to',
                                              'load_batch_id',
                                              'source_update_timestamp',
                                              'loaded_at']},
 'clinical_transfusion_event': {'join_keys': ['transfusion_event_key'],
                                'support_columns': ['identity_status',
                                                    'record_status',
                                                    'load_batch_id',
                                                    'loaded_at']},
 'clinical_vital_sign': {'join_keys': ['patient_event_key'],
                         'support_columns': ['identity_status',
                                             'record_status',
                                             'record_status_effective_from',
                                             'record_status_effective_to',
                                             'person_id_resolved',
                                             'encounter_id_resolved',
                                             'load_batch_id',
                                             'source_update_timestamp',
                                             'loaded_at']},
 'clinical_waiting_list_entry': {'join_keys': ['patient_event_key', 'row_source', 'source_version_id'],
                                 'support_columns': ['identity_status',
                                                     'record_status',
                                                     'record_status_effective_from',
                                                     'record_status_effective_to',
                                                     'load_batch_id',
                                                     'source_update_timestamp',
                                                     'loaded_at']},
 'clinical_waiting_list_snapshot': {'join_keys': ['waiting_list_snapshot_key'],
                                    'support_columns': ['identity_status', 'loaded_at']},
 'reference_concept_map': {'join_keys': ['concept_map_key'], 'support_columns': ['loaded_at']},
 'reference_cost_line_item': {'join_keys': ['cost_line_item_key'],
                              'support_columns': ['record_status', 'loaded_at']},
 'reference_device_mapping': {'join_keys': ['device_mapping_key'], 'support_columns': ['loaded_at']},
 'reference_elective_access_procedure': {'join_keys': ['elective_access_procedure_key'],
                                         'support_columns': ['record_status', 'loaded_at']},
 'reference_encounter_attribute': {'join_keys': ['encounter_attribute_key'],
                                   'support_columns': ['loaded_at']},
 'reference_encounter_bounds': {'join_keys': ['encounter_bounds_key'], 'support_columns': ['loaded_at']},
 'reference_gene_tested': {'join_keys': ['gene_tested_row_key'], 'support_columns': ['loaded_at']},
 'reference_location': {'join_keys': ['location_key'], 'support_columns': ['load_batch_id', 'loaded_at']},
 'reference_organization': {'join_keys': ['organization_key'],
                            'support_columns': ['load_batch_id', 'loaded_at']},
 'reference_person_address': {'join_keys': ['person_address_key'], 'support_columns': ['loaded_at']},
 'reference_person_attribute': {'join_keys': ['person_attribute_key'], 'support_columns': ['loaded_at']},
 'reference_person_death_evidence': {'join_keys': ['person_death_evidence_key'],
                                     'support_columns': ['record_status', 'loaded_at']},
 'reference_practitioner': {'join_keys': ['practitioner_key'],
                            'support_columns': ['record_status', 'load_batch_id', 'loaded_at']},
 'reference_practitioner_group': {'join_keys': ['practitioner_group_key'],
                                  'support_columns': ['record_status', 'loaded_at']},
 'reference_practitioner_identifier': {'join_keys': ['practitioner_identifier_key'],
                                       'support_columns': ['record_status', 'loaded_at']},
 'reference_practitioner_location_evidence': {'join_keys': ['practitioner_location_evidence_key'],
                                              'support_columns': ['loaded_at']},
 'reference_pregnancy_reconciliation': {'join_keys': ['pregnancy_reconciliation_key'],
                                        'support_columns': ['record_status',
                                                            'source_update_timestamp',
                                                            'loaded_at']},
 'reference_research_study': {'join_keys': ['research_study_key'],
                              'support_columns': ['record_status', 'loaded_at']},
 'reference_service': {'join_keys': ['service_key'], 'support_columns': ['load_batch_id', 'loaded_at']},
 'reference_theatre_attendance': {'join_keys': ['theatre_attendance_key'], 'support_columns': ['loaded_at']},
 'reference_theatre_case': {'join_keys': ['theatre_case_key'], 'support_columns': ['loaded_at']},  # PMS_THEATRE_CASE_V1
 'reference_theatre_case_milestone': {'join_keys': ['theatre_case_milestone_key'],
                                      'support_columns': ['loaded_at']},
 'reference_theatre_implant': {'join_keys': ['theatre_implant_key'], 'support_columns': ['loaded_at']},
 'reference_value_set': {'join_keys': ['value_set_key'], 'support_columns': ['load_batch_id', 'loaded_at']},
 'spine_care_participation': {'join_keys': ['care_participation_key'],
                              'support_columns': ['load_batch_id', 'loaded_at']},
 'spine_encounter': {'join_keys': ['encounter_key'],
                     'support_columns': ['record_status',
                                         'record_status_effective_from',
                                         'record_status_effective_to',
                                         'load_batch_id',
                                         'source_update_timestamp',
                                         'loaded_at']},
 'spine_encounter_identifier': {'join_keys': ['encounter_identifier_key'],
                                'support_columns': ['load_batch_id', 'loaded_at']},
 'spine_episode': {'join_keys': ['episode_key'],
                   'support_columns': ['record_status',
                                       'record_status_effective_from',
                                       'record_status_effective_to',
                                       'load_batch_id',
                                       'loaded_at']},
 'spine_episode_encounter': {'join_keys': ['episode_encounter_key'],
                             'support_columns': ['record_status', 'load_batch_id', 'loaded_at']},
 'spine_journey': {'join_keys': ['journey_key'],
                   'support_columns': ['load_batch_id', 'source_update_timestamp', 'loaded_at']},
 'spine_journey_link': {'join_keys': ['journey_link_key'],
                        'support_columns': ['record_status', 'load_batch_id', 'loaded_at']},
 'spine_journey_participant': {'join_keys': ['journey_participant_key'],
                               'support_columns': ['record_status',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'spine_location_stay': {'join_keys': ['location_stay_key'],
                         'support_columns': ['record_status',
                                             'record_status_effective_to',
                                             'load_batch_id',
                                             'loaded_at']},
 'spine_person': {'join_keys': ['person_id'],
                  'support_columns': ['record_status',
                                      'record_status_effective_from',
                                      'record_status_effective_to',
                                      'load_batch_id',
                                      'source_update_timestamp',
                                      'loaded_at']},
 'spine_person_identifier': {'join_keys': ['person_identifier_key'],
                             'support_columns': ['identity_status', 'load_batch_id', 'loaded_at']},
 'spine_person_relationship': {'join_keys': ['person_relationship_key'],
                               'support_columns': ['record_status',
                                                   'load_batch_id',
                                                   'source_update_timestamp',
                                                   'loaded_at']},
 'spine_request_thread': {'join_keys': ['request_thread_key'],
                          'support_columns': ['record_status',
                                              'record_status_effective_from',
                                              'record_status_effective_to',
                                              'load_batch_id',
                                              'source_update_timestamp',
                                              'loaded_at']},
 'text_document': {'join_keys': ['patient_event_key'],
                   'support_columns': ['identity_status',
                                       'record_status',
                                       'record_status_effective_from',
                                       'record_status_effective_to',
                                       'person_id_resolved',
                                       'encounter_id_resolved',
                                       'load_batch_id',
                                       'source_update_timestamp',
                                       'loaded_at']}}
GOLD_V2_DATE_FLAGS = {'clinical_allergy_intolerance': ['event_after_death_30d', 'event_before_birth'],
 'clinical_appointment': ['event_after_death_30d', 'event_before_birth'],
 'clinical_cancer_treatment': ['event_after_death_30d', 'event_before_birth'],
 'clinical_clinical_finding': ['event_after_death_30d', 'event_before_birth'],
 'clinical_clinical_score': ['event_after_death_30d', 'event_before_birth'],
 'clinical_community_care_activity': ['event_after_death_30d', 'event_before_birth'],
 'clinical_community_care_contact': ['event_after_death_30d', 'event_before_birth'],
 'clinical_condition': ['event_after_death_30d', 'event_before_birth'],
 'clinical_condition_stage': ['event_after_death_30d', 'event_before_birth'],
 'clinical_costed_activity': ['event_after_death_30d', 'event_before_birth'],
 'clinical_critical_care_activity': ['event_before_birth'],
 'clinical_critical_care_admission': ['event_after_death_30d', 'event_before_birth'],
 'clinical_critical_care_daily_score': ['event_after_death_30d'],
 'clinical_critical_care_period': ['event_after_death_30d', 'event_before_birth'],
 'clinical_device': ['event_after_death_30d', 'event_before_birth'],
 'clinical_drug_expenditure': ['event_after_death_30d', 'event_before_birth'],
 'clinical_elective_access_entry': ['event_after_death_30d', 'event_before_birth'],
 'clinical_family_history': ['event_after_death_30d'],
 'clinical_form': ['event_after_death_30d', 'event_before_birth'],
 'clinical_genomic_test': ['event_after_death_30d'],
 'clinical_hrg_grouping': ['event_after_death_30d', 'event_before_birth'],
 'clinical_imaging_exam': ['event_after_death_30d', 'event_before_birth'],
 'clinical_indication': ['event_after_death_30d', 'event_before_birth'],
 'clinical_labour_delivery': ['event_after_death_30d'],
 'clinical_maternity_care_contact': ['event_after_death_30d', 'event_before_birth'],
 'clinical_medication_admin': ['event_after_death_30d', 'event_before_birth'],
 'clinical_medication_dispense': ['event_before_birth'],
 'clinical_medication_order': ['event_after_death_30d', 'event_before_birth'],
 'clinical_medication_supply': ['event_after_death_30d'],
 'clinical_neonatal_care_day': ['event_after_death_30d', 'event_before_birth'],
 'clinical_neonatal_episode': ['event_before_birth'],
 'clinical_neonatal_examination': ['event_after_death_30d', 'event_before_birth'],
 'clinical_pathology_order': ['event_after_death_30d', 'event_before_birth'],
 'clinical_pathology_report': ['event_after_death_30d', 'event_before_birth'],
 'clinical_pathology_result': ['event_after_death_30d', 'event_before_birth'],
 'clinical_pathway_tracking': ['event_after_death_30d', 'event_before_birth'],
 'clinical_procedure': ['event_after_death_30d', 'event_before_birth'],
 'clinical_referral': ['event_after_death_30d', 'event_before_birth'],
 'clinical_registry_entry': ['event_after_death_30d', 'event_before_birth'],
 'clinical_research_enrollment': ['event_after_death_30d', 'event_before_birth'],
 'clinical_rtt_activity': ['event_after_death_30d', 'event_before_birth'],
 'clinical_rtt_pathway': ['event_after_death_30d', 'event_before_birth'],
 'clinical_specimen': ['event_after_death_30d', 'event_before_birth'],
 'clinical_transfusion': ['event_after_death_30d'],
 'clinical_transfusion_event': ['event_after_death_30d'],
 'clinical_vital_sign': ['event_after_death_30d', 'event_before_birth'],
 'clinical_waiting_list_entry': ['event_after_death_30d', 'event_before_birth'],
 'text_document': ['event_after_death_30d', 'event_before_birth']}
GOLD_V2_SOURCE_SUPPORT = {'text_document': ['is_latest_version']}

# DQ4_G_GOLD_RULES_V1: rule dq4_g2_sex_plausibility_v1. A concept that only one sex can carry (an
# obstetric diagnosis, a prostatectomy) recorded on a person of the other recorded sex is
# flagged, never removed: either the code or the person's recorded sex may be the wrong side.
# The concept list is the DQ2 plausibility list promoted to 3_lookup (287 concepts, v1).
GOLD_SEX_PLAUSIBILITY_COLUMNS = {
    "clinical_condition": "condition_omop_concept_id",
    "clinical_procedure": "procedure_omop_concept_id",
}
GOLD_SEX_PLAUSIBILITY_TABLE = "3_lookup.omop.concept_sex_plausibility"

# S3B_GOLD_PATCH_APPLIED v1
# S3C_GOLD_PATCH_APPLIED v1
# INSR R1 (mapping into bronze): the admission policy is data (3_lookup.omop.mapping_admission_policy), read once at
# graph build; one gate module serves gold and omop_gold (design rev 3 decision 7; R1 plan deviation D6). In
# omop_gold a policy change re-plans the gated MVs once, as a journey.s3b_policies change did. After R4 no gold dataset
# calls this literal form: views join the policy table and materialised tables join the per-key _mapping_admission MV
# (Ben's 2026-11-01 ruling), so for gold a policy change is a data change.
import sys as _r1_sys
_r1_sys.path.append(spark.conf.get("journey.r1_shared_path", "/Workspace/Shared/ADC-DB/Prod/Pipelines/_shared"))
import mapping_admission_gate as R1G  # noqa: E402
R1_POLICY = R1G.load_policy(spark, spark.conf.get("journey.r1_policy_table",
                                                  "`3_lookup`.omop.mapping_admission_policy"))
GOLD_V2_POLICIES = R1_POLICY.banded
GOVERNED_TRANSLATION_RULES = R1_POLICY.translation_prefixes
GOVERNED_ROLLUP_RULES = R1_POLICY.rollup_prefixes

# GOLD_COMPACT_GATE_PATCH_APPLIED v1
def _s3_axis_gate_and_policy(item):
    """Decide whether a mapped target meets the configured policy and return its provenance: the shared gate
    (mapping_admission_gate.gate_and_policy, AST-equal to the previous body) over the table-held policy."""
    return R1G.gate_and_policy(item, R1_POLICY)

def _s3_axis_gate(item):
    """Return only the admission decision for one terminology mapping."""
    return _s3_axis_gate_and_policy(item)[0]

GOLD_V2_VARIANT_COLUMNS = {'clinical_allergy_intolerance': ['substance_code'],
 'clinical_cancer_treatment': ['drug_code'],
 'clinical_clinical_finding': ['finding_code'],
 'clinical_clinical_score': ['score_code'],
 'clinical_condition': ['condition_code'],
 'clinical_condition_stage': ['stage_code'],
 'clinical_device': ['device_code'],
 'clinical_endoscopy_finding': ['finding_code'],
 'clinical_family_history': ['condition_code'],
 'clinical_imaging_exam': ['exam_code'],
 'clinical_medication_admin': ['medication_code'],
 'clinical_medication_dispense': ['medication_code'],
 'clinical_medication_order': ['medication_code'],
 'clinical_pathology_report': ['report_code'],
 'clinical_pathology_result': ['result_code'],
 'clinical_procedure': ['procedure_code', 'device_code'],
 'clinical_specimen': ['specimen_type'],
 'clinical_vital_sign': ['vital_code'],
 'text_document': ['document_type']}

GOLD_S3_AXIS_SPECS = {
 'clinical_allergy_intolerance': {'substance_code': [('substance', False)]},
 'clinical_cancer_treatment': {'drug_code': [('drug', False)]},
 'clinical_clinical_finding': {'finding_code': [('finding', False), ('value', False)]},
 'clinical_clinical_score': {'score_code': [('score', False)]},
 'clinical_condition': {'condition_code': [('condition', False)]},
 'clinical_condition_stage': {'stage_code': [('stage_diagnosis', False), ('stage_group', False)]},
 'clinical_device': {'device_code': [('device', False)]},
 'clinical_endoscopy_finding': {'finding_code': [('finding', False)]},
 'clinical_family_history': {'condition_code': [('condition', False)]},
 'clinical_imaging_exam': {'exam_code': [('exam', False)]},
 'clinical_medication_admin': {'medication_code': [('medication', False)]},
 'clinical_medication_dispense': {'medication_code': [('medication', False)]},
 'clinical_medication_order': {'medication_code': [('medication', False)]},
 'clinical_pathology_report': {'report_code': [('report', True)]},
 'clinical_pathology_result': {'result_code': [('test', False), ('result', False)]},
 'clinical_procedure': {'procedure_code': [('procedure', False)], 'device_code': [('device', False)]},
 'clinical_specimen': {'specimen_type': [('specimen_type', False)]},
 'clinical_vital_sign': {'vital_code': [('vital', False)]},
 'text_document': {'document_type': [('document_type', True)]},
}

# Flat S3b axes do not cross the legacy VARIANT bridge.  They are governed by
# the same predicate and retain the selected policy id on the QC twin.
GOLD_S3B_DIRECT_AXES = {
    "clinical_presenting_complaint": [("complaint", False)],
    "clinical_medication_admin_ingredient": [("ingredient", False)],
    "clinical_medication_order_ingredient": [("ingredient", False)],
    "spine_person": [("ethnicity", False)],
    "clinical_form_smoking_status": [
        ("smoking_status", False), ("cessation_advice", False),
        ("cessation_referral", False), ("nicotine_treatment", False),
        ("dependence_level", False),
    ],
    "clinical_vital_sign": [
        ("unit", False), ("method", False), ("body_site", False), ("interpretation", False),
    ],
    "clinical_clinical_score_component": [("component", False)],
    "clinical_form_response": [("question", False), ("answer", False), ("unit", False)],
}
GOLD_V2_LIFECYCLE["clinical_presenting_complaint"] = {
    "join_keys": ["patient_event_key"],
    "support_columns": [
        "identity_status", "record_status", "record_status_effective_from",
        "record_status_effective_to", "person_id_resolved", "encounter_id_resolved",
        "load_batch_id", "source_update_timestamp", "loaded_at",
    ],
}
GOLD_V2_DATE_FLAGS["clinical_presenting_complaint"] = ["event_after_death_30d", "event_before_birth"]
GOLD_V2_VARIANT_COLUMNS["clinical_presenting_complaint"] = []

def _s3_axis_columns(axis, source_only=False):
    """List the source, mapped-target and provenance columns belonging to a coding axis."""
    out=[f"{axis}_source_system",f"{axis}_source_code",f"{axis}_source_display"]
    if not source_only:
        out += [f"{axis}_snomed_code",f"{axis}_snomed_display",f"{axis}_omop_concept_id",
                f"{axis}_map_method",f"{axis}_map_version",f"{axis}_map_confidence",
                f"{axis}_map_rule_id",f"{axis}_map_candidate_count",
                f"{axis}_target_system",f"{axis}_target_code",f"{axis}_target_display",
                f"{axis}_omop_source_concept_id",f"{axis}_map_cosine",f"{axis}_map_scoring_model",
                f"{axis}_map_status",f"{axis}_map_status_reason",f"{axis}_map_competing_count",
                f"{axis}_map_candidate_set_id",f"{axis}_map_component_index",
                f"{axis}_map_component_count",f"{axis}_map_rollup_levels"]
    return out

_S3_GOLD_AXIS_SCHEMA = "struct<source_system:string,source_code:string,source_display:string,snomed_code:string,snomed_display:string,omop_concept_id:bigint,map_method:string,map_version:string,map_confidence:double,map_rule_id:string,map_candidate_count:int,target_system:string,target_code:string,target_display:string,omop_source_concept_id:bigint,map_cosine:double,map_scoring_model:string,map_status:string,map_status_reason:string,map_competing_count:int,map_candidate_set_id:string,map_component_index:int,map_component_count:int,map_rollup_levels:int>"

def _s3_axis_payload(source, axes):
    """Carry coding axes across QC joins as JSON strings, avoiding join-shaped VARIANT output."""
    fields=[]
    for axis, source_only in axes:
        names=_s3_axis_columns(axis,source_only)
        fields.append(F.struct(*[F.col(name).alias(name[len(axis)+1:]) for name in names]).alias(axis))
    return F.to_json(F.struct(*fields))

def _s3_flatten_gold_public(df, table_name):
    """Restore flat coding columns and null targets that fail the mapping policy."""
    specs=GOLD_S3_AXIS_SPECS.get(table_name,{})
    for old_name, axes in specs.items():
        # Parse once and gate once (see GOLD_PARSE_ONCE in _qc): inline from_json and a per-field gate
        # copy the parse into every reference.
        parsed="__gold_parsed_"+old_name
        df=df.withColumn(parsed,F.from_json(F.col(old_name), "struct<"+",".join(
            f"{axis}:{_S3_GOLD_AXIS_SCHEMA}" for axis,_ in axes)+">"))
        payload=F.col(parsed)
        scratch=[parsed]
        replacement=[]
        for axis, source_only in axes:
            a=payload[axis]
            for field in ("source_system","source_code","source_display"):
                df=df.withColumn(f"{axis}_{field}",a[field])
            if not source_only:
                gate, _ = _s3_axis_gate_and_policy(a)
                df=df.withColumn("__gold_gate_"+axis,gate)
                scratch.append("__gold_gate_"+axis)
                gate=F.col("__gold_gate_"+axis)
                for field in ("snomed_code","snomed_display","omop_concept_id",
                              "target_system","target_code","target_display"):
                    df=df.withColumn(f"{axis}_{field}",F.when(gate,a[field]))
                for field in ("map_method","map_version","map_confidence","map_rule_id","map_candidate_count",
                              "omop_source_concept_id","map_cosine","map_scoring_model","map_status",
                              "map_status_reason","map_competing_count","map_candidate_set_id",
                              "map_component_index","map_component_count","map_rollup_levels"):
                    df=df.withColumn(f"{axis}_{field}",a[field])
                # BBR_FOLDIN_V1 (BBR-10, review fix 11-03): "mapped" means a code landed. A mapping the
                # admission gate withholds (below its band cutoff, or no matching policy) publishes no code, so
                # it reads no_candidate with the reason; method, rule and score stay as provenance.
                withheld=(~gate)&(F.coalesce(a["map_status"],F.lit(""))=="mapped")
                df=df.withColumn(f"{axis}_map_status_reason",F.when(withheld,F.lit("withheld_by_admission_policy"))
                                 .otherwise(a["map_status_reason"]))
                df=df.withColumn(f"{axis}_map_status",F.when(withheld,F.lit("no_candidate")).otherwise(a["map_status"]))
            replacement.extend(_s3_axis_columns(axis,source_only))
        order=[]
        for name in df.columns:
            if name==old_name: order.extend(replacement)
            elif name not in replacement: order.append(name)
        df=df.select(*[name for name in order if name not in scratch])
    # SCT_TRUD_V1: the W4 value rules on the restored axis columns (deferred by insr_w4_apply_value_rules, e.g.
    # sct_concept_only: SNOMED columns hold concept IDs only) apply here, where those columns first exist.
    restored = {name for axes in specs.values() for axis, source_only in axes
                for name in _s3_axis_columns(axis, source_only)}
    for rule_id, rule in sorted(INSR_W4_VALUE_RULES.items()):
        targets = [rule["column"], *rule["companions"]]
        if rule["table"] != table_name or not set(targets) <= restored:
            continue
        df = df.withColumn("__insr_w4_hit", F.coalesce(F.expr(rule["match"]), F.lit(False)))
        for column in targets:
            df = df.withColumn(column, F.when(~F.col("__insr_w4_hit"), F.col(column)))
        df = df.drop("__insr_w4_hit")
    return df

def _s3_gate_direct_public(df, table_name):
    """Null governed target columns on flat axes while preserving provenance."""
    for axis, source_only in GOLD_S3B_DIRECT_AXES.get(table_name, ()):
        if source_only:
            continue
        gate = F.coalesce(F.col(f"_{axis}_gate_pass"), F.lit(False))
        for suffix in ("snomed_code", "snomed_display", "omop_concept_id",
                       "target_system", "target_code", "target_display"):
            name = f"{axis}_{suffix}"
            if name in df.columns:
                df = df.withColumn(name, F.when(gate, F.col(name)))
    return df

def _select_alias(expression):
    """Split an explicitly aliased SQL projection into its output name and expression."""
    match = re.match(r"(?is)^\s*([\s\S]*?)\s+AS\s+\x60?([A-Za-z_][A-Za-z0-9_]*)\x60?\s*$", expression)
    if not match:
        raise ValueError(f"Gold SELECT expression has no explicit alias: {expression}")
    return match.group(2), match.group(1)


# LIFECYCLE_ONE_ROW_V1 (2026-10-25)
# Each internal silver metadata MV must hold one row per join key, but a ROW_BASED silver
# update on 2026-10-17 left 5.47M stale duplicate rows in _clinical_pathology_order_metadata
# and the left join in _qc() copied every affected gold row twice. For tables listed here,
# gold first reduces that metadata to one row per key in its own internal MV
# (<internal>._qc_lifecycle_<table>, declared next to the table's twin) and _qc() joins that
# MV instead. A separate MV keeps the twin's plan shape unchanged; doing the group-by inside
# the twin made Enzyme reject its incremental plan (EXCESSIVE_OPERATOR_NESTING, dev 10-25).
# Adding a table here also needs its _qc_lifecycle_ MV declared, and fully recomputes that
# twin once. silver_journey_post_refresh reports duplicate keys in every metadata MV.
GOLD_LIFECYCLE_ONE_ROW_PER_KEY = {"clinical_pathology_order"}


def _lifecycle_one_row_source(table_name):
    """One row per join key of a table's internal silver metadata (body of its _qc_lifecycle_ MV).

    Per key it keeps the greatest struct of the support columns, so a whole row survives
    intact and the same input always gives the same survivor (dropDuplicates would keep an
    arbitrary row). The struct leads with the load clocks, so the most recently loaded copy
    wins; copies from the same load fall back to comparing the remaining columns in order,
    which for identity_status keeps 'unresolved' over 'resolved' (the conservative choice:
    the row leaves the research surface rather than being published on a disputed identity;
    9 such keys in prod on 2026-10-25). Identical duplicates collapse to themselves and clean
    keys are unchanged. A single-source group-by is Enzyme-eligible (EXPLAIN CREATE
    MATERIALIZED VIEW on 8_dev.incr_probe; dev updates planned GROUP_AGGREGATE).
    """
    lifecycle = GOLD_V2_LIFECYCLE[table_name]
    keys = lifecycle["join_keys"]
    internal_source = spark.conf.get("journey.silver_internal_catalog_schema")
    metadata = spark.read.table(internal_source + "._" + table_name + "_metadata")
    support = [column for column in lifecycle["support_columns"] if column in metadata.columns]
    clocks = [column for column in ("load_batch_id", "loaded_at", "source_update_timestamp") if column in support]
    ordered = clocks + [column for column in support if column not in clocks]
    chosen = F.max(F.struct(*[F.col(column) for column in ordered])).alias("__gold_lifecycle_row")
    return metadata.groupBy(*keys).agg(chosen).select(
        *keys, *[F.col("__gold_lifecycle_row." + column).alias(column) for column in support]
    )


# INSR_W3 W3.02 (C0 ruling 1, 2026-10-29) -----------------------------------------------------------------------
# Gold person scope. Silver reference_person_scope holds one row per spine person with
# in_scope = has_encounter OR has_clinical_data; gold_qc._person_scope projects it. Gold drops the rows of a spine
# person whose in_scope is false. A row with no person_id is not person-attributable and is kept; a row whose
# person_id is not a spine person is an orphan, which the fk_columns containment in _qc nulls exactly as before.
# _qc (flag + mandatory rule) and _tdx_product / the direct twins (filter) share _person_scope_join, so the
# predicate cannot diverge between the paths.


def _person_scope_join(df, person_col="person_id"):
    """Left-join the gold person scope on person_col.

    Adds _qc_scope_person_id (NULL when the person is not a spine person) and _qc_person_in_scope (FALSE only for
    a spine person whose in_scope is false; TRUE for in-scope persons, NULL person_id and orphans). The scope MV
    holds only these two columns, so a change to a person descriptor never touches the person twins.
    """
    scope = spark.read.table(_n("gold_qc._person_scope")).select("_qc_scope_person_id", "_qc_scope_in_scope")
    person = F.expr(f"try_cast(`{person_col}` AS BIGINT)")
    return (
        df.join(scope, person == F.col("_qc_scope_person_id"), "left")
        .withColumn("_qc_person_in_scope", F.coalesce(F.col("_qc_scope_in_scope"), F.lit(True)))
        .drop("_qc_scope_in_scope")
    )


def _apply_person_scope(df, person_col="person_id"):
    """Filter form of the gold person scope, for products without a QC twin (TDX, direct twins).

    Keeps in-scope persons, rows with no person and orphan persons; a table without person_col passes unchanged.
    """
    if person_col not in df.columns:
        return df
    return (
        _person_scope_join(df, person_col)
        .where(F.col("_qc_person_in_scope"))
        .drop("_qc_scope_person_id", "_qc_person_in_scope")
    )


def _flag_person_scope_any(df, person_cols):
    """Two-person rows (spine_person_relationship): flag _qc_person_in_scope TRUE when any named person is in scope.

    Each column is joined to the same scope MV in turn. A NULL or orphan person counts as in scope (as in
    _person_scope_join), so the row leaves gold only when every named person is a spine person whose in_scope is
    false. The twin's generated mandatory rule reads the flag, so Lakeflow counts the drops.
    """
    flags = []
    for position, column in enumerate(person_cols):
        flag = f"_qc_person_in_scope_{position}"
        df = (_person_scope_join(df, column)
              .withColumnRenamed("_qc_person_in_scope", flag)
              .drop("_qc_scope_person_id"))
        flags.append(flag)
    any_in_scope = F.col(flags[0])
    for flag in flags[1:]:
        any_in_scope = any_in_scope | F.col(flag)
    return df.withColumn("_qc_person_in_scope", any_in_scope).drop(*flags)


def _qc(table_name, select_exprs, fk_columns=(), date_flags=()):
    """Apply Gold repairs while keeping lifecycle and QC fields internal.

    Public silver-v2 supplies research fields and source history. Separate
    internal metadata supplies the inputs used by unchanged Gold expectations. Date flags are
    recomputed from Gold's repaired event time. VARIANT values cross joins as
    JSON strings and are parsed only by the single-source public wrapper.
    """
    source = spark.read.table(_src(table_name))
    variants = set(GOLD_V2_VARIANT_COLUMNS.get(table_name, []))
    if variants:
        for old_name, axes in GOLD_S3_AXIS_SPECS.get(table_name, {}).items():
            if old_name not in source.columns:
                source = source.withColumn("__gold_source_json_" + old_name, _s3_axis_payload(source, axes))
        source = source.select(
            *[
                F.to_json(F.col(column)).alias("__gold_source_json_" + column)
                if column in variants
                else F.col(column)
                for column in source.columns
            ]
        )
    lifecycle = GOLD_V2_LIFECYCLE.get(table_name)
    if lifecycle:
        keys = lifecycle["join_keys"]
        support = lifecycle["support_columns"]
        # Source history now comes directly from the research table. Only QC
        # and batch metadata requires the separate internal projection.
        internal_support = [column for column in support if column not in source.columns]
        if internal_support:
            internal_source = spark.conf.get("journey.silver_internal_catalog_schema")
            life_table = internal_source + "._" + table_name + "_metadata"
            if table_name in GOLD_LIFECYCLE_ONE_ROW_PER_KEY:  # LIFECYCLE_ONE_ROW_V1
                life_table = _n("gold_qc._lifecycle_" + table_name)
            life = spark.read.table(life_table).select(*keys, *internal_support)
            source = source.join(life, keys, "left")
    else:
        support = []

    source_support = GOLD_V2_SOURCE_SUPPORT.get(table_name, [])
    prepared = []
    selected_names = set()
    for expression in select_exprs:
        alias, body = _select_alias(expression)
        selected_names.add(alias)
        if alias in variants:
            prepared.append(
                f"`__gold_source_json_{alias}` "
                f"AS `__gold_json_{alias}`"
            )
        else:
            prepared.append(expression)
    for column in [*support, *source_support]:
        if column not in selected_names:
            prepared.append(f"`{column}` AS `{column}`")

    sex_column = GOLD_SEX_PLAUSIBILITY_COLUMNS.get(table_name)
    if sex_column:
        prepared.append(f"`{sex_column}` AS `_qc_sex_concept_id`")
    df = source.selectExpr(*prepared)
    df = insr_w4_apply_value_rules(table_name, df)  # INSR_W4
    if sex_column:
        # DQ4 G2: internal flag event_sex_implausible; the public wrappers select explicitly.
        rules = spark.read.table(GOLD_SEX_PLAUSIBILITY_TABLE).select(
            F.col("concept_id").alias("_qc_sex_rule_concept_id"),
            F.col("plausible_gender").alias("_qc_sex_plausible"),
        )
        sexes = (
            spark.read.table(_src("spine_person"))
            .select(F.col("person_id").alias("_qc_sex_person_id"), F.col("gender_display").alias("_qc_person_sex"))
            .where(F.col("_qc_sex_person_id").isNotNull() & F.col("_qc_person_sex").isin("Male", "Female"))
            .dropDuplicates(["_qc_sex_person_id"])
        )
        df = (
            df.join(F.broadcast(rules), df._qc_sex_concept_id == rules._qc_sex_rule_concept_id, "left")
            .join(sexes, df.person_id == sexes._qc_sex_person_id, "left")
            .withColumn(
                "event_sex_implausible",
                F.coalesce(F.col("_qc_sex_plausible") != F.col("_qc_person_sex"), F.lit(False)),
            )
            .drop("_qc_sex_concept_id", "_qc_sex_rule_concept_id", "_qc_sex_plausible", "_qc_sex_person_id", "_qc_person_sex")
        )

    if "person_id" in df.columns:
        # INSR_W3 W3.02: one join gives the scope flag (mandatory rule <table>.person_id.person_in_scope) and the
        # spine containment below; the scope MV holds exactly the silver spine persons (one row each).
        df = _person_scope_join(df)
        if "person_id" in fk_columns:
            # INSR_W3 W3.08 (L5240, F6): pre-null orphan flag, counted by <table>.person_id.orphan_nulled.
            df = df.withColumn("_qc_person_id_orphan",
                               F.col("person_id").isNotNull() & F.col("_qc_scope_person_id").isNull())
            df = df.withColumn(
                "person_id",
                F.when(F.col("_qc_scope_person_id").isNotNull(), F.col("person_id")),
            )
        df = df.drop("_qc_scope_person_id")
    if "encounter_id" in fk_columns:
        parent = spark.read.table("_gold_encounter_keys")
        df = (
            df.join(parent, df.encounter_id == parent._qc_encounter_id, "left")
            .withColumn("_qc_encounter_id_orphan", F.col("encounter_id").isNotNull() & F.col("_qc_encounter_id").isNull())  # INSR_W3 W3.08
            .withColumn(
                "encounter_id",
                F.when(F.col("_qc_encounter_id").isNotNull(), F.col("encounter_id")),
            )
            .drop("_qc_encounter_id")
        )
    if "episode_id" in fk_columns:
        parent = spark.read.table("_gold_episode_keys")
        df = (
            df.join(parent, df.episode_id == parent._qc_episode_id, "left")
            .withColumn("_qc_episode_id_orphan", F.col("episode_id").isNotNull() & F.col("_qc_episode_id").isNull())  # INSR_W3 W3.08
            .withColumn(
                "episode_id",
                F.when(F.col("_qc_episode_id").isNotNull(), F.col("episode_id")),
            )
            .drop("_qc_episode_id")
        )

    requested_date_flags = tuple(
        dict.fromkeys((*date_flags, *GOLD_V2_DATE_FLAGS.get(table_name, [])))
    )
    requested_date_flags = insr_w4_date_flags(table_name, requested_date_flags)  # INSR_W4
    if requested_date_flags:
        dates = spark.read.table("_gold_person_dates")
        df = df.join(dates, df.person_id == dates._qc_date_person_id, "left")
        if "event_before_birth" in requested_date_flags:
            df = df.withColumn(
                "event_before_birth",
                F.coalesce(
                    F.col("event_datetime") < F.col("_qc_birth_datetime"),
                    F.lit(False),
                ),
            )
        if "event_after_death_30d" in requested_date_flags:
            df = df.withColumn(
                "event_after_death_30d",
                F.coalesce(
                    F.col("event_datetime")
                    > F.col("_qc_deceased_datetime") + F.expr("INTERVAL 30 DAYS"),
                    F.lit(False),
                ),
            )
        df = insr_w4_birth_flags(table_name, df)  # INSR_W4
        df = df.drop(
            "_qc_date_person_id",
            "_qc_birth_datetime",
            "_qc_deceased_datetime",
        )

    # Keep an internal per-axis gate flag on the QC twin. The public wrapper
    # applies the same predicate to code columns and leaves provenance visible.
    for old_name, axes in GOLD_S3_AXIS_SPECS.get(table_name, {}).items():
        transport = "__gold_json_" + old_name
        if transport not in df.columns:
            continue
        # GOLD_PARSE_ONCE_PATCH_APPLIED v1
        # Parse once into a column. An inline from_json is copied into every field reference of the
        # policy gate (495 copies per axis at 22 policies), and Enzyme then plans the join-shaped twin
        # for tens of minutes until the driver runs out of memory (2026-09-25/26 gold failures).
        parsed = "__gold_parsed_" + old_name
        df = df.withColumn(parsed, F.from_json(F.col(transport), "struct<" + ",".join(
            f"{axis}:{_S3_GOLD_AXIS_SCHEMA}" for axis, _ in axes) + ">"))
        payload = F.col(parsed)
        for axis, source_only in axes:
            if not source_only:
                item = payload[axis]
                gate, policy_id = _s3_axis_gate_and_policy(item)
                has_target = item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
                df = df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
                df = df.withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id))
        df = df.drop(parsed)
    for axis, source_only in GOLD_S3B_DIRECT_AXES.get(table_name, ()):
        if not source_only:
            required = _s3_axis_columns(axis, False)
            if any(name not in df.columns for name in required):
                continue
            item = {name[len(axis)+1:]: F.col(name) for name in required}
            gate, policy_id = _s3_axis_gate_and_policy(item)
            has_target = item["target_code"].isNotNull() | item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
            df = df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
            df = df.withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id))
    return df



def _with_parent_status(child, parent, keys):
    """Keep rejected children until the native expectation counts them; parents must be unique on keys."""
    # Each caller projects and deduplicates only its parent keys before this join.
    # A marker, rather than a nullable key, also handles composite keys without ambiguity.
    return child.join(parent.withColumn("__gold_parent_present", F.lit(True)), keys, "left")


# INSR_W3 W3.09 (J31, L2167; review finding 9): view-level row_key for keyless gold children under 50M rows.
# to_json(named_struct(...)) keeps field names and order, writes NULL explicitly and escapes delimiters, so two
# different key tuples cannot encode to the same string (concat_ws would drop NULLs and not escape '|').
# Timestamps are unix_micros and doubles DECIMAL(38,10): the key depends on neither time zone nor float format.
GOLD_ROW_KEY_SQL = {
    'clinical_appointment_booking': "sha2(to_json(named_struct('sch_event_id', `sch_event_id`, 'sequence', `sequence`, 'schedule_id', `schedule_id`, 'schedule_sequence', `schedule_sequence`, 'status_code', `status_code`, 'status_display', `status_display`, 'status_meaning', `status_meaning`, 'effective_start', unix_micros(`effective_start`), 'effective_end', unix_micros(`effective_end`), 'location_code', `location_code`, 'location_display', `location_display`, 'source_present_ind', `source_present_ind`, 'loaded_at', unix_micros(`loaded_at`)), map('ignoreNullFields', 'false')), 256)",
    'clinical_condition_revision': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'clinical_hrg_grouping_diagnosis': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'clinical_hrg_grouping_procedure': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'clinical_medication_order_ingredient': "sha2(to_json(named_struct('order_id', `order_id`, 'action_sequence', `action_sequence`, 'component_sequence', `component_sequence`, 'ingredient_map_component_index', `ingredient_map_component_index`), map('ignoreNullFields', 'false')), 256)",
    'clinical_neonatal_care_day_activity': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'clinical_neonatal_care_day_high_cost_drug': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'clinical_procedure_implant_attribute': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'implant_event_id', `implant_event_id`, 'implant_sequence', `implant_sequence`, 'sequence', `sequence`, 'clinical_event_id', `clinical_event_id`, 'attribute_name', `attribute_name`, 'attribute_value', `attribute_value`, 'event_tag', `event_tag`, 'event_title', `event_title`, 'result_value', `result_value`, 'value_source', `value_source`, 'value_conflict_ind', `value_conflict_ind`, 'performed_datetime', unix_micros(`performed_datetime`), 'valid_from', unix_micros(`valid_from`), 'loaded_at', unix_micros(`loaded_at`)), map('ignoreNullFields', 'false')), 256)",
    'clinical_rtt_pathway_encounter_type': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
    'text_document_sensitivity_label': "sha2(to_json(named_struct('patient_event_key', `patient_event_key`, 'sequence', `sequence`), map('ignoreNullFields', 'false')), 256)",
}
GOLD_ROW_KEY_COLUMNS = {
    'clinical_appointment_booking': 'sch_event_id, sequence, schedule_id, schedule_sequence, status_code, status_display, status_meaning, effective_start, effective_end, location_code, location_display, source_present_ind, loaded_at',
    'clinical_condition_revision': 'patient_event_key, sequence',
    'clinical_hrg_grouping_diagnosis': 'patient_event_key, sequence',
    'clinical_hrg_grouping_procedure': 'patient_event_key, sequence',
    'clinical_medication_order_ingredient': 'order_id, action_sequence, component_sequence, ingredient_map_component_index',
    'clinical_neonatal_care_day_activity': 'patient_event_key, sequence',
    'clinical_neonatal_care_day_high_cost_drug': 'patient_event_key, sequence',
    'clinical_procedure_implant_attribute': 'patient_event_key, implant_event_id, implant_sequence, sequence, clinical_event_id, attribute_name, attribute_value, event_tag, event_title, result_value, value_source, value_conflict_ind, performed_datetime, valid_from, loaded_at',
    'clinical_rtt_pathway_encounter_type': 'patient_event_key, sequence',
    'text_document_sensitivity_label': 'patient_event_key, sequence',
}


def _with_row_key(df, table_name):
    """Add the public row_key declared for table_name (W7.04 reads it as the table key)."""
    comment = ("Row key: SHA-256 of the JSON of (" + GOLD_ROW_KEY_COLUMNS[table_name] + "), NULLs written "
               "explicitly. Unique per row; stable while those values are unchanged. INSR_W3 W3.09.")
    return df.withColumn("row_key", F.expr(GOLD_ROW_KEY_SQL[table_name])).withMetadata("row_key", {"comment": comment})


def _tdx_comments(frame, product):
    """INSR_W8_COMMENTS_V1 (W8.04): attach the canonical dictionary comment to every column of a TDX product (or its
    "<product> QC" evidence twin). A column missing from gold_tdx_comments.py raises KeyError at graph build."""
    from gold_tdx_comments import GOLD_TDX_COLUMN_COMMENTS
    comments = GOLD_TDX_COLUMN_COMMENTS[product]
    for column in frame.columns:
        text = comments[column]
        _assert_not_boilerplate(text, f"{product}.{column}")
        frame = frame.withMetadata(column, {"comment": text})
    return frame

# S3D_GOLD_ADMISSION_PATCH_APPLIED v1
def _released(frame):
    """Release text when anonymisation succeeded and its input digest and context still match.

    Any redactor version is released: the version travels with the text as provenance.
    """
    columns = set(frame.columns)
    required = {
        "anon_status", "anon_source_text_sha",
        "anon_input_digest", "anon_context_fingerprint", "context_fingerprint_current",
    }
    if not required.issubset(columns):
        return F.lit(False)
    return (
        (F.col("anon_status") == "anonymized")
        & F.col("anon_source_text_sha").eqNullSafe(F.col("anon_input_digest"))
        & F.col("anon_context_fingerprint").eqNullSafe(F.col("context_fingerprint_current"))
    )

# INSR_W3 W3.06/W3.07: value repairs on TDX columns that have no `_clean` twin (where a `_clean` twin exists it
# already is the repaired value, so the raw column stays verbatim). column -> SQL condition that makes it NULL.
TDX_VALUE_REPAIRS = {
    "clinical_safety_incident_participant": {"con_dob": "CAST(`con_dob` AS DATE) = DATE'1900-01-01'"},
    "clinical_prearrival": {"birth_dt_tm": "CAST(`birth_dt_tm` AS DATE) = DATE'1900-01-01'"},
    "reference_location_unit": {"end_effective_dt_tm": "CAST(`end_effective_dt_tm` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`end_effective_dt_tm` AS DATE)) < 9999"},  # INSR_W3 W3.07
}


def _tdx_product(target_name):
    """Prepare TDX values and fail-closed text; native decorators handle row admission."""
    df = spark.read.table(f"{SILVER_CATALOG_SCHEMA}.{target_name}")
    df = _apply_person_scope(df)  # INSR_W3 W3.02: same predicate as _qc (C0 ruling 1)
    for column, condition in TDX_VALUE_REPAIRS.get(target_name, {}).items():
        # INSR_W3 W3.06: Gold: NULL where the TDX repair condition holds (placeholder dates, open-end markers).
        df = df.withColumn(column, F.when(~F.coalesce(F.expr(condition), F.lit(False)), F.col(column)))
    if target_name == "text_safety_document":
        released = _released(df) & F.col("text_is_anonymised")
        df = df.withColumn("source_text_sha256", F.col("text_sha256"))
        df = df.withColumn("document_text", F.when(released, F.col("document_text_anonymised")))
        # GOLD_COLUMN_PARITY_V1: the source narrative and silver's anonymised field are published too (Ben,
        # 2026-10-17: nothing is withheld from gold except QC failures); document_text stays the released
        # anonymised text, and text_sha256/text_length/text_is_anonymised describe document_text.
        df = df.withColumn("text_sha256", F.sha2("document_text", 256))
        df = df.withColumn("text_length", F.length("document_text"))
        df = df.withColumn("text_is_anonymised", released)
    substitutions = {
        "clinical_prearrival": [("chief_complaint", "anon_chief_complaint")],
        "clinical_tracking_location_stay": [("tracking_reason_comment", "anon_tracking_reason_comment")],
        "clinical_pending_movement": [("transaction_reason", "anon_transaction_reason")],
        "reference_location_attribute_history": [
            ("value_string", "anon_value_string"),
            ("description", "anon_description"),
        ],
    }
    if target_name in substitutions:
        released = _released(df)
        for original, anonymised in substitutions[target_name]:
            if original in df.columns and anonymised in df.columns:
                # GOLD_COLUMN_PARITY_V1: the anon_* column itself is published as well.
                df = df.withColumn(original, F.when(released, F.col(anonymised)))
    df = insr_w4_apply_value_rules(target_name, df)  # INSR_W4
    return _tdx_comments(df, target_name)

def _tdx_qc(target_name, key_name):
    """Retain the old admitted-key evidence table without treating advisory warnings as failures."""
    frame = spark.read.table(f"{PUBLIC_SCHEMA}.{target_name}")
    return _tdx_comments(
        frame.select(F.col(key_name), F.lit("passed").alias("qc_status"), F.col("loaded_at").alias("qc_checked_at")),
        f"{target_name} QC",
    )


# ---------------------------------------------------------------------------------------------------------------
# INSR_W4_ROWRULES_V1: W4 row rules of release INSR_20261029 (catalogue insr-v1).
# Generated from ~/work/insr_20261029/w4/rowrules (rules.py, gold_section.py): edit the generator, not this text.
# Value rules NULL a column where their match condition holds. They run on the QC twin right after the projection
# (_qc), at the end of _tdx_product, and in the parent-child twins that call insr_w4_apply_value_rules directly.
# Warn rules are dp.expect_all entries in the notebooks' *_ADVISORY_RULES dicts. Every rule id has a dq_rule row.
# ---------------------------------------------------------------------------------------------------------------
INSR_W4_VALUE_RULES = {'gold.clinical.allergy_intolerance.substance_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'substance_snomed_code',
                                                                                   'companions': ['substance_snomed_display',
                                                                                                  'substance_target_code',
                                                                                                  'substance_target_display'],
                                                                                   'match': '`substance_snomed_code` '
                                                                                            'IS NOT NULL AND (NOT '
                                                                                            '`substance_snomed_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                                            'substring(`substance_snomed_code`, '
                                                                                            'length(`substance_snomed_code`) '
                                                                                            "- 2, 2) NOT IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`substance_snomed_code`, '
                                                                                            'length(`substance_snomed_code`) '
                                                                                            "- 2, 2) = '10' AND "
                                                                                            'length(`substance_snomed_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_allergy_intolerance'},
 'gold.clinical.allergy_intolerance.substance_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'substance_target_code',
                                                                                   'companions': ['substance_target_display',
                                                                                                  'substance_snomed_code',
                                                                                                  'substance_snomed_display'],
                                                                                   'match': '`substance_target_system` '
                                                                                            '= '
                                                                                            "'http://snomed.info/sct' "
                                                                                            'AND '
                                                                                            '`substance_target_code` '
                                                                                            'IS NOT NULL AND (NOT '
                                                                                            '`substance_target_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                                            'substring(`substance_target_code`, '
                                                                                            'length(`substance_target_code`) '
                                                                                            "- 2, 2) NOT IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`substance_target_code`, '
                                                                                            'length(`substance_target_code`) '
                                                                                            "- 2, 2) = '10' AND "
                                                                                            'length(`substance_target_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_allergy_intolerance'},
 'gold.clinical.appointment.allocated_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'allocated_practitioner_id',
                                                                         'companions': [],
                                                                         'match': '`allocated_practitioner_id` = 0',
                                                                         'table': 'clinical_appointment'},
 'gold.clinical.appointment.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                               'column': 'organization_id',
                                                               'companions': [],
                                                               'match': '`organization_id` = 0',
                                                               'table': 'clinical_appointment'},
 'gold.clinical.appointment.requested_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'requested_practitioner_id',
                                                                         'companions': [],
                                                                         'match': '`requested_practitioner_id` = 0',
                                                                         'table': 'clinical_appointment'},
 'gold.clinical.appointment_action.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'organization_id',
                                                                      'companions': [],
                                                                      'match': '`organization_id` = 0',
                                                                      'table': 'clinical_appointment_action'},
 'gold.clinical.appointment_action.req_action_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                    'column': 'req_action_id',
                                                                    'companions': [],
                                                                    'match': '`req_action_id` = 0',
                                                                    'table': 'clinical_appointment_action'},
 'gold.clinical.appointment_action.resource_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                    'column': 'resource_cd',
                                                                    'companions': [],
                                                                    'match': '`resource_cd` = 0 AND '
                                                                             '`resource_display` IS NULL',
                                                                    'table': 'clinical_appointment_action'},
 'gold.clinical.appointment_action.schedule_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                  'column': 'schedule_id',
                                                                  'companions': [],
                                                                  'match': '`schedule_id` = 0',
                                                                  'table': 'clinical_appointment_action'},
 'gold.clinical.bed_status_observation.bed_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'bed_status_cd',
                                                                          'companions': [],
                                                                          'match': '`bed_status_cd` = 0 AND '
                                                                                   '`bed_status_display` IS NULL',
                                                                          'table': 'clinical_bed_status_observation'},
 'gold.clinical.cancer_treatment.drug_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'drug_snomed_code',
                                                                           'companions': ['drug_snomed_display',
                                                                                          'drug_target_code',
                                                                                          'drug_target_display'],
                                                                           'match': '`drug_snomed_code` IS NOT NULL '
                                                                                    'AND (NOT `drug_snomed_code` '
                                                                                    "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                    'substring(`drug_snomed_code`, '
                                                                                    'length(`drug_snomed_code`) - 2, '
                                                                                    "2) NOT IN ('00', '10') OR "
                                                                                    '(substring(`drug_snomed_code`, '
                                                                                    'length(`drug_snomed_code`) - 2, '
                                                                                    "2) = '10' AND "
                                                                                    'length(`drug_snomed_code`) < '
                                                                                    '11))',
                                                                           'table': 'clinical_cancer_treatment'},
 'gold.clinical.cancer_treatment.drug_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'drug_target_code',
                                                                           'companions': ['drug_target_display',
                                                                                          'drug_snomed_code',
                                                                                          'drug_snomed_display'],
                                                                           'match': '`drug_target_system` = '
                                                                                    "'http://snomed.info/sct' AND "
                                                                                    '`drug_target_code` IS NOT NULL '
                                                                                    'AND (NOT `drug_target_code` '
                                                                                    "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                    'substring(`drug_target_code`, '
                                                                                    'length(`drug_target_code`) - 2, '
                                                                                    "2) NOT IN ('00', '10') OR "
                                                                                    '(substring(`drug_target_code`, '
                                                                                    'length(`drug_target_code`) - 2, '
                                                                                    "2) = '10' AND "
                                                                                    'length(`drug_target_code`) < '
                                                                                    '11))',
                                                                           'table': 'clinical_cancer_treatment'},
 'gold.clinical.clinical_finding.finding_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'finding_snomed_code',
                                                                              'companions': ['finding_snomed_display',
                                                                                             'finding_target_code',
                                                                                             'finding_target_display'],
                                                                              'match': '`finding_snomed_code` IS NOT '
                                                                                       'NULL AND (NOT '
                                                                                       '`finding_snomed_code` RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' OR "
                                                                                       'substring(`finding_snomed_code`, '
                                                                                       'length(`finding_snomed_code`) '
                                                                                       "- 2, 2) NOT IN ('00', '10') "
                                                                                       'OR '
                                                                                       '(substring(`finding_snomed_code`, '
                                                                                       'length(`finding_snomed_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`finding_snomed_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.finding_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'finding_target_code',
                                                                              'companions': ['finding_target_display',
                                                                                             'finding_snomed_code',
                                                                                             'finding_snomed_display'],
                                                                              'match': '`finding_target_system` = '
                                                                                       "'http://snomed.info/sct' AND "
                                                                                       '`finding_target_code` IS NOT '
                                                                                       'NULL AND (NOT '
                                                                                       '`finding_target_code` RLIKE '
                                                                                       "'^[1-9][0-9]{5,17}$' OR "
                                                                                       'substring(`finding_target_code`, '
                                                                                       'length(`finding_target_code`) '
                                                                                       "- 2, 2) NOT IN ('00', '10') "
                                                                                       'OR '
                                                                                       '(substring(`finding_target_code`, '
                                                                                       'length(`finding_target_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`finding_target_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.order_id.insr_f7_zero_id': {'action': 'invalidate',
                                                             'column': 'order_id',
                                                             'companions': [],
                                                             'match': "TRIM(`order_id`) = '0'",
                                                             'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.organization_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                    'column': 'organization_id',
                                                                    'companions': [],
                                                                    'match': '`organization_id` = 0',
                                                                    'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'performer_practitioner_id',
                                                                              'companions': [],
                                                                              'match': '`performer_practitioner_id` '
                                                                                       '= 0',
                                                                              'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.value_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'value_snomed_code',
                                                                            'companions': ['value_snomed_display',
                                                                                           'value_target_code',
                                                                                           'value_target_display'],
                                                                            'match': '`value_snomed_code` IS NOT '
                                                                                     'NULL AND (NOT '
                                                                                     '`value_snomed_code` RLIKE '
                                                                                     "'^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`value_snomed_code`, '
                                                                                     'length(`value_snomed_code`) - '
                                                                                     "2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`value_snomed_code`, '
                                                                                     'length(`value_snomed_code`) - '
                                                                                     "2, 2) = '10' AND "
                                                                                     'length(`value_snomed_code`) < '
                                                                                     '11))',
                                                                            'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.value_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'value_target_code',
                                                                            'companions': ['value_target_display',
                                                                                           'value_snomed_code',
                                                                                           'value_snomed_display'],
                                                                            'match': '`value_target_system` = '
                                                                                     "'http://snomed.info/sct' AND "
                                                                                     '`value_target_code` IS NOT '
                                                                                     'NULL AND (NOT '
                                                                                     '`value_target_code` RLIKE '
                                                                                     "'^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`value_target_code`, '
                                                                                     'length(`value_target_code`) - '
                                                                                     "2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`value_target_code`, '
                                                                                     'length(`value_target_code`) - '
                                                                                     "2, 2) = '10' AND "
                                                                                     'length(`value_target_code`) < '
                                                                                     '11))',
                                                                            'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_finding.verifier_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                             'column': 'verifier_practitioner_id',
                                                                             'companions': [],
                                                                             'match': '`verifier_practitioner_id` = '
                                                                                      '0',
                                                                             'table': 'clinical_clinical_finding'},
 'gold.clinical.clinical_score.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'performer_practitioner_id',
                                                                            'companions': [],
                                                                            'match': '`performer_practitioner_id` = '
                                                                                     '0',
                                                                            'table': 'clinical_clinical_score'},
 'gold.clinical.clinical_score.score_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'score_snomed_code',
                                                                          'companions': ['score_snomed_display',
                                                                                         'score_target_code',
                                                                                         'score_target_display'],
                                                                          'match': '`score_snomed_code` IS NOT NULL '
                                                                                   'AND (NOT `score_snomed_code` '
                                                                                   "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`score_snomed_code`, '
                                                                                   'length(`score_snomed_code`) - 2, '
                                                                                   "2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`score_snomed_code`, '
                                                                                   'length(`score_snomed_code`) - 2, '
                                                                                   "2) = '10' AND "
                                                                                   'length(`score_snomed_code`) < '
                                                                                   '11))',
                                                                          'table': 'clinical_clinical_score'},
 'gold.clinical.clinical_score.score_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'score_target_code',
                                                                          'companions': ['score_target_display',
                                                                                         'score_snomed_code',
                                                                                         'score_snomed_display'],
                                                                          'match': '`score_target_system` = '
                                                                                   "'http://snomed.info/sct' AND "
                                                                                   '`score_target_code` IS NOT NULL '
                                                                                   'AND (NOT `score_target_code` '
                                                                                   "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`score_target_code`, '
                                                                                   'length(`score_target_code`) - 2, '
                                                                                   "2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`score_target_code`, '
                                                                                   'length(`score_target_code`) - 2, '
                                                                                   "2) = '10' AND "
                                                                                   'length(`score_target_code`) < '
                                                                                   '11))',
                                                                          'table': 'clinical_clinical_score'},
 'gold.clinical.clinical_score_component.component_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                        'column': 'component_snomed_code',
                                                                                        'companions': ['component_snomed_display',
                                                                                                       'component_target_code',
                                                                                                       'component_target_display'],
                                                                                        'match': '`component_snomed_code` '
                                                                                                 'IS NOT NULL AND '
                                                                                                 '(NOT '
                                                                                                 '`component_snomed_code` '
                                                                                                 'RLIKE '
                                                                                                 "'^[1-9][0-9]{5,17}$' "
                                                                                                 'OR '
                                                                                                 'substring(`component_snomed_code`, '
                                                                                                 'length(`component_snomed_code`) '
                                                                                                 '- 2, 2) NOT IN '
                                                                                                 "('00', '10') OR "
                                                                                                 '(substring(`component_snomed_code`, '
                                                                                                 'length(`component_snomed_code`) '
                                                                                                 "- 2, 2) = '10' AND "
                                                                                                 'length(`component_snomed_code`) '
                                                                                                 '< 11))',
                                                                                        'table': 'clinical_clinical_score_component'},
 'gold.clinical.clinical_score_component.component_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                        'column': 'component_target_code',
                                                                                        'companions': ['component_target_display',
                                                                                                       'component_snomed_code',
                                                                                                       'component_snomed_display'],
                                                                                        'match': '`component_target_system` '
                                                                                                 '= '
                                                                                                 "'http://snomed.info/sct' "
                                                                                                 'AND '
                                                                                                 '`component_target_code` '
                                                                                                 'IS NOT NULL AND '
                                                                                                 '(NOT '
                                                                                                 '`component_target_code` '
                                                                                                 'RLIKE '
                                                                                                 "'^[1-9][0-9]{5,17}$' "
                                                                                                 'OR '
                                                                                                 'substring(`component_target_code`, '
                                                                                                 'length(`component_target_code`) '
                                                                                                 '- 2, 2) NOT IN '
                                                                                                 "('00', '10') OR "
                                                                                                 '(substring(`component_target_code`, '
                                                                                                 'length(`component_target_code`) '
                                                                                                 "- 2, 2) = '10' AND "
                                                                                                 'length(`component_target_code`) '
                                                                                                 '< 11))',
                                                                                        'table': 'clinical_clinical_score_component'},
 'gold.clinical.condition.asserter_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'asserter_practitioner_id',
                                                                      'companions': [],
                                                                      'match': '`asserter_practitioner_id` = 0',
                                                                      'table': 'clinical_condition'},
 'gold.clinical.condition.condition_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                         'column': 'condition_snomed_code',
                                                                         'companions': ['condition_snomed_display',
                                                                                        'condition_target_code',
                                                                                        'condition_target_display'],
                                                                         'match': '`condition_snomed_code` IS NOT '
                                                                                  'NULL AND (NOT '
                                                                                  '`condition_snomed_code` RLIKE '
                                                                                  "'^[1-9][0-9]{5,17}$' OR "
                                                                                  'substring(`condition_snomed_code`, '
                                                                                  'length(`condition_snomed_code`) - '
                                                                                  "2, 2) NOT IN ('00', '10') OR "
                                                                                  '(substring(`condition_snomed_code`, '
                                                                                  'length(`condition_snomed_code`) - '
                                                                                  "2, 2) = '10' AND "
                                                                                  'length(`condition_snomed_code`) < '
                                                                                  '11))',
                                                                         'table': 'clinical_condition'},
 'gold.clinical.condition.condition_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                         'column': 'condition_target_code',
                                                                         'companions': ['condition_target_display',
                                                                                        'condition_snomed_code',
                                                                                        'condition_snomed_display'],
                                                                         'match': '`condition_target_system` = '
                                                                                  "'http://snomed.info/sct' AND "
                                                                                  '`condition_target_code` IS NOT '
                                                                                  'NULL AND (NOT '
                                                                                  '`condition_target_code` RLIKE '
                                                                                  "'^[1-9][0-9]{5,17}$' OR "
                                                                                  'substring(`condition_target_code`, '
                                                                                  'length(`condition_target_code`) - '
                                                                                  "2, 2) NOT IN ('00', '10') OR "
                                                                                  '(substring(`condition_target_code`, '
                                                                                  'length(`condition_target_code`) - '
                                                                                  "2, 2) = '10' AND "
                                                                                  'length(`condition_target_code`) < '
                                                                                  '11))',
                                                                         'table': 'clinical_condition'},
 'gold.clinical.condition.encounter_slice_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                'column': 'encounter_slice_id',
                                                                'companions': [],
                                                                'match': '`encounter_slice_id` = 0',
                                                                'table': 'clinical_condition'},
 'gold.clinical.condition.recorder_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'recorder_practitioner_id',
                                                                      'companions': [],
                                                                      'match': '`recorder_practitioner_id` = 0',
                                                                      'table': 'clinical_condition'},
 'gold.clinical.condition_revision.severity_code.insr_f7_zero_code': {'action': 'normalize',
                                                                      'column': 'severity_code',
                                                                      'companions': [],
                                                                      'match': "TRIM(`severity_code`) = '0' AND "
                                                                               '`severity_display` IS NULL',
                                                                      'table': 'clinical_condition_revision'},
 'gold.clinical.condition_revision.verification_status_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                 'column': 'verification_status_code',
                                                                                 'companions': [],
                                                                                 'match': 'TRIM(`verification_status_code`) '
                                                                                          "= '0' AND "
                                                                                          '`verification_status_display` '
                                                                                          'IS NULL',
                                                                                 'table': 'clinical_condition_revision'},
 'gold.clinical.condition_stage.stage_diagnosis_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                     'column': 'stage_diagnosis_snomed_code',
                                                                                     'companions': ['stage_diagnosis_snomed_display',
                                                                                                    'stage_diagnosis_target_code',
                                                                                                    'stage_diagnosis_target_display'],
                                                                                     'match': '`stage_diagnosis_snomed_code` '
                                                                                              'IS NOT NULL AND (NOT '
                                                                                              '`stage_diagnosis_snomed_code` '
                                                                                              'RLIKE '
                                                                                              "'^[1-9][0-9]{5,17}$' "
                                                                                              'OR '
                                                                                              'substring(`stage_diagnosis_snomed_code`, '
                                                                                              'length(`stage_diagnosis_snomed_code`) '
                                                                                              "- 2, 2) NOT IN ('00', "
                                                                                              "'10') OR "
                                                                                              '(substring(`stage_diagnosis_snomed_code`, '
                                                                                              'length(`stage_diagnosis_snomed_code`) '
                                                                                              "- 2, 2) = '10' AND "
                                                                                              'length(`stage_diagnosis_snomed_code`) '
                                                                                              '< 11))',
                                                                                     'table': 'clinical_condition_stage'},
 'gold.clinical.condition_stage.stage_diagnosis_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                     'column': 'stage_diagnosis_target_code',
                                                                                     'companions': ['stage_diagnosis_target_display',
                                                                                                    'stage_diagnosis_snomed_code',
                                                                                                    'stage_diagnosis_snomed_display'],
                                                                                     'match': '`stage_diagnosis_target_system` '
                                                                                              '= '
                                                                                              "'http://snomed.info/sct' "
                                                                                              'AND '
                                                                                              '`stage_diagnosis_target_code` '
                                                                                              'IS NOT NULL AND (NOT '
                                                                                              '`stage_diagnosis_target_code` '
                                                                                              'RLIKE '
                                                                                              "'^[1-9][0-9]{5,17}$' "
                                                                                              'OR '
                                                                                              'substring(`stage_diagnosis_target_code`, '
                                                                                              'length(`stage_diagnosis_target_code`) '
                                                                                              "- 2, 2) NOT IN ('00', "
                                                                                              "'10') OR "
                                                                                              '(substring(`stage_diagnosis_target_code`, '
                                                                                              'length(`stage_diagnosis_target_code`) '
                                                                                              "- 2, 2) = '10' AND "
                                                                                              'length(`stage_diagnosis_target_code`) '
                                                                                              '< 11))',
                                                                                     'table': 'clinical_condition_stage'},
 'gold.clinical.condition_stage.stage_group_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'stage_group_snomed_code',
                                                                                 'companions': ['stage_group_snomed_display',
                                                                                                'stage_group_target_code',
                                                                                                'stage_group_target_display'],
                                                                                 'match': '`stage_group_snomed_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`stage_group_snomed_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`stage_group_snomed_code`, '
                                                                                          'length(`stage_group_snomed_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`stage_group_snomed_code`, '
                                                                                          'length(`stage_group_snomed_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`stage_group_snomed_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_condition_stage'},
 'gold.clinical.condition_stage.stage_group_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'stage_group_target_code',
                                                                                 'companions': ['stage_group_target_display',
                                                                                                'stage_group_snomed_code',
                                                                                                'stage_group_snomed_display'],
                                                                                 'match': '`stage_group_target_system` '
                                                                                          '= '
                                                                                          "'http://snomed.info/sct' "
                                                                                          'AND '
                                                                                          '`stage_group_target_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`stage_group_target_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`stage_group_target_code`, '
                                                                                          'length(`stage_group_target_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`stage_group_target_code`, '
                                                                                          'length(`stage_group_target_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`stage_group_target_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_condition_stage'},
 'gold.clinical.device.device_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                   'column': 'device_snomed_code',
                                                                   'companions': ['device_snomed_display',
                                                                                  'device_target_code',
                                                                                  'device_target_display'],
                                                                   'match': '`device_snomed_code` IS NOT NULL AND '
                                                                            '(NOT `device_snomed_code` RLIKE '
                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                            'substring(`device_snomed_code`, '
                                                                            'length(`device_snomed_code`) - 2, 2) '
                                                                            "NOT IN ('00', '10') OR "
                                                                            '(substring(`device_snomed_code`, '
                                                                            'length(`device_snomed_code`) - 2, 2) = '
                                                                            "'10' AND length(`device_snomed_code`) < "
                                                                            '11))',
                                                                   'table': 'clinical_device'},
 'gold.clinical.device.device_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                   'column': 'device_target_code',
                                                                   'companions': ['device_target_display',
                                                                                  'device_snomed_code',
                                                                                  'device_snomed_display'],
                                                                   'match': '`device_target_system` = '
                                                                            "'http://snomed.info/sct' AND "
                                                                            '`device_target_code` IS NOT NULL AND '
                                                                            '(NOT `device_target_code` RLIKE '
                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                            'substring(`device_target_code`, '
                                                                            'length(`device_target_code`) - 2, 2) '
                                                                            "NOT IN ('00', '10') OR "
                                                                            '(substring(`device_target_code`, '
                                                                            'length(`device_target_code`) - 2, 2) = '
                                                                            "'10' AND length(`device_target_code`) < "
                                                                            '11))',
                                                                   'table': 'clinical_device'},
 'gold.clinical.endoscopy_finding.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                  'column': 'encounter_id',
                                                                  'companions': [],
                                                                  'match': '`encounter_id` = 0',
                                                                  'table': 'clinical_endoscopy_finding'},
 'gold.clinical.endoscopy_finding.finding_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'finding_snomed_code',
                                                                               'companions': ['finding_snomed_display',
                                                                                              'finding_target_code',
                                                                                              'finding_target_display'],
                                                                               'match': '`finding_snomed_code` IS '
                                                                                        'NOT NULL AND (NOT '
                                                                                        '`finding_snomed_code` RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' OR "
                                                                                        'substring(`finding_snomed_code`, '
                                                                                        'length(`finding_snomed_code`) '
                                                                                        "- 2, 2) NOT IN ('00', '10') "
                                                                                        'OR '
                                                                                        '(substring(`finding_snomed_code`, '
                                                                                        'length(`finding_snomed_code`) '
                                                                                        "- 2, 2) = '10' AND "
                                                                                        'length(`finding_snomed_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_endoscopy_finding'},
 'gold.clinical.endoscopy_finding.finding_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'finding_target_code',
                                                                               'companions': ['finding_target_display',
                                                                                              'finding_snomed_code',
                                                                                              'finding_snomed_display'],
                                                                               'match': '`finding_target_system` = '
                                                                                        "'http://snomed.info/sct' "
                                                                                        'AND `finding_target_code` '
                                                                                        'IS NOT NULL AND (NOT '
                                                                                        '`finding_target_code` RLIKE '
                                                                                        "'^[1-9][0-9]{5,17}$' OR "
                                                                                        'substring(`finding_target_code`, '
                                                                                        'length(`finding_target_code`) '
                                                                                        "- 2, 2) NOT IN ('00', '10') "
                                                                                        'OR '
                                                                                        '(substring(`finding_target_code`, '
                                                                                        'length(`finding_target_code`) '
                                                                                        "- 2, 2) = '10' AND "
                                                                                        'length(`finding_target_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_endoscopy_finding'},
 'gold.clinical.family_history.condition_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'condition_snomed_code',
                                                                              'companions': ['condition_snomed_display',
                                                                                             'condition_target_code',
                                                                                             'condition_target_display'],
                                                                              'match': '`condition_snomed_code` IS '
                                                                                       'NOT NULL AND (NOT '
                                                                                       '`condition_snomed_code` '
                                                                                       "RLIKE '^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`condition_snomed_code`, '
                                                                                       'length(`condition_snomed_code`) '
                                                                                       "- 2, 2) NOT IN ('00', '10') "
                                                                                       'OR '
                                                                                       '(substring(`condition_snomed_code`, '
                                                                                       'length(`condition_snomed_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`condition_snomed_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_family_history'},
 'gold.clinical.family_history.condition_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'condition_target_code',
                                                                              'companions': ['condition_target_display',
                                                                                             'condition_snomed_code',
                                                                                             'condition_snomed_display'],
                                                                              'match': '`condition_target_system` = '
                                                                                       "'http://snomed.info/sct' AND "
                                                                                       '`condition_target_code` IS '
                                                                                       'NOT NULL AND (NOT '
                                                                                       '`condition_target_code` '
                                                                                       "RLIKE '^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`condition_target_code`, '
                                                                                       'length(`condition_target_code`) '
                                                                                       "- 2, 2) NOT IN ('00', '10') "
                                                                                       'OR '
                                                                                       '(substring(`condition_target_code`, '
                                                                                       'length(`condition_target_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`condition_target_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_family_history'},
 'gold.clinical.family_history.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                               'column': 'encounter_id',
                                                               'companions': [],
                                                               'match': '`encounter_id` = 0',
                                                               'table': 'clinical_family_history'},
 'gold.clinical.family_history.onset_age.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                      'column': 'onset_age',
                                                                      'companions': [],
                                                                      'match': '`onset_age` = 0 AND `onset_age_unit` '
                                                                               'IS NULL',
                                                                      'table': 'clinical_family_history'},
 'gold.clinical.form_response.answer_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'answer_snomed_code',
                                                                          'companions': ['answer_snomed_display',
                                                                                         'answer_target_code',
                                                                                         'answer_target_display'],
                                                                          'match': '`answer_snomed_code` IS NOT NULL '
                                                                                   'AND (NOT `answer_snomed_code` '
                                                                                   "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`answer_snomed_code`, '
                                                                                   'length(`answer_snomed_code`) - '
                                                                                   "2, 2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`answer_snomed_code`, '
                                                                                   'length(`answer_snomed_code`) - '
                                                                                   "2, 2) = '10' AND "
                                                                                   'length(`answer_snomed_code`) < '
                                                                                   '11))',
                                                                          'table': 'clinical_form_response'},
 'gold.clinical.form_response.answer_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'answer_target_code',
                                                                          'companions': ['answer_target_display',
                                                                                         'answer_snomed_code',
                                                                                         'answer_snomed_display'],
                                                                          'match': '`answer_target_system` = '
                                                                                   "'http://snomed.info/sct' AND "
                                                                                   '`answer_target_code` IS NOT NULL '
                                                                                   'AND (NOT `answer_target_code` '
                                                                                   "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`answer_target_code`, '
                                                                                   'length(`answer_target_code`) - '
                                                                                   "2, 2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`answer_target_code`, '
                                                                                   'length(`answer_target_code`) - '
                                                                                   "2, 2) = '10' AND "
                                                                                   'length(`answer_target_code`) < '
                                                                                   '11))',
                                                                          'table': 'clinical_form_response'},
 'gold.clinical.form_response.question_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'question_snomed_code',
                                                                            'companions': ['question_snomed_display',
                                                                                           'question_target_code',
                                                                                           'question_target_display'],
                                                                            'match': '`question_snomed_code` IS NOT '
                                                                                     'NULL AND (NOT '
                                                                                     '`question_snomed_code` RLIKE '
                                                                                     "'^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`question_snomed_code`, '
                                                                                     'length(`question_snomed_code`) '
                                                                                     "- 2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`question_snomed_code`, '
                                                                                     'length(`question_snomed_code`) '
                                                                                     "- 2, 2) = '10' AND "
                                                                                     'length(`question_snomed_code`) '
                                                                                     '< 11))',
                                                                            'table': 'clinical_form_response'},
 'gold.clinical.form_response.question_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'question_target_code',
                                                                            'companions': ['question_target_display',
                                                                                           'question_snomed_code',
                                                                                           'question_snomed_display'],
                                                                            'match': '`question_target_system` = '
                                                                                     "'http://snomed.info/sct' AND "
                                                                                     '`question_target_code` IS NOT '
                                                                                     'NULL AND (NOT '
                                                                                     '`question_target_code` RLIKE '
                                                                                     "'^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`question_target_code`, '
                                                                                     'length(`question_target_code`) '
                                                                                     "- 2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`question_target_code`, '
                                                                                     'length(`question_target_code`) '
                                                                                     "- 2, 2) = '10' AND "
                                                                                     'length(`question_target_code`) '
                                                                                     '< 11))',
                                                                            'table': 'clinical_form_response'},
 'gold.clinical.form_response.unit_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                        'column': 'unit_snomed_code',
                                                                        'companions': ['unit_snomed_display',
                                                                                       'unit_target_code',
                                                                                       'unit_target_display'],
                                                                        'match': '`unit_snomed_code` IS NOT NULL AND '
                                                                                 '(NOT `unit_snomed_code` RLIKE '
                                                                                 "'^[1-9][0-9]{5,17}$' OR "
                                                                                 'substring(`unit_snomed_code`, '
                                                                                 'length(`unit_snomed_code`) - 2, 2) '
                                                                                 "NOT IN ('00', '10') OR "
                                                                                 '(substring(`unit_snomed_code`, '
                                                                                 'length(`unit_snomed_code`) - 2, 2) '
                                                                                 "= '10' AND "
                                                                                 'length(`unit_snomed_code`) < 11))',
                                                                        'table': 'clinical_form_response'},
 'gold.clinical.form_response.unit_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                        'column': 'unit_target_code',
                                                                        'companions': ['unit_target_display',
                                                                                       'unit_snomed_code',
                                                                                       'unit_snomed_display'],
                                                                        'match': '`unit_target_system` = '
                                                                                 "'http://snomed.info/sct' AND "
                                                                                 '`unit_target_code` IS NOT NULL AND '
                                                                                 '(NOT `unit_target_code` RLIKE '
                                                                                 "'^[1-9][0-9]{5,17}$' OR "
                                                                                 'substring(`unit_target_code`, '
                                                                                 'length(`unit_target_code`) - 2, 2) '
                                                                                 "NOT IN ('00', '10') OR "
                                                                                 '(substring(`unit_target_code`, '
                                                                                 'length(`unit_target_code`) - 2, 2) '
                                                                                 "= '10' AND "
                                                                                 'length(`unit_target_code`) < 11))',
                                                                        'table': 'clinical_form_response'},
 'gold.clinical.form_smoking_status.cessation_advice_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                          'column': 'cessation_advice_snomed_code',
                                                                                          'companions': ['cessation_advice_snomed_display',
                                                                                                         'cessation_advice_target_code',
                                                                                                         'cessation_advice_target_display'],
                                                                                          'match': '`cessation_advice_snomed_code` '
                                                                                                   'IS NOT NULL AND '
                                                                                                   '(NOT '
                                                                                                   '`cessation_advice_snomed_code` '
                                                                                                   'RLIKE '
                                                                                                   "'^[1-9][0-9]{5,17}$' "
                                                                                                   'OR '
                                                                                                   'substring(`cessation_advice_snomed_code`, '
                                                                                                   'length(`cessation_advice_snomed_code`) '
                                                                                                   '- 2, 2) NOT IN '
                                                                                                   "('00', '10') OR "
                                                                                                   '(substring(`cessation_advice_snomed_code`, '
                                                                                                   'length(`cessation_advice_snomed_code`) '
                                                                                                   "- 2, 2) = '10' "
                                                                                                   'AND '
                                                                                                   'length(`cessation_advice_snomed_code`) '
                                                                                                   '< 11))',
                                                                                          'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.cessation_advice_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                          'column': 'cessation_advice_target_code',
                                                                                          'companions': ['cessation_advice_target_display',
                                                                                                         'cessation_advice_snomed_code',
                                                                                                         'cessation_advice_snomed_display'],
                                                                                          'match': '`cessation_advice_target_system` '
                                                                                                   '= '
                                                                                                   "'http://snomed.info/sct' "
                                                                                                   'AND '
                                                                                                   '`cessation_advice_target_code` '
                                                                                                   'IS NOT NULL AND '
                                                                                                   '(NOT '
                                                                                                   '`cessation_advice_target_code` '
                                                                                                   'RLIKE '
                                                                                                   "'^[1-9][0-9]{5,17}$' "
                                                                                                   'OR '
                                                                                                   'substring(`cessation_advice_target_code`, '
                                                                                                   'length(`cessation_advice_target_code`) '
                                                                                                   '- 2, 2) NOT IN '
                                                                                                   "('00', '10') OR "
                                                                                                   '(substring(`cessation_advice_target_code`, '
                                                                                                   'length(`cessation_advice_target_code`) '
                                                                                                   "- 2, 2) = '10' "
                                                                                                   'AND '
                                                                                                   'length(`cessation_advice_target_code`) '
                                                                                                   '< 11))',
                                                                                          'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.cessation_referral_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'cessation_referral_snomed_code',
                                                                                            'companions': ['cessation_referral_snomed_display',
                                                                                                           'cessation_referral_target_code',
                                                                                                           'cessation_referral_target_display'],
                                                                                            'match': '`cessation_referral_snomed_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`cessation_referral_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`cessation_referral_snomed_code`, '
                                                                                                     'length(`cessation_referral_snomed_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`cessation_referral_snomed_code`, '
                                                                                                     'length(`cessation_referral_snomed_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`cessation_referral_snomed_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.cessation_referral_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'cessation_referral_target_code',
                                                                                            'companions': ['cessation_referral_target_display',
                                                                                                           'cessation_referral_snomed_code',
                                                                                                           'cessation_referral_snomed_display'],
                                                                                            'match': '`cessation_referral_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`cessation_referral_target_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`cessation_referral_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`cessation_referral_target_code`, '
                                                                                                     'length(`cessation_referral_target_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`cessation_referral_target_code`, '
                                                                                                     'length(`cessation_referral_target_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`cessation_referral_target_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.dependence_level_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                          'column': 'dependence_level_snomed_code',
                                                                                          'companions': ['dependence_level_snomed_display',
                                                                                                         'dependence_level_target_code',
                                                                                                         'dependence_level_target_display'],
                                                                                          'match': '`dependence_level_snomed_code` '
                                                                                                   'IS NOT NULL AND '
                                                                                                   '(NOT '
                                                                                                   '`dependence_level_snomed_code` '
                                                                                                   'RLIKE '
                                                                                                   "'^[1-9][0-9]{5,17}$' "
                                                                                                   'OR '
                                                                                                   'substring(`dependence_level_snomed_code`, '
                                                                                                   'length(`dependence_level_snomed_code`) '
                                                                                                   '- 2, 2) NOT IN '
                                                                                                   "('00', '10') OR "
                                                                                                   '(substring(`dependence_level_snomed_code`, '
                                                                                                   'length(`dependence_level_snomed_code`) '
                                                                                                   "- 2, 2) = '10' "
                                                                                                   'AND '
                                                                                                   'length(`dependence_level_snomed_code`) '
                                                                                                   '< 11))',
                                                                                          'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.dependence_level_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                          'column': 'dependence_level_target_code',
                                                                                          'companions': ['dependence_level_target_display',
                                                                                                         'dependence_level_snomed_code',
                                                                                                         'dependence_level_snomed_display'],
                                                                                          'match': '`dependence_level_target_system` '
                                                                                                   '= '
                                                                                                   "'http://snomed.info/sct' "
                                                                                                   'AND '
                                                                                                   '`dependence_level_target_code` '
                                                                                                   'IS NOT NULL AND '
                                                                                                   '(NOT '
                                                                                                   '`dependence_level_target_code` '
                                                                                                   'RLIKE '
                                                                                                   "'^[1-9][0-9]{5,17}$' "
                                                                                                   'OR '
                                                                                                   'substring(`dependence_level_target_code`, '
                                                                                                   'length(`dependence_level_target_code`) '
                                                                                                   '- 2, 2) NOT IN '
                                                                                                   "('00', '10') OR "
                                                                                                   '(substring(`dependence_level_target_code`, '
                                                                                                   'length(`dependence_level_target_code`) '
                                                                                                   "- 2, 2) = '10' "
                                                                                                   'AND '
                                                                                                   'length(`dependence_level_target_code`) '
                                                                                                   '< 11))',
                                                                                          'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.nicotine_treatment_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'nicotine_treatment_snomed_code',
                                                                                            'companions': ['nicotine_treatment_snomed_display',
                                                                                                           'nicotine_treatment_target_code',
                                                                                                           'nicotine_treatment_target_display'],
                                                                                            'match': '`nicotine_treatment_snomed_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`nicotine_treatment_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`nicotine_treatment_snomed_code`, '
                                                                                                     'length(`nicotine_treatment_snomed_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`nicotine_treatment_snomed_code`, '
                                                                                                     'length(`nicotine_treatment_snomed_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`nicotine_treatment_snomed_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.nicotine_treatment_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'nicotine_treatment_target_code',
                                                                                            'companions': ['nicotine_treatment_target_display',
                                                                                                           'nicotine_treatment_snomed_code',
                                                                                                           'nicotine_treatment_snomed_display'],
                                                                                            'match': '`nicotine_treatment_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`nicotine_treatment_target_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`nicotine_treatment_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`nicotine_treatment_target_code`, '
                                                                                                     'length(`nicotine_treatment_target_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`nicotine_treatment_target_code`, '
                                                                                                     'length(`nicotine_treatment_target_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`nicotine_treatment_target_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.smoking_status_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                        'column': 'smoking_status_snomed_code',
                                                                                        'companions': ['smoking_status_snomed_display',
                                                                                                       'smoking_status_target_code',
                                                                                                       'smoking_status_target_display'],
                                                                                        'match': '`smoking_status_snomed_code` '
                                                                                                 'IS NOT NULL AND '
                                                                                                 '(NOT '
                                                                                                 '`smoking_status_snomed_code` '
                                                                                                 'RLIKE '
                                                                                                 "'^[1-9][0-9]{5,17}$' "
                                                                                                 'OR '
                                                                                                 'substring(`smoking_status_snomed_code`, '
                                                                                                 'length(`smoking_status_snomed_code`) '
                                                                                                 '- 2, 2) NOT IN '
                                                                                                 "('00', '10') OR "
                                                                                                 '(substring(`smoking_status_snomed_code`, '
                                                                                                 'length(`smoking_status_snomed_code`) '
                                                                                                 "- 2, 2) = '10' AND "
                                                                                                 'length(`smoking_status_snomed_code`) '
                                                                                                 '< 11))',
                                                                                        'table': 'clinical_form_smoking_status'},
 'gold.clinical.form_smoking_status.smoking_status_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                        'column': 'smoking_status_target_code',
                                                                                        'companions': ['smoking_status_target_display',
                                                                                                       'smoking_status_snomed_code',
                                                                                                       'smoking_status_snomed_display'],
                                                                                        'match': '`smoking_status_target_system` '
                                                                                                 '= '
                                                                                                 "'http://snomed.info/sct' "
                                                                                                 'AND '
                                                                                                 '`smoking_status_target_code` '
                                                                                                 'IS NOT NULL AND '
                                                                                                 '(NOT '
                                                                                                 '`smoking_status_target_code` '
                                                                                                 'RLIKE '
                                                                                                 "'^[1-9][0-9]{5,17}$' "
                                                                                                 'OR '
                                                                                                 'substring(`smoking_status_target_code`, '
                                                                                                 'length(`smoking_status_target_code`) '
                                                                                                 '- 2, 2) NOT IN '
                                                                                                 "('00', '10') OR "
                                                                                                 '(substring(`smoking_status_target_code`, '
                                                                                                 'length(`smoking_status_target_code`) '
                                                                                                 "- 2, 2) = '10' AND "
                                                                                                 'length(`smoking_status_target_code`) '
                                                                                                 '< 11))',
                                                                                        'table': 'clinical_form_smoking_status'},
 'gold.clinical.genomic_test.indication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                             'column': 'indication_snomed_code',
                                                                             'companions': [],
                                                                             'match': '`indication_snomed_code` IS '
                                                                                      'NOT NULL AND (NOT '
                                                                                      '`indication_snomed_code` '
                                                                                      "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                      'substring(`indication_snomed_code`, '
                                                                                      'length(`indication_snomed_code`) '
                                                                                      "- 2, 2) NOT IN ('00', '10') "
                                                                                      'OR '
                                                                                      '(substring(`indication_snomed_code`, '
                                                                                      'length(`indication_snomed_code`) '
                                                                                      "- 2, 2) = '10' AND "
                                                                                      'length(`indication_snomed_code`) '
                                                                                      '< 11))',
                                                                             'table': 'clinical_genomic_test'},
 'gold.clinical.genomic_test.sample_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                              'column': 'sample_site_snomed_code',
                                                                              'companions': [],
                                                                              'match': '`sample_site_snomed_code` IS '
                                                                                       'NOT NULL AND (NOT '
                                                                                       '`sample_site_snomed_code` '
                                                                                       "RLIKE '^[1-9][0-9]{5,17}$' "
                                                                                       'OR '
                                                                                       'substring(`sample_site_snomed_code`, '
                                                                                       'length(`sample_site_snomed_code`) '
                                                                                       "- 2, 2) NOT IN ('00', '10') "
                                                                                       'OR '
                                                                                       '(substring(`sample_site_snomed_code`, '
                                                                                       'length(`sample_site_snomed_code`) '
                                                                                       "- 2, 2) = '10' AND "
                                                                                       'length(`sample_site_snomed_code`) '
                                                                                       '< 11))',
                                                                              'table': 'clinical_genomic_test'},
 'gold.clinical.genomic_test.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                       'column': 'test_snomed_code',
                                                                       'companions': [],
                                                                       'match': '`test_snomed_code` IS NOT NULL AND '
                                                                                '(NOT `test_snomed_code` RLIKE '
                                                                                "'^[1-9][0-9]{5,17}$' OR "
                                                                                'substring(`test_snomed_code`, '
                                                                                'length(`test_snomed_code`) - 2, 2) '
                                                                                "NOT IN ('00', '10') OR "
                                                                                '(substring(`test_snomed_code`, '
                                                                                'length(`test_snomed_code`) - 2, 2) '
                                                                                "= '10' AND "
                                                                                'length(`test_snomed_code`) < 11))',
                                                                       'table': 'clinical_genomic_test'},
 'gold.clinical.imaging_exam.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'body_site_snomed_code',
                                                                            'companions': ['body_site_snomed_display'],
                                                                            'match': '`body_site_snomed_code` IS NOT '
                                                                                     'NULL AND (NOT '
                                                                                     '`body_site_snomed_code` RLIKE '
                                                                                     "'^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`body_site_snomed_code`, '
                                                                                     'length(`body_site_snomed_code`) '
                                                                                     "- 2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`body_site_snomed_code`, '
                                                                                     'length(`body_site_snomed_code`) '
                                                                                     "- 2, 2) = '10' AND "
                                                                                     'length(`body_site_snomed_code`) '
                                                                                     '< 11))',
                                                                            'table': 'clinical_imaging_exam'},
 'gold.clinical.imaging_exam.exam_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                       'column': 'exam_snomed_code',
                                                                       'companions': ['exam_snomed_display',
                                                                                      'exam_target_code',
                                                                                      'exam_target_display'],
                                                                       'match': '`exam_snomed_code` IS NOT NULL AND '
                                                                                '(NOT `exam_snomed_code` RLIKE '
                                                                                "'^[1-9][0-9]{5,17}$' OR "
                                                                                'substring(`exam_snomed_code`, '
                                                                                'length(`exam_snomed_code`) - 2, 2) '
                                                                                "NOT IN ('00', '10') OR "
                                                                                '(substring(`exam_snomed_code`, '
                                                                                'length(`exam_snomed_code`) - 2, 2) '
                                                                                "= '10' AND "
                                                                                'length(`exam_snomed_code`) < 11))',
                                                                       'table': 'clinical_imaging_exam'},
 'gold.clinical.imaging_exam.exam_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                       'column': 'exam_target_code',
                                                                       'companions': ['exam_target_display',
                                                                                      'exam_snomed_code',
                                                                                      'exam_snomed_display'],
                                                                       'match': '`exam_target_system` = '
                                                                                "'http://snomed.info/sct' AND "
                                                                                '`exam_target_code` IS NOT NULL AND '
                                                                                '(NOT `exam_target_code` RLIKE '
                                                                                "'^[1-9][0-9]{5,17}$' OR "
                                                                                'substring(`exam_target_code`, '
                                                                                'length(`exam_target_code`) - 2, 2) '
                                                                                "NOT IN ('00', '10') OR "
                                                                                '(substring(`exam_target_code`, '
                                                                                'length(`exam_target_code`) - 2, 2) '
                                                                                "= '10' AND "
                                                                                'length(`exam_target_code`) < 11))',
                                                                       'table': 'clinical_imaging_exam'},
 'gold.clinical.medication_admin.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'medication_snomed_code',
                                                                                 'companions': ['medication_snomed_display',
                                                                                                'medication_target_code',
                                                                                                'medication_target_display'],
                                                                                 'match': '`medication_snomed_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`medication_snomed_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`medication_snomed_code`, '
                                                                                          'length(`medication_snomed_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`medication_snomed_code`, '
                                                                                          'length(`medication_snomed_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`medication_snomed_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_medication_admin'},
 'gold.clinical.medication_admin.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'medication_target_code',
                                                                                 'companions': ['medication_target_display',
                                                                                                'medication_snomed_code',
                                                                                                'medication_snomed_display'],
                                                                                 'match': '`medication_target_system` '
                                                                                          '= '
                                                                                          "'http://snomed.info/sct' "
                                                                                          'AND '
                                                                                          '`medication_target_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`medication_target_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`medication_target_code`, '
                                                                                          'length(`medication_target_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`medication_target_code`, '
                                                                                          'length(`medication_target_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`medication_target_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_medication_admin'},
 'gold.clinical.medication_admin.verifier_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                             'column': 'verifier_practitioner_id',
                                                                             'companions': [],
                                                                             'match': '`verifier_practitioner_id` = '
                                                                                      '0',
                                                                             'table': 'clinical_medication_admin'},
 'gold.clinical.medication_admin_ingredient.ingredient_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'ingredient_snomed_code',
                                                                                            'companions': ['ingredient_snomed_display',
                                                                                                           'ingredient_target_code',
                                                                                                           'ingredient_target_display'],
                                                                                            'match': '`ingredient_snomed_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`ingredient_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`ingredient_snomed_code`, '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`ingredient_snomed_code`, '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_medication_admin_ingredient'},
 'gold.clinical.medication_admin_ingredient.ingredient_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'ingredient_target_code',
                                                                                            'companions': ['ingredient_target_display',
                                                                                                           'ingredient_snomed_code',
                                                                                                           'ingredient_snomed_display'],
                                                                                            'match': '`ingredient_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`ingredient_target_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`ingredient_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`ingredient_target_code`, '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`ingredient_target_code`, '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_medication_admin_ingredient'},
 'gold.clinical.medication_admin_status.status_code.insr_f7_zero_code': {'action': 'normalize',
                                                                         'column': 'status_code',
                                                                         'companions': [],
                                                                         'match': "TRIM(`status_code`) = '0' AND "
                                                                                  '`status_display` IS NULL',
                                                                         'table': 'clinical_medication_admin_status'},
 'gold.clinical.medication_dispense.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'medication_snomed_code',
                                                                                    'companions': ['medication_snomed_display',
                                                                                                   'medication_target_code',
                                                                                                   'medication_target_display'],
                                                                                    'match': '`medication_snomed_code` '
                                                                                             'IS NOT NULL AND (NOT '
                                                                                             '`medication_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`medication_snomed_code`, '
                                                                                             'length(`medication_snomed_code`) '
                                                                                             "- 2, 2) NOT IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`medication_snomed_code`, '
                                                                                             'length(`medication_snomed_code`) '
                                                                                             "- 2, 2) = '10' AND "
                                                                                             'length(`medication_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_medication_dispense'},
 'gold.clinical.medication_dispense.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'medication_target_code',
                                                                                    'companions': ['medication_target_display',
                                                                                                   'medication_snomed_code',
                                                                                                   'medication_snomed_display'],
                                                                                    'match': '`medication_target_system` '
                                                                                             '= '
                                                                                             "'http://snomed.info/sct' "
                                                                                             'AND '
                                                                                             '`medication_target_code` '
                                                                                             'IS NOT NULL AND (NOT '
                                                                                             '`medication_target_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`medication_target_code`, '
                                                                                             'length(`medication_target_code`) '
                                                                                             "- 2, 2) NOT IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`medication_target_code`, '
                                                                                             'length(`medication_target_code`) '
                                                                                             "- 2, 2) = '10' AND "
                                                                                             'length(`medication_target_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_medication_dispense'},
 'gold.clinical.medication_order.frequency_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                 'column': 'frequency_id',
                                                                 'companions': [],
                                                                 'match': "TRIM(`frequency_id`) = '0'",
                                                                 'table': 'clinical_medication_order'},
 'gold.clinical.medication_order.medication_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'medication_snomed_code',
                                                                                 'companions': ['medication_snomed_display',
                                                                                                'medication_target_code',
                                                                                                'medication_target_display'],
                                                                                 'match': '`medication_snomed_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`medication_snomed_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`medication_snomed_code`, '
                                                                                          'length(`medication_snomed_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`medication_snomed_code`, '
                                                                                          'length(`medication_snomed_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`medication_snomed_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_medication_order'},
 'gold.clinical.medication_order.medication_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                 'column': 'medication_target_code',
                                                                                 'companions': ['medication_target_display',
                                                                                                'medication_snomed_code',
                                                                                                'medication_snomed_display'],
                                                                                 'match': '`medication_target_system` '
                                                                                          '= '
                                                                                          "'http://snomed.info/sct' "
                                                                                          'AND '
                                                                                          '`medication_target_code` '
                                                                                          'IS NOT NULL AND (NOT '
                                                                                          '`medication_target_code` '
                                                                                          'RLIKE '
                                                                                          "'^[1-9][0-9]{5,17}$' OR "
                                                                                          'substring(`medication_target_code`, '
                                                                                          'length(`medication_target_code`) '
                                                                                          "- 2, 2) NOT IN ('00', "
                                                                                          "'10') OR "
                                                                                          '(substring(`medication_target_code`, '
                                                                                          'length(`medication_target_code`) '
                                                                                          "- 2, 2) = '10' AND "
                                                                                          'length(`medication_target_code`) '
                                                                                          '< 11))',
                                                                                 'table': 'clinical_medication_order'},
 'gold.clinical.medication_order.requester_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'requester_practitioner_id',
                                                                              'companions': [],
                                                                              'match': '`requester_practitioner_id` '
                                                                                       '= 0',
                                                                              'table': 'clinical_medication_order'},
 'gold.clinical.medication_order_ingredient.dose_value.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                                    'column': 'dose_value',
                                                                                    'companions': [],
                                                                                    'match': '`dose_value` = 0 AND '
                                                                                             '`dose_unit` IS NULL',
                                                                                    'table': 'clinical_medication_order_ingredient'},
 'gold.clinical.medication_order_ingredient.ingredient_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'ingredient_snomed_code',
                                                                                            'companions': ['ingredient_snomed_display',
                                                                                                           'ingredient_target_code',
                                                                                                           'ingredient_target_display'],
                                                                                            'match': '`ingredient_snomed_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`ingredient_snomed_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`ingredient_snomed_code`, '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`ingredient_snomed_code`, '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`ingredient_snomed_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_medication_order_ingredient'},
 'gold.clinical.medication_order_ingredient.ingredient_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                            'column': 'ingredient_target_code',
                                                                                            'companions': ['ingredient_target_display',
                                                                                                           'ingredient_snomed_code',
                                                                                                           'ingredient_snomed_display'],
                                                                                            'match': '`ingredient_target_system` '
                                                                                                     '= '
                                                                                                     "'http://snomed.info/sct' "
                                                                                                     'AND '
                                                                                                     '`ingredient_target_code` '
                                                                                                     'IS NOT NULL '
                                                                                                     'AND (NOT '
                                                                                                     '`ingredient_target_code` '
                                                                                                     'RLIKE '
                                                                                                     "'^[1-9][0-9]{5,17}$' "
                                                                                                     'OR '
                                                                                                     'substring(`ingredient_target_code`, '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     '- 2, 2) NOT IN '
                                                                                                     "('00', '10') "
                                                                                                     'OR '
                                                                                                     '(substring(`ingredient_target_code`, '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     "- 2, 2) = '10' "
                                                                                                     'AND '
                                                                                                     'length(`ingredient_target_code`) '
                                                                                                     '< 11))',
                                                                                            'table': 'clinical_medication_order_ingredient'},
 'gold.clinical.medication_order_ingredient.rate_value.insr_f8_zero_without_unit': {'action': 'invalidate',
                                                                                    'column': 'rate_value',
                                                                                    'companions': [],
                                                                                    'match': '`rate_value` = 0 AND '
                                                                                             '`rate_unit` IS NULL',
                                                                                    'table': 'clinical_medication_order_ingredient'},
 'gold.clinical.medication_order_status.action_qualifier_code.insr_f7_zero_code': {'action': 'normalize',
                                                                                   'column': 'action_qualifier_code',
                                                                                   'companions': [],
                                                                                   'match': 'TRIM(`action_qualifier_code`) '
                                                                                            "= '0' AND "
                                                                                            '`action_qualifier_display` '
                                                                                            'IS NULL',
                                                                                   'table': 'clinical_medication_order_status'},
 'gold.clinical.microbiology_isolate.organism_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'organism_snomed_code',
                                                                                   'companions': [],
                                                                                   'match': '`organism_snomed_code` '
                                                                                            'IS NOT NULL AND (NOT '
                                                                                            '`organism_snomed_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                                            'substring(`organism_snomed_code`, '
                                                                                            'length(`organism_snomed_code`) '
                                                                                            "- 2, 2) NOT IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`organism_snomed_code`, '
                                                                                            'length(`organism_snomed_code`) '
                                                                                            "- 2, 2) = '10' AND "
                                                                                            'length(`organism_snomed_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_microbiology_isolate'},
 'gold.clinical.pathology_order.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'test_snomed_code',
                                                                          'companions': [],
                                                                          'match': '`test_snomed_code` IS NOT NULL '
                                                                                   'AND (NOT `test_snomed_code` '
                                                                                   "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`test_snomed_code`, '
                                                                                   'length(`test_snomed_code`) - 2, '
                                                                                   "2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`test_snomed_code`, '
                                                                                   'length(`test_snomed_code`) - 2, '
                                                                                   "2) = '10' AND "
                                                                                   'length(`test_snomed_code`) < '
                                                                                   '11))',
                                                                          'table': 'clinical_pathology_order'},
 'gold.clinical.pathology_result.result_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                             'column': 'result_snomed_code',
                                                                             'companions': ['result_snomed_display',
                                                                                            'result_target_code',
                                                                                            'result_target_display'],
                                                                             'match': '`result_snomed_code` IS NOT '
                                                                                      'NULL AND (NOT '
                                                                                      '`result_snomed_code` RLIKE '
                                                                                      "'^[1-9][0-9]{5,17}$' OR "
                                                                                      'substring(`result_snomed_code`, '
                                                                                      'length(`result_snomed_code`) '
                                                                                      "- 2, 2) NOT IN ('00', '10') "
                                                                                      'OR '
                                                                                      '(substring(`result_snomed_code`, '
                                                                                      'length(`result_snomed_code`) '
                                                                                      "- 2, 2) = '10' AND "
                                                                                      'length(`result_snomed_code`) '
                                                                                      '< 11))',
                                                                             'table': 'clinical_pathology_result'},
 'gold.clinical.pathology_result.result_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                             'column': 'result_target_code',
                                                                             'companions': ['result_target_display',
                                                                                            'result_snomed_code',
                                                                                            'result_snomed_display'],
                                                                             'match': '`result_target_system` = '
                                                                                      "'http://snomed.info/sct' AND "
                                                                                      '`result_target_code` IS NOT '
                                                                                      'NULL AND (NOT '
                                                                                      '`result_target_code` RLIKE '
                                                                                      "'^[1-9][0-9]{5,17}$' OR "
                                                                                      'substring(`result_target_code`, '
                                                                                      'length(`result_target_code`) '
                                                                                      "- 2, 2) NOT IN ('00', '10') "
                                                                                      'OR '
                                                                                      '(substring(`result_target_code`, '
                                                                                      'length(`result_target_code`) '
                                                                                      "- 2, 2) = '10' AND "
                                                                                      'length(`result_target_code`) '
                                                                                      '< 11))',
                                                                             'table': 'clinical_pathology_result'},
 'gold.clinical.pathology_result.test_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'test_snomed_code',
                                                                           'companions': ['test_snomed_display',
                                                                                          'test_target_code',
                                                                                          'test_target_display'],
                                                                           'match': '`test_snomed_code` IS NOT NULL '
                                                                                    'AND (NOT `test_snomed_code` '
                                                                                    "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                    'substring(`test_snomed_code`, '
                                                                                    'length(`test_snomed_code`) - 2, '
                                                                                    "2) NOT IN ('00', '10') OR "
                                                                                    '(substring(`test_snomed_code`, '
                                                                                    'length(`test_snomed_code`) - 2, '
                                                                                    "2) = '10' AND "
                                                                                    'length(`test_snomed_code`) < '
                                                                                    '11))',
                                                                           'table': 'clinical_pathology_result'},
 'gold.clinical.pathology_result.test_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                           'column': 'test_target_code',
                                                                           'companions': ['test_target_display',
                                                                                          'test_snomed_code',
                                                                                          'test_snomed_display'],
                                                                           'match': '`test_target_system` = '
                                                                                    "'http://snomed.info/sct' AND "
                                                                                    '`test_target_code` IS NOT NULL '
                                                                                    'AND (NOT `test_target_code` '
                                                                                    "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                    'substring(`test_target_code`, '
                                                                                    'length(`test_target_code`) - 2, '
                                                                                    "2) NOT IN ('00', '10') OR "
                                                                                    '(substring(`test_target_code`, '
                                                                                    'length(`test_target_code`) - 2, '
                                                                                    "2) = '10' AND "
                                                                                    'length(`test_target_code`) < '
                                                                                    '11))',
                                                                           'table': 'clinical_pathology_result'},
 'gold.clinical.pathology_result.ucum_code.insr_bbr_radiation_unit': {'action': 'invalidate',
                                                                      'column': 'ucum_code',
                                                                      'companions': ['unit_concept_id',
                                                                                     'canonical_ucum_code',
                                                                                     'value_canonical'],
                                                                      'match': "`ucum_code` IN ('Gy', 'cGy', 'mGy', "
                                                                               "'Sv', 'mSv')",
                                                                      'table': 'clinical_pathology_result'},
 'gold.clinical.pending_movement.accommodation_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                       'column': 'accommodation_cd',
                                                                       'companions': [],
                                                                       'match': '`accommodation_cd` = 0 AND '
                                                                                '`accommodation_display` IS NULL',
                                                                       'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.attenddoc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                 'column': 'attenddoc_id',
                                                                 'companions': [],
                                                                 'match': '`attenddoc_id` = 0',
                                                                 'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.disch_disposition_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'disch_disposition_cd',
                                                                           'companions': [],
                                                                           'match': '`disch_disposition_cd` = 0 AND '
                                                                                    '`disch_disposition_display` IS '
                                                                                    'NULL',
                                                                           'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.disch_to_loctn_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                        'column': 'disch_to_loctn_cd',
                                                                        'companions': [],
                                                                        'match': '`disch_to_loctn_cd` = 0 AND '
                                                                                 '`disch_to_loctn_display` IS NULL',
                                                                        'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.isolation_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                   'column': 'isolation_cd',
                                                                   'companions': [],
                                                                   'match': '`isolation_cd` = 0 AND '
                                                                            '`isolation_display` IS NULL',
                                                                   'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.med_service_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                     'column': 'med_service_cd',
                                                                     'companions': [],
                                                                     'match': '`med_service_cd` = 0 AND '
                                                                              '`med_service_display` IS NULL',
                                                                     'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pend_bed_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                  'column': 'pend_bed_cd',
                                                                  'companions': [],
                                                                  'match': '`pend_bed_cd` = 0 AND `pend_bed_display` '
                                                                           'IS NULL',
                                                                  'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pend_building_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                       'column': 'pend_building_cd',
                                                                       'companions': [],
                                                                       'match': '`pend_building_cd` = 0 AND '
                                                                                '`pend_building_display` IS NULL',
                                                                       'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pend_facility_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                       'column': 'pend_facility_cd',
                                                                       'companions': [],
                                                                       'match': '`pend_facility_cd` = 0 AND '
                                                                                '`pend_facility_display` IS NULL',
                                                                       'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pend_nurse_unit_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                         'column': 'pend_nurse_unit_cd',
                                                                         'companions': [],
                                                                         'match': '`pend_nurse_unit_cd` = 0 AND '
                                                                                  '`pend_nurse_unit_display` IS NULL',
                                                                         'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pend_room_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                   'column': 'pend_room_cd',
                                                                   'companions': [],
                                                                   'match': '`pend_room_cd` = 0 AND '
                                                                            '`pend_room_display` IS NULL',
                                                                   'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pending_priority_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'pending_priority_cd',
                                                                          'companions': [],
                                                                          'match': '`pending_priority_cd` = 0 AND '
                                                                                   '`pending_priority_display` IS '
                                                                                   'NULL',
                                                                          'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.pending_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                        'column': 'pending_status_cd',
                                                                        'companions': [],
                                                                        'match': '`pending_status_cd` = 0 AND '
                                                                                 '`pending_status_display` IS NULL',
                                                                        'table': 'clinical_pending_movement'},
 'gold.clinical.pending_movement.transaction_reason_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                            'column': 'transaction_reason_cd',
                                                                            'companions': [],
                                                                            'match': '`transaction_reason_cd` = 0 '
                                                                                     'AND '
                                                                                     '`transaction_reason_display` '
                                                                                     'IS NULL',
                                                                            'table': 'clinical_pending_movement'},
 'gold.clinical.prearrival.pcp_provider_id.insr_f7_zero_id': {'action': 'invalidate',
                                                              'column': 'pcp_provider_id',
                                                              'companions': [],
                                                              'match': '`pcp_provider_id` = 0',
                                                              'table': 'clinical_prearrival'},
 'gold.clinical.prearrival.prearrival_type_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                   'column': 'prearrival_type_cd',
                                                                   'companions': [],
                                                                   'match': '`prearrival_type_cd` = 0 AND '
                                                                            '`prearrival_type_display` IS NULL',
                                                                   'table': 'clinical_prearrival'},
 'gold.clinical.prearrival.referring_source_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                  'column': 'referring_source_id',
                                                                  'companions': [],
                                                                  'match': '`referring_source_id` = 0',
                                                                  'table': 'clinical_prearrival'},
 'gold.clinical.prearrival.reg_prsnl_id.insr_f7_zero_id': {'action': 'invalidate',
                                                           'column': 'reg_prsnl_id',
                                                           'companions': [],
                                                           'match': '`reg_prsnl_id` = 0',
                                                           'table': 'clinical_prearrival'},
 'gold.clinical.prearrival.sex_cd.insr_f7_zero_code': {'action': 'normalize',
                                                       'column': 'sex_cd',
                                                       'companions': [],
                                                       'match': '`sex_cd` = 0 AND `sex_display` IS NULL',
                                                       'table': 'clinical_prearrival'},
 'gold.clinical.presenting_complaint.complaint_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'complaint_snomed_code',
                                                                                    'companions': ['complaint_snomed_display',
                                                                                                   'complaint_target_code',
                                                                                                   'complaint_target_display'],
                                                                                    'match': '`complaint_snomed_code` '
                                                                                             'IS NOT NULL AND (NOT '
                                                                                             '`complaint_snomed_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`complaint_snomed_code`, '
                                                                                             'length(`complaint_snomed_code`) '
                                                                                             "- 2, 2) NOT IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`complaint_snomed_code`, '
                                                                                             'length(`complaint_snomed_code`) '
                                                                                             "- 2, 2) = '10' AND "
                                                                                             'length(`complaint_snomed_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_presenting_complaint'},
 'gold.clinical.presenting_complaint.complaint_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                    'column': 'complaint_target_code',
                                                                                    'companions': ['complaint_target_display',
                                                                                                   'complaint_snomed_code',
                                                                                                   'complaint_snomed_display'],
                                                                                    'match': '`complaint_target_system` '
                                                                                             '= '
                                                                                             "'http://snomed.info/sct' "
                                                                                             'AND '
                                                                                             '`complaint_target_code` '
                                                                                             'IS NOT NULL AND (NOT '
                                                                                             '`complaint_target_code` '
                                                                                             'RLIKE '
                                                                                             "'^[1-9][0-9]{5,17}$' "
                                                                                             'OR '
                                                                                             'substring(`complaint_target_code`, '
                                                                                             'length(`complaint_target_code`) '
                                                                                             "- 2, 2) NOT IN ('00', "
                                                                                             "'10') OR "
                                                                                             '(substring(`complaint_target_code`, '
                                                                                             'length(`complaint_target_code`) '
                                                                                             "- 2, 2) = '10' AND "
                                                                                             'length(`complaint_target_code`) '
                                                                                             '< 11))',
                                                                                    'table': 'clinical_presenting_complaint'},
 'gold.clinical.procedure.device_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                      'column': 'device_snomed_code',
                                                                      'companions': ['device_snomed_display',
                                                                                     'device_target_code',
                                                                                     'device_target_display'],
                                                                      'match': '`device_snomed_code` IS NOT NULL AND '
                                                                               '(NOT `device_snomed_code` RLIKE '
                                                                               "'^[1-9][0-9]{5,17}$' OR "
                                                                               'substring(`device_snomed_code`, '
                                                                               'length(`device_snomed_code`) - 2, 2) '
                                                                               "NOT IN ('00', '10') OR "
                                                                               '(substring(`device_snomed_code`, '
                                                                               'length(`device_snomed_code`) - 2, 2) '
                                                                               "= '10' AND "
                                                                               'length(`device_snomed_code`) < 11))',
                                                                      'table': 'clinical_procedure'},
 'gold.clinical.procedure.device_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                      'column': 'device_target_code',
                                                                      'companions': ['device_target_display',
                                                                                     'device_snomed_code',
                                                                                     'device_snomed_display'],
                                                                      'match': '`device_target_system` = '
                                                                               "'http://snomed.info/sct' AND "
                                                                               '`device_target_code` IS NOT NULL AND '
                                                                               '(NOT `device_target_code` RLIKE '
                                                                               "'^[1-9][0-9]{5,17}$' OR "
                                                                               'substring(`device_target_code`, '
                                                                               'length(`device_target_code`) - 2, 2) '
                                                                               "NOT IN ('00', '10') OR "
                                                                               '(substring(`device_target_code`, '
                                                                               'length(`device_target_code`) - 2, 2) '
                                                                               "= '10' AND "
                                                                               'length(`device_target_code`) < 11))',
                                                                      'table': 'clinical_procedure'},
 'gold.clinical.procedure.performer_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'performer_practitioner_id',
                                                                       'companions': [],
                                                                       'match': '`performer_practitioner_id` = 0',
                                                                       'table': 'clinical_procedure'},
 'gold.clinical.procedure.procedure_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                         'column': 'procedure_snomed_code',
                                                                         'companions': ['procedure_snomed_display',
                                                                                        'procedure_target_code',
                                                                                        'procedure_target_display'],
                                                                         'match': '`procedure_snomed_code` IS NOT '
                                                                                  'NULL AND (NOT '
                                                                                  '`procedure_snomed_code` RLIKE '
                                                                                  "'^[1-9][0-9]{5,17}$' OR "
                                                                                  'substring(`procedure_snomed_code`, '
                                                                                  'length(`procedure_snomed_code`) - '
                                                                                  "2, 2) NOT IN ('00', '10') OR "
                                                                                  '(substring(`procedure_snomed_code`, '
                                                                                  'length(`procedure_snomed_code`) - '
                                                                                  "2, 2) = '10' AND "
                                                                                  'length(`procedure_snomed_code`) < '
                                                                                  '11))',
                                                                         'table': 'clinical_procedure'},
 'gold.clinical.procedure.procedure_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                         'column': 'procedure_target_code',
                                                                         'companions': ['procedure_target_display',
                                                                                        'procedure_snomed_code',
                                                                                        'procedure_snomed_display'],
                                                                         'match': '`procedure_target_system` = '
                                                                                  "'http://snomed.info/sct' AND "
                                                                                  '`procedure_target_code` IS NOT '
                                                                                  'NULL AND (NOT '
                                                                                  '`procedure_target_code` RLIKE '
                                                                                  "'^[1-9][0-9]{5,17}$' OR "
                                                                                  'substring(`procedure_target_code`, '
                                                                                  'length(`procedure_target_code`) - '
                                                                                  "2, 2) NOT IN ('00', '10') OR "
                                                                                  '(substring(`procedure_target_code`, '
                                                                                  'length(`procedure_target_code`) - '
                                                                                  "2, 2) = '10' AND "
                                                                                  'length(`procedure_target_code`) < '
                                                                                  '11))',
                                                                         'table': 'clinical_procedure'},
 'gold.clinical.registry_eracs_episode.cryo_number_of_units_transfused.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                                 'column': 'cryo_number_of_units_transfused',
                                                                                                 'companions': [],
                                                                                                 'match': '`cryo_number_of_units_transfused` '
                                                                                                          '> 50',
                                                                                                 'table': 'clinical_registry_eracs_episode'},
 'gold.clinical.registry_eracs_episode.ffp_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                     'column': 'ffp_number_of_units',
                                                                                     'companions': [],
                                                                                     'match': '`ffp_number_of_units` '
                                                                                              '> 50',
                                                                                     'table': 'clinical_registry_eracs_episode'},
 'gold.clinical.registry_eracs_episode.platelet_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                          'column': 'platelet_number_of_units',
                                                                                          'companions': [],
                                                                                          'match': '`platelet_number_of_units` '
                                                                                                   '> 50',
                                                                                          'table': 'clinical_registry_eracs_episode'},
 'gold.clinical.registry_eracs_episode.rbc_number_of_units.insr_f9_unit_count_cap': {'action': 'invalidate',
                                                                                     'column': 'rbc_number_of_units',
                                                                                     'companions': [],
                                                                                     'match': '`rbc_number_of_units` '
                                                                                              '> 50',
                                                                                     'table': 'clinical_registry_eracs_episode'},
 'gold.clinical.research_enrollment.encounter_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                    'column': 'encounter_id',
                                                                    'companions': [],
                                                                    'match': '`encounter_id` = 0',
                                                                    'table': 'clinical_research_enrollment'},
 'gold.clinical.research_enrollment.protocol_arm_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'protocol_arm_id',
                                                                       'companions': [],
                                                                       'match': "TRIM(`protocol_arm_id`) = '0'",
                                                                       'table': 'clinical_research_enrollment'},
 'gold.clinical.research_enrollment.source_episode_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'source_episode_id',
                                                                         'companions': [],
                                                                         'match': "TRIM(`source_episode_id`) = '0'",
                                                                         'table': 'clinical_research_enrollment'},
 'gold.clinical.rtt_activity.appointment_oid.insr_f7_zero_id': {'action': 'invalidate',
                                                                'column': 'appointment_oid',
                                                                'companions': [],
                                                                'match': '`appointment_oid` = 0',
                                                                'table': 'clinical_rtt_activity'},
 'gold.clinical.rtt_activity.event_datetime.insr_f6_rtt_event_floor': {'action': 'invalidate',
                                                                       'column': 'event_datetime',
                                                                       'companions': ['event_datetime_local'],
                                                                       'match': '`event_datetime` < '
                                                                                "TIMESTAMP'1990-01-01 00:00:00'",
                                                                       'table': 'clinical_rtt_activity'},
 'gold.clinical.rtt_pathway.event_datetime.insr_f6_rtt_event_floor': {'action': 'invalidate',
                                                                      'column': 'event_datetime',
                                                                      'companions': ['event_datetime_local'],
                                                                      'match': '`event_datetime` < '
                                                                               "TIMESTAMP'1990-01-01 00:00:00'",
                                                                      'table': 'clinical_rtt_pathway'},
 'gold.clinical.safety_incident_factor.factor_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                                   'column': 'factor_target_code',
                                                                                   'companions': ['factor_target_display'],
                                                                                   'match': '`factor_target_system` '
                                                                                            '= '
                                                                                            "'http://snomed.info/sct' "
                                                                                            'AND '
                                                                                            '`factor_target_code` IS '
                                                                                            'NOT NULL AND (NOT '
                                                                                            '`factor_target_code` '
                                                                                            'RLIKE '
                                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                                            'substring(`factor_target_code`, '
                                                                                            'length(`factor_target_code`) '
                                                                                            "- 2, 2) NOT IN ('00', "
                                                                                            "'10') OR "
                                                                                            '(substring(`factor_target_code`, '
                                                                                            'length(`factor_target_code`) '
                                                                                            "- 2, 2) = '10' AND "
                                                                                            'length(`factor_target_code`) '
                                                                                            '< 11))',
                                                                                   'table': 'clinical_safety_incident_factor'},
 'gold.clinical.specimen.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                        'column': 'body_site_snomed_code',
                                                                        'companions': [],
                                                                        'match': '`body_site_snomed_code` IS NOT '
                                                                                 'NULL AND (NOT '
                                                                                 '`body_site_snomed_code` RLIKE '
                                                                                 "'^[1-9][0-9]{5,17}$' OR "
                                                                                 'substring(`body_site_snomed_code`, '
                                                                                 'length(`body_site_snomed_code`) - '
                                                                                 "2, 2) NOT IN ('00', '10') OR "
                                                                                 '(substring(`body_site_snomed_code`, '
                                                                                 'length(`body_site_snomed_code`) - '
                                                                                 "2, 2) = '10' AND "
                                                                                 'length(`body_site_snomed_code`) < '
                                                                                 '11))',
                                                                        'table': 'clinical_specimen'},
 'gold.clinical.specimen.specimen_type_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'specimen_type_snomed_code',
                                                                            'companions': ['specimen_type_snomed_display',
                                                                                           'specimen_type_target_code',
                                                                                           'specimen_type_target_display'],
                                                                            'match': '`specimen_type_snomed_code` IS '
                                                                                     'NOT NULL AND (NOT '
                                                                                     '`specimen_type_snomed_code` '
                                                                                     "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`specimen_type_snomed_code`, '
                                                                                     'length(`specimen_type_snomed_code`) '
                                                                                     "- 2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`specimen_type_snomed_code`, '
                                                                                     'length(`specimen_type_snomed_code`) '
                                                                                     "- 2, 2) = '10' AND "
                                                                                     'length(`specimen_type_snomed_code`) '
                                                                                     '< 11))',
                                                                            'table': 'clinical_specimen'},
 'gold.clinical.specimen.specimen_type_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                            'column': 'specimen_type_target_code',
                                                                            'companions': ['specimen_type_target_display',
                                                                                           'specimen_type_snomed_code',
                                                                                           'specimen_type_snomed_display'],
                                                                            'match': '`specimen_type_target_system` '
                                                                                     "= 'http://snomed.info/sct' AND "
                                                                                     '`specimen_type_target_code` IS '
                                                                                     'NOT NULL AND (NOT '
                                                                                     '`specimen_type_target_code` '
                                                                                     "RLIKE '^[1-9][0-9]{5,17}$' OR "
                                                                                     'substring(`specimen_type_target_code`, '
                                                                                     'length(`specimen_type_target_code`) '
                                                                                     "- 2, 2) NOT IN ('00', '10') OR "
                                                                                     '(substring(`specimen_type_target_code`, '
                                                                                     'length(`specimen_type_target_code`) '
                                                                                     "- 2, 2) = '10' AND "
                                                                                     'length(`specimen_type_target_code`) '
                                                                                     '< 11))',
                                                                            'table': 'clinical_specimen'},
 'gold.clinical.tracking_attendance.checkin_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                  'column': 'checkin_id',
                                                                  'companions': [],
                                                                  'match': '`checkin_id` = 0',
                                                                  'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.checkout_disposition_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                                 'column': 'checkout_disposition_cd',
                                                                                 'companions': [],
                                                                                 'match': '`checkout_disposition_cd` '
                                                                                          '= 0 AND '
                                                                                          '`checkout_disposition_display` '
                                                                                          'IS NULL',
                                                                                 'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.checkout_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                   'column': 'checkout_id',
                                                                   'companions': [],
                                                                   'match': '`checkout_id` = 0',
                                                                   'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.family_present_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                           'column': 'family_present_cd',
                                                                           'companions': [],
                                                                           'match': '`family_present_cd` = 0 AND '
                                                                                    '`family_present_display` IS '
                                                                                    'NULL',
                                                                           'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.primary_doc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'primary_doc_id',
                                                                      'companions': [],
                                                                      'match': '`primary_doc_id` = 0',
                                                                      'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.primary_nurse_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                        'column': 'primary_nurse_id',
                                                                        'companions': [],
                                                                        'match': '`primary_nurse_id` = 0',
                                                                        'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.reactivate_user_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'reactivate_user_id',
                                                                          'companions': [],
                                                                          'match': '`reactivate_user_id` = 0',
                                                                          'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.registration_status_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'registration_status_id',
                                                                              'companions': [],
                                                                              'match': '`registration_status_id` = 0',
                                                                              'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.secondary_doc_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                        'column': 'secondary_doc_id',
                                                                        'companions': [],
                                                                        'match': '`secondary_doc_id` = 0',
                                                                        'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.secondary_nurse_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                          'column': 'secondary_nurse_id',
                                                                          'companions': [],
                                                                          'match': '`secondary_nurse_id` = 0',
                                                                          'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.specialty_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                    'column': 'specialty_id',
                                                                    'companions': [],
                                                                    'match': '`specialty_id` = 0',
                                                                    'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_attendance.team_id.insr_f7_zero_id': {'action': 'invalidate',
                                                               'column': 'team_id',
                                                               'companions': [],
                                                               'match': '`team_id` = 0',
                                                               'table': 'clinical_tracking_attendance'},
 'gold.clinical.tracking_episode.base_loc_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                  'column': 'base_loc_cd',
                                                                  'companions': [],
                                                                  'match': '`base_loc_cd` = 0 AND `base_loc_display` '
                                                                           'IS NULL',
                                                                  'table': 'clinical_tracking_episode'},
 'gold.clinical.tracking_episode.cur_tracking_locator_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                            'column': 'cur_tracking_locator_id',
                                                                            'companions': [],
                                                                            'match': '`cur_tracking_locator_id` = 0',
                                                                            'table': 'clinical_tracking_episode'},
 'gold.clinical.tracking_episode.parent_entity_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                     'column': 'parent_entity_id',
                                                                     'companions': [],
                                                                     'match': '`parent_entity_id` = 0',
                                                                     'table': 'clinical_tracking_episode'},
 'gold.clinical.tracking_location_stay.loc_bed_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                       'column': 'loc_bed_cd',
                                                                       'companions': [],
                                                                       'match': '`loc_bed_cd` = 0 AND '
                                                                                '`loc_bed_display` IS NULL',
                                                                       'table': 'clinical_tracking_location_stay'},
 'gold.clinical.tracking_location_stay.loc_room_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                        'column': 'loc_room_cd',
                                                                        'companions': [],
                                                                        'match': '`loc_room_cd` = 0 AND '
                                                                                 '`loc_room_display` IS NULL',
                                                                        'table': 'clinical_tracking_location_stay'},
 'gold.clinical.tracking_location_stay.tracking_acuity_level_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                   'column': 'tracking_acuity_level_id',
                                                                                   'companions': [],
                                                                                   'match': '`tracking_acuity_level_id` '
                                                                                            '= 0',
                                                                                   'table': 'clinical_tracking_location_stay'},
 'gold.clinical.tracking_location_stay.tracking_reason_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'tracking_reason_cd',
                                                                               'companions': [],
                                                                               'match': '`tracking_reason_cd` = 0 '
                                                                                        'AND '
                                                                                        '`tracking_reason_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_tracking_location_stay'},
 'gold.clinical.tracking_location_stay.unavail_tracking_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                    'column': 'unavail_tracking_event_id',
                                                                                    'companions': [],
                                                                                    'match': '`unavail_tracking_event_id` '
                                                                                             '= 0',
                                                                                    'table': 'clinical_tracking_location_stay'},
 'gold.clinical.tracking_milestone.complete_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                  'column': 'complete_id',
                                                                  'companions': [],
                                                                  'match': '`complete_id` = 0',
                                                                  'table': 'clinical_tracking_milestone'},
 'gold.clinical.tracking_milestone.event_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                        'column': 'event_status_cd',
                                                                        'companions': [],
                                                                        'match': '`event_status_cd` = 0 AND '
                                                                                 '`event_status_display` IS NULL',
                                                                        'table': 'clinical_tracking_milestone'},
 'gold.clinical.tracking_milestone.onset_id.insr_f7_zero_id': {'action': 'invalidate',
                                                               'column': 'onset_id',
                                                               'companions': [],
                                                               'match': '`onset_id` = 0',
                                                               'table': 'clinical_tracking_milestone'},
 'gold.clinical.tracking_milestone.requested_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                   'column': 'requested_id',
                                                                   'companions': [],
                                                                   'match': '`requested_id` = 0',
                                                                   'table': 'clinical_tracking_milestone'},
 'gold.clinical.tracking_milestone.tracking_event_type_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                               'column': 'tracking_event_type_cd',
                                                                               'companions': [],
                                                                               'match': '`tracking_event_type_cd` = '
                                                                                        '0 AND '
                                                                                        '`tracking_event_type_display` '
                                                                                        'IS NULL',
                                                                               'table': 'clinical_tracking_milestone'},
 'gold.clinical.vital_sign.body_site_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'body_site_snomed_code',
                                                                          'companions': ['body_site_snomed_display',
                                                                                         'body_site_target_code',
                                                                                         'body_site_target_display'],
                                                                          'match': '`body_site_snomed_code` IS NOT '
                                                                                   'NULL AND (NOT '
                                                                                   '`body_site_snomed_code` RLIKE '
                                                                                   "'^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`body_site_snomed_code`, '
                                                                                   'length(`body_site_snomed_code`) '
                                                                                   "- 2, 2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`body_site_snomed_code`, '
                                                                                   'length(`body_site_snomed_code`) '
                                                                                   "- 2, 2) = '10' AND "
                                                                                   'length(`body_site_snomed_code`) '
                                                                                   '< 11))',
                                                                          'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.body_site_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                          'column': 'body_site_target_code',
                                                                          'companions': ['body_site_target_display',
                                                                                         'body_site_snomed_code',
                                                                                         'body_site_snomed_display'],
                                                                          'match': '`body_site_target_system` = '
                                                                                   "'http://snomed.info/sct' AND "
                                                                                   '`body_site_target_code` IS NOT '
                                                                                   'NULL AND (NOT '
                                                                                   '`body_site_target_code` RLIKE '
                                                                                   "'^[1-9][0-9]{5,17}$' OR "
                                                                                   'substring(`body_site_target_code`, '
                                                                                   'length(`body_site_target_code`) '
                                                                                   "- 2, 2) NOT IN ('00', '10') OR "
                                                                                   '(substring(`body_site_target_code`, '
                                                                                   'length(`body_site_target_code`) '
                                                                                   "- 2, 2) = '10' AND "
                                                                                   'length(`body_site_target_code`) '
                                                                                   '< 11))',
                                                                          'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.interpretation_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'interpretation_snomed_code',
                                                                               'companions': ['interpretation_snomed_display',
                                                                                              'interpretation_target_code',
                                                                                              'interpretation_target_display'],
                                                                               'match': '`interpretation_snomed_code` '
                                                                                        'IS NOT NULL AND (NOT '
                                                                                        '`interpretation_snomed_code` '
                                                                                        "RLIKE '^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`interpretation_snomed_code`, '
                                                                                        'length(`interpretation_snomed_code`) '
                                                                                        "- 2, 2) NOT IN ('00', '10') "
                                                                                        'OR '
                                                                                        '(substring(`interpretation_snomed_code`, '
                                                                                        'length(`interpretation_snomed_code`) '
                                                                                        "- 2, 2) = '10' AND "
                                                                                        'length(`interpretation_snomed_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.interpretation_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                               'column': 'interpretation_target_code',
                                                                               'companions': ['interpretation_target_display',
                                                                                              'interpretation_snomed_code',
                                                                                              'interpretation_snomed_display'],
                                                                               'match': '`interpretation_target_system` '
                                                                                        "= 'http://snomed.info/sct' "
                                                                                        'AND '
                                                                                        '`interpretation_target_code` '
                                                                                        'IS NOT NULL AND (NOT '
                                                                                        '`interpretation_target_code` '
                                                                                        "RLIKE '^[1-9][0-9]{5,17}$' "
                                                                                        'OR '
                                                                                        'substring(`interpretation_target_code`, '
                                                                                        'length(`interpretation_target_code`) '
                                                                                        "- 2, 2) NOT IN ('00', '10') "
                                                                                        'OR '
                                                                                        '(substring(`interpretation_target_code`, '
                                                                                        'length(`interpretation_target_code`) '
                                                                                        "- 2, 2) = '10' AND "
                                                                                        'length(`interpretation_target_code`) '
                                                                                        '< 11))',
                                                                               'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.method_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                       'column': 'method_snomed_code',
                                                                       'companions': ['method_snomed_display',
                                                                                      'method_target_code',
                                                                                      'method_target_display'],
                                                                       'match': '`method_snomed_code` IS NOT NULL '
                                                                                'AND (NOT `method_snomed_code` RLIKE '
                                                                                "'^[1-9][0-9]{5,17}$' OR "
                                                                                'substring(`method_snomed_code`, '
                                                                                'length(`method_snomed_code`) - 2, '
                                                                                "2) NOT IN ('00', '10') OR "
                                                                                '(substring(`method_snomed_code`, '
                                                                                'length(`method_snomed_code`) - 2, '
                                                                                "2) = '10' AND "
                                                                                'length(`method_snomed_code`) < 11))',
                                                                       'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.method_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                       'column': 'method_target_code',
                                                                       'companions': ['method_target_display',
                                                                                      'method_snomed_code',
                                                                                      'method_snomed_display'],
                                                                       'match': '`method_target_system` = '
                                                                                "'http://snomed.info/sct' AND "
                                                                                '`method_target_code` IS NOT NULL '
                                                                                'AND (NOT `method_target_code` RLIKE '
                                                                                "'^[1-9][0-9]{5,17}$' OR "
                                                                                'substring(`method_target_code`, '
                                                                                'length(`method_target_code`) - 2, '
                                                                                "2) NOT IN ('00', '10') OR "
                                                                                '(substring(`method_target_code`, '
                                                                                'length(`method_target_code`) - 2, '
                                                                                "2) = '10' AND "
                                                                                'length(`method_target_code`) < 11))',
                                                                       'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.unit_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                     'column': 'unit_snomed_code',
                                                                     'companions': ['unit_snomed_display',
                                                                                    'unit_target_code',
                                                                                    'unit_target_display'],
                                                                     'match': '`unit_snomed_code` IS NOT NULL AND '
                                                                              '(NOT `unit_snomed_code` RLIKE '
                                                                              "'^[1-9][0-9]{5,17}$' OR "
                                                                              'substring(`unit_snomed_code`, '
                                                                              'length(`unit_snomed_code`) - 2, 2) '
                                                                              "NOT IN ('00', '10') OR "
                                                                              '(substring(`unit_snomed_code`, '
                                                                              'length(`unit_snomed_code`) - 2, 2) = '
                                                                              "'10' AND length(`unit_snomed_code`) < "
                                                                              '11))',
                                                                     'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.unit_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                     'column': 'unit_target_code',
                                                                     'companions': ['unit_target_display',
                                                                                    'unit_snomed_code',
                                                                                    'unit_snomed_display'],
                                                                     'match': '`unit_target_system` = '
                                                                              "'http://snomed.info/sct' AND "
                                                                              '`unit_target_code` IS NOT NULL AND '
                                                                              '(NOT `unit_target_code` RLIKE '
                                                                              "'^[1-9][0-9]{5,17}$' OR "
                                                                              'substring(`unit_target_code`, '
                                                                              'length(`unit_target_code`) - 2, 2) '
                                                                              "NOT IN ('00', '10') OR "
                                                                              '(substring(`unit_target_code`, '
                                                                              'length(`unit_target_code`) - 2, 2) = '
                                                                              "'10' AND length(`unit_target_code`) < "
                                                                              '11))',
                                                                     'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.value_number.insr_bbr_anthropometric_range': {'action': 'invalidate',
                                                                         'column': 'value_number',
                                                                         'companions': ['value_canonical'],
                                                                         'match': '`value_number` IS NOT NULL AND '
                                                                                  '((`_qc_vital_snomed_code` = '
                                                                                  "'27113001' AND (`value_canonical` "
                                                                                  '<= 0 OR `value_canonical` > 400)) '
                                                                                  'OR (`_qc_vital_snomed_code` = '
                                                                                  "'50373000' AND (`value_canonical` "
                                                                                  '< 20 OR `value_canonical` > 250)) '
                                                                                  'OR (`_qc_vital_snomed_code` = '
                                                                                  "'60621009' AND (`value_canonical` "
                                                                                  '< 5 OR `value_canonical` > 150)))',
                                                                         'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.vital_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                      'column': 'vital_snomed_code',
                                                                      'companions': ['vital_snomed_display',
                                                                                     'vital_target_code',
                                                                                     'vital_target_display'],
                                                                      'match': '`vital_snomed_code` IS NOT NULL AND '
                                                                               '(NOT `vital_snomed_code` RLIKE '
                                                                               "'^[1-9][0-9]{5,17}$' OR "
                                                                               'substring(`vital_snomed_code`, '
                                                                               'length(`vital_snomed_code`) - 2, 2) '
                                                                               "NOT IN ('00', '10') OR "
                                                                               '(substring(`vital_snomed_code`, '
                                                                               'length(`vital_snomed_code`) - 2, 2) '
                                                                               "= '10' AND "
                                                                               'length(`vital_snomed_code`) < 11))',
                                                                      'table': 'clinical_vital_sign'},
 'gold.clinical.vital_sign.vital_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                      'column': 'vital_target_code',
                                                                      'companions': ['vital_target_display',
                                                                                     'vital_snomed_code',
                                                                                     'vital_snomed_display'],
                                                                      'match': '`vital_target_system` = '
                                                                               "'http://snomed.info/sct' AND "
                                                                               '`vital_target_code` IS NOT NULL AND '
                                                                               '(NOT `vital_target_code` RLIKE '
                                                                               "'^[1-9][0-9]{5,17}$' OR "
                                                                               'substring(`vital_target_code`, '
                                                                               'length(`vital_target_code`) - 2, 2) '
                                                                               "NOT IN ('00', '10') OR "
                                                                               '(substring(`vital_target_code`, '
                                                                               'length(`vital_target_code`) - 2, 2) '
                                                                               "= '10' AND "
                                                                               'length(`vital_target_code`) < 11))',
                                                                      'table': 'clinical_vital_sign'},
 'gold.clinical.waiting_list_entry.location_code.insr_f7_zero_code': {'action': 'normalize',
                                                                      'column': 'location_code',
                                                                      'companions': [],
                                                                      'match': "TRIM(`location_code`) = '0' AND "
                                                                               '`location_display` IS NULL',
                                                                      'table': 'clinical_waiting_list_entry'},
 'gold.clinical.waiting_list_entry.sch_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                   'column': 'sch_event_id',
                                                                   'companions': [],
                                                                   'match': '`sch_event_id` = 0',
                                                                   'table': 'clinical_waiting_list_entry'},
 'gold.clinical.waiting_list_snapshot.location_code.insr_f7_zero_code': {'action': 'normalize',
                                                                         'column': 'location_code',
                                                                         'companions': [],
                                                                         'match': "TRIM(`location_code`) = '0' AND "
                                                                                  '`location_display` IS NULL',
                                                                         'table': 'clinical_waiting_list_snapshot'},
 'gold.clinical.waiting_list_snapshot.sch_event_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                      'column': 'sch_event_id',
                                                                      'companions': [],
                                                                      'match': '`sch_event_id` = 0',
                                                                      'table': 'clinical_waiting_list_snapshot'},
 'gold.reference.location_attribute_history.value_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'value_cd',
                                                                          'companions': [],
                                                                          'match': '`value_cd` = 0 AND '
                                                                                   '`value_display` IS NULL',
                                                                          'table': 'reference_location_attribute_history'},
 'gold.reference.location_attribute_history.value_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                        'column': 'value_id',
                                                                        'companions': [],
                                                                        'match': '`value_id` = 0',
                                                                        'table': 'reference_location_attribute_history'},
 'gold.reference.location_unit.current_bed_status_cd.insr_f7_zero_code': {'action': 'normalize',
                                                                          'column': 'current_bed_status_cd',
                                                                          'companions': [],
                                                                          'match': '`current_bed_status_cd` = 0 AND '
                                                                                   '`current_bed_status_display` IS '
                                                                                   'NULL',
                                                                          'table': 'reference_location_unit'},
 'gold.reference.person_death_evidence.age_at_death.insr_f8_zero_placeholder': {'action': 'invalidate',
                                                                                'column': 'age_at_death',
                                                                                'companions': [],
                                                                                'match': '`age_at_death` = 0',
                                                                                'table': 'reference_person_death_evidence'},
 'gold.reference.theatre_attendance.attendee_personnel_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                             'column': 'attendee_personnel_id',
                                                                             'companions': [],
                                                                             'match': 'TRIM(`attendee_personnel_id`) '
                                                                                      "= '0'",
                                                                             'table': 'reference_theatre_attendance'},
 'gold.reference.theatre_attendance.attendee_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                'column': 'attendee_practitioner_id',
                                                                                'companions': [],
                                                                                'match': '`attendee_practitioner_id` '
                                                                                         '= 0',
                                                                                'table': 'reference_theatre_attendance'},
 'gold.reference.theatre_case.anaesthetist_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                              'column': 'anaesthetist_practitioner_id',
                                                                              'companions': [],
                                                                              'match': '`anaesthetist_practitioner_id` '
                                                                                       '= 0',
                                                                              'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.asa_class_code.insr_f7_zero_code': {'action': 'normalize',
                                                                  'column': 'asa_class_code',
                                                                  'companions': [],
                                                                  'match': "TRIM(`asa_class_code`) = '0' AND "
                                                                           '`asa_class_display` IS NULL',
                                                                  'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.cancel_reason_code.insr_f7_zero_code': {'action': 'normalize',
                                                                      'column': 'cancel_reason_code',
                                                                      'companions': [],
                                                                      'match': "TRIM(`cancel_reason_code`) = '0' AND "
                                                                               '`cancel_reason_display` IS NULL',
                                                                      'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.case_level_code.insr_f7_zero_code': {'action': 'normalize',
                                                                   'column': 'case_level_code',
                                                                   'companions': [],
                                                                   'match': "TRIM(`case_level_code`) = '0' AND "
                                                                            '`case_level_display` IS NULL',
                                                                   'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.patient_type_code.insr_f7_zero_code': {'action': 'normalize',
                                                                     'column': 'patient_type_code',
                                                                     'companions': [],
                                                                     'match': "TRIM(`patient_type_code`) = '0' AND "
                                                                              '`patient_type_display` IS NULL',
                                                                     'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.surgeon_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                         'column': 'surgeon_practitioner_id',
                                                                         'companions': [],
                                                                         'match': '`surgeon_practitioner_id` = 0',
                                                                         'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.surgery_duration_minutes.insr_f6_duration_range': {'action': 'invalidate',
                                                                                 'column': 'surgery_duration_minutes',
                                                                                 'companions': [],
                                                                                 'match': '`surgery_duration_minutes` '
                                                                                          '< 0 OR '
                                                                                          '`surgery_duration_minutes` '
                                                                                          '> 1440',
                                                                                 'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.surgical_specialty_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                       'column': 'surgical_specialty_id',
                                                                       'companions': [],
                                                                       'match': '`surgical_specialty_id` = 0',
                                                                       'table': 'reference_theatre_case'},
 'gold.reference.theatre_case.wound_class_code.insr_f7_zero_code': {'action': 'normalize',
                                                                    'column': 'wound_class_code',
                                                                    'companions': [],
                                                                    'match': "TRIM(`wound_class_code`) = '0' AND "
                                                                             '`wound_class_display` IS NULL',
                                                                    'table': 'reference_theatre_case'},
 'gold.reference.theatre_implant.implanted_by_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                                 'column': 'implanted_by_practitioner_id',
                                                                                 'companions': [],
                                                                                 'match': '`implanted_by_practitioner_id` '
                                                                                          '= 0',
                                                                                 'table': 'reference_theatre_implant'},
 'gold.reference.theatre_implant.item_id.insr_f7_zero_id': {'action': 'invalidate',
                                                            'column': 'item_id',
                                                            'companions': [],
                                                            'match': "TRIM(`item_id`) = '0'",
                                                            'table': 'reference_theatre_implant'},
 'gold.spine.care_participation.practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                                   'column': 'practitioner_id',
                                                                   'companions': [],
                                                                   'match': '`practitioner_id` = 0',
                                                                   'table': 'spine_care_participation'},
 'gold.spine.journey_link.member_end_datetime.insr_f6_inversion_long': {'action': 'invalidate',
                                                                        'column': 'member_end_datetime',
                                                                        'companions': [],
                                                                        'match': '`member_end_datetime` < '
                                                                                 '`member_start_datetime` - INTERVAL '
                                                                                 '24 HOURS',
                                                                        'table': 'spine_journey_link'},
 'gold.spine.person.ethnicity_snomed_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                   'column': 'ethnicity_snomed_code',
                                                                   'companions': ['ethnicity_snomed_display',
                                                                                  'ethnicity_target_code',
                                                                                  'ethnicity_target_display'],
                                                                   'match': '`ethnicity_snomed_code` IS NOT NULL AND '
                                                                            '(NOT `ethnicity_snomed_code` RLIKE '
                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                            'substring(`ethnicity_snomed_code`, '
                                                                            'length(`ethnicity_snomed_code`) - 2, 2) '
                                                                            "NOT IN ('00', '10') OR "
                                                                            '(substring(`ethnicity_snomed_code`, '
                                                                            'length(`ethnicity_snomed_code`) - 2, 2) '
                                                                            "= '10' AND "
                                                                            'length(`ethnicity_snomed_code`) < 11))',
                                                                   'table': 'spine_person'},
 'gold.spine.person.ethnicity_source_code.insr_f7_zero_code': {'action': 'normalize',
                                                               'column': 'ethnicity_source_code',
                                                               'companions': [],
                                                               'match': "TRIM(`ethnicity_source_code`) = '0' AND "
                                                                        '`ethnicity_source_display` IS NULL',
                                                               'table': 'spine_person'},
 'gold.spine.person.ethnicity_target_code.insr_sct_concept_only': {'action': 'invalidate',
                                                                   'column': 'ethnicity_target_code',
                                                                   'companions': ['ethnicity_target_display',
                                                                                  'ethnicity_snomed_code',
                                                                                  'ethnicity_snomed_display'],
                                                                   'match': '`ethnicity_target_system` = '
                                                                            "'http://snomed.info/sct' AND "
                                                                            '`ethnicity_target_code` IS NOT NULL AND '
                                                                            '(NOT `ethnicity_target_code` RLIKE '
                                                                            "'^[1-9][0-9]{5,17}$' OR "
                                                                            'substring(`ethnicity_target_code`, '
                                                                            'length(`ethnicity_target_code`) - 2, 2) '
                                                                            "NOT IN ('00', '10') OR "
                                                                            '(substring(`ethnicity_target_code`, '
                                                                            'length(`ethnicity_target_code`) - 2, 2) '
                                                                            "= '10' AND "
                                                                            'length(`ethnicity_target_code`) < 11))',
                                                                   'table': 'spine_person'},
 'gold.text.document.author_practitioner_id.insr_f7_zero_id': {'action': 'invalidate',
                                                               'column': 'author_practitioner_id',
                                                               'companions': [],
                                                               'match': '`author_practitioner_id` = 0',
                                                               'table': 'text_document'},
 'gold.text.document.author_source_id.insr_f7_zero_id': {'action': 'invalidate',
                                                         'column': 'author_source_id',
                                                         'companions': [],
                                                         'match': "TRIM(`author_source_id`) = '0'",
                                                         'table': 'text_document'}}

INSR_W4_BIRTH_FLAG_COLUMNS = {'clinical_waiting_list_entry': ['admit_decision_datetime', 'recommend_datetime', 'referral_datetime'],
 'clinical_waiting_list_snapshot': ['admit_decision_datetime', 'recommend_datetime', 'referral_datetime']}


def insr_w4_apply_value_rules(table_name, df):
    """NULL each W4 value-rule column (and its *_local companion) where the rule's match condition holds.

    The condition is materialised into a helper column before any column changes, so a condition that reads the
    column it NULLs is evaluated on the original value. A rule whose columns are absent fails the update loudly.
    """
    # SCT_TRUD_V1: the coding-axis columns _s3_flatten_gold_public restores from a JSON column do not exist yet
    # when _qc runs this; a rule on those columns only (none present) is applied by the flatten instead.
    restored = {name for axes in GOLD_S3_AXIS_SPECS.get(table_name, {}).values()
                for axis, source_only in axes for name in _s3_axis_columns(axis, source_only)}
    for rule_id, rule in sorted(INSR_W4_VALUE_RULES.items()):
        if rule["table"] != table_name:
            continue
        targets = [rule["column"], *rule["companions"]]
        missing = [column for column in targets if column not in df.columns]
        if missing and set(targets) <= restored and len(missing) == len(targets):
            continue  # SCT_TRUD_V1: deferred to _s3_flatten_gold_public
        if missing:
            raise ValueError(f"{rule_id}: {missing} not in {table_name}")
        df = df.withColumn("__insr_w4_hit", F.coalesce(F.expr(rule["match"]), F.lit(False)))
        for column in targets:
            df = df.withColumn(column, F.when(~F.col("__insr_w4_hit"), F.col(column)))
        df = df.drop("__insr_w4_hit")
    return df


def insr_w4_date_flags(table_name, requested):
    """Add the token that makes _qc join _gold_person_dates for tables with W4 before-birth rules."""
    if table_name in INSR_W4_BIRTH_FLAG_COLUMNS and "insr_w4_birth" not in requested:
        return (*requested, "insr_w4_birth")
    return requested


def insr_w4_birth_flags(table_name, df):
    """insr_w4_before_birth_<column>: the column precedes the person's birth (FALSE when either side is NULL).

    Internal twin columns read by the F5(g) warn rules; the public wrappers select explicitly, so they stay internal.
    """
    for column in INSR_W4_BIRTH_FLAG_COLUMNS.get(table_name, ()):
        df = df.withColumn(f"insr_w4_before_birth_{column}",
                           F.coalesce(F.col(column) < F.col("_qc_birth_datetime"), F.lit(False)))
    return df
