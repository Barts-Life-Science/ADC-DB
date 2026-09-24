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
 'events_patient_event': {'join_keys': ['patient_event_row_key'],
                          'support_columns': ['identity_status',
                                              'record_status',
                                              'load_batch_id',
                                              'loaded_at']},
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
 'events_patient_event': ['event_after_death_30d', 'event_before_birth'],
 'text_document': ['event_after_death_30d', 'event_before_birth']}
GOLD_V2_SOURCE_SUPPORT = {'text_document': ['is_latest_version']}

# S3B_GOLD_PATCH_APPLIED v1
# S3C_GOLD_PATCH_APPLIED v1
GOLD_V2_POLICIES = json.loads(spark.conf.get("journey.s3b_policies", "{}"))
GOVERNED_TRANSLATION_RULES = (
    "omop:maps_to:", "trud:icd10:closest", "trud:icdsctmap:single",
    "trud:opcs4:closest", "trud:opcssctmap:single", "trud.nicip.",
    "omop:concept_name:exact", "ukcore:ethnic_category:",
    "form:smoking_status_instrument:", "powerform:S5:validated", "ucum:unit:",
    "score:component:validated", "omop:concept_synonym:exact", "nicip:",
    "bronze.map_medication_order:", "bronze.map_med_admin:LOOKUP_STANDARD_MAP",
    "bronze.map_med_admin:EXACT_UNIQUE_STANDARD_NAME", "bronze.map_med_admin:RXNORM_CODE",
    "bronze.map_med_admin:SNOMED_CODE", "bronze.map_condition:NATIVE",
    "bronze.map_procedure:NATIVE", "omop:snomed_concept_crosswalk", "source:",
    "omop:concept_synonym:exact:lower", "concept_synonym:exact:lower",
    "rxnorm:mmsl_gn:snomedct_us:", "ecds:chief_complaint:description_exact",
    "ecds:chief_complaint:pool_seed_exact", "ecds:chief_complaint:rfv_exact", "ecds:chief_complaint:closest",
    "nicip:preferred_term:exact:", "nicip:synonym:exact:", "nicip:preferred_term:closest",
    "dmd:id:native", "dmd:name:exact",
    "bronze.map_implant_details:LAYER1_UDI_GUDID", "bronze.map_implant_details:LAYER1B_CATALOGUE_GUDID",
    "bronze.map_implant_details:LAYER2_EXACT_ANCHOR", "bronze.map_implant_details:LAYER3_BRAND_RULE",
    "bronze.map_implant_details:LAYER4_CLEANED_ANCHOR", "bronze.map_implant_details:LAYER6_OPCS_STANDARD",
)
GOVERNED_ROLLUP_RULES = ("trud:icdsctmap:rollup", "trud:opcssctmap:rollup")

def _s3_axis_gate_and_policy(item):
    """Decide whether a mapped target meets the configured policy and return its provenance."""
    method = item["map_method"]
    rule = F.coalesce(item["map_rule_id"], F.lit(""))
    native = method == "source_native"
    translation = F.lit(False)
    for prefix in GOVERNED_TRANSLATION_RULES:
        translation = translation | (method.isin("rule", "exact_synonym") & rule.startswith(prefix))
    for prefix in GOVERNED_ROLLUP_RULES:
        translation = translation | ((method == "ancestor_rollup") & rule.startswith(prefix) &
                                     (F.coalesce(item["map_rollup_levels"], F.lit(99)) <= F.lit(4)))
    translation = translation & ~rule.contains("EXACT_string_similarity")
    banded = F.lit(False)
    policy_id = (F.when(native, F.lit("source_native"))
                 .when(translation, F.lit("governed_translation")))
    for current_id, policy in sorted(GOLD_V2_POLICIES.items()):
        max_levels = int(policy.get("rollup_config", {}).get("max_levels", 3))
        matched = (
            (method == policy["method"]) &
            rule.contains(f":{policy['lane']}:") &
            (F.coalesce(item["map_scoring_model"], F.lit("")) == F.lit(policy["scoring_model"])) &
            F.coalesce(item["map_version"], F.lit("")).contains(F.lit(policy["hierarchy_version"])) &
            (F.coalesce(item["map_cosine"], F.lit(0.0)) >= F.lit(float(policy["cutoff"]))) &
            ((method != "ancestor_rollup") |
             (F.coalesce(item["map_rollup_levels"], F.lit(99)) <= F.lit(max_levels)))
        )
        banded = banded | matched
        policy_id = F.when(matched, F.lit(current_id)).otherwise(policy_id)
    rejected = F.coalesce(item["map_status"], F.lit("")).isin("judge_rejected", "vetoed_polarity")
    gate = F.coalesce((native | translation | banded) & ~rejected, F.lit(False))
    policy_id = F.when(rejected, F.concat(F.lit("withheld:"), item["map_status"])).otherwise(policy_id)
    return gate, F.coalesce(policy_id, F.lit("withheld:no_matching_policy"))

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
        payload=F.from_json(F.col(old_name), "struct<"+",".join(
            f"{axis}:{_S3_GOLD_AXIS_SCHEMA}" for axis,_ in axes)+">")
        replacement=[]
        for axis, source_only in axes:
            a=payload[axis]
            for field in ("source_system","source_code","source_display"):
                df=df.withColumn(f"{axis}_{field}",a[field])
            if not source_only:
                gate, _ = _s3_axis_gate_and_policy(a)
                for field in ("snomed_code","snomed_display","omop_concept_id",
                              "target_system","target_code","target_display"):
                    df=df.withColumn(f"{axis}_{field}",F.when(gate,a[field]))
                for field in ("map_method","map_version","map_confidence","map_rule_id","map_candidate_count",
                              "omop_source_concept_id","map_cosine","map_scoring_model","map_status",
                              "map_status_reason","map_competing_count","map_candidate_set_id",
                              "map_component_index","map_component_count","map_rollup_levels"):
                    df=df.withColumn(f"{axis}_{field}",a[field])
            replacement.extend(_s3_axis_columns(axis,source_only))
        order=[]
        for name in df.columns:
            if name==old_name: order.extend(replacement)
            elif name not in replacement: order.append(name)
        df=df.select(*order)
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
            life = spark.read.table(internal_source + "._" + table_name + "_metadata").select(*keys, *internal_support)
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

    df = source.selectExpr(*prepared)

    if "person_id" in fk_columns:
        parent = spark.read.table("_gold_person_keys")
        df = (
            df.join(parent, df.person_id == parent._qc_person_id, "left")
            .withColumn(
                "person_id",
                F.when(F.col("_qc_person_id").isNotNull(), F.col("person_id")),
            )
            .drop("_qc_person_id")
        )
    if "encounter_id" in fk_columns:
        parent = spark.read.table("_gold_encounter_keys")
        df = (
            df.join(parent, df.encounter_id == parent._qc_encounter_id, "left")
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
            .withColumn(
                "episode_id",
                F.when(F.col("_qc_episode_id").isNotNull(), F.col("episode_id")),
            )
            .drop("_qc_episode_id")
        )

    requested_date_flags = tuple(
        dict.fromkeys((*date_flags, *GOLD_V2_DATE_FLAGS.get(table_name, [])))
    )
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
        payload = F.from_json(F.col(transport), "struct<" + ",".join(
            f"{axis}:{_S3_GOLD_AXIS_SCHEMA}" for axis, _ in axes) + ">")
        for axis, source_only in axes:
            if not source_only:
                item = payload[axis]
                gate, policy_id = _s3_axis_gate_and_policy(item)
                has_target = item["snomed_code"].isNotNull() | item["omop_concept_id"].isNotNull()
                df = df.withColumn(f"_{axis}_gate_pass", F.when(has_target, gate))
                df = df.withColumn(f"_{axis}_policy_id", F.when(has_target, policy_id))
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


def _tdx_comments(frame, product):
    """Preserve the existing TDX field comments on the returned schema."""
    for column in frame.columns:
        frame = frame.withMetadata(column, {"comment": f"{product} field {column}; quality-controlled TDX publication."})
    return frame

def _released(frame):
    """Release text only when redaction version, input digest and context still match."""
    columns = set(frame.columns)
    required = {
        "anon_status", "anon_redactor_version", "anon_source_text_sha",
        "anon_input_digest", "anon_context_fingerprint", "context_fingerprint_current",
    }
    if not required.issubset(columns):
        return F.lit(False)
    return (
        (F.col("anon_status") == "anonymized")
        & F.col("anon_redactor_version").isin("v3.3")
        & F.col("anon_source_text_sha").eqNullSafe(F.col("anon_input_digest"))
        & F.col("anon_context_fingerprint").eqNullSafe(F.col("context_fingerprint_current"))
    )

def _tdx_product(target_name):
    """Prepare TDX values and fail-closed text; native decorators handle row admission."""
    df = spark.read.table(f"{SILVER_CATALOG_SCHEMA}.{target_name}")
    if target_name == "text_safety_document":
        released = _released(df) & F.col("text_is_anonymised")
        df = df.withColumn("source_text_sha256", F.col("text_sha256"))
        df = df.withColumn("document_text", F.when(released, F.col("document_text_anonymised")))
        df = df.drop("text", "document_text_anonymised")
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
                df = df.withColumn(original, F.when(released, F.col(anonymised))).drop(anonymised)
    return _tdx_comments(df, target_name)

def _tdx_qc(target_name, key_name):
    """Retain the old admitted-key evidence table without treating advisory warnings as failures."""
    frame = spark.read.table(f"{PUBLIC_SCHEMA}.{target_name}")
    return _tdx_comments(
        frame.select(F.col(key_name), F.lit("passed").alias("qc_status"), F.col("loaded_at").alias("qc_checked_at")),
        f"{target_name} QC",
    )
