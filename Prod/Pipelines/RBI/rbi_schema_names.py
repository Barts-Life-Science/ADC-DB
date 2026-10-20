"""Column-name lists shared by rbi_schema (Spark) and rbi_docs (pure Python). Order matters."""
STREAM_COLUMN_NAMES = [
    "resource_use_event_key", "domain", "stream", "unit_type", "quantity", "quantity_requested",
    "quantity_performed_result", "quantity_performed_reported", "quantity_unknown", "quantity_scheduled", "consumption_status",
    "consumption_evidence", "event_start", "event_end", "event_date_local", "financial_year", "person_id",
    "encounter_id", "attribution_method", "attribution_score", "candidate_count", "nurse_unit_code", "site_code",
    "service_code", "clinic_code", "theatre_code", "tracking_class", "stratum_l1", "stratum_l2",
    "priced_by_other_stream", "drug_concept_id", "source_amount", "source_table", "source_key", "rule_version",
]
COST_COLUMN_NAMES = [
    "cost_total", "cost_overhead", "cost_basis", "cost_reason", "cost_modelled_shadow", "cost_superseded",
    "cost_stratum_level", "cost_base_fy", "uplift_factor", "cost_method_version",
]
