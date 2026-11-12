"""Gold specialist.pregnancy_reconciliation: 2 dataset(s).

- datasets: _qc_reference_pregnancy_reconciliation, reference_pregnancy_reconciliation
- moved verbatim from: 3_gold_specialist_care:2655, 3_gold_specialist_care:2678, 3_gold_specialist_care:2685, 3_gold_specialist_care:2715, 3_gold_specialist_care:2727

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.pregnancy_reconciliation ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PREGNANCY_RECONCILIATION_SELECT = [
    '`pregnancy_reconciliation_key` AS `pregnancy_reconciliation_key`',
    '`unmatched_reason` AS `unmatched_reason`',
    '`pregnancy_id_raw` AS `pregnancy_id_raw`',
    '`pregnancy_id` AS `pregnancy_id`',
    '`lpid_mother` AS `lpid_mother`',
    '`antenatal_appointment_date` AS `antenatal_appointment_date`',
    '`pregnancy_first_contact_date` AS `pregnancy_first_contact_date`',
    '`expected_delivery_date` AS `expected_delivery_date`',
    '`last_mens_period_date` AS `last_mens_period_date`',
    '`folic_acid_supplement_cd` AS `folic_acid_supplement_cd`',
    '`previous_live_births` AS `previous_live_births`',
    '`previous_still_births` AS `previous_still_births`',
    '`previous_losses_under_24_weeks` AS `previous_losses_under_24_weeks`',
    '`previous_caesarean_sections` AS `previous_caesarean_sections`',
    '`source_system_code` AS `source_system_code`',
    '`msds_source_version` AS `msds_source_version`',
    '`record_status` AS `record_status`',
    '`source_update_timestamp` AS `source_update_timestamp`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: source history comes directly from the main research table; no separate metadata input is needed.
REFERENCE_PREGNANCY_RECONCILIATION_MANDATORY_RULES = {
    # The research surface. record_status = 'active' keeps the 59 rows of 59 that are
    # current and attributable. Superseded versions and rows whose identity was never
    # resolved are not research data, and a consumer who wants them has silver.
    "research_surface": "(record_status = 'active')",
}


REFERENCE_PREGNANCY_RECONCILIATION_ADVISORY_RULES = {
    # Counts what the research surface removed: rows failing record_status = 'active'. The
    # mandatory rule above has already dropped them, so this is how many went, not how many
    # survive. 0 of 59 at the profile.
    "gold.reference.pregnancy_reconciliation.record_status.default_view_active":
        "record_status = 'active'",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_pregnancy_reconciliation"),
    refresh_policy="auto",
)
@dp.expect_all_or_drop(REFERENCE_PREGNANCY_RECONCILIATION_MANDATORY_RULES)
@dp.expect_all(REFERENCE_PREGNANCY_RECONCILIATION_ADVISORY_RULES)
def _gold_qc_reference_pregnancy_reconciliation():
    """Quality-controlled twin of journey_reference.pregnancy_reconciliation."""
    df = _qc("reference_pregnancy_reconciliation", REFERENCE_PREGNANCY_RECONCILIATION_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.pregnancy_reconciliation"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_pregnancy_reconciliation():
    """Contract-v2 public twin of reference_pregnancy_reconciliation; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_pregnancy_reconciliation")).selectExpr(
        '`pregnancy_reconciliation_key` AS `pregnancy_reconciliation_key`',
        '`unmatched_reason` AS `unmatched_reason`',
        '`pregnancy_id_raw` AS `pregnancy_id_raw`',
        '`pregnancy_id` AS `pregnancy_id`',
        '`lpid_mother` AS `lpid_mother`',
        '`antenatal_appointment_date` AS `antenatal_appointment_date`',
        '`pregnancy_first_contact_date` AS `pregnancy_first_contact_date`',
        '`expected_delivery_date` AS `expected_delivery_date`',
        '`last_mens_period_date` AS `last_mens_period_date`',
        '`folic_acid_supplement_cd` AS `folic_acid_supplement_cd`',
        '`previous_live_births` AS `previous_live_births`',
        '`previous_still_births` AS `previous_still_births`',
        '`previous_losses_under_24_weeks` AS `previous_losses_under_24_weeks`',
        '`previous_caesarean_sections` AS `previous_caesarean_sections`',
        '`source_system_code` AS `source_system_code`',
        '`msds_source_version` AS `msds_source_version`',
        '`record_status` AS `record_status`',
        '`source_update_timestamp` AS `source_update_timestamp`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_pregnancy_reconciliation': GoldRules(
        table='reference_pregnancy_reconciliation',
        select=REFERENCE_PREGNANCY_RECONCILIATION_SELECT,
        mandatory=REFERENCE_PREGNANCY_RECONCILIATION_MANDATORY_RULES,
        advisory=REFERENCE_PREGNANCY_RECONCILIATION_ADVISORY_RULES,
        lifecycle={'join_keys': ['pregnancy_reconciliation_key'],
         'support_columns': ['record_status', 'source_update_timestamp', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_pregnancy_reconciliation': {'columns': {'antenatal_appointment_date': 'MSDS booking '
                                                                                      'appointment date.',
                                                        'expected_delivery_date': 'MSDS agreed EDD.',
                                                        'folic_acid_supplement_cd': 'MSDS folic-acid '
                                                                                    'supplement code.',
                                                        'last_mens_period_date': 'MSDS last menstrual period '
                                                                                 'date.',
                                                        'loaded_at': 'map_mat_pregnancy_msds_unmatched.ADC_UPDT '
                                                                     'carried unchanged for the unmatched '
                                                                     'pregnancy row. No matched '
                                                                     'pregnancy-spine clock is joined or '
                                                                     'substituted; this is bronze ingestion '
                                                                     'provenance, distinct from '
                                                                     'RECORD_UPDATED_DT and Silver refresh '
                                                                     'time.',
                                                        'lpid_mother': 'Direct identifier published and '
                                                                       'IG-governed at serve time',
                                                        'msds_source_version': 'MSDS Delta version used for '
                                                                               'the snapshot.',
                                                        'pregnancy_first_contact_date': 'MSDS first '
                                                                                        'pregnancy-contact '
                                                                                        'date.',
                                                        'pregnancy_id': 'MSDS pregnancy identifier after '
                                                                        'checked numeric parsing.',
                                                        'pregnancy_id_raw': 'Unchanged MSDS pregnancy '
                                                                            'identifier.',
                                                        'pregnancy_reconciliation_key': 'Deterministic '
                                                                                        'SHA-256 key for the '
                                                                                        'unmatched MSDS '
                                                                                        'pregnancy row.',
                                                        'previous_caesarean_sections': 'MSDS '
                                                                                       'previous-caesarean '
                                                                                       'count.',
                                                        'previous_live_births': 'MSDS previous live-birth '
                                                                                'count.',
                                                        'previous_losses_under_24_weeks': 'MSDS '
                                                                                          'terminations/losses '
                                                                                          'under 24 weeks.',
                                                        'previous_still_births': 'MSDS previous stillbirth '
                                                                                 'count.',
                                                        'record_status': 'Always active in this projection, '
                                                                         'including unmatched or '
                                                                         'invalid-pregnancy-identifier rows '
                                                                         'retained by the source. The label '
                                                                         'is not derived from IS_VALID, '
                                                                         'unmatched reason, pregnancy '
                                                                         'outcome or source presence.',
                                                        'source_system_code': 'MSDS source system.',
                                                        'source_update_timestamp': 'MSDS record-updated '
                                                                                   'timestamp.',
                                                        'unmatched_reason': 'INVALID_PREGNANCY_ID or '
                                                                            'NO_MAT_PREGNANCY_MATCH.'},
                                            'quantity_override': True,
                                            'table': 'Internal quality-controlled twin of '
                                                     'reference_pregnancy_reconciliation: the Gold repairs, '
                                                     'nulls and expectations are applied here and the '
                                                     'published Gold MV reads this object. Materialized '
                                                     'rather than temporary so the published MV can refresh '
                                                     'incrementally.'},
 'reference_pregnancy_reconciliation': {'columns': {'antenatal_appointment_date': 'MSDS booking appointment '
                                                                                  'date.',
                                                    'expected_delivery_date': 'MSDS agreed EDD.',
                                                    'folic_acid_supplement_cd': 'MSDS folic-acid supplement '
                                                                                'code.',
                                                    'last_mens_period_date': 'MSDS last menstrual period '
                                                                             'date.',
                                                    'loaded_at': 'map_mat_pregnancy_msds_unmatched.ADC_UPDT '
                                                                 'carried unchanged for the unmatched '
                                                                 'pregnancy row. No matched pregnancy-spine '
                                                                 'clock is joined or substituted; this is '
                                                                 'bronze ingestion provenance, distinct from '
                                                                 'RECORD_UPDATED_DT and Silver refresh time.',
                                                    'lpid_mother': 'Direct identifier published and '
                                                                   'IG-governed at serve time',
                                                    'msds_source_version': 'MSDS Delta version used for the '
                                                                           'snapshot.',
                                                    'pregnancy_first_contact_date': 'MSDS first '
                                                                                    'pregnancy-contact date.',
                                                    'pregnancy_id': 'MSDS pregnancy identifier after checked '
                                                                    'numeric parsing.',
                                                    'pregnancy_id_raw': 'Unchanged MSDS pregnancy '
                                                                        'identifier.',
                                                    'pregnancy_reconciliation_key': 'Deterministic SHA-256 '
                                                                                    'key for the unmatched '
                                                                                    'MSDS pregnancy row.',
                                                    'previous_caesarean_sections': 'MSDS previous-caesarean '
                                                                                   'count.',
                                                    'previous_live_births': 'MSDS previous live-birth count.',
                                                    'previous_losses_under_24_weeks': 'MSDS '
                                                                                      'terminations/losses '
                                                                                      'under 24 weeks.',
                                                    'previous_still_births': 'MSDS previous stillbirth '
                                                                             'count.',
                                                    'record_status': 'Always active in this projection, '
                                                                     'including unmatched or '
                                                                     'invalid-pregnancy-identifier rows '
                                                                     'retained by the source. The label is '
                                                                     'not derived from IS_VALID, unmatched '
                                                                     'reason, pregnancy outcome or source '
                                                                     'presence.',
                                                    'source_system_code': 'MSDS source system.',
                                                    'source_update_timestamp': 'MSDS record-updated '
                                                                               'timestamp.',
                                                    'unmatched_reason': 'INVALID_PREGNANCY_ID or '
                                                                        'NO_MAT_PREGNANCY_MATCH.'},
                                        'quantity_override': False,
                                        'table': 'Unmatched MSDS pregnancy evidence documenting a '
                                                 'person-spine coverage gap. Gold QC twin of the silver '
                                                 'product: 0 columns are repaired or nulled, 1 rule(s) drop '
                                                 'rows, 1 check(s) are advisory. Each rule states its reason '
                                                 'in the pipeline notebook, and Lakeflow expectation metrics '
                                                 'report what every rule matched on each update.'}}
