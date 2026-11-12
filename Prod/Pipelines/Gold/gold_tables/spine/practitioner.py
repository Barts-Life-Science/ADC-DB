"""Gold spine.practitioner: 2 dataset(s).

- datasets: _qc_reference_practitioner, reference_practitioner
- moved verbatim from: 1_gold_spine_and_reference:3825, 1_gold_spine_and_reference:3852, 1_gold_spine_and_reference:3893, 1_gold_spine_and_reference:3904

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from gold_core.gold_core import _n, spark
from gold_core.gold_spec import DatasetCollector, GoldRules, QcBinding, activate, finalise


GOLD_SPECS = []
dp = DatasetCollector(GOLD_SPECS)
activate(dp)


# ==== journey_reference.practitioner ====

# contract v2: identifier names follow the native-id contract; lifecycle/QC and extracted child payloads are omitted.
REFERENCE_PRACTITIONER_SELECT = [
    '`practitioner_key` AS `practitioner_key`',
    '`practitioner_id` AS `practitioner_id`',
    '`source_practitioner_id` AS `source_practitioner_id`',
    '`active` AS `active`',
    '`name` AS `name`',
    '`physician_ind` AS `physician_ind`',
    "CASE WHEN UPPER(TRIM(CAST(`position_code` AS STRING))) = '0' THEN NULL ELSE `position_code` END AS `position_code`",
    '`position_display` AS `position_display`',
    "CASE WHEN UPPER(TRIM(CAST(`practitioner_type_code` AS STRING))) = '0' THEN NULL ELSE `practitioner_type_code` END AS `practitioner_type_code`",
    '`practitioner_type_display` AS `practitioner_type_display`',
    '`primary_location_key` AS `primary_location_key`',
    '`medical_service_key` AS `medical_service_key`',
    '`npi` AS `npi`',
    '`doctor_number` AS `doctor_number`',
    '`gdp_number` AS `gdp_number`',
    '`external_provider_id` AS `external_provider_id`',
    '`valid_from` AS `valid_from`',
    # INSR_W3 W3.07 (F4): open-end marker = any date in [2099-12-31, 9999); a begin date in 2100+ is invalid.
    "CASE WHEN `valid_from` IS NOT NULL AND `valid_to` IS NOT NULL AND `valid_from` > `valid_to` THEN NULL ELSE CASE WHEN (CAST(`valid_to` AS DATE) >= DATE'2099-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999) OR CAST(`valid_to` AS DATE) > DATE'2100-12-31' AND YEAR(CAST(`valid_to` AS DATE)) < 9999 OR CAST(`valid_to` AS DATE) = DATE'2100-01-01' THEN NULL ELSE `valid_to` END END AS `valid_to`",
    '`source_table` AS `source_table`',
    '`source_row_id` AS `source_row_id`',
    '`record_status` AS `record_status`',
    '`loaded_at` AS `loaded_at`',
]


# contract v2: QC/batch inputs come from internal _reference_practitioner_metadata; source history stays on the main research table.
REFERENCE_PRACTITIONER_ADVISORY_RULES = {
    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 2 of 351,201 rows
    # (0.000569%) when profiled on 2026-08-24.
    "gold.reference.practitioner.valid_from.future_owner":
        "NOT COALESCE((CAST(`valid_from` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",

    # This bounds a period of validity, and a future end is exactly how the source says a
    # record is still current -- nulling it would assert the record is valid forever, which
    # is a stronger and worse claim than the one being corrected. Seen on 373 of 351,201
    # rows (0.106%) when profiled on 2026-08-24.
    "gold.reference.practitioner.valid_to.future_owner":
        "NOT COALESCE((CAST(`valid_to` AS TIMESTAMP) > `loaded_at` + INTERVAL 90 DAYS), FALSE)",
}


@dp.materialized_view(
    name=_n("gold_qc._reference_practitioner"),
    refresh_policy="auto",
)
@dp.expect_all(REFERENCE_PRACTITIONER_ADVISORY_RULES)
def _gold_qc_reference_practitioner():
    """Quality-controlled twin of journey_reference.practitioner."""
    df = _qc("reference_practitioner", REFERENCE_PRACTITIONER_SELECT)
    return df


@dp.materialized_view(
    name=_n("gold_reference.practitioner"),
    table_properties={"quality": "gold"},
    refresh_policy="auto",
)
def gold_reference_practitioner():
    """Contract-v2 public twin of reference_practitioner; QC support columns remain internal."""
    df = spark.read.table(_n("gold_qc._reference_practitioner")).selectExpr(
        '`practitioner_key` AS `practitioner_key`',
        '`practitioner_id` AS `practitioner_id`',
        '`source_practitioner_id` AS `source_practitioner_id`',
        '`active` AS `active`',
        '`name` AS `name`',
        '`physician_ind` AS `physician_ind`',
        '`position_code` AS `position_code`',
        '`position_display` AS `position_display`',
        '`practitioner_type_code` AS `practitioner_type_code`',
        '`practitioner_type_display` AS `practitioner_type_display`',
        '`primary_location_key` AS `primary_location_key`',
        '`medical_service_key` AS `medical_service_key`',
        '`npi` AS `npi`',
        '`doctor_number` AS `doctor_number`',
        '`gdp_number` AS `gdp_number`',
        '`external_provider_id` AS `external_provider_id`',
        '`valid_from` AS `valid_from`',
        '`valid_to` AS `valid_to`',
        '`source_table` AS `source_table`',
        '`source_row_id` AS `source_row_id`',
        '`record_status` AS `record_status`',
        '`loaded_at` AS `loaded_at`',
    )
    return df


RULES = {
    'reference_practitioner': GoldRules(
        table='reference_practitioner',
        select=REFERENCE_PRACTITIONER_SELECT,
        advisory=REFERENCE_PRACTITIONER_ADVISORY_RULES,
        lifecycle={'join_keys': ['practitioner_key'],
         'support_columns': ['record_status', 'load_batch_id', 'loaded_at']}),
}
_rules = QcBinding(RULES)
_qc = _rules._qc


finalise()


COMMENTS = {'_qc_reference_practitioner': {'columns': {'active': 'Source active indicator.',
                                            'doctor_number': 'Selected organization doctor number.',
                                            'external_provider_id': 'Selected external provider identifier.',
                                            'gdp_number': 'Selected dental practitioner number.',
                                            'loaded_at': 'Directory rows carry '
                                                         'map_medical_personnel.ADC_UPDT only; '
                                                         'clinical-reference clocks are not added to known '
                                                         'personnel. For personnel absent from that '
                                                         'directory, placeholders use the maximum clock '
                                                         'across their contributing encounter, diagnosis, '
                                                         'problem, implant, form, event, medication, text, '
                                                         'appointment and theatre references. Implant '
                                                         'references first coalesce SOURCE_MAX_ADC_UPDT with '
                                                         'ADC_UPDT; other references use ADC_UPDT. This is '
                                                         'arm-specific ingestion provenance, not a uniform '
                                                         'implant clock or Silver refresh time.',
                                            'medical_service_key': 'Deterministic key of the selected '
                                                                   'medical service.',
                                            'name': 'Source formatted practitioner name.',
                                            'npi': 'Selected NPI identifier.',
                                            'physician_ind': 'Source physician indicator.',
                                            'position_code': 'Source position code.',
                                            'position_display': 'Source position display.',
                                            'practitioner_id': 'Millennium personnel PERSON_ID; primary key '
                                                               'of this table.',
                                            'practitioner_key': 'Deterministic SHA-256 practitioner key.',
                                            'practitioner_type_code': 'Source personnel type code.',
                                            'practitioner_type_display': 'Source personnel type display.',
                                            'primary_location_key': 'Deterministic key of the primary '
                                                                    'assigned or inferred location.',
                                            'record_status': 'Directory rows are active when '
                                                             'map_medical_personnel.ACTIVE_IND equals 1, '
                                                             'otherwise superseded, including when the flag '
                                                             'is null. Personnel referenced by clinical '
                                                             'records but absent from the directory are '
                                                             'published as placeholders with literal active '
                                                             'status. Neither arm emits retracted; '
                                                             'placeholder active status is not evidence of '
                                                             'an active personnel-directory record.',
                                            'source_practitioner_id': 'Millennium personnel PERSON_ID as '
                                                                      'BIGINT.',
                                            'source_row_id': 'Stable source row identifier.',
                                            'source_table': 'Fully qualified bronze source table.',
                                            'valid_from': 'Source validity start.',
                                            'valid_to': 'Source validity end.'},
                                'quantity_override': True,
                                'table': 'Internal quality-controlled twin of reference_practitioner: the '
                                         'Gold repairs, nulls and expectations are applied here and the '
                                         'published Gold MV reads this object. Materialized rather than '
                                         'temporary so the published MV can refresh incrementally.'},
 'reference_practitioner': {'columns': {'active': 'Source active indicator.',
                                        'doctor_number': 'Selected organization doctor number.',
                                        'external_provider_id': 'Selected external provider identifier.',
                                        'gdp_number': 'Selected dental practitioner number.',
                                        'loaded_at': 'Directory rows carry map_medical_personnel.ADC_UPDT '
                                                     'only; clinical-reference clocks are not added to known '
                                                     'personnel. For personnel absent from that directory, '
                                                     'placeholders use the maximum clock across their '
                                                     'contributing encounter, diagnosis, problem, implant, '
                                                     'form, event, medication, text, appointment and theatre '
                                                     'references. Implant references first coalesce '
                                                     'SOURCE_MAX_ADC_UPDT with ADC_UPDT; other references '
                                                     'use ADC_UPDT. This is arm-specific ingestion '
                                                     'provenance, not a uniform implant clock or Silver '
                                                     'refresh time.',
                                        'medical_service_key': 'Deterministic key of the selected medical '
                                                               'service.',
                                        'name': 'Source formatted practitioner name.',
                                        'npi': 'Selected NPI identifier.',
                                        'physician_ind': 'Source physician indicator.',
                                        'position_code': 'Source position code.',
                                        'position_display': 'Source position display.',
                                        'practitioner_id': 'Millennium personnel PERSON_ID; primary key of '
                                                           'this table.',
                                        'practitioner_key': 'Deterministic SHA-256 practitioner key.',
                                        'practitioner_type_code': 'Source personnel type code.',
                                        'practitioner_type_display': 'Source personnel type display.',
                                        'primary_location_key': 'Deterministic key of the primary assigned '
                                                                'or inferred location.',
                                        'record_status': 'Directory rows are active when '
                                                         'map_medical_personnel.ACTIVE_IND equals 1, '
                                                         'otherwise superseded, including when the flag is '
                                                         'null. Personnel referenced by clinical records but '
                                                         'absent from the directory are published as '
                                                         'placeholders with literal active status. Neither '
                                                         'arm emits retracted; placeholder active status is '
                                                         'not evidence of an active personnel-directory '
                                                         'record.',
                                        'source_practitioner_id': 'Millennium personnel PERSON_ID as BIGINT.',
                                        'source_row_id': 'Stable source row identifier.',
                                        'source_table': 'Fully qualified bronze source table.',
                                        'valid_from': 'Source validity start.',
                                        'valid_to': 'Source validity end.'},
                            'quantity_override': False,
                            'table': 'One effective-dated Millennium practitioner or personnel record. Gold '
                                     'QC twin of the silver product: 3 columns are repaired or nulled, 2 '
                                     'check(s) are advisory. Each rule states its reason in the pipeline '
                                     'notebook, and Lakeflow expectation metrics report what every rule '
                                     'matched on each update.'}}
