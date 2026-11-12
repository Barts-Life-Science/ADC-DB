"""Silver forms.form_config_census: 1 dataset(s).

- datasets: _clinical_form_config_census
- moved verbatim from: 6_silver_forms:6554, 6_silver_forms:88

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


from silver_core.journey_core import spark
from silver_tables.forms._shared import _n


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Reviewed form config seeds, promoted from 8_dev.s2_work to 3_lookup.omop on 2026-09-26.
# FORM_CONFIG_PROD_SOURCE_PATCH_APPLIED v1
FORM_CONFIG_CENSUS_SOURCE = spark.conf.get(
    "journey.form_config_census_source",
    "3_lookup.omop.form_config_census",
)


@materialized_view(
    name=_n("journey_clinical._form_config_census"),
    private=True,
    refresh_policy="auto",
)
def _form_config_census():
    # Build the declared dataset: Internal census of the 30 reviewed PowerForm instrument
    # configurations.
    return spark.read.table(FORM_CONFIG_CENSUS_SOURCE).select(
        "slug",
        "display",
        "source_displays",
        "elements",
        "multi_elements",
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'_clinical_form_config_census': {'columns': {'display': 'Reviewed human-readable instrument description.',
                                              'elements': 'Number of configured response elements published '
                                                          'by the instrument.',
                                              'multi_elements': 'Number of configured elements retaining '
                                                                'every active response.',
                                              'slug': 'Stable instrument slug used in the '
                                                      'clinical_form_<slug> table name.',
                                              'source_displays': 'Number of Millennium FORM_DESC_TXT values '
                                                                 'assigned to the instrument.'},
                                  'table': 'Internal census of the 30 reviewed PowerForm instrument '
                                           'configurations.'}}
