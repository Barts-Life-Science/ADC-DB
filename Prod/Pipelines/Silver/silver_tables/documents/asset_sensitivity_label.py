"""Silver documents.asset_sensitivity_label: 1 dataset(s).

- datasets: artifact_asset_sensitivity_label
- moved verbatim from: 5_silver_documents:273, 5_silver_documents:288

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n
from silver_tables._common.journey import _artifact_asset_canonical


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


# Artifact / asset sensitivity label

# ==== journey_artifact.asset_sensitivity_label ====
ARTIFACT_ASSET_SENSITIVITY_LABEL_PUBLIC_COLUMNS = [
    "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
    "sequence", "sensitivity_label", "loaded_at",
]


@materialized_view(
    name=_n("journey_artifact.asset_sensitivity_label"),
    refresh_policy="auto",
)
def artifact_asset_sensitivity_label():
    # Build the declared dataset: One ordered sensitivity label per governed artifact asset.
    source = _artifact_asset_canonical().select(
        "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
        "_sensitivity_labels_json", "loaded_at",
    )
    return (
        source.select(
            "patient_event_key", "pacs_examination_id", "dicom_path",
            "artifact_key", "loaded_at",
            F.posexplode_outer(
                F.from_json(F.col("_sensitivity_labels_json"), "array<string>")
            ).alias("_position", "sensitivity_label"),
        )
        .where(F.col("sensitivity_label").isNotNull())
        .select(
            "patient_event_key", "pacs_examination_id", "dicom_path", "artifact_key",
            (F.col("_position") + 1).cast("long").alias("sequence"),
            F.col("sensitivity_label").cast("string").alias("sensitivity_label"),
            "loaded_at",
        )
        .select(*ARTIFACT_ASSET_SENSITIVITY_LABEL_PUBLIC_COLUMNS)
    )


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'artifact_asset_sensitivity_label': {'columns': {'artifact_key': 'Deterministic artifact key carried by the '
                                                                  'parent.',
                                                  'dicom_path': 'DICOM_PATH for the dicom_file parent arm.',
                                                  'loaded_at': 'Bronze load timestamp inherited from the '
                                                               'parent source row.',
                                                  'pacs_examination_id': 'PACS_EXAMINATION_ID for the '
                                                                         'pacs_study parent arm.',
                                                  'patient_event_key': 'Deterministic key of the parent '
                                                                       'artifact_asset row.',
                                                  'sensitivity_label': 'Verbatim source sensitivity or '
                                                                       'security label.',
                                                  'sequence': 'One-based order of the source sensitivity '
                                                              'label.'},
                                      'table': 'One ordered sensitivity label per governed artifact asset.'}}
