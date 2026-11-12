"""Silver activity._shared: 0 dataset(s).

- shared helpers (no datasets)
- moved verbatim from: 4_silver_care_pathways:4653, 4_silver_care_pathways:962

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import read_source
from silver_tables._common.journey import SRC_THEATRE_CASE


# Clinical /  appointment schedule grouped
SRC_APPOINTMENT_SCHEDULE = "4_prod.bronze.map_appointment_schedule"


# Reference / theatre attendance
def _theatre_parent_keys():
    # Read distinct surgical-case identifiers used to validate links from theatre child records.
    return read_source(SRC_THEATRE_CASE).select(
        F.col("SURG_CASE_ID").alias("_parent_surg_case_id")
    ).distinct()
