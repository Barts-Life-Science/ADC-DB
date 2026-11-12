"""R1 hand-ported bronze input projections: the arms whose silver projection joins another table (mediconnect device
type map) or has no bridge at all (presenting complaint, three arms). Every function cites the silver lines it ports;
the per-row parity harness (r1/dev/R1_Dev_Parity.py) is the proof that they agree with silver."""
import pyspark.sql.functions as F

import mapping_inputs as GEN
from mapping_chain import read_source

SRC_MEDICONNECT_TYPE_MAP = "3_lookup.omop.mediconnect_device_type_map"   # silver_journey_shared.py:8294


def arm__mediconnect_device__device(s):
    """Port of _device_canonical_pregate (silver_journey_shared.py:8198-8248): the bridge joins the device-type map.

    The map is joined here, in the key projection, so a map change changes keys; mediconnect_device (37k rows) is
    rebuilt weekly with an inline attach and re-keyed then (D1)."""
    d = s
    m = read_source(SRC_MEDICONNECT_TYPE_MAP).select(
        F.col("DEVICE_TYPE").cast("string").alias("_m_device_type"),
        F.col("DEVICE_ROLE").alias("_m_device_role"),
        F.col("SNOMED_CONCEPT_ID").cast("string").alias("_m_snomed_code"),
        F.col("SNOMED_CONCEPT_NAME").alias("_m_snomed_display"),
        F.col("MAPPING_STATUS").alias("_m_mapping_status"),
    )
    s = d.join(F.broadcast(m), d.DEVICE_TYPE.cast("string") == m._m_device_type, "left")
    raw_type = F.when(s.DEVICE_TYPE.cast("long") != 0, s.DEVICE_TYPE.cast("string"))
    source_display = F.coalesce(s.TYPE, s.MODEL_NAME)
    source_code = GEN._code_or_display(raw_type, source_display)
    snomed_code = F.coalesce(s.DEVICE_SNOMED_CONCEPT_ID.cast("string"), s._m_snomed_code)
    snomed_display = F.coalesce(s.DEVICE_SNOMED_CONCEPT_NAME, s._m_snomed_display)
    bridge = GEN.codeable_concept_json(
        GEN.coding_obj(F.lit("urn:barts:mediconnect:device-type"), source_code, source_display, True),
        GEN.coding_obj(F.lit("http://snomed.info/sct"), snomed_code, snomed_display, False,
                       "lookup.mediconnect_device_type_map:applied", None),
    )
    return s.select(*[d[c] for c in d.columns], bridge.alias("_r1_bridge"), F.lit(True).alias("_r1_in_scope"))


def _complaint(s, scope, source_object, coding_system, text, keep_code):
    """The common shape of the three complaint arms (silver_journey_shared.py:1925-1997): normalised text, and the
    INSR_W2 W2.04 rule (:1991-1995) that free-text arms keep source_code NULL and carry the text instead."""
    normalised = F.lower(F.trim(F.regexp_replace(text, r"\s+", " ")))
    return s.select("*", F.coalesce(scope, F.lit(False)).alias("_r1_in_scope"),
                    F.lit(source_object).alias("_r1_in_source_object"),
                    F.lit(coding_system).alias("_r1_in_source_coding_system"),
                    (normalised if keep_code else F.lit(None).cast("string")).alias("_r1_in_source_code"),
                    text.alias("_r1_in_source_display"),
                    normalised.alias("_r1_in_complaint_text_normalised"))


def arm__map_encounter__presenting_complaint(s):
    """rfv arm (:1931-1945): rows with a non-blank REASON_FOR_VISIT; free text, so no source_code."""
    return _complaint(s, F.length(F.trim(s.REASON_FOR_VISIT)) > 0, "registration_reason_for_visit",
                      "urn:cerner:encounter:reason-for-visit", s.REASON_FOR_VISIT, keep_code=False)


def arm__map_nomen_events__presenting_complaint(s):
    """ED nomenclature arm (:1947-1965): EVENT_CD 3844529 with a SOURCE_STRING; a picklist code, kept."""
    return _complaint(s, (s.EVENT_CD == 3844529) & s.SOURCE_STRING.isNotNull(), "ed_presenting_complaint",
                      "urn:cerner:nomenclature:ed-presenting-complaint", s.SOURCE_STRING, keep_code=True)


def arm__map_text_events__presenting_complaint(s):
    """ED free-text arm (:1967-1985): EVENT_CD 3844529, 1-120 characters; free text, so no source_code."""
    return _complaint(s, (s.EVENT_CD == 3844529) & F.length(F.trim(s.TEXT_RESULT)).between(1, 120),
                      "ed_presenting_complaint_text", "urn:cerner:clinical-event:presenting-complaint-text",
                      s.TEXT_RESULT, keep_code=False)


# Bronze SNOMED provenance read from the row itself (replaces the per-row re-join of
# _s3_apply_bronze_snomed_provenance, silver_journey_shared.py:772-812). Arms without these columns get NULLs, which
# is what the left join gave them (their id never matched).
META_COLUMNS = {"map_diagnosis", "map_problem", "map_procedure", "map_family_history"}


def meta_extras(s, table):
    """_r1_in__s3_meta_type / _similarity / _candidates from SNOMED_TYPE / SNOMED_SIMILARITY / SNOMED_MATCH_NUMBER."""
    if table in META_COLUMNS:
        return s.select("*", F.col("SNOMED_TYPE").alias("_r1_in__s3_meta_type"),
                        F.col("SNOMED_SIMILARITY").cast("double").alias("_r1_in__s3_meta_similarity"),
                        F.col("SNOMED_MATCH_NUMBER").cast("int").alias("_r1_in__s3_meta_candidates"))
    return s.select("*", F.lit(None).cast("string").alias("_r1_in__s3_meta_type"),
                    F.lit(None).cast("double").alias("_r1_in__s3_meta_similarity"),
                    F.lit(None).cast("int").alias("_r1_in__s3_meta_candidates"))


MANUAL = {"arm__mediconnect_device__device": arm__mediconnect_device__device,
          "arm__map_encounter__presenting_complaint": arm__map_encounter__presenting_complaint,
          "arm__map_nomen_events__presenting_complaint": arm__map_nomen_events__presenting_complaint,
          "arm__map_text_events__presenting_complaint": arm__map_text_events__presenting_complaint}
