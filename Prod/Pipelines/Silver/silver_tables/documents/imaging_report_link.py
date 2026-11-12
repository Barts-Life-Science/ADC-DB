"""Silver documents.imaging_report_link: 1 dataset(s).

- datasets: clinical_imaging_report_link
- moved verbatim from: 2_silver_clinical_events:6958, silver_journey_shared.py:10099, silver_journey_shared.py:10159, silver_journey_shared.py:10160, silver_journey_shared.py:10168, silver_journey_shared.py:10189, silver_journey_shared.py:10280, silver_journey_shared.py:10388

Generated once by INSR R3 (release INSR_20261029); edit this file directly from now on.
"""


import pyspark.sql.functions as F
from silver_core.journey_core import _n, _present, read_source, spark, stable_id
from silver_tables._common.journey import (
    MILL_RADIOLOGY_EXAM_CLASS_CD,
    SRC_MILL_BLOB_TEXT,
    SRC_PACS_REPORT,
    SRC_PACS_TEXT_BRIDGE,
    SRC_RADIOLOGY_EVENT,
    _anon_output_or_null,
    _mill_blob_version_rows,
)


from silver_core.table_spec import (CrossQc, CrossQcBinding, TableCollector, ValuePolicy,
                                    ValuePolicyBinding, activate, finalise)

SPECS = []
COLLECT = TableCollector(SPECS)
activate(COLLECT)
materialized_view = COLLECT.materialized_view
dp = COLLECT


SRC_PACS_EXAM_REPORT_LINK = "4_prod.bronze.map_pacs_exam_request_report_link"


MILL_RADIOLOGY_DOCUMENT_CLASS_CD = 224


def _anon_lane(df, output_column, text_columns):
    # Approved output plus its redactor version and processing time, both NULL whenever the
    # output is NULL, so provenance never describes text that is not published.
    text = _anon_output_or_null(df, output_column, text_columns)

    def meta(c, t):
        if c not in df.columns:
            return F.lit(None).cast(t)
        return F.when(text.isNotNull(), df[c].cast(t))

    return text, meta("anon_redactor_version", "string"), meta("anon_processed_at", "timestamp")


# ---- clinical_imaging_report_link ----
IMAGING_REPORT_LINK_COLUMNS = [
    "link_key",
    "link_scope",
    "link_method",
    "accession_key",
    "sectra_accession_number",
    "member_patient_event_key",
    "document_patient_event_key",
    "document_source_feed",
    "pacs_report_id",
    "cerner_document_event_id",
    "document_version_id",
    "document_version_count",
    "document_version_decision",
    "report_resolved_ind",
    "report_datetime",
    "report_status_code",
    "text_source",
    "text_sha256",
    "text_integrity_status",
    "report_text_available_ind",
    "approved_anonymised_text_available_ind",
    "report_text_anonymised",
    "anonymisation_redactor_version",
    "anonymised_at",
    "person_id",
    "record_status",
    "loaded_at",
]


IMAGING_REPORT_LINK_IG = {c: ("0", "0") for c in IMAGING_REPORT_LINK_COLUMNS}


IMAGING_REPORT_LINK_IG.update({
    "accession_key": ("4", "2"), "sectra_accession_number": ("4", "2"),
    "person_id": ("2", "1"), "cerner_document_event_id": ("0", "1"),
    "report_datetime": ("1", "1"), "text_sha256": ("1", "1"),
    "report_text_anonymised": ("3", "2"),
})


def _document_versions():
    # Silver document versions, read once for both arms; the link never re-parses text.
    d = spark.read.table(_n("journey_text.document"))
    return d.select(
        F.col("patient_event_key").alias("_doc_key"),
        F.col("event_id").alias("_doc_event_id"),
        F.col("update_count").alias("_doc_update_count"),
        F.col("valid_from_datetime").alias("_doc_valid_from"),
        F.col("pacs_report_id").alias("_doc_pacs_report_id"),
        F.col("version_id").alias("_doc_version_id"),
        F.col("text_sha256").alias("_doc_text_sha256"),
        (F.col("document_text").isNotNull() & (F.trim(F.col("document_text")) != ""))
        .alias("_doc_has_text"),
        F.col("record_status").alias("_doc_record_status"),
        F.col("event_datetime").alias("_doc_event_datetime"),
        F.col("status_code").alias("_doc_status_code"),
        F.col("person_id").alias("_doc_person_id"),
        F.col("source_feed").alias("_doc_source_feed"),
    )


def _pacs_report_links(docs):
    lk = read_source(SRC_PACS_EXAM_REPORT_LINK).where(
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True))
        & F.col("LINK_SCOPE").isin("EXAMINATION", "REQUEST")
    )
    report = read_source(SRC_PACS_REPORT)
    native_text = report.REPORT_TEXT.isNotNull() & (F.trim(report.REPORT_TEXT) != "")
    r_anon, r_anon_version, r_anon_at = _anon_lane(report, "anon_report_text", ["REPORT_TEXT"])
    report = report.select(
        F.col("PACS_REPORT_ID").alias("_r_id"),
        native_text.alias("_r_native_text"),
        r_anon.alias("_r_anon_text"),
        r_anon_version.alias("_r_anon_version"),
        r_anon_at.alias("_r_anon_at"),
        F.coalesce(F.col("SOURCE_PRESENT_IND"), F.lit(True)).alias("_r_present"),
    )
    bridge_src = read_source(SRC_PACS_TEXT_BRIDGE)
    bridge_present = F.coalesce(bridge_src.SOURCE_PRESENT_IND, F.lit(True))
    b_anon, b_anon_version, b_anon_at = _anon_lane(
        bridge_src, "anon_bridged_text", ["BRIDGED_TEXT"])
    bridge = bridge_src.where(bridge_present).select(
        F.col("REPORT_ID").alias("_b_report_id"),
        F.col("EVENT_ID").cast("bigint").alias("_b_event_id"),
        (F.col("BRIDGED_TEXT").isNotNull() & (F.trim(F.col("BRIDGED_TEXT")) != ""))
        .alias("_b_has_text"),
        (F.col("TEXT_INTEGRITY_STATUS") if "TEXT_INTEGRITY_STATUS" in bridge_src.columns
         else F.lit(None).cast("string")).alias("_b_integrity"),
        b_anon.alias("_b_anon_text"),
        b_anon_version.alias("_b_anon_version"),
        b_anon_at.alias("_b_anon_at"),
    )
    pacs_docs = docs.where(F.col("_doc_source_feed") == F.lit("pacs_report"))
    j = (
        lk.join(report, lk.PACS_REPORT_ID == F.col("_r_id"), "left")
        .join(bridge, lk.PACS_REPORT_ID == F.col("_b_report_id"), "left")
        .join(pacs_docs, lk.PACS_REPORT_ID == F.col("_doc_pacs_report_id"), "left")
    )
    resolved = F.coalesce(F.col("REPORT_RESOLVED_IND"), F.lit(False)) & F.col("_r_id").isNotNull()
    native = F.coalesce(F.col("_r_native_text"), F.lit(False))
    bridged = ~native & F.coalesce(F.col("_b_has_text"), F.lit(False))
    scope = F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"), F.lit("examination")) \
        .otherwise(F.lit("accession"))
    member = F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"),
                    stable_id("imaging_exam:pacs", F.col("PACS_EXAMINATION_ID")))
    accession_key = F.upper(F.trim(F.col("SECTRA_ACCESSION_NBR")))
    retracted = ~F.coalesce(F.col("_r_present"), F.lit(True))
    native_anon = native & F.col("_r_anon_text").isNotNull()
    bridged_anon = bridged & F.col("_b_anon_text").isNotNull()
    approved = resolved & ~retracted & (native_anon | bridged_anon)
    return j.select(
        stable_id("imaging_report_link", scope, F.coalesce(member, accession_key),
                  F.lit("pacs_report"), F.col("PACS_REPORT_ID")).alias("link_key"),
        scope.alias("link_scope"),
        F.when(F.col("LINK_SCOPE") == F.lit("EXAMINATION"), F.lit("PACS_EXAMINATION_REPORT"))
        .otherwise(F.lit("PACS_REQUEST_REPORT")).alias("link_method"),
        accession_key.alias("accession_key"),
        F.col("SECTRA_ACCESSION_NBR").alias("sectra_accession_number"),
        member.alias("member_patient_event_key"),
        F.when(resolved, F.col("_doc_key")).alias("document_patient_event_key"),
        F.lit("pacs_report").alias("document_source_feed"),
        F.col("PACS_REPORT_ID").cast("bigint").alias("pacs_report_id"),
        F.when(bridged, F.col("_b_event_id")).alias("cerner_document_event_id"),
        F.col("_doc_version_id").alias("document_version_id"),
        F.when(resolved, F.lit(1)).cast("int").alias("document_version_count"),
        F.when(~resolved, F.lit(None).cast("string"))
        .when(native, F.lit("native_report"))
        .when(bridged, F.lit("single_current"))
        .otherwise(F.lit("no_current_version")).alias("document_version_decision"),
        resolved.alias("report_resolved_ind"),
        F.col("REPORT_DT_TM").alias("report_datetime"),
        F.col("REPORT_STATUS_CD").cast("string").alias("report_status_code"),
        F.when(resolved & native, F.lit("pacs_native"))
        .when(resolved & bridged, F.lit("mill_blob_text_bridge")).alias("text_source"),
        F.when(resolved & (native | bridged), F.col("_doc_text_sha256")).alias("text_sha256"),
        F.when(bridged, F.col("_b_integrity")).alias("text_integrity_status"),
        (resolved & (native | bridged) & ~retracted).alias("report_text_available_ind"),
        approved.alias("approved_anonymised_text_available_ind"),
        F.when(approved & native_anon, F.col("_r_anon_text"))
        .when(approved & bridged_anon, F.col("_b_anon_text")).alias("report_text_anonymised"),
        F.when(approved & native_anon, F.col("_r_anon_version"))
        .when(approved & bridged_anon, F.col("_b_anon_version"))
        .alias("anonymisation_redactor_version"),
        F.when(approved & native_anon, F.col("_r_anon_at"))
        .when(approved & bridged_anon, F.col("_b_anon_at")).alias("anonymised_at"),
        F.coalesce(F.col("_doc_person_id"), F.col("EXAMINATION_PERSON_ID")).cast("bigint")
        .alias("person_id"),
        F.when(retracted, F.lit("retracted")).otherwise(F.lit("active")).alias("record_status"),
        F.col("ADC_UPDT").alias("loaded_at"),
    )


def _cerner_report_links(docs):
    r = read_source(SRC_RADIOLOGY_EVENT)
    exams = r.where(
        (F.col("EVENT_CLASS_CD").cast("bigint") == F.lit(MILL_RADIOLOGY_EXAM_CLASS_CD))
        & _present(F.col("REF_EXAM_KEY"))
    ).select(
        F.col("EVENT_ID").alias("_exam_event_id"), F.col("REF_EXAM_KEY").alias("_k"),
        F.col("PERSON_ID").alias("_exam_person"), F.col("SECTRA_ACCESSION_NBR").alias("_acc"),
    )
    doc_events = r.where(
        (F.col("EVENT_CLASS_CD").cast("bigint") == F.lit(MILL_RADIOLOGY_DOCUMENT_CLASS_CD))
        & ~F.coalesce(F.col("IN_ERROR_IND"), F.lit(False))
        & _present(F.col("REF_EXAM_KEY"))
    ).select(
        F.col("EVENT_ID").alias("_doc_evt"), F.col("REF_EXAM_KEY").alias("_k"),
        F.col("PERSON_ID").alias("_doc_evt_person"),
        F.col("PIPELINE_UPDT_DT_TM").alias("_doc_loaded_at"),
    )
    # Identity-compatible siblings only, the same rule as the bronze pointer; conflicting
    # pairs are counted in bronze (REPORT_IDENTITY_CONFLICT_COUNT) and never linked.
    pairs = exams.join(doc_events, "_k").where(
        F.col("_exam_person").isNull() | F.col("_doc_evt_person").isNull()
        | (F.col("_exam_person") == F.col("_doc_evt_person"))
    )
    current = docs.where(
        (F.col("_doc_source_feed") == F.lit("mill_blob_text"))
        & (F.col("_doc_record_status") == F.lit("active"))
    )
    # Multi-current blob versions stay visible: count them and their distinct texts. No
    # timestamp-only winner is picked (the silver is_latest_version flag is not used here).
    version_stats = current.groupBy("_doc_event_id").agg(
        F.count(F.lit(1)).alias("_versions"),
        F.countDistinct(F.when(F.col("_doc_has_text"), F.col("_doc_text_sha256")))
        .alias("_distinct_texts"),
    )
    # SILVER_RELEASE_20261025 item 2: anonymised text comes from the same collapsed decoded version row the
    # document lane published, so the (event, update count, valid-from) join below is one-to-one. Raw
    # mill_blob_text can hold several rows per version key (blob version-dup defect), which repeated link_key.
    # Only radiology report events are collapsed: narrowing by EVENT_ID keeps every row of a kept key.
    report_events = doc_events.select(F.col("_doc_evt").cast("bigint").alias("_blob_report_event_id"))
    decoded = read_source(SRC_MILL_BLOB_TEXT).where(F.col("STATUS") == F.lit("Decoded"))
    decoded = decoded.join(report_events, decoded["EVENT_ID"].cast("bigint") == F.col("_blob_report_event_id"),
                           "left_semi")
    # anon_processed_at is published as anonymised_at: carry it unconverted (the JSON round trip keeps milliseconds).
    blob = _mill_blob_version_rows(decoded, native_columns=("anon_processed_at",))
    a_anon, a_anon_version, a_anon_at = _anon_lane(blob, "anon_text", ["BLOB_TEXT"])
    blob_anon = blob.select(
        F.col("EVENT_ID").cast("bigint").alias("_a_event_id"),
        F.col("UPDT_CNT").cast("bigint").alias("_a_update_count"),
        F.col("VALID_FROM_DT_TM").alias("_a_valid_from"),
        a_anon.alias("_a_anon_text"),
        a_anon_version.alias("_a_anon_version"),
        a_anon_at.alias("_a_anon_at"),
    )
    j = (
        pairs.join(current, pairs._doc_evt == current._doc_event_id, "left")
        .join(version_stats.withColumnRenamed("_doc_event_id", "_vs_event_id"),
              F.col("_doc_evt") == F.col("_vs_event_id"), "left")
        .join(blob_anon,
              (F.col("_doc_event_id") == F.col("_a_event_id"))
              & (F.col("_doc_update_count") == F.col("_a_update_count"))
              & (F.col("_doc_valid_from") == F.col("_a_valid_from")), "left")
    )
    versions = F.coalesce(F.col("_versions"), F.lit(0))
    decision = (
        F.when(versions == 0, F.lit("no_current_version"))
        .when(F.col("_distinct_texts") > 1, F.lit("text_version_ambiguous"))
        .when(versions == 1, F.lit("single_current"))
        .otherwise(F.lit("identical_text_collapsed"))
    )
    certified = decision.isin("single_current", "identical_text_collapsed") \
        & F.coalesce(F.col("_doc_has_text"), F.lit(False))
    approved = certified & F.col("_a_anon_text").isNotNull()
    member = stable_id("imaging_exam:mill", F.col("_exam_event_id"))
    return j.select(
        stable_id("imaging_report_link", F.lit("direct_cerner_event"), member,
                  F.lit("mill_blob_text"), F.col("_doc_evt"),
                  F.coalesce(F.col("_doc_key"), F.lit("NONE"))).alias("link_key"),
        F.lit("direct_cerner_event").alias("link_scope"),
        F.lit("CERNER_REF_EXAM_KEY").alias("link_method"),
        F.upper(F.trim(F.col("_acc"))).alias("accession_key"),
        F.col("_acc").alias("sectra_accession_number"),
        member.alias("member_patient_event_key"),
        F.col("_doc_key").alias("document_patient_event_key"),
        F.lit("mill_blob_text").alias("document_source_feed"),
        F.lit(None).cast("bigint").alias("pacs_report_id"),
        F.col("_doc_evt").cast("bigint").alias("cerner_document_event_id"),
        F.col("_doc_version_id").alias("document_version_id"),
        versions.cast("int").alias("document_version_count"),
        decision.alias("document_version_decision"),
        F.col("_doc_key").isNotNull().alias("report_resolved_ind"),
        F.col("_doc_event_datetime").alias("report_datetime"),
        F.col("_doc_status_code").alias("report_status_code"),
        F.when(certified, F.lit("mill_blob_text")).alias("text_source"),
        F.when(certified, F.col("_doc_text_sha256")).alias("text_sha256"),
        F.lit(None).cast("string").alias("text_integrity_status"),
        certified.alias("report_text_available_ind"),
        F.coalesce(approved, F.lit(False)).alias("approved_anonymised_text_available_ind"),
        F.when(approved, F.col("_a_anon_text")).alias("report_text_anonymised"),
        F.when(approved, F.col("_a_anon_version")).alias("anonymisation_redactor_version"),
        F.when(approved, F.col("_a_anon_at")).alias("anonymised_at"),
        F.coalesce(F.col("_doc_person_id"), F.col("_doc_evt_person")).cast("bigint")
        .alias("person_id"),
        F.lit("active").alias("record_status"),
        F.col("_doc_loaded_at").alias("loaded_at"),
    )


def _imaging_report_link_canonical():
    """Relational imaging report links: every examination/accession-to-document relationship.

    Multiple reports and addenda stay separate rows. Dangling PACS report ids are kept with
    report_resolved_ind false and never make a report available.
    """
    docs = _document_versions()
    return (
        _pacs_report_links(docs).select(*IMAGING_REPORT_LINK_COLUMNS)
        .unionByName(_cerner_report_links(docs).select(*IMAGING_REPORT_LINK_COLUMNS))
    )


@materialized_view(
    name=_n("journey_clinical.imaging_report_link"),
    cluster_by=["accession_key"],
    refresh_policy="auto",
)
def imaging_report_link():
    # Build the declared dataset: imaging report links over silver text_document versions.
    return _imaging_report_link_canonical().select(*IMAGING_REPORT_LINK_COLUMNS)


VALUE_POLICIES = {}


WRAP_POLICY = frozenset()


finalise(SPECS, VALUE_POLICIES, WRAP_POLICY)


COMMENTS = {'clinical_imaging_report_link': {'columns': {'accession_key': 'upper(trim(sectra_accession_number)); joins '
                                                               'clinical_imaging_accession_member. '
                                                               'Identifier.',
                                              'anonymisation_redactor_version': 'anon_redactor_version of '
                                                                                'the lane row that produced '
                                                                                'report_text_anonymised; '
                                                                                'NULL when '
                                                                                'report_text_anonymised is '
                                                                                'NULL.',
                                              'anonymised_at': 'anon_processed_at of the lane row that '
                                                               'produced report_text_anonymised; NULL when '
                                                               'report_text_anonymised is NULL.',
                                              'approved_anonymised_text_available_ind': 'The approved '
                                                                                        'anonymisation lane '
                                                                                        'holds output for '
                                                                                        'the current text '
                                                                                        '(anon_status '
                                                                                        'anonymized and '
                                                                                        'matching source '
                                                                                        'hash). False when '
                                                                                        'missing or stale; '
                                                                                        'never inferred from '
                                                                                        'raw text.',
                                              'cerner_document_event_id': 'Cerner report document EVENT_ID: '
                                                                          'the REF_EXAM_KEY sibling for '
                                                                          'Cerner links, or the bridge '
                                                                          'EVENT_ID supplying a PACS '
                                                                          "report's text.",
                                              'document_patient_event_key': 'text_document patient_event_key '
                                                                            'of the linked document version; '
                                                                            'NULL for a dangling PACS report '
                                                                            'id.',
                                              'document_source_feed': 'pacs_report or mill_blob_text.',
                                              'document_version_count': 'Cerner links: current (active) '
                                                                        'silver document versions for the '
                                                                        'event. 1 for PACS links.',
                                              'document_version_decision': 'single_current, '
                                                                           'identical_text_collapsed '
                                                                           '(several current versions, one '
                                                                           'text), text_version_ambiguous '
                                                                           '(conflicting current texts: no '
                                                                           'text is certified) or '
                                                                           'no_current_version. PACS native '
                                                                           'reports: native_report.',
                                              'document_version_id': 'Silver document version_id of the '
                                                                     'linked version.',
                                              'link_key': 'Deterministic SHA-256 over scope, '
                                                          'member/accession and document identity.',
                                              'link_method': 'PACS_EXAMINATION_REPORT, PACS_REQUEST_REPORT '
                                                             'or CERNER_REF_EXAM_KEY.',
                                              'link_scope': 'examination (Sectra report attached to one '
                                                            'examination), direct_cerner_event (Cerner '
                                                            "report document sharing the examination's "
                                                            'REF_EXAM_KEY), or accession (Sectra report '
                                                            'attached to the request only; '
                                                            'member_patient_event_key is NULL).',
                                              'loaded_at': 'Bronze load clock of the link source row.',
                                              'member_patient_event_key': 'clinical_imaging_exam / accession '
                                                                          'member patient_event_key; NULL '
                                                                          'for accession-scope links.',
                                              'pacs_report_id': 'Sectra ReportId (PACS links). May be '
                                                                'dangling (report_resolved_ind false).',
                                              'person_id': 'Report/document person when resolved.',
                                              'record_status': 'Link record status: active, or the linked '
                                                               'document/report record status when that is '
                                                               'not active.',
                                              'report_datetime': 'Report time (Sectra REPORT_DT_TM; Cerner '
                                                                 'document event_datetime).',
                                              'report_resolved_ind': 'True when the report resolves to a '
                                                                     'real report/document row. A dangling '
                                                                     'id never makes a report available.',
                                              'report_status_code': 'Source report status code.',
                                              'report_text_anonymised': 'Approved-lane anonymised report '
                                                                        'text for this link: '
                                                                        'map_pacs_report.anon_report_text '
                                                                        '(pacs_native), '
                                                                        'map_pacs_report_text_bridge.anon_bridged_text '
                                                                        '(mill_blob_text_bridge) or '
                                                                        'mill_blob_text.anon_text for the '
                                                                        'linked version (mill_blob_text). '
                                                                        'Set exactly when '
                                                                        'approved_anonymised_text_available_ind '
                                                                        'is true; never falls back to raw '
                                                                        'text.',
                                              'report_text_available_ind': 'Non-empty report text is '
                                                                           'available for this link and not '
                                                                           'version-ambiguous.',
                                              'sectra_accession_number': 'Accession of the linked '
                                                                         'examination or request. '
                                                                         'Identifier.',
                                              'text_integrity_status': 'Bridge v4 TEXT_INTEGRITY_STATUS for '
                                                                       'bridged PACS text; NULL otherwise.',
                                              'text_sha256': 'SHA-256 of the linked document text (silver '
                                                             'text_document.text_sha256).',
                                              'text_source': 'pacs_native, mill_blob_text_bridge (bridge v4, '
                                                             'version-safe), mill_blob_text, or NULL when no '
                                                             'text.'},
                                  'table': 'Relational imaging report links: examination-, request- and '
                                           'Cerner-event-scoped report documents with version decisions, '
                                           'text and approved-anonymisation availability. Multiple reports '
                                           'and addenda stay separate rows.'}}
