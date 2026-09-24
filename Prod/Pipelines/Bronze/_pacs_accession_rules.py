# Databricks notebook source
# MAGIC %md
# MAGIC # PACS / Cerner accession rules (shared)
# MAGIC
# MAGIC One definition of the Sectra accession parser, `%run` by `pacs_pipeline` and
# MAGIC `radiology_event_pipeline` so both sides of the PACS <-> Millennium join classify
# MAGIC identifiers identically. Pure Spark SQL expressions (no UDFs), portable between
# MAGIC Photon and the JVM engine: no regex `.` wildcard anywhere.
# MAGIC
# MAGIC Rules (anchored, longest format first; a format must be followed by a non-digit or
# MAGIC the end of the value, so no digit run is ever cut short):
# MAGIC
# MAGIC | Format | Pattern | Accession |
# MAGIC |---|---|---|
# MAGIC | SECTRA_16 | `UK` + 3-letter site + 11 digits (`UKSBH02003761858`) | first 16 |
# MAGIC | SITE_16 | 3 letters + 13 digits (`APT…`) | first 16 |
# MAGIC | RNH_14 | `RNH` + digit + 2 letters + 8 digits (`RNH0XR…`) | first 14 |
# MAGIC | OTHER_SITE_14 | 3 letters + 11 digits (`WXH02…`) | first 14 |
# MAGIC | NUMERIC_16 / LEGACY_7_8 / LEGACY_6 / NUMERIC_OTHER | leading digit run | the WHOLE digit run, leading zeros kept |
# MAGIC | UNRECOGNISED | anything else | NULL - evidence, never a guessed truncation |
# MAGIC
# MAGIC The Cerner reference tail after the accession is `<exam code>[<exam code>]<digit>`,
# MAGIC optionally followed by `_SECTRA`. Measured 2026-09-24 on live accessions: the single
# MAGIC trailing digit is an EVENT-ROLE marker, not an exam sequence - the class-234 exam event
# MAGIC is `CODE1`, its class-224 DOC report event `CODECODE0` (doubled code) and the class-231
# MAGIC section `CODE0`; two exams under one accession each carry their own code. The doubled
# MAGIC code is collapsed and `exam_key` = accession + '|' + collapsed code (no digit) is the
# MAGIC exam-scoped join key between an exam and its report events. Codes may carry digits and
# MAGIC underscores; only the LAST digit is taken as the role marker.
# MAGIC
# MAGIC Fixes relative to the legacy `pacs_data_transformations` list: 14-digit references are
# MAGIC no longer cut to 7 digits, and `RNH0…` references no longer all collapse to `RNH098`.

# COMMAND ----------

ACCESSION_RULE_VERSION = "pacs_accession_rules.v1.1.2026-09-24"

# Ordered (format, anchored regex, accession length or None = whole leading digit run).
ACCESSION_FORMATS = [
    ("SECTRA_16",     "^[A-Z]{5}[0-9]{11}([^0-9]|$)", 16),
    ("SITE_16",       "^[A-Z]{3}[0-9]{13}([^0-9]|$)", 16),
    ("RNH_14",        "^RNH[0-9][A-Z]{2}[0-9]{8}([^0-9]|$)", 14),
    ("OTHER_SITE_14", "^[A-Z]{3}[0-9]{11}([^0-9]|$)", 14),
    ("NUMERIC",       "^[0-9]", None),
]


def _sql_str(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def accession_parse_sql(column_sql):
    """Spark SQL expressions parsing one Cerner/Sectra reference column.

    Returns a dict of output name -> SQL expression string over `column_sql`:
      value      normalised input (upper/trim; '' and VALUE_TOO_LONG -> NULL)
      suffix     true when a trailing _SECTRA marker was present
      format     ACCESSION_FORMATS name, NUMERIC_* sub-class, or UNRECOGNISED
      accession  parsed accession (NULL when unrecognised)
      tail       text after the accession (suffix removed)
      code       examination code parsed from the tail (doubled code collapsed)
      seq        the single trailing event-role digit (string)
      method     tail parse outcome
      exam_key   accession || '|' || code  (exam-scoped join key; role digit excluded)
    """
    v = (f"nullif(nullif(upper(trim(cast({column_sql} as string))), ''), "
         f"'VALUE_TOO_LONG')")
    b = f"regexp_replace({v}, '_SECTRA$', '')"
    suffix = f"({v} rlike '_SECTRA$')"
    digits = f"regexp_extract({b}, '^([0-9]+)', 1)"
    numeric_fmt = (f"CASE length({digits}) WHEN 16 THEN 'NUMERIC_16' WHEN 8 THEN 'LEGACY_7_8' "
                   f"WHEN 7 THEN 'LEGACY_7_8' WHEN 6 THEN 'LEGACY_6' ELSE 'NUMERIC_OTHER' END")
    fmt_whens, acc_whens = [], []
    for name, pattern, length in ACCESSION_FORMATS:
        if length is None:
            fmt_whens.append(f"WHEN {b} rlike {_sql_str(pattern)} THEN {numeric_fmt}")
            acc_whens.append(f"WHEN {b} rlike {_sql_str(pattern)} THEN {digits}")
        else:
            fmt_whens.append(f"WHEN {b} rlike {_sql_str(pattern)} THEN '{name}'")
            acc_whens.append(f"WHEN {b} rlike {_sql_str(pattern)} THEN left({b}, {length})")
    fmt = f"CASE WHEN {v} IS NULL THEN NULL {' '.join(fmt_whens)} ELSE 'UNRECOGNISED' END"
    acc = f"CASE WHEN {v} IS NULL THEN NULL {' '.join(acc_whens)} ELSE NULL END"
    tail = f"substring({b}, length({acc}) + 1)"
    doubled = _sql_str("^([A-Z][A-Z0-9_]*)\\1([0-9]?)$")
    single = _sql_str("^([A-Z][A-Z0-9_]*?)([0-9]?)$")
    is_doubled = f"({tail} rlike {doubled})"
    is_single = f"({tail} rlike {single})"
    code = (f"CASE WHEN {is_doubled} THEN regexp_extract({tail}, {doubled}, 1) "
            f"WHEN {is_single} THEN regexp_extract({tail}, {single}, 1) END")
    seq = (f"nullif(CASE WHEN {is_doubled} THEN regexp_extract({tail}, {doubled}, 2) "
           f"WHEN {is_single} THEN regexp_extract({tail}, {single}, 2) END, '')")
    method = (f"CASE WHEN {v} IS NULL THEN 'NO_REFERENCE' "
              f"WHEN {acc} IS NULL THEN 'UNRECOGNISED_FORMAT' "
              f"WHEN {tail} = '' THEN 'ACCESSION_ONLY' "
              f"WHEN {is_doubled} THEN 'CODE_DOUBLED_SEQ' "
              f"WHEN {is_single} THEN 'CODE_SEQ' "
              f"ELSE 'TAIL_UNPARSED' END")
    method = f"concat({method}, CASE WHEN {suffix} THEN '+SECTRA_SUFFIX' ELSE '' END)"
    exam_key = (f"CASE WHEN {acc} IS NOT NULL AND ({code}) IS NOT NULL "
                f"THEN concat({acc}, '|', {code}) END")
    return {"value": v, "suffix": suffix, "format": fmt, "accession": acc, "tail": tail,
            "code": code, "seq": seq, "method": method, "exam_key": exam_key}


def sectra_accession_sql(column_sql):
    """PACS side: the Sectra extraction accession IS the trimmed REQUEST_ID_STRING.
    Returns (accession, format) SQL. The format is the shared classifier's only when the
    classifier consumes the whole value; any trailing text makes it OTHER (the value is
    still the accession - Sectra defines it, the classifier only describes it)."""
    p = accession_parse_sql(column_sql)
    acc = (f"nullif(nullif(trim(cast({column_sql} as string)), ''), 'VALUE_TOO_LONG')")
    fmt = (f"CASE WHEN {acc} IS NULL THEN NULL "
           f"WHEN {p['accession']} = upper({acc}) THEN {p['format']} ELSE 'OTHER' END")
    return acc, fmt


# Fixture shared by the local reference tests and the in-notebook assertion cell:
# (input, format, accession, code, seq, method)
ACCESSION_FIXTURE = [
    ("UKSBH02003761858CCHAPC1", "SECTRA_16", "UKSBH02003761858", "CCHAPC", "1", "CODE_SEQ"),
    ("UKSBH02003761858CCHAPCCCHAPC0", "SECTRA_16", "UKSBH02003761858", "CCHAPC", "0", "CODE_DOUBLED_SEQ"),
    (" ukrlh02010784302xches0 ", "SECTRA_16", "UKRLH02010784302", "XCHES", "0", "CODE_SEQ"),
    ("UKRLH02010784302", "SECTRA_16", "UKRLH02010784302", None, None, "ACCESSION_ONLY"),
    ("UKWXH00011622453XCHES1_SECTRA", "SECTRA_16", "UKWXH00011622453", "XCHES", "1", "CODE_SEQ+SECTRA_SUFFIX"),
    ("APT1234567890123IPICCIIPICCI2", "SITE_16", "APT1234567890123", "IPICCI", "2", "CODE_DOUBLED_SEQ"),
    ("RNH0XR12345678XCHES1", "RNH_14", "RNH0XR12345678", "XCHES", "1", "CODE_SEQ"),
    ("RNH0MR87654321MHEAD0", "RNH_14", "RNH0MR87654321", "MHEAD", "0", "CODE_SEQ"),
    ("WXH02123456789XCHES1", "OTHER_SITE_14", "WXH02123456789", "XCHES", "1", "CODE_SEQ"),
    ("0012345XCHES9", "LEGACY_7_8", "0012345", "XCHES", "9", "CODE_SEQ"),
    ("12345678XLSPNXLSPN1", "LEGACY_7_8", "12345678", "XLSPN", "1", "CODE_DOUBLED_SEQ"),
    ("12345678901234XCHES1", "NUMERIC_OTHER", "12345678901234", "XCHES", "1", "CODE_SEQ"),
    ("UKSBH02003761858XCT21", "SECTRA_16", "UKSBH02003761858", "XCT2", "1", "CODE_SEQ"),
    ("123456", "LEGACY_6", "123456", None, None, "ACCESSION_ONLY"),
    ("000000001234567890AB", "NUMERIC_OTHER", "000000001234567890", "AB", None, "CODE_SEQ"),
    ("XCT2XCT21", "UNRECOGNISED", None, None, None, "UNRECOGNISED_FORMAT"),
    ("UKSBH02003761858XCT2XCT21", "SECTRA_16", "UKSBH02003761858", "XCT2", "1", "CODE_DOUBLED_SEQ"),
    ("UKSBH02003761858X-1", "SECTRA_16", "UKSBH02003761858", None, None, "TAIL_UNPARSED"),
    ("VALUE_TOO_LONG", None, None, None, None, "NO_REFERENCE"),
    ("", None, None, None, None, "NO_REFERENCE"),
    (None, None, None, None, None, "NO_REFERENCE"),
]


def assert_accession_rules():
    """Evaluates the fixture through the SQL expressions (no table read)."""
    from pyspark.sql import functions as F
    p = accession_parse_sql("ref")
    rows = (spark.createDataFrame([(r[0],) for r in ACCESSION_FIXTURE], "ref string")
            .select("ref", *[F.expr(p[k]).alias(k) for k in
                             ("format", "accession", "code", "seq", "method", "exam_key")])
            .collect())
    got = {r["ref"]: r for r in rows}
    for ref, fmt, acc, code, seq, method in ACCESSION_FIXTURE:
        r = got[ref]
        actual = (r["format"], r["accession"], r["code"], r["seq"], r["method"])
        assert actual == (fmt, acc, code, seq, method), f"accession rule {ref!r}: {actual}"
    # The exam event (CODE1) and its doubled-code DOC report (CODECODE0) share one key.
    assert got["UKSBH02003761858CCHAPC1"]["exam_key"] == "UKSBH02003761858|CCHAPC"
    assert got["UKSBH02003761858CCHAPCCCHAPC0"]["exam_key"] == "UKSBH02003761858|CCHAPC"
    assert got["UKSBH02003761858XCT21"]["exam_key"] == got["UKSBH02003761858XCT2XCT21"]["exam_key"]
    print(f"[ACCESSION] {len(ACCESSION_FIXTURE)} rule cases passed ({ACCESSION_RULE_VERSION})")

