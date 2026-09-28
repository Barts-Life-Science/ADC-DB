# Databricks notebook source
# Version-grain repair staged 2026-08-25; anonymization MERGE keyed by BLOB_VERSION_ID.
# Drop-in replacement staged 2026-08-15 from the current production source.
# Base production SHA-256: adf156ca6d4bd1c1e1e0e92b3493eb9604c971feefc9265db969eac944ced1b7
# Validated changes: deterministic cursor, redaction ordering, latest-row selection, and batch-local progress count.
# Lab-only fixture redirects and frozen timestamps are intentionally not included.
import re
import calendar
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, row_number,
    udf, struct, collect_list, collect_set, create_map, expr,
    max as spark_max, array, coalesce, array_sort, sort_array, length, sha2
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType
import os
import time


def _sort_redaction_values(values):
    """Longest values first, then lexical order, for stable overlap handling."""
    return array_sort(
        values,
        lambda left, right: (
            when(length(left) > length(right), lit(-1))
            .when(length(left) < length(right), lit(1))
            .when(left < right, lit(-1))
            .when(left > right, lit(1))
            .otherwise(lit(0))
        ),
    )


# COMMAND ----------

def is_valid_nhs_number(nhs_number):
    """Validate NHS number using checksum algorithm.
    Accepts digits possibly separated by spaces or dashes.
    """
    if not isinstance(nhs_number, str):
        return False
    nhs_digits = re.sub(r'[\s-]', '', nhs_number)
    if not nhs_digits.isdigit() or len(nhs_digits) != 10:
        return False

    weights = [10, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(int(digit) * weight for digit, weight in zip(nhs_digits[:9], weights))
    remainder = total % 11
    check_digit = 11 - remainder

    if check_digit == 11:
        check_digit = 0
    elif check_digit == 10:
        return False

    return check_digit == int(nhs_digits[9])

# COMMAND ----------

def _build_dob_patterns(dob_dt):
    """Build a comprehensive set of regex patterns for a given DOB."""
    if dob_dt is None:
        return []

    day = dob_dt.day
    month = dob_dt.month
    year = dob_dt.year

    d = str(day)
    dd = f"{day:02d}"
    m = str(month)
    mm = f"{month:02d}"
    yyyy = str(year)
    yy = f"{year % 100:02d}"

    month_full = calendar.month_name[month]
    month_abbr = calendar.month_abbr[month]
    months_regex = f"(?:{re.escape(month_full)}|{re.escape(month_abbr)})"

    sep = r"[.\-/\s]"
    ord_suffix = r"(?:st|nd|rd|th)?"

    patterns = [
        fr"(?<!\d){dd}{sep}{mm}{sep}{yyyy}(?!\d)",
        fr"(?<!\d){d}{sep}{m}{sep}{yyyy}(?!\d)",
        fr"(?<!\d){dd}{sep}{mm}{sep}{yy}(?!\d)",
        fr"(?<!\d){d}{sep}{m}{sep}{yy}(?!\d)",
        fr"(?<!\d){yyyy}{sep}{mm}{sep}{dd}(?!\d)",
        fr"(?<!\d){yyyy}{sep}{m}{sep}{d}(?!\d)",
        fr"(?<!\d){dd}{mm}{yyyy}(?!\d)",
        fr"(?<!\d){yyyy}{mm}{dd}(?!\d)",
        fr"(?<!\d){dd}{mm}{yy}(?!\d)",
    ]

    patterns += [
        fr"\b{d}{ord_suffix}{sep}+{months_regex}{sep}+{yyyy}\b",
        fr"\b{dd}{ord_suffix}{sep}+{months_regex}{sep}+{yyyy}\b",
        fr"\b{d}{ord_suffix}\s+(?:of\s+)?{months_regex}{sep}+{yyyy}\b",
        fr"\b{dd}{ord_suffix}\s+(?:of\s+)?{months_regex}{sep}+{yyyy}\b",
        fr"\b{months_regex}{sep}+{d}{ord_suffix}{sep}+{yyyy}\b",
        fr"\b{months_regex}{sep}+{dd}{ord_suffix}{sep}+{yyyy}\b",
        fr"\b{d}{ord_suffix}{sep}+{months_regex}{sep}+{yy}\b",
        fr"\b{dd}{ord_suffix}{sep}+{months_regex}{sep}+{yy}\b",
        fr"\b{months_regex}{sep}+{d}{ord_suffix}{sep}+{yy}\b",
        fr"\b{months_regex}{sep}+{dd}{ord_suffix}{sep}+{yy}\b",
    ]

    patterns += [
        fr"\b{d}{ord_suffix}\s+(?:of\s+)?{months_regex}[,\s\-]+{yyyy}\b",
        fr"\b{months_regex}[,\s\-]+{d}{ord_suffix}[,\s\-]+{yyyy}\b",
        fr"\b{d}{ord_suffix}\s+(?:of\s+)?{months_regex}[,\s\-]+{yy}\b",
        fr"\b{months_regex}[,\s\-]+{d}{ord_suffix}[,\s\-]+{yy}\b",
    ]

    return patterns

def _redact_dob(text, dob_dt):
    """Redact all common renderings of the given DOB from the text."""
    if not dob_dt:
        return text

    patterns = _build_dob_patterns(dob_dt)
    for p in patterns:
        text = re.sub(p, "[[DATE OF BIRTH]]", text, flags=re.IGNORECASE)

    text = re.sub(r"(\[\[DATE OF BIRTH\]\])\s+\d{1,2}:\d{2}(?::\d{2})?", r"\1", text)

    return text

# COMMAND ----------

TOKEN_SPLIT = re.compile(r"[\s\-]+")
LEFT_B  = r"(?<![A-Za-z0-9])"
RIGHT_B = r"(?![A-Za-z0-9])"

NAME_STOPWORDS = {
    # Cerner placeholder values that should never be redacted as real names
    "INFANT", "BABY", "MALE", "FEMALE", "BOY", "GIRL", "TWIN", "TRIPLET",
    "UNKNOWN", "NONE", "TEST", "ANON", "ANONYMOUS", "TRAUMA",
    "WITHHELD", "REFUSED", "DECLINED", "STATED", "RECORDED",
    # Titles / honorifics — would over-redact if treated as names
    "MR", "MRS", "MISS", "MS", "DR", "PROF", "REV", "SIR", "JR", "SR",
    # Common glue words appearing in multi-word surnames
    "OF", "AND", "THE",
}

def expand_name_tokens(name):
    """Yield the full name and each component token (>=3 chars, not a stopword)."""
    if not name:
        return
    n = str(name).strip()
    if not n:
        return
    n_tokens = [t for t in TOKEN_SPLIT.split(n)
                if len(t) >= 3 and t.upper() not in NAME_STOPWORDS]
    if not n_tokens:
        return
    if len(n_tokens) > 1:
        yield n
    for tok in n_tokens:
        yield tok

def collect_tokens(name_list):
    """Flatten a list of raw names into a deduplicated list of search tokens."""
    seen, out = set(), []
    for raw in (name_list or []):
        for tok in expand_name_tokens(raw):
            key = tok.upper()
            if key in seen:
                continue
            seen.add(key)
            out.append(tok)
    return out

def _redact_name_tokens(text, tokens, placeholder):
    for tok in tokens:
        pattern = LEFT_B + re.escape(tok) + RIGHT_B
        text = re.sub(pattern, placeholder, text, flags=re.IGNORECASE)
    return text

# COMMAND ----------

def simple_phi_redaction(text, first_names=None, middle_names=None, last_names=None,
                         dob=None, addresses=None, aliases=None,
                         relatives=None, informants=None, whitelist=None):
    """
    PHI redaction (v2).

    Args:
        first_names / middle_names / last_names: lists of raw name strings (incl. multi-word).
        dob: a datetime/date for the patient.
        addresses: list of address structs/dicts.
        aliases: list of patient alias strings.
        relatives: list of relative name strings (first, last, and free-text NOK).
        informants: list of encounter informant name strings.
        whitelist: words to skip case-insensitively.
    """
    if text is None or text == '':
        return text

    whitelist_lower = {w.lower() for w in (whitelist or [])}

    # NHS number (checksum-validated)
    def replace_nhs_number(match):
        raw = match.group()
        if is_valid_nhs_number(raw):
            return "[[NHS Number]]"
        return raw

    text = re.sub(r'(?<!\d)(?:\d[ -]?){9}\d(?!\d)', replace_nhs_number, text)

    # Build deduped, expanded, stopword-filtered token sets per category.
    def _filter_whitelist(tokens):
        return [t for t in tokens if t.lower() not in whitelist_lower]

    alias_tokens     = _filter_whitelist(collect_tokens(aliases))
    first_tokens     = _filter_whitelist(collect_tokens(first_names))
    middle_tokens    = _filter_whitelist(collect_tokens(middle_names))
    last_tokens      = _filter_whitelist(collect_tokens(last_names))
    relative_tokens  = _filter_whitelist(collect_tokens(relatives))
    informant_tokens = _filter_whitelist(collect_tokens(informants))

    text = _redact_name_tokens(text, alias_tokens,     "[[PATIENT IDENTIFIER]]")
    text = _redact_name_tokens(text, first_tokens,     "[[PATIENT FORENAME]]")
    text = _redact_name_tokens(text, middle_tokens,    "[[PATIENT MIDDLE NAME]]")
    text = _redact_name_tokens(text, last_tokens,      "[[PATIENT SURNAME]]")
    text = _redact_name_tokens(text, relative_tokens,  "[[RELATIVE NAME]]")
    text = _redact_name_tokens(text, informant_tokens, "[[INFORMANT NAME]]")

    text = _redact_dob(text, dob)

    # Addresses
    if addresses:
        for addr in addresses:
            if not addr:
                continue
            for i, field in enumerate(['STREET_ADDR', 'STREET_ADDR2', 'STREET_ADDR3', 'STREET_ADDR4'], 1):
                addr_val = addr.get(field) if isinstance(addr, dict) else getattr(addr, field, None)
                if addr_val and len(str(addr_val)) > 2:
                    addr_text = str(addr_val).strip()
                    if len(addr_text) > 2:
                        text = re.sub(LEFT_B + re.escape(addr_text) + RIGHT_B,
                                      f"[[STREET ADDRESS {i}]]", text, flags=re.IGNORECASE)

            for field, placeholder in [('CITY', '[[CITY]]'), ('COUNTY', '[[COUNTY]]'),
                                       ('STATE', '[[STATE]]'), ('COUNTRY', '[[COUNTRY]]'),
                                       ('ZIPCODE', '[[POSTCODE]]'), ('POSTAL_IDENTIFIER', '[[POSTAL IDENTIFIER]]')]:
                val = addr.get(field) if isinstance(addr, dict) else getattr(addr, field, None)
                if val and len(str(val)) > 2:
                    text = re.sub(LEFT_B + re.escape(str(val)) + RIGHT_B,
                                  placeholder, text, flags=re.IGNORECASE)

    return text

# COMMAND ----------

def get_eligible_person_ids(limit=100000, batch_size=10000, after_person_id=None):
    """Return a deterministic person batch with any pending blob version."""
    eligible_blobs = (
        spark.table("4_prod.bronze.mill_blob_text")
        .filter(col("STATUS") == "Decoded")
        .filter((col("anon_text").isNull()) | (col("anon_text") == "") | col("anon_text").startswith("{\\rtf"))
        .filter(col("BLOB_TEXT").isNotNull() & (col("BLOB_TEXT") != ""))
        .filter(col("BLOB_VERSION_ID").isNotNull())
        .select("EVENT_ID")
        .distinct()
    )
    event_person = (
        eligible_blobs
        .join(
            spark.table("4_prod.raw.mill_clinical_event")
            .filter(col("VALID_UNTIL_DT_TM") > current_timestamp())
            .select("EVENT_ID", "ENCNTR_ID"),
            on="EVENT_ID", how="inner",
        )
        .join(
            spark.table("4_prod.raw.mill_encounter").select("ENCNTR_ID", "PERSON_ID"),
            on="ENCNTR_ID", how="inner",
        )
        .groupBy("EVENT_ID")
        .agg(
            expr("count(distinct PERSON_ID)").alias("_person_count"),
            spark_max("PERSON_ID").alias("PERSON_ID"),
        )
        .filter(col("_person_count") == 1)
        .drop("_person_count")
    )
    person_ids_with_blobs = event_person.select("PERSON_ID").distinct()
    if after_person_id is not None:
        person_ids_with_blobs = person_ids_with_blobs.filter(col("PERSON_ID") > lit(after_person_id))
    person_ids = [
        row.PERSON_ID
        for row in person_ids_with_blobs.orderBy(col("PERSON_ID")).limit(limit).collect()
    ]
    print(f"Found {len(person_ids):,} eligible person_ids for anonymization")
    return person_ids

# COMMAND ----------

def update_blob_text_for_persons(person_ids=None, whitelist=None, limit=100000, after_person_id=None):
    """
    Main function to update blob text for person IDs.
    Spark Connect compatible — uses DataFrame operations instead of broadcast variables.
    """
    if whitelist is None:
        whitelist = ['Lady', 'Barts', 'Bartshealth', 'Newham', 'Homerton', 'Hospital']

    if person_ids is None:
        person_ids = get_eligible_person_ids(
            limit=limit, after_person_id=after_person_id
        )

    if not person_ids:
        print("No eligible person_ids found for anonymization")
        return 0, None

    next_cursor = max(person_ids)
    if after_person_id is not None and next_cursor <= after_person_id:
        raise RuntimeError(
            f"PERSON_ID cursor did not advance: previous={after_person_id!r}, "
            f"next={next_cursor!r}"
        )

    print(f"Processing {len(person_ids):,} person IDs...")

    # Step 1: Patient name variants (all NAME_TYPE_CDs — current, preferred, maiden, previous, alternate)
    print("Fetching patient names...")
    patient_names_agg = spark.table("4_prod.raw.mill_person_name") \
        .filter(col("PERSON_ID").isin(person_ids)) \
        .filter(
            (col("NAME_FIRST").isNotNull() & (col("NAME_FIRST") != "")) |
            (col("NAME_MIDDLE").isNotNull() & (col("NAME_MIDDLE") != "")) |
            (col("NAME_LAST").isNotNull() & (col("NAME_LAST") != ""))
        ) \
        .groupBy("PERSON_ID") \
        .agg(
            _sort_redaction_values(
                collect_set(when(col("NAME_FIRST").isNotNull(), col("NAME_FIRST")))
            ).alias("first_names"),
            _sort_redaction_values(
                collect_set(when(col("NAME_MIDDLE").isNotNull(), col("NAME_MIDDLE")))
            ).alias("middle_names"),
            _sort_redaction_values(
                collect_set(when(col("NAME_LAST").isNotNull(), col("NAME_LAST")))
            ).alias("last_names_pn")
        )

    # Step 1b: DOB + mother's maiden name (last_names append)
    print("Fetching patient DOBs and mother's maiden names...")
    patient_person = spark.table("4_prod.raw.mill_person") \
        .filter(col("PERSON_ID").isin(person_ids)) \
        .select(
            "PERSON_ID",
            col("BIRTH_DT_TM").alias("dob"),
            col("MOTHER_MAIDEN_NAME").alias("mother_maiden")
        )

    # Step 2: Patient addresses
    print("Fetching patient addresses...")
    patient_addresses_agg = spark.table("4_prod.raw.mill_address") \
        .filter(col("PARENT_ENTITY_NAME") == "PERSON") \
        .filter(col("PARENT_ENTITY_ID").isin(person_ids)) \
        .filter(col("ACTIVE_IND") == 1) \
        .select(
            col("PARENT_ENTITY_ID").alias("PERSON_ID"),
            struct(
                "STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4",
                "CITY", "COUNTY", "STATE", "COUNTRY", "ZIPCODE", "POSTAL_IDENTIFIER"
            ).alias("address")
        ) \
        .groupBy("PERSON_ID") \
        .agg(sort_array(collect_list("address")).alias("addresses"))

    # Step 3: Patient aliases
    print("Fetching patient aliases...")
    patient_aliases_agg = spark.table("4_prod.raw.mill_person_alias") \
        .filter(col("PERSON_ID").isin(person_ids)) \
        .filter(col("ACTIVE_IND") == 1) \
        .filter(col("ALIAS").isNotNull()) \
        .filter(col("ALIAS") != "") \
        .groupBy("PERSON_ID") \
        .agg(_sort_redaction_values(collect_set("ALIAS")).alias("aliases"))

    # Step 4: Relatives (related-person names + free-text NOK names)
    print("Fetching relatives (NOK / family / guardian / carer / emergency)...")
    relatives_agg = spark.table("4_prod.raw.mill_person_person_reltn").alias("ppr") \
        .filter(col("ppr.PERSON_ID").isin(person_ids)) \
        .filter(col("ppr.ACTIVE_IND") == 1) \
        .join(
            spark.table("4_prod.raw.mill_person").alias("rp")
                .select("PERSON_ID", "NAME_FIRST", "NAME_LAST"),
            col("ppr.RELATED_PERSON_ID") == col("rp.PERSON_ID"),
            how="left"
        ) \
        .groupBy(col("ppr.PERSON_ID").alias("PERSON_ID")) \
        .agg(
            _sort_redaction_values(
                collect_set(col("rp.NAME_FIRST"))
            ).alias("relative_firsts"),
            _sort_redaction_values(
                collect_set(col("rp.NAME_LAST"))
            ).alias("relative_lasts"),
            _sort_redaction_values(
                collect_set(col("ppr.FT_REL_PERSON_NAME"))
            ).alias("relative_ft")
        ) \
        .withColumn(
            "relatives",
            _sort_redaction_values(
                expr("array_distinct(concat(coalesce(relative_firsts, array()), "
                     "coalesce(relative_lasts, array()), coalesce(relative_ft, array())))")
            )
        ) \
        .select("PERSON_ID", "relatives")

    # Step 5: Encounter informant (per-person, aggregated across all the person's encounters)
    print("Fetching encounter informants...")
    # We need PERSON_ID -> all INFO_GIVEN_BY across that person's encounters that are referenced by the rows we'll process.
    informants_agg = spark.table("4_prod.raw.mill_encounter") \
        .filter(col("PERSON_ID").isin(person_ids)) \
        .filter(col("INFO_GIVEN_BY").isNotNull()) \
        .filter(col("INFO_GIVEN_BY") != "") \
        .groupBy("PERSON_ID") \
        .agg(_sort_redaction_values(collect_set("INFO_GIVEN_BY")).alias("informants"))

    # Step 6: Encounter mapping (event_id -> person_id)
    print("Getting encounter mappings...")
    encounter_df = spark.table("4_prod.raw.mill_clinical_event") \
        .filter(col("VALID_UNTIL_DT_TM") > current_timestamp()) \
        .select("EVENT_ID", "ENCNTR_ID") \
        .join(
            spark.table("4_prod.raw.mill_encounter")
                .filter(col("PERSON_ID").isin(person_ids))
                .select("ENCNTR_ID", "PERSON_ID"),
            on="ENCNTR_ID",
            how="inner"
        ) \
        .select("EVENT_ID", "PERSON_ID") \
        .distinct()

    print("Identifying rows to update...")
    event_ids_df = encounter_df.select("EVENT_ID").distinct()

    event_person = (
        encounter_df
        .groupBy("EVENT_ID")
        .agg(
            expr("count(distinct PERSON_ID)").alias("_person_count"),
            spark_max("PERSON_ID").alias("PERSON_ID"),
        )
        .filter(col("_person_count") == 1)
        .drop("_person_count")
    )

    rows_to_update = (
        spark.table("4_prod.bronze.mill_blob_text")
        .filter(col("STATUS") == "Decoded")
        .filter((col("anon_text").isNull()) | (col("anon_text") == "") | col("anon_text").startswith("{\\rtf"))
        .filter(col("BLOB_TEXT").isNotNull() & (col("BLOB_TEXT") != ""))
        .filter(col("BLOB_VERSION_ID").isNotNull())
        .join(event_ids_df, on="EVENT_ID", how="inner")
        .join(event_person, on="EVENT_ID", how="inner")
        .select("BLOB_VERSION_ID", "EVENT_ID", "PERSON_ID", "BLOB_TEXT")
        .dropDuplicates(["BLOB_VERSION_ID"])
    )

    # Merge mother's maiden into last_names list at the Spark level.
    last_names_agg = patient_names_agg \
        .join(patient_person.select("PERSON_ID", "mother_maiden"), on="PERSON_ID", how="left") \
        .withColumn(
            "last_names",
            _sort_redaction_values(
                expr("array_distinct(concat(coalesce(last_names_pn, array()), "
                     "case when mother_maiden is not null and mother_maiden != '' "
                     "then array(mother_maiden) else array() end))")
            )
        ) \
        .select("PERSON_ID", "first_names", "middle_names", "last_names")

    print("Joining patient information...")
    rows_with_info = rows_to_update \
        .join(last_names_agg, on="PERSON_ID", how="left") \
        .join(patient_person.select("PERSON_ID", "dob"), on="PERSON_ID", how="left") \
        .join(patient_addresses_agg, on="PERSON_ID", how="left") \
        .join(patient_aliases_agg, on="PERSON_ID", how="left") \
        .join(relatives_agg, on="PERSON_ID", how="left") \
        .join(informants_agg, on="PERSON_ID", how="left") \
        .repartition(2000, col("EVENT_ID"))

    row_count = rows_with_info.count()
    print(f"Found {row_count:,} rows to anonymize")

    if row_count == 0:
        print("No rows to update; advancing PERSON_ID cursor")
        return 0, next_cursor

    # UDF
    def anonymize_udf_impl(blob_text, first_names, middle_names, last_names,
                           dob, addresses, aliases, relatives, informants):
        if blob_text is None or blob_text == '':
            return blob_text
        return simple_phi_redaction(
            blob_text,
            first_names or [],
            middle_names or [],
            last_names or [],
            dob,
            addresses or [],
            aliases or [],
            relatives or [],
            informants or [],
            whitelist
        )

    anonymize_text_udf = udf(anonymize_udf_impl, StringType())

    print("Applying anonymization...")
    anonymized_df = rows_with_info \
        .withColumn(
            "anon_text",
            anonymize_text_udf(
                col("BLOB_TEXT"),
                col("first_names"),
                col("middle_names"),
                col("last_names"),
                col("dob"),
                col("addresses"),
                col("aliases"),
                col("relatives"),
                col("informants")
            )
        ) \
        .select("BLOB_VERSION_ID", "anon_text") \
        .dropDuplicates(["BLOB_VERSION_ID"])

    print("Updating the table...")
    temp_table_name = f"temp_updates_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    anonymized_df.write.mode("overwrite").saveAsTable(f"4_prod.tmp.{temp_table_name}")

    try:
        merge_query = f"""
          MERGE INTO 4_prod.bronze.mill_blob_text AS target
          USING 4_prod.tmp.{temp_table_name} AS source
          ON target.BLOB_VERSION_ID = source.BLOB_VERSION_ID
             AND target.STATUS = 'Decoded'
             AND (
                  target.anon_text IS NULL
                  OR target.anon_text = ''
                  OR left(target.anon_text, 5) = concat(chr(123), chr(92), 'rtf')
             )
          WHEN MATCHED THEN
              UPDATE SET anon_text = source.anon_text
        """
        spark.sql(merge_query)
        print("Update completed successfully!")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS 4_prod.tmp.{temp_table_name}")

    batch_updated_count = int(row_count)
    print(f"Successfully updated {batch_updated_count:,} rows in this batch")

    print("\nAnonymization summary:")
    stats = rows_with_info.select(
        col("PERSON_ID"),
        expr("size(coalesce(first_names, array()))").alias("first_count"),
        expr("size(coalesce(middle_names, array()))").alias("middle_count"),
        expr("size(coalesce(last_names, array()))").alias("last_count"),
        expr("size(coalesce(addresses, array()))").alias("address_count"),
        expr("size(coalesce(aliases, array()))").alias("alias_count"),
        expr("size(coalesce(relatives, array()))").alias("relative_count"),
        expr("size(coalesce(informants, array()))").alias("informant_count")
    ).agg(
        expr("count(distinct PERSON_ID)").alias("patient_count"),
        expr("sum(first_count)").alias("total_first"),
        expr("sum(middle_count)").alias("total_middle"),
        expr("sum(last_count)").alias("total_last"),
        expr("sum(address_count)").alias("total_addresses"),
        expr("sum(alias_count)").alias("total_aliases"),
        expr("sum(relative_count)").alias("total_relatives"),
        expr("sum(informant_count)").alias("total_informants")
    ).collect()[0]

    print(f"- Patients processed: {stats['patient_count']:,}")
    print(f"- Name variants found:")
    print(f"  - First names: {stats['total_first']:,}")
    print(f"  - Middle names: {stats['total_middle']:,}")
    print(f"  - Last names (incl. mother's maiden): {stats['total_last']:,}")
    print(f"- Addresses processed: {stats['total_addresses']:,}")
    print(f"- Aliases processed: {stats['total_aliases']:,}")
    print(f"- Relatives processed: {stats['total_relatives']:,}")
    print(f"- Informants processed: {stats['total_informants']:,}")

    return batch_updated_count, next_cursor

# COMMAND ----------

if __name__ == "__main__":
    BATCH_SIZE = 100_000
    MAX_BATCHES = 200            # hard cap — 200 × 100k = 20M person_ids; bump if you need more
    CONSECUTIVE_FAILURES = 3     # bail if this many batches raise in a row

    batch_num = 0
    failures = 0
    person_id_cursor = None

    while batch_num < MAX_BATCHES:
        batch_num += 1
        print(f"\n{'='*60}")
        print(f"Batch {batch_num} (limit={BATCH_SIZE:,})")
        print(f"{'='*60}")

        batch_start = time.time()
        try:
            result, next_cursor = update_blob_text_for_persons(
                limit=BATCH_SIZE,
                after_person_id=person_id_cursor,
            )
            failures = 0
        except Exception as e:
            failures += 1
            print(f"Error in batch {batch_num} (failure {failures}/{CONSECUTIVE_FAILURES}): {e}")
            if failures >= CONSECUTIVE_FAILURES:
                print("Too many consecutive failures — stopping.")
                break
            continue

        elapsed = time.time() - batch_start
        print(
            f"Batch {batch_num} finished in {elapsed/60:.1f} min "
            f"(updated rows: {result:,}; next PERSON_ID cursor: {next_cursor!r})"
        )

        if next_cursor is None:
            print(f"No more eligible person_ids — done after {batch_num} batch(es).")
            break
        if person_id_cursor is not None and next_cursor <= person_id_cursor:
            raise RuntimeError(
                f"PERSON_ID cursor did not advance: previous={person_id_cursor!r}, "
                f"next={next_cursor!r}"
            )
        person_id_cursor = next_cursor

    else:
        print(f"Reached MAX_BATCHES cap ({MAX_BATCHES}). Re-run the notebook to continue.")

