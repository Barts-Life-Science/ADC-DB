# Databricks notebook source
"""Shared identifier matcher and redaction primitives for the anonymous text plane.

This notebook is the single source of truth used by both ``anon_hunt`` and
``anon_engine``.  Keep person-specific value collection and global structural
patterns here so survey coverage cannot silently diverge from redaction coverage.
"""

import calendar
import json
import re
import traceback
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType


CONTROL_SCHEMA_DEFAULT = "8_dev.anon"
NICKNAME_TABLE_DEFAULT = "8_dev.anon.nicknames"
DEFAULT_WHITELIST = ["Lady", "Barts", "Bartshealth", "Newham", "Homerton", "Hospital"]


def variant_string_leaves(value):
    """Return actual string leaves from a VARIANT JSON rendering.

    Hunting the serialized JSON directly creates synthetic identifiers from object
    keys, punctuation, and escape sequences (for example ``\\n54yr`` resembles a
    postcode).  Parse first, then emit each string leaf independently.
    """
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []

    output = []

    def visit(node):
        if isinstance(node, str):
            if node:
                output.append(node)
        elif isinstance(node, list):
            for item in node:
                visit(item)
        elif isinstance(node, dict):
            for item in node.values():
                visit(item)

    visit(parsed)
    return output


def variant_string_text(value):
    """Join decoded leaves with a boundary no identifier pattern can cross."""
    return "\x00".join(variant_string_leaves(value))


def _sort_redaction_values(values):
    """Longest values first, then lexical order, for stable overlap handling."""
    return F.array_sort(
        values,
        lambda left, right: (
            F.when(F.length(left) > F.length(right), F.lit(-1))
            .when(F.length(left) < F.length(right), F.lit(1))
            .when(left < right, F.lit(-1))
            .when(left > right, F.lit(1))
            .otherwise(F.lit(0))
        ),
    )


def is_valid_nhs_number(nhs_number):
    """Validate an NHS number using its modulus-11 checksum."""
    if not isinstance(nhs_number, str):
        return False
    nhs_digits = re.sub(r"[\s-]", "", nhs_number)
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
    for pattern in _build_dob_patterns(dob_dt):
        text = re.sub(pattern, "[[DATE OF BIRTH]]", text, flags=re.IGNORECASE)
    return re.sub(
        r"(\[\[DATE OF BIRTH\]\])\s+\d{1,2}:\d{2}(?::\d{2})?",
        r"\1",
        text,
    )


TOKEN_SPLIT = re.compile(r"[\s\-]+")
LEFT_B = r"(?<![A-Za-z0-9])"
RIGHT_B = r"(?![A-Za-z0-9])"

NAME_STOPWORDS = {
    "INFANT", "BABY", "MALE", "FEMALE", "BOY", "GIRL", "TWIN", "TRIPLET",
    "UNKNOWN", "NONE", "TEST", "ANON", "ANONYMOUS", "TRAUMA",
    "WITHHELD", "REFUSED", "DECLINED", "STATED", "RECORDED",
    "MR", "MRS", "MISS", "MS", "DR", "PROF", "REV", "SIR", "JR", "SR",
    "OF", "AND", "THE",
}


def expand_name_tokens(name):
    """Yield the full name and each component token (>=3 chars, not a stopword)."""
    if not name:
        return
    n = str(name).strip()
    if not n:
        return
    n_tokens = [
        token for token in TOKEN_SPLIT.split(n)
        if len(token) >= 3 and token.upper() not in NAME_STOPWORDS
    ]
    if not n_tokens:
        return
    if len(n_tokens) > 1:
        yield n
    yield from n_tokens


def collect_tokens(name_list):
    """Flatten raw names into a deduplicated list of search tokens."""
    seen, output = set(), []
    for raw in name_list or []:
        for token in expand_name_tokens(raw):
            key = token.upper()
            if key in seen:
                continue
            seen.add(key)
            output.append(token)
    return output


def _redact_name_tokens(text, tokens, placeholder):
    for token in tokens:
        pattern = LEFT_B + re.escape(token) + RIGHT_B
        text = re.sub(pattern, placeholder, text, flags=re.IGNORECASE)
    return text


def simple_phi_redaction(
    text,
    first_names=None,
    middle_names=None,
    last_names=None,
    dob=None,
    addresses=None,
    aliases=None,
    relatives=None,
    informants=None,
    whitelist=None,
):
    """Record-driven PHI redaction retained from the validated v2 pipeline."""
    if text is None or text == "":
        return text

    whitelist_lower = {word.lower() for word in whitelist or []}

    def replace_nhs_number(match):
        raw = match.group()
        return "[[NHS Number]]" if is_valid_nhs_number(raw) else raw

    text = re.sub(r"(?<!\d)(?:\d[ -]?){9}\d(?!\d)", replace_nhs_number, text)

    def _filter_whitelist(tokens):
        return [token for token in tokens if token.lower() not in whitelist_lower]

    alias_tokens = _filter_whitelist(collect_tokens(aliases))
    first_tokens = _filter_whitelist(collect_tokens(first_names))
    middle_tokens = _filter_whitelist(collect_tokens(middle_names))
    last_tokens = _filter_whitelist(collect_tokens(last_names))
    relative_tokens = _filter_whitelist(collect_tokens(relatives))
    informant_tokens = _filter_whitelist(collect_tokens(informants))

    text = _redact_name_tokens(text, alias_tokens, "[[PATIENT IDENTIFIER]]")
    text = _redact_name_tokens(text, first_tokens, "[[PATIENT FORENAME]]")
    text = _redact_name_tokens(text, middle_tokens, "[[PATIENT MIDDLE NAME]]")
    text = _redact_name_tokens(text, last_tokens, "[[PATIENT SURNAME]]")
    text = _redact_name_tokens(text, relative_tokens, "[[RELATIVE NAME]]")
    text = _redact_name_tokens(text, informant_tokens, "[[INFORMANT NAME]]")
    text = _redact_dob(text, dob)

    for address in addresses or []:
        if not address:
            continue
        for index, field in enumerate(
            ["STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4"], 1
        ):
            value = address.get(field) if isinstance(address, dict) else getattr(address, field, None)
            if value and len(str(value).strip()) > 2:
                text = re.sub(
                    LEFT_B + re.escape(str(value).strip()) + RIGHT_B,
                    f"[[STREET ADDRESS {index}]]",
                    text,
                    flags=re.IGNORECASE,
                )
        for field, placeholder in [
            ("CITY", "[[CITY]]"),
            ("COUNTY", "[[COUNTY]]"),
            ("STATE", "[[STATE]]"),
            ("COUNTRY", "[[COUNTRY]]"),
            ("ZIPCODE", "[[POSTCODE]]"),
            ("POSTAL_IDENTIFIER", "[[POSTAL IDENTIFIER]]"),
        ]:
            value = address.get(field) if isinstance(address, dict) else getattr(address, field, None)
            if value and len(str(value).strip()) > 2:
                text = re.sub(
                    LEFT_B + re.escape(str(value).strip()) + RIGHT_B,
                    placeholder,
                    text,
                    flags=re.IGNORECASE,
                )
    return text


NHS_CANDIDATE_RE = re.compile(r"(?i)(?<![A-Z0-9])(?:\d[ -]?){9}\d(?![A-Z0-9])")
UK_POSTCODE_AREAS = (
    "AB", "AL", "B", "BA", "BB", "BD", "BH", "BL", "BN", "BR", "BS", "BT",
    "CA", "CB", "CF", "CH", "CM", "CO", "CR", "CT", "CV", "CW", "DA", "DD",
    "DE", "DG", "DH", "DL", "DN", "DT", "DY", "E", "EC", "EH", "EN", "EX",
    "FK", "FY", "G", "GL", "GU", "HA", "HD", "HG", "HP", "HR", "HS", "HU",
    "HX", "IG", "IM", "IP", "IV", "JE", "KA", "KT", "KW", "KY", "L", "LA",
    "LD", "LE", "LL", "LN", "LS", "LU", "M", "ME", "MK", "ML", "N", "NE",
    "NG", "NN", "NP", "NR", "NW", "OL", "OX", "PA", "PE", "PH", "PL", "PO",
    "PR", "RG", "RH", "RM", "S", "SA", "SE", "SG", "SK", "SL", "SM", "SN",
    "SO", "SP", "SR", "SS", "ST", "SW", "SY", "TA", "TD", "TF", "TN", "TQ",
    "TR", "TS", "TW", "UB", "W", "WA", "WC", "WD", "WF", "WN", "WR", "WS",
    "WV", "YO", "ZE",
)
_POSTCODE_AREA_RE = "(?:" + "|".join(sorted(UK_POSTCODE_AREAS, key=len, reverse=True)) + ")"
POSTCODE_RE = re.compile(
    rf"(?i)(?<![A-Z0-9])(?:GIR ?0AA|{_POSTCODE_AREA_RE}[0-9][A-Z0-9]? ?"
    r"[0-9][ABD-HJLNP-UW-Z]{2})(?![A-Z0-9])"
)


def redact_global(text):
    """Redact structural identifiers independently of person resolution."""
    if not text:
        return text

    def replace_nhs_number(match):
        raw = match.group()
        return "[[NHS Number]]" if is_valid_nhs_number(raw) else raw

    return POSTCODE_RE.sub(
        "[[POSTCODE]]",
        NHS_CANDIDATE_RE.sub(replace_nhs_number, text),
    )


_PLACEHOLDER_RE = re.compile(r"\[\[[^\]]+\]\]")
HIT_CATEGORIES = [
    "forename", "surname", "alias", "dob", "nhs_global",
    "postcode_global", "address", "relative", "informant", "nickname",
]


def redact_with_count(
    text,
    first_names=None,
    middle_names=None,
    last_names=None,
    dob=None,
    addresses=None,
    aliases=None,
    relatives=None,
    informants=None,
    whitelist=None,
):
    """Return redacted text plus the number of placeholders introduced."""
    if text is None or text == "":
        return text, 0
    before = len(_PLACEHOLDER_RE.findall(text))
    redacted = simple_phi_redaction(
        text,
        first_names,
        middle_names,
        last_names,
        dob,
        addresses,
        aliases,
        relatives,
        informants,
        whitelist,
    )
    redacted = redact_global(redacted)
    after = len(_PLACEHOLDER_RE.findall(redacted))
    return redacted, max(after - before, 0)


def redact_variant_json(
    value,
    first_names=None,
    middle_names=None,
    last_names=None,
    dob=None,
    addresses=None,
    aliases=None,
    relatives=None,
    informants=None,
    whitelist=None,
):
    """Redact string leaves while preserving JSON keys and non-string values."""
    if value is None or value == "":
        return value, 0
    parsed = json.loads(str(value))
    total = 0

    def visit(node):
        nonlocal total
        if isinstance(node, str):
            redacted, count = redact_with_count(
                node, first_names, middle_names, last_names, dob, addresses,
                aliases, relatives, informants, whitelist,
            )
            total += count
            return redacted
        if isinstance(node, list):
            return [visit(item) for item in node]
        if isinstance(node, dict):
            return {key: visit(item) for key, item in node.items()}
        return node

    return json.dumps(visit(parsed), ensure_ascii=False, separators=(",", ":")), total


def _match_context(text, start, end, radius=60):
    return text[max(0, start - radius):min(len(text), end + radius)]


def _identifier_value_matches(output, text, category, values, whitelist=None):
    whitelist_lower = {word.lower() for word in whitelist or []}
    seen = set()
    for value in values or []:
        for token in collect_tokens([value]):
            if token.lower() in whitelist_lower:
                continue
            key = token.upper()
            if key in seen:
                continue
            seen.add(key)
            pattern = re.compile(LEFT_B + re.escape(token) + RIGHT_B, re.IGNORECASE)
            for match in pattern.finditer(text):
                output.append((category, match.group(), _match_context(text, match.start(), match.end())))


def find_identifier_hits(
    text,
    first_names=None,
    middle_names=None,
    last_names=None,
    nickname_tokens=None,
    aliases=None,
    relatives=None,
    informants=None,
    addresses=None,
    dob=None,
    whitelist=DEFAULT_WHITELIST,
):
    """Return matcher-covered hit spans using the same values as redaction."""
    if not text:
        return []
    # A placeholder is a hard semantic boundary. Replacing it with whitespace can
    # synthesize a match across redacted fields (for example day/month + two
    # placeholders + year looking like a DOB). NUL is not consumed by any matcher
    # separator, so identifiers cannot bridge across prior redactions.
    clean = _PLACEHOLDER_RE.sub("\x00", str(text))
    output = []
    _identifier_value_matches(
        output, clean, "forename", list(first_names or []) + list(middle_names or []),
        whitelist,
    )
    _identifier_value_matches(output, clean, "surname", last_names, whitelist)
    _identifier_value_matches(output, clean, "nickname", nickname_tokens, whitelist)
    _identifier_value_matches(output, clean, "alias", aliases, whitelist)
    _identifier_value_matches(output, clean, "relative", relatives, whitelist)
    _identifier_value_matches(output, clean, "informant", informants, whitelist)

    if dob:
        for pattern in _build_dob_patterns(dob):
            for match in re.finditer(pattern, clean, flags=re.IGNORECASE):
                output.append(("dob", match.group(), _match_context(clean, match.start(), match.end())))

    for address in addresses or []:
        if not address:
            continue
        for field in [
            "STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4",
            "CITY", "COUNTY", "STATE", "COUNTRY", "ZIPCODE", "POSTAL_IDENTIFIER",
        ]:
            value = address.get(field) if isinstance(address, dict) else getattr(address, field, None)
            if value and len(str(value).strip()) > 2:
                pattern = re.compile(
                    LEFT_B + re.escape(str(value).strip()) + RIGHT_B, re.IGNORECASE
                )
                for match in pattern.finditer(clean):
                    output.append(
                        ("address", match.group(), _match_context(clean, match.start(), match.end()))
                    )

    for match in NHS_CANDIDATE_RE.finditer(clean):
        if is_valid_nhs_number(match.group()):
            output.append(
                ("nhs_global", match.group(), _match_context(clean, match.start(), match.end()))
            )
    for match in POSTCODE_RE.finditer(clean):
        output.append(
            ("postcode_global", match.group(), _match_context(clean, match.start(), match.end()))
        )
    return output


# Audit of filters inherited from Anonymization_Pipeline_REPLACEMENT_20260815:
# KEPT person scoping: every source joins the supplied PERSON_ID frame.
# KEPT junk filters: nonblank name/alias/informant values; address parent=PERSON.
# REMOVED currency predicates: mill_address.ACTIVE_IND==1;
#   mill_person_alias.ACTIVE_IND==1; mill_person_person_reltn.ACTIVE_IND==1.
# Names had no currency filter in the precedent. No effective-date predicate is used.
def build_identifier_frame(spark, person_ids_df, nickname_table=NICKNAME_TABLE_DEFAULT):
    """Build one row per person containing all record-driven identifier values."""
    empty_strings = F.expr("cast(array() as array<string>)")
    address_type = (
        "array<struct<STREET_ADDR:string,STREET_ADDR2:string,STREET_ADDR3:string,"
        "STREET_ADDR4:string,CITY:string,COUNTY:string,STATE:string,COUNTRY:string,"
        "ZIPCODE:string,POSTAL_IDENTIFIER:string>>"
    )
    empty_addresses = F.expr(f"cast(array() as {address_type})")

    person_ids = person_ids_df.select(
        F.col(person_ids_df.columns[0]).cast("long").alias("PERSON_ID")
    ).where(F.col("PERSON_ID").isNotNull()).distinct()

    names = (
        spark.table("4_prod.raw.mill_person_name")
        .join(person_ids, "PERSON_ID", "inner")
        .where(
            F.col("NAME_FIRST").isNotNull()
            | F.col("NAME_MIDDLE").isNotNull()
            | F.col("NAME_LAST").isNotNull()
        )
        .groupBy("PERSON_ID")
        .agg(
            _sort_redaction_values(F.collect_set("NAME_FIRST")).alias("first_names_raw"),
            _sort_redaction_values(F.collect_set("NAME_MIDDLE")).alias("middle_names_raw"),
            _sort_redaction_values(F.collect_set("NAME_LAST")).alias("last_names_raw"),
        )
    )

    person = (
        spark.table("4_prod.raw.mill_person")
        .join(person_ids, "PERSON_ID", "inner")
        .groupBy("PERSON_ID")
        .agg(
            F.max(F.col("BIRTH_DT_TM").cast("date")).alias("dob"),
            _sort_redaction_values(F.collect_set("MOTHER_MAIDEN_NAME")).alias("maiden_names"),
        )
    )

    addresses = (
        spark.table("4_prod.raw.mill_address")
        .where(F.col("PARENT_ENTITY_NAME") == "PERSON")
        .select(
            F.col("PARENT_ENTITY_ID").cast("long").alias("PERSON_ID"),
            F.struct(
                *[
                    F.col(name).cast("string").alias(name)
                    for name in [
                        "STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4",
                        "CITY", "COUNTY", "STATE", "COUNTRY", "ZIPCODE",
                        "POSTAL_IDENTIFIER",
                    ]
                ]
            ).alias("address"),
        )
        .join(person_ids, "PERSON_ID", "inner")
        .groupBy("PERSON_ID")
        .agg(F.sort_array(F.collect_set("address")).alias("addresses"))
    )

    aliases = (
        spark.table("4_prod.raw.mill_person_alias")
        .join(person_ids, "PERSON_ID", "inner")
        .where(F.col("ALIAS").isNotNull() & (F.trim("ALIAS") != ""))
        .groupBy("PERSON_ID")
        .agg(_sort_redaction_values(F.collect_set("ALIAS")).alias("aliases"))
    )

    relationship_rows = (
        spark.table("4_prod.raw.mill_person_person_reltn")
        .select(
            F.col("PERSON_ID").cast("long").alias("PERSON_ID"),
            F.col("RELATED_PERSON_ID").cast("long").alias("_RELATED_PERSON_ID"),
            F.col("FT_REL_PERSON_NAME").alias("_FT_REL_PERSON_NAME"),
        )
        .join(person_ids, "PERSON_ID", "inner")
    )
    relatives = (
        relationship_rows.alias("ppr")
        .join(
            spark.table("4_prod.raw.mill_person").select(
                F.col("PERSON_ID").cast("long").alias("RELATED_PERSON_ID"),
                F.col("NAME_FIRST").alias("RELATIVE_FIRST"),
                F.col("NAME_LAST").alias("RELATIVE_LAST"),
            ),
            F.col("ppr._RELATED_PERSON_ID") == F.col("RELATED_PERSON_ID"),
            "left",
        )
        .groupBy(F.col("ppr.PERSON_ID").cast("long").alias("PERSON_ID"))
        .agg(
            _sort_redaction_values(
                F.array_distinct(
                    F.flatten(
                        F.collect_list(
                            F.array(
                                F.col("RELATIVE_FIRST"),
                                F.col("RELATIVE_LAST"),
                                F.col("ppr._FT_REL_PERSON_NAME"),
                            )
                        )
                    )
                )
            ).alias("relatives")
        )
    )

    informants = (
        spark.table("4_prod.raw.mill_encounter")
        .select(F.col("PERSON_ID").cast("long").alias("PERSON_ID"), "INFO_GIVEN_BY")
        .join(person_ids, "PERSON_ID", "inner")
        .where(F.col("INFO_GIVEN_BY").isNotNull() & (F.trim("INFO_GIVEN_BY") != ""))
        .groupBy("PERSON_ID")
        .agg(_sort_redaction_values(F.collect_set("INFO_GIVEN_BY")).alias("informants"))
    )

    base = (
        person_ids
        .join(names, "PERSON_ID", "left")
        .join(person, "PERSON_ID", "left")
        .join(addresses, "PERSON_ID", "left")
        .join(aliases, "PERSON_ID", "left")
        .join(relatives, "PERSON_ID", "left")
        .join(informants, "PERSON_ID", "left")
        .withColumn("first_names_raw", F.coalesce("first_names_raw", empty_strings))
        .withColumn("middle_names_raw", F.coalesce("middle_names_raw", empty_strings))
        .withColumn("last_names_raw", F.coalesce("last_names_raw", empty_strings))
        .withColumn("maiden_names", F.coalesce("maiden_names", empty_strings))
        .withColumn("aliases", F.coalesce("aliases", empty_strings))
        .withColumn("relatives", F.coalesce("relatives", empty_strings))
        .withColumn("informants", F.coalesce("informants", empty_strings))
        .withColumn("addresses", F.coalesce("addresses", empty_addresses))
        .withColumn(
            "last_names",
            _sort_redaction_values(F.array_distinct(F.concat("last_names_raw", "maiden_names"))),
        )
    )

    name_members = (
        base.select(
            "PERSON_ID",
            F.explode(F.array_distinct(F.concat("first_names_raw", "middle_names_raw"))).alias("raw_name"),
        )
        .where(F.col("raw_name").isNotNull() & (F.trim("raw_name") != ""))
        .select("PERSON_ID", F.upper(F.trim("raw_name")).alias("name_token"))
    )
    nickname_expansions = (
        name_members.join(spark.table(nickname_table), "name_token", "inner")
        .groupBy("PERSON_ID")
        .agg(_sort_redaction_values(F.collect_set("expansion")).alias("nickname_tokens"))
    )

    return (
        base.join(nickname_expansions, "PERSON_ID", "left")
        .withColumn("nickname_tokens", F.coalesce("nickname_tokens", empty_strings))
        .withColumn("first_names", F.col("first_names_raw"))
        .withColumn("middle_names", F.col("middle_names_raw"))
        .select(
            F.col("PERSON_ID").alias("person_id"),
            "first_names", "middle_names", "last_names", "nickname_tokens",
            "aliases", "relatives", "informants", "addresses", "dob",
        )
    )


def with_identity_fingerprint(id_frame):
    """Attach SHA-256 of canonical sorted identifier arrays and DOB."""
    return id_frame.withColumn(
        "identity_fingerprint",
        F.sha2(
            F.to_json(
                F.struct(
                    F.array_sort("first_names").alias("first_names"),
                    F.array_sort("middle_names").alias("middle_names"),
                    F.array_sort("last_names").alias("last_names"),
                    F.array_sort("nickname_tokens").alias("nickname_tokens"),
                    F.array_sort("aliases").alias("aliases"),
                    F.array_sort("relatives").alias("relatives"),
                    F.array_sort("informants").alias("informants"),
                    F.col("dob").cast("string").alias("dob"),
                    F.array_sort(F.transform("addresses", lambda a: F.to_json(a))).alias("addresses"),
                )
            ),
            256,
        ),
    )


def _selftest():
    control_schema = dbutils.widgets.get("control_schema")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {control_schema}")
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {control_schema}.matcher_selftest_persons AS
        SELECT CAST(PERSON_ID AS BIGINT) AS person_id
        FROM 4_prod.raw.mill_person
        WHERE PERSON_ID IS NOT NULL
        ORDER BY PERSON_ID
        LIMIT 5"""
    )
    spark.sql(
        f"ALTER TABLE {control_schema}.matcher_selftest_persons ALTER COLUMN person_id "
        "SET TAGS ('ig_risk'='4','ig_severity'='2')"
    )
    ids = spark.table(f"{control_schema}.matcher_selftest_persons")
    first = with_identity_fingerprint(build_identifier_frame(spark, ids))
    second = with_identity_fingerprint(build_identifier_frame(spark, ids))
    assert first.count() == 5
    assert first.where(
        F.col("first_names").isNull()
        | F.col("middle_names").isNull()
        | F.col("last_names").isNull()
        | F.col("aliases").isNull()
        | F.col("relatives").isNull()
        | F.col("informants").isNull()
        | F.col("addresses").isNull()
    ).count() == 0
    assert first.select("person_id", "identity_fingerprint").exceptAll(
        second.select("person_id", "identity_fingerprint")
    ).count() == 0

    nicknames = spark.table(NICKNAME_TABLE_DEFAULT)
    triple = (
        nicknames.alias("a")
        .join(
            nicknames.alias("b"),
            (F.col("a.name_token") == F.col("b.expansion"))
            & (F.col("a.expansion") == F.col("b.name_token")),
            "inner",
        )
        .limit(1)
        .count()
    )
    assert triple == 1, "nickname closure is not symmetric"

    fixture = "NHS 943 476 5919, postcode E1 4NS, invalid 1234567890"
    redacted = redact_global(fixture)
    assert "943 476 5919" not in redacted and "E1 4NS" not in redacted
    assert "1234567890" in redacted
    assert NHS_CANDIDATE_RE.search("ab2473922403cd") is None
    for clinical_measurement in ["x10mm", "C2H5OH", "A15fr", "x2 3cm"]:
        assert POSTCODE_RE.search(clinical_measurement) is None
    variant_fixture = variant_string_text(
        r'{"section_text":"line one\n54yr","nested":["C2H5OH",7],"flag":true}'
    )
    assert variant_fixture == "line one\n54yr\x00C2H5OH"
    assert POSTCODE_RE.search(variant_fixture) is None
    split_dob = "24 September [[PATIENT IDENTIFIER]], [[PATIENT SURNAME]], 1979"
    assert not any(
        category == "dob"
        for category, _, _ in find_identifier_hits(split_dob, dob=date(1979, 9, 24))
    )
    print("SELFTEST PASS")


if "dbutils" in globals():
    dbutils.widgets.text("control_schema", CONTROL_SCHEMA_DEFAULT)
    dbutils.widgets.text("run_selftest", "0")
    if dbutils.widgets.get("run_selftest") == "1":
        _control_schema = dbutils.widgets.get("control_schema")
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {_control_schema}.matcher_selftest_results (
              run_at TIMESTAMP, status STRING, detail STRING
            ) USING DELTA"""
        )
        for _column in ["run_at", "status", "detail"]:
            spark.sql(
                f"ALTER TABLE {_control_schema}.matcher_selftest_results ALTER COLUMN {_column} "
                "SET TAGS ('ig_risk'='1','ig_severity'='1')"
            )
        try:
            _selftest()
            _status, _detail = "PASS", "SELFTEST PASS"
        except Exception as _exc:
            _status = "FAIL"
            _detail = f"{type(_exc).__name__}: {_exc}\n{traceback.format_exc()[:12000]}"
        spark.createDataFrame([(_status, _detail)], ["status", "detail"]).select(
            F.current_timestamp().alias("run_at"), "status", "detail"
        ).write.mode("append").saveAsTable(f"{_control_schema}.matcher_selftest_results")
        if _status != "PASS":
            raise AssertionError(_detail)

