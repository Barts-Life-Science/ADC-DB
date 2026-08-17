# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Configuration & Dependencies

# COMMAND ----------

# Widgets
dbutils.widgets.text("input_folder", "/Volumes/1_inland/sectra/vone/Hackathon2/", "Input DICOM Folder")
dbutils.widgets.text("ocr_log_table", "8_dev.pacs.image_ocr_text", "OCR Log Table")
dbutils.widgets.text("preprocessing", "True", "OCR Preprocessing (True/False)")
dbutils.widgets.text("min_text_length", "3", "Min OCR Text Length")
dbutils.widgets.text("padding", "10", "Redaction Padding (px)")

# COMMAND ----------

# Install dependencies
%pip install paddlepaddle paddleocr opencv-python-headless SimpleITK --quiet

# COMMAND ----------

# Must restart Python after pip install for paddle to pick up env vars
dbutils.library.restartPython()

# COMMAND ----------

import os
import re
import math
import calendar
import datetime
import json
from pathlib import Path

# PaddleOCR environment — must be set before import
os.environ.setdefault("PADDLE_PDX_ENABLE_MKLDNN_BYDEFAULT", "False")
os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

import numpy as np
import cv2
import SimpleITK as sitk
from paddleocr import PaddleOCR, TextDetection

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
)

# Read widget values
INPUT_FOLDER = dbutils.widgets.get("input_folder")
OCR_LOG_TABLE = dbutils.widgets.get("ocr_log_table")
PREPROCESSING = dbutils.widgets.get("preprocessing").strip().lower() == "true"
MIN_TEXT_LENGTH = int(dbutils.widgets.get("min_text_length"))
PADDING = int(dbutils.widgets.get("padding"))

# Derive output folder with timestamp
_run_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#OUTPUT_FOLDER = f"/Volumes/8_dev/pacs/anon_images/{_run_ts}/"
OUTPUT_FOLDER = f"/Volumes/1_inland/evan_demo/misc/{_run_ts}/"
print(f"Input folder:  {INPUT_FOLDER}")
print(f"Output folder: {OUTPUT_FOLDER}")
print(f"OCR log table: {OCR_LOG_TABLE}")
print(f"Preprocessing: {PREPROCESSING}")
print(f"Min text len:  {MIN_TEXT_LENGTH}")
print(f"Padding:       {PADDING}")

# COMMAND ----------

# ── OCR Engine ────────────────────────────────────────────────────────────────

_ocr_engine = None
_det_engine = None


def _get_engine():
    """Lazy-init full OCR engine (detection + recognition)."""
    global _ocr_engine
    if _ocr_engine is None:
        _ocr_engine = PaddleOCR(
            lang="en",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
        )
    return _ocr_engine


def _get_det_engine():
    """Lazy-init detection-only engine (no recognition) for blanket mode."""
    global _det_engine
    if _det_engine is None:
        _det_engine = TextDetection()
    return _det_engine


def _normalize_to_uint8(image_array):
    """Normalize any numeric array to uint8 0-255 for OCR input."""
    if image_array.ndim == 3:
        image_array = np.squeeze(image_array)
    if image_array.ndim == 3:
        image_array = image_array[0]
    img = image_array.astype(np.float32)
    img_min, img_max = img.min(), img.max()
    if img_max - img_min > 0:
        img = (img - img_min) / (img_max - img_min) * 255
    return img.astype(np.uint8)


def _preprocess(img_uint8):
    """Invert + CLAHE for white-on-dark ultrasound text."""
    inverted = cv2.bitwise_not(img_uint8)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    return clahe.apply(inverted)


def _polygon_to_bbox(polygon):
    """Convert PaddleOCR quadrilateral to {x, y, width, height}."""
    xs = [pt[0] for pt in polygon]
    ys = [pt[1] for pt in polygon]
    x_min, x_max = float(min(xs)), float(max(xs))
    y_min, y_max = float(min(ys)), float(max(ys))
    return {"x": x_min, "y": y_min, "width": x_max - x_min, "height": y_max - y_min}


def run_ocr_structured(numpy_array, preprocessing=True):
    """Full OCR: detection + recognition. Returns dict with text and bounding boxes."""
    img = _normalize_to_uint8(numpy_array)
    if preprocessing:
        img = _preprocess(img)
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

    engine = _get_engine()
    results = engine.predict(img)

    ocr_results = {}
    line_index = 0
    for page_result in results:
        rec_texts = page_result.get("rec_texts", [])
        rec_polys = page_result.get("rec_polys", [])
        for text, polygon in zip(rec_texts, rec_polys):
            ocr_results[f"line_{line_index}"] = {
                "text": text,
                "bounding_box": _polygon_to_bbox(polygon),
                "has_pii": False,
                "pii_details": [],
            }
            line_index += 1
    return ocr_results


def run_detection_only(numpy_array, preprocessing=True):
    """Detection only: returns bounding boxes for all text regions (blanket mode)."""
    img = _normalize_to_uint8(numpy_array)
    if preprocessing:
        img = _preprocess(img)
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

    det_engine = _get_det_engine()
    results = det_engine.predict(img)

    ocr_results = {}
    line_index = 0
    for det_result in results:
        polys = det_result.get("dt_polys", [])
        for polygon in polys:
            ocr_results[f"line_{line_index}"] = {
                "text": "",
                "bounding_box": _polygon_to_bbox(polygon),
                "has_pii": True,
                "pii_details": [],
            }
            line_index += 1
    return ocr_results

# COMMAND ----------

# ── Text Filter ───────────────────────────────────────────────────────────────

def filter_ocr_results(ocr_results, min_length=2, min_alpha_ratio=0.0):
    """Filter OCR results, marking short/non-text detections as non-PII.

    Returns new dict with added 'filtered' and 'filter_reason' fields.
    """
    filtered = {}
    for line_id, data in ocr_results.items():
        text = data["text"].strip()
        entry = {
            "text": data["text"],
            "bounding_box": data["bounding_box"],
            "pii_details": data.get("pii_details", []),
            "filtered": False,
            "filter_reason": None,
        }

        if len(text) < min_length:
            entry["has_pii"] = False
            entry["filtered"] = True
            entry["filter_reason"] = f"too_short (len={len(text)} < {min_length})"
            filtered[line_id] = entry
            continue

        if min_alpha_ratio > 0:
            total = len(text.replace(" ", ""))
            alpha = sum(1 for c in text if c.isalpha())
            ratio = alpha / max(total, 1)
            if ratio < min_alpha_ratio:
                entry["has_pii"] = False
                entry["filtered"] = True
                entry["filter_reason"] = f"low_alpha (ratio={ratio:.0%} < {min_alpha_ratio:.0%})"
                filtered[line_id] = entry
                continue

        # Passed filters — mark as candidate (PII detection will decide)
        entry["has_pii"] = True
        filtered[line_id] = entry

    return filtered

# COMMAND ----------

# ── PII Detection ─────────────────────────────────────────────────────────────

# Regex patterns for structured identifiers 
_PII_PATTERNS = [
    ("MRN", re.compile(r"\bMRN[:\s]*\d{6,10}\b", re.IGNORECASE)),
    ("ACCESSION", re.compile(r"\bACC(?:ESSION)?[:\s#]*\w{4,15}\b", re.IGNORECASE)),
    ("PATIENT_ID", re.compile(r"\bPID[:\s]*\d{4,12}\b", re.IGNORECASE)),
    ("GENDER", re.compile(r"\b(?:Male|Female|MALE|FEMALE)\b")),
    ("DATE", re.compile(
        r"\b\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}\b"
        r"|\b\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}\b"
        r"|\b\d{8}\b"
    )),
]

# Ultrasound allowlist — terms frequently misclassified as PII
_ULTRASOUND_ALLOWLIST = {
    "sos", "map", "b/0", "b/1", "b/2", "m/0", "m/1",
    "logiq", "voluson", "vivid",
    "epiq", "affiniti", "iu22", "cx50",
    "acuson", "sequoia", "juniper", "helx",
    "aplio", "xario", "toshiba", "canon",
    "samsung", "rs85", "hs70", "hs60",
    "mindray", "resona", "dc-80", "dc-70",
    "arietta", "lisendo", "hitachi", "fujifilm",
    "frq", "freq", "gain", "tis", "tib", "tic", "tls",
    "prf", "wf", "thi", "cpa", "cpd", "pwr", "fps",
    "cw", "pw", "sv", "cf", "cci",
    "breast", "abdomen", "thyroid", "liver", "kidney", "ob",
    "cardiac", "vascular", "msk", "carotid", "renal",
    "dist", "area", "vol", "circ", "vel", "ari", "eri",
}


def is_valid_nhs_number(nhs_number):
    """Validate NHS number using checksum algorithm (from Anonymization Pipeline)."""
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


def _build_dob_patterns(dob_dt):
    """Build regex patterns for common DOB renderings (from Anonymization Pipeline)."""
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
        fr"\b{d}{ord_suffix}{sep}+{months_regex}{sep}+{yyyy}\b",
        fr"\b{dd}{ord_suffix}{sep}+{months_regex}{sep}+{yyyy}\b",
        fr"\b{months_regex}{sep}+{d}{ord_suffix}{sep}+{yyyy}\b",
        fr"\b{months_regex}{sep}+{dd}{ord_suffix}{sep}+{yyyy}\b",
        fr"\b{d}{ord_suffix}{sep}+{months_regex}{sep}+{yy}\b",
        fr"\b{dd}{ord_suffix}{sep}+{months_regex}{sep}+{yy}\b",
        fr"\b{months_regex}{sep}+{d}{ord_suffix}{sep}+{yy}\b",
        fr"\b{months_regex}{sep}+{dd}{ord_suffix}{sep}+{yy}\b",
    ]
    return patterns


def detect_pii_in_ocr(ocr_results, deny_list):
    """Classify OCR text lines as PII using deny-list matching + regex patterns.

    Args:
        ocr_results: dict from filter_ocr_results (only non-filtered entries).
        deny_list: list of known PHI strings to match against.

    Returns:
        Updated dict with has_pii and pii_details populated.
    """
    deny_set = {s.strip().lower() for s in deny_list if s and s.strip()}

    pii_results = {}
    for line_id, data in ocr_results.items():
        text = data["text"]
        found_entities = []

        # 1. Deny-list matching (case-insensitive substring)
        text_lower = text.strip().lower()
        for deny_term in deny_set:
            if len(deny_term) < 2:
                continue
            if deny_term in text_lower:
                found_entities.append({
                    "entity_text": text,
                    "type": "PHI_MATCH",
                    "source": "deny_list",
                    "matched_term": deny_term,
                })
                break

        # 2. Regex patterns
        for pattern_name, pattern in _PII_PATTERNS:
            for match in pattern.finditer(text):
                matched = match.group()
                # Skip allowlisted terms
                if matched.strip().lower() in _ULTRASOUND_ALLOWLIST:
                    continue
                found_entities.append({
                    "entity_text": matched,
                    "type": pattern_name,
                    "source": "regex",
                })

        # 3. NHS number validation — check any 10-digit sequences
        for m in re.finditer(r'(?<!\d)(\d[\s-]?){9}\d(?!\d)', text):
            raw = m.group()
            if is_valid_nhs_number(raw):
                found_entities.append({
                    "entity_text": raw,
                    "type": "NHS_NUMBER",
                    "source": "nhs_checksum",
                })

        pii_results[line_id] = {
            "text": text,
            "bounding_box": data["bounding_box"],
            "has_pii": len(found_entities) > 0,
            "pii_details": found_entities,
            "filtered": data.get("filtered", False),
            "filter_reason": data.get("filter_reason"),
        }

    return pii_results

# COMMAND ----------

# ── Redaction ─────────────────────────────────────────────────────────────────

def redact_pii_from_image(image_array, pii_report, padding=10):
    """Black-fill bounding boxes flagged as PII in the image array.

    Args:
        image_array: 2D NumPy array (grayscale image).
        pii_report: dict where each value has 'has_pii' and 'bounding_box'.
        padding: pixels added to each side of each bounding box.
    """
    redacted = image_array.copy()
    img_h, img_w = redacted.shape[:2]
    fill_value = int(redacted.min())

    for item in pii_report.values():
        if not item.get("has_pii"):
            continue
        box = item["bounding_box"]
        x_start = int(max(0, math.floor(box["x"] - padding)))
        y_start = int(max(0, math.floor(box["y"] - padding)))
        x_end = int(min(img_w, math.ceil(box["x"] + box["width"] + padding)))
        y_end = int(min(img_h, math.ceil(box["y"] + box["height"] + padding)))
        redacted[y_start:y_end, x_start:x_end] = fill_value

    return redacted

# COMMAND ----------

# ── DICOM Metadata Stripping ─────────────────────────────────────────────────

# DICOM tags containing PII that should be scrubbed
_PII_DICOM_TAGS = [
    "0010|0010",  # PatientName
    "0010|0020",  # PatientID
    "0010|0030",  # PatientBirthDate
    "0010|0040",  # PatientSex
    "0010|1000",  # OtherPatientIDs
    "0010|1001",  # OtherPatientNames
    "0010|2160",  # EthnicGroup
    "0008|0050",  # AccessionNumber
    "0008|0080",  # InstitutionName
    "0008|0090",  # ReferringPhysicianName
    "0008|1050",  # PerformingPhysicianName
]

# Tags to extract values from for the deny list (used before stripping)
_DENY_LIST_DICOM_TAGS = {
    "0010|0010": "PatientName",
    "0010|0020": "PatientID",
    "0010|0030": "PatientBirthDate",
    "0008|0080": "InstitutionName",
    "0008|0090": "ReferringPhysicianName",
    "0008|0050": "AccessionNumber",
}


def _extract_metadata_deny_list(sitk_image):
    """Extract PII values from DICOM headers for use as deny-list terms."""
    deny_list = []
    for tag in _DENY_LIST_DICOM_TAGS:
        if sitk_image.HasMetaDataKey(tag):
            value = sitk_image.GetMetaData(tag).strip()
            if value:
                deny_list.append(value)
                # DICOM names use ^ as separator (e.g. "SMITH^JOHN")
                if "^" in value:
                    deny_list.append(value.replace("^", " "))
                    for part in value.split("^"):
                        part = part.strip()
                        if part and len(part) > 1:
                            deny_list.append(part)
    return deny_list


def _extract_dicom_accession(sitk_image):
    """Extract AccessionNumber from DICOM headers."""
    tag = "0008|0050"
    if sitk_image.HasMetaDataKey(tag):
        return sitk_image.GetMetaData(tag).strip()
    return None


def _extract_dicom_study_uid(sitk_image):
    """Extract StudyInstanceUID from DICOM headers."""
    tag = "0020|000d"
    if sitk_image.HasMetaDataKey(tag):
        return sitk_image.GetMetaData(tag).strip()
    return None


def _extract_dicom_patient_id(sitk_image):
    """Extract PatientID from DICOM headers."""
    tag = "0010|0020"
    if sitk_image.HasMetaDataKey(tag):
        return sitk_image.GetMetaData(tag).strip()
    return None


def strip_dicom_pii(sitk_image):
    """Remove PII tags from a SimpleITK image's DICOM metadata (in-place)."""
    for tag in _PII_DICOM_TAGS:
        if sitk_image.HasMetaDataKey(tag):
            sitk_image.SetMetaData(tag, "")
    return sitk_image

# COMMAND ----------

# ── Patient Linking & PHI Gathering ──────────────────────────────────────────

def link_images_to_patients(file_headers):
    """Link DICOM files to patients via imaging_metadata.

    Args:
        file_headers: dict of {file_path: {"accession": str|None, "study_uid": str|None, "patient_id": str|None}}

    Returns:
        dict of {file_path: person_id (int) or None}
    """
    # Collect all accession numbers and study UIDs for a single batch query
    accessions = {}
    study_uids = {}
    for fp, hdr in file_headers.items():
        if hdr.get("accession"):
            accessions[hdr["accession"]] = fp
        if hdr.get("study_uid"):
            study_uids[hdr["study_uid"]] = fp

    result = {fp: None for fp in file_headers}

    if not accessions and not study_uids:
        return result

    imaging_meta = spark.table("4_prod.pacs.imaging_metadata")

    # Try accession number first
    if accessions:
        acc_list = list(accessions.keys())
        matches = (
            imaging_meta
            .filter(col("AccessionNbr").isin(acc_list))
            .filter(col("PersonId").isNotNull())
            .select("AccessionNbr", "PersonId")
            .distinct()
            .collect()
        )
        for row in matches:
            fp = accessions.get(row.AccessionNbr)
            if fp and result[fp] is None:
                result[fp] = row.PersonId

    # Fall back to StudyInstanceUID for unlinked files
    unlinked_uids = {uid: fp for uid, fp in study_uids.items() if result.get(fp) is None}
    if unlinked_uids:
        uid_list = list(unlinked_uids.keys())
        matches = (
            imaging_meta
            .filter(col("ExaminationStudyUid").isin(uid_list))
            .filter(col("PersonId").isNotNull())
            .select("ExaminationStudyUid", "PersonId")
            .distinct()
            .collect()
        )
        for row in matches:
            fp = unlinked_uids.get(row.ExaminationStudyUid)
            if fp and result[fp] is None:
                result[fp] = row.PersonId

    return result


def gather_patient_phi(person_ids):
    """Gather PHI for a set of person IDs from Millennium tables.

    Reuses the same sources as the Anonymization Pipeline:
    - mill_person_name: first/middle/last names
    - mill_person: DOB
    - mill_address: address components
    - mill_person_alias: MRN, NHS, other aliases

    Returns:
        dict of {person_id: {"first_names": [...], "middle_names": [...],
                              "last_names": [...], "dob": datetime|None,
                              "addresses": [...], "aliases": [...]}}
    """
    if not person_ids:
        return {}

    pid_list = list(set(person_ids))
    phi = {pid: {"first_names": [], "middle_names": [], "last_names": [],
                  "dob": None, "addresses": [], "aliases": []} for pid in pid_list}

    # Names
    from pyspark.sql.functions import collect_set, when
    names_df = (
        spark.table("4_prod.raw.mill_person_name")
        .filter(col("PERSON_ID").isin(pid_list))
        .groupBy("PERSON_ID")
        .agg(
            collect_set(when(col("NAME_FIRST").isNotNull(), col("NAME_FIRST"))).alias("first_names"),
            collect_set(when(col("NAME_MIDDLE").isNotNull(), col("NAME_MIDDLE"))).alias("middle_names"),
            collect_set(when(col("NAME_LAST").isNotNull(), col("NAME_LAST"))).alias("last_names"),
        )
        .collect()
    )
    for row in names_df:
        pid = row.PERSON_ID
        if pid in phi:
            phi[pid]["first_names"] = [n for n in (row.first_names or []) if n and n.strip()]
            phi[pid]["middle_names"] = [n for n in (row.middle_names or []) if n and n.strip()]
            phi[pid]["last_names"] = [n for n in (row.last_names or []) if n and n.strip()]

    # DOB
    dob_df = (
        spark.table("4_prod.raw.mill_person")
        .filter(col("PERSON_ID").isin(pid_list))
        .select("PERSON_ID", "BIRTH_DT_TM")
        .collect()
    )
    for row in dob_df:
        if row.PERSON_ID in phi and row.BIRTH_DT_TM:
            phi[row.PERSON_ID]["dob"] = row.BIRTH_DT_TM

    # Addresses
    from pyspark.sql.functions import struct, collect_list
    addr_df = (
        spark.table("4_prod.raw.mill_address")
        .filter(col("PARENT_ENTITY_NAME") == "PERSON")
        .filter(col("PARENT_ENTITY_ID").isin(pid_list))
        .filter(col("ACTIVE_IND") == 1)
        .select(
            col("PARENT_ENTITY_ID").alias("PERSON_ID"),
            struct(
                "STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4",
                "CITY", "COUNTY", "STATE", "COUNTRY", "ZIPCODE", "POSTAL_IDENTIFIER"
            ).alias("address"),
        )
        .groupBy("PERSON_ID")
        .agg(collect_list("address").alias("addresses"))
        .collect()
    )
    for row in addr_df:
        if row.PERSON_ID in phi:
            phi[row.PERSON_ID]["addresses"] = row.addresses or []

    # Aliases (MRN, NHS, etc.)
    alias_df = (
        spark.table("4_prod.raw.mill_person_alias")
        .filter(col("PERSON_ID").isin(pid_list))
        .filter(col("ACTIVE_IND") == 1)
        .filter(col("ALIAS").isNotNull())
        .filter(col("ALIAS") != "")
        .groupBy("PERSON_ID")
        .agg(collect_set("ALIAS").alias("aliases"))
        .collect()
    )
    for row in alias_df:
        if row.PERSON_ID in phi:
            phi[row.PERSON_ID]["aliases"] = [a for a in (row.aliases or []) if a and a.strip()]

    return phi


def build_deny_list(phi_dict, sitk_image):
    """Build a combined deny-list from Millennium PHI + DICOM header values.

    Args:
        phi_dict: single patient's PHI dict from gather_patient_phi
        sitk_image: SimpleITK image to extract DICOM header values from

    Returns:
        list of strings to match against OCR text
    """
    deny_list = []

    # DICOM header values
    deny_list.extend(_extract_metadata_deny_list(sitk_image))

    # Names (all variants)
    for name_list in [phi_dict.get("first_names", []),
                      phi_dict.get("middle_names", []),
                      phi_dict.get("last_names", [])]:
        for name in name_list:
            if name and len(name) > 1:
                deny_list.append(name)

    # Aliases (MRN, NHS, etc.)
    for alias in phi_dict.get("aliases", []):
        if alias and len(alias) > 2:
            deny_list.append(alias)

    # DOB formatted strings
    dob = phi_dict.get("dob")
    if dob:
        dob_patterns = _build_dob_patterns(dob)
        # Also add common plain string formats to the deny list
        dd = f"{dob.day:02d}"
        mm = f"{dob.month:02d}"
        yyyy = str(dob.year)
        deny_list.append(f"{dd}/{mm}/{yyyy}")
        deny_list.append(f"{dd}-{mm}-{yyyy}")
        deny_list.append(f"{dd}.{mm}.{yyyy}")
        deny_list.append(f"{yyyy}{mm}{dd}")
        deny_list.append(f"{dd}{mm}{yyyy}")

    # Address components
    for addr in phi_dict.get("addresses", []):
        for field in ["STREET_ADDR", "STREET_ADDR2", "STREET_ADDR3", "STREET_ADDR4",
                      "CITY", "COUNTY", "STATE", "COUNTRY", "ZIPCODE", "POSTAL_IDENTIFIER"]:
            val = addr[field] if isinstance(addr, dict) else getattr(addr, field, None)
            if val and len(str(val).strip()) > 2:
                deny_list.append(str(val).strip())

    return deny_list

# COMMAND ----------

# ── Main Processing Loop ─────────────────────────────────────────────────────

def _find_dicoms(root_dir):
    """Recursively find DICOM files in a volume directory."""
    exts = {".dcm", ".mhd", ".nii", ".nrrd"}
    for dirpath, _, filenames in os.walk(root_dir):
        for f in sorted(filenames):
            if Path(f).suffix.lower() in exts or "." not in f:
                # Include extensionless files (common for DICOM)
                yield os.path.join(dirpath, f)


def _is_metadata_only(dcm_path):
    """Check if a DICOM file has no pixel data."""
    try:
        img = sitk.ReadImage(dcm_path)
        sitk.GetArrayFromImage(img)
        return False
    except Exception:
        return True


def process_images(input_folder, output_folder, preprocessing, min_text_length, padding):
    """Main processing function: links, OCRs, redacts, and saves DICOM images.

    Returns:
        tuple of (ocr_log_rows, stats_dict)
    """
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # 1. Discover DICOM files
    print(f"Scanning {input_folder} for DICOM files...")
    dicom_files = list(_find_dicoms(input_folder))
    print(f"Found {len(dicom_files)} DICOM files")
    if not dicom_files:
        return [], {"total": 0, "linked": 0, "unlinked": 0, "skipped": 0,
                    "text_regions": 0, "redacted_regions": 0, "errors": 0}

    # 2. Read DICOM headers for linking
    print("Reading DICOM headers for patient linking...")
    file_headers = {}
    valid_files = []
    skipped = 0
    for dcm_path in dicom_files:
        try:
            img = sitk.ReadImage(dcm_path)
            # Check for pixel data
            try:
                sitk.GetArrayFromImage(img)
            except Exception:
                skipped += 1
                continue
            file_headers[dcm_path] = {
                "accession": _extract_dicom_accession(img),
                "study_uid": _extract_dicom_study_uid(img),
                "patient_id": _extract_dicom_patient_id(img),
            }
            valid_files.append(dcm_path)
        except Exception as e:
            print(f"  Could not read {dcm_path}: {e}")
            skipped += 1

    print(f"  {len(valid_files)} readable, {skipped} skipped (no pixel data or unreadable)")

    # 3. Link to patients
    print("Linking images to patients via imaging_metadata...")
    linking_map = link_images_to_patients(file_headers)
    linked_count = sum(1 for v in linking_map.values() if v is not None)
    unlinked_count = len(valid_files) - linked_count
    print(f"  {linked_count} linked, {unlinked_count} unlinked (will blanket-redact)")

    # 4. Gather PHI for linked patients
    linked_person_ids = [pid for pid in linking_map.values() if pid is not None]
    phi_data = {}
    if linked_person_ids:
        print("Gathering PHI for linked patients...")
        phi_data = gather_patient_phi(linked_person_ids)
        print(f"  PHI gathered for {len(phi_data)} patients")

    # 5. Process each file
    ocr_log_rows = []
    total_text_regions = 0
    total_redacted = 0
    errors = 0
    processed_at = datetime.datetime.now().isoformat(timespec="seconds")

    for i, dcm_path in enumerate(valid_files, 1):
        rel_path = os.path.relpath(dcm_path, input_folder)
        person_id = linking_map.get(dcm_path)
        mode_str = "selective" if person_id else "blanket"
        print(f"[{i}/{len(valid_files)}] {rel_path} (mode={mode_str})...")

        try:
            # Read image
            image = sitk.ReadImage(dcm_path)
            image_array = sitk.GetArrayFromImage(image)
            target_slice = image_array[0] if image_array.ndim == 3 else image_array

            accession_nbr = _extract_dicom_accession(image) or ""

            if person_id is not None:
                # ── Selective mode: redact only PII-matching text ──
                patient_phi = phi_data.get(person_id, {})
                deny_list = build_deny_list(patient_phi, image)

                ocr_results = run_ocr_structured(target_slice, preprocessing=preprocessing)
                filtered = filter_ocr_results(ocr_results, min_length=min_text_length)

                # Run PII detection only on non-filtered entries
                candidates = {k: v for k, v in filtered.items() if not v.get("filtered")}
                pii_detected = detect_pii_in_ocr(candidates, deny_list)

                # Also check DOB patterns via regex on candidates
                dob = patient_phi.get("dob")
                if dob:
                    dob_pats = _build_dob_patterns(dob)
                    for line_id, data in pii_detected.items():
                        if not data["has_pii"]:
                            for pat in dob_pats:
                                if re.search(pat, data["text"], re.IGNORECASE):
                                    data["has_pii"] = True
                                    data["pii_details"].append({
                                        "entity_text": data["text"],
                                        "type": "DOB_PATTERN",
                                        "source": "dob_regex",
                                    })
                                    break

                # Merge filtered-out entries back in
                pii_report = {**filtered}
                pii_report.update(pii_detected)

            else:
                # ── Blanket mode: redact ALL detected text ──
                ocr_results = run_ocr_structured(target_slice, preprocessing=preprocessing)
                pii_report = filter_ocr_results(ocr_results, min_length=min_text_length)
                # In blanket mode, all non-filtered entries stay has_pii=True (set by filter)

            # Count
            redacted_count = sum(1 for d in pii_report.values() if d.get("has_pii"))
            filtered_count = sum(1 for d in pii_report.values() if d.get("filtered"))
            total_text_regions += len(pii_report)
            total_redacted += redacted_count
            print(f"  OCR: {len(pii_report)} regions, {redacted_count} redacted, {filtered_count} filtered")

            # Redact pixel data
            redacted_slice = redact_pii_from_image(target_slice, pii_report, padding=padding)

            # Build output DICOM
            if image_array.ndim == 3 and redacted_slice.ndim == 2:
                redacted_for_sitk = redacted_slice[np.newaxis, ...]
            else:
                redacted_for_sitk = redacted_slice
            redacted_sitk = sitk.GetImageFromArray(redacted_for_sitk)
            redacted_sitk.CopyInformation(image)

            # Copy all metadata then strip PII tags
            for key in image.GetMetaDataKeys():
                redacted_sitk.SetMetaData(key, image.GetMetaData(key))
            strip_dicom_pii(redacted_sitk)

            # Save to output folder (preserve subdirectory structure)
            rel_dir = os.path.dirname(rel_path)
            base_name = Path(rel_path).stem
            out_subdir = os.path.join(output_folder, rel_dir)
            os.makedirs(out_subdir, exist_ok=True)
            dcm_out = os.path.join(out_subdir, f"{base_name}_anon.dcm")
            sitk.WriteImage(redacted_sitk, dcm_out)
            print(f"  Saved: {dcm_out}")

            # Collect OCR log rows
            for line_id, data in pii_report.items():
                if data.get("filtered"):
                    action = f"skipped ({data.get('filter_reason', '')})"
                elif data["has_pii"]:
                    action = "redacted"
                else:
                    action = "kept"

                ocr_log_rows.append({
                    "source_file": rel_path,
                    "accession_nbr": accession_nbr,
                    "person_id": str(person_id) if person_id else None,
                    "line_index": int(line_id.split("_")[1]) if "_" in line_id else 0,
                    "text": data.get("text", ""),
                    "bounding_box": json.dumps(data.get("bounding_box", {})),
                    "has_pii": data.get("has_pii", False),
                    "action": action,
                    "pii_details": json.dumps(data.get("pii_details", [])),
                    "filter_reason": data.get("filter_reason", ""),
                    "processed_at": processed_at,
                })

        except Exception as e:
            print(f"  FAILED: {e}")
            errors += 1

    stats = {
        "total": len(valid_files),
        "linked": linked_count,
        "unlinked": unlinked_count,
        "skipped": skipped,
        "text_regions": total_text_regions,
        "redacted_regions": total_redacted,
        "errors": errors,
    }
    return ocr_log_rows, stats

# COMMAND ----------

# ── Save OCR Log ──────────────────────────────────────────────────────────────

def save_ocr_log(ocr_log_rows, table_name):
    """Write OCR text log to a Delta table (create if not exists, then append)."""
    if not ocr_log_rows:
        print("No OCR log rows to save.")
        return

    schema = StructType([
        StructField("source_file", StringType(), True),
        StructField("accession_nbr", StringType(), True),
        StructField("person_id", StringType(), True),
        StructField("line_index", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("bounding_box", StringType(), True),
        StructField("has_pii", BooleanType(), True),
        StructField("action", StringType(), True),
        StructField("pii_details", StringType(), True),
        StructField("filter_reason", StringType(), True),
        StructField("processed_at", StringType(), True),
    ])

    # Ensure schema/database exists
    catalog, schema_name, _ = table_name.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

    df = spark.createDataFrame(ocr_log_rows, schema=schema)
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    print(f"Saved {len(ocr_log_rows)} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Execute

# COMMAND ----------

# ── Run ───────────────────────────────────────────────────────────────────────

ocr_log_rows, stats = process_images(
    input_folder=INPUT_FOLDER,
    output_folder=OUTPUT_FOLDER,
    preprocessing=PREPROCESSING,
    min_text_length=MIN_TEXT_LENGTH,
    padding=PADDING,
)

save_ocr_log(ocr_log_rows, OCR_LOG_TABLE)

# Print summary
print("\n" + "=" * 60)
print("ANONYMIZATION SUMMARY")
print("=" * 60)
print(f"  Input folder:      {INPUT_FOLDER}")
print(f"  Output folder:     {OUTPUT_FOLDER}")
print(f"  Total files found: {stats['total'] + stats['skipped']}")
print(f"  Processed:         {stats['total']}")
print(f"  Skipped:           {stats['skipped']}")
print(f"  Linked (selective):{stats['linked']}")
print(f"  Unlinked (blanket):{stats['unlinked']}")
print(f"  Errors:            {stats['errors']}")
print(f"  Text regions:      {stats['text_regions']}")
print(f"  Redacted regions:  {stats['redacted_regions']}")
print(f"  OCR log table:     {OCR_LOG_TABLE}")
print("=" * 60)

# COMMAND ----------


