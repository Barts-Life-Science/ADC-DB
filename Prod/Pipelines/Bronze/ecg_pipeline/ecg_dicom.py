"""Pure ECG DICOM waveform parser for the bronze ECG pipeline.

No Spark imports: ecg_pipeline.py calls parse_ecg() inside mapInPandas, and the
unit tests run it locally against a synthetic 12-lead-style DICOM.
"""
import hashlib
import io
import re

import pydicom
from pydicom.multival import MultiValue
from pydicom.waveforms import multiplex_array

PARSER_VERSION = "ecg_dicom_1.0.0"

KEY_FIELDS = [
    ("RECORD_TYPE", "string"),
    ("FILE_PATH", "string"),
]
STUDY_FIELDS = [
    ("SOP_CLASS_UID", "string"),
    ("SOP_INSTANCE_UID", "string"),
    ("DICOM_STUDY_INSTANCE_UID", "string"),
    ("SERIES_INSTANCE_UID", "string"),
    ("MODALITY", "string"),
    ("MANUFACTURER", "string"),
    ("MANUFACTURER_MODEL_NAME", "string"),
    ("SOFTWARE_VERSIONS", "string"),
    ("DEVICE_SERIAL_NUMBER", "string"),
    ("STATION_NAME", "string"),
    ("INSTITUTION_NAME", "string"),
    ("INSTITUTIONAL_DEPARTMENT_NAME", "string"),
    ("PATIENT_NAME", "string"),
    ("DICOM_PATIENT_ID", "string"),
    ("PATIENT_BIRTH_DATE_RAW", "string"),
    ("PATIENT_SEX", "string"),
    ("PATIENT_AGE_RAW", "string"),
    ("PATIENT_SIZE_M", "double"),
    ("PATIENT_WEIGHT_KG", "double"),
    ("ACCESSION_NBR", "string"),
    ("REFERRING_PHYSICIAN_NAME", "string"),
    ("OPERATORS_NAME", "string"),
    ("STUDY_DATE_RAW", "string"),
    ("STUDY_TIME_RAW", "string"),
    ("ACQUISITION_DATETIME_RAW", "string"),
    ("CONTENT_DATE_RAW", "string"),
    ("CONTENT_TIME_RAW", "string"),
    ("WAVEFORM_GROUP_COUNT", "int"),
    ("ANNOTATION_COUNT", "int"),
    ("FILE_SIZE_BYTES", "bigint"),
    ("FILE_SHA256", "string"),
    ("PARSE_STATUS", "string"),
    ("PARSE_ERROR", "string"),
]
WAVEFORM_FIELDS = [
    ("WAVEFORM_GROUP_INDEX", "int"),
    ("MULTIPLEX_GROUP_LABEL", "string"),
    ("WAVEFORM_ORIGINALITY", "string"),
    ("SAMPLING_FREQUENCY_HZ", "double"),
    ("NUMBER_OF_SAMPLES", "int"),
    ("NUMBER_OF_CHANNELS", "int"),
    ("WAVEFORM_BITS_ALLOCATED", "int"),
    ("WAVEFORM_SAMPLE_INTERPRETATION", "string"),
    ("MULTIPLEX_GROUP_TIME_OFFSET_MS", "double"),
    ("CHANNEL_INDEX", "int"),
    ("CHANNEL_LABEL", "string"),
    ("CHANNEL_SOURCE_CODE_VALUE", "string"),
    ("CHANNEL_SOURCE_CODING_SCHEME", "string"),
    ("CHANNEL_SOURCE_MEANING", "string"),
    ("CHANNEL_SENSITIVITY", "double"),
    ("CHANNEL_SENSITIVITY_UNITS", "string"),
    ("CHANNEL_SENSITIVITY_CORRECTION_FACTOR", "double"),
    ("CHANNEL_BASELINE", "double"),
    ("CHANNEL_TIME_SKEW", "double"),
    ("CHANNEL_SAMPLE_SKEW", "double"),
    ("FILTER_LOW_FREQUENCY_HZ", "double"),
    ("FILTER_HIGH_FREQUENCY_HZ", "double"),
    ("NOTCH_FILTER_FREQUENCY_HZ", "double"),
    ("SAMPLES", "array<int>"),
]
ANNOTATION_FIELDS = [
    ("ANNOTATION_INDEX", "int"),
    ("ANNOTATION_GROUP_NUMBER", "int"),
    ("UNFORMATTED_TEXT_VALUE", "string"),
    ("CONCEPT_NAME_CODE_VALUE", "string"),
    ("CONCEPT_NAME_CODING_SCHEME", "string"),
    ("CONCEPT_NAME_MEANING", "string"),
    ("NUMERIC_VALUE", "double"),
    ("MEASUREMENT_UNITS_CODE_VALUE", "string"),
    ("REFERENCED_WAVEFORM_CHANNELS", "string"),
    ("REFERENCED_SAMPLE_POSITIONS", "string"),
    ("TEMPORAL_RANGE_TYPE", "string"),
]
RECORD_FIELDS = KEY_FIELDS + STUDY_FIELDS + WAVEFORM_FIELDS + ANNOTATION_FIELDS
RECORD_COLUMNS = [name for name, _ in RECORD_FIELDS]
RECORD_SCHEMA = ", ".join(f"{name} {dtype}" for name, dtype in RECORD_FIELDS)

# DICOM PS3.3 C.10.9.1.4: baseline is in physical units, added after scaling (same as
# pydicom multiplex_array(as_raw=False)). Missing attributes default to 1 / 1 / 0.
SCALING_FORMULA = ("SAMPLES * CHANNEL_SENSITIVITY * CHANNEL_SENSITIVITY_CORRECTION_FACTOR "
                   "+ CHANNEL_BASELINE, in CHANNEL_SENSITIVITY_UNITS")


def physical_values(record):
    """Reference scaling of one waveform record's stored SAMPLES to physical units."""
    sensitivity = 1.0 if record["CHANNEL_SENSITIVITY"] is None else record["CHANNEL_SENSITIVITY"]
    correction = (1.0 if record["CHANNEL_SENSITIVITY_CORRECTION_FACTOR"] is None
                  else record["CHANNEL_SENSITIVITY_CORRECTION_FACTOR"])
    baseline = 0.0 if record["CHANNEL_BASELINE"] is None else record["CHANNEL_BASELINE"]
    return [x * sensitivity * correction + baseline for x in record["SAMPLES"]]

# <landing_root>/batches/<claim batch_id>/<study_uid>__<device_id>__i<instance_number>of<instance_count>.dcm,
# written by ADF EcgFetchBatch; the folder, not the name, identifies the claim.
_LANDING_NAME = re.compile(r"^([0-9.]+__[0-9]+)__i([0-9]+)of([0-9]+)\.dcm$")


def parse_landing_name(name):
    """Return (file_stem, instance_number, instance_count), or None for foreign files."""
    m = _LANDING_NAME.match(name)
    if not m:
        return None
    return m.group(1), int(m.group(2)), int(m.group(3))


def _blank(record_type, file_path):
    rec = dict.fromkeys(RECORD_COLUMNS)
    rec["RECORD_TYPE"] = record_type
    rec["FILE_PATH"] = file_path
    return rec


def _s(ds, keyword):
    v = ds.get(keyword)
    if v is None:
        return None
    if isinstance(v, (list, MultiValue)):
        v = "\\".join(str(x) for x in v)
    v = str(v).strip()
    return v or None


def _f(v):
    if isinstance(v, (list, MultiValue)):
        v = v[0] if len(v) else None
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v):
    f = _f(v)
    return None if f is None else int(f)


def _code(ds, sequence_keyword):
    seq = ds.get(sequence_keyword)
    if not seq:
        return None, None, None
    item = seq[0]
    return _s(item, "CodeValue"), _s(item, "CodingSchemeDesignator"), _s(item, "CodeMeaning")


def _study_record(ds, file_path):
    rec = _blank("study", file_path)
    for column, keyword in (
        ("SOP_CLASS_UID", "SOPClassUID"),
        ("SOP_INSTANCE_UID", "SOPInstanceUID"),
        ("DICOM_STUDY_INSTANCE_UID", "StudyInstanceUID"),
        ("SERIES_INSTANCE_UID", "SeriesInstanceUID"),
        ("MODALITY", "Modality"),
        ("MANUFACTURER", "Manufacturer"),
        ("MANUFACTURER_MODEL_NAME", "ManufacturerModelName"),
        ("SOFTWARE_VERSIONS", "SoftwareVersions"),
        ("DEVICE_SERIAL_NUMBER", "DeviceSerialNumber"),
        ("STATION_NAME", "StationName"),
        ("INSTITUTION_NAME", "InstitutionName"),
        ("INSTITUTIONAL_DEPARTMENT_NAME", "InstitutionalDepartmentName"),
        ("PATIENT_NAME", "PatientName"),
        ("DICOM_PATIENT_ID", "PatientID"),
        ("PATIENT_BIRTH_DATE_RAW", "PatientBirthDate"),
        ("PATIENT_SEX", "PatientSex"),
        ("PATIENT_AGE_RAW", "PatientAge"),
        ("ACCESSION_NBR", "AccessionNumber"),
        ("REFERRING_PHYSICIAN_NAME", "ReferringPhysicianName"),
        ("OPERATORS_NAME", "OperatorsName"),
        ("STUDY_DATE_RAW", "StudyDate"),
        ("STUDY_TIME_RAW", "StudyTime"),
        ("ACQUISITION_DATETIME_RAW", "AcquisitionDateTime"),
        ("CONTENT_DATE_RAW", "ContentDate"),
        ("CONTENT_TIME_RAW", "ContentTime"),
    ):
        rec[column] = _s(ds, keyword)
    rec["PATIENT_SIZE_M"] = _f(ds.get("PatientSize"))
    rec["PATIENT_WEIGHT_KG"] = _f(ds.get("PatientWeight"))
    rec["WAVEFORM_GROUP_COUNT"] = len(ds.get("WaveformSequence") or [])
    rec["ANNOTATION_COUNT"] = len(ds.get("WaveformAnnotationSequence") or [])
    return rec


def _waveform_records(ds, file_path):
    out = []
    for gi, grp in enumerate(ds.WaveformSequence):
        raw = multiplex_array(ds, gi, as_raw=True)  # shape (samples, channels), stored integers
        for ci, ch in enumerate(grp.ChannelDefinitionSequence):
            src = _code(ch, "ChannelSourceSequence")
            units = _code(ch, "ChannelSensitivityUnitsSequence")
            rec = _blank("waveform", file_path)
            rec.update(
                WAVEFORM_GROUP_INDEX=gi,
                MULTIPLEX_GROUP_LABEL=_s(grp, "MultiplexGroupLabel"),
                WAVEFORM_ORIGINALITY=_s(grp, "WaveformOriginality"),
                SAMPLING_FREQUENCY_HZ=_f(grp.get("SamplingFrequency")),
                NUMBER_OF_SAMPLES=_i(grp.get("NumberOfWaveformSamples")),
                NUMBER_OF_CHANNELS=_i(grp.get("NumberOfWaveformChannels")),
                WAVEFORM_BITS_ALLOCATED=_i(grp.get("WaveformBitsAllocated")),
                WAVEFORM_SAMPLE_INTERPRETATION=_s(grp, "WaveformSampleInterpretation"),
                MULTIPLEX_GROUP_TIME_OFFSET_MS=_f(grp.get("MultiplexGroupTimeOffset")),
                CHANNEL_INDEX=ci,
                CHANNEL_LABEL=_s(ch, "ChannelLabel"),
                CHANNEL_SOURCE_CODE_VALUE=src[0],
                CHANNEL_SOURCE_CODING_SCHEME=src[1],
                CHANNEL_SOURCE_MEANING=src[2],
                CHANNEL_SENSITIVITY=_f(ch.get("ChannelSensitivity")),
                CHANNEL_SENSITIVITY_UNITS=units[0],
                CHANNEL_SENSITIVITY_CORRECTION_FACTOR=_f(ch.get("ChannelSensitivityCorrectionFactor")),
                CHANNEL_BASELINE=_f(ch.get("ChannelBaseline")),
                CHANNEL_TIME_SKEW=_f(ch.get("ChannelTimeSkew")),
                CHANNEL_SAMPLE_SKEW=_f(ch.get("ChannelSampleSkew")),
                FILTER_LOW_FREQUENCY_HZ=_f(ch.get("FilterLowFrequency")),
                FILTER_HIGH_FREQUENCY_HZ=_f(ch.get("FilterHighFrequency")),
                NOTCH_FILTER_FREQUENCY_HZ=_f(ch.get("NotchFilterFrequency")),
                SAMPLES=[int(x) for x in raw[:, ci].tolist()],
            )
            out.append(rec)
    return out


def _annotation_records(ds, file_path):
    out = []
    for ai, ann in enumerate(ds.get("WaveformAnnotationSequence") or []):
        name = _code(ann, "ConceptNameCodeSequence")
        unit = _code(ann, "MeasurementUnitsCodeSequence")
        rec = _blank("annotation", file_path)
        rec.update(
            ANNOTATION_INDEX=ai,
            ANNOTATION_GROUP_NUMBER=_i(ann.get("AnnotationGroupNumber")),
            UNFORMATTED_TEXT_VALUE=_s(ann, "UnformattedTextValue"),
            CONCEPT_NAME_CODE_VALUE=name[0],
            CONCEPT_NAME_CODING_SCHEME=name[1],
            CONCEPT_NAME_MEANING=name[2],
            NUMERIC_VALUE=_f(ann.get("NumericValue")),
            MEASUREMENT_UNITS_CODE_VALUE=unit[0],
            REFERENCED_WAVEFORM_CHANNELS=_s(ann, "ReferencedWaveformChannels"),
            REFERENCED_SAMPLE_POSITIONS=_s(ann, "ReferencedSamplePositions"),
            TEMPORAL_RANGE_TYPE=_s(ann, "TemporalRangeType"),
        )
        out.append(rec)
    return out


def parse_ecg(content, file_path):
    """Parse one CAMM ECG DICOM into flat union records (study + waveform + annotation).

    Always returns exactly one study record; on failure it carries PARSE_STATUS='failed'
    and PARSE_ERROR, so every landed file is accounted for in map_ecg_study.
    """
    content = bytes(content)
    size, sha = len(content), hashlib.sha256(content).hexdigest()
    try:
        ds = pydicom.dcmread(io.BytesIO(content), force=True)
        if "SOPClassUID" not in ds:
            raise ValueError("not a DICOM object (no SOPClassUID)")
        study = _study_record(ds, file_path)
        if not study["WAVEFORM_GROUP_COUNT"]:
            study.update(PARSE_STATUS="no_waveform", FILE_SIZE_BYTES=size, FILE_SHA256=sha)
            return [study] + _annotation_records(ds, file_path)
        detail = _waveform_records(ds, file_path) + _annotation_records(ds, file_path)
        study.update(PARSE_STATUS="ok", FILE_SIZE_BYTES=size, FILE_SHA256=sha)
        return [study] + detail
    except Exception as exc:  # noqa: BLE001 — every failure becomes a recorded row, not a lost file
        study = _blank("study", file_path)
        study.update(PARSE_STATUS="failed", PARSE_ERROR=f"{type(exc).__name__}: {exc}"[:1000],
                     FILE_SIZE_BYTES=size, FILE_SHA256=sha)
        return [study]
