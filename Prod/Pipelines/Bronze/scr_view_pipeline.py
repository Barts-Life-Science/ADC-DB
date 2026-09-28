# Databricks notebook source
# MAGIC %md
# MAGIC # scr_view_pipeline — Somerset `BIvw*` views rebuilt as bronze tables
# MAGIC
# MAGIC Bronze pipeline step, task `scr_view_pipeline` in job `622989700577569`
# MAGIC (`Bronze_Pipeline_Parallel`). Rebuilds the Somerset Cancer Register reporting views Kai
# MAGIC asked for, from the base tables we land in `4_prod.ancil_scr`, and publishes them as
# MAGIC Delta **tables** in the bronze target schema. One notebook, no siblings: copying this
# MAGIC file and registering the task is the whole deployment.
# MAGIC
# MAGIC ## Why tables, not materialized views
# MAGIC
# MAGIC 1. The `ancil_scr` sources are `wt_updt` **full-overwrite** feeds refreshed weekly.
# MAGIC    Every row is rewritten every week, so there is no incremental delta for the
# MAGIC    Enzyme planner to exploit — an MV would `COMPLETE_RECOMPUTE` every refresh
# MAGIC    anyway, at strictly higher cost than a CTAS.
# MAGIC 2. Every one of these views is join-shaped (up to 45 joins in
# MAGIC    `BIvwTreatmentSurgery`). Join-shaped MVs are where this estate has repeatedly
# MAGIC    hit Enzyme planner problems.
# MAGIC 3. The bronze pipeline is job-orchestrated. An MV runs on its own Lakeflow schedule,
# MAGIC    which the job cannot sequence — so a downstream silver step could read a stale MV.
# MAGIC    A task notebook cannot have that failure mode.
# MAGIC
# MAGIC ## Data quality over bit-faithfulness
# MAGIC
# MAGIC `fix_somerset_defects` defaults to **true**. Somerset's published view SQL carries
# MAGIC real defects — a duplicated `UNION ALL` branch, `NULL`-swallowing string
# MAGIC concatenation, a NULLIF sentinel that never matches, `SPACE(30)` filler standing in
# MAGIC for genuinely absent columns, and `ELSE 'No'` branches that report an unrecorded
# MAGIC flag as a negative finding. Reproducing those bit-for-bit would publish known-wrong
# MAGIC data. Set the widget to `false` only to diff against Somerset's own output.
# MAGIC
# MAGIC Every difference from the published SQL — in either mode — is enumerated in
# MAGIC `DIVERGENCES` and written to the run log. Nothing diverges silently.
# MAGIC
# MAGIC ## The schema blocks are the documentation
# MAGIC
# MAGIC Each table is defined by one block below, and each column in that block is
# MAGIC
# MAGIC ```
# MAGIC (sql_expression, bronze_name, somerset_display_name, ig_class, plain_English_meaning)
# MAGIC ```
# MAGIC
# MAGIC The meaning is not a comment about the code; it is the value that becomes the column's
# MAGIC comment in Unity Catalog, as `"<meaning> Source: [<display name>] in <view> (<release>)."`
# MAGIC Pre-flight refuses to build if any column lacks one, so a column cannot reach a
# MAGIC researcher undocumented. `CREATE OR REPLACE` drops comments and tags, so both are
# MAGIC re-applied on every run.
# MAGIC
# MAGIC ## Identifiers
# MAGIC
# MAGIC NHS Number / Hospital Number / Full Name / clinician names are **published**, and
# MAGIC every column carries `ig_risk` / `ig_severity` tags. De-identification is a
# MAGIC serve-time concern driven by those tags. Tag values follow live `4_prod.bronze`
# MAGIC counterparts, not invented scales.

# COMMAND ----------

# MAGIC %run /Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/_bronze_common

# COMMAND ----------

# Absolute %run path above (not ./_bronze_common). Absolute resolves
# identically from the Bronze folder and from the CC staging folder, so this
# notebook is runnable for dev testing before it is installed as a step.

import re

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark = bronze_active_spark()

# Step-local widgets. These are NOT in _BRONZE_WIDGET_DEFAULTS (that dict is the
# pipeline-wide contract), so register them here or they are invisible in the run UI
# and cannot be overridden for a one-off re-run.
for _name, _default in [
    ("scr_source_schema", "4_prod.ancil_scr"),
    ("fix_somerset_defects", "true"),
    ("apply_ig_tags", "true"),
    ("continue_on_error", "false"),
    ("only_targets", ""),
]:
    try:
        dbutils.widgets.text(_name, _default)
    except Exception:  # noqa: BLE001 - widget already supplied by the caller
        pass

TARGET_SCHEMA = bronze_value("target_schema", "8_dev.bronze")
SOURCE_SCHEMA = bronze_value("scr_source_schema", "4_prod.ancil_scr")
ALLOW_PROD_WRITE = bronze_bool("allow_production_write", False)
APPLY_IG_TAGS = bronze_bool("apply_ig_tags", True)
FIX_SOMERSET_DEFECTS = bronze_bool("fix_somerset_defects", True)
CONTINUE_ON_ERROR = bronze_bool("continue_on_error", False)
# Comma-separated short table names; empty builds everything. Re-running one
# table after a fix costs minutes rather than a full sixteen-table rebuild.
ONLY_TARGETS = [
    t.strip() for t in bronze_value("only_targets", "").split(",") if t.strip()
]

assert TARGET_SCHEMA.startswith("8_dev.") or ALLOW_PROD_WRITE, (
    f"Refusing non-dev target {TARGET_SCHEMA} without allow_production_write=true"
)

CONTROL_SCHEMA = bronze_control_schema(TARGET_SCHEMA)
RUN_ID = bronze_run_id()
SRC = SOURCE_SCHEMA
T_LOG = f"{CONTROL_SCHEMA}.scr_view_pipeline_log"
T_SQL = f"{CONTROL_SCHEMA}.scr_view_pipeline_sql"

SCR_VIEW_LOGIC_VERSION = "2026.09.03.3"
SOMERSET_RELEASE = "scr_22_02"

print(
    f"run_id={RUN_ID} target={TARGET_SCHEMA} source={SOURCE_SCHEMA} "
    f"control={CONTROL_SCHEMA} fix_defects={FIX_SOMERSET_DEFECTS}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## IG classification
# MAGIC
# MAGIC Class -> (`ig_risk`, `ig_severity`). Values copied from live `4_prod.bronze`
# MAGIC counterpart columns rather than guessed:
# MAGIC
# MAGIC | class | tags | counterpart precedent |
# MAGIC |---|---|---|
# MAGIC | `id` | 4 / 2 | `nhs_number`, `mrn`, `surname`, `forename`, `person_id` |
# MAGIC | `clin` | 4 / 1 | `consultant_responsible_for_procedure` |
# MAGIC | `txt` | 4 / 2 | `report_text`, `evidence_text` |
# MAGIC | `dt` | 1 / 1 | `active_status_dt_tm`, `comment_dt_tm` |
# MAGIC | `meas` | 1 / 1 | `height`, `weight` |
# MAGIC | `lu` | 0 / 0 | `site_desc`, `active_status_desc` |
# MAGIC | `ctl` | 0 / 0 | `pipeline_run_id`, `record_updated_dt` |
# MAGIC
# MAGIC Free text is `ig_risk=4` on the standing rule that "free text can embed
# MAGIC identifiers" is a tag value, not an exclusion reason.

# COMMAND ----------

IG_CLASSES = {
    "id": ("4", "2"),
    "clin": ("4", "1"),
    "txt": ("4", "2"),
    "dt": ("1", "1"),
    "meas": ("1", "1"),
    "lu": ("0", "0"),
    "ctl": ("0", "0"),
}

SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def q(text):
    """Single-quote a SQL string literal."""
    return "'" + str(text).replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Fix-aware expression helpers. Every one of these is a T-SQL -> Spark SQL
# translation where Somerset's original behaviour is defensible-but-wrong, so the
# translation is not mechanical. FIX_SOMERSET_DEFECTS selects which behaviour we
# publish; both are recorded in DIVERGENCES.
# --------------------------------------------------------------------------- #

# T-SQL `x + ''` in an ELSE branch. Empty string is not a value; it is a missing
# value wearing a value's clothes, and it defeats every IS NULL filter downstream.
_EMPTY_ELSE = "NULL" if FIX_SOMERSET_DEFECTS else "''"


def cat(*parts, sep=" "):
    """Somerset's `a + ' ' + b` string building.

    T-SQL `+` propagates NULL, and so does Spark `concat`, so the faithful form is a
    straight translation. The consequence at source is that a patient with no recorded
    forename has no Full Name at all, and a consultant with no CON_CODE loses their
    name as well. Under fix mode we drop the NULL and empty parts and join what is
    left, returning NULL only when nothing survives.
    """
    if FIX_SOMERSET_DEFECTS:
        inner = ", ".join(f"nullif(trim(CAST({p} AS STRING)), '')" for p in parts)
        return f"nullif(concat_ws({q(sep)}, {inner}), '')"
    return "concat(" + f", {q(sep)}, ".join(parts) + ")"


def blank():
    """Somerset `SPACE(30)`.

    Proven by a column sweep of `4_prod.ancil_scr` to mark a field the branch's
    extension table genuinely does not have — colorectal has no start/end time,
    specimens, drains, closure or post-op columns at all; dermatology has no
    specimens; lung has no comments. 30 spaces is not the value of those fields.
    NULL is.
    """
    return "CAST(NULL AS STRING)" if FIX_SOMERSET_DEFECTS else "space(30)"


def time_of_day(col):
    """Somerset `CONVERT(varchar, t, 8)` — style 8 renders `hh:mi:ss`.

    Kept as a rendered string rather than a TIME because these columns are typed
    inconsistently at source; the coalesce falls back to the trimmed raw text when the
    value will not cast, so a malformed entry survives as evidence instead of
    silently becoming NULL.
    """
    return (
        f"coalesce(date_format(try_cast({col} AS TIMESTAMP), 'HH:mm:ss'), "
        f"nullif(trim(CAST({col} AS STRING)), ''))"
    )


def yes_no(col, yes="1", no="0", unknown=None, else_no=False):
    """Somerset's coded yes/no flags.

    `else_no=True` reproduces a source `ELSE 'No'`, which reports an unrecorded flag
    as a recorded negative. Under fix mode an unrecorded flag stays NULL, because
    "nobody wrote it down" and "the clinician recorded its absence" are different
    facts and only one of them belongs in a cohort denominator.
    """
    parts = [f"WHEN {q(yes)} THEN 'Yes'", f"WHEN {q(no)} THEN 'No'"]
    if unknown is not None:
        parts.append(f"WHEN {q(unknown)} THEN 'Unknown'")
    tail = "'No'" if (else_no and not FIX_SOMERSET_DEFECTS) else "NULL"
    return f"CASE {col} " + " ".join(parts) + f" ELSE {tail} END"


def flag_yes(col, value="1"):
    """Somerset's comorbidity pattern: `CASE x WHEN '1' THEN 'Yes' ELSE NULL END`.

    Deliberately NOT widened to Yes/No: at source an unticked comorbidity box and an
    unassessed patient are indistinguishable, so NULL is the honest answer and this
    one needs no fix.
    """
    return f"CASE {col} WHEN {q(value)} THEN 'Yes' ELSE NULL END"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specifications
# MAGIC
# MAGIC Everything from here to the SQL generation section is data: helper CTEs, the shared
# MAGIC column fragments, the nine single-source views, the branch machinery for the UNION
# MAGIC roll-ups, and the seven roll-up specs.
# MAGIC
# MAGIC A spec is a dict:
# MAGIC
# MAGIC | key | meaning |
# MAGIC |---|---|
# MAGIC | `view` | the Somerset view being reproduced |
# MAGIC | `target` | the bronze table name |
# MAGIC | `ctes` | helper CTEs needed, resolved transitively |
# MAGIC | `comment` | the table comment; `{src}` is substituted |
# MAGIC | `columns` | `(sql_expression, bronze_name, somerset_display_name, ig_class, meaning)` |
# MAGIC | `from_sql` | the FROM clause, aliases matching the column expressions |
# MAGIC | `branches` | for the UNION roll-ups: `{label, from_sql, exprs}` against one shared `columns` superset |
# MAGIC | `requires` | products this spec reads, which must be built earlier in `VIEW_SPECS` |
# MAGIC
# MAGIC A branch supplies SQL only for the columns its arm has; everything else comes through
# MAGIC as a typed NULL rather than being dropped from the product, and `somerset_branch`
# MAGIC records which arm each row came from.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper CTEs and the nine single-source views
# MAGIC
# MAGIC Somerset composes several of these views out of other views. Three of those
# MAGIC intermediate views could not be landed by ADF, so they are reproduced here as CTEs:
# MAGIC
# MAGIC - **`ConsultantsByCode`** — a windowed CTE over `ltblCONSULTANTS` that cannot be
# MAGIC   pushed through the `SCR_NEW` linked-server hop (it returned rows and then never
# MAGIC   completed, timing out at exactly 30 minutes on two attempts). We land the base
# MAGIC   table and redo the de-duplication here.
# MAGIC - **`vwPLANNED_TREATMENT`** — a `LEN(code) = 2` filter over `ltblTREATMENT_TYPE`.
# MAGIC - **`vwMDT_MEETINGS`** — trimmed to the two columns `BIvwCarePlanMDT` consumes.
# MAGIC
# MAGIC The CTE form has a second advantage over landing them: it is inspectable. The dedup
# MAGIC ordering below is explicit about NULLs and about which duplicate wins, which a
# MAGIC frozen copy of the view would not be.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper CTEs
# MAGIC
# MAGIC Somerset composes several of these views out of other views. Three of those
# MAGIC intermediate views could not be landed by ADF, so they are reproduced here as CTEs:
# MAGIC
# MAGIC - **`ConsultantsByCode`** — a windowed CTE over `ltblCONSULTANTS` that cannot be
# MAGIC   pushed through the `SCR_NEW` linked-server hop (it returned rows and then never
# MAGIC   completed, timing out at exactly 30 minutes on two attempts). We land the base
# MAGIC   table and redo the de-duplication here.
# MAGIC - **`vwPLANNED_TREATMENT`** — a `LEN(code) = 2` filter over `ltblTREATMENT_TYPE`.
# MAGIC - **`vwMDT_MEETINGS`** — trimmed to the two columns `BIvwCarePlanMDT` consumes.
# MAGIC
# MAGIC The CTE form has a second advantage over landing them: it is inspectable. The dedup
# MAGIC ordering below is explicit about NULLs and about which duplicate wins, which a
# MAGIC frozen copy of the view would not be.

# COMMAND ----------

CTES = {
    "consultants_by_code": (
        [],
        f"""consultants_by_code AS (
    -- Somerset's ConsultantsByCode: one row per NATIONAL_CODE. Ordering is explicit
    -- because Spark's default ASC puts NULLs first, which would elect a deleted or
    -- unidentified consultant over a live one. Preference: not-deleted first, then the
    -- highest CON_ID (the most recently created record for that national code).
    SELECT * EXCEPT (rn) FROM (
        SELECT c.*,
               ROW_NUMBER() OVER (
                   PARTITION BY c.NATIONAL_CODE
                   ORDER BY c.IS_DELETED ASC NULLS FIRST,
                            c.CON_ID DESC NULLS LAST
               ) AS rn
        FROM {SRC}.scr_ltblconsultants c
    ) WHERE rn = 1
)""",
    ),
    "planned_treatment": (
        [],
        f"""planned_treatment AS (
    SELECT TREATMENT_CODE, TREATMENT_DESC
    FROM {SRC}.scr_ltbltreatment_type
    WHERE length(TREATMENT_CODE) = 2
)""",
    ),
    "all_treatment_types": (
        ["planned_treatment"],
        f"""all_treatment_types AS (
    SELECT TreatTypeCode AS TREATMENT_CODE, TreatTypeDesc AS TREATMENT_DESC
    FROM {SRC}.scr_ltblprostatetreatmenttype
    UNION
    SELECT TREATMENT_CODE, TREATMENT_DESC FROM planned_treatment
)""",
    ),
    "mdt_meetings": (
        [],
        f"""mdt_meetings AS (
    SELECT m.MEETING_ID, css.SUB_DESC
    FROM {SRC}.scr_tblmdt_meetings m
    LEFT JOIN {SRC}.scr_ltblcancer_sub_site css ON m.SUB_SITE = css.SUB_ID
)""",
    ),
    "dukes_ann_stage_code": (
        [],
        f"""dukes_ann_stage_code AS (
    -- Dukes and Ann Arbor stage share one source column (N6_9_SITE_CLASSIFICATION) and
    -- are told apart only by cancer site. UNION ALL, not UNION: the two code spaces
    -- overlap numerically and collapsing them would silently drop a stage description.
    SELECT cast(DUKES_CODE AS string) AS IdCode, DUKES_DESC AS DescName
    FROM {SRC}.scr_ltbldukes
    UNION ALL
    SELECT cast(STAGE_CODE AS string) AS IdCode, STAGE_DESC AS DescName
    FROM {SRC}.scr_ltblhaem_ann_stage
)""",
    ),
}


def resolve_ctes(names):
    """Emit required CTEs in dependency order, de-duplicated."""
    ordered, seen = [], set()

    def visit(name):
        if name in seen:
            return
        seen.add(name)
        deps, sql = CTES[name]
        for dep in deps:
            visit(dep)
        ordered.append(sql)

    for name in names:
        visit(name)
    return ordered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared column fragments

# COMMAND ----------

# Patient identifiers, joined the same way by every view that carries them.
_DEMOG_COLS = [
    (
        "d.N1_1_NHS_NUMBER",
        "nhs_number", "NHS Number", "id",
        "NHS Number as held by the cancer registry. Direct patient identifier; may be "
        "unverified or absent for overseas and unregistered patients.",
    ),
    (
        "d.N1_2_HOSPITAL_NUMBER",
        "hospital_number", "Hospital Number", "id",
        "Local hospital number (MRN) as held by the cancer registry. Direct patient "
        "identifier; the trust-local counterpart of NHS Number.",
    ),
    (
        cat("d.N1_6_FORENAME", "d.N1_5_SURNAME"),
        "full_name", "Full Name", "id",
        "Patient forename and surname concatenated by the source view. Direct patient "
        "identifier, carried because de-identification here is a serve-time decision "
        "driven by the ig tags, not a bronze exclusion.",
    ),
]

# Consultant display: "Name - NationalCode".
def _consultant(alias):
    return cat(f"{alias}.CON_DESC", f"{alias}.CON_CODE", sep=" - ")


def _stage_part(stage, letter):
    """TNM stage value with its letter suffix, e.g. T2 + 'a' -> 'T2a'.

    Somerset writes ISNULL(stage,'') + ISNULL(letter,''), which yields '' when neither is
    recorded; the outer NULLIF turns that back into NULL. Under fix mode the parts are
    also trimmed, so a whitespace-only entry does not masquerade as a stage.
    """
    if FIX_SOMERSET_DEFECTS:
        return (
            f"nullif(concat_ws('', nullif(trim(CAST({stage} AS STRING)), ''), "
            f"nullif(trim(CAST({letter} AS STRING)), '')), '')"
        )
    return f"nullif(concat(coalesce({stage}, ''), coalesce({letter}, '')), '')"


def _staging_string(prefix):
    """Combined 'T..N..M..' staging string, spaces stripped, NULL when empty."""
    t, tl = f"mr.{prefix}TStage", f"mr.{prefix}TLetter"
    n, nl = f"mr.{prefix}NStage", f"mr.{prefix}NLetter"
    m, ml = f"mr.{prefix}MStage", f"mr.{prefix}MLetter"
    return (
        f"nullif(replace(concat('T', coalesce({t}, ''), coalesce({tl}, ''), "
        f"'N', coalesce({n}, ''), coalesce({nl}, ''), "
        f"'M', coalesce({m}, ''), coalesce({ml}, '')), ' ', ''), 'TNM')"
    )


def _cert_part(label, col):
    return (
        f"CASE WHEN nullif(trim(CAST({col} AS STRING)), '') IS NOT NULL "
        f"THEN concat('{label}', CAST({col} AS STRING)) END"
    )


def certainty_staging(prefix):
    """The four TNM certainty factors as one display string.

    Somerset emits concat('T', t, 'N', n, 'M', m, ' Overall: ', o). Because T-SQL `+`
    propagates NULL, a single unrecorded certainty factor nulls the entire string — so
    the column is populated only for the minority of records where all four were
    recorded. Under fix mode each factor is emitted only when present.
    """
    t, n, m, o = (
        f"mr.{prefix}TCertainty",
        f"mr.{prefix}NCertainty",
        f"mr.{prefix}MCertainty",
        f"mr.{prefix}OverallCertainty",
    )
    if not FIX_SOMERSET_DEFECTS:
        return f"concat('T', {t}, 'N', {n}, 'M', {m}, ' Overall: ', {o})"
    return (
        "nullif(concat_ws(' ', "
        + _cert_part("T", t)
        + ", "
        + _cert_part("N", n)
        + ", "
        + _cert_part("M", m)
        + ", "
        + _cert_part("Overall: ", o)
        + "), '')"
    )


def _path_stage():
    """BIvwPathology [Pathological Staging].

    Somerset builds 'pT: ' + t + tl + ' pN: ' + n + nl + ' pM: ' + m + ml over ISNULL'd
    parts, then NULLIFs the result against 'pT: pN: pM: '. That sentinel is wrong: with
    all parts empty the expression actually produces 'pT:  pN:  pM: ' — two spaces after
    each colon — so the NULLIF never fires and every unstaged report is published as a
    non-empty string that looks like a value. Faithful mode reproduces the defect with
    the correct sentinel restored (otherwise the column would be a string of colons);
    fix mode emits only the axes that were actually recorded, and NULL if none were.
    """
    axes = [
        ("pT: ", "p.N8_16_PATH_T_STAGE", "p.L_PATH_T_LETTER"),
        ("pN: ", "p.N8_17_PATH_N_STAGE", "p.L_PATH_N_LETTER"),
        ("pM: ", "p.N8_18_PATH_M_STAGE", "p.L_PATH_M_LETTER"),
    ]
    if FIX_SOMERSET_DEFECTS:
        parts = [
            f"CASE WHEN {_stage_part(stage, letter)} IS NOT NULL "
            f"THEN concat('{label}', {_stage_part(stage, letter)}) END"
            for label, stage, letter in axes
        ]
        return "nullif(concat_ws(' ', " + ", ".join(parts) + "), '')"
    concat_parts = []
    for label, stage, letter in axes:
        concat_parts += [f"'{label}'", f"coalesce({stage}, '')", f"coalesce({letter}, '')"]
    return "nullif(concat(" + ", ".join(concat_parts) + "), 'pT:  pN:  pM: ')"


_PATH_STAGE_EXPR = _path_stage()

# The 14 specialty referral tables. BIvwStaging LEFT JOINs all of them and projects no
# column from any; the build asserts each is unique on CARE_ID, which makes dropping the
# joins provably lossless rather than merely plausible.
REFERRAL_TABLES = [
    "scr_tblreferral_brain", "scr_tblreferral_breast", "scr_tblreferral_colorectal",
    "scr_tblreferral_cup", "scr_tblreferral_gynaecology", "scr_tblreferral_haematology",
    "scr_tblreferral_head_neck", "scr_tblreferral_lung", "scr_tblreferral_other",
    "scr_tblreferral_paediatrics", "scr_tblreferral_sarcoma",
    "scr_tblreferral_dermatology", "scr_tblreferral_upper_gi",
    "scr_tblreferral_urology",
]

# COMMAND ----------

LEAF_SPECS = [
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwMetastases",
        "target": "scr_metastases",
        "ctes": [],
        "comment": (
            "Somerset Cancer Register metastatic-site records, rebuilt from base tables "
            "in {src} to the scr_22_02 BIvwMetastases definition. One row per "
            "tblMetastases record — the site, type and diagnostic certainty of each "
            "recorded metastasis. NOTE: Somerset uses an INNER join to tblMAIN_REFERRALS, "
            "so metastasis rows whose CARE_ID has no referral are dropped; the run log "
            "records how many. Identifiers are published and ig-tagged."
        ),
        "columns": [
            (
                "mets.CareID",
                "care_id", "CareID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mref.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
            (
                "msite.MetastaticSiteDesc",
                "location", "Location", "lu",
                "Anatomical site of the metastasis, decoded from the registry's "
                "metastatic-site lookup.",
            ),
            (
                "mstate.MetastaticStateDesc",
                "certainty", "Certainty", "lu",
                "How firmly the metastasis is established — for example suspected "
                "versus confirmed.",
            ),
            (
                "mtype.MetastaticTypeDesc",
                "metastasis_type", "Type", "lu",
                "Type of metastatic spread recorded.",
            ),
            (
                "mets.OtherMets",
                "other_mets_details", "Other Mets Details", "txt",
                "Free text describing metastatic sites not on the coded picklist.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmetastases mets
JOIN {SRC}.scr_tblmain_referrals mref ON mref.CARE_ID = mets.CareID
LEFT JOIN {SRC}.scr_ltblmetastaticsite msite ON msite.MetastaticSiteID = mets.MetastaticSiteID
LEFT JOIN {SRC}.scr_ltblmetastaticstate mstate ON mstate.MetastaticStateID = mets.MetastaticStateID
LEFT JOIN {SRC}.scr_ltblmetastatictype mtype ON mtype.MetastaticTypeID = mets.MetastaticTypeID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwTreatmentAntiCancerDrugs",
        "target": "scr_treatment_anticancer_drugs",
        "ctes": ["consultants_by_code"],
        "comment": (
            "Systemic anti-cancer drug treatment episodes from the Somerset Cancer "
            "Register, rebuilt from base tables in {src} to the scr_22_02 "
            "BIvwTreatmentAntiCancerDrugs definition. One row per tblMAIN_CHEMOTHERAPY "
            "record — regimen, intent, setting, planned cycles and delivery dates. "
            "Registry-recorded SACT: distinct from and thinner than the ARIA/iQemo feed "
            "behind bronze.map_cancer_treatment, so use it as a registry supplement, not "
            "as the systemic-therapy spine."
        ),
        "columns": [
            (
                "chemo.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                _consultant("c"),
                "consultant", "Consultant", "clin",
                "Responsible consultant, rendered as name then national code. "
                "Clinician identifier: identifying of staff rather than of the "
                "patient, hence a lower severity tag than patient identifiers.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "chemo.N9_4_DECISION_DATE",
                "date_decision_to_treat", "Date Decision to Treat", "dt",
                "Date the decision to treat was made. The start of the 31-day "
                "decision-to-treatment waiting-time standard.",
            ),
            (
                "org1.Description",
                "organisation_dtt", "Organisation (DTT)", "lu",
                "Organisation at which the decision to treat was made. Distinct from "
                "the treating organisation: national cancer waiting-time rules "
                "attribute the clock to the decision site, which may not be where "
                "treatment happened.",
            ),
            (
                "chemo.N9_10_START_DATE",
                "date_start_of_treatment", "Date Start of Treatment", "dt",
                "Date this treatment episode started.",
            ),
            (
                "chemo.N9_1_SITE_CODE",
                "organisation_treatment_site_code", "Organisation (Treatment) Site Code", "lu",
                "ODS site code of the organisation that delivered the treatment, as "
                "recorded. Raw code, kept alongside the resolved name so an unmatched "
                "code is still visible.",
            ),
            (
                "org2.Description",
                "organisation_treatment_name", "Organisation (Treatment) Name", "lu",
                "Name of the organisation that delivered the treatment, resolved from "
                "the site code.",
            ),
            (
                "chemo.N9_16_CYCLE_NO",
                "planned_cycles_courses", "Planned Cycles/Courses", "meas",
                "Number of chemotherapy cycles or courses planned.",
            ),
            (
                "CASE chemo.N_CHEMORADIO WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' ELSE NULL END",
                "chemo_radiotherapy", "Chemo-Radiotherapy", "lu",
                "Whether chemotherapy and radiotherapy were given concurrently as "
                "chemoradiation.",
            ),
            (
                "te.EVENT_DESC",
                "treatment_event_type", "Treatment Event Type", "lu",
                "Whether this is first definitive treatment, a subsequent treatment, "
                "or a recurrence treatment — the national treatment-event "
                "classification.",
            ),
            (
                "ts.SET_DESC",
                "treatment_setting", "Treatment Setting", "lu",
                "Setting in which treatment was delivered, for example inpatient, day "
                "case or outpatient.",
            ),
            (
                "CASE chemo.L_TRIAL WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' ELSE NULL END",
                "clinical_trial", "Clinical Trial", "lu",
                "Whether this treatment was delivered as part of a clinical trial.",
            ),
            (
                "dtyp.DRUG_TYPE_DESC",
                "drug_therapy_type", "Drug Therapy Type", "lu",
                "Class of systemic therapy — cytotoxic chemotherapy, hormone therapy, "
                "immunotherapy and so on.",
            ),
            (
                "ti.NonSurgicalIntentDesc",
                "treatment_intent", "Treatment Intent", "lu",
                "Whether treatment was given with curative or palliative intent. "
                "Drives almost every downstream outcome analysis.",
            ),
            (
                "adj.AdjunctiveTherapyDesc",
                "adjunctive_therapy", "Adjunctive Therapy", "lu",
                "Additional therapy given alongside the main treatment.",
            ),
            (
                "drt.ROUTE_DESC",
                "route_of_administration", "Route of Administration", "lu",
                "Route by which the drug was given, for example IV or oral.",
            ),
            (
                "chemo.N9_9_DRUG_REGIMEN",
                "drug_regimen_acronym", "Drug Regimen Acronym", "lu",
                "Named chemotherapy regimen, usually as its standard acronym (for "
                "example FOLFOX, R-CHOP).",
            ),
            (
                "CASE chemo.TRANS_ARTERIAL_CHEMOEMBO WHEN 'Y' THEN 'Yes' WHEN 'N' THEN 'No' WHEN '9' THEN 'Not Known' ELSE NULL END",
                "tace_performed_indicator", "TACE Performed Indicator", "lu",
                "Whether trans-arterial chemoembolisation was performed — "
                "chemotherapy delivered directly into a tumour's arterial supply, "
                "mainly in liver cancer.",
            ),
            (
                "pta.PTA_DESCRIPTION",
                "chemotherapy_consultant_age_specialty", "Chemotherapy Consultant Age Specialty", "lu",
                "Whether the treating consultant's practice is adult or paediatric. "
                "Determines which national dataset the activity is reported under.",
            ),
            (
                "chemo.L_COMMENTS",
                "comments", "Comments", "txt",
                "Free-text registry comments recorded against this record.",
            ),
            (
                "chemo.L_END_DATE",
                "review_end_date", "Review/End Date", "dt",
                "Review or end date of the treatment episode, as recorded.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_chemotherapy chemo
LEFT JOIN {SRC}.scr_ltblpta pta ON chemo.CONSULTANT_AGE_SPECIALTY = pta.PTA_ID
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON chemo.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_organisationsites org1 ON chemo.N_SITE_CODE_DTT = org1.Code
LEFT JOIN {SRC}.scr_organisationsites org2 ON chemo.N9_1_SITE_CODE = org2.Code
LEFT JOIN {SRC}.scr_ltbltreatment_event te ON chemo.N_TREATMENT_EVENT = te.EVENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_setting ts ON chemo.N_TREATMENT_SETTING = ts.SET_CODE
LEFT JOIN {SRC}.scr_ltbldrug_route drt ON chemo.L_ROUTE = drt.ROUTE_CODE
LEFT JOIN {SRC}.scr_ltbldrug_type dtyp ON chemo.N9_7_THERAPY_TYPE = dtyp.DRUG_TYPE_CODE
LEFT JOIN {SRC}.scr_ltbltreatmentintent ti ON chemo.N9_8_TREATMENT_INTENT = ti.IntentID
LEFT JOIN consultants_by_code c ON chemo.N9_2_CONSULTANT = c.NATIONAL_CODE
LEFT JOIN {SRC}.scr_ltbladjunctivetherapy adj ON chemo.AdjunctiveTherapyID = adj.AdjunctiveTherapyID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwTreatmentTeletherapy",
        "target": "scr_treatment_teletherapy",
        "ctes": ["consultants_by_code"],
        "comment": (
            "External-beam radiotherapy episodes from the Somerset Cancer Register, "
            "rebuilt from base tables in {src} to the scr_22_02 BIvwTreatmentTeletherapy "
            "definition. One row per tblMAIN_TELETHERAPY record — prescribed and actual "
            "dose, fractions and duration, so an interrupted course is visible as the "
            "gap between the two. The live SCR catalogue exposes this view under the "
            "later name BIvwTreatmentRadiotherapy; the two agree on base table, lookups "
            "and column count (38), so it is the same object renamed after the scr_22_02 "
            "documentation release."
        ),
        "columns": [
            (
                "tt.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                _consultant("c"),
                "consultant", "Consultant", "clin",
                "Responsible consultant, rendered as name then national code. "
                "Clinician identifier: identifying of staff rather than of the "
                "patient, hence a lower severity tag than patient identifiers.",
            ),
            (
                "rp.URGENCY_DESC",
                "priority", "Priority", "lu",
                "Clinical priority assigned to the radiotherapy request.",
            ),
            (
                "tt.N10_3_DECISION_DATE",
                "date_decision_to_treat", "Date Decision to Treat", "dt",
                "Date the decision to treat was made. The start of the 31-day "
                "decision-to-treatment waiting-time standard.",
            ),
            (
                "org1.Description",
                "organisation_dtt", "Organisation (DTT)", "lu",
                "Organisation at which the decision to treat was made. Distinct from "
                "the treating organisation: national cancer waiting-time rules "
                "attribute the clock to the decision site, which may not be where "
                "treatment happened.",
            ),
            (
                "tt.N10_8_START_DATE",
                "date_start_of_treatment", "Date Start of Treatment", "dt",
                "Date this treatment episode started.",
            ),
            (
                "tt.N10_1_SITE_CODE",
                "organisation_treatment_site_code", "Organisation (Treatment) Site Code", "lu",
                "ODS site code of the organisation that delivered the treatment, as "
                "recorded. Raw code, kept alongside the resolved name so an unmatched "
                "code is still visible.",
            ),
            (
                "org2.Description",
                "organisation_treatment_name", "Organisation (Treatment) Name", "lu",
                "Name of the organisation that delivered the treatment, resolved from "
                "the site code.",
            ),
            (
                "te.EVENT_DESC",
                "treatment_event_type", "Treatment Event Type", "lu",
                "Whether this is first definitive treatment, a subsequent treatment, "
                "or a recurrence treatment — the national treatment-event "
                "classification.",
            ),
            (
                "ts.SET_DESC",
                "treatment_setting", "Treatment Setting", "lu",
                "Setting in which treatment was delivered, for example inpatient, day "
                "case or outpatient.",
            ),
            (
                "CASE tt.L_TRIAL WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' WHEN '99' THEN 'Unknown' ELSE NULL END",
                "clinical_trial", "Clinical Trial", "lu",
                "Whether this treatment was delivered as part of a clinical trial.",
            ),
            (
                "ti.NonSurgicalIntentDesc",
                "treatment_intent", "Treatment Intent", "lu",
                "Whether treatment was given with curative or palliative intent. "
                "Drives almost every downstream outcome analysis.",
            ),
            (
                "adj.AdjunctiveTherapyDesc",
                "adjunctive_therapy", "Adjunctive Therapy", "lu",
                "Additional therapy given alongside the main treatment.",
            ),
            (
                "tt.L_SHORT_LONG",
                "short_long_course", "Short/Long Course", "lu",
                "Whether a short or long course of radiotherapy was prescribed.",
            ),
            (
                "CASE tt.N_CHEMORADIO WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' ELSE NULL END",
                "chemo_radiotherapy", "Chemo-Radiotherapy", "lu",
                "Whether chemotherapy and radiotherapy were given concurrently as "
                "chemoradiation.",
            ),
            (
                "bq.BEAM_DESC",
                "beam_quality", "Beam Quality", "lu",
                "Type and energy of the radiotherapy beam.",
            ),
            (
                "tsite.TREAT_DESC",
                "treatment_site", "Treatment Site", "lu",
                "Body site the radiotherapy was directed at.",
            ),
            (
                "prc.PROC_DESC",
                "anatomical_site", "Anatomical Site", "lu",
                "Anatomical site treated, decoded from the procedure lookup.",
            ),
            (
                "cgrp.GROUP_DESC",
                "complexity_group", "Complexity Group", "lu",
                "National complexity grouping of the radiotherapy plan.",
            ),
            (
                "tt.N10_20_ANAESTHETIC",
                "anaesthetic_required", "Anaesthetic Required", "lu",
                "Whether an anaesthetic was required to deliver the treatment.",
            ),
            (
                "rfld.FIELD_DESC",
                "fields", "Fields", "lu",
                "Number or configuration of treatment fields used.",
            ),
            (
                "tt.N10_21_MULTIPLE_PLANNING",
                "multiple_planning", "Multiple Planning", "lu",
                "Whether more than one radiotherapy plan was produced.",
            ),
            (
                "tt.N10_10_DOSE",
                "prescribed_dose_gy", "Prescribed Dose (Gy)", "meas",
                "Total radiation dose prescribed, in Gray.",
            ),
            (
                "tt.L_DOSE_FRACTIONS",
                "dose_per_fraction", "Dose per Fraction", "meas",
                "Radiation dose delivered per fraction, in Gray.",
            ),
            (
                "tt.N10_11_FRACTIONS",
                "prescribed_fractions", "Prescribed Fractions", "meas",
                "Number of fractions the prescribed dose was to be split into.",
            ),
            (
                "tt.L_FRACTIONS_WEEK",
                "fraction_per_week", "Fraction per Week", "meas",
                "Number of fractions delivered per week.",
            ),
            (
                "tt.N10_12_DURATION",
                "prescribed_duration", "Prescribed Duration", "meas",
                "Planned duration of the radiotherapy course, in days.",
            ),
            (
                "tt.N10_13_ACTUAL_DOSE",
                "actual_dose_gy", "Actual Dose (Gy)", "meas",
                "Total radiation dose actually delivered, in Gray. Compare with the "
                "prescribed dose to detect an interrupted or abandoned course.",
            ),
            (
                "tt.N10_15_ACTUAL_DURATION",
                "actual_duration", "Actual Duration", "meas",
                "Actual duration of the radiotherapy course, in days.",
            ),
            (
                "tt.N10_14_ACTUAL_FRACTIONS",
                "actual_fractions", "Actual Fractions", "meas",
                "Number of fractions actually delivered.",
            ),
            (
                "ncr.Description",
                "treatment_outcome", "Treatment Outcome", "lu",
                "Where treatment was not completed as planned, the recorded reason. "
                "NULL normally means the course completed.",
            ),
            (
                "CASE tt.BRAIN_RADIOSURGERY_INDICATOR WHEN 'Y' THEN 'Yes' WHEN 'N' THEN 'No' WHEN '9' THEN 'Not Known' ELSE NULL END",
                "radio_surgery_performed_indicator", "Radio Surgery Performed Indicator", "lu",
                "Whether stereotactic radiosurgery was performed — a single high dose "
                "delivered with stereotactic precision, chiefly for brain lesions.",
            ),
            (
                "tt.N10_9_END_DATE",
                "end_date", "End Date", "dt",
                "Date the radiotherapy course ended.",
            ),
            (
                "tt.L_COMMENTS",
                "comments", "Comments", "txt",
                "Free-text registry comments recorded against this record.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_teletherapy tt
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON tt.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN consultants_by_code c ON tt.N10_2_CONSULTANT = c.NATIONAL_CODE
LEFT JOIN {SRC}.scr_ltblradio_priority rp ON tt.N_PRIORITY = rp.URGENCY_CODE
LEFT JOIN {SRC}.scr_organisationsites org1 ON tt.N_SITE_CODE_DTT = org1.Code
LEFT JOIN {SRC}.scr_organisationsites org2 ON tt.N10_1_SITE_CODE = org2.Code
LEFT JOIN {SRC}.scr_ltbltreatment_event te ON tt.N_TREATMENT_EVENT = te.EVENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_setting ts ON tt.N_TREATMENT_SETTING = ts.SET_CODE
LEFT JOIN {SRC}.scr_ltbltreatmentintent ti ON tt.N10_6_TREATMENT_INTENT = ti.IntentID
LEFT JOIN {SRC}.scr_ltblradio_beam_type bq ON tt.N10_16_BEAM_TYPE = bq.BEAM_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_site tsite ON tt.R_TREAT_TO = tsite.TREAT_ID
LEFT JOIN {SRC}.scr_ltblprocedures prc ON tt.N10_7_TREATMENT_SITE = prc.PROC_CODE
LEFT JOIN {SRC}.scr_ltblradio_complex_group cgrp ON tt.N10_19_COMPLEXITY = cgrp.GROUP_CODE
LEFT JOIN {SRC}.scr_ltblradio_fields rfld ON tt.N10_18_FIELDS = rfld.FIELD_CODE
LEFT JOIN {SRC}.scr_ltblteletherapynotcompletereason ncr ON tt.NotCompleteReasonID = ncr.ID
LEFT JOIN {SRC}.scr_ltbladjunctivetherapy adj ON tt.AdjunctiveTherapyID = adj.AdjunctiveTherapyID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwTreatmentBrachytherapy",
        "target": "scr_treatment_brachytherapy",
        "ctes": ["consultants_by_code"],
        "comment": (
            "Brachytherapy and unsealed-source treatment episodes from the Somerset "
            "Cancer Register, rebuilt from base tables in {src} to the scr_22_02 "
            "BIvwTreatmentBrachytherapy definition. One row per tblMAIN_BRACHYTHERAPY "
            "record — isotope, delivery type, insertions and dose. A thin table at Barts "
            "(391 rows at landing), which is a finding about local recording practice, "
            "not a load failure. Dates are kept as DATE; Somerset renders them "
            "dd/mm/yyyy via convert(varchar, d, 103)."
        ),
        "columns": [
            (
                "br.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                _consultant("c"),
                "consultant", "Consultant", "clin",
                "Responsible consultant, rendered as name then national code. "
                "Clinician identifier: identifying of staff rather than of the "
                "patient, hence a lower severity tag than patient identifiers.",
            ),
            (
                "br.N11_3_DECISION_DATE",
                "date_decision_to_treat", "Date Decision to Treat", "dt",
                "Date the decision to treat was made. The start of the 31-day "
                "decision-to-treatment waiting-time standard.",
            ),
            (
                "org1.Description",
                "organisation_dtt", "Organisation (DTT)", "lu",
                "Organisation at which the decision to treat was made. Distinct from "
                "the treating organisation: national cancer waiting-time rules "
                "attribute the clock to the decision site, which may not be where "
                "treatment happened.",
            ),
            (
                "br.N11_9_START_DATE",
                "date_start_of_treatment", "Date Start of Treatment", "dt",
                "Date this treatment episode started.",
            ),
            (
                "br.N11_1_SITE_CODE",
                "organisation_treatment_site_code", "Organisation (Treatment) Site Code", "lu",
                "ODS site code of the organisation that delivered the treatment, as "
                "recorded. Raw code, kept alongside the resolved name so an unmatched "
                "code is still visible.",
            ),
            (
                "org2.Description",
                "organisation_treatment_name", "Organisation (Treatment) Name", "lu",
                "Name of the organisation that delivered the treatment, resolved from "
                "the site code.",
            ),
            (
                "te.EVENT_DESC",
                "treatment_event_type", "Treatment Event Type", "lu",
                "Whether this is first definitive treatment, a subsequent treatment, "
                "or a recurrence treatment — the national treatment-event "
                "classification.",
            ),
            (
                "ts.SET_DESC",
                "treatment_setting", "Treatment Setting", "lu",
                "Setting in which treatment was delivered, for example inpatient, day "
                "case or outpatient.",
            ),
            (
                "CASE br.L_TRIAL WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' WHEN '99' THEN 'Unknown' ELSE NULL END",
                "clinical_trial", "Clinical Trial", "lu",
                "Whether this treatment was delivered as part of a clinical trial.",
            ),
            (
                "ti.NonSurgicalIntentDesc",
                "treatment_intent", "Treatment Intent", "lu",
                "Whether treatment was given with curative or palliative intent. "
                "Drives almost every downstream outcome analysis.",
            ),
            (
                "adj.AdjunctiveTherapyDesc",
                "adjunctive_therapy", "Adjunctive Therapy", "lu",
                "Additional therapy given alongside the main treatment.",
            ),
            (
                "br.L_SHORT_LONG",
                "short_long_course", "Short/Long Course", "lu",
                "Whether a short or long course of radiotherapy was prescribed.",
            ),
            (
                "tsite.TREAT_DESC",
                "treatment_site", "Treatment Site", "lu",
                "Body site the radiotherapy was directed at.",
            ),
            (
                "prc.PROC_DESC",
                "anatomical_site", "Anatomical Site", "lu",
                "Anatomical site treated, decoded from the procedure lookup.",
            ),
            (
                "bt.BRACH_TYPE_DESC",
                "brachytherapy_type", "Brachytherapy Type", "lu",
                "Type of brachytherapy delivered.",
            ),
            (
                "iso.ISOTOPE_DESC",
                "isotope_type", "Isotope Type", "lu",
                "Radioactive isotope used.",
            ),
            (
                "dlv.DEL_DESC",
                "delivery_type", "Delivery Type", "lu",
                "How the source was delivered, for example manual or afterloading.",
            ),
            (
                "br.N11_18_ANAESTHETIC",
                "anaesthetic_required", "Anaesthetic Required", "lu",
                "Whether an anaesthetic was required to deliver the treatment.",
            ),
            (
                "ptyp.PATIENT_DESC",
                "unsealed_source_pt_type", "Unsealed Source Pt Type", "lu",
                "Patient category for unsealed-source (radioisotope) therapy, which "
                "governs the radiation protection regime.",
            ),
            (
                "br.N11_11_DOSE",
                "prescribed_dose_gy", "Prescribed Dose (Gy)", "meas",
                "Total radiation dose prescribed, in Gray.",
            ),
            (
                "br.N11_12_DURATION",
                "prescribed_duration", "Prescribed Duration", "meas",
                "Planned duration of the radiotherapy course, in days.",
            ),
            (
                "br.N11_13_FRACTIONS",
                "prescribed_fractions", "Prescribed Fractions", "meas",
                "Number of fractions the prescribed dose was to be split into.",
            ),
            (
                "br.N11_14_ACTUAL_DOSE",
                "actual_dose_gy", "Actual Dose (Gy)", "meas",
                "Total radiation dose actually delivered, in Gray. Compare with the "
                "prescribed dose to detect an interrupted or abandoned course.",
            ),
            (
                "br.N11_16_ACTUAL_DURATION",
                "actual_duration", "Actual Duration", "meas",
                "Actual duration of the radiotherapy course, in days.",
            ),
            (
                "br.R_INSERTIONS",
                "no_of_insertions", "No of Insertions", "meas",
                "Number of separate brachytherapy source insertions.",
            ),
            (
                "br.R_DOSE_INSERTION",
                "dose_per_insertion", "Dose per Insertion", "meas",
                "Radiation dose delivered per insertion, in Gray.",
            ),
            (
                "rdose.DOSE_DESC",
                "dose_rate", "Dose Rate", "lu",
                "Dose rate category of the brachytherapy — low, medium, high or "
                "pulsed.",
            ),
            (
                "ncr.Description",
                "treatment_outcome", "Treatment Outcome", "lu",
                "Where treatment was not completed as planned, the recorded reason. "
                "NULL normally means the course completed.",
            ),
            (
                "br.N11_10_END_DATE",
                "end_date", "End Date", "dt",
                "Date the brachytherapy course ended.",
            ),
            (
                "br.L_COMMENTS",
                "comments", "Comments", "txt",
                "Free-text registry comments recorded against this record.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_brachytherapy br
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON br.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN consultants_by_code c ON br.N11_2_CONSULTANT = c.NATIONAL_CODE
LEFT JOIN {SRC}.scr_organisationsites org1 ON br.N_SITE_CODE_DTT = org1.Code
LEFT JOIN {SRC}.scr_organisationsites org2 ON br.N11_1_SITE_CODE = org2.Code
LEFT JOIN {SRC}.scr_ltbltreatment_event te ON br.N_TREATMENT_EVENT = te.EVENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_setting ts ON br.N_TREATMENT_SETTING = ts.SET_CODE
LEFT JOIN {SRC}.scr_ltbltreatmentintent ti ON br.N11_6_TREATMENT_INTENT = ti.IntentID
LEFT JOIN {SRC}.scr_ltbltreatment_site tsite ON br.R_TREAT_TO = tsite.TREAT_ID
LEFT JOIN {SRC}.scr_ltblprocedures prc ON br.N11_8_TREATMENT_SITE = prc.PROC_CODE
LEFT JOIN {SRC}.scr_ltblbrachy_type bt ON br.N11_7_TYPE = bt.BRACH_TYPE_CODE
LEFT JOIN {SRC}.scr_ltblradio_isotope iso ON br.N11_17_ISOTOPE_TYPE = iso.ISOTOPE_CODE
LEFT JOIN {SRC}.scr_ltblradio_delivery dlv ON br.N11_20_DELIVERY_TYPE = dlv.DEL_CODE
LEFT JOIN {SRC}.scr_ltblpatient_type ptyp ON br.N11_19_UNSEALED = ptyp.PATIENT_CODE
LEFT JOIN {SRC}.scr_ltblradio_dose rdose ON br.N11_15_DOSE_RATE = rdose.DOSE_CODE
LEFT JOIN {SRC}.scr_ltblteletherapynotcompletereason ncr ON br.NotCompleteReasonID = ncr.ID
LEFT JOIN {SRC}.scr_ltbladjunctivetherapy adj ON br.AdjunctiveTherapyID = adj.AdjunctiveTherapyID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwTreatmentClinicalTrial",
        "target": "scr_treatment_clinical_trial",
        "ctes": ["consultants_by_code"],
        "comment": (
            "Clinical-trial entry, consent and decline records from the Somerset Cancer "
            "Register, rebuilt from base tables in {src} to the scr_22_02 "
            "BIvwTreatmentClinicalTrial definition. One row per tblMAIN_TRIAL record "
            "(1,140 at landing). Carries the reason-not-offered, reason-declined and "
            "reason-failed-screening free text, which is the part with research value "
            "and the reason those columns are ig_risk 4."
        ),
        "columns": [
            (
                "tr.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "st.STATUS_DESC",
                "clinical_trial_status", "Clinical Trial Status", "lu",
                "Whether the patient was entered into a trial, declined, was not "
                "offered one, or failed screening.",
            ),
            (
                "to_date(tr.L_CONSENT_DATE)",
                "consent_date", "Consent Date", "dt",
                "Date the patient consented to the trial.",
            ),
            (
                "tr.L_NOT_OFFERED",
                "reason_not_offered", "Reason Not Offered", "txt",
                "Free text reason a trial was not offered to this patient.",
            ),
            (
                "tr.L_NOT_ENTERED",
                "reason_declined", "Reason Declined", "txt",
                "Free text reason the patient declined the trial.",
            ),
            (
                "tr.ReasonFailedScreening",
                "reason_failed", "Reason Failed", "txt",
                "Free text reason the patient failed trial screening.",
            ),
            (
                "tr.L_TRIAL_NAME",
                "trial_name", "Trial Name", "lu",
                "Name of the clinical trial.",
            ),
            (
                "tty.TRIAL_TYPE_DESC",
                "trial_type", "Trial Type", "lu",
                "Type of trial, for example interventional or observational.",
            ),
            (
                "tr.R_LOC_NAT",
                "local_national_trial", "Local/National Trial", "lu",
                "Whether the trial is local or national.",
            ),
            (
                "tr.L_TRIAL_NO",
                "trial_number", "Trial Number", "lu",
                "The trial's registration or protocol number.",
            ),
            (
                "tr.L_REGIMEN",
                "regimen", "Regimen", "lu",
                "Treatment regimen the patient received under the trial protocol.",
            ),
            (
                "tr.L_DATE_RANDOMISED",
                "randomised_date", "Randomised Date", "dt",
                "Date the patient was randomised into a trial arm.",
            ),
            (
                "tr.L_START_DATE",
                "start_date", "Start Date", "dt",
                "Date the patient started treatment under the trial protocol.",
            ),
            (
                _consultant("c"),
                "consultant", "Consultant", "clin",
                "Responsible consultant, rendered as name then national code. "
                "Clinician identifier: identifying of staff rather than of the "
                "patient, hence a lower severity tag than patient identifiers.",
            ),
            (
                "tr.L_INVEST",
                "investigator", "Investigator", "clin",
                "Named clinical-trial investigator, free text as typed by the "
                "registry.",
            ),
            (
                "tr.L_COMMENTS",
                "comments", "Comments", "txt",
                "Free-text comments recorded against the trial record.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_trial tr
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON tr.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_ltbltrial_status st ON tr.N13_1_ENTERED = st.STATUS_CODE
LEFT JOIN {SRC}.scr_ltbltrial_type tty ON tr.N13_2_TREATMENT = tty.TRIAL_TYPE_CODE
LEFT JOIN consultants_by_code c ON tr.L_CONSULTANT = c.NATIONAL_CODE""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwCarePlanMDT",
        "target": "scr_care_plan_mdt",
        "ctes": ["consultants_by_code", "all_treatment_types", "mdt_meetings"],
        "comment": (
            "MDT discussion and cancer care-plan decisions from the Somerset Cancer "
            "Register, rebuilt from base tables in {src} to the scr_22_02 BIvwCarePlanMDT "
            "definition. One row per tblMAIN_CARE_PLAN record (304,986 at landing) — the "
            "largest treatment-plane product and the registry-only facet SCR is genuinely "
            "worth using for, since the MDT decision and its narrative exist nowhere in "
            "the Cerner record. First..Fourth Treatment resolve through vwAllTreatmentTypes "
            "for Urology only (Somerset's own site-conditional join), falling back to "
            "ltblTREATMENT_TYPE elsewhere."
        ),
        "columns": [
            (
                "mr.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "cp.N5_3_PLAN_AGREE_DATE",
                "care_plan_agreed_date", "Care Plan Agreed Date", "dt",
                "Date the cancer care plan was agreed.",
            ),
            (
                "org.Description",
                "organisation", "Organisation", "lu",
                "Organisation at which the care-plan decision was made.",
            ),
            (
                "intent.INTENT_DESC",
                "cancer_care_plan_intent", "Cancer Care Plan Intent", "lu",
                "Whether the agreed care plan is curative or palliative in intent.",
            ),
            (
                "coalesce(att1.TREATMENT_DESC, tt1.TREATMENT_DESC)",
                "first_treatment", "First Treatment", "lu",
                "First treatment modality in the agreed care plan.",
            ),
            (
                "coalesce(att2.TREATMENT_DESC, tt2.TREATMENT_DESC)",
                "second_treatment", "Second Treatment", "lu",
                "Second treatment modality in the agreed care plan.",
            ),
            (
                "coalesce(att3.TREATMENT_DESC, tt3.TREATMENT_DESC)",
                "third_treatment", "Third Treatment", "lu",
                "Third treatment modality in the agreed care plan.",
            ),
            (
                "coalesce(att4.TREATMENT_DESC, tt4.TREATMENT_DESC)",
                "fourth_treatment", "Fourth Treatment", "lu",
                "Fourth treatment modality in the agreed care plan.",
            ),
            (
                "cp.L_MDT_ACTION_BY",
                "to_be_actioned_by", "To be actioned by", "clin",
                "Free text naming who is to action the agreed MDT care plan.",
            ),
            (
                cat("cast(who.WHO_CODE AS string)", "who.WHO_DESC", sep=" - "),
                "performance_status", "Performance Status", "lu",
                "WHO/ECOG performance status, rendered as code then description — 0 "
                "fully active through 4 completely disabled. The standard "
                "fitness-for-treatment measure.",
            ),
            (
                "cmi.INDEX_DESC",
                "co_morbidity_index", "Co-morbidity Index", "lu",
                "Recorded comorbidity index band for the patient at the time of the "
                "care plan.",
            ),
            (
                "CASE cp.N5_1_MDT_MEETING_YN WHEN 'Y' THEN 'Yes' WHEN 'N' THEN 'No' ELSE NULL END",
                "was_patient_discussed_at_mdt", "Was patient discussed at MDT", "lu",
                "Whether the patient was discussed at a multidisciplinary team "
                "meeting.",
            ),
            (
                "mmt.MEETING_TYPE_DESC",
                "mdt_meeting_site", "MDT Meeting Site", "lu",
                "Which MDT meeting the patient was discussed at.",
            ),
            (
                "vwmeet.SUB_DESC",
                "sub_site", "Sub Site", "lu",
                "Cancer sub-site the MDT meeting covers.",
            ),
            (
                "cp.N5_2_MDT_DATE",
                "mdt_meeting_date", "MDT Meeting Date", "dt",
                "Date of the MDT meeting.",
            ),
            (
                "cp.L_LOCATION",
                "location", "Location", "lu",
                "Where the MDT meeting was held, as free text. A place, not a body "
                "site.",
            ),
            (
                "CASE cp.L_CARE_PLAN_AGREED WHEN 'Y' THEN 'Yes' WHEN 'N' THEN 'No' ELSE NULL END",
                "was_care_plan_agreed_at_mdt", "Was Care Plan Agreed at MDT", "lu",
                "Whether the care plan was agreed at the MDT meeting.",
            ),
            (
                "cp.L_MDT_COMMENTS",
                "mdt_comments", "MDT Comments", "txt",
                "Free-text record of the MDT discussion. Often the only narrative "
                "account of why a treatment decision was taken, which is why it is "
                "tagged as identifying free text.",
            ),
            (
                "cp.L_REFERRED_TO",
                "referred_to", "Referred To", "clin",
                "Free text naming the team or clinician the patient was referred on "
                "to.",
            ),
            (
                _consultant("c"),
                "who_referred_to", "Who Referred To", "clin",
                "Consultant the patient was referred on to at the MDT, as name then "
                "national code.",
            ),
            (
                "cp.L_PATH_REVIEW",
                "reviewed_by_pathologist_prior_to_mdt", "Reviewed by Pathologist prior to MDT", "lu",
                "Whether a pathologist reviewed the case before the MDT.",
            ),
            (
                "cp.L_REVIEW_WHO",
                "reviewed_by", "Reviewed By", "clin",
                "Free-text record of who reviewed the pathology ahead of the MDT.",
            ),
            (
                "notx.REASON_DESC",
                "no_treatment_reason", "No Treatment Reason", "lu",
                "Coded reason no active treatment was planned.",
            ),
            (
                "cp.L_NETWORK",
                "was_patient_discussed_at_network_mdt", "Was patient discussed at Network MDT", "lu",
                "Whether the case went to a cancer-network (cross-trust specialist) "
                "MDT.",
            ),
            (
                "cp.L_DATE_NETWORK_MEETING",
                "date_of_network_meeting", "Date of Network Meeting", "dt",
                "Date of the network MDT meeting.",
            ),
            (
                "cp.L_NETWORK_FEEDBACK",
                "network_decision", "Network Decision", "txt",
                "Free-text decision or feedback from the network MDT.",
            ),
            (
                "cp.L_NETWORK_ACTIONED",
                "network_to_be_actioned_by", "Network To be actioned by", "clin",
                "Free text naming who is to action the decision taken at the network "
                "MDT.",
            ),
            (
                "cp.L_NETWORK_COMMENTS",
                "network_comments", "Network Comments", "txt",
                "Free-text comments recorded against the network MDT discussion.",
            ),
            (
                "CASE lung.R_EGFR_STATUS WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' ELSE NULL END",
                "egfr_status_requested", "eGFR Status requested?", "lu",
                "Lung pathway: whether EGFR mutation testing was requested. EGFR "
                "status determines eligibility for targeted therapy.",
            ),
            (
                "CASE cp.L_RESECTIVE_PATHOLOGY WHEN 0 THEN 'No' WHEN 1 THEN 'Yes' END",
                "was_resective_pathology_discussed", "Was Resective Pathology Discussed?", "lu",
                "Whether resection pathology was discussed at the MDT.",
            ),
            (
                "CASE cp.CON_RES_DENTIST_ASKED_MDT WHEN 0 THEN 'No' WHEN 1 THEN 'Yes' WHEN 2 THEN 'Not present at MDT' END",
                "restorative_dentist_asked_if_assessment_needed", "Restorative Dentist asked if Restorative Assessment Needed?", "lu",
                "Head and neck pathway: whether the restorative dentist was asked at "
                "the MDT whether a restorative assessment was needed. Radiotherapy to "
                "the jaw makes later dental work hazardous, so this is asked before "
                "treatment starts.",
            ),
            (
                "CASE cp.DENTAL_ASSESS_NECESSARY WHEN 0 THEN 'No' WHEN 1 THEN 'Yes' END",
                "restorative_dental_assessment_necessary", "Restorative dental assessment necessary?", "lu",
                "Whether a restorative dental assessment was judged necessary.",
            ),
            (
                "distype.Description",
                "discussion_type", "Discussion Type", "lu",
                "Type of MDT discussion held.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_care_plan cp
LEFT JOIN {SRC}.scr_ltblno_treatment notx ON cp.N5_8_NO_TREATMENT = notx.REASON_CODE
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON cp.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_organisationsites org ON cp.N1_3_ORG_CODE_DECISION = org.Code
LEFT JOIN {SRC}.scr_ltblintent intent ON cp.N5_5_CARE_INTENT = intent.INTENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_type tt1 ON cp.N5_6_TREATMENT_TYPE_1 = tt1.TREATMENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_type tt2 ON cp.N5_6_TREATMENT_TYPE_2 = tt2.TREATMENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_type tt3 ON cp.N5_6_TREATMENT_TYPE_3 = tt3.TREATMENT_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_type tt4 ON cp.N5_6_TREATMENT_TYPE_4 = tt4.TREATMENT_CODE
LEFT JOIN all_treatment_types att1 ON mr.L_CANCER_SITE = 'Urology' AND cp.N5_6_TREATMENT_TYPE_1 = att1.TREATMENT_CODE
LEFT JOIN all_treatment_types att2 ON mr.L_CANCER_SITE = 'Urology' AND cp.N5_6_TREATMENT_TYPE_2 = att2.TREATMENT_CODE
LEFT JOIN all_treatment_types att3 ON mr.L_CANCER_SITE = 'Urology' AND cp.N5_6_TREATMENT_TYPE_3 = att3.TREATMENT_CODE
LEFT JOIN all_treatment_types att4 ON mr.L_CANCER_SITE = 'Urology' AND cp.N5_6_TREATMENT_TYPE_4 = att4.TREATMENT_CODE
LEFT JOIN {SRC}.scr_ltblwho who ON cp.N5_10_WHO_STATUS = who.WHO_CODE
LEFT JOIN {SRC}.scr_ltblcomorbidity_index cmi ON cp.N5_9_CO_MORBIDITY = cmi.INDEX_CODE
LEFT JOIN {SRC}.scr_ltblmdt_meeting_type mmt ON cp.L_MDT_SITE = mmt.MEETING_TYPE_ID
LEFT JOIN consultants_by_code c ON cp.L_WHO_REFERRED_TO = c.NATIONAL_CODE
LEFT JOIN {SRC}.scr_tblmdt_list ml1 ON cp.TEMP_ID = cast(ml1.MDT_ID AS string)
LEFT JOIN {SRC}.scr_tblmdt_list ml2 ON ml1.MEETING_ID = ml2.MDT_ID
LEFT JOIN mdt_meetings vwmeet ON ml2.MEETING_ID = vwmeet.MEETING_ID
LEFT JOIN {SRC}.scr_tbllung_mdt lung ON cp.PLAN_ID = lung.PLAN_ID
LEFT JOIN {SRC}.scr_ltblmdtdiscussiontype distype ON distype.ID = cp.MDTDiscussionType""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwPathology",
        "target": "scr_pathology",
        "ctes": ["consultants_by_code"],
        "comment": (
            "Cancer-registry pathology reports from the Somerset Cancer Register, rebuilt "
            "from base tables in {src} to the scr_22_02 BIvwPathology definition. One row "
            "per tblMAIN_PATHOLOGY record (11,583 at landing). This is registry-abstracted "
            "pathology — orders of magnitude smaller than the Cerner/LIMS pathology plane "
            "behind bronze.pathology_*; it earns its place on staging, grade, margins and "
            "SNOMED morphology, not on volume."
        ),
        "columns": [
            (
                "p.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "p.N8_2_RECEIPT_DATE",
                "date_of_receipt", "Date of Receipt", "dt",
                "Date the laboratory received the specimen.",
            ),
            (
                "p.N8_3_RESULT_DATE",
                "date_of_reporting", "Date of Reporting", "dt",
                "Date the pathology report was issued.",
            ),
            (
                "p.N8_20_REPORT_NUMBER",
                "report_no", "Report No", "id",
                "The laboratory's report number for this pathology report, as "
                "transcribed into the registry. Use to tie a registry-abstracted "
                "report back to the source lab report.",
            ),
            (
                "it.TYPE_DESC",
                "investigation_type", "Investigation Type", "lu",
                "Type of pathological investigation performed.",
            ),
            (
                "sp.SPECIMEN_DESC",
                "nature_of_specimen", "Nature of Specimen", "lu",
                "What kind of specimen was examined (biopsy, resection and so on).",
            ),
            (
                "rs.STATUS_DESC",
                "report_status", "Report Status", "lu",
                "Status of the pathology report, for example provisional or final.",
            ),
            (
                _consultant("c1"),
                "surgeon", "Surgeon", "clin",
                "Clinician who requested the pathology investigation, as name then "
                "national code. Somerset labels this Surgeon, but it is the requester "
                "rather than the operator.",
            ),
            (
                "org1.Description",
                "requesting_organisation", "Requesting Organisation", "lu",
                "Organisation that requested the pathology investigation.",
            ),
            (
                _consultant("c2"),
                "pathologist", "Pathologist", "clin",
                "Pathologist who reported the specimen, as name then national code.",
            ),
            (
                "org2.Description",
                "reporting_organisation", "Reporting Organisation", "lu",
                "Organisation whose laboratory reported the pathology result.",
            ),
            (
                cat("diag.DIAG_CODE", "diag.DIAG_DESC", sep=" - "),
                "diagnosis_site_icd", "Diagnosis Site (ICD)", "lu",
                "Anatomical site of the diagnosis as an ICD code then description. "
                "The registry's abstracted site, not the raw laboratory text.",
            ),
            (
                cat("cast(snc.SNOMEDCode AS string)", "snc.Description", sep=" - "),
                "diagnosis_site_snomed", "Diagnosis Site (SNOMED)", "lu",
                "Anatomical site of the diagnosis as a SNOMED CT code then "
                "description.",
            ),
            (
                "lat.LAT_DESC",
                "laterality", "Laterality", "lu",
                "Which side of a paired organ the tumour is on.",
            ),
            (
                "p.N8_8_INVASIVE_LESION",
                "max_tumour_diameter_mm", "Max Tumour Diameter (mm)", "meas",
                "Maximum diameter of the invasive lesion in millimetres, as "
                "abstracted from the pathology report.",
            ),
            (
                "p.N8_14_NODES",
                "no_of_nodes_examined", "No of Nodes Examined", "meas",
                "Number of lymph nodes examined in the specimen.",
            ),
            (
                "p.N8_15_POSITIVE_NODES",
                "no_of_positive_nodes", "No of Positive Nodes", "meas",
                "Number of examined lymph nodes found to contain tumour.",
            ),
            (
                cat("cast(snct.CT_Concept_ID AS string)", "snct.CT_Description", sep=" - "),
                "type_snomed", "Type (SNOMed)", "lu",
                "Tumour morphology as a SNOMED code then description.",
            ),
            (
                cat("diff.GRADE_CODE", "diff.GRADE_DESC", sep=" - "),
                "grade_of_differentiation", "Grade of Differentiation", "lu",
                "How closely the tumour resembles normal tissue, as code then "
                "description — well, moderately or poorly differentiated. A core "
                "prognostic factor.",
            ),
            (
                "mg.MARGIN_DESC",
                "excision_margins", "Excision Margins", "lu",
                "Whether the excision margins were clear of tumour, as assessed by "
                "the pathologist.",
            ),
            (
                "vli.VASC_LYMPH_DESC",
                "vascular_lymphatic_invasion", "Vascular/Lymphatic Invasion", "lu",
                "Whether tumour was seen invading blood or lymphatic vessels — a "
                "prognostic factor for spread.",
            ),
            (
                "CASE p.N8_9_SYNC_TUMOUR WHEN 'Y' THEN 'Yes' WHEN 'N' THEN 'No' WHEN '9' THEN 'Not Known' ELSE NULL END",
                "synchronous_tumour", "Synchronous Tumour", "lu",
                "Whether a second tumour was found at the same time as the primary.",
            ),
            (
                _PATH_STAGE_EXPR,
                "pathological_staging", "Pathological Staging", "lu",
                "Pathological T, N and M rendered as one display string of the form "
                "'pT: <t> pN: <n> pM: <m>'. NULL where none of the three is recorded.",
            ),
            (
                "p.L_COMMENTS",
                "comments", "Comments", "txt",
                "Free-text comments recorded against the pathology report.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_pathology p
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON p.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_ltblinvestigation_type it ON p.N8_1_PATHOLOGY_TYPE = it.TYPE_CODE
LEFT JOIN {SRC}.scr_ltblspecimen sp ON p.N8_22_SPECIMEN_NATURE = sp.SPECIMEN_CODE
LEFT JOIN {SRC}.scr_ltblreport_status rs ON p.N8_21_REPORT_STATUS = rs.STATUS_CODE
LEFT JOIN consultants_by_code c1 ON p.N8_24_REQUEST_BY = c1.NATIONAL_CODE
LEFT JOIN {SRC}.scr_organisationsites org1 ON p.N8_23_REQUEST_ORG = org1.Code
LEFT JOIN consultants_by_code c2 ON p.N8_4_PATHOLOGIST = c2.NATIONAL_CODE
LEFT JOIN {SRC}.scr_organisationsites org2 ON p.N8_5_ORG_CODE = org2.Code
LEFT JOIN {SRC}.scr_ltbldiagnosis diag ON p.N8_6_DIAGNOSIS = diag.DIAG_CODE
LEFT JOIN {SRC}.scr_ltbllaterality lat ON p.N8_7_TUMOUR_LATERALITY = lat.LAT_CODE
LEFT JOIN {SRC}.scr_ltblsnomedct snct ON snct.CT_Snomed_ID = p.SNOMedCT
LEFT JOIN {SRC}.scr_ltbldifferentiation diff ON p.N8_11_GRADE_DIFF = diff.GRADE_CODE
LEFT JOIN {SRC}.scr_ltblmargins mg ON p.N8_13_EXCISION_MARGINS = mg.MARGIN_CODE
LEFT JOIN {SRC}.scr_snomedctconcepts snc ON snc.ID = p.SNOMEDDiagnosisID
LEFT JOIN {SRC}.scr_ltblvasc_lymph_invasion vli ON vli.VASC_LYMPH_CODE = p.N8_12_CANCER_INVASION""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwStaging",
        "target": "scr_staging",
        "ctes": ["dukes_ann_stage_code"],
        "comment": (
            "Pre-treatment (clinical) and final (pathological) TNM staging plus certainty "
            "factors from the Somerset Cancer Register, rebuilt from base tables in {src} "
            "to the scr_22_02 BIvwStaging definition. One row per tblMAIN_REFERRALS record "
            "(514,784 at landing) — note that is every referral, so most rows carry no "
            "stage at all; stage presence is itself the finding. DIVERGENCE: Somerset's 14 "
            "LEFT JOINs to tblREFERRAL_* project no columns and are dropped here; the "
            "build asserts each is unique on CARE_ID so the drop is provably lossless. "
            "Dukes and Ann Arbor share one source column (N6_9_SITE_CLASSIFICATION) and "
            "are split by cancer site."
        ),
        "columns": [
            (
                "mr.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "to_date(mr.ClinicalTNMDate)",
                "pre_treatment_staging_date", "Pre-Treatment Staging Date", "dt",
                "Date the pre-treatment (clinical) TNM stage was assigned.",
            ),
            (
                _stage_part("mr.ClinicalTStage", "mr.ClinicalTLetter"),
                "pre_treatment_t_stage", "Pre-Treatment T Stage", "lu",
                "Clinical T stage — primary tumour extent — with its letter suffix "
                "appended.",
            ),
            (
                _stage_part("mr.ClinicalNStage", "mr.ClinicalNLetter"),
                "pre_treatment_n_stage", "Pre-Treatment N Stage", "lu",
                "Clinical N stage — regional lymph node involvement — with its letter "
                "suffix.",
            ),
            (
                _stage_part("mr.ClinicalMStage", "mr.ClinicalMLetter"),
                "pre_treatment_m_stage", "Pre-Treatment M Stage", "lu",
                "Clinical M stage — distant metastasis — with its letter suffix.",
            ),
            (
                _staging_string("Clinical"),
                "pre_treatment_staging", "Pre-Treatment Staging", "lu",
                "Clinical T, N and M concatenated as a single TNM string with spaces "
                "stripped. NULL where all three are absent (the bare string 'TNM').",
            ),
            (
                "mr.ClinicalTCertainty",
                "pre_treatment_certainty_factor_t_stage", "Pre-Treatment Certainty Factor T Stage", "lu",
                "Certainty factor (C-factor) for the clinical T stage — the strength "
                "of evidence behind it, from clinical examination through to "
                "histopathology.",
            ),
            (
                "mr.ClinicalNCertainty",
                "pre_treatment_certainty_factor_n_stage", "Pre-Treatment Certainty Factor N Stage", "lu",
                "Certainty factor for the clinical N stage.",
            ),
            (
                "mr.ClinicalMCertainty",
                "pre_treatment_certainty_factor_m_stage", "Pre-Treatment Certainty Factor M Stage", "lu",
                "Certainty factor for the clinical M stage.",
            ),
            (
                "mr.ClinicalOverallCertainty",
                "pre_treatment_certainty_factor_overall", "Pre-Treatment Certainty Factor Overall", "lu",
                "Overall certainty factor for the clinical stage.",
            ),
            (
                certainty_staging("Clinical"),
                "pre_treatment_certainty_factors_staging", "Pre-Treatment Certainty Factors Staging", "lu",
                "The four clinical certainty factors rendered as one display string.",
            ),
            (
                "to_date(mr.PathologicalTNMDate)",
                "final_staging_date", "Final Staging Date", "dt",
                "Date the final (integrated, post-pathology) TNM stage was assigned.",
            ),
            (
                _stage_part("mr.PathologicalTStage", "mr.PathologicalTLetter"),
                "final_t_stage", "Final T Stage", "lu",
                "Final pathological T stage with its letter suffix.",
            ),
            (
                _stage_part("mr.PathologicalNStage", "mr.PathologicalNLetter"),
                "final_n_stage", "Final N Stage", "lu",
                "Final pathological N stage with its letter suffix.",
            ),
            (
                _stage_part("mr.PathologicalMStage", "mr.PathologicalMLetter"),
                "final_m_stage", "Final M Stage", "lu",
                "Final pathological M stage with its letter suffix.",
            ),
            (
                _staging_string("Pathological"),
                "final_staging", "Final Staging", "lu",
                "Final T, N and M concatenated as a single TNM string with spaces "
                "stripped. NULL where all three are absent.",
            ),
            (
                "mr.PathologicalTCertainty",
                "final_certainty_factor_t_stage", "Final Certainty Factor T Stage", "lu",
                "Certainty factor for the final T stage.",
            ),
            (
                "mr.PathologicalNCertainty",
                "final_certainty_factor_n_stage", "Final Certainty Factor N Stage", "lu",
                "Certainty factor for the final N stage.",
            ),
            (
                "mr.PathologicalMCertainty",
                "final_certainty_factor_m_stage", "Final Certainty Factor M Stage", "lu",
                "Certainty factor for the final M stage.",
            ),
            (
                "mr.PathologicalOverallCertainty",
                "final_certainty_factor_overall", "Final Certainty Factor Overall", "lu",
                "Overall certainty factor for the final stage.",
            ),
            (
                certainty_staging("Pathological"),
                "final_certainty_factors_staging", "Final Certainty Factors Staging", "lu",
                "The four final certainty factors rendered as one display string.",
            ),
            (
                f"CASE mr.L_CANCER_SITE WHEN 'Colorectal' THEN dukes.DescName ELSE {_EMPTY_ELSE} END",
                "dukes", "Dukes", "lu",
                "Dukes stage for colorectal cancer (A to D). Dukes and Ann Arbor "
                "share one source column and are separated by cancer site, so this is "
                "populated for Colorectal only.",
            ),
            (
                f"CASE mr.L_CANCER_SITE WHEN 'Haematology' THEN dukes.DescName ELSE {_EMPTY_ELSE} END",
                "ann_arbor_stage", "Ann Arbor Stage", "lu",
                "Ann Arbor stage for lymphoma (I to IV). Shares its source column "
                "with Dukes and is populated for Haematology only.",
            ),
            (
                "spec.SPECIALTY_DESC",
                "specialty", "Specialty", "lu",
                "Main specialty the patient was referred under, decoded to its "
                "national description.",
            ),
            (
                "mr.FETOPROTEIN",
                "alpha_fetoprotein_csf", "Alpha Fetoprotein (Cerebrospinal Fluid)", "meas",
                "Alpha-fetoprotein measured in cerebrospinal fluid — a CNS germ-cell "
                "tumour marker.",
            ),
            (
                "mr.GONADOTROPIN",
                "beta_hcg", "Beta Human Chorionic Gonadotropin", "meas",
                "Beta human chorionic gonadotropin measured in cerebrospinal fluid — "
                "a CNS germ-cell tumour marker.",
            ),
            (
                "germ.GERM_CELL_NON_CNS_DESC",
                "tnm_non_cns_germ", "TNM Non CNS Germ", "lu",
                "Staging classification for non-CNS germ-cell tumours.",
            ),
            (
                "mr.GONADOTROPIN_SERUM",
                "beta_hcg_serum", "Beta Human Chorionic Gonadotropin (Serum)", "meas",
                "Beta human chorionic gonadotropin measured in serum, a germ-cell "
                "tumour marker.",
            ),
            (
                "mr.FETOPROTEIN_SERUM",
                "alpha_fetoprotein_serum", "Alpha Fetoprotein (Serum)", "meas",
                "Alpha-fetoprotein measured in serum, a germ-cell tumour marker.",
            ),
            (
                "orgpre.Description",
                "organisation_pre_treatment", "Organisation (Pre-Treatment)", "lu",
                "Organisation that recorded the pre-treatment (clinical) TNM stage.",
            ),
            (
                "orgfin.Description",
                "organisation_final", "Organisation (Final)", "lu",
                "Organisation that recorded the final (post-pathology, integrated) "
                "TNM stage.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_referrals mr
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_ltblspecialties spec ON mr.N2_8_SPECIALTY = spec.SPECIALTY_CODE
LEFT JOIN dukes_ann_stage_code dukes ON mr.N6_9_SITE_CLASSIFICATION = dukes.IdCode
LEFT JOIN {SRC}.scr_ltblgerm_cell_non_cns germ ON mr.GERM_CELL_NON_CNS_ID = germ.GERM_CELL_NON_CNS_ID
LEFT JOIN {SRC}.scr_organisationsites orgpre ON mr.TNMOrganisation = orgpre.ID
LEFT JOIN {SRC}.scr_organisationsites orgfin ON mr.TNMOrganisation_Integrated = orgfin.ID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwFollowUp",
        "target": "scr_follow_up",
        "ctes": [],
        "comment": (
            "Post-treatment assessment / follow-up records from the Somerset Cancer "
            "Register, rebuilt from base tables in {src} to the scr_22_02 BIvwFollowUp "
            "definition. One row per tblMAIN_ASSESSMENT record. WARNING: Barts populates "
            "8 rows in total — this view is effectively dead locally and is published as "
            "evidence of that, not as a usable follow-up product. Do not build a research "
            "cohort on it; use Cerner encounter and appointment data for follow-up."
        ),
        "columns": [
            (
                "a.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "org.Description",
                "organisation", "Organisation", "lu",
                "Organisation that carried out the follow-up assessment.",
            ),
            (
                "a.N14_1_ASSESSMENT_DATE",
                "date_of_assessment", "Date of Assessment", "dt",
                "Date of the assessment this record describes.",
            ),
            (
                "a.L_FOLLOWUP",
                "follow_up_period", "Follow Up Period", "lu",
                "Which follow-up interval this assessment represents.",
            ),
            (
                "fu.FOLLOWUP_DESC",
                "follow_up_status", "Follow Up Status", "lu",
                "Outcome status recorded at follow-up.",
            ),
            (
                "sft.Description",
                "stratified_follow_up_type", "Stratified Follow-up Type", "lu",
                "Which stratified follow-up pathway the patient is on — for example "
                "patient-initiated rather than routine scheduled review.",
            ),
            (
                "tum.TUMOUR_DESC",
                "primary_tumour_status", "Primary Tumour Status", "lu",
                "Status of the primary tumour at follow-up: absent, present or "
                "progressing.",
            ),
            (
                "nod.NODAL_DESC",
                "nodal_status", "Nodal Status", "lu",
                "Status of nodal disease at follow-up.",
            ),
            (
                "mets.METS_DESC",
                "metastatic_status", "Metastatic Status", "lu",
                "Status of metastatic disease at follow-up.",
            ),
            (
                "mkr.MARKER_DESC",
                "marker_response_status", "Marker Response Status", "lu",
                "Tumour-marker response recorded at follow-up.",
            ),
            (
                cat("cast(who.WHO_CODE AS string)", "who.WHO_DESC", sep=" - "),
                "performance_status_who", "Performance Status (WHO)", "lu",
                "WHO/ECOG performance status recorded at follow-up, as code then "
                "description.",
            ),
            (
                "morb.TYPE_DESC",
                "treatment_morbidity", "Treatment Morbidity", "lu",
                "Type of treatment-related morbidity recorded at follow-up.",
            ),
            (
                "a.L_HEIGHT",
                "height", "Height", "meas",
                "Height as recorded at assessment. Units are as typed; usually "
                "metres.",
            ),
            (
                "a.L_WEIGHT",
                "weight", "Weight", "meas",
                "Weight as recorded at assessment. Units are as typed; usually "
                "kilograms.",
            ),
            (
                "colo.MODE_DESC",
                "mode_of_follow_up", "Mode of Follow-up", "lu",
                "How the follow-up contact took place.",
            ),
            (
                "a.L_OTHER_MODE",
                "other_mode_of_follow_up", "Other Mode of Follow-up", "txt",
                "Free text describing a follow-up mode not on the picklist.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_assessment a
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON a.CARE_ID = mr.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON mr.PATIENT_ID = d.PATIENT_ID
LEFT JOIN {SRC}.scr_organisationsites org ON a.R_ORG_CODE = org.Code
LEFT JOIN {SRC}.scr_ltblfollow_up fu ON a.N14_9_FOLLOW_UP = fu.FOLLOWUP_CODE
LEFT JOIN {SRC}.scr_ltbltumour_status tum ON a.N14_2_TUMOUR_STATUS = tum.TUMOUR_CODE
LEFT JOIN {SRC}.scr_ltblnodal_status nod ON a.N14_3_NODE_STATUS = nod.NODAL_CODE
LEFT JOIN {SRC}.scr_ltblmets_status mets ON a.N14_4_METS_STATUS = mets.METS_CODE
LEFT JOIN {SRC}.scr_ltblmarker_response mkr ON a.N14_5_MARKER_RESPONSE = mkr.MARKER_CODE
LEFT JOIN {SRC}.scr_ltblwho who ON a.N14_6_WHO_STATUS = who.WHO_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_morbid morb ON a.N14_7_MORBIDITY_TYPE = morb.TYPE_CODE
LEFT JOIN {SRC}.scr_ltblcolo_followup colo ON colo.MODE_CODE = a.R_MODE_FOLLOWUP
LEFT JOIN {SRC}.scr_stratifiedfollowuptype sft ON sft.ID = a.StratifiedFollowupType""",
    },
]

print(
    f"{len(LEAF_SPECS)} leaf specs, "
    f"{sum(len(s['columns']) for s in LEAF_SPECS)} mapped columns"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The UNION roll-ups
# MAGIC
# MAGIC **Why the roll-ups are in scope.** Ben's ruling was to include them if they are
# MAGIC genuinely more comprehensible than the parts. They are, and the reason is
# MAGIC harmonisation rather than convenience: `BIvwAllSurgery` is the only place where
# MAGIC twelve differently-shaped specialty surgery tables are reconciled onto one column
# MAGIC list, and reproducing that reconciliation is most of the work. Handing a researcher
# MAGIC the twelve extension tables instead would hand them the same job.
# MAGIC
# MAGIC `SURGERY_COLUMNS` and `COMPLICATION_COLUMNS` below are the schema blocks for
# MAGIC `scr_all_surgery` and `scr_all_complications` — one `_col(...)` per published column,
# MAGIC carrying its meaning, with the per-arm SQL supplied by the branch builders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Divergences from the Somerset definitions
# MAGIC
# MAGIC Recorded here rather than buried in comments, and echoed into the step's exit
# MAGIC payload so a pipeline run reports them.

# COMMAND ----------

DIVERGENCES = [
    (
        "BIvwBreastReferrals (and its 13 site siblings)",
        "tblDEFINITIVE_TREATMENT is not unique on CARE_ID at TREAT_NO = 1: three "
        "CARE_IDs (58239, 126576, 127179) carry a second row in the 2026-09-03 "
        "landing, so the Somerset join fans the referral spine out by three rows. "
        "In each pair one row holds the treatment, its decision and start dates "
        "and its organisation, and the other is a stub carrying only tracking "
        "comments; for CARE_ID 58239 the populated row is the LOWER TREATMENT_ID, "
        "so a latest-wins rule would elect the empty one. The join goes through a "
        "definitive_treatment_first CTE that orders on has-data tests first and "
        "falls back to the highest TREATMENT_ID, and the build asserts the "
        "published spine is one row per care_id.",
    ),
    (
        "BIvwAllSurgery / BIvwTreatmentSurgery",
        "Somerset decodes the specialty procedure lookups on PROC_CODE, which is not "
        "their key. In the eight site lookups (colorectal, gynaecology, haematology, "
        "lung, paediatric, skin, upper GI, urology) PROC_CODE is a coarse category and "
        "PROC_ID is the row identity: ltblCOLO_PROCEDURES holds 521 rows across 17 "
        "distinct PROC_CODE values, and code 1 alone spans 161 different descriptions. "
        "tblMAIN_SURGERY.L_PROCEDURE holds PROC_ID values — every populated value "
        "matches PROC_ID (colorectal 7,689, gynaecology 2,436, urology 1,588, skin "
        "1,331, lung 1,498, upper GI 1,033, haematology 314, paediatric 11) while only "
        "a stray minority fall inside the PROC_CODE range. Joining on PROC_CODE both "
        "fans the roll-up out (87,596 rows for 51,169 surgeries, which breaks the 1:1 "
        "join into BIvwTreatmentSurgery) and attaches the wrong procedure name to the "
        "rows that do match. These eight arms join on PROC_ID. Breast "
        "(ltblBREAST_PROCEDURE, 77 rows unique on PROC_CODE, 2,216/2,216 matched), "
        "Head and Neck (ltblHNPROCEDURE, keyed ID, 1,191/1,191) and the generic "
        "ltblPROCEDURES behind the Brain and Sarcoma arms (unique on PROC_CODE across "
        "11,428 rows; all 103 Brain and 1 Sarcoma values match PROC_CODE and none "
        "match ID) are already keyed correctly and are unchanged.",
    ),
    (
        "BIvwAllComplications",
        "Somerset UNIONs BIvwUpperGIComplications twice — it is the first and the last "
        "arm of the UNION ALL. Every Upper GI surgery therefore appears twice in "
        "AllComplications, and because BIvwTreatmentSurgery joins AllComplications on "
        "(CARE_ID, SURGERY_ID), every Upper GI row of that 68-column view is doubled. "
        "The duplicate arm is dropped. This is the single highest-impact fix in the "
        "rebuild; the build asserts the roll-up is unique on (care_id, surgery_id).",
    ),
    (
        "BIvwAllComplications",
        "ltblCOMPLICATIONS_BRAIN is a populated lookup with no complications view, so "
        "Brain surgical complications are absent from Somerset's roll-up. A Brain arm is "
        "added on the same pattern as the other site arms. CUP and Paediatric remain "
        "uncovered: neither has a site lookup nor a home in the 'General' site list, so "
        "there is nothing to decode against.",
    ),
    (
        "BIvwAllSurgery",
        "Somerset has no Sarcoma arm, so 2,424 sarcoma referrals' surgery is invisible in "
        "the roll-up. A Sarcoma arm is added over tblSURGERY_SARCOMA. Its extension table "
        "uses different column names (Incision, IndicationSurgery, PostOpInstructions, "
        "OpNote) and has no findings/specimens/drains/closure columns at all, so those "
        "publish as NULL. Procedures resolve through ltblPROCEDURES, which is what "
        "Somerset's own BIvwTreatmentSurgery already does for Sarcoma.",
    ),
    (
        "BIvwAllSurgery",
        "Somerset's roll-up projects 25 columns and drops every SNOMED procedure code and "
        "every branch-specific column (breast laterality and reconstruction, colorectal "
        "laparoscopic approach, head-and-neck neck dissection, the CNS operative-smear "
        "block). Those are carried here on the shared column list instead of discarded — "
        "the harmonisation is the point of the roll-up, the truncation is not.",
    ),
    (
        "BIvwAllSurgery",
        "The Paediatric arm omits [Assistant 2 Grade] where all other arms carry it; "
        "tblMAIN_SURGERY.L_GRADE_2 is populated regardless of specialty, so under "
        "fix_somerset_defects the column is emitted for Paediatric too.",
    ),
    (
        "BIvwColorectalSurgery",
        "Six columns are hard-coded to SPACE(30) — a 30-space string, not a NULL: Start "
        "Time, End Time, Specimens, Drains, Closure, Post-Op Instructions. The colorectal "
        "extension table genuinely has no L_SPECIMENS/L_DRAINS/L_CLOSURE/L_POST_OP and no "
        "L_START_TIME/L_END_TIME columns (confirmed against the landed schema), so the "
        "data is absent — but SPACE(30) makes absent look recorded to any consumer "
        "testing for NULL. Under fix_somerset_defects these publish as NULL. Same "
        "treatment for Skin [Specimens] and Lung [Procedure Comments], both of which are "
        "likewise absent from their extension tables.",
    ),
    (
        "BIvwTreatmentSurgery",
        "The [Primary Procedure] CASE routes CUP through ltblPROCEDURES on "
        "N7_11_PROCEDURE_2 — the *second* procedure column — while every other arm reads "
        "N7_10_PROCEDURE_1. A CUP patient's primary procedure is therefore reported as "
        "their sub-procedure. Under fix_somerset_defects the CUP arm is dropped so CUP "
        "falls through to the ELSE branch and reads N7_10_PROCEDURE_1.",
    ),
    (
        "BIvwTreatmentSurgery",
        "Somerset's FROM clause is a nested RIGHT OUTER JOIN chain emitted by the SQL "
        "Server view designer. It resolves to tblMAIN_SURGERY as the preserved spine with "
        "everything else LEFT JOINed; it is rewritten in that form. Same for "
        "BIvwPresentation (spine tblINITIAL_ASSESSMENT) and BIvwHaematologySurgery.",
    ),
    (
        "BIvwHaematologySurgery",
        "The nested RIGHT OUTER chain makes tblSURGERY_HAEMATOLOGY the preserved side, "
        "not tblMAIN_SURGERY — the opposite of every other arm, so a haematology "
        "extension row with no parent surgery would survive. Written as an INNER join "
        "like the other arms; the build counts orphaned extension rows per branch and "
        "reports them, so the divergence is measured rather than assumed harmless.",
    ),
    (
        "BIvwStaging",
        "14 LEFT JOINs to tblREFERRAL_* project no columns. Dropped, with an asserted "
        "uniqueness check on CARE_ID for all 14 so the drop is provably lossless.",
    ),
    (
        "BIvwPathology",
        "[Pathological Staging] NULLIFs its concatenation against 'pT: pN: pM: ', but the "
        "expression actually produces 'pT:  pN:  pM: ' (two spaces after each colon) when "
        "nothing is staged, so the sentinel never matches and every unstaged report is "
        "published as a non-empty string. See _path_stage() above.",
    ),
    (
        "BIvw*Referrals",
        "14 near-identical site views over one spine that L_CANCER_SITE partitions "
        "exactly (14 sites, no NULLs, verified over 517,281 rows). Rebuilt as one pass "
        "with a site-tagged symptom lookup union — provably equivalent, and it scans the "
        "spine once rather than fourteen times.",
    ),
    (
        "BIvw*Referrals",
        "CUP and Other declare no symptom lookup (Other declares ltblSYMPTOMS_BRAIN and "
        "then never uses it) and emit SPACE(30). Published as NULL. Decoding a Brain "
        "symptom lookup against a non-Brain referral is not attempted — the symptom IDs "
        "are per-site and the result would be confidently wrong.",
    ),
    (
        "BIvwTreatmentSurgery",
        "[Urgency of Surgery] is published from tblMAIN_SURGERY.L_URGENCY, which is "
        "already free text ('Elective', 'Scheduled', 'Urgent', 'Emergency') and populated "
        "on 28,351 rows. The newer UrgencyOfSurgeryID is populated on 739 rows and "
        "disagrees with L_URGENCY where both exist (e.g. 'Elective' appears against IDs "
        "1, 2 and 4), so it is not used and no lookup is inferred for it.",
    ),
    (
        "BIvwPresentation / BIvwBreastReferrals",
        "Three flags coerce unrecorded to a negative: UroHaematuria and UroRaisedPSA use "
        "ELSE 'No', and VALIDATED uses ELSE 'No' where the definitive-treatment row may "
        "simply be absent. Under fix_somerset_defects these publish NULL when unrecorded, "
        "so 'not recorded' stops masquerading as 'tested negative'.",
    ),
]

UNRESOLVED_SOURCE_QUESTIONS = [
    "BIvwPresentation resolves [Clinical Assessment Result (Left breast)] from "
    "tblINITIAL_ASSESSMENT.R_FINDINGS and the right-breast column from L_FINDINGS. That "
    "looks inverted, but L_/R_ are namespace prefixes throughout this schema (L_ = local "
    "field, R_ = registry field), not laterality, so the mapping may well be correct and "
    "swapping it could INTRODUCE an inversion. Reproduced faithfully and flagged in both "
    "column comments. Confirm against a known case before using either column for "
    "laterality.",
    "The Brain surgery extension carries an operative-smear and anti-cancer-implant block "
    "mirroring the Paediatric CNS block, but BIvwBrainSurgery projects none of it and the "
    "1/0 encoding Paediatric uses is not confirmed for Brain. Left unpublished rather "
    "than decoded on an assumed encoding.",
    "BIvwPresentation's symptom-onset columns guard the fallback from N_HN11_SYMPTOM_DATE "
    "with a multi-part condition that the rendered Somerset documentation truncates. "
    "Implemented as coalesce(explicit part, part extracted from the date), which "
    "preserves any explicitly recorded value and falls back only when it is absent.",
    "tblSURGERY_BREAST.L_CANCELLED is a 0/1 flag (5,503 zeros, 15 ones) rather than the "
    "free text its pairing with L_CANCEL_OP suggests; decoded as Yes/No. Head and Neck "
    "and Lung use the same column pair and are assumed to share the encoding.",
    "Colorectal Start/End Time are SPACE(30) in Somerset even though R_START_TIME and "
    "R_END_TIME exist on the extension table. Not substituted: the names differ from "
    "every other arm and Somerset never decoded them, so promoting them would publish a "
    "value no Somerset consumer has seen. Worth confirming with the registry team.",
]

KNOWN_MISSING_LOOKUPS = [
    ("ltblGYNAE_INCISION", "scr_all_surgery.gynae_incision_code", "gynaecology incision site"),
    ("ltblHN_PROC_NECK", "scr_all_surgery.hn_neck_procedure_code", "head & neck neck-dissection procedure"),
    ("ltblHNReconstruction", "scr_all_surgery.hn_reconstruction_code / _2_code", "head & neck reconstruction"),
    ("ltblEXCISION_TYPE", "scr_all_surgery.cns_excision_type_code", "CNS excision type"),
    ("ltblBRAIN_LESION_LOCATION", "scr_all_surgery.cns_tumour_location_code", "CNS tumour location"),
]

SURGERY_EXTENSION_TABLES = [
    "scr_tblsurgery_brain", "scr_tblsurgery_breast", "scr_tblsurgery_colorectal",
    "scr_tblsurgery_dermatology", "scr_tblsurgery_gynaecology",
    "scr_tblsurgery_haematology", "scr_tblsurgery_head_neck", "scr_tblsurgery_lung",
    "scr_tblsurgery_paediatrics", "scr_tblsurgery_sarcoma", "scr_tblsurgery_upper_gi",
    "scr_tblsurgery_urology",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## The shared surgery column list

# COMMAND ----------

def _col(name, display, ig, meaning):
    """One column of a UNION roll-up's superset list: bronze name, Somerset display
    name, IG class and plain-English meaning. The expression is a placeholder — each
    branch supplies values through `exprs`, and anything an arm omits becomes NULL."""
    return ("CAST(NULL AS STRING)", name, display, ig, meaning)


SURGERY_COLUMNS = [
    _col("care_id", "CARE_ID", "id",
         "Somerset's identifier for one patient's care pathway for one cancer. A "
         "patient with two primaries has two CARE_IDs, so this is the cancer-episode "
         "key, not a patient key — join to scr_tblmain_referrals for the patient."),
    _col("surgery_id", "SURGERY_ID", "id",
         "Somerset's key for one operation record in tblMAIN_SURGERY. The join key "
         "that links an operation to its specialty-specific detail table."),
    _col("somerset_branch", "(added) source branch view", "ctl",
         "Which specialty branch view of Somerset's UNION supplied this row. "
         "Provenance added by this pipeline, not present in Somerset's own output; it "
         "is how you tell a NULL that means 'this specialty does not record that "
         "field' from a NULL that means 'not recorded for this patient'."),
    _col("cancer_site", "(added) Cancer Site", "lu",
         "The tumour-site pathway this operation sits on, taken from the referral "
         "record. Determines which specialty detail table and procedure lookup "
         "applied."),
    _col("operation_not_performed", "Operation Not Performed", "lu",
         "Whether the planned operation did not go ahead. NULL means it went ahead or "
         "nothing was recorded; the source only ever sets this to 'Yes'."),
    _col("reason_operation_not_performed", "Reason Operation Not Performed", "lu",
         "Reason the planned operation did not go ahead — a coded reason on most "
         "pathways, free text on the breast and head and neck pathways."),
    _col("asa_grade", "ASA Grade", "lu",
         "ASA physical status classification, 1 (healthy) to 5 (moribund). The "
         "anaesthetic risk measure, and the main case-mix adjuster for surgical "
         "outcomes."),
    _col("start_time", "Start Time", "dt",
         "Time the operation started, as hh:mm:ss."),
    _col("end_time_wound_closure", "End Time (Wound Closure)", "dt",
         "Time the operation finished at wound closure, as hh:mm:ss."),
    _col("procedure_comments", "Procedure Comments", "txt",
         "Free-text operative comments."),
    _col("indication_for_surgery", "Indication for Surgery", "txt",
         "Free-text clinical indication for operating."),
    _col("incision", "Incision", "txt",
         "Free-text description of the surgical incision used."),
    _col("findings", "Findings", "txt",
         "Free-text description of the operative findings."),
    _col("specimens", "Specimens", "txt",
         "Free-text description of specimens taken during the operation."),
    _col("drains", "Drains", "txt",
         "Free-text description of drains placed during the operation."),
    _col("closure", "Closure", "txt",
         "Free-text description of how the wound was closed."),
    _col("post_op_instructions", "Post-Op Instructions", "txt",
         "Free-text post-operative instructions."),
    _col("surgeon", "Surgeon", "clin",
         "Operating surgeon, as name then national code. In scr_pathology this is "
         "instead the clinician who requested the specimen."),
    _col("surgeon_grade", "Surgeon Grade", "clin",
         "Grade of the operating surgeon, for example consultant or registrar."),
    _col("assistant", "Assistant", "clin",
         "First assisting surgeon, as name then national code."),
    _col("assistant_grade", "Assistant Grade", "clin",
         "Grade of the first assisting surgeon."),
    _col("assistant_2", "Assistant 2", "clin",
         "Second assisting surgeon, as name then national code."),
    _col("assistant_2_grade", "Assistant 2 Grade", "clin",
         "Grade of the second assisting surgeon."),
    _col("main_procedure", "Main Procedure", "lu",
         "The principal operative procedure, decoded through the specialty's own "
         "procedure lookup. The lookups differ per specialty, so codes are not "
         "comparable across cancer sites — the decoded description is."),
    _col("sub_procedure_1", "Sub Procedure 1", "lu",
         "First additional procedure performed at the same operation."),
    _col("sub_procedure_2", "Sub Procedure 2", "lu",
         "Second additional procedure performed at the same operation."),
    _col("sub_procedure_3", "Sub Procedure 3", "lu",
         "Third additional procedure. Only the gynaecology pathway records a fourth "
         "procedure slot, so this is NULL everywhere else."),
    _col("main_procedure_snomed", "Primary Procedure (SNOMED)", "lu",
         "SNOMED CT description of the principal procedure."),
    _col("main_procedure_snomed_code", "Primary Procedure (SNOMED) code", "lu",
         "SNOMED CT concept code for the principal procedure."),
    _col("sub_procedure_1_snomed", "Sub Procedure 1 (SNOMED)", "lu",
         "SNOMED CT description of the first additional procedure."),
    _col("sub_procedure_1_snomed_code", "Sub Procedure 1 (SNOMED) code", "lu",
         "SNOMED CT concept code for the first additional procedure."),
    _col("sub_procedure_2_snomed", "Sub Procedure 2 (SNOMED)", "lu",
         "SNOMED CT description of the second additional procedure."),
    _col("sub_procedure_2_snomed_code", "Sub Procedure 2 (SNOMED) code", "lu",
         "SNOMED CT concept code for the second additional procedure."),
    _col("sub_procedure_3_snomed", "Sub Procedure 3 (SNOMED)", "lu",
         "SNOMED CT description of the third additional procedure."),
    _col("sub_procedure_3_snomed_code", "Sub Procedure 3 (SNOMED) code", "lu",
         "SNOMED CT concept code for the third additional procedure."),
    _col("primary_procedure_laterality", "Laterality", "lu",
         "Which breast the primary procedure was performed on."),
    _col("primary_procedure_lymph_node_procedure", "Lymph Node Procedure", "lu",
         "Lymph node procedure performed alongside the primary breast procedure, for "
         "example sentinel node biopsy or axillary clearance."),
    _col("primary_procedure_reconstruction", "Reconstruction", "lu",
         "Breast reconstruction performed alongside the primary procedure."),
    _col("primary_procedure_symmetrisation", "Symmetrisation", "lu",
         "Symmetrisation procedure on the opposite breast, performed to match the "
         "treated side."),
    _col("breast_procedure_laterality", "Laterality (Breast Procedure)", "lu",
         "Which breast the second breast procedure was performed on."),
    _col("breast_procedure_lymph_node_procedure", "Lymph Node Procedure (Breast Procedure)", "lu",
         "Lymph node procedure performed alongside the second breast procedure."),
    _col("breast_procedure_reconstruction", "Reconstruction (Breast Procedure)", "lu",
         "Breast reconstruction performed alongside the second breast procedure."),
    _col("breast_procedure_symmetrisation", "Symmetrisation (Breast Procedure)", "lu",
         "Symmetrisation procedure performed alongside the second breast procedure."),
    _col("laparoscopic_open", "Laparoscopic/Open", "lu",
         "Whether the colorectal operation was laparoscopic or open."),
    _col("laparoscopy", "Laparoscopy", "lu",
         "Whether a laparoscopic operation was converted to open, and related "
         "laparoscopy detail."),
    _col("hn_neck_procedure_code", "Neck", "lu",
         "Raw code for the neck dissection procedure performed. Undecoded: the "
         "ltblHN_PROC_NECK lookup is not yet landed, so the code is published rather "
         "than the column being dropped."),
    _col("hn_neck_laterality", "Neck Laterality", "lu",
         "Which side of the neck the neck dissection was performed on."),
    _col("hn_reconstruction_code", "Reconstruction", "lu",
         "Raw code for the first head and neck reconstruction. Undecoded: the "
         "ltblHNReconstruction lookup is not yet landed."),
    _col("hn_reconstruction_2_code", "Reconstruction 2", "lu",
         "Raw code for the second head and neck reconstruction. Undecoded for the "
         "same reason as the first."),
    _col("gynae_incision_code", "Incision", "lu",
         "Raw code for the gynaecological incision type. Undecoded: the "
         "ltblGYNAE_INCISION lookup is not yet landed."),
    _col("cns_excision_type_code", "Excision Type", "lu",
         "Raw code for the extent of CNS tumour excision. Undecoded: the "
         "ltblEXCISION_TYPE lookup is not yet landed."),
    _col("cns_tumour_location_code", "Tumour Location", "lu",
         "Raw code for the anatomical location of the brain lesion. Undecoded: the "
         "ltblBRAIN_LESION_LOCATION lookup is not yet landed."),
    _col("cns_procedure_comments", "Brain Procedure Comments", "txt",
         "Free-text operative comments specific to the CNS procedure."),
    _col("cns_intra_op_smear", "Intra-op Smear", "lu",
         "Whether an intra-operative smear was taken — rapid cytology used to guide "
         "the operation while it is still in progress."),
    _col("cns_smear_details", "Smear Details", "txt",
         "Free-text detail of the intra-operative smear result."),
    _col("cns_smear_reported_to", "Smear Reported To", "clin",
         "Clinician the intra-operative smear result was reported to, as name then "
         "code."),
    _col("cns_anti_cancer_implants", "Anti-Cancer Implants", "lu",
         "Whether anti-cancer implants (for example carmustine wafers) were placed in "
         "the resection cavity."),
    _col("cns_implant_details", "Implant Details", "txt",
         "Free-text detail of the anti-cancer implants placed."),
]

_SURGERY_TEXT_DEFAULTS = {
    "procedure_comments": "L_COMMENTS",
    "indication_for_surgery": "L_INDICATION",
    "incision": "L_INCISION",
    "findings": "L_FINDINGS",
    "specimens": "L_SPECIMENS",
    "drains": "L_DRAINS",
    "closure": "L_CLOSURE",
    "post_op_instructions": "L_POST_OP",
}

# The generic not-operated pattern: an L_NOT_OP flag plus a reason decoded through
# ltblCOLO_REASON (which despite the name is the shared reason lookup).
_NOT_OP_STD = ("CASE x.L_NOT_OP WHEN '1' THEN 'Yes' ELSE NULL END", "reason.REASON_DESC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The surgery branch builder
# MAGIC
# MAGIC Procedure columns mostly live on `tblMAIN_SURGERY` (alias `ms`) — including
# MAGIC `L_PROCEDURE`..`L_PROCEDURE4`, which despite reading like extension columns are on
# MAGIC the parent. Breast is the exception: `L_BREAST_PRO`/`L_BREAST_PRO2` are on
# MAGIC `tblSURGERY_BREAST`. A column given with an explicit alias is used as written;
# MAGIC anything bare is qualified `ms.`.

# COMMAND ----------

def surgery_branch(
    label,
    site,
    ext_table,
    procedures,
    not_op=_NOT_OP_STD,
    reason_join="x.L_NO_OP_REASON",
    text_cols=None,
    blanks=(),
    times="convert",
    snomed=3,
    assistant2_grade=True,
    extras=None,
    extra_joins="",
):
    """Build one arm of scr_all_surgery.

    label / site   the branch name and the L_CANCER_SITE value it corresponds to
    ext_table      the tblSURGERY_* extension table, aliased x
    procedures     (lookup_table, code_col, desc_col, [procedure columns in slot order])
    not_op         (operation_not_performed expr, reason expr), or None for neither
    text_cols      overrides onto _SURGERY_TEXT_DEFAULTS; value None means "not present"
    blanks         bronze names Somerset hard-codes to SPACE(30)
    times          "convert" (time-of-day cast), "raw" (as stored), or "blank"
    snomed         how many SNOMED procedure slots the branch carries (0 for none)
    """
    proc_table, proc_code, proc_desc, proc_cols = procedures
    exprs = {
        "care_id": "ms.CARE_ID",
        "surgery_id": "ms.SURGERY_ID",
        "cancer_site": "mr.L_CANCER_SITE",
        "asa_grade": "asa.ASA_DESC",
        "surgeon": "surgeon.CON_DESC",
        "surgeon_grade": "ms.L_GRADE",
        "assistant": "assistant1.CON_DESC",
        "assistant_grade": "ms.L_GRADE_1",
        "assistant_2": "assistant2.CON_DESC",
    }

    # Somerset omits Assistant 2 Grade for Paediatric only. L_GRADE_2 lives on
    # tblMAIN_SURGERY and is populated regardless of specialty, so fix mode emits it.
    if assistant2_grade or FIX_SOMERSET_DEFECTS:
        exprs["assistant_2_grade"] = "ms.L_GRADE_2"

    if not_op:
        exprs["operation_not_performed"] = not_op[0]
        exprs["reason_operation_not_performed"] = not_op[1]

    if times == "convert":
        exprs["start_time"] = time_of_day("x.L_START_TIME")
        exprs["end_time_wound_closure"] = time_of_day("x.L_END_TIME")
    elif times == "raw":
        exprs["start_time"] = "CAST(x.L_START_TIME AS STRING)"
        exprs["end_time_wound_closure"] = "CAST(x.L_END_TIME AS STRING)"
    else:
        exprs["start_time"] = blank()
        exprs["end_time_wound_closure"] = blank()

    text_map = dict(_SURGERY_TEXT_DEFAULTS)
    text_map.update(text_cols or {})
    for name, source in text_map.items():
        if source is not None:
            exprs[name] = f"x.{source}"
    for name in blanks:
        exprs[name] = blank()

    slots = ["main_procedure", "sub_procedure_1", "sub_procedure_2", "sub_procedure_3"]
    proc_joins = []
    for i, proc_col in enumerate(proc_cols):
        alias = f"proc{i + 1}"
        qualified = proc_col if "." in proc_col else f"ms.{proc_col}"
        exprs[slots[i]] = f"{alias}.{proc_desc}"
        proc_joins.append(
            f"LEFT JOIN {SRC}.{proc_table} {alias} ON {alias}.{proc_code} = {qualified}"
        )

    snomed_joins = []
    for i in range(snomed):
        alias = f"sn{i + 1}"
        suffix = "" if i == 0 else str(i + 1)
        exprs[f"{slots[i]}_snomed"] = f"{alias}.Description"
        exprs[f"{slots[i]}_snomed_code"] = f"CAST({alias}.SNOMEDCode AS STRING)"
        snomed_joins.append(
            f"LEFT JOIN {SRC}.scr_snomedctconcepts {alias} "
            f"ON ms.SNOMEDProcedure{suffix} = {alias}.ID"
        )

    exprs.update(extras or {})

    from_sql = "\n".join(
        [
            f"{SRC}.scr_tblmain_surgery ms",
            f"JOIN {SRC}.{ext_table} x ON x.SURGERY_ID = ms.SURGERY_ID",
            f"LEFT JOIN {SRC}.scr_tblmain_referrals mr ON mr.CARE_ID = ms.CARE_ID",
            f"LEFT JOIN {SRC}.scr_ltblasa asa ON asa.ASA_CODE = ms.ASAGrade",
            "LEFT JOIN consultants_by_code surgeon ON surgeon.NATIONAL_CODE = ms.L_SURGEON",
            "LEFT JOIN consultants_by_code assistant1 ON assistant1.NATIONAL_CODE = ms.L_ASSISTANT_1",
            "LEFT JOIN consultants_by_code assistant2 ON assistant2.NATIONAL_CODE = ms.L_ASSISTANT_2",
        ]
        + ([f"LEFT JOIN {SRC}.scr_ltblcolo_reason reason ON reason.REASON_CODE = {reason_join}"] if reason_join else [])
        + proc_joins
        + snomed_joins
        + ([extra_joins] if extra_joins else [])
    )
    return {"label": label, "site": site, "from_sql": from_sql, "exprs": exprs}


_GENERIC_PROCS = ("scr_ltblprocedures", "PROC_CODE", "PROC_DESC")
_MS_PROCS = ["N7_10_PROCEDURE_1", "N7_11_PROCEDURE_2", "N7_11_PROCEDURE_3"]
_SITE_PROCS = ["L_PROCEDURE", "L_PROCEDURE2", "L_PROCEDURE3"]

# COMMAND ----------

SURGERY_BRANCHES = [
    surgery_branch(
        "Brain", "Brain", "scr_tblsurgery_brain", _GENERIC_PROCS + (_MS_PROCS,),
    ),
    surgery_branch(
        "Breast", "Breast", "scr_tblsurgery_breast",
        ("scr_ltblbreast_procedure", "PROC_CODE", "PROC_DESC",
         ["x.L_BREAST_PRO", "x.L_BREAST_PRO2"]),
        not_op=(yes_no("x.L_CANCELLED"), "x.L_CANCEL_OP"),
        reason_join=None,
        times="raw",
        snomed=0,
        extras={
            "primary_procedure_laterality": "lat1.LAT_DESC",
            "primary_procedure_lymph_node_procedure": "lymph1.SAMPLE_DESC",
            "primary_procedure_reconstruction": "recon1.RECON_DESC",
            "primary_procedure_symmetrisation": "symm1.SYM_DESC",
            "breast_procedure_laterality": "lat2.LAT_DESC",
            "breast_procedure_lymph_node_procedure": "lymph2.SAMPLE_DESC",
            "breast_procedure_reconstruction": "recon2.RECON_DESC",
            "breast_procedure_symmetrisation": "symm2.SYM_DESC",
        },
        extra_joins=f"""LEFT JOIN {SRC}.scr_ltbllaterality lat1 ON lat1.LAT_CODE = x.L_LATERALITY
LEFT JOIN {SRC}.scr_ltbllaterality lat2 ON lat2.LAT_CODE = x.L_LATERALITY2
LEFT JOIN {SRC}.scr_ltblbreast_nodes lymph1 ON lymph1.SAMPLE_CODE = x.L_NODE_PRO
LEFT JOIN {SRC}.scr_ltblbreast_nodes lymph2 ON lymph2.SAMPLE_CODE = x.L_NODE_PRO2
LEFT JOIN {SRC}.scr_ltblbreast_reconstruction recon1 ON recon1.RECON_CODE = x.L_RECONSTRUCTION
LEFT JOIN {SRC}.scr_ltblbreast_reconstruction recon2 ON recon2.RECON_CODE = x.L_RECONSTRUCTION2
LEFT JOIN {SRC}.scr_ltblbreast_symmetrisation symm1 ON symm1.SYM_CODE = x.L_SYMMETRISATION
LEFT JOIN {SRC}.scr_ltblbreast_symmetrisation symm2 ON symm2.SYM_CODE = x.L_SYMMETRISATION2""",
    ),
    surgery_branch(
        "Colorectal", "Colorectal", "scr_tblsurgery_colorectal",
        ("scr_ltblcolo_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
        not_op=("CASE WHEN coalesce(x.R_NOT_OP, '') IN ('1', 'Yes') THEN 'Yes' ELSE NULL END",
                "reason.REASON_DESC"),
        reason_join="x.R_NO_OP_REASON",
        text_cols={"specimens": None, "drains": None, "closure": None, "post_op_instructions": None},
        blanks=("specimens", "drains", "closure", "post_op_instructions"),
        times="blank",
        extras={"laparoscopic_open": "x.L_LAPAROSCOPIC", "laparoscopy": "x.L_LAP_OPEN"},
    ),
    surgery_branch(
        "Gynaecology", "Gynaecology", "scr_tblsurgery_gynaecology",
        ("scr_ltblgynae_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS + ["L_PROCEDURE4"]),
        text_cols={"incision": None},
        snomed=4,
        # ltblGYNAE_INCISION is not landed, so the code is published raw rather than
        # decoded, and `incision` stays NULL rather than carrying an undecoded code.
        extras={"gynae_incision_code": "CAST(x.R_INCISION AS STRING)"},
    ),
    surgery_branch(
        "Haematology", "Haematology", "scr_tblsurgery_haematology",
        ("scr_ltblhaem_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
    ),
    surgery_branch(
        "Head and Neck", "Head and Neck", "scr_tblsurgery_head_neck",
        ("scr_ltblhnprocedure", "ID", "Description", _SITE_PROCS),
        not_op=(yes_no("x.L_CANCELLED"), "x.L_CANCEL_OP"),
        reason_join=None,
        extras={
            "hn_neck_procedure_code": "CAST(x.NECK_PROC AS STRING)",
            "hn_neck_laterality": "necklat.LAT_DESC",
            "hn_reconstruction_code": "CAST(x.L_RECONSTRUCTION AS STRING)",
            "hn_reconstruction_2_code": "CAST(x.RECONSTRUCTION2 AS STRING)",
        },
        extra_joins=f"LEFT JOIN {SRC}.scr_ltbllaterality necklat ON necklat.LAT_CODE = x.NECK_PROC_LATERALITY",
    ),
    surgery_branch(
        "Lung", "Lung", "scr_tblsurgery_lung",
        ("scr_ltbllung_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
        not_op=(yes_no("x.L_CANCELLED"), "x.L_CANCEL_OP"),
        reason_join=None,
        text_cols={"procedure_comments": None},
        blanks=("procedure_comments",),
    ),
    surgery_branch(
        "Paediatric", "Paediatric", "scr_tblsurgery_paediatrics",
        ("scr_ltblpaed_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
        assistant2_grade=False,
        extras={
            "cns_excision_type_code": "CAST(x.EXCISION_TYPE_SURGICAL AS STRING)",
            "cns_tumour_location_code": "CAST(x.TUMOUR_LOCATION_SURGICAL AS STRING)",
            "cns_procedure_comments": "x.BrainProcComments",
            "cns_intra_op_smear": "CASE x.IntraopSmear WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' END",
            "cns_smear_details": "x.SmearDetails",
            "cns_smear_reported_to": _consultant("smear"),
            "cns_anti_cancer_implants": "CASE x.AntiCancerImplants WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' END",
            "cns_implant_details": "x.ImpantDetails",
        },
        extra_joins="LEFT JOIN consultants_by_code smear ON smear.NATIONAL_CODE = x.SmearReportedBy",
    ),
    surgery_branch(
        # Added: Somerset has no Sarcoma arm at all. The extension table names its columns
        # differently and carries no findings/specimens/drains/closure, which is why so
        # much of this arm is NULL — that is the source, not a mapping gap.
        "Sarcoma", "Sarcoma", "scr_tblsurgery_sarcoma", _GENERIC_PROCS + (_MS_PROCS,),
        not_op=None,
        reason_join=None,
        text_cols={
            "procedure_comments": "OpNote",
            "indication_for_surgery": "IndicationSurgery",
            "incision": "Incision",
            "post_op_instructions": "PostOpInstructions",
            "findings": None, "specimens": None, "drains": None, "closure": None,
        },
    ),
    surgery_branch(
        "Skin", "Skin", "scr_tblsurgery_dermatology",
        ("scr_ltblskin_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
        text_cols={"specimens": None},
        blanks=("specimens",),
    ),
    surgery_branch(
        "Upper GI", "Upper GI", "scr_tblsurgery_upper_gi",
        ("scr_ltblugi_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
        not_op=("CASE x.R_DELAY WHEN '1' THEN 'Yes' ELSE NULL END",
                "CAST(x.R_DELAY_REASON AS STRING)"),
        reason_join=None,
    ),
    surgery_branch(
        "Urology", "Urology", "scr_tblsurgery_urology",
        ("scr_ltblurology_procedures", "PROC_ID", "PROC_DESC", _SITE_PROCS),
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complications branches

# COMMAND ----------

COMPLICATION_COLUMNS = [
    _col("care_id", "CARE_ID", "id",
         "Somerset's identifier for one patient's care pathway for one cancer. A "
         "patient with two primaries has two CARE_IDs, so this is the cancer-episode "
         "key, not a patient key — join to scr_tblmain_referrals for the patient."),
    _col("surgery_id", "SURGERY_ID", "id",
         "Somerset's key for one operation record in tblMAIN_SURGERY. The join key "
         "that links an operation to its specialty-specific detail table."),
    _col("somerset_branch", "(added) source branch view", "ctl",
         "Which specialty branch view of Somerset's UNION supplied this row. "
         "Provenance added by this pipeline, not present in Somerset's own output; it "
         "is how you tell a NULL that means 'this specialty does not record that "
         "field' from a NULL that means 'not recorded for this patient'."),
    _col("cancer_site", "Cancer Site", "lu",
         "The tumour-site pathway this care episode sits on — Breast, Colorectal, "
         "Skin, Gynaecology, Head and Neck, Urology, Upper GI, Lung, Haematology, "
         "CUP, Brain, Sarcoma, Paediatric or Other. Somerset's own pathway grouping, "
         "which drives which specialty forms and lookups apply; it is not a coded "
         "diagnosis site."),
    _col("complication_1", "Complication 1", "lu",
         "First recorded post-operative complication, decoded through the specialty's "
         "own complication lookup."),
    _col("complication_2", "Complication 2", "lu",
         "Second recorded post-operative complication."),
    _col("complication_3", "Complication 3", "lu",
         "Third recorded post-operative complication."),
]


def complication_branch(label, sites, lookup, code_col, desc_col):
    """One arm of scr_all_complications.

    Somerset INNER JOINs the first complication lookup, so a surgery with nothing
    recorded is absent rather than present-with-NULLs. That is kept: this table means
    "complications that were recorded", and scr_treatment_surgery LEFT JOINs it, so
    nothing is lost downstream.
    """
    site_list = ", ".join(q(s) for s in sites)
    return {
        "label": label,
        "site": sites[0],
        "from_sql": f"""{SRC}.scr_tblmain_surgery ms
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON mr.CARE_ID = ms.CARE_ID
JOIN {SRC}.{lookup} comp1 ON comp1.{code_col} = ms.L_COMPLICATIONS
LEFT JOIN {SRC}.{lookup} comp2 ON comp2.{code_col} = ms.L_COMPLICATIONS2
LEFT JOIN {SRC}.{lookup} comp3 ON comp3.{code_col} = ms.L_COMPLICATIONS3
WHERE mr.L_CANCER_SITE IN ({site_list})""",
        "exprs": {
            "care_id": "ms.CARE_ID",
            "surgery_id": "ms.SURGERY_ID",
            "cancer_site": "mr.L_CANCER_SITE",
            "complication_1": f"comp1.{desc_col}",
            "complication_2": f"comp2.{desc_col}",
            "complication_3": f"comp3.{desc_col}",
        },
    }


COMPLICATION_BRANCHES = [
    complication_branch("Upper GI", ["Upper GI"], "scr_ltblcomplications_upper_gi", "COMP_CODE", "COMP_DESC"),
    complication_branch("Breast", ["Breast"], "scr_ltblcomplications_breast", "COMP_CODE", "COMP_DESC"),
    complication_branch("Colorectal", ["Colorectal"], "scr_ltblcomplications_colorectal", "COMP_CODE", "COMP_DESC"),
    complication_branch("Gynaecology", ["Gynaecology"], "scr_ltblcomplications_gynae", "COMP_CODE", "COMP_DESC"),
    complication_branch("Head and Neck", ["Head and Neck"], "scr_ltblcomplications_head_neck", "COMP_CODE", "COMP_DESC"),
    complication_branch(
        "General", ["Haematology", "Lung", "Other", "Sarcoma", "Skin", "Urology"],
        "scr_ltblcomplications", "COMPLICATION_CODE", "COMPLICATION_DESC",
    ),
]

# Added arm: ltblCOMPLICATIONS_BRAIN is populated but Somerset publishes no Brain
# complications view, so brain surgical complications are missing from its roll-up.
if FIX_SOMERSET_DEFECTS:
    COMPLICATION_BRANCHES.append(
        complication_branch("Brain", ["Brain"], "scr_ltblcomplications_brain", "COMP_CODE", "COMP_DESC")
    )
else:
    # Faithful mode reproduces the duplicated Upper GI arm, doubling every Upper GI row.
    COMPLICATION_BRANCHES.append(
        complication_branch("Upper GI (duplicate arm)", ["Upper GI"],
                            "scr_ltblcomplications_upper_gi", "COMP_CODE", "COMP_DESC")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## The roll-up specs and the referral spine
# MAGIC
# MAGIC The seven specs that use the branch machinery above, plus the symptom-lookup CTEs for
# MAGIC the fourteen site referral views, the comorbidity block for `scr_presentation`, and the
# MAGIC assembly of `VIEW_SPECS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referral symptom CTEs
# MAGIC
# MAGIC The 14 site referral views differ only in which `ltblSYMPTOMS_*` table they decode
# MAGIC against, and `L_CANCER_SITE` partitions the spine exactly. Tagging each lookup with
# MAGIC its site and joining on `(SymptomID, site)` makes one pass equivalent to fourteen.

# COMMAND ----------

_SYMPTOM_SITES = [
    ("Brain", "scr_ltblsymptoms_brain"),
    ("Breast", "scr_ltblsymptoms_breast"),
    ("Colorectal", "scr_ltblsymptoms_colorectal"),
    ("Skin", "scr_ltblsymptoms_dermatology"),
    ("Gynaecology", "scr_ltblsymptoms_gynaecology"),
    ("Haematology", "scr_ltblsymptoms_haematology"),
    ("Head and Neck", "scr_ltblsymptoms_head_neck"),
    ("Lung", "scr_ltblsymptoms_lung"),
    ("Paediatric", "scr_ltblsymptoms_paediatric"),
    ("Sarcoma", "scr_ltblsymptoms_sarcoma"),
    ("Upper GI", "scr_ltblsymptoms_upper_gi"),
    ("Urology", "scr_ltblsymptoms_urology"),
]

CTES["referral_symptom_lookup"] = (
    [],
    "referral_symptom_lookup AS (\n    "
    + "\n    UNION ALL\n    ".join(
        f"SELECT {q(site)} AS site, SYM_ID, SYM_DESC FROM {SRC}.{tbl}"
        for site, tbl in _SYMPTOM_SITES
    )
    + "\n)",
)


def _symptom_cte(name, link_table):
    """Group-concatenated symptoms per referral.

    Somerset does this with STUFF(... FOR XML PATH ...), which produces an
    insertion-ordered, comma-joined string. array_sort(collect_set(...)) is used instead:
    deterministic across runs, and de-duplicated, which the XML form is not.
    """
    return (
        ["referral_symptom_lookup"],
        f"""{name} AS (
    SELECT l.CareID AS CARE_ID,
           array_join(array_sort(collect_set(s.SYM_DESC)), '; ') AS symptoms
    FROM {SRC}.{link_table} l
    JOIN {SRC}.scr_tblmain_referrals r2 ON r2.CARE_ID = l.CareID
    JOIN referral_symptom_lookup s ON s.SYM_ID = l.SymptomID AND s.site = r2.L_CANCER_SITE
    GROUP BY l.CareID
)""",
    )


CTES["nice_ref_symptoms"] = _symptom_cte("nice_ref_symptoms", "scr_lnknicereferringsymptoms")
CTES["other_ref_symptoms"] = _symptom_cte("other_ref_symptoms", "scr_lnkreferralsymptoms")


# tblDEFINITIVE_TREATMENT holds more than one TREAT_NO = 1 row for a handful of
# CARE_IDs, so joining it raw fans the referral spine out. Ordering leads with
# has-data tests because the populated row is not reliably the later TREATMENT_ID,
# and NULLS LAST is explicit because the Spark ASC default puts NULLs first.
CTES["definitive_treatment_first"] = (
    [],
    f"""definitive_treatment_first AS (
    SELECT * EXCEPT (rn) FROM (
        SELECT dt.*,
               ROW_NUMBER() OVER (
                   PARTITION BY dt.CARE_ID
                   ORDER BY CASE WHEN dt.START_DATE IS NULL THEN 1 ELSE 0 END ASC,
                            CASE WHEN dt.TREATMENT IS NULL THEN 1 ELSE 0 END ASC,
                            CASE WHEN dt.DECISION_DATE IS NULL THEN 1 ELSE 0 END ASC,
                            dt.TREATMENT_ID DESC NULLS LAST
               ) AS rn
        FROM {SRC}.scr_tbldefinitive_treatment dt
        WHERE dt.TREAT_NO = 1
    ) WHERE rn = 1
)""",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared fragments for the wide views

# COMMAND ----------

_VALIDATED = (
    "CASE WHEN dt.VALIDATED = 1 THEN 'Yes' WHEN dt.VALIDATED IS NULL THEN NULL ELSE 'No' END"
    if FIX_SOMERSET_DEFECTS
    else "CASE WHEN dt.VALIDATED = 1 THEN 'Yes' ELSE 'No' END"
)


def _bit_flag(col):
    """Decode a Somerset checkbox column.

    These land as BOOLEAN NOT NULL — a SQL Server BIT DEFAULT 0 — so every row is true or
    false and none is NULL (verified across all 4,365 tblINITIAL_ASSESSMENT rows). There
    is no 'unrecorded' state to protect here: false is what the source stores, and what it
    means — 'the box was not ticked' — is a caveat for the column comment rather than a
    reason to publish NULL and drop the rows out of every IS NOT NULL filter. The ELSE arm
    fires only if the source ever starts admitting NULL.

    Somerset writes these as `CASE col WHEN 1 ...`, comparing a BOOLEAN to an INT, which
    Spark rejects outright under ANSI. That is what broke the first build of this table.
    """
    return f"CASE WHEN {col} THEN 'Yes' WHEN NOT {col} THEN 'No' ELSE NULL END"


def _symptom_part(unit, part_col):
    """Symptom-onset year/month/day: the recorded part, else derived from the date.

    Both arms are INT. The recorded parts land as zero-padded STRING ('01'..'31', with ''
    for not-recorded), and coalescing STRING against year()/month()/day() has no common
    type under ANSI — the second thing that broke the first build. try_cast turns both ''
    and any future unparseable value into NULL, so the fallback takes over.
    """
    return f"coalesce(try_cast(ia.{part_col} AS INT), {unit}(ia.N_HN11_SYMPTOM_DATE))"


# The 32 comorbidity checkboxes: bronze column, source column, Somerset display
# name, and the phrase for what the clinician ticked. The shared template below
# turns that last field into each column's published meaning.
_COMORBIDITY_FLAGS = [
    ("cm_acute_pancreatitis", "L_CM_ACUTE", "Acute Pancreatitis", "acute pancreatitis"),
    ("cm_alcohol_induced_pancreatitis", "L_CM_ALCOHOL", "Alcohol Induced Pancreatitis", "alcohol-induced pancreatitis"),
    ("cm_anaemia", "L_CM_ANAEMIA", "Anaemia", "anaemia"),
    ("cm_angina", "L_CM_ANGINA", "Angina", "angina"),
    ("cm_on_asprin", "L_CM_ASPRIN", "On Asprin", "current aspirin therapy"),
    ("cm_inflammatory_bowel_disease", "L_CM_BOWEL", "Inflammatory Bowel Disease", "inflammatory bowel disease"),
    ("cm_cardiovascular_disease", "L_CM_CARDIAC", "Cardiovascular Disease", "cardiovascular disease"),
    ("cm_cerebral_vascular_disease", "CerebrovascularDisease", "Cerebral Vascular Disease", "cerebrovascular disease"),
    ("cm_chronic_pancreatitis", "L_CM_CHRONIC", "Chronic Pancreatitis", "chronic pancreatitis"),
    ("cm_cirrhosis_liver_disease", "L_CM_CIRRHOSIS", "Cirrhosis Liver Disease", "cirrhosis or other liver disease"),
    ("cm_blood_clotting_disorder", "L_CM_CLOTTING", "Blood Clotting Disorder", "a blood clotting disorder"),
    ("cm_copd_chronic_respiratory", "L_CM_COAD", "COPD/Chronic Respiratory", "COPD or another chronic respiratory illness"),
    ("cm_history_of_cva_stroke", "L_CM_CVA", "History of CVA/Stroke", "a history of stroke"),
    ("cm_type_i_diabetes", "L_CM_DIABETES1", "Type I Diabetes", "type I diabetes"),
    ("cm_type_ii_diabetes", "L_CM_DIABETES2", "Type II Diabetes", "type II diabetes"),
    ("cm_history_of_cardiac_failure", "L_CM_FAILURE", "History of Cardiac Failure", "a history of cardiac failure"),
    ("cm_ischaemic_heart_disease", "L_CM_HEART", "Ischaemic Heart Disease", "ischaemic heart disease"),
    ("cm_hypertension", "L_CM_HYPERTENSION", "Hypertension", "hypertension"),
    ("cm_liver_disease", "L_CM_LIVER", "Liver Disease", "liver disease"),
    ("cm_mental_illness", "L_CM_MENTAL", "Mental Illness", "mental illness"),
    ("cm_neurological_disease", "L_CM_NEURO", "Neurological Disease", "neurological disease"),
    ("cm_other", "L_CM_OTHER", "Other", "a comorbidity not covered by the other flags"),
    ("cm_peripheral_vascular_disease", "L_CM_PERIPHERAL", "Peripheral Vascular Disease", "peripheral vascular disease"),
    ("cm_renal_disease", "L_CM_RENAL", "Renal Disease", "renal disease"),
    ("cm_respiratory_disease", "L_CM_RESPIRATORY", "Respiratory Disease", "respiratory disease"),
    ("cm_creatinine_over_170", "L_CM_SERUM", "Creatinine > 170", "a serum creatinine above 170 mmol/l"),
    ("cm_on_systemic_steroids", "L_CM_STEROIDS", "On Systemic Steroids", "current systemic steroid therapy"),
    ("cm_other_vascular_disease", "L_CM_VASCULAR", "Other Vascular Disease", "other vascular disease"),
    ("cm_on_warfarin", "L_CM_WARFARIN", "On Warfarin", "current warfarin therapy"),
    ("cm_on_dipyridamole", "L_CM_DIP", "On Dipyridamole", "current dipyridamole therapy"),
    ("cm_on_clopidogrel", "L_CM_CLOP", "On Clopidogrel", "current clopidogrel therapy"),
    ("cm_pacemaker", "L_CM_PACE", "Pacemaker", "a pacemaker"),
]

_CM_MEANING = (
    "Comorbidity flag: 'Yes' where the assessing clinician ticked {what}. The source is a "
    "BIT DEFAULT 0 that is never NULL, so 'No' means only that the box was left unticked "
    "— an assessed negative and an unassessed patient are indistinguishable in it. The "
    "'Yes' cohort is usable; its complement is not a control group."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation corrected against the landed columns
# MAGIC
# MAGIC The Somerset documentation implies an unticked comorbidity box is simply unrecorded, so
# MAGIC a column built from it would publish NULL. The landed columns disprove that: all 32 are
# MAGIC `BOOLEAN NOT NULL` with no NULLs in any of the 4,365 rows. The corrected wording lives in
# MAGIC the meaning template above, and in the comments on `haematuria`, `raised_psa` and the
# MAGIC three symptom-onset columns, next to the decode and the measurement that justify it.

# COMMAND ----------

_IA_ROWS = "4,365"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The roll-up specs

# COMMAND ----------

ROLLUP_SPECS = [
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwAllComplications",
        "target": "scr_all_complications",
        "ctes": [],
        "comment": (
            "Recorded surgical complications across cancer sites, rebuilt from base "
            "tables in {src} to the scr_22_02 BIvwAllComplications definition. One row "
            "per surgery that has at least one complication recorded — surgeries with "
            "none are absent, which is what the source means, not a load gap. TWO FIXES: "
            "Somerset UNIONs the Upper GI arm twice (doubling every Upper GI row, and "
            "doubling them again in BIvwTreatmentSurgery), and omits Brain despite "
            "ltblCOMPLICATIONS_BRAIN being populated. Under fix_somerset_defects the "
            "duplicate is dropped and Brain is added. CUP and Paediatric remain "
            "uncovered: neither has a site lookup. somerset_branch records the arm."
        ),
        "columns": COMPLICATION_COLUMNS,
        "branches": COMPLICATION_BRANCHES,
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwAllSurgery",
        "target": "scr_all_surgery",
        "ctes": ["consultants_by_code"],
        "comment": (
            "Cancer surgery harmonised across every specialty, rebuilt from base tables "
            "in {src} to the scr_22_02 BIvwAllSurgery definition. One row per surgery "
            "record that has a specialty extension row. This is the reconciliation "
            "product: twelve differently-shaped tblSURGERY_* tables mapped onto one "
            "column list. Somerset's own roll-up projects 25 columns; this one carries "
            "the SNOMED procedure codes and the branch-specific blocks (breast "
            "laterality/reconstruction, colorectal approach, head-and-neck neck "
            "dissection, CNS operative smear) rather than discarding them, and adds a "
            "Sarcoma arm Somerset has never had. NULL in a branch-specific column means "
            "the row came from a different specialty — read somerset_branch first. Five "
            "*_code columns are published undecoded because their lookup tables are not "
            "yet landed; see KNOWN_MISSING_LOOKUPS in the run payload."
        ),
        "columns": SURGERY_COLUMNS,
        "branches": SURGERY_BRANCHES,
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwBreastTumourMarkers",
        "target": "scr_breast_tumour_markers",
        "ctes": [],
        "comment": (
            "Breast tumour immunohistochemistry markers (ER, PR, HER2, Bcl-2, pS2, "
            "C-erb B2) with their scores, rebuilt from base tables in {src} to the "
            "scr_22_02 BIvwBreastTumourMarkers definition. One row per "
            "tblPATHOLOGY_BREAST record (811 at landing). Every join in the Somerset "
            "definition is an INNER join, so a marker panel whose pathology report or "
            "referral is missing is dropped — reproduced, since a panel with no patient "
            "is not usable."
        ),
        "columns": [(
                        "mr.CARE_ID",
                        "care_id", "CARE_ID", "id",
                        "Somerset's identifier for one patient's care pathway for one "
                        "cancer. A patient with two primaries has two CARE_IDs, so "
                        "this is the cancer-episode key, not a patient key — join to "
                        "scr_tblmain_referrals for the patient.",
                    )]
        + _DEMOG_COLS
        + [
            (
                "p.N8_20_REPORT_NUMBER",
                "report_number", "Report Number", "id",
                "The laboratory's report number for the pathology report these tumour "
                "markers were read from, as transcribed into the registry.",
            ),
            (
                "pb.L_TM_DATE",
                "tumour_marker_date", "Tumour Marker Date", "dt",
                "Date the breast tumour-marker assays were reported.",
            ),
            (
                "pb.L_pS2",
                "ps2", "pS2", "meas",
                "pS2 (trefoil factor 1) result — a legacy oestrogen-regulated breast "
                "marker.",
            ),
            (
                "pb.L_pS2_SCORE",
                "ps2_score", "pS2 Score", "meas",
                "Numeric score behind the pS2 result.",
            ),
            (
                "pb.L_ER",
                "er", "ER", "meas",
                "Oestrogen receptor status. Positive disease is eligible for "
                "endocrine therapy, so this is one of the two markers that determine "
                "breast treatment.",
            ),
            (
                "pb.L_ER_SCORE",
                "er_score", "ER Score", "meas",
                "Numeric oestrogen receptor score, typically an Allred or quick score "
                "out of 8.",
            ),
            (
                "pb.L_PR",
                "pr", "PR", "meas",
                "Progesterone receptor status.",
            ),
            (
                "pb.L_PR_SCORE",
                "pr_score", "PR Score", "meas",
                "Numeric progesterone receptor score.",
            ),
            (
                "pb.L_CERBB2",
                "cerb_b2", "C-erb B2", "meas",
                "C-erbB-2 result — the historical name for HER2, reported by "
                "immunohistochemistry.",
            ),
            (
                "pb.L_CERBB2_SCORE",
                "cerb_b2_score", "C-erb B2 Score", "meas",
                "Numeric C-erbB-2 score, conventionally 0 to 3+.",
            ),
            (
                "pb.L_BCL2",
                "bcl2", "Bcl-2", "meas",
                "Bcl-2 result — an apoptosis-regulator marker.",
            ),
            (
                "pb.L_BCL2_SCORE",
                "bcl2_score", "Bcl-2 Score", "meas",
                "Numeric Bcl-2 score.",
            ),
            (
                "pb.L_HER2",
                "her2", "HER2", "meas",
                "HER2 status. Positive disease is eligible for anti-HER2 therapy, so "
                "with ER this determines the treatment pathway.",
            ),
            (
                "pb.L_FISH",
                "her2_fish", "HER2 - FISH", "meas",
                "HER2 result by fluorescence in-situ hybridisation, used to resolve "
                "an equivocal immunohistochemistry score.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblpathology_breast pb
JOIN {SRC}.scr_tblmain_pathology p ON p.PATHOLOGY_ID = pb.PATHOLOGY_ID
JOIN {SRC}.scr_tblmain_referrals mr ON mr.CARE_ID = p.CARE_ID
JOIN {SRC}.scr_tbldemographics d ON d.PATIENT_ID = mr.PATIENT_ID""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwBreastDiagnosisDetail",
        "target": "scr_breast_diagnosis_detail",
        "ctes": [],
        "comment": (
            "Breast tumour size, lymph node score and grade, and the derived Nottingham "
            "Prognostic Index, rebuilt from base tables in {src} to the scr_22_02 "
            "BIvwBreastDiagnosisDetail definition. One row per tblREFERRAL_BREAST record "
            "— that is every breast referral, so most rows carry no measurements. NPI is "
            "computed by Somerset as (size_mm * 0.02) + lymph score code + grade code; it "
            "is recomputed here rather than trusted, and is NULL unless all three inputs "
            "are present and numeric."
        ),
        "columns": [
            (
                "rb.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "rb.L_SIZE",
                "tumour_size_mm", "Tumour Size (mm)", "meas",
                "Breast tumour size in millimetres, as recorded at referral.",
            ),
            (
                "sc.SCORE_DESC",
                "lymph_score", "Lymph Score", "lu",
                "Lymph node score component of the Nottingham Prognostic Index.",
            ),
            (
                "gr.GRADE_DESC",
                "lymph_grade", "Lymph Grade", "lu",
                "Lymph node grade component of the Nottingham Prognostic Index.",
            ),
            (
                "round((try_cast(rb.L_SIZE AS DOUBLE) * 0.02) "
                "+ try_cast(sc.SCORE_CODE AS DOUBLE) "
                "+ try_cast(gr.GRADE_CODE AS DOUBLE), 2)",
                "npi_score", "NPI Score", "meas",
                "Nottingham Prognostic Index, computed by the source view as (size in "
                "mm x 0.02) plus the node score plus the grade. Standard breast "
                "prognostic score; NULL if any component is missing.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblreferral_breast rb
LEFT JOIN {SRC}.scr_ltblbreast_p_grade gr ON gr.GRADE_CODE = rb.L_TUMOUR_GRADE
LEFT JOIN {SRC}.scr_ltblbreast_p_score sc ON sc.SCORE_CODE = rb.L_LYMPH_SCORE""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwPresentation",
        "target": "scr_presentation",
        "ctes": [],
        "comment": (
            "Initial clinical assessment at presentation: performance status, the 32 "
            "comorbidity flags, smoking and alcohol status, height/weight/BMI, frailty, "
            "family history and site-specific findings. Rebuilt from base tables in {src} "
            "to the scr_22_02 BIvwPresentation definition, one row per "
            "tblINITIAL_ASSESSMENT record (4,365 at landing — small relative to 517,281 "
            "referrals, so treat presence as a selection effect rather than a sample). "
            "READ THE COMORBIDITY COLUMN COMMENTS BEFORE USING THEM: those 32 flags are "
            "BIT DEFAULT 0 columns that are never NULL, so 'No' means the box was not "
            "ticked and conflates an assessed negative with an unassessed patient — the "
            "'Yes' cohort is trustworthy, its complement is not a control group. The two "
            "urology screening flags share that shape. Symptom-onset year/month/day are "
            "recorded on 242/242/236 rows and their documented fallback is inert here, "
            "N_HN11_SYMPTOM_DATE being NULL on every row. The left/right breast finding "
            "columns carry an unresolved labelling question — see their comments."
        ),
        "columns": [
            (
                "ia.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "ia.L_ASSESS_DATE",
                "date_of_assessment", "Date of Assessment", "dt",
                "Date of the assessment this record describes.",
            ),
            (
                "hcp.HCP_DESC",
                "assessment_by", "Assessment By", "clin",
                "Type of healthcare professional who carried out the initial "
                "assessment (for example consultant, clinical nurse specialist) — a "
                "role, not a named person.",
            ),
            (
                cat("cast(who.WHO_CODE AS string)", "who.WHO_DESC", sep=" - "),
                "performance_status", "Performance Status", "lu",
                "WHO/ECOG performance status recorded at initial assessment, as code "
                "then description — 0 fully active through 4 completely disabled.",
            ),
            (
                "CASE fh.L_FAMILY_TAKEN WHEN 0 THEN 'No' WHEN 1 THEN 'Yes' "
                "WHEN 2 THEN 'Not Taken' ELSE NULL END",
                "family_history_taken", "Family History Taken", "lu",
                "Whether a family history was taken: Yes, No, or explicitly Not "
                "Taken.",
            ),
        ]
        + [
            (
                _bit_flag(f"ia.{_src}"),
                _name, _display, "clin",
                _CM_MEANING.format(what=_what),
            )
            for _name, _src, _display, _what in _COMORBIDITY_FLAGS
        ]
        + [
            (
                "ia.L_OTHER_MORBIDITIES",
                "other_morbidities", "Other Morbidities", "txt",
                "Free text for comorbidities not covered by the coded flags.",
            ),
            (
                "tob.SMOKING_DESC",
                "smoking_status", "Smoking Status", "clin",
                "Recorded smoking status at assessment.",
            ),
            (
                "alc.ALCOHOL_DESC",
                "alcohol_status", "Alcohol Status", "clin",
                "Recorded current alcohol consumption at assessment.",
            ),
            (
                "alcpast.ALCOHOL_DESC",
                "alcohol_status_past", "Alcohol Status (Past)", "clin",
                "Recorded past alcohol consumption, where different from current.",
            ),
            (
                "ia.L_OTHER_INFO",
                "social_history", "Social History", "txt",
                "Free-text social history recorded at assessment.",
            ),
            (
                "ia.L_MEDICATION",
                "medication", "Medication", "txt",
                "Free-text list of the patient's medication at assessment.",
            ),
            (
                "ia.L_ALLERGIES",
                "allergies", "Allergies", "txt",
                "Free-text record of the patient's allergies.",
            ),
            (
                "ia.L_OTHER_PMH",
                "other_pmh", "Other PMH", "txt",
                "Free-text past medical history not captured by the coded fields.",
            ),
            (
                "ia.L_HEIGHT",
                "height", "Height", "meas",
                "Height as recorded at assessment. Units are as typed; usually "
                "metres.",
            ),
            (
                "ia.L_WEIGHT",
                "weight", "Weight", "meas",
                "Weight as recorded at assessment. Units are as typed; usually "
                "kilograms.",
            ),
            (
                "CASE WHEN coalesce(ia.L_WEIGHT, 0) <> 0 AND coalesce(ia.L_HEIGHT, 0) <> 0 "
                "THEN round(ia.L_WEIGHT / (ia.L_HEIGHT * ia.L_HEIGHT), 1) ELSE NULL END",
                "bmi", "BMI", "meas",
                "Body mass index computed by the source view as weight / height "
                "squared, rounded to one decimal place, and only where both are "
                "non-zero. Inherits whatever units were typed into height and weight, "
                "so treat implausible values as unit errors.",
            ),
            (
                cat("cast(cfs.ID AS string)", "cfs.Description", sep=" - "),
                "clinical_frailty_scale", "Clinical Frailty Scale", "clin",
                "Rockwood Clinical Frailty Scale, rendered as score then description "
                "— 1 very fit through 9 terminally ill.",
            ),
            (
                "fh.L_FAMILY_DATE",
                "family_history_date", "Family History Date", "dt",
                "Date the family history was recorded.",
            ),
            (
                "rel.RELATION_DESC",
                "family_relationship", "Relationship", "lu",
                "Relationship to the patient of the affected family member.",
            ),
            (
                cat("dg1.DIAG_CODE", "dg1.DIAG_DESC", sep=" - "),
                "family_diagnosis", "Diagnosis", "clin",
                "Diagnosis of the affected family member, as ICD code then "
                "description.",
            ),
            (
                cat("dg2.DIAG_CODE", "dg2.DIAG_DESC", sep=" - "),
                "family_diagnosis_2", "Diagnosis 2", "clin",
                "Second recorded diagnosis of the affected family member, as ICD code "
                "then description.",
            ),
            (
                _symptom_part("year", "N_HN11_SYMPTOM_YEAR"),
                "symptom_year", "Symptom Year", "dt",
                "Year the patient's symptoms began, as an integer. Taken from the "
                "explicitly recorded part, populated on 242 of 4,365 assessments; the "
                "documented fallback to N_HN11_SYMPTOM_DATE is inert at Barts, where "
                "that column is NULL on every row.",
            ),
            (
                _symptom_part("month", "N_HN11_SYMPTOM_MONTH"),
                "symptom_month", "Symptom Month", "dt",
                "Month the patient's symptoms began, as an integer. Taken from the "
                "explicitly recorded part, populated on 242 of 4,365 assessments; the "
                "documented fallback to N_HN11_SYMPTOM_DATE is inert at Barts, where "
                "that column is NULL on every row.",
            ),
            (
                _symptom_part("day", "N_HN11_SYMPTOM_DAY"),
                "symptom_day", "Symptom Day", "dt",
                "Day of month the patient's symptoms began, as an integer. Taken from "
                "the explicitly recorded part, populated on 236 of 4,365 assessments; "
                "the documented fallback to N_HN11_SYMPTOM_DATE is inert at Barts, "
                "where that column is NULL on every row.",
            ),
            (
                "ia.L_PRE_TREAT_DENTAL_DATE",
                "pre_treatment_dental_date", "Pre-Treatment Dental Date", "dt",
                "Date of the pre-treatment dental assessment, relevant to head and "
                "neck radiotherapy planning.",
            ),
            (
                "org.Description",
                "organisation_site", "Organisation", "lu",
                "Name of the NHS organisation site where the assessment took place.",
            ),
            (
                "bpleft.DESCRIPTION",
                "clinical_assessment_result_left_breast", "Clinical Assessment Result (Left breast)", "clin",
                "Breast clinical assessment P-score, labelled by Somerset as the left "
                "breast. CAUTION: Somerset resolves this from the column prefixed R_ "
                "and its right-breast counterpart from the column prefixed L_. "
                "Elsewhere in this schema L_ and R_ are namespace prefixes, not "
                "laterality, so the labelling is unverified. Confirm against a known "
                "case before using either column for laterality.",
            ),
            (
                "bpright.DESCRIPTION",
                "clinical_assessment_result_right_breast", "Clinical Assessment Result (Right breast)", "clin",
                "Breast clinical assessment P-score, labelled by Somerset as the "
                "right breast. See the caution on the left-breast column: the two may "
                "be transposed and this has not been resolved against a known case.",
            ),
            (
                "fh.L_FAMILY_COMMENTS",
                "family_history_comments", "Family History Comments", "txt",
                "Free-text notes on the family history.",
            ),
            (
                _bit_flag("ia.UroHaematuria"),
                "haematuria", "Haematuria", "clin",
                "Whether haematuria was recorded at urology presentation. Same BIT "
                "DEFAULT 0 shape as the comorbidity flags — 'No' means unticked, not "
                "tested-negative — and 'Yes' on 1 of 4,365 assessments.",
            ),
            (
                _bit_flag("ia.UroRaisedPSA"),
                "raised_psa", "Raised PSA", "clin",
                "Whether a raised PSA was recorded at urology presentation. Same BIT "
                "DEFAULT 0 shape as the comorbidity flags — 'No' means unticked, not "
                "tested-negative — and 'Yes' on 93 of 4,365 assessments.",
            ),
            (
                "men.MENSTRAL_DESC",
                "menstrual_status", "Menstrual Status", "clin",
                "Menstrual status recorded at breast presentation.",
            ),
            (
                "ia.L_AGE_MENO",
                "age_at_menopause", "Age at Menopause", "meas",
                "Age at menopause, where recorded.",
            ),
            (
                "CAST(ia.L_ON_HRT AS STRING)",
                "on_hrt", "HRT", "clin",
                "Whether the patient was taking hormone replacement therapy.",
            ),
            (
                "ia.L_HRT",
                "hrt_length_of_usage_years", "HRT Length of Usage (Years)", "meas",
                "Years of hormone replacement therapy use, where recorded.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblinitial_assessment ia
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON mr.CARE_ID = ia.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON d.PATIENT_ID = mr.PATIENT_ID
LEFT JOIN {SRC}.scr_tblfamily_history fh ON fh.CARE_ID = ia.CARE_ID
LEFT JOIN {SRC}.scr_ltblhcp hcp ON hcp.HCP_CODE = ia.L_AS_BY
LEFT JOIN {SRC}.scr_ltblwho who ON who.WHO_CODE = ia.R_PERFORMANCE
LEFT JOIN {SRC}.scr_ltbltobacco tob ON tob.SMOKING_CODE = ia.N_HN5_SMOKER
LEFT JOIN {SRC}.scr_ltblalcohol alc ON alc.ALCOHOL_CODE = ia.N_HN9_ALCOHOL
LEFT JOIN {SRC}.scr_ltblalcohol alcpast ON alcpast.ALCOHOL_CODE = ia.N_HN9_ALCOHOL_PAST
LEFT JOIN {SRC}.scr_clinicalfrailtyscale cfs ON cfs.ID = ia.ClinicalFrailtyScale
LEFT JOIN {SRC}.scr_ltblrelationship rel ON rel.RELATION_ID = fh.L_FAMILY_RELATION
LEFT JOIN {SRC}.scr_ltbldiagnosis dg1 ON dg1.DIAG_CODE = fh.L_FAMILY_DIAGNOSIS
LEFT JOIN {SRC}.scr_ltbldiagnosis dg2 ON dg2.DIAG_CODE = fh.L_FAMILY_DIAGNOSIS_2
LEFT JOIN {SRC}.scr_organisationsites org ON org.Code = ia.ASSESS_SITE_CODE
LEFT JOIN {SRC}.scr_ltblbreast_pcodes bpleft ON bpleft.CODES_DESC = ia.R_FINDINGS
LEFT JOIN {SRC}.scr_ltblbreast_pcodes bpright ON bpright.CODES_DESC = ia.L_FINDINGS
LEFT JOIN {SRC}.scr_ltblmenstrual men ON men.MENSTRAL_CODE = ia.N_B4_MENSTRUAL_STATUS""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwBreastReferrals (and its 13 site siblings)",
        "target": "scr_referrals",
        "ctes": [
                "consultants_by_code",
                "definitive_treatment_first",
                "nice_ref_symptoms",
                "other_ref_symptoms",
            ],
        "comment": (
            "Cancer referrals: source, priority, route, referring GP/practice/CCG, first "
            "appointment, specialist review, referring symptoms and waiting-time "
            "adjustments. Rebuilt from base tables in {src} to the scr_22_02 "
            "BIvw*Referrals definitions — 14 near-identical site views collapsed into one "
            "pass, since L_CANCER_SITE partitions the spine exactly (verified: 14 sites, "
            "no NULLs, 517,281 rows). One row per tblMAIN_REFERRALS record, so this is "
            "the referral spine of the whole register. Symptom columns are NULL for CUP "
            "and Other, which declare no symptom lookup. Referring branch code is "
            "near-empty at Barts (the Branch reference table holds a single row) — a "
            "source population gap, not a join defect."
        ),
        "columns": [
            (
                "r.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "r.PATIENT_ID",
                "patient_id", "PATIENT_ID", "id",
                "Somerset's internal patient key, stable across all care pathways for "
                "that person. Local to the registry: it is not the Cerner person_id "
                "and not an MRN.",
            ),
            (
                "r.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
            (
                "crs.Description",
                "subtype", "Subtype", "lu",
                "Cancer sub-site the referral was made against, within the cancer "
                "site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                "opref.REF_DESC",
                "source_of_referral", "Source of Referral", "lu",
                "Where the referral came from — GP, screening service, other "
                "consultant, A&E and so on. Drives cancer waiting-time reporting.",
            ),
            (
                "pri.PRIORITY_DESC",
                "priority_type", "Priority Type", "lu",
                "Referral priority: routine, urgent, or two-week-wait suspected "
                "cancer.",
            ),
            (
                "rr.Description",
                "referral_route", "Referral Route", "lu",
                "Route by which the referral reached the service.",
            ),
            (
                "r.ReferralOtherRoute",
                "other_route", "Other Route", "txt",
                "Free text describing the referral route when the coded route is "
                "'other'.",
            ),
            (
                "CASE r.RapidDiagnostic WHEN '01' THEN 'Yes' WHEN '02' THEN 'No' ELSE NULL END",
                "rapid_diagnostic_service", "Rapid Diagnostic Service", "lu",
                "Whether the referral came through a Rapid Diagnostic Service "
                "pathway.",
            ),
            (
                "r.N_UPGRADE_DATE",
                "upgrade_date", "Upgrade Date", "dt",
                "Date a non-urgent referral was upgraded to a suspected-cancer "
                "pathway by a clinician. Upgrading restarts the waiting-time clock.",
            ),
            (
                "ct.CANCER_TYPE_DESC",
                "referral_type", "Referral Type", "lu",
                "Registry classification of what kind of cancer referral this is.",
            ),
            (
                "gp.Code",
                "referring_gp_code", "Referring GP Code", "clin",
                "National code of the referring GP.",
            ),
            (
                "gp.Name",
                "referring_gp_name", "Referring GP Name", "clin",
                "Name of the referring GP.",
            ),
            (
                "prac.Code",
                "referring_practice_code", "Referring Practice Code", "clin",
                "National code of the referring GP practice.",
            ),
            (
                "brn.Code",
                "referring_branch_code", "Referring Branch Code", "clin",
                "National code of the referring practice branch surgery. WARNING: "
                "Barts populates one row in the whole Branch reference table, so this "
                "is very nearly always NULL — an artefact of local recording, not of "
                "the join.",
            ),
            (
                "ccgi.Name",
                "ccg", "CCG", "lu",
                "Name of the commissioning organisation (CCG/ICB) for the referring "
                "practice.",
            ),
            (
                "ccgi.CCG",
                "ccg_code", "CCG Code", "lu",
                "Code of the commissioning organisation (CCG/ICB) for the referring "
                "practice.",
            ),
            (
                "st.STATUS_DESC",
                "patient_status", "Patient Status", "lu",
                "The patient's cancer status at referral — new primary, recurrence, "
                "or metastatic disease, per the registry's coding.",
            ),
            (
                "r.N2_5_DECISION_DATE",
                "decision_to_refer_date", "Decision to Refer Date", "dt",
                "Date the decision to refer was made.",
            ),
            (
                "r.N2_6_RECEIPT_DATE",
                "referral_receipt_date", "Referral Receipt Date", "dt",
                "Date the receiving organisation received the referral. Day zero of "
                "the two-week-wait clock.",
            ),
            (
                "r.N2_9_FIRST_SEEN_DATE",
                "date_first_seen", "Date First Seen", "dt",
                "Date the patient was first seen by the receiving specialist service.",
            ),
            (
                "CASE r.L_FIRST_APPOINTMENT WHEN '1' THEN 'Yes' WHEN '2' THEN 'No' ELSE NULL END",
                "first_appointment", "First Appointment", "lu",
                "Whether this was the patient's first appointment on the pathway.",
            ),
            (
                "noapp.APP_DESC",
                "reason_no_appointment", "Reason No Appointment", "lu",
                "Coded reason no first appointment took place.",
            ),
            (
                "orgseen.Description",
                "organisation", "Organisation", "lu",
                "Organisation where the patient was first seen on this pathway.",
            ),
            (
                _consultant("c1"),
                "consultant_first_appointment", "Consultant (First Appointment)", "clin",
                "Consultant who saw the patient at the first cancer-pathway "
                "appointment, as name then national code.",
            ),
            (
                "CASE r.L_APPROPRIATE WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' ELSE NULL END",
                "appropriate_referral", "Appropriate Referral", "lu",
                "Whether the receiving clinician judged the referral appropriate.",
            ),
            (
                "apptype.TYPE_DESC",
                "appointment_type", "Appointment Type", "lu",
                "Type of first appointment (for example clinic, telephone).",
            ),
            (
                "r.L_SPECIALIST_DATE",
                "specialist_referral_date", "Specialist Referral Date", "dt",
                "Date the patient was referred on for a specialist opinion.",
            ),
            (
                "orgspec.Description",
                "organisation_specialist_referral", "Organisation (Specialist Referral)", "lu",
                "Organisation the patient was referred on to for specialist opinion.",
            ),
            (
                "r.L_SPECIALIST_SEEN_DATE",
                "specialist_seen_date", "Specialist Seen Date", "dt",
                "Date the patient was seen by the specialist team.",
            ),
            (
                "orgspecseen.Description",
                "organisation_specialist_seen", "Organisation (Specialist Seen)", "lu",
                "Organisation where the patient was actually seen by the specialist "
                "team.",
            ),
            (
                _consultant("c2"),
                "consultant", "Consultant", "clin",
                "Consultant the referral was made to, as name then national code.",
            ),
            (
                "spec.SPECIALTY_DESC",
                "specialty", "Specialty", "lu",
                "Main specialty the patient was referred under, decoded to its "
                "national description.",
            ),
            (
                "nice.symptoms",
                "nice_referring_symptoms", "NICE Referring Symptoms", "clin",
                "Semicolon-separated list of the NICE suspected-cancer referral "
                "symptoms recorded against this referral. Multi-valued at source (one "
                "row per symptom); flattened here into a sorted, de-duplicated list. "
                "NULL where the site has no NICE symptom lookup — CUP and Other — "
                "which is absence of a picklist, not absence of symptoms.",
            ),
            (
                "othersym.symptoms",
                "referring_symptoms", "Referring Symptoms", "clin",
                "Semicolon-separated list of the non-NICE referring symptoms recorded "
                "against this referral, flattened from the multi-valued source the "
                "same way.",
            ),
            (
                "r.L_OTHER_SYMPS",
                "other_symptoms", "Other Symptoms", "txt",
                "Free text for symptoms not on the site's coded symptom picklist.",
            ),
            (
                "refm.METHOD_DESC",
                "referral_method", "Referral Method", "lu",
                "How the referral was transmitted (for example e-Referral, letter, "
                "fax).",
            ),
            (
                "app.APP_DESC",
                "appropriate", "Appropriate", "lu",
                "Coded judgement of referral appropriateness, from the registry's "
                "appropriateness lookup.",
            ),
            (
                "r.L_WRONG_REASON",
                "reason_inappropriate", "Reason Inappropriate", "txt",
                "Free text reason the referral was judged inappropriate.",
            ),
            (
                "r.L_COMMENTS",
                "additional_comments", "Additional Comments", "txt",
                "Free-text registry comments recorded against the referral.",
            ),
            (
                "r.L_CANCELLED_DATE",
                "date_dna_appt_rebooked", "Date DNA/Appt Rebooked", "dt",
                "Date a did-not-attend appointment was rebooked. Used in waiting-time "
                "adjustment.",
            ),
            (
                "cancel.CANCELLED_DESC",
                "adjustment_reason", "Adjustment Reason", "lu",
                "Coded reason the waiting-time clock was adjusted, for example "
                "patient-initiated delay.",
            ),
            (
                "r.N2_14_ADJ_TIME",
                "waiting_time_adj", "Waiting Time Adj", "meas",
                "Days by which the waiting-time clock was adjusted.",
            ),
            (
                "delay.DELAY_DESC",
                "first_seen_delay_reason", "First Seen Delay Reason", "lu",
                "Coded reason the patient was not seen within target.",
            ),
            (
                "r.N2_11_FIRST_SEEN_REASON",
                "delay_reason_comments", "Delay Reason Comments", "txt",
                "Free-text explanation of the delay in first being seen.",
            ),
            (
                "datediff(dt.START_DATE, dt.DECISION_DATE)",
                "waiting_time_adj_dtt_days", "Waiting Time Adj (DTT) Days", "meas",
                "Days between the decision to treat and the start of first definitive "
                "treatment, computed by the source view from the first treatment "
                "record.",
            ),
            (
                "r.ADT_REF_ID",
                "adt_ref_id", "ADT Ref ID", "id",
                "Reference to the originating PAS/ADT referral record, where the "
                "registry captured one. The hook back to the operational patient "
                "administration system.",
            ),
            (
                "orgref.Description",
                "referring_organisation", "Referring Organisation", "lu",
                "Organisation that made the referral into this cancer pathway.",
            ),
            (
                _VALIDATED,
                "validated_for_upload", "Validated for Upload", "ctl",
                "Whether the registry record has been validated for national "
                "cancer-registration upload. A data-management flag, not a clinical "
                "one.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_referrals r
LEFT JOIN {SRC}.scr_tbldemographics d ON d.PATIENT_ID = r.PATIENT_ID
LEFT JOIN {SRC}.scr_cancerreferralsubsites crs ON crs.ID = r.SubsiteID
LEFT JOIN {SRC}.scr_ltblout_patient_referral opref ON opref.REF_CODE = r.N2_16_OP_REFERRAL
LEFT JOIN {SRC}.scr_ltblpriority_type pri ON pri.PRIORITY_CODE = r.N2_4_PRIORITY_TYPE
LEFT JOIN {SRC}.scr_ltblreferralroute rr ON rr.ID = r.ReferralRoute
LEFT JOIN {SRC}.scr_ltblcancer_type ct ON ct.CANCER_TYPE_CODE = r.N2_12_CANCER_TYPE
LEFT JOIN {SRC}.scr_gp gp ON gp.ID = r.ReferringGP
LEFT JOIN {SRC}.scr_practice prac ON prac.ID = r.ReferringPractice
LEFT JOIN {SRC}.scr_branch brn ON brn.ID = r.ReferringBranch
LEFT JOIN {SRC}.scr_ltblnational_ccg_practicecode ccgp ON ccgp.PracticeCode = prac.Code
LEFT JOIN {SRC}.scr_ltblnational_ccg_info ccgi ON ccgi.CCG = ccgp.CCG
LEFT JOIN {SRC}.scr_ltblstatus st ON st.STATUS_CODE = r.N2_13_CANCER_STATUS
LEFT JOIN {SRC}.scr_ltblno_app noapp ON noapp.APP_CODE = r.L_NO_APP
LEFT JOIN {SRC}.scr_organisationsites orgseen ON orgseen.Code = r.N1_3_ORG_CODE_SEEN
LEFT JOIN consultants_by_code c1 ON c1.NATIONAL_CODE = r.L_FIRST_CONSULTANT
LEFT JOIN {SRC}.scr_ltblapp_type apptype ON apptype.TYPE_CODE = r.L_FIRST_APP
LEFT JOIN {SRC}.scr_organisationsites orgspec ON orgspec.Code = r.L_ORG_CODE_SPECIALIST
LEFT JOIN {SRC}.scr_organisationsites orgspecseen ON orgspecseen.Code = r.N1_3_ORG_CODE_SPEC_SEEN
LEFT JOIN consultants_by_code c2 ON c2.NATIONAL_CODE = r.N2_7_CONSULTANT
LEFT JOIN {SRC}.scr_ltblspecialties spec ON spec.SPECIALTY_CODE = r.N2_8_SPECIALTY
LEFT JOIN nice_ref_symptoms nice ON nice.CARE_ID = r.CARE_ID
LEFT JOIN other_ref_symptoms othersym ON othersym.CARE_ID = r.CARE_ID
LEFT JOIN {SRC}.scr_ltblref_method refm ON refm.METHOD_CODE = r.L_REFERRAL_METHOD
LEFT JOIN {SRC}.scr_ltblappropriate app ON app.APP_CODE = r.L_WRONG_REF
LEFT JOIN {SRC}.scr_ltblcancellation cancel ON cancel.CANCELLED_CODE = r.N2_15_ADJ_REASON
LEFT JOIN {SRC}.scr_ltbldelay_reason delay ON delay.DELAY_CODE = r.N2_10_FIRST_SEEN_DELAY
LEFT JOIN definitive_treatment_first dt ON dt.CARE_ID = r.CARE_ID
LEFT JOIN {SRC}.scr_organisationsites orgref ON orgref.Code = r.N2_2_ORG_CODE_REF""",
    },
    # ------------------------------------------------------------------ #
    {
        "view": "BIvwTreatmentSurgery",
        "target": "scr_treatment_surgery",
        "ctes": ["consultants_by_code"],
        "requires": ["scr_all_surgery", "scr_all_complications"],
        "comment": (
            "Surgical treatment episodes with their operative detail and complications, "
            "rebuilt from base tables in {src} to the scr_22_02 BIvwTreatmentSurgery "
            "definition. One row per tblMAIN_SURGERY record (51,257 at landing). This is "
            "the wide surgical product: the treatment-episode header (decision, "
            "admission, surgery and discharge dates, intent, setting, length of stay) "
            "joined to the harmonised operative narrative from scr_all_surgery and the "
            "recorded complications from scr_all_complications. TWO FIXES carried in from "
            "its inputs and one of its own: the duplicated Upper GI complications arm is "
            "gone (it doubled every Upper GI row here), and the primary-procedure decode "
            "no longer routes CUP through the second procedure column. Procedure "
            "resolution is site-conditional — the same code means different things in "
            "different specialty lookups, which is why Somerset's CASE exists."
        ),
        "columns": [
            (
                "ms.CARE_ID",
                "care_id", "CARE_ID", "id",
                "Somerset's identifier for one patient's care pathway for one cancer. "
                "A patient with two primaries has two CARE_IDs, so this is the "
                "cancer-episode key, not a patient key — join to "
                "scr_tblmain_referrals for the patient.",
            ),
            (
                "ms.SURGERY_ID",
                "surgery_id", "SURGERY_ID", "id",
                "Somerset's key for one operation record in tblMAIN_SURGERY. The join "
                "key that links an operation to its specialty-specific detail table.",
            ),
            (
                "mr.L_CANCER_SITE",
                "cancer_site", "Cancer Site", "lu",
                "The tumour-site pathway this care episode sits on — Breast, "
                "Colorectal, Skin, Gynaecology, Head and Neck, Urology, Upper GI, "
                "Lung, Haematology, CUP, Brain, Sarcoma, Paediatric or Other. "
                "Somerset's own pathway grouping, which drives which specialty forms "
                "and lookups apply; it is not a coded diagnosis site.",
            ),
        ]
        + _DEMOG_COLS
        + [
            (
                _consultant("c"),
                "consultant", "Consultant", "clin",
                "Responsible consultant, rendered as name then national code. "
                "Clinician identifier: identifying of staff rather than of the "
                "patient, hence a lower severity tag than patient identifiers.",
            ),
            (
                "ms.L_URGENCY",
                "urgency_of_surgery", "Urgency of Surgery", "lu",
                "Urgency with which the operation was carried out.",
            ),
            (
                "ti.SurgicalIntentDesc",
                "treatment_intent", "Treatment Intent", "lu",
                "Whether treatment was given with curative or palliative intent. "
                "Drives almost every downstream outcome analysis.",
            ),
            (
                "adj.AdjunctiveTherapyDesc",
                "adjunctive_therapy", "Adjunctive Therapy", "lu",
                "Additional therapy given alongside the main treatment.",
            ),
            (
                "ms.N7_5_DECISION_DATE",
                "date_of_decision_to_operate", "Date of Decision to Operate", "dt",
                "Date the decision to operate was made.",
            ),
            (
                "org1.Description",
                "organisation_dtt", "Organisation (DTT)", "lu",
                "Organisation at which the decision to treat was made. Distinct from "
                "the treating organisation: national cancer waiting-time rules "
                "attribute the clock to the decision site, which may not be where "
                "treatment happened.",
            ),
            (
                "to_date(ms.N7_8_ADMISSION_DATE)",
                "date_of_admission", "Date of Admission", "dt",
                "Date the patient was admitted for the operation.",
            ),
            (
                "ms.N7_1_SITE_CODE",
                "organisation_treatment_site_code", "Organisation (Treatment) Site Code", "lu",
                "ODS site code of the organisation that delivered the treatment, as "
                "recorded. Raw code, kept alongside the resolved name so an unmatched "
                "code is still visible.",
            ),
            (
                "org2.Description",
                "organisation_treatment_name", "Organisation (Treatment) Name", "lu",
                "Name of the organisation that delivered the treatment, resolved from "
                "the site code.",
            ),
            (
                "te.EVENT_DESC",
                "treatment_event_type", "Treatment Event Type", "lu",
                "Whether this is first definitive treatment, a subsequent treatment, "
                "or a recurrence treatment — the national treatment-event "
                "classification.",
            ),
            (
                "ts.SET_DESC",
                "treatment_setting", "Treatment Setting", "lu",
                "Setting in which treatment was delivered, for example inpatient, day "
                "case or outpatient.",
            ),
            (
                "CASE ms.L_TRIAL WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' WHEN 99 THEN 'Unknown' "
             "ELSE NULL END",
                "clinical_trial", "Clinical Trial", "lu",
                "Whether this treatment was delivered as part of a clinical trial.",
            ),
            (
                "to_date(ms.N7_9_SURGERY_DATE)",
                "date_of_surgery", "Date of Surgery", "dt",
                "Date the operation was performed.",
            ),
            (
                "_PRIMARY_PROCEDURE_",
                "primary_procedure", "Primary Procedure", "lu",
                "The principal operative procedure, resolved through whichever "
                "specialty procedure lookup matches the cancer site.",
            ),
            (
                "srg.sub_procedure_1",
                "sub_procedure_1", "Sub Procedure 1", "lu",
                "First additional procedure performed at the same operation.",
            ),
            (
                "srg.sub_procedure_2",
                "sub_procedure_2", "Sub Procedure 2", "lu",
                "Second additional procedure performed at the same operation.",
            ),
            (
                "srg.sub_procedure_3",
                "sub_procedure_3", "Sub Procedure 3", "lu",
                "Third additional procedure. Only the gynaecology pathway records a "
                "fourth procedure slot, so this is NULL everywhere else.",
            ),
            (
                "srg.operation_not_performed",
                "operation_not_performed", "Operation Not Performed", "lu",
                "Whether the planned operation did not go ahead. NULL means it went "
                "ahead or nothing was recorded; the source only ever sets this to "
                "'Yes'.",
            ),
            (
                "srg.reason_operation_not_performed",
                "reason_operation_not_performed", "Reason Operation Not Performed", "lu",
                "Reason the planned operation did not go ahead — a coded reason on "
                "most pathways, free text on the breast and head and neck pathways.",
            ),
            (
                "surgeon.CON_DESC",
                "surgeon", "Surgeon", "clin",
                "Operating surgeon, as name then national code. In scr_pathology this "
                "is instead the clinician who requested the specimen.",
            ),
            (
                "ms.L_GRADE",
                "grade_of_surgeon", "Grade of Surgeon", "clin",
                "Grade of the operating surgeon.",
            ),
            (
                "assistant1.CON_DESC",
                "assistant", "Assistant", "clin",
                "First assisting surgeon, as name then national code.",
            ),
            (
                "ms.L_GRADE_1",
                "grade_of_assistant", "Grade of Assistant", "clin",
                "Grade of the assisting surgeon.",
            ),
            (
                "ms.L_ANAESTHETIST_NAME",
                "anaesthetist_name", "Anaesthetist Name", "clin",
                "Name of the anaesthetist, as free text.",
            ),
            (
                "ms.L_ANAESTHETIST",
                "grade_of_anaesthetist", "Grade of Anaesthetist", "clin",
                "Grade of the anaesthetist.",
            ),
            (
                "srg.asa_grade",
                "asa_grade", "ASA Grade", "lu",
                "ASA physical status classification, 1 (healthy) to 5 (moribund). The "
                "anaesthetic risk measure, and the main case-mix adjuster for "
                "surgical outcomes.",
            ),
            (
                "srg.start_time",
                "start_time", "Start Time", "dt",
                "Time the operation started, as hh:mm:ss.",
            ),
            (
                "srg.end_time_wound_closure",
                "end_time_wound_closure", "End Time (Wound Closure)", "dt",
                "Time the operation finished at wound closure, as hh:mm:ss.",
            ),
            (
                "srg.procedure_comments",
                "procedure_comments", "Procedure Comments", "txt",
                "Free-text operative comments.",
            ),
            (
                "srg.indication_for_surgery",
                "indication_for_surgery", "Indication for Surgery", "txt",
                "Free-text clinical indication for operating.",
            ),
            (
                "srg.incision",
                "incision", "Incision", "txt",
                "Free-text description of the surgical incision used.",
            ),
            (
                "srg.findings",
                "findings", "Findings", "txt",
                "Free-text description of the operative findings.",
            ),
            (
                "srg.specimens",
                "specimens", "Specimens", "txt",
                "Free-text description of specimens taken during the operation.",
            ),
            (
                "srg.drains",
                "drains", "Drains", "txt",
                "Free-text description of drains placed during the operation.",
            ),
            (
                "srg.closure",
                "closure", "Closure", "txt",
                "Free-text description of how the wound was closed.",
            ),
            (
                "srg.post_op_instructions",
                "post_op_instructions", "Post-Op Instructions", "txt",
                "Free-text post-operative instructions.",
            ),
            (
                "ms.L_POST_COMP",
                "complications", "Complications", "txt",
                "Free-text summary of post-operative complications.",
            ),
            (
                "cmp.complication_1",
                "complication_1", "Complication 1", "lu",
                "First recorded post-operative complication, decoded through the "
                "specialty's own complication lookup.",
            ),
            (
                "cmp.complication_2",
                "complication_2", "Complication 2", "lu",
                "Second recorded post-operative complication.",
            ),
            (
                "cmp.complication_3",
                "complication_3", "Complication 3", "lu",
                "Third recorded post-operative complication.",
            ),
            (
                "ms.L_OTHER_COMP",
                "complication_details", "Other Complications", "txt",
                "Free-text detail of complications not on the coded picklist.",
            ),
            (
                "ms.L_TRANSFER",
                "transfer", "Transfer", "lu",
                "Whether and where the patient was transferred after the operation.",
            ),
            (
                "ms.L_DAYS",
                "days", "Days", "meas",
                "Days recorded against the post-operative stay, as captured by the "
                "registry.",
            ),
            (
                "ms.L_DISCHARGE_COMMENTS",
                "discharge_comments", "Discharge Comments", "txt",
                "Free-text comments recorded at discharge.",
            ),
            (
                "ms.L_RETURN_ITU",
                "return_to_itu", "Return to ITU", "lu",
                "Whether the patient returned to intensive care after the operation.",
            ),
            (
                "ms.L_DAYS_BACK",
                "days_back", "Days Back", "meas",
                "Days before the patient returned to intensive care.",
            ),
            (
                "ms.N7_12_DISCHARGE_DATE",
                "discharge_date", "Discharge Date", "dt",
                "Date the patient was discharged after the operation.",
            ),
            (
                "dest.DEST_DESC",
                "discharge_destination", "Discharge Destination", "lu",
                "Where the patient was discharged to.",
            ),
            (
                "datediff(ms.N7_12_DISCHARGE_DATE, ms.N7_8_ADMISSION_DATE)",
                "length_of_stay", "Length of Stay", "meas",
                "Days between admission and discharge, computed by the source view. "
                "NULL if either date is missing; can be negative if the dates are "
                "transposed at source.",
            ),
            (
                "scs1.SOURCE_DESC",
                "stem_cell_infusion_source_1", "Stem Cell Infusion Source 1", "lu",
                "First recorded source of stem cells for infusion — autologous or "
                "allogeneic, and the harvest site. Haematology pathway only.",
            ),
            (
                "scs2.SOURCE_DESC",
                "stem_cell_infusion_source_2", "Stem Cell Infusion Source 2", "lu",
                "Second recorded stem cell infusion source.",
            ),
            (
                "scs3.SOURCE_DESC",
                "stem_cell_infusion_source_3", "Stem Cell Infusion Source 3", "lu",
                "Third recorded stem cell infusion source.",
            ),
            (
                "scd1.DONOR_DESC",
                "stem_cell_donor_1", "Stem Cell Donor 1", "lu",
                "First recorded stem cell donor relationship, for example sibling or "
                "unrelated matched donor.",
            ),
            (
                "scd2.DONOR_DESC",
                "stem_cell_donor_2", "Stem Cell Donor 2", "lu",
                "Second recorded stem cell donor relationship.",
            ),
            (
                "scd3.DONOR_DESC",
                "stem_cell_donor_3", "Stem Cell Donor 3", "lu",
                "Third recorded stem cell donor relationship.",
            ),
            (
                "pcomp1.COMPLICATION_DESC",
                "postoperative_complications_1", "Postoperative Complications (1)", "lu",
                "First post-operative complication decoded through the general "
                "(cross-specialty) complication lookup, as the treatment-surgery view "
                "reports it.",
            ),
            (
                "pcomp2.COMPLICATION_DESC",
                "postoperative_complications_2", "Postoperative Complications (2)", "lu",
                "Second post-operative complication from the general complication "
                "lookup.",
            ),
            (
                "pcomp3.COMPLICATION_DESC",
                "postoperative_complications_3", "Postoperative Complications (3)", "lu",
                "Third post-operative complication from the general complication "
                "lookup.",
            ),
            (
                "gp1.PROC_DESC",
                "surgical_procedure_1", "Surgical Procedure 1", "lu",
                "First operative procedure decoded through the general procedure "
                "lookup, independent of the site-specific decoding in "
                "primary_procedure.",
            ),
            (
                "gp2.PROC_DESC",
                "surgical_procedure_2", "Surgical Procedure 2", "lu",
                "Second operative procedure from the general procedure lookup.",
            ),
            (
                "gp3.PROC_DESC",
                "surgical_procedure_3", "Surgical Procedure 3", "lu",
                "Third operative procedure from the general procedure lookup.",
            ),
            (
                "gp4.PROC_DESC",
                "surgical_procedure_4", "Surgical Procedure 4", "lu",
                "Fourth operative procedure from the general procedure lookup.",
            ),
        ],
        "from_sql": f"""{SRC}.scr_tblmain_surgery ms
LEFT JOIN {SRC}.scr_tblmain_referrals mr ON mr.CARE_ID = ms.CARE_ID
LEFT JOIN {SRC}.scr_tbldemographics d ON d.PATIENT_ID = mr.PATIENT_ID
LEFT JOIN consultants_by_code c ON c.NATIONAL_CODE = ms.N7_2_CONSULTANT
LEFT JOIN consultants_by_code surgeon ON surgeon.NATIONAL_CODE = ms.L_SURGEON
LEFT JOIN consultants_by_code assistant1 ON assistant1.NATIONAL_CODE = ms.L_ASSISTANT_1
LEFT JOIN {SRC}.scr_ltbltreatmentintent ti ON ti.IntentID = ms.N7_4_TREATMENT_INTENT
LEFT JOIN {SRC}.scr_ltbladjunctivetherapy adj ON adj.AdjunctiveTherapyID = ms.AdjunctiveTherapyID
LEFT JOIN {SRC}.scr_organisationsites org1 ON org1.Code = ms.N_SITE_CODE_DTT
LEFT JOIN {SRC}.scr_organisationsites org2 ON org2.Code = ms.N7_1_SITE_CODE
LEFT JOIN {SRC}.scr_ltbltreatment_event te ON te.EVENT_CODE = ms.N_TREATMENT_EVENT
LEFT JOIN {SRC}.scr_ltbltreatment_setting ts ON ts.SET_CODE = ms.N_TREATMENT_SETTING
LEFT JOIN {SRC}.scr_ltbldestination dest ON dest.DEST_CODE = ms.N7_13_DISCHARGE_DESTINATION
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_source scs1 ON scs1.SOURCE_ID = ms.STEM_CELL_INFUSION_SOURCE_ID
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_source scs2 ON scs2.SOURCE_ID = ms.STEM_CELL_INFUSION_SOURCE_ID2
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_source scs3 ON scs3.SOURCE_ID = ms.STEM_CELL_INFUSION_SOURCE_ID3
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_donor scd1 ON scd1.DONOR_ID = ms.STEM_CELL_INFUSION_DONOR_ID
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_donor scd2 ON scd2.DONOR_ID = ms.STEM_CELL_INFUSION_DONOR_ID2
LEFT JOIN {SRC}.scr_ltblstem_cell_infusion_donor scd3 ON scd3.DONOR_ID = ms.STEM_CELL_INFUSION_DONOR_ID3
LEFT JOIN {SRC}.scr_ltblcomplications pcomp1 ON pcomp1.COMPLICATION_CODE = ms.L_COMPLICATIONS
LEFT JOIN {SRC}.scr_ltblcomplications pcomp2 ON pcomp2.COMPLICATION_CODE = ms.L_COMPLICATIONS2
LEFT JOIN {SRC}.scr_ltblcomplications pcomp3 ON pcomp3.COMPLICATION_CODE = ms.L_COMPLICATIONS3
LEFT JOIN {SRC}.scr_ltblprocedures gp1 ON gp1.PROC_CODE = ms.N7_10_PROCEDURE_1
LEFT JOIN {SRC}.scr_ltblprocedures gp2 ON gp2.PROC_CODE = ms.N7_11_PROCEDURE_2
LEFT JOIN {SRC}.scr_ltblprocedures gp3 ON gp3.PROC_CODE = ms.N7_11_PROCEDURE_3
LEFT JOIN {SRC}.scr_ltblprocedures gp4 ON gp4.PROC_CODE = ms.N7_11_PROCEDURE_4
LEFT JOIN {TARGET_SCHEMA}.scr_all_surgery srg ON srg.surgery_id = ms.SURGERY_ID
LEFT JOIN {TARGET_SCHEMA}.scr_all_complications cmp
       ON cmp.care_id = ms.CARE_ID AND cmp.surgery_id = ms.SURGERY_ID""",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## The site-conditional primary-procedure decode
# MAGIC
# MAGIC Somerset resolves `[Primary Procedure]` through a different lookup per cancer site,
# MAGIC because the same procedure code means different things in different specialty
# MAGIC lookups. `scr_all_surgery.main_procedure` already did exactly that work per branch,
# MAGIC so the 7-arm CASE is replaced by reading it — with a fallback to the generic
# MAGIC `ltblPROCEDURES` decode for surgeries that have no specialty extension row and so
# MAGIC never reach the roll-up.
# MAGIC
# MAGIC That also removes the CUP defect for free: Somerset's CUP arm read
# MAGIC `N7_11_PROCEDURE_2`, and the fallback reads `N7_10_PROCEDURE_1` like every other
# MAGIC site. Under `fix_somerset_defects = false` the original CUP behaviour is restored.

# COMMAND ----------

# Somerset's CASE exists only to route CUP through the second procedure column.
# Removing that arm leaves no branches, and `CASE x ELSE y END` without a WHEN does
# not parse — so the fixed form is the bare fallback, not an empty CASE.
_PRIMARY_PROCEDURE = (
    "coalesce(srg.main_procedure, gp1.PROC_DESC)"
    if FIX_SOMERSET_DEFECTS
    else (
        "coalesce(srg.main_procedure, CASE mr.L_CANCER_SITE "
        "WHEN 'CUP' THEN gp2.PROC_DESC ELSE gp1.PROC_DESC END)"
    )
)

for _spec in ROLLUP_SPECS:
    if _spec["target"] != "scr_treatment_surgery":
        continue
    _spec["columns"] = [
        (
            _PRIMARY_PROCEDURE if expr == "_PRIMARY_PROCEDURE_" else expr,
            name, display, ig, meaning,
        )
        for expr, name, display, ig, meaning in _spec["columns"]
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## A divergence corrected against the landed columns
# MAGIC
# MAGIC The DIVERGENCES list above records the two urology screening flags alongside
# MAGIC `VALIDATED` as flags that coerce "unrecorded" to a negative. Measuring them
# MAGIC disproved half of it, so the entry is replaced rather than appended to — a
# MAGIC divergence log that contradicts itself is worse than one that is merely
# MAGIC incomplete. The replacement asserts its target, so editing that text upstream
# MAGIC fails the build instead of silently dropping the correction.

# COMMAND ----------

_superseded = [
    i for i, (_view, _text) in enumerate(DIVERGENCES)
    if _text.startswith("Three flags coerce unrecorded")
]
assert len(_superseded) == 1, (
    f"expected exactly one superseded divergence entry, found {len(_superseded)}"
)

DIVERGENCES[_superseded[0]] = (
    "BIvwBreastReferrals / BIvwPresentation",
    "[Validated for Upload] uses ELSE 'No', and VALIDATED is a nullable INT reached "
    "through a LEFT JOIN to tblDEFINITIVE_TREATMENT, so a referral with no treatment "
    "record yet is published as 'not validated'. Under fix_somerset_defects it publishes "
    "NULL. The urology screening flags were recorded here as the same defect and are not: "
    "UroHaematuria, UroRaisedPSA and all 32 comorbidity boxes land as BOOLEAN NOT NULL "
    f"(zero NULLs across {_IA_ROWS} rows) — a BIT DEFAULT 0 — so there is no unrecorded "
    "state to coerce and Somerset's ELSE 'No' is faithful. They are decoded Yes/No in "
    "both modes; the caveat that a false bit means 'not ticked' rather than 'assessed and "
    "absent' is carried in the column comments, where a researcher will actually meet it. "
    "Somerset's own `CASE col WHEN 1` over a BOOLEAN is a type error in Spark regardless.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fan-out baselines and the assembled spec list
# MAGIC
# MAGIC `EXPECTED_BASE` is the spine row count each product must not exceed. It is measured
# MAGIC live rather than frozen as a literal: a hard-coded baseline silently goes stale as
# MAGIC the register grows and then fails a run for a reason that has nothing to do with
# MAGIC the build. (The baseline first written for `scr_staging`, 514,784, is already wrong
# MAGIC — tblMAIN_REFERRALS now holds 517,281 rows.)

# COMMAND ----------

_SPINE = {
    "scr_metastases": "scr_tblmetastases",
    "scr_treatment_anticancer_drugs": "scr_tblmain_chemotherapy",
    "scr_treatment_teletherapy": "scr_tblmain_teletherapy",
    "scr_treatment_brachytherapy": "scr_tblmain_brachytherapy",
    "scr_treatment_clinical_trial": "scr_tblmain_trial",
    "scr_care_plan_mdt": "scr_tblmain_care_plan",
    "scr_pathology": "scr_tblmain_pathology",
    "scr_staging": "scr_tblmain_referrals",
    "scr_follow_up": "scr_tblmain_assessment",
    "scr_all_complications": "scr_tblmain_surgery",
    "scr_all_surgery": "scr_tblmain_surgery",
    "scr_breast_tumour_markers": "scr_tblpathology_breast",
    "scr_breast_diagnosis_detail": "scr_tblreferral_breast",
    "scr_presentation": "scr_tblinitial_assessment",
    "scr_referrals": "scr_tblmain_referrals",
    "scr_treatment_surgery": "scr_tblmain_surgery",
}

_counts = spark.sql(
    "SELECT "
    + ", ".join(
        f"(SELECT count(*) FROM {SRC}.{tbl}) AS {target}"
        for target, tbl in _SPINE.items()
    )
).collect()[0].asDict()

EXPECTED_BASE = {target: int(_counts[target]) for target in _SPINE}

VIEW_SPECS = LEAF_SPECS + ROLLUP_SPECS

print(f"{len(VIEW_SPECS)} specs ({len(LEAF_SPECS)} leaf + {len(ROLLUP_SPECS)} roll-up)")
print(f"{len(SURGERY_BRANCHES)} surgery branches, {len(COMPLICATION_BRANCHES)} complication branches")
print(f"{len(DIVERGENCES)} recorded divergences, {len(UNRESOLVED_SOURCE_QUESTIONS)} open questions")
for _t, _b in sorted(EXPECTED_BASE.items()):
    print(f"  base {_t:<32} {_b:>9,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL generation
# MAGIC
# MAGIC A spec is either **single-source** (`from_sql`) or **branched** (`branches`, a list
# MAGIC of `{label, from_sql, exprs}` evaluated against one shared superset column list).
# MAGIC Branched specs are how the `UNION ALL` roll-ups are expressed: a branch supplies
# MAGIC SQL only for the columns it has, and everything else comes through as a typed NULL
# MAGIC rather than being dropped from the product.

# COMMAND ----------

_CONTROL_COLUMNS = [
    ("pipeline_run_id", "ctl"),
    ("ADC_UPDT", "ctl"),
]

_NULL_TYPES = {"int": "CAST(NULL AS INT)", "str": "CAST(NULL AS STRING)"}


def _select_list(spec, branch=None):
    out = []
    for expr, name, _display, _cls, _meaning in spec["columns"]:
        if branch is None:
            value = expr
        elif name == "somerset_branch":
            value = q(branch["label"])
        else:
            value = branch["exprs"].get(name, _NULL_TYPES["str"])
        out.append(f"{value} AS {name}")
    out.append(f"{q(RUN_ID)} AS pipeline_run_id")
    out.append("current_timestamp() AS ADC_UPDT")
    return ",\n    ".join(out)


def build_sql(spec):
    ctes = resolve_ctes(spec.get("ctes", []))
    with_clause = ("WITH " + ",\n".join(ctes) + "\n") if ctes else ""
    if spec.get("branches"):
        blocks = [
            f"SELECT\n    {_select_list(spec, br)}\nFROM {br['from_sql']}"
            for br in spec["branches"]
        ]
        return with_clause + "\nUNION ALL\n".join(blocks)
    return f"{with_clause}SELECT\n    {_select_list(spec)}\nFROM {spec['from_sql']}"


_TABLE_REF = re.compile(re.escape(SRC) + r"\.([a-z0-9_]+)", re.IGNORECASE)


def spec_sources(spec):
    """Source tables a spec reads, discovered from the generated SQL.

    Reading the emitted SQL rather than the spec fields means CTE dependencies and
    branch FROM clauses are covered automatically, and references to TARGET_SCHEMA
    (the roll-ups that feed BIvwTreatmentSurgery) are correctly excluded — they are
    built by this notebook, not landed by ADF.
    """
    return sorted({m.lower() for m in _TABLE_REF.findall(build_sql(spec))})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight
# MAGIC
# MAGIC Fail before writing anything, not halfway through. Checks: every source table
# MAGIC referenced by any spec or CTE exists; every bronze column name is safe, unique and
# MAGIC ig-classified; **every column has a plain-English description**; a spec that reads
# MAGIC another product is built after it.

# COMMAND ----------

all_sources = sorted({t for s in VIEW_SPECS for t in spec_sources(s)})
missing_sources = [t for t in all_sources if not bronze_table_exists(f"{SRC}.{t}")]

problems = []
built_by_now = set()

for spec in VIEW_SPECS:
    target = spec["target"]
    names = [c[1] for c in spec["columns"]]
    for name in names:
        if not SAFE_NAME.match(name):
            problems.append(f"{target}.{name}: unsafe column name")
    for dupe in {n for n in names if names.count(n) > 1}:
        problems.append(f"{target}.{dupe}: duplicate column name")
    for col in spec["columns"]:
        if len(col) != 5:
            problems.append(
                f"{target}: {col[1] if len(col) > 1 else col} is not a 5-tuple "
                "(expr, name, display, ig_class, meaning)"
            )
            continue
        _expr, name, _display, ig_class, meaning = col
        if ig_class not in IG_CLASSES:
            problems.append(f"{target}.{name}: unknown ig class {ig_class}")
        if not str(meaning or "").strip():
            problems.append(f"{target}.{name}: no plain-English meaning")
    for br in spec.get("branches", []):
        unknown = set(br["exprs"]) - set(names)
        if unknown:
            problems.append(f"{target}/{br['label']}: expressions for unknown columns {sorted(unknown)}")
    for need in spec.get("requires", []):
        if need not in built_by_now:
            problems.append(f"{target}: reads {need}, which is not built earlier in VIEW_SPECS")
    built_by_now.add(target)

print(f"specs:              {len(VIEW_SPECS)}")
print(f"mapped columns:     {sum(len(s['columns']) for s in VIEW_SPECS)}")
print(f"sources referenced: {len(all_sources)}")
print(f"missing sources:    {missing_sources or 'none'}")
print(f"problems:           {problems or 'none'}")

assert not missing_sources, (
    f"Pre-flight failed — {len(missing_sources)} source tables absent from {SRC}: "
    f"{missing_sources}. Register them in the IncrUpdtV2 watermark before running."
)
assert not problems, f"Pre-flight failed — {problems}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source freshness
# MAGIC
# MAGIC `ancil_scr` is a weekly `wt_updt` full-overwrite landing. If it has not refreshed,
# MAGIC rebuilding the views just restates stale data — worth seeing in the run log rather
# MAGIC than discovering downstream. Advisory only; never fatal.

# COMMAND ----------

freshness_rows = []
for table in all_sources:
    try:
        row = spark.sql(f"DESCRIBE HISTORY {SRC}.{table} LIMIT 1").select("timestamp").collect()
        last = str(row[0][0]) if row else None
    except Exception as exc:  # noqa: BLE001 - freshness is advisory, never fatal
        last = f"ERROR: {exc}"
    freshness_rows.append((table, last))

stalest = sorted(
    (r for r in freshness_rows if r[1] and not str(r[1]).startswith("ERROR")),
    key=lambda r: r[1],
)
if stalest:
    print(f"oldest source write: {stalest[0][0]} @ {stalest[0][1]}")
    print(f"newest source write: {stalest[-1][0]} @ {stalest[-1][1]}")
    SOURCE_OLDEST_WRITE = stalest[0][1]
else:
    SOURCE_OLDEST_WRITE = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build
# MAGIC
# MAGIC One `CREATE OR REPLACE TABLE ... AS SELECT` per view. Full snapshot every run,
# MAGIC because the sources are themselves full-overwrite — there is no delta to merge and
# MAGIC no natural unique key on most of these views. CTAS is a single statement, so it is
# MAGIC serverless-safe (no TEMP VIEWs crossing cell boundaries, no caching).
# MAGIC
# MAGIC `CREATE OR REPLACE` drops column tags, so IG tags and comments are re-applied on
# MAGIC every run rather than assumed to persist.

# COMMAND ----------

def apply_metadata(target, spec):
    for _expr, name, display, _cls, meaning in spec["columns"]:
        comment = f"{meaning} Source: [{display}] in Somerset {spec['view']} ({SOMERSET_RELEASE})."
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN {name} COMMENT {q(comment)}")
    spark.sql(
        f"ALTER TABLE {target} ALTER COLUMN pipeline_run_id COMMENT "
        + q(
            "Bronze pipeline run that last rebuilt this snapshot. Every row of a given "
            "rebuild shares one value, so it identifies the snapshot, not the record."
        )
    )
    spark.sql(
        f"ALTER TABLE {target} ALTER COLUMN ADC_UPDT COMMENT "
        + q(
            "Time this snapshot was rebuilt by the bronze pipeline. NOT a clinical "
            "timestamp and NOT when the underlying registry record changed."
        )
    )
    if not APPLY_IG_TAGS:
        return
    tagged = [(c[1], c[3]) for c in spec["columns"]] + _CONTROL_COLUMNS
    for name, ig_class in tagged:
        risk, severity = IG_CLASSES[ig_class]
        spark.sql(
            f"ALTER TABLE {target} ALTER COLUMN {name} "
            f"SET TAGS ('ig_risk' = {q(risk)}, 'ig_severity' = {q(severity)})"
        )


def record_sql(target, sql):
    """Persist a generated statement before it runs.

    A build that kills the driver rather than raising leaves no run-log row, so
    without this the failing statement cannot be recovered or reproduced.
    """
    (
        spark.createDataFrame(
            [(RUN_ID, target, sql)],
            "run_id STRING, target_table STRING, statement STRING",
        )
        .withColumn("logged_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(T_SQL)
    )


def build(spec):
    target = f"{TARGET_SCHEMA}.{spec['target']}"
    comment = spec["comment"].format(src=SRC)
    started = bronze_utc_now()
    ddl = (
        f"CREATE OR REPLACE TABLE {target}\n"
        f"COMMENT {q(comment)}\n"
        f"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',\n"
        f"               'scr_source_view' = {q(spec['view'])},\n"
        f"               'scr_somerset_release' = {q(SOMERSET_RELEASE)},\n"
        f"               'scr_logic_version' = {q(SCR_VIEW_LOGIC_VERSION)},\n"
        f"               'scr_fix_somerset_defects' = {q(str(FIX_SOMERSET_DEFECTS).lower())})\n"
        f"AS\n{build_sql(spec)}"
    )
    record_sql(target, ddl)
    try:
        spark.sql(ddl)
        apply_metadata(target, spec)
        rows = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        print(f"[OK]   {target}: {rows:,} rows, {len(spec['columns'])} mapped columns")
        return {
            "view": spec["view"],
            "target": target,
            "status": "OK",
            "row_count": int(rows),
            "column_count": len(spec["columns"]),
            "sources": spec_sources(spec),
            "started_at": started,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - collect and report all failures
        print(f"[FAIL] {target}: {exc}")
        return {
            "view": spec["view"],
            "target": target,
            "status": "FAILED",
            "row_count": None,
            "column_count": len(spec["columns"]),
            "sources": spec_sources(spec),
            "started_at": started,
            "error": str(exc)[:4000],
        }


selected = [s for s in VIEW_SPECS if not ONLY_TARGETS or s["target"] in ONLY_TARGETS]
assert selected, f"only_targets matched no spec: {ONLY_TARGETS}"
results = [build(spec) for spec in selected]
failed = [r for r in results if r["status"] != "OK"]

if failed and not CONTINUE_ON_ERROR:
    raise RuntimeError(
        f"{len(failed)} of {len(results)} views failed to build: "
        f"{[r['target'] for r in failed]}. Re-run with continue_on_error=true to "
        "publish the rest and triage from the run log."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Advisory except where a check guards a divergence **we** introduced — those are
# MAGIC asserted, because a divergence that stops being safe must stop the build.

# COMMAND ----------

checks = []


def record(name, status, detail):
    checks.append({"check": name, "status": status, "detail": detail})
    print(f"[{status}] {name}: {detail}")


built = {r["target"].split(".")[-1]: r for r in results if r["status"] == "OK"}


def scalar(sql):
    return spark.sql(sql).collect()[0][0]


# 1. ConsultantsByCode dedup behaves as documented.
cons = spark.sql(
    f"""WITH {CTES['consultants_by_code'][1]}
SELECT count(*) AS deduped,
       count(DISTINCT NATIONAL_CODE) AS distinct_codes,
       (SELECT count(*) FROM {SRC}.scr_ltblconsultants) AS base_rows
FROM consultants_by_code"""
).collect()[0]
record(
    "consultants_dedup",
    "PASS" if cons["deduped"] == cons["distinct_codes"] else "FAIL",
    f"{cons['base_rows']} base rows -> {cons['deduped']} deduped "
    f"({cons['distinct_codes']} distinct NATIONAL_CODE); "
    f"{cons['base_rows'] - cons['deduped']} rows dropped by the ROW_NUMBER dedup",
)

# 2. BIvwStaging join pruning is lossless: each tblREFERRAL_* must be 1:1 on CARE_ID.
fanout = []
for table in REFERRAL_TABLES:
    row = spark.sql(
        f"SELECT count(*) AS n, count(DISTINCT CARE_ID) AS d FROM {SRC}.{table}"
    ).collect()[0]
    if row["n"] != row["d"]:
        fanout.append(f"{table}: {row['n']} rows / {row['d']} distinct CARE_ID")
record(
    "staging_fanout",
    "PASS" if not fanout else "FAIL",
    "all 14 tblREFERRAL_* are unique on CARE_ID — dropping Somerset's vestigial "
    "joins is lossless" if not fanout else f"NOT unique: {fanout}",
)
assert not fanout, (
    "BIvwStaging divergence is unsafe: " + str(fanout) + ". Somerset's view fans these "
    "rows out; either restore the 14 LEFT JOINs or dedup deliberately before publishing."
)

# 3. BIvwReferrals joins tblDEFINITIVE_TREATMENT on TREAT_NO = 1; if that is not unique
#    on CARE_ID the referral spine fans out and every count built on it is wrong.
dt = spark.sql(
    f"""SELECT count(*) AS n, count(DISTINCT CARE_ID) AS d
FROM {SRC}.scr_tbldefinitive_treatment WHERE TREAT_NO = 1"""
).collect()[0]
record(
    "definitive_treatment_unique",
    "PASS" if dt["n"] == dt["d"] else "DEDUPED",
    f"tblDEFINITIVE_TREATMENT TREAT_NO=1: {dt['n']} rows / {dt['d']} distinct CARE_ID"
    + (
        ""
        if dt["n"] == dt["d"]
        else f"; {dt['n'] - dt['d']} CARE_IDs carry a duplicate, collapsed by the "
        "definitive_treatment_first CTE (has-data ordering, then highest TREATMENT_ID). "
        "Somerset ships the fan-out"
    ),
)

# The guard that matters is on the product, not the source: the referral spine must
# be one row per CARE_ID however many duplicates the registry holds.
if "scr_referrals" in built:
    ref = spark.sql(
        f"SELECT count(*) AS n, count(DISTINCT care_id) AS d "
        f"FROM {TARGET_SCHEMA}.scr_referrals"
    ).collect()[0]
    record(
        "referrals_grain",
        "PASS" if ref["n"] == ref["d"] else "FAIL",
        f"{ref['n']} rows / {ref['d']} distinct care_id",
    )
    assert ref["n"] == ref["d"], (
        f"scr_referrals fans out by {ref['n'] - ref['d']} rows — the referral spine "
        "must be one row per CARE_ID. The definitive-treatment dedup did not hold."
    )

# 4. BIvwMetastases INNER join loss.
mets = spark.sql(
    f"""SELECT (SELECT count(*) FROM {SRC}.scr_tblmetastases) AS base,
           (SELECT count(*) FROM {TARGET_SCHEMA}.scr_metastases) AS published"""
).collect()[0]
record(
    "metastases_inner_join_loss",
    "PASS" if mets["published"] <= mets["base"] else "REVIEW",
    f"{mets['base']} tblMetastases rows -> {mets['published']} published; "
    f"{mets['base'] - mets['published']} dropped by Somerset's INNER join to "
    "tblMAIN_REFERRALS",
)

# 5. Roll-up grain. scr_all_surgery is keyed on SURGERY_ID and scr_all_complications on
#    (CARE_ID, SURGERY_ID); BIvwTreatmentSurgery LEFT JOINs both, so a duplicate in
#    either multiplies the 68-column product. This is the check that catches Somerset's
#    duplicated UpperGI complications branch when fix_somerset_defects=false.
for short, keys in [("scr_all_surgery", "surgery_id"), ("scr_all_complications", "care_id, surgery_id")]:
    if short not in built:
        continue
    row = spark.sql(
        f"SELECT count(*) AS n, count(DISTINCT {keys}) AS d FROM {TARGET_SCHEMA}.{short}"
    ).collect()[0]
    excess = row["n"] - row["d"]
    record(
        f"{short}_grain",
        "PASS" if excess == 0 else ("EXPECTED" if not FIX_SOMERSET_DEFECTS else "FAIL"),
        f"{row['n']} rows / {row['d']} distinct ({keys}); {excess} duplicate keys"
        + ("" if excess == 0 else " — Somerset's duplicated BIvwUpperGIComplications branch"),
    )
    if excess and FIX_SOMERSET_DEFECTS:
        raise AssertionError(
            f"{short} has {excess} duplicate keys with fix_somerset_defects=true. "
            "The de-duplication of Somerset's roll-up did not take effect; "
            "scr_treatment_surgery would be inflated."
        )

# 6. Surgery extension rows with no tblMAIN_SURGERY parent. Somerset's Haematology
#    branch is written as a nested RIGHT OUTER chain that would preserve such orphans;
#    we use INNER JOIN throughout. If this is zero the two are equivalent.
orphans = []
for ext in SURGERY_EXTENSION_TABLES:
    n = scalar(
        f"""SELECT count(*) FROM {SRC}.{ext} x
LEFT JOIN {SRC}.scr_tblmain_surgery ms ON ms.SURGERY_ID = x.SURGERY_ID
WHERE ms.SURGERY_ID IS NULL"""
    )
    if n:
        orphans.append(f"{ext}: {n}")
record(
    "surgery_extension_orphans",
    "PASS" if not orphans else "REVIEW",
    "every surgery-extension row has a tblMAIN_SURGERY parent, so using INNER JOIN "
    "where Somerset used a nested RIGHT OUTER chain is lossless"
    if not orphans
    else f"orphaned extension rows: {orphans}",
)

# 7. Empty products.
empty = [r["target"] for r in results if r["status"] == "OK" and r["row_count"] == 0]
record(
    "no_empty_products",
    "PASS" if not empty else "WARN",
    "no empty tables" if not empty else f"empty: {empty}",
)

# 8. Row counts against the Barts base-table figures recorded at landing.
drift = []
for short, r in built.items():
    expected = EXPECTED_BASE.get(short)
    if expected and r["row_count"] > expected:
        drift.append(f"{short}: {r['row_count']} > base {expected} (row fan-out?)")
record(
    "no_unexpected_fanout",
    "PASS" if not drift else "WARN",
    "no product exceeds its base-table row count"
    if not drift
    else f"{drift} — base figures are the 2026-09-03 landing snapshot and will drift "
    "as the weekly feed grows; investigate only if the excess is large",
)

# 9. What the fixes actually changed. Quantified, not asserted — this is the evidence
#    for the fix_somerset_defects default, and it belongs in the run log where anyone
#    reviewing the product can see it.
fix_impact = {}
if "scr_all_surgery" in built:
    fix_impact["all_surgery_sarcoma_rows"] = scalar(
        f"SELECT count(*) FROM {TARGET_SCHEMA}.scr_all_surgery WHERE somerset_branch = 'Sarcoma'"
    )
if "scr_all_complications" in built:
    fix_impact["complications_brain_rows"] = scalar(
        f"SELECT count(*) FROM {TARGET_SCHEMA}.scr_all_complications WHERE somerset_branch = 'Brain'"
    )
    fix_impact["complications_upper_gi_rows"] = scalar(
        f"SELECT count(*) FROM {TARGET_SCHEMA}.scr_all_complications WHERE somerset_branch = 'Upper GI'"
    )
if "scr_pathology" in built:
    fix_impact["pathology_empty_staging_nulled"] = scalar(
        f"SELECT count(*) FROM {TARGET_SCHEMA}.scr_pathology WHERE pathological_staging IS NULL"
    )
if "scr_referrals" in built:
    fix_impact["referrals_with_symptoms"] = scalar(
        f"""SELECT count(*) FROM {TARGET_SCHEMA}.scr_referrals
WHERE nice_referring_symptoms IS NOT NULL OR referring_symptoms IS NOT NULL"""
    )
    fix_impact["referrals_full_name_present"] = scalar(
        f"SELECT count(*) FROM {TARGET_SCHEMA}.scr_referrals WHERE full_name IS NOT NULL"
    )
record("fix_impact", "INFO", str(fix_impact))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run log

# COMMAND ----------

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("logic_version", StringType(), True),
    StructField("somerset_release", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("target_schema", StringType(), True),
    StructField("source_view", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("sources", ArrayType(StringType()), True),
    StructField("fix_somerset_defects", StringType(), True),
    StructField("error", StringType(), True),
])

log_rows = [
    (
        RUN_ID, SCR_VIEW_LOGIC_VERSION, SOMERSET_RELEASE, SRC, TARGET_SCHEMA,
        r["view"], r["target"], r["status"],
        None if r["row_count"] is None else int(r["row_count"]),
        int(r["column_count"]), r["sources"],
        str(FIX_SOMERSET_DEFECTS).lower(), r["error"],
    )
    for r in results
]

# Explicit StructType: the happy path leaves `error` None on every row, and schema
# inference rejects an all-None column with CANNOT_DETERMINE_TYPE.
(
    spark.createDataFrame(log_rows, LOG_SCHEMA)
    .withColumn("logged_at", F.current_timestamp())
    .write.mode("append")
    .option("mergeSchema", "false")
    .saveAsTable(T_LOG)
)
print(f"run log appended to {T_LOG}")

# COMMAND ----------

summary = {
    "step": "scr_view_pipeline",
    "run_id": RUN_ID,
    "logic_version": SCR_VIEW_LOGIC_VERSION,
    "somerset_release": SOMERSET_RELEASE,
    "source_schema": SRC,
    "target_schema": TARGET_SCHEMA,
    "control_schema": CONTROL_SCHEMA,
    "materialisation": "delta_table_full_snapshot",
    "fix_somerset_defects": FIX_SOMERSET_DEFECTS,
    "ig_tags_applied": APPLY_IG_TAGS,
    "oldest_source_write": SOURCE_OLDEST_WRITE,
    "views_built": len(built),
    "views_failed": len(failed),
    "row_counts": {r["target"]: r["row_count"] for r in results},
    "checks": checks,
    "divergences": DIVERGENCES,
    "unresolved_source_questions": UNRESOLVED_SOURCE_QUESTIONS,
    "lookups_not_landed": KNOWN_MISSING_LOOKUPS,
    "follow_ups": [
        "Register the 5 lookups in KNOWN_MISSING_LOOKUPS so the *_code columns they "
        "block (gynaecology incision, head and neck neck-procedure and reconstruction, "
        "CNS excision type and tumour location) can be decoded.",
        "Every table published here needs a silver consumer — zero consumption is a "
        "defect, not a neutral outcome.",
        "scr_follow_up has 8 rows at Barts and scr_branch 1 row; carry both caveats to "
        "any consumer before they build a cohort on either.",
        "Resolve the BIvwPresentation left/right breast question in "
        "UNRESOLVED_SOURCE_QUESTIONS against a known case before anyone uses those two "
        "columns for laterality.",
    ],
}
print(bronze_json(summary))
dbutils.notebook.exit(bronze_json(summary))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying this notebook
# MAGIC
# MAGIC The live weekly orchestrator is job `622989700577569`, `Bronze_Pipeline_Parallel`: a
# MAGIC DAG of ~61 tasks on pooled DBR 18.x Photon job clusters. It has no schedule of its own
# MAGIC and is triggered by parent job `1008776761501829`.
# MAGIC
# MAGIC The `_BRONZE_STEPS` list inside the `bronze_pipeline` notebook is the superseded serial
# MAGIC orchestrator, still carrying a `map_pipeline` dependency the job stopped using on
# MAGIC 2026-08-22. Registering there would change nothing about what runs.
# MAGIC
# MAGIC `CC/SCR/scr_deploy_to_bronze` does the deployment: it copies this notebook to
# MAGIC `/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/scr_view_pipeline`, adds or updates the
# MAGIC task below, and deletes the notebooks this one replaced. Both of those touch production,
# MAGIC so that notebook is run by hand, by you.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "scr_view_pipeline",
# MAGIC   "depends_on": [],
# MAGIC   "run_if": "ALL_SUCCESS",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/Workspace/Shared/ADC-DB/Prod/Pipelines/Bronze/scr_view_pipeline",
# MAGIC     "source": "WORKSPACE",
# MAGIC     "base_parameters": {
# MAGIC       "pipeline_run_id": "{{job.run_id}}",
# MAGIC       "target_schema": "{{job.parameters.target_schema}}",
# MAGIC       "allow_production_write": "true",
# MAGIC       "scr_source_schema": "4_prod.ancil_scr",
# MAGIC       "fix_somerset_defects": "true",
# MAGIC       "apply_ig_tags": "true",
# MAGIC       "continue_on_error": "false"
# MAGIC     }
# MAGIC   },
# MAGIC   "job_cluster_key": "bronze_pool_general",
# MAGIC   "timeout_seconds": 18000
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Why each field is what it is**
# MAGIC
# MAGIC | Field | Reason |
# MAGIC | --- | --- |
# MAGIC | `depends_on: []` | The sources are `4_prod.ancil_scr`, landed by ADF/IncrUpdtV2, not by any bronze task. It is a root task, starting at t=0 alongside the other roots. At about 15 minutes against a 2 to 6 hour run it adds nothing to the critical path. |
# MAGIC | `bronze_pool_general` | The lane the other ancillary feeds use (`cancer_pipeline`, `endobase_pipeline`, `slam_finance_pipeline`, `registry_pipeline`); autoscale 1 to 3 on pool `0827-140343-heart49-pool-n67z9ajy`. The workload is sixteen modest CTAS statements, so the map lanes and the GPU lane would both be wrong. |
# MAGIC | `timeout_seconds: 18000` | Matches the general-lane peers. Roughly twenty times the observed 874 second dev run. |
# MAGIC | `continue_on_error: "false"` | A failed table fails the task, so the job surfaces it rather than publishing a partial product. Use `only_targets` for the surgical re-run afterwards. |
# MAGIC | no `force_full_refresh` | Peers pass it; this notebook has no incremental mode to switch. Every run is an unconditional `CREATE OR REPLACE` of all sixteen tables, so passing an ignored parameter would imply a behaviour that does not exist. |
# MAGIC
# MAGIC Step-local widgets (`scr_source_schema`, `fix_somerset_defects`, `apply_ig_tags`,
# MAGIC `continue_on_error`, `only_targets`) are registered inside this notebook rather than as
# MAGIC job parameters, so they do not widen the job-wide contract. They are passed as literals
# MAGIC in `base_parameters`, the same way `allergy_pipeline` passes `full_reconciliation` and
# MAGIC `bootstrap_mode`.
# MAGIC
# MAGIC **What lands where in production.** `target_schema` resolves to `4_prod.bronze`, so
# MAGIC `bronze_control_schema()` puts the two control tables in `6_mgmt.bronze`:
# MAGIC `scr_view_pipeline_log` (one row per target per run) and `scr_view_pipeline_sql` (the
# MAGIC generated statement for every target, written before it executes, so a build that kills
# MAGIC the driver still leaves the failing statement behind). In dev both sit beside the data
# MAGIC in the target schema.

