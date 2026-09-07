# Databricks notebook source
# Bronze-owned pathology polarity/negation correction task.
dbutils.widgets.text("MAP_TABLE",     "3_lookup.omop.pathology_result_concept_map")
dbutils.widgets.text("FUSION_LOOKUP", "3_lookup.omop.pathology_result_fusion_lookup")
dbutils.widgets.text("ANCHORS",       "3_lookup.omop.concept_result_anchors")
dbutils.widgets.text("GATE_TABLE",    "3_lookup.omop.pathology_polarity_gate")
dbutils.widgets.text("WORKLIST",      "3_lookup.omop._polgate_worklist")
dbutils.widgets.text("MODEL_DIR",     "/Volumes/4_prod/models/models/DeBERTa-v3-large-mnli-fever-anli-ling-wanli")
dbutils.widgets.text("MODEL_VERSION", "deberta-v3-large-mnli-fever-anli-ling-wanli")
dbutils.widgets.text("GATE_VERSION",  "1")
dbutils.widgets.text("CONF_MIN",       "0.50")   # winning side must reach this entailment (scorer)
dbutils.widgets.text("DISAGREE_MARGIN","0.15")   # opposite side must beat assigned side by this to veto (scorer)
dbutils.widgets.text("VETO_MARGIN",    "0.15")   # derive: opposite pole beats assigned by >= this -> veto
dbutils.widgets.text("AGREE_MARGIN",   "0.15")   # derive: assigned pole confirmed by >= this -> agree
dbutils.widgets.text("CORRECTION_APPLY","true") # Step 4: false = DRY-RUN (count only). The SCHEDULED job sets true.
dbutils.widgets.text("MAX_SCORE_ROWS", "5000")   # bounded, resumable GPU batch per Bronze run
MAP_TABLE     = dbutils.widgets.get("MAP_TABLE").strip()
FUSION_LOOKUP = dbutils.widgets.get("FUSION_LOOKUP").strip()
ANCHORS       = dbutils.widgets.get("ANCHORS").strip()
GATE_TABLE    = dbutils.widgets.get("GATE_TABLE").strip()
WORKLIST      = dbutils.widgets.get("WORKLIST").strip()
MODEL_DIR     = dbutils.widgets.get("MODEL_DIR").strip()
MODEL_VERSION = dbutils.widgets.get("MODEL_VERSION").strip()
GATE_VERSION  = int(dbutils.widgets.get("GATE_VERSION"))
CONF_MIN        = float(dbutils.widgets.get("CONF_MIN"))
DISAGREE_MARGIN = float(dbutils.widgets.get("DISAGREE_MARGIN"))
VETO_MARGIN     = float(dbutils.widgets.get("VETO_MARGIN"))
AGREE_MARGIN    = float(dbutils.widgets.get("AGREE_MARGIN"))
CORRECTION_APPLY = dbutils.widgets.get("CORRECTION_APPLY").strip().lower() == "true"
MAX_SCORE_ROWS = max(1, int(dbutils.widgets.get("MAX_SCORE_ROWS")))
print(f"MODEL_DIR={MODEL_DIR}  GATE_TABLE={GATE_TABLE}  model_version={MODEL_VERSION} gate_version={GATE_VERSION}")
print(f"CORRECTION_APPLY={CORRECTION_APPLY}  (false => Step 4 dry-run/count-only)")

# COMMAND ----------

# ── 1. CACHE (FULL pole-probe schema, idempotent) + WORKLIST (prod-safe) ─────────────────────────────────
# Cache accumulates (FIX-1): created here with the full schema so the scorer MERGEs in place (never drops).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GATE_TABLE} (
  result_normalized STRING, embedding_anchor STRING, embedding_pole STRING,
  nli_pos FLOAT, nli_neg FLOAT, nli_pole STRING, verdict STRING,
  model_version STRING, gate_version INT, scored_at TIMESTAMP
) USING DELTA
""")

# Worklist (FIX-2): UNION of anchor-pole emits from the surfaces the veto is applied to, reduced to distinct
# (result_normalized, embedding_pole), anti-joined against the cache. anchor_label is provenance only
# (non-unique), NOT a key.
#   emit_map    — the live concept_map (pole via anchors). ALWAYS present.
#   emit_fusion — the fusion lookup (pole = anchor_polarity). Present ONLY during a dev/human-gated bulk
#                 rebuild; the weekly prod pipeline has no fusion lookup, so it is UNIONed only when it exists.
_has_fusion = spark.catalog.tableExists(FUSION_LOOKUP)
_emit_fusion_cte = (f"""
emit_fusion AS (
  SELECT result_normalized, anchor_polarity AS embedding_pole, anchor_label
  FROM {FUSION_LOOKUP}
  WHERE route='anchor' AND final_tier<>'unmapped'
    AND anchor_polarity IN ('positive','negative') AND result_normalized IS NOT NULL
),""" if _has_fusion else "")
_emit_union = ("SELECT * FROM emit_fusion UNION ALL SELECT * FROM emit_map"
               if _has_fusion else "SELECT * FROM emit_map")
print(f"fusion lookup {'PRESENT -> UNION emit_fusion + emit_map' if _has_fusion else 'ABSENT -> emit_map only (weekly prod path)'}")

spark.sql(f"""
CREATE OR REPLACE TABLE {WORKLIST} AS
WITH ap AS (
  SELECT concept_id, polarity_class, anchor_label FROM (
    SELECT snomed_concept_id AS concept_id, polarity_class, anchor_label FROM {ANCHORS}
      WHERE is_active AND polarity_class IN ('positive','negative') AND snomed_concept_id IS NOT NULL
    UNION
    SELECT loinc_concept_id AS concept_id, polarity_class, anchor_label FROM {ANCHORS}
      WHERE is_active AND polarity_class IN ('positive','negative') AND loinc_concept_id IS NOT NULL
  )
),
{_emit_fusion_cte}
emit_map AS (
  SELECT m.result_normalized, ap.polarity_class AS embedding_pole, ap.anchor_label
  FROM {MAP_TABLE} m JOIN ap ON m.value_as_concept_id = ap.concept_id
  WHERE m.value_as_concept_id IS NOT NULL AND m.confidence_tier <> 'unmapped'
    AND m.result_normalized IS NOT NULL
),
emitted AS (
  SELECT result_normalized, embedding_pole, MIN(anchor_label) AS anchor_label
  FROM ({_emit_union})
  GROUP BY result_normalized, embedding_pole
)
SELECT e.result_normalized, e.anchor_label, e.embedding_pole,
       CAST({MODEL_VERSION!r} AS STRING) AS model_version,
       CAST({GATE_VERSION} AS INT)       AS gate_version
FROM emitted e
LEFT ANTI JOIN {GATE_TABLE} g
  ON  e.result_normalized = g.result_normalized
  AND e.embedding_pole    = g.embedding_pole
  AND g.model_version     = {MODEL_VERSION!r}
  AND g.gate_version      = {GATE_VERSION}
""")
n_work = spark.table(WORKLIST).count()
print(f"WORKLIST to score: {n_work:,}")

# COMMAND ----------

# ── 2. DeBERTa NLI POLE-PROBE scoring (GPU) -> MERGE into the accumulating cache ─────────────────────────
# Probe each phrase against canonical positive AND negative pole anchors (max-entail per side, both
# orderings); the phrase's INDEPENDENT NLI pole is whichever side wins. A single-anchor contradiction test
# misses the dangerous inversions (they read NLI-neutral vs the assigned anchor), so we compare poles.
if n_work == 0:
    print("worklist empty -> NO_OP (nothing new to score this cycle; gate fails open on unscored phrases)")
else:
    import torch, numpy as np, time
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, FloatType

    POS_PROBES = ["detected","positive","present","isolated","reactive","growth","seen"]
    NEG_PROBES = ["not detected","negative","absent","not isolated","non-reactive","no growth","not seen"]

    work = (spark.table(WORKLIST)
            .orderBy("result_normalized", "embedding_pole")
            .limit(MAX_SCORE_ROWS)
            .toPandas())
    print(f"bounded GPU batch: {len(work):,} of {n_work:,}; remaining after success: {max(0, n_work-len(work)):,}")
    DEV = "cuda" if torch.cuda.is_available() else "cpu"
    tok = AutoTokenizer.from_pretrained(MODEL_DIR)
    mdl = AutoModelForSequenceClassification.from_pretrained(MODEL_DIR, torch_dtype=torch.float32).to(DEV).eval()
    id2 = {int(k): v.lower() for k, v in mdl.config.id2label.items()}
    ENT = {v: k for k, v in id2.items()}["entailment"]
    print(f"device={DEV} resolved ENT={ENT} id2label={id2}")

    @torch.no_grad()
    def entail_scores(prem, hyp, bs=192):
        out = []
        for s in range(0, len(prem), bs):
            enc = tok(prem[s:s+bs], hyp[s:s+bs], return_tensors="pt", truncation=True,
                      max_length=64, padding=True).to(DEV)
            out.append(torch.softmax(mdl(**enc).logits, dim=-1).cpu().numpy()[:, ENT])
        return np.concatenate(out)

    def tmpl(x): return f"The result is {x}."
    phrases  = work["result_normalized"].tolist()
    phrase_t = [tmpl(p) for p in phrases]
    N = len(phrases)

    def max_entail_over(probes):
        best = np.zeros(N, dtype=np.float32)
        for pr in probes:
            h = [tmpl(pr)] * N
            best = np.maximum(best, 0.5*(entail_scores(phrase_t, h) + entail_scores(h, phrase_t)))
        return best

    t0 = time.time()
    nli_pos = max_entail_over(POS_PROBES)
    nli_neg = max_entail_over(NEG_PROBES)
    print(f"scored {N:,} phrases vs {len(POS_PROBES)}+{len(NEG_PROBES)} probes in {time.time()-t0:.0f}s")

    rows = []
    model_version = str(work["model_version"].iloc[0]); gate_version = int(work["gate_version"].iloc[0])
    for i in range(N):
        p, ng = float(nli_pos[i]), float(nli_neg[i])
        pole = work["embedding_pole"].iloc[i]
        nli_pole = "positive" if p > ng else "negative"
        if pole == "positive":
            veto  = (ng >= CONF_MIN and ng - p >= DISAGREE_MARGIN)
            agree = (p >= CONF_MIN and p >= ng)
        else:  # negative
            veto  = (p >= CONF_MIN and p - ng >= DISAGREE_MARGIN)
            agree = (ng >= CONF_MIN and ng >= p)
        verdict = "veto" if veto else ("agree" if agree else "abstain")
        r = work.iloc[i]
        rows.append((r["result_normalized"], r["anchor_label"], pole, p, ng, nli_pole,
                     verdict, model_version, gate_version))

    SCHEMA = StructType([
        StructField("result_normalized", StringType()), StructField("embedding_anchor", StringType()),
        StructField("embedding_pole", StringType()), StructField("nli_pos", FloatType()),
        StructField("nli_neg", FloatType()), StructField("nli_pole", StringType()),
        StructField("verdict", StringType()), StructField("model_version", StringType()),
        StructField("gate_version", StringType()),
    ])
    stg = (spark.createDataFrame(rows, SCHEMA)
           .withColumn("gate_version", F.col("gate_version").cast("int"))
           .withColumn("scored_at", F.current_timestamp()))
    stg.createOrReplaceTempView("polgate_stg")
    # FIX-1: MERGE in place (cache created with full schema above) so prior verdicts accumulate.
    spark.sql(f"""
    MERGE INTO {GATE_TABLE} t USING polgate_stg s
    ON t.result_normalized=s.result_normalized AND t.embedding_anchor=s.embedding_anchor
    AND t.embedding_pole=s.embedding_pole AND t.model_version=s.model_version AND t.gate_version=s.gate_version
    WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)
    print("scored-batch verdicts:")
    stg.groupBy("verdict").count().show()

# COMMAND ----------

# ── 3. DERIVE verdicts from the cached raw scores (retunable margins; no GPU needed) ────────────────────
# Lets the veto/agree thresholds be tuned independently of the scorer's CONF_MIN/DISAGREE_MARGIN. Margin-based:
#   veto = opposite pole beats the assigned pole by >= VETO_MARGIN; agree = assigned confirmed by >= AGREE_MARGIN.
spark.sql(f"""
MERGE INTO {GATE_TABLE} t
USING (
  SELECT result_normalized, embedding_anchor, embedding_pole, model_version, gate_version,
    CASE
      WHEN embedding_pole='positive' AND (nli_neg - nli_pos) >= {VETO_MARGIN} THEN 'veto'
      WHEN embedding_pole='negative' AND (nli_pos - nli_neg) >= {VETO_MARGIN} THEN 'veto'
      WHEN embedding_pole='positive' AND (nli_pos - nli_neg) >= {AGREE_MARGIN} THEN 'agree'
      WHEN embedding_pole='negative' AND (nli_neg - nli_pos) >= {AGREE_MARGIN} THEN 'agree'
      ELSE 'abstain' END AS new_verdict
  FROM {GATE_TABLE} WHERE model_version={MODEL_VERSION!r} AND gate_version={GATE_VERSION}
) s
ON t.result_normalized=s.result_normalized AND t.embedding_anchor=s.embedding_anchor
AND t.embedding_pole=s.embedding_pole AND t.model_version=s.model_version AND t.gate_version=s.gate_version
WHEN MATCHED THEN UPDATE SET t.verdict = s.new_verdict, t.scored_at = current_timestamp()
""")

print("=== scoring complete — cache verdict distribution ===")
spark.sql(f"SELECT verdict, embedding_pole, count(*) n FROM {GATE_TABLE} GROUP BY verdict, embedding_pole ORDER BY verdict, embedding_pole").show()
print("cache rows:", spark.table(GATE_TABLE).count())

# COMMAND ----------

# ── 4. CORRECTION (was polgate_03_correction) — retro-downgrade live map rows the gate now vetoes ────────
# Reads the SAME cache Steps 1-3 just updated. A concept_map row is corrected iff its result_normalized has a
# gate verdict='veto' for the SAME pole the row emitted (pole inferred from the anchor concept it points to via
# concept_result_anchors). Only pos/neg anchor emits eligible; unmapped rows untouched. In-place MERGE
# (BBV-patch pattern) — does NOT re-run the bulk pipeline. Idempotent + downgrade-only. Serverless-OK, but runs
# here on the GPU task so score+correct are ONE atomic schedule step (no cross-task cache staleness).

# anchor concept_id -> polarity_class (both snomed & loinc ids map to the same pole)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _anchor_pole AS
SELECT concept_id, polarity_class FROM (
  SELECT snomed_concept_id AS concept_id, polarity_class FROM {ANCHORS}
    WHERE is_active AND polarity_class IN ('positive','negative') AND snomed_concept_id IS NOT NULL
  UNION
  SELECT loinc_concept_id AS concept_id, polarity_class FROM {ANCHORS}
    WHERE is_active AND polarity_class IN ('positive','negative') AND loinc_concept_id IS NOT NULL
) GROUP BY concept_id, polarity_class
""")

# candidate corrections: mapped row whose emitted-concept pole == a vetoed pole for that phrase
# FIX-3: key on the FULL 4-tuple incl. description (result-map grain) — no cross-context downgrade.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _to_correct AS
SELECT DISTINCT m.code_system, m.code, m.description, m.result_normalized
FROM {MAP_TABLE} m
JOIN _anchor_pole ap ON m.value_as_concept_id = ap.concept_id
JOIN {GATE_TABLE} g
  ON g.result_normalized = m.result_normalized
 AND g.embedding_pole    = ap.polarity_class
 AND g.verdict='veto' AND g.model_version={MODEL_VERSION!r} AND g.gate_version={GATE_VERSION}
WHERE m.value_as_concept_id IS NOT NULL AND m.confidence_tier <> 'unmapped'
""")
n_corr = spark.table("_to_correct").count()
print(f"rows to correct (phrase-code-description tuples): {n_corr}")
if n_corr > 0:
    spark.sql("SELECT * FROM _to_correct LIMIT 20").show(truncate=False)

if not CORRECTION_APPLY:
    print("CORRECTION_APPLY=false -> Step 4 dry-run (count only, no writes). The scheduled job sets CORRECTION_APPLY=true.")
else:
    # FIX-3: MERGE on the full 4-tuple incl. description (IS NOT DISTINCT FROM — description can be NULL).
    spark.sql(f"""
    MERGE INTO {MAP_TABLE} t
    USING _to_correct s
    ON t.code_system=s.code_system AND t.code=s.code
       AND t.description IS NOT DISTINCT FROM s.description
       AND t.result_normalized=s.result_normalized
    WHEN MATCHED THEN UPDATE SET
      t.value_as_concept_id = NULL,
      t.concept_name        = NULL,
      t.confidence_tier     = 'unmapped',
      t.result_mapping_source = 'polarity_veto',
      t.ADC_UPDT            = current_timestamp()
    """)
    print(f"CORRECTION_OK corrected {n_corr} phrase-code-description tuples in place")
