# Databricks notebook source
displayHTML("""
<h1>DQ4 Full Assessment Battery</h1>
<p>This folder reruns the accepted DQ4 statistical/clinical plausibility assessment against Journey silver and the post-O4 OMOP CDM.</p>
<h2>Run</h2>
<ol>
  <li>Attach <b>01_RUN_FULL_BATTERY</b> to a general-purpose cluster with enough memory for the OMOP measurement and fact_relationship domains.</li>
  <li>Leave the suffix blank for timestamped run IDs, or provide an alphanumeric/underscore suffix.</li>
  <li>Run all. The launcher records independent silver and OMOP source-pin brackets and stops if either source pipeline is active.</li>
  <li>Open <b>90_RESULTS</b> after completion. Blank widgets select the latest valid silver and OMOP runs.</li>
</ol>
<h2>Scope</h2>
<ul>
  <li>Silver: numeric profiles, range/outlier/sentinel/digit/unit/feed/trend checks, contradiction, near-duplicate, MCE age/sex, gender conflict, and PowerForm reconciliation.</li>
  <li>OMOP: DQD 2.8.9, Achilles, Achilles Heel 1.6.3, Themis, issueification, and evidence.</li>
</ul>
<h2>Safety and reproducibility</h2>
<ul>
  <li>Writes are restricted to 6_mgmt.silver_qc and 6_mgmt.silver_qc_tmp.</li>
  <li>No production table or pipeline is modified.</li>
  <li>Every SQL statement is intent-logged in dq_exec_log.</li>
  <li>Every run ID must be new. A failed run is retained as an invalid audit record; retry with a new suffix.</li>
  <li>The battery is frozen to the accepted 25 August 2026 DQ4 specification. Routine runs recompute measurements but do not silently retune clinical fences or tool SQL. Recalibration remains a repository-controlled change.</li>
</ul>
<p>The full run scans multi-billion-row domains and is expected to take several hours. Run <b>10_RUN_SILVER_BATTERY</b> or <b>20_RUN_OMOP_BATTERY</b> independently when only one half is required.</p>
""")

