"""INSR R4 job gate (design R4 "Release boundary"; Deviation D7): pure rewrite of a job's settings so it skips while
a release is open. A first notebook task checks the marker and sets the task value release_open; a condition task
lets the original root tasks run only when it is "false"; skipped tasks show as Excluded, the run succeeds, and the
check is logged (release_gate_log). The job gains a release_context parameter (Silver_Domains passes silver_domains
to OMOP). Idempotent; the pre-install settings are snapshotted by the caller for rollback.
"""
import copy

GATE_TASK = "r4_release_gate"
CHECK_TASK = "r4_release_open_check"
COMPUTE_KEYS = ("job_cluster_key", "existing_cluster_id", "environment_key")


def wrapped(settings):
    """True when the gate is already installed."""
    return any(t["task_key"] == GATE_TASK for t in settings.get("tasks", []))


def wrap(settings, notebook_path, schema, context_route="job_parameter"):
    """New settings with the gate in front of every root task (unchanged when already wrapped).

    context_route (release/caller_audit.json, R4.13 Step 7b):
      "job_parameter"   the job gains a job-level release_context (callers must use job_parameters);
      "base_parameters" no job parameter is added: the gate task's base_parameters carry release_context = "",
                        which a caller's legacy notebook_params override (keeps callers that use notebook_params)."""
    assert context_route in ("job_parameter", "base_parameters"), context_route
    if wrapped(settings):
        return settings
    out = copy.deepcopy(settings)
    tasks = out["tasks"]
    roots = [t for t in tasks if not t.get("depends_on")]
    assert roots, "a job without a root task cannot be gated"
    compute = {k: roots[0][k] for k in COMPUTE_KEYS if k in roots[0]}
    context = ""
    if context_route == "job_parameter":
        params = out.setdefault("parameters", [])
        if not any(p["name"] == "release_context" for p in params):
            params.append({"name": "release_context", "default": ""})
        context = "{{job.parameters.release_context}}"
    gate = {"task_key": GATE_TASK, **compute,
            "notebook_task": {"notebook_path": notebook_path, "base_parameters": {
                "action": "check", "schema": schema, "release_context": context,
                "consumer": "{{job.id}}", "run_id": "{{job.run_id}}"}}}
    check = {"task_key": CHECK_TASK, "depends_on": [{"task_key": GATE_TASK}],
             "condition_task": {"op": "EQUAL_TO", "left": "{{tasks." + GATE_TASK + ".values.release_open}}", "right": "false"}}
    for t in roots:
        t["depends_on"] = [{"task_key": CHECK_TASK, "outcome": "true"}]
    out["tasks"] = [gate, check] + tasks
    return out
