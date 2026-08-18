"""Register the pathology Python modules with Spark Connect workers."""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path
import sys
import uuid


RUNTIME_MODULES = (
    "pathology_contracts.py",
    "pathology_rules.py",
    "pathology_pipeline.py",
    "pathology_audits.py",
    "pathology_genetics.py",
    "pathology_indications.py",
    "pathology_amr.py",
    "pathology_incremental.py",
    "pathology_validation.py",
)


def register_runtime_artifact(spark, module_directory: str | Path | None = None) -> str:
    """Register checked-in modules for both Spark Connect and Python workers.

    ``addArtifact`` is the normal Spark Connect path.  Registering already-imported
    modules by value is a second, deterministic guard for managed runtimes whose
    reused Python workers do not immediately expose a newly-added zip on
    ``sys.path``.
    """

    root = Path(module_directory) if module_directory else Path(__file__).resolve().parent
    missing = [name for name in RUNTIME_MODULES if not (root / name).exists()]
    if missing:
        raise FileNotFoundError(f"Pathology runtime modules missing from {root}: {missing}")
    # Spark Connect rejects re-registering the same local artifact URI in some
    # reused notebook sessions, even when the payload is unchanged.
    artifact = Path(tempfile.gettempdir()) / (
        f"pathology_expansion_runtime_{uuid.uuid4().hex}.zip"
    )
    with zipfile.ZipFile(artifact, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in RUNTIME_MODULES:
            archive.write(root / name, arcname=name)
    # Spark Connect uses addArtifact; classic clusters expose addPyFile instead.
    # Keep the same zip usable by both execution modes because the dev build is
    # deliberately benchmarked on classic Bronze compute.
    try:
        spark.addArtifact(str(artifact), pyfile=True)
    except Exception as add_artifact_error:
        try:
            spark.sparkContext.addPyFile(str(artifact))
        except Exception:
            raise add_artifact_error
    try:
        from pyspark import cloudpickle

        for filename in RUNTIME_MODULES:
            module = sys.modules.get(Path(filename).stem)
            if module is not None:
                cloudpickle.register_pickle_by_value(module)
    except (ImportError, AttributeError):
        # Local contract tests intentionally run without PySpark.  The artifact is
        # still valid and Databricks runtimes that lack this API use addArtifact.
        pass
    return str(artifact)
