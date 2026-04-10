"""
mf.py — MetricFlow CLI wrapper.

run_metric_query(spec) calls the `mf` CLI via subprocess,
writes results to a temp CSV, and returns a DataFrame.
"""

import os
import subprocess
import tempfile
import pandas as pd

# Project root = two levels up from this file (app/utils/mf.py)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DBT_PROJECT_DIR = os.path.join(_PROJECT_ROOT, "dbt_project")

# mf binary lives in venv312 (Python 3.12 — MetricFlow requires it)
_MF_BIN = os.path.join(_PROJECT_ROOT, "venv312", "bin", "mf")


def run_metric_query(spec: dict) -> pd.DataFrame:
    """
    Execute a MetricFlow query from a spec dict and return results as DataFrame.

    spec keys:
      metric   (str, required)  — metric name
      group_by (list, optional) — dimension names
      where    (str, optional)  — filter expression
      order_by (str, optional)  — sort column
      limit    (int, optional)  — row limit (default 50)

    Raises ValueError if spec contains {"error": true, ...}.
    Raises RuntimeError if the mf CLI exits with a non-zero code.
    """
    if spec.get("error"):
        raise ValueError(spec.get("message", "Query is outside available metrics."))

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        cmd = [_MF_BIN, "query", "--metrics", spec["metric"]]

        group_by = spec.get("group_by", [])
        if group_by:
            cmd += ["--group-by", ",".join(group_by)]

        where = spec.get("where", "")
        if where:
            cmd += ["--where", where]

        order_by = spec.get("order_by", "")
        if order_by:
            cmd += ["--order", order_by]

        limit = spec.get("limit", 50)
        cmd += ["--limit", str(limit)]

        cmd += ["--csv", tmp_path]

        result = subprocess.run(
            cmd,
            cwd=_DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"MetricFlow query failed:\n{result.stderr or result.stdout}"
            )

        df = pd.read_csv(tmp_path)
        return df

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
