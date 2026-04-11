"""
mf.py — MetricFlow CLI wrapper.

run_metric_query(spec) calls the `mf` CLI via subprocess,
writes results to a temp CSV, and returns a DataFrame.

DuckDB lock strategy:
  dbt-duckdb hardcodes read_only=False so MetricFlow always wants an exclusive
  write lock. To avoid conflicting with the Streamlit process, we copy
  warehouse.duckdb to a temp file, point MetricFlow at that copy via a temp
  profiles.yml, and clean up afterwards. The copy is ~3 MB and takes < 100 ms.
"""

import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import pandas as pd

# Project root = two levels up from this file (app/utils/mf.py)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DBT_PROJECT_DIR = os.path.join(_PROJECT_ROOT, "dbt_project")
_WAREHOUSE_PATH = os.path.join(_DBT_PROJECT_DIR, "warehouse.duckdb")

# mf binary lives in venv312 (Python 3.12 — MetricFlow requires it)
_MF_BIN = os.path.join(_PROJECT_ROOT, "venv312", "bin", "mf")


def _ensure_dimension_syntax(where: str) -> str:
    """
    Auto-wrap bare dimension references missing {{ Dimension(...) }}.
    e.g. "session__is_weekend = true" → "{{ Dimension('session__is_weekend') }} = true"
    """
    return re.sub(
        r"(?<!\')(?<!\()\b([a-z]+__[a-z_]+)\b(?!\')(?!\))",
        lambda m: f"{{{{ Dimension('{m.group(1)}') }}}}",
        where,
    )


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

    # Work in a temp directory: copy warehouse + write a temp profiles.yml
    # so MetricFlow gets exclusive access to the copy without conflicting
    # with the Streamlit process that holds the original file open.
    with tempfile.TemporaryDirectory() as tmp_dir:
        # 1. Copy warehouse to temp dir
        tmp_db = os.path.join(tmp_dir, "warehouse.duckdb")
        shutil.copy2(_WAREHOUSE_PATH, tmp_db)

        # 2. Write a profiles.yml pointing at the temp copy
        profiles_yml = textwrap.dedent(f"""\
            parking_semantic_layer:
              target: dev
              outputs:
                dev:
                  type: duckdb
                  path: '{tmp_db}'
                  schema: main
                  threads: 1
        """)
        profiles_dir = os.path.join(tmp_dir, "profiles")
        os.makedirs(profiles_dir)
        with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
            f.write(profiles_yml)

        # 3. Temp CSV for results
        tmp_csv = os.path.join(tmp_dir, "result.csv")

        # 4. Build mf command
        cmd = [_MF_BIN, "query", "--metrics", spec["metric"]]

        group_by = spec.get("group_by", [])
        if group_by:
            cmd += ["--group-by", ",".join(group_by)]

        where = spec.get("where", "")
        if where:
            where = _ensure_dimension_syntax(where)
            cmd += ["--where", where]

        order_by = spec.get("order_by", "")
        if order_by:
            cmd += ["--order", order_by]

        limit = spec.get("limit", 50)
        cmd += ["--limit", str(limit)]

        cmd += ["--csv", tmp_csv]

        # 5. Run with DBT_PROFILES_DIR pointing at temp profiles
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = profiles_dir

        result = subprocess.run(
            cmd,
            cwd=_DBT_PROJECT_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"MetricFlow query failed:\n{result.stderr or result.stdout}"
            )

        # MetricFlow doesn't write the CSV when the result is 0 rows
        if not os.path.exists(tmp_csv):
            raise ValueError(
                "No data found for that query. The dataset covers Jan–Mar 2024. "
                "Try asking about a specific date range, day of week, or lot — "
                "for example: \"Which city had the highest revenue in February 2024?\""
            )

        df = pd.read_csv(tmp_csv)
        return _prettify_columns(df)


def _prettify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make MetricFlow output column names human-readable.

    MetricFlow uses entity__dimension naming (e.g. lot__market_type,
    session__day_of_week). Strip the entity prefix, replace underscores
    with spaces, and title-case everything.

    Examples:
      lot__market_type    → Market Type
      session__day_of_week → Day Of Week
      avg_session_duration → Avg Session Duration
      total_revenue        → Total Revenue
    """
    def _clean(col: str) -> str:
        if "__" in col:
            col = col.split("__", 1)[1]
        return col.replace("_", " ").title()

    return df.rename(columns=_clean)
