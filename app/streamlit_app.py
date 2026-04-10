"""
streamlit_app.py — Parking Portfolio Intelligence
Main entrypoint. Builds the DuckDB warehouse on first load if needed.

Run from project root:
  streamlit run app/streamlit_app.py
"""

import os
import subprocess
import sys
import streamlit as st

# ── Paths ─────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DBT_PROJECT_DIR = os.path.join(_PROJECT_ROOT, "dbt_project")
_WAREHOUSE_PATH = os.path.join(_DBT_PROJECT_DIR, "warehouse.duckdb")
_GENERATE_SCRIPT = os.path.join(_PROJECT_ROOT, "generate_data", "generate_raw_data.py")
_VENV_DBT = os.path.join(_PROJECT_ROOT, "venv", "bin", "dbt")
_VENV_PYTHON = os.path.join(_PROJECT_ROOT, "venv", "bin", "python")


def _run(cmd: list, cwd: str):
    """Run a subprocess command, streaming output to st.status."""
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)


@st.cache_resource(show_spinner=False)
def build_warehouse():
    """
    Ensure the DuckDB warehouse exists.
    On first run (no warehouse.duckdb): generates data, seeds, and runs dbt.
    Subsequent runs return immediately (cached).
    """
    if os.path.exists(_WAREHOUSE_PATH):
        return  # already built

    with st.spinner("Building data warehouse — first load only, takes about 20 seconds..."):
        # 1. Generate synthetic CSVs into data/
        _run([_VENV_PYTHON, _GENERATE_SCRIPT], cwd=_PROJECT_ROOT)

        # 2. Copy generated CSVs to seeds/ (overwrite)
        import shutil
        data_dir = os.path.join(_PROJECT_ROOT, "data")
        seeds_dir = os.path.join(_DBT_PROJECT_DIR, "seeds")
        for fname in ("raw_parking_events.csv", "raw_lots.csv", "raw_local_events.csv"):
            src = os.path.join(data_dir, fname)
            if os.path.exists(src):
                shutil.copy2(src, seeds_dir)

        # 3. dbt seed
        _run([_VENV_DBT, "seed", "--threads", "1"], cwd=_DBT_PROJECT_DIR)

        # 4. dbt run
        _run([_VENV_DBT, "run", "--threads", "1"], cwd=_DBT_PROJECT_DIR)


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Parking Portfolio Intelligence",
    page_icon="🅿️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Build warehouse (no-op after first run)
build_warehouse()

# ── Landing page ──────────────────────────────────────────────────────────────
st.title("🅿️ Parking Portfolio Intelligence")
st.markdown(
    """
    A dbt semantic layer on synthetic parking data — demonstrating how raw LPR
    camera events become trusted, AI-queryable metrics.

    **Use the sidebar to navigate:**
    - **Portfolio Dashboard** — KPI cards, revenue trends, occupancy, pricing signal
    - **Ask Your Data** — natural language queries powered by MetricFlow + Claude
    - **What Can I Ask?** — full metrics catalog with available dimensions
    """
)
