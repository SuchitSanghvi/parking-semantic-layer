"""
db.py — DuckDB connection helper.

Connects read-only to the dbt-built warehouse at
dbt_project/warehouse.duckdb (relative to project root).
"""

import os
import duckdb
import pandas as pd
import streamlit as st

# Project root = two levels up from this file (app/utils/db.py)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WAREHOUSE_PATH = os.path.join(_PROJECT_ROOT, "dbt_project", "warehouse.duckdb")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a cached read-only DuckDB connection to the warehouse."""
    if not os.path.exists(WAREHOUSE_PATH):
        raise FileNotFoundError(
            f"Warehouse not found at {WAREHOUSE_PATH}. "
            "Run `dbt seed && dbt run` inside dbt_project/ first."
        )
    return duckdb.connect(WAREHOUSE_PATH, read_only=True)


def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL string and return results as a DataFrame."""
    conn = get_connection()
    return conn.execute(sql).df()
