"""
03_what_can_i_ask.py — Metrics Catalog

Dynamically parses dbt_project/models/marts/marts.yml to display
all available metrics, their definitions, dimensions, and example questions.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
import pandas as pd
import streamlit as st

st.set_page_config(page_title="What Can I Ask?", layout="wide")
st.title("What Can I Ask?")
st.markdown(
    "Below are all metrics available in the semantic layer, their definitions, "
    "and the dimensions you can slice them by."
)

# ── Parse marts.yml ───────────────────────────────────────────────────────────
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_MARTS_YML = os.path.join(_PROJECT_ROOT, "dbt_project", "models", "marts", "marts.yml")

@st.cache_data
def load_metrics_catalog() -> pd.DataFrame:
    with open(_MARTS_YML) as f:
        yml = yaml.safe_load(f)

    # Collect all dimension names from all semantic models
    all_dims: set[str] = set()
    for sm in yml.get("semantic_models", []):
        for dim in sm.get("dimensions", []):
            prefix = "session" if sm["name"] == "fct_sessions" else "lot"
            all_dims.add(f"{prefix}__{dim['name']}")

    # Example questions per metric (hand-written, not parsed — kept in sync by design)
    _examples = {
        "total_revenue": "What is total revenue by city this month?",
        "sessions_count": "How many sessions happened on Fridays?",
        "avg_session_duration": "What is avg session duration by market type?",
        "avg_revenue_per_session": "Which lot has the highest avg revenue per session?",
        "dynamic_pricing_lift": "Which lots show the highest weekend pricing lift?",
    }

    rows = []
    for metric in yml.get("metrics", []):
        name = metric["name"]
        description = metric.get("description", "").strip()

        # Determine valid dimensions: ratio metrics referencing is_weekend
        # have fewer valid dims — detect from filters in type_params
        if metric.get("type") == "ratio":
            # Check if numerator/denominator have is_weekend filter
            tp = metric.get("type_params", {})
            has_weekend_filter = any(
                "is_weekend" in str(tp.get(side, {}).get("filter", ""))
                for side in ("numerator", "denominator")
            )
            if has_weekend_filter:
                valid_dims = sorted(d for d in all_dims if "is_weekend" not in d)
            else:
                valid_dims = sorted(all_dims)
        else:
            valid_dims = sorted(all_dims)

        rows.append({
            "Metric": name,
            "Definition": description,
            "Available Dimensions": ", ".join(valid_dims),
            "Example Question": _examples.get(name, ""),
        })

    return pd.DataFrame(rows)


try:
    catalog_df = load_metrics_catalog()
    st.dataframe(
        catalog_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Metric": st.column_config.TextColumn(width="small"),
            "Definition": st.column_config.TextColumn(width="medium"),
            "Available Dimensions": st.column_config.TextColumn(width="large"),
            "Example Question": st.column_config.TextColumn(width="medium"),
        },
    )
except FileNotFoundError:
    st.error(f"marts.yml not found at {_MARTS_YML}. Make sure the dbt project is set up.")
except Exception as e:
    st.error(f"Error loading metrics catalog: {e}")

st.divider()
st.caption(
    "Questions outside these metrics will return a helpful error explaining "
    "what IS available that's closest to what you asked."
)
