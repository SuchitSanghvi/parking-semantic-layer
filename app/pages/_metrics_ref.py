"""Metrics Reference tab — catalog parsed from marts.yml."""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
import streamlit as st

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_MARTS_YML    = os.path.join(_PROJECT_ROOT, "dbt_project", "models", "marts", "marts.yml")


def _pretty(name: str) -> str:
    if "__" in name:
        name = name.split("__", 1)[1]
    return name.replace("_", " ").title()


@st.cache_data
def _load_catalog() -> list:
    with open(_MARTS_YML) as f:
        yml = yaml.safe_load(f)

    all_dims: set[str] = set()
    for sm in yml.get("semantic_models", []):
        prefix = "session" if sm["name"] == "fct_sessions" else "lot"
        for dim in sm.get("dimensions", []):
            all_dims.add(f"{prefix}__{dim['name']}")

    examples = {
        "total_revenue":           "What is total revenue by city in March 2024?",
        "sessions_count":          "How many sessions happened on Fridays?",
        "avg_session_duration":    "What is avg session duration by market type?",
        "avg_revenue_per_session": "Which lot has the highest avg revenue per session?",
        "dynamic_pricing_lift":    "Which lots show the highest weekend pricing lift?",
    }

    rows = []
    for m in yml.get("metrics", []):
        tp = m.get("type_params", {})
        has_weekend = any(
            "is_weekend" in str(tp.get(s, {}).get("filter", ""))
            for s in ("numerator", "denominator")
        )
        dims = sorted(d for d in all_dims if "is_weekend" not in d) \
               if has_weekend else sorted(all_dims)
        rows.append({
            "name": m["name"],
            "desc": m.get("description", "").strip(),
            "dims": dims,
            "example": examples.get(m["name"], ""),
        })
    return rows


def render():
    st.markdown(
        "<div style='font-size:0.85rem;color:#64748b;margin-bottom:1.5rem'>"
        "All metrics available in the semantic layer and the dimensions you can slice them by."
        "</div>",
        unsafe_allow_html=True
    )

    try:
        catalog = _load_catalog()
    except FileNotFoundError:
        st.error(f"marts.yml not found at {_MARTS_YML}.")
        return
    except Exception as e:
        st.error(f"Error loading catalog: {e}")
        return

    for metric in catalog:
        with st.container(border=True):
            left, right = st.columns([1, 2])
            with left:
                st.markdown(
                    f"<div style='font-size:1rem;font-weight:600;color:#0f172a'>"
                    f"{_pretty(metric['name'])}</div>"
                    f"<div style='font-size:0.78rem;color:#64748b;margin-top:4px'>"
                    f"{metric['desc']}</div>",
                    unsafe_allow_html=True
                )
                if metric["example"]:
                    st.markdown(
                        f"<div style='font-size:0.75rem;color:#2563eb;margin-top:8px'>"
                        f"💬 {metric['example']}</div>",
                        unsafe_allow_html=True
                    )
            with right:
                st.markdown(
                    "<div style='font-size:0.72rem;font-weight:600;color:#64748b;"
                    "text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px'>"
                    "Available Dimensions</div>",
                    unsafe_allow_html=True
                )
                badges = " ".join(
                    f'<span class="dim-badge">{_pretty(d)}</span>'
                    for d in metric["dims"]
                )
                st.markdown(badges, unsafe_allow_html=True)

        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:0.75rem;color:#94a3b8;margin-top:1rem'>"
        "Questions outside these metrics will return a helpful explanation of "
        "what is available closest to what you asked."
        "</div>",
        unsafe_allow_html=True
    )
