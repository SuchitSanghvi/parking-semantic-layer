"""
02_ask_your_data.py — Ask Your Parking Data

Natural language queries translated to MetricFlow specs via Claude,
executed against the semantic layer, results summarized in plain English.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import json
import streamlit as st
from app.utils.llm import translate_to_metric_spec, summarize_result
from app.utils.mf import run_metric_query

st.set_page_config(page_title="Ask Your Data", layout="wide")
st.title("Ask Your Parking Data")
st.markdown(
    "Ask any question about your portfolio — revenue, occupancy, session patterns — "
    "and get an answer powered by a trusted semantic layer."
)

# ── Example question chips ────────────────────────────────────────────────────
st.markdown("**Try an example:**")

EXAMPLES = [
    "Which city had the highest revenue last weekend?",
    "What is avg session duration by market type?",
    "Which lots have the most sessions on Fridays?",
]

if "prefill" not in st.session_state:
    st.session_state["prefill"] = ""

cols = st.columns(len(EXAMPLES))
for col, example in zip(cols, EXAMPLES):
    if col.button(example, use_container_width=True):
        st.session_state["prefill"] = example

# ── Question input ────────────────────────────────────────────────────────────
question = st.text_input(
    "Ask a question about your parking portfolio",
    value=st.session_state["prefill"],
    placeholder="e.g. Which lot had the highest revenue last weekend?",
)

submit = st.button("Ask", type="primary")

if submit and question.strip():
    with st.spinner("Translating to metric query..."):
        try:
            spec = translate_to_metric_spec(question)
        except Exception as e:
            st.error(f"Failed to translate question: {e}")
            st.stop()

    # Show if out-of-scope
    if spec.get("error"):
        st.warning(spec.get("message", "That question is outside the available metrics."))
        st.stop()

    # Run MetricFlow query
    with st.spinner("Querying semantic layer..."):
        try:
            df = run_metric_query(spec)
        except ValueError as e:
            st.warning(str(e))
            st.stop()
        except RuntimeError as e:
            st.error(f"MetricFlow error: {e}")
            st.stop()

    # Results
    st.dataframe(df, use_container_width=True)

    # One-line summary
    with st.spinner("Summarizing..."):
        try:
            summary = summarize_result(question, df)
            st.caption(f"💡 {summary}")
        except Exception:
            pass  # summary is nice-to-have, don't block on failure

    # Query spec for technical reviewers
    with st.expander("View query spec"):
        st.json(spec)

elif submit and not question.strip():
    st.warning("Please enter a question first.")
