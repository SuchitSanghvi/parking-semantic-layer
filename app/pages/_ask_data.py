"""Ask Your Data tab — NL query via Claude + MetricFlow."""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
from app.utils.llm import translate_to_metric_spec, summarize_result
from app.utils.mf  import run_metric_query

_EXAMPLES = [
    "Which city had the highest revenue last weekend?",
    "What is avg session duration by market type?",
    "Which lots have the most sessions on Fridays?",
]


def render():
    st.markdown(
        "<div style='font-size:0.85rem;color:#64748b;margin-bottom:1.2rem'>"
        "Ask any question about revenue, occupancy, or session patterns — "
        "answered by a trusted semantic layer."
        "</div>",
        unsafe_allow_html=True
    )

    # ── Example chips ──────────────────────────────────────────────────────────
    st.markdown("**Try an example**")
    if "prefill" not in st.session_state:
        st.session_state["prefill"] = ""

    cols = st.columns(len(_EXAMPLES))
    for col, ex in zip(cols, _EXAMPLES):
        if col.button(ex, use_container_width=True):
            st.session_state["prefill"] = ex

    # ── Input ──────────────────────────────────────────────────────────────────
    with st.form("ask_form", clear_on_submit=False):
        question = st.text_input(
            "Question",
            value=st.session_state["prefill"],
            placeholder="e.g. Which lot had the highest revenue in March 2024?",
            label_visibility="collapsed",
        )
        ask = st.form_submit_button("Ask", type="primary")

    if ask and question.strip():
        # Step 1 — translate to metric spec
        with st.spinner("Translating question…"):
            try:
                spec = translate_to_metric_spec(question)
            except Exception as e:
                st.error(f"Translation error: {e}")
                return

        if spec.get("error"):
            st.warning(spec.get("message", "That question is outside the available metrics."))
            return

        # Step 2 — run MetricFlow query
        with st.spinner("Querying semantic layer…"):
            try:
                df = run_metric_query(spec)
            except ValueError as e:
                st.warning(str(e))
                return
            except RuntimeError as e:
                st.error(f"MetricFlow error: {e}")
                return

        # Step 3 — results
        with st.container(border=True):
            st.dataframe(df, use_container_width=True, hide_index=True)

            with st.spinner("Summarising…"):
                try:
                    summary = summarize_result(question, df)
                    # Use markdown=False equivalent — wrap in plain div to avoid
                    # Streamlit rendering any markdown characters from the LLM
                    st.markdown(
                        f"<div style='font-size:0.8rem;color:#64748b;padding:6px 0'>"
                        f"💡 {summary}</div>",
                        unsafe_allow_html=True
                    )
                except Exception:
                    pass

        with st.expander("View query spec"):
            st.json(spec)

    elif ask and not question.strip():
        st.warning("Please enter a question first.")

