"""Ask Your Data tab — hero input, example chips, two-column results layout."""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import html as _html
import streamlit as st
from app.utils.catalog import get_metrics, pretty_dim
from app.utils.llm import translate_to_metric_spec, summarize_result
from app.utils.mf  import run_metric_query

# Extra hand-coded examples that show cross-metric or multi-dimension questions
_EXTRA_EXAMPLES = [
    "What is total revenue on days with local events vs without?",
    "Which lots had the biggest week-over-week revenue increase?",
    "Compare avg session duration across market types on weekends",
]


def render():
    try:
        _render()
    except Exception as e:
        st.error(
            f"Something went wrong: {e}\n\n"
            "Please try refreshing the page or asking a different question."
        )


def _render():
    # ── Session state init ─────────────────────────────────────────────────────
    # Use a separate "_prefill" key for button-driven pre-fill because
    # Streamlit's text_input widget owns its key and cannot be modified
    # externally after instantiation.
    if "_prefill" not in st.session_state:
        st.session_state["_prefill"] = ""
    if "last_result" not in st.session_state:
        st.session_state["last_result"] = None
    if "last_spec" not in st.session_state:
        st.session_state["last_spec"] = None
    if "last_df" not in st.session_state:
        st.session_state["last_df"] = None

    # ── Load catalog once ──────────────────────────────────────────────────────
    try:
        catalog = get_metrics()
    except Exception as e:
        st.error(f"Error loading metrics catalog: {e}")
        catalog = []

    # ── Section 1: Hero input ──────────────────────────────────────────────────
    with st.container(border=True):
        st.markdown("### Ask Your Parking Data")
        st.markdown(
            "<div style='font-size:0.85rem;color:#64748b;margin-bottom:0.8rem'>"
            "Ask any question about revenue, occupancy, or session patterns "
            "-- answered by a trusted semantic layer."
            "</div>",
            unsafe_allow_html=True,
        )
        input_col, btn_col = st.columns([5, 1])
        with input_col:
            question = st.text_input(
                "Question",
                value=st.session_state["_prefill"],
                placeholder="e.g. Which city had the highest revenue in February 2024?",
                label_visibility="collapsed",
            )
        with btn_col:
            ask = st.button("Ask", type="primary", use_container_width=True)

    st.divider()

    # ── Section 2: Example questions ───────────────────────────────────────────
    # Build example list from catalog + hand-coded extras (deduplicated)
    catalog_examples = [m["example"] for m in catalog if m.get("example")]
    seen = set(catalog_examples)
    all_examples = list(catalog_examples)
    for ex in _EXTRA_EXAMPLES:
        if ex not in seen:
            all_examples.append(ex)
            seen.add(ex)

    if all_examples:
        st.caption("Try asking:")
        show_first = min(5, len(all_examples))
        first_batch = all_examples[:show_first]
        rest_batch = all_examples[show_first:]

        cols = st.columns(len(first_batch))
        for col, ex in zip(cols, first_batch):
            if col.button(ex, use_container_width=True, key=f"ex_{hash(ex)}"):
                st.session_state["_prefill"] = ex
                st.rerun()

        if rest_batch:
            with st.expander(f"+ {len(rest_batch)} more examples"):
                more_cols = st.columns(min(len(rest_batch), 3))
                for i, ex in enumerate(rest_batch):
                    if more_cols[i % len(more_cols)].button(
                        ex, use_container_width=True, key=f"exm_{hash(ex)}"
                    ):
                        st.session_state["_prefill"] = ex
                        st.rerun()

    st.divider()

    # ── Process query on Ask click ─────────────────────────────────────────────
    if ask and question.strip():
        with st.spinner("Translating question..."):
            try:
                spec = translate_to_metric_spec(question)
            except Exception as e:
                st.session_state["last_result"] = "error"
                st.session_state["last_spec"] = {"translation_error": str(e)}
                st.session_state["last_df"] = None
                st.error(f"Translation error: {e}")
                return

        if spec.get("error"):
            st.session_state["last_result"] = "out_of_scope"
            st.session_state["last_spec"] = spec
            st.session_state["last_df"] = None
        else:
            with st.spinner("Querying semantic layer..."):
                try:
                    df = run_metric_query(spec)
                except ValueError as e:
                    st.session_state["last_result"] = "out_of_scope"
                    st.session_state["last_spec"] = spec
                    st.session_state["last_df"] = None
                    st.warning(str(e))
                    return
                except RuntimeError as e:
                    st.session_state["last_result"] = "error"
                    st.session_state["last_spec"] = spec
                    st.session_state["last_df"] = None
                    st.error(f"MetricFlow error: {e}")
                    return

            with st.spinner("Summarising..."):
                try:
                    summary = summarize_result(question, df)
                except Exception:
                    summary = None

            st.session_state["last_result"] = "success"
            st.session_state["last_spec"] = spec
            st.session_state["last_df"] = df
            st.session_state["last_summary"] = summary

    elif ask and not question.strip():
        st.warning("Please enter a question first.")

    # ── Section 3: Two-column layout ───────────────────────────────────────────
    left, right = st.columns([1, 2], gap="large")

    # ── Left column: Available Metrics ─────────────────────────────────────────
    with left:
        st.subheader("Available Metrics")
        st.caption(
            "Questions outside these metrics return a helpful message "
            "explaining what is available."
        )
        if not catalog:
            st.caption("No metrics found.")
        else:
            for metric in catalog:
                with st.expander(metric["label"]):
                    st.caption(metric["description"])
                    if metric["dims"]:
                        st.markdown("**Dimensions:**")
                        badges = " ".join(
                            f'<span style="'
                            f"background-color:#f0f2f6;"
                            f"border:1px solid #d1d5db;"
                            f"border-radius:12px;"
                            f"padding:2px 10px;"
                            f"font-size:12px;"
                            f"margin:2px;"
                            f"display:inline-block;"
                            f'">{pretty_dim(d)}</span>'
                            for d in metric["dims"]
                        )
                        st.markdown(badges, unsafe_allow_html=True)
                    if metric.get("example"):
                        if st.button(
                            "Ask this example",
                            key=f"ask_{metric['name']}",
                            use_container_width=True,
                        ):
                            st.session_state["_prefill"] = metric["example"]
                            st.rerun()

    # ── Right column: Results ──────────────────────────────────────────────────
    with right:
        result = st.session_state.get("last_result")

        if result is None:
            st.info(
                "Type a question above or click an example to get started. "
                "Results appear here."
            )

        elif result == "out_of_scope":
            spec = st.session_state.get("last_spec", {})
            st.warning(
                spec.get("message", "That question is outside the available metrics.")
            )

        elif result == "success":
            df = st.session_state.get("last_df")
            summary = st.session_state.get("last_summary")
            spec = st.session_state.get("last_spec", {})

            if summary:
                # Use html.escape so markdown characters in the summary
                # (e.g. **bold**, $numbers) render as plain text, not markdown.
                st.markdown(
                    f"<div style='background:#d1fae5;border:1px solid #6ee7b7;"
                    f"border-radius:6px;padding:10px 14px;color:#065f46;"
                    f"font-size:0.88rem'>{_html.escape(summary)}</div>",
                    unsafe_allow_html=True,
                )

            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            elif df is not None and df.empty:
                st.info(
                    "No data matched this filter for the available "
                    "date range (Jan-Mar 2024)."
                )

            with st.expander("View query spec"):
                st.caption(
                    "Claude translated your question into this spec. "
                    "MetricFlow generated the SQL deterministically."
                )
                st.json(spec)
