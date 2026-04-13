"""Dashboard tab — KPI cards, charts, top performers."""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from datetime import date, timedelta
import streamlit as st
import plotly.express as px
from app.utils.db import run_query


def render():
    # ── Date controls ─────────────────────────────────────────────────────────
    max_row    = run_query("SELECT MAX(session_date) AS d FROM main.mart_lot_daily")
    max_date   = max_row["d"].iloc[0]
    if hasattr(max_date, "date"):
        max_date = max_date.date()
    default_start = max_date - timedelta(days=29)

    ctrl1, ctrl2, _ = st.columns([1.4, 1.4, 7])
    with ctrl1:
        start_date = st.date_input("From", value=default_start,
                                   min_value=date(2024, 1, 1), max_value=max_date)
    with ctrl2:
        end_date = st.date_input("To", value=max_date,
                                 min_value=date(2024, 1, 1), max_value=max_date)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Date validation ───────────────────────────────────────────────────────
    if end_date < start_date:
        st.error("'To' date must be on or after 'From' date.")
        return

    # ── KPI query ─────────────────────────────────────────────────────────────
    kpi = run_query(f"""
        SELECT
            SUM(total_revenue)                                        AS total_revenue,
            AVG(CASE WHEN occupancy_rate IS NOT NULL
                     THEN occupancy_rate END)                         AS avg_occupancy,
            SUM(total_sessions)                                       AS total_sessions,
            AVG(avg_duration_minutes)                                 AS avg_duration
        FROM main.mart_lot_daily
        WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
    """).iloc[0]

    import math

    def _safe_int(v):
        try:
            return int(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
        except Exception:
            return None

    def _safe_float(v):
        try:
            return float(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
        except Exception:
            return None

    rev      = _safe_float(kpi['total_revenue'])
    occ      = _safe_float(kpi['avg_occupancy'])
    sessions = _safe_int(kpi['total_sessions'])
    dur      = _safe_float(kpi['avg_duration'])

    no_data = rev is None and sessions is None
    if no_data:
        st.warning("No data found for the selected date range.")
        return

    # ── KPI cards ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    _kpi_card(c1, "Total Revenue",
              f"${rev:,.0f}" if rev is not None else "N/A",
              f"{start_date} – {end_date}")
    _kpi_card(c2, "Avg Occupancy Rate",
              f"{occ*100:.1f}%" if occ is not None else "N/A",
              "Lots with known capacity")
    _kpi_card(c3, "Total Sessions",
              f"{sessions:,}" if sessions is not None else "N/A",
              "Valid sessions only")
    _kpi_card(c4, "Avg Session Duration",
              f"{dur:.0f} min" if dur is not None else "N/A",
              f"≈ {dur/60:.1f} hours" if dur is not None else "")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Charts ────────────────────────────────────────────────────────────────
    ch1, ch2 = st.columns(2)

    with ch1:
        with st.container(border=True):
            st.markdown('<div class="section-title">Revenue by Lot</div>'
                        '<div class="section-sub">Total revenue for selected period</div>',
                        unsafe_allow_html=True)
            rev_df = run_query(f"""
                SELECT m.lot_id, d.market_type, SUM(m.total_revenue) AS revenue
                FROM main.mart_lot_daily m JOIN main.dim_lots d USING (lot_id)
                WHERE m.session_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY m.lot_id, d.market_type ORDER BY revenue DESC
            """)
            fig = px.bar(rev_df, x="lot_id", y="revenue", color="market_type",
                         color_discrete_sequence=["#2563eb", "#7c3aed", "#0891b2"],
                         labels={"lot_id": "", "revenue": "Revenue ($)",
                                 "market_type": "Market"})
            fig.update_layout(margin=dict(t=10, b=0), plot_bgcolor="white",
                              paper_bgcolor="white", legend_title_text="",
                              xaxis_tickangle=-30, showlegend=True)
            fig.update_yaxes(gridcolor="#f1f5f9")
            st.plotly_chart(fig, use_container_width=True)

    with ch2:
        with st.container(border=True):
            st.markdown('<div class="section-title">Occupancy Trend</div>'
                        '<div class="section-sub">Daily occupancy rate by lot</div>',
                        unsafe_allow_html=True)
            all_lots = run_query(
                "SELECT DISTINCT lot_id FROM main.mart_lot_daily ORDER BY lot_id"
            )["lot_id"].tolist()
            st.markdown(
                "<div style='font-size:0.75rem;color:#64748b;margin-bottom:4px'>"
                "Select one or more lots to display</div>",
                unsafe_allow_html=True
            )
            selected = st.multiselect("Lots", all_lots, default=all_lots[:6],
                                      label_visibility="collapsed",
                                      placeholder="Choose lots…")
            if selected:
                lots_in = ", ".join(f"'{l}'" for l in selected)
                occ_df = run_query(f"""
                    SELECT session_date, lot_id, occupancy_rate
                    FROM main.mart_lot_daily
                    WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
                      AND lot_id IN ({lots_in}) AND occupancy_rate IS NOT NULL
                    ORDER BY session_date
                """)
                fig2 = px.line(occ_df, x="session_date", y="occupancy_rate",
                               color="lot_id",
                               labels={"session_date": "", "occupancy_rate": "Occupancy",
                                       "lot_id": "Lot"},
                               color_discrete_sequence=px.colors.qualitative.Set2)
                fig2.update_layout(margin=dict(t=10, b=0), plot_bgcolor="white",
                                   paper_bgcolor="white", legend_title_text="")
                fig2.update_yaxes(tickformat=".0%", gridcolor="#f1f5f9")
                st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Top performers ─────────────────────────────────────────────────────────
    with st.container(border=True):
        st.markdown('<div class="section-title">Top Performing Lots</div>'
                    '<div class="section-sub">Ranked by total revenue for selected period</div>',
                    unsafe_allow_html=True)

        top_df = run_query(f"""
            SELECT m.lot_id, d.lot_name, d.city, d.state, d.capacity,
                   SUM(m.total_revenue) AS revenue,
                   AVG(m.occupancy_rate) AS avg_occ
            FROM main.mart_lot_daily m JOIN main.dim_lots d USING (lot_id)
            WHERE m.session_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY m.lot_id, d.lot_name, d.city, d.state, d.capacity
            ORDER BY revenue DESC LIMIT 8
        """)

        null_cap = run_query(
            "SELECT lot_id FROM main.dim_lots WHERE is_capacity_missing = true"
        )["lot_id"].tolist()

        for rank, row in top_df.iterrows():
            cap_str = (f"{int(row['capacity'])} spaces"
                       if row["lot_id"] not in null_cap and row["capacity"]
                       else "capacity unknown")
            occ_str = (f"{row['avg_occ']*100:.0f}% avg occupancy"
                       if row["avg_occ"] else "")
            badge = f'<div class="lot-badge">{occ_str}</div>' if occ_str else ""
            st.markdown(f"""
            <div class="lot-row">
              <div class="lot-left">
                <span style="color:#3b82f6;font-size:1.1rem">📍</span>
                <div>
                  <div class="lot-name">{row['lot_name'] or row['lot_id']}</div>
                  <div class="lot-meta">{row['city']}, {row['state']} &nbsp;·&nbsp; {cap_str}</div>
                </div>
              </div>
              <div class="lot-right">
                <div class="lot-rev">${row['revenue']:,.0f}</div>
                {badge}
              </div>
            </div>""", unsafe_allow_html=True)

    # ── Pricing signal ─────────────────────────────────────────────────────────
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown('<div class="section-title">Dynamic Pricing Signal</div>'
                    '<div class="section-sub">Average price per session — weekday vs weekend</div>',
                    unsafe_allow_html=True)
        pricing_df = run_query(f"""
            SELECT lot_id,
                AVG(CASE WHEN DAYOFWEEK(session_date) NOT IN (0,6)
                         THEN avg_price_per_session END) AS weekday,
                AVG(CASE WHEN DAYOFWEEK(session_date) IN (0,6)
                         THEN avg_price_per_session END) AS weekend
            FROM main.mart_lot_daily
            WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY lot_id ORDER BY lot_id
        """).dropna()

        if not pricing_df.empty:
            pricing_df["lift"] = (
                (pricing_df["weekend"] - pricing_df["weekday"])
                / pricing_df["weekday"] * 100
            ).round(1)
            import pandas as pd
            melted = pricing_df.melt(
                id_vars=["lot_id", "lift"],
                value_vars=["weekday", "weekend"],
                var_name="period", value_name="avg_price"
            )
            melted["period"] = melted["period"].str.title()
            fig3 = px.bar(melted, x="lot_id", y="avg_price", color="period",
                          barmode="group",
                          color_discrete_map={"Weekday": "#2563eb", "Weekend": "#7c3aed"},
                          labels={"lot_id": "", "avg_price": "Avg Price ($)",
                                  "period": ""})
            fig3.update_layout(margin=dict(t=10, b=0), plot_bgcolor="white",
                               paper_bgcolor="white", legend_title_text="",
                               xaxis_tickangle=-30)
            fig3.update_yaxes(gridcolor="#f1f5f9")
            for _, row in pricing_df.iterrows():
                fig3.add_annotation(
                    x=row["lot_id"],
                    y=max(row["weekday"], row["weekend"]) + 0.5,
                    text=f"+{row['lift']:.0f}%" if row["lift"] >= 0 else f"{row['lift']:.0f}%",
                    showarrow=False,
                    font=dict(size=10, color="#16a34a" if row["lift"] >= 0 else "#dc2626")
                )
            st.plotly_chart(fig3, use_container_width=True)


def _kpi_card(col, label, value, sub):
    with col:
        with st.container(border=True):
            st.markdown(
                f'<div class="kpi-label">{label}</div>'
                f'<div class="kpi-value">{value}</div>'
                f'<div class="kpi-sub">{sub}</div>',
                unsafe_allow_html=True
            )
