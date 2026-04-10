"""
01_portfolio_dashboard.py — Portfolio Dashboard

Four sections:
  1. KPI cards (total revenue, avg occupancy, total sessions, avg duration)
  2. Revenue by lot (bar chart, colored by market_type)
  3. Occupancy trend (line chart, one line per lot)
  4. Dynamic pricing signal (weekday vs weekend avg price per session)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date, timedelta
from app.utils.db import run_query

st.set_page_config(page_title="Portfolio Dashboard", layout="wide")
st.title("Portfolio Dashboard")

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

# Date range (default: last 30 days of data)
max_date_row = run_query("SELECT MAX(session_date) AS d FROM main.mart_lot_daily")
max_date = max_date_row["d"].iloc[0]
if hasattr(max_date, "date"):
    max_date = max_date.date()

default_start = max_date - timedelta(days=29)

date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=date(2024, 1, 1),
    max_value=max_date,
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, max_date

# Lot multi-select (for occupancy chart)
all_lots = run_query("SELECT DISTINCT lot_id FROM main.mart_lot_daily ORDER BY lot_id")["lot_id"].tolist()
selected_lots = st.sidebar.multiselect("Lots (occupancy chart)", all_lots, default=all_lots)

# ── Section 1: KPI cards ──────────────────────────────────────────────────────
st.subheader("Portfolio KPIs")

kpi_sql = f"""
SELECT
    SUM(total_revenue)                                           AS total_revenue,
    AVG(CASE WHEN occupancy_rate IS NOT NULL
             THEN occupancy_rate END)                           AS avg_occupancy_rate,
    SUM(total_sessions)                                          AS total_sessions,
    AVG(avg_duration_minutes)                                    AS avg_duration_minutes
FROM main.mart_lot_daily
WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
"""
kpi = run_query(kpi_sql).iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue", f"${kpi['total_revenue']:,.0f}")
c2.metric("Avg Occupancy Rate", f"{kpi['avg_occupancy_rate'] * 100:.1f}%" if kpi['avg_occupancy_rate'] else "N/A")
c3.metric("Total Sessions", f"{int(kpi['total_sessions']):,}")
c4.metric("Avg Session Duration", f"{kpi['avg_duration_minutes']:.0f} min")

st.divider()

# ── Section 2: Revenue by lot ─────────────────────────────────────────────────
st.subheader("Revenue by Lot")

rev_sql = f"""
SELECT
    m.lot_id,
    d.market_type,
    SUM(m.total_revenue) AS total_revenue
FROM main.mart_lot_daily m
JOIN main.dim_lots d USING (lot_id)
WHERE m.session_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY m.lot_id, d.market_type
ORDER BY total_revenue DESC
"""
rev_df = run_query(rev_sql)

fig_rev = px.bar(
    rev_df,
    x="lot_id",
    y="total_revenue",
    color="market_type",
    labels={"lot_id": "Lot", "total_revenue": "Revenue ($)", "market_type": "Market Type"},
    title=f"Total Revenue by Lot ({start_date} to {end_date})",
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_rev.update_layout(xaxis_tickangle=-30)
st.plotly_chart(fig_rev, use_container_width=True)

st.divider()

# ── Section 3: Occupancy trend ────────────────────────────────────────────────
st.subheader("Occupancy Rate Over Time")

# Exclude lots with NULL capacity
null_cap_lots = run_query(
    "SELECT lot_id FROM main.dim_lots WHERE is_capacity_missing = true"
)["lot_id"].tolist()

if null_cap_lots:
    st.caption(
        f"⚠️ Lots excluded (no capacity on record): {', '.join(null_cap_lots)}"
    )

lots_clause = ", ".join(f"'{l}'" for l in selected_lots) if selected_lots else "''"
occ_sql = f"""
SELECT session_date, lot_id, occupancy_rate
FROM main.mart_lot_daily
WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
  AND lot_id IN ({lots_clause})
  AND occupancy_rate IS NOT NULL
ORDER BY session_date
"""
occ_df = run_query(occ_sql)

if occ_df.empty:
    st.info("No occupancy data for the selected lots and date range.")
else:
    fig_occ = px.line(
        occ_df,
        x="session_date",
        y="occupancy_rate",
        color="lot_id",
        labels={"session_date": "Date", "occupancy_rate": "Occupancy Rate", "lot_id": "Lot"},
        title="Daily Occupancy Rate by Lot",
    )
    fig_occ.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig_occ, use_container_width=True)

st.divider()

# ── Section 4: Dynamic pricing signal ────────────────────────────────────────
st.subheader("Dynamic Pricing Signal — Weekday vs Weekend")

pricing_sql = f"""
SELECT
    lot_id,
    AVG(CASE WHEN DAYOFWEEK(session_date) IN (0, 6) THEN avg_price_per_session END) AS weekend_avg_price,
    AVG(CASE WHEN DAYOFWEEK(session_date) NOT IN (0, 6) THEN avg_price_per_session END) AS weekday_avg_price
FROM main.mart_lot_daily
WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY lot_id
ORDER BY lot_id
"""
pricing_df = run_query(pricing_sql).dropna()

if pricing_df.empty:
    st.info("No pricing data for the selected date range.")
else:
    pricing_df["lift_pct"] = (
        (pricing_df["weekend_avg_price"] - pricing_df["weekday_avg_price"])
        / pricing_df["weekday_avg_price"]
        * 100
    ).round(1)

    melted = pricing_df.melt(
        id_vars=["lot_id", "lift_pct"],
        value_vars=["weekday_avg_price", "weekend_avg_price"],
        var_name="period",
        value_name="avg_price",
    )
    melted["period"] = melted["period"].map(
        {"weekday_avg_price": "Weekday", "weekend_avg_price": "Weekend"}
    )

    fig_price = px.bar(
        melted,
        x="lot_id",
        y="avg_price",
        color="period",
        barmode="group",
        labels={"lot_id": "Lot", "avg_price": "Avg Price per Session ($)", "period": "Period"},
        title="Avg Price per Session: Weekday vs Weekend",
        color_discrete_map={"Weekday": "#636EFA", "Weekend": "#EF553B"},
    )

    # Annotate lift % above each bar pair
    for _, row in pricing_df.iterrows():
        fig_price.add_annotation(
            x=row["lot_id"],
            y=max(row["weekday_avg_price"], row["weekend_avg_price"]) + 0.3,
            text=f"+{row['lift_pct']:.1f}%" if row["lift_pct"] >= 0 else f"{row['lift_pct']:.1f}%",
            showarrow=False,
            font=dict(size=11, color="gray"),
        )

    st.plotly_chart(fig_price, use_container_width=True)
