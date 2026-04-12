# Parking Portfolio Intelligence

A dbt semantic layer on raw parking event data, with a Streamlit app for portfolio analytics and natural language querying.

---

## Live Demo

[SCREENSHOT: dashboard tab showing KPI cards and occupancy chart]

[SCREENSHOT: Ask Your Data tab showing a question and result]

Live app: [Streamlit Cloud URL -- add after deployment]

---

## The Problem

AirGarage replaces fragmented parking operations with a data-rich OS. Their intelligence dashboard gives property owners real-time occupancy, revenue, and pricing signals -- but that only works if the underlying data is well-defined and trustworthy. This project models what that data layer looks like: raw LPR camera events (messy, split entry/exit rows) cleaned, tested, and exposed as named business metrics that a dashboard or an AI can query reliably. The semantic layer is not just internal tooling -- it is the data contract between raw events and everything downstream: owner dashboards, pricing algorithms, and natural language queries.

---

## Architecture

```
Raw LPR Events (3 messy CSVs)
  entry + exit as separate rows, inconsistent casing,
  duplicate camera triggers, NULL capacity, unknown lots
        |
        v
  dbt Staging         clean + flag (bad rows kept, not dropped)
        |
        v
  dbt Intermediate    pair entry+exit into sessions via window function
        |
        v
  dbt Marts
  ├── fct_sessions       valid sessions only, 15 dbt tests
  ├── dim_lots           capacity tier, city, market type, lot name
  ├── mart_lot_daily     pre-aggregated: occupancy rate, turnover rate
  └── Semantic Layer     MetricFlow: 5 metrics, 12 dimensions
        |
        v
  Streamlit App
  ├── Portfolio Dashboard    queries mart_lot_daily directly (no MetricFlow)
  └── Ask Your Data          NL question → metric spec → MetricFlow → DuckDB
```

---

## Defined Metrics

**MetricFlow-managed** (defined in `marts.yml`, SQL generated deterministically):

| Metric | Definition | Business Use |
|--------|-----------|--------------|
| `total_revenue` | `sum(amount_charged)` on valid sessions | NOI tracking |
| `sessions_count` | count of valid sessions | Demand signal |
| `avg_session_duration` | avg session length in minutes | Space planning |
| `avg_revenue_per_session` | `total_revenue / sessions_count` | Yield analysis |
| `dynamic_pricing_lift` | weekend avg revenue / weekday avg revenue | Pricing model input |

**Pre-computed in `mart_lot_daily`** (not MetricFlow metrics):

| Column | Definition | Why pre-computed, not in the semantic layer |
|--------|-----------|---------------------------------------------|
| `occupancy_rate` | peak concurrent sessions / capacity | Requires a sweep-line window function over event timestamps, then a join to `dim_lots` for capacity. MetricFlow expects additive measures on a single model -- this computation spans two models and requires a non-additive intermediate step. |
| `turnover_rate` | sessions / capacity | Same: depends on capacity from the dimension table, not a measure available in the fact table. |

**Available dimensions** (MetricFlow resolves the `fct_sessions` + `dim_lots` join automatically):

- From `fct_sessions`: `session_date`, `lot_id`, `time_of_day_bucket`, `day_of_week`, `is_weekend`, `payment_method`, `has_local_event`
- From `dim_lots` via lot entity: `lot_name`, `city`, `state`, `market_type`, `lot_capacity_tier`

---

## Data Quality

| Issue | Where it appears | How detected | How handled |
|-------|-----------------|-------------|-------------|
| Duplicate ENTRY events | Camera misfire | `is_duplicate_entry` flag in `int_sessions` | Filtered in `fct_sessions` |
| EXIT with no ENTRY | Pre-activation vehicles | `is_orphaned_exit` flag | Excluded from `fct_sessions` |
| ENTRY with no EXIT | Drive-offs / still parked | `is_orphaned_entry` flag | Excluded from `fct_sessions` |
| Negative duration | Timestamp clock sync bug | `is_negative_duration` flag | Filtered; caught by custom dbt test |
| Unknown `lot_id` | Decommissioned lot | No join match to `dim_lots` | Drops from `fct_sessions` naturally |
| NULL capacity | No record on file | `is_capacity_missing` flag in `dim_lots` | Excluded from occupancy rate; shown as `'unknown'` tier |

Bad rows are flagged at staging and intermediate layers, not dropped. The flags stay in `int_sessions`, so the full raw event history remains queryable if needed. `fct_sessions` applies the filters; the intermediate model does not.

---

## Why Semantic Layer, Not Text-to-SQL

Two approaches exist for natural language analytics on structured data.

**Text-to-SQL:** Claude reads the schema and writes raw SQL. This is flexible -- it can answer almost any question the schema supports. The problem is correctness: Claude can produce a syntactically valid query with a wrong join condition or a misapplied filter that returns plausible-looking but incorrect results. There is no correctness guarantee baked into the architecture. As dbt Labs documented in their 2026 benchmark: text-to-SQL will "cheerfully give you a wrong number."

**Semantic layer (this project):** Claude translates the natural language question into a structured spec: one metric name and a list of dimension names. MetricFlow takes that spec and generates the SQL deterministically. Claude never writes a `JOIN` clause and has no knowledge of table schemas. If Claude picks the correct metric and dimensions, the SQL is guaranteed correct -- the semantic model defines exactly how `fct_sessions` and `dim_lots` join and how each measure aggregates. If the question references data outside the semantic layer's scope, the app returns an explicit error message rather than firing a query that might silently return incorrect data.

The tradeoff is coverage. The semantic layer answers questions within its defined 5-metric, 12-dimension scope. That is the constraint. The benefit is trust: every result is computed the same way every time, and that computation is readable in `marts.yml`.

From dbt Labs' 2026 benchmark: "For questions within the Semantic Layer's scope, both models return correct results 100% of the time."

Reference: https://docs.getdbt.com/blog/semantic-layer-vs-text-to-sql-2026

---

## Current Limitations

- **Single-question scope.** One question maps to one MetricFlow query and one result set. Complex analysis questions like "find underperforming lots and recommend pricing changes" require multiple chained queries with reasoning in between. The current design does not support that.
- **Synthetic data.** 8 lots, 90 days (Jan-Mar 2024). Patterns are realistic but not real AirGarage operational data.
- **DuckDB in demo.** Production would target Snowflake or BigQuery. Only `profiles.yml` changes -- all dbt models and MetricFlow definitions are warehouse-agnostic SQL.

---

## How to Run Locally

```bash
git clone <repo>
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python generate_data/generate_raw_data.py
cp data/*.csv dbt_project/seeds/
cd dbt_project && dbt seed && dbt run && dbt test && cd ..
streamlit run app/streamlit_app.py
```

On first load the app detects that `warehouse.duckdb` does not exist and runs the full pipeline automatically (seed, run). Subsequent loads skip that step.

---

## Future Extensions

- **MCP server wrapping MetricFlow as callable tools.** The single-question limitation above is the most important gap. An MCP server that exposes each metric as an individually callable tool lets Claude chain 3-4 metric queries with reasoning between steps -- the pattern that complex portfolio questions actually require. This is the natural next step for turning the NL tab from a demo into a real analysis tool.

- **Real-time ingestion via Snowpipe Streaming or Kafka.** Today, seeds are the data source. Replacing them with a streaming consumer is the gap between demo architecture and production architecture: it tests whether the staging layer handles schema drift, late-arriving events, and continuous `dbt run` scheduling -- which is how LPR data actually arrives.

- **Reverse ETL via Polytomic.** MetricFlow metrics are currently only accessible through the Streamlit app. Polytomic would push computed metrics back to whatever system property owners already use -- a CRM, a Slack notification, a Google Sheet -- after each dbt run. That closes the loop from computation to owner action without requiring owners to log into another tool.

- **dbt Cloud with Semantic Layer API.** The Streamlit app is a demo harness, not a production BI integration. Direct Semantic Layer API access from Hex or Tableau removes the app dependency and lets any connected BI tool query named metrics with the same governance guarantees -- without each team writing their own SQL or maintaining their own MetricFlow subprocess wrapper.
