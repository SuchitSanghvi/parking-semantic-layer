"""
llm.py — Claude API helpers for the NL query tab.

Functions:
  translate_to_metric_spec(question) -> dict
    Converts a natural language question into a MetricFlow query spec.
    Returns {"error": true, "message": "..."} for out-of-scope questions.

  summarize_result(question, df) -> str
    One-sentence plain English summary of a query result,
    written for a property owner (not a data analyst).
"""

import json
import os
import pandas as pd
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

_client = Anthropic()

# ── Metric catalog (kept in sync with marts.yml) ──────────────────────────────

_METRIC_CATALOG = """
Available MetricFlow metrics and their valid dimensions:

1. total_revenue
   Definition: Sum of amount_charged across all valid parking sessions.
   Valid dimensions: session__lot_id, session__session_date, session__time_of_day_bucket,
     session__day_of_week, session__is_weekend, session__payment_method,
     lot__city, lot__state, lot__market_type, lot__lot_capacity_tier

2. sessions_count
   Definition: Count of valid parking sessions.
   Valid dimensions: same as total_revenue

3. avg_session_duration
   Definition: Average duration of a parking session in minutes.
   Valid dimensions: same as total_revenue

4. avg_revenue_per_session
   Definition: Total revenue divided by number of sessions (ratio metric).
   Valid dimensions: same as total_revenue

5. dynamic_pricing_lift
   Definition: Ratio of total revenue on weekends vs weekdays. Values above 1.0
     mean weekend sessions generate more revenue — a proxy for dynamic pricing
     effectiveness.
   Valid dimensions: session__lot_id, session__session_date,
     lot__city, lot__state, lot__market_type, lot__lot_capacity_tier

Grain / time dimension:
  session__session_date — use this for any date or time-based grouping.
  Granularities: day (default), week, month.

IMPORTANT — data date range: the dataset covers January 1 2024 through March 31 2024 only.
  Never generate date filters referencing 2025 or 2026 — no data exists for those years.
  When a question uses relative time ("last month", "this year", "recently"), interpret it
  relative to the dataset window (Q1 2024), not the current calendar date.

Date filter syntax — always use {{ Dimension('session__session_date') }} with ISO dates:
  "last month" (March 2024):
    "{{ Dimension('session__session_date') }} >= '2024-03-01' AND {{ Dimension('session__session_date') }} <= '2024-03-31'"
  "in February":
    "{{ Dimension('session__session_date') }} >= '2024-02-01' AND {{ Dimension('session__session_date') }} <= '2024-02-29'"
  "last weekend" → do NOT add a date filter, just use: "{{ Dimension('session__is_weekend') }} = true"
  "on Fridays" → "{{ Dimension('session__day_of_week') }} = 'Friday'"
  Never use __month, __week, or __year suffix on dimension names in WHERE clauses.

WHERE filter syntax — MUST use MetricFlow Dimension() wrapper exactly as shown:
  "{{ Dimension('session__is_weekend') }} = true"
  "{{ Dimension('session__day_of_week') }} = 'Friday'"
  "{{ Dimension('session__time_of_day_bucket') }} = 'evening'"
  "{{ Dimension('lot__city') }} = 'San Francisco'"
  "{{ Dimension('lot__market_type') }} = 'urban'"

Combine multiple filters with AND:
  "{{ Dimension('session__is_weekend') }} = true AND {{ Dimension('lot__city') }} = 'Seattle'"

The {{ Dimension('...') }} wrapper is required — bare column names will fail.

Return format — JSON object with these keys:
  {
    "metric": "<metric_name>",
    "group_by": ["<dim1>", "<dim2>"],   // list, can be empty
    "where": "<filter_string>",          // optional, omit if no filter
    "order_by": "<dim_or_metric>",       // optional
    "limit": <int>                       // optional, default 50
  }

If the question asks about something NOT covered by these metrics or dimensions
(e.g. individual vehicle tracking, payment fraud, lot revenue by owner name),
return:
  {"error": true, "message": "<explain what IS available that's closest to what they asked>"}
"""

_TRANSLATE_SYSTEM = f"""You are a query translator for a parking analytics semantic layer.
Convert the user's natural language question into a MetricFlow query spec.

{_METRIC_CATALOG}

Return valid JSON only. No explanation, no markdown, no code fences."""

_SUMMARIZE_SYSTEM = """You are a data analyst summarizing query results for a parking property owner.
Write exactly one sentence. Be specific — include the top value or key number from the data.
Avoid jargon. Write as if explaining to a non-technical business owner.
Use plain text only — no markdown, no asterisks, no underscores, no bold, no italics.
Example: San Francisco generated the highest total revenue this month at $55,841.
"""


def translate_to_metric_spec(question: str) -> dict:
    """
    Translate a natural language question into a MetricFlow query spec dict.
    Returns {"error": True, "message": "..."} if the question is out of scope.
    """
    response = _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        system=_TRANSLATE_SYSTEM,
        messages=[{"role": "user", "content": question}],
    )
    raw = response.content[0].text.strip()
    return json.loads(raw)


def summarize_result(question: str, df: pd.DataFrame) -> str:
    """
    Produce a one-sentence plain English summary of a query result DataFrame,
    written for a property owner.
    """
    # Convert first 20 rows to a compact string so we don't blow the context
    data_preview = df.head(20).to_string(index=False)
    prompt = f"Question: {question}\n\nData:\n{data_preview}"

    response = _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=150,
        system=_SUMMARIZE_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()
