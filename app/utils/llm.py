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

WHERE filter syntax (MetricFlow DSL):
  session__is_weekend = true
  session__day_of_week = 'Friday'
  session__time_of_day_bucket = 'evening'
  lot__city = 'San Francisco'
  lot__market_type = 'urban'

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
Example: "Mission St lot generated the most revenue last weekend at $1,240."
"""


def translate_to_metric_spec(question: str) -> dict:
    """
    Translate a natural language question into a MetricFlow query spec dict.
    Returns {"error": True, "message": "..."} if the question is out of scope.
    """
    response = _client.messages.create(
        model="claude-sonnet-4-20250514",
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
        model="claude-sonnet-4-20250514",
        max_tokens=150,
        system=_SUMMARIZE_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()
