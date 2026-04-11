"""
catalog.py — Single source of truth for the semantic layer catalog.

Uses MetricFlow CLI (`mf list metrics`, `mf list dimensions`) as the
authoritative source for metric names and valid dimensions. MetricFlow
reads from target/semantic_manifest.json — no DuckDB connection needed,
no file lock conflicts.

YAML is only read for human-readable descriptions (not available via CLI).

Everything is cached after first call so CLI is only invoked once per
app session.
"""

import os
import functools
import re
import subprocess
import yaml

_PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DBT_PROJECT_DIR = os.path.join(_PROJECT_ROOT, "dbt_project")
_MARTS_YML       = os.path.join(_PROJECT_ROOT, "dbt_project", "models", "marts", "marts.yml")
_MF_BIN          = os.path.join(_PROJECT_ROOT, "venv312", "bin", "mf")

# Example questions per metric — the one piece of content that can't be
# derived from MetricFlow or marts.yml.
_EXAMPLES = {
    "total_revenue":           "What is total revenue by city in March 2024?",
    "sessions_count":          "How many sessions happened on Fridays?",
    "avg_session_duration":    "What is avg session duration by market type?",
    "avg_revenue_per_session": "Which lot has the highest avg revenue per session?",
    "dynamic_pricing_lift":    "Which lots show the highest weekend pricing lift?",
}


def _run_mf(args: list[str]) -> str:
    """Run a MetricFlow CLI metadata command and return stdout."""
    result = subprocess.run(
        [_MF_BIN] + args,
        cwd=_DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mf {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def _parse_bullet_list(output: str) -> list[str]:
    """Extract '• value' lines from mf CLI output."""
    return [
        line.strip().lstrip("•").strip()
        for line in output.splitlines()
        if line.strip().startswith("•")
    ]


@functools.lru_cache(maxsize=1)
def get_metric_names() -> set[str]:
    """Return all metric names from MetricFlow (authoritative).

    mf list metrics outputs bullets in the format:
      • metric_name: dim1, dim2, ... and N more
    We extract only the part before the colon.
    """
    output = _run_mf(["list", "metrics"])
    names = set()
    for item in _parse_bullet_list(output):
        # Strip inline dimension summary: "total_revenue: lot__city, ..."
        name = item.split(":")[0].strip()
        if name:
            names.add(name)
    return names


@functools.lru_cache(maxsize=1)
def get_dimensions_for_metric(metric: str) -> list[str]:
    """Return valid dimension names for a given metric from MetricFlow."""
    output = _run_mf(["list", "dimensions", "--metrics", metric])
    # Exclude MetricFlow's internal metric_time dimension — not useful for grouping
    return [d for d in _parse_bullet_list(output) if d != "metric_time"]


@functools.lru_cache(maxsize=1)
def get_all_dimensions() -> set[str]:
    """Union of all dimensions across all metrics."""
    dims: set[str] = set()
    for metric in get_metric_names():
        dims.update(get_dimensions_for_metric(metric))
    return dims


@functools.lru_cache(maxsize=1)
def _load_descriptions() -> dict[str, str]:
    """Load metric descriptions from marts.yml (not exposed by mf CLI)."""
    with open(_MARTS_YML) as f:
        yml = yaml.safe_load(f)
    return {
        m["name"]: m.get("description", "").strip()
        for m in yml.get("metrics", [])
    }


@functools.lru_cache(maxsize=1)
def get_metrics() -> list[dict]:
    """
    Return structured metric list for UI rendering and LLM prompting.

    Each dict:
      name        str   — metric identifier (from MetricFlow)
      label       str   — human-readable title
      description str   — from marts.yml
      dims        list  — valid dimensions (from MetricFlow)
      example     str   — example question
    """
    descriptions = _load_descriptions()
    rows = []
    for name in sorted(get_metric_names()):
        rows.append({
            "name":        name,
            "label":       name.replace("_", " ").title(),
            "description": descriptions.get(name, ""),
            "dims":        get_dimensions_for_metric(name),
            "example":     _EXAMPLES.get(name, ""),
        })
    return rows


@functools.lru_cache(maxsize=1)
def build_llm_catalog() -> str:
    """
    Build the metric + dimension catalog string for Claude's system prompt.
    Sourced entirely from MetricFlow — always in sync with marts.yml.
    """
    lines = ["Available MetricFlow metrics and their valid dimensions:\n"]
    for i, m in enumerate(get_metrics(), 1):
        lines.append(f"{i}. {m['name']}")
        lines.append(f"   Definition: {m['description']}")
        lines.append(f"   Valid dimensions: {', '.join(m['dims'])}")
        if m["example"]:
            lines.append(f"   Example: \"{m['example']}\"")
        lines.append("")
    return "\n".join(lines)


def pretty_dim(dim: str) -> str:
    """'lot__market_type' → 'Market Type'"""
    if "__" in dim:
        dim = dim.split("__", 1)[1]
    return dim.replace("_", " ").title()
