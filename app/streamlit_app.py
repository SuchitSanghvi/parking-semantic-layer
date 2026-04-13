"""
streamlit_app.py — Parking Asset Intelligence
Main entrypoint. Single-page app with horizontal tabs.

Run from project root:
  streamlit run app/streamlit_app.py
"""

import os
import subprocess
import sys

import streamlit as st

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

_DBT_PROJECT_DIR = os.path.join(_PROJECT_ROOT, "dbt_project")
_WAREHOUSE_PATH  = os.path.join(_DBT_PROJECT_DIR, "warehouse.duckdb")
_VENV_DBT        = os.path.join(_PROJECT_ROOT, "venv", "bin", "dbt")
_VENV_PYTHON     = os.path.join(_PROJECT_ROOT, "venv", "bin", "python")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Parking Asset Intelligence",
    page_icon="🅿️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Layout ──────────────────────────────────────────────────────── */
[data-testid="stSidebar"],
[data-testid="collapsedControl"] { display: none !important; }
#MainMenu, footer, header { visibility: hidden; }
.stApp { background: #f8fafc; }
.block-container { padding: 2rem 2.5rem 3rem !important; max-width: 1200px; }

/* ── App header ──────────────────────────────────────────────────── */
.app-header {
    display: flex; align-items: center; gap: 10px;
    padding-bottom: 1.2rem;
    border-bottom: 1px solid #e2e8f0;
    margin-bottom: 0.25rem;
}
.app-header-title { font-size: 1.25rem; font-weight: 700; color: #0f172a; margin: 0; }
.app-header-icon  { font-size: 1.5rem; }

/* ── Tabs ─────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0; border-bottom: 2px solid #e2e8f0;
    background: transparent; margin-bottom: 1.5rem;
}
.stTabs [data-baseweb="tab"] {
    padding: 10px 22px; font-size: 0.88rem;
    font-weight: 500; color: #64748b;
    background: transparent; border-radius: 0;
}
.stTabs [aria-selected="true"] { color: #2563eb; font-weight: 600; }
.stTabs [data-baseweb="tab-highlight"] { background-color: #2563eb !important; }
.stTabs [data-baseweb="tab-border"]    { display: none; }

/* ── KPI cards ────────────────────────────────────────────────────── */
.kpi-label { font-size: 0.72rem; font-weight: 600; color: #64748b;
             text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px; }
.kpi-value { font-size: 1.9rem; font-weight: 700; color: #0f172a; line-height: 1.1; }
.kpi-sub   { font-size: 0.72rem; color: #94a3b8; margin-top: 4px; }

/* ── Section labels ───────────────────────────────────────────────── */
.section-title { font-size: 1rem; font-weight: 600; color: #0f172a; margin-bottom: 2px; }
.section-sub   { font-size: 0.78rem; color: #64748b; margin-bottom: 1rem; }

/* ── Top performers list ──────────────────────────────────────────── */
.lot-row {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 0; border-bottom: 1px solid #f1f5f9;
}
.lot-left   { display: flex; align-items: center; gap: 12px; }
.lot-name   { font-weight: 600; font-size: 0.88rem; color: #0f172a; }
.lot-meta   { font-size: 0.73rem; color: #94a3b8; margin-top: 1px; }
.lot-right  { text-align: right; }
.lot-rev    { font-weight: 700; font-size: 0.9rem; color: #0f172a; }
.lot-badge  { font-size: 0.7rem; color: #16a34a; font-weight: 600; margin-top: 2px; }

/* ── Dim badge (metrics reference) ───────────────────────────────── */
.dim-badge {
    display: inline-block; background: #eff6ff; color: #2563eb;
    border: 1px solid #bfdbfe; border-radius: 4px;
    padding: 2px 9px; margin: 2px 2px;
    font-size: 0.73rem; font-weight: 500;
}

/* ── Multiselect tags — blue not red ─────────────────────────────── */
[data-baseweb="tag"] {
    background-color: #eff6ff !important;
    border: 1px solid #bfdbfe !important;
}
[data-baseweb="tag"] span { color: #2563eb !important; }
[data-baseweb="tag"] button svg { fill: #2563eb !important; }

/* ── Example question chips ───────────────────────────────────────── */
div[data-testid="stHorizontalBlock"] button {
    border: 1px solid #e2e8f0 !important;
    border-radius: 20px !important;
    background: #ffffff !important;
    color: #374151 !important;
    font-size: 0.82rem !important;
    padding: 6px 14px !important;
}
div[data-testid="stHorizontalBlock"] button:hover {
    border-color: #2563eb !important;
    color: #2563eb !important;
}
</style>
""", unsafe_allow_html=True)


# ── Warehouse bootstrap ───────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def build_warehouse():
    if os.path.exists(_WAREHOUSE_PATH):
        return
    with st.spinner("Building data warehouse — first load only, ~20 seconds…"):
        import shutil
        subprocess.run([_VENV_PYTHON,
                        os.path.join(_PROJECT_ROOT, "generate_data", "generate_raw_data.py")],
                       cwd=_PROJECT_ROOT, check=True)
        data_dir  = os.path.join(_PROJECT_ROOT, "data")
        seeds_dir = os.path.join(_DBT_PROJECT_DIR, "seeds")
        for f in ("raw_parking_events.csv", "raw_lots.csv", "raw_local_events.csv"):
            src = os.path.join(data_dir, f)
            if os.path.exists(src):
                shutil.copy2(src, seeds_dir)
        subprocess.run([_VENV_DBT, "seed", "--threads", "1"],
                       cwd=_DBT_PROJECT_DIR, check=True)
        subprocess.run([_VENV_DBT, "run",  "--threads", "1"],
                       cwd=_DBT_PROJECT_DIR, check=True)

build_warehouse()

# ── App header ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
  <span class="app-header-icon">🅿️</span>
  <span class="app-header-title">Parking Asset Intelligence</span>
</div>
""", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
from app.pages._dashboard import render as render_dashboard
from app.pages._ask_data  import render as render_ask_data
from app.pages._about     import render as render_about

tab1, tab2, tab3 = st.tabs(["Dashboard", "Ask Your Data", "About"])

with tab1:
    render_dashboard()

with tab2:
    render_ask_data()

with tab3:
    render_about()
