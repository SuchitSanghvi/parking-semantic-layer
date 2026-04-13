"""About tab — renders README.md from the repo with a GitHub link."""

import os
import re
import streamlit as st

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_README_PATH  = os.path.join(_PROJECT_ROOT, "README.md")
_GITHUB_URL   = "https://github.com/SuchitSanghvi/parking-asset-intelligence"


def _strip_screenshots(content: str) -> str:
    """Remove local <img> tags that won't resolve in Streamlit Cloud."""
    return re.sub(r'<img\s+src="docs/screenshots/[^"]*"[^/]*/>', "", content)


def render():
    # GitHub link at top
    st.markdown(
        f"<div style='margin-bottom:1.2rem'>"
        f"<a href='{_GITHUB_URL}' target='_blank' style='font-size:0.88rem;"
        f"color:#2563eb;text-decoration:none;font-weight:500'>"
        f"⭐ View on GitHub &rarr;</a></div>",
        unsafe_allow_html=True,
    )

    try:
        with open(_README_PATH, "r") as f:
            content = f.read()
        # Remove local image tags -- they won't resolve inside Streamlit
        content = _strip_screenshots(content)
        st.markdown(content, unsafe_allow_html=True)
    except FileNotFoundError:
        st.error("README.md not found.")
