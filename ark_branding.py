
"""
ARK branding helpers for Streamlit.

Drop this file next to `ark_app.py` and import as:
    import ark_branding as ui
    brand = ui.load_brand()
    ui.inject_global_css(brand)
    ui.render_brand_header(brand, role="admin", user_name="Kyle")
    ui.render_timeline(schedule_df)  # or ui.render_timeline(schedule_df, employee="Dave")
"""

from __future__ import annotations

import json
import pathlib
from typing import Dict, Optional

import pandas as pd
import streamlit as st


# ---------- Brand config ----------

def load_brand(path: str = "branding.json") -> Dict:
    """
    Load brand settings (colors, name, optional logo) from a JSON file.
    Provides sensible defaults if the file doesn't exist.
    """
    p = pathlib.Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass  # fall through to defaults
    # Defaults tuned for ARK Furniture
    return {
        "brand_name": "ARK Furniture",
        "logo_url": "",  # optional
        "primary": "#7A5C3C",        # warm wood tone
        "accent": "#0B6E4F",         # deep green
        "bg": "#F8F6F2",             # off white
        "bg_secondary": "#FFFFFF",   # card background
        "text": "#1E1E1E",
        "muted": "#6B7280",
        "font": "Inter"
    }


def inject_global_css(brand: Dict, css_path: str = "styles.css") -> None:
    """
    Inject global CSS. If styles.css is missing, inject a small, safe fallback.
    """
    try:
        css = pathlib.Path(css_path).read_text(encoding="utf-8")
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
        return
    except Exception:
        pass

    # Fallback: minimal theming that works without external files
    primary = brand.get("primary", "#7A5C3C")
    accent = brand.get("accent", "#0B6E4F")
    bg = brand.get("bg", "#F8F6F2")
    bg2 = brand.get("bg_secondary", "#FFFFFF")
    text = brand.get("text", "#1E1E1E")
    font = brand.get("font", "Inter")

    css_fallback = f"""
    @import url('https://fonts.googleapis.com/css2?family={font.replace(' ', '+')}:wght@400;600;700&display=swap');
    html, body, .stApp {{
      background: {bg} !important;
      color: {text} !important;
      font-family: '{font}', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }}
    .stTabs [data-baseweb="tab"] {{
      background: {bg2};
      border: 1px solid #ececec;
      border-bottom: none;
      padding: 0.4rem 0.75rem;
      border-radius: 8px 8px 0 0;
    }}
    .stTabs [aria-selected="true"] {{
      color: {primary} !important;
      border-color: {primary};
    }}
    .stButton button {{
      background: {primary} !important;
      color: #fff !important;
      border: 0;
      border-radius: 8px;
      padding: 0.5rem 0.9rem;
    }}
    .ark-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.75rem 1rem;
      margin: 0 0 1rem 0;
      border-radius: 12px;
      background: linear-gradient(135deg, {primary} 0%, {accent} 100%);
      color: #fff;
    }}
    .ark-header .brand {{
      font-weight: 700;
      font-size: 1.25rem;
      letter-spacing: 0.3px;
    }}
    .ark-header .chip {{
      font-size: 0.8rem;
      padding: 0.25rem 0.6rem;
      border: 1px solid rgba(255,255,255,0.35);
      border-radius: 999px;
      background: rgba(255,255,255,0.15);
      color: #fff;
    }}
    """
    st.markdown(f"<style>{css_fallback}</style>", unsafe_allow_html=True)


def render_brand_header(brand: Dict, *, role: Optional[str] = None, user_name: Optional[str] = None) -> None:
    """
    Renders a compact, branded header with optional role and user name.
    If brand['logo_url'] is set, shows it to the left of the brand name.
    """
    title = brand.get("brand_name", "ARK")
    role_chip = f'<span class="chip">{role}</span>' if role else ""
    who = f'<span style="opacity:0.9;">{user_name}</span>' if user_name else ""
    logo_url = brand.get("logo_url", "").strip()

    if logo_url:
        logo_html = f'<img src="{logo_url}" alt="{title}" style="height:26px;margin-right:10px;border-radius:3px;" />'
    else:
        logo_html = ""

    html = f"""
    <div class="ark-header">
      <div style="display:flex;align-items:center;gap:.5rem;">
        {logo_html}
        <div class="brand">{title}</div>
      </div>
      <div>{who} {role_chip}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def _colorway_from_brand(brand: Dict):
    # Basic colorway using brand accents; Plotly will cycle through.
    primary = brand.get("primary", "#7A5C3C")
    accent = brand.get("accent", "#0B6E4F")
    muted = brand.get("muted", "#6B7280")
    return [primary, accent, "#AF7E5C", "#3D8361", "#C9ADA1", muted]


def render_timeline(df, *, employee: Optional[str] = None, title: Optional[str] = None) -> None:
    """
    Draw a compact Gantt/timeline using Plotly.

    Expected columns in df:
      "Start" (datetime-like or str), "End", "Assigned To", "Stage", "Customer", "Job"
    """
    if df is None or getattr(df, "empty", True):
        st.info("No schedule to plot yet.")
        return

    # Validate columns
    needed = ["Start", "End", "Assigned To", "Stage"]
    for c in needed:
        if c not in df.columns:
            st.info("Timeline requires columns: " + ", ".join(needed))
            return

    # Try to import plotly lazily
    try:
        import plotly.express as px
    except Exception:
        st.info("Plotly is not installed. Add `plotly>=5.22.0` to requirements.txt.")
        return

    data = df.copy()
    if employee:
        data = data[data["Assigned To"] == employee]

    # Coerce datetimes if needed
    try:
        data["Start"] = pd.to_datetime(data["Start"])
        data["End"] = pd.to_datetime(data["End"])
    except Exception:
        # Keep as-is if already OK
        pass

    colorway = _colorway_from_brand(load_brand())

    fig = px.timeline(
        data,
        x_start="Start",
        x_end="End",
        y="Assigned To",
        color="Stage",
        color_discrete_sequence=colorway,
        hover_data=[c for c in ["Customer", "Job"] if c in data.columns]
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        title=title or None,
        height=420,
        margin=dict(l=10, r=10, t=30, b=30),
        legend_title_text="Stage"
    )
    # Streamlit ≥1.51: use width='stretch' instead of use_container_width
    st.plotly_chart(fig, width="stretch")
