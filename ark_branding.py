
"""
ARK UI helpers for Streamlit: branding, accessible styles, timelines, and card/table renderers.
Place this file next to `ark_app.py` and import as:
    import ark_branding as ui
    brand = ui.load_brand()
    ui.inject_global_css(brand)
    ui.render_brand_header(brand, role="admin", user_name="Kyle")
    ui.render_timeline(schedule_df)  # or ui.render_timeline(schedule_df, employee="Dave")
    ui.render_cards(df, title="job", subtitle=["customer","service"], meta=[("Qty","qty"),("Stage","stage_completed")])
"""

from __future__ import annotations

import json
import pathlib
from typing import Dict, List, Optional, Sequence, Tuple, Union

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
        "muted": "#4B5563",
        "font": "Inter"
    }


def inject_global_css(brand: Dict, css_path: str = "styles.css") -> None:
    """
    Inject global CSS. If styles.css is missing, inject a small, accessible fallback.
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
    muted = brand.get("muted", "#4B5563")
    font = brand.get("font", "Inter")

    css_fallback = f"""
    @import url('https://fonts.googleapis.com/css2?family={font.replace(' ', '+')}:wght@400;600;700&display=swap');
    :root {{
      --ark-primary: {primary};
      --ark-accent: {accent};
      --ark-bg: {bg};
      --ark-bg2: {bg2};
      --ark-text: {text};
      --ark-muted: {muted};
    }}
    html, body, .stApp {{
      background: var(--ark-bg) !important;
      color: var(--ark-text) !important;
      font-family: '{font}', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }}
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
      gap: 0.25rem;
      padding-bottom: 0.25rem;
      border-bottom: 1px solid #e5e7eb;
    }}
    .stTabs [data-baseweb="tab"] {{
      background: var(--ark-bg2);
      border: 1px solid #ececec;
      border-bottom: none;
      padding: 0.45rem 0.8rem;
      border-radius: 8px 8px 0 0;
      color: var(--ark-text) !important;
    }}
    .stTabs [aria-selected="true"] {{
      background: #fff !important;
      color: var(--ark-primary) !important;
      border-color: var(--ark-primary) !important;
    }}
    /* Buttons */
    .stButton button, div[data-testid="stDownloadButton"] button {{
      background: var(--ark-primary) !important;
      color: #fff !important;
      border: 0;
      border-radius: 10px;
      padding: 0.55rem 0.95rem;
      font-weight: 600;
    }}
    .stButton button:hover, div[data-testid="stDownloadButton"] button:hover {{
      filter: brightness(1.05);
    }}
    /* Inputs & labels */
    label, .stMarkdown p, .stSelectbox label, .stTextInput label, .stNumberInput label {{
      color: var(--ark-text) !important;
    }}
    /* Header card */
    .ark-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: .5rem;
      padding: 0.85rem 1rem;
      margin: 0 0 1rem 0;
      border-radius: 14px;
      background: linear-gradient(135deg, var(--ark-primary) 0%, var(--ark-accent) 100%);
      color: #fff;
    }}
    .ark-header .brand {{
      font-weight: 700;
      font-size: 1.25rem;
      letter-spacing: .2px;
    }}
    .ark-header .chip {{
      font-size: .8rem;
      padding: .25rem .6rem;
      border: 1px solid rgba(255,255,255,.35);
      border-radius: 999px;
      background: rgba(255,255,255,.15);
      color: #fff;
    }}
    /* Card list */
    .ark-card {{
      background: var(--ark-bg2);
      border: 1px solid #e9e9e9;
      border-radius: 12px;
      padding: 0.9rem;
      margin-bottom: 0.6rem;
      box-shadow: 0 1px 1px rgba(0,0,0,.02);
    }}
    .ark-card .title {{
      font-weight: 700;
      margin-bottom: .25rem;
      color: var(--ark-text);
    }}
    .ark-card .subtitle {{
      color: var(--ark-muted);
      margin-bottom: .35rem;
    }}
    .ark-meta {{
      display: flex; flex-wrap: wrap; gap: .4rem;
    }}
    .ark-badge {{
      display: inline-block;
      background: var(--ark-primary);
      color: #fff;
      padding: .15rem .5rem;
      border-radius: 999px;
      font-size: .75rem;
      font-weight: 600;
    }}
    /* Tables - soften look */
    div[data-testid="stDataFrame"] .row_heading, div[data-testid="stDataFrame"] .blank {{
      display: none !important; /* hide raw index */
    }}
    div[data-testid="stDataFrame"] {{ border: 1px solid #eee; border-radius: 10px; }}
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
    muted = brand.get("muted", "#4B5563")
    return [primary, accent, "#AF7E5C", "#3D8361", "#C9ADA1", muted]


def render_timeline(df, *, employee: Optional[str] = None, title: Optional[str] = None) -> None:
    """
    Draw a compact Gantt/timeline using Plotly.
    Expected columns in df: "Start", "End", "Assigned To", "Stage" (plus "Customer", "Job" optional)
    """
    if df is None or getattr(df, "empty", True):
        st.info("No schedule to plot yet.")
        return

    # Validate columns
    needed = ["Start", "End", "Assigned To", "Stage"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
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


# ---------- "Less spreadsheet-y" renderers ----------

def render_cards(
    df: pd.DataFrame,
    *,
    title: Optional[str] = None,
    subtitle: Optional[Sequence[str]] = None,
    meta: Optional[Sequence[Union[str, Tuple[str, str]]]] = None,
    empty_text: str = "Nothing to show yet."
) -> None:
    """
    Render each row of `df` as a simple card.
      - title: column to emphasize at top (str; e.g., "job" or "customer")
      - subtitle: list of columns to show under title (joined with " · ")
      - meta: list of columns or (label, column) tuples rendered as small badges
    """
    if df is None or df.empty:
        st.info(empty_text)
        return

    # Normalize meta spec to (label, col) tuples
    meta_spec: List[Tuple[str, str]] = []
    if meta:
        for m in meta:
            if isinstance(m, tuple):
                meta_spec.append((m[0], m[1]))
            else:
                meta_spec.append((m, m))

    for _, row in df.iterrows():
        title_txt = str(row[title]) if title and title in df.columns else ""
        subtitle_vals = []
        if subtitle:
            for s in subtitle:
                if s in df.columns and pd.notna(row[s]):
                    subtitle_vals.append(str(row[s]))
        subtitle_txt = " · ".join(subtitle_vals)

        badges = []
        for label, col in meta_spec:
            if col in df.columns and pd.notna(row[col]):
                badges.append(f'<span class="ark-badge">{label}: {row[col]}</span>')
        badges_html = " ".join(badges)

        block = f"""
        <div class="ark-card">
          <div class="title">{title_txt}</div>
          <div class="subtitle">{subtitle_txt}</div>
          <div class="ark-meta">{badges_html}</div>
        </div>
        """
        st.markdown(block, unsafe_allow_html=True)


def nice_table(
    df: pd.DataFrame,
    *,
    editable: bool = False,
    hide_index: bool = True,
    height: int = 300,
    column_config: Optional[Dict[str, object]] = None
) -> Optional[pd.DataFrame]:
    """
    A gentler table wrapper around st.data_editor with softened look.
    - editable=False by default to prevent accidental edits
    - hide_index=True hides numeric index
    Returns the possibly-edited dataframe if editable=True, else None.
    """
    if df is None or df.empty:
        st.info("No rows yet.")
        return None

    # Streamlit will render a cleaner-looking editor than raw dataframe
    changed = st.data_editor(
        df,
        disabled=not editable,
        hide_index=hide_index,
        height=height,
        column_config=column_config or {},
    )
    return changed if editable else None
