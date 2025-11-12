
import json, pathlib
import streamlit as st

def load_brand(path="branding.json"):
    p = pathlib.Path(path)
    if p.exists():
        return json.loads(p.read_text())
    # fallback defaults
    return {
        "brand_name": "ARK Furniture",
        "logo_url": "",
        "primary": "#7A5C3C",
        "accent": "#0B6E4F",
        "bg": "#F8F6F2",
        "bg_secondary": "#FFFFFF",
        "text": "#1E1E1E",
        "muted": "#6B7280",
        "font": "Inter"
    }

def inject_global_css(brand, css_path="styles.css"):
    try:
        css = pathlib.Path(css_path).read_text()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    except Exception:
        pass

def render_brand_header(brand, role=None, user_name=None):
    title = brand.get("brand_name","ARK")
    chip = f'<span class="chip">{role or ""}</span>' if role else ""
    who = f'<span style="opacity:0.85;">{user_name or ""}</span>' if user_name else ""
    html = f'''
    <div class="ark-header">
      <div class="brand">{title}</div>
      <div>{who} {chip}</div>
    </div>
    '''
    st.markdown(html, unsafe_allow_html=True)

def render_timeline(df, employee=None):
    try:
        import plotly.express as px
    except Exception as e:
        st.info("Plotly is not installed; install plotly in requirements.txt for the timeline view.")
        return
    if df is None or df.empty: 
        st.info("No schedule to plot."); return
    cols = ["Start","End","Assigned To","Stage","Customer","Job"]
    for c in cols:
        if c not in df.columns:
            st.info("Schedule missing required columns for timeline."); return
    data = df.copy()
    if employee:
        data = data[data["Assigned To"]==employee]
    fig = px.timeline(
        data,
        x_start="Start",
        x_end="End",
        y="Assigned To",
        color="Stage",
        hover_data=["Customer","Job","Piece Type","Qty"] if "Piece Type" in data.columns else ["Customer","Job","Qty"],
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        height=420,
        margin=dict(l=10,r=10,t=30,b=30),
        legend_title_text="Stage"
    )
    st.plotly_chart(fig, use_container_width=True)
