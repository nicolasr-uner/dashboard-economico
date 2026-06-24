import streamlit as st
import os
import pandas as pd
from datetime import datetime

from data.database import get_countries, get_variables, get_all_variable_names
from ui.components import load_css

st.set_page_config(
    page_title="Cerebro Económico NLA",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_css()

def render_sidebar_home():
    st.sidebar.markdown(
        "<div style='font-size:1.05em;font-weight:800;color:#1e3a8a;"
        "letter-spacing:-0.01em;margin-bottom:2px'>🧠 Cerebro Económico NLA</div>"
        "<div style='font-size:0.72em;color:#9ca3af;margin-bottom:8px'>"
        "Inteligencia macroeconómica multi-país</div>",
        unsafe_allow_html=True)
    
    st.sidebar.divider()
    
    # Fuentes status
    st.sidebar.markdown("<div class='sb-label'>Fuentes de datos</div>", unsafe_allow_html=True)
    _fred = bool(os.getenv("FRED_API_KEY"))
    _bnx  = bool(os.getenv("BANXICO_TOKEN"))
    st.sidebar.markdown(
        f"<div class='sb-src'>"
        f"{'✅' if True else '❌'} BCB &nbsp; {'✅' if True else '❌'} World Bank<br>"
        f"{'✅' if _fred else '⚠️'} FRED {'<small>(activo)</small>' if _fred else '<small>(sin FRED_API_KEY)</small>'}<br>"
        f"{'✅' if _bnx  else '⚠️'} Banxico {'<small>(activo)</small>' if _bnx else '<small>(sin BANXICO_TOKEN)</small>'}"
        f"</div>", unsafe_allow_html=True)

    st.sidebar.divider()
    dark = st.sidebar.toggle("🌙 Modo oscuro", value=st.session_state.get('dark_mode', False), key="dark_mode_toggle")
    st.session_state['dark_mode'] = dark
    import streamlit.components.v1 as _stcomponents
    _stcomponents.html(
        f"""<script>
        var b = window.parent.document.body;
        {"b.classList.add('dark-mode');" if dark else "b.classList.remove('dark-mode');"}
        </script>""", height=0)


def main():
    render_sidebar_home()
    
    st.markdown("<h1 style='text-align:center;font-weight:800'>🧠 Cerebro Económico Múlti-País NLA</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:gray;font-size:1.05em'>"
                "Plataforma automatizada de inteligencia macroeconómica · "
                "Colombia · México · Brasil · Ecuador</p>", unsafe_allow_html=True)
    st.divider()

    st.markdown("""
    ### Bienvenido al Panel Principal
    
    Usa la **barra lateral** para navegar entre los distintos módulos:
    
    - 📊 **Datos Macro:** Panel central con indicadores por país.
    - 🔮 **Proyecciones:** Análisis predictivo (Consenso vs ARIMA vs Entidades).
    - 🌍 **Contexto Global:** Commodities y comparativa regional.
    - ⚡ **Energía:** Mercados eléctricos.
    - 📚 **Data Hub:** Gestión y actualización de la biblioteca de variables.
    
    > **Nota Arquitectónica:** El dashboard ha sido actualizado para usar navegación Multipage, Supabase como base de datos, y está preparado para actualizarse automáticamente mediante integraciones externas.
    """)

if __name__ == "__main__":
    main()
