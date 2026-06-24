import streamlit as st
import pandas as pd
from ui.components import load_css, render_bloomberg_card
from data.database import get_countries, get_variables, get_historical_data

st.set_page_config(page_title="Datos Macro", page_icon="📊", layout="wide")
load_css()

def render_sidebar():
    countries_df = get_countries()
    if countries_df.empty:
        st.error("⚠️ Error de base de datos.")
        st.stop()
        
    country_opts  = dict(zip(countries_df['name'], countries_df['id']))
    country_names = list(country_opts.keys())
    FLAG = {"Colombia":"🇨🇴","México":"🇲🇽","Brasil":"🇧🇷","Ecuador":"🇪🇨"}
    
    st.sidebar.markdown("<div class='sb-label'>País principal</div>", unsafe_allow_html=True)
    sel_idx = st.sidebar.selectbox("País", range(len(country_names)), index=0, 
                                   format_func=lambda i: f"{FLAG.get(country_names[i],'🌍')} {country_names[i]}", label_visibility="collapsed")
    name = country_names[sel_idx]
    
    # Modo oscuro
    dark = st.sidebar.toggle("🌙 Modo oscuro", value=st.session_state.get('dark_mode', False), key="dark_mode_toggle_p1")
    st.session_state['dark_mode'] = dark
    import streamlit.components.v1 as _stcomponents
    _stcomponents.html(f"<script>var b = window.parent.document.body; {'b.classList.add(`dark-mode`);' if dark else 'b.classList.remove(`dark-mode`);'}</script>", height=0)
    
    return name, country_opts[name]

def main():
    st.title("📊 Datos Macroeconómicos")
    sel_name, sel_id = render_sidebar()
    variables_df = get_variables(sel_id)

    if variables_df.empty:
        st.info("No hay variables configuradas para este país.")
        return

    c_cat, c_dens, c_info = st.columns([2, 1, 2])
    with c_cat:
        cats = ['Todas'] + sorted(variables_df['category'].dropna().unique().tolist()) if 'category' in variables_df.columns else ['Todas']
        sel_cat = st.selectbox("Filtrar por categoría", cats, key="t1_cat")
    with c_dens:
        compact_mode = st.toggle("Vista compacta", value=False, key="compact_toggle")
    with c_info:
        st.info("📅 Cada tarjeta tiene su propio selector de rango.")

    n_cols = 4 if compact_mode else 3
    filtered = variables_df if sel_cat == 'Todas' else variables_df[variables_df['category'] == sel_cat] if 'category' in variables_df.columns else variables_df

    SECS = {
        "🌐 Sector Externo":      ['external','fx_rates'],
        "📈 Inflación y Tasas":   ['prices_inflation','rates_monetary','macro'],
        "🏭 Actividad Económica": ['gdp_activity'],
    }
    
    if sel_cat == 'Todas' and 'category' in variables_df.columns:
        mapped = [c for cl in SECS.values() for c in cl]
        for stitle, scats in SECS.items():
            sv = variables_df[variables_df['category'].isin(scats)]
            if sv.empty: continue
            st.subheader(stitle)
            cols = st.columns(min(n_cols, len(sv)))
            for i, (_, row) in enumerate(sv.iterrows()):
                with cols[i % n_cols]: 
                    render_bloomberg_card(row, get_historical_data(row['id']), "sec", compact_mode)
        
        ov = variables_df[~variables_df['category'].isin(mapped)]
        if not ov.empty:
            st.subheader("📌 Otros Indicadores")
            cols = st.columns(min(n_cols, len(ov)))
            for i, (_, row) in enumerate(ov.iterrows()):
                with cols[i % n_cols]: 
                    render_bloomberg_card(row, get_historical_data(row['id']), "oth", compact_mode)
    else:
        if len(filtered) > 0:
            cols = st.columns(min(n_cols, len(filtered)))
            for i, (_, row) in enumerate(filtered.iterrows()):
                with cols[i % n_cols]: 
                    render_bloomberg_card(row, get_historical_data(row['id']), "flt", compact_mode)
        else: 
            st.info("No hay variables en esta categoría.")

if __name__ == "__main__":
    main()
