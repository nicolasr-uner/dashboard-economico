import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from data.database import get_countries, get_variables, get_historical_data
from data.agent import VariableAgent

# Configuración de página
st.set_page_config(
    page_title="Dashboard Económico AI",
    page_icon="📈",
    layout="wide"
)

# Cargar caché para base de datos
@st.cache_data(ttl=600)
def load_countries():
    return get_countries()

@st.cache_data(ttl=600)
def load_variables(country_id=None):
    return get_variables(country_id)

@st.cache_data(ttl=600)
def load_history(variable_id):
    return get_historical_data(variable_id)

def main():
    st.title("📈 Modelación y Dashboard Económico")
    st.sidebar.header("Filtros")
    
    countries_df = load_countries()
    if countries_df.empty:
        st.warning("No hay países en la base de datos.")
        return
        
    country_opts = dict(zip(countries_df['name'], countries_df['id']))
    selected_country_name = st.sidebar.selectbox("Seleccione un País", options=list(country_opts.keys()))
    selected_country_id = country_opts[selected_country_name]
    
    variables_df = load_variables(selected_country_id)
    
    tab1, tab2, tab3 = st.tabs(["📊 Vista General", "🔮 Modelación", "⚙️ Agente de Datos"])
    
    with tab1:
        st.subheader(f"Indicadores de {selected_country_name}")
        if variables_df.empty:
            st.info("No hay variables configuradas para este país.")
        else:
            cols = st.columns(min(3, len(variables_df)))
            for idx, row in variables_df.iterrows():
                hist = load_history(row['id'])
                col_idx = idx % 3
                with cols[col_idx]:
                    if not hist.empty:
                        last_val = hist['value'].iloc[-1]
                        prev_val = hist['value'].iloc[-2] if len(hist) > 1 else last_val
                        delta = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
                        st.metric(label=f"{row['name']} ({row['unit']})", value=f"{last_val}", delta=f"{delta}%")
                        
                        fig = px.line(hist, x='date', y='value', title=f"Evolución {row['name']}")
                        fig.update_layout(height=200, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # --- NLA Context Module ---
                        try:
                            from models.db import engine
                            logs = pd.read_sql(f"SELECT * FROM ai_analysis_log WHERE variable_id={row['id']} ORDER BY analyzed_at DESC LIMIT 1", engine)
                            if not logs.empty:
                                last_log = logs.iloc[0]
                                with st.expander(f"Contexto NLA ({last_log['ai_verdict'].upper()})"):
                                    st.write(f"**Justificación:** {last_log['justification']}")
                                    if last_log.get('news_context'):
                                        st.caption(f"**Contexto Vectorial:**\n{last_log['news_context']}")
                        except Exception as e:
                            pass
                    else:
                        st.metric(label=f"{row['name']}", value="Sin datos")
                        
    with tab2:
        st.subheader("Modelación y Proyecciones")
        if not variables_df.empty:
            var_opts = dict(zip(variables_df['name'], variables_df['id']))
            sel_var_name = st.selectbox("Seleccione Variable a Modelar", options=list(var_opts.keys()))
            sel_var_id = var_opts[sel_var_name]
            
            hist_df = load_history(sel_var_id)
            if not hist_df.empty and len(hist_df) > 2:
                proj_df = VariableAgent.calculate_projection(hist_df, periods=6)
                
                # Combinar real y proyección para gráfica
                if 'data_type' in hist_df.columns:
                    hist_df['type'] = hist_df['data_type'].replace({'REAL_OFFICIAL': 'Real', 'PROJECTION': 'Proyección', 'ESTIMATION': 'Estimado'})
                else:
                    hist_df['type'] = 'Real'
                    
                merged = pd.concat([hist_df[['date', 'value', 'type']], proj_df])
                
                fig2 = px.line(merged, x='date', y='value', color='type', title=f"Proyección a 6 meses: {sel_var_name}")
                fig2.update_traces(line=dict(dash="dot"), selector=dict(name="Proyección"))
                st.plotly_chart(fig2, use_container_width=True)
                
                st.dataframe(proj_df, use_container_width=True)
            else:
                st.warning("No hay suficientes datos históricos para realizar una proyección.")

    with tab3:
        st.subheader("Agente de Ingestión")
        st.write("Actualiza las variables obteniendo el último dato disponible.")
        if not variables_df.empty:
            for idx, row in variables_df.iterrows():
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.write(f"**{row['name']}** - Última fuente: {row['source_url']}")
                with col_b:
                    if st.button(f"Actualizar", key=f"btn_{row['id']}"):
                        with st.spinner("Agente extrayendo datos..."):
                            res = VariableAgent.ingest_variable(row)
                            if res['success']:
                                st.success(res.get('message', 'Éxito'))
                                load_history.clear() # Limpiar caché
                            else:
                                st.error(res.get('error', 'Error desconocido'))
                st.divider()

if __name__ == "__main__":
    main()
