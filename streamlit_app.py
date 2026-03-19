import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import io

from data.database import (
    get_countries, get_variables, get_historical_data,
    get_ai_logs, get_all_variable_names, get_variables_by_name
)
from data.agent import VariableAgent

# ── Configuración de página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Cerebro Económico NLA",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_ebar="expanded" if False else "auto",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .stMetric { background: white; padding: 12px 16px; border-radius: 10px;
               box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
  .badge-api   { background:#d1fae5; color:#065f46; padding:2px 8px; border-radius:12px; font-size:0.75em; font-weight:600; }
  .badge-scraper { background:#dbeafe; color:#1e40af; padding:2px 8px; border-radius:12px; font-size:0.75em; font-weight:600; }
  .badge-manual  { background:#fef3c7; color:#92400e; padding:2px 8px; border-radius:12px; font-size:0.75em; font-weight:600; }
  h1 { color: #1e3a8a; }
</style>
""", unsafe_allow_html=True)

# ── Cachés ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_countries():
    return get_countries()

@st.cache_data(ttl=600)
def load_variables(country_id=None):
    return get_variables(country_id)

@st.cache_data(ttl=600)
def load_history(variable_id):
    return get_historical_data(variable_id)

@st.cache_data(ttl=600)
def load_all_variables():
    return get_variables()

# ── Helpers ──────────────────────────────────────────────────────────────────
def badge_html(connector_type: str) -> str:
    ct = (connector_type or 'SCRAPER').upper()
    cls = {'API': 'badge-api', 'SCRAPER': 'badge-scraper', 'MANUAL': 'badge-manual'}.get(ct, 'badge-scraper')
    return f'<span class="{cls}">{ct}</span>'


def render_metric_with_history(row, hist):
    """Renderiza métrica + minichart en columna."""
    if not hist.empty:
        last_val = hist['value'].iloc[-1]
        prev_val = hist['value'].iloc[-2] if len(hist) > 1 else last_val
        delta = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
        ct = row.get('connector_type', 'SCRAPER') if hasattr(row, 'get') else 'SCRAPER'
        st.markdown(badge_html(ct), unsafe_allow_html=True)
        st.metric(
            label=f"{row['name']} ({row.get('unit','') or ''})",
            value=f"{last_val:,.3g}",
            delta=f"{delta}%"
        )
        fig = px.line(hist, x='date', y='value')
        fig.update_layout(height=150, margin=dict(l=0, r=0, t=5, b=0),
                          showlegend=False,
                          xaxis=dict(showticklabels=False, showgrid=False),
                          yaxis=dict(showticklabels=True, showgrid=True))
        fig.update_traces(line_color='#3b82f6', line_width=2)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.metric(label=f"{row['name']}", value="Sin datos")


# ── Función principal ─────────────────────────────────────────────────────────
def main():
    st.markdown(
        "<h1 style='text-align:center;font-weight:800;'>🧠 Cerebro Económico Múlti-País NLA</h1>",
        unsafe_allow_html=True
    )
    st.markdown(
        "<p style='text-align:center;color:gray;font-size:1.05em;'>"
        "Plataforma automatizada de inteligencia macroeconómica • Colombia • México • Brasil • Ecuador</p>",
        unsafe_allow_html=True
    )
    st.divider()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    st.sidebar.header("🌎 Filtros Globales")
    countries_df = load_countries()
    if countries_df.empty:
        st.warning("No hay países en la base de datos. Ejecuta `python scripts/seed_variables_v2.py`.")
        return

    country_opts = dict(zip(countries_df['name'], countries_df['id']))
    selected_country_name = st.sidebar.selectbox("País principal", options=list(country_opts.keys()))
    selected_country_id = country_opts[selected_country_name]
    variables_df = load_variables(selected_country_id)

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab1, tab_energy, tab_comp, tab_proj, tab_data, tab_agent = st.tabs([
        "📊 Vista General",
        "⚡ Sector Energético",
        "🌎 Comparativa Regional",
        "🔮 Proyecciones",
        "📋 Datos y Exportación",
        "⚙️ Agente de Datos"
    ])

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — Vista General
    # ════════════════════════════════════════════════════════════════════════
    with tab1:
        st.subheader(f"📊 Indicadores de {selected_country_name}")

        if variables_df.empty:
            st.info("No hay variables configuradas para este país.")
        else:
            # Filtro por categoría
            cats = ['Todas'] + sorted(variables_df['category'].dropna().unique().tolist()) \
                if 'category' in variables_df.columns else ['Todas']
            sel_cat = st.selectbox("Filtrar por categoría", cats, key="t1_cat")

            filtered_vars = variables_df
            if sel_cat != 'Todas' and 'category' in variables_df.columns:
                filtered_vars = variables_df[variables_df['category'] == sel_cat]

            cols = st.columns(min(3, len(filtered_vars)))
            for idx, (_, row) in enumerate(filtered_vars.iterrows()):
                hist = load_history(row['id'])
                col_idx = idx % 3
                with cols[col_idx]:
                    render_metric_with_history(row, hist)

                    # NLA Context
                    try:
                        logs = get_ai_logs(row['id'])
                        if not logs.empty:
                            last_log = logs.iloc[0]
                            verdict = str(last_log.get('ai_verdict', '')).upper()
                            with st.expander(f"Contexto NLA ({verdict})"):
                                st.write(f"**Justificación:** {last_log.get('justification','')}")
                                if last_log.get('news_context'):
                                    st.caption(f"**Contexto Vectorial:**\n{last_log['news_context']}")
                    except Exception:
                        pass

    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — Sector Energético
    # ════════════════════════════════════════════════════════════════════════
    with tab_energy:
        st.subheader("⚡ Sector Energético")
        st.markdown("Variables del mercado eléctrico mayorista de Colombia y commodities energéticos globales.")

        all_vars = load_all_variables()
        if all_vars.empty or 'category' not in all_vars.columns:
            st.info("Variables de energía pendientes de carga. Ejecute el agente de datos o el backfill.")
        else:
            energy_vars = all_vars[all_vars['category'] == 'energy']
            if energy_vars.empty:
                st.info(
                    "Variables de energía pendientes de carga. "
                    "Ejecute `python scripts/seed_variables_v2.py` y luego `python scripts/backfill.py`."
                )
            else:
                # Recopilar datos energéticos
                energy_data = {}
                for _, erow in energy_vars.iterrows():
                    h = load_history(erow['id'])
                    if not h.empty:
                        energy_data[erow['name']] = {'df': h, 'unit': erow.get('unit', ''), 'id': erow['id']}

                if not energy_data:
                    st.info(
                        "Variables energéticas sin datos aún. "
                        "Ejecute `python scripts/backfill.py` para cargar datos históricos."
                    )
                else:
                    # KPI resumen
                    kpi_cols = st.columns(min(4, len(energy_data)))
                    for ki, (vname, vinfo) in enumerate(list(energy_data.items())[:4]):
                        h = vinfo['df']
                        last_val = h['value'].iloc[-1]
                        prev_val = h['value'].iloc[-2] if len(h) > 1 else last_val
                        delta = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
                        kpi_cols[ki % 4].metric(
                            label=f"{vname} ({vinfo['unit']})",
                            value=f"{last_val:,.3g}",
                            delta=f"{delta}%"
                        )

                    st.divider()

                    # Precio Bolsa vs Mc (si existen ambos)
                    bolsa_key = next((k for k in energy_data if 'Bolsa' in k or 'PrecBol' in k.lower()), None)
                    mc_key = next((k for k in energy_data if 'Mc' in k or 'contratos' in k.lower()), None)

                    if bolsa_key and mc_key:
                        st.markdown("#### 📈 Precio de Bolsa vs Índice Mc")
                        fig_bm = go.Figure()
                        fig_bm.add_trace(go.Scatter(
                            x=energy_data[bolsa_key]['df']['date'],
                            y=energy_data[bolsa_key]['df']['value'],
                            name="Precio Bolsa", line=dict(color='#f59e0b', width=2)
                        ))
                        fig_bm.add_trace(go.Scatter(
                            x=energy_data[mc_key]['df']['date'],
                            y=energy_data[mc_key]['df']['value'],
                            name="Índice Mc", line=dict(color='#6366f1', width=2, dash='dash')
                        ))
                        fig_bm.update_layout(height=300, hovermode='x unified',
                                             yaxis_title="COP/kWh",
                                             legend=dict(orientation='h', y=1.1))
                        st.plotly_chart(fig_bm, use_container_width=True)

                    # Demanda y Solar
                    for label, keywords in [("Demanda Nacional", ['Demanda', 'DemaNal']),
                                            ("Generación Solar", ['Solar', 'Gene'])]:
                        key = next((k for k in energy_data for kw in keywords if kw.lower() in k.lower()), None)
                        if key:
                            st.markdown(f"#### {label}")
                            fig_e = px.area(energy_data[key]['df'], x='date', y='value',
                                            title=f"{key} ({energy_data[key]['unit']})")
                            fig_e.update_layout(height=250)
                            st.plotly_chart(fig_e, use_container_width=True)

                    # Aportes Hídricos
                    aporte_key = next((k for k in energy_data if 'Aporte' in k or 'Hídr' in k), None)
                    if aporte_key:
                        h_ap = energy_data[aporte_key]['df']
                        st.markdown("#### 💧 Aportes Hídricos (% media histórica)")
                        last_ap = h_ap['value'].iloc[-1]
                        color = "🔴" if last_ap < 70 else ("🟡" if last_ap < 90 else "🟢")
                        st.metric(f"Aportes actuales {color}", f"{last_ap:.1f}%")
                        if last_ap < 70:
                            st.warning("Nivel bajo de aportes hídricos — esperar presión alcista en precios de bolsa.")
                        fig_ap = px.line(h_ap, x='date', y='value',
                                         title="Aportes Hídricos Energéticos (% media histórica)")
                        fig_ap.add_hline(y=100, line_dash="dash", line_color="gray",
                                         annotation_text="Media histórica")
                        fig_ap.update_layout(height=250)
                        st.plotly_chart(fig_ap, use_container_width=True)

                    # WTI y Henry Hub
                    wti_key = next((k for k in energy_data if 'WTI' in k or 'Crude' in k), None)
                    hh_key = next((k for k in energy_data if 'Henry' in k or 'Gas' in k), None)
                    if wti_key or hh_key:
                        st.markdown("#### 🛢️ Commodities Globales")
                        fig_c = go.Figure()
                        if wti_key:
                            fig_c.add_trace(go.Scatter(
                                x=energy_data[wti_key]['df']['date'],
                                y=energy_data[wti_key]['df']['value'],
                                name="WTI Crude Oil (USD/bbl)", line=dict(color='#dc2626')
                            ))
                        if hh_key:
                            fig_c.add_trace(go.Scatter(
                                x=energy_data[hh_key]['df']['date'],
                                y=energy_data[hh_key]['df']['value'],
                                name="Henry Hub (USD/MMBtu)", line=dict(color='#0891b2'),
                                yaxis='y2'
                            ))
                            fig_c.update_layout(yaxis2=dict(overlaying='y', side='right'))
                        fig_c.update_layout(height=280, hovermode='x unified',
                                            legend=dict(orientation='h', y=1.1))
                        st.plotly_chart(fig_c, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — Comparativa Regional
    # ════════════════════════════════════════════════════════════════════════
    with tab_comp:
        st.subheader("🌎 Comparativa Macro Regional")
        st.markdown("Cruza y correlaciona el rendimiento de métricas clave a lo largo de América Latina.")

        all_names_df = get_all_variable_names()
        if all_names_df.empty:
            st.warning("No hay variables definidas.")
        else:
            var_names = all_names_df['name'].tolist()

            # Filtro por categoría
            all_vars_full = load_all_variables()
            cat_opts = ['Todas']
            if not all_vars_full.empty and 'category' in all_vars_full.columns:
                cat_opts += sorted(all_vars_full['category'].dropna().unique().tolist())
            sel_cat_comp = st.selectbox("Categoría", cat_opts, key="t3_cat")

            if sel_cat_comp != 'Todas' and not all_vars_full.empty and 'category' in all_vars_full.columns:
                filtered_names = all_vars_full[all_vars_full['category'] == sel_cat_comp]['name'].unique().tolist()
                var_names = [n for n in var_names if n in filtered_names]

            if not var_names:
                st.info("No hay variables en esa categoría.")
            else:
                selected_var_name = st.selectbox("Seleccione el Indicador a cruzar", var_names)
                vars_to_compare = get_variables_by_name(selected_var_name)

                compare_data = []
                for _, v_row in vars_to_compare.iterrows():
                    h_df = load_history(v_row['id'])
                    if not h_df.empty:
                        h_df['value'] = pd.to_numeric(h_df['value'], errors='coerce')
                        h_df['País'] = v_row['country']
                        compare_data.append(h_df)

                if compare_data:
                    combined_df = pd.concat(compare_data, ignore_index=True)
                    fig_comp = px.line(
                        combined_df, x='date', y='value', color='País', markers=True,
                        title=f"Evolución Histórica Cruzada: {selected_var_name}"
                    )
                    fig_comp.update_layout(height=420, hovermode="x unified")
                    st.plotly_chart(fig_comp, use_container_width=True)

                    st.divider()
                    st.subheader("📊 Ranking — Último Dato")
                    cols_comp = st.columns(len(compare_data))
                    for i, df_c in enumerate(compare_data):
                        country = df_c['País'].iloc[0]
                        current_val = df_c.iloc[-1]['value']
                        prev_val = df_c.iloc[-2]['value'] if len(df_c) > 1 else current_val
                        delta_comp = round(((current_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
                        cols_comp[i].metric(label=country, value=f"{current_val:,.3g}", delta=f"{delta_comp}%")
                else:
                    st.info("No hay datos históricos para comparar este indicador.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 4 — Proyecciones
    # ════════════════════════════════════════════════════════════════════════
    with tab_proj:
        st.subheader("🔮 Proyecciones y Consenso de Analistas")

        if variables_df.empty:
            st.info("No hay variables para el país seleccionado.")
        else:
            var_opts = dict(zip(variables_df['name'], variables_df['id']))
            sel_var_name = st.selectbox("Variable a modelar", list(var_opts.keys()))
            sel_var_id = var_opts[sel_var_name]
            hist_df = load_history(sel_var_id)

            # ── Sección 1: Proyección del modelo ──────────────────────────
            st.markdown("#### 📈 Proyección del Modelo")
            if not hist_df.empty and len(hist_df) > 2:
                proj_result = VariableAgent.calculate_projection(hist_df, periods=6)

                if not proj_result.empty:
                    # Preparar datos históricos
                    plot_hist = hist_df[['date', 'value']].copy()
                    plot_hist['type'] = hist_df.get('data_type', pd.Series(['Real'] * len(hist_df)))
                    plot_hist['type'] = plot_hist['type'].replace(
                        {'REAL_OFFICIAL': 'Real', 'PROJECTION': 'Proyección', 'ESTIMATION': 'Estimado'}
                    )

                    fig_proj = go.Figure()

                    # Banda 95%
                    if 'lower_95' in proj_result.columns and 'upper_95' in proj_result.columns:
                        x_band = list(proj_result['date']) + list(reversed(list(proj_result['date'])))
                        y_band = list(proj_result['upper_95']) + list(reversed(list(proj_result['lower_95'])))
                        fig_proj.add_trace(go.Scatter(
                            x=x_band, y=y_band, fill='toself',
                            fillcolor='rgba(59,130,246,0.1)', line=dict(color='rgba(255,255,255,0)'),
                            name='IC 95%', showlegend=True
                        ))

                    # Banda 80%
                    if 'lower_80' in proj_result.columns and 'upper_80' in proj_result.columns:
                        x_band80 = list(proj_result['date']) + list(reversed(list(proj_result['date'])))
                        y_band80 = list(proj_result['upper_80']) + list(reversed(list(proj_result['lower_80'])))
                        fig_proj.add_trace(go.Scatter(
                            x=x_band80, y=y_band80, fill='toself',
                            fillcolor='rgba(59,130,246,0.2)', line=dict(color='rgba(255,255,255,0)'),
                            name='IC 80%', showlegend=True
                        ))

                    # Histórico
                    fig_proj.add_trace(go.Scatter(
                        x=plot_hist['date'], y=plot_hist['value'],
                        name='Histórico', line=dict(color='#1e3a8a', width=2)
                    ))
                    # Proyección
                    fig_proj.add_trace(go.Scatter(
                        x=proj_result['date'], y=proj_result['value'],
                        name=f"Proyección ({proj_result.get('model_name', ['Ensemble']).iloc[0] if 'model_name' in proj_result.columns else 'Ensemble'})",
                        line=dict(color='#f59e0b', width=2, dash='dot'),
                        mode='lines+markers'
                    ))

                    fig_proj.update_layout(
                        height=420, hovermode='x unified',
                        title=f"Proyección a 6 meses: {sel_var_name}",
                        legend=dict(orientation='h', y=1.12)
                    )
                    st.plotly_chart(fig_proj, use_container_width=True)
                    st.dataframe(proj_result[['date', 'value']].round(4), use_container_width=True)
                else:
                    st.warning("Proyección no disponible.")
            else:
                st.warning("No hay suficientes datos históricos (mínimo 3 puntos).")

            # ── Sección 2: Consenso de Analistas ──────────────────────────
            st.markdown("#### 🏦 Consenso de Analistas")
            try:
                from data.consensus import get_latest_consensus_by_variable
                consensus_df = get_latest_consensus_by_variable(sel_var_id)
                if not consensus_df.empty:
                    # Agregar fila del modelo
                    if not hist_df.empty and len(hist_df) > 2:
                        proj_now = VariableAgent.calculate_projection(hist_df, periods=12)
                        if not proj_now.empty:
                            last_proj_val = proj_now['value'].iloc[-1]
                            model_row = pd.DataFrame([{
                                'source_institution': '🤖 Modelo Cerebro',
                                'forecast_value': round(last_proj_val, 4),
                                'scenario': 'Ensemble',
                                'forecast_date': datetime.now().strftime('%Y-%m-%d'),
                                'target_date': proj_now['date'].iloc[-1].strftime('%Y-%m-%d') if hasattr(proj_now['date'].iloc[-1], 'strftime') else str(proj_now['date'].iloc[-1])
                            }])
                            consensus_df = pd.concat([consensus_df, model_row], ignore_index=True)

                    st.dataframe(
                        consensus_df[['source_institution', 'forecast_value', 'scenario', 'target_date']].rename(
                            columns={
                                'source_institution': 'Institución',
                                'forecast_value': 'Proyección',
                                'scenario': 'Escenario',
                                'target_date': 'Fecha Objetivo'
                            }
                        ),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.info("Sin proyecciones de consenso para esta variable. Agréguelas en el tab ⚙️ Agente.")
            except Exception as e:
                st.info(f"Módulo de consenso no disponible: {e}")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 5 — Datos y Exportación
    # ════════════════════════════════════════════════════════════════════════
    with tab_data:
        st.subheader("📋 Datos y Exportación")
        st.markdown("Filtra, pivotea y exporta los datos económicos a CSV o Excel.")

        all_vars_df = load_all_variables()
        all_countries_df = load_countries()

        if all_vars_df.empty:
            st.info("No hay variables disponibles.")
        else:
            # Filtros
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                country_filter = st.multiselect(
                    "Países", all_countries_df['name'].tolist(),
                    default=all_countries_df['name'].tolist()[:2]
                )
            with col_f2:
                cat_filter_opts = ['macro', 'energy', 'fiscal', 'external']
                if 'category' in all_vars_df.columns:
                    cat_filter_opts = sorted(all_vars_df['category'].dropna().unique().tolist())
                cat_filter = st.multiselect("Categorías", cat_filter_opts, default=cat_filter_opts)

            col_f3, col_f4 = st.columns(2)
            with col_f3:
                date_start = st.date_input("Desde", value=date(2024, 1, 1))
            with col_f4:
                date_end = st.date_input("Hasta", value=date.today())

            only_real = st.checkbox("Solo datos reales (REAL_OFFICIAL)", value=True)

            # Recolectar datos
            filtered_country_ids = all_countries_df[
                all_countries_df['name'].isin(country_filter)
            ]['id'].tolist() if country_filter else []

            rows = []
            for _, vrow in all_vars_df.iterrows():
                # Filtrar por país
                if filtered_country_ids and vrow.get('country_id') not in filtered_country_ids:
                    continue
                # Filtrar por categoría
                if cat_filter and vrow.get('category') not in cat_filter:
                    continue

                h = load_history(vrow['id'])
                if h.empty:
                    continue

                h = h.copy()
                h['date'] = pd.to_datetime(h['date'])
                h = h[(h['date'] >= pd.Timestamp(date_start)) & (h['date'] <= pd.Timestamp(date_end))]
                if only_real:
                    h = h[h['data_type'] == 'REAL_OFFICIAL']
                if h.empty:
                    continue

                # Añadir metadata
                country_name = all_countries_df[
                    all_countries_df['id'] == vrow.get('country_id')
                ]['name'].values
                country_name = country_name[0] if len(country_name) > 0 else 'N/A'
                h['País'] = country_name
                h['Variable'] = vrow['name']
                h['Unidad'] = vrow.get('unit', '')
                h['Fuente'] = vrow.get('connector_type', 'SCRAPER')
                rows.append(h)

            if rows:
                master_df = pd.concat(rows, ignore_index=True)
                master_df = master_df.rename(columns={'date': 'Fecha', 'value': 'Valor', 'data_type': 'Tipo'})
                master_df['Fecha'] = master_df['Fecha'].dt.strftime('%Y-%m-%d')
                master_df = master_df[['Fecha', 'País', 'Variable', 'Valor', 'Unidad', 'Tipo', 'Fuente']].sort_values(
                    ['Variable', 'Fecha']
                )

                view_mode = st.radio("Vista", ["Tabla plana", "Pivot (fechas × series)", "Resumen estadístico"],
                                     horizontal=True)

                if view_mode == "Tabla plana":
                    st.dataframe(master_df, use_container_width=True, hide_index=True)

                elif view_mode == "Pivot (fechas × series)":
                    pivot_df = master_df.pivot_table(
                        index='Fecha', columns='Variable', values='Valor', aggfunc='mean'
                    )
                    st.dataframe(pivot_df, use_container_width=True)

                elif view_mode == "Resumen estadístico":
                    stats = master_df.groupby('Variable')['Valor'].agg(
                        Último='last', Min='min', Max='max',
                        Promedio='mean', Mediana='median', StdDev='std', N='count'
                    ).round(4).reset_index()
                    st.dataframe(stats, use_container_width=True, hide_index=True)

                st.divider()
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    csv_data = master_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "⬇️ Descargar CSV", data=csv_data,
                        file_name=f"cerebro_economico_{date.today()}.csv", mime="text/csv"
                    )
                with col_d2:
                    try:
                        import openpyxl
                        xlsx_buf = io.BytesIO()
                        with pd.ExcelWriter(xlsx_buf, engine='openpyxl') as writer:
                            master_df.to_excel(writer, sheet_name='Datos', index=False)
                            if view_mode == "Pivot (fechas × series)":
                                pivot_df.to_excel(writer, sheet_name='Pivot')
                        xlsx_buf.seek(0)
                        st.download_button(
                            "⬇️ Descargar XLSX", data=xlsx_buf.getvalue(),
                            file_name=f"cerebro_economico_{date.today()}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    except ImportError:
                        st.info("Instala `openpyxl` para exportar a Excel.")
            else:
                st.info("No hay datos para los filtros seleccionados.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 6 — Agente de Datos
    # ════════════════════════════════════════════════════════════════════════
    with tab_agent:
        st.subheader("⚙️ Agente de Datos")

        # Estado del sistema
        with st.expander("📊 Estado del Sistema", expanded=True):
            all_v = load_all_variables()
            total_vars = len(all_v) if not all_v.empty else 0
            vars_with_data = 0
            vars_with_errors = 0
            if not all_v.empty:
                for _, row in all_v.iterrows():
                    h = load_history(row['id'])
                    if not h.empty:
                        vars_with_data += 1
                    if row.get('fetch_error_count', 0) and int(row.get('fetch_error_count', 0)) > 0:
                        vars_with_errors += 1

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Variables totales", total_vars)
            c2.metric("Con datos", f"{vars_with_data}/{total_vars}")
            c3.metric("Errores activos", vars_with_errors)
            c4.metric("Última revisión", datetime.now().strftime('%H:%M'))

        st.divider()

        # Actualizar una variable individual
        st.markdown("#### 🔄 Actualizar Variables")
        if not variables_df.empty:
            col_t, col_b = st.columns([3, 1])
            with col_t:
                if st.button("🚀 Actualizar TODAS las variables activas", type="primary"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    ok_count = 0
                    for i, (_, row) in enumerate(variables_df.iterrows()):
                        status_text.text(f"Actualizando {row['name']}...")
                        res = VariableAgent.ingest_variable(row)
                        if res.get('success'):
                            ok_count += 1
                        progress_bar.progress((i + 1) / len(variables_df))
                    status_text.text(f"✅ {ok_count}/{len(variables_df)} variables actualizadas.")
                    load_history.clear()

            st.markdown("##### Variables individuales")
            for _, row in variables_df.iterrows():
                col_a, col_info, col_btn = st.columns([2, 2, 1])
                with col_a:
                    ct = row.get('connector_type', 'SCRAPER') or 'SCRAPER'
                    last_fetch = row.get('last_successful_fetch', '')
                    st.markdown(
                        f"**{row['name']}** {badge_html(ct)} "
                        f"<small style='color:gray'>{row.get('api_provider','scraper') or 'scraper'}</small>",
                        unsafe_allow_html=True
                    )
                with col_info:
                    if last_fetch:
                        st.caption(f"Última actualización: {str(last_fetch)[:16]}")
                    errs = row.get('fetch_error_count', 0)
                    if errs and int(errs) > 0:
                        st.caption(f"⚠️ {errs} errores")
                with col_btn:
                    if st.button("Actualizar", key=f"btn_{row['id']}"):
                        with st.spinner("Extrayendo..."):
                            res = VariableAgent.ingest_variable(row)
                            if res.get('success'):
                                st.success(res.get('message', 'Éxito'))
                                load_history.clear()
                            else:
                                st.error(res.get('error', 'Error desconocido'))
                st.divider()

        # Formulario de proyecciones de consenso
        st.markdown("#### 🏦 Agregar Proyección de Consenso")
        try:
            from data.consensus import save_consensus_forecast
            with st.expander("➕ Nueva Proyección de Consenso"):
                all_v2 = load_all_variables()
                if not all_v2.empty:
                    var_consensus_opts = dict(zip(all_v2['name'], all_v2['id']))
                    sel_vc = st.selectbox("Variable", list(var_consensus_opts.keys()), key="cons_var")
                    institution = st.text_input("Institución", placeholder="Bancolombia, BanRep, BBVA...")
                    target_dt = st.date_input("Fecha objetivo", key="cons_date")
                    cons_value = st.number_input("Valor proyectado", key="cons_val")
                    scenario = st.selectbox("Escenario", ["base", "optimista", "pesimista"], key="cons_scen")
                    notes = st.text_area("Notas", placeholder="Fuente, fecha publicación...", key="cons_notes")
                    if st.button("💾 Guardar proyección", key="cons_save"):
                        save_consensus_forecast(
                            variable_id=var_consensus_opts[sel_vc],
                            source_institution=institution,
                            forecast_date=datetime.now(),
                            target_date=datetime.combine(target_dt, datetime.min.time()),
                            value=cons_value,
                            scenario=scenario,
                            notes=notes
                        )
                        st.success("Proyección guardada exitosamente.")
        except ImportError:
            st.info("Módulo de consenso no disponible aún.")
        except Exception as e:
            st.error(f"Error guardando proyección: {e}")


if __name__ == "__main__":
    main()
