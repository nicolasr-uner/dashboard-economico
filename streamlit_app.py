import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import io
import os

from data.database import (
    get_countries, get_variables, get_historical_data,
    get_ai_logs, get_all_variable_names, get_variables_by_name,
    get_last_known_value
)
from data.agent import VariableAgent

# ── Configuración de página ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Cerebro Económico NLA",
    page_icon="🧠",
    layout="wide",
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
@st.cache_data(ttl=3600)
def load_countries():
    return get_countries()

@st.cache_data(ttl=3600)
def load_variables(country_id=None):
    return get_variables(country_id)

@st.cache_data(ttl=3600)
def load_history(variable_id):
    return get_historical_data(variable_id)

@st.cache_data(ttl=3600)
def load_all_variables():
    return get_variables()

@st.cache_data(ttl=3600)
def load_last_known(variable_id):
    return get_last_known_value(variable_id)

@st.cache_data(ttl=3600)
def _cached_sparkline_proj(var_id: int, periods: int = 3):
    """Proyección cacheada para overlay en sparklines. Falla silenciosamente."""
    try:
        h = load_history(var_id)
        if h.empty or len(h) < 3:
            return None
        return VariableAgent.calculate_projection(h, periods=periods)
    except Exception:
        return None

# ── Helpers ──────────────────────────────────────────────────────────────────
def badge_html(connector_type: str) -> str:
    ct = (connector_type or 'SCRAPER').upper()
    cls = {'API': 'badge-api', 'SCRAPER': 'badge-scraper', 'MANUAL': 'badge-manual'}.get(ct, 'badge-scraper')
    return f'<span class="{cls}">{ct}</span>'


# ── Formateo numérico universal ───────────────────────────────────────────────
_MONETARY_UNITS = {
    'COP', 'COP/USD', 'USD', 'COP/kWh', 'USD/bbl', 'USD/MMBtu',
    'USD M', 'USD/MWh', 'COP B', 'BRL/USD', 'MXN/USD', 'EUR/USD', 'COP/kWp'
}
_PERCENT_UNITS = {'%', '% PIB'}


def format_number(value, unit: str = '') -> str:
    """Formato institucional: elimina notación científica."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    u = (unit or '').strip()
    if u in _MONETARY_UNITS:
        return f"{value:,.2f}"
    if u in _PERCENT_UNITS or u.endswith('%'):
        return f"{value:.2f}"
    if abs(value) >= 1_000:
        return f"{value:,.2f}"
    if abs(value) >= 1:
        return f"{value:.4f}"
    return f"{value:.6f}"


# ── Captions por indicador ────────────────────────────────────────────────────
_METRIC_CAPTIONS = {
    'TRM':                  "Tasa Representativa del Mercado: precio oficial COP por 1 USD (BanRep).",
    'IPC':                  "Índice de Precios al Consumidor: variación % anual de la inflación (DANE).",
    'IBR':                  "Indicador Bancario de Referencia: costo del dinero entre bancos colombianos.",
    'PIB':                  "Producto Interno Bruto: crecimiento % del valor agregado de la economía.",
    'Desempleo':            "Tasa de desempleo: % de la PEA sin empleo (DANE).",
    'Tasa de Intervención': "Tasa de política monetaria del Banco de la República.",
    'DTF':                  "Depósito a Término Fijo a 90 días; referencia del costo del crédito.",
    'EMBI':                 "Spread sobre US Treasuries que refleja el riesgo soberano del país.",
    'WTI':                  "West Texas Intermediate: precio de referencia del petróleo crudo (USD/barril).",
    'WACC':                 "Costo promedio ponderado del capital del proyecto.",
}


def _metric_caption(var_name: str) -> str:
    for kw, cap in _METRIC_CAPTIONS.items():
        if kw.lower() in var_name.lower():
            return cap
    return ""


def render_metric_with_history(row, hist, key_prefix="chart"):
    """Renderiza métrica + minichart en columna (Bloomberg-style)."""
    unit = row.get('unit', '') or ''
    if not hist.empty:
        last_val = hist['value'].iloc[-1]
        prev_val = hist['value'].iloc[-2] if len(hist) > 1 else last_val
        delta = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
        ct = row.get('connector_type', 'SCRAPER') if hasattr(row, 'get') else 'SCRAPER'
        st.markdown(badge_html(ct), unsafe_allow_html=True)
        st.metric(
            label=f"{row['name']} ({unit})",
            value=format_number(last_val, unit),
            delta=f"{delta}%"
        )
        cap = _metric_caption(row['name'])
        if cap:
            st.caption(cap)
        fig = px.line(hist, x='date', y='value',
                      labels={'value': unit, 'date': 'Fecha'})
        fig.update_layout(height=150, margin=dict(l=0, r=0, t=5, b=0),
                          showlegend=False,
                          xaxis=dict(showticklabels=False, showgrid=False),
                          yaxis=dict(showticklabels=True, showgrid=True, title=unit))
        fig.update_traces(
            line_color='#3b82f6', line_width=2,
            hovertemplate=f"%{{x|%d %b %Y}}<br>%{{y:,.4g}} {unit}<extra></extra>"
        )
        source_url = row.get('source_url') or ''
        provider = str(row.get('api_provider') or row.get('connector_type') or 'Fuente oficial').upper()
        if source_url and source_url != '#':
            annotation_text = f"Fuente: {provider} — <a href='{source_url}'>{source_url[:60]}</a>"
        else:
            annotation_text = f"Fuente: {provider}"
        fig.add_annotation(
            text=annotation_text,
            xref="paper", yref="paper",
            x=0, y=-0.35, showarrow=False,
            font=dict(size=9, color="#6b7280"),
            xanchor="left", align="left"
        )
        fig.update_layout(margin=dict(l=0, r=0, t=5, b=40))
        # ── Proyección overlay (3 períodos, solo si hay datos suficientes) ────
        try:
            if len(hist) >= 3:
                proj = _cached_sparkline_proj(int(row['id']), 3)
                if proj is not None and not proj.empty:
                    if 'lower_80' in proj.columns and 'upper_80' in proj.columns:
                        x_band = list(proj['date']) + list(reversed(list(proj['date'])))
                        y_band = list(proj['upper_80']) + list(reversed(list(proj['lower_80'])))
                        fig.add_trace(go.Scatter(
                            x=x_band, y=y_band, fill='toself',
                            fillcolor='rgba(251,146,60,0.12)',
                            line=dict(color='rgba(0,0,0,0)'),
                            showlegend=False, hoverinfo='skip'
                        ))
                    fig.add_trace(go.Scatter(
                        x=proj['date'], y=proj['value'],
                        mode='lines',
                        line=dict(color='#f97316', width=1.5, dash='dot'),
                        showlegend=False,
                        hovertemplate=f"Proy: %{{y:,.4g}} {unit}<extra></extra>"
                    ))
        except Exception:
            pass  # fallo silencioso — sparkline histórico sigue normal
        st.plotly_chart(fig, width='stretch', key=f"{key_prefix}_{row['id']}")
    else:
        lkg = load_last_known(row['id'])
        if lkg:
            date_label = lkg['date'].strftime('%d %b %Y')
            st.metric(
                label=f"{row['name']} ({unit})",
                value=format_number(lkg['value'], unit)
            )
            st.caption(f"Dato histórico: {date_label}")
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

    # ── Banner de estado de API keys ─────────────────────────────────────────
    _missing_keys = []
    def _has_secret(key: str) -> bool:
        try:
            return bool(st.secrets.get(key, ""))
        except Exception:
            return False

    if not os.getenv("FRED_API_KEY") and not _has_secret("FRED_API_KEY"):
        _missing_keys.append("`FRED_API_KEY` — WTI, Brent, Treasuries, tasas globales (fred.stlouisfed.org)")
    if not os.getenv("BANXICO_TOKEN") and not _has_secret("BANXICO_TOKEN"):
        _missing_keys.append("`BANXICO_TOKEN` — datos México: tasas, peso MXN, inflacion (si.banxico.org.mx)")
    if _missing_keys:
        with st.expander("⚠️ Configuración Incompleta — algunas fuentes no están activas", expanded=False):
            st.warning(
                "Las siguientes API keys no están configuradas en `.env`. "
                "Los datos de estas fuentes no se actualizarán automáticamente:\n\n" +
                "\n".join(f"- {k}" for k in _missing_keys) +
                "\n\nVer `.env.example` en el repositorio para instrucciones. "
                "Los demás datos (BCB Brasil, World Bank, XM Colombia) funcionan sin API key."
            )

    # ── Sidebar ──────────────────────────────────────────────────────────────
    st.sidebar.header("🌎 Filtros Globales")
    countries_df = load_countries()
    if countries_df.empty:
        st.warning("No hay países en la base de datos. Ejecuta `python scripts/seed_variables_v2.py`.")
        return

    country_opts = dict(zip(countries_df['name'], countries_df['id']))

    # Forzar Colombia como default preseleccionado
    colombia_idx = 0
    country_names = list(country_opts.keys())
    for i, name in enumerate(country_names):
        if 'colombia' in name.lower():
            colombia_idx = i
            break

    # ── Sync URL → sidebar (Query Params) ────────────────────────────────────
    _qp_country = st.query_params.get("pais", None)
    if _qp_country:
        for i, name in enumerate(country_names):
            if name.lower() == _qp_country.lower():
                colombia_idx = i
                break

    selected_country_name = st.sidebar.selectbox(
        "País principal", options=country_names, index=colombia_idx
    )
    selected_country_id = country_opts[selected_country_name]
    # ── Sync sidebar → URL ────────────────────────────────────────────────────
    st.query_params["pais"] = selected_country_name

    variables_df = load_variables(selected_country_id)

    # ── Aviso Legal ───────────────────────────────────────────────────────────
    st.sidebar.divider()
    st.sidebar.warning(
        "**Aviso Legal:** La información presentada tiene carácter meramente informativo "
        "y no constituye asesoría de inversión ni recomendación financiera. "
        "Las proyecciones son estimaciones estadísticas y no garantizan resultados futuros. "
        "Datos capturados de fuentes públicas: BanRep, DANE, XM, FRED, BCB, Banxico. "
        "Puede existir rezago en la publicación. Ley 1581/2012."
    )

    # ── Biblioteca de Datos ───────────────────────────────────────────────────
    st.sidebar.divider()
    with st.sidebar.expander("📚 Biblioteca de Datos", expanded=False):
        st.markdown("<small style='color:gray'>Fuentes oficiales y enlaces directos.</small>", unsafe_allow_html=True)
        lib_vars = variables_df  # already loaded
        for _, lib_row in lib_vars.iterrows():
            source_url = lib_row.get('source_url') or '#'
            provider = str(lib_row.get('api_provider') or 'Entidad Oficial').upper()
            desc = lib_row.get('description') or lib_row['name']
            st.markdown(f"**{lib_row['name']}**")
            if source_url and source_url != '#':
                st.markdown(f"[{provider} — {desc[:80]}]({source_url})", unsafe_allow_html=False)
            else:
                st.caption(f"{provider} — {desc[:80]}")
            st.divider()

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab_global, tab1, tab_energy, tab_comp, tab_proj, tab_data, tab_corp, tab_agent = st.tabs([
        "🌍 Global",
        "📊 Vista General",
        "⚡ Sector Energético",
        "🌎 América Latina",
        "🔮 Proyecciones",
        "📋 Datos y Exportación",
        "🏢 Finanzas Corporativas",
        "⚙️ Agente de Datos"
    ])

    # ════════════════════════════════════════════════════════════════════════
    # TAB GLOBAL — Mercados Globales + Resumen 4 Países
    # ════════════════════════════════════════════════════════════════════════
    with tab_global:
        st.subheader("🌍 Mercados & Economía Global")
        st.markdown(
            "Commodities energéticos, metales críticos para la transición energética, "
            "índices financieros y resumen macro de los 4 países."
        )

        global_vars_df = load_variables(5)  # country_id=5 = Global (WW)
        all_countries_df_g = load_countries()

        if global_vars_df.empty:
            st.info(
                "No hay variables globales configuradas. "
                "Ejecuta `python -X utf8 scripts/seed_variables_v4.py` para cargarlas."
            )
        else:
            # ── Sección A: KPI Row (6 métricas principales) ───────────────────
            GLOBAL_KPIS = [
                "WTI Crude Oil", "Brent Crude Oil",
                "Gold (Oro) Price", "DXY (Índice Dólar)",
                "S&P 500 Index", "VIX (Índice de Volatilidad)",
            ]
            kpi_cols = st.columns(len(GLOBAL_KPIS))
            for ki, vname in enumerate(GLOBAL_KPIS):
                match = global_vars_df[global_vars_df['name'] == vname]
                if not match.empty:
                    g_row = match.iloc[0]
                    h_g = load_history(int(g_row['id']))
                    if not h_g.empty:
                        last_g = h_g['value'].iloc[-1]
                        prev_g = h_g['value'].iloc[-2] if len(h_g) > 1 else last_g
                        delta_g = round(((last_g - prev_g) / prev_g * 100), 2) if prev_g != 0 else 0
                        kpi_cols[ki].metric(
                            vname.split('(')[0].strip(),
                            format_number(last_g, g_row['unit']),
                            f"{delta_g}%"
                        )
                    else:
                        kpi_cols[ki].metric(vname.split('(')[0].strip(), "N/D")
                else:
                    kpi_cols[ki].metric(vname.split('(')[0].strip(), "—")

            st.divider()

            # ── Sección B: Gráficos normalizados por grupo de commodity ───────
            COMMODITY_GROUPS = {
                "⚡ Energéticos": ["WTI Crude Oil", "Brent Crude Oil", "Henry Hub Natural Gas"],
                "🔩 Metales Críticos (Renovables)": [
                    "Copper (Cobre) Price", "Aluminum (Aluminio) Price",
                    "Lithium Carbonate Price", "Gold (Oro) Price"
                ],
                "🌾 Agrícolas (LATAM)": [
                    "Cafe (Coffee) Arabica Price", "Soja (Soybean) Price", "Maiz (Corn) Price"
                ],
            }

            for group_title, group_vars in COMMODITY_GROUPS.items():
                st.markdown(f"#### {group_title}")
                frames = []
                for vname in group_vars:
                    match = global_vars_df[global_vars_df['name'] == vname]
                    if match.empty:
                        continue
                    h_c = load_history(int(match.iloc[0]['id']))
                    if h_c.empty or len(h_c) < 2:
                        continue
                    h_c = h_c.copy()
                    h_c['date'] = pd.to_datetime(h_c['date'])
                    h_c = h_c.sort_values('date')
                    base_val = h_c['value'].iloc[0]
                    if base_val and base_val != 0:
                        h_c['value_norm'] = (h_c['value'] / base_val * 100).round(2)
                    else:
                        h_c['value_norm'] = 100.0
                    h_c['Variable'] = vname
                    frames.append(h_c[['date', 'value_norm', 'Variable']])

                if frames:
                    combined_c = pd.concat(frames, ignore_index=True)
                    fig_c = px.line(
                        combined_c, x='date', y='value_norm', color='Variable',
                        labels={'value_norm': '% vs base (=100)', 'date': 'Fecha'},
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    fig_c.update_layout(
                        height=280, hovermode='x unified',
                        margin=dict(l=0, r=0, t=10, b=0),
                        legend=dict(orientation='h', y=-0.2)
                    )
                    fig_c.add_hline(y=100, line_dash='dash', line_color='gray',
                                    annotation_text="Base", annotation_position="right")
                    st.plotly_chart(fig_c, width='stretch', key=f"glob_comm_{group_title[:8]}")
                else:
                    st.info(f"Sin datos para {group_title}. Ejecuta `backfill.py` con FRED_API_KEY.")

            st.divider()

            # ── Sección C: Tabla Resumen Macro 4 Países ───────────────────────
            st.markdown("#### 🌎 Comparativo Macro — 4 Países")
            st.caption("Últimos valores disponibles por país. Fuentes: BanRep, Banxico, BCB, World Bank, FRED.")

            # Cargar variables de todos los países
            all_vars_g = load_all_variables()
            COUNTRY_SUMMARY_MAP = {
                "🇨🇴 Colombia": {
                    "Inflación (%)":   "IPC CO (var. anual)",
                    "PIB var. (%)":    "PIB Trimestral CO",
                    "Tasa política (%)":"Tasa de Intervención BanRep",
                    "FX (COP/USD)":    "TRM (COP/USD)",
                    "EMBI (bps)":      "EMBI Colombia",
                    "Desempleo (%)":   "Desempleo CO",
                },
                "🇲🇽 México": {
                    "Inflación (%)":    "IPC MX (var. anual)",
                    "PIB var. (%)":     "PIB Trimestral MX",
                    "Tasa política (%)":"Tasa Objetivo Banxico",
                    "FX (MXN/USD)":     "Tipo de Cambio USD/MXN",
                    "EMBI (bps)":       "EMBI México",
                    "Desempleo (%)":    "Desempleo MX",
                },
                "🇧🇷 Brasil": {
                    "Inflación (%)":    "IPCA BR (var. anual)",
                    "PIB var. (%)":     "PIB Trimestral BR",
                    "Tasa política (%)":"Tasa Selic BR",
                    "FX (BRL/USD)":     "USD/BRL",
                    "EMBI (bps)":       "EMBI Brasil",
                    "Desempleo (%)":    "Desempleo BR",
                },
                "🇪🇨 Ecuador": {
                    "Inflación (%)":    "IPC Ecuador (var. anual)",
                    "PIB var. (%)":     "PIB Ecuador",
                    "Tasa política (%)":"Tasa Interbancaria EC",
                    "FX (USD)":         "USD (dolarizado)",
                    "EMBI (bps)":       "CDS Ecuador 5Y",
                    "Desempleo (%)":    "Tasa de Desempleo",
                },
            }

            summary_data = {}
            for country_label, metrics in COUNTRY_SUMMARY_MAP.items():
                row_data = {}
                for metric_label, var_name_frag in metrics.items():
                    if not all_vars_g.empty:
                        matches = all_vars_g[
                            all_vars_g['name'].str.lower().str.contains(
                                var_name_frag.lower()[:20], na=False, regex=False
                            )
                        ]
                        if not matches.empty:
                            h_s = load_history(int(matches.iloc[0]['id']))
                            if not h_s.empty:
                                row_data[metric_label] = round(h_s['value'].iloc[-1], 2)
                            else:
                                row_data[metric_label] = None
                        else:
                            row_data[metric_label] = None
                    else:
                        row_data[metric_label] = None
                summary_data[country_label] = row_data

            summary_df = pd.DataFrame(summary_data).T
            # Colorear con Styler
            def _color_inflation(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return ''
                if val > 8:
                    return 'background-color: #fca5a5; color: #7f1d1d'
                elif val > 5:
                    return 'background-color: #fed7aa; color: #7c2d12'
                elif val < 2:
                    return 'background-color: #bbf7d0; color: #14532d'
                return ''

            def _color_embi(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return ''
                if val > 800:
                    return 'background-color: #fca5a5; color: #7f1d1d'
                elif val > 400:
                    return 'background-color: #fed7aa; color: #7c2d12'
                return ''

            try:
                styled = (
                    summary_df.style
                    .applymap(_color_inflation, subset=["Inflación (%)"] if "Inflación (%)" in summary_df.columns else [])
                    .applymap(_color_embi, subset=["EMBI (bps)"] if "EMBI (bps)" in summary_df.columns else [])
                    .format(lambda x: f"{x:.2f}" if isinstance(x, float) and not pd.isna(x) else ("—" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x)))
                )
                st.dataframe(styled, use_container_width=True)
            except Exception:
                st.dataframe(summary_df, use_container_width=True)

            st.caption(
                "Verde = inflación baja/buena | Naranja/Rojo = inflación alta o EMBI elevado. "
                "Los valores EMBI corresponden al diferencial de riesgo soberano sobre US Treasuries."
            )

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — Vista General
    # ════════════════════════════════════════════════════════════════════════
    with tab1:
        st.subheader(f"📊 Indicadores de {selected_country_name}")

        if variables_df.empty:
            st.info("No hay variables configuradas para este país.")
        else:
            st.caption("💡 Usa el selector de país en el panel lateral para cambiar el país visualizado.")

            # Filtro por categoría
            cats = ['Todas'] + sorted(variables_df['category'].dropna().unique().tolist()) \
                if 'category' in variables_df.columns else ['Todas']
            sel_cat = st.selectbox("Filtrar por categoría", cats, key="t1_cat")

            filtered_vars = variables_df if sel_cat == 'Todas' else \
                variables_df[variables_df['category'] == sel_cat] \
                if 'category' in variables_df.columns else variables_df

            # Secciones temáticas cuando se muestran todas las categorías
            _SECTIONS = {
                "🌐 Sector Externo": ['external', 'fx_rates'],
                "📈 Inflación y Tasas": ['prices_inflation', 'rates_monetary', 'macro'],
                "🏭 Actividad Económica": ['gdp_activity'],
            }

            if sel_cat == 'Todas' and 'category' in variables_df.columns:
                _mapped_cats = [c for cats_list in _SECTIONS.values() for c in cats_list]
                for section_title, section_cats in _SECTIONS.items():
                    sec_vars = variables_df[variables_df['category'].isin(section_cats)]
                    if sec_vars.empty:
                        continue
                    st.subheader(section_title)
                    cols = st.columns(min(3, len(sec_vars)))
                    for idx, (_, row) in enumerate(sec_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            with st.container(border=True):
                                render_metric_with_history(row, hist, key_prefix="t1_sec")
                # Indicadores que no encajan en las secciones anteriores
                other_vars = variables_df[~variables_df['category'].isin(_mapped_cats)]
                if not other_vars.empty:
                    st.subheader("📌 Otros Indicadores")
                    cols = st.columns(min(3, len(other_vars)))
                    for idx, (_, row) in enumerate(other_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            with st.container(border=True):
                                render_metric_with_history(row, hist, key_prefix="t1_oth")
            else:
                if len(filtered_vars) > 0:
                    cols = st.columns(min(3, len(filtered_vars)))
                    for idx, (_, row) in enumerate(filtered_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            with st.container(border=True):
                                render_metric_with_history(row, hist, key_prefix="t1_flt")
                else:
                    st.info("No hay variables en esta categoría.")

            # Monitor de Noticias Simulado
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"### 📰 MONITOR DE VARIABLES Y NOTICIAS: {selected_country_name.upper()}")
            
            news_data = [
                {"Fecha": "2026-04-08", "Titular": "Entidad oficial reporta sorpresa en desempleo nacional y revisa expectativas", "Variable Afectada": "Desempleo", "Riesgo": "🔴 Alto", "Link": "https://www.dane.gov.co"},
                {"Fecha": "2026-04-05", "Titular": "Se mantienen las tasas de intervención en la más reciente reunión", "Variable Afectada": "Tasa de Intervención", "Riesgo": "🟢 Bajo", "Link": "https://www.banrep.gov.co"},
                {"Fecha": "2026-04-01", "Titular": "Acuerdo en el mercado energético afecta el Índice de Contratos", "Variable Afectada": "Índice Mc", "Riesgo": "🟡 Medio", "Link": "https://www.xm.com.co"}
            ]
            st.dataframe(pd.DataFrame(news_data), use_container_width=True, hide_index=True)

    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — Sector Energético
    # ════════════════════════════════════════════════════════════════════════
    with tab_energy:
        st.subheader("⚡ Sector Energético")
        st.markdown(f"Variables del mercado energético de **{selected_country_name}** y commodities globales.")

        all_vars = load_all_variables()
        if all_vars.empty or 'category' not in all_vars.columns:
            st.info("Variables de energía pendientes de carga. Ejecute el agente de datos o el backfill.")
        else:
            energy_vars = all_vars[all_vars['category'] == 'energy']

            # Contexto según país seleccionado
            COUNTRY_ENERGY_CONTEXT = {
                "Colombia": {
                    "operator": "XM — Mercado Eléctrico Mayorista",
                    "note": "Precio de Bolsa, Índice Mc, Aportes Hídricos, Cargo por Confiabilidad.",
                    "key_vars": ["Bolsa", "Mc", "Aporte", "Escasez", "CERE"]
                },
                "Ecuador": {
                    "operator": "CENACE — Centro Nacional de Control de Energía",
                    "note": "Ecuador opera con despacho centralizado. No existe mercado spot/bolsa como Colombia. La tarifa la fija ARCERNNR. Aproximadamente 70% de generación es hidráulica.",
                    "key_vars": ["Hidro", "Solar", "Capacidad"]
                },
                "Brasil": {
                    "operator": "ONS / CCEE — Operador Nacional do Sistema / Câmara de Comercialização",
                    "note": "PLD (Preço de Liquidação das Diferenças) equivale al Precio de Bolsa colombiano. Reservatórios de embalses son el indicador crítico.",
                    "key_vars": ["Solar", "Capacidad"]
                },
                "México": {
                    "operator": "CENACE México — Centro Nacional de Control de Energía",
                    "note": "Precio Marginal Local (PML) es el equivalente al Precio de Bolsa. Mercado liberalizado desde reforma 2013.",
                    "key_vars": ["Solar", "Capacidad"]
                }
            }
            ctx = COUNTRY_ENERGY_CONTEXT.get(selected_country_name, {})
            if ctx:
                st.info(f"**{ctx['operator']}** — {ctx['note']}")

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
                            value=format_number(last_val, vinfo['unit']),
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
                        st.plotly_chart(fig_bm, width='stretch', key="energy_bolsa_mc")

                    # Demanda y Solar
                    for label, keywords in [("Demanda Nacional", ['Demanda', 'DemaNal']),
                                            ("Generación Solar", ['Solar', 'Gene'])]:
                        ekey = next((k for k in energy_data for kw in keywords if kw.lower() in k.lower()), None)
                        if ekey:
                            st.markdown(f"#### {label}")
                            fig_e = px.area(energy_data[ekey]['df'], x='date', y='value',
                                            title=f"{ekey} ({energy_data[ekey]['unit']})")
                            fig_e.update_layout(height=250)
                            chart_key = f"energy_{label.lower().replace(' ', '_')}"
                            st.plotly_chart(fig_e, width='stretch', key=chart_key)

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
                        st.plotly_chart(fig_ap, width='stretch', key="energy_aportes_hidricos")

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
                        st.plotly_chart(fig_c, width='stretch', key="energy_commodities")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — Comparativa Regional
    # ════════════════════════════════════════════════════════════════════════
    with tab_comp:
        st.subheader("🌎 Comparativa Macro Regional")
        st.markdown("Cruza y correlaciona el rendimiento de métricas clave a lo largo de América Latina.")
        st.info("🌎 Esta vista muestra **todos los países (CO, MX, BR, EC)** simultáneamente, independientemente del país seleccionado en el filtro lateral.")

        all_vars_full = load_all_variables()
        if all_vars_full.empty:
            st.warning("No hay variables definidas.")
        else:
            # Concept Mapping para unificar métricas internacionalmente
            COMPARABLE_METRICS = {
                "Inflación Anual (%)": ["IPC CO (var. anual)", "IPC MX (var. anual)", "IPCA BR (var. anual)", "IPC Ecuador (var. anual)", "CPI USA (var. anual)"],
                "Crecimiento PIB (Trimestral/Anual)": ["PIB Trimestral CO (var. anual)", "PIB Trimestral MX (var. anual)", "PIB Trimestral BR (var. %)", "PIB Ecuador (USD corrientes)"],
                "Tasa de Desempleo (%)": ["Desempleo CO", "Desempleo MX", "Desempleo BR"],
                "Tasa de Política Monetaria (%)": ["Tasa de Intervención BanRep", "Tasa Objetivo Banxico", "Tasa Selic BR", "Fed Funds Rate (USA)"],
                "Tipo de Cambio (Moneda Local / USD)": ["TRM (COP/USD)", "USD/MXN", "USD/BRL", "EUR/USD"],
                "Riesgo País (EMBI)": ["EMBI Colombia (Riesgo País)", "EMBI México", "EMBI Brasil"],
                "Cuenta Corriente (% PIB)": ["Cuenta Corriente CO (% PIB)", "Cuenta Corriente BR (% PIB)", "Cuenta Corriente MX (% PIB)"]
            }

            selected_concept = st.selectbox("Seleccione el Concepto Macroeconómico a cruzar", list(COMPARABLE_METRICS.keys()))
            names_in_concept = COMPARABLE_METRICS[selected_concept]
            
            vars_to_compare = all_vars_full[all_vars_full['name'].isin(names_in_concept)]

            compare_data = []
            if not vars_to_compare.empty:
                countries_list = load_countries()
                for _, v_row in vars_to_compare.iterrows():
                    h_df = load_history(v_row['id'])
                    if not h_df.empty:
                        h_df['value'] = pd.to_numeric(h_df['value'], errors='coerce')
                        
                        country_name = "Desconocido"
                        if not countries_list.empty:
                            c_match = countries_list[countries_list['id'] == v_row['country_id']]
                            if not c_match.empty:
                                country_name = c_match.iloc[0]['name']
                                
                        h_df['País'] = country_name
                        compare_data.append(h_df)

                if compare_data:
                    combined_df = pd.concat(compare_data, ignore_index=True)
                    fig_comp = px.line(
                        combined_df, x='date', y='value', color='País', markers=True,
                        title=f"Evolución Histórica Cruzada: {selected_concept}"
                    )
                    fig_comp.update_layout(height=420, hovermode="x unified")
                    st.plotly_chart(fig_comp, width='stretch', key="regional_comparison")

                    st.divider()
                    st.subheader("📊 Ranking — Último Dato")
                    cols_comp = st.columns(len(compare_data))
                    for i, df_c in enumerate(compare_data):
                        country = df_c['País'].iloc[0]
                        current_val = df_c.iloc[-1]['value']
                        prev_val = df_c.iloc[-2]['value'] if len(df_c) > 1 else current_val
                        delta_comp = round(((current_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
                        cols_comp[i].metric(label=country, value=format_number(current_val), delta=f"{delta_comp}%")
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
                    _model_label = proj_result['model_name'].iloc[0] \
                        if 'model_name' in proj_result.columns else 'Ensemble'
                    fig_proj.add_trace(go.Scatter(
                        x=proj_result['date'], y=proj_result['value'],
                        name=f"Proyección ({_model_label})",
                        line=dict(color='#f59e0b', width=2, dash='dot'),
                        mode='lines+markers'
                    ))

                    _sel_unit = variables_df[variables_df['id'] == sel_var_id]['unit'].values
                    _sel_unit = _sel_unit[0] if len(_sel_unit) > 0 else ''

                    # ── Overlay: puntos de proyección institucional ───────────
                    try:
                        from data.consensus import get_latest_consensus_by_variable
                        consensus_overlay = get_latest_consensus_by_variable(sel_var_id)
                        if not consensus_overlay.empty:
                            SCENARIO_SYMBOLS = {
                                'base': 'diamond', 'optimista': 'triangle-up',
                                'pessimista': 'triangle-down', 'actual': 'circle'
                            }
                            INSTITUTION_COLORS = {
                                'IMF WEO': '#1f77b4', 'IMF WEO (API)': '#1f77b4',
                                'Focus BCB (mediana)': '#2ca02c', 'Focus BCB (mediana, API)': '#2ca02c',
                                'Banxico Encuesta': '#d62728', 'BanRep': '#9467bd',
                                'BanRep (encuesta)': '#9467bd',
                                'Goldman Sachs': '#8c564b', 'JPMorgan': '#e377c2',
                                'BBVA Research': '#7f7f7f', 'Bancolombia': '#bcbd22',
                                'Corficolombiana': '#17becf', 'EIA': '#aec7e8',
                                'EIA (Energy Info Agency)': '#aec7e8',
                                'BCE': '#ffbb78', 'CEPAL': '#98df8a',
                                'Banco Mundial': '#c5b0d5', 'ANIF': '#f7b6d2',
                                'Citibank': '#c49c94',
                            }
                            for inst, grp in consensus_overlay.groupby('source_institution'):
                                color = INSTITUTION_COLORS.get(inst, '#636363')
                                scen = grp['scenario'].iloc[0] if 'scenario' in grp.columns else 'base'
                                symbol = SCENARIO_SYMBOLS.get(scen, 'circle')
                                fig_proj.add_trace(go.Scatter(
                                    x=pd.to_datetime(grp['target_date']),
                                    y=grp['forecast_value'],
                                    mode='markers+text',
                                    name=inst,
                                    marker=dict(size=10, symbol=symbol, color=color,
                                                line=dict(color='white', width=1)),
                                    text=[f"{v:.2f}" for v in grp['forecast_value']],
                                    textposition='top center',
                                    textfont=dict(size=9),
                                    hovertemplate=(
                                        f"<b>{inst}</b><br>"
                                        "Objetivo: %{x|%b %Y}<br>"
                                        f"Valor: %{{y:.2f}} {_sel_unit}<br>"
                                        "Escenario: " + scen + "<extra></extra>"
                                    ),
                                ))
                    except Exception:
                        pass  # consenso no disponible — chart sigue funcionando

                    fig_proj.update_layout(
                        height=460, hovermode='x unified',
                        title=f"Proyección 6 meses + Consenso Analistas: {sel_var_name} ({_sel_unit})",
                        yaxis_title=_sel_unit,
                        legend=dict(orientation='h', y=1.15, font=dict(size=10))
                    )
                    st.plotly_chart(fig_proj, width='stretch', key="projection_chart")
                    st.caption("◆ Diamante = escenario base | ▲ Triángulo arriba = optimista | ▼ Triángulo abajo = pesimista")
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

                    display_cols = ['source_institution', 'forecast_value', 'scenario', 'target_date']
                    if 'notes' in consensus_df.columns:
                        display_cols.append('notes')
                    rename_map = {
                        'source_institution': 'Institución',
                        'forecast_value': 'Proyección',
                        'scenario': 'Escenario',
                        'target_date': 'Fecha Objetivo',
                        'notes': 'Fuente / Notas',
                    }
                    st.dataframe(
                        consensus_df[display_cols].rename(columns=rename_map),
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
    # ====================================================================
    # TAB CORP — Finanzas Corporativas
    # ====================================================================
    with tab_corp:
        st.subheader("🏢 Finanzas Corporativas — Modelos Exagon & Ruitoque")
        st.markdown(
            "Panel de control financiero extraido automáticamente de los modelos Excel internos. "
            "Valores de **WACC, Kd, Ke, CAPEX y Tarifa PPA** ingresados como estimaciones 2026."
        )

        all_corp = load_all_variables()
        if all_corp.empty:
            st.info("Carga las variables con `seed_variables_v3.py`.")
        else:
            corp_vars = all_corp[all_corp.get('category', pd.Series()) == 'corporate_finance'] \
                if 'category' in all_corp.columns else pd.DataFrame()

            if corp_vars.empty:
                st.info("No hay variables de finanzas corporativas. Ejecuta `seed_variables_v3.py`.")
            else:
                # --- KPI Panel ---
                EXCEL_VARS = [
                    "WACC - Costo Promedio de Capital",
                    "Costo de la Deuda (Kd)",
                    "Costo del Equity (Ke)",
                    "Tarifa PPA (Precio Venta de Energía)",
                    "TIR Proyecto (IRR)",
                    "CAPEX Solar Total (USD por proyecto)",
                ]
                excel_subset = corp_vars[corp_vars['name'].isin(EXCEL_VARS)]

                st.markdown("#### 📌 Indicadores Clave del Proyecto")
                kpi_cols = st.columns(3)
                for ki, (_, row) in enumerate(excel_subset.iterrows()):
                    h = load_history(row['id'])
                    with kpi_cols[ki % 3]:
                        with st.container(border=True):
                            if not h.empty:
                                val = h['value'].iloc[-1]
                                unit = row.get('unit', '')
                                # Formatear segun tipo
                                if unit == '%':
                                    display = f"{val:.2f}%"
                                elif unit == 'USD':
                                    display = f"USD {val:,.0f}"
                                elif unit == 'COP/kWh':
                                    display = f"{val:.0f} COP/kWh"
                                else:
                                    display = f"{val:,.3g} {unit}"
                                src = row.get('description', '')
                                st.metric(label=row['name'], value=display)
                                st.caption(src[:120] if src else "")
                            else:
                                st.metric(label=row['name'], value="Sin datos")
                                st.caption("Ejecuta `scripts/read_excel_models.py` para poblar.")

                st.divider()

                # --- Grafico WACC Waterfall ---
                st.markdown("#### 📉 Estructura del WACC")
                wacc_row = corp_vars[corp_vars['name'] == "WACC - Costo Promedio de Capital"]
                kd_row   = corp_vars[corp_vars['name'] == "Costo de la Deuda (Kd)"]
                ke_row   = corp_vars[corp_vars['name'] == "Costo del Equity (Ke)"]

                wacc_vals = {}
                for label, row_df in [("Kd (Deuda)", kd_row), ("Ke (Equity)", ke_row), ("WACC", wacc_row)]:
                    if not row_df.empty:
                        h = load_history(int(row_df.iloc[0]['id']))
                        if not h.empty:
                            wacc_vals[label] = h['value'].iloc[-1]

                if wacc_vals:
                    fig_w = go.Figure(go.Bar(
                        x=list(wacc_vals.keys()),
                        y=list(wacc_vals.values()),
                        marker_color=['#6366f1', '#f59e0b', '#10b981'],
                        text=[f"{v:.2f}%" for v in wacc_vals.values()],
                        textposition='outside'
                    ))
                    fig_w.update_layout(
                        title="WACC vs Componentes de Capital (%)",
                        yaxis_title="%", height=320,
                        plot_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig_w, width='stretch', key="corp_wacc_chart")
                else:
                    st.info("Pobla datos con `scripts/read_excel_models.py` para ver el gráfico WACC.")

                st.divider()

                # --- Sensibilidad PPA vs Inflacion ---
                st.markdown("#### 📊 Sensibilidad PPA vs Indexador IPP")
                ppa_row = corp_vars[corp_vars['name'] == "Tarifa PPA (Precio Venta de Energía)"]
                ipp_row = all_corp[all_corp['name'] == "IPP CO (var. anual)"]

                col_s1, col_s2 = st.columns(2)
                with col_s1:
                    if not ppa_row.empty:
                        h_ppa = load_history(int(ppa_row.iloc[0]['id']))
                        ppa_base = h_ppa['value'].iloc[-1] if not h_ppa.empty else 300
                    else:
                        ppa_base = 300
                    st.metric("PPA Base (COP/kWh)", f"{ppa_base:.0f}")

                with col_s2:
                    if not ipp_row.empty:
                        h_ipp = load_history(int(ipp_row.iloc[0]['id']))
                        ipp_val = h_ipp['value'].iloc[-1] if not h_ipp.empty else 4.4
                    else:
                        ipp_val = 4.4
                    st.metric("IPP CO Reciente (%)", f"{ipp_val:.2f}%")

                # Tabla de sensibilidad
                ipp_scenarios = [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
                years = [1, 2, 3, 5, 10]
                sens_data = {}
                for yr in years:
                    row_sens = {}
                    for ipp in ipp_scenarios:
                        ppa_proj = ppa_base * ((1 + ipp / 100) ** yr)
                        row_sens[f"IPP {ipp:.0f}%"] = round(ppa_proj, 1)
                    sens_data[f"Año {yr}"] = row_sens

                sens_df = pd.DataFrame(sens_data).T
                st.caption(f"Proyección PPA (COP/kWh) indexado a IPP, base {ppa_base:.0f} COP/kWh")
                st.dataframe(sens_df.style.highlight_max(axis=0, color='#d1fae5')
                                         .highlight_min(axis=0, color='#fee2e2'),
                             use_container_width=True)

                st.divider()

                # --- Resto de variables corporativas ---
                st.markdown("#### 🗒 Todas las Variables Corporativas")
                other_corp = corp_vars[~corp_vars['name'].isin(EXCEL_VARS)]
                if not other_corp.empty:
                    c1, c2, c3 = st.columns(3)
                    cols3 = [c1, c2, c3]
                    for ci, (_, row) in enumerate(other_corp.iterrows()):
                        h = load_history(row['id'])
                        with cols3[ci % 3]:
                            with st.container(border=True):
                                render_metric_with_history(row, h, key_prefix="t6_corp")


if __name__ == "__main__":
    main()
