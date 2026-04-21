import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import io
import os
import streamlit.components.v1 as _stcomponents

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
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* Bloomberg-style card */
  .bb-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 14px 16px 10px;
    transition: border-color 0.15s, box-shadow 0.15s;
  }
  .bb-card:hover { border-color: #3b82f6; box-shadow: 0 2px 12px rgba(59,130,246,0.12); }

  .bb-ticker { font-size: 0.72em; font-weight: 700; letter-spacing: 0.08em;
               color: #6b7280; text-transform: uppercase; margin-bottom: 2px; }
  .bb-value  { font-family: 'JetBrains Mono', monospace; font-size: 1.55em;
               font-weight: 600; color: #111827; line-height: 1.1; }
  .bb-delta-pos { color: #10b981; font-size: 0.82em; font-weight: 600; }
  .bb-delta-neg { color: #ef4444; font-size: 0.82em; font-weight: 600; }
  .bb-delta-neu { color: #6b7280; font-size: 0.82em; font-weight: 600; }
  .bb-date   { font-size: 0.72em; color: #9ca3af; margin-top: 4px; }
  .bb-stale  { color: #f59e0b; font-size: 0.72em; font-weight: 600; }

  /* Sidebar Bloomberg */
  .sb-section { font-size: 0.68em; font-weight: 700; letter-spacing: 0.1em;
                color: #9ca3af; text-transform: uppercase; margin: 10px 0 4px; }
  .sb-status-row { display: flex; gap: 12px; align-items: center;
                   font-size: 0.82em; margin-bottom: 4px; }
  .sb-dot-green  { color: #10b981; font-size: 1.1em; }
  .sb-dot-yellow { color: #f59e0b; font-size: 1.1em; }
  .sb-dot-red    { color: #ef4444; font-size: 1.1em; }

  /* Badges */
  .badge-api     { background:#d1fae5; color:#065f46; padding:2px 7px; border-radius:10px; font-size:0.7em; font-weight:700; }
  .badge-scraper { background:#dbeafe; color:#1e40af; padding:2px 7px; border-radius:10px; font-size:0.7em; font-weight:700; }
  .badge-manual  { background:#fef3c7; color:#92400e; padding:2px 7px; border-radius:10px; font-size:0.7em; font-weight:700; }

  /* Data Hub table */
  .hub-row { padding: 8px 12px; border-bottom: 1px solid #f3f4f6;
             font-size: 0.88em; }
  .hub-row:hover { background: #f9fafb; }

  h1 { color: #1e3a8a; }
  .stMetric { background: white; padding: 12px 16px; border-radius: 10px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
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
    try:
        h = load_history(var_id)
        if h.empty or len(h) < 3:
            return None
        return VariableAgent.calculate_projection(h, periods=periods)
    except Exception:
        return None

# ── Frecuencia — resample helper ─────────────────────────────────────────────
FREQ_LABELS  = ['D', 'S', 'M', 'T', 'A']
FREQ_PANDAS  = {'D': None, 'S': 'W', 'M': 'ME', 'T': 'QE', 'A': 'YE'}
FREQ_NAMES   = {'D': 'Diario', 'S': 'Semanal', 'M': 'Mensual', 'T': 'Trimestral', 'A': 'Anual'}

def resample_hist(df: pd.DataFrame, freq_code: str) -> pd.DataFrame:
    """Resamplea un df con columnas date/value al código de frecuencia dado."""
    if df.empty or freq_code == 'D' or FREQ_PANDAS[freq_code] is None:
        return df
    d = df.copy()
    d['date'] = pd.to_datetime(d['date'])
    d = d.set_index('date').sort_index()
    agg = d['value'].resample(FREQ_PANDAS[freq_code]).last().dropna()
    return agg.reset_index().rename(columns={'date': 'date', 'value': 'value'})

def get_card_freq(var_id: int) -> str:
    """Devuelve la frecuencia activa para una tarjeta (per-card override o global)."""
    return st.session_state.get(f'freq_{var_id}',
           st.session_state.get('global_freq', 'M'))

# ── Helpers ───────────────────────────────────────────────────────────────────
def copy_to_clipboard_button(data_string: str, label: str = "📋 Copiar", key: str = "clip"):
    safe = data_string.replace('\\', '\\\\').replace('`', '\\`').replace('${', '\\${')
    _stcomponents.html(
        f"""<button id="btn_{key}"
            onclick="navigator.clipboard.writeText(`{safe}`)
                .then(()=>{{this.textContent='✅ Copiado!';setTimeout(()=>this.textContent='{label}',2000)}})
                .catch(()=>this.textContent='❌ Error');"
            style="padding:5px 14px;border-radius:6px;border:1px solid #d1d5db;
                   background:#f9fafb;cursor:pointer;font-size:13px;font-family:sans-serif;">
            {label}
        </button>""",
        height=42
    )

def badge_html(connector_type: str) -> str:
    ct = (connector_type or 'SCRAPER').upper()
    cls = {'API': 'badge-api', 'SCRAPER': 'badge-scraper', 'MANUAL': 'badge-manual'}.get(ct, 'badge-scraper')
    return f'<span class="{cls}">{ct}</span>'

# ── Formateo numérico ─────────────────────────────────────────────────────────
_MONETARY_UNITS = {
    'COP', 'COP/USD', 'USD', 'COP/kWh', 'USD/bbl', 'USD/MMBtu',
    'USD M', 'USD/MWh', 'COP B', 'BRL/USD', 'MXN/USD', 'EUR/USD', 'COP/kWp'
}
_PERCENT_UNITS = {'%', '% PIB'}

def format_number(value, unit: str = '') -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return str(value)
    u = (unit or '').strip()
    if u in _PERCENT_UNITS or u.endswith('%'):
        return f"{value:.2f}%"
    _PRESCALED = {'USD M', 'COP B', 'USD B', 'COP M', 'COP/kWh', 'USD/kWh',
                  'COP/MWh', 'USD/MWh', 'USD/bbl', 'USD/MMBtu', 'COP/kWp'}
    if u in _PRESCALED:
        return f"{value:,.2f}"
    if '/' in u:
        return f"{value:,.4f}"
    abs_val = abs(value)
    if abs_val >= 1e12: return f"{value/1e12:.2f} T"
    if abs_val >= 1e9:  return f"{value/1e9:.2f} B"
    if abs_val >= 1e6:  return f"{value/1e6:.2f} M"
    if abs_val >= 1e3:  return f"{value/1e3:.2f} K"
    if abs_val >= 1:    return f"{value:.4f}"
    return f"{value:.6f}"

_METRIC_CAPTIONS = {
    'TRM':                  "Tasa Representativa del Mercado: precio oficial COP por 1 USD (BanRep).",
    'IPC':                  "Índice de Precios al Consumidor: variación % anual (DANE).",
    'IBR':                  "Indicador Bancario de Referencia: costo del dinero entre bancos.",
    'PIB':                  "Producto Interno Bruto: crecimiento % del valor agregado.",
    'Desempleo':            "Tasa de desempleo: % de la PEA sin empleo.",
    'Tasa de Intervención': "Tasa de política monetaria del Banco de la República.",
    'DTF':                  "Depósito a Término Fijo a 90 días.",
    'EMBI':                 "Spread sobre US Treasuries que refleja el riesgo soberano.",
    'WTI':                  "West Texas Intermediate: referencia del crudo (USD/barril).",
    'WACC':                 "Costo promedio ponderado del capital del proyecto.",
}

def _metric_caption(var_name: str) -> str:
    for kw, cap in _METRIC_CAPTIONS.items():
        if kw.lower() in var_name.lower():
            return cap
    return ""

def _days_old(last_date_str) -> int | None:
    try:
        dt = pd.to_datetime(last_date_str)
        return (datetime.now() - dt.replace(tzinfo=None)).days
    except Exception:
        return None

def _freshness_label(days: int | None) -> str:
    if days is None:
        return "⚪ Sin fecha"
    if days <= 7:
        return f"🟢 Hace {days}d"
    if days <= 30:
        return f"🟡 Hace {days}d"
    return f"🔴 Hace {days}d"

# ── Dialog: Detalle Bloomberg de variable ────────────────────────────────────
@st.dialog("📊 Detalle de Variable", width="large")
def show_variable_detail(var_id: int, var_name: str, unit: str,
                         connector_type: str, source_url: str, description: str):
    hist_full = load_history(var_id)
    if hist_full.empty:
        st.warning("Sin datos históricos para esta variable.")
        return

    hist_full = hist_full.copy()
    hist_full['date'] = pd.to_datetime(hist_full['date'])
    hist_full = hist_full.sort_values('date')

    last_val   = hist_full['value'].iloc[-1]
    prev_val   = hist_full['value'].iloc[-2] if len(hist_full) > 1 else last_val
    delta_pct  = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
    last_date  = hist_full['date'].iloc[-1]
    days_since = (datetime.now() - last_date).days

    # ── Header KPIs ──────────────────────────────────────────────────────
    col_name, col_badge = st.columns([3, 1])
    with col_name:
        st.markdown(f"### {var_name}")
        if description:
            st.caption(description[:140])
    with col_badge:
        st.markdown(badge_html(connector_type), unsafe_allow_html=True)
        if source_url and source_url != '#':
            st.markdown(f"[🔗 Fuente oficial]({source_url})")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Último valor", format_number(last_val, unit))
    delta_color = "normal" if delta_pct == 0 else ("normal" if delta_pct > 0 else "inverse")
    c2.metric("Δ vs anterior", f"{delta_pct:+.2f}%")
    c3.metric("Máximo", format_number(hist_full['value'].max(), unit))
    c4.metric("Mínimo", format_number(hist_full['value'].min(), unit))
    c5.metric("Promedio", format_number(hist_full['value'].mean(), unit))

    st.divider()

    # ── Controles de rango y frecuencia ──────────────────────────────────
    col_r, col_f = st.columns([2, 2])
    with col_r:
        rango = st.radio("Rango", ["1M", "3M", "6M", "1A", "3A", "MAX"],
                         index=2, horizontal=True, key=f"det_range_{var_id}")
    with col_f:
        freq_d = st.radio("Frecuencia", FREQ_LABELS, index=2, horizontal=True,
                          key=f"det_freq_{var_id}",
                          format_func=lambda x: FREQ_NAMES[x])

    # Aplicar rango
    now = pd.Timestamp.now()
    range_map = {"1M": now - pd.DateOffset(months=1),
                 "3M": now - pd.DateOffset(months=3),
                 "6M": now - pd.DateOffset(months=6),
                 "1A": now - pd.DateOffset(years=1),
                 "3A": now - pd.DateOffset(years=3),
                 "MAX": hist_full['date'].min()}
    since = range_map[rango]
    hist_ranged = hist_full[hist_full['date'] >= since].copy()

    # Aplicar frecuencia
    hist_plot = resample_hist(hist_ranged, freq_d)

    # ── Gráfico principal ─────────────────────────────────────────────────
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist_plot['date'], y=hist_plot['value'],
        mode='lines+markers',
        line=dict(color='#3b82f6', width=2),
        marker=dict(size=4),
        name='Histórico',
        hovertemplate=f"%{{x|%d %b %Y}}<br><b>%{{y:,.4g}}</b> {unit}<extra></extra>"
    ))

    # Proyección overlay
    try:
        proj = VariableAgent.calculate_projection(hist_full, periods=6)
        if proj is not None and not proj.empty:
            if 'lower_80' in proj.columns:
                xb = list(proj['date']) + list(reversed(list(proj['date'])))
                yb = list(proj['upper_80']) + list(reversed(list(proj['lower_80'])))
                fig.add_trace(go.Scatter(x=xb, y=yb, fill='toself',
                    fillcolor='rgba(251,146,60,0.15)',
                    line=dict(color='rgba(0,0,0,0)'),
                    showlegend=False, hoverinfo='skip'))
            fig.add_trace(go.Scatter(
                x=proj['date'], y=proj['value'], mode='lines',
                line=dict(color='#f97316', width=2, dash='dot'),
                name='Proyección (6m)',
                hovertemplate=f"Proy: %{{y:,.4g}} {unit}<extra></extra>"
            ))
    except Exception:
        pass

    fig.update_layout(
        height=340,
        hovermode='x unified',
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis_title=unit,
        legend=dict(orientation='h', y=1.12),
        xaxis=dict(showgrid=True, gridcolor='#f3f4f6'),
        yaxis=dict(showgrid=True, gridcolor='#f3f4f6'),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"det_chart_{var_id}")

    # ── Tabla de últimos 12 valores ───────────────────────────────────────
    st.caption(f"Mostrando frecuencia: **{FREQ_NAMES[freq_d]}** · "
               f"Última actualización: **{last_date.strftime('%d %b %Y')}** "
               f"({_freshness_label(days_since)})")

    with st.expander("Ver últimos 24 valores"):
        tbl = hist_plot.tail(24)[['date', 'value']].copy()
        tbl.columns = ['Fecha', 'Valor']
        tbl['Fecha'] = tbl['Fecha'].dt.strftime('%d %b %Y')
        tbl['Valor'] = tbl['Valor'].apply(lambda x: format_number(x, unit))
        st.dataframe(tbl, hide_index=True, use_container_width=True)

# ── Tarjeta Bloomberg (render en lista) ──────────────────────────────────────
def render_bloomberg_card(row, hist, key_prefix="card"):
    """Tarjeta compacta Bloomberg con selector de frecuencia y botón de detalle."""
    var_id   = int(row['id'])
    unit     = row.get('unit', '') or ''
    ct       = row.get('connector_type', 'SCRAPER') or 'SCRAPER'
    src_url  = row.get('source_url') or '#'
    desc     = row.get('description') or ''

    # Frecuencia activa (global o per-card)
    active_freq = get_card_freq(var_id)
    hist_rs = resample_hist(hist, active_freq) if not hist.empty else hist

    with st.container(border=True):
        if not hist_rs.empty:
            last_val  = hist_rs['value'].iloc[-1]
            prev_val  = hist_rs['value'].iloc[-2] if len(hist_rs) > 1 else last_val
            delta     = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
            last_date = pd.to_datetime(hist_rs['date'].iloc[-1])
            days_old  = (datetime.now() - last_date).days

            # Cabecera: badge + frecuencia
            hcol1, hcol2 = st.columns([1, 1])
            with hcol1:
                st.markdown(badge_html(ct), unsafe_allow_html=True)
            with hcol2:
                new_freq = st.radio(
                    "", FREQ_LABELS, index=FREQ_LABELS.index(active_freq),
                    horizontal=True, key=f"freq_{var_id}_{key_prefix}",
                    label_visibility="collapsed"
                )
                if new_freq != st.session_state.get(f'freq_{var_id}'):
                    st.session_state[f'freq_{var_id}'] = new_freq

            # Nombre + valor
            delta_cls = "bb-delta-pos" if delta > 0 else ("bb-delta-neg" if delta < 0 else "bb-delta-neu")
            delta_arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "●")
            st.markdown(
                f"<div class='bb-ticker'>{row['name']}</div>"
                f"<div class='bb-value'>{format_number(last_val, unit)}"
                f"  <span class='{delta_cls}'>{delta_arrow} {abs(delta):.2f}%</span></div>"
                f"<div class='bb-date'>{'⚠️' if days_old > 30 else '📅'} "
                f"{last_date.strftime('%d %b %Y')} · {unit}</div>",
                unsafe_allow_html=True
            )

            # Mini-chart
            _y_fmt = ".2f" if unit in _PERCENT_UNITS or unit.endswith('%') else \
                     ".2s" if abs(last_val) >= 1e9 else ".4g"
            fig = px.line(hist_rs, x='date', y='value',
                          labels={'value': unit, 'date': 'Fecha'})
            fig.update_layout(
                height=120, margin=dict(l=0, r=0, t=4, b=28),
                showlegend=False,
                xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
                yaxis=dict(showticklabels=True, showgrid=False, tickformat=_y_fmt, title=''),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
            )
            line_color = '#10b981' if delta >= 0 else '#ef4444'
            fig.update_traces(line_color=line_color, line_width=2)

            # Proyección overlay en mini-chart
            try:
                proj = _cached_sparkline_proj(var_id, 3)
                if proj is not None and not proj.empty:
                    if 'lower_80' in proj.columns:
                        xb = list(proj['date']) + list(reversed(list(proj['date'])))
                        yb = list(proj['upper_80']) + list(reversed(list(proj['lower_80'])))
                        fig.add_trace(go.Scatter(x=xb, y=yb, fill='toself',
                            fillcolor='rgba(251,146,60,0.12)',
                            line=dict(color='rgba(0,0,0,0)'),
                            showlegend=False, hoverinfo='skip'))
                    fig.add_trace(go.Scatter(
                        x=proj['date'], y=proj['value'], mode='lines',
                        line=dict(color='#f97316', width=1.5, dash='dot'),
                        showlegend=False,
                        hovertemplate=f"Proy: %{{y:,.4g}} {unit}<extra></extra>"
                    ))
            except Exception:
                pass

            provider = str(row.get('api_provider') or ct).upper()
            ann_text = f"Fuente: {provider}"
            if src_url and src_url != '#':
                ann_text += f" — <a href='{src_url}'>{src_url[:50]}</a>"
            fig.add_annotation(text=ann_text, xref="paper", yref="paper",
                               x=0, y=-0.38, showarrow=False,
                               font=dict(size=8, color="#9ca3af"), xanchor="left")
            fig.update_layout(margin=dict(l=0, r=0, t=4, b=36))
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_{var_id}")

            # Caption + botón detalle
            cap = _metric_caption(row['name'])
            if cap:
                st.caption(cap)
            if st.button("Ver detalle →", key=f"det_btn_{key_prefix}_{var_id}",
                         use_container_width=True):
                show_variable_detail(var_id, row['name'], unit, ct, src_url, desc)

        else:
            # Sin datos — mostrar último conocido o placeholder
            lkg = load_last_known(var_id)
            st.markdown(badge_html(ct), unsafe_allow_html=True)
            if lkg:
                lkg_date = pd.to_datetime(lkg['date'])
                days_old = (datetime.now() - lkg_date).days
                st.markdown(
                    f"<div class='bb-ticker'>{row['name']}</div>"
                    f"<div class='bb-value'>{format_number(lkg['value'], unit)}</div>"
                    f"<div class='bb-stale'>⚠️ Último dato: {lkg_date.strftime('%d %b %Y')} "
                    f"(hace {days_old}d)</div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"<div class='bb-ticker'>{row['name']}</div>"
                    f"<div style='color:#9ca3af;font-size:0.9em;padding:8px 0'>📊 Pendiente de datos</div>",
                    unsafe_allow_html=True
                )
                source_hint = row.get('api_provider') or row.get('connector_type', '')
                if source_hint:
                    st.caption(f"Fuente configurada: **{str(source_hint).upper()}**")

# ── Sidebar Bloomberg ─────────────────────────────────────────────────────────
def render_sidebar(countries_df, variables_df):
    st.sidebar.markdown(
        "<div style='font-size:1.1em;font-weight:800;color:#1e3a8a;"
        "letter-spacing:-0.01em;margin-bottom:4px'>🧠 Cerebro Económico NLA</div>",
        unsafe_allow_html=True
    )

    # ── País ─────────────────────────────────────────────────────────────
    st.sidebar.markdown("<div class='sb-section'>País principal</div>", unsafe_allow_html=True)
    FLAG_MAP = {"Colombia": "🇨🇴", "México": "🇲🇽", "Brasil": "🇧🇷", "Ecuador": "🇪🇨"}
    country_opts = dict(zip(countries_df['name'], countries_df['id']))
    country_names = list(country_opts.keys())

    colombia_idx = next((i for i, n in enumerate(country_names) if 'colombia' in n.lower()), 0)
    _qp = st.query_params.get("pais", None)
    if _qp:
        for i, n in enumerate(country_names):
            if n.lower() == _qp.lower():
                colombia_idx = i
                break

    _opts_display = [f"{FLAG_MAP.get(n, '🌍')} {n}" for n in country_names]
    sel_idx = st.sidebar.selectbox("País", range(len(country_names)),
                                   index=colombia_idx,
                                   format_func=lambda i: _opts_display[i],
                                   label_visibility="collapsed")
    selected_name = country_names[sel_idx]
    selected_id   = country_opts[selected_name]
    st.query_params["pais"] = selected_name

    # ── Estado del sistema ───────────────────────────────────────────────
    st.sidebar.markdown("<div class='sb-section'>Estado del sistema</div>", unsafe_allow_html=True)
    try:
        _all_v = load_all_variables()
        _sv = _sa = _sr = 0
        _ahora = datetime.utcnow()
        if not _all_v.empty:
            for _, _hv in _all_v.iterrows():
                _lf = _hv.get('last_successful_fetch')
                if _lf:
                    try:
                        _d = (_ahora - pd.to_datetime(_lf).replace(tzinfo=None)).days
                        if _d <= 7:  _sv += 1
                        else:        _sa += 1
                    except Exception:
                        _sr += 1
                else:
                    _sr += 1
        _total = _sv + _sa + _sr or 1
        st.sidebar.progress(_sv / _total, text=f"🟢 {_sv}  🟡 {_sa}  🔴 {_sr}")
        _sync_time = _all_v['last_successful_fetch'].max() if not _all_v.empty and 'last_successful_fetch' in _all_v.columns else None
        if _sync_time:
            try:
                _sync_d = (datetime.utcnow() - pd.to_datetime(_sync_time).replace(tzinfo=None))
                _sync_h = int(_sync_d.total_seconds() // 3600)
                st.sidebar.caption(f"🕐 Última sync: hace {_sync_h}h")
            except Exception:
                pass
    except Exception:
        pass

    # ── Fuentes activas ──────────────────────────────────────────────────
    st.sidebar.markdown("<div class='sb-section'>Fuentes activas</div>", unsafe_allow_html=True)
    _fred_ok    = bool(os.getenv("FRED_API_KEY"))
    _banxico_ok = bool(os.getenv("BANXICO_TOKEN"))
    sources_md = (
        f"{'✅' if True else '❌'} BCB (Brasil)  "
        f"{'✅' if True else '❌'} World Bank  \n"
        f"{'✅' if _fred_ok else '⚠️'} FRED {'(activo)' if _fred_ok else '(sin clave)'}  "
        f"{'✅' if _banxico_ok else '⚠️'} Banxico {'(activo)' if _banxico_ok else '(sin clave)'}"
    )
    st.sidebar.caption(sources_md)

    # ── Frecuencia global ────────────────────────────────────────────────
    st.sidebar.markdown("<div class='sb-section'>Frecuencia global de gráficas</div>",
                        unsafe_allow_html=True)
    gf = st.sidebar.radio("Frecuencia global", FREQ_LABELS, index=2, horizontal=True,
                           key="global_freq", label_visibility="collapsed",
                           format_func=lambda x: FREQ_NAMES[x])

    # ── Acceso rápido a Data Hub ─────────────────────────────────────────
    st.sidebar.divider()
    st.sidebar.caption("📚 La **Biblioteca de Datos** está disponible en la pestaña *Data Hub*.")

    # ── Aviso Legal colapsado ────────────────────────────────────────────
    with st.sidebar.expander("⚖️ Aviso Legal", expanded=False):
        st.caption(
            "Información meramente informativa. No constituye asesoría de inversión ni "
            "recomendación financiera. Proyecciones son estimaciones estadísticas. "
            "Fuentes: BanRep, DANE, XM, FRED, BCB, Banxico. Ley 1581/2012."
        )

    return selected_name, selected_id

# ── Función principal ─────────────────────────────────────────────────────────
def main():
    st.markdown(
        "<h1 style='text-align:center;font-weight:800;'>🧠 Cerebro Económico Múlti-País NLA</h1>",
        unsafe_allow_html=True
    )
    st.markdown(
        "<p style='text-align:center;color:gray;font-size:1.05em;'>"
        "Plataforma automatizada de inteligencia macroeconómica · "
        "Colombia · México · Brasil · Ecuador</p>",
        unsafe_allow_html=True
    )
    st.divider()

    countries_df = load_countries()
    if countries_df.empty:
        st.error("⚠️ Error de configuración. Por favor contacta al administrador.")
        return

    selected_country_name, selected_country_id = render_sidebar(countries_df, load_variables(None))
    variables_df = load_variables(selected_country_id)

    # ── Tabs dinámicos ───────────────────────────────────────────────────
    _all_tabs_vars = load_all_variables()
    _show_corp  = False
    _show_latam = False
    if not _all_tabs_vars.empty and 'category' in _all_tabs_vars.columns:
        _corp_check = _all_tabs_vars[_all_tabs_vars['category'] == 'corporate_finance']
        for _, _cv in _corp_check.iterrows():
            if not load_history(_cv['id']).empty:
                _show_corp = True
                break
    if not _all_tabs_vars.empty and 'country_id' in _all_tabs_vars.columns:
        _cwd = set()
        for _, _lv in _all_tabs_vars.iterrows():
            if _lv.get('country_id') and not load_history(_lv['id']).empty:
                _cwd.add(_lv['country_id'])
                if len(_cwd) >= 2:
                    _show_latam = True
                    break

    _tab_labels = ["🌍 Global", "📊 Vista General", "⚡ Energía"]
    if _show_latam:
        _tab_labels.append("🌎 América Latina")
    _tab_labels += ["🔮 Proyecciones", "📚 Data Hub", "📋 Exportación"]
    if _show_corp:
        _tab_labels.append("🏢 Finanzas Corp.")
    _tab_labels.append("🤖 Asistente")

    _tabs_list = st.tabs(_tab_labels)
    _ti = 0
    tab_global  = _tabs_list[_ti]; _ti += 1
    tab1        = _tabs_list[_ti]; _ti += 1
    tab_energy  = _tabs_list[_ti]; _ti += 1
    tab_comp    = _tabs_list[_ti] if _show_latam else None; _ti += (1 if _show_latam else 0)
    tab_proj    = _tabs_list[_ti]; _ti += 1
    tab_hub     = _tabs_list[_ti]; _ti += 1
    tab_data    = _tabs_list[_ti]; _ti += 1
    tab_corp    = _tabs_list[_ti] if _show_corp else None;  _ti += (1 if _show_corp else 0)
    tab_agent   = _tabs_list[_ti]

    # ════════════════════════════════════════════════════════════════════════
    # TAB GLOBAL
    # ════════════════════════════════════════════════════════════════════════
    with tab_global:
        st.subheader("🌍 Mercados & Economía Global")
        st.caption("Commodities energéticos, metales críticos y resumen macro de los 4 países.")

        _fred_ok = bool(os.getenv("FRED_API_KEY"))
        global_vars_df = load_variables(5)
        all_countries_df_g = load_countries()

        if global_vars_df.empty:
            st.info("📊 Datos de mercados globales en proceso de configuración.")
        else:
            GLOBAL_KPIS = ["WTI Crude Oil", "Brent Crude Oil",
                           "Gold (Oro) Price", "DXY (Índice Dólar)",
                           "S&P 500 Index", "VIX (Índice de Volatilidad)"]
            _kpi_data = []
            for vname in GLOBAL_KPIS:
                match = global_vars_df[global_vars_df['name'] == vname]
                if not match.empty:
                    g_row = match.iloc[0]
                    h_g = load_history(int(g_row['id']))
                    if not h_g.empty:
                        _kpi_data.append((vname, g_row, h_g))

            if _kpi_data:
                kpi_cols = st.columns(len(_kpi_data))
                for ki, (vname, g_row, h_g) in enumerate(_kpi_data):
                    last_g = h_g['value'].iloc[-1]
                    prev_g = h_g['value'].iloc[-2] if len(h_g) > 1 else last_g
                    delta_g = round(((last_g - prev_g) / prev_g * 100), 2) if prev_g != 0 else 0
                    kpi_cols[ki].metric(vname.split('(')[0].strip(),
                                        format_number(last_g, g_row['unit']),
                                        f"{delta_g}%")
                    if 'VIX' in vname:
                        kpi_cols[ki].caption(
                            "🟢 Baja (<20)" if last_g < 20 else
                            "🟡 Moderada (20–30)" if last_g < 30 else "🔴 Alta (>30)")
            elif not _fred_ok:
                st.info("📊 Mercados globales requieren la variable de entorno `FRED_API_KEY`.")

            st.divider()

            COMMODITY_GROUPS = {
                "⚡ Energéticos": ["WTI Crude Oil", "Brent Crude Oil", "Henry Hub Natural Gas"],
                "🔩 Metales Críticos": ["Copper (Cobre) Price", "Aluminum (Aluminio) Price",
                                       "Lithium Carbonate Price", "Gold (Oro) Price"],
                "🌾 Agrícolas LATAM": ["Cafe (Coffee) Arabica Price", "Soja (Soybean) Price",
                                      "Maiz (Corn) Price"],
            }
            _gfreq = st.session_state.get('global_freq', 'M')
            for group_title, group_vars in COMMODITY_GROUPS.items():
                st.markdown(f"#### {group_title}")
                frames = []
                for vname in group_vars:
                    match = global_vars_df[global_vars_df['name'] == vname]
                    if match.empty: continue
                    h_c = load_history(int(match.iloc[0]['id']))
                    if h_c.empty or len(h_c) < 2: continue
                    h_c = resample_hist(h_c.copy(), _gfreq)
                    h_c['date'] = pd.to_datetime(h_c['date'])
                    h_c = h_c.sort_values('date')
                    base_val = h_c['value'].iloc[0]
                    h_c['value_norm'] = (h_c['value'] / base_val * 100).round(2) if base_val else 100.0
                    h_c['Variable'] = vname
                    frames.append(h_c[['date', 'value_norm', 'Variable']])
                if frames:
                    combined_c = pd.concat(frames, ignore_index=True)
                    fig_c = px.line(combined_c, x='date', y='value_norm', color='Variable',
                                   labels={'value_norm': '% vs base (=100)', 'date': 'Fecha'},
                                   color_discrete_sequence=px.colors.qualitative.Set2)
                    fig_c.update_layout(height=260, hovermode='x unified',
                                       margin=dict(l=0, r=0, t=10, b=0),
                                       legend=dict(orientation='h', y=-0.25))
                    fig_c.add_hline(y=100, line_dash='dash', line_color='gray',
                                   annotation_text="Base", annotation_position="right")
                    st.plotly_chart(fig_c, use_container_width=True, key=f"glob_comm_{group_title[:8]}")
                else:
                    st.info(f"📊 Datos de {group_title} requieren `FRED_API_KEY`." if not _fred_ok
                            else f"📊 {group_title} en proceso de actualización.")

            st.divider()
            st.markdown("#### 🌎 Comparativo Macro — 4 Países")
            st.caption("Últimos valores disponibles. Fuentes: BanRep, Banxico, BCB, World Bank.")

            all_vars_g = load_all_variables()
            COUNTRY_SUMMARY_MAP = {
                "🇨🇴 Colombia": {"Inflación (%)": "IPC CO (var. anual)", "PIB var. (%)": "PIB Trimestral CO",
                                 "Tasa política (%)": "Tasa de Intervención BanRep", "FX (COP/USD)": "TRM (COP/USD)",
                                 "EMBI (bps)": "EMBI Colombia", "Desempleo (%)": "Desempleo CO"},
                "🇲🇽 México":   {"Inflación (%)": "IPC MX (var. anual)", "PIB var. (%)": "PIB Trimestral MX",
                                 "Tasa política (%)": "Tasa Objetivo Banxico", "FX (MXN/USD)": "Tipo de Cambio USD/MXN",
                                 "EMBI (bps)": "EMBI México", "Desempleo (%)": "Desempleo MX"},
                "🇧🇷 Brasil":   {"Inflación (%)": "IPCA BR (var. anual)", "PIB var. (%)": "PIB Trimestral BR",
                                 "Tasa política (%)": "Tasa Selic BR", "FX (BRL/USD)": "USD/BRL",
                                 "EMBI (bps)": "EMBI Brasil", "Desempleo (%)": "Desempleo BR"},
                "🇪🇨 Ecuador":  {"Inflación (%)": "IPC Ecuador (var. anual)", "PIB var. (%)": "PIB Ecuador",
                                 "Tasa política (%)": "Tasa Interbancaria EC", "FX (USD)": "USD (dolarizado)",
                                 "EMBI (bps)": "CDS Ecuador 5Y", "Desempleo (%)": "Tasa de Desempleo"},
            }
            summary_data = {}
            for country_label, metrics in COUNTRY_SUMMARY_MAP.items():
                row_data = {}
                for metric_label, var_name_frag in metrics.items():
                    val = None
                    if not all_vars_g.empty:
                        matches = all_vars_g[all_vars_g['name'].str.lower().str.contains(
                            var_name_frag.lower()[:20], na=False, regex=False)]
                        if not matches.empty:
                            h_s = load_history(int(matches.iloc[0]['id']))
                            if not h_s.empty:
                                val = round(h_s['value'].iloc[-1], 2)
                    row_data[metric_label] = val
                summary_data[country_label] = row_data

            summary_df = pd.DataFrame(summary_data).T
            def _col_inf(val):
                if val is None or (isinstance(val, float) and pd.isna(val)): return ''
                if val > 8:  return 'background-color:#fca5a5;color:#7f1d1d'
                if val > 5:  return 'background-color:#fed7aa;color:#7c2d12'
                if val < 2:  return 'background-color:#bbf7d0;color:#14532d'
                return ''
            def _col_embi(val):
                if val is None or (isinstance(val, float) and pd.isna(val)): return ''
                if val > 800: return 'background-color:#fca5a5;color:#7f1d1d'
                if val > 400: return 'background-color:#fed7aa;color:#7c2d12'
                return ''
            try:
                styled = (summary_df.style
                    .applymap(_col_inf, subset=["Inflación (%)"] if "Inflación (%)" in summary_df.columns else [])
                    .applymap(_col_embi, subset=["EMBI (bps)"] if "EMBI (bps)" in summary_df.columns else [])
                    .format(lambda x: f"{x:.2f}" if isinstance(x, float) and not pd.isna(x) else "—"))
                st.dataframe(styled, use_container_width=True)
            except Exception:
                st.dataframe(summary_df, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — Vista General (Bloomberg cards)
    # ════════════════════════════════════════════════════════════════════════
    with tab1:
        st.subheader(f"📊 Indicadores de {selected_country_name}")
        if variables_df.empty:
            st.info("No hay variables configuradas para este país.")
        else:
            # Filtro por categoría
            cats = ['Todas'] + sorted(variables_df['category'].dropna().unique().tolist()) \
                if 'category' in variables_df.columns else ['Todas']
            col_cat, col_freq_info = st.columns([2, 2])
            with col_cat:
                sel_cat = st.selectbox("Filtrar por categoría", cats, key="t1_cat")
            with col_freq_info:
                _gf = st.session_state.get('global_freq', 'M')
                st.info(f"📅 Frecuencia global activa: **{FREQ_NAMES[_gf]}** · Cámbiala en la barra lateral.")

            filtered_vars = variables_df if sel_cat == 'Todas' else \
                variables_df[variables_df['category'] == sel_cat] \
                if 'category' in variables_df.columns else variables_df

            _SECTIONS = {
                "🌐 Sector Externo": ['external', 'fx_rates'],
                "📈 Inflación y Tasas": ['prices_inflation', 'rates_monetary', 'macro'],
                "🏭 Actividad Económica": ['gdp_activity'],
            }

            if sel_cat == 'Todas' and 'category' in variables_df.columns:
                _mapped = [c for cl in _SECTIONS.values() for c in cl]
                for sec_title, sec_cats in _SECTIONS.items():
                    sec_vars = variables_df[variables_df['category'].isin(sec_cats)]
                    if sec_vars.empty: continue
                    st.subheader(sec_title)
                    cols = st.columns(min(3, len(sec_vars)))
                    for idx, (_, row) in enumerate(sec_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            render_bloomberg_card(row, hist, key_prefix="t1s")
                other_vars = variables_df[~variables_df['category'].isin(_mapped)]
                if not other_vars.empty:
                    st.subheader("📌 Otros Indicadores")
                    cols = st.columns(min(3, len(other_vars)))
                    for idx, (_, row) in enumerate(other_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            render_bloomberg_card(row, hist, key_prefix="t1o")
            else:
                if len(filtered_vars) > 0:
                    cols = st.columns(min(3, len(filtered_vars)))
                    for idx, (_, row) in enumerate(filtered_vars.iterrows()):
                        hist = load_history(row['id'])
                        with cols[idx % 3]:
                            render_bloomberg_card(row, hist, key_prefix="t1f")
                else:
                    st.info("No hay variables en esta categoría.")

            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"### 📰 Monitor de Noticias — {selected_country_name.upper()}")
            news_data = [
                {"Fecha": "2026-04-08", "Titular": "Entidad oficial reporta sorpresa en desempleo nacional",
                 "Variable Afectada": "Desempleo", "Riesgo": "🔴 Alto", "Link": "https://www.dane.gov.co"},
                {"Fecha": "2026-04-05", "Titular": "Se mantienen tasas de intervención en última reunión",
                 "Variable Afectada": "Tasa de Intervención", "Riesgo": "🟢 Bajo",
                 "Link": "https://www.banrep.gov.co"},
                {"Fecha": "2026-04-01", "Titular": "Acuerdo en mercado energético afecta el Índice Mc",
                 "Variable Afectada": "Índice Mc", "Riesgo": "🟡 Medio", "Link": "https://www.xm.com.co"},
            ]
            st.dataframe(pd.DataFrame(news_data), use_container_width=True, hide_index=True)

    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — Sector Energético
    # ════════════════════════════════════════════════════════════════════════
    with tab_energy:
        st.subheader("⚡ Sector Energético")
        st.markdown(f"Variables del mercado energético de **{selected_country_name}** y commodities globales.")

        _gfreq = st.session_state.get('global_freq', 'M')
        all_vars = load_all_variables()
        if all_vars.empty or 'category' not in all_vars.columns:
            st.info("📊 Variables de energía en proceso de configuración.")
        else:
            energy_vars = all_vars[all_vars['category'] == 'energy']
            COUNTRY_ENERGY_CONTEXT = {
                "Colombia": {"operator": "XM — Mercado Eléctrico Mayorista",
                             "note": "Precio de Bolsa, Índice Mc, Aportes Hídricos, Cargo por Confiabilidad."},
                "Ecuador":  {"operator": "CENACE — Centro Nacional de Control de Energía",
                             "note": "Despacho centralizado. ~70% generación hidráulica. Tarifa fija por ARCERNNR."},
                "Brasil":   {"operator": "ONS / CCEE",
                             "note": "PLD equivale al Precio de Bolsa. Reservatórios = indicador crítico."},
                "México":   {"operator": "CENACE México",
                             "note": "Precio Marginal Local (PML) equivale al Precio de Bolsa."},
            }
            ctx = COUNTRY_ENERGY_CONTEXT.get(selected_country_name, {})
            if ctx:
                st.info(f"**{ctx['operator']}** — {ctx['note']}")

            if energy_vars.empty:
                st.info("📊 Variables de energía pendientes de configuración inicial.")
            else:
                energy_data = {}
                for _, erow in energy_vars.iterrows():
                    h = load_history(erow['id'])
                    if not h.empty:
                        energy_data[erow['name']] = {'df': h, 'unit': erow.get('unit', ''), 'id': erow['id']}

                if not energy_data:
                    st.info("📊 Datos energéticos en proceso de carga.")
                else:
                    kpi_cols = st.columns(min(4, len(energy_data)))
                    for ki, (vname, vinfo) in enumerate(list(energy_data.items())[:4]):
                        h = vinfo['df']
                        last_val = h['value'].iloc[-1]
                        prev_val = h['value'].iloc[-2] if len(h) > 1 else last_val
                        delta    = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val != 0 else 0
                        kpi_cols[ki % 4].metric(f"{vname} ({vinfo['unit']})",
                                                format_number(last_val, vinfo['unit']), f"{delta}%")

                    st.divider()
                    bolsa_key = next((k for k in energy_data if 'Bolsa' in k or 'PrecBol' in k.lower()), None)
                    mc_key    = next((k for k in energy_data if 'Mc' in k or 'contratos' in k.lower()), None)

                    if bolsa_key and mc_key:
                        st.markdown("#### 📈 Precio de Bolsa vs Índice Mc")
                        fig_bm = go.Figure()
                        for key, col, name in [(bolsa_key, '#f59e0b', 'Precio Bolsa'),
                                               (mc_key, '#6366f1', 'Índice Mc')]:
                            _h = resample_hist(energy_data[key]['df'], _gfreq)
                            fig_bm.add_trace(go.Scatter(x=_h['date'], y=_h['value'],
                                name=name, line=dict(color=col, width=2,
                                dash='dash' if name == 'Índice Mc' else 'solid')))
                        fig_bm.update_layout(height=300, hovermode='x unified',
                                             yaxis_title="COP/kWh",
                                             legend=dict(orientation='h', y=1.1))
                        st.plotly_chart(fig_bm, use_container_width=True, key="energy_bolsa_mc")

                    aporte_key = next((k for k in energy_data if 'Aporte' in k or 'Hídr' in k), None)
                    if aporte_key:
                        h_ap = resample_hist(energy_data[aporte_key]['df'], _gfreq)
                        st.markdown("#### 💧 Aportes Hídricos (% media histórica)")
                        last_ap = h_ap['value'].iloc[-1]
                        color   = "🔴" if last_ap < 70 else ("🟡" if last_ap < 90 else "🟢")
                        st.metric(f"Aportes actuales {color}", f"{last_ap:.1f}%")
                        if last_ap < 70:
                            st.warning("Nivel bajo — presión alcista esperada en precios de bolsa.")
                        fig_ap = px.line(h_ap, x='date', y='value')
                        fig_ap.add_hline(y=100, line_dash="dash", line_color="gray",
                                        annotation_text="Media histórica")
                        fig_ap.update_layout(height=250)
                        st.plotly_chart(fig_ap, use_container_width=True, key="energy_aportes")

                    wti_key = next((k for k in energy_data if 'WTI' in k or 'Crude' in k), None)
                    hh_key  = next((k for k in energy_data if 'Henry' in k or 'Gas' in k), None)
                    if wti_key or hh_key:
                        st.markdown("#### 🛢️ Commodities Globales")
                        fig_c = go.Figure()
                        if wti_key:
                            _h = resample_hist(energy_data[wti_key]['df'], _gfreq)
                            fig_c.add_trace(go.Scatter(x=_h['date'], y=_h['value'],
                                name="WTI (USD/bbl)", line=dict(color='#dc2626')))
                        if hh_key:
                            _h = resample_hist(energy_data[hh_key]['df'], _gfreq)
                            fig_c.add_trace(go.Scatter(x=_h['date'], y=_h['value'],
                                name="Henry Hub (USD/MMBtu)", line=dict(color='#0891b2'),
                                yaxis='y2'))
                            fig_c.update_layout(yaxis2=dict(overlaying='y', side='right'))
                        fig_c.update_layout(height=280, hovermode='x unified',
                                           legend=dict(orientation='h', y=1.1))
                        st.plotly_chart(fig_c, use_container_width=True, key="energy_commodities")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — LATAM (condicional)
    # ════════════════════════════════════════════════════════════════════════
    if tab_comp is not None:
        with tab_comp:
            st.subheader("🌎 Comparativa Macro Regional")
            st.info("Esta vista muestra **todos los países** simultáneamente, "
                    "independientemente del filtro lateral.")
            all_vars_full = load_all_variables()
            if all_vars_full.empty:
                st.warning("No hay variables definidas.")
            else:
                COMPARABLE_METRICS = {
                    "Inflación Anual (%)": ["IPC CO (var. anual)", "IPC MX (var. anual)",
                                           "IPCA BR (var. anual)", "IPC Ecuador (var. anual)"],
                    "Crecimiento PIB": ["PIB Trimestral CO (var. anual)", "PIB Trimestral MX (var. anual)",
                                       "PIB Trimestral BR (var. %)", "PIB Ecuador (USD corrientes)"],
                    "Tasa de Desempleo (%)": ["Desempleo CO", "Desempleo MX", "Desempleo BR"],
                    "Tasa Política Monetaria (%)": ["Tasa de Intervención BanRep", "Tasa Objetivo Banxico",
                                                   "Tasa Selic BR", "Fed Funds Rate (USA)"],
                    "Tipo de Cambio (Local/USD)": ["TRM (COP/USD)", "USD/MXN", "USD/BRL", "EUR/USD"],
                    "Riesgo País (EMBI bps)": ["EMBI Colombia (Riesgo País)", "EMBI México", "EMBI Brasil"],
                }
                sel_concept = st.selectbox("Concepto macroeconómico", list(COMPARABLE_METRICS.keys()))
                _gfreq = st.session_state.get('global_freq', 'M')
                vars_to_compare = all_vars_full[all_vars_full['name'].isin(COMPARABLE_METRICS[sel_concept])]
                compare_data = []
                if not vars_to_compare.empty:
                    countries_list = load_countries()
                    for _, v_row in vars_to_compare.iterrows():
                        h_df = load_history(v_row['id'])
                        if not h_df.empty:
                            h_df = resample_hist(h_df.copy(), _gfreq)
                            h_df['value'] = pd.to_numeric(h_df['value'], errors='coerce')
                            c_match = countries_list[countries_list['id'] == v_row['country_id']]
                            h_df['País'] = c_match.iloc[0]['name'] if not c_match.empty else "N/A"
                            compare_data.append(h_df)
                if compare_data:
                    combined_df = pd.concat(compare_data, ignore_index=True)
                    fig_comp = px.line(combined_df, x='date', y='value', color='País',
                                      markers=True, title=f"Evolución: {sel_concept}")
                    fig_comp.update_layout(height=420, hovermode="x unified")
                    st.plotly_chart(fig_comp, use_container_width=True, key="regional_comparison")
                    st.subheader("📊 Ranking — Último dato")
                    cols_comp = st.columns(len(compare_data))
                    for i, df_c in enumerate(compare_data):
                        cur = df_c.iloc[-1]['value']
                        prv = df_c.iloc[-2]['value'] if len(df_c) > 1 else cur
                        d   = round(((cur - prv) / prv * 100), 2) if prv != 0 else 0
                        cols_comp[i].metric(df_c['País'].iloc[0], format_number(cur), f"{d}%")
                else:
                    st.info("Sin datos históricos para comparar este indicador.")

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
            sel_var_id   = var_opts[sel_var_name]
            hist_df      = load_history(sel_var_id)

            st.markdown("#### 📈 Proyección del Modelo")
            if not hist_df.empty and len(hist_df) > 2:
                proj_result = VariableAgent.calculate_projection(hist_df, periods=6)
                if not proj_result.empty:
                    fig_proj = go.Figure()
                    if 'lower_95' in proj_result.columns:
                        xb = list(proj_result['date']) + list(reversed(list(proj_result['date'])))
                        yb = list(proj_result['upper_95']) + list(reversed(list(proj_result['lower_95'])))
                        fig_proj.add_trace(go.Scatter(x=xb, y=yb, fill='toself',
                            fillcolor='rgba(59,130,246,0.1)',
                            line=dict(color='rgba(255,255,255,0)'), name='IC 95%'))
                    if 'lower_80' in proj_result.columns:
                        xb80 = list(proj_result['date']) + list(reversed(list(proj_result['date'])))
                        yb80 = list(proj_result['upper_80']) + list(reversed(list(proj_result['lower_80'])))
                        fig_proj.add_trace(go.Scatter(x=xb80, y=yb80, fill='toself',
                            fillcolor='rgba(59,130,246,0.2)',
                            line=dict(color='rgba(255,255,255,0)'), name='IC 80%'))
                    fig_proj.add_trace(go.Scatter(x=hist_df['date'], y=hist_df['value'],
                        name='Histórico', line=dict(color='#1e3a8a', width=2)))
                    _model_lbl = proj_result['model_name'].iloc[0] if 'model_name' in proj_result.columns else 'Ensemble'
                    fig_proj.add_trace(go.Scatter(x=proj_result['date'], y=proj_result['value'],
                        name=f"Proyección ({_model_lbl})",
                        line=dict(color='#f59e0b', width=2, dash='dot'), mode='lines+markers'))

                    _sel_unit = variables_df[variables_df['id'] == sel_var_id]['unit'].values
                    _sel_unit = _sel_unit[0] if len(_sel_unit) > 0 else ''
                    try:
                        from data.consensus import get_latest_consensus_by_variable
                        consensus_overlay = get_latest_consensus_by_variable(sel_var_id)
                        if not consensus_overlay.empty:
                            SCENARIO_SYMBOLS = {'base': 'diamond', 'optimista': 'triangle-up',
                                               'pessimista': 'triangle-down', 'actual': 'circle'}
                            INST_COLORS = {'IMF WEO': '#1f77b4', 'Focus BCB (mediana)': '#2ca02c',
                                          'Banxico Encuesta': '#d62728', 'BanRep': '#9467bd',
                                          'Goldman Sachs': '#8c564b', 'JPMorgan': '#e377c2',
                                          'BBVA Research': '#7f7f7f', 'Bancolombia': '#bcbd22',
                                          'Corficolombiana': '#17becf', 'EIA': '#aec7e8'}
                            for inst, grp in consensus_overlay.groupby('source_institution'):
                                color  = INST_COLORS.get(inst, '#636363')
                                scen   = grp['scenario'].iloc[0] if 'scenario' in grp.columns else 'base'
                                symbol = SCENARIO_SYMBOLS.get(scen, 'circle')
                                fig_proj.add_trace(go.Scatter(
                                    x=pd.to_datetime(grp['target_date']),
                                    y=grp['forecast_value'],
                                    mode='markers+text', name=inst,
                                    marker=dict(size=10, symbol=symbol, color=color,
                                                line=dict(color='white', width=1)),
                                    text=[f"{v:.2f}" for v in grp['forecast_value']],
                                    textposition='top center', textfont=dict(size=9),
                                    hovertemplate=(f"<b>{inst}</b><br>Objetivo: %{{x|%b %Y}}<br>"
                                                  f"Valor: %{{y:.2f}} {_sel_unit}<extra></extra>")))
                    except Exception:
                        pass

                    fig_proj.update_layout(height=460, hovermode='x unified',
                        title=f"Proyección 6 meses + Consenso: {sel_var_name} ({_sel_unit})",
                        yaxis_title=_sel_unit,
                        legend=dict(orientation='h', y=1.15, font=dict(size=10)))
                    st.plotly_chart(fig_proj, use_container_width=True, key="projection_chart")
                    st.caption("◆ Diamante = base | ▲ = optimista | ▼ = pesimista")
                    st.dataframe(proj_result[['date', 'value']].round(4), use_container_width=True)
                else:
                    st.warning("Proyección no disponible.")
            else:
                st.warning("No hay suficientes datos históricos (mínimo 3 puntos).")

            st.markdown("#### 🏦 Consenso de Analistas")
            try:
                from data.consensus import get_latest_consensus_by_variable
                consensus_df = get_latest_consensus_by_variable(sel_var_id)
                if not consensus_df.empty:
                    if not hist_df.empty and len(hist_df) > 2:
                        proj_now = VariableAgent.calculate_projection(hist_df, periods=12)
                        if not proj_now.empty:
                            model_row = pd.DataFrame([{
                                'source_institution': '🤖 Modelo Cerebro',
                                'forecast_value': round(proj_now['value'].iloc[-1], 4),
                                'scenario': 'Ensemble',
                                'forecast_date': datetime.now().strftime('%Y-%m-%d'),
                                'target_date': str(proj_now['date'].iloc[-1])[:10]
                            }])
                            consensus_df = pd.concat([consensus_df, model_row], ignore_index=True)
                    display_cols = ['source_institution', 'forecast_value', 'scenario', 'target_date']
                    if 'notes' in consensus_df.columns: display_cols.append('notes')
                    st.dataframe(
                        consensus_df[display_cols].rename(columns={
                            'source_institution': 'Institución', 'forecast_value': 'Proyección',
                            'scenario': 'Escenario', 'target_date': 'Fecha Objetivo', 'notes': 'Notas'}),
                        use_container_width=True, hide_index=True)
                else:
                    st.info("Sin proyecciones de consenso para esta variable.")
            except Exception as e:
                st.info(f"Módulo de consenso no disponible: {e}")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 5 — DATA HUB (Biblioteca + Estado + Gestión)
    # ════════════════════════════════════════════════════════════════════════
    with tab_hub:
        st.subheader("📚 Data Hub — Biblioteca & Estado de Datos")
        st.caption("Explora, busca y gestiona todas las variables del dashboard. "
                   "Haz click en una variable para ver su detalle completo.")

        all_v_hub  = load_all_variables()
        all_c_hub  = load_countries()
        _ahora_hub = datetime.utcnow()

        if all_v_hub.empty:
            st.info("No hay variables configuradas.")
        else:
            # ── Barra de búsqueda y filtros ──────────────────────────────
            col_srch, col_cpais, col_ccat, col_cest = st.columns([3, 1.5, 1.5, 1.5])
            with col_srch:
                hub_search = st.text_input("🔍 Buscar variable", placeholder="TRM, IPC, Selic...",
                                           label_visibility="collapsed")
            with col_cpais:
                hub_paises = ['Todos'] + list(all_c_hub['name'].unique()) if not all_c_hub.empty else ['Todos']
                hub_pais_sel = st.selectbox("País", hub_paises, label_visibility="collapsed")
            with col_ccat:
                hub_cats_list = ['Todas'] + sorted(all_v_hub['category'].dropna().unique().tolist()) \
                    if 'category' in all_v_hub.columns else ['Todas']
                hub_cat_sel = st.selectbox("Categoría", hub_cats_list, label_visibility="collapsed")
            with col_cest:
                hub_est_sel = st.selectbox("Estado", ["Todos", "🟢 Al día", "🟡 Desact.", "🔴 Sin datos"],
                                           label_visibility="collapsed")

            # ── Construir tabla de estado ─────────────────────────────────
            hub_rows = []
            for _, hv in all_v_hub.iterrows():
                # Días desde última actualización
                _lf = hv.get('last_successful_fetch')
                if _lf:
                    try:
                        _d = (_ahora_hub - pd.to_datetime(_lf).replace(tzinfo=None)).days
                        if _d <= 7:  _est = "🟢"; _est_key = "al_dia"
                        elif _d <= 30: _est = "🟡"; _est_key = "desact_leve"
                        else:        _est = "🟡"; _est_key = "desact"
                    except Exception:
                        _d = None; _est = "🔴"; _est_key = "sin_datos"
                else:
                    _d = None; _est = "🔴"; _est_key = "sin_datos"

                # País
                c_match = all_c_hub[all_c_hub['id'] == hv.get('country_id')] if not all_c_hub.empty else pd.DataFrame()
                pais_name = c_match.iloc[0]['name'] if not c_match.empty else "Global"
                FLAG_MAP2 = {"Colombia": "🇨🇴", "México": "🇲🇽", "Brasil": "🇧🇷",
                             "Ecuador": "🇪🇨", "Global": "🌍"}
                pais_flag = FLAG_MAP2.get(pais_name, "🌍")

                hub_rows.append({
                    "_id": int(hv['id']),
                    "Variable": hv.get('name', ''),
                    "País": f"{pais_flag} {pais_name}",
                    "_pais_name": pais_name,
                    "Categoría": hv.get('category', '—') or '—',
                    "Fuente": str(hv.get('api_provider') or hv.get('connector_type', '—') or '—').upper(),
                    "Tipo": str(hv.get('connector_type', '—') or '—').upper(),
                    "Días": _d if _d is not None else "—",
                    "Estado": _est,
                    "_est_key": _est_key,
                    "URL": hv.get('source_url') or '',
                    "Descripción": hv.get('description') or '',
                    "Unit": hv.get('unit', ''),
                })

            hub_df = pd.DataFrame(hub_rows)

            # Aplicar filtros
            if hub_search:
                hub_df = hub_df[hub_df['Variable'].str.lower().str.contains(hub_search.lower(), na=False)]
            if hub_pais_sel != 'Todos':
                hub_df = hub_df[hub_df['_pais_name'] == hub_pais_sel]
            if hub_cat_sel != 'Todas':
                hub_df = hub_df[hub_df['Categoría'] == hub_cat_sel]
            if hub_est_sel == "🟢 Al día":
                hub_df = hub_df[hub_df['_est_key'] == 'al_dia']
            elif hub_est_sel == "🟡 Desact.":
                hub_df = hub_df[hub_df['_est_key'].str.startswith('desact')]
            elif hub_est_sel == "🔴 Sin datos":
                hub_df = hub_df[hub_df['_est_key'] == 'sin_datos']

            # ── Métricas rápidas ──────────────────────────────────────────
            _hub_ok  = (hub_df['_est_key'] == 'al_dia').sum()
            _hub_des = hub_df['_est_key'].str.startswith('desact').sum()
            _hub_sin = (hub_df['_est_key'] == 'sin_datos').sum()
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total variables", len(hub_df))
            m2.metric("🟢 Al día (≤7d)", _hub_ok)
            m3.metric("🟡 Desactualizadas", _hub_des)
            m4.metric("🔴 Sin datos", _hub_sin)

            # ── Tabla principal ───────────────────────────────────────────
            st.divider()
            display_cols_hub = ['Variable', 'País', 'Categoría', 'Fuente', 'Tipo', 'Días', 'Estado']
            st.dataframe(
                hub_df[display_cols_hub].reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
                height=min(520, len(hub_df) * 36 + 60),
                column_config={
                    "Estado": st.column_config.TextColumn("Estado", width="small"),
                    "Días":   st.column_config.NumberColumn("Días", format="%d"),
                    "Fuente": st.column_config.TextColumn("Fuente", width="small"),
                }
            )

            # ── Panel de detalle / acciones ───────────────────────────────
            st.divider()
            st.markdown("#### Acciones por variable")
            col_sel, col_act = st.columns([3, 1])
            with col_sel:
                if not hub_df.empty:
                    sel_var_hub = st.selectbox("Selecciona una variable para actuar",
                                              hub_df['Variable'].tolist(), key="hub_var_sel")
                    sel_row_hub = hub_df[hub_df['Variable'] == sel_var_hub].iloc[0]
                else:
                    st.info("Sin variables para los filtros seleccionados.")
                    sel_row_hub = None

            if sel_row_hub is not None:
                a1, a2, a3 = st.columns(3)
                with a1:
                    if st.button("📊 Ver detalle Bloomberg", use_container_width=True, key="hub_detail"):
                        show_variable_detail(
                            int(sel_row_hub['_id']), sel_row_hub['Variable'],
                            sel_row_hub['Unit'], sel_row_hub['Tipo'],
                            sel_row_hub['URL'], sel_row_hub['Descripción'])
                with a2:
                    if sel_row_hub['URL']:
                        st.link_button("🔗 Abrir fuente oficial", sel_row_hub['URL'],
                                       use_container_width=True)
                    else:
                        st.button("🔗 Sin URL configurada", disabled=True, use_container_width=True)
                with a3:
                    if st.button("🔄 Forzar actualización", use_container_width=True, key="hub_update"):
                        _var_row_full = all_v_hub[all_v_hub['id'] == sel_row_hub['_id']].iloc[0]
                        with st.spinner(f"Actualizando {sel_row_hub['Variable']}..."):
                            res = VariableAgent.ingest_variable(_var_row_full)
                            if res.get('success'):
                                st.success(res.get('message', '✅ Actualización exitosa'))
                                load_history.clear()
                            else:
                                st.error(res.get('error', 'Error desconocido'))

            # ── Checklist de brechas por país ─────────────────────────────
            st.divider()
            with st.expander("🗺️ Checklist de brechas por país", expanded=False):
                st.markdown("Variables marcadas como **🔴 sin datos** o con brecha de fuente conocida.")
                BRECHAS = {
                    "🇨🇴 Colombia": [
                        ("Cuenta Corriente CO (% PIB)", "🔴", "WorldBank BX.CAB.XOKA.GD.ZS"),
                        ("IED CO (% PIB)", "🔴", "WorldBank BX.KLT.DINV.CD.WD"),
                        ("IPC CO (var. anual)", "🟡", "BanRep API deprecada → usar WorldBank NY.CPI"),
                        ("Tasa de Intervención BanRep", "🟡", "BanRep API → scraper directo"),
                        ("Desempleo CO", "🟡", "WorldBank SL.UEM.TOTL.ZS"),
                        ("EMBI Colombia", "🟡", "Entrada manual / scraper BanRep"),
                        ("TRM (COP/USD)", "🟡", "BanRep webscraper — verificar selector"),
                    ],
                    "🇲🇽 México": [
                        ("PIB MX (var. anual)", "🔴", "Banxico SR16734 o WorldBank"),
                        ("IPC MX (var. anual)", "🔴", "Banxico SP74635 — requiere BANXICO_TOKEN"),
                        ("Tasa Objetivo Banxico", "🔴", "Banxico SF61745 — requiere BANXICO_TOKEN"),
                        ("Desempleo MX", "🔴", "Banxico SL11299 — requiere BANXICO_TOKEN"),
                        ("USD/MXN", "🔴", "Banxico SF43718 — requiere BANXICO_TOKEN"),
                    ],
                    "🇧🇷 Brasil": [
                        ("Tasa Selic BR", "🟢", "BCB SGS 432 — activo"),
                        ("IPCA BR (var. anual)", "🟢", "BCB SGS 13522 — activo"),
                        ("PIB BR (var. %)", "🟡", "BCB SGS 7326 — verificar"),
                        ("Desempleo BR", "🔴", "BCB SGS 28763 — pendiente"),
                    ],
                    "🌍 Global": [
                        ("WTI Crude Oil", "🔴", "FRED DCOILWTICO — requiere FRED_API_KEY"),
                        ("Brent Crude Oil", "🔴", "FRED DCOILBRENTEU — requiere FRED_API_KEY"),
                        ("S&P 500 Index", "🔴", "FRED SP500 — requiere FRED_API_KEY"),
                        ("VIX", "🔴", "FRED VIXCLS — requiere FRED_API_KEY"),
                        ("Gold (Oro)", "🔴", "FRED GOLDAMGBD228NLBM — requiere FRED_API_KEY"),
                        ("DXY (Índice Dólar)", "🔴", "FRED DTWEXBGS — requiere FRED_API_KEY"),
                    ],
                }
                for pais_b, items_b in BRECHAS.items():
                    st.markdown(f"**{pais_b}**")
                    for var_b, estado_b, fuente_b in items_b:
                        st.markdown(f"&nbsp;&nbsp;{estado_b} `{var_b}` — {fuente_b}")
                    st.markdown("")

            # ── Entrada manual de datos ───────────────────────────────────
            st.divider()
            with st.expander("✏️ Entrada manual de datos", expanded=False):
                st.caption("Para variables marcadas como MANUAL o sin conector API.")
                manual_vars = all_v_hub[all_v_hub.get('connector_type', pd.Series()) == 'MANUAL'] \
                    if 'connector_type' in all_v_hub.columns else pd.DataFrame()
                if not manual_vars.empty:
                    sel_manual = st.selectbox("Variable manual", manual_vars['name'].tolist(),
                                             key="hub_manual_var")
                    col_mv, col_md = st.columns(2)
                    with col_mv:
                        manual_val = st.number_input("Valor", key="hub_manual_val")
                    with col_md:
                        manual_date = st.date_input("Fecha del dato", value=date.today(),
                                                   key="hub_manual_date")
                    if st.button("💾 Guardar dato manual", key="hub_manual_save"):
                        try:
                            from data.database import save_manual_data_point
                            mv_id = manual_vars[manual_vars['name'] == sel_manual].iloc[0]['id']
                            save_manual_data_point(int(mv_id), manual_date, float(manual_val))
                            st.success(f"✅ Dato guardado: {sel_manual} = {manual_val} ({manual_date})")
                            load_history.clear()
                        except Exception as e:
                            st.error(f"No se pudo guardar: {e}")
                else:
                    st.info("No hay variables MANUAL configuradas. "
                            "Configura connector_type='MANUAL' en la base de datos.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 6 — Exportación
    # ════════════════════════════════════════════════════════════════════════
    with tab_data:
        st.subheader("📋 Datos y Exportación")
        st.markdown("Filtra, pivotea y exporta los datos económicos a CSV o Excel.")

        all_vars_df   = load_all_variables()
        all_countries_df = load_countries()

        if all_vars_df.empty:
            st.info("No hay variables disponibles.")
        else:
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                country_filter = st.multiselect("Países", all_countries_df['name'].tolist(),
                                                default=all_countries_df['name'].tolist()[:2])
            with col_f2:
                cat_filter_opts = sorted(all_vars_df['category'].dropna().unique().tolist()) \
                    if 'category' in all_vars_df.columns else ['macro']
                cat_filter = st.multiselect("Categorías", cat_filter_opts, default=cat_filter_opts)

            col_f3, col_f4 = st.columns(2)
            with col_f3:
                date_start = st.date_input("Desde", value=date(2024, 1, 1))
            with col_f4:
                date_end = st.date_input("Hasta", value=date.today())

            only_real = st.checkbox("Solo datos reales (REAL_OFFICIAL)", value=True)

            filtered_country_ids = all_countries_df[
                all_countries_df['name'].isin(country_filter)]['id'].tolist() if country_filter else []

            rows = []
            for _, vrow in all_vars_df.iterrows():
                if filtered_country_ids and vrow.get('country_id') not in filtered_country_ids: continue
                if cat_filter and vrow.get('category') not in cat_filter: continue
                h = load_history(vrow['id'])
                if h.empty: continue
                h = h.copy()
                h['date'] = pd.to_datetime(h['date'])
                h = h[(h['date'] >= pd.Timestamp(date_start)) & (h['date'] <= pd.Timestamp(date_end))]
                if only_real: h = h[h['data_type'] == 'REAL_OFFICIAL']
                if h.empty: continue
                c_name = all_countries_df[all_countries_df['id'] == vrow.get('country_id')]['name'].values
                h['País'] = c_name[0] if len(c_name) > 0 else 'N/A'
                h['Variable'] = vrow['name']; h['Unidad'] = vrow.get('unit', '')
                h['Fuente'] = vrow.get('connector_type', 'SCRAPER')
                rows.append(h)

            if rows:
                master_df = pd.concat(rows, ignore_index=True)
                master_df = master_df.rename(columns={'date': 'Fecha', 'value': 'Valor', 'data_type': 'Tipo'})
                master_df['Fecha'] = master_df['Fecha'].dt.strftime('%Y-%m-%d')
                master_df = master_df[['Fecha', 'País', 'Variable', 'Valor', 'Unidad', 'Tipo', 'Fuente']].sort_values(['Variable', 'Fecha'])

                view_mode = st.radio("Vista", ["Tabla plana", "Pivot (fechas × series)", "Resumen estadístico"],
                                     horizontal=True)
                if view_mode == "Tabla plana":
                    st.dataframe(master_df, use_container_width=True, hide_index=True)
                elif view_mode == "Pivot (fechas × series)":
                    pivot_df = master_df.pivot_table(index='Fecha', columns='Variable',
                                                     values='Valor', aggfunc='mean')
                    st.dataframe(pivot_df, use_container_width=True)
                elif view_mode == "Resumen estadístico":
                    stats = master_df.groupby('Variable')['Valor'].agg(
                        Último='last', Min='min', Max='max',
                        Promedio='mean', Mediana='median', StdDev='std', N='count'
                    ).round(4).reset_index()
                    st.dataframe(stats, use_container_width=True, hide_index=True)

                st.divider()
                _export_df = pivot_df if view_mode == "Pivot (fechas × series)" else master_df
                col_d1, col_d2, col_d3 = st.columns(3)
                with col_d1:
                    st.download_button("⬇️ CSV", _export_df.to_csv(index=(view_mode=="Pivot (fechas × series)")).encode('utf-8'),
                                       f"cerebro_{date.today()}.csv", "text/csv")
                with col_d2:
                    try:
                        import openpyxl
                        xlsx_buf = io.BytesIO()
                        with pd.ExcelWriter(xlsx_buf, engine='openpyxl') as writer:
                            master_df.to_excel(writer, sheet_name='Datos', index=False)
                            if view_mode == "Pivot (fechas × series)": pivot_df.to_excel(writer, sheet_name='Pivot')
                            for sheet in writer.sheets.values():
                                for col_cells in sheet.columns:
                                    sheet.column_dimensions[col_cells[0].column_letter].width = \
                                        min(max(len(str(c.value or '')) for c in col_cells) + 4, 40)
                        xlsx_buf.seek(0)
                        st.download_button("⬇️ XLSX", xlsx_buf.getvalue(),
                                           f"cerebro_{date.today()}.xlsx",
                                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    except ImportError:
                        st.caption("Instala `openpyxl` para exportar XLSX.")
                with col_d3:
                    copy_to_clipboard_button(_export_df.to_csv(index=False, sep='\t'),
                                            label="📋 Copiar TSV", key="exp_tsv")
            else:
                st.info("No hay datos para los filtros seleccionados.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB CORP — Finanzas Corporativas
    # ════════════════════════════════════════════════════════════════════════
    if tab_corp is not None:
        with tab_corp:
            st.subheader("🏢 Finanzas Corporativas — Modelos Exagon & Ruitoque")
            all_corp = load_all_variables()
            if all_corp.empty:
                st.info("📊 Datos en proceso de carga.")
            else:
                corp_vars = all_corp[all_corp['category'] == 'corporate_finance'] \
                    if 'category' in all_corp.columns else pd.DataFrame()
                if corp_vars.empty:
                    st.info("📊 Habilitado cuando haya datos de finanzas corporativas.")
                else:
                    EXCEL_VARS = ["WACC - Costo Promedio de Capital", "Costo de la Deuda (Kd)",
                                  "Costo del Equity (Ke)", "Tarifa PPA (Precio Venta de Energía)",
                                  "TIR Proyecto (IRR)", "CAPEX Solar Total (USD por proyecto)"]
                    excel_subset = corp_vars[corp_vars['name'].isin(EXCEL_VARS)]
                    st.markdown("#### 📌 Indicadores Clave del Proyecto")
                    kpi_cols_c = st.columns(3)
                    for ki, (_, row) in enumerate(excel_subset.iterrows()):
                        h = load_history(row['id'])
                        with kpi_cols_c[ki % 3]:
                            with st.container(border=True):
                                if not h.empty:
                                    val  = h['value'].iloc[-1]
                                    unit = row.get('unit', '')
                                    disp = f"{val:.2f}%" if unit == '%' else \
                                           f"USD {val:,.0f}" if unit == 'USD' else \
                                           f"{val:.0f} COP/kWh" if unit == 'COP/kWh' else \
                                           f"{val:,.3g} {unit}"
                                    st.metric(row['name'], disp)
                                    st.caption(str(row.get('description', ''))[:120])
                                else:
                                    st.metric(row['name'], "—")
                                    st.caption("📊 Datos en proceso.")

                    st.divider()
                    st.markdown("#### 📉 Estructura del WACC")
                    wacc_vals = {}
                    for label, var_name in [("Kd (Deuda)", "Costo de la Deuda (Kd)"),
                                            ("Ke (Equity)", "Costo del Equity (Ke)"),
                                            ("WACC", "WACC - Costo Promedio de Capital")]:
                        r = corp_vars[corp_vars['name'] == var_name]
                        if not r.empty:
                            h = load_history(int(r.iloc[0]['id']))
                            if not h.empty: wacc_vals[label] = h['value'].iloc[-1]
                    if wacc_vals:
                        fig_w = go.Figure(go.Bar(
                            x=list(wacc_vals.keys()), y=list(wacc_vals.values()),
                            marker_color=['#6366f1', '#f59e0b', '#10b981'],
                            text=[f"{v:.2f}%" for v in wacc_vals.values()], textposition='outside'))
                        fig_w.update_layout(title="WACC vs Componentes (%)", yaxis_title="%",
                                           height=300, plot_bgcolor='rgba(0,0,0,0)')
                        st.plotly_chart(fig_w, use_container_width=True, key="corp_wacc")
                    else:
                        st.info("📊 Sin datos suficientes para la estructura de capital.")

                    st.divider()
                    st.markdown("#### 📊 Sensibilidad PPA vs IPP")
                    ppa_row = corp_vars[corp_vars['name'] == "Tarifa PPA (Precio Venta de Energía)"]
                    ipp_row = all_corp[all_corp['name'] == "IPP CO (var. anual)"]
                    ppa_base = load_history(int(ppa_row.iloc[0]['id']))['value'].iloc[-1] \
                        if not ppa_row.empty and not load_history(int(ppa_row.iloc[0]['id'])).empty else 300
                    ipp_val  = load_history(int(ipp_row.iloc[0]['id']))['value'].iloc[-1] \
                        if not ipp_row.empty and not load_history(int(ipp_row.iloc[0]['id'])).empty else 4.4
                    c_s1, c_s2 = st.columns(2)
                    c_s1.metric("PPA Base (COP/kWh)", f"{ppa_base:.0f}")
                    c_s2.metric("IPP CO (%)", f"{ipp_val:.2f}%")
                    sens_data = {f"Año {y}": {f"IPP {i:.0f}%": round(ppa_base*((1+i/100)**y), 1)
                                              for i in [2,3,4,5,6,7]} for y in [1,2,3,5,10]}
                    sens_df = pd.DataFrame(sens_data).T
                    st.caption(f"Proyección PPA (COP/kWh) indexado a IPP, base {ppa_base:.0f}")
                    st.dataframe(sens_df.style.highlight_max(axis=0, color='#d1fae5')
                                            .highlight_min(axis=0, color='#fee2e2'),
                                use_container_width=True)

    # ════════════════════════════════════════════════════════════════════════
    # TAB AGENTE
    # ════════════════════════════════════════════════════════════════════════
    with tab_agent:
        st.subheader("🤖 Asistente de Datos")
        st.markdown("Haz preguntas sobre los datos económicos. "
                    "El asistente busca en la base de datos y responde con valores actuales.")

        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []

        for msg in st.session_state.chat_history:
            with st.chat_message(msg['role']):
                st.markdown(msg['content'])

        user_q = st.chat_input("¿Cuál es la TRM hoy? / ¿Cómo está la inflación en Colombia?")
        if user_q:
            st.session_state.chat_history.append({'role': 'user', 'content': user_q})
            with st.chat_message('user'):
                st.markdown(user_q)
            with st.chat_message('assistant'):
                with st.spinner("Consultando datos..."):
                    try:
                        from ai_engine.chatbot import answer_question
                        response = answer_question(user_q, load_all_variables(), load_history)
                    except Exception:
                        response = ("Lo siento, no pude procesar tu pregunta. Intenta de nuevo.\n\n"
                                   "*Análisis informativo. Verifica en la fuente oficial.*")
                st.markdown(response)
                st.session_state.chat_history.append({'role': 'assistant', 'content': response})

        if st.session_state.chat_history:
            if st.button("🗑 Limpiar conversación", key="clear_chat"):
                st.session_state.chat_history = []
                st.rerun()

        st.divider()
        st.markdown("### 🔧 Administración de Variables")

        with st.expander("📊 Estado del Sistema", expanded=False):
            all_v_adm = load_all_variables()
            total_v = len(all_v_adm)
            v_data  = sum(1 for _, r in all_v_adm.iterrows() if not load_history(r['id']).empty)
            v_err   = sum(1 for _, r in all_v_adm.iterrows()
                         if r.get('fetch_error_count', 0) and int(r.get('fetch_error_count', 0) or 0) > 0)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Variables totales", total_v)
            c2.metric("Con datos", f"{v_data}/{total_v}")
            c3.metric("Errores activos", v_err)
            c4.metric("Revisión", datetime.now().strftime('%H:%M'))

        st.markdown("#### 🔄 Actualizar Variables")
        if not variables_df.empty:
            if st.button("🚀 Actualizar TODAS las variables activas", type="primary"):
                pb = st.progress(0)
                status_txt = st.empty()
                ok_c = 0
                for i, (_, row) in enumerate(variables_df.iterrows()):
                    status_txt.text(f"Actualizando {row['name']}...")
                    if VariableAgent.ingest_variable(row).get('success'):
                        ok_c += 1
                    pb.progress((i + 1) / len(variables_df))
                status_txt.text(f"✅ {ok_c}/{len(variables_df)} variables actualizadas.")
                load_history.clear()

            st.markdown("##### Variables individuales")
            for _, row in variables_df.iterrows():
                ca, ci, cb = st.columns([2, 2, 1])
                with ca:
                    st.markdown(f"**{row['name']}** {badge_html(row.get('connector_type','SCRAPER'))}",
                               unsafe_allow_html=True)
                with ci:
                    lf = row.get('last_successful_fetch', '')
                    if lf: st.caption(f"Última: {str(lf)[:16]}")
                    errs = row.get('fetch_error_count', 0)
                    if errs and int(errs) > 0: st.caption(f"⚠️ {errs} errores")
                with cb:
                    if st.button("↺", key=f"btn_{row['id']}"):
                        with st.spinner("..."):
                            res = VariableAgent.ingest_variable(row)
                            if res.get('success'):
                                st.success("✅"); load_history.clear()
                            else:
                                st.error("❌")
                st.divider()

        st.markdown("#### 🏦 Agregar Proyección de Consenso")
        try:
            from data.consensus import save_consensus_forecast
            with st.expander("➕ Nueva Proyección"):
                all_v3 = load_all_variables()
                if not all_v3.empty:
                    var_cons_opts = dict(zip(all_v3['name'], all_v3['id']))
                    sel_vc    = st.selectbox("Variable", list(var_cons_opts.keys()), key="cons_var")
                    inst      = st.text_input("Institución", placeholder="Bancolombia, BanRep...")
                    tgt_dt    = st.date_input("Fecha objetivo", key="cons_date")
                    cons_val  = st.number_input("Valor proyectado", key="cons_val")
                    scen      = st.selectbox("Escenario", ["base", "optimista", "pesimista"], key="cons_scen")
                    notes_c   = st.text_area("Notas", key="cons_notes")
                    if st.button("💾 Guardar", key="cons_save"):
                        save_consensus_forecast(
                            variable_id=var_cons_opts[sel_vc],
                            source_institution=inst,
                            forecast_date=datetime.now(),
                            target_date=datetime.combine(tgt_dt, datetime.min.time()),
                            value=cons_val, scenario=scen, notes=notes_c)
                        st.success("Proyección guardada.")
        except ImportError:
            st.info("Módulo de consenso no disponible.")
        except Exception:
            st.error("No se pudo guardar la proyección.")


if __name__ == "__main__":
    main()
