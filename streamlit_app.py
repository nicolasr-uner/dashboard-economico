import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import difflib
import io
import os
import streamlit.components.v1 as _stcomponents

from data.database import (
    get_countries, get_variables, get_historical_data,
    get_ai_logs, get_all_variable_names, get_variables_by_name,
    get_last_known_value
)
from data.agent import VariableAgent

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

  /* ── Tarjeta Bloomberg ─────────────────────────────────────────────── */
  .bb-ticker { font-size: 0.70em; font-weight: 700; letter-spacing: 0.09em;
               color: #6b7280; text-transform: uppercase; margin-bottom: 1px; }
  .bb-value  { font-family: 'JetBrains Mono', monospace; font-size: 1.45em;
               font-weight: 600; color: #111827; line-height: 1.15; }
  .bb-delta-pos { color: #10b981; font-size: 0.80em; font-weight: 700; }
  .bb-delta-neg { color: #ef4444; font-size: 0.80em; font-weight: 700; }
  .bb-delta-neu { color: #9ca3af; font-size: 0.80em; font-weight: 600; }
  .bb-meta   { font-size: 0.68em; color: #9ca3af; margin-top: 3px; }
  .bb-stale  { color: #f59e0b; font-size: 0.68em; font-weight: 600; }
  .bb-freq-tag { display:inline-block; font-size:0.60em; font-weight:700;
                 letter-spacing:0.06em; background:#f3f4f6; color:#6b7280;
                 padding:1px 6px; border-radius:4px; text-transform:uppercase; }

  /* ── Badges ───────────────────────────────────────────────────────── */
  .badge-api     { background:#d1fae5; color:#065f46; padding:2px 7px; border-radius:10px; font-size:0.68em; font-weight:700; }
  .badge-scraper { background:#dbeafe; color:#1e40af; padding:2px 7px; border-radius:10px; font-size:0.68em; font-weight:700; }
  .badge-manual  { background:#fef3c7; color:#92400e; padding:2px 7px; border-radius:10px; font-size:0.68em; font-weight:700; }

  /* ── Sidebar limpio ───────────────────────────────────────────────── */
  .sb-label { font-size:0.65em; font-weight:700; letter-spacing:0.10em;
              color:#9ca3af; text-transform:uppercase; margin:10px 0 3px; }
  .sb-src   { font-size:0.78em; line-height:1.6; }

  /* ── KPIs en modal ────────────────────────────────────────────────── */
  .kpi-box  { background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px;
              padding:10px 14px; text-align:center; }
  .kpi-lbl  { font-size:0.65em; color:#9ca3af; font-weight:600; text-transform:uppercase; }
  .kpi-val  { font-family:'JetBrains Mono',monospace; font-size:1.15em; font-weight:700;
              color:#111827; margin-top:2px; }

  h1 { color: #1e3a8a; }
  .stMetric { background:white; padding:12px 16px; border-radius:10px;
              box-shadow:0 2px 8px rgba(0,0,0,0.08); }

  /* ── Modo oscuro ────────────────────────────────────────────── */
  body.dark-mode, body.dark-mode [class*="css"],
  body.dark-mode .stApp { background:#0d1117 !important; color:#e6edf3 !important; }
  body.dark-mode .bb-value  { color:#e6edf3 !important; }
  body.dark-mode .bb-ticker { color:#8b949e !important; }
  body.dark-mode .bb-meta   { color:#8b949e !important; }
  body.dark-mode .kpi-box   { background:#161b22 !important; border-color:#30363d !important; }
  body.dark-mode .kpi-val   { color:#e6edf3 !important; }
  body.dark-mode .bb-freq-tag { background:#21262d !important; color:#8b949e !important; }
  body.dark-mode h1 { color:#58a6ff !important; }
  body.dark-mode .stMetric { background:#161b22 !important; }
  body.dark-mode [data-baseweb="tab-list"] { background:#161b22 !important; }
</style>
""", unsafe_allow_html=True)

# ── Cachés ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_countries():      return get_countries()

@st.cache_data(ttl=3600)
def load_variables(country_id=None): return get_variables(country_id)

@st.cache_data(ttl=3600)
def load_history(variable_id):       return get_historical_data(variable_id)

@st.cache_data(ttl=3600)
def load_all_variables():            return get_variables()

@st.cache_data(ttl=3600)
def load_last_known(variable_id):    return get_last_known_value(variable_id)

@st.cache_data(ttl=3600)
def _cached_proj(var_id: int, periods: int = 6, frequency: str = 'monthly'):
    try:
        h = load_history(var_id)
        if h.empty or len(h) < 3: return None
        # Ajustar períodos de proyección según la frecuencia natural del dato
        freq_periods = {
            'daily': 30,       # 30 días hacia adelante
            'weekly': 12,      # 12 semanas (~3 meses)
            'monthly': 6,      # 6 meses
            'quarterly': 4,    # 4 trimestres (1 año)
            'annual': 3,       # 3 años
        }
        adjusted_periods = freq_periods.get(frequency, periods)
        return VariableAgent.calculate_projection(h, periods=adjusted_periods)
    except Exception: return None

# ── Frecuencia natural del dato ───────────────────────────────────────────────
# Jerarquía: cuántos puntos representa cada nivel (meses)
_FREQ_MONTHS = {'daily': 0.033, 'weekly': 0.25, 'monthly': 1,
                'quarterly': 3,  'annual': 12}
_FREQ_LABEL  = {'daily': 'Diario', 'weekly': 'Semanal', 'monthly': 'Mensual',
                'quarterly': 'Trimestral', 'annual': 'Anual'}

# Opciones de agregación válidas para cada frecuencia natural
_VALID_AGG = {
    'daily':     [('D','Diario'), ('S','Semanal'), ('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'weekly':    [('S','Semanal'), ('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'monthly':   [('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'quarterly': [('T','Trimestral'), ('A','Anual')],
    'annual':    [('A','Anual')],
}
_PANDAS_FREQ = {'D': None, 'S': 'W', 'M': 'ME', 'T': 'QE', 'A': 'YE'}

# Rango por defecto según frecuencia natural
_DEFAULT_RANGE = {'daily': '6M', 'weekly': '1A', 'monthly': '2A',
                  'quarterly': '5A', 'annual': 'MAX'}

RANGE_OPTIONS = ['1M', '3M', '6M', '1A', '2A', '5A', 'MAX']

def _range_since(rng: str) -> pd.Timestamp | None:
    now = pd.Timestamp.now()
    m = {'1M':1,'3M':3,'6M':6,'1A':12,'2A':24,'5A':60}
    return now - pd.DateOffset(months=m[rng]) if rng in m else None

def resample_hist(df: pd.DataFrame, agg_code: str) -> pd.DataFrame:
    if df.empty or _PANDAS_FREQ.get(agg_code) is None: return df
    d = df.copy()
    d['date'] = pd.to_datetime(d['date'])
    return d.set_index('date').sort_index()['value'] \
            .resample(_PANDAS_FREQ[agg_code]).last().dropna() \
            .reset_index()

def filter_range(df: pd.DataFrame, rng: str) -> pd.DataFrame:
    if df.empty: return df
    since = _range_since(rng)
    if since is None: return df
    d = df.copy(); d['date'] = pd.to_datetime(d['date'])
    return d[d['date'] >= since]

# ── Helpers ───────────────────────────────────────────────────────────────────
def copy_to_clipboard_button(data_string: str, label="📋 Copiar", key="clip"):
    safe = data_string.replace('\\','\\\\').replace('`','\\`').replace('${','\\${')
    _stcomponents.html(
        f"""<button id="btn_{key}"
            onclick="navigator.clipboard.writeText(`{safe}`)
              .then(()=>{{this.textContent='✅ Copiado!';setTimeout(()=>this.textContent='{label}',2000)}})
              .catch(()=>this.textContent='❌ Error');"
            style="padding:5px 14px;border-radius:6px;border:1px solid #d1d5db;
                   background:#f9fafb;cursor:pointer;font-size:13px;">{label}</button>""",
        height=42)

def badge_html(ct: str) -> str:
    ct = (ct or 'SCRAPER').upper()
    cls = {'API':'badge-api','SCRAPER':'badge-scraper','MANUAL':'badge-manual'}.get(ct,'badge-scraper')
    return f'<span class="{cls}">{ct}</span>'

_PERCENT_UNITS = {'%', '% PIB'}
_PRESCALED     = {'USD M','COP B','USD B','COP M','COP/kWh','USD/kWh',
                  'COP/MWh','USD/MWh','USD/bbl','USD/MMBtu','COP/kWp'}

def format_number(v, unit=''):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "—"
    try: v = float(v)
    except: return str(v)
    u = (unit or '').strip()
    if u in _PERCENT_UNITS or u.endswith('%'): return f"{v:.2f}%"
    if u in _PRESCALED: return f"{v:,.2f}"
    if '/' in u: return f"{v:,.4f}"
    av = abs(v)
    if av >= 1e12: return f"{v/1e12:.2f} T"
    if av >= 1e9:  return f"{v/1e9:.2f} B"
    if av >= 1e6:  return f"{v/1e6:.2f} M"
    if av >= 1e3:  return f"{v/1e3:.2f} K"
    if av >= 1:    return f"{v:.4f}"
    return f"{v:.6f}"

_CAPTIONS = {
    'TRM': "Tasa Representativa del Mercado: COP por 1 USD (BanRep).",
    'IPC': "Índice de Precios al Consumidor: variación % anual (DANE).",
    'IBR': "Indicador Bancario de Referencia: costo del dinero interbancario.",
    'PIB': "Producto Interno Bruto: crecimiento % del valor agregado.",
    'Desempleo': "Tasa de desempleo: % de la PEA sin empleo.",
    'Tasa de Intervención': "Tasa de política monetaria del Banco de la República.",
    'EMBI': "Spread sobre US Treasuries — riesgo soberano del país.",
    'WTI': "West Texas Intermediate: crudo de referencia (USD/bbl).",
    'WACC': "Costo promedio ponderado del capital.",
}
def _caption(name):
    for k, v in _CAPTIONS.items():
        if k.lower() in name.lower(): return v
    return ""

# ── Dialog de Detalle Bloomberg ───────────────────────────────────────────────
@st.dialog("📊 Detalle de Variable", width="large")
def show_variable_detail(var_id, var_name, unit, ct, src_url, desc, nat_freq='monthly'):
    hist_full = load_history(var_id)
    if hist_full.empty:
        st.warning("Sin datos históricos para esta variable."); return

    hist_full = hist_full.copy()
    hist_full['date'] = pd.to_datetime(hist_full['date'])
    hist_full = hist_full.sort_values('date')

    last_val  = hist_full['value'].iloc[-1]
    prev_val  = hist_full['value'].iloc[-2] if len(hist_full) > 1 else last_val
    delta_pct = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val else 0
    last_date = hist_full['date'].iloc[-1]
    days_old  = (datetime.now() - last_date).days

    # ── Cabecera ──────────────────────────────────────────────────────────
    c_name, c_badge = st.columns([4, 1])
    with c_name:
        st.markdown(f"### {var_name}")
        freq_lbl = _FREQ_LABEL.get(nat_freq, nat_freq)
        st.caption(f"{desc[:120] if desc else ''} · Frecuencia natural: **{freq_lbl}**")
    with c_badge:
        st.markdown(badge_html(ct), unsafe_allow_html=True)
        if src_url and src_url != '#':
            st.markdown(f"[🔗 Fuente]({src_url})")

    # ── KPIs ─────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    for col, lbl, val in [
        (k1, "Último valor",   format_number(last_val, unit)),
        (k2, "Δ vs anterior",  f"{delta_pct:+.2f}%"),
        (k3, "Máximo",         format_number(hist_full['value'].max(), unit)),
        (k4, "Mínimo",         format_number(hist_full['value'].min(), unit)),
        (k5, "Promedio",       format_number(hist_full['value'].mean(), unit)),
    ]:
        col.markdown(
            f"<div class='kpi-box'><div class='kpi-lbl'>{lbl}</div>"
            f"<div class='kpi-val'>{val}</div></div>",
            unsafe_allow_html=True)

    st.divider()

    # ── Controles: Rango + Agregación ─────────────────────────────────────
    # Rango: siempre disponible, con default inteligente
    default_rng = _DEFAULT_RANGE.get(nat_freq, '2A')
    c_rng, c_agg = st.columns([3, 2])
    with c_rng:
        rng = st.radio("Rango de tiempo", RANGE_OPTIONS,
                       index=RANGE_OPTIONS.index(default_rng),
                       horizontal=True, key=f"det_rng_{var_id}")
    with c_agg:
        # Agregación: solo opciones válidas para la frecuencia natural
        valid_opts  = _VALID_AGG.get(nat_freq, [('M', 'Mensual')])
        agg_labels  = [lbl for _, lbl in valid_opts]
        agg_codes   = [code for code, _ in valid_opts]
        default_agg = agg_labels[0]  # primera opción = frecuencia natural
        sel_agg_lbl = st.radio("Agregación", agg_labels, horizontal=True,
                               key=f"det_agg_{var_id}")
        agg_code = agg_codes[agg_labels.index(sel_agg_lbl)]

    # Aplicar rango
    hist_ranged = filter_range(hist_full, rng)
    # Aplicar agregación (solo upward)
    hist_plot = resample_hist(hist_ranged, agg_code)
    if hist_plot.empty: hist_plot = hist_ranged

    # ── Gráfico ───────────────────────────────────────────────────────────
    line_col = '#10b981' if delta_pct >= 0 else '#ef4444'
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist_plot['date'], y=hist_plot['value'],
        mode='lines+markers', line=dict(color=line_col, width=2),
        marker=dict(size=4), name='Histórico',
        hovertemplate=f"%{{x|%d %b %Y}}<br><b>%{{y:,.4g}}</b> {unit}<extra></extra>"))

    # Proyección (solo si estamos viendo al menos 6 meses de datos)
    try:
        if len(hist_plot) >= 3:
            proj = _cached_proj(var_id, 6)
            if proj is not None and not proj.empty:
                if 'lower_80' in proj.columns:
                    xb = list(proj['date']) + list(reversed(list(proj['date'])))
                    yb = list(proj['upper_80']) + list(reversed(list(proj['lower_80'])))
                    fig.add_trace(go.Scatter(x=xb, y=yb, fill='toself',
                        fillcolor='rgba(251,146,60,0.12)',
                        line=dict(color='rgba(0,0,0,0)'), showlegend=False, hoverinfo='skip'))
                fig.add_trace(go.Scatter(
                    x=proj['date'], y=proj['value'], mode='lines',
                    line=dict(color='#f97316', width=2, dash='dot'),
                    name='Proyección',
                    hovertemplate=f"Proy: %{{y:,.4g}} {unit}<extra></extra>"))
    except Exception: pass

    fig.update_layout(
        height=320, hovermode='x unified',
        margin=dict(l=0, r=0, t=8, b=0),
        yaxis_title=unit,
        legend=dict(orientation='h', y=1.12),
        xaxis=dict(showgrid=True, gridcolor='#f3f4f6'),
        yaxis=dict(showgrid=True, gridcolor='#f3f4f6'),
        plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True, key=f"det_chart_{var_id}")

    staleness = f"🔴 Hace {days_old}d" if days_old > 30 else f"🟡 Hace {days_old}d" if days_old > 7 else f"🟢 Hace {days_old}d"
    st.caption(f"Frecuencia visualizada: **{sel_agg_lbl}** · Última actualización: **{last_date.strftime('%d %b %Y')}** · {staleness}")

    with st.expander("Ver últimos 20 valores"):
        tbl = hist_plot.tail(20)[['date','value']].copy()
        tbl.columns = ['Fecha','Valor']
        tbl['Fecha'] = tbl['Fecha'].dt.strftime('%d %b %Y')
        tbl['Valor'] = tbl['Valor'].apply(lambda x: format_number(x, unit))
        st.dataframe(tbl, hide_index=True, use_container_width=True)

# ── Tarjeta Bloomberg con rango de tiempo ─────────────────────────────────────
def render_bloomberg_card(row, hist, key_prefix="card", compact=False):
    """
    Tarjeta Bloomberg/Yahoo Finance con rango per-card.
    compact=True → chart más pequeño, sin botón detalle expandido.
    """
    var_id   = int(row['id'])
    unit     = row.get('unit','') or ''
    ct       = row.get('connector_type','SCRAPER') or 'SCRAPER'
    src_url  = row.get('source_url') or '#'
    desc     = row.get('description') or ''
    nat_freq = (row.get('frequency') or 'monthly').lower()
    freq_lbl = _FREQ_LABEL.get(nat_freq, nat_freq)

    # Rango activo (por tarjeta, default inteligente según frecuencia natural)
    default_rng = _DEFAULT_RANGE.get(nat_freq, '2A')
    active_rng  = st.session_state.get(f'rng_{var_id}', default_rng)

    with st.container(border=True):
        if not hist.empty:
            hist = hist.copy()
            hist['date'] = pd.to_datetime(hist['date'])

            # Filtrar por rango ANTES de calcular deltas
            hist_full = hist.sort_values('date')
            hist_view = filter_range(hist_full, active_rng)
            if hist_view.empty: hist_view = hist_full

            last_val  = hist_full['value'].iloc[-1]  # delta siempre vs dato anterior real
            prev_val  = hist_full['value'].iloc[-2] if len(hist_full) > 1 else last_val
            delta     = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val else 0
            last_date = hist_full['date'].iloc[-1]
            days_old  = (datetime.now() - last_date).days

            # ── Cabecera: badge + frecuencia + staleness dot + rango ─────────
            staleness_dot = (
                "<span style='color:#ef4444;font-size:0.75em' title='Datos con más de 30 días'>🔴</span>"
                if days_old > 30 else
                "<span style='color:#f59e0b;font-size:0.75em' title='Datos de hace 7-30 días'>🟡</span>"
                if days_old > 7 else
                "<span style='color:#10b981;font-size:0.75em' title='Datos recientes'>🟢</span>"
            )
            hc1, hc2 = st.columns([1, 2])
            with hc1:
                st.markdown(
                    f"{badge_html(ct)} "
                    f"<span class='bb-freq-tag'>{freq_lbl}</span> {staleness_dot}",
                    unsafe_allow_html=True)
            with hc2:
                # Selector de rango como radio compacto
                rng_opts = ['1M','3M','6M','1A','2A','5A','MAX']
                new_rng = st.radio("", rng_opts, index=rng_opts.index(active_rng),
                                   horizontal=True, key=f"rng_{var_id}_{key_prefix}",
                                   label_visibility="collapsed")
                if new_rng != st.session_state.get(f'rng_{var_id}'):
                    st.session_state[f'rng_{var_id}'] = new_rng

            # ── Valor principal ───────────────────────────────────────────
            delta_cls = "bb-delta-pos" if delta > 0 else ("bb-delta-neg" if delta < 0 else "bb-delta-neu")
            arrow     = "▲" if delta > 0 else ("▼" if delta < 0 else "●")
            stale_ico = "⚠️ " if days_old > 30 else ""
            st.markdown(
                f"<div class='bb-ticker'>{row['name']}</div>"
                f"<div class='bb-value'>{format_number(last_val, unit)}"
                f"  <span class='{delta_cls}'>{arrow} {abs(delta):.2f}%</span></div>"
                f"<div class='bb-meta'>{stale_ico}{last_date.strftime('%d %b %Y')} · {unit}</div>",
                unsafe_allow_html=True)

            # ── Mini-chart (respeta el rango, sin resamplear) ─────────────
            _yfmt = ".2f" if unit in _PERCENT_UNITS or unit.endswith('%') else \
                    ".2s" if abs(last_val) >= 1e9 else ".4g"
            line_color = '#10b981' if delta >= 0 else '#ef4444'

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hist_view['date'], y=hist_view['value'],
                mode='lines', line=dict(color=line_color, width=2),
                hovertemplate=f"%{{x|%d %b %Y}}<br>%{{y:,.4g}} {unit}<extra></extra>"))

            # Proyección solo si rango ≥ 1A y hay suficientes datos
            try:
                if active_rng in ('1A','2A','5A','MAX') and len(hist_full) >= 6:
                    proj = _cached_proj(var_id, 3, nat_freq)
                    if proj is not None and not proj.empty:
                        if 'lower_80' in proj.columns:
                            xb = list(proj['date']) + list(reversed(list(proj['date'])))
                            yb = list(proj['upper_80']) + list(reversed(list(proj['lower_80'])))
                            fig.add_trace(go.Scatter(x=xb, y=yb, fill='toself',
                                fillcolor='rgba(251,146,60,0.12)',
                                line=dict(color='rgba(0,0,0,0)'), showlegend=False, hoverinfo='skip'))
                        fig.add_trace(go.Scatter(
                            x=proj['date'], y=proj['value'], mode='lines',
                            line=dict(color='#f97316', width=1.5, dash='dot'),
                            showlegend=False,
                            hovertemplate=f"Proy: %{{y:,.4g}} {unit}<extra></extra>"))
            except Exception: pass

            provider = str(row.get('api_provider') or ct).upper()
            ann = f"Fuente: {provider}"
            if src_url and src_url != '#': ann += f" — <a href='{src_url}'>{src_url[:45]}</a>"
            ch = 75 if compact else 110
            fig.update_layout(
                height=ch, margin=dict(l=0, r=0, t=2, b=34),
                showlegend=False, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
                yaxis=dict(showticklabels=True, showgrid=False, tickformat=_yfmt, title=''))
            fig.add_annotation(text=ann, xref="paper", yref="paper",
                               x=0, y=-0.42, showarrow=False,
                               font=dict(size=8, color="#9ca3af"), xanchor="left")
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_{var_id}")

            if not compact:
                cap = _caption(row['name'])
                if cap: st.caption(cap)
            if st.button("Ver detalle →", key=f"det_{key_prefix}_{var_id}",
                         use_container_width=True):
                show_variable_detail(var_id, row['name'], unit, ct, src_url, desc, nat_freq)

        else:
            # Sin datos
            lkg = load_last_known(var_id)
            nat_freq_v = (row.get('frequency') or 'monthly').lower()
            st.markdown(badge_html(ct), unsafe_allow_html=True)
            if lkg:
                lkg_date = pd.to_datetime(lkg['date'])
                days_old = (datetime.now() - lkg_date).days
                st.markdown(
                    f"<div class='bb-ticker'>{row['name']}</div>"
                    f"<div class='bb-value'>{format_number(lkg['value'], unit)}</div>"
                    f"<div class='bb-stale'>⚠️ Último: {lkg_date.strftime('%d %b %Y')} ({days_old}d)</div>",
                    unsafe_allow_html=True)
            else:
                src = row.get('api_provider') or ''
                ct_type = row.get('connector_type', 'MANUAL')
                if ct_type == 'MANUAL':
                    hint = "Usa **Entrada manual** en Data Hub para cargar valores."
                elif src:
                    hint = f"Conector `{src.upper()}` configurado. Usa **🔄 Forzar actualización** en Data Hub."
                else:
                    hint = "Variable pendiente de configuración de fuente."
                st.markdown(
                    f"<div class='bb-ticker'>{row['name']}</div>"
                    f"<div style='color:#9ca3af;font-size:0.85em;padding:6px 0'>"
                    f"📊 Sin datos — {hint}</div>",
                    unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
def render_sidebar(countries_df):
    st.sidebar.markdown(
        "<div style='font-size:1.05em;font-weight:800;color:#1e3a8a;"
        "letter-spacing:-0.01em;margin-bottom:2px'>🧠 Cerebro Económico NLA</div>"
        "<div style='font-size:0.72em;color:#9ca3af;margin-bottom:8px'>"
        "Inteligencia macroeconómica multi-país</div>",
        unsafe_allow_html=True)

    # País
    st.sidebar.markdown("<div class='sb-label'>País principal</div>", unsafe_allow_html=True)
    country_opts  = dict(zip(countries_df['name'], countries_df['id']))
    country_names = list(country_opts.keys())
    FLAG = {"Colombia":"🇨🇴","México":"🇲🇽","Brasil":"🇧🇷","Ecuador":"🇪🇨"}

    default_idx = next((i for i,n in enumerate(country_names) if 'colombia' in n.lower()), 0)
    _qp = st.query_params.get("pais","")
    if _qp:
        for i,n in enumerate(country_names):
            if n.lower() == _qp.lower(): default_idx = i; break

    sel_idx = st.sidebar.selectbox(
        "País", range(len(country_names)), index=default_idx,
        format_func=lambda i: f"{FLAG.get(country_names[i],'🌍')} {country_names[i]}",
        label_visibility="collapsed")
    name = country_names[sel_idx]
    st.query_params["pais"] = name

    # Estado del sistema
    st.sidebar.markdown("<div class='sb-label'>Estado del sistema</div>", unsafe_allow_html=True)
    try:
        av = load_all_variables()
        sv=sa=sr=0; _now = datetime.utcnow()
        if not av.empty:
            for _, hv in av.iterrows():
                lf = hv.get('last_successful_fetch')
                if lf:
                    try:
                        d = (_now - pd.to_datetime(lf).replace(tzinfo=None)).days
                        sv += 1 if d <= 7 else 0; sa += 1 if d > 7 else 0
                    except: sr += 1
                else: sr += 1
        tot = sv+sa+sr or 1
        st.sidebar.progress(sv/tot)
        st.sidebar.caption(f"🟢 {sv} al día  ·  🟡 {sa} rezagadas  ·  🔴 {sr} sin datos")
        last_sync = av['last_successful_fetch'].max() if not av.empty and 'last_successful_fetch' in av.columns else None
        if last_sync:
            h = int((datetime.utcnow() - pd.to_datetime(last_sync).replace(tzinfo=None)).total_seconds()//3600)
            st.sidebar.caption(f"🕐 Última sync: hace {h}h")
    except: pass

    # Fuentes
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
    st.sidebar.caption("📚 Biblioteca completa en la pestaña **Data Hub**.")
    st.sidebar.caption("📊 Cada gráfica tiene su propio selector de rango y frecuencia.")

    # Modo oscuro
    st.sidebar.divider()
    dark = st.sidebar.toggle("🌙 Modo oscuro", value=st.session_state.get('dark_mode', False),
                             key="dark_mode_toggle")
    st.session_state['dark_mode'] = dark
    _stcomponents.html(
        f"""<script>
        var b = window.parent.document.body;
        {"b.classList.add('dark-mode');" if dark else "b.classList.remove('dark-mode');"}
        </script>""", height=0)

    with st.sidebar.expander("⚖️ Aviso Legal", expanded=False):
        st.caption("Información meramente informativa. No constituye asesoría de inversión. "
                   "Fuentes: BanRep, DANE, XM, FRED, BCB, Banxico. Ley 1581/2012.")

    return name, country_opts[name]

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    st.markdown("<h1 style='text-align:center;font-weight:800'>🧠 Cerebro Económico Múlti-País NLA</h1>",
                unsafe_allow_html=True)
    st.markdown("<p style='text-align:center;color:gray;font-size:1.05em'>"
                "Plataforma automatizada de inteligencia macroeconómica · "
                "Colombia · México · Brasil · Ecuador</p>", unsafe_allow_html=True)
    st.divider()

    countries_df = load_countries()
    if countries_df.empty:
        st.error("⚠️ Error de configuración. Contacta al administrador."); return

    sel_name, sel_id = render_sidebar(countries_df)
    variables_df     = load_variables(sel_id)
    _all_v           = load_all_variables()

    # ── Panel de onboarding (colapsable) ─────────────────────────────────────
    if not st.session_state.get('onboarding_dismissed', False):
        with st.expander("💡 ¿Cómo usar este dashboard? (click para colapsar)", expanded=False):
            st.markdown("""
**Navegación rápida**
- Usa las **pestañas** para navegar entre vistas: *Dashboard* (tarjetas por país) · *Proyecciones* (consensus + modelo estadístico) · *LATAM* (comparación regional) · *Energía* (mercados eléctricos).
- El **selector de país** en el sidebar cambia las tarjetas del Dashboard.

**Tarjetas Bloomberg**
- Cada tarjeta tiene su propio **selector de rango** (1M · 3M · 6M · 1A · 2A · 5A · MAX). El default se elige según la frecuencia del dato.
- El punto de color (🟢🟡🔴) indica la antigüedad del último dato: verde < 7 días, amarillo < 30, rojo > 30.
- El botón **"Ver detalle →"** abre un panel con KPIs históricos, gráfico ampliado y proyecciones.

**Proyecciones**
- La **línea naranja punteada** en los gráficos es la proyección estadística (Ensemble Holt-Winters + ARIMA).
- El tab **Proyecciones** superpone también las proyecciones de instituciones (IMF, BanRep, Bancolombia, Goldman Sachs, etc.) con distintos marcadores.

**Actualización de datos**
- En el tab **Data Hub** puedes forzar la actualización de cualquier variable o cargar datos manualmente.
            """)
            if st.button("✅ Entendido, no mostrar de nuevo", key="dismiss_onboarding"):
                st.session_state['onboarding_dismissed'] = True
                st.rerun()

    # ── Búsqueda global con fuzzy matching ────────────────────────────────────
    if not _all_v.empty:
        all_names = sorted(_all_v['name'].dropna().unique().tolist())
        sc1, sc2 = st.columns([3, 1])
        with sc1:
            search_q = st.text_input("", placeholder="🔍  Buscar variable  (ej: TRM, IPC, PIB, Selic, WACC...)",
                                     label_visibility="collapsed", key="global_search")
        if search_q.strip():
            q = search_q.strip().lower()
            # Coincidencia exacta parcial primero, luego fuzzy
            exact_matches = [n for n in all_names if q in n.lower()]
            fuzzy_matches = difflib.get_close_matches(q, [n.lower() for n in all_names],
                                                       n=5, cutoff=0.4)
            fuzzy_names = [all_names[[n.lower() for n in all_names].index(m)]
                           for m in fuzzy_matches if m not in [n.lower() for n in exact_matches]]
            matches = exact_matches + [n for n in fuzzy_names if n not in exact_matches]
            if matches:
                with sc2:
                    pick = st.selectbox("", matches, label_visibility="collapsed", key="search_pick")
                hit = _all_v[_all_v['name'] == pick]
                if not hit.empty:
                    r = hit.iloc[0]
                    nat_f = (r.get('frequency') or 'monthly').lower()
                    show_variable_detail(int(r['id']), r['name'],
                                         r.get('unit',''), r.get('connector_type','API'),
                                         r.get('source_url','#'), r.get('description',''), nat_f)
            else:
                st.caption(f"Sin resultados para «{search_q}» — intenta con: TRM, IPC, Selic, PIB, WACC")

    # Tabs dinámicos
    _show_corp  = False; _show_latam = False
    if not _all_v.empty and 'category' in _all_v.columns:
        for _, cv in _all_v[_all_v['category']=='corporate_finance'].iterrows():
            if not load_history(cv['id']).empty: _show_corp = True; break
    if not _all_v.empty and 'country_id' in _all_v.columns:
        cwd = set()
        for _, lv in _all_v.iterrows():
            if lv.get('country_id') and not load_history(lv['id']).empty:
                cwd.add(lv['country_id'])
                if len(cwd) >= 2: _show_latam = True; break

    # Tier 1: Análisis principal
    labels = ["📊 Dashboard", "🔮 Proyecciones", "🌎 Comparación LATAM", "⚡ Mercados Energía"]
    # Tier 2: Contexto global y finanzas
    labels.append("🌍 Contexto Global")
    if _show_corp: labels.append("🏢 Finanzas de Proyectos")
    # Tier 3: Herramientas
    labels += ["📚 Data Hub", "📋 Exportar Datos", "🤖 Asistente IA"]

    # Nuevo orden: Dashboard | Proyecciones | LATAM | Energía | Global | [Corp] | Data Hub | Exportar | Asistente
    tabs = st.tabs(labels); ti=0
    tab_vista   = tabs[ti]; ti+=1   # 📊 Dashboard
    tab_proj    = tabs[ti]; ti+=1   # 🔮 Proyecciones
    tab_latam   = tabs[ti]; ti+=1   # 🌎 Comparación LATAM
    tab_energy  = tabs[ti]; ti+=1   # ⚡ Mercados Energía
    tab_global  = tabs[ti]; ti+=1   # 🌍 Contexto Global
    tab_corp    = tabs[ti] if _show_corp else None; ti+=(1 if _show_corp else 0)  # 🏢 Finanzas de Proyectos
    tab_hub     = tabs[ti]; ti+=1   # 📚 Data Hub
    tab_export  = tabs[ti]; ti+=1   # 📋 Exportar Datos
    tab_agent   = tabs[ti]          # 🤖 Asistente IA

    # ════════════════════════════════════════════════════════════════════════
    # GLOBAL
    # ════════════════════════════════════════════════════════════════════════
    with tab_global:
        st.subheader("🌍 Mercados & Economía Global")
        st.caption("Commodities energéticos, metales y resumen macro de los 4 países.")
        _fred_ok  = bool(os.getenv("FRED_API_KEY"))
        gvars     = load_variables(5)

        if gvars.empty:
            st.info("📊 Mercados globales en proceso de configuración.")
        else:
            # KPI row
            KPIS = ["WTI Crude Oil","Brent Crude Oil","Gold (Oro) Price",
                    "DXY (Índice Dólar)","S&P 500 Index","VIX (Índice de Volatilidad)"]
            kpi_data = [(n, r, h) for n in KPIS
                        for r in [gvars[gvars['name']==n].iloc[0]] if not gvars[gvars['name']==n].empty
                        for h in [load_history(int(r['id']))] if not h.empty]
            if kpi_data:
                kcols = st.columns(len(kpi_data))
                for ki,(vn,gr,hg) in enumerate(kpi_data):
                    lv=hg['value'].iloc[-1]; pv=hg['value'].iloc[-2] if len(hg)>1 else lv
                    d=round(((lv-pv)/pv*100),2) if pv else 0
                    kcols[ki].metric(vn.split('(')[0].strip(), format_number(lv,gr['unit']), f"{d}%")
                    if 'VIX' in vn:
                        kcols[ki].caption("🟢 <20" if lv<20 else "🟡 20-30" if lv<30 else "🔴 >30")
            elif not _fred_ok:
                st.info("📊 Mercados globales requieren `FRED_API_KEY`.")
            st.divider()

            # Grupos de commodities — rango propio por grupo
            GROUPS = {
                "⚡ Energéticos": ["WTI Crude Oil","Brent Crude Oil","Henry Hub Natural Gas"],
                "🔩 Metales Críticos": ["Copper (Cobre) Price","Aluminum (Aluminio) Price",
                                       "Lithium Carbonate Price","Gold (Oro) Price"],
                "🌾 Agrícolas LATAM": ["Cafe (Coffee) Arabica Price","Soja (Soybean) Price","Maiz (Corn) Price"],
            }
            for gtitle, gvnames in GROUPS.items():
                st.markdown(f"#### {gtitle}")
                # Rango propio para este grupo (datos diarios → default 1A)
                grng = st.radio("Rango", ['3M','6M','1A','3A','MAX'],
                                index=2, horizontal=True, key=f"grng_{gtitle[:6]}",
                                label_visibility="collapsed")
                frames=[]
                for vn in gvnames:
                    m = gvars[gvars['name']==vn]
                    if m.empty: continue
                    hc = load_history(int(m.iloc[0]['id']))
                    if hc.empty or len(hc)<2: continue
                    hc = filter_range(hc.copy(), grng)
                    if hc.empty: continue
                    hc['date'] = pd.to_datetime(hc['date']); hc=hc.sort_values('date')
                    b = hc['value'].iloc[0]
                    hc['pct'] = (hc['value']/b*100).round(2) if b else 100.0
                    hc['Variable'] = vn; frames.append(hc[['date','pct','Variable']])
                if frames:
                    df_c = pd.concat(frames,ignore_index=True)
                    fig_c = px.line(df_c, x='date', y='pct', color='Variable',
                                   labels={'pct':'% vs inicio período','date':'Fecha'},
                                   color_discrete_sequence=px.colors.qualitative.Set2)
                    fig_c.update_layout(height=240, hovermode='x unified',
                                       margin=dict(l=0,r=0,t=6,b=0),
                                       legend=dict(orientation='h',y=-0.28))
                    fig_c.add_hline(y=100, line_dash='dash', line_color='#9ca3af',
                                   annotation_text="Base", annotation_position="right")
                    st.plotly_chart(fig_c, use_container_width=True, key=f"gc_{gtitle[:6]}")
                else:
                    st.info(f"📊 {gtitle} requiere `FRED_API_KEY`." if not _fred_ok
                            else f"📊 {gtitle} en proceso de actualización.")
            st.divider()

            # Tabla comparativa 4 países
            st.markdown("#### 🌎 Comparativo Macro — 4 Países")
            CMAP = {
                "🇨🇴 Colombia": {"Inflación (%)":"IPC CO (var. anual)","PIB var. (%)":"PIB Trimestral CO",
                                 "Tasa política (%)":"Tasa de Intervención BanRep","FX (COP/USD)":"TRM (COP/USD)",
                                 "EMBI (bps)":"EMBI Colombia","Desempleo (%)":"Desempleo CO"},
                "🇲🇽 México":   {"Inflación (%)":"IPC MX (var. anual)","PIB var. (%)":"PIB Trimestral MX",
                                 "Tasa política (%)":"Tasa Objetivo Banxico","FX (MXN/USD)":"Tipo de Cambio USD/MXN",
                                 "EMBI (bps)":"EMBI México","Desempleo (%)":"Desempleo MX"},
                "🇧🇷 Brasil":   {"Inflación (%)":"IPCA BR (var. anual)","PIB var. (%)":"PIB Trimestral BR",
                                 "Tasa política (%)":"Tasa Selic BR","FX (BRL/USD)":"USD/BRL",
                                 "EMBI (bps)":"EMBI Brasil","Desempleo (%)":"Desempleo BR"},
                "🇪🇨 Ecuador":  {"Inflación (%)":"IPC Ecuador (var. anual)","PIB var. (%)":"PIB Ecuador",
                                 "Tasa política (%)":"Tasa Interbancaria EC","FX (USD)":"USD (dolarizado)",
                                 "EMBI (bps)":"CDS Ecuador 5Y","Desempleo (%)":"Tasa de Desempleo"},
            }
            all_vg = load_all_variables()
            sdata = {}
            for clbl, mets in CMAP.items():
                rd = {}
                for ml, vf in mets.items():
                    val = None
                    if not all_vg.empty:
                        mt = all_vg[all_vg['name'].str.lower().str.contains(vf.lower()[:20],na=False,regex=False)]
                        if not mt.empty:
                            hs = load_history(int(mt.iloc[0]['id']))
                            if not hs.empty: val = round(hs['value'].iloc[-1],2)
                    rd[ml] = val
                sdata[clbl] = rd
            sdf = pd.DataFrame(sdata).T
            def _ci(v):
                if v is None or (isinstance(v,float) and pd.isna(v)): return ''
                if v>8: return 'background-color:#fca5a5;color:#7f1d1d'
                if v>5: return 'background-color:#fed7aa;color:#7c2d12'
                if v<2: return 'background-color:#bbf7d0;color:#14532d'
                return ''
            def _ce(v):
                if v is None or (isinstance(v,float) and pd.isna(v)): return ''
                if v>800: return 'background-color:#fca5a5;color:#7f1d1d'
                if v>400: return 'background-color:#fed7aa;color:#7c2d12'
                return ''
            try:
                st.dataframe(
                    sdf.style
                    .applymap(_ci, subset=["Inflación (%)"] if "Inflación (%)" in sdf.columns else [])
                    .applymap(_ce, subset=["EMBI (bps)"]    if "EMBI (bps)"    in sdf.columns else [])
                    .format(lambda x: f"{x:.2f}" if isinstance(x,float) and not pd.isna(x) else "—"),
                    use_container_width=True)
            except: st.dataframe(sdf, use_container_width=True)
            st.caption("Verde = inflación baja | Naranja/Rojo = inflación alta o EMBI elevado.")

    # ════════════════════════════════════════════════════════════════════════
    # VISTA GENERAL — Bloomberg cards con rango coherente
    # ════════════════════════════════════════════════════════════════════════
    with tab_vista:
        st.subheader(f"📊 Indicadores de {sel_name}")
        if variables_df.empty:
            st.info("No hay variables configuradas para este país.")
        else:
            c_cat, c_dens, c_info = st.columns([2, 1, 2])
            with c_cat:
                cats = ['Todas'] + sorted(variables_df['category'].dropna().unique().tolist()) \
                    if 'category' in variables_df.columns else ['Todas']
                sel_cat = st.selectbox("Filtrar por categoría", cats, key="t1_cat")
            with c_dens:
                compact_mode = st.toggle("Vista compacta", value=False, key="compact_toggle")
            with c_info:
                st.info("📅 Cada tarjeta tiene su propio selector de rango · "
                        "Click en **Ver detalle →** para cambiar agregación.")

            n_cols = 4 if compact_mode else 3
            filtered = variables_df if sel_cat=='Todas' else \
                variables_df[variables_df['category']==sel_cat] \
                if 'category' in variables_df.columns else variables_df

            SECS = {
                "🌐 Sector Externo":      ['external','fx_rates'],
                "📈 Inflación y Tasas":   ['prices_inflation','rates_monetary','macro'],
                "🏭 Actividad Económica": ['gdp_activity'],
            }
            if sel_cat=='Todas' and 'category' in variables_df.columns:
                mapped = [c for cl in SECS.values() for c in cl]
                for stitle, scats in SECS.items():
                    sv = variables_df[variables_df['category'].isin(scats)]
                    if sv.empty: continue
                    st.subheader(stitle)
                    cols = st.columns(min(n_cols, len(sv)))
                    for i,(_, row) in enumerate(sv.iterrows()):
                        with cols[i % n_cols]: render_bloomberg_card(row, load_history(row['id']), "sec", compact_mode)
                ov = variables_df[~variables_df['category'].isin(mapped)]
                if not ov.empty:
                    st.subheader("📌 Otros Indicadores")
                    cols = st.columns(min(n_cols, len(ov)))
                    for i,(_, row) in enumerate(ov.iterrows()):
                        with cols[i % n_cols]: render_bloomberg_card(row, load_history(row['id']), "oth", compact_mode)
            else:
                if len(filtered)>0:
                    cols = st.columns(min(n_cols, len(filtered)))
                    for i,(_, row) in enumerate(filtered.iterrows()):
                        with cols[i % n_cols]: render_bloomberg_card(row, load_history(row['id']), "flt", compact_mode)
                else: st.info("No hay variables en esta categoría.")

            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown(f"### 📰 Monitor de Noticias — {sel_name.upper()}")
            st.dataframe(pd.DataFrame([
                {"Fecha":"2026-04-08","Titular":"Entidad oficial reporta sorpresa en desempleo nacional",
                 "Variable":"Desempleo","Riesgo":"🔴 Alto","Fuente":"https://www.dane.gov.co"},
                {"Fecha":"2026-04-05","Titular":"Se mantienen tasas de intervención en última reunión",
                 "Variable":"Tasa Intervención","Riesgo":"🟢 Bajo","Fuente":"https://www.banrep.gov.co"},
                {"Fecha":"2026-04-01","Titular":"Acuerdo energético afecta Índice Mc",
                 "Variable":"Índice Mc","Riesgo":"🟡 Medio","Fuente":"https://www.xm.com.co"},
            ]), use_container_width=True, hide_index=True)

    # ════════════════════════════════════════════════════════════════════════
    # ENERGÍA — rango propio, sin frecuencia global
    # ════════════════════════════════════════════════════════════════════════
    with tab_energy:
        st.subheader("⚡ Sector Energético")
        st.markdown(f"Variables del mercado energético de **{sel_name}** y commodities globales.")
        all_vars_e = load_all_variables()

        CTX = {
            "Colombia": ("XM — Mercado Eléctrico Mayorista",
                         "Precio de Bolsa, Índice Mc, Aportes Hídricos, Cargo por Confiabilidad."),
            "Ecuador":  ("CENACE — Centro Nacional de Control de Energía",
                         "Despacho centralizado. ~70% generación hidráulica."),
            "Brasil":   ("ONS / CCEE",
                         "PLD equivale al Precio de Bolsa. Reservatórios = indicador crítico."),
            "México":   ("CENACE México",
                         "Precio Marginal Local (PML) equivale al Precio de Bolsa."),
        }
        ctx = CTX.get(sel_name)
        if ctx: st.info(f"**{ctx[0]}** — {ctx[1]}")

        if all_vars_e.empty or 'category' not in all_vars_e.columns:
            st.info("📊 Variables de energía en proceso de configuración.")
        else:
            evars = all_vars_e[all_vars_e['category']=='energy']
            if evars.empty:
                st.info("📊 Variables de energía pendientes de configuración inicial.")
            else:
                edata = {r['name']:{'df':load_history(r['id']),'unit':r.get('unit',''),'id':r['id'],'freq':r.get('frequency','daily')}
                         for _,r in evars.iterrows() if not load_history(r['id']).empty}
                if not edata:
                    st.info("📊 Datos energéticos en proceso de carga.")
                else:
                    # KPIs
                    kcols = st.columns(min(4,len(edata)))
                    for ki,(vn,vi) in enumerate(list(edata.items())[:4]):
                        lv=vi['df']['value'].iloc[-1]; pv=vi['df']['value'].iloc[-2] if len(vi['df'])>1 else lv
                        d=round(((lv-pv)/pv*100),2) if pv else 0
                        kcols[ki%4].metric(f"{vn} ({vi['unit']})", format_number(lv,vi['unit']), f"{d}%")
                    st.divider()

                    # Rango único para todas las gráficas de energía
                    e_rng = st.radio("Rango de tiempo", ['1M','3M','6M','1A','2A'],
                                     index=1, horizontal=True, key="energy_rng",
                                     label_visibility="collapsed")
                    st.caption(f"Rango seleccionado: **{e_rng}** — aplica a todas las gráficas de energía.")

                    bk = next((k for k in edata if 'Bolsa' in k or 'PrecBol' in k.lower()), None)
                    mk = next((k for k in edata if ' Mc' in k or 'contratos' in k.lower()), None)
                    if bk and mk:
                        st.markdown("#### 📈 Precio de Bolsa vs Índice Mc")
                        fig_bm = go.Figure()
                        for k, col, nm in [(bk,'#f59e0b','Precio Bolsa'),(mk,'#6366f1','Índice Mc')]:
                            hf = filter_range(edata[k]['df'].copy(), e_rng)
                            fig_bm.add_trace(go.Scatter(x=hf['date'],y=hf['value'],
                                name=nm, line=dict(color=col,width=2,dash='dash' if nm=='Índice Mc' else 'solid')))
                        fig_bm.update_layout(height=280, hovermode='x unified', yaxis_title="COP/kWh",
                                             legend=dict(orientation='h',y=1.1))
                        st.plotly_chart(fig_bm, use_container_width=True, key="e_bolsa_mc")

                    ak = next((k for k in edata if 'Aporte' in k or 'Hídr' in k), None)
                    if ak:
                        hap = filter_range(edata[ak]['df'].copy(), e_rng)
                        la = hap['value'].iloc[-1] if not hap.empty else 0
                        st.markdown("#### 💧 Aportes Hídricos (% media histórica)")
                        ico = "🔴" if la<70 else ("🟡" if la<90 else "🟢")
                        st.metric(f"Aportes actuales {ico}", f"{la:.1f}%")
                        if la<70: st.warning("Nivel bajo — presión alcista esperada en precios de bolsa.")
                        fa = px.line(hap, x='date', y='value')
                        fa.add_hline(y=100,line_dash="dash",line_color="#9ca3af",annotation_text="Media histórica")
                        fa.update_layout(height=220)
                        st.plotly_chart(fa, use_container_width=True, key="e_aportes")

                    wk = next((k for k in edata if 'WTI' in k or 'Crude' in k), None)
                    hk = next((k for k in edata if 'Henry' in k or 'Gas' in k), None)
                    if wk or hk:
                        st.markdown("#### 🛢️ Commodities Globales")
                        fc = go.Figure()
                        if wk:
                            hw = filter_range(edata[wk]['df'].copy(), e_rng)
                            fc.add_trace(go.Scatter(x=hw['date'],y=hw['value'],name="WTI (USD/bbl)",line=dict(color='#dc2626')))
                        if hk:
                            hh = filter_range(edata[hk]['df'].copy(), e_rng)
                            fc.add_trace(go.Scatter(x=hh['date'],y=hh['value'],name="Henry Hub (USD/MMBtu)",line=dict(color='#0891b2'),yaxis='y2'))
                            fc.update_layout(yaxis2=dict(overlaying='y',side='right'))
                        fc.update_layout(height=260,hovermode='x unified',legend=dict(orientation='h',y=1.1))
                        st.plotly_chart(fc, use_container_width=True, key="e_comm")

    # ════════════════════════════════════════════════════════════════════════
    # LATAM — rango inteligente según frecuencia del concepto seleccionado
    # ════════════════════════════════════════════════════════════════════════
    if tab_latam is not None:
        with tab_latam:
            st.subheader("🌎 Comparativa Macro Regional")
            st.info("Vista de **todos los países** simultáneamente.")
            all_vf = load_all_variables()
            if all_vf.empty:
                st.warning("No hay variables definidas.")
            else:
                CONCEPTS = {
                    "Inflación Anual (%)":         (["IPC CO (var. anual)","IPC MX (var. anual)","IPCA BR (var. anual)","IPC Ecuador (var. anual)"], 'monthly'),
                    "Crecimiento PIB":              (["PIB Trimestral CO (var. anual)","PIB Trimestral MX (var. anual)","PIB Trimestral BR (var. %)","PIB Ecuador (USD corrientes)"], 'quarterly'),
                    "Tasa de Desempleo (%)":        (["Desempleo CO","Desempleo MX","Desempleo BR"], 'monthly'),
                    "Tasa Política Monetaria (%)":  (["Tasa de Intervención BanRep","Tasa Objetivo Banxico","Tasa Selic BR"], 'monthly'),
                    "Tipo de Cambio (Local/USD)":   (["TRM (COP/USD)","USD/MXN","USD/BRL","EUR/USD"], 'daily'),
                    "Riesgo País — EMBI (bps)":     (["EMBI Colombia (Riesgo País)","EMBI México","EMBI Brasil"], 'monthly'),
                }
                sel_c = st.selectbox("Concepto macroeconómico", list(CONCEPTS.keys()))
                names_c, nat_f_c = CONCEPTS[sel_c]

                # Rango con default inteligente según frecuencia natural del concepto
                def_rng_c = _DEFAULT_RANGE.get(nat_f_c, '2A')
                rng_opts_c = ['6M','1A','2A','5A','MAX']
                rng_c = st.radio("Rango de tiempo", rng_opts_c,
                                 index=rng_opts_c.index(def_rng_c) if def_rng_c in rng_opts_c else 2,
                                 horizontal=True, key="latam_rng")
                # Nota de coherencia
                st.caption(f"Frecuencia natural de este indicador: **{_FREQ_LABEL.get(nat_f_c, nat_f_c)}** · "
                           f"Rango por defecto: **{def_rng_c}**")

                clist = load_countries()
                vtc   = all_vf[all_vf['name'].isin(names_c)]
                cdata = []
                if not vtc.empty:
                    for _, vr in vtc.iterrows():
                        hd = load_history(vr['id'])
                        if hd.empty: continue
                        hd = filter_range(hd.copy(), rng_c)
                        hd['value'] = pd.to_numeric(hd['value'], errors='coerce')
                        cm = clist[clist['id']==vr['country_id']]
                        hd['País'] = cm.iloc[0]['name'] if not cm.empty else "N/A"
                        cdata.append(hd)
                if cdata:
                    cdf = pd.concat(cdata, ignore_index=True)
                    fig_l = px.line(cdf, x='date', y='value', color='País',
                                   markers=True, title=f"{sel_c}  —  últimos {rng_c}")
                    fig_l.update_layout(height=400, hovermode="x unified",
                                       xaxis_title="Fecha", yaxis_title="Valor")
                    st.plotly_chart(fig_l, use_container_width=True, key="latam_chart")
                    st.subheader("📊 Ranking — Último dato")
                    rcols = st.columns(len(cdata))
                    for i, df_c in enumerate(cdata):
                        cur=df_c.iloc[-1]['value']; prv=df_c.iloc[-2]['value'] if len(df_c)>1 else cur
                        d=round(((cur-prv)/prv*100),2) if prv else 0
                        rcols[i].metric(df_c['País'].iloc[0], format_number(cur), f"{d}%")
                else:
                    st.info("Sin datos históricos para este indicador en el rango seleccionado.")

    # ════════════════════════════════════════════════════════════════════════
    # PROYECCIONES
    # ════════════════════════════════════════════════════════════════════════
    with tab_proj:
        st.subheader("🔮 Proyecciones y Consenso de Analistas")
        if variables_df.empty:
            st.info("No hay variables para el país seleccionado.")
        else:
            vopts = dict(zip(variables_df['name'], variables_df['id']))
            svn   = st.selectbox("Variable a modelar", list(vopts.keys()))
            svid  = vopts[svn]
            hdf   = load_history(svid)
            sunit = variables_df[variables_df['id']==svid]['unit'].values
            sunit = sunit[0] if len(sunit)>0 else ''
            sfreq = variables_df[variables_df['id']==svid]['frequency'].values
            sfreq = (sfreq[0] if len(sfreq)>0 else 'monthly') or 'monthly'

            # Rango histórico coherente con frecuencia natural
            def_rng_p = _DEFAULT_RANGE.get(sfreq,'2A')
            rp_opts   = ['6M','1A','2A','5A','MAX']
            rp = st.radio("Rango histórico", rp_opts,
                          index=rp_opts.index(def_rng_p) if def_rng_p in rp_opts else 2,
                          horizontal=True, key="proj_rng")
            st.caption(f"Frecuencia natural: **{_FREQ_LABEL.get(sfreq,sfreq)}** — "
                       f"la proyección es siempre a 6 períodos hacia adelante.")

            st.markdown("#### 📈 Proyección del Modelo")
            if not hdf.empty and len(hdf)>2:
                pr = VariableAgent.calculate_projection(hdf, periods=6)
                if not pr.empty:
                    hdf_plot = filter_range(hdf.copy(), rp)
                    fig_p = go.Figure()
                    if 'lower_95' in pr.columns:
                        xb=list(pr['date'])+list(reversed(list(pr['date'])))
                        yb=list(pr['upper_95'])+list(reversed(list(pr['lower_95'])))
                        fig_p.add_trace(go.Scatter(x=xb,y=yb,fill='toself',
                            fillcolor='rgba(59,130,246,0.08)',line=dict(color='rgba(0,0,0,0)'),name='IC 95%'))
                    if 'lower_80' in pr.columns:
                        xb=list(pr['date'])+list(reversed(list(pr['date'])))
                        yb=list(pr['upper_80'])+list(reversed(list(pr['lower_80'])))
                        fig_p.add_trace(go.Scatter(x=xb,y=yb,fill='toself',
                            fillcolor='rgba(59,130,246,0.18)',line=dict(color='rgba(0,0,0,0)'),name='IC 80%'))
                    fig_p.add_trace(go.Scatter(x=hdf_plot['date'],y=hdf_plot['value'],
                        name='Histórico',line=dict(color='#1e3a8a',width=2)))
                    ml = pr['model_name'].iloc[0] if 'model_name' in pr.columns else 'Ensemble'
                    fig_p.add_trace(go.Scatter(x=pr['date'],y=pr['value'],
                        name=f"Proyección ({ml})",
                        line=dict(color='#f59e0b',width=2,dash='dot'),mode='lines+markers'))
                    try:
                        from data.consensus import get_latest_consensus_by_variable
                        co = get_latest_consensus_by_variable(svid)
                        if not co.empty:
                            SYM={'base':'diamond','optimista':'triangle-up','pessimista':'triangle-down','actual':'circle'}
                            ICOL={'IMF WEO':'#1f77b4','Focus BCB (mediana)':'#2ca02c',
                                  'Banxico Encuesta':'#d62728','BanRep':'#9467bd',
                                  'Goldman Sachs':'#8c564b','JPMorgan':'#e377c2',
                                  'BBVA Research':'#7f7f7f','Bancolombia':'#bcbd22','EIA':'#aec7e8'}
                            for inst,grp in co.groupby('source_institution'):
                                sc=grp['scenario'].iloc[0] if 'scenario' in grp.columns else 'base'
                                fig_p.add_trace(go.Scatter(
                                    x=pd.to_datetime(grp['target_date']),y=grp['forecast_value'],
                                    mode='markers+text',name=inst,
                                    marker=dict(size=10,symbol=SYM.get(sc,'circle'),
                                                color=ICOL.get(inst,'#636363'),line=dict(color='white',width=1)),
                                    text=[f"{v:.2f}" for v in grp['forecast_value']],
                                    textposition='top center',textfont=dict(size=9),
                                    hovertemplate=f"<b>{inst}</b><br>%{{x|%b %Y}}<br>%{{y:.2f}} {sunit}<extra></extra>"))
                    except: pass
                    fig_p.update_layout(height=440,hovermode='x unified',
                        title=f"Proyección — {svn} ({sunit})",yaxis_title=sunit,
                        legend=dict(orientation='h',y=1.15,font=dict(size=10)))
                    st.plotly_chart(fig_p, use_container_width=True, key="proj_chart")
                    st.caption("◆ Diamante = base | ▲ = optimista | ▼ = pesimista")
                    st.dataframe(pr[['date','value']].round(4), use_container_width=True)
                else: st.warning("Proyección no disponible.")
            else: st.warning("No hay suficientes datos históricos (mínimo 3 puntos).")

            st.markdown("#### 🏦 Consenso de Analistas")
            try:
                from data.consensus import get_latest_consensus_by_variable
                cd = get_latest_consensus_by_variable(svid)
                if not cd.empty:
                    if not hdf.empty and len(hdf)>2:
                        pn = VariableAgent.calculate_projection(hdf,periods=12)
                        if not pn.empty:
                            cd = pd.concat([cd, pd.DataFrame([{
                                'source_institution':'🤖 Modelo Cerebro',
                                'forecast_value':round(pn['value'].iloc[-1],4),
                                'scenario':'Ensemble','forecast_date':datetime.now().strftime('%Y-%m-%d'),
                                'target_date':str(pn['date'].iloc[-1])[:10]}])], ignore_index=True)
                    dcols=['source_institution','forecast_value','scenario','target_date']
                    if 'notes' in cd.columns: dcols.append('notes')
                    st.dataframe(cd[dcols].rename(columns={
                        'source_institution':'Institución','forecast_value':'Proyección',
                        'scenario':'Escenario','target_date':'Fecha Objetivo','notes':'Notas'}),
                        use_container_width=True, hide_index=True)
                else: st.info("Sin proyecciones de consenso.")
            except Exception as e: st.info(f"Módulo consenso no disponible: {e}")

    # ════════════════════════════════════════════════════════════════════════
    # DATA HUB
    # ════════════════════════════════════════════════════════════════════════
    with tab_hub:
        st.subheader("📚 Data Hub — Biblioteca & Estado de Datos")
        st.caption("Explora, busca y gestiona todas las variables. Click en 'Ver detalle →' para el panel Bloomberg.")

        ahub = load_all_variables(); chub = load_countries()
        _now_h = datetime.utcnow()

        if ahub.empty:
            st.info("No hay variables configuradas.")
        else:
            sc, sp, sca, se = st.columns([3,1.5,1.5,1.5])
            with sc:  hub_srch = st.text_input("🔍","",placeholder="TRM, IPC, Selic...",label_visibility="collapsed")
            with sp:  hub_p   = st.selectbox("País",['Todos']+list(chub['name'].unique() if not chub.empty else []),label_visibility="collapsed")
            with sca: hub_cat = st.selectbox("Cat",['Todas']+sorted(ahub['category'].dropna().unique().tolist()) if 'category' in ahub.columns else ['Todas'],label_visibility="collapsed")
            with se:  hub_est = st.selectbox("Est",["Todos","🟢 Al día","🟡 Desact.","🔴 Sin datos"],label_visibility="collapsed")

            FLAG2={"Colombia":"🇨🇴","México":"🇲🇽","Brasil":"🇧🇷","Ecuador":"🇪🇨","Global":"🌍"}
            rows_h=[]
            for _,hv in ahub.iterrows():
                lf=hv.get('last_successful_fetch')
                if lf:
                    try:
                        d=(_now_h-pd.to_datetime(lf).replace(tzinfo=None)).days
                        est="🟢" if d<=7 else "🟡"; ek="ok" if d<=7 else "des"
                    except: d=None; est="🔴"; ek="sin"
                else: d=None; est="🔴"; ek="sin"
                cm=chub[chub['id']==hv.get('country_id')] if not chub.empty else pd.DataFrame()
                pn=cm.iloc[0]['name'] if not cm.empty else "Global"
                nat_f_h=(hv.get('frequency') or 'monthly').lower()
                rows_h.append({"_id":int(hv['id']),"Variable":hv.get('name',''),
                    "País":f"{FLAG2.get(pn,'🌍')} {pn}","_pais":pn,
                    "Categoría":hv.get('category','—') or '—',
                    "Fuente":str(hv.get('api_provider') or hv.get('connector_type','—') or '—').upper(),
                    "Tipo":str(hv.get('connector_type','—') or '—').upper(),
                    "Frecuencia":_FREQ_LABEL.get(nat_f_h,nat_f_h),
                    "Días":d if d is not None else "—","Estado":est,"_ek":ek,
                    "URL":hv.get('source_url') or '','Desc':hv.get('description') or '',
                    "Unit":hv.get('unit',''),"_freq":nat_f_h})
            hdf2 = pd.DataFrame(rows_h)

            if hub_srch: hdf2=hdf2[hdf2['Variable'].str.lower().str.contains(hub_srch.lower(),na=False)]
            if hub_p!='Todos': hdf2=hdf2[hdf2['_pais']==hub_p]
            if hub_cat!='Todas': hdf2=hdf2[hdf2['Categoría']==hub_cat]
            if hub_est=="🟢 Al día":  hdf2=hdf2[hdf2['_ek']=='ok']
            elif hub_est=="🟡 Desact.": hdf2=hdf2[hdf2['_ek']=='des']
            elif hub_est=="🔴 Sin datos": hdf2=hdf2[hdf2['_ek']=='sin']

            m1,m2,m3,m4=st.columns(4)
            m1.metric("Total variables",len(hdf2))
            m2.metric("🟢 Al día (≤7d)",int((hdf2['_ek']=='ok').sum()))
            m3.metric("🟡 Rezagadas",int((hdf2['_ek']=='des').sum()))
            m4.metric("🔴 Sin datos",int((hdf2['_ek']=='sin').sum()))
            st.divider()

            st.dataframe(hdf2[['Variable','País','Categoría','Fuente','Tipo','Frecuencia','Días','Estado']].reset_index(drop=True),
                use_container_width=True, hide_index=True,
                height=min(500,len(hdf2)*36+60),
                column_config={"Estado":st.column_config.TextColumn("Estado",width="small"),
                               "Días":st.column_config.NumberColumn("Días",format="%d"),
                               "Frecuencia":st.column_config.TextColumn("Frecuencia",width="medium")})

            st.divider()
            st.markdown("#### Acciones por variable")
            if not hdf2.empty:
                sv_h = st.selectbox("Variable", hdf2['Variable'].tolist(), key="hub_sel")
                sr_h = hdf2[hdf2['Variable']==sv_h].iloc[0]
                a1,a2,a3 = st.columns(3)
                with a1:
                    if st.button("📊 Ver detalle Bloomberg", use_container_width=True, key="hub_det"):
                        show_variable_detail(int(sr_h['_id']),sr_h['Variable'],sr_h['Unit'],
                                            sr_h['Tipo'],sr_h['URL'],sr_h['Desc'],sr_h['_freq'])
                with a2:
                    if sr_h['URL']: st.link_button("🔗 Fuente oficial",sr_h['URL'],use_container_width=True)
                    else: st.button("🔗 Sin URL",disabled=True,use_container_width=True)
                with a3:
                    if st.button("🔄 Forzar actualización",use_container_width=True,key="hub_upd"):
                        vrf=ahub[ahub['id']==sr_h['_id']].iloc[0]
                        with st.spinner(f"Actualizando {sv_h}..."):
                            res=VariableAgent.ingest_variable(vrf)
                            if res.get('success'):
                                st.success("✅ "+res.get('message','Actualización exitosa'))
                                load_history.clear()
                                load_last_known.clear()
                            else:
                                err = res.get('error', 'Error desconocido')
                                provider = str(vrf.get('api_provider') or '').upper()
                                if 'token' in err.lower() or 'api_key' in err.lower() or 'unauthorized' in err.lower():
                                    st.error(f"🔑 **{provider}** requiere clave API. Configura la variable de entorno correspondiente en tu archivo `.env`.")
                                elif 'timeout' in err.lower() or 'connection' in err.lower():
                                    st.error(f"🌐 **Timeout** conectando a {provider}. Verifica tu conexión o intenta más tarde.")
                                elif 'not found' in err.lower() or '404' in err:
                                    st.error(f"📭 Serie no encontrada en **{provider}**. El `api_serie_id` puede estar desactualizado.")
                                else:
                                    st.error(f"❌ {err}")

            st.divider()
            with st.expander("🗺️ Checklist de brechas por país", expanded=False):
                BRECHAS={
                    "🇨🇴 Colombia":[("Cuenta Corriente CO (% PIB)","🔴","WorldBank BX.CAB.XOKA.GD.ZS · Anual"),
                                    ("IED CO (% PIB)","🔴","WorldBank BX.KLT.DINV.CD.WD · Anual"),
                                    ("IPC CO (var. anual)","🟡","BanRep API deprecada → usar WorldBank · Mensual"),
                                    ("Tasa de Intervención BanRep","🟡","BanRep scraper directo · Mensual"),
                                    ("EMBI Colombia","🟡","Entrada manual / scraper BanRep · Mensual")],
                    "🇲🇽 México":[("PIB MX","🔴","Banxico SR16734 — requiere BANXICO_TOKEN · Trimestral"),
                                  ("IPC MX","🔴","Banxico SP74635 — requiere BANXICO_TOKEN · Mensual"),
                                  ("Tasa Objetivo Banxico","🔴","Banxico SF61745 — requiere BANXICO_TOKEN · Mensual"),
                                  ("USD/MXN","🔴","Banxico SF43718 — requiere BANXICO_TOKEN · Diario")],
                    "🇧🇷 Brasil":[("Tasa Selic BR","🟢","BCB SGS 432 — activo · Diario"),
                                  ("IPCA BR","🟢","BCB SGS 13522 — activo · Mensual"),
                                  ("Desempleo BR","🔴","BCB SGS 28763 — pendiente · Mensual")],
                    "🌍 Global":[("WTI / Brent","🔴","FRED DCOILWTICO — requiere FRED_API_KEY · Diario"),
                                 ("S&P 500 / VIX","🔴","FRED SP500/VIXCLS — requiere FRED_API_KEY · Diario"),
                                 ("Gold / DXY","🔴","FRED GOLDAMGBD/DTWEXBGS — requiere FRED_API_KEY · Diario")],
                }
                for pais_b, items_b in BRECHAS.items():
                    st.markdown(f"**{pais_b}**")
                    for vb,eb,fb in items_b: st.markdown(f"&nbsp;&nbsp;{eb} `{vb}` — {fb}")
                    st.markdown("")

            with st.expander("✏️ Entrada manual de datos", expanded=False):
                mvar = ahub[ahub['connector_type']=='MANUAL'] if 'connector_type' in ahub.columns else pd.DataFrame()
                if not mvar.empty:
                    sm=st.selectbox("Variable manual",mvar['name'].tolist(),key="hub_mv")
                    cv,cd=st.columns(2)
                    mv=cv.number_input("Valor",key="hub_mval")
                    md=cd.date_input("Fecha",value=date.today(),key="hub_mdate")
                    if st.button("💾 Guardar",key="hub_msave"):
                        try:
                            from data.database import save_manual_data_point
                            mid=mvar[mvar['name']==sm].iloc[0]['id']
                            save_manual_data_point(int(mid),md,float(mv))
                            st.success(f"✅ Guardado: {sm} = {mv} ({md})"); load_history.clear()
                        except Exception as e: st.error(f"No se pudo guardar: {e}")
                else: st.info("No hay variables MANUAL configuradas.")

    # ════════════════════════════════════════════════════════════════════════
    # EXPORTACIÓN
    # ════════════════════════════════════════════════════════════════════════
    with tab_export:
        st.subheader("📋 Datos y Exportación")
        avd = load_all_variables(); acd = load_countries()
        if avd.empty:
            st.info("No hay variables disponibles.")
        else:
            cf1,cf2=st.columns(2)
            with cf1: cflt=st.multiselect("Países",acd['name'].tolist(),default=acd['name'].tolist()[:2])
            with cf2:
                catl=sorted(avd['category'].dropna().unique().tolist()) if 'category' in avd.columns else []
                catf=st.multiselect("Categorías",catl,default=catl)
            cf3,cf4=st.columns(2)
            with cf3: ds=st.date_input("Desde",value=date(2024,1,1))
            with cf4: de=st.date_input("Hasta",value=date.today())
            only_r=st.checkbox("Solo datos reales (REAL_OFFICIAL)",value=True)

            fcids=acd[acd['name'].isin(cflt)]['id'].tolist() if cflt else []
            rows_e=[]
            for _,vr in avd.iterrows():
                if fcids and vr.get('country_id') not in fcids: continue
                if catf and vr.get('category') not in catf: continue
                h=load_history(vr['id'])
                if h.empty: continue
                h=h.copy(); h['date']=pd.to_datetime(h['date'])
                h=h[(h['date']>=pd.Timestamp(ds))&(h['date']<=pd.Timestamp(de))]
                if only_r: h=h[h['data_type']=='REAL_OFFICIAL']
                if h.empty: continue
                cn=acd[acd['id']==vr.get('country_id')]['name'].values
                h['País']=cn[0] if len(cn)>0 else 'N/A'
                h['Variable']=vr['name']; h['Unidad']=vr.get('unit',''); h['Fuente']=vr.get('connector_type','SCRAPER')
                rows_e.append(h)

            if rows_e:
                mdf=pd.concat(rows_e,ignore_index=True)
                mdf=mdf.rename(columns={'date':'Fecha','value':'Valor','data_type':'Tipo'})
                mdf['Fecha']=mdf['Fecha'].dt.strftime('%Y-%m-%d')
                mdf=mdf[['Fecha','País','Variable','Valor','Unidad','Tipo','Fuente']].sort_values(['Variable','Fecha'])
                vm=st.radio("Vista",["Tabla plana","Pivot (fechas × series)","Resumen estadístico"],horizontal=True)
                if vm=="Tabla plana": st.dataframe(mdf,use_container_width=True,hide_index=True)
                elif vm=="Pivot (fechas × series)":
                    pvt=mdf.pivot_table(index='Fecha',columns='Variable',values='Valor',aggfunc='mean')
                    st.dataframe(pvt,use_container_width=True)
                elif vm=="Resumen estadístico":
                    st.dataframe(mdf.groupby('Variable')['Valor'].agg(Último='last',Min='min',Max='max',Promedio='mean',Mediana='median',StdDev='std',N='count').round(4).reset_index(),use_container_width=True,hide_index=True)
                st.divider()
                _exp=pvt if vm=="Pivot (fechas × series)" else mdf
                d1,d2,d3=st.columns(3)
                with d1: st.download_button("⬇️ CSV",_exp.to_csv(index=(vm=="Pivot (fechas × series)")).encode(),"cerebro.csv","text/csv")
                with d2:
                    try:
                        import openpyxl; buf=io.BytesIO()
                        with pd.ExcelWriter(buf,engine='openpyxl') as wr:
                            mdf.to_excel(wr,sheet_name='Datos',index=False)
                            if vm=="Pivot (fechas × series)": pvt.to_excel(wr,sheet_name='Pivot')
                        buf.seek(0)
                        st.download_button("⬇️ XLSX",buf.getvalue(),"cerebro.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    except ImportError: st.caption("Instala `openpyxl` para XLSX.")
                with d3: copy_to_clipboard_button(_exp.to_csv(index=False,sep='\t'),label="📋 Copiar TSV",key="tsv")
            else: st.info("No hay datos para los filtros seleccionados.")

    # ════════════════════════════════════════════════════════════════════════
    # FINANZAS CORP.
    # ════════════════════════════════════════════════════════════════════════
    if tab_corp is not None:
        with tab_corp:
            st.subheader("🏢 Finanzas Corporativas")
            ac=load_all_variables()
            cv=ac[ac['category']=='corporate_finance'] if not ac.empty and 'category' in ac.columns else pd.DataFrame()
            if cv.empty: st.info("📊 Habilitado cuando haya datos de finanzas corporativas.")
            else:
                EV=["WACC - Costo Promedio de Capital","Costo de la Deuda (Kd)","Costo del Equity (Ke)",
                    "Tarifa PPA (Precio Venta de Energía)","TIR Proyecto (IRR)","CAPEX Solar Total (USD por proyecto)"]
                es=cv[cv['name'].isin(EV)]; kc=st.columns(3)
                for ki,(_,r) in enumerate(es.iterrows()):
                    h=load_history(r['id'])
                    with kc[ki%3]:
                        with st.container(border=True):
                            if not h.empty:
                                vv=h['value'].iloc[-1]; uu=r.get('unit','')
                                st.metric(r['name'],f"{vv:.2f}%" if uu=='%' else f"USD {vv:,.0f}" if uu=='USD' else f"{vv:.0f} {uu}")
                                st.caption(str(r.get('description',''))[:120])
                            else: st.metric(r['name'],"—"); st.caption("📊 Pendiente.")
                st.divider()
                wv={}
                for lbl,vn in [("Kd","Costo de la Deuda (Kd)"),("Ke","Costo del Equity (Ke)"),("WACC","WACC - Costo Promedio de Capital")]:
                    rr=cv[cv['name']==vn]
                    if not rr.empty:
                        h=load_history(int(rr.iloc[0]['id']))
                        if not h.empty: wv[lbl]=h['value'].iloc[-1]
                if wv:
                    fw=go.Figure(go.Bar(x=list(wv.keys()),y=list(wv.values()),
                        marker_color=['#6366f1','#f59e0b','#10b981'],
                        text=[f"{v:.2f}%" for v in wv.values()],textposition='outside'))
                    fw.update_layout(title="Estructura de Capital (%)",yaxis_title="%",height=280,plot_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fw, use_container_width=True, key="corp_wacc")

    # ════════════════════════════════════════════════════════════════════════
    # ASISTENTE
    # ════════════════════════════════════════════════════════════════════════
    with tab_agent:
        st.subheader("🤖 Asistente de Datos")
        st.markdown("Haz preguntas sobre los datos económicos del dashboard.")
        if 'chat_history' not in st.session_state: st.session_state.chat_history=[]
        for msg in st.session_state.chat_history:
            with st.chat_message(msg['role']): st.markdown(msg['content'])
        uq=st.chat_input("¿Cuál es la TRM hoy? / ¿Cómo está la inflación en Colombia?")
        if uq:
            st.session_state.chat_history.append({'role':'user','content':uq})
            with st.chat_message('user'): st.markdown(uq)
            with st.chat_message('assistant'):
                with st.spinner("Consultando..."):
                    try:
                        from ai_engine.chatbot import answer_question
                        resp=answer_question(uq,load_all_variables(),load_history)
                    except: resp="Lo siento, no pude procesar tu pregunta. *Análisis informativo.*"
                st.markdown(resp)
                st.session_state.chat_history.append({'role':'assistant','content':resp})
        if st.session_state.chat_history:
            if st.button("🗑 Limpiar",key="clr_chat"): st.session_state.chat_history=[]; st.rerun()

        st.divider(); st.markdown("### 🔧 Administración")
        with st.expander("📊 Estado del Sistema"):
            av2=load_all_variables(); tv=len(av2)
            vd=sum(1 for _,r in av2.iterrows() if not load_history(r['id']).empty)
            ve=sum(1 for _,r in av2.iterrows() if r.get('fetch_error_count',0) and int(r.get('fetch_error_count',0) or 0)>0)
            c1,c2,c3,c4=st.columns(4)
            c1.metric("Variables",tv); c2.metric("Con datos",f"{vd}/{tv}")
            c3.metric("Errores",ve); c4.metric("Revisión",datetime.now().strftime('%H:%M'))

        if not variables_df.empty:
            if st.button("🚀 Actualizar TODAS las variables activas",type="primary"):
                pb=st.progress(0); st_txt=st.empty(); ok=0
                for i,(_,r) in enumerate(variables_df.iterrows()):
                    st_txt.text(f"Actualizando {r['name']}...")
                    if VariableAgent.ingest_variable(r).get('success'): ok+=1
                    pb.progress((i+1)/len(variables_df))
                st_txt.text(f"✅ {ok}/{len(variables_df)} variables actualizadas."); load_history.clear()

        try:
            from data.consensus import save_consensus_forecast
            with st.expander("➕ Nueva Proyección de Consenso"):
                av3=load_all_variables()
                if not av3.empty:
                    vc=dict(zip(av3['name'],av3['id']))
                    sv3=st.selectbox("Variable",list(vc.keys()),key="cv3")
                    inst=st.text_input("Institución",placeholder="Bancolombia, BanRep...")
                    td=st.date_input("Fecha objetivo",key="td3")
                    val3=st.number_input("Valor",key="val3")
                    scen3=st.selectbox("Escenario",["base","optimista","pesimista"],key="sc3")
                    notes3=st.text_area("Notas",key="nt3")
                    if st.button("💾 Guardar",key="sv3"):
                        save_consensus_forecast(variable_id=vc[sv3],source_institution=inst,
                            forecast_date=datetime.now(),target_date=datetime.combine(td,datetime.min.time()),
                            value=val3,scenario=scen3,notes=notes3)
                        st.success("Proyección guardada.")
        except ImportError: st.info("Módulo consenso no disponible.")
        except Exception: st.error("No se pudo guardar.")


if __name__ == "__main__":
    main()
