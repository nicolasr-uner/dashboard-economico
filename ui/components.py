import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import streamlit.components.v1 as _stcomponents

from data.database import get_historical_data, get_last_known_value
from data.agent import VariableAgent

_PERCENT_UNITS = {'%', '% PIB'}
_PRESCALED     = {'USD M','COP B','USD B','COP M','COP/kWh','USD/kWh',
                  'COP/MWh','USD/MWh','USD/bbl','USD/MMBtu','COP/kWp'}

_FREQ_LABEL  = {'daily': 'Diario', 'weekly': 'Semanal', 'monthly': 'Mensual',
                'quarterly': 'Trimestral', 'annual': 'Anual'}
_VALID_AGG = {
    'daily':     [('D','Diario'), ('S','Semanal'), ('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'weekly':    [('S','Semanal'), ('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'monthly':   [('M','Mensual'), ('T','Trimestral'), ('A','Anual')],
    'quarterly': [('T','Trimestral'), ('A','Anual')],
    'annual':    [('A','Anual')],
}
_PANDAS_FREQ = {'D': None, 'S': 'W', 'M': 'ME', 'T': 'QE', 'A': 'YE'}
_DEFAULT_RANGE = {'daily': '6M', 'weekly': '1A', 'monthly': '2A',
                  'quarterly': '5A', 'annual': 'MAX'}
RANGE_OPTIONS = ['1M', '3M', '6M', '1A', '2A', '5A', 'MAX']

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

def load_css():
    with open("assets/style.css", "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

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

def _caption(name):
    for k, v in _CAPTIONS.items():
        if k.lower() in name.lower(): return v
    return ""

def _range_since(rng: str) -> pd.Timestamp | None:
    now = pd.Timestamp.now()
    m = {'1M':1,'3M':3,'6M':6,'1A':12,'2A':24,'5A':60}
    return now - pd.DateOffset(months=m[rng]) if rng in m else None

def filter_range(df: pd.DataFrame, rng: str) -> pd.DataFrame:
    if df.empty: return df
    since = _range_since(rng)
    if since is None: return df
    d = df.copy()
    d['date'] = pd.to_datetime(d['date'])
    return d[d['date'] >= since]

def resample_hist(df: pd.DataFrame, agg_code: str) -> pd.DataFrame:
    if df.empty or _PANDAS_FREQ.get(agg_code) is None: return df
    d = df.copy()
    d['date'] = pd.to_datetime(d['date'])
    return d.set_index('date').sort_index()['value'] \
            .resample(_PANDAS_FREQ[agg_code]).last().dropna() \
            .reset_index()

def badge_html(ct: str) -> str:
    ct = (ct or 'SCRAPER').upper()
    cls = {'API':'badge-api','SCRAPER':'badge-scraper','MANUAL':'badge-manual'}.get(ct,'badge-scraper')
    return f'<span class="{cls}">{ct}</span>'

@st.cache_data(ttl=3600)
def _cached_proj(var_id: int, periods: int = 6, frequency: str = 'monthly'):
    try:
        h = get_historical_data(var_id)
        if h.empty or len(h) < 3: return None
        freq_periods = {
            'daily': 30, 'weekly': 12, 'monthly': 6, 'quarterly': 4, 'annual': 3,
        }
        adjusted_periods = freq_periods.get(frequency, periods)
        return VariableAgent.calculate_projection(h, periods=adjusted_periods)
    except Exception: return None

@st.dialog("📊 Detalle de Variable", width="large")
def show_variable_detail(var_id, var_name, unit, ct, src_url, desc, nat_freq='monthly'):
    hist_full = get_historical_data(var_id)
    if hist_full.empty:
        st.warning("Sin datos históricos para esta variable.")
        return

    hist_full = hist_full.copy()
    hist_full['date'] = pd.to_datetime(hist_full['date'])
    hist_full = hist_full.sort_values('date')

    last_val  = hist_full['value'].iloc[-1]
    prev_val  = hist_full['value'].iloc[-2] if len(hist_full) > 1 else last_val
    delta_pct = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val else 0
    last_date = hist_full['date'].iloc[-1]
    days_old  = (datetime.now() - last_date).days

    c_name, c_badge = st.columns([4, 1])
    with c_name:
        st.markdown(f"### {var_name}")
        freq_lbl = _FREQ_LABEL.get(nat_freq, nat_freq)
        st.caption(f"{desc[:120] if desc else ''} · Frecuencia natural: **{freq_lbl}**")
    with c_badge:
        st.markdown(badge_html(ct), unsafe_allow_html=True)
        if src_url and src_url != '#':
            st.markdown(f"[🔗 Fuente Oficial]({src_url})")

    k1, k2, k3, k4, k5 = st.columns(5)
    for col, lbl, val in [
        (k1, "Último valor",   format_number(last_val, unit)),
        (k2, "Δ vs anterior",  f"{delta_pct:+.2f}%"),
        (k3, "Máximo",         format_number(hist_full['value'].max(), unit)),
        (k4, "Mínimo",         format_number(hist_full['value'].min(), unit)),
        (k5, "Promedio",       format_number(hist_full['value'].mean(), unit)),
    ]:
        col.markdown(f"<div class='kpi-box'><div class='kpi-lbl'>{lbl}</div><div class='kpi-val'>{val}</div></div>", unsafe_allow_html=True)

    st.divider()

    default_rng = _DEFAULT_RANGE.get(nat_freq, '2A')
    c_rng, c_agg = st.columns([3, 2])
    with c_rng:
        rng = st.radio("Rango de tiempo", RANGE_OPTIONS, index=RANGE_OPTIONS.index(default_rng), horizontal=True, key=f"det_rng_{var_id}")
    with c_agg:
        valid_opts  = _VALID_AGG.get(nat_freq, [('M', 'Mensual')])
        agg_labels  = [lbl for _, lbl in valid_opts]
        agg_codes   = [code for code, _ in valid_opts]
        sel_agg_lbl = st.radio("Agregación", agg_labels, horizontal=True, key=f"det_agg_{var_id}")
        agg_code = agg_codes[agg_labels.index(sel_agg_lbl)]

    hist_ranged = filter_range(hist_full, rng)
    hist_plot = resample_hist(hist_ranged, agg_code)
    if hist_plot.empty: hist_plot = hist_ranged

    line_col = '#10b981' if delta_pct >= 0 else '#ef4444'
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist_plot['date'], y=hist_plot['value'],
        mode='lines+markers', line=dict(color=line_col, width=2),
        marker=dict(size=4), name='Histórico',
        hovertemplate=f"%{{x|%d %b %Y}}<br><b>%{{y:,.4g}}</b> {unit}<extra></extra>"))

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
        height=320, hovermode='x unified', margin=dict(l=0, r=0, t=8, b=0),
        yaxis_title=unit, legend=dict(orientation='h', y=1.12),
        plot_bgcolor='white', paper_bgcolor='white')
    st.plotly_chart(fig, use_container_width=True, key=f"det_chart_{var_id}")

    with st.expander("Ver últimos valores"):
        tbl = hist_plot.tail(20)[['date','value']].copy()
        tbl.columns = ['Fecha','Valor']
        tbl['Fecha'] = tbl['Fecha'].dt.strftime('%d %b %Y')
        tbl['Valor'] = tbl['Valor'].apply(lambda x: format_number(x, unit))
        st.dataframe(tbl, hide_index=True, use_container_width=True)

def render_bloomberg_card(row, hist, key_prefix="card", compact=False):
    var_id   = int(row['id'])
    unit     = row.get('unit','') or ''
    ct       = row.get('connector_type','SCRAPER') or 'SCRAPER'
    src_url  = row.get('source_url') or '#'
    desc     = row.get('description') or ''
    nat_freq = (row.get('frequency') or 'monthly').lower()
    freq_lbl = _FREQ_LABEL.get(nat_freq, nat_freq)

    default_rng = _DEFAULT_RANGE.get(nat_freq, '2A')
    active_rng  = st.session_state.get(f'rng_{var_id}', default_rng)

    with st.container(border=True):
        if not hist.empty:
            hist = hist.copy()
            hist['date'] = pd.to_datetime(hist['date'])
            hist_full = hist.sort_values('date')
            hist_view = filter_range(hist_full, active_rng)
            if hist_view.empty: hist_view = hist_full

            last_val  = hist_full['value'].iloc[-1]
            prev_val  = hist_full['value'].iloc[-2] if len(hist_full) > 1 else last_val
            delta     = round(((last_val - prev_val) / prev_val * 100), 2) if prev_val else 0
            last_date = hist_full['date'].iloc[-1]
            days_old  = (datetime.now() - last_date).days

            staleness_dot = ("<span style='color:#ef4444;font-size:0.75em'>🔴</span>" if days_old > 30 else
                             "<span style='color:#f59e0b;font-size:0.75em'>🟡</span>" if days_old > 7 else
                             "<span style='color:#10b981;font-size:0.75em'>🟢</span>")
            hc1, hc2 = st.columns([1, 2])
            with hc1:
                st.markdown(f"{badge_html(ct)} <span class='bb-freq-tag'>{freq_lbl}</span> {staleness_dot}", unsafe_allow_html=True)
            with hc2:
                rng_opts = ['1M','3M','6M','1A','2A','5A','MAX']
                new_rng = st.radio("", rng_opts, index=rng_opts.index(active_rng), horizontal=True, key=f"rng_{var_id}_{key_prefix}", label_visibility="collapsed")
                if new_rng != st.session_state.get(f'rng_{var_id}'):
                    st.session_state[f'rng_{var_id}'] = new_rng

            delta_cls = "bb-delta-pos" if delta > 0 else ("bb-delta-neg" if delta < 0 else "bb-delta-neu")
            arrow     = "▲" if delta > 0 else ("▼" if delta < 0 else "●")
            st.markdown(
                f"<div class='bb-ticker'>{row['name']}</div>"
                f"<div class='bb-value'>{format_number(last_val, unit)}  <span class='{delta_cls}'>{arrow} {abs(delta):.2f}%</span></div>"
                f"<div class='bb-meta'>{last_date.strftime('%d %b %Y')} · {unit}</div>", unsafe_allow_html=True)

            _yfmt = ".2f" if unit in _PERCENT_UNITS or unit.endswith('%') else ".2s" if abs(last_val) >= 1e9 else ".4g"
            line_color = '#10b981' if delta >= 0 else '#ef4444'

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=hist_view['date'], y=hist_view['value'], mode='lines', line=dict(color=line_color, width=2)))
            
            try:
                if active_rng in ('1A','2A','5A','MAX') and len(hist_full) >= 6:
                    proj = _cached_proj(var_id, 3, nat_freq)
                    if proj is not None and not proj.empty:
                        fig.add_trace(go.Scatter(x=proj['date'], y=proj['value'], mode='lines', line=dict(color='#f97316', width=1.5, dash='dot')))
            except: pass

            provider = str(row.get('api_provider') or ct).upper()
            ann = f"Fuente: {provider}"
            if src_url and src_url != '#': ann += f" — <a href='{src_url}'>Enlace Oficial</a>"
            
            fig.update_layout(height=75 if compact else 110, margin=dict(l=0, r=0, t=2, b=34), showlegend=False, 
                              xaxis=dict(showticklabels=False), yaxis=dict(showticklabels=True, tickformat=_yfmt),
                              plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)')
            fig.add_annotation(text=ann, xref="paper", yref="paper", x=0, y=-0.42, showarrow=False, font=dict(size=8, color="#9ca3af"), xanchor="left")
            st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_{var_id}")

            if not compact:
                if _caption(row['name']): st.caption(_caption(row['name']))
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Detalle →", key=f"det_{key_prefix}_{var_id}", use_container_width=True):
                    show_variable_detail(var_id, row['name'], unit, ct, src_url, desc, nat_freq)
            with col2:
                if src_url and src_url != '#':
                    st.link_button("🔗 Fuente Oficial", src_url, use_container_width=True)

        else:
            lkg = get_last_known_value(var_id)
            st.markdown(badge_html(ct), unsafe_allow_html=True)
            if lkg:
                st.markdown(f"<div class='bb-ticker'>{row['name']}</div><div class='bb-value'>{format_number(lkg['value'], unit)}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='bb-ticker'>{row['name']}</div><div style='color:#9ca3af;font-size:0.85em;padding:6px 0'>📊 Sin datos</div>", unsafe_allow_html=True)
