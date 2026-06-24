import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from ui.components import load_css, filter_range, _DEFAULT_RANGE, _FREQ_LABEL
from data.database import get_countries, get_variables, get_historical_data
from data.agent import VariableAgent

st.set_page_config(page_title="Proyecciones", page_icon="🔮", layout="wide")
load_css()

def render_sidebar():
    countries_df = get_countries()
    if countries_df.empty:
        st.stop()
    country_opts  = dict(zip(countries_df['name'], countries_df['id']))
    country_names = list(country_opts.keys())
    FLAG = {"Colombia":"🇨🇴","México":"🇲🇽","Brasil":"🇧🇷","Ecuador":"🇪🇨"}
    st.sidebar.markdown("<div class='sb-label'>País principal</div>", unsafe_allow_html=True)
    sel_idx = st.sidebar.selectbox("País", range(len(country_names)), index=0, format_func=lambda i: f"{FLAG.get(country_names[i],'🌍')} {country_names[i]}", label_visibility="collapsed")
    return country_names[sel_idx], country_opts[country_names[sel_idx]]

def main():
    st.title("🔮 Proyecciones de 3-Vías")
    st.markdown("""
    Esta vista unifica tres fuentes de proyecciones para ofrecer un panorama completo:
    1. **Entidades Oficiales** (Metas de Gobierno / Banco Central).
    2. **Consenso de Analistas** (Mercado y Bancos de Inversión).
    3. **Modelo Estadístico** (ARIMA / Holt-Winters interno).
    """)
    
    sel_name, sel_id = render_sidebar()
    variables_df = get_variables(sel_id)

    if variables_df.empty:
        st.info("No hay variables para el país seleccionado.")
        return

    vopts = dict(zip(variables_df['name'], variables_df['id']))
    svn   = st.selectbox("Seleccione la variable a proyectar", list(vopts.keys()))
    svid  = vopts[svn]
    hdf   = get_historical_data(svid)
    sunit = variables_df[variables_df['id']==svid]['unit'].values
    sunit = sunit[0] if len(sunit)>0 else ''
    sfreq = variables_df[variables_df['id']==svid]['frequency'].values
    sfreq = (sfreq[0] if len(sfreq)>0 else 'monthly') or 'monthly'

    def_rng_p = _DEFAULT_RANGE.get(sfreq,'2A')
    rp_opts   = ['6M','1A','2A','5A','MAX']
    rp = st.radio("Rango histórico visible", rp_opts, index=rp_opts.index(def_rng_p) if def_rng_p in rp_opts else 2, horizontal=True)

    if not hdf.empty and len(hdf)>2:
        pr = VariableAgent.calculate_projection(hdf, periods=12) # 1 año hacia adelante
        if not pr.empty:
            hdf_plot = filter_range(hdf.copy(), rp)
            fig_p = go.Figure()
            
            # Intervalos de Confianza (Modelo Estadístico)
            if 'lower_95' in pr.columns:
                xb=list(pr['date'])+list(reversed(list(pr['date'])))
                yb=list(pr['upper_95'])+list(reversed(list(pr['lower_95'])))
                fig_p.add_trace(go.Scatter(x=xb,y=yb,fill='toself', fillcolor='rgba(59,130,246,0.08)',line=dict(color='rgba(0,0,0,0)'),name='IC 95%'))
            
            # Histórico
            fig_p.add_trace(go.Scatter(x=hdf_plot['date'],y=hdf_plot['value'], name='Valor Histórico',line=dict(color='#1e3a8a',width=2)))
            
            # 1. Modelo Estadístico
            ml = pr['model_name'].iloc[0] if 'model_name' in pr.columns else 'ARIMA/HW'
            fig_p.add_trace(go.Scatter(x=pr['date'],y=pr['value'], name=f"Curva C: Modelo ({ml})", line=dict(color='#f59e0b',width=2,dash='dot'),mode='lines+markers'))
            
            # Consenso y Gobierno
            try:
                from models.db import SessionLocal
                from models.schema import ConsensusForecast
                with SessionLocal() as session:
                    import pandas as pd
                    res = session.query(ConsensusForecast).filter(ConsensusForecast.variable_id == svid).all()
                    if res:
                        co = pd.DataFrame([{
                            'source_institution': r.source_institution, 'forecast_value': r.forecast_value,
                            'scenario': r.scenario, 'target_date': r.target_date
                        } for r in res])
                        
                        SYM={'base':'diamond','optimista':'triangle-up','pesimista':'triangle-down','meta_gobierno':'star'}
                        ICOL={'IMF WEO':'#1f77b4','Focus BCB':'#2ca02c', 'Banxico':'#d62728','BanRep':'#9467bd',
                              'Gobierno/Hacienda':'#10b981', 'Bancolombia':'#bcbd22'}
                              
                        for inst,grp in co.groupby('source_institution'):
                            sc=grp['scenario'].iloc[0] if 'scenario' in grp.columns else 'base'
                            is_gov = 'gobierno' in inst.lower() or 'hacienda' in inst.lower()
                            curva_label = f"Curva A: Meta {inst}" if is_gov else f"Curva B: Consenso {inst}"
                            
                            fig_p.add_trace(go.Scatter(
                                x=pd.to_datetime(grp['target_date']),y=grp['forecast_value'],
                                mode='markers+text',name=curva_label,
                                marker=dict(size=12 if is_gov else 10,symbol=SYM.get('meta_gobierno' if is_gov else sc,'circle'),
                                            color=ICOL.get(inst,'#636363'),line=dict(color='white',width=1)),
                                text=[f"{v:.2f}" for v in grp['forecast_value']],
                                textposition='top center',textfont=dict(size=9),
                                hovertemplate=f"<b>{inst}</b><br>%{{x|%b %Y}}<br>%{{y:.2f}} {sunit}<extra></extra>"))
            except Exception as e:
                st.warning(f"Error cargando consensos: {e}")

            fig_p.update_layout(height=500,hovermode='x unified', title=f"Proyección a 3-Vías — {svn} ({sunit})",yaxis_title=sunit, legend=dict(orientation='h',y=1.15,font=dict(size=10)))
            st.plotly_chart(fig_p, use_container_width=True)
            st.caption("★ Estrella = Metas Oficiales (Curva A) | ◆ Diamante = Consenso de Mercado (Curva B) | Línea Punteada = ARIMA Propio (Curva C)")
            
            # Data table
            st.subheader("📋 Tabla de Datos de Proyección")
            
            # Merge both in a single table
            pr_table = pr[['date', 'value', 'type']].copy()
            pr_table['Fuente'] = f"Modelo Propio ({ml})"
            pr_table.rename(columns={'date': 'Fecha Objetivo', 'value': 'Valor'}, inplace=True)
            
            try:
                if 'co' in locals() and not co.empty:
                    co_table = co[['target_date', 'forecast_value', 'source_institution']].copy()
                    co_table['type'] = 'Consenso / Institución'
                    co_table.rename(columns={'target_date': 'Fecha Objetivo', 'forecast_value': 'Valor', 'source_institution': 'Fuente'}, inplace=True)
                    pr_table = pd.concat([pr_table, co_table], ignore_index=True)
            except: pass
            
            pr_table['Fecha Objetivo'] = pd.to_datetime(pr_table['Fecha Objetivo']).dt.strftime('%Y-%m-%d')
            pr_table = pr_table.sort_values('Fecha Objetivo')
            st.dataframe(pr_table, use_container_width=True, hide_index=True)
            
        else: st.warning("Proyección estadística no disponible (el modelo falló o retornó vacío).")
    else: st.warning("No hay suficientes datos históricos (mínimo 3 puntos).")

if __name__ == "__main__":
    main()
