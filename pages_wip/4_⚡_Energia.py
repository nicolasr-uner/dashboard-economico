import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ui.components import load_css, format_number, filter_range
from data.database import get_countries, get_all_variable_names, get_historical_data
import os

st.set_page_config(page_title="Energía", page_icon="⚡", layout="wide")
load_css()

def main():
    st.title("⚡ Mercados de Energía")
    st.markdown("Variables del mercado energético por país y commodities globales.")
    
    countries_df = get_countries()
    country_names = countries_df['name'].tolist() if not countries_df.empty else ["Colombia"]
    sel_name = st.selectbox("País", country_names)
    
    CTX = {
        "Colombia": ("XM — Mercado Eléctrico Mayorista", "Precio de Bolsa, Índice Mc, Aportes Hídricos, Cargo por Confiabilidad."),
        "Ecuador":  ("CENACE — Centro Nacional de Control de Energía", "Despacho centralizado. ~70% generación hidráulica."),
        "Brasil":   ("ONS / CCEE", "PLD equivale al Precio de Bolsa. Reservatórios = indicador crítico."),
        "México":   ("CENACE México", "Precio Marginal Local (PML) equivale al Precio de Bolsa."),
    }
    ctx = CTX.get(sel_name)
    if ctx: st.info(f"**{ctx[0]}** — {ctx[1]}")
    
    st.info("📊 Módulo en construcción: Gráficas de Precio en Bolsa e Índice Mc migrándose a esta vista dedicada.")

if __name__ == "__main__":
    main()
