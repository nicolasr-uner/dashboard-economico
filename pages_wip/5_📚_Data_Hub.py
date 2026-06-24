import streamlit as st
import pandas as pd
from ui.components import load_css, _FREQ_LABEL
from data.database import get_countries, get_all_variable_names
import os

st.set_page_config(page_title="Data Hub", page_icon="📚", layout="wide")
load_css()

def main():
    st.title("📚 Data Hub")
    st.markdown("Gestión de biblioteca y actualización de fuentes.")
    st.info("📊 Esta vista está migrándose a la nueva arquitectura. Todas las variables están operando mediante GitHub Actions de fondo (según lo configurado en `.github/workflows`).")
    
if __name__ == "__main__":
    main()
