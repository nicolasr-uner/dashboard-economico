import streamlit as st
from ui.components import load_css

st.set_page_config(page_title="Contexto Global", page_icon="🌍", layout="wide")
load_css()

def main():
    st.title("🌍 Contexto Global")
    st.markdown("Commodities globales y comparativa entre los 4 países.")
    st.info("📊 Modulo en construcción en la nueva arquitectura Multipage.")

if __name__ == "__main__":
    main()
