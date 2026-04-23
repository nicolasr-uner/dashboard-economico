"""
chatbot.py — Asistente de Datos para el Cerebro Económico.
Responde preguntas en lenguaje natural buscando en la base de datos.
Si GEMINI_API_KEY está disponible, usa Gemini Flash para respuestas enriquecidas.
Si no, usa un sistema basado en patrones/templates (sin costo).
"""
import re
import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

# Patrones de búsqueda: regex → nombre de variable a buscar en la DB
QUERY_PATTERNS = [
    (r'\btrm\b|tasa representativa|dólar.*cop|cop.*dólar|peso.*dolar|dolar.*peso', 'TRM'),
    (r'\bipc\b|inflaci[oó]n|precios.*consumidor|cpi\b', 'IPC'),
    (r'\btasa.*intervenci[oó]n|banco.*rep[uú]blica|pol[ií]tica monetaria', 'Tasa de Intervención'),
    (r'\bdtf\b|dep[oó]sito.*t[eé]rmino', 'DTF'),
    (r'\bibr\b|indicador bancario', 'IBR'),
    (r'\bpib\b|producto interno bruto|crecimiento econ[oó]mico', 'PIB'),
    (r'\bdesempleo\b|desocupaci[oó]n|tasa.*empleo', 'Desempleo'),
    (r'\bwti\b|petr[oó]leo.*crudo|crude oil', 'WTI'),
    (r'\bbrent\b', 'Brent'),
    (r'\boreservinas\b|reservas internacionales', 'Reservas'),
    (r'\bbalanza comercial\b|exportaciones.*importaciones', 'Balanza Comercial'),
    (r'\bembi\b|riesgo país|riesgo soberano|spread', 'EMBI'),
    (r'\bselic\b|tasa.*brasil', 'Selic'),
    (r'\bipca\b|inflaci[oó]n.*brasil', 'IPCA'),
    (r'\btasa.*banxico\b|tasa.*m[eé]xico\b', 'Tasa Objetivo Banxico'),
    (r'\bsolar\b|generaci[oó]n solar', 'Solar'),
    (r'\baportes h[ií]dricos\b|embalses\b', 'AporEner'),
    (r'\bprecio.*bolsa\b|precio.*energ[ií]a\b', 'Precio Bolsa'),
]


def _find_variable(question: str, variables_df: pd.DataFrame) -> pd.Series | None:
    """Busca la variable más relevante en la DB para la pregunta dada."""
    q = question.lower()
    # 1. Buscar por patrones predefinidos
    for pattern, var_keyword in QUERY_PATTERNS:
        if re.search(pattern, q, re.IGNORECASE):
            matches = variables_df[
                variables_df['name'].str.lower().str.contains(
                    var_keyword.lower(), na=False, regex=False
                )
            ]
            if not matches.empty:
                return matches.iloc[0]

    # 2. Búsqueda directa por palabras clave en nombres de variables
    words = [w for w in re.split(r'\s+', q) if len(w) > 3]
    for word in words:
        matches = variables_df[
            variables_df['name'].str.lower().str.contains(word, na=False, regex=False)
        ]
        if not matches.empty:
            return matches.iloc[0]

    return None


def _format_response(var_row: pd.Series, history_df: pd.DataFrame) -> str:
    """Formatea la respuesta del asistente con datos, fecha y fuente."""
    name = var_row.get('name', 'Variable')
    unit = str(var_row.get('unit', '') or '')
    provider = str(var_row.get('api_provider', '') or var_row.get('connector_type', 'Fuente oficial') or '').upper()
    source_url = str(var_row.get('source_url', '') or '')

    if history_df.empty:
        return (
            f"**{name}**: No hay datos disponibles en este momento. "
            f"La información se actualiza periódicamente.\n\n"
            f"*Este análisis es informativo. Verifica los datos en la fuente oficial.*"
        )

    last_row = history_df.iloc[-1]
    last_val = last_row['value']
    last_date = pd.to_datetime(last_row['date'])
    date_str = last_date.strftime('%d de %B de %Y')

    # Variación respecto al período anterior
    delta_str = ""
    if len(history_df) >= 2:
        prev_val = history_df.iloc[-2]['value']
        if prev_val and prev_val != 0:
            delta_pct = ((last_val - prev_val) / abs(prev_val)) * 100
            delta_abs = last_val - prev_val
            sign = "+" if delta_pct >= 0 else ""
            if unit == '%':
                delta_str = f" | Variación: {sign}{delta_abs:.2f} pp"
            else:
                delta_str = f" | Variación: {sign}{delta_pct:.2f}%"

    # Formato del valor
    try:
        if unit == '%':
            val_str = f"{last_val:.2f}%"
        elif abs(last_val) >= 1e9:
            val_str = f"{last_val/1e9:.2f} B {unit}"
        elif abs(last_val) >= 1e6:
            val_str = f"{last_val/1e6:.2f} M {unit}"
        elif '/' in unit:
            val_str = f"{last_val:,.4f} {unit}"
        else:
            val_str = f"{last_val:,.2f} {unit}"
    except Exception:
        val_str = f"{last_val} {unit}"

    # Fuente link
    source_line = f"Fuente: {provider}"
    if source_url and source_url != '#':
        source_line = f"[Fuente: {provider} ↗]({source_url})"

    # Días de antigüedad
    days_old = (datetime.now() - last_date).days
    freshness = ""
    if days_old > 30:
        freshness = f" ⚠️ *Dato con {days_old} días de antigüedad.*"

    response = (
        f"**{name}** al {date_str}: **{val_str}**{delta_str}\n\n"
        f"{source_line}{freshness}\n\n"
        f"*Este análisis es informativo. Verifica los datos en la fuente oficial.*"
    )
    return response


def _llm_response(question: str, context: str) -> str:
    """Usa Gemini Flash si GEMINI_API_KEY está disponible, de lo contrario retorna None."""
    try:
        api_key = os.getenv('GEMINI_API_KEY', '')
        if not api_key:
            try:
                import streamlit as st
                api_key = st.secrets.get('GEMINI_API_KEY', '')
            except Exception:
                pass
        if not api_key:
            return None

        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name='gemini-2.0-flash',
            system_instruction=(
                "Eres el Asistente de Datos del Cerebro Económico NLA, una plataforma de "
                "inteligencia macroeconómica para Colombia, México, Brasil y Ecuador. "
                "Respondes preguntas sobre datos económicos de forma concisa y en español. "
                "Siempre incluyes la fecha del dato, la fuente y un disclaimer breve. "
                "Si no tienes datos concretos en el contexto, dilo honestamente."
            )
        )
        user_message = f"Datos disponibles:\n{context}\n\nPregunta: {question}"
        response = model.generate_content(user_message)
        reply = response.text if response.text else None
        if reply:
            return reply + "\n\n*Este análisis es informativo. Verifica los datos en la fuente oficial.*"
        return None
    except Exception as e:
        logger.warning(f"[chatbot/llm] Gemini no disponible: {e}")
        return None


def answer_question(question: str, variables_df: pd.DataFrame, get_history_fn) -> str:
    """
    Función principal del asistente. Recibe una pregunta y retorna una respuesta.
    get_history_fn: callable(variable_id) -> DataFrame
    """
    if not question or not question.strip():
        return "Por favor escribe una pregunta."

    # 1. Encontrar variable relevante
    var_row = _find_variable(question, variables_df)

    if var_row is None:
        # Respuesta genérica con contexto de las variables disponibles
        available = variables_df['name'].head(10).tolist() if not variables_df.empty else []
        available_str = ", ".join(available)
        return (
            f"No encontré una variable específica para tu pregunta. "
            f"Puedes preguntarme sobre: {available_str}, entre otras.\n\n"
            f"*Este análisis es informativo. Verifica los datos en la fuente oficial.*"
        )

    # 2. Obtener datos históricos
    try:
        history_df = get_history_fn(int(var_row['id']))
    except Exception:
        history_df = pd.DataFrame()

    # 3. Intentar LLM primero si hay API key
    if not history_df.empty:
        last_val = history_df.iloc[-1]['value']
        last_date = pd.to_datetime(history_df.iloc[-1]['date']).strftime('%Y-%m-%d')
        context = (
            f"Variable: {var_row.get('name')}\n"
            f"Unidad: {var_row.get('unit', '')}\n"
            f"Último valor: {last_val} al {last_date}\n"
            f"Fuente: {var_row.get('api_provider', 'N/A')}\n"
            f"Registros disponibles: {len(history_df)}"
        )
        llm_reply = _llm_response(question, context)
        if llm_reply:
            return llm_reply

    # 4. Fallback: respuesta basada en templates
    return _format_response(var_row, history_df)
