import os
import json
from nla_engine.vector_store import search_news_context

def analyze_anomaly(variable, detected_change: float, previous_value: float, new_value: float) -> dict:
    """Analiza una anomalía usando Claude AI inyectando contexto de noticias (RAG)."""
    try:
        from models.db import SessionLocal
        from models.schema import TimeSeriesData
        with SessionLocal() as session:
            hist_qs = (
                session.query(TimeSeriesData)
                .filter_by(variable_id=variable.id)
                .order_by(TimeSeriesData.date.desc())
                .limit(6)
                .all()
            )
            historial = ', '.join([f"{h.date.date()}: {h.value}" for h in reversed(hist_qs)])
    except Exception:
        historial = "Sin datos históricos disponibles"

    api_key = os.getenv('ANTHROPIC_API_KEY', '')
    if not api_key:
        result = {
            'success': False,
            'verdict': 'indeterminado',
            'justification': 'API key de Anthropic no configurada.',
            'risk_level': 'medio',
            'recommendation': 'Configure la API key ANTHROPIC_API_KEY.',
            'news_context': ''
        }
        _save_log(variable, detected_change, result)
        return result

    # Módulo NLA: RAG con ChromaDB
    query_str = f"Economía {variable.name}"
    news_context_list = search_news_context(query_str, n_results=3)

    news_text_prompt = ""
    news_saved_context = ""
    if news_context_list:
        news_text_prompt = "\nNoticias recientes (ChromaDB):\n"
        for n in news_context_list:
            news_text_prompt += f"- {n['metadata']['title']}: {n['text']}\n"
            news_saved_context += f"- {n['metadata']['title']}\n"

    prompt = f"""Eres un analista macroeconómico.
Variable: {variable.name}
Cambio: {detected_change:.2f}%
Valor anterior: {previous_value} | Nuevo: {new_value}
Historial 6 meses: {historial}
{news_text_prompt}

¿El cambio es TRANSITORIO o ESTRUCTURAL?
Responde ÚNICAMENTE en JSON exacto:
{{
  "verdict": "transitorio",
  "justification": "Max 3 oraciones usando las noticias si aplican",
  "risk_level": "bajo|medio|alto",
  "recommendation": "Acción sugerida"
}}"""

    # Modelos en orden de preferencia
    models_to_try = ["claude-3-5-haiku-20241022", "claude-3-haiku-20240307"]

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        message = None
        for model_name in models_to_try:
            try:
                message = client.messages.create(
                    model=model_name,
                    max_tokens=300,
                    messages=[{"role": "user", "content": prompt}]
                )
                break
            except Exception:
                continue

        if message is None:
            raise Exception("Ningún modelo de Claude disponible.")

        response_text = message.content[0].text.strip()

        json_match = response_text
        if '```json' in response_text:
            json_match = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            json_match = response_text.split('```')[1].strip()

        parsed = json.loads(json_match)
        result = {
            'success': True,
            'verdict': parsed.get('verdict', 'indeterminado'),
            'justification': parsed.get('justification', ''),
            'risk_level': parsed.get('risk_level', 'medio'),
            'recommendation': parsed.get('recommendation', ''),
            'news_context': news_saved_context
        }
    except Exception as e:
        result = {
            'success': False,
            'verdict': 'indeterminado',
            'justification': f'Error en análisis IA: {str(e)}',
            'risk_level': 'medio',
            'recommendation': '',
            'news_context': news_saved_context
        }

    _save_log(variable, detected_change, result)
    return result


def _save_log(variable, detected_change, result):
    try:
        from models.db import SessionLocal
        from models.schema import AIAnalysisLog
        with SessionLocal() as session:
            log = AIAnalysisLog(
                variable_id=variable.id,
                detected_change=detected_change,
                ai_verdict=result.get('verdict', 'indeterminado'),
                justification=result.get('justification', ''),
                news_context=result.get('news_context', ''),
                risk_level=result.get('risk_level', 'medio'),
                recommendation=result.get('recommendation', '')
            )
            session.add(log)
            session.commit()
    except Exception as e:
        print(f"Error guardando log AI: {e}")
