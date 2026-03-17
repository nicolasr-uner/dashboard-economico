import os
import json
import traceback


def analyze_anomaly(variable, detected_change: float, previous_value: float, new_value: float) -> dict:
    """Analiza una anomalía usando Claude AI."""

    # Historial reciente
    try:
        hist_qs = variable.historical_data.order_by('-date')[:6]
        historial = ', '.join([f"{h.date}: {h.value} {variable.unit}" for h in reversed(list(hist_qs))])
    except Exception:
        historial = "Sin datos históricos disponibles"

    api_key = os.getenv('ANTHROPIC_API_KEY', '')

    if not api_key:
        result = {
            'success': False,
            'verdict': 'indeterminado',
            'justification': 'API key de Anthropic no configurada. Configure ANTHROPIC_API_KEY en el archivo .env para habilitar el análisis IA.',
            'risk_level': 'medio',
            'recommendation': 'Configure la API key para obtener análisis automático.',
        }
        _save_log(variable, detected_change, result)
        return result

    prompt = f"""Eres un analista macroeconómico experto en economías latinoamericanas.

Variable: {variable.name}
País: {variable.country.name}
Cambio detectado: {detected_change:.2f}%
Valor anterior: {previous_value} {variable.unit}
Valor nuevo: {new_value} {variable.unit}
Historial reciente (últimos 6 meses): {historial}

Basándote en el contexto histórico y tu conocimiento de la economía de {variable.country.name},
determina si este cambio es:
- TRANSITORIO: causado por factores temporales, estacionales o puntuales
- ESTRUCTURAL: refleja un cambio profundo en la economía

Responde ÚNICAMENTE en este formato JSON exacto:
{{
  "verdict": "transitorio",
  "justification": "explicación breve en español de máximo 3 oraciones",
  "risk_level": "bajo",
  "recommendation": "acción sugerida en 1 oración"
}}

Los valores válidos son: verdict=transitorio|estructural, risk_level=bajo|medio|alto"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = message.content[0].text.strip()

        # Extraer JSON de la respuesta
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
        }

    except json.JSONDecodeError:
        result = {
            'success': False,
            'verdict': 'indeterminado',
            'justification': f'Error al parsear respuesta de IA: {response_text[:200]}',
            'risk_level': 'medio',
            'recommendation': 'Revisar manualmente.',
        }
    except Exception as e:
        result = {
            'success': False,
            'verdict': 'indeterminado',
            'justification': f'Error en análisis IA: {str(e)}',
            'risk_level': 'medio',
            'recommendation': 'Revisar manualmente.',
        }

    _save_log(variable, detected_change, result)
    return result


def _save_log(variable, detected_change, result):
    try:
        from core.models import AIAnalysisLog
        AIAnalysisLog.objects.create(
            variable=variable,
            detected_change=detected_change,
            ai_verdict=result.get('verdict', 'indeterminado'),
            justification=result.get('justification', ''),
            risk_level=result.get('risk_level', 'medio'),
            recommendation=result.get('recommendation', ''),
        )
    except Exception:
        pass
