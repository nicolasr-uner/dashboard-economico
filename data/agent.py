import logging
import pandas as pd
from datetime import datetime, timedelta
from scraper.engine import scrape
from data.database import save_historical_data

logger = logging.getLogger(__name__)


class VariableAgent:

    @staticmethod
    def ingest_variable(variable_row: pd.Series) -> dict:
        """
        Ingesta una variable: primero intenta conector API, luego fallback a scraper.
        Actualiza last_successful_fetch y fetch_error_count en la DB.
        """
        connector_type = str(variable_row.get('connector_type') or 'SCRAPER').upper()
        api_provider   = variable_row.get('api_provider') or ''
        api_serie_id   = variable_row.get('api_serie_id') or ''
        var_id         = variable_row.get('id')
        var_name       = variable_row.get('name', 'Variable desconocida')

        result = {"success": False, "value": None, "error": "No se ejecutó ingesta."}

        # ── Ruta A: Conector API ──────────────────────────────────────────
        if connector_type == 'API' and api_provider and api_serie_id:
            try:
                from connectors.registry import get_connector_for_variable
                connector, serie_id = get_connector_for_variable(variable_row)
                if connector and serie_id:
                    end_date   = datetime.now().strftime("%Y-%m-%d")
                    start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
                    df = connector.fetch_series(serie_id, start_date, end_date)
                    if not df.empty:
                        latest = df.iloc[-1]
                        value  = float(latest['value'])
                        date_str = latest['date'].strftime("%Y-%m-%d") if hasattr(latest['date'], 'strftime') else str(latest['date'])[:10]
                        saved = save_historical_data(var_id, value, date_str)
                        _update_fetch_stats(var_id, success=saved)
                        if saved:
                            result = {
                                "success": True,
                                "value": value,
                                "message": f"[API/{api_provider}] {var_name}: {value} ({date_str})"
                            }
                        else:
                            result = {"success": False, "error": "Error guardando en BD."}
                        # NLA anomaly check
                        _check_anomaly(variable_row, value)
                        return result
                    else:
                        logger.warning(f"[agent] {api_provider}/{serie_id} retornó vacío — sin API key quizás.")
                        result = {"success": False, "error": f"[{api_provider}] Sin datos (¿falta API key?)."}
                        _update_fetch_stats(var_id, success=False)
                        return result
            except Exception as e:
                logger.error(f"[agent] Error en conector API para {var_name}: {e}")
                result = {"success": False, "error": f"Error conector API: {e}"}
                _update_fetch_stats(var_id, success=False)
                return result

        # ── Ruta B: Scraper (fallback) ────────────────────────────────────
        if connector_type == 'MANUAL':
            return {"success": False, "error": "Variable manual — ingresa el valor desde el formulario."}

        url      = variable_row.get('source_url') or ''
        selector = variable_row.get('css_selector') or ''
        if not url or not selector:
            return {"success": False, "error": "URL o Selector CSS no definidos."}

        is_dynamic = bool(variable_row.get('is_dynamic', False))
        scrape_result = scrape(url, selector, is_dynamic)

        if scrape_result.get('success') and scrape_result.get('value') is not None:
            value    = scrape_result['value']
            date_str = datetime.now().strftime("%Y-%m-%d")
            saved    = save_historical_data(var_id, value, date_str)
            _update_fetch_stats(var_id, success=saved)
            if saved:
                _check_anomaly(variable_row, value)
                result = {
                    "success": True,
                    "value": value,
                    "message": f"[Scraper] {var_name}: {value} guardado."
                }
            else:
                result = {"success": False, "error": "Error guardando en BD."}
        else:
            result = {"success": False, "error": scrape_result.get('error', 'Error scraping.')}
            _update_fetch_stats(var_id, success=False)

        return result

    @staticmethod
    def calculate_projection(historical_df: pd.DataFrame, periods: int = 6) -> pd.DataFrame:
        """
        Calcula proyección usando el ensemble de modelos estadísticos.
        Incluye bandas de confianza al 80% y 95%.
        Fallback a media móvil simple si statsmodels no está disponible.
        """
        if historical_df.empty or len(historical_df) < 2:
            return pd.DataFrame()

        try:
            from projections.models import forecast_ensemble
            result = forecast_ensemble(historical_df['value'], periods)
            if result:
                last_date  = historical_df['date'].iloc[-1]
                last_value = historical_df['value'].iloc[-1]

                forecast_rows = []
                for i, (fv, lo80, hi80, lo95, hi95) in enumerate(zip(
                    result['forecast'], result.get('lower_80', [None]*periods),
                    result.get('upper_80', [None]*periods),
                    result.get('lower_95', [None]*periods),
                    result.get('upper_95', [None]*periods),
                ), 1):
                    next_date = last_date + pd.Timedelta(days=30 * i)
                    forecast_rows.append({
                        'date': next_date, 'value': round(fv, 4),
                        'type': 'Proyección',
                        'model_name': result.get('model_name', 'Ensemble'),
                        'lower_80': round(lo80, 4) if lo80 is not None else None,
                        'upper_80': round(hi80, 4) if hi80 is not None else None,
                        'lower_95': round(lo95, 4) if lo95 is not None else None,
                        'upper_95': round(hi95, 4) if hi95 is not None else None,
                    })
                return pd.DataFrame(forecast_rows)
        except Exception as e:
            logger.warning(f"[agent] Ensemble falló, usando media móvil: {e}")

        # Fallback: media móvil simple
        return VariableAgent._moving_avg_projection(historical_df, periods)

    @staticmethod
    def _moving_avg_projection(historical_df: pd.DataFrame, periods: int) -> pd.DataFrame:
        """Media móvil simple como fallback de proyección."""
        last_values = historical_df['value'].tail(min(6, len(historical_df)))
        avg_change  = last_values.diff().mean()
        if pd.isna(avg_change):
            avg_change = 0

        last_date  = historical_df['date'].iloc[-1]
        last_value = historical_df['value'].iloc[-1]
        std_val    = historical_df['value'].std() * 0.5 if len(historical_df) > 3 else abs(last_value * 0.05)

        rows = []
        current_val = last_value
        for i in range(1, periods + 1):
            next_date = last_date + pd.Timedelta(days=30 * i)
            current_val += avg_change
            rows.append({
                'date': next_date, 'value': round(current_val, 4),
                'type': 'Proyección', 'model_name': 'Media Móvil',
                'lower_80': round(current_val - std_val, 4),
                'upper_80': round(current_val + std_val, 4),
                'lower_95': round(current_val - std_val * 1.5, 4),
                'upper_95': round(current_val + std_val * 1.5, 4),
            })
        return pd.DataFrame(rows)


def _update_fetch_stats(var_id, success: bool):
    """Actualiza last_successful_fetch o fetch_error_count en la DB."""
    try:
        from models.db import engine
        from sqlalchemy import text
        if success:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE dim_variable SET last_successful_fetch=:now, fetch_error_count=0 WHERE id=:id"
                ), {"now": datetime.utcnow(), "id": var_id})
        else:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE dim_variable SET fetch_error_count=COALESCE(fetch_error_count,0)+1 WHERE id=:id"
                ), {"id": var_id})
    except Exception as e:
        logger.warning(f"[agent] No se pudo actualizar stats de fetch: {e}")


def _check_anomaly(variable_row, new_value: float):
    """Verifica anomalía y lanza análisis IA si el cambio supera 2%."""
    try:
        from data.database import get_historical_data
        hist_df = get_historical_data(variable_row.get('id'))
        if hist_df.empty:
            return
        prev_val = hist_df['value'].iloc[-1]
        if prev_val != 0:
            delta = ((new_value - prev_val) / prev_val) * 100
            if abs(delta) > 2.0:
                from models.db import SessionLocal
                from models.schema import MacroVariable
                from ai_engine.analyzer import analyze_anomaly
                with SessionLocal() as session:
                    var_obj = session.get(MacroVariable, variable_row.get('id'))
                    if var_obj:
                        analyze_anomaly(var_obj, delta, prev_val, new_value)
    except Exception as e:
        logger.warning(f"[agent] Error en check_anomaly: {e}")
