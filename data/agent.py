import pandas as pd
from datetime import datetime
from scraper.engine import scrape
from data.database import save_historical_data

class VariableAgent:
    
    @staticmethod
    def ingest_variable(variable_row: pd.Series) -> dict:
        if not variable_row.get('source_url') or not variable_row.get('css_selector'):
            return {"success": False, "error": "URL o Selector CSS no definidos."}
        
        url = variable_row['source_url']
        selector = variable_row['css_selector']
        is_dynamic = bool(variable_row.get('is_dynamic', False))
        
        result = scrape(url, selector, is_dynamic)
        
        if result['success'] and result['value'] is not None:
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            # -------- Integración NLA (Contexto de Noticias & IA) --------
            try:
                from data.database import get_historical_data
                hist_df = get_historical_data(variable_row['id'])
                if not hist_df.empty:
                    prev_val = hist_df['value'].iloc[-1]
                    new_val = result['value']
                    if prev_val != 0:
                        delta = ((new_val - prev_val) / prev_val) * 100
                        if abs(delta) > 2.0: # Anomalía: umbral de 2% 
                            from models.db import SessionLocal
                            from models.schema import MacroVariable
                            from ai_engine.analyzer import analyze_anomaly
                            
                            session = SessionLocal()
                            var_obj = session.query(MacroVariable).get(variable_row['id'])
                            if var_obj:
                                # Invocar análisis asíncrono
                                analyze_anomaly(var_obj, delta, prev_val, new_val)
                            session.close()
            except Exception as e:
                print(f"Error procesando anomalía NLA: {e}")
            
            saved = save_historical_data(variable_row['id'], result['value'], today_str)
            if saved:
                result['message'] = f"Valor {result['value']} guardado exitosamente."
            else:
                result['success'] = False
                result['error'] = "Error al guardar en base de datos."
                
        return result

    @staticmethod
    def calculate_projection(historical_df: pd.DataFrame, periods: int = 3) -> pd.DataFrame:
        """
        Calcula una proyección simple (Media Móvil Simple) basada en el histórico.
        Para un uso avanzado se podría usar Prophet o ARIMA.
        """
        if historical_df.empty or len(historical_df) < 2:
            return pd.DataFrame()
            
        # Calcular media móvil como proyección básica
        last_values = historical_df['value'].tail(min(5, len(historical_df)))
        avg_change = last_values.diff().mean()
        
        if pd.isna(avg_change):
            avg_change = 0
            
        last_date = historical_df['date'].iloc[-1]
        last_value = historical_df['value'].iloc[-1]
        
        forecast = []
        current_val = last_value
        
        # Asumimos proyecciones mensuales aproximadamente (30 días)
        for i in range(1, periods + 1):
            next_date = last_date + pd.Timedelta(days=30*i)
            current_val += avg_change 
            forecast.append({
                'date': next_date,
                'value': round(current_val, 2),
                'type': 'Proyección'
            })
            
        return pd.DataFrame(forecast)
