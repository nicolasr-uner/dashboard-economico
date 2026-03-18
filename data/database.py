import pandas as pd
from datetime import datetime
from models.db import engine
from sqlalchemy import text

def get_countries() -> pd.DataFrame:
    """Obtiene la lista de países configurados desde PostgreSQL."""
    query = "SELECT id, name, code, flag_emoji FROM dim_country ORDER BY name;"
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error reading countries: {e}")
        return pd.DataFrame()

def get_variables(country_id: int | None = None) -> pd.DataFrame:
    """Obtiene las variables macroeconómicas activas."""
    query = "SELECT * FROM dim_variable WHERE is_active = true"
    if country_id:
        query += f" AND country_id = {country_id}"
    query += " ORDER BY name;"
    
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error reading variables: {e}")
        return pd.DataFrame()

def get_historical_data(variable_id: int) -> pd.DataFrame:
    """Obtiene la serie temporal de datos históricos de una variable, ordenada cronológicamente."""
    query = f'''
        SELECT date, value, data_type 
        FROM fact_timeseries 
        WHERE variable_id = {variable_id}
        ORDER BY date ASC;
    '''
    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()

def save_historical_data(variable_id: int, value: float, date_str: str, data_type: str = 'REAL_OFFICIAL') -> bool:
    """Guarda un nuevo registro histórico hipertabla (TimescaleDB)."""
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        
        query = """
        INSERT INTO fact_timeseries (variable_id, value, date, data_type, is_anomaly, version_timestamp)
        VALUES (:variable_id, :value, :date, :data_type, false, CURRENT_TIMESTAMP)
        ON CONFLICT (variable_id, date, data_type) DO UPDATE SET value=EXCLUDED.value, version_timestamp=CURRENT_TIMESTAMP;
        """
        
        with engine.begin() as conn:
            conn.execute(text(query), {
                "variable_id": variable_id, 
                "value": value, 
                "date": date_obj,
                "data_type": data_type
            })
        return True
    except Exception as e:
        print(f"Error al guardar datos: {e}")
        return False
