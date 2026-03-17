import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db.sqlite3"

def get_db_connection():
    """Retorna una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def get_countries() -> pd.DataFrame:
    """Obtiene la lista de países configurados."""
    query = "SELECT * FROM core_country ORDER BY name;"
    with get_db_connection() as conn:
        return pd.read_sql(query, conn)

def get_variables(country_id: int = None) -> pd.DataFrame:
    """Obtiene las variables macroeconómicas activas."""
    query = "SELECT * FROM core_macrovariable WHERE is_active = 1"
    if country_id:
        query += f" AND country_id = {country_id}"
    query += " ORDER BY name;"
    
    with get_db_connection() as conn:
        return pd.read_sql(query, conn)

def get_historical_data(variable_id: int) -> pd.DataFrame:
    """Obtiene la serie temporal de datos históricos de una variable, ordenada cronológicamente."""
    query = f'''
        SELECT * FROM core_historicaldata 
        WHERE variable_id = {variable_id}
        ORDER BY date ASC;
    '''
    with get_db_connection() as conn:
        df = pd.read_sql(query, conn)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

def save_historical_data(variable_id: int, value: float, date_str: str) -> bool:
    """Guarda un nuevo registro histórico en la base de datos (Ingesta Manual o Agente)."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = """
            INSERT INTO core_historicaldata (variable_id, value, date, is_anomaly, scraped_at)
            VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP)
            ON CONFLICT(variable_id, date) DO UPDATE SET value=excluded.value, scraped_at=CURRENT_TIMESTAMP;
            """
            cursor.execute(query, (variable_id, value, date_str))
            conn.commit()
            return True
    except Exception as e:
        print(f"Error al guardar datos: {e}")
        return False
