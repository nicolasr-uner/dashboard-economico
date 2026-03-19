import pandas as pd
from datetime import datetime
from models.db import engine
from sqlalchemy import text


def get_countries() -> pd.DataFrame:
    """Obtiene la lista de países configurados."""
    query = text("SELECT id, name, code, flag_emoji FROM dim_country ORDER BY name;")
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error reading countries: {e}")
        return pd.DataFrame()


def get_variables(country_id: int | None = None) -> pd.DataFrame:
    """Obtiene las variables macroeconómicas activas."""
    try:
        with engine.connect() as conn:
            if country_id:
                query = text(
                    "SELECT * FROM dim_variable WHERE is_active = 1 AND country_id = :country_id ORDER BY name;"
                )
                return pd.read_sql(query, conn, params={"country_id": country_id})
            else:
                query = text("SELECT * FROM dim_variable WHERE is_active = 1 ORDER BY name;")
                return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error reading variables: {e}")
        return pd.DataFrame()


def get_historical_data(variable_id: int) -> pd.DataFrame:
    """Obtiene la serie temporal de datos históricos de una variable."""
    query = text(
        """
        SELECT date, value, data_type
        FROM fact_timeseries
        WHERE variable_id = :variable_id
        ORDER BY date ASC;
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"variable_id": variable_id})
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()


def save_historical_data(variable_id: int, value: float, date_str: str, data_type: str = 'REAL_OFFICIAL') -> bool:
    """Guarda un nuevo registro histórico. Compatible con SQLite y PostgreSQL+TimescaleDB."""
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        # SQLite usa INSERT OR REPLACE; PostgreSQL usa ON CONFLICT ... DO UPDATE
        try:
            query = text(
                """
                INSERT INTO fact_timeseries (variable_id, value, date, data_type, is_anomaly, version_timestamp)
                VALUES (:variable_id, :value, :date, :data_type, 0, CURRENT_TIMESTAMP)
                ON CONFLICT (variable_id, date, data_type)
                DO UPDATE SET value=EXCLUDED.value, version_timestamp=CURRENT_TIMESTAMP;
                """
            )
            with engine.begin() as conn:
                conn.execute(query, {
                    "variable_id": variable_id,
                    "value": value,
                    "date": date_obj,
                    "data_type": data_type
                })
        except Exception:
            # Fallback para SQLite
            query = text(
                """
                INSERT OR REPLACE INTO fact_timeseries (variable_id, value, date, data_type, is_anomaly, version_timestamp)
                VALUES (:variable_id, :value, :date, :data_type, 0, CURRENT_TIMESTAMP);
                """
            )
            with engine.begin() as conn:
                conn.execute(query, {
                    "variable_id": variable_id,
                    "value": value,
                    "date": date_obj,
                    "data_type": data_type
                })
        return True
    except Exception as e:
        print(f"Error al guardar datos: {e}")
        return False


def get_ai_logs(variable_id: int) -> pd.DataFrame:
    """Obtiene el último log de análisis IA para una variable."""
    query = text(
        """
        SELECT * FROM ai_analysis_log
        WHERE variable_id = :variable_id
        ORDER BY analyzed_at DESC
        LIMIT 1;
        """
    )
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={"variable_id": variable_id})
    except Exception:
        return pd.DataFrame()


def get_all_variable_names() -> pd.DataFrame:
    """Obtiene todos los nombres únicos de variables (para comparativa regional)."""
    query = text("SELECT DISTINCT name FROM dim_variable ORDER BY name;")
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


def get_variables_by_name(var_name: str) -> pd.DataFrame:
    """Obtiene todas las instancias de una variable (por nombre) con su país."""
    query = text(
        """
        SELECT v.id, v.name, c.name as country
        FROM dim_variable v
        JOIN dim_country c ON v.country_id = c.id
        WHERE v.name = :var_name;
        """
    )
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params={"var_name": var_name})
    except Exception:
        return pd.DataFrame()
