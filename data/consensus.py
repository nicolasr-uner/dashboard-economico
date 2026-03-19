from datetime import datetime
import pandas as pd
from sqlalchemy import text
from models.db import engine, SessionLocal
from models.schema import ConsensusForecast

def save_consensus_forecast(variable_id: int, source_institution: str, forecast_date: datetime, 
                            target_date: datetime, value: float, scenario: str = 'base', notes: str = ''):
    """Guarda una proyección de consenso en dim_consensus_forecast."""
    try:
        with SessionLocal() as session:
            # Recrea el objeto si ya existe una proyeccion identica (institucion, variable, fecha proyectada, y escenario) 
            # para actualizarla, en vez de duplicarla.
            existing = session.query(ConsensusForecast).filter_by(
                variable_id=variable_id,
                source_institution=source_institution,
                target_date=target_date,
                scenario=scenario
            ).first()

            if existing:
                existing.forecast_date = forecast_date
                existing.forecast_value = value
                existing.notes = notes
            else:
                forecast = ConsensusForecast(
                    variable_id=variable_id,
                    source_institution=source_institution,
                    forecast_date=forecast_date,
                    target_date=target_date,
                    forecast_value=value,
                    scenario=scenario,
                    notes=notes
                )
                session.add(forecast)
            session.commit()
            return True
    except Exception as e:
        print(f"Error al guardar consenso: {e}")
        return False


def get_consensus_forecasts(variable_id: int = None, country_id: int = None) -> pd.DataFrame:
    """Obtiene las últimas proyecciones de consenso, filtrables por variable y/o país."""
    query_str = """
        SELECT c.*, v.name as variable_name
        FROM dim_consensus_forecast c
        JOIN dim_variable v ON c.variable_id = v.id
        WHERE 1=1
    """
    params = {}
    
    if variable_id:
        query_str += " AND c.variable_id = :vid"
        params['vid'] = variable_id
        
    if country_id:
        query_str += " AND v.country_id = :cid"
        params['cid'] = country_id
        
    query_str += " ORDER BY c.target_date ASC, c.source_institution ASC;"
    
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query_str), conn, params=params)
    except Exception as e:
        print(f"Error leyendo consensos: {e}")
        return pd.DataFrame()


def get_latest_consensus_by_variable(variable_id: int) -> pd.DataFrame:
    """Retorna las proyecciones más recientes de cada institución para una variable."""
    # Group by source_institution and scenario to get the latest forecast
    query_str = """
        WITH RankedForecasts AS (
            SELECT target_date, source_institution, forecast_value, scenario, forecast_date, notes,
                   ROW_NUMBER() OVER(PARTITION BY source_institution, scenario ORDER BY forecast_date DESC) as rk
            FROM dim_consensus_forecast
            WHERE variable_id = :vid
        )
        SELECT target_date, source_institution, forecast_value, scenario, forecast_date, notes
        FROM RankedForecasts
        WHERE rk = 1
        ORDER BY target_date ASC, source_institution ASC;
    """
    try:
        # SQLite support for CTEs and Window Functions is good since SQLite 3.25
        with engine.connect() as conn:
            return pd.read_sql(text(query_str), conn, params={'vid': variable_id})
    except Exception as e:
        print(f"Error leyendo los consensos más recientes: {e}")
        return pd.DataFrame()
