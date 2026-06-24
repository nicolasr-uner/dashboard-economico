import pandas as pd
from datetime import datetime
from sqlalchemy import select, desc
from models.db import engine, SessionLocal
from models.schema import Country, MacroVariable, TimeSeriesData, AIAnalysisLog

def get_countries() -> pd.DataFrame:
    """Obtiene la lista de países configurados usando ORM."""
    try:
        with SessionLocal() as session:
            stmt = select(Country.id, Country.name, Country.code, Country.flag_emoji).order_by(Country.name)
            return pd.read_sql(stmt, session.bind)
    except Exception as e:
        print(f"Error reading countries: {e}")
        return pd.DataFrame()


def get_variables(country_id: int | None = None) -> pd.DataFrame:
    """Obtiene las variables macroeconómicas activas usando ORM."""
    try:
        with SessionLocal() as session:
            stmt = select(MacroVariable).where(MacroVariable.is_active == True)
            if country_id:
                stmt = stmt.where(MacroVariable.country_id == country_id)
            stmt = stmt.order_by(MacroVariable.name)
            return pd.read_sql(stmt, session.bind)
    except Exception as e:
        print(f"Error reading variables: {e}")
        return pd.DataFrame()


def get_historical_data(variable_id: int) -> pd.DataFrame:
    """Obtiene la serie temporal de datos históricos de una variable usando ORM."""
    try:
        with SessionLocal() as session:
            stmt = select(TimeSeriesData.date, TimeSeriesData.value, TimeSeriesData.data_type)\
                   .where(TimeSeriesData.variable_id == variable_id)\
                   .order_by(TimeSeriesData.date.asc())
            df = pd.read_sql(stmt, session.bind)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
            return df
    except Exception as e:
        print(f"Error reading historical data: {e}")
        return pd.DataFrame()


def get_last_known_value(variable_id: int) -> dict | None:
    """Devuelve el dato más reciente disponible, sin filtro de fechas. Usado como fallback."""
    try:
        with SessionLocal() as session:
            res = session.execute(
                select(TimeSeriesData.value, TimeSeriesData.date)
                .where(TimeSeriesData.variable_id == variable_id)
                .order_by(desc(TimeSeriesData.date))
                .limit(1)
            ).first()
            
            if res:
                return {
                    'value': float(res.value),
                    'date': pd.to_datetime(res.date)
                }
    except Exception as e:
        print(f"Error reading last known value: {e}")
    return None


def save_historical_data(variable_id: int, value: float, date_str: str, data_type: str = 'REAL_OFFICIAL') -> bool:
    """Guarda un nuevo registro histórico usando UPSERT nativo de SQLAlchemy (PostgreSQL / SQLite)."""
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        is_postgres = engine.name == 'postgresql'
        
        with SessionLocal() as session:
            if is_postgres:
                from sqlalchemy.dialects.postgresql import insert
                stmt = insert(TimeSeriesData).values(
                    variable_id=variable_id,
                    value=value,
                    date=date_obj,
                    data_type=data_type,
                    is_anomaly=False,
                    version_timestamp=datetime.utcnow()
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['variable_id', 'date', 'data_type'],
                    set_=dict(value=stmt.excluded.value, version_timestamp=datetime.utcnow())
                )
            else:
                from sqlalchemy.dialects.sqlite import insert
                stmt = insert(TimeSeriesData).values(
                    variable_id=variable_id,
                    value=value,
                    date=date_obj,
                    data_type=data_type,
                    is_anomaly=False,
                    version_timestamp=datetime.utcnow()
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['variable_id', 'date', 'data_type'],
                    set_=dict(value=stmt.excluded.value, version_timestamp=datetime.utcnow())
                )
            
            session.execute(stmt)
            session.commit()
            return True
    except Exception as e:
        print(f"Error al guardar datos: {e}")
        return False


def get_ai_logs(variable_id: int) -> pd.DataFrame:
    """Obtiene el último log de análisis IA para una variable."""
    try:
        with SessionLocal() as session:
            stmt = select(AIAnalysisLog).where(AIAnalysisLog.variable_id == variable_id).order_by(desc(AIAnalysisLog.analyzed_at)).limit(1)
            return pd.read_sql(stmt, session.bind)
    except Exception as e:
        return pd.DataFrame()


def get_all_variable_names() -> pd.DataFrame:
    """Obtiene todos los nombres únicos de variables."""
    try:
        with SessionLocal() as session:
            stmt = select(MacroVariable.name).distinct().order_by(MacroVariable.name)
            return pd.read_sql(stmt, session.bind)
    except Exception as e:
        return pd.DataFrame()


def get_variables_by_name(var_name: str) -> pd.DataFrame:
    """Obtiene todas las instancias de una variable (por nombre) con su país."""
    try:
        with SessionLocal() as session:
            stmt = select(MacroVariable.id, MacroVariable.name, Country.name.label('country'))\
                   .join(Country, MacroVariable.country_id == Country.id)\
                   .where(MacroVariable.name == var_name)
            return pd.read_sql(stmt, session.bind)
    except Exception as e:
        return pd.DataFrame()


def save_manual_data_point(variable_id: int, date_val, value: float) -> bool:
    """Wrapper para entrada manual desde el Data Hub."""
    from datetime import date as date_type
    date_str = date_val.strftime("%Y-%m-%d") if hasattr(date_val, 'strftime') else str(date_val)
    return save_historical_data(variable_id, value, date_str, data_type='REAL_OFFICIAL')
