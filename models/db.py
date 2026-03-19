import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
DEFAULT_SQLITE = f"sqlite:///{BASE_DIR / 'db.sqlite3'}"

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _add_column_if_missing(conn, table: str, col_name: str, col_def: str):
    """Intenta agregar una columna a una tabla existente (SQLite-compatible)."""
    try:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def};"))
        conn.commit()
    except Exception:
        pass  # La columna ya existe


def init_db():
    """Crea las tablas y configura hipertablas si es PostgreSQL.
    Para SQLite con tabla existente, agrega columnas faltantes con ALTER TABLE.
    """
    from models.schema import Country, MacroVariable, TimeSeriesData, AIAnalysisLog, ConsensusForecast

    Base.metadata.create_all(bind=engine)

    # Migración suave para SQLite: agregar columnas nuevas si no existen
    if engine.name == 'sqlite':
        with engine.connect() as conn:
            _add_column_if_missing(conn, 'dim_variable', 'connector_type', "VARCHAR(20) DEFAULT 'SCRAPER'")
            _add_column_if_missing(conn, 'dim_variable', 'api_provider', 'VARCHAR(50)')
            _add_column_if_missing(conn, 'dim_variable', 'api_serie_id', 'VARCHAR(200)')
            _add_column_if_missing(conn, 'dim_variable', 'last_successful_fetch', 'DATETIME')
            _add_column_if_missing(conn, 'dim_variable', 'fetch_error_count', 'INTEGER DEFAULT 0')
            _add_column_if_missing(conn, 'dim_variable', 'category', "VARCHAR(50) DEFAULT 'macro'")
        print("SQLite: migración de columnas nuevas completada.")

    if engine.name == 'postgresql':
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                conn.execute(text("SELECT create_hypertable('fact_timeseries', 'date', if_not_exists => TRUE);"))
                conn.commit()
                print("PostgreSQL + TimescaleDB inicializados exitosamente.")
        except Exception as e:
            print(f"Nota: No se pudo configurar TimescaleDB: {e}")
    else:
        print("Usando SQLite. Se omiten las optimizaciones de TimescaleDB.")
