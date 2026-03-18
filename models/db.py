import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

load_dotenv()

# Streamlit Cloud necesita una DB embebida por ahora, a menos que se configure una remota.
BASE_DIR = Path(__file__).parent.parent
DEFAULT_SQLITE = f"sqlite:///{BASE_DIR / 'db.sqlite3'}"

DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE)

# Para SQLite necesitamos check_same_thread=False
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

def init_db():
    """Crea las tablas y configura hipertablas si es PostgreSQL."""
    from models.schema import Country, MacroVariable, TimeSeriesData, AIAnalysisLog
    
    Base.metadata.create_all(bind=engine)
    
    if engine.name == 'postgresql':
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                conn.execute(text("SELECT create_hypertable('fact_timeseries', 'date', if_not_exists => TRUE);"))
                conn.commit()
                print("Base de datos y TimescaleDB inicializados de forma exitosa.")
        except Exception as e:
            print(f"Nota: No se pudo configurar TimeScaleDB {e}")
    else:
        print("Usando SQLite. Se omiten las optimizaciones de TimescaleDB.")
