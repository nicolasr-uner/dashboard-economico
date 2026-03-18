import os
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
from models.db import init_db, SessionLocal
from models.schema import Country, MacroVariable, TimeSeriesData, DataTypeEnum

def migrate():
    print("Inicializando nuevo esquema en la BD actual...")
    init_db()
    
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / 'db.sqlite3'
    
    if not DB_PATH.exists():
        print("La base de datos original no existe. Abortando migración.")
        return
        
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    print("Migrando core_country -> dim_country")
    countries = conn.execute("SELECT * FROM core_country").fetchall()
    
    session = SessionLocal()
    try:
        for c in countries:
            existing = session.query(Country).filter_by(code=c['code']).first()
            if not existing:
                nc = Country(
                    id=c['id'],
                    name=c['name'],
                    code=c['code'],
                    flag_emoji=c['flag_emoji']
                )
                session.add(nc)
        session.commit()
        
        print("Migrando core_macrovariable -> dim_variable")
        variables = conn.execute("SELECT * FROM core_macrovariable").fetchall()
        for v in variables:
            existing = session.query(MacroVariable).filter_by(id=v['id']).first()
            if not existing:
                nv = MacroVariable(
                    id=v['id'],
                    country_id=v['country_id'],
                    name=v['name'],
                    description=v['description'],
                    source_url=v['source_url'],
                    css_selector=v['css_selector'],
                    frequency=v['frequency'],
                    is_dynamic=bool(v['is_dynamic']),
                    unit=v['unit'],
                    is_active=bool(v['is_active'])
                )
                session.add(nv)
        session.commit()
        
        print("Migrando core_historicaldata -> fact_timeseries")
        
        # En SQLite, SQLAlchemy maneja Enums como VARCHAR. 
        history = conn.execute("SELECT * FROM core_historicaldata").fetchall()
        for h in history:
            try:
                date_str = h['date']
                if ' ' in date_str:
                    date_str = date_str.split(' ')[0]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except Exception:
                date_obj = pd.to_datetime(h['date']).to_pydatetime()
                
            existing = session.query(TimeSeriesData).filter_by(
                variable_id=h['variable_id'], 
                date=date_obj,
                data_type=DataTypeEnum.REAL_OFFICIAL
            ).first()
            
            if not existing:
                nh = TimeSeriesData(
                    variable_id=h['variable_id'],
                    value=h['value'],
                    date=date_obj,
                    data_type=DataTypeEnum.REAL_OFFICIAL,
                    is_anomaly=bool(h['is_anomaly'])
                )
                session.add(nh)
        session.commit()
        print("Migración completada con éxito.")
    except Exception as e:
        session.rollback()
        print(f"Error en migración: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    migrate()
