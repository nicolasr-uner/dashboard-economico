import sqlite3
import pandas as pd
from pathlib import Path
from models.db import init_db, SessionLocal
from models.schema import MacroVariable, TimeSeriesData, AIAnalysisLog, DataTypeEnum
from datetime import datetime, timedelta

def main():
    BASE_DIR = Path(__file__).parent
    DB_PATH = BASE_DIR / 'db.sqlite3'
    
    session = SessionLocal()
    
    # 1. Update one variable to an official URL (e.g. Banco de Mexico for TIIE or DANE for IPC)
    # Let's see what variables exist:
    var = session.query(MacroVariable).filter(MacroVariable.name.ilike('%Inflacion%')).first()
    
    if not var:
        print("No se encontró variable Inflacion. Buscando IPC o Tasa...")
        var = session.query(MacroVariable).first()
        
    print(f"Actualizando la variable: {var.name} (ID: {var.id})")
    
    # Supongamos que es Inflacion o IPC Colombia
    var.source_url = "https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-al-consumidor-ipc"
    var.css_selector = "table.table-striped tbody tr:nth-child(1) td:nth-child(2)"
    var.is_dynamic = True
    session.commit()
    
    # 2. Inyectar una Anomalía para NLA
    print("Inyectando anomalía simulada...")
    
    # Crear dos datos historicos recentes
    today = datetime.now()
    yesterday = today - timedelta(days=30)
    
    val_prev = 5.0
    val_new = 8.5 # Gran salto del 3.5 puntos porcentuales (anomalía del 70%)
    
    dp1 = TimeSeriesData(
        variable_id=var.id,
        value=val_prev,
        date=yesterday,
        data_type=DataTypeEnum.REAL_OFFICIAL,
        is_anomaly=False
    )
    dp2 = TimeSeriesData(
        variable_id=var.id,
        value=val_new,
        date=today,
        data_type=DataTypeEnum.REAL_OFFICIAL,
        is_anomaly=True
    )
    
    # Si ya existen, las borramos primero
    session.query(TimeSeriesData).filter_by(variable_id=var.id).delete()
    session.add(dp1)
    session.add(dp2)
    session.commit()
    
    # 3. Inyectar Log de IA
    session.query(AIAnalysisLog).filter_by(variable_id=var.id).delete()
    log = AIAnalysisLog(
        variable_id=var.id,
        detected_change=70.0,
        ai_verdict='estructural',
        justification='El salto de 3.5 puntos porcentuales en un solo mes es histórico. Según las noticias indexadas por la base vectorial, se debe a un corte en la cadena de suministros provocado por el reciente fenómeno climático extremo, lo cual devaluó la moneda y encareció los alimentos de forma estructural.',
        news_context='- El Niño destruye 40% de los cultivos (BBC Mundo)\n- Exportaciones caen a su mínimo en la década (El Mundo Economía)',
        risk_level='alto',
        recommendation='Ajustar portafolios indexados a inflación e incrementar bonos vinculados a tasa fija.',
        analyzed_at=datetime.now()
    )
    session.add(log)
    session.commit()
    
    print("Simulacro inyectado exitosamente.")
    session.close()

if __name__ == '__main__':
    main()
