"""
backfill.py — Carga datos históricos reales desde APIs oficiales.
Soporte para ejecución en Github Actions.
"""
import sys
import os
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta
from models.db import SessionLocal, init_db
from models.schema import MacroVariable
from connectors.registry import get_connector_for_variable
from data.database import save_historical_data

def backfill(days_back=None, headless=False):
    init_db()

    end_date = date.today()
    if days_back:
        start_date = end_date - timedelta(days=days_back)
    else:
        start_date = date(2020, 1, 1)

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    with SessionLocal() as session:
        variables = session.query(MacroVariable).filter(
            MacroVariable.connector_type == 'API',
            MacroVariable.is_active == True
        ).all()

    print(f"\n🔄 Backfill iniciado: {len(variables)} variables API | {start_date_str} → {end_date_str}\n")

    ok_count = skip_count = error_count = 0

    for var in variables:
        connector, serie_id = get_connector_for_variable(var)
        if not connector:
            if not headless: print(f"  [SKIP ] {var.name} — sin conector")
            skip_count += 1
            continue

        try:
            df = connector.fetch_series(serie_id, start_date_str, end_date_str)

            if df.empty:
                if not headless: print(f"  [SKIP ] {var.name} — sin datos")
                skip_count += 1
                continue

            inserted = 0
            for _, row in df.iterrows():
                date_str = row['date'].strftime("%Y-%m-%d") if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
                value = float(row['value'])
                if save_historical_data(var.id, value, date_str):
                    inserted += 1

            if not headless: print(f"  [OK   ] {var.name}: {inserted} registros guardados.")
            ok_count += 1

        except Exception as e:
            if not headless: print(f"  [ERROR] {var.name}: {e}")
            error_count += 1

    print(f"\n{'='*60}")
    print(f"✅  OK:     {ok_count}")
    print(f"⏭️  SKIP:   {skip_count}")
    print(f"❌  ERROR:  {error_count}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta de datos macroeconómicos.")
    parser.add_argument('--days', type=int, help="Número de días hacia atrás a consultar.", default=None)
    parser.add_argument('--headless', action='store_true', help="Reducir output para logs.")
    args = parser.parse_args()
    
    backfill(days_back=args.days, headless=args.headless)

