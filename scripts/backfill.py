"""
backfill.py — Carga datos históricos reales desde APIs oficiales.
Ejecutar: python scripts/backfill.py

Descarga desde 2024-01-01 hasta hoy para todas las variables con connector_type='API'.
Variables sin API key (Banxico, FRED) son registradas como SKIP.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from models.db import SessionLocal, engine, init_db
from models.schema import MacroVariable
from connectors.registry import get_connector_for_variable
from sqlalchemy import text

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE = "2024-01-01"
END_DATE   = date.today().strftime("%Y-%m-%d")


def backfill():
    init_db()

    with SessionLocal() as session:
        variables = session.query(MacroVariable).filter(
            MacroVariable.connector_type == 'API',
            MacroVariable.is_active == True
        ).all()

    print(f"\n🔄 Backfill iniciado: {len(variables)} variables API | {START_DATE} → {END_DATE}\n")

    ok_count    = 0
    skip_count  = 0
    error_count = 0

    for var in variables:
        connector, serie_id = get_connector_for_variable(var)
        if not connector:
            print(f"  [SKIP ] {var.name} — sin conector para '{var.api_provider}'")
            skip_count += 1
            continue

        try:
            df = connector.fetch_series(serie_id, START_DATE, END_DATE)

            if df.empty:
                print(f"  [SKIP ] {var.name} ({var.api_provider}/{serie_id}) — sin datos (¿falta API key?)")
                skip_count += 1
                continue

            # Borrar datos previos REAL_OFFICIAL para esta variable antes de reinsertar
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM fact_timeseries WHERE variable_id = :vid AND data_type = 'REAL_OFFICIAL'"),
                    {"vid": var.id}
                )

            # Insertar datos reales
            inserted = 0
            for _, row in df.iterrows():
                date_str = row['date'].strftime("%Y-%m-%d") if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
                value    = float(row['value'])
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT OR REPLACE INTO fact_timeseries
                                (variable_id, value, date, data_type, is_anomaly, version_timestamp)
                            VALUES
                                (:vid, :val, :dt, 'REAL_OFFICIAL', 0, CURRENT_TIMESTAMP)
                        """), {"vid": var.id, "val": value, "dt": date_str})
                    inserted += 1
                except Exception as e_ins:
                    pass  # duplicado u otro error — continuar

            # Actualizar last_successful_fetch
            with engine.begin() as conn:
                from datetime import datetime
                conn.execute(text(
                    "UPDATE dim_variable SET last_successful_fetch=:now, fetch_error_count=0 WHERE id=:id"
                ), {"now": datetime.utcnow(), "id": var.id})

            print(f"  [OK   ] {var.name} ({var.api_provider}/{serie_id}): {inserted} registros cargados.")
            ok_count += 1

        except Exception as e:
            print(f"  [ERROR] {var.name}: {e}")
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE dim_variable SET fetch_error_count=COALESCE(fetch_error_count,0)+1 WHERE id=:id"
                ), {"id": var.id})
            error_count += 1

    # ── Resumen ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"✅  OK:     {ok_count}")
    print(f"⏭️  SKIP:   {skip_count}")
    print(f"❌  ERROR:  {error_count}")
    print(f"{'='*60}")

    # Total registros en la DB
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM fact_timeseries WHERE data_type='REAL_OFFICIAL'")).fetchone()[0]
        print(f"📊  Total registros REAL_OFFICIAL en DB: {total}")

    # Verificar valores de sentido para variables clave
    print("\n🔍 Verificación de valores (últimos datos por variable):")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT v.name, v.unit, t.value, t.date
            FROM fact_timeseries t
            JOIN dim_variable v ON t.variable_id = v.id
            WHERE t.data_type = 'REAL_OFFICIAL'
              AND t.date = (
                  SELECT MAX(t2.date) FROM fact_timeseries t2
                  WHERE t2.variable_id = t.variable_id AND t2.data_type = 'REAL_OFFICIAL'
              )
            ORDER BY v.name;
        """)).fetchall()
        for r in rows:
            print(f"   {r[0]:45s} {r[2]:>12.4f} {r[1] or '':10s}  [{str(r[3])[:10]}]")


if __name__ == "__main__":
    backfill()
