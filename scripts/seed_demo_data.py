"""
seed_demo_data.py — Siembra datos de referencia para variables MANUAL/EXCEL.
=====================================================================
Inserta valores realistas con data_type='ESTIMATION' en fact_timeseries
para las variables que no tienen datos y cuyo conector es MANUAL o EXCEL.

NO sobreescribe datos existentes de tipo REAL_OFFICIAL.
Los valores son de referencia para Colombia 2026 y mercado energetico.

Uso: python scripts/seed_demo_data.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date
from models.db import engine, init_db
from sqlalchemy import text

# ── Valores de Referencia (Colombia 2026) ─────────────────────────────────────
# Fuentes: BanRep (jul 2025), XM (2025), UPME Plan Expansion 2024-2038,
#          Damodaran (Emerging Markets 2025), DANE, Asobancaria
DEMO_VALUES: dict[str, float] = {
    # ── Finanzas Corporativas - Proyectos Solares ──────────────────────────
    "WACC - Costo Promedio de Capital":          12.5,
    "TIR Proyecto (IRR)":                        18.5,
    "Costo de la Deuda (Kd)":                    10.8,
    "Costo del Equity (Ke)":                     14.2,
    "Tarifa PPA (Precio Venta de Energía)":      310.0,
    "CAPEX Solar Total (USD por proyecto)":      850000.0,
    "CAPEX Solar (USD/kWp instalado)":           680.0,
    "Factor de Planta Solar CO (P90)":           19.5,
    "Beta Sector Energía Renovable (Damodaran)": 0.85,
    "Prima de Mercado CO (Rm - Rf)":             6.5,
    "Ke (Costo del Equity) Proyectos Solares CO": 14.2,
    "Kd (Costo de Deuda) Proyectos Solares CO":  10.8,
    "WACC Proyectos Solares CO":                 12.5,
    "Deuda/Capital (D/E) Target Proyectos Solares": 60.0,
    "DSCR (Deuda Service Coverage Ratio)":       1.35,
    "Tax Benefit CAPEX Renovable (% deducible)": 50.0,
    "Depreciación Acelerada Solar (años)":       5.0,
    "Período Tax Benefits (años elegibles)":     5.0,
    "Renta Gravable Anual Tax Partner":          3500000000.0,
    "IRR Proyecto Solar (sin deuda, sin Tax)":   15.8,
    "IRR con Tax Benefits Ley 1715":             18.5,
    "IRR con Deuda (Leveraged IRR)":             22.1,
    "Retorno Tax Partner Unergy (% EA)":         14.8,
    "Spread Tax Partner vs CDT 360d":            3.6,
    "Potencia Instalada por MiniGranja (kWp)":   100.0,
    "Degradación Panel Solar (% anual)":         0.5,
    "OPEX O&M Solar (% CAPEX anual)":            1.5,
    "Costos Regulatorios Generación (COP/kWh)":  2.8,
    "Vida Útil Proyectos Solares CO":            25.0,
    "Plazo Préstamo Bancario Solar CO":          12.0,
    "SPV Fee Administración (SMMLV/mes)":        2.0,

    # ── Tasas de Interes Colombia ──────────────────────────────────────────
    "TES 10Y CO (Rf WACC)":                      11.82,
    "TES 5Y CO":                                 11.15,
    "IBR Overnight CO":                          9.25,
    "IBR E.A. CO":                               9.52,
    "IBR Trimestral CO":                         9.76,
    "DTF E.A. CO":                               9.88,
    "CDT 360 días CO":                           11.20,
    "Tasa Crédito Comercial CO":                 16.85,
    "Tasa de Intervención BanRep":               9.25,
    "Expectativas Inflación 12m CO":             5.20,

    # ── Inflacion y Precios Colombia ───────────────────────────────────────
    "IPC CO (var. anual)":                       5.42,
    "IPC CO (var. mensual)":                     0.41,
    "IPC CO Core (sin alimentos ni regulados)":  5.82,
    "IPP CO (var. anual)":                       3.81,
    "IPP CO (valor índice)":                     167.4,

    # ── Actividad Economica Colombia ───────────────────────────────────────
    "PIB Trimestral CO (var. anual)":            2.80,
    "Producción Industrial CO (ISE)":            1.20,
    "SMMLV (Salario Mínimo CO)":                 1423500.0,

    # ── Sector Externo Colombia ────────────────────────────────────────────
    "EMBI Colombia (Riesgo País)":               310.0,
    "CDS Colombia 5Y":                           185.0,

    # ── Fiscal Colombia ────────────────────────────────────────────────────
    "Déficit Fiscal CO (% PIB)":                -4.20,
    "Deuda Pública CO (% PIB)":                 56.80,
    "Tasa Impositiva Corporativa CO (Renta)":    35.0,
    "ICA CO (Impuesto Industria y Comercio)":    0.69,

    # ── Energia Colombia ───────────────────────────────────────────────────
    "Precio de Escasez":                         1052.0,
    "Capacidad Instalada Solar CO":              0.92,
    "Capacidad Instalada Renovable CO":          3.82,
    "CERE (Cargo por Confiabilidad)":            15.24,
    "Precio PPA Bilateral CO (Mc+spread)":       290.0,
    "Índice Mc (Precio contratos regulados)":    285.0,
    "Índice Mc (Precio contratos regulado)":     285.0,

    # ── Sostenibilidad ─────────────────────────────────────────────────────
    "Precio I-REC (Cert. Energía Renovable)":    0.85,
    "Precio Carbon Offset CO2":                  8.50,

    # ── Mexico ─────────────────────────────────────────────────────────────
    "EMBI México":                               390.0,

    # ── Brasil ─────────────────────────────────────────────────────────────
    "EMBI Brasil":                               215.0,
}

# ── Fecha de referencia ───────────────────────────────────────────────────────
REF_DATE = "2026-01-01"


def seed_demo():
    init_db()

    print(f"\n🌱 Sembrando datos de referencia ({REF_DATE}): {len(DEMO_VALUES)} variables\n")

    with engine.connect() as conn:
        variables = conn.execute(text("""
            SELECT v.id, v.name, v.connector_type
            FROM dim_variable v
            WHERE v.is_active = 1
            ORDER BY v.name
        """)).fetchall()

        # Construir mapa de nombre → datos existentes
        existing = {}
        rows = conn.execute(text("""
            SELECT variable_id, data_type, COUNT(*) as cnt
            FROM fact_timeseries
            GROUP BY variable_id, data_type
        """)).fetchall()
        for r in rows:
            existing[r[0]] = existing.get(r[0], {})
            existing[r[0]][r[1]] = r[2]

    inserted = 0
    skipped_real = 0
    skipped_no_demo = 0

    for var in variables:
        var_id   = var[0]
        var_name = var[1]
        var_conn = var[2]

        if var_name not in DEMO_VALUES:
            skipped_no_demo += 1
            continue

        # No tocar si ya tiene datos REAL_OFFICIAL
        has_real = existing.get(var_id, {}).get('REAL_OFFICIAL', 0) > 0
        if has_real:
            print(f"  [SKIP REAL] {var_name} — ya tiene datos REAL_OFFICIAL")
            skipped_real += 1
            continue

        value = DEMO_VALUES[var_name]

        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT OR REPLACE INTO fact_timeseries
                        (variable_id, value, date, data_type, is_anomaly, version_timestamp)
                    VALUES
                        (:vid, :val, :dt, 'ESTIMATION', 0, CURRENT_TIMESTAMP)
                """), {"vid": var_id, "val": value, "dt": REF_DATE})
            print(f"  [OK   ] {var_name:<55s} = {value}")
            inserted += 1
        except Exception as e:
            print(f"  [ERROR] {var_name}: {e}")

    print(f"\n{'='*60}")
    print(f"✅  Insertados: {inserted}")
    print(f"⏭️  Sin demo:   {skipped_no_demo}")
    print(f"ℹ️  Ya reales:  {skipped_real}")
    print(f"{'='*60}")

    # Verificar totales
    with engine.connect() as conn:
        counts = conn.execute(text("""
            SELECT data_type, COUNT(*) as cnt
            FROM fact_timeseries
            GROUP BY data_type
        """)).fetchall()
        print("\n📊 Registros en fact_timeseries por tipo:")
        for r in counts:
            print(f"   {r[0]:20s}: {r[1]:>6d}")

        total_vars = conn.execute(text("""
            SELECT COUNT(DISTINCT variable_id) FROM fact_timeseries
        """)).fetchone()[0]
        all_vars = conn.execute(text("SELECT COUNT(*) FROM dim_variable WHERE is_active=1")).fetchone()[0]
        print(f"\n   Variables con datos: {total_vars}/{all_vars} "
              f"({round(total_vars/all_vars*100,1)}% cobertura)")


if __name__ == "__main__":
    seed_demo()
