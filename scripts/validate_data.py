"""
validate_data.py — Script de validacion de integridad del Data Lake Economico
==================================================================================
Verifica: missing values, outliers (IQR), consistencia temporal, cobertura,
frescura y gaps entre registros para todas las variables activas.

Uso:
    python scripts/validate_data.py               # reporte por consola
    python scripts/validate_data.py --output md    # genera docs/validation_report.md
"""
import sys, os, argparse
from pathlib import Path
from datetime import datetime, timedelta

# Asegurar raiz del proyecto en sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import text
from models.db import engine


# ── Constantes ────────────────────────────────────────────────────────────────
IQR_MULTIPLIER = 3.0          # Factor para deteccion de outliers (conservador)
STALE_DAYS_DAILY = 7          # Datos diarios se consideran stale si > N dias
STALE_DAYS_MONTHLY = 60       # Datos mensuales
STALE_DAYS_QUARTERLY = 120    # Datos trimestrales
MIN_RECORDS_FOR_STATS = 5     # Minimo registros para calcular estadisticas


def load_variables() -> pd.DataFrame:
    q = text("""
        SELECT v.id, v.name, v.category, v.unit, v.frequency,
               v.connector_type, v.api_provider, v.api_serie_id,
               v.is_active, v.last_successful_fetch, v.fetch_error_count,
               c.name as country, c.code as country_code
        FROM dim_variable v
        JOIN dim_country c ON v.country_id = c.id
        WHERE v.is_active = 1
        ORDER BY c.code, v.category, v.name
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn)


def load_timeseries(variable_id: int) -> pd.DataFrame:
    q = text("""
        SELECT date, value, data_type
        FROM fact_timeseries
        WHERE variable_id = :vid
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"vid": variable_id})
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    return df


# ── Checks ────────────────────────────────────────────────────────────────────
def check_missing(ts: pd.DataFrame) -> dict:
    """Cuenta NaN / null en la columna value."""
    total = len(ts)
    missing = int(ts['value'].isna().sum())
    return {"total_records": total, "missing_values": missing,
            "missing_pct": round(missing / total * 100, 2) if total else 0}


def check_outliers(ts: pd.DataFrame) -> dict:
    """Deteccion por IQR (inter-quartile range)."""
    if len(ts) < MIN_RECORDS_FOR_STATS:
        return {"outlier_count": 0, "outlier_pct": 0, "bounds": None}
    vals = ts['value'].dropna()
    q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - IQR_MULTIPLIER * iqr, q3 + IQR_MULTIPLIER * iqr
    outliers = vals[(vals < lo) | (vals > hi)]
    return {
        "outlier_count": len(outliers),
        "outlier_pct": round(len(outliers) / len(vals) * 100, 2),
        "bounds": (round(lo, 4), round(hi, 4))
    }


def check_temporal(ts: pd.DataFrame, frequency: str) -> dict:
    """Verifica gaps temporales segun frecuencia declarada."""
    if len(ts) < 2:
        return {"gaps": [], "max_gap_days": None}
    dates = ts['date'].sort_values()
    diffs = dates.diff().dt.days.dropna()

    expected_gap = {"daily": 3, "monthly": 45, "quarterly": 120,
                    "annual": 400, "weekly": 10}.get(frequency, 45)

    gaps = []
    for i, d in enumerate(diffs):
        if d > expected_gap:
            idx = diffs.index[i]
            gaps.append({
                "from": str(dates.iloc[idx - 1].date()) if idx > 0 else "?",
                "to": str(dates.iloc[idx].date()),
                "gap_days": int(d)
            })
    return {"gaps": gaps[:10], "max_gap_days": int(diffs.max()) if len(diffs) else None}


def check_freshness(ts: pd.DataFrame, frequency: str) -> dict:
    """Evalua si el ultimo dato es reciente."""
    if ts.empty:
        return {"last_date": None, "days_since": None, "is_stale": True}
    last = ts['date'].max()
    days_since = (datetime.now() - last).days
    thresholds = {"daily": STALE_DAYS_DAILY, "monthly": STALE_DAYS_MONTHLY,
                  "quarterly": STALE_DAYS_QUARTERLY}
    threshold = thresholds.get(frequency, STALE_DAYS_MONTHLY)
    return {
        "last_date": str(last.date()),
        "days_since": days_since,
        "is_stale": days_since > threshold
    }


# ── Runner ────────────────────────────────────────────────────────────────────
def run_validation() -> list[dict]:
    variables = load_variables()
    results = []
    for _, var in variables.iterrows():
        ts = load_timeseries(var['id'])
        freq = var.get('frequency', 'monthly') or 'monthly'
        result = {
            "id": int(var['id']),
            "name": var['name'],
            "country": var['country_code'],
            "category": var['category'],
            "unit": var.get('unit', ''),
            "connector": var['connector_type'],
            "provider": var.get('api_provider', ''),
            "frequency": freq,
            **check_missing(ts),
            **check_outliers(ts),
            **check_temporal(ts, freq),
            **check_freshness(ts, freq),
        }
        results.append(result)
    return results


def severity(r: dict) -> str:
    if r['total_records'] == 0:
        return "CRITICAL"
    if r['is_stale']:
        return "WARNING"
    if r['outlier_count'] > 0 or r['missing_values'] > 0:
        return "INFO"
    return "OK"


# ── Reports ───────────────────────────────────────────────────────────────────
def print_console(results: list[dict]):
    print("=" * 90)
    print(f"  DATA VALIDATION REPORT  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Variables auditadas: {len(results)}")
    print("=" * 90)

    for sev in ("CRITICAL", "WARNING", "INFO", "OK"):
        items = [r for r in results if severity(r) == sev]
        if not items:
            continue
        print(f"\n{'#' * 3} {sev} ({len(items)} variables) {'#' * 40}")
        for r in items:
            flag = {"CRITICAL": "[!!]", "WARNING": "[! ]", "INFO": "[i ]", "OK": "[ok]"}[sev]
            print(f"  {flag} {r['country']:3s} | {r['name'][:40]:<40s} | "
                  f"records={r['total_records']:<5d} | last={r['last_date'] or 'NONE':<12s} | "
                  f"outliers={r['outlier_count']}")
            if r['gaps']:
                for g in r['gaps'][:3]:
                    print(f"       gap: {g['from']} -> {g['to']} ({g['gap_days']} days)")

    # Resumen
    total = len(results)
    empty = sum(1 for r in results if r['total_records'] == 0)
    stale = sum(1 for r in results if r['is_stale'] and r['total_records'] > 0)
    with_outliers = sum(1 for r in results if r['outlier_count'] > 0)
    healthy = sum(1 for r in results if severity(r) == 'OK')

    print(f"\n{'=' * 90}")
    print(f"  RESUMEN: {total} variables | {healthy} OK | {empty} sin datos | "
          f"{stale} desactualizadas | {with_outliers} con outliers")
    print(f"  Cobertura: {round((total - empty) / total * 100, 1)}%")
    print(f"{'=' * 90}")


def generate_markdown(results: list[dict]) -> str:
    lines = [
        f"# Data Validation Report",
        f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  ",
        f"**Variables auditadas:** {len(results)}\n",
    ]

    total = len(results)
    empty = sum(1 for r in results if r['total_records'] == 0)
    stale = sum(1 for r in results if r['is_stale'] and r['total_records'] > 0)
    healthy = sum(1 for r in results if severity(r) == 'OK')

    lines.append("## Resumen Ejecutivo\n")
    lines.append(f"| Metrica | Valor |")
    lines.append(f"|---------|-------|")
    lines.append(f"| Total variables | {total} |")
    lines.append(f"| Con datos | {total - empty} |")
    lines.append(f"| Sin datos (CRITICAL) | {empty} |")
    lines.append(f"| Desactualizadas (WARNING) | {stale} |")
    lines.append(f"| Saludables (OK) | {healthy} |")
    lines.append(f"| Cobertura | {round((total - empty) / total * 100, 1)}% |\n")

    # Tabla detallada por pais
    for country_code in sorted(set(r['country'] for r in results)):
        country_results = [r for r in results if r['country'] == country_code]
        lines.append(f"## {country_code}\n")
        lines.append("| Variable | Cat | Records | Last Date | Stale | Outliers | Severity |")
        lines.append("|----------|-----|---------|-----------|-------|----------|----------|")
        for r in country_results:
            sev = severity(r)
            lines.append(
                f"| {r['name'][:35]} | {r['category'][:12]} | {r['total_records']} | "
                f"{r['last_date'] or 'NONE'} | {'Yes' if r['is_stale'] else 'No'} | "
                f"{r['outlier_count']} | {sev} |"
            )
        lines.append("")

    # Variables criticas (sin datos)
    critical = [r for r in results if r['total_records'] == 0]
    if critical:
        lines.append("## Variables Criticas (Sin Datos)\n")
        lines.append("Estas variables estan definidas pero no tienen ningun registro en fact_timeseries:\n")
        for r in critical:
            prov = f" / {r['provider']}" if r['provider'] else ''
            lines.append(f"- **{r['country']}** | `{r['name']}` | connector: `{r['connector']}{prov}`")
        lines.append("")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validacion de datos del Data Lake Economico")
    parser.add_argument("--output", choices=["console", "md"], default="console",
                        help="Formato de salida (default: console)")
    args = parser.parse_args()

    results = run_validation()

    if args.output == "md":
        md = generate_markdown(results)
        out_path = ROOT / "docs" / "validation_report.md"
        out_path.parent.mkdir(exist_ok=True)
        out_path.write_text(md, encoding="utf-8")
        print(f"Reporte generado en: {out_path}")
    else:
        print_console(results)
