"""
seed_external_projections.py — Proyecciones externas de instituciones y bancos.
Carga en dim_consensus_forecast:
  1. Proyecciones hardcoded de bancos/analistas (Bancolombia, Goldman, JPMorgan, IMF, CEPAL, BCE)
  2. Proyecciones dinámicas desde IMF DataMapper API (sin auth)
  3. Proyecciones dinámicas desde BCB Focus OData (sin auth)

Ejecutar: python -X utf8 scripts/seed_external_projections.py
Idempotente: save_consensus_forecast() hace upsert por (variable_id, institution, target_date, scenario).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from models.db import init_db, SessionLocal
from models.schema import MacroVariable
from data.consensus import save_consensus_forecast

init_db()


def get_var_id(session, name_fragment: str):
    """Busca variable_id por fragmento de nombre (case-insensitive)."""
    vars_all = session.query(MacroVariable).all()
    for v in vars_all:
        if name_fragment.lower() in v.name.lower():
            return v.id
    return None


def get_var_id_exact(session, name: str):
    """Busca variable_id por nombre exacto."""
    v = session.query(MacroVariable).filter(MacroVariable.name == name).first()
    return v.id if v else None


# ── Proyecciones hardcoded de analistas/bancos/instituciones ──────────────────
# Formato: (nombre_variable_fragment, institution, year, value, scenario, notes)
# variable_fragment: coincidencia parcial case-insensitive
INSTITUTIONAL_FORECASTS = [

    # ── Colombia ──────────────────────────────────────────────────────────────
    # PIB Colombia
    ("PIB Trimestral CO", "Bancolombia",       2025, 2.9,  "base",       "Informe Perspectivas Económicas dic 2024"),
    ("PIB Trimestral CO", "BanRep",            2025, 2.8,  "base",       "Informe de Política Monetaria ene 2025"),
    ("PIB Trimestral CO", "ANIF",              2025, 3.0,  "base",       "Perspectivas Económicas ANIF 2025"),
    ("PIB Trimestral CO", "BBVA Research",     2025, 2.6,  "base",       "Situación Colombia BBVA 2025"),
    ("PIB Trimestral CO", "Corficolombiana",   2025, 2.9,  "base",       "Informe Macroeconómico Corficolombiana dic 2024"),
    ("PIB Trimestral CO", "Goldman Sachs",     2025, 2.5,  "base",       "LATAM 2025 Macro Outlook — GS"),
    ("PIB Trimestral CO", "JPMorgan",          2025, 2.7,  "base",       "Emerging Markets Outlook 2025 — JPM"),
    ("PIB Trimestral CO", "Citibank",          2025, 2.8,  "base",       "LATAM Macro Strategy 2025 — Citi"),
    ("PIB Trimestral CO", "IMF WEO",           2025, 2.8,  "base",       "World Economic Outlook Abr 2025 — FMI"),
    ("PIB Trimestral CO", "IMF WEO",           2026, 3.1,  "base",       "World Economic Outlook Abr 2025 — FMI"),
    ("PIB Trimestral CO", "CEPAL",             2025, 2.6,  "base",       "Balance Preliminar CEPAL 2024"),
    ("PIB Trimestral CO", "Banco Mundial",     2025, 2.7,  "base",       "Global Economic Prospects Ene 2025 — WB"),

    # IPC Colombia
    ("IPC CO (var. anual)", "BanRep",          2025, 5.2,  "base",       "IPM ene 2025 — BanRep"),
    ("IPC CO (var. anual)", "BanRep",          2026, 3.6,  "base",       "IPM ene 2025 — BanRep"),
    ("IPC CO (var. anual)", "Bancolombia",     2025, 5.5,  "base",       "Perspectivas Económicas Bancolombia dic 2024"),
    ("IPC CO (var. anual)", "ANIF",            2025, 5.0,  "base",       "ANIF 2025"),
    ("IPC CO (var. anual)", "IMF WEO",         2025, 5.3,  "base",       "WEO Abr 2025 — FMI"),
    ("IPC CO (var. anual)", "IMF WEO",         2026, 3.8,  "base",       "WEO Abr 2025 — FMI"),
    ("IPC CO (var. anual)", "BBVA Research",   2025, 5.1,  "base",       "Situación Colombia BBVA 2025"),
    ("IPC CO (var. anual)", "Corficolombiana", 2025, 5.4,  "base",       "Informe Mensual Corficolombiana"),

    # Tasa de Intervención BanRep
    ("Tasa de Intervención BanRep", "BanRep (encuesta)",   2025, 8.75, "base",   "Encuesta Expectativas Analistas feb 2025"),
    ("Tasa de Intervención BanRep", "Bancolombia",         2025, 8.25, "base",   "Perspectivas Bancolombia dic 2024"),
    ("Tasa de Intervención BanRep", "Corficolombiana",     2025, 9.00, "base",   "Informe Corficolombiana dic 2024"),
    ("Tasa de Intervención BanRep", "BBVA Research",       2025, 8.50, "base",   "Situación Colombia BBVA 2025"),
    ("Tasa de Intervención BanRep", "Goldman Sachs",       2025, 8.00, "base",   "LATAM Rates Outlook 2025 — GS"),

    # TRM Colombia
    ("TRM (COP/USD)", "Bancolombia",    2025, 4200.0, "base",      "Perspectivas Bancolombia dic 2024"),
    ("TRM (COP/USD)", "Corficolombiana",2025, 4350.0, "base",      "Informe Corficolombiana dic 2024"),
    ("TRM (COP/USD)", "BBVA Research",  2025, 4150.0, "base",      "Situación Colombia BBVA 2025"),
    ("TRM (COP/USD)", "Goldman Sachs",  2025, 4100.0, "base",      "LATAM FX Outlook 2025 — GS"),
    ("TRM (COP/USD)", "JPMorgan",       2025, 4300.0, "base",      "EM FX Outlook 2025 — JPM"),
    ("TRM (COP/USD)", "Citibank",       2025, 4500.0, "pessimista","LATAM FX Bear Case — Citi"),

    # Desempleo Colombia
    ("Desempleo CO", "DANE/ANIF",  2025, 9.8,  "base", "Proyección ANIF 2025"),
    ("Desempleo CO", "IMF WEO",    2025, 10.1, "base", "WEO Abr 2025 — FMI"),
    ("Desempleo CO", "IMF WEO",    2026, 9.6,  "base", "WEO Abr 2025 — FMI"),

    # ── México ────────────────────────────────────────────────────────────────
    # PIB México
    ("PIB Trimestral MX", "Banxico Encuesta",  2025, 0.8,  "base",       "Encuesta Analistas Banxico feb 2025"),
    ("PIB Trimestral MX", "BBVA Research",     2025, 1.0,  "base",       "Situación México BBVA 2025"),
    ("PIB Trimestral MX", "Goldman Sachs",     2025, 0.6,  "base",       "LATAM 2025 Macro Outlook — GS"),
    ("PIB Trimestral MX", "JPMorgan",          2025, 0.9,  "base",       "EM Outlook 2025 — JPM"),
    ("PIB Trimestral MX", "IMF WEO",           2025, 1.0,  "base",       "WEO Abr 2025 — FMI"),
    ("PIB Trimestral MX", "IMF WEO",           2026, 1.5,  "base",       "WEO Abr 2025 — FMI"),
    ("PIB Trimestral MX", "CEPAL",             2025, 1.3,  "base",       "Balance Preliminar CEPAL 2024"),
    ("PIB Trimestral MX", "Banco Mundial",     2025, 1.2,  "base",       "Global Economic Prospects WB ene 2025"),
    ("PIB Trimestral MX", "Citibank",          2025, 0.7,  "pessimista", "LATAM Bear Case Citi 2025"),

    # IPC México
    ("IPC MX (var. anual)", "Banxico Encuesta", 2025, 3.8,  "base", "Encuesta Analistas Banxico feb 2025"),
    ("IPC MX (var. anual)", "IMF WEO",          2025, 3.9,  "base", "WEO Abr 2025 — FMI"),
    ("IPC MX (var. anual)", "IMF WEO",          2026, 3.4,  "base", "WEO Abr 2025 — FMI"),
    ("IPC MX (var. anual)", "BBVA Research",    2025, 3.7,  "base", "Situación México BBVA 2025"),
    ("IPC MX (var. anual)", "Goldman Sachs",    2025, 3.6,  "base", "LATAM Macro Outlook GS 2025"),

    # Tipo de cambio USD/MXN
    ("Tipo de Cambio USD/MXN", "Banxico Encuesta", 2025, 20.5,  "base",       "Encuesta Analistas Banxico feb 2025"),
    ("Tipo de Cambio USD/MXN", "BBVA Research",    2025, 19.8,  "base",       "Situación México BBVA 2025"),
    ("Tipo de Cambio USD/MXN", "Goldman Sachs",    2025, 20.2,  "base",       "LATAM FX Outlook GS 2025"),
    ("Tipo de Cambio USD/MXN", "JPMorgan",         2025, 21.0,  "pessimista", "EM FX Bear Case JPM 2025"),
    ("Tipo de Cambio USD/MXN", "Citibank",         2025, 21.5,  "pessimista", "LATAM FX Bear Case Citi"),

    # Tasa Objetivo Banxico
    ("Tasa Objetivo Banxico", "Banxico Encuesta", 2025, 8.50,  "base", "Encuesta Analistas Banxico feb 2025"),
    ("Tasa Objetivo Banxico", "Goldman Sachs",    2025, 8.25,  "base", "GS Rates Outlook 2025"),
    ("Tasa Objetivo Banxico", "BBVA Research",    2025, 8.75,  "base", "Situación México BBVA 2025"),

    # ── Brasil ────────────────────────────────────────────────────────────────
    # PIB Brasil
    ("PIB Trimestral BR", "Focus BCB (mediana)", 2025, 2.0,  "base",  "Focus Relatório de Mercado fev 2025 — BCB"),
    ("PIB Trimestral BR", "Focus BCB (mediana)", 2026, 1.5,  "base",  "Focus fev 2025 — BCB"),
    ("PIB Trimestral BR", "Goldman Sachs",       2025, 1.8,  "base",  "LATAM Outlook 2025 — GS"),
    ("PIB Trimestral BR", "JPMorgan",            2025, 2.0,  "base",  "EM Outlook 2025 — JPM"),
    ("PIB Trimestral BR", "IMF WEO",             2025, 2.2,  "base",  "WEO Abr 2025 — FMI"),
    ("PIB Trimestral BR", "IMF WEO",             2026, 1.8,  "base",  "WEO Abr 2025 — FMI"),
    ("PIB Trimestral BR", "CEPAL",               2025, 2.1,  "base",  "Balance Preliminar CEPAL 2024"),
    ("PIB Trimestral BR", "Banco Mundial",       2025, 2.0,  "base",  "GEP WB ene 2025"),
    ("PIB Trimestral BR", "BBVA Research",       2025, 2.1,  "base",  "Brazil Economic Watch BBVA 2025"),

    # IPCA Brasil
    ("IPCA BR (var. anual)", "Focus BCB (mediana)", 2025, 4.8,  "base", "Focus fev 2025 — BCB"),
    ("IPCA BR (var. anual)", "Focus BCB (mediana)", 2026, 4.0,  "base", "Focus fev 2025 — BCB"),
    ("IPCA BR (var. anual)", "IMF WEO",             2025, 4.7,  "base", "WEO Abr 2025 — FMI"),
    ("IPCA BR (var. anual)", "IMF WEO",             2026, 3.8,  "base", "WEO Abr 2025 — FMI"),
    ("IPCA BR (var. anual)", "Goldman Sachs",       2025, 4.9,  "base", "Brazil Macro Outlook GS 2025"),
    ("IPCA BR (var. anual)", "JPMorgan",            2025, 4.6,  "base", "Brazil Strategy JPM 2025"),

    # Tasa Selic Brasil
    ("Tasa Selic BR", "Focus BCB (mediana)", 2025, 15.0, "base", "Focus fev 2025 — BCB"),
    ("Tasa Selic BR", "Focus BCB (mediana)", 2026, 12.5, "base", "Focus fev 2025 — BCB"),
    ("Tasa Selic BR", "Goldman Sachs",       2025, 14.5, "base", "GS Brazil Rates 2025"),
    ("Tasa Selic BR", "JPMorgan",            2025, 15.25,"base", "JPM Brazil Rates 2025"),

    # USD/BRL
    ("USD/BRL", "Focus BCB (mediana)", 2025, 5.85, "base",       "Focus fev 2025 — BCB"),
    ("USD/BRL", "Focus BCB (mediana)", 2026, 5.70, "base",       "Focus fev 2025 — BCB"),
    ("USD/BRL", "Goldman Sachs",       2025, 5.90, "pessimista", "GS LATAM FX Bear Case 2025"),
    ("USD/BRL", "JPMorgan",            2025, 5.75, "base",       "JPM EM FX 2025"),
    ("USD/BRL", "BBVA Research",       2025, 5.60, "optimista",  "Brazil Economic Watch BBVA 2025"),

    # ── Ecuador ───────────────────────────────────────────────────────────────
    # PIB Ecuador
    ("PIB Ecuador", "IMF WEO",    2025, 1.3,  "base",       "WEO Abr 2025 — FMI"),
    ("PIB Ecuador", "IMF WEO",    2026, 1.8,  "base",       "WEO Abr 2025 — FMI"),
    ("PIB Ecuador", "CEPAL",      2025, 1.2,  "base",       "Balance Preliminar CEPAL 2024"),
    ("PIB Ecuador", "BCE",        2025, 2.1,  "base",       "Previsiones Macroeconómicas BCE 2025-2028"),
    ("PIB Ecuador", "BCE",        2026, 2.5,  "base",       "Previsiones Macroeconómicas BCE 2025-2028"),
    ("PIB Ecuador", "Banco Mundial",2025,1.5, "base",       "GEP WB ene 2025 — Ecuador"),
    ("PIB Ecuador", "Goldman Sachs",2025,1.0, "pessimista", "LATAM Macro Outlook GS 2025"),

    # IPC Ecuador
    ("IPC Ecuador (var. anual)", "IMF WEO", 2025, 2.1, "base", "WEO Abr 2025 — FMI"),
    ("IPC Ecuador (var. anual)", "IMF WEO", 2026, 1.8, "base", "WEO Abr 2025 — FMI"),
    ("IPC Ecuador (var. anual)", "BCE",     2025, 1.9, "base", "Previsiones BCE 2025"),

    # CDS Ecuador (riesgo país)
    ("CDS Ecuador 5Y", "JP Morgan EMBI",  2025, 1150, "base",       "EMBI Ecuador promedio 2025 — JPM"),
    ("CDS Ecuador 5Y", "Goldman Sachs",   2025, 1300, "pessimista", "Ecuador Risk Outlook GS 2025"),
    ("CDS Ecuador 5Y", "Citibank",        2025, 1050, "optimista",  "Ecuador Outlook Citi 2025"),

    # ── Commodities / Global ─────────────────────────────────────────────────
    # WTI Crude Oil
    ("WTI Crude Oil", "EIA (Energy Info Agency)", 2025, 72.0, "base",       "STEO (Short-Term Energy Outlook) EIA feb 2025"),
    ("WTI Crude Oil", "Goldman Sachs",            2025, 75.0, "base",       "Commodities Outlook 2025 — GS"),
    ("WTI Crude Oil", "JPMorgan",                 2025, 73.0, "base",       "Energy Outlook 2025 — JPM"),
    ("WTI Crude Oil", "IMF (commodity price)",    2025, 70.0, "pessimista", "WEO Commodity Assumptions Abr 2025"),
    ("WTI Crude Oil", "Goldman Sachs",            2025, 85.0, "optimista",  "GS Oil Supply Shock Scenario"),
    ("WTI Crude Oil", "EIA (Energy Info Agency)", 2026, 68.0, "base",       "EIA Long-Term Outlook"),

    # Brent Crude Oil
    ("Brent Crude Oil", "EIA",           2025, 76.0, "base",       "STEO EIA feb 2025"),
    ("Brent Crude Oil", "Goldman Sachs", 2025, 78.0, "base",       "Commodities Outlook GS 2025"),
    ("Brent Crude Oil", "JPMorgan",      2025, 76.0, "base",       "Energy Outlook JPM 2025"),
    ("Brent Crude Oil", "OPEC",          2025, 80.0, "base",       "OPEC World Oil Outlook 2025"),
    ("Brent Crude Oil", "IEA",           2025, 72.0, "pessimista", "IEA World Energy Outlook 2025"),

    # Copper Price
    ("Copper (Cobre) Price", "Goldman Sachs",   2025, 4.50, "base",      "Metals Outlook 2025 GS (USD/lb)"),
    ("Copper (Cobre) Price", "Banco Mundial",   2025, 4.25, "base",      "Commodity Markets Outlook WB 2025"),
    ("Copper (Cobre) Price", "Bloomberg cons.", 2025, 4.35, "base",      "Bloomberg Consensus feb 2025"),
    ("Copper (Cobre) Price", "Goldman Sachs",   2026, 4.80, "optimista", "Energy Transition Demand Scenario GS"),

    # Natural Gas
    ("Henry Hub Natural Gas", "EIA",          2025, 3.20, "base",  "STEO EIA feb 2025 (USD/MMBtu)"),
    ("Henry Hub Natural Gas", "Goldman Sachs",2025, 3.50, "base",  "Natural Gas Outlook GS 2025"),
]

# ── Ejecución principal ───────────────────────────────────────────────────────
def run_hardcoded(session):
    """Carga las proyecciones hardcoded."""
    print("Cargando proyecciones hardcoded de bancos/instituciones...")
    fdate = datetime.now()
    ok = 0
    skipped_vars = set()

    for (name_frag, institution, year, value, scenario, notes) in INSTITUTIONAL_FORECASTS:
        var_id = get_var_id(session, name_frag)
        if not var_id:
            if name_frag not in skipped_vars:
                print(f"  [SKIP] Variable no encontrada: {name_frag!r}")
                skipped_vars.add(name_frag)
            continue

        target_date = datetime(year, 12, 31)
        result = save_consensus_forecast(
            variable_id=var_id,
            source_institution=institution,
            forecast_date=fdate,
            target_date=target_date,
            value=value,
            scenario=scenario,
            notes=notes
        )
        if result:
            ok += 1

    print(f"  Hardcoded: {ok} registros guardados | {len(skipped_vars)} variables no encontradas")


def run_imf_api(session):
    """Carga proyecciones desde IMF DataMapper API."""
    print("\nCargando proyecciones IMF DataMapper API...")
    try:
        from connectors.imf_weo import fetch_all_imf_projections
    except ImportError as e:
        print(f"  [ERROR] No se pudo importar imf_weo: {e}")
        return

    # Mapeo IMF indicator → fragmento del nombre de variable en DB
    IMF_VAR_MAP = {
        "NGDP_RPCH": {
            "COL": "PIB Trimestral CO",
            "MEX": "PIB Trimestral MX",
            "BRA": "PIB Trimestral BR",
            "ECU": "PIB Ecuador",
        },
        "PCPIPCH": {
            "COL": "IPC CO (var. anual)",
            "MEX": "IPC MX (var. anual)",
            "BRA": "IPCA BR (var. anual)",
            "ECU": "IPC Ecuador (var. anual)",
        },
        "LUR": {
            "COL": "Desempleo CO",
            "MEX": "Desempleo MX",
            "BRA": "Desempleo BR",
        },
        "BCA_NGDPD": {
            "COL": "CuentaCorriente",
            "MEX": "CuentaCorriente MX",
            "BRA": "CuentaCorriente BR",
            "ECU": "CuentaCorriente EC",
        },
    }

    projections = fetch_all_imf_projections(
        horizon_years=[datetime.now().year, datetime.now().year + 1, datetime.now().year + 2]
    )

    fdate = datetime.now()
    ok = 0
    for proj in projections:
        indicator = proj['indicator']
        imf_code = proj['imf_code']
        year = proj['year']
        value = proj['value']

        country_var_map = IMF_VAR_MAP.get(indicator, {})
        name_frag = country_var_map.get(imf_code)
        if not name_frag:
            continue

        var_id = get_var_id(session, name_frag)
        if not var_id:
            continue

        result = save_consensus_forecast(
            variable_id=var_id,
            source_institution="IMF WEO (API)",
            forecast_date=fdate,
            target_date=datetime(year, 12, 31),
            value=value,
            scenario="base",
            notes=f"IMF DataMapper: {indicator} — carga automática {fdate.strftime('%Y-%m-%d')}"
        )
        if result:
            ok += 1

    print(f"  IMF API: {ok} proyecciones cargadas")


def run_bcb_focus(session):
    """Carga expectativas desde BCB Focus OData."""
    print("\nCargando expectativas BCB Focus...")
    try:
        from connectors.bcb_focus import fetch_all_focus_expectations
    except ImportError as e:
        print(f"  [ERROR] No se pudo importar bcb_focus: {e}")
        return

    years = [datetime.now().year, datetime.now().year + 1]
    expectations = fetch_all_focus_expectations(years=years)

    fdate = datetime.now()
    ok = 0
    for exp in expectations:
        var_name = exp.get("variable_name")
        year = exp.get("year")
        median = exp.get("median")
        survey_date_str = exp.get("survey_date", "")

        if not var_name or not year or median is None:
            continue

        var_id = get_var_id(session, var_name)
        if not var_id:
            continue

        try:
            forecast_date = datetime.strptime(survey_date_str[:10], "%Y-%m-%d") if survey_date_str else fdate
        except ValueError:
            forecast_date = fdate

        result = save_consensus_forecast(
            variable_id=var_id,
            source_institution="Focus BCB (mediana, API)",
            forecast_date=forecast_date,
            target_date=datetime(year, 12, 31),
            value=median,
            scenario="base",
            notes=f"Focus BCB OData mediana — encuesta {survey_date_str[:10] if survey_date_str else 'N/D'}"
        )
        if result:
            ok += 1

    print(f"  BCB Focus: {ok} expectativas cargadas")


if __name__ == "__main__":
    with SessionLocal() as session:
        run_hardcoded(session)
        run_imf_api(session)
        run_bcb_focus(session)

    print("\nSeed de proyecciones externas completado.")
