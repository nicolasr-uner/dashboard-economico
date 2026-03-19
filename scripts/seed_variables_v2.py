"""
seed_variables_v2.py — Actualiza variables existentes y agrega nuevas (v2).
Ejecutar: python scripts/seed_variables_v2.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db import init_db, SessionLocal
from models.schema import Country, MacroVariable

# ── Ejecutar migraciones primero ──────────────────────────────────────────────
init_db()

session = SessionLocal()

# ── Mapa de países ────────────────────────────────────────────────────────────
def get_or_create_country(session, name, code, flag):
    c = session.query(Country).filter_by(code=code).first()
    if not c:
        c = Country(name=name, code=code, flag_emoji=flag)
        session.add(c)
        session.flush()
    return c

colombia = get_or_create_country(session, "Colombia", "CO", "🇨🇴")
mexico   = get_or_create_country(session, "México",   "MX", "🇲🇽")
brasil   = get_or_create_country(session, "Brasil",   "BR", "🇧🇷")
ecuador  = get_or_create_country(session, "Ecuador",  "EC", "🇪🇨")
global_c = get_or_create_country(session, "Global",   "WW", "🌐")

session.commit()
print("Países OK.")

# ── Actualizar variables existentes con api_provider / api_serie_id ───────────
EXISTING_UPDATES = [
    # (name_fragment, api_provider, api_serie_id, connector_type, category)
    ("IPC",            "banrep",  "IPC_variacion_anual",  "API", "macro"),   # CO IPC
    ("TRM",            "banrep",  "TRM",                  "API", "macro"),
    ("Precio Energia", "xm",      "PrecBolNac",           "API", "energy"),
    ("Desempleo",      "banrep",  "Desempleo",            "API", "macro"),   # CO
    ("IPC Mex",        "banxico", "SP68257",              "API", "macro"),
    ("USD/MXN",        "banxico", "SF43718",              "API", "macro"),
    ("Desempleo MX",   "banxico", "SR16734",              "API", "macro"),
    ("IPC Ecu",        "fred",    "FPCPITOTLZGECU",       "API", "macro"),
    ("Petroleo",       "fred",    "DCOILWTICO",           "API", "energy"),
    ("IPCA",           "bcb",     "433",                  "API", "macro"),
    ("USD/BRL",        "bcb",     "1",                    "API", "macro"),
]

all_vars = session.query(MacroVariable).all()
for frag, prov, serie, ctype, cat in EXISTING_UPDATES:
    for v in all_vars:
        if frag.lower() in v.name.lower():
            v.api_provider = prov
            v.api_serie_id = serie
            v.connector_type = ctype
            v.category = cat
            print(f"  [UPDATE] {v.name} → {prov}/{serie}")
            break

session.commit()

# ── Variables nuevas ──────────────────────────────────────────────────────────
NEW_VARIABLES = [
    # ── Colombia — Energéticas (XM) ───────────────────────────────────────
    dict(country=colombia, name="Precio de Bolsa Nacional (XM)", unit="COP/kWh",
         frequency="daily", api_provider="xm", api_serie_id="PrecBolNac",
         connector_type="API", category="energy",
         source_url="https://servapibi.xm.com.co/daily",
         description="Precio promedio de bolsa del sistema eléctrico colombiano"),
    dict(country=colombia, name="Índice Mc (Precio contratos regulado)", unit="COP/kWh",
         frequency="monthly", api_provider="xm", api_serie_id="PrecPromContReg",
         connector_type="MANUAL", category="energy",
         source_url="https://sinergox.xm.com.co",
         description="Precio promedio contratos regulados (Índice Mc)"),
    dict(country=colombia, name="Precio de Escasez", unit="COP/kWh",
         frequency="monthly", api_provider="xm", api_serie_id="PrecEscworking",
         connector_type="MANUAL", category="energy",
         source_url="https://sinergox.xm.com.co",
         description="Precio de escasez del sistema eléctrico colombiano"),
    dict(country=colombia, name="Demanda Energía Nacional", unit="GWh",
         frequency="daily", api_provider="xm", api_serie_id="DemaNal",
         connector_type="API", category="energy",
         source_url="https://servapibi.xm.com.co/daily",
         description="Demanda real de energía eléctrica nacional"),
    dict(country=colombia, name="Generación Solar", unit="GWh",
         frequency="daily", api_provider="xm", api_serie_id="GeneReal",
         connector_type="API", category="energy",
         source_url="https://servapibi.xm.com.co/daily",
         description="Generación real solar en el sistema eléctrico colombiano"),
    dict(country=colombia, name="CERE (Cargo por Confiabilidad)", unit="COP/kWh",
         frequency="monthly", api_provider="xm", api_serie_id="CERE",
         connector_type="MANUAL", category="energy",
         source_url="https://sinergox.xm.com.co",
         description="Cargo por Confiabilidad del sistema eléctrico colombiano"),
    dict(country=colombia, name="Aportes Hídricos (% media histórica)", unit="%",
         frequency="daily", api_provider="xm", api_serie_id="AporEner",
         connector_type="API", category="energy",
         source_url="https://servapibi.xm.com.co/daily",
         description="Aportes hídricos como porcentaje de la media histórica"),

    # ── Colombia — Financieras (BanRep) ───────────────────────────────────
    dict(country=colombia, name="Tasa de Intervención BanRep", unit="%",
         frequency="daily", api_provider="banrep", api_serie_id="TasIntPol",
         connector_type="API", category="macro",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa de política monetaria del Banco de la República"),
    dict(country=colombia, name="IBR Overnight", unit="%",
         frequency="daily", api_provider="banrep", api_serie_id="IBR_ON",
         connector_type="API", category="macro",
         source_url="https://suameca.banrep.gov.co",
         description="Indicador Bancario de Referencia - plazo overnight"),
    dict(country=colombia, name="DTF E.A.", unit="%",
         frequency="weekly", api_provider="banrep", api_serie_id="DTF",
         connector_type="API", category="macro",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa depósitos a término fijo efectiva anual"),
    dict(country=colombia, name="PIB Trimestral CO (crecimiento anual)", unit="%",
         frequency="quarterly", api_provider="banrep", api_serie_id="PIB_trim",
         connector_type="API", category="macro",
         source_url="https://suameca.banrep.gov.co",
         description="Variación anual del PIB trimestral de Colombia"),

    # ── México — Financieras (Banxico) ────────────────────────────────────
    dict(country=mexico, name="Tasa Objetivo Banxico", unit="%",
         frequency="daily", api_provider="banxico", api_serie_id="SF61745",
         connector_type="API", category="macro",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tasa de fondeo interbancario objetivo (Banxico)"),
    dict(country=mexico, name="TIIE 28 días", unit="%",
         frequency="daily", api_provider="banxico", api_serie_id="SF43783",
         connector_type="API", category="macro",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tasa de interés interbancaria de equilibrio a 28 días"),

    # ── Brasil — Financieras (BCB) ────────────────────────────────────────
    dict(country=brasil, name="Tasa Selic", unit="%",
         frequency="daily", api_provider="bcb", api_serie_id="432",
         connector_type="API", category="macro",
         source_url="https://api.bcb.gov.br",
         description="Tasa Selic diaria - política monetaria del Banco Central do Brasil"),
    dict(country=brasil, name="PIB Trimestral BR (var %)", unit="%",
         frequency="quarterly", api_provider="bcb", api_serie_id="22099",
         connector_type="API", category="macro",
         source_url="https://api.bcb.gov.br",
         description="Variación trimestral del PIB de Brasil (BCB)"),

    # ── Globales — Commodities Energéticos (FRED) ─────────────────────────
    dict(country=global_c, name="WTI Crude Oil", unit="USD/barrel",
         frequency="daily", api_provider="fred", api_serie_id="DCOILWTICO",
         connector_type="API", category="energy",
         source_url="https://fred.stlouisfed.org",
         description="Precio del petróleo crudo WTI (West Texas Intermediate)"),
    dict(country=global_c, name="Henry Hub Natural Gas", unit="USD/MMBtu",
         frequency="daily", api_provider="fred", api_serie_id="DHHNGSP",
         connector_type="API", category="energy",
         source_url="https://fred.stlouisfed.org",
         description="Precio del gas natural en Henry Hub"),
]

added = 0
skipped = 0
for v_data in NEW_VARIABLES:
    country = v_data.pop('country')
    existing = session.query(MacroVariable).filter_by(
        country_id=country.id, name=v_data['name']
    ).first()
    if existing:
        # Actualizar campos nuevos si está incompleto
        for k, val in v_data.items():
            setattr(existing, k, val)
        skipped += 1
        print(f"  [SKIP] {v_data['name']} ya existe — actualizado.")
    else:
        mv = MacroVariable(country_id=country.id, **v_data)
        session.add(mv)
        added += 1
        print(f"  [ADD]  {v_data['name']}")

session.commit()
session.close()

print(f"\n✅ Seed completado: {added} variables nuevas, {skipped} actualizadas.")
print("Verificando total...")

# Verificación
from models.db import engine
from sqlalchemy import text
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM dim_variable"))
    total = result.fetchone()[0]
    print(f"Total variables en dim_variable: {total}")
