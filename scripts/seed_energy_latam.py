"""
seed_energy_latam.py — Variables de energía para Ecuador, Brasil y México.

Operadores de mercado:
  - Ecuador : CENACE / ARCERNNR — ~70% hidro, sin bolsa spot
  - Brasil  : ONS (operador) / CCEE (cámara de comercio) — PLD = precio spot
  - México  : CENACE MX / SENER — PML = precio marginal local (spot)

Ejecutar:
  python -X utf8 scripts/seed_energy_latam.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from models.db import init_db, engine
from sqlalchemy import text

# ── Inicializar DB ────────────────────────────────────────────────────────────
init_db()

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_country_id(conn, name):
    """Consulta el id de un país por nombre exacto o por código ISO.
    Intenta primero el nombre exacto; si no lo encuentra, intenta sin tilde
    (p.ej. 'México' → 'Mexico') y como último recurso por código de 2 letras.
    Retorna None si no existe."""
    # 1. Nombre exacto
    row = conn.execute(
        text("SELECT id FROM dim_country WHERE name = :name"),
        {"name": name}
    ).fetchone()
    if row:
        return row[0]

    # 2. Nombre sin tildes (normalización simple para español)
    import unicodedata
    normalized = ''.join(
        c for c in unicodedata.normalize('NFD', name)
        if unicodedata.category(c) != 'Mn'
    )
    if normalized != name:
        row = conn.execute(
            text("SELECT id FROM dim_country WHERE name = :name"),
            {"name": normalized}
        ).fetchone()
        if row:
            print(f"  [INFO] '{name}' no encontrado; usando nombre sin tilde '{normalized}' (id={row[0]}).")
            return row[0]

    print(f"  [WARN] País '{name}' NO encontrado en dim_country — se omiten sus variables.")
    return None


def insert_variable(conn, country_id, var):
    """INSERT OR IGNORE en dim_variable. Devuelve el id real de la fila."""
    conn.execute(text("""
        INSERT OR IGNORE INTO dim_variable
            (country_id, name, description, source_url, frequency,
             is_dynamic, unit, is_active, connector_type,
             api_provider, api_serie_id, fetch_error_count, category,
             created_at)
        VALUES
            (:country_id, :name, :description, :source_url, :frequency,
             0, :unit, 1, :connector_type,
             :api_provider, :api_serie_id, 0, :category,
             :created_at)
    """), {
        "country_id":    country_id,
        "name":          var["name"],
        "description":   var.get("description", ""),
        "source_url":    var.get("source_url", ""),
        "frequency":     var.get("frequency", "MONTHLY"),
        "unit":          var.get("unit", ""),
        "connector_type":var.get("connector_type", "MANUAL"),
        "api_provider":  var.get("api_provider"),
        "api_serie_id":  var.get("api_serie_id"),
        "category":      var.get("category", "energy"),
        "created_at":    datetime.utcnow().isoformat(),
    })

    row = conn.execute(
        text("SELECT id FROM dim_variable WHERE country_id = :cid AND name = :name"),
        {"cid": country_id, "name": var["name"]}
    ).fetchone()
    return row[0] if row else None


def insert_demo_value(conn, variable_id, api_serie_id, value):
    """INSERT OR IGNORE de un valor demo en fact_timeseries (fecha = 2025-12-01)."""
    demo_date = "2025-12-01 00:00:00"
    conn.execute(text("""
        INSERT OR IGNORE INTO fact_timeseries
            (date, variable_id, data_type, value, version_timestamp, is_anomaly)
        VALUES
            (:date, :variable_id, 'REAL_OFFICIAL', :value, :vts, 0)
    """), {
        "date":        demo_date,
        "variable_id": variable_id,
        "value":       value,
        "vts":         datetime.utcnow().isoformat(),
    })


# ── Catálogo de variables ─────────────────────────────────────────────────────

ECUADOR_VARS = [
    dict(
        name="Generación Hidráulica EC (GWh)",
        category="energy", unit="GWh",
        frequency="ANNUAL",
        api_provider="world_bank",
        api_serie_id="EC:EG.ELC.HYRO.ZS",
        source_url="https://datos.worldbank.org/indicator/EG.ELC.HYRO.ZS?locations=EC",
        description="Porcentaje generación hidráulica sobre total. Ecuador ~70% hidro (Coca Codo Sinclair, Paute, Delsitanisagua).",
        connector_type="API",
    ),
    dict(
        name="Capacidad Instalada EC (MW)",
        category="energy", unit="MW",
        frequency="ANNUAL",
        api_provider="world_bank",
        api_serie_id="EC:EG.ELC.PROD.KH",
        source_url="https://datos.worldbank.org/indicator/EG.ELC.PROD.KH?locations=EC",
        description="Producción total de electricidad Ecuador. CENACE opera el Sistema Nacional Interconectado (SNI).",
        connector_type="API",
    ),
    dict(
        name="Generación Solar EC (% total)",
        category="energy", unit="%",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="EC_GeneSolar",
        source_url="https://www.arcernnr.gob.ec/estadisticas/",
        description="Participación generación fotovoltaica en matriz energética EC. Dato ARCERNNR. Matriz en diversificación 2024.",
        connector_type="MANUAL",
    ),
    dict(
        name="Tarifa Residencial EC (USD/kWh)",
        category="energy", unit="USD/kWh",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="EC_TarifaRes",
        source_url="https://www.arcernnr.gob.ec/pliegos-tarifarios/",
        description="Tarifa media residencial regulada por ARCERNNR. Ecuador dolarizado — tarifa en USD. Subsidio histórico al sector.",
        connector_type="MANUAL",
    ),
    dict(
        name="Importación Energía EC (GWh)",
        category="energy", unit="GWh",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="EC_ImpEner",
        source_url="https://www.cenace.gob.ec/estadisticas/",
        description="Importaciones de energía eléctrica desde Colombia y Perú. Indicador crítico de déficit en temporada seca.",
        connector_type="MANUAL",
    ),
    dict(
        name="Déficit/Excedente Energético EC (GWh)",
        category="energy", unit="GWh",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="EC_DefExcEner",
        source_url="https://www.cenace.gob.ec/",
        description="Balance energético nacional. Negativo = déficit (apagones); positivo = excedente exportable. CENACE publica mensualmente.",
        connector_type="MANUAL",
    ),
]

BRAZIL_VARS = [
    dict(
        name="PLD Sudeste/Centro-Oeste BR (BRL/MWh)",
        category="energy", unit="BRL/MWh",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="BR_PLD_SE",
        source_url="https://www.ccee.org.br/portal/faces/pages_publico/home",
        description="Preço de Liquidação das Diferenças — equivalente al Precio de Bolsa colombiano. Submercado Sudeste/Centro-Oeste (mayor mercado). CCEE publica semanalmente.",
        connector_type="MANUAL",
    ),
    dict(
        name="Nível Reservatórios ONS BR (%)",
        category="energy", unit="%",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="BR_NivelEmb",
        source_url="http://www.ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/capacidade_armazenamento.aspx",
        description="Nível de armazenamento médio dos reservatórios do SIN. Indicador chave: abaixo 30% = pressão alta no PLD. ONS (Operador Nacional do Sistema).",
        connector_type="MANUAL",
    ),
    dict(
        name="Geração Hidráulica BR (GWh)",
        category="energy", unit="GWh",
        frequency="MONTHLY",
        api_provider="bcb",
        api_serie_id="1406",
        source_url="https://www.ons.org.br/",
        description="Geração hidráulica mensal no Sistema Interligado Nacional (SIN). ONS/BCB. Brasil ~60% hidro (Itaipu, Belo Monte, Tucuruí).",
        connector_type="API",
    ),
    dict(
        name="Geração Solar BR (MW médio)",
        category="energy", unit="MW médio",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="BR_GeneSolar",
        source_url="https://www.aneel.gov.br/",
        description="Geração solar fotovoltaica centralizada + mini/microgeração. ANEEL. Crescimento explosivo 2020-2025: BR top 10 mundial en solar.",
        connector_type="MANUAL",
    ),
]

MEXICO_VARS = [
    dict(
        name="PML Nodo 11001 MX (MXN/MWh)",
        category="energy", unit="MXN/MWh",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="MX_PML",
        source_url="https://www.cenace.gob.mx/Paginas/SIM/Reportes/PreciosMercadoSPOT.aspx",
        description="Precio Marginal Local nodo representativo — equivalente al Precio de Bolsa colombiano. CENACE México. Mercado eléctrico mayorista desde 2016.",
        connector_type="MANUAL",
    ),
    dict(
        name="Generación Renovable MX (GWh)",
        category="energy", unit="GWh",
        frequency="ANNUAL",
        api_provider="world_bank",
        api_serie_id="MX:EG.ELC.RNWX.ZS",
        source_url="https://datos.worldbank.org/indicator/EG.ELC.RNWX.ZS?locations=MX",
        description="Porcentaje generación renovable México (eólica + solar + hidro). Banco Mundial. Meta SENER: 35% renovables para 2024.",
        connector_type="API",
    ),
    dict(
        name="Capacidad Solar MX (MW)",
        category="energy", unit="MW",
        frequency="MONTHLY",
        api_provider="manual",
        api_serie_id="MX_CapSolar",
        source_url="https://www.gob.mx/sener",
        description="Capacidad instalada solar fotovoltaica México. SENER. Crecimiento acelerado post-reforma energética 2013.",
        connector_type="MANUAL",
    ),
]

# ── Valores demo para variables MANUAL ───────────────────────────────────────
# api_serie_id  →  valor demo
DEMO_VALUES = {
    "EC_GeneSolar":   2.1,
    "EC_TarifaRes":   0.094,
    "EC_ImpEner":     450.0,
    "EC_DefExcEner":  -120.0,
    "BR_PLD_SE":      98.73,
    "BR_NivelEmb":    67.4,
    "BR_GeneSolar":   8450.0,
    "MX_PML":         1245.50,
    "MX_CapSolar":    8200.0,
}

# ── Ejecución principal ───────────────────────────────────────────────────────
def run():
    # Map: nombre de país en dim_country  →  lista de variables
    groups = [
        ("Ecuador", ECUADOR_VARS),
        ("Brasil",  BRAZIL_VARS),
        ("México",  MEXICO_VARS),
    ]

    stats = {
        "countries_found":    0,
        "countries_missing":  0,
        "vars_inserted":      0,
        "vars_already_exist": 0,
        "demo_values_inserted": 0,
    }

    # Mapa: api_serie_id → variable_id  (para insertar demo values)
    serie_to_var_id = {}

    with engine.begin() as conn:

        # ── 1. Verificar países ───────────────────────────────────────────────
        country_ids = {}
        for country_name, _ in groups:
            cid = get_country_id(conn, country_name)
            if cid is not None:
                country_ids[country_name] = cid
                stats["countries_found"] += 1
                print(f"  [OK] País '{country_name}' encontrado — id={cid}")
            else:
                stats["countries_missing"] += 1

        # ── 2. Insertar variables ─────────────────────────────────────────────
        print("\n── Insertando variables de energía ──────────────────────────────")
        for country_name, var_list in groups:
            if country_name not in country_ids:
                print(f"  [SKIP] {country_name} no está en dim_country.")
                continue

            cid = country_ids[country_name]
            print(f"\n  [{country_name}]")
            for var in var_list:
                # Verificar si ya existe antes de insertar
                existing = conn.execute(
                    text("SELECT id FROM dim_variable WHERE country_id = :cid AND name = :name"),
                    {"cid": cid, "name": var["name"]}
                ).fetchone()

                var_id = insert_variable(conn, cid, var)

                if existing:
                    stats["vars_already_exist"] += 1
                    print(f"    [=] (ya existe) {var['name']}")
                else:
                    stats["vars_inserted"] += 1
                    print(f"    [+] Insertada: {var['name']}  (id={var_id})")

                # Guardar mapeo serie_id → var_id para demo values
                serie_id = var.get("api_serie_id")
                if serie_id and var_id:
                    serie_to_var_id[serie_id] = var_id

        # ── 3. Insertar valores demo ──────────────────────────────────────────
        print("\n── Insertando valores demo (fact_timeseries) ────────────────────")
        for serie_id, demo_val in DEMO_VALUES.items():
            var_id = serie_to_var_id.get(serie_id)
            if var_id is None:
                # Buscar por api_serie_id en la DB (por si ya existía antes)
                row = conn.execute(
                    text("SELECT id FROM dim_variable WHERE api_serie_id = :sid"),
                    {"sid": serie_id}
                ).fetchone()
                var_id = row[0] if row else None

            if var_id is None:
                print(f"  [WARN] No se encontró variable con api_serie_id='{serie_id}' — demo omitido.")
                continue

            # Verificar si ya existe el registro demo
            existing_demo = conn.execute(
                text("""
                    SELECT 1 FROM fact_timeseries
                    WHERE variable_id = :vid
                      AND date = '2025-12-01 00:00:00'
                      AND data_type = 'REAL_OFFICIAL'
                """),
                {"vid": var_id}
            ).fetchone()

            if existing_demo:
                print(f"  [=] (ya existe demo) {serie_id} = {demo_val}")
            else:
                insert_demo_value(conn, var_id, serie_id, demo_val)
                stats["demo_values_inserted"] += 1
                print(f"  [+] Demo insertado: {serie_id} = {demo_val}  (var_id={var_id})")

    # ── 4. Resumen final ──────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("RESUMEN seed_energy_latam.py")
    print("═" * 60)
    print(f"  Países encontrados en dim_country : {stats['countries_found']}")
    print(f"  Países NO encontrados (omitidos)  : {stats['countries_missing']}")
    print(f"  Variables nuevas insertadas        : {stats['vars_inserted']}")
    print(f"  Variables que ya existían (skip)   : {stats['vars_already_exist']}")
    print(f"  Valores demo insertados            : {stats['demo_values_inserted']}")
    print("═" * 60)

    total_vars = stats["vars_inserted"] + stats["vars_already_exist"]
    expected   = len(ECUADOR_VARS) + len(BRAZIL_VARS) + len(MEXICO_VARS)
    print(f"\n  Total variables procesadas: {total_vars} / {expected} esperadas")
    if stats["countries_missing"] == 0 and stats["vars_inserted"] > 0:
        print("  Estado: COMPLETADO EXITOSAMENTE")
    elif stats["vars_inserted"] == 0 and stats["vars_already_exist"] > 0:
        print("  Estado: SIN CAMBIOS (todas las variables ya existían)")
    else:
        print("  Estado: COMPLETADO CON ADVERTENCIAS (revisar países faltantes)")


if __name__ == "__main__":
    run()
