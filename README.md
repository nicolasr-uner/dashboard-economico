# Cerebro Economico NLA

Plataforma de inteligencia macroeconomica multi-pais para Colombia, Mexico, Brasil y Ecuador. Integra datos oficiales de bancos centrales, mercados energeticos y organismos internacionales en un dashboard interactivo con modelos de proyeccion estadistica.

## Arquitectura

```
streamlit_app.py          UI principal (Streamlit, 7 tabs)
connectors/               Adaptadores de fuentes de datos
  banrep.py               Banco de la Republica (CO) + World Bank fallback
  bcb.py                  Banco Central do Brasil
  banxico.py              Banco de Mexico (requiere BANXICO_TOKEN)
  fred.py                 Federal Reserve Economic Data (requiere FRED_API_KEY)
  xm_energy.py            Mercado electrico mayorista CO (XM/SIMEM)
  world_bank.py           World Bank Open Data
  base.py                 Clase base: retry, rate-limiting, timeout
  registry.py             Factory de conectores
data/
  agent.py                VariableAgent: ingesta + proyeccion
  database.py             Queries SQLAlchemy
  consensus.py            Forecasts de consenso (instituciones/analistas)
  staging/                Datos crudos pre-validacion
  exports/                CSVs exportados por usuarios
models/
  schema.py               ORM: dim_country, dim_variable, fact_timeseries,
                           dim_consensus_forecast, ai_analysis_log
  db.py                   Engine SQLAlchemy (SQLite local / PostgreSQL+TimescaleDB)
projections/
  models.py               Holt-Winters, ARIMA(1,1,1), Ensemble con IC 80%/95%
scripts/
  seed_variables_v3.py    Carga definiciones de 132 variables
  backfill.py             Backfill historico desde APIs
  validate_data.py        Validacion: missing, outliers (IQR), gaps temporales
  seed_consensus.py       Forecasts de consenso de analistas
  read_excel_models.py    Ingestor de modelos financieros Excel
config/
  data_catalog.yaml       Inventario maestro: variables, fuentes, gaps, unidades
docs/
  audit_report.md         Auditoria completa del data lake
  validation_report.md    Reporte automatico de validacion de datos
```

## Data Lineage

### Flujo de Ingesta

```
Fuente Externa          Conector              DB (fact_timeseries)         UI
─────────────          ─────────              ────────────────────         ──
BanRep API ──┐
World Bank ──┤──> banrep.py ──────────────┐
             │                            │
BCB API ─────┤──> bcb.py ───────────────┐ │
             │                          │ │
Banxico API ─┤──> banxico.py ─────────┐ │ │
             │                        │ │ │
FRED API ────┤──> fred.py ──────────┐ │ │ │
             │                      ▼ ▼ ▼ ▼
XM API ──────┤──> xm_energy.py ──> VariableAgent ──> fact_timeseries ──> Streamlit
             │                      (data/agent.py)  (data_type:           (tabs 1-7)
Web Pages ───┤──> scraper/engine     │                REAL_OFFICIAL)
             │                       │
Excel Files ─┘──> read_excel_models  │
                                     ▼
                              forecast_ensemble ──> fact_timeseries
                              (projections/         (data_type:
                               models.py)            PROJECTION)
```

### Tipos de Datos en fact_timeseries

| data_type | Descripcion | Fuente |
|-----------|-------------|--------|
| `REAL_OFFICIAL` | Datos oficiales de bancos centrales y agencias | APIs + scrapers |
| `PROJECTION` | Proyecciones del modelo estadistico | Holt-Winters/ARIMA |
| `ESTIMATION` | Estimaciones de modelos financieros | Excel + manual |

### Frecuencia de Actualizacion

| Fuente | Frecuencia | Variables |
|--------|------------|-----------|
| BCB (Brasil) | Diaria | Selic, USD/BRL, CDI |
| XM (Colombia) | Diaria | Precio Bolsa, Demanda, Generacion |
| FRED | Diaria | WTI, Brent, Treasuries, FX |
| Banxico | Diaria | TIIE, USD/MXN, Tasa Objetivo |
| BanRep/WB | Mensual-Anual | IPC, PIB, Desempleo CO |
| Consensos | Mensual | Analistas institucionales |

## Setup Rapido

```bash
pip install -r requirements.txt
python -c "from models.db import init_db; init_db()"
python scripts/seed_variables_v4.py
streamlit run streamlit_app.py
```

> Para la guia completa de instalacion, API keys y cobertura mundial, ver **[SETUP.md](SETUP.md)**.

El dashboard funciona sin API keys (datos de Brasil via BCB y Colombia energetico via XM). Para activar mercados globales y Mexico, agregar las keys en `.streamlit/secrets.toml` (ver `SETUP.md` seccion 3).

## Validacion de Datos

El script `scripts/validate_data.py` ejecuta las siguientes verificaciones:

| Check | Descripcion | Umbral |
|-------|-------------|--------|
| Missing values | NaN/null en columna value | 0% esperado |
| Outliers | IQR x3 (conservador) | Reporta, no elimina |
| Gaps temporales | Huecos segun frecuencia declarada | daily: 3d, monthly: 45d |
| Frescura | Tiempo desde ultimo dato | daily: 7d, monthly: 60d |
| Cobertura | % de variables con datos | Objetivo: >80% |

```bash
# Reporte por consola
python scripts/validate_data.py

# Generar Markdown
python scripts/validate_data.py --output md
# → docs/validation_report.md
```

## Estado del Proyecto

**Cobertura real (medida 2026-06-24):** **~92 %** — 208 de 225 variables con datos, 79.624 registros en `fact_timeseries`. La base de datos está madura; el reto es **operativo**: la frescura depende de que el pipeline de ingesta corra a diario (ver `.github/workflows/data_ingestion.yml`).

| Fuente | Estado | Key? | Variables |
|--------|--------|------|-----------|
| BCB Brasil | ✅ Funcional | No | Selic, IPCA, USD/BRL, CDI, Desempleo |
| XM Colombia | ✅ Funcional | No | Precio de Bolsa, Aportes Hídricos |
| World Bank | ✅ Funcional | No | PIB, Inflacion, Desempleo (anual, 190 paises) |
| FRED | ⚡ Requiere key | Si | WTI, Brent, Gold, S&P500, VIX, Fed Funds, etc. |
| Banxico | ⚡ Requiere key | Si | Tasa objetivo, TIIE, USD/MXN, IPC MX |
| BanRep Colombia | ⚠️ Deprecado 2025 | No | Redirige a World Bank automaticamente |

> **Entrypoint canónico:** `streamlit_app.py` (monolito, 100 % funcional). La migración multipágina está aparcada en `pages_wip/` (incompleta).

Ver **[docs/AUDIT_cerebro-economico-nla_2026-06-24.md](docs/AUDIT_cerebro-economico-nla_2026-06-24.md)** para el diagnóstico actual y el plan de acción. (`docs/audit_report.md` y `docs/validation_report.md` son de abril 2026 y están **obsoletos**.)
