# Auditoría Técnica y Master Action Plan — Cerebro Económico NLA

**Fecha:** 2026-06-24 · **Auditor:** Claude (Opus 4.8) · **Alcance de la auditoría:** solo lectura
**Repo:** github.com/nicolasr-uner/dashboard-economico · **Clasificación:** Tipo 1 (dashboard público liviano) con auditoría de seguridad reforzada (matices Tipo 2)

> Nota metodológica: los Pasos 0-3 (identificación, reconocimiento, código, calidad/seguridad) se ejecutaron en modo solo lectura. La cobertura de datos se **midió en vivo** sobre `db.sqlite3` el 2026-06-24 (consulta `SELECT` de solo lectura), no se estimó del informe obsoleto de abril.

---

## 1. Resumen Ejecutivo

**Qué es (según lo encontrado):** plataforma de inteligencia macroeconómica multi-país (Colombia, México, Brasil, Ecuador + Global) construida sobre **Streamlit** (UI) + **SQLAlchemy** (ORM) + conectores `httpx`/BeautifulSoup a bancos centrales y organismos (BCB, FRED, Banxico, BanRep, XM, World Bank, BCE, IMF) + modelos `statsmodels` (Holt-Winters / ARIMA / Ensemble). Convive un andamiaje **Django legacy** (apps `core`/`scraper`/`ai_engine`) que **está muerto y aislado** (ningún archivo del stack vivo lo importa). **Tipo 1 + matices Tipo 2.**

**Semáforo global: 🟡 (avanzando pero estancado operativamente).**
El producto central (datos + dashboard) está **mucho más maduro de lo que decía la documentación**, pero hay tres frenos para producción: datos sin refrescar 63 días, cero pruebas, y deuda legacy + despliegue sin confirmar.

**Completitud estimada: ~70 % (rango 65-75 %).**
*Metodología (ponderada por componente; el % global pondera madurez de cada bloque, densidad de TODOs, cobertura de tests, estado de deploy y si corre):*

| Componente | Estado | Madurez |
|---|---|---|
| Ingesta de datos / cobertura | 92,4 % cobertura (208/225 vars con datos, 79.624 filas) **PERO frescura nula desde 2026-04-22** | ~70 % efectivo |
| Conectores | 9/9 implementados (verificado: `bce.py` e `imf_weo.py` NO son stubs) | ~90 % |
| UI dashboard | Monolito `streamlit_app.py` 100 % funcional; migración multipágina al 40 % (2/5 páginas reales, 3 stubs) | ~70 % |
| Proyecciones estadísticas | Implementadas; se calculan en vivo (0 filas `PROJECTION` persistidas) | ~80 % |
| Capa IA (chatbot/analyzer) | Implementada pero infrautilizada (`ai_analysis_log` = 1 fila) | ~50 % |
| Pruebas automatizadas | 0 reales | 0 % |
| Linters / formatters | Ninguno configurado | 0 % |
| CI/CD + despliegue | Backfill diario existe pero **caído**; deploy a Streamlit Cloud no confirmado ni automatizado | ~40 % |
| Seguridad / repo hygiene | Secrets bien aislados, pero `db.sqlite3` + `memoria_chat.txt` commiteados y credenciales hardcodeadas en `docker-compose.yml` | ~60 % |
| Limpieza arquitectónica | Stack Django legacy aún presente; doble-ORM | ~30 % |

**Qué falta esencialmente para producción (una frase):** reparar el pipeline de ingesta (los datos llevan 63 días sin actualizarse), confirmar y automatizar el despliegue (Streamlit Cloud), y eliminar la deuda legacy (Django + higiene de repo + un mínimo de pruebas y linting).

---

## 2. Auditoría Técnica

### 2.1 Stack y Arquitectura (real)

- **Vivo:** `streamlit_app.py` (~1.389 líneas, 87 KB) + estructura multipágina `🏠_Home.py` + `pages/` (duplicación de entrypoint). Datos vía `data/database.py` → `models/schema.py` (SQLAlchemy) → `db.sqlite3` (local) o PostgreSQL/Supabase (`DATABASE_URL`). Ingesta orquestada por `data/agent.py` (`VariableAgent`) usando `connectors/registry.py` → `connectors/*.py`.
- **Legacy/muerto (aislado):** `manage.py`, `config/` (Django settings/urls/wsgi/asgi), apps `core`/`scraper`/`ai_engine`, `templates/`, `static/`, y orquestación Celery/Redis en `docker-compose.yml` + `scheduler/`. **Verificado:** ningún archivo del stack Streamlit importa Django.
- **Entrypoints:** ambigüedad real entre `streamlit_app.py` (monolito) y `🏠_Home.py` (multipágina). Hay que fijar el canónico para Streamlit Cloud.

### 2.2 Estado de Calidad (verificado)

- **Pruebas:** **0 reales.** `core/scraper/ai_engine/tests.py` son stubs de `startapp`; `test_apis_temp.py` es ad-hoc (gitignored). Sin `pytest.ini`, `.coveragerc`, ni `[tool.pytest]`.
- **Linters/formatters:** **ninguno** (sin `ruff`/`black`/`flake8`/`isort`/`pre-commit`/`.editorconfig`).
- **Build/run:** se ejecuta con `streamlit run streamlit_app.py`. `Dockerfile` sin `CMD`/`EXPOSE`. `Procfile` (Heroku) y `docker-compose.yml` apuntan a Django/Celery → **rotos/abandonados** (ver 2.4).

### 2.3 Estado de Datos (medido en vivo, 2026-06-24)

| Indicador | Valor |
|---|---|
| Variables definidas / activas | 225 / 225 |
| Variables con datos | **208 (92,4 %)** |
| Filas `fact_timeseries` | 79.624 (`REAL_OFFICIAL` 78.258 · `ESTIMATION` 1.366 · `PROJECTION` 0) |
| **Frescura (máx. fecha `REAL_OFFICIAL`)** | **2026-04-22 → 63 días de retraso** 🔴 |
| Cobertura por país | CO 96/99 · BR 36/37 · MX 28/33 · EC 24/31 · Global 24/25 |
| `dim_consensus_forecast` | 158 |
| `project_financials` | **0** (ingestor Excel no pobló la DB) |
| `ai_analysis_log` | 1 (IA casi sin uso) |

> El `docs/audit_report.md` (abril) reportaba ~13 % de cobertura: está **obsoleto** y subestima masivamente el proyecto.

### 2.4 Deuda Técnica

- **Doble-ORM sobre las mismas entidades** (Django `core/models.py` vs SQLAlchemy `models/schema.py`); `db.sqlite3` contiene ambas familias de tablas (`core_*`/`auth_*`/`django_*` y `dim_*`/`fact_timeseries`).
- **Stack Django legacy completo** sin servir (apps, urls, views, templates, static, manage.py).
- **Migración multipágina a medias:** 3/5 páginas son stubs "en construcción".
- **`requirements.txt` incompleto para los paths legacy:** sin Django/DRF/Celery/Redis → confirma que Heroku/Docker no son el target real. Versiones laxas (`>=`), sin lockfile.
- **`imf_weo.py` con interfaz divergente:** funciones a nivel de módulo (no hereda `BaseConnector`, sin `fetch_series`) → revisar integración con `registry.py`.
- **103 variables con `api_provider = NULL`** (100 con datos): etiquetado de procedencia incompleto.

### 2.5 Seguridad (críticos marcados)

| # | Hallazgo | Sev. | Evidencia |
|---|---|---|---|
| S1 | `db.sqlite3` (9,9 MB) **commiteado** y NO cubierto por `.gitignore` (solo ignora `-wal`/`-shm`) | 🔴 | `git ls-files` |
| S2 | `memoria_chat.txt` (54 KB, log de chat de desarrollo) **commiteado** | 🟡 | `git ls-files` |
| S3 | `docker-compose.yml` **commiteado con credenciales hardcodeadas** (`dashboard_user`/`dashboard_password`) | 🔴 | `docker-compose.yml` |
| S4 | `DATABASE_URL` de Supabase con contraseña embebida en `.env`/`secrets.toml` (gitignored, **no** en git) → rotar por precaución | ⚠️ | `.env`, `.streamlit/secrets.toml` |
| S5 | `.streamlit/config.toml`: `enableXsrfProtection=false`, `enableCORS=false` | 🟡 | `.streamlit/config.toml` |
| S6 | `.env`: `SECRET_KEY="django-insecure-..."`, `DEBUG=True` (solo afecta Django legacy) | 🟡 | `.env` |
| — | ✅ `.env`, `secrets.toml`, `*.xlsx` (Exagon/Ruitoque) NO trackeados; sin API keys hardcodeadas en `.py` | 🟢 | verificado |

### 2.6 Inventario de Pendientes (TODOs)

Solo **3 marcadores** en todo el código (excl. `venv`), los 3 textos de UI en `streamlit_app.py` (L463, L867, L1200) — no es deuda de código. Codebase limpio de marcadores `TODO`/`FIXME`/`HACK`/`XXX`.

### 2.7 Features Incompletas

| Ítem | Ubicación | Estado |
|---|---|---|
| Página Contexto Global | `pages/3_🌍_Contexto_Global.py` | Stub "en construcción" |
| Página Energía | `pages/4_⚡_Energia.py` | Stub "en construcción" |
| Página Data Hub | `pages/5_📚_Data_Hub.py` | Stub "en construcción" |
| Finanzas corporativas | `scripts/read_excel_models.py` → `project_financials` | Ingestor construido, DB vacía (0 filas) |
| Persistencia de proyecciones | `projections/models.py` | 0 filas `PROJECTION`; se calculan en vivo |
| Análisis IA de anomalías | `ai_engine/analyzer.py` | Implementado, 1 sola ejecución registrada |

---

## 3. Master Action Plan — Ruta Crítica hacia Producción

> MoSCoW: **M**ust / **S**hould / **C**ould / **W**on't. Esfuerzo: S(≤2 h) · M(½-1 día) · L(2-3 días).
> Cada tarea está redactada para convertirse en un prompt ONESHOT ejecutable.

### Fase 1 — Bloqueantes (correr/desplegar limpio)

| ID | Tarea | Archivo/Módulo | MoSCoW | Esf. | Depende |
|---|---|---|---|---|---|
| B1 | Diagnosticar por qué la ingesta se detuvo el 2026-04-22: revisar runs de GitHub Actions y correr `python scripts/backfill.py --days 7` para ver si las APIs responden y dónde escribe (sqlite local vs Supabase) | `.github/workflows/data_ingestion.yml`, `scripts/backfill.py`, `models/db.py` | Must | M | — |
| B2 | Reparar el pipeline: asegurar que el cron escribe en la DB de producción (Supabase) con los secrets correctos y vuelve a refrescar a diario | `.github/workflows/data_ingestion.yml`, `data/agent.py` | Must | M | B1 |
| B3 | Fijar el entrypoint canónico (decidir monolito `streamlit_app.py` vs multipágina `🏠_Home.py`) y declararlo para Streamlit Cloud | `streamlit_app.py`, `🏠_Home.py`, `.streamlit/` | Must | S | — |

### Fase 2 — Features Faltantes (mínimo de alcance)

| ID | Tarea | Archivo/Módulo | MoSCoW | Esf. | Depende |
|---|---|---|---|---|---|
| F1 | Resolver las 3 páginas stub: completarlas con la lógica del monolito **o** eliminarlas si se mantiene el monolito | `pages/3_*.py`, `pages/4_*.py`, `pages/5_*.py` | Must | M | B3 |
| F2 | Poblar `project_financials` ejecutando el ingestor de Excel (si el tab de finanzas corporativas es alcance), o desactivar el tab | `scripts/read_excel_models.py`, `models/schema.py` | Should | M | — |
| F3 | Integrar `imf_weo.py` al patrón de `registry.py` (adaptar a `BaseConnector`/`fetch_series`) o documentar su uso aparte | `connectors/imf_weo.py`, `connectors/registry.py` | Could | M | — |
| F4 | Cerrar brechas por proveedor: 5 vars Banxico (token) y 4 vars Ecuador sin datos | `connectors/banxico.py`, `connectors/bce.py` | Should | M | B2 |

### Fase 3 — Refactorización y Pruebas

| ID | Tarea | Archivo/Módulo | MoSCoW | Esf. | Depende |
|---|---|---|---|---|---|
| R1 | Eliminar el stack Django legacy (apps `core`/`scraper`/`ai_engine`, `config/` Django, `manage.py`, `templates/`, `static/`, `scheduler/`) tras rescatar lo útil | árbol Django | Should | L | B3 |
| R2 | Consolidar a un único ORM (SQLAlchemy) e introducir migraciones formales (Alembic) | `models/`, nueva carpeta `migrations/` | Should | L | R1 |
| R3 | Añadir suite mínima de tests (pytest): parsing de conectores, queries de `data/database.py`, `forecast_ensemble` | nuevo `tests/`, `pytest.ini` | Must | M | — |
| R4 | Limpiar `api_provider = NULL` en las 103 variables (etiquetar procedencia) | `scripts/seed_variables_v4.py`, `db.sqlite3` | Could | S | — |

### Fase 4 — Seguridad y Hardening

| ID | Tarea | Archivo/Módulo | MoSCoW | Esf. | Depende |
|---|---|---|---|---|---|
| H1 | Añadir a `.gitignore`: `db.sqlite3`, `*.sqlite3`, `memoria_chat.txt`; `git rm --cached` de los tres | `.gitignore` | Must | S | — |
| H2 | (Opcional) Purgar `db.sqlite3`/`memoria_chat.txt` del historial con `git filter-repo`/BFG si se considera sensible | repo | Could | M | H1 |
| H3 | Parametrizar credenciales de `docker-compose.yml` con `${VAR}` o eliminar el archivo si Docker/Celery queda descartado | `docker-compose.yml` | Must | S | R1 |
| H4 | Rotar la credencial de Supabase (precaución) y confirmar que prod lee de `st.secrets`/env | Supabase, `.streamlit/secrets.toml` | Should | S | — |
| H5 | Endurecer `.streamlit/config.toml` (`enableXsrfProtection=true`) si el dashboard será público | `.streamlit/config.toml` | Should | S | B3 |
| H6 | Sincronizar `.env.example` con las vars reales (o eliminar las de Django si R1 se ejecuta) | `.env.example` | Should | S | R1 |

### Fase 5 — Despliegue / Lanzamiento

| ID | Tarea | Archivo/Módulo | MoSCoW | Esf. | Depende |
|---|---|---|---|---|---|
| D1 | Confirmar Streamlit Cloud como target, configurar secrets en el panel y alinear `requirements.txt` al stack vivo | `requirements.txt`, Streamlit Cloud | Must | S | B3, F1 |
| D2 | Ampliar CI: `ruff` + `pytest` + `pip-audit` en PR, y **notificación si el backfill falla** (se cayó silenciosamente en abril) | `.github/workflows/` | Should | M | R3 |
| D3 | Fijar dependencias / lockfile (`pip freeze` → `requirements.lock` o pin de versiones) | `requirements.txt` | Should | S | — |
| D4 | Actualizar documentación obsoleta (`docs/audit_report.md`, README "Estado del Proyecto") con la cobertura real | `docs/`, `README.md` | Could | S | — |

---

## 4. Definition of Done

- [ ] El cron de ingesta corre a diario y la frescura de `REAL_OFFICIAL` es ≤ 7 días.
- [ ] Un único entrypoint Streamlit, desplegado y accesible por URL (Streamlit Cloud) con secrets en el panel.
- [ ] No quedan páginas "en construcción": las 5 funcionan o se consolidan en el monolito.
- [ ] Stack Django legacy eliminado (o explícitamente conservado y documentado); un solo ORM.
- [ ] `db.sqlite3` y `memoria_chat.txt` fuera del repo; sin credenciales hardcodeadas en archivos versionados.
- [ ] Suite de tests mínima en verde + `ruff` sin errores, ambos en CI.
- [ ] `requirements.txt` alineado al stack vivo y con versiones reproducibles.
- [ ] Documentación (`README`, `docs/`) refleja la cobertura y arquitectura reales.

---

## 5. Siguientes Pasos Inmediatos (propuestos, NO ejecutados)

1. **Diagnosticar el pipeline caído (B1):** `gh run list --workflow=data_ingestion.yml` para ver los últimos runs, y `python scripts/backfill.py --days 7` (con permiso) para comprobar si las APIs responden y por qué se detuvo el 2026-04-22.
2. **Higiene de repo (H1):** en una rama nueva, `git rm --cached db.sqlite3 memoria_chat.txt` y añadir `db.sqlite3`, `*.sqlite3`, `memoria_chat.txt` a `.gitignore`.
3. **Decidir entrypoint y plan de limpieza Django (B3 + R1):** confirmar `streamlit_app.py` vs multipágina y planear el `git rm -r` del árbol Django legacy.

---

*Generado por auditoría técnica de solo lectura. La auditoría no modificó ningún archivo del proyecto; esta entrega solo crea archivos nuevos en `docs/`.*
