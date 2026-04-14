# Auditoria de Arquitectura de Datos — Cerebro Economico NLA
**Fecha:** 2026-04-14  
**Tipo:** Diagnostico profundo + Gap Analysis  
**Alcance:** CO (core), MX, BR, EC (secundarios), WW (global)

---

## 1. Estado Actual: Resumen Ejecutivo

| Metrica | Valor | Evaluacion |
|---------|-------|------------|
| Variables definidas | 132 | Buen inventario |
| Variables con datos | 17 (12.9%) | **CRITICO** |
| Registros en fact_timeseries | 1,502 | Bajo |
| Conectores API activos | 2 de 6 (BCB, SCRAPER) | **CRITICO** |
| Series con >100 registros | 2 (Selic, USD/BRL) | Solo Brasil |
| Forecasts de consenso | 17 | Aceptable |
| Tipo PROJECTION en DB | 0 registros | Gap total |
| Categoria Sostenibilidad | No existe | Gap total |

### Veredicto
El repositorio tiene una **estructura bien disenada** (ORM, conectores, forecasting) pero sufre de **deficit critico de ingesta**: 87% de las variables estan vacias. La unica fuente confiable es BCB (Brasil). BanRep esta deprecada sin migracion completa.

---

## 2. Inventario por Pais, Frecuencia y Tipo

### 2.1 Colombia (CO) — 78 variables, 8 con datos (10.3%)

| Categoria | Total | Con datos | Connector | Estado |
|-----------|-------|-----------|-----------|--------|
| macro | 8 | 3 | API:banrep | Parcial — WB fallback |
| rates_monetary | 12 | 0 | API:banrep | Fallback no implementado |
| prices_inflation | 6 | 0 | API:banrep/MANUAL | Sin datos |
| gdp_activity | 4 | 0 | API:banrep/MANUAL | Sin datos |
| energy | 14 | 1 | API:xm | XM responde 0 datos |
| fx_rates | 3 | 0 | API:banrep/MANUAL | TRM sin fuente confiable |
| external | 5 | 0 | API:banrep/fred | FRED sin API key |
| fiscal | 6 | 0 | MANUAL | Sin ingesta |
| corporate_finance | 26 | 4 | MANUAL/EXCEL | Solo estimaciones |

**Problema raiz:** BanRep depreco SUAMECA/Totoro en 2025. El fallback a World Bank solo cubre 3 indicadores (IPC, Desempleo, PIB) con datos anuales, no mensuales.

### 2.2 Mexico (MX) — 12 variables, 3 con datos (25%)

| Categoria | Total | Con datos | Connector | Estado |
|-----------|-------|-----------|-----------|--------|
| macro | 4 | 3 | SCRAPER/banxico | Scrapers funcionan |
| rates_monetary | 3 | 0 | API:banxico | Sin BANXICO_TOKEN |
| prices_inflation | 2 | 0 | API:banxico | Sin BANXICO_TOKEN |
| fx_rates | 1 | 0 | API:banxico | Sin BANXICO_TOKEN |
| gdp_activity | 2 | 0 | API:banxico | Sin BANXICO_TOKEN |

**Problema raiz:** `BANXICO_TOKEN` no esta configurado. 9 variables quedan bloqueadas.

### 2.3 Brasil (BR) — 16 variables, 4 con datos (25%)

| Categoria | Total | Con datos | Connector | Estado |
|-----------|-------|-----------|-----------|--------|
| macro | 4 | 4 | API:bcb | **Funcionando** |
| rates_monetary | 3 | 0 | API:bcb | Serie IDs definidos, no ejecutado |
| prices_inflation | 3 | 0 | API:bcb | Serie IDs definidos, no ejecutado |
| fx_rates | 2 | 0 | API:bcb/MANUAL | Definido |
| gdp_activity | 2 | 0 | API:bcb | Definido |
| fiscal | 1 | 0 | API:bcb | Definido |

**Estado:** BCB es el conector mas confiable. Con un backfill completo se puede subir a ~80% de cobertura BR.

### 2.4 Ecuador (EC) — 4 variables, 2 con datos (50%)

Solo IPC y WTI. Sin conectores propios (BCE no tiene API publica moderna).

### 2.5 Global (WW) — 22 variables, 0 con datos (0%)

Todas dependen de FRED. **`FRED_API_KEY` no esta configurado.** Con la API key, se obtendrian 17 variables automaticamente.

---

## 3. Gap Analysis Critico

### 3.1 Variables Faltantes en la DB (No existen en dim_variable)

| Variable | Pais | Tipo | Fuente Propuesta | Prioridad |
|----------|------|------|------------------|-----------|
| Matriz generacion por fuente | CO | Energia | XM/SIMEM GeneRealFuen | **Alta** |
| Demanda proyectada (UPME) | CO | Energia | UPME Plan Expansion | **Alta** |
| Intensidad carbono grid | CO | Sostenibilidad | UPME Factor Emision | **Alta** |
| Penetracion FNCER | CO | Sostenibilidad | XM/derivado | **Alta** |
| Precio bonos carbono | WW | Sostenibilidad | Verra/Ecosystem Marketplace | Media |
| I-REC precio | WW | Sostenibilidad | TradeREC | Media |
| Emisiones CO2 sector electrico | CO | Sostenibilidad | IDEAM | Media |
| Demanda proyectada (EPE PDE) | BR | Energia | EPE.gov.br | Media |
| Demanda proyectada (PRODESEN) | MX | Energia | SENER | Media |
| Capacidad instalada por fuente | CO/MX/BR | Energia | IRENA | Media |

### 3.2 Variables Definidas pero Bloqueadas (API Keys faltantes)

| Bloqueo | Variables afectadas | Solucion |
|---------|---------------------|----------|
| `FRED_API_KEY` | 17 (WW + EC + CO external) | Obtener en fred.stlouisfed.org/docs/api |
| `BANXICO_TOKEN` | 9 (MX completo) | Obtener en banxico.org.mx/SieAPIRest |
| BanRep deprecado | 27 (CO macro/rates/fx) | Migrar a API v2 o scraping SFC |

### 3.3 Categoria Completa Ausente: Sostenibilidad

No existe ninguna variable de sostenibilidad en la base de datos. Esto es un gap completo para el nexo Energia-Economia-Sostenibilidad.

---

## 4. Diagnostico de Infraestructura ETL

### 4.1 Conectores: Estado Operativo

| Conector | Estado | Tasa Exito | Problema |
|----------|--------|------------|----------|
| `bcb` | OK | 26.7% (4/15) | Backfill no ejecutado para series secundarias |
| `banrep` | ROTO | 11.1% (3/27) | API deprecada, WB fallback parcial |
| `fred` | BLOQUEADO | 0% (0/17) | Sin API key |
| `banxico` | BLOQUEADO | 0% (0/9) | Sin token |
| `xm` | PARCIAL | 9% (1/11) | API responde vacio, SIMEM fallback limitado |
| `world_bank` | FUNCIONAL | N/A | Solo como fallback, datos anuales |

### 4.2 Separacion Series Historicas vs Proyecciones

**Diseno actual (correcto):** `fact_timeseries.data_type` = REAL_OFFICIAL / PROJECTION / ESTIMATION

**Problema:** 0 registros tipo PROJECTION. Consensos en tabla separada, no reflejados en fact_timeseries.

**Directorio de trabajo:**
```
data/
  agent.py        # Ingesta de series oficiales
  consensus.py    # Consensos (tabla aparte)
  database.py     # Queries de lectura
  staging/        # [NUEVO] Datos crudos pre-validacion
  exports/        # [NUEVO] CSVs exportados por usuarios
```

### 4.3 Normalizacion de Unidades

Las unidades no estan normalizadas para comparabilidad regional. Ver `config/data_catalog.yaml` seccion `unit_normalization` para las convenciones definidas.

---

## 5. Proximos Pasos para Automatizacion Total

### Fase 1: Quick Wins (1-2 dias)
1. Configurar `FRED_API_KEY` y `BANXICO_TOKEN` en `.env`
2. Ejecutar `python scripts/backfill.py` (esperado: 17 → ~55 variables)
3. Ejecutar `python scripts/validate_data.py` para verificar

### Fase 2: Reparacion de Conectores (1 semana)
4. BanRep: Implementar nueva fuente (API v2 o scraping SFC)
5. XM Energy: Debug del formato POST actualizado
6. TRM: Scraping superfinanciera.gov.co

### Fase 3: Sostenibilidad y Proyecciones (2 semanas)
7. Crear categoria `sustainability` con 5 variables minimas
8. Pipeline: forecast_ensemble → fact_timeseries (PROJECTION)
9. Ingestar fuentes de largo plazo (UPME, EPE, PRODESEN)

### Fase 4: Hardening
10. Activar Celery beat (scheduler ya configurado en docker-compose)
11. Integrar validate_data.py como tarea periodica
12. Deprecar tablas Django ORM duplicadas (core_*)
