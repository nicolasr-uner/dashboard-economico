# Guía de Configuración — Cerebro Económico NLA

Esta guía cubre todo lo necesario para poner en marcha el dashboard desde cero: instalación, obtención de API keys, carga de datos históricos y despliegue.

---

## 1. Prerequisitos

- **Python 3.11+** (recomendado 3.11 o 3.12; el proyecto usa 3.14 en local)
- **Git** para clonar el repositorio
- **SQLite** viene incluido con Python — no se necesita instalar nada extra para desarrollo local
- Conexión a internet para las llamadas a APIs externas

---

## 2. Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/nicolasr-uner/dashboard-economico.git
cd dashboard-economico

# 2. Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Inicializar la base de datos
python -c "from models.db import init_db; init_db()"

# 5. Cargar definiciones de variables (132 indicadores)
python scripts/seed_variables_v4.py

# 6. Iniciar el dashboard (ya funciona con datos parciales)
streamlit run streamlit_app.py
```

> **Nota:** Sin API keys, el dashboard muestra datos reales de Brasil (BCB) y Colombia energía (XM). Las secciones de mercados globales y México aparecen con mensajes amigables indicando que requieren configuración.

---

## 3. API Keys — Cuáles se necesitan y cómo obtenerlas

El dashboard usa **3 fuentes que requieren credenciales** y **4 fuentes totalmente públicas**:

| Credencial | Para qué sirve | Costo | Tiempo de obtención |
|-----------|----------------|-------|---------------------|
| `FRED_API_KEY` | Commodities globales, mercados USA | Gratis | ~2 minutos |
| `BANXICO_TOKEN` | Datos macro de México | Gratis | ~2 minutos |
| `ANTHROPIC_API_KEY` | Chatbot inteligente (opcional) | De pago | Inmediato |

---

### 3.1 FRED API Key (Federal Reserve Economic Data)

**Desbloquea:** WTI, Brent, Henry Hub, Gold, Copper, Lithium, DXY, EUR/USD, S&P 500, VIX, Fed Funds Rate, Treasuries 2Y/10Y, CPI USA, y más (17 variables).

**Paso a paso:**

1. Ir a → **https://fredaccount.stlouisfed.org/login/secure/**
2. Crear cuenta gratuita (solo email y contraseña)
3. Una vez dentro: **My Account → API Keys → Request API Key**
4. En el campo *"Describe the application"*, escribir:

   > *Macroeconomic intelligence dashboard for Colombia, Mexico, Brazil and Ecuador. Fetches commodity prices (WTI, Brent, Natural Gas), precious metals, FX rates (EUR/USD, DXY), equity indices (S&P 500, VIX), US interest rates (Fed Funds, Treasury yields), and US inflation data for display in a Streamlit-based monitoring platform.*

5. Marcar la casilla de términos de uso → **Request API Key**
6. La key aparece **instantáneamente** en pantalla. Copiarla.

---

### 3.2 Banxico Token (Banco de México)

**Desbloquea:** Tasa objetivo Banxico, TIIE 28 días, CETES 28 días, USD/MXN, IPC México, PIB México (9 variables).

**Paso a paso:**

1. Ir a → **https://www.banxico.org.mx/SieAPIRest/service/v1/token**
2. Llenar el formulario:
   - **Nombre:** Tu nombre
   - **Institución:** Empresa o "Independiente"
   - **Correo:** Tu email
   - **Descripción:** *"Dashboard de inteligencia macroeconómica multi-país"*
3. Hacer clic en **Solicitar token**
4. El token llega al correo en **~1 minuto**

---

### 3.3 Anthropic API Key (Opcional — para chatbot inteligente)

**Desbloquea:** El asistente de datos usa Claude para respuestas enriquecidas con contexto. Sin esta key, el chatbot funciona con respuestas basadas en plantillas (también útil).

1. Ir a → **https://console.anthropic.com/**
2. Crear cuenta → **API Keys → Create Key**
3. Nota: tiene costo por uso (muy bajo para uso normal del dashboard)

---

## 4. Configurar las keys

### Opción A: `.streamlit/secrets.toml` (recomendado para desarrollo local y Streamlit Cloud)

```bash
# Copiar el archivo de ejemplo
cp .streamlit/secrets.toml.example .streamlit/secrets.toml

# Editar con tus keys reales
```

Contenido de `.streamlit/secrets.toml`:

```toml
FRED_API_KEY = "abcd1234efgh5678..."      # tu key de FRED
BANXICO_TOKEN = "xyz-token-banxico..."    # tu token de Banxico
ANTHROPIC_API_KEY = "sk-ant-..."          # opcional
```

> ⚠️ **IMPORTANTE:** `secrets.toml` está en `.gitignore` — nunca se sube al repositorio. Nunca compartas este archivo.

### Opción B: Variables de entorno (`.env`)

```bash
cp .env.example .env
# Editar .env con tus valores
```

---

## 5. Cobertura Mundial — Qué datos activa cada fuente

### Fuentes que funcionan SIN key (gratuitas y automáticas)

| Región | País | Indicadores disponibles | Fuente | Frecuencia |
|--------|------|------------------------|--------|------------|
| América Latina | 🇧🇷 Brasil | Tasa Selic, IPCA, CDI, USD/BRL, Desempleo, PIB, Deuda Pública | BCB (Banco Central do Brasil) | Diaria |
| América Latina | 🇨🇴 Colombia | Aportes Hídricos energéticos | XM (mercado mayorista) | Diaria |
| América Latina | 🇨🇴 Colombia | IPC, PIB, Desempleo, TRM, Cuenta Corriente | World Bank (fallback BanRep) | Anual |
| América Latina | 🇪🇨 Ecuador | IPC, PIB, Desempleo | World Bank | Anual |
| Global | 190 países | PIB, Inflación, Deuda, Reservas (indicadores anuales) | World Bank Open Data | Anual |

### Fuentes que requieren key

| Región | País/Mercado | Indicadores | Fuente | Key necesaria |
|--------|-------------|-------------|--------|---------------|
| América Latina | 🇲🇽 México | Tasa Objetivo Banxico, TIIE, CETES, USD/MXN, IPC, PIB | Banxico SIE API | `BANXICO_TOKEN` |
| Norte América | 🇺🇸 USA | Fed Funds Rate, Treasury 2Y/10Y, CPI, PCE | FRED | `FRED_API_KEY` |
| Global — Energía | Mundo | WTI Crude Oil, Brent Crude, Henry Hub Natural Gas | FRED | `FRED_API_KEY` |
| Global — Metales | Mundo | Gold (Oro), Copper (Cobre), Lithium, Aluminum | FRED | `FRED_API_KEY` |
| Global — Agrícola | Mundo | Coffee Arabica, Soybean, Corn (Maíz) | FRED | `FRED_API_KEY` |
| Global — FX | Mundo | EUR/USD, DXY (Índice Dólar) | FRED | `FRED_API_KEY` |
| Global — Mercados | Mundo | S&P 500 Index, VIX (Volatilidad) | FRED | `FRED_API_KEY` |

### Resumen de cobertura por configuración

| Configuración | Cobertura estimada | Regiones activas |
|--------------|-------------------|------------------|
| Sin keys (base) | ~23% | Brasil completo, Colombia energía |
| + `BANXICO_TOKEN` | ~28% | + México macro |
| + `FRED_API_KEY` | ~55% | + Commodities globales, USA, FX, mercados |
| + Ambas keys | ~60% | Todo lo anterior |
| + Anthropic | ~60% + chatbot inteligente | Sin cambio en datos |

> **Para ampliar cobertura a Europa y Asia** en el futuro se podrían agregar conectores para el BCE (Banco Central Europeo — API pública), BOJ (Banco de Japón) y RBA (Australia). La arquitectura de conectores del proyecto está diseñada para ello.

---

## 6. Cargar datos históricos (Backfill)

Una vez configuradas las keys, carga los datos históricos desde 2020 hasta hoy:

```bash
python scripts/backfill.py
```

El script:
- Detecta automáticamente qué APIs tienen key configurada
- Descarga datos desde `2020-01-01` hasta hoy para **todas** las variables API activas
- Omite silenciosamente las variables sin key configurada (no falla)
- Muestra un resumen al final: OK / SKIP / ERROR por proveedor

**Salida esperada:**
```
🔄 Backfill iniciado: 48 variables API | 2020-01-01 → 2026-04-16

  [OK   ] Tasa Selic BR (bcb/11): 2297 registros cargados.
  [OK   ] USD/BRL (bcb/1): 1577 registros cargados.
  [SKIP ] WTI Crude Oil (fred/DCOILWTICO) — sin datos (¿falta API key?)
  ...

✅  OK:    15
⏭️  SKIP:  30
❌  ERROR:  3
```

> **Nota:** El primer backfill puede tardar 3–8 minutos dependiendo de la conexión. Los datos se guardan en `db.sqlite3`.

---

## 7. Verificar que los datos se cargaron

```bash
# Validación de calidad de datos
python scripts/validate_data.py

# Ver reporte detallado
python scripts/validate_data.py --output md
# → genera docs/validation_report.md
```

O directamente en el dashboard: el sidebar muestra el widget "📊 Estado de datos" con 🟢/🟡/🔴 por variable.

---

## 8. Despliegue en Streamlit Cloud

1. Hacer push del repositorio a GitHub (sin `secrets.toml` — está en `.gitignore`)
2. Ir a → **https://share.streamlit.io/** → **New app**
3. Conectar el repositorio y seleccionar `streamlit_app.py` como archivo principal
4. En **Advanced settings → Secrets**, pegar el contenido de tu `secrets.toml`:
   ```toml
   FRED_API_KEY = "tu_key_aqui"
   BANXICO_TOKEN = "tu_token_aqui"
   ANTHROPIC_API_KEY = "sk-ant-..."
   ```
5. **Deploy** — el app queda público con URL tipo `https://tu-usuario-dashboard.streamlit.app`

> **Nota sobre la base de datos en Cloud:** Streamlit Cloud usa un filesystem efímero — la base de datos SQLite se reinicia en cada redeploy. Para persistencia real en producción, se recomienda migrar a PostgreSQL (la variable `DATABASE_URL` en `.env` ya soporta esta configuración).

---

## 9. Fuentes de datos — Referencias oficiales

| Fuente | URL | Documentación API |
|--------|-----|-------------------|
| FRED (Federal Reserve) | https://fred.stlouisfed.org | https://fred.stlouisfed.org/docs/api/ |
| BCB (Banco Central do Brasil) | https://www.bcb.gov.br | https://dadosabertos.bcb.gov.br |
| Banxico | https://www.banxico.org.mx | https://www.banxico.org.mx/SieAPIRest/service/v1/ |
| World Bank | https://data.worldbank.org | https://datahelpdesk.worldbank.org/knowledgebase/topics/125589 |
| XM Colombia | https://www.xm.com.co | https://servapibi.xm.com.co |
| SIMEM Colombia | https://www.simem.co | https://www.simem.co/backend-files/api/ |
| Anthropic Claude | https://console.anthropic.com | https://docs.anthropic.com |

---

## Preguntas frecuentes

**¿Puedo usar el dashboard sin ninguna API key?**
Sí. Brasil (BCB) y Colombia energía (XM) funcionan sin key. Las demás secciones muestran mensajes informativos.

**¿Las API keys tienen costo?**
FRED y Banxico son 100% gratuitas, sin límites prácticos para uso normal. Anthropic tiene costo por uso (muy bajo — el chatbot usa `claude-haiku`, el modelo más económico).

**¿Con qué frecuencia se actualizan los datos?**
Depende de la fuente. BCB actualiza diario, XM actualiza diario, FRED actualiza según la serie (diario para precios, mensual para macro), World Bank actualiza anualmente.

**¿Cómo agrego nuevas variables o países?**
Editar `scripts/seed_variables_v4.py`, agregar la definición de la variable con su `api_provider` y `api_serie_id`, luego correr `python scripts/seed_variables_v4.py` y `python scripts/backfill.py`.

**¿El proyecto soporta más países?**
La arquitectura soporta cualquier país con datos en World Bank (190+ países). Para agregar cobertura diaria de un país nuevo, se necesita implementar un conector específico para su banco central en `connectors/`.
