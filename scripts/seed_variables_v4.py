"""
seed_variables_v4.py — 74 variables adicionales de alta prioridad:
  - 12 variables Globales (commodities, metales críticos, índices)
  - 14 variables Colombia (curva soberana, generación por tecnología, comercio)
  - 16 variables México (bonos, comercio exterior, energía CENACE)
  - 17 variables Brasil (NTN-B, energía ONS/CCEE, comercio)
  - 15 variables Ecuador (reservas, energía SNI, tarifas)

Ejecutar: python -X utf8 scripts/seed_variables_v4.py
Idempotente: SELECT antes de INSERT por (country_id, name).
"""
import sys, os, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from models.db import init_db, SessionLocal
from models.schema import Country, MacroVariable, TimeSeriesData, DataTypeEnum

init_db()
session = SessionLocal()

def get_country(code):
    c = session.query(Country).filter_by(code=code).first()
    if not c:
        raise ValueError(f"Country with code={code!r} not found. Run seed_variables_v3.py first.")
    return c

colombia = get_country("CO")
mexico   = get_country("MX")
brasil   = get_country("BR")
ecuador  = get_country("EC")
global_c = get_country("WW")

# ── Catálogo de 74 variables nuevas ───────────────────────────────────────────
ALL_VARIABLES = [

    # ══════════════════════════════════════════════════════════════════════════
    # GLOBAL (12)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=global_c, name="Copper (Cobre) Price",
         unit="USD/lb", frequency="monthly", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="PCOPPUSDM",
         source_url="https://fred.stlouisfed.org/series/PCOPPUSDM",
         description="Precio del cobre en USD/lb - FMI vía FRED. Indicador clave de demanda industrial y renovables"),

    dict(country=global_c, name="Aluminum (Aluminio) Price",
         unit="USD/MT", frequency="monthly", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="PALUMUSDM",
         source_url="https://fred.stlouisfed.org/series/PALUMUSDM",
         description="Precio del aluminio en USD/MT - FMI vía FRED. Insumo clave para paneles solares y transmisión"),

    dict(country=global_c, name="Gold (Oro) Price",
         unit="USD/oz", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="fred", api_serie_id="GOLDAMGBD228NLBM",
         source_url="https://fred.stlouisfed.org/series/GOLDAMGBD228NLBM",
         description="Precio del oro AM Fix Londres en USD/oz - FRED. Indicador de aversión al riesgo global"),

    dict(country=global_c, name="Lithium Carbonate Price",
         unit="USD/MT", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://tradingeconomics.com/commodity/lithium",
         description="Precio del carbonato de litio en USD/MT. Insumo crítico para baterías de almacenamiento y vehículos eléctricos"),

    dict(country=global_c, name="S&P 500 Index",
         unit="Índice", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="fred", api_serie_id="SP500",
         source_url="https://fred.stlouisfed.org/series/SP500",
         description="Índice S&P 500 - FRED. Referencia principal del mercado accionario global"),

    dict(country=global_c, name="VIX (Índice de Volatilidad)",
         unit="Índice", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="fred", api_serie_id="VIXCLS",
         source_url="https://fred.stlouisfed.org/series/VIXCLS",
         description="CBOE VIX: índice de volatilidad implícita del S&P 500. Termómetro del miedo del mercado global"),

    dict(country=global_c, name="MSCI Emerging Markets Index",
         unit="Índice", frequency="monthly", category="fx_rates",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.msci.com/emerging-markets",
         description="MSCI EM: índice de acciones de mercados emergentes. Correlaciona con flujos a LATAM"),

    dict(country=global_c, name="PMI Global Manufacturing",
         unit="Índice", frequency="monthly", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.spglobal.com/marketintelligence/en/mi/research-analysis/pmi.html",
         description="PMI Global Manufacturero S&P Global. Por encima de 50 = expansión. Indicador líder de actividad industrial"),

    dict(country=global_c, name="CO2 EU ETS Price",
         unit="EUR/tCO2", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.eex.com/en/market-data/environmentals/spot",
         description="Precio del carbono en el mercado EU ETS en EUR/tCO2. Referencia internacional para proyectos de descarbonización"),

    dict(country=global_c, name="Soja (Soybean) Price",
         unit="USD/bu", frequency="monthly", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="PSOYBUSDM",
         source_url="https://fred.stlouisfed.org/series/PSOYBUSDM",
         description="Precio de la soja en USD/bushel - FMI vía FRED. Relevante para exportaciones Brasil/Argentina"),

    dict(country=global_c, name="Cafe (Coffee) Arabica Price",
         unit="USD/lb", frequency="monthly", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="PCOFFINDUSDM",
         source_url="https://fred.stlouisfed.org/series/PCOFFINDUSDM",
         description="Precio del café Arábica ICO en USD/lb - FMI vía FRED. Principal exportación no minera de Colombia"),

    dict(country=global_c, name="Maiz (Corn) Price",
         unit="USD/bu", frequency="monthly", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="PMAIZMTUSDM",
         source_url="https://fred.stlouisfed.org/series/PMAIZMTUSDM",
         description="Precio del maíz en USD/bushel - FMI vía FRED. Indicador de seguridad alimentaria LATAM"),

    # ══════════════════════════════════════════════════════════════════════════
    # COLOMBIA (14)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="PIB CO (var. trimestral)",
         unit="%", frequency="quarterly", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co/index.php/estadisticas-por-tema/cuentas-nacionales/cuentas-nacionales-trimestrales",
         description="Variación trimestral del PIB de Colombia (variación t vs t-1) - DANE"),

    dict(country=colombia, name="ISE CO (Indice Seguimiento Economia)",
         unit="Índice", frequency="monthly", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co/index.php/estadisticas-por-tema/cuentas-nacionales/indicador-de-seguimiento-a-la-economia-ise",
         description="ISE: proxy mensual del PIB. Variación % anual - DANE"),

    dict(country=colombia, name="Informalidad Laboral CO",
         unit="%", frequency="monthly", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co",
         description="Tasa de informalidad laboral Colombia (DANE). Relevante para análisis de mercado laboral y consumo"),

    dict(country=colombia, name="FBKF CO (% PIB)",
         unit="% PIB", frequency="annual", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co",
         description="Formación Bruta de Capital Fijo como % del PIB - DANE. Indicador de inversión"),

    dict(country=colombia, name="TES 2Y CO",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co/es/estadisticas/tes",
         description="Tasa de los TES colombianos a 2 años. Parte de la curva soberana COP"),

    dict(country=colombia, name="Spread TES 10Y vs US Treasury 10Y",
         unit="bps", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co",
         description="Diferencial de tasas entre TES 10Y CO y UST 10Y. Proxy del riesgo país en curva soberana"),

    dict(country=colombia, name="Exportaciones Petroleo CO",
         unit="USD M", frequency="monthly", category="external",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.minenergia.gov.co/estadisticas",
         description="Exportaciones de petróleo crudo Colombia en USD millones - MinMinas/DANE"),

    dict(country=colombia, name="IPC Alimentos CO",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banrep", api_serie_id="IPC_alimentos",
         source_url="https://www.banrep.gov.co/es/estadisticas/indice-de-precios-del-consumidor-ipc",
         description="IPC subgrupo alimentos Colombia - BanRep/DANE"),

    dict(country=colombia, name="IPC Regulados CO",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banrep", api_serie_id="IPC_regulados",
         source_url="https://www.banrep.gov.co",
         description="IPC subgrupo regulados Colombia (incluye energía eléctrica, gas) - BanRep/DANE"),

    dict(country=colombia, name="Capacidad Hidraulica CO (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.xm.com.co/generacion/capacidad-efectiva-neta",
         description="Capacidad efectiva neta hidráulica instalada en Colombia en MW - XM"),

    dict(country=colombia, name="Capacidad Termica CO (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.xm.com.co",
         description="Capacidad efectiva neta térmica instalada en Colombia en MW - XM"),

    dict(country=colombia, name="Capacidad Eolica CO (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.xm.com.co",
         description="Capacidad efectiva neta eólica instalada en Colombia en MW - XM. Incluye La Guajira"),

    dict(country=colombia, name="Generacion Hidraulica CO (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.xm.com.co/generacion/generacion-real",
         description="Generación real hidráulica mensual Colombia en GWh - XM"),

    dict(country=colombia, name="Generacion Termica CO (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.xm.com.co",
         description="Generación real térmica mensual Colombia en GWh - XM"),

    # ══════════════════════════════════════════════════════════════════════════
    # MEXICO (16)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=mexico, name="TIIE 91 dias MX",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banxico", api_serie_id="SF43784",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="TIIE (Tasa de Interés Interbancaria de Equilibrio) a 91 días - Banxico"),

    dict(country=mexico, name="Bono 10Y Mexico",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banxico", api_serie_id="SF43936",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tasa del bono gubernamental M a 10 años México - Banxico"),

    dict(country=mexico, name="Bono 5Y Mexico",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banxico.org.mx/tipcambio/cmbTiposDeCambioAction.do",
         description="Tasa del bono gubernamental M a 5 años México. Parte de la curva soberana MXN"),

    dict(country=mexico, name="Bono 2Y Mexico",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banxico.org.mx",
         description="Tasa del bono gubernamental M a 2 años México. Curva soberana corto plazo"),

    dict(country=mexico, name="CDS Mexico 5Y",
         unit="bps", frequency="daily", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.worldgovernmentbonds.com/cds-historical-data/mexico/5-years/",
         description="Credit Default Swap México a 5 años en bps. Indicador de riesgo soberano"),

    dict(country=mexico, name="Reservas Internacionales MX",
         unit="USD M", frequency="weekly", category="fx_rates",
         connector_type="API", api_provider="banxico", api_serie_id="SF290383",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Reservas internacionales netas de México en USD millones - Banxico"),

    dict(country=mexico, name="Exportaciones MX",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="banxico", api_serie_id="SE1",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Exportaciones totales México en USD millones - Banxico/INEGI"),

    dict(country=mexico, name="Importaciones MX",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="banxico", api_serie_id="SE2",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Importaciones totales México en USD millones - Banxico/INEGI"),

    dict(country=mexico, name="Balanza Comercial MX",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="banxico", api_serie_id="SE26",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Balanza comercial México (Exp - Imp) en USD millones - Banxico"),

    dict(country=mexico, name="IPC MX (var. mensual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banxico", api_serie_id="SP68256",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="IPC México variación mensual - Banxico/INEGI"),

    dict(country=mexico, name="PML Nodo Norte MX (MXN/MWh)",
         unit="MXN/MWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.mx/Paginas/Publicas/MercadoOperacion/PreciosEnergiaSisMEM.aspx",
         description="Precio Marginal Local Nodo Norte México - CENACE. Referencia para proyectos solares en el norte"),

    dict(country=mexico, name="PML Nodo Peninsular MX (MXN/MWh)",
         unit="MXN/MWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.mx",
         description="PML Nodo Peninsular (Yucatán) México - CENACE"),

    dict(country=mexico, name="Generacion Solar MX (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.mx/Paginas/Publicas/Info/DemandaRegional.aspx",
         description="Generación solar mensual México en GWh - CENACE/SENER"),

    dict(country=mexico, name="Generacion Eolica MX (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.mx",
         description="Generación eólica mensual México en GWh - CENACE/SENER"),

    dict(country=mexico, name="Capacidad Solar MX Instalada (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.gob.mx/sener/acciones-y-programas/estadisticas-e-indicadores",
         description="Capacidad solar fotovoltaica instalada en México en MW - SENER"),

    dict(country=mexico, name="Tarifa CFE Media Tension MX (MXN/kWh)",
         unit="MXN/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cfe.mx/hogar/tarifas-de-energía",
         description="Tarifa CFE Media Tensión México - Referencia para evaluación de proyectos PMGD/AGPE"),

    # ══════════════════════════════════════════════════════════════════════════
    # BRASIL (17)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=brasil, name="IBC-Br (Proxy PIB Mensual BR)",
         unit="Índice", frequency="monthly", category="gdp_activity",
         connector_type="API", api_provider="bcb", api_serie_id="24363",
         source_url="https://api.bcb.gov.br",
         description="IBC-Br: índice de atividade econômica do Banco Central - proxy mensal do PIB"),

    dict(country=brasil, name="IPC Alimentos BR",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="bcb", api_serie_id="1635",
         source_url="https://api.bcb.gov.br",
         description="IPCA subgrupo alimentação no domicílio - BCB/IBGE"),

    dict(country=brasil, name="NTN-B 10Y BR (IPCA+)",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.tesourodireto.com.br",
         description="Taxa do título NTN-B (Tesouro IPCA+) a 10 anos - Tesouro Nacional. Taxa real de longo prazo"),

    dict(country=brasil, name="NTN-B 5Y BR (IPCA+)",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.tesourodireto.com.br",
         description="Taxa do título NTN-B (Tesouro IPCA+) a 5 anos - Tesouro Nacional"),

    dict(country=brasil, name="CDS Brasil 5Y",
         unit="bps", frequency="daily", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.worldgovernmentbonds.com/cds-historical-data/brazil/5-years/",
         description="Credit Default Swap Brasil a 5 anos em bps. Indicador de risco soberano"),

    dict(country=brasil, name="Exportacoes BR",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="bcb", api_serie_id="2254",
         source_url="https://api.bcb.gov.br",
         description="Exportações totais do Brasil em USD milhões - SECEX/MDIC via BCB"),

    dict(country=brasil, name="Importacoes BR",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="bcb", api_serie_id="2255",
         source_url="https://api.bcb.gov.br",
         description="Importações totais do Brasil em USD milhões - SECEX/MDIC via BCB"),

    dict(country=brasil, name="Balanca Comercial BR",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="bcb", api_serie_id="2256",
         source_url="https://api.bcb.gov.br",
         description="Balança comercial do Brasil (Exp - Imp) em USD milhões - BCB"),

    dict(country=brasil, name="PLD Nordeste BR (BRL/MWh)",
         unit="BRL/MWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ccee.org.br/portal/faces/pages_publico/o-que-fazemos/como_ccee_atua/precos/precos_medios",
         description="PLD (Precio de Liquidación de Diferencias) Submercado Nordeste - CCEE Brasil"),

    dict(country=brasil, name="ENA (Energia Natural Afluente) BR",
         unit="MWmed", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/energia_armazenada.aspx",
         description="Energia Natural Afluente ao SIN em MWmed - ONS Brasil. Indicador hídrico crítico"),

    dict(country=brasil, name="Nivel Reservatorios SE/CO BR (%)",
         unit="%", frequency="weekly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ons.org.br",
         description="Nível dos reservatórios do Submercado Sudeste/Centro-Oeste em % - ONS Brasil"),

    dict(country=brasil, name="Geracao Eolica BR (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/geracao_energia.aspx",
         description="Geração eólica mensal do SIN em GWh - ONS Brasil"),

    dict(country=brasil, name="Geracao Solar BR (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ons.org.br",
         description="Geração solar (centralizada + distribuída) mensal do SIN em GWh - ONS/ANEEL"),

    dict(country=brasil, name="Geracao Biomassa BR (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ons.org.br",
         description="Geração a partir de biomassa (bagaço de cana) mensal em GWh - ONS Brasil"),

    dict(country=brasil, name="Tarifa ANEEL Residencial BR (BRL/kWh)",
         unit="BRL/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.aneel.gov.br/tarifa-de-energia",
         description="Tarifa média residencial regulada por ANEEL en BRL/kWh. Benchmark para GD solar"),

    dict(country=brasil, name="Capacidade Instalada Renovavel BR (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.aneel.gov.br/bandeirastarifaras",
         description="Capacidade instalada de fontes renováveis (eólica+solar+hídrica+bio) Brasil en MW - ANEEL"),

    dict(country=brasil, name="PLD Norte BR (BRL/MWh)",
         unit="BRL/MWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ccee.org.br",
         description="PLD Submercado Norte em BRL/MWh - CCEE Brasil"),

    # ══════════════════════════════════════════════════════════════════════════
    # ECUADOR (15)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=ecuador, name="Reservas Internacionales EC",
         unit="USD M", frequency="monthly", category="fx_rates",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.bce.fin.ec/index.php/informacioneconomica/sector-monetario-y-financiero",
         description="Reservas internacionales Ecuador en USD millones - BCE. Indicador de liquidez dolarizada"),

    dict(country=ecuador, name="Exportaciones Petroleo EC",
         unit="USD M", frequency="monthly", category="external",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.bce.fin.ec/index.php/informacioneconomica/sector-externo",
         description="Exportaciones de petróleo Ecuador en USD millones - BCE"),

    dict(country=ecuador, name="Exportaciones Banano EC",
         unit="USD M", frequency="monthly", category="external",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.bce.fin.ec",
         description="Exportaciones de banano Ecuador en USD millones - BCE. Principal exportación no petrolera"),

    dict(country=ecuador, name="Balanza Comercial EC",
         unit="USD M", frequency="monthly", category="external",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.bce.fin.ec/index.php/informacioneconomica/sector-externo",
         description="Balanza comercial Ecuador (Exp - Imp) en USD millones - BCE"),

    dict(country=ecuador, name="IPC Ecuador (var. mensual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.inec.gob.ec/estadisticas/?option=com_content&view=article&id=103&Itemid=68",
         description="IPC variación mensual Ecuador - INEC"),

    dict(country=ecuador, name="Tasa Interbancaria EC",
         unit="%", frequency="monthly", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.bce.fin.ec/index.php/informacioneconomica/sector-monetario-y-financiero",
         description="Tasa de interés interbancaria Ecuador en USD - BCE. Referencia costo de dinero"),

    dict(country=ecuador, name="Bono Soberano 10Y EC",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.worldgovernmentbonds.com/country/ecuador/",
         description="Tasa del bono soberano Ecuador a 10 años en USD. Referencia del costo de financiamiento"),

    dict(country=ecuador, name="CDS Ecuador 5Y",
         unit="bps", frequency="daily", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.worldgovernmentbonds.com/cds-historical-data/ecuador/5-years/",
         description="Credit Default Swap Ecuador a 5 años en bps. Uno de los CDS más altos de LATAM"),

    dict(country=ecuador, name="Generacion Termica EC (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.ec/estadistica-e-informacion-del-sni/",
         description="Generación térmica mensual del SNI Ecuador en GWh - CENACE Ecuador"),

    dict(country=ecuador, name="Generacion Total SNI EC (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.ec",
         description="Generación total del Sistema Nacional Interconectado Ecuador en GWh - CENACE"),

    dict(country=ecuador, name="Demanda Nacional EC (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.ec/estadistica-e-informacion-del-sni/",
         description="Demanda máxima del SNI Ecuador en MW - CENACE"),

    dict(country=ecuador, name="Capacidad Hidraulica EC (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.arconel.gob.ec",
         description="Capacidad instalada hidráulica Ecuador en MW - ARCERNNR. 70% de la matriz eléctrica"),

    dict(country=ecuador, name="Capacidad Solar EC (MW)",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.arconel.gob.ec",
         description="Capacidad instalada solar fotovoltaica Ecuador en MW - ARCERNNR. En rápida expansión"),

    dict(country=ecuador, name="Tarifa Comercial EC (USD/kWh)",
         unit="USD/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.arconel.gob.ec/pliego-tarifario/",
         description="Tarifa eléctrica comercial regulada Ecuador en USD/kWh - ARCERNNR"),

    dict(country=ecuador, name="Tarifa Industrial EC (USD/kWh)",
         unit="USD/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.arconel.gob.ec/pliego-tarifario/",
         description="Tarifa eléctrica industrial regulada Ecuador en USD/kWh - ARCERNNR. Referencia autogeneración"),

    # ── Ecuador: variables críticas faltantes ─────────────────────────────────
    dict(country=ecuador, name="Desempleo EC",
         unit="%", frequency="quarterly", category="macro",
         connector_type="API", api_provider="bce", api_serie_id="Desempleo",
         source_url="https://www.ecuadorencifras.gob.ec/empleo-subempleo-y-desempleo/",
         description="Tasa de desempleo Ecuador % - INEC. Levantamiento trimestral (ENEMDU)"),

    dict(country=ecuador, name="Deficit Fiscal EC (% PIB)",
         unit="% PIB", frequency="annual", category="fiscal",
         connector_type="API", api_provider="bce", api_serie_id="DeficitFiscal",
         source_url="https://www.finanzas.gob.ec/estadisticas/",
         description="Déficit fiscal Ecuador como % del PIB - Ministerio de Finanzas / BCE"),

    dict(country=ecuador, name="Deuda Publica EC (% PIB)",
         unit="% PIB", frequency="annual", category="fiscal",
         connector_type="API", api_provider="bce", api_serie_id="DeudaPublica",
         source_url="https://www.finanzas.gob.ec/estadisticas/",
         description="Deuda pública total Ecuador como % del PIB - Ministerio de Finanzas. Incluye deuda interna y externa"),

    dict(country=ecuador, name="Generacion Hidraulica EC (GWh)",
         unit="GWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.ec/estadistica-e-informacion-del-sni/",
         description="Generación hidráulica mensual Ecuador en GWh - CENACE. Representa ~70% de la matriz eléctrica nacional"),

    dict(country=ecuador, name="Penetracion Renovable EC (%)",
         unit="%", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.cenace.gob.ec",
         description="Porcentaje de generación renovable en la matriz eléctrica Ecuador - CENACE/ARCERNNR"),

    dict(country=ecuador, name="Cuenta Corriente EC (% PIB)",
         unit="% PIB", frequency="annual", category="external",
         connector_type="API", api_provider="bce", api_serie_id="CuentaCorriente",
         source_url="https://www.bce.fin.ec/index.php/informacioneconomica/sector-externo",
         description="Cuenta corriente Ecuador como % del PIB - BCE"),
]

# ── Upsert idempotente ────────────────────────────────────────────────────────
print(f"Procesando {len(ALL_VARIABLES)} variables...")
inserted = 0
skipped = 0
for v in ALL_VARIABLES:
    country_obj = v.pop('country')
    existing = session.query(MacroVariable).filter_by(
        country_id=country_obj.id,
        name=v['name']
    ).first()

    if existing:
        skipped += 1
    else:
        var = MacroVariable(country_id=country_obj.id, **v)
        session.add(var)
        inserted += 1

session.commit()
print(f"  Insertadas: {inserted} | Ya existian: {skipped}")

# ── Patch: actualizar Ecuador MANUAL → conector bce donde aplica ──────────────
BCE_CONNECTOR_UPDATES = {
    "Reservas Internacionales EC":   ("bce", "ReservasInt"),
    "Balanza Comercial EC":          ("bce", "BalanzaComercial"),
    "Exportaciones Petroleo EC":     ("bce", "ExportacionesPetroleo"),
    "Exportaciones Banano EC":       ("bce", "ExportacionesBanano"),
    "IPC Ecuador (var. mensual)":    ("bce", "IPC_mensual"),
    "Tasa Interbancaria EC":         ("bce", "TasaInterbancaria"),
    "Cuenta Corriente EC (% PIB)":   ("bce", "CuentaCorriente"),
}

patched = 0
for var_name, (provider, serie_id) in BCE_CONNECTOR_UPDATES.items():
    v = session.query(MacroVariable).filter_by(country_id=ecuador.id, name=var_name).first()
    if v and v.connector_type == "MANUAL":
        v.connector_type = "API"
        v.api_provider = provider
        v.api_serie_id = serie_id
        patched += 1

session.commit()
print(f"  Actualizadas a conector BCE: {patched}")

# ── Demo data (ESTIMATION) para variables MANUAL — 24 meses 2024-01 → 2025-12 ─
# Valores base por nombre de variable
DEMO_VALUES = {
    # Global
    "Lithium Carbonate Price": (12500, -150, 300),   # (base, trend/month, noise)
    "MSCI Emerging Markets Index": (1050, 5, 20),
    "PMI Global Manufacturing": (49.8, 0, 0.4),
    "CO2 EU ETS Price": (65.0, 0.3, 2.0),
    # Colombia
    "PIB CO (var. trimestral)": (0.8, 0.05, 0.2),
    "ISE CO (Indice Seguimiento Economia)": (1.2, 0.05, 0.3),
    "Informalidad Laboral CO": (57.8, -0.05, 0.3),
    "FBKF CO (% PIB)": (22.5, 0.0, 0.2),
    "TES 2Y CO": (10.8, -0.05, 0.15),
    "Spread TES 10Y vs US Treasury 10Y": (450, -2, 8),
    "Exportaciones Petroleo CO": (780, 5, 30),
    "IPC Alimentos CO": (8.2, -0.15, 0.2),
    "IPC Regulados CO": (7.1, -0.1, 0.15),
    "Capacidad Hidraulica CO (MW)": (12250, 10, 50),
    "Capacidad Termica CO (MW)": (4800, 2, 20),
    "Capacidad Eolica CO (MW)": (350, 15, 10),
    "Generacion Hidraulica CO (GWh)": (4100, 20, 150),
    "Generacion Termica CO (GWh)": (650, 5, 30),
    # Mexico
    "Bono 5Y Mexico": (10.2, -0.05, 0.12),
    "Bono 2Y Mexico": (9.8, -0.05, 0.10),
    "CDS Mexico 5Y": (390, -2, 10),
    "PML Nodo Norte MX (MXN/MWh)": (1180, 10, 60),
    "PML Nodo Peninsular MX (MXN/MWh)": (1420, 12, 70),
    "Generacion Solar MX (GWh)": (4800, 50, 200),
    "Generacion Eolica MX (GWh)": (3200, 30, 120),
    "Capacidad Solar MX Instalada (MW)": (8200, 80, 100),
    "Tarifa CFE Media Tension MX (MXN/kWh)": (1.85, 0.01, 0.05),
    # Brasil
    "NTN-B 10Y BR (IPCA+)": (7.8, 0.02, 0.1),
    "NTN-B 5Y BR (IPCA+)": (7.2, 0.02, 0.08),
    "CDS Brasil 5Y": (215, -1, 8),
    "PLD Nordeste BR (BRL/MWh)": (92.45, 0.5, 5),
    "ENA (Energia Natural Afluente) BR": (85000, 200, 3000),
    "Nivel Reservatorios SE/CO BR (%)": (67.4, 0.2, 2),
    "Geracao Eolica BR (GWh)": (22500, 200, 800),
    "Geracao Solar BR (GWh)": (8450, 150, 400),
    "Geracao Biomassa BR (GWh)": (5200, 30, 200),
    "Tarifa ANEEL Residencial BR (BRL/kWh)": (0.72, 0.005, 0.02),
    "Capacidade Instalada Renovavel BR (MW)": (82000, 500, 500),
    "PLD Norte BR (BRL/MWh)": (88.20, 0.4, 4),
    # Ecuador
    "Reservas Internacionales EC": (4200, -20, 100),
    "Exportaciones Petroleo EC": (580, 5, 25),
    "Exportaciones Banano EC": (370, 3, 15),
    "Balanza Comercial EC": (320, -5, 30),
    "IPC Ecuador (var. mensual)": (0.21, 0.0, 0.05),
    "Tasa Interbancaria EC": (8.5, -0.02, 0.1),
    "Bono Soberano 10Y EC": (14.8, -0.1, 0.5),
    "CDS Ecuador 5Y": (1200, -5, 50),
    "Generacion Termica EC (GWh)": (380, 2, 20),
    "Generacion Total SNI EC (GWh)": (2850, 15, 80),
    "Demanda Nacional EC (MW)": (2950, 10, 50),
    "Capacidad Hidraulica EC (MW)": (4900, 5, 20),
    "Capacidad Solar EC (MW)": (420, 20, 15),
    "Tarifa Comercial EC (USD/kWh)": (0.115, 0.0005, 0.003),
    "Tarifa Industrial EC (USD/kWh)": (0.098, 0.0003, 0.002),
}

print("\nInsertando demo data (ESTIMATION) para variables MANUAL...")
demo_inserted = 0
demo_skipped = 0

# Reload variable list from DB for IDs
all_new_vars = session.query(MacroVariable).all()
var_name_to_id = {v.name: v.id for v in all_new_vars}

random.seed(42)  # reproducible

for var_name, (base, trend, noise_scale) in DEMO_VALUES.items():
    var_id = var_name_to_id.get(var_name)
    if not var_id:
        print(f"  [WARN] Variable not found: {var_name}")
        continue

    for month_offset in range(24):  # 2024-01 to 2025-12
        date = datetime(2024, 1, 1) + timedelta(days=30 * month_offset)
        value = base + trend * month_offset + random.gauss(0, noise_scale)

        # Check if exists
        existing_ts = session.query(TimeSeriesData).filter_by(
            variable_id=var_id,
            date=date,
            data_type=DataTypeEnum.ESTIMATION
        ).first()

        if existing_ts:
            demo_skipped += 1
            continue

        ts = TimeSeriesData(
            variable_id=var_id,
            date=date,
            data_type=DataTypeEnum.ESTIMATION,
            value=round(value, 4),
            is_anomaly=False
        )
        session.add(ts)
        demo_inserted += 1

session.commit()
print(f"  Demo data insertada: {demo_inserted} | Ya existia: {demo_skipped}")
print("\nSeed v4 completado.")
