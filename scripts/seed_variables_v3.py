"""
seed_variables_v3.py — Catálogo completo expandido basado en:
 - Modelo financiero Exagon 13 Minifarms
 - Modelo Tax Partner Ruitoque
 - Variables macroeconómicas completas para Colombia, México, Brasil, Ecuador
 - Variables de Finanzas Corporativas Unergy (solar)
Ejecutar: python scripts/seed_variables_v3.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db import init_db, SessionLocal
from models.schema import Country, MacroVariable

init_db()
session = SessionLocal()

# ── Helpers ────────────────────────────────────────────────────────────────────
def get_or_create_country(session, name, code, flag):
    c = session.query(Country).filter_by(code=code).first()
    if not c:
        c = Country(name=name, code=code, flag_emoji=flag)
        session.add(c)
        session.flush()
    return c

colombia  = get_or_create_country(session, "Colombia",  "CO", "🇨🇴")
mexico    = get_or_create_country(session, "México",    "MX", "🇲🇽")
brasil    = get_or_create_country(session, "Brasil",    "BR", "🇧🇷")
ecuador   = get_or_create_country(session, "Ecuador",  "EC", "🇪🇨")
global_c  = get_or_create_country(session, "Global",   "WW", "🌐")
session.commit()
print("País OK.\n")

# ── Catálogo completo ─────────────────────────────────────────────────────────
# Formato: dict con todos los campos de MacroVariable
# category: gdp_activity | prices_inflation | rates_monetary | fx_rates |
#           external | fiscal | energy | corporate_finance
ALL_VARIABLES = [

    # ══════════════════════════════════════════════════════════════════════════
    # 1. ACTIVIDAD ECONÓMICA
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="PIB Trimestral CO (var. anual)",
         unit="%", frequency="quarterly", category="gdp_activity",
         connector_type="API", api_provider="banrep", api_serie_id="PIB_trim",
         source_url="https://suameca.banrep.gov.co",
         description="Variación anual del PIB trimestral de Colombia - BanRep"),

    dict(country=colombia, name="Producción Industrial CO (ISE)",
         unit="Índice", frequency="monthly", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co",
         description="Índice de Seguimiento a la Economía - DANE"),

    dict(country=colombia, name="Desempleo CO",
         unit="%", frequency="monthly", category="gdp_activity",
         connector_type="API", api_provider="banrep", api_serie_id="Desempleo",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa de desempleo nacional - DANE/BanRep"),

    dict(country=colombia, name="SMMLV (Salario Mínimo CO)",
         unit="COP", frequency="annual", category="gdp_activity",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.mintrabajo.gov.co",
         description="Salario Mínimo Mensual Legal Vigente - Colombia. Valor 2026: 1.750.905"),

    dict(country=mexico, name="PIB Trimestral MX (var. anual)",
         unit="%", frequency="quarterly", category="gdp_activity",
         connector_type="API", api_provider="banxico", api_serie_id="SR16643",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Variación anual del PIB trimestral - Banxico/INEGI"),

    dict(country=mexico, name="Desempleo MX",
         unit="%", frequency="monthly", category="gdp_activity",
         connector_type="API", api_provider="banxico", api_serie_id="SR16734",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tasa de desocupación nacional - Banxico/INEGI"),

    dict(country=brasil, name="PIB Trimestral BR (var. %)",
         unit="%", frequency="quarterly", category="gdp_activity",
         connector_type="API", api_provider="bcb", api_serie_id="22099",
         source_url="https://api.bcb.gov.br",
         description="Variación trimestral del PIB de Brasil - BCB/IBGE"),

    dict(country=brasil, name="Desempleo BR",
         unit="%", frequency="quarterly", category="gdp_activity",
         connector_type="API", api_provider="bcb", api_serie_id="24369",
         source_url="https://api.bcb.gov.br",
         description="Tasa de desempleo Brasil - PNAD Contínua"),

    dict(country=ecuador, name="PIB Ecuador (USD corrientes)",
         unit="USD", frequency="annual", category="gdp_activity",
         connector_type="API", api_provider="fred", api_serie_id="MKTGDPECA646NWDB",
         source_url="https://fred.stlouisfed.org",
         description="PIB de Ecuador en USD corrientes - World Bank/FRED"),

    # ══════════════════════════════════════════════════════════════════════════
    # 2. PRECIOS E INFLACIÓN — Clave para indexación PPA (IPP/IPC)
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="IPC CO (var. anual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banrep", api_serie_id="IPC_variacion_anual",
         source_url="https://suameca.banrep.gov.co",
         description="IPC variación anual Colombia - BanRep/DANE. Usado para indexación PPA"),

    dict(country=colombia, name="IPC CO (var. mensual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banrep", api_serie_id="IPC_variacion_mensual",
         source_url="https://suameca.banrep.gov.co",
         description="IPC variación mensual Colombia - BanRep/DANE"),

    dict(country=colombia, name="IPP CO (var. anual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co/index.php/estadisticas-por-tema/precios-y-costos/indice-de-precios-del-productor-ipp",
         description="Índice de Precios al Productor Colombia - DANE. INDEXADOR DEL PPA en modelos Exagon/Ruitoque"),

    dict(country=colombia, name="IPP CO (valor índice)",
         unit="Índice", frequency="monthly", category="prices_inflation",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dane.gov.co",
         description="Valor absoluto del IPP Colombia (base 2019=100) - DANE. Valor 2025: ~187.7"),

    dict(country=colombia, name="IPC CO Core (sin alimentos ni regulados)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banrep", api_serie_id="IPC_sin_alim_reg",
         source_url="https://suameca.banrep.gov.co",
         description="IPC Core Colombia - BanRep"),

    dict(country=colombia, name="Expectativas Inflación 12m CO",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co/es/encuesta-de-expectativas-de-inflacion",
         description="Expectativas de inflación a 12 meses - Encuesta BanRep (mediana de analistas)"),

    dict(country=global_c, name="CPI USA (var. anual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="fred", api_serie_id="CPIAUCSL",
         source_url="https://fred.stlouisfed.org",
         description="Consumer Price Index USA (YoY) - BLS/FRED. Supuesto Exagon 2026: 2.5%"),

    dict(country=mexico, name="IPC MX (var. anual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banxico", api_serie_id="SP68257",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="IPC México variación anual - Banxico/INEGI"),

    dict(country=mexico, name="IPC MX Core",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="banxico", api_serie_id="SP68258",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="IPC Core México (sin alimentos ni energía) - Banxico"),

    dict(country=brasil, name="IPCA BR (var. anual)",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="bcb", api_serie_id="433",
         source_url="https://api.bcb.gov.br",
         description="IPCA variación anual Brasil - BCB/IBGE"),

    dict(country=brasil, name="IPCA BR Core",
         unit="%", frequency="monthly", category="prices_inflation",
         connector_type="API", api_provider="bcb", api_serie_id="11427",
         source_url="https://api.bcb.gov.br",
         description="IPCA Core Brasil - BCB"),

    dict(country=brasil, name="Expectativas IPCA 12m (Focus BCB)",
         unit="%", frequency="weekly", category="prices_inflation",
         connector_type="API", api_provider="bcb", api_serie_id="13522",
         source_url="https://api.bcb.gov.br",
         description="Expectativas de inflación Focus BCB a 12 meses"),

    dict(country=ecuador, name="IPC Ecuador (var. anual)",
         unit="%", frequency="annual", category="prices_inflation",
         connector_type="API", api_provider="fred", api_serie_id="FPCPITOTLZGECU",
         source_url="https://fred.stlouisfed.org",
         description="IPC Ecuador anual - World Bank/FRED"),

    # ══════════════════════════════════════════════════════════════════════════
    # 3. TASAS DE INTERÉS Y MERCADO MONETARIO
    #    Clave: IBR (tasa deuda proyecto), TES 10Y (Rf WACC), DTF
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="Tasa de Intervención BanRep",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="TasIntPol",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa de política monetaria BanRep. Determina el IBR y costo de deuda de proyectos"),

    dict(country=colombia, name="IBR Overnight CO",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="IBR_ON",
         source_url="https://suameca.banrep.gov.co",
         description="IBR Overnight - Indicador Bancario de Referencia. Supuesto Exagon 2026: 12%"),

    dict(country=colombia, name="IBR E.A. CO",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="IBR_1M",
         source_url="https://suameca.banrep.gov.co",
         description="IBR Efectivo Anual. Usado en el modelo de deuda Ruitoque como base de tasa"),

    dict(country=colombia, name="IBR Trimestral CO",
         unit="%", frequency="quarterly", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="IBR_3M",
         source_url="https://suameca.banrep.gov.co",
         description="IBR trimestral - usado para cálculo de cuotas de deuda en COP"),

    dict(country=colombia, name="DTF E.A. CO",
         unit="%", frequency="weekly", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="DTF",
         source_url="https://suameca.banrep.gov.co",
         description="DTF Efectiva Anual Colombia. Referencia histórica de tasas pasivas"),

    dict(country=colombia, name="TES 10Y CO (Rf WACC)",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co/es/tasa-de-rendimiento-tes",
         description="Rendimiento TES 10 años Colombia. Tasa libre de riesgo para WACC. Supuesto modelos: 11.1%"),

    dict(country=colombia, name="TES 5Y CO",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co",
         description="Rendimiento TES 5 años Colombia - BanRep"),

    dict(country=colombia, name="Tasa Crédito Comercial CO",
         unit="%", frequency="monthly", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="TasColCom",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa de colocación crédito comercial Colombia - BanRep"),

    dict(country=colombia, name="CDT 360 días CO",
         unit="%", frequency="weekly", category="rates_monetary",
         connector_type="API", api_provider="banrep", api_serie_id="CDT_360",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa CDT 360 días. Benchmark vs retorno Tax Partner Unergy"),

    dict(country=mexico, name="Tasa Objetivo Banxico",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banxico", api_serie_id="SF61745",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tasa de fondeo interbancario objetivo - Banxico"),

    dict(country=mexico, name="TIIE 28 días MX",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="banxico", api_serie_id="SF43783",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="TIIE 28 días México - referencia de costo de deuda"),

    dict(country=mexico, name="CETES 28 días MX",
         unit="%", frequency="weekly", category="rates_monetary",
         connector_type="API", api_provider="banxico", api_serie_id="SF43936",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="CETES 28 días México - tasa libre de riesgo México"),

    dict(country=brasil, name="Tasa Selic BR",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="bcb", api_serie_id="432",
         source_url="https://api.bcb.gov.br",
         description="Tasa Selic diaria Brasil - BCB"),

    dict(country=brasil, name="CDI BR",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="bcb", api_serie_id="4389",
         source_url="https://api.bcb.gov.br",
         description="Certificado Depósito Interbancario BR - tasa referencia mercado"),

    dict(country=brasil, name="Expectativas Selic 12m (Focus)",
         unit="%", frequency="weekly", category="rates_monetary",
         connector_type="API", api_provider="bcb", api_serie_id="4175",
         source_url="https://api.bcb.gov.br",
         description="Expectativa mediana para Selic - Focus BCB"),

    dict(country=global_c, name="Fed Funds Rate (USA)",
         unit="%", frequency="monthly", category="rates_monetary",
         connector_type="API", api_provider="fred", api_serie_id="FEDFUNDS",
         source_url="https://fred.stlouisfed.org",
         description="Tasa Fed Funds rate USA - FED/FRED. Afecta SOFR y costo deuda USD"),

    dict(country=global_c, name="SOFR (Secured Overnight Financing Rate)",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="fred", api_serie_id="SOFR",
         source_url="https://fred.stlouisfed.org",
         description="SOFR USA. Supuesto Exagon: 5.33%. Base de deuda en USD"),

    dict(country=global_c, name="US Treasury 10Y",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="fred", api_serie_id="DGS10",
         source_url="https://fred.stlouisfed.org",
         description="US Treasury 10 años - FED/FRED. Tasa libre de riesgo global"),

    dict(country=global_c, name="US Treasury 2Y",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="fred", api_serie_id="DGS2",
         source_url="https://fred.stlouisfed.org",
         description="US Treasury 2 años - diferencial con 10Y para curva invertida"),

    dict(country=global_c, name="Spread Curva 10Y-2Y USA",
         unit="%", frequency="daily", category="rates_monetary",
         connector_type="API", api_provider="fred", api_serie_id="T10Y2Y",
         source_url="https://fred.stlouisfed.org",
         description="Diferencial 10Y - 2Y USA. Indicador de recesión anticipada"),

    # ══════════════════════════════════════════════════════════════════════════
    # 4. TIPO DE CAMBIO
    #    Crítico: TRM COP/USD para convertir CAPEX y deuda USD a COP
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="TRM (COP/USD)",
         unit="COP/USD", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="banrep", api_serie_id="TRM",
         source_url="https://suameca.banrep.gov.co",
         description="Tasa Representativa del Mercado. CRÍTICA: convierte CAPEX USD a COP. Supuesto Exagon 2026: 4.294"),

    dict(country=colombia, name="Reservas Internacionales CO",
         unit="USD M", frequency="monthly", category="fx_rates",
         connector_type="API", api_provider="banrep", api_serie_id="ReservasInt",
         source_url="https://suameca.banrep.gov.co",
         description="Reservas internacionales Colombia - BanRep"),

    dict(country=colombia, name="EMBI Colombia (Riesgo País)",
         unit="bps", frequency="daily", category="fx_rates",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.banrep.gov.co",
         description="EMBI Colombia - prima de riesgo país. Supuesto modelos: 300 bps (3.0%). Entra en WACC como CRP"),

    dict(country=mexico, name="USD/MXN",
         unit="MXN/USD", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="banxico", api_serie_id="SF43718",
         source_url="https://www.banxico.org.mx/SieAPIRest",
         description="Tipo de cambio peso mexicano por dólar - Banxico"),

    dict(country=brasil, name="USD/BRL",
         unit="BRL/USD", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="bcb", api_serie_id="1",
         source_url="https://api.bcb.gov.br",
         description="Tipo de cambio real brasileño por dólar - BCB"),

    dict(country=global_c, name="EUR/USD",
         unit="EUR/USD", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="fred", api_serie_id="DEXUSEU",
         source_url="https://fred.stlouisfed.org",
         description="Tipo de cambio Euro/Dólar - FED/FRED"),

    dict(country=global_c, name="DXY (Índice Dólar)",
         unit="Índice", frequency="daily", category="fx_rates",
         connector_type="API", api_provider="fred", api_serie_id="DTWEXBGS",
         source_url="https://fred.stlouisfed.org",
         description="DXY Broad Dollar Index - FED/FRED. Fortaleza global del dólar"),

    dict(country=brasil, name="EMBI Brasil",
         unit="bps", frequency="daily", category="fx_rates",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.morganmarkets.com",
         description="EMBI Brasil - riesgo país JPMorgan"),

    dict(country=mexico, name="EMBI México",
         unit="bps", frequency="daily", category="fx_rates",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.morganmarkets.com",
         description="EMBI México - riesgo país JPMorgan"),

    # ══════════════════════════════════════════════════════════════════════════
    # 5. SECTOR EXTERNO
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="Balanza Comercial CO",
         unit="USD M", frequency="monthly", category="external",
         connector_type="API", api_provider="banrep", api_serie_id="BalCom",
         source_url="https://suameca.banrep.gov.co",
         description="Balanza comercial Colombia (Exportaciones - Importaciones) - BanRep"),

    dict(country=colombia, name="Cuenta Corriente CO (% PIB)",
         unit="% PIB", frequency="annual", category="external",
         connector_type="API", api_provider="fred", api_serie_id="BNCABXOKA646NWDBGDP",
         source_url="https://fred.stlouisfed.org",
         description="Cuenta corriente Colombia % PIB - World Bank/FRED"),

    dict(country=colombia, name="IED CO (% PIB)",
         unit="% PIB", frequency="annual", category="external",
         connector_type="API", api_provider="fred", api_serie_id="BXKLTDINV646NWDBGDP",
         source_url="https://fred.stlouisfed.org",
         description="Inversión Extranjera Directa Colombia % PIB - World Bank/FRED"),

    dict(country=colombia, name="Remesas CO",
         unit="USD M", frequency="quarterly", category="external",
         connector_type="API", api_provider="banrep", api_serie_id="Remesas",
         source_url="https://suameca.banrep.gov.co",
         description="Remesas recibidas Colombia - BanRep"),

    dict(country=colombia, name="Términos de Intercambio CO",
         unit="Índice", frequency="monthly", category="external",
         connector_type="API", api_provider="banrep", api_serie_id="TermIntCom",
         source_url="https://suameca.banrep.gov.co",
         description="Índice de términos de intercambio Colombia - BanRep"),

    dict(country=brasil, name="Cuenta Corriente BR (% PIB)",
         unit="% PIB", frequency="annual", category="external",
         connector_type="API", api_provider="fred", api_serie_id="BNCABXOKA076NWDBGDP",
         source_url="https://fred.stlouisfed.org",
         description="Cuenta corriente Brasil % PIB - World Bank/FRED"),

    dict(country=mexico, name="Cuenta Corriente MX (% PIB)",
         unit="% PIB", frequency="annual", category="external",
         connector_type="API", api_provider="fred", api_serie_id="BNCABXOKA484NWDBGDP",
         source_url="https://fred.stlouisfed.org",
         description="Cuenta corriente México % PIB - World Bank/FRED"),

    # ══════════════════════════════════════════════════════════════════════════
    # 6. SECTOR FISCAL
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="Déficit Fiscal CO (% PIB)",
         unit="% PIB", frequency="annual", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.minhacienda.gov.co",
         description="Déficit del Sector Público Consolidado - MinHacienda"),

    dict(country=colombia, name="Deuda Pública CO (% PIB)",
         unit="% PIB", frequency="annual", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.minhacienda.gov.co",
         description="Deuda pública bruta Colombia % PIB - MinHacienda"),

    dict(country=colombia, name="Recaudo DIAN CO",
         unit="COP B", frequency="monthly", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dian.gov.co/dian/cifras",
         description="Recaudo tributario total DIAN Colombia"),

    dict(country=colombia, name="Tasa Impositiva Corporativa CO (Renta)",
         unit="%", frequency="annual", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dian.gov.co",
         description="Tasa impuesto de renta corporativo Colombia. Valor actual: 35% (modelos Exagon/Ruitoque)"),

    dict(country=colombia, name="ICA CO (Impuesto Industria y Comercio)",
         unit="%", frequency="annual", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.dian.gov.co",
         description="Tasa ICA sobre ingresos. Usado en modelos: 0.6%"),

    dict(country=brasil, name="Deuda Pública BR (% PIB)",
         unit="% PIB", frequency="monthly", category="fiscal",
         connector_type="API", api_provider="bcb", api_serie_id="13762",
         source_url="https://api.bcb.gov.br",
         description="Deuda pública líquida Brasil % PIB - BCB"),

    dict(country=colombia, name="CDS Colombia 5Y",
         unit="bps", frequency="daily", category="fiscal",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.economiayfinanzas.gov.co",
         description="Credit Default Swaps Colombia 5 años - referencia de riesgo soberano"),

    # ══════════════════════════════════════════════════════════════════════════
    # 7. SECTOR ENERGÉTICO
    #    Variables del mercado eléctrico colombiano y commodities globales
    # ══════════════════════════════════════════════════════════════════════════
    dict(country=colombia, name="Precio de Bolsa Nacional (XM)",
         unit="COP/kWh", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="PrecBolNac",
         source_url="https://servapibi.xm.com.co/daily",
         description="Precio spot energía eléctrica Colombia. Supuesto Exagon 2026: 271 COP/kWh"),

    dict(country=colombia, name="Índice Mc (Precio contratos regulados)",
         unit="COP/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider="xm", api_serie_id="PrecPromContReg",
         source_url="https://sinergox.xm.com.co",
         description="Precio promedio contratos regulados (Mc). Datos hist: 2024=308.4, 2025=300.0, 2026=311.4 COP/kWh"),

    dict(country=colombia, name="Precio de Escasez",
         unit="COP/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider="xm", api_serie_id="PrecEscworking",
         source_url="https://sinergox.xm.com.co",
         description="Precio de escasez del sistema eléctrico colombiano - CREG/XM"),

    dict(country=colombia, name="CERE (Cargo por Confiabilidad)",
         unit="COP/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider="xm", api_serie_id="CERE",
         source_url="https://sinergox.xm.com.co",
         description="Cargo por Confiabilidad - XM/CREG"),

    dict(country=colombia, name="Demanda Energía Nacional",
         unit="GWh", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="DemaNal",
         source_url="https://servapibi.xm.com.co/daily",
         description="Demanda real de energía eléctrica nacional - XM"),

    dict(country=colombia, name="Generación Real Total CO",
         unit="GWh", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="GeneReal",
         source_url="https://servapibi.xm.com.co/daily",
         description="Generación real total del sistema eléctrico colombiano - XM"),

    dict(country=colombia, name="Generación Solar CO",
         unit="GWh", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="GeneSolar",
         source_url="https://servapibi.xm.com.co/daily",
         description="Generación solar en el sistema eléctrico - XM"),

    dict(country=colombia, name="Aportes Hídricos (% media histórica)",
         unit="%", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="AporEner",
         source_url="https://servapibi.xm.com.co/daily",
         description="Aportes hídricos % media histórica. Indicador de presión en precio bolsa - XM"),

    dict(country=colombia, name="Volumen Útil Diario Embalses",
         unit="%", frequency="daily", category="energy",
         connector_type="API", api_provider="xm", api_serie_id="VolUtilDiari",
         source_url="https://servapibi.xm.com.co/daily",
         description="Volumen útil embalses hidroeléctricos % capacidad total - XM"),

    dict(country=colombia, name="Capacidad Instalada Solar CO",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.upme.gov.co",
         description="Capacidad solar instalada acumulada Colombia - UPME/XM"),

    dict(country=colombia, name="Capacidad Instalada Renovable CO",
         unit="MW", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.upme.gov.co",
         description="Capacidad total FNCER instalada Colombia - UPME"),

    dict(country=colombia, name="Precio PPA Bilateral CO (Mc+spread)",
         unit="COP/kWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://sinergox.xm.com.co",
         description="Precio PPA contratos bilaterales. Exagon: 300 COP/kWh, Ruitoque: 295 COP/kWh. Indexado IPP"),

    dict(country=global_c, name="Precio I-REC (Cert. Energía Renovable)",
         unit="USD/MWh", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.traderec.com",
         description="Precio I-REC Colombia. Supuesto Exagon: 1.5 USD/MWh activo"),

    dict(country=global_c, name="Precio Carbon Offset CO2",
         unit="COP/tCO2", frequency="monthly", category="energy",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.ecosystemmarketplace.com",
         description="Precio carbon offsets. Supuesto modelos: 15.000 COP/tCO2 (creciendo con IPC)"),

    dict(country=global_c, name="WTI Crude Oil",
         unit="USD/barrel", frequency="daily", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="DCOILWTICO",
         source_url="https://fred.stlouisfed.org",
         description="Precio petróleo WTI - EIA/FRED"),

    dict(country=global_c, name="Brent Crude Oil",
         unit="USD/barrel", frequency="daily", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="DCOILBRENTEU",
         source_url="https://fred.stlouisfed.org",
         description="Precio petróleo Brent - EIA/FRED"),

    dict(country=global_c, name="Henry Hub Natural Gas",
         unit="USD/MMBtu", frequency="daily", category="energy",
         connector_type="API", api_provider="fred", api_serie_id="DHHNGSP",
         source_url="https://fred.stlouisfed.org",
         description="Precio gas natural Henry Hub - EIA/FRED"),

    # ══════════════════════════════════════════════════════════════════════════
    # 8. FINANZAS CORPORATIVAS — UNERGY / PROYECTOS SOLARES
    #    Basado en parámetros exactos de los modelos Exagon y Ruitoque
    # ══════════════════════════════════════════════════════════════════════════

    # 8a. WACC y Costo de Capital
    dict(country=colombia, name="Beta Sector Energía Renovable (Damodaran)",
         unit="ratio", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://pages.stern.nyu.edu/~adamodar/",
         description="Beta apalancado sector power/renewable energy. Supuesto modelos: 1.11"),

    dict(country=colombia, name="Prima de Mercado CO (Rm - Rf)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://pages.stern.nyu.edu/~adamodar/",
         description="Market return Colombia. Supuesto Exagon/Ruitoque: 12% anual"),

    dict(country=colombia, name="Ke (Costo del Equity) Proyectos Solares CO",
         unit="%", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Calculado: CAPM = Rf + Beta*(Rm - Rf) + EMBI",
         description="Ke = TES10Y + Beta*(Rm-Rf) + EMBI. Resultado modelos: 12.099%. Actualizar con TES y EMBI vigentes"),

    dict(country=colombia, name="Kd (Costo de Deuda) Proyectos Solares CO",
         unit="%", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="IBR + Spread bancario",
         description="Kd deuda proyectos. Exagon (COP): 8.18%, SOFR+3.8% (USD). Ruitoque: IBR+spread. Actualizar con IBR"),

    dict(country=colombia, name="WACC Proyectos Solares CO",
         unit="%", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Calculado: Ke*(E/V) + Kd*(1-t)*(D/V)",
         description="WACC = Ke*(E/V) + Kd*(1-t)*(D/V). Exagon: 9.36% (D/V=70%, t=35%). Ruitoque: 10.02%"),

    dict(country=colombia, name="Deuda/Capital (D/E) Target Proyectos Solares",
         unit="ratio", frequency="quarterly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Apalancamiento financiero target. Ambos modelos: 70% Deuda / 30% Equity"),

    dict(country=colombia, name="DSCR (Deuda Service Coverage Ratio)",
         unit="ratio", frequency="quarterly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="DSCR mínimo para covenants bancarios. Threshold modelos: 1.2x"),

    # 8b. Tax Benefits Ley 1715 / 2099
    dict(country=colombia, name="Tax Benefit CAPEX Renovable (% deducible)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.upme.gov.co/ley1715",
         description="Deducción fiscal sobre el CAPEX de proyectos FNCER - Ley 1715/2099. Modelos: 50% del CAPEX"),

    dict(country=colombia, name="Depreciación Acelerada Solar (años)",
         unit="años", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.upme.gov.co/ley1715",
         description="Período depreciación acelerada activos solares - Ley 1715. Tasa máx anual: 33.33% (3 años = ambos modelos)"),

    dict(country=colombia, name="Período Tax Benefits (años elegibles)",
         unit="años", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="https://www.upme.gov.co/ley1715",
         description="Ventana para aplicar beneficios tributarios post-inicio-operación. Modelos: 5 años"),

    dict(country=colombia, name="Renta Gravable Anual Tax Partner",
         unit="COP B", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Renta anual gravable del Tax Partner. Ruitoque supuesto: 26.000 M COP anuales"),

    # 8c. Estructura Financiera y Rentabilidad
    dict(country=colombia, name="IRR Proyecto Solar (sin deuda, sin Tax)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="TIR del proyecto sin deuda ni beneficios tributarios. Exagon 13 MF: 14.56%"),

    dict(country=colombia, name="IRR con Tax Benefits Ley 1715",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="TIR del inversionista incluyendo beneficios tributarios. Exagon: 16.32%, Ruitoque Tax Partner: 99%"),

    dict(country=colombia, name="IRR con Deuda (Leveraged IRR)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="TIR con estructura de deuda 70/30. Exagon: 14.50%, Mejora por escudo fiscal"),

    dict(country=colombia, name="Retorno Tax Partner Unergy (% EA)",
         unit="%", frequency="quarterly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Retorno efectivo anual ofrecido al Tax Partner. Histórico portafolio: 9.5%-13%"),

    dict(country=colombia, name="Spread Tax Partner vs CDT 360d",
         unit="bps", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Calculado: Retorno TP - CDT 360",
         description="Prima de retorno Tax Partner sobre CDT a 360 días. Diferencial clave para fundraising"),

    # 8d. CAPEX / OPEX Solar
    dict(country=colombia, name="CAPEX Solar Total (USD por proyecto)",
         unit="USD", frequency="quarterly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="CAPEX total por MiniGranja Solar (1 MW). Exagon: 1.034 M USD/MG, Total 13MG: 13.44 M USD"),

    dict(country=colombia, name="CAPEX Solar (USD/kWp instalado)",
         unit="USD/kWp", frequency="quarterly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy / IRENA",
         description="Costo por kWp pico instalado. Referencia: 1.320 kWp/MG → CAPEX ÷ kWp total"),

    dict(country=colombia, name="Factor de Planta Solar CO (P90)",
         unit="%", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="XM / IDEAM / Modelos Planta",
         description="Factor de planta simulación P90 con mejora 10% por tracker. Variable determinante de ingresos"),

    dict(country=colombia, name="Potencia Instalada por MiniGranja (kWp)",
         unit="kWp", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Potencia pico instalada por MiniGranja. Valor estándar modelos: 1.320 kWp / 1.000 kVA AC"),

    dict(country=colombia, name="Degradación Panel Solar (% anual)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="NREL / Fabricante",
         description="Factor de degradación anual de paneles. Modelos Exagon/Ruitoque: 0.35% anual"),

    dict(country=colombia, name="OPEX O&M Solar (% CAPEX anual)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Costos O&M como % del CAPEX anual. Modelos: O&O 3.8% + Seguros 0.19-0.41%"),

    dict(country=colombia, name="Costos Regulatorios Generación (COP/kWh)",
         unit="COP/kWh", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="CREG / XM",
         description="Cargos regulatorios CGM 6 + Representación 6 + Generación XM 9.5 = 21.5 COP/kWh (~9.98 en 2026)"),

    dict(country=colombia, name="Vida Útil Proyectos Solares CO",
         unit="años", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Interno Unergy",
         description="Vida útil esperada por portfolio. Modelos: 30 años (360 meses). Fin portafolio: 2057"),

    dict(country=colombia, name="Plazo Préstamo Bancario Solar CO",
         unit="años", frequency="annual", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Banca Colombia",
         description="Plazo típico crédito bancario para proyectos solares. Modelos: 10 años con 12 meses de gracia"),

    dict(country=colombia, name="SPV Fee Administración (SMMLV/mes)",
         unit="SMMLV", frequency="monthly", category="corporate_finance",
         connector_type="MANUAL", api_provider=None, api_serie_id=None,
         source_url="Fiduciarias Colombia",
         description="Costo administración SPV (fideicomiso). Exagon: 16 SMMLV/mes, Ruitoque: 10 SMMLV/mes"),
         
    # 8e. Key Financials Extractables de Excel
    dict(country=colombia, name="WACC - Costo Promedio de Capital",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="EXCEL", api_provider=None, api_serie_id="WACC",
         source_url="Modelos Excel Internos",
         description="Weighted Average Cost of Capital calculado del proyecto"),
         
    dict(country=colombia, name="TIR Proyecto (IRR)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="EXCEL", api_provider=None, api_serie_id="IRR",
         source_url="Modelos Excel Internos",
         description="Tasa Interna de Retorno (TIR) proyectada para el inversionista"),
         
    dict(country=colombia, name="Costo de la Deuda (Kd)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="EXCEL", api_provider=None, api_serie_id="Kd",
         source_url="Modelos Excel Internos",
         description="Costo de Deuda estipulado"),
         
    dict(country=colombia, name="Costo del Equity (Ke)",
         unit="%", frequency="annual", category="corporate_finance",
         connector_type="EXCEL", api_provider=None, api_serie_id="Ke",
         source_url="Modelos Excel Internos",
         description="Costo del Capital Propio / Equity"),
         
    dict(country=colombia, name="Tarifa PPA (Precio Venta de Energía)",
         unit="COP/kWh", frequency="annual", category="corporate_finance",
         connector_type="EXCEL", api_provider=None, api_serie_id="PPA Price",
         source_url="Modelos Excel Internos",
         description="Tarifa PPA pactada base del proyecto, sin indexación"),
]

# ── Upsert en DB ──────────────────────────────────────────────────────────────
added = 0
updated = 0

for v_data in ALL_VARIABLES:
    country_obj = v_data.pop('country')
    name = v_data['name']
    
    existing = session.query(MacroVariable).filter_by(
        country_id=country_obj.id, name=name
    ).first()
    
    if existing:
        for k, val in v_data.items():
            setattr(existing, k, val)
        updated += 1
        print(f"  [UPDATE] {name[:60]}")
    else:
        mv = MacroVariable(country_id=country_obj.id, **v_data)
        session.add(mv)
        added += 1
        print(f"  [ADD]    {name[:60]}")

session.commit()
session.close()

print(f"\n✅ Seed v3 completado: {added} nuevas | {updated} actualizadas")

# Verificación por categoría
from models.db import engine
from sqlalchemy import text
with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM dim_variable WHERE is_active=1")).fetchone()[0]
    print(f"   Total variables activas: {total}")
    cats = conn.execute(text("SELECT category, COUNT(*) as n FROM dim_variable WHERE is_active=1 GROUP BY category ORDER BY n DESC")).fetchall()
    for cat, n in cats:
        print(f"   {cat or 'N/A':30s}: {n}")
