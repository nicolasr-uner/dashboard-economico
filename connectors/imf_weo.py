"""
imf_weo.py — Conector para el IMF World Economic Outlook DataMapper API.
Sin autenticación. Devuelve datos históricos + proyecciones por país e indicador.
Docs: https://www.imf.org/external/datamapper/api/help
"""
import logging
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)

IMF_API_BASE = "https://www.imf.org/external/datamapper/api/v1"

# Indicadores disponibles en WEO DataMapper
IMF_INDICATORS = {
    "NGDP_RPCH":   "PIB crecimiento real (%)",
    "PCPIPCH":     "Inflación promedio (%)",
    "LUR":         "Desempleo (%)",
    "BCA_NGDPD":   "Cuenta corriente (% PIB)",
    "GGXCNL_NGDP": "Balance fiscal neto (% PIB)",
    "GGXWDG_NGDP": "Deuda bruta gobierno (% PIB)",
    "PPPGDP":      "PIB PPP (billones USD)",
}

# Mapeo de código ISO3 a nombre de país
IMF_COUNTRIES = {
    "COL": "Colombia",
    "MEX": "México",
    "BRA": "Brasil",
    "ECU": "Ecuador",
}

TIMEOUT = 20.0


def fetch_imf_projections(indicator: str, imf_country_code: str,
                           horizon_years: list = None) -> dict:
    """
    Obtiene proyecciones del IMF WEO para un indicador y país.

    Args:
        indicator: Código WEO (ej. 'NGDP_RPCH')
        imf_country_code: Código ISO3 (ej. 'COL')
        horizon_years: Lista de años futuros deseados (default: año actual + 2 siguientes)

    Returns:
        dict {year_int: value_float} solo para años de proyección (futuros)
    """
    if horizon_years is None:
        current_year = datetime.now().year
        horizon_years = [current_year, current_year + 1, current_year + 2]

    url = f"{IMF_API_BASE}/data/{indicator}/{imf_country_code}"
    try:
        resp = httpx.get(url, timeout=TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[imf_weo] Error fetching {indicator}/{imf_country_code}: {e}")
        return {}

    try:
        # Estructura opción A: {"values": {"COL": {"2020": 1.6, ...}}}
        # Estructura opción B: {"values": {"NGDP_RPCH": {"COL": {"2020": 1.6, ...}}}}
        values_root = data.get("values", {})
        # Detectar cuál es la estructura
        if imf_country_code in values_root:
            # opción A
            country_values = values_root.get(imf_country_code, {})
        elif indicator in values_root:
            # opción B
            country_values = values_root.get(indicator, {}).get(imf_country_code, {})
        else:
            # Intentar iterar el primer nivel para encontrar el país
            country_values = {}
            for k, v in values_root.items():
                if isinstance(v, dict) and imf_country_code in v:
                    country_values = v[imf_country_code]
                    break
                elif k == imf_country_code and isinstance(v, dict):
                    country_values = v
                    break

        if not country_values:
            logger.warning(f"[imf_weo] No data for {indicator}/{imf_country_code}")
            return {}

        result = {}
        for yr in horizon_years:
            yr_str = str(yr)
            if yr_str in country_values and country_values[yr_str] is not None:
                try:
                    result[yr] = float(country_values[yr_str])
                except (ValueError, TypeError):
                    pass

        logger.info(f"[imf_weo] {indicator}/{imf_country_code}: {result}")
        return result

    except Exception as e:
        logger.error(f"[imf_weo] Error parsing response for {indicator}/{imf_country_code}: {e}")
        return {}


def fetch_all_imf_projections(horizon_years: list = None) -> list:
    """
    Obtiene todas las proyecciones IMF disponibles para todos los países e indicadores.

    Returns:
        Lista de dicts: [{
            'country': 'Colombia',
            'imf_code': 'COL',
            'indicator': 'NGDP_RPCH',
            'indicator_desc': '...',
            'year': 2025,
            'value': 2.8,
        }, ...]
    """
    if horizon_years is None:
        current_year = datetime.now().year
        horizon_years = [current_year, current_year + 1, current_year + 2]

    results = []
    for imf_code, country_name in IMF_COUNTRIES.items():
        for indicator, description in IMF_INDICATORS.items():
            projections = fetch_imf_projections(indicator, imf_code, horizon_years)
            for yr, val in projections.items():
                results.append({
                    'country': country_name,
                    'imf_code': imf_code,
                    'indicator': indicator,
                    'indicator_desc': description,
                    'year': yr,
                    'value': val,
                })

    logger.info(f"[imf_weo] Total projections fetched: {len(results)}")
    return results
