"""
bcb_focus.py — Conector para el sistema Focus del Banco Central do Brasil.
Expectativas de mercado (medianas de analistas) vía OData.
Sin autenticación.
Docs: https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/swagger-ui3
"""
import logging
import httpx
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BCB_FOCUS_BASE = (
    "https://olinda.bcb.gov.br/olinda/servico/Expectativas/"
    "versao/v1/odata/ExpectativasMercadoAnuais"
)
TIMEOUT = 20.0

# Indicadores Focus disponibles → nombre en la plataforma
BCB_FOCUS_MAP = {
    "IPCA":                        "IPCA BR (var. anual)",
    "PIB Total":                   "PIB Trimestral BR (var. %)",
    "Taxa de juros - Over/Selic":  "Tasa Selic BR",
    "Câmbio":                      "USD/BRL",
    "IGP-M":                       "IGP-M BR",
    "Balança comercial":           "Balança Comercial BR",
}


def fetch_focus_expectations(indicator: str, reference_year: int) -> dict:
    """
    Obtiene la expectativa mediana más reciente del Focus BCB para un indicador y año.

    Args:
        indicator: Nombre del indicador Focus (ej. 'IPCA', 'PIB Total')
        reference_year: Año de referencia de la proyección (ej. 2025)

    Returns:
        dict con: {
            'median': float,
            'mean': float,
            'std': float,
            'survey_date': str (ISO date),
            'source': 'Focus BCB (mediana)'
        }
        o {} si falla
    """
    # Fecha de corte: buscar las últimas 30 encuestas
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    # Construir filtro OData
    # Suavizado='S' = expectativas suavizadas (descontam outliers)
    indicator_escaped = indicator.replace("'", "''")
    odata_filter = (
        f"Indicador eq '{indicator_escaped}' and "
        f"DataReferencia eq '{reference_year}' and "
        f"Suavizado eq 'S' and "
        f"Data ge '{cutoff}'"
    )
    params = {
        "$filter": odata_filter,
        "$select": "Indicador,Data,DataReferencia,Mediana,Media,DesvioPadrao",
        "$orderby": "Data desc",
        "$top": "5",
        "$format": "json",
    }

    try:
        resp = httpx.get(BCB_FOCUS_BASE, params=params, timeout=TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[bcb_focus] Error fetching {indicator} {reference_year}: {e}")
        return {}

    try:
        values = data.get("value", [])
        if not values:
            # Retry without Suavizado filter
            params["$filter"] = (
                f"Indicador eq '{indicator_escaped}' and "
                f"DataReferencia eq '{reference_year}' and "
                f"Data ge '{cutoff}'"
            )
            resp2 = httpx.get(BCB_FOCUS_BASE, params=params, timeout=TIMEOUT, follow_redirects=True)
            resp2.raise_for_status()
            values = resp2.json().get("value", [])

        if not values:
            logger.warning(f"[bcb_focus] No data for {indicator} {reference_year}")
            return {}

        latest = values[0]  # Most recent survey (ordered desc)
        return {
            "median": float(latest.get("Mediana") or 0),
            "mean":   float(latest.get("Media") or 0),
            "std":    float(latest.get("DesvioPadrao") or 0),
            "survey_date": str(latest.get("Data", "")),
            "source": "Focus BCB (mediana)",
        }

    except Exception as e:
        logger.error(f"[bcb_focus] Parse error for {indicator} {reference_year}: {e}")
        return {}


def fetch_all_focus_expectations(years: list = None) -> list:
    """
    Obtiene todas las expectativas Focus disponibles para los años especificados.

    Returns:
        Lista de dicts: [{
            'indicator': 'IPCA',
            'variable_name': 'IPCA BR (var. anual)',
            'year': 2025,
            'median': 4.8,
            'survey_date': '2025-02-14',
        }, ...]
    """
    if years is None:
        current_year = datetime.now().year
        # BCB Focus tiene datos hasta el año en curso y el siguiente como máximo
        # Usar año anterior y año actual para mayor disponibilidad
        years = [current_year - 1, current_year]

    results = []
    for indicator, var_name in BCB_FOCUS_MAP.items():
        for yr in years:
            exp = fetch_focus_expectations(indicator, yr)
            if exp:
                results.append({
                    "indicator": indicator,
                    "variable_name": var_name,
                    "year": yr,
                    "median": exp.get("median"),
                    "mean": exp.get("mean"),
                    "std": exp.get("std"),
                    "survey_date": exp.get("survey_date"),
                    "source": exp.get("source"),
                })

    logger.info(f"[bcb_focus] Total expectations fetched: {len(results)}")
    return results
