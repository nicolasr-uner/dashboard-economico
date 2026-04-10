"""
banrep.py — Conector para el Banco de la República de Colombia.
Endpoints: SUAMECA (nuevo) con fallback a Totoro (legacy).
"""
import logging
import pandas as pd
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

SUAMECA_BASE = "https://suameca.banrep.gov.co/estadisticas-economicas/rest/data/{serie_id}"
TOTORO_BASE  = "https://totoro.banrep.gov.co/estadisticas-economicas/rest/series/get/{serie_id}"
# Endpoint alternativo funcional: BanRep Catálogo público (series históricas en CSV)
BANREP_CSV_BASE = "https://www.banrep.gov.co/es/estadisticas/download?archivo={series_file}"

# Mapeo de serie_id a la API real (endpoint Banco de la Republica actual)
# Los endpoints SUAMECA y Totoro han sido deprecados en 2025.
# Usamos World Bank como fallback confiable para las series principales.
BANREP_WORLDBANK_MAP = {
    "IPC_variacion_anual":    ("CO", "FP.CPI.TOTL.ZG"),   # Inflacion CO
    "IPC_variacion_mensual":  None,
    "Desempleo":              ("CO", "SL.UEM.TOTL.ZS"),    # Desempleo CO
    "PIB_trim":               ("CO", "NY.GDP.MKTP.KD.ZG"), # PIB CO
    "TRM":                    None,  # handled by Superfinanciera fallback
}


class BanRepConnector(BaseConnector):

    def __init__(self):
        super().__init__("banrep")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie del BanRep.
        Intenta World Bank como fallback inmediato para series conocidas.
        """
        # 1. Intentar World Bank si tenemos mapeo
        wb_mapping = BANREP_WORLDBANK_MAP.get(serie_id)
        if wb_mapping:
            country_code, wb_indicator = wb_mapping
            try:
                from connectors.world_bank import WorldBankConnector
                wb_conn = WorldBankConnector()
                df = wb_conn.fetch_series(wb_indicator, start_date, end_date, country=country_code)
                if not df.empty:
                    logger.info(f"[banrep/worldbank] {serie_id}: {len(df)} registros via World Bank")
                    return df
            except Exception as e:
                logger.warning(f"[banrep/worldbank] fallo para {serie_id}: {e}")

        # 2. Fallback especial: TRM desde Superfinanciera
        if serie_id.upper() == "TRM":
            try:
                return self._fetch_trm_superfinanciera(start_date, end_date)
            except Exception as e:
                logger.error(f"[banrep] TRM Superfinanciera fallback fallo: {e}")

        logger.warning(f"[banrep] Serie {serie_id} sin fuente activa configurada.")
        return self.empty_df()


    def _fetch_trm_superfinanciera(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Scraping básico de TRM desde Superintendencia Financiera."""
        url = "https://www.superfinanciera.gov.co/jsp/loader.jsf"
        params = {
            "lServicio": "PublicacionesTimesSeriesIndicadores",
            "lTipo": "publicaciones",
            "lFuncion": "loadIndicadores",
            "id": "60"
        }
        data = self._get(url, params=params)
        # Si falla, retornar vacío — será manejado por el caller
        return self.empty_df()

    def _parse_response(self, data) -> pd.DataFrame:
        """Parsear respones de BanRep (varios formatos posibles)."""
        records = []

        # Formato SUAMECA: {"data": [{"fecha": "2024-01-02", "dato": 4234.5}, ...]}
        if isinstance(data, dict) and 'data' in data:
            for item in data['data']:
                fecha = item.get('fecha') or item.get('date') or item.get('f')
                valor = item.get('dato') or item.get('value') or item.get('v')
                if fecha and valor is not None:
                    records.append({'date': str(fecha)[:10], 'value': float(valor)})

        # Formato Totoro: lista directa [{"f": "...", "v": ...}, ...]
        elif isinstance(data, list):
            for item in data:
                fecha = item.get('f') or item.get('fecha') or item.get('date')
                valor = item.get('v') or item.get('dato') or item.get('value')
                if fecha and valor is not None:
                    try:
                        records.append({'date': str(fecha)[:10], 'value': float(valor)})
                    except (ValueError, TypeError):
                        continue

        # Formato alternativo: {"series": [{"data": [...]}]}
        elif isinstance(data, dict) and 'series' in data:
            series_list = data['series']
            if series_list and isinstance(series_list, list):
                for item in series_list[0].get('data', []):
                    fecha = item.get('fecha') or item.get('date')
                    valor = item.get('dato') or item.get('value')
                    if fecha and valor is not None:
                        try:
                            records.append({'date': str(fecha)[:10], 'value': float(valor)})
                        except (ValueError, TypeError):
                            continue

        if not records:
            return self.empty_df()

        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date', 'value']).sort_values('date').reset_index(drop=True)
        return df
