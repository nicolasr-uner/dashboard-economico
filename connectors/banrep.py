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


class BanRepConnector(BaseConnector):

    def __init__(self):
        super().__init__("banrep")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie del BanRep.
        start_date / end_date en formato 'YYYY-MM-DD'.
        Retorna DataFrame ['date', 'value'].
        """
        # Intentar SUAMECA primero
        try:
            url = SUAMECA_BASE.format(serie_id=serie_id)
            params = {"startDate": start_date, "endDate": end_date}
            data = self._get(url, params=params)
            df = self._parse_response(data)
            if not df.empty:
                logger.info(f"[banrep/suameca] {serie_id}: {len(df)} registros")
                return df
        except Exception as e:
            logger.warning(f"[banrep/suameca] fallo para {serie_id}: {e}. Intentando Totoro...")

        # Fallback Totoro
        try:
            # Totoro usa MM/DD/YYYY
            from datetime import datetime
            sd = datetime.strptime(start_date, "%Y-%m-%d").strftime("%m/%d/%Y")
            ed = datetime.strptime(end_date, "%Y-%m-%d").strftime("%m/%d/%Y")
            url = TOTORO_BASE.format(serie_id=serie_id)
            params = {"startDate": sd, "endDate": ed}
            data = self._get(url, params=params)
            df = self._parse_response(data)
            if not df.empty:
                logger.info(f"[banrep/totoro] {serie_id}: {len(df)} registros")
                return df
        except Exception as e:
            logger.warning(f"[banrep/totoro] fallo para {serie_id}: {e}")

        # Fallback especial: TRM desde Superfinanciera
        if serie_id.upper() == "TRM":
            try:
                return self._fetch_trm_superfinanciera(start_date, end_date)
            except Exception as e:
                logger.error(f"[banrep] TRM Superfinanciera fallback también falló: {e}")

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
