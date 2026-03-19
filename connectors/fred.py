"""
fred.py — Conector para FRED (Federal Reserve Economic Data).
Requiere API key gratuita: os.getenv('FRED_API_KEY').
Sin key, retorna DataFrame vacío con warning.
"""
import os
import logging
import pandas as pd
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


class FREDConnector(BaseConnector):

    def __init__(self):
        super().__init__("fred")
        self.api_key = os.getenv('FRED_API_KEY', '')
        try:
            import streamlit as st
            if not self.api_key and hasattr(st, 'secrets') and 'FRED_API_KEY' in st.secrets:
                self.api_key = st.secrets['FRED_API_KEY']
        except Exception:
            pass

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga observaciones de una serie FRED.
        start_date / end_date en formato 'YYYY-MM-DD'.
        """
        if not self.api_key:
            logger.warning(
                "[fred] No hay FRED_API_KEY configurada. "
                "Obtén una gratis en https://fred.stlouisfed.org/docs/api/api_key.html "
                "y agrégala como variable de entorno FRED_API_KEY."
            )
            return self.empty_df()

        params = {
            "series_id": serie_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }

        try:
            data = self._get(FRED_BASE, params=params)
            return self._parse_response(data)
        except Exception as e:
            logger.error(f"[fred] fallo descargando serie {serie_id}: {e}")
            return self.empty_df()

    def _parse_response(self, data) -> pd.DataFrame:
        """
        Parsear respuesta de FRED.
        Formato: {"observations": [{"date": "YYYY-MM-DD", "value": "12.34"}, ...]}
        """
        try:
            observations = data.get('observations', [])
            records = []
            for obs in observations:
                date_str = obs.get('date', '')
                value_str = obs.get('value', '.')
                if value_str in ('.', '', None):
                    continue
                try:
                    from datetime import datetime
                    fecha = datetime.strptime(date_str, "%Y-%m-%d")
                    valor = float(value_str)
                    records.append({'date': fecha, 'value': valor})
                except (ValueError, TypeError):
                    continue

            if not records:
                return self.empty_df()

            df = pd.DataFrame(records).sort_values('date').reset_index(drop=True)
            logger.info(f"[fred] {len(df)} observaciones descargadas.")
            return df
        except Exception as e:
            logger.error(f"[fred] Error parseando respuesta: {e}")
            return self.empty_df()
