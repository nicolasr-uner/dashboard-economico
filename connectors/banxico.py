"""
banxico.py — Conector para el Banco de México (SIE API).
Requiere token gratuito: os.getenv('BANXICO_TOKEN').
Si no está configurado, retorna DataFrame vacío con warning.
"""
import os
import logging
import pandas as pd
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

BANXICO_BASE = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie_id}/datos/{start}/{end}"


class BanxicoConnector(BaseConnector):

    def __init__(self):
        super().__init__("banxico")
        self.token = os.getenv('BANXICO_TOKEN', '')
        try:
            import streamlit as st
            if not self.token and hasattr(st, 'secrets') and 'BANXICO_TOKEN' in st.secrets:
                self.token = st.secrets['BANXICO_TOKEN']
        except Exception:
            pass

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie del SIE Banxico.
        start_date / end_date en formato 'YYYY-MM-DD'.
        """
        if not self.token:
            logger.warning(
                "[banxico] No hay BANXICO_TOKEN configurado. "
                "Obtén uno gratis en https://si.banxico.org.mx/tokens-api.html "
                "y agrégalo como variable de entorno o secreto de Streamlit."
            )
            return self.empty_df()

        url = BANXICO_BASE.format(serie_id=serie_id, start=start_date, end=end_date)
        headers = {"Bmx-Token": self.token}

        try:
            data = self._get(url, headers=headers)
            return self._parse_response(data)
        except Exception as e:
            logger.error(f"[banxico] fallo descargando serie {serie_id}: {e}")
            return self.empty_df()

    def _parse_response(self, data) -> pd.DataFrame:
        """
        Parsear respuesta de Banxico SIE.
        Formato: {"bmx": {"series": [{"datos": [{"fecha": "DD/MM/YYYY", "dato": "12.34"}]}]}}
        """
        try:
            series_list = data.get('bmx', {}).get('series', [])
            if not series_list:
                return self.empty_df()

            datos = series_list[0].get('datos', [])
            records = []
            for item in datos:
                fecha_str = item.get('fecha', '')
                valor_str = item.get('dato', '')
                if not fecha_str or valor_str in ('N/E', '', None):
                    continue
                try:
                    from datetime import datetime
                    fecha = datetime.strptime(fecha_str, "%d/%m/%Y")
                    valor = float(str(valor_str).replace(',', ''))
                    records.append({'date': fecha, 'value': valor})
                except (ValueError, TypeError):
                    continue

            if not records:
                return self.empty_df()

            df = pd.DataFrame(records).sort_values('date').reset_index(drop=True)
            logger.info(f"[banxico] {len(df)} registros descargados.")
            return df
        except Exception as e:
            logger.error(f"[banxico] Error parseando respuesta: {e}")
            return self.empty_df()
