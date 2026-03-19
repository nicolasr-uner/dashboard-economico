"""
bcb.py — Conector para el Banco Central do Brasil (SGS API).
Sin autenticación. Endpoint público JSON.
"""
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

BCB_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_id}/dados"


class BCBConnector(BaseConnector):

    def __init__(self):
        super().__init__("bcb")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie del BCB SGS.
        start_date / end_date en formato 'YYYY-MM-DD'.
        Retorna DataFrame ['date', 'value'].
        """
        # BCB API espera DD/MM/YYYY
        try:
            sd = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")
            ed = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            logger.error(f"[bcb] Formato de fecha inválido: {start_date} / {end_date}")
            return self.empty_df()

        url = BCB_BASE.format(serie_id=serie_id)
        params = {"formato": "json", "dataInicial": sd, "dataFinal": ed}

        try:
            data = self._get(url, params=params)
            return self._parse_response(data)
        except Exception as e:
            logger.error(f"[bcb] fallo descargando serie {serie_id}: {e}")
            return self.empty_df()

    def _parse_response(self, data) -> pd.DataFrame:
        """Parsear respuesta de BCB: [{"data": "DD/MM/YYYY", "valor": "12.34"}, ...]"""
        if not isinstance(data, list):
            return self.empty_df()

        records = []
        for item in data:
            fecha_str = item.get('data', '')
            valor_str = item.get('valor', '')
            if not fecha_str or not valor_str:
                continue
            try:
                fecha = datetime.strptime(fecha_str, "%d/%m/%Y")
                valor = float(str(valor_str).replace(',', '.'))
                records.append({'date': fecha, 'value': valor})
            except (ValueError, TypeError):
                continue

        if not records:
            return self.empty_df()

        df = pd.DataFrame(records)
        df = df.sort_values('date').reset_index(drop=True)
        logger.info(f"[bcb] {len(df)} registros descargados.")
        return df
