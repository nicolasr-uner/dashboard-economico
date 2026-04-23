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
            # Fallback: algunas series antiguas del BCB (Selic, USD/BRL, CDI)
            # no soportan fecha+formato conjuntos → usar /ultimos/N.
            # BCB SGS limita N a ~50-99; probamos valores decrecientes.
            logger.warning(f"[bcb] fallo con rango de fechas para serie {serie_id}, "
                           f"intentando /ultimos fallback: {e}")
            for n in (99, 75, 50, 30, 10):
                try:
                    url_ult = BCB_BASE.format(serie_id=serie_id) + f"/ultimos/{n}"
                    data2 = self._get(url_ult, params={"formato": "json"})
                    df = self._parse_response(data2)
                    if not df.empty:
                        df['date'] = pd.to_datetime(df['date'])
                        inicio = pd.to_datetime(start_date)
                        fim    = pd.to_datetime(end_date)
                        mask = (df['date'] >= inicio) & (df['date'] <= fim)
                        df_filtered = df[mask].reset_index(drop=True)
                        logger.info(
                            f"[bcb] fallback /ultimos/{n}: {len(df_filtered)} registros "
                            f"(de {len(df)} totales) para serie {serie_id}"
                        )
                        return df_filtered if not df_filtered.empty else df
                except Exception as e2:
                    logger.debug(f"[bcb] /ultimos/{n} falló para serie {serie_id}: {e2}")
                    continue
            logger.error(f"[bcb] fallo total serie {serie_id}: todos los /ultimos/N fallaron")
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
