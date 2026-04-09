"""
world_bank.py — Conector para la API pública del Banco Mundial (sin autenticación).
Endpoint: https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_id}?format=json
"""
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

WB_BASE = "https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_id}"

# Mapa de código de país (ISO2) para la API del World Bank
COUNTRY_CODE_MAP = {
    'CO': 'COL',  # Colombia
    'MX': 'MEX',  # México
    'BR': 'BRA',  # Brasil
    'EC': 'ECU',  # Ecuador
}


class WorldBankConnector(BaseConnector):

    def __init__(self):
        super().__init__("world_bank")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie del World Bank.
        serie_id formato: '{country_code}:{indicator_id}' ej. 'CO:NY.GDP.PCAP.CD'
        O solo '{indicator_id}' si el país se infiere.
        """
        # Parsear el serie_id — puede ser 'CO:NY.GDP.PCAP.CD' o indicador solo
        if ':' in serie_id:
            country_code_iso2, indicator_id = serie_id.split(':', 1)
        else:
            # Intentar inferir el país desde el nombre de la variable (fallback)
            country_code_iso2 = 'CO'  # default Colombia
            indicator_id = serie_id

        country_code_iso3 = COUNTRY_CODE_MAP.get(country_code_iso2.upper(), country_code_iso2)

        # World Bank acepta rango de años
        try:
            start_year = datetime.strptime(start_date, "%Y-%m-%d").year
            end_year   = datetime.strptime(end_date,   "%Y-%m-%d").year
        except ValueError:
            start_year, end_year = 2000, datetime.now().year

        url = WB_BASE.format(country_code=country_code_iso3, indicator_id=indicator_id)
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": "500",
        }

        all_records = []
        page = 1

        while True:
            params["page"] = page
            try:
                data = self._get(url, params=params)
                if not isinstance(data, list) or len(data) < 2:
                    break

                meta = data[0]
                records = data[1]

                if not records:
                    break

                for item in records:
                    year        = item.get('date')
                    value_raw   = item.get('value')
                    if year and value_raw is not None:
                        try:
                            all_records.append({
                                'date': datetime(int(year), 12, 31),
                                'value': float(value_raw)
                            })
                        except (ValueError, TypeError):
                            continue

                total_pages = meta.get('pages', 1)
                if page >= total_pages:
                    break
                page += 1

            except Exception as e:
                logger.warning(f"[world_bank] {indicator_id} página {page}: {e}")
                break

        if not all_records:
            logger.warning(f"[world_bank] {indicator_id} sin datos para {country_code_iso3}")
            return self.empty_df()

        df = pd.DataFrame(all_records)
        df = df.dropna(subset=['value'])
        df = df.sort_values('date').reset_index(drop=True)
        logger.info(f"[world_bank] {country_code_iso3}/{indicator_id}: {len(df)} registros")
        return df
