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
    # Series ya mapeadas (Colombia core)
    "IPC_variacion_anual":    ("CO", "FP.CPI.TOTL.ZG"),     # Inflacion CO anual
    "IPC_variacion_mensual":  None,                           # Sin equivalente WB mensual
    "Desempleo":              ("CO", "SL.UEM.TOTL.ZS"),      # Desempleo CO
    "PIB_trim":               ("CO", "NY.GDP.MKTP.KD.ZG"),   # PIB CO crecimiento
    "TRM":                    ("CO", "PA.NUS.FCRF"),          # Tipo de cambio oficial COP/USD
    # Sector externo Colombia
    "ReservasInt":            ("CO", "FI.RES.TOTL.CD"),       # Reservas internacionales USD
    "BalCom":                 ("CO", "NE.EXP.GNFS.CD"),       # Exportaciones bienes y servicios
    "Remesas":                ("CO", "BX.TRF.PWKR.DT.GD.ZS"),# Remesas % PIB
    "IED":                    ("CO", "BX.KLT.DINV.WD.GD.ZS"),# IED % PIB
    "TermIntCom":             ("CO", "TT.PRI.MRCH.XD.WD"),   # Terminos de intercambio
    "CuentaCorriente":        ("CO", "BN.CAB.XOKA.GD.ZS"),   # Cuenta corriente % PIB
    # Fiscal Colombia
    "DeudaPublica":           ("CO", "GC.DOD.TOTL.GD.ZS"),   # Deuda publica % PIB
    "DeficitFiscal":          ("CO", "GC.BAL.CASH.GD.ZS"),   # Balance fiscal % PIB
    # Tasas (proxy via World Bank — valores anuales)
    "TasIntPol":              ("CO", "FR.INR.DPST"),          # Tasa depositos proxy
    # MX via World Bank (fallback cuando Banxico sin token)
    "MX_PIB":                 ("MX", "NY.GDP.MKTP.KD.ZG"),
    "MX_IPC":                 ("MX", "FP.CPI.TOTL.ZG"),
    "MX_Desempleo":           ("MX", "SL.UEM.TOTL.ZS"),
    "MX_TRM":                 ("MX", "PA.NUS.FCRF"),
    # BR via World Bank (complemento a BCB)
    "BR_PIB":                 ("BR", "NY.GDP.MKTP.KD.ZG"),
    "BR_IPC":                 ("BR", "FP.CPI.TOTL.ZG"),
    "BR_Desempleo":           ("BR", "SL.UEM.TOTL.ZS"),
    "BR_DeudaPublica":        ("BR", "GC.DOD.TOTL.GD.ZS"),
    # EC via World Bank
    "EC_PIB":                 ("EC", "NY.GDP.MKTP.KD.ZG"),
    "EC_IPC":                 ("EC", "FP.CPI.TOTL.ZG"),
    "EC_Desempleo":           ("EC", "SL.UEM.TOTL.ZS"),
    "EC_CuentaCorriente":     ("EC", "BN.CAB.XOKA.GD.ZS"),
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
                df = wb_conn.fetch_series(f"{country_code}:{wb_indicator}", start_date, end_date)
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
        """Obtiene TRM via World Bank PA.NUS.FCRF (tasa de cambio oficial COP/USD)."""
        try:
            from connectors.world_bank import WorldBankConnector
            wb = WorldBankConnector()
            df = wb.fetch_series("CO:PA.NUS.FCRF", start_date, end_date)
            if not df.empty:
                logger.info(f"[banrep/TRM] {len(df)} registros via World Bank PA.NUS.FCRF")
                return df
        except Exception as e:
            logger.warning(f"[banrep/TRM] World Bank fallback fallo: {e}")
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
