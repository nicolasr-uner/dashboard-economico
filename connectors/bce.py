"""
bce.py — Conector para el Banco Central del Ecuador (BCE) y fuentes complementarias EC.

Estrategia por serie:
  - Datos mensuales/diarios: BCE REST API o descarga directa de archivos CSV/JSON publicados.
  - Datos anuales / series largas: World Bank como fallback confiable.
  - IPC mensual: INEC Ecuador (Instituto Nacional de Estadística y Censos).

BCE API base: https://contenido.bce.fin.ec
Los endpoints de descarga masiva son archivos CSV/Excel. Aquí usamos las rutas
de descarga directa JSON donde estén disponibles, con fallback a WB.
"""
import logging
import io
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

# ── Mapeo de serie_id → (fuente, url_o_identificador) ─────────────────────────
# Fuente 'bce_csv'  : descarga de CSV bulk del BCE Ecuador
# Fuente 'wb'       : World Bank fallback (siempre disponible, delay ~12 meses)
# Fuente 'inec_csv' : descarga de CSV del INEC (IPC mensual)

BCE_SERIES_MAP = {
    # ── Sector Monetario ──────────────────────────────────────────────────────
    "IPC_mensual":          ("inec_csv", "https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/2024/ipc_series_historicas.xlsx"),
    "TasaInterbancaria":    ("bce_csv",  "https://contenido.bce.fin.ec/documentos/Estadisticas/SectorMonFin/TasasInteres/Indice.htm"),
    # ── Sector Externo ────────────────────────────────────────────────────────
    "ReservasInt":          ("wb",       "EC:FI.RES.TOTL.CD"),
    "BalanzaComercial":     ("wb",       "EC:NE.EXP.GNFS.CD"),
    "ExportacionesPetroleo":("wb",       "EC:TX.VAL.FUEL.ZS.UN"),
    "ExportacionesBanano":  ("wb",       "EC:TX.VAL.AGRI.ZS.UN"),
    "CuentaCorriente":      ("wb",       "EC:BN.CAB.XOKA.GD.ZS"),
    # ── Sector Real ───────────────────────────────────────────────────────────
    "PIB_anual":            ("wb",       "EC:NY.GDP.MKTP.KD.ZG"),
    "PIB_USD":              ("wb",       "EC:NY.GDP.MKTP.CD"),
    "IPC_anual":            ("wb",       "EC:FP.CPI.TOTL.ZG"),
    "Desempleo":            ("wb",       "EC:SL.UEM.TOTL.ZS"),
    # ── Sector Fiscal ─────────────────────────────────────────────────────────
    "DeudaPublica":         ("wb",       "EC:GC.DOD.TOTL.GD.ZS"),
    "DeficitFiscal":        ("wb",       "EC:GC.BAL.CASH.GD.ZS"),
}

# Headers para evitar bloqueos en descargas de archivos
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EconomicDashboard/1.0)",
    "Accept": "application/json, text/html, */*",
}


class BCEConnector(BaseConnector):
    """Conector para el Banco Central del Ecuador y fuentes complementarias."""

    def __init__(self):
        super().__init__("bce")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie económica de Ecuador.
        serie_id: clave en BCE_SERIES_MAP o formato 'EC:WB_INDICATOR'.
        """
        # Soporte directo para format WB: 'EC:NY.GDP.MKTP.KD.ZG'
        if serie_id.startswith("EC:") or serie_id.startswith("ec:"):
            return self._fetch_wb(serie_id, start_date, end_date)

        mapping = BCE_SERIES_MAP.get(serie_id)
        if not mapping:
            logger.warning(f"[bce] Serie '{serie_id}' no mapeada. Intentando World Bank directo.")
            return self._fetch_wb(f"EC:{serie_id}", start_date, end_date)

        source, identifier = mapping

        if source == "wb":
            return self._fetch_wb(identifier, start_date, end_date)
        elif source == "inec_csv":
            df = self._fetch_inec_ipc(start_date, end_date)
            if not df.empty:
                return df
            # Fallback a WB anual si INEC falla
            logger.warning("[bce/inec] Falló descarga INEC IPC, usando World Bank anual.")
            return self._fetch_wb("EC:FP.CPI.TOTL.ZG", start_date, end_date)
        elif source == "bce_csv":
            df = self._fetch_bce_tasa_interbancaria(start_date, end_date)
            if not df.empty:
                return df
            logger.warning("[bce] Falló fuente BCE directa, sin fallback para tasa interbancaria.")
            return self.empty_df()

        return self.empty_df()

    # ── Implementaciones por fuente ────────────────────────────────────────────

    def _fetch_wb(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Delegado al WorldBankConnector."""
        try:
            from connectors.world_bank import WorldBankConnector
            wb = WorldBankConnector()
            df = wb.fetch_series(serie_id, start_date, end_date)
            if not df.empty:
                logger.info(f"[bce/wb] {serie_id}: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"[bce/wb] fallo para {serie_id}: {e}")
            return self.empty_df()

    def _fetch_inec_ipc(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga el IPC mensual desde INEC Ecuador.
        El INEC publica un Excel con series históricas mensuales del IPC.
        URL de descarga directa (actualizada anualmente por INEC).
        """
        urls_candidatos = [
            "https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/2024/ipc_series_historicas.xlsx",
            "https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/Inflacion-2024/Inflacion_Acumulada.xlsx",
        ]

        for url in urls_candidatos:
            try:
                import httpx
                resp = httpx.get(url, headers=_HEADERS, timeout=self.TIMEOUT, follow_redirects=True)
                resp.raise_for_status()
                df_raw = pd.read_excel(io.BytesIO(resp.content), engine='openpyxl')
                df = self._parse_inec_ipc(df_raw, start_date, end_date)
                if not df.empty:
                    logger.info(f"[bce/inec] IPC mensual: {len(df)} registros desde {url}")
                    return df
            except Exception as e:
                logger.warning(f"[bce/inec] URL {url} falló: {e}")
                continue

        return self.empty_df()

    def _parse_inec_ipc(self, df_raw: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Parsea el Excel del INEC. El formato típico tiene:
        - Primera columna: fecha (nombre de mes o fecha)
        - Columnas subsecuentes: variación mensual, anual, acumulada
        Intenta detectar automáticamente la columna de variación anual.
        """
        try:
            # Buscar la primera columna que parezca fecha
            df = df_raw.copy()
            date_col = df.columns[0]
            val_col = None

            # Detectar columna de variación anual (contiene "anual" o "ipa")
            for col in df.columns[1:]:
                col_str = str(col).lower()
                if any(kw in col_str for kw in ['anual', 'year', 'ipa', 'inflación']):
                    val_col = col
                    break
            if val_col is None and len(df.columns) > 1:
                val_col = df.columns[1]

            df = df[[date_col, val_col]].copy()
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['date', 'value'])

            # Filtrar por rango
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df['date'] >= sd) & (df['date'] <= ed)]
            return df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.warning(f"[bce/inec] parse IPC Excel falló: {e}")
            return self.empty_df()

    def _fetch_bce_tasa_interbancaria(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Intenta descargar la tasa interbancaria del BCE Ecuador.
        El BCE publica boletines mensuales; aquí usamos el endpoint de descarga CSV.
        """
        try:
            # BCE Ecuador — portal de estadísticas monetarias y financieras
            # Endpoint de consulta de tasas referenciales (JSON cuando está disponible)
            url = "https://contenido.bce.fin.ec/documentos/Estadisticas/SectorMonFin/TasasInteres/tasas_interes.csv"
            import httpx
            resp = httpx.get(url, headers=_HEADERS, timeout=self.TIMEOUT, follow_redirects=True)
            resp.raise_for_status()

            df_raw = pd.read_csv(io.StringIO(resp.text), sep=None, engine='python')
            return self._parse_bce_tasas(df_raw, "interbancaria", start_date, end_date)
        except Exception as e:
            logger.warning(f"[bce] tasa interbancaria CSV: {e}")
            return self.empty_df()

    def _parse_bce_tasas(self, df_raw: pd.DataFrame, tipo: str,
                         start_date: str, end_date: str) -> pd.DataFrame:
        """Parsea el CSV de tasas de interés del BCE Ecuador."""
        try:
            date_col = df_raw.columns[0]
            val_col = None
            for col in df_raw.columns:
                if tipo.lower() in str(col).lower():
                    val_col = col
                    break
            if val_col is None:
                val_col = df_raw.columns[1]

            df = df_raw[[date_col, val_col]].copy()
            df.columns = ['date', 'value']
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['date', 'value'])

            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df['date'] >= sd) & (df['date'] <= ed)]
            return df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.warning(f"[bce] parse tasas falló: {e}")
            return self.empty_df()
