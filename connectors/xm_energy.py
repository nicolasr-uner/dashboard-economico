"""
xm_energy.py — Conector para XM Colombia (Mercado de Energía Mayorista).
API pública sin autenticación. Endpoints: SIMEM y servapibi.xm.com.co.
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

XM_API_BASE = "https://servapibi.xm.com.co"
SIMEM_BASE  = "https://www.simem.co/backend-files/api/PublicData"

# Mapeo de metric_id a tipo de endpoint (daily/monthly)
METRIC_ENDPOINTS = {
    "PrecBolNac":      ("daily",   "Sistema"),
    "DemaNal":         ("daily",   "Sistema"),
    "GeneReal":        ("daily",   "Sistema"),
    "AporEner":        ("daily",   "Sistema"),
    "VolUtilDiari":    ("daily",   "Sistema"),
    "PrecPromContReg": ("monthly", "Sistema"),
    "PrecEscworking":  ("monthly", "Sistema"),
    "CERE":            ("monthly", "Sistema"),
}


class XMEnergyConnector(BaseConnector):

    def __init__(self):
        super().__init__("xm")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie de XM Colombia.
        serie_id corresponde a MetricId (ej. 'PrecBolNac').
        Retorna DataFrame ['date', 'value'].
        """
        # XM API tiene límite de 30 días por request — iterar si el rango es mayor
        try:
            sd = datetime.strptime(start_date, "%Y-%m-%d")
            ed = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return self.empty_df()

        freq, entity = METRIC_ENDPOINTS.get(serie_id, ("daily", "Sistema"))
        endpoint_type = freq  # 'daily' o 'monthly'

        all_dfs = []
        # Partir en chunks de 30 días para el endpoint diario
        chunk_days = 30 if endpoint_type == "daily" else 365
        current = sd

        while current <= ed:
            chunk_end = min(current + timedelta(days=chunk_days - 1), ed)
            df_chunk = self._fetch_chunk(
                serie_id, current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"), endpoint_type, entity
            )
            if not df_chunk.empty:
                all_dfs.append(df_chunk)
            current = chunk_end + timedelta(days=1)

        if not all_dfs:
            # Intentar SIMEM como fallback
            df_simem = self._fetch_simem(serie_id, start_date, end_date)
            if not df_simem.empty:
                return df_simem
            logger.warning(f"[xm] Serie {serie_id} sin datos disponibles.")
            return self.empty_df()

        combined = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset='date')
        combined = combined.sort_values('date').reset_index(drop=True)
        logger.info(f"[xm] {serie_id}: {len(combined)} registros descargados.")
        return combined

    def _fetch_chunk(self, metric_id: str, start: str, end: str,
                     endpoint_type: str, entity: str) -> pd.DataFrame:
        """Llama al endpoint de XM API para un chunk de fechas."""
        url = f"{XM_API_BASE}/{endpoint_type}"
        body = {
            "MetricId": metric_id,
            "StartDate": start,
            "EndDate": end,
            "Entity": entity
        }
        try:
            data = self._post(url, json_body=body)
            return self._parse_xm_response(data, metric_id)
        except Exception as e:
            logger.warning(f"[xm] chunk {metric_id} {start}–{end} falló: {e}")
            return self.empty_df()

    def _fetch_simem(self, metric_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fallback: consultar SIMEM (portal de datos abiertos de energía)."""
        # Dataset IDs conocidos en SIMEM
        SIMEM_DATASETS = {
            "PrecBolNac": "1",
            "DemaNal":    "3",
        }
        dataset_id = SIMEM_DATASETS.get(metric_id)
        if not dataset_id:
            return self.empty_df()

        params = {
            "startDate": start_date,
            "endDate": end_date,
            "datasetId": dataset_id
        }
        try:
            data = self._get(SIMEM_BASE, params=params)
            return self._parse_simem_response(data)
        except Exception as e:
            logger.warning(f"[xm/simem] {metric_id} falló: {e}")
            return self.empty_df()

    def _parse_xm_response(self, data, metric_id: str) -> pd.DataFrame:
        """Parsear respuesta de servapibi.xm.com.co."""
        try:
            items = data if isinstance(data, list) else data.get('Items', data.get('items', []))
            if not items:
                return self.empty_df()

            records = []
            for item in items:
                # Posibles keys de fecha y valor
                fecha = (item.get('Date') or item.get('date') or
                         item.get('StartDate') or item.get('HourStartDate', '')[:10])
                valor = (item.get('Value') or item.get('value') or
                         item.get('Total') or item.get(metric_id))
                if fecha and valor is not None:
                    try:
                        records.append({'date': pd.to_datetime(str(fecha)[:10]), 'value': float(valor)})
                    except (ValueError, TypeError):
                        continue

            if not records:
                return self.empty_df()

            df = pd.DataFrame(records)
            # Agregar por fecha (promedio diario si hay múltiples horas)
            df = df.groupby('date', as_index=False)['value'].mean()
            return df.sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.error(f"[xm] Error parseando respuesta: {e}")
            return self.empty_df()

    def _parse_simem_response(self, data) -> pd.DataFrame:
        """Parsear respuesta de SIMEM."""
        try:
            records_raw = data.get('data', data.get('records', []))
            if not records_raw:
                return self.empty_df()
            df = pd.DataFrame(records_raw)
            # Intentar detectar columnas de fecha y valor
            date_col = next((c for c in df.columns if 'fecha' in c.lower() or 'date' in c.lower()), None)
            val_col = next((c for c in df.columns if 'valor' in c.lower() or 'value' in c.lower() or 'precio' in c.lower()), None)
            if not date_col or not val_col:
                return self.empty_df()
            df = df[[date_col, val_col]].rename(columns={date_col: 'date', val_col: 'value'})
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna().sort_values('date').reset_index(drop=True)
            return df
        except Exception as e:
            logger.error(f"[xm/simem] Error parseando: {e}")
            return self.empty_df()
