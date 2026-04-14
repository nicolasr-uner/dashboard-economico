"""
xm_energy.py — Conector para XM Colombia (Mercado de Energía Mayorista).
API pública sin autenticación. Endpoints: SIMEM y servapibi.xm.com.co.
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

XM_API_BASE  = "https://servapibi.xm.com.co"
SIMEM_BASE   = "https://www.simem.co/backend-files/api/GetDataset"
SIMEM_BASE_V1 = "https://www.simem.co/backend-files/api/PublicData"  # fallback legacy

# Mapeo de metric_id a tipo de endpoint (daily/monthly)
# NOTA: La API de servapibi.xm.com.co actualizó sus metric IDs en 2025.
# Solo los IDs verificados como funcionales están incluidos aquí.
METRIC_ENDPOINTS = {
    "AporEner":        ("daily",   "Sistema"),   # Aportes Hídricos — FUNCIONANDO
    # Los siguientes IDs ya no son válidos en la API actual.
    # Se intentará SIMEM como fallback.
    "PrecBolNac":      ("daily",   "Sistema"),
    "DemaNal":         ("daily",   "Sistema"),
    "GeneReal":        ("daily",   "Sistema"),
    "VolUtilDiari":    ("daily",   "Sistema"),
    "GeneSolar":       ("daily",   "Sistema"),
    "PrecPromContReg": ("monthly", "Sistema"),
    "PrecEscworking":  ("monthly", "Sistema"),
    "CERE":            ("monthly", "Sistema"),
}

# IDs verificados como válidos en la API actual (2025)
VALID_METRIC_IDS = {"AporEner"}


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

        # Si el metric_id no está en los IDs validados, saltar directo a SIMEM
        if serie_id not in VALID_METRIC_IDS:
            df_simem = self._fetch_simem(serie_id, start_date, end_date)
            if not df_simem.empty:
                return df_simem
            logger.warning(f"[xm] Serie {serie_id} no disponible en API actual ni en SIMEM.")
            return self.empty_df()

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
        """Fallback: consultar SIMEM (portal de datos abiertos de energía) v2 y v1."""
        SIMEM_DATASETS = {
            "PrecBolNac":   "PrecBolNal",
            "DemaNal":      "DemaNal",
            "GeneReal":     "GeneReal",
            "AporEner":     "AporHidEnerg",
            "VolUtilDiari": "VolUtilEmb",
        }
        dataset_id = SIMEM_DATASETS.get(metric_id)
        if not dataset_id:
            return self.empty_df()

        # Intentar SIMEM v2 primero
        params_v2 = {
            "startDate": start_date,
            "endDate": end_date,
            "datasetId": dataset_id,
            "format": "json"
        }
        try:
            data = self._get(SIMEM_BASE, params=params_v2)
            df = self._parse_simem_response(data)
            if not df.empty:
                return df
        except Exception as e:
            logger.warning(f"[xm/simem-v2] {metric_id} falló: {e}")

        # Intentar SIMEM v1 legacy
        legacy_ids = {"PrecBolNac": "1", "DemaNal": "3"}
        legacy_id = legacy_ids.get(metric_id)
        if legacy_id:
            params_v1 = {"startDate": start_date, "endDate": end_date, "datasetId": legacy_id}
            try:
                data = self._get(SIMEM_BASE_V1, params=params_v1)
                df = self._parse_simem_response(data)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"[xm/simem-v1] {metric_id} falló: {e}")

        return self.empty_df()

    def _parse_xm_response(self, data, metric_id: str) -> pd.DataFrame:
        """Parsear respuesta de servapibi.xm.com.co."""
        try:
            items = (data if isinstance(data, list)
                     else data.get('Items', data.get('items',
                          data.get('Values', data.get('values',
                          data.get('data', data.get('records', [])))))))
            if not items:
                return self.empty_df()

            records = []
            for item in items:
                # Posibles keys de fecha y valor
                fecha = (item.get('Date') or item.get('date') or
                         item.get('StartDate') or item.get('HourStartDate', '')[:10])

                # Formato nuevo: DailyEntities/HourlyEntities con lista [{Id, Value}]
                valor = None
                for entities_key in ('DailyEntities', 'HourlyEntities', 'MonthlyEntities'):
                    entities = item.get(entities_key)
                    if entities and isinstance(entities, list):
                        # Sumar todos los valores (o tomar el primero si es por entidad)
                        try:
                            valor = sum(float(e.get('Value', 0) or 0) for e in entities if e.get('Value') is not None)
                        except (ValueError, TypeError):
                            pass
                        break

                # Formato legado: Value directo
                if valor is None:
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
