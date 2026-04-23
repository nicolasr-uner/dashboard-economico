"""
xm_energy.py — Conector para XM Colombia (Mercado de Energía Mayorista).
API pública sin autenticación: servapibi.xm.com.co

Notas de actualización (2025-2026):
  - Los MetricIds antiguos (DemaNal, GeneReal, PrecBolNac…) fueron renombrados.
  - Varios pasaron del endpoint /daily al /hourly con estructura Hour01-Hour24.
  - Los IDs actuales se obtienen con POST /lists {MetricId: 'ListadoMetricas'}.
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector

logger = logging.getLogger(__name__)

XM_API_BASE = "https://servapibi.xm.com.co"

# Mapeo: serie_id usado en la DB → (nuevo_metric_id, endpoint, agregación)
# endpoint: 'daily' | 'hourly'
# agregación: 'sum' (energía kWh) | 'mean' (precio COP/kWh) | 'direct' (ya es diario)
METRIC_MAP = {
    # ── Activos y funcionando (endpoint /daily) ──────────────────────────────
    "AporEner":        ("AporEner",         "daily",   "direct"),  # Aportes Hídricos kWh
    "VoluUtilDiarEner":("VoluUtilDiarEner", "daily",   "direct"),  # Volumen Útil diario kWh
    "PorcVoluUtilDiar":("PorcVoluUtilDiar", "daily",   "direct"),  # Volumen Útil diario %
    "PrecEsca":        ("PrecEsca",         "daily",   "direct"),  # Precio Escasez COP/kWh
    "PrecEscaAct":     ("PrecEscaAct",      "daily",   "direct"),  # Precio Escasez Activación

    # ── IDs de DB antiguos → nuevos /daily ───────────────────────────────────
    "VolUtilDiari":    ("VoluUtilDiarEner", "daily",   "direct"),  # Volumen Útil Diario
    "PrecEscworking":  ("PrecEsca",         "daily",   "direct"),  # Precio Escasez (renombrado)

    # ── Renombrados a /hourly ────────────────────────────────────────────────
    "DemaNal":         ("DemaReal",         "hourly",  "sum"),     # Demanda Real kWh/día
    "GeneReal":        ("Gene",             "hourly",  "sum"),     # Generación Total kWh/día
    "PrecBolNac":      ("PrecBolsNaci",     "hourly",  "mean"),    # Precio Bolsa COP/kWh
    "GeneSolar":       ("Gene",             "hourly",  "sum"),     # Generación (proxy total)

    # ── /monthly ─────────────────────────────────────────────────────────────
    "CERE":            ("CERE",             "monthly", "direct"),  # Cargo Energía Regulada
    "PrecPromContReg": ("PrecPromContRegu", "hourly",  "mean"),    # Precio Promedio Contratos
    "PrecPromContRegu":("PrecPromContRegu", "hourly",  "mean"),

    # ── Alias directos con nuevos IDs ────────────────────────────────────────
    "DemaReal":        ("DemaReal",         "hourly",  "sum"),
    "Gene":            ("Gene",             "hourly",  "sum"),
    "PrecBolsNaci":    ("PrecBolsNaci",     "hourly",  "mean"),
}


class XMEnergyConnector(BaseConnector):

    def __init__(self):
        super().__init__("xm")

    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie de XM Colombia.
        serie_id: MetricId de la DB (puede ser el nombre antiguo o nuevo).
        Retorna DataFrame ['date', 'value'] con granularidad diaria.
        """
        try:
            sd = datetime.strptime(start_date, "%Y-%m-%d")
            ed = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"[xm] Formato de fecha inválido: {start_date} / {end_date}")
            return self.empty_df()

        # Resolver el metric_id actual y endpoint
        if serie_id not in METRIC_MAP:
            logger.warning(f"[xm] MetricId desconocido: {serie_id}")
            return self.empty_df()

        new_metric_id, endpoint_type, agg = METRIC_MAP[serie_id]

        # XM limita a 30 días por request (hourly/daily); monthly acepta más
        chunk_days = 731 if endpoint_type == "monthly" else 30
        all_dfs = []
        current = sd

        while current <= ed:
            chunk_end = min(current + timedelta(days=chunk_days - 1), ed)
            df_chunk = self._fetch_chunk(
                new_metric_id,
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
                endpoint_type,
                agg
            )
            if not df_chunk.empty:
                all_dfs.append(df_chunk)
            current = chunk_end + timedelta(days=1)

        if not all_dfs:
            logger.warning(f"[xm] Serie {serie_id} ({new_metric_id}) sin datos.")
            return self.empty_df()

        combined = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset='date')
        combined = combined.sort_values('date').reset_index(drop=True)
        logger.info(f"[xm] {serie_id} → {new_metric_id}: {len(combined)} registros.")
        return combined

    def _fetch_chunk(self, metric_id: str, start: str, end: str,
                     endpoint_type: str, agg: str) -> pd.DataFrame:
        """Llama al endpoint XM y agrega la respuesta a granularidad diaria."""
        url = f"{XM_API_BASE}/{endpoint_type}"
        body = {
            "MetricId": metric_id,
            "StartDate": start,
            "EndDate": end,
            "Entity": "Sistema"
        }
        try:
            data = self._post(url, json_body=body)
            if endpoint_type == "hourly":
                return self._parse_hourly(data, agg)
            else:
                # daily and monthly have the same structure; only the entities key differs
                entities_key = "MonthlyEntities" if endpoint_type == "monthly" else "DailyEntities"
                return self._parse_daily(data, entities_key=entities_key)
        except Exception as e:
            logger.warning(f"[xm] chunk {metric_id} {start}–{end} falló: {e}")
            return self.empty_df()

    # ── Parsers ──────────────────────────────────────────────────────────────

    def _parse_daily(self, data, entities_key: str = "DailyEntities") -> pd.DataFrame:
        """Parsear respuesta /daily o /monthly: Items[{Date, <entities_key>:[{Id, Value}]}]"""
        try:
            items = data.get('Items', []) if isinstance(data, dict) else data
            records = []
            for item in items:
                date_str = item.get('Date', '')[:10]
                entities = item.get(entities_key, [])
                for ent in entities:
                    if ent.get('Id') == 'Sistema':
                        try:
                            val = float(ent.get('Value', 0) or 0)
                            records.append({'date': pd.to_datetime(date_str), 'value': val})
                        except (ValueError, TypeError):
                            pass
                        break
            if not records:
                return self.empty_df()
            return pd.DataFrame(records).sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.error(f"[xm] Error parseando /{entities_key}: {e}")
            return self.empty_df()

    def _parse_hourly(self, data, agg: str) -> pd.DataFrame:
        """
        Parsear respuesta /hourly: Items[{Date, HourlyEntities:[{Id, Values:{Hour01..Hour24}}]}]
        Agregar las 24 horas según agg ('sum' o 'mean').
        """
        try:
            items = data.get('Items', []) if isinstance(data, dict) else data
            records = []
            for item in items:
                date_str = item.get('Date', '')[:10]
                entities = item.get('HourlyEntities', [])
                for ent in entities:
                    if ent.get('Id') == 'Sistema':
                        vals_dict = ent.get('Values', {})
                        hour_values = []
                        for h in range(1, 25):
                            hkey = f'Hour{h:02d}'
                            raw = vals_dict.get(hkey)
                            if raw is not None:
                                try:
                                    hour_values.append(float(raw))
                                except (ValueError, TypeError):
                                    pass
                        if hour_values:
                            if agg == 'sum':
                                daily_val = sum(hour_values)
                            else:  # mean
                                daily_val = sum(hour_values) / len(hour_values)
                            records.append({
                                'date': pd.to_datetime(date_str),
                                'value': daily_val
                            })
                        break
            if not records:
                return self.empty_df()
            return pd.DataFrame(records).sort_values('date').reset_index(drop=True)
        except Exception as e:
            logger.error(f"[xm] Error parseando /hourly: {e}")
            return self.empty_df()
