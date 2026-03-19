"""
models.py — Modelos de proyección estadística (Holt-Winters, ARIMA, Ensemble).
Retornan proyecciones y bandas de confianza.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def _get_confidence_intervals(forecast_series, std_error, factor_80=1.28, factor_95=1.96):
    """Calcula intervalos de confianza dado el forecast y el error estándar (o aproximación)."""
    return {
        'lower_80': forecast_series - (factor_80 * std_error),
        'upper_80': forecast_series + (factor_80 * std_error),
        'lower_95': forecast_series - (factor_95 * std_error),
        'upper_95': forecast_series + (factor_95 * std_error)
    }


def forecast_holtwinters(series: pd.Series, periods: int = 6, seasonal_periods: int = 12) -> dict:
    """Holt-Winters Exponencial Smoothing con intervalos."""
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        
        # Holt-Winters requires enough data for seasonality, else use simple exponential smoothing or trend only
        if len(series) >= 2 * seasonal_periods:
            model = ExponentialSmoothing(
                series, trend='add', seasonal='add', seasonal_periods=seasonal_periods, initialization_method="estimated"
            )
        elif len(series) >= 4:
            model = ExponentialSmoothing(series, trend='add', initialization_method="estimated")
        else:
            return {}

        fit_model = model.fit()
        forecast = fit_model.forecast(periods)
        
        # Aproximación del error estándar para intervalos
        rmse = np.sqrt(fit_model.sse / len(series))
        # Para proyecciones a futuro el error aumenta sqrt(h)
        horizons = np.arange(1, periods + 1)
        std_error = rmse * np.sqrt(horizons)

        result = {
            'forecast': forecast.values,
            'model_name': 'Holt-Winters'
        }
        result.update(_get_confidence_intervals(forecast.values, std_error))
        return result
    except Exception as e:
        logger.warning(f"[models] Holt-Winters falló: {e}")
        return {}


def forecast_arima(series: pd.Series, periods: int = 6) -> dict:
    """ARIMA con auto-setup básico e intervalos."""
    # ARIMA requere suficientes datos
    if len(series) < 15:
        return {}
        
    try:
        from statsmodels.tsa.arima.model import ARIMA
        
        # Setup (1,1,1) como estándar general macro si hay datos
        model = ARIMA(series, order=(1, 1, 1))
        fit_model = model.fit()
        
        # Obtener predicciones con intervalos (alpha=0.2 para 80%, alpha=0.05 para 95%)
        pred_80 = fit_model.get_forecast(steps=periods)
        pred_95 = fit_model.get_forecast(steps=periods)
        
        conf_int_80 = pred_80.conf_int(alpha=0.2)
        conf_int_95 = pred_95.conf_int(alpha=0.05)
        
        return {
            'forecast': pred_80.predicted_mean.values,
            'model_name': 'ARIMA',
            'lower_80': conf_int_80.iloc[:, 0].values,
            'upper_80': conf_int_80.iloc[:, 1].values,
            'lower_95': conf_int_95.iloc[:, 0].values,
            'upper_95': conf_int_95.iloc[:, 1].values,
        }
    except Exception as e:
        logger.warning(f"[models] ARIMA falló: {e}")
        return {}


def forecast_ensemble(series: pd.Series, periods: int = 6) -> dict:
    """Ensemble: promedio de Holt-Winters y ARIMA si ambos están disponibles."""
    if len(series) < 3:
        return {}
        
    s = series.reset_index(drop=True).astype(float)
    
    hw_res = forecast_holtwinters(s, periods)
    ar_res = forecast_arima(s, periods)
    
    # Combinar
    if hw_res and ar_res:
        forecast = (hw_res['forecast'] + ar_res['forecast']) / 2
        lo80 = (hw_res['lower_80'] + ar_res['lower_80']) / 2
        hi80 = (hw_res['upper_80'] + ar_res['upper_80']) / 2
        lo95 = (hw_res['lower_95'] + ar_res['lower_95']) / 2
        hi95 = (hw_res['upper_95'] + ar_res['upper_95']) / 2
        
        return {
            'forecast': forecast,
            'model_name': 'Ensemble (HW+ARIMA)',
            'lower_80': lo80, 'upper_80': hi80,
            'lower_95': lo95, 'upper_95': hi95
        }
    elif hw_res:
        return hw_res
    elif ar_res:
        return ar_res
    else:
        return {}
