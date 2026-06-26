"""Tests de los modelos de proyección (projections/models.py).

Requieren statsmodels; si no está instalado, se omiten (no fallan).
"""
import pytest

pytest.importorskip("statsmodels")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from projections.models import forecast_ensemble  # noqa: E402


def test_short_series_returns_empty():
    assert forecast_ensemble(pd.Series([1.0, 2.0])) == {}


def test_ensemble_produces_forecast_of_requested_length():
    s = pd.Series(np.linspace(10.0, 40.0, 30))
    res = forecast_ensemble(s, periods=6)
    assert res, "el ensemble debería producir resultado con 30 puntos"
    assert len(res["forecast"]) == 6
    assert "model_name" in res
