"""Tests de parsing puro del conector de Ecuador (connectors/bce.py).

No hacen red: ejercitan los _parse_* con DataFrames sintéticos.
"""
import pandas as pd
from connectors.bce import BCEConnector


def test_parse_inec_ipc_selects_annual_column_and_filters_range():
    bce = BCEConnector()
    raw = pd.DataFrame({
        "Fecha": ["2020-06-01", "2021-06-01", "2022-06-01"],
        "Variacion anual": [3.0, 1.5, 2.2],
    })
    out = bce._parse_inec_ipc(raw, "2021-01-01", "2021-12-31")
    assert list(out.columns) == ["date", "value"]
    assert len(out) == 1
    assert abs(float(out.iloc[0]["value"]) - 1.5) < 1e-9


def test_parse_inec_ipc_empty_on_garbage():
    bce = BCEConnector()
    raw = pd.DataFrame({"x": ["no-date"], "y": ["no-num"]})
    out = bce._parse_inec_ipc(raw, "2020-01-01", "2020-12-31")
    assert out.empty
