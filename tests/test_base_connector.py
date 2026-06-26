"""Tests de la clase base de conectores (connectors/base.py)."""
from connectors.base import BaseConnector


class _Dummy(BaseConnector):
    def fetch_series(self, serie_id, start_date, end_date):
        return self.empty_df()


def test_empty_df_shape():
    df = BaseConnector.empty_df()
    assert list(df.columns) == ["date", "value"]
    assert df.empty


def test_subclass_instantiation_and_contract():
    d = _Dummy("dummy")
    assert d.provider_name == "dummy"
    out = d.fetch_series("x", "2020-01-01", "2020-12-31")
    assert list(out.columns) == ["date", "value"]
    assert out.empty
