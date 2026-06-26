"""Tests del dispatcher de conectores (connectors/registry.py)."""
from connectors.registry import get_connector_for_variable


def test_missing_provider_returns_none():
    assert get_connector_for_variable({"api_provider": "", "api_serie_id": ""}) == (None, None)


def test_missing_serie_id_returns_none():
    assert get_connector_for_variable({"api_provider": "bcb", "api_serie_id": ""}) == (None, None)


def test_unknown_provider_returns_none():
    assert get_connector_for_variable({"api_provider": "nope", "api_serie_id": "X"}) == (None, None)


def test_world_bank_provider_resolves():
    conn, sid = get_connector_for_variable(
        {"api_provider": "world_bank", "api_serie_id": "CO:NY.GDP.MKTP.CD"}
    )
    assert conn is not None
    assert sid == "CO:NY.GDP.MKTP.CD"


def test_provider_is_case_insensitive():
    conn, _ = get_connector_for_variable({"api_provider": "WORLD_BANK", "api_serie_id": "X"})
    assert conn is not None
