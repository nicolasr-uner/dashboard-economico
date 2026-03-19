"""
registry.py — Mapeo entre api_provider → conector + instancia.
Uso: connector, serie_id = get_connector_for_variable(variable_row)
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def get_connector_for_variable(variable_row):
    """
    Retorna (instancia_conector, serie_id) basado en api_provider y api_serie_id.
    variable_row puede ser un ORM MacroVariable o un pd.Series.
    
    Retorna (None, None) si no hay conector disponible.
    """
    # Soporte tanto para ORM como para pd.Series / dict
    if hasattr(variable_row, '__getitem__'):
        provider = str(variable_row.get('api_provider') or '').lower().strip()
        serie_id = str(variable_row.get('api_serie_id') or '').strip()
    else:
        provider = str(getattr(variable_row, 'api_provider', '') or '').lower().strip()
        serie_id = str(getattr(variable_row, 'api_serie_id', '') or '').strip()

    if not provider or not serie_id:
        return None, None

    CONNECTOR_CLASSES = {
        'banrep':  _get_banrep,
        'bcb':     _get_bcb,
        'banxico': _get_banxico,
        'fred':    _get_fred,
        'xm':      _get_xm,
    }

    factory = CONNECTOR_CLASSES.get(provider)
    if not factory:
        logger.warning(f"[registry] Proveedor desconocido: {provider!r}")
        return None, None

    try:
        connector = factory()
        return connector, serie_id
    except Exception as e:
        logger.error(f"[registry] Error instanciando conector para {provider}: {e}")
        return None, None


def _get_banrep():
    from connectors.banrep import BanRepConnector
    return BanRepConnector()

def _get_bcb():
    from connectors.bcb import BCBConnector
    return BCBConnector()

def _get_banxico():
    from connectors.banxico import BanxicoConnector
    return BanxicoConnector()

def _get_fred():
    from connectors.fred import FREDConnector
    return FREDConnector()

def _get_xm():
    from connectors.xm_energy import XMEnergyConnector
    return XMEnergyConnector()
