"""
base.py — Clase base abstracta para todos los conectores de API.
"""
import time
import logging
import abc
import httpx
import pandas as pd
from datetime import date

logger = logging.getLogger(__name__)


class BaseConnector(abc.ABC):
    """Conector base con retry exponencial, rate limiting y timeout estándar."""

    MAX_RETRIES = 3
    TIMEOUT = 30.0
    RATE_LIMIT_SECONDS = 1.0    # mínimo segundos entre requests

    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self._last_request_at: float = 0.0

    def _rate_limit(self):
        """Espera si es necesario para respetar el rate limit."""
        elapsed = time.time() - self._last_request_at
        if elapsed < self.RATE_LIMIT_SECONDS:
            time.sleep(self.RATE_LIMIT_SECONDS - elapsed)
        self._last_request_at = time.time()

    def _get(self, url: str, params: dict = None, headers: dict = None) -> dict | list:
        """Realiza un GET con retry y backoff exponencial."""
        self._rate_limit()
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = httpx.get(
                    url, params=params, headers=headers,
                    timeout=self.TIMEOUT, follow_redirects=True
                )
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                wait = 2 ** attempt
                logger.warning(f"[{self.provider_name}] intento {attempt}/{self.MAX_RETRIES} falló: {e}. "
                                f"Reintentando en {wait}s...")
                if attempt < self.MAX_RETRIES:
                    time.sleep(wait)
                else:
                    logger.error(f"[{self.provider_name}] todos los intentos fallaron para {url}")
                    raise

    def _post(self, url: str, json_body: dict, headers: dict = None) -> dict | list:
        """Realiza un POST con retry y backoff exponencial."""
        self._rate_limit()
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = httpx.post(
                    url, json=json_body, headers=headers,
                    timeout=self.TIMEOUT, follow_redirects=True
                )
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                wait = 2 ** attempt
                logger.warning(f"[{self.provider_name}] POST intento {attempt}/{self.MAX_RETRIES} falló: {e}. "
                                f"Reintentando en {wait}s...")
                if attempt < self.MAX_RETRIES:
                    time.sleep(wait)
                else:
                    logger.error(f"[{self.provider_name}] POST todos los intentos fallaron.")
                    raise

    @abc.abstractmethod
    def fetch_series(self, serie_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Descarga una serie de tiempo.
        Retorna DataFrame con columnas ['date', 'value'] o vacío si falla.
        """

    @staticmethod
    def empty_df() -> pd.DataFrame:
        return pd.DataFrame(columns=['date', 'value'])
