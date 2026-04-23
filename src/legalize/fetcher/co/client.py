"""SUIN-Juriscol HTTP client (Colombia).

SUIN-Juriscol has no REST API. Each law is a single HTML page at
/viewDocument.asp?id={numeric_id} that contains both consolidated text and
metadata. The site currently serves a broken certificate chain, so this
client disables TLS verification and logs that decision on init.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.suin-juriscol.gov.co"
DEFAULT_USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"
_RETRY_STATUS_CODES = (429, 503)


class SuinClient(HttpClient):
    """HTTP client for Colombian legislation via SUIN-Juriscol HTML scraping."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("base_url", DEFAULT_BASE_URL)
        kwargs.setdefault("user_agent", DEFAULT_USER_AGENT)
        kwargs.setdefault("max_retries", 3)
        kwargs.setdefault("requests_per_second", 1.0)
        super().__init__(**kwargs)
        logger.warning("TLS verification disabled for SUIN-Juriscol (broken cert chain)")

    @classmethod
    def create(cls, country_config: CountryConfig) -> SuinClient:
        """Create SuinClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", DEFAULT_BASE_URL),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
            requests_per_second=source.get("requests_per_second", 1.0),
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
        json: dict | None = None,
        data: bytes | None = None,
        timeout: int | None = None,
    ) -> requests.Response:
        """HTTP request with SUIN-specific verify=False and transient retries."""
        self._wait_rate_limit()
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    json=json,
                    data=data,
                    timeout=timeout or self._timeout,
                    verify=False,
                )
                if response.status_code in _RETRY_STATUS_CODES and attempt < self._max_retries - 1:
                    wait = 2**attempt
                    logger.warning(
                        "%s %d on %s, retrying in %ds (attempt %d/%d)",
                        method,
                        response.status_code,
                        url,
                        wait,
                        attempt + 1,
                        self._max_retries,
                    )
                    time.sleep(wait)
                    self._wait_rate_limit()
                    continue
                response.raise_for_status()
                return response
            except requests.HTTPError:
                raise
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt < self._max_retries - 1:
                    wait = 2**attempt
                    logger.warning(
                        "Request error (attempt %d/%d): %s",
                        attempt + 1,
                        self._max_retries,
                        exc,
                    )
                    time.sleep(wait)
                    self._wait_rate_limit()
                    continue
                raise
            except requests.RequestException as exc:
                last_exc = exc
                raise
        raise last_exc or RuntimeError(f"Failed {method} {url}")

    def get_text(self, norm_id: str, meta_data: bytes | None = None) -> bytes:
        """Fetch the full HTML page for a SUIN document.

        ``meta_data`` reuses the body fetched by the prior ``get_metadata``
        call, since SUIN serves text + metadata on the same page. Without
        it, every law triggers two identical GETs.
        """
        if meta_data is not None:
            return meta_data
        return self._get(f"{self._base_url}/viewDocument.asp?id={norm_id}")

    def get_metadata(self, norm_id: str) -> bytes:
        """Same as get_text — metadata is in the same HTML page."""
        return self.get_text(norm_id)
