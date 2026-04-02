"""Lithuania TAR / data.gov.lt HTTP client.

Metadata source: https://get.data.gov.lt (Spinta API, UAPI spec)
Text source: https://www.e-tar.lt (Register of Legal Acts)
License: Open data (Creative Commons)
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import LegislativeClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_API_URL = "https://get.data.gov.lt"
DEFAULT_DATASET = "datasets/gov/lrsk/teises_aktai/Dokumentas"
DEFAULT_TEXT_BASE_URL = "https://www.e-tar.lt"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_RATE_LIMIT = 2.0  # requests per second


class TARClient(LegislativeClient):
    """HTTP client for Lithuanian legislation.

    Dual-source approach:
    - data.gov.lt Spinta API for metadata and discovery
    - e-tar.lt for consolidated law text (HTML)
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> TARClient:
        """Create TARClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            api_url=source.get("api_url", DEFAULT_API_URL),
            dataset=source.get("dataset", DEFAULT_DATASET),
            text_base_url=source.get("text_base_url", DEFAULT_TEXT_BASE_URL),
            timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RATE_LIMIT),
        )

    def __init__(
        self,
        api_url: str = DEFAULT_API_URL,
        dataset: str = DEFAULT_DATASET,
        text_base_url: str = DEFAULT_TEXT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = DEFAULT_RATE_LIMIT,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._dataset = dataset
        self._text_base_url = text_base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._delay = 1.0 / requests_per_second
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "legalize-bot/1.0"})

    def get_text(self, norm_id: str) -> bytes:
        """Fetch consolidated HTML text from e-tar.lt.

        Args:
            norm_id: TAR identifier (e.g., "TAR-2000-12345")

        Returns:
            HTML bytes of the consolidated version.
        """
        url = f"{self._text_base_url}/portal/lt/legalAct/{norm_id}/asr"
        return self._fetch_with_retry(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata JSON from data.gov.lt Spinta API.

        Args:
            norm_id: TAR identifier (e.g., "TAR-2000-12345")

        Returns:
            JSON bytes with document metadata.
        """
        url = f"{self._api_url}/{self._dataset}"
        params = f"select(pavadinimas,trumpas_pavadinimas,numeris,rusis,priemimo_data,isigaliojimo_data,galiojimo_pabaigos_data,statusas,institucija,tar_identifikatorius,eli_identifikatorius,rusis_kodas)&tar_identifikatorius={norm_id}&limit(1)"
        full_url = f"{url}?{params}"
        return self._fetch_with_retry(full_url)

    def get_page(self, page_size: int = 100, cursor: str | None = None) -> bytes:
        """Fetch a page of documents from the Spinta API.

        Args:
            page_size: Number of results per page.
            cursor: Cursor token for pagination (from _page.next).

        Returns:
            JSON bytes with _data array and _page.next cursor.
        """
        url = f"{self._api_url}/{self._dataset}"
        params = f"select(tar_identifikatorius,rusis,statusas,priemimo_data,pavadinimas)&sort(tar_identifikatorius)&limit({page_size})"
        if cursor:
            params += f"&_page.next={cursor}"
        full_url = f"{url}?{params}"
        return self._fetch_with_retry(full_url)

    def close(self) -> None:
        self._session.close()

    # ── Internal helpers ──

    def _fetch_with_retry(self, url: str) -> bytes:
        """Fetch URL with exponential backoff retry."""
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                time.sleep(self._delay)
                r = self._session.get(url, timeout=self._timeout)
                if r.status_code in (429, 503):
                    wait = 2**attempt
                    logger.warning("Rate limited (%d), waiting %ds", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.content
            except requests.RequestException as exc:
                last_exc = exc
                wait = 2**attempt
                logger.warning("Request failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(wait)
        raise ConnectionError(f"Failed after {self._max_retries} retries: {last_exc}")
