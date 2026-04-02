"""Chile BCN (Biblioteca del Congreso Nacional) HTTP client.

Data source: https://www.leychile.cl/
Discovery: https://nuevo.leychile.cl/servicios/Consulta/script/exportarBSimpleMetas
Full text: https://www.leychile.cl/Consulta/obtxml?opt=7&idNorma={id}
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

TEXT_URL = "https://www.leychile.cl/Consulta/obtxml"
SEARCH_URL = "https://nuevo.leychile.cl/servicios/Consulta/script/exportarBSimpleMetas"

# CloudFront blocks default python-requests UA; use a browser-like one.
USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)"
# Fallback if the polite UA gets blocked:
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class BCNClient(LegislativeClient):
    """HTTP client for the Chilean BCN Ley Chile API.

    Two main endpoints:
    1. obtxml?opt=7 — full XML text per norm (by idNorma)
    2. exportarBSimpleMetas — paginated CSV search/discovery
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> BCNClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", "https://www.leychile.cl"),
            search_url=source.get("search_url", SEARCH_URL),
            rate_delay=1.0 / source.get("requests_per_second", 1.0),
            timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
        )

    def __init__(
        self,
        base_url: str = "https://www.leychile.cl",
        search_url: str = SEARCH_URL,
        rate_delay: float = 1.0,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._search_url = search_url
        self._rate_delay = rate_delay
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = requests.Session()
        # BCN CloudFront rejects bare python-requests UA with 401.
        self._session.headers.update(
            {
                "User-Agent": _BROWSER_UA,
                "Accept": "application/xml, text/xml, text/csv, */*",
            }
        )

    def get_text(self, norm_id: str) -> bytes:
        """Fetch full XML text for a norm by idNorma."""
        url = f"{self._base_url}/Consulta/obtxml"
        return self._get(url, params={"opt": "7", "idNorma": norm_id})

    def get_metadata(self, norm_id: str) -> bytes:
        """Return same XML — metadata is embedded in the XML response."""
        return self.get_text(norm_id)

    def search(
        self,
        page: int = 1,
        items_per_page: int = 100,
        total: str = "",
        **filters: str,
    ) -> bytes:
        """Search norms via exportarBSimpleMetas. Returns CSV bytes."""
        params = {
            "cadena": "",
            "fc_tn": filters.get("tipo_norma", ""),
            "fc_de": filters.get("fc_de", ""),
            "fc_pb": filters.get("fc_pb", ""),
            "fc_pr": "",
            "fc_ra": "",
            "fc_rp": "",
            "npagina": str(page),
            "itemsporpagina": str(items_per_page),
            "totalitems": str(total),
            "orden": "2",
            "exacta": "0",
            "tipoviene": "1",
            "seleccionado": "0",
        }
        return self._get(self._search_url, params=params)

    def close(self) -> None:
        self._session.close()

    # ── Internal helpers ──

    def _get(self, url: str, params: dict | None = None) -> bytes:
        """GET with retry and rate limiting."""
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                r = self._session.get(url, params=params, timeout=self._timeout)
                r.raise_for_status()
                time.sleep(self._rate_delay)
                return r.content
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    wait = 2**attempt
                    logger.warning(
                        "Request failed (attempt %d/%d): %s — retrying in %ds",
                        attempt,
                        self._max_retries,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
        raise last_exc  # type: ignore[misc]
