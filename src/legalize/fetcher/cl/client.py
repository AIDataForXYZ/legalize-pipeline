"""Chile BCN (Biblioteca del Congreso Nacional) HTTP client.

Data source: https://www.leychile.cl/
Discovery: https://nuevo.leychile.cl/servicios/Consulta/script/exportarBSimpleMetas
Full text: https://www.leychile.cl/Consulta/obtxml?opt=7&idNorma={id}
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

TEXT_URL = "https://www.leychile.cl/Consulta/obtxml"
SEARCH_URL = "https://nuevo.leychile.cl/servicios/Consulta/script/exportarBSimpleMetas"

# CloudFront blocks default python-requests UA; use a browser-like one.
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class BCNClient(HttpClient):
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
            requests_per_second=source.get("requests_per_second", 1.0),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
        )

    def __init__(
        self,
        base_url: str = "https://www.leychile.cl",
        search_url: str = SEARCH_URL,
        requests_per_second: float = 1.0,
        request_timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        super().__init__(
            base_url=base_url,
            user_agent=_BROWSER_UA,
            request_timeout=request_timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
            extra_headers={"Accept": "application/xml, text/xml, text/csv, */*"},
        )
        self._search_url = search_url

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
