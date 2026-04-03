"""Lithuania data.gov.lt Spinta API client.

Single source: https://get.data.gov.lt (Spinta API, UAPI spec)
All data (metadata + full text via tekstas_lt) comes from data.gov.lt.
e-tar.lt is only used for source URLs, not for fetching.
License: Open data (Creative Commons)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

DEFAULT_API_URL = "https://get.data.gov.lt"
DEFAULT_DATASET = "datasets/gov/lrsk/teises_aktai/Dokumentas"

# Fields needed for metadata
_META_FIELDS = (
    "dokumento_id,pavadinimas,alt_pavadinimas,rusis,galioj_busena,"
    "priimtas,isigalioja,negalioja,priemusi_inst,nuoroda,tar_kodas,pakeista"
)

# Fields needed for discovery
_DISCOVERY_FIELDS = "dokumento_id,rusis,galioj_busena,priimtas,pavadinimas"


class TARClient(HttpClient):
    """HTTP client for Lithuanian legislation via data.gov.lt Spinta API.

    Single-source: both metadata and full text (tekstas_lt field)
    come from the same API. e-tar.lt has Cloudflare protection and
    is not used for fetching.
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> TARClient:
        """Create TARClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            api_url=source.get("api_url", DEFAULT_API_URL),
            dataset=source.get("dataset", DEFAULT_DATASET),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 5),
            requests_per_second=source.get("requests_per_second", 2.0),
        )

    def __init__(
        self,
        api_url: str = DEFAULT_API_URL,
        dataset: str = DEFAULT_DATASET,
        **kwargs,
    ) -> None:
        super().__init__(base_url=api_url, **kwargs)
        self._dataset = dataset

    def get_text(self, norm_id: str) -> bytes:
        """Fetch full text from data.gov.lt via the tekstas_lt field."""
        url = (
            f"{self._base_url}/{self._dataset}"
            f'?dokumento_id="{norm_id}"&select(tekstas_lt,priimtas)&limit(1)'
        )
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata JSON from data.gov.lt Spinta API."""
        url = (
            f"{self._base_url}/{self._dataset}"
            f'?dokumento_id="{norm_id}"&select({_META_FIELDS})&limit(1)'
        )
        return self._get(url)

    def get_page(self, page_size: int = 100, cursor: str | None = None) -> bytes:
        """Fetch a page of documents from the Spinta API."""
        url = (
            f"{self._base_url}/{self._dataset}"
            f"?select({_DISCOVERY_FIELDS})&sort(dokumento_id)&limit({page_size})"
        )
        if cursor:
            url += f'&page("{cursor}")'
        return self._get(url)

    def get_page_by_date(
        self, target_date: str, page_size: int = 100, cursor: str | None = None
    ) -> bytes:
        """Fetch documents adopted on a specific date (server-side filter)."""
        url = (
            f"{self._base_url}/{self._dataset}"
            f'?priimtas="{target_date}"'
            f"&select({_DISCOVERY_FIELDS})&sort(dokumento_id)&limit({page_size})"
        )
        if cursor:
            url += f'&page("{cursor}")'
        return self._get(url)
