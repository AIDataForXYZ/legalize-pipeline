"""e-Sbírka HTTP client for Czech Republic legislation.

Base: https://e-sbirka.gov.cz/sbr-cache/
No authentication required for the cache layer.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
from typing import TYPE_CHECKING, Any

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

_DEFAULT_BASE = "https://e-sbirka.gov.cz/sbr-cache"


class ESbirkaClient(HttpClient):
    """HTTP client for the Czech e-Sbírka legislation API.

    Endpoints used:
    - GET  /dokumenty-sbirky/{encoded_url}            → metadata JSON
    - GET  /dokumenty-sbirky/{encoded_url}/fragmenty   → text fragments JSON
    - POST /jednoducha-vyhledavani                     → search JSON
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> ESbirkaClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", _DEFAULT_BASE),
            requests_per_second=source.get("requests_per_second", 2.0),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
        )

    def __init__(
        self,
        base_url: str = _DEFAULT_BASE,
        requests_per_second: float = 2.0,
        request_timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        super().__init__(
            base_url=base_url,
            requests_per_second=requests_per_second,
            request_timeout=request_timeout,
            max_retries=max_retries,
            extra_headers={"Accept": "application/json"},
        )

    @staticmethod
    def _encode_url(stale_url: str) -> str:
        """URL-encode a staleUrl for use in API paths.

        e.g. "/sb/1993/1" → "%2Fsb%2F1993%2F1"
        """
        return urllib.parse.quote(stale_url, safe="")

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata for a law by its staleUrl.

        norm_id is the staleUrl without date, e.g. "/sb/1993/1".
        """
        url = f"{self._base_url}/dokumenty-sbirky/{self._encode_url(norm_id)}"
        return self._get(url)

    def get_metadata_at_date(self, norm_id: str, version_date: str) -> bytes:
        """Fetch metadata for a specific version of a law.

        version_date is ISO format, e.g. "2026-01-01".
        The versioned staleUrl is e.g. "/sb/1993/1/2026-01-01".
        """
        versioned = f"{norm_id}/{version_date}"
        url = f"{self._base_url}/dokumenty-sbirky/{self._encode_url(versioned)}"
        return self._get(url)

    def get_text(self, norm_id: str) -> bytes:
        """Fetch ALL text fragments for a law (auto-paginates).

        Returns JSON list of all fragments merged across pages.
        The pipeline calls this via generic_daily/bootstrap.
        """
        fragments = self._fetch_all_pages(norm_id)
        return json.dumps(fragments).encode("utf-8")

    def get_text_page(self, norm_id: str, page: int = 0) -> bytes:
        """Fetch a specific page of text fragments."""
        url = f"{self._base_url}/dokumenty-sbirky/{self._encode_url(norm_id)}/fragmenty"
        return self._get(url, params={"cisloStranky": str(page)})

    def get_text_at_date(self, norm_id: str, version_date: str, page: int = 0) -> bytes:
        """Fetch text fragments for a specific version of a law."""
        versioned = f"{norm_id}/{version_date}"
        url = f"{self._base_url}/dokumenty-sbirky/{self._encode_url(versioned)}/fragmenty"
        return self._get(url, params={"cisloStranky": str(page)})

    def get_all_fragments(self, norm_id: str) -> list[dict]:
        """Fetch ALL text fragments across all pages for a law."""
        return self._fetch_all_pages(norm_id)

    def get_all_fragments_at_date(self, norm_id: str, version_date: str) -> list[dict]:
        """Fetch ALL text fragments for a specific version."""
        versioned = f"{norm_id}/{version_date}"
        return self._fetch_all_pages(versioned)

    def _fetch_all_pages(self, stale_url: str) -> list[dict]:
        """Paginate through all fragment pages and collect into a single list."""
        first_page = json.loads(self.get_text_page(stale_url, page=0))
        all_fragments = first_page.get("seznam", [])
        total_pages = first_page.get("pocetStranek", 1)

        for page_num in range(1, total_pages):
            page_data = json.loads(self.get_text_page(stale_url, page=page_num))
            all_fragments.extend(page_data.get("seznam", []))

        return all_fragments

    def search(
        self,
        *,
        fulltext: str = "",
        start: int = 0,
        count: int = 25,
        facet_filter: dict[str, Any] | None = None,
    ) -> dict:
        """Search the e-Sbírka catalog.

        Returns dict with keys: pocetCelkem, seznam, fazetovyFiltr.
        """
        body: dict[str, Any] = {
            "fulltext": fulltext,
            "start": start,
            "pocet": count,
            "razeni": ["+relevance"],
        }
        if facet_filter:
            body["fazetovyFiltr"] = facet_filter

        url = f"{self._base_url}/jednoducha-vyhledavani"
        resp = self._request("POST", url, json=body)
        return resp.json()
