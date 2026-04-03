"""Client for the Riksdagen Open Data API (Swedish legislation).

Fetches Swedish statutes (SFS) from:
  https://data.riksdagen.se/dokumentlista/ — document search
  https://data.riksdagen.se/dokument/{dok_id}.json — full document
  https://rkrattsbaser.gov.se/sfsr?bet={SFS} — amendment register (SFSR)

Rate limited to 10 req/s with retry on 429/503.
"""

from __future__ import annotations

import logging
from urllib.parse import quote

from legalize.fetcher.base import HttpClient

logger = logging.getLogger(__name__)

_RIKSDAGEN_LIST_URL = "https://data.riksdagen.se/dokumentlista"
_RIKSDAGEN_DOC_URL = "https://data.riksdagen.se/dokument"
_SFSR_URL = "https://rkrattsbaser.gov.se/sfsr"

_USER_AGENT = "legalize-bot/1.0"
_RATE_LIMIT_RPS = 10.0  # 100ms between requests
_MAX_RETRIES = 3


class SwedishClient(HttpClient):
    """Fetches Swedish legislation from the Riksdagen Open Data API.

    Two-step fetch for text:
    1. Search /dokumentlista/?sok={SFS}&doktyp=sfs to find dok_id
    2. Fetch /dokument/{dok_id}.json for full document with text

    Amendment register is fetched from rkrattsbaser.gov.se (SFSR HTML).
    """

    @classmethod
    def create(cls, country_config):
        """Create SwedishClient from CountryConfig."""
        return cls()

    def __init__(self) -> None:
        super().__init__(
            user_agent=_USER_AGENT,
            request_timeout=30,
            max_retries=_MAX_RETRIES,
            requests_per_second=_RATE_LIMIT_RPS,
            extra_headers={"Accept": "application/json"},
        )

    def get_text(self, norm_id: str) -> bytes:
        """Fetch the full document JSON for a Swedish statute.

        Searches Riksdagen API by SFS number, extracts dok_id,
        then fetches the full document JSON.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            Full document JSON as bytes.
        """
        dok_id = self._find_dok_id(norm_id)
        url = f"{_RIKSDAGEN_DOC_URL}/{dok_id}.json"
        logger.info("Fetching document text: %s", url)
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata for a Swedish statute.

        Uses the same endpoint as get_text — metadata is embedded
        in the full document JSON (dokumentstatus.dokuppgift).

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            Full document JSON as bytes (same as get_text).
        """
        return self.get_text(norm_id)

    def get_amendment_register(self, norm_id: str) -> bytes:
        """Fetch the SFSR amendment register for a statute.

        Fetches HTML from rkrattsbaser.gov.se/sfsr?bet={SFS}.
        Contains amendment history with affected sections.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            SFSR HTML page as bytes.
        """
        url = f"{_SFSR_URL}?bet={quote(norm_id)}"
        logger.info("Fetching SFSR amendment register: %s", url)
        return self._get(url, headers={"Accept": "text/html"})

    # ── Internal helpers ──

    def _find_dok_id(self, norm_id: str) -> str:
        """Search Riksdagen API by SFS number to find the dok_id.

        Args:
            norm_id: SFS number, e.g. "1962:700"

        Returns:
            The dok_id string for the matching document.

        Raises:
            ValueError: If no document is found for the SFS number.
        """
        url = f"{_RIKSDAGEN_LIST_URL}/?sok={quote(norm_id)}&doktyp=sfs&format=json&utformat=json"
        logger.debug("Searching Riksdagen for SFS %s", norm_id)
        data = self._get(url)

        import json

        result = json.loads(data)
        documents = result.get("dokumentlista", {}).get("dokument") or []

        if not documents:
            raise ValueError(f"No Riksdagen document found for SFS {norm_id}")

        # Prefer exact match on beteckning
        for doc in documents:
            if doc.get("beteckning") == norm_id:
                dok_id = doc["dok_id"]
                logger.debug("Found dok_id %s for SFS %s", dok_id, norm_id)
                return dok_id

        # Fallback: first result
        dok_id = documents[0]["dok_id"]
        logger.warning(
            "No exact match for SFS %s, using first result: %s",
            norm_id,
            dok_id,
        )
        return dok_id
