"""Uruguay IMPO (Centro de Informacion Oficial) HTTP client.

Data source: https://www.impo.com.uy/bases/
License: Licencia de Datos Abiertos del Uruguay (Decreto 54/2017) — attribution only.

Any norm URL + ?json=true returns structured JSON with full text and metadata.
Encoding: Latin-1 (ISO-8859-1).
"""

from __future__ import annotations

import logging

import requests

from legalize.fetcher.base import HttpClient

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.impo.com.uy"


class IMPOClient(HttpClient):
    """HTTP client for the Uruguayan IMPO open data API.

    Append ?json=true to any norm URL to get structured JSON.
    Schema: https://www.impo.com.uy/resources/basesIMPO.json
    """

    @classmethod
    def create(cls, country_config):
        """Create IMPOClient from CountryConfig."""
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", DEFAULT_BASE_URL),
            requests_per_second=source.get("requests_per_second", 1.0),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
        )

    def get_text(self, norm_id: str) -> bytes:
        """Fetch JSON for a norm.

        Returns raw JSON bytes. Empty bytes if the norm does not exist.
        """
        return self._fetch_json(norm_id)

    def get_metadata(self, norm_id: str) -> bytes:
        """Same as get_text — metadata is embedded in the JSON response."""
        return self.get_text(norm_id)

    def _fetch_json(self, norm_id: str) -> bytes:
        """Fetch JSON from IMPO, handling IMPO's non-standard 404 behaviour.

        IMPO returns HTTP 200 with an HTML login page (not 404) for
        non-existent norms. We detect this by checking that the response
        body starts with '{' (valid JSON object).
        """
        url = f"{self._base_url}/bases/{norm_id}?json=true"

        try:
            resp = self._request("GET", url)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return b""
            raise

        content = resp.content.lstrip()
        if not content or not content.startswith(b"{"):
            logger.debug("Non-JSON response for %s, treating as not found", norm_id)
            return b""

        return resp.content
