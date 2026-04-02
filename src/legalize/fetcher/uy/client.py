"""Uruguay IMPO (Centro de Informacion Oficial) HTTP client.

Data source: https://www.impo.com.uy/bases/
License: Licencia de Datos Abiertos del Uruguay (Decreto 54/2017) — attribution only.

Any norm URL + ?json=true returns structured JSON with full text and metadata.
Encoding: Latin-1 (ISO-8859-1).
"""

from __future__ import annotations

import logging
import time

import requests

from legalize.fetcher.base import LegislativeClient

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.impo.com.uy"
DEFAULT_RATE_LIMIT = 1.0  # requests per second (conservative, undocumented)
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3


class IMPOClient(LegislativeClient):
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
            requests_per_second=source.get("requests_per_second", DEFAULT_RATE_LIMIT),
            request_timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
        )

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        requests_per_second: float = DEFAULT_RATE_LIMIT,
        request_timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._delay = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self._timeout = request_timeout
        self._max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)",
            }
        )

    def get_text(self, norm_id: str) -> bytes:
        """Fetch JSON for a norm.

        Args:
            norm_id: URL path segment, e.g. "leyes/19996-2021"

        Returns:
            Raw JSON bytes (Latin-1 encoded from IMPO).
            Empty bytes if the norm does not exist (404).
        """
        return self._fetch_json(norm_id)

    def get_metadata(self, norm_id: str) -> bytes:
        """Same as get_text — metadata is embedded in the JSON response."""
        return self.get_text(norm_id)

    def close(self) -> None:
        self._session.close()

    def _fetch_json(self, norm_id: str) -> bytes:
        """Fetch JSON from IMPO with rate limiting and retries.

        IMPO returns HTTP 200 with an HTML login page (not 404) for
        non-existent norms. We detect this by checking that the response
        body starts with '{' (valid JSON object).
        """
        url = f"{self._base_url}/bases/{norm_id}?json=true"

        for attempt in range(self._max_retries):
            try:
                time.sleep(self._delay)
                resp = self._session.get(url, timeout=self._timeout)

                if resp.status_code == 404:
                    return b""
                resp.raise_for_status()

                # IMPO returns 200 + HTML page for missing norms instead of 404.
                # Valid responses are JSON objects starting with '{'.
                content = resp.content.lstrip()
                if not content or not content.startswith(b"{"):
                    logger.debug("Non-JSON response for %s, treating as not found", norm_id)
                    return b""

                return resp.content

            except requests.RequestException:
                if attempt == self._max_retries - 1:
                    raise
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "IMPO request failed (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1,
                    self._max_retries,
                    wait,
                    url,
                )
                time.sleep(wait)

        return b""  # unreachable, but satisfies type checker
