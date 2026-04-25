"""Mexico HTTP client — scaffold.

Source not wired up yet. Subclass of HttpClient so rate limiting, retries,
and the context-manager contract are inherited; concrete fetch methods raise
NotImplementedError until research lands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

DEFAULT_BASE_URL = "https://www.diputados.gob.mx"
DEFAULT_USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"


class MXClient(HttpClient):
    """HTTP client for Mexican legislation. Source TBD — see fetcher/mx/__init__.py."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("base_url", DEFAULT_BASE_URL)
        kwargs.setdefault("user_agent", DEFAULT_USER_AGENT)
        kwargs.setdefault("requests_per_second", 1.0)
        super().__init__(**kwargs)

    @classmethod
    def create(cls, country_config: CountryConfig) -> MXClient:
        source = country_config.source or {}
        return cls(
            base_url=source.get("base_url", DEFAULT_BASE_URL),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
            requests_per_second=source.get("requests_per_second", 1.0),
        )

    def get_text(self, norm_id: str) -> bytes:
        raise NotImplementedError("MX fetcher is a scaffold; wire the source first.")

    def get_metadata(self, norm_id: str) -> bytes:
        raise NotImplementedError("MX fetcher is a scaffold; wire the source first.")
