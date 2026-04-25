"""Mexico HTTP client — multi-source scaffold.

Mexican legislation is not concentrated in a single portal. Each source
(Cámara de Diputados, Diario Oficial de la Federación, Orden Jurídico
Nacional, …) has its own host, URL pattern, and document format. This client
holds a registry of named sources and routes each fetch to the right one
based on a norm_id prefix (``DIP-…``, ``DOF-…``, ``OJN-…``).

Sources are configured via ``config.yaml::countries.mx.source.sources`` —
adding a new source is a config change plus a per-source URL builder; the
generic plumbing (rate limiting, retries, session reuse) is inherited from
``HttpClient``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"

# Default seed sources — wire each one up in Step 0 research before flipping
# its `enabled` flag in config.yaml.
DEFAULT_SOURCES: dict[str, dict] = {
    "diputados": {
        "base_url": "https://www.diputados.gob.mx",
        "id_prefix": "DIP",
    },
    "dof": {
        "base_url": "https://www.dof.gob.mx",
        "id_prefix": "DOF",
    },
    "ojn": {
        "base_url": "https://www.ordenjuridico.gob.mx",
        "id_prefix": "OJN",
    },
}


@dataclass(frozen=True)
class MXSource:
    """A single Mexican legislative source registered with MXClient."""

    name: str
    base_url: str
    id_prefix: str


class MXClient(HttpClient):
    """Multi-source HTTP client for Mexican legislation.

    Sources are passed as ``{name: {base_url, id_prefix}}``. The single shared
    ``requests.Session`` (with rate limiting and retries) is used across all
    sources; per-source URL construction lives in ``_url_for``.
    """

    def __init__(self, sources: dict[str, dict] | None = None, **kwargs) -> None:
        kwargs.setdefault("user_agent", DEFAULT_USER_AGENT)
        kwargs.setdefault("requests_per_second", 1.0)
        super().__init__(**kwargs)
        raw = sources or DEFAULT_SOURCES
        self._sources: dict[str, MXSource] = {}
        self._by_prefix: dict[str, MXSource] = {}
        for name, conf in raw.items():
            src = MXSource(
                name=name,
                base_url=str(conf["base_url"]).rstrip("/"),
                id_prefix=str(conf["id_prefix"]),
            )
            self._sources[name] = src
            self._by_prefix[src.id_prefix] = src

    @classmethod
    def create(cls, country_config: CountryConfig) -> MXClient:
        source = country_config.source or {}
        return cls(
            sources=source.get("sources"),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
            requests_per_second=source.get("requests_per_second", 1.0),
        )

    @property
    def sources(self) -> dict[str, MXSource]:
        return dict(self._sources)

    def source_for(self, norm_id: str) -> MXSource:
        """Resolve which source owns ``norm_id`` based on its prefix."""
        prefix = norm_id.split("-", 1)[0] if "-" in norm_id else norm_id
        if prefix not in self._by_prefix:
            available = ", ".join(sorted(self._by_prefix)) or "<none>"
            raise ValueError(
                f"No MX source registered for id prefix '{prefix}' "
                f"(norm_id={norm_id!r}). Available prefixes: {available}"
            )
        return self._by_prefix[prefix]

    def _url_for(self, source: MXSource, norm_id: str) -> str:
        """Build the canonical URL for a norm. Per-source logic lives here."""
        raise NotImplementedError(
            f"URL builder for source '{source.name}' not implemented yet."
        )

    def get_text(self, norm_id: str) -> bytes:
        source = self.source_for(norm_id)
        return self._get(self._url_for(source, norm_id))

    def get_metadata(self, norm_id: str) -> bytes:
        # Most candidate sources serve text + metadata on the same URL; override
        # per-source if a separate metadata endpoint exists.
        return self.get_text(norm_id)
