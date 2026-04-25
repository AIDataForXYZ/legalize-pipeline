"""Mexico norm discovery — multi-source scaffold.

Iterates each registered source in ``MXClient`` and yields prefixed norm IDs.
Each source's actual catalog walk is implemented as a per-source helper
(``_discover_diputados``, ``_discover_dof``, …) once the source is wired up.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.mx.client import MXClient


class MXDiscovery(NormDiscovery):
    """Catalog discovery across the registered Mexican sources."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        if not isinstance(client, MXClient):
            raise TypeError(f"MXDiscovery requires MXClient, got {type(client).__name__}")
        for source in client.sources.values():
            yield from self._discover_source_all(client, source.name, **kwargs)

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        if not isinstance(client, MXClient):
            raise TypeError(f"MXDiscovery requires MXClient, got {type(client).__name__}")
        for source in client.sources.values():
            yield from self._discover_source_daily(client, source.name, target_date, **kwargs)

    def _discover_source_all(
        self, client: MXClient, source_name: str, **kwargs
    ) -> Iterator[str]:
        """Per-source catalog walk. Override per source once research lands."""
        return iter([])

    def _discover_source_daily(
        self,
        client: MXClient,
        source_name: str,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """Per-source daily feed. Override per source once research lands."""
        return iter([])
