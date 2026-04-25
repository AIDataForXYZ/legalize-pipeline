"""Mexico norm discovery — multi-source.

Walks the catalog of each registered source and yields prefixed norm IDs.
Today only Diputados is wired; the other sources yield nothing until their
discovery is implemented.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.mx.client import MXClient

logger = logging.getLogger(__name__)


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
        if source_name == "diputados":
            index = client.diputados_index()
            prefix = client.sources["diputados"].id_prefix
            for abbrev in sorted(index):
                yield f"{prefix}-{abbrev}"
            return
        # Other sources: not yet wired.
        return iter([])

    def _discover_source_daily(
        self,
        client: MXClient,
        source_name: str,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        # Diputados has no per-day publication feed (DOF is the daily gazette).
        # Until DOF discovery lands, daily yields nothing for every source.
        return iter([])
