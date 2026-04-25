"""Mexico norm discovery — scaffold.

Yields nothing until the source is wired up. See ADDING_A_COUNTRY.md Step 0.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery


class MXDiscovery(NormDiscovery):
    """Catalog discovery for Mexican legislation."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        return iter([])

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        return iter([])
