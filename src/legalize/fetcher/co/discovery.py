"""Discovery for Colombia SUIN-Juriscol integer document IDs."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery


class SuinDiscovery(NormDiscovery):
    """Enumerate SUIN integer IDs."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Enumerate SUIN integer IDs.

        SUIN has no catalog API. We enumerate integer IDs in the range
        id_start..id_end. The pipeline filters valid law pages via the
        client (non-200 or missing title = skip).
        """
        id_start = int(kwargs.get("id_start", 1_000_000))
        id_end = int(kwargs.get("id_end", 1_950_000))
        for i in range(id_start, id_end + 1):
            yield str(i)

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """SUIN has no daily feed. Yields nothing."""
        return iter([])
