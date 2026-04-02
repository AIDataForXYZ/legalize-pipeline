"""Discovery of Lithuanian legal acts via the data.gov.lt Spinta API."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.lt.client import TARClient


class TARDiscovery(NormDiscovery):
    """Discovers all legal acts in the Lithuanian TAR catalog via data.gov.lt."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all TAR identifiers by paginating the Spinta API.

        Uses cursor-based pagination via _page.next tokens.
        """
        assert isinstance(client, TARClient)
        seen: set[str] = set()
        cursor: str | None = None

        while True:
            raw = client.get_page(page_size=100, cursor=cursor)
            data = json.loads(raw)
            items = data.get("_data", [])

            if not items:
                break

            for item in items:
                tar_id = item.get("tar_identifikatorius", "")
                if tar_id and tar_id not in seen:
                    seen.add(tar_id)
                    yield tar_id

            next_cursor = data.get("_page", {}).get("next")
            if not next_cursor:
                break
            cursor = next_cursor

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield TAR identifiers registered on target_date.

        Fetches all recent items and filters client-side by priemimo_data.
        """
        assert isinstance(client, TARClient)
        seen: set[str] = set()
        date_str = target_date.isoformat()
        cursor: str | None = None

        while True:
            raw = client.get_page(page_size=100, cursor=cursor)
            data = json.loads(raw)
            items = data.get("_data", [])

            if not items:
                break

            for item in items:
                priemimo = item.get("priemimo_data", "")
                if priemimo != date_str:
                    continue
                tar_id = item.get("tar_identifikatorius", "")
                if tar_id and tar_id not in seen:
                    seen.add(tar_id)
                    yield tar_id

            next_cursor = data.get("_page", {}).get("next")
            if not next_cursor:
                break
            cursor = next_cursor
