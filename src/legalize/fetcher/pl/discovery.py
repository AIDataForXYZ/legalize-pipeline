"""Norm discovery for Poland's ELI API.

Two strategies:
- discover_all: paginate /acts/search by year (1918..current), filter HTML-only.
- discover_daily: use /changes/acts?since=DATE to find acts published on a date.

The API's WAF rejects unknown query parameters, so textHTML/inForce filtering
has to happen on the JSON response client-side.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date, datetime

from legalize.fetcher.base import NormDiscovery
from legalize.fetcher.pl.client import SEARCH_LIMIT_MAX, EliClient, eli_to_norm_id

logger = logging.getLogger(__name__)


class EliDiscovery(NormDiscovery):
    """Discover Polish legislative acts via the Sejm ELI API."""

    def __init__(
        self,
        *,
        year_start: int = 1918,
        year_end: int | None = None,
        html_only: bool = True,
    ) -> None:
        self.year_start = year_start
        self.year_end = year_end  # None → current year at discovery time
        self.html_only = html_only

    @classmethod
    def create(cls, source: dict) -> EliDiscovery:
        return cls(
            year_start=int(source.get("year_start", 1918)),
            year_end=source.get("year_end"),
            html_only=bool(source.get("html_only", True)),
        )

    def _iter_year(self, client: EliClient, year: int) -> Iterator[dict]:
        """Yield every item in the search response for a given year."""
        offset = 0
        while True:
            try:
                data = client.search_year(year, offset=offset, limit=SEARCH_LIMIT_MAX)
            except Exception as exc:
                logger.warning("search_year(%d, offset=%d) failed: %s", year, offset, exc)
                return
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                logger.warning("search_year(%d, offset=%d) returned non-JSON", year, offset)
                return

            items = payload.get("items") or []
            total = int(payload.get("totalCount") or 0)
            count = int(payload.get("count") or len(items))

            for item in items:
                yield item

            if count == 0 or offset + count >= total:
                return
            offset += count

    def discover_all(self, client: EliClient, **kwargs) -> Iterator[str]:  # type: ignore[override]
        """Yield every DU act ID with HTML available, year by year."""
        current_year = datetime.now().year
        end_year = self.year_end or current_year

        for year in range(self.year_start, end_year + 1):
            count_in_year = 0
            count_html = 0
            for item in self._iter_year(client, year):
                count_in_year += 1
                if self.html_only and not item.get("textHTML"):
                    continue
                count_html += 1
                eli = item.get("ELI")
                if not eli:
                    continue
                yield eli_to_norm_id(eli)
            if count_in_year:
                logger.info(
                    "PL discovery year=%d total=%d html=%d", year, count_in_year, count_html
                )

    def discover_daily(
        self,
        client: EliClient,
        target_date: date,
        **kwargs,  # noqa: ARG002
    ) -> Iterator[str]:
        """Yield acts whose announcementDate equals target_date.

        Uses the /changes/acts feed: asks for changes since midnight of the
        target date, then filters to only items whose announcementDate
        actually matches (the changes feed returns any touched record,
        including late metadata edits on older acts).
        """
        since = target_date.strftime("%Y-%m-%dT00:00:00")
        target_iso = target_date.isoformat()
        offset = 0
        seen: set[str] = set()

        while True:
            try:
                data = client.get_changes(since, offset=offset, limit=SEARCH_LIMIT_MAX)
            except Exception as exc:
                logger.warning("get_changes(since=%s, offset=%d) failed: %s", since, offset, exc)
                return
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                logger.warning("get_changes(since=%s) returned non-JSON", since)
                return

            items = payload.get("items") or []
            total = int(payload.get("totalCount") or 0)
            count = int(payload.get("count") or len(items))

            for item in items:
                if item.get("announcementDate") != target_iso:
                    continue
                if self.html_only and not item.get("textHTML"):
                    continue
                eli = item.get("ELI")
                if not eli:
                    continue
                norm_id = eli_to_norm_id(eli)
                if norm_id in seen:
                    continue
                seen.add(norm_id)
                yield norm_id

            if count == 0 or offset + count >= total:
                return
            offset += count
