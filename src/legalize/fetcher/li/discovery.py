"""Norm discovery for Liechtenstein (Lilex / gesetze.li).

Lilex exposes the catalog via the systematic register
(`/konso/gebietssystematik?lrstart={code}`). The tree splits into
17 top-level codes (0.1-0.9 international treaties, 1-9 domestic law).
Each page lists either deeper categories or the consolidated laws
under that category (or both). A recursive crawl yields the full set
of in-force consolidated laws.

Daily discovery scrapes `/chrono/neueste-lgbl` and filters by date.
"""

from __future__ import annotations

import logging
import re
from collections import deque
from collections.abc import Iterator
from datetime import date, datetime

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.li.client import LilexClient, to_dotted_id

logger = logging.getLogger(__name__)

# Top-level Gebietssystematik branches (Landesrecht 1-9, Staatsverträge 0.1-0.9)
_TOP_LEVEL_LR_CODES = (
    "0.1",
    "0.2",
    "0.3",
    "0.4",
    "0.5",
    "0.6",
    "0.7",
    "0.8",
    "0.9",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
)

_NAV_RE = re.compile(r'href="/konso/gebietssystematik\?lrstart=([^"&]+)"')
_LAW_RE = re.compile(r'href="/konso/(\d{10})"')

# Chrono listing rows look like:
#   <td>...<a href="/chrono/2026071000">...
#   ...<td class="hidden-for-mobile-portrait">15.04.2026</td>
_CHRONO_ROW_RE = re.compile(
    r'href="/chrono/(\d{10})"[^<]*</a>.*?(\d{2}\.\d{2}\.\d{4})',
    re.DOTALL,
)


class LilexDiscovery(NormDiscovery):
    """Crawl the Lilex systematic register to enumerate all consolidated laws."""

    @classmethod
    def create(cls, source: dict) -> "LilexDiscovery":
        return cls()

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield LGBl numbers (dotted, e.g. '1921.015') for every law in the catalog."""
        if not isinstance(client, LilexClient):
            raise TypeError("LilexDiscovery requires LilexClient")

        seen_nav: set[str] = set(_TOP_LEVEL_LR_CODES)
        seen_laws: set[str] = set()
        queue: deque[str] = deque(_TOP_LEVEL_LR_CODES)

        pages = 0
        while queue:
            code = queue.popleft()
            try:
                body = client.get_page("/konso/gebietssystematik", lrstart=code)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Discovery: failed to fetch lrstart=%s: %s", code, exc)
                continue
            pages += 1
            html = body.decode("utf-8", errors="replace")

            for nav in _NAV_RE.findall(html):
                if nav not in seen_nav:
                    seen_nav.add(nav)
                    queue.append(nav)

            for url_id in _LAW_RE.findall(html):
                if url_id in seen_laws:
                    continue
                seen_laws.add(url_id)
                yield to_dotted_id(url_id)

            if pages % 25 == 0:
                logger.info(
                    "Discovery progress: %d nav pages, %d laws found, %d queued",
                    pages,
                    len(seen_laws),
                    len(queue),
                )

        logger.info(
            "Discovery complete: %d nav pages crawled, %d laws found",
            pages,
            len(seen_laws),
        )

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """Yield LGBl numbers published on a specific date.

        Reads `/chrono/neueste-lgbl` (the recent gazette listing) and filters
        rows whose publication date matches `target_date`.
        """
        if not isinstance(client, LilexClient):
            raise TypeError("LilexDiscovery requires LilexClient")

        try:
            body = client.get_page("/chrono/neueste-lgbl")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Daily discovery: failed to fetch chrono listing: %s", exc)
            return

        html = body.decode("utf-8", errors="replace")
        target_str = target_date.strftime("%d.%m.%Y")

        seen: set[str] = set()
        for url_id, date_text in _CHRONO_ROW_RE.findall(html):
            if date_text != target_str:
                continue
            try:
                datetime.strptime(date_text, "%d.%m.%Y")
            except ValueError:
                continue
            if url_id in seen:
                continue
            seen.add(url_id)
            yield to_dotted_id(url_id)
