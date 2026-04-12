"""Norm discovery for Czech Republic via e-Sbírka API.

The e-Sbírka search API does not support facet filtering (the
fazetovyFiltr field in the request is ignored). Discovery uses two
strategies:

- discover_all: paginated search yielding all staleUrls in the Sbírka
  zákonů collection (prefix /sb/). Type filtering happens downstream
  when metadata is fetched.
- discover_daily: sequential probe of law numbers for the target year,
  checking publication dates against the target date. Czech laws are
  numbered sequentially per year (/sb/{year}/1, /sb/{year}/2, ...).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING

import requests

from legalize.fetcher.base import LegislativeClient, NormDiscovery

if TYPE_CHECKING:
    from legalize.fetcher.cz.client import ESbirkaClient

logger = logging.getLogger(__name__)

# Search page size.
_PAGE_SIZE = 100

# Maximum law number to probe per year before giving up.
# Czech Republic publishes ~400-700 acts per year.
_MAX_LAW_NUMBER = 800


class ESbirkaDiscovery(NormDiscovery):
    """Discover Czech laws via the e-Sbírka API."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield staleUrls for all acts in the Sbírka zákonů.

        Paginates through the search results. Since the API ignores
        facet filters, we yield all results and filter by collection
        prefix (/sb/) to exclude treaties (/sm/) and official gazette
        (/ul/) entries.
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        start = 0
        total = None

        while total is None or start < total:
            result = esbirka.search(start=start, count=_PAGE_SIZE)
            total = result.get("pocetCelkem", 0)
            items = result.get("seznam", [])

            if not items:
                break

            for item in items:
                stale_url = item.get("staleUrl", "")
                # Only yield Sbírka zákonů entries
                if stale_url.startswith("/sb/"):
                    yield stale_url

            start += len(items)
            if start % 1000 == 0:
                logger.info("Discovery progress: %d / %d", start, total)

        logger.info("Discovery complete: %d items scanned", start)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield staleUrls for laws published on a specific date.

        Czech laws are numbered sequentially per year. This method
        probes /sb/{year}/{n} for n=1..N, fetches metadata for each
        existing law, and yields those whose publication date matches.

        Stops after hitting 5 consecutive 400/404 responses (end of
        the sequence for the year so far).
        """
        esbirka: ESbirkaClient = client  # type: ignore[assignment]
        year = target_date.year
        target_str = target_date.isoformat()
        consecutive_misses = 0

        for n in range(1, _MAX_LAW_NUMBER + 1):
            stale_url = f"/sb/{year}/{n}"
            try:
                meta_bytes = esbirka.get_metadata(stale_url)
                consecutive_misses = 0
            except requests.HTTPError:
                consecutive_misses += 1
                if consecutive_misses >= 5:
                    logger.debug(
                        "Stopping at /sb/%d/%d after 5 consecutive misses",
                        year,
                        n,
                    )
                    break
                continue

            try:
                meta = json.loads(meta_bytes)
            except (json.JSONDecodeError, TypeError):
                continue

            pub_date = meta.get("datumCasVyhlaseni", "")[:10]
            if pub_date == target_str:
                yield stale_url
