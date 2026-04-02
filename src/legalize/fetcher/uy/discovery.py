"""Discovery of Uruguayan norms via IMPO number range iteration.

IMPO has no catalog/search endpoint. Discovery works by iterating through
sequential law numbers and checking which ones exist. Law numbers run from
~1 (1826) to ~20500 (2026), but post-1935 laws (9500+) are most reliable.

IMPORTANT: IMPO requires the correct year in the URL. There is no redirect
or fallback — a wrong year returns an HTML login page (treated as not found).
We use an approximate number→year mapping to estimate the year, then try
a few candidates around that estimate.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.uy.client import IMPOClient

logger = logging.getLogger(__name__)

# Collections to iterate during full discovery
DEFAULT_COLLECTIONS = ("leyes", "decretos-ley", "constitucion")

# Approximate law number → year mapping (based on real IMPO data).
# Used to estimate the year for a given law number.
# Format: (law_number, year) — sorted by law_number.
_NUMBER_YEAR_LANDMARKS = (
    (1, 1826),
    (1000, 1870),
    (3000, 1905),
    (5000, 1914),
    (7000, 1919),
    (9000, 1932),
    (10000, 1941),
    (11000, 1948),
    (12000, 1953),
    (13000, 1960),
    (14000, 1971),
    (15000, 1980),
    (16000, 1989),
    (17000, 1998),
    (18000, 2006),
    (19000, 2012),
    (19500, 2017),
    (20000, 2021),
    (20200, 2023),
    (20400, 2025),
    (20500, 2026),
)


def _estimate_year(law_number: int) -> int:
    """Estimate the year for a law number using linear interpolation."""
    if law_number <= _NUMBER_YEAR_LANDMARKS[0][0]:
        return _NUMBER_YEAR_LANDMARKS[0][1]
    if law_number >= _NUMBER_YEAR_LANDMARKS[-1][0]:
        return _NUMBER_YEAR_LANDMARKS[-1][1]

    for i in range(len(_NUMBER_YEAR_LANDMARKS) - 1):
        n0, y0 = _NUMBER_YEAR_LANDMARKS[i]
        n1, y1 = _NUMBER_YEAR_LANDMARKS[i + 1]
        if n0 <= law_number <= n1:
            # Linear interpolation
            frac = (law_number - n0) / (n1 - n0)
            return int(y0 + frac * (y1 - y0))

    return _NUMBER_YEAR_LANDMARKS[-1][1]


def _year_candidates(law_number: int) -> list[int]:
    """Return a list of candidate years to try for a law number.

    Tries the estimated year first, then ±1, ±2 to handle estimation error.
    """
    est = _estimate_year(law_number)
    return [est, est - 1, est + 1, est - 2, est + 2]


class IMPODiscovery(NormDiscovery):
    """Discovers Uruguayan norms by iterating number ranges on IMPO."""

    @classmethod
    def create(cls, source: dict) -> IMPODiscovery:
        collections = source.get("collections", list(DEFAULT_COLLECTIONS))
        law_number_max = source.get("law_number_max", 20500)
        return cls(collections=collections, law_number_max=law_number_max)

    def __init__(
        self,
        collections: list[str] | None = None,
        law_number_max: int = 20500,
    ) -> None:
        self._collections = collections or list(DEFAULT_COLLECTIONS)
        self._law_number_max = law_number_max

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield norm IDs for all collections.

        For 'leyes': iterates numbers 1 to law_number_max, trying
        estimated years based on the number→year mapping.

        For 'constitucion': yields the single known entry.
        For 'decretos-ley': iterates the 1973-1985 range.
        """
        assert isinstance(client, IMPOClient)

        for collection in self._collections:
            if collection == "constitucion":
                yield from self._discover_constitucion(client)
            elif collection == "leyes":
                yield from self._discover_leyes(client, **kwargs)
            elif collection == "decretos-ley":
                yield from self._discover_decretos_ley(client, **kwargs)
            else:
                logger.warning("Unknown collection: %s", collection)

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Check recent law numbers above the last known max.

        Tries numbers from last_known+1 to last_known+100,
        using target_date.year and the previous year as candidates.
        """
        assert isinstance(client, IMPOClient)
        start = kwargs.get("last_known_number", self._law_number_max)
        year = target_date.year

        for num in range(start + 1, start + 100):
            for y in (year, year - 1):
                norm_id = f"leyes/{num}-{y}"
                data = client.get_text(norm_id)
                if data:
                    logger.info("Found new law: %s", norm_id)
                    yield norm_id
                    break

    def _discover_constitucion(self, client: IMPOClient) -> Iterator[str]:
        """Yield the single Constitution entry."""
        norm_id = "constitucion/1967-1967"
        data = client.get_text(norm_id)
        if data:
            yield norm_id

    def _discover_leyes(self, client: IMPOClient, **kwargs) -> Iterator[str]:
        """Iterate law numbers, trying estimated year candidates for each."""
        limit = kwargs.get("limit", self._law_number_max)
        start = kwargs.get("start", 1)
        found = 0

        for num in range(start, min(limit + 1, self._law_number_max + 1)):
            for year in _year_candidates(num):
                norm_id = f"leyes/{num}-{year}"
                data = client.get_text(norm_id)
                if data:
                    found += 1
                    if found % 100 == 0:
                        logger.info("Laws discovered so far: %d (at number %d)", found, num)
                    yield norm_id
                    break  # found the right year, move to next number

    def _discover_decretos_ley(self, client: IMPOClient, **kwargs) -> Iterator[str]:
        """Iterate decreto-ley numbers (1973-1985 period)."""
        limit = kwargs.get("limit", 16000)
        start = kwargs.get("start", 14000)

        for num in range(start, limit + 1):
            for year in range(1973, 1986):
                norm_id = f"decretos-ley/{num}-{year}"
                data = client.get_text(norm_id)
                if data:
                    yield norm_id
                    break  # found the right year, move to next number
