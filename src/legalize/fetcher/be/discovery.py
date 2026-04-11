"""Discovery of Belgian legal acts via Justel ELI listing pages.

Justel has no sitemap and no bulk dump. Discovery iterates through year-level
ELI listing pages, one request per (document_type, year) tuple, and extracts
the individual ELI URLs from the HTML.

A listing page at /eli/loi/2024 contains hundreds of <A HREF=...> links of
the form:

    http://www.ejustice.just.fgov.be/eli/loi/2024/01/07/2024000164/justel

We extract each one into a composite norm_id "loi:2024:01:07:2024000164" so
the client can rebuild the URL without a separate lookup.

Each document type has a meaningful start year:
- constitution: only the 1994 coordination (single entry)
- loi:         since 1831 (Belgian independence)
- decret:      since 1972 (regional/community parliaments)
- ordonnance:  since 1989 (Brussels Region)

Daily updates come from /cgi_loi/summary.pl which groups the ~100 most
recently consolidated texts by consolidation date. The page HTML uses one
``<div id='list-title-1'>`` per day, each containing a
``<h2 class='list-title'>DD MOIS YYYY</h2>`` header followed by the laws
consolidated on that date. We parse those date headers so ``discover_daily``
only yields the NUMACs whose consolidation date matches ``target_date``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date

from lxml import html as lxml_html

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.be.client import DOCUMENT_TYPES, JustelClient

logger = logging.getLogger(__name__)

# French month names (no accents, lowercase) for parsing the summary
# page's date headers ("10 avril 2026" etc).
_FR_MONTHS: dict[str, int] = {
    "janvier": 1,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
}
_ACCENT_MAP = str.maketrans("éèêëàâäôöùûüç", "eeeeaaaoouuuc")


def _parse_fr_date_header(text: str) -> date | None:
    """Parse a summary-page date header like '10 avril 2026' into a date."""
    normalized = text.strip().lower().translate(_ACCENT_MAP)
    match = re.match(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", normalized)
    if not match:
        return None
    day = int(match.group(1))
    month = _FR_MONTHS.get(match.group(2))
    year = int(match.group(3))
    if not month:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


# Default start years per document type. Chosen from the earliest actual
# content on Justel -- iterating earlier years just wastes HTTP requests.
START_YEAR: dict[str, int] = {
    "constitution": 1994,  # Single coordination document
    "loi": 1831,
    "decret": 1972,
    "ordonnance": 1989,
    "arrete": 1830,
}

# Regex to extract ELI URLs from a listing page. Matches absolute URLs only
# (the listing links always have the full hostname) and is case-insensitive
# because Justel's HTML 3.0 uses uppercase attribute names (HREF=).
_ELI_URL_RE = re.compile(
    r"https?://www\.ejustice\.just\.fgov\.be/eli/"
    r"(?P<dt>constitution|loi|decret|ordonnance|arrete)/"
    r"(?P<yyyy>\d{4})/"
    r"(?P<mm>\d{2})/"
    r"(?P<dd>\d{2})/"
    r"(?P<numac>\d{10})/justel",
    re.IGNORECASE,
)


def extract_norm_ids_from_listing(html_bytes: bytes) -> Iterator[str]:
    """Yield composite norm IDs from a listing page's HTML.

    Each norm ID has the shape 'dt:yyyy:mm:dd:numac'. Listing pages use
    ISO-8859-1 encoding, but since we only match ASCII URLs we can decode
    with errors='replace' for maximum tolerance.
    """
    html_str = html_bytes.decode("iso-8859-1", errors="replace")
    for match in _ELI_URL_RE.finditer(html_str):
        dt = match.group("dt").lower()
        yyyy = match.group("yyyy")
        mm = match.group("mm")
        dd = match.group("dd")
        numac = match.group("numac")
        yield f"{dt}:{yyyy}:{mm}:{dd}:{numac}"


class JustelDiscovery(NormDiscovery):
    """Discovers Belgian legal acts via Justel year-level ELI listing pages.

    Bootstrap cost: ~265 HTTP requests (1 listing per year per document type).
    Daily cost:     1 HTTP request (summary.pl).
    """

    def discover_all(
        self,
        client: LegislativeClient,
        **kwargs,
    ) -> Iterator[str]:
        """Yield all norm IDs for primary Belgian legislation.

        Iterates (document_type, year) tuples, fetching each year-level listing
        and extracting composite norm IDs. De-duplicates across types/years.
        """
        assert isinstance(client, JustelClient)

        today = date.today()
        end_year = today.year
        seen: set[str] = set()

        for dt in DOCUMENT_TYPES:
            start = START_YEAR.get(dt, 1900)
            dt_count = 0
            for year in range(start, end_year + 1):
                try:
                    listing = client.get_listing(dt, year)
                except Exception as exc:
                    logger.warning(
                        "Failed to fetch %s listing for %d: %s",
                        dt,
                        year,
                        exc,
                    )
                    continue
                for norm_id in extract_norm_ids_from_listing(listing):
                    if norm_id in seen:
                        continue
                    seen.add(norm_id)
                    dt_count += 1
                    yield norm_id
            logger.info("Discovered %d %s entries", dt_count, dt)

        logger.info("Total unique Justel norms discovered: %d", len(seen))

    def discover_daily(
        self,
        client: LegislativeClient,
        target_date: date,
        **kwargs,
    ) -> Iterator[str]:
        """Yield norm IDs consolidated on ``target_date``.

        Scrapes /cgi_loi/summary.pl, which groups recent consolidations by
        date in ``<div id='list-title-1'>`` blocks preceded by a French date
        header. We parse the header, match it against ``target_date``, and
        yield every NUMAC in that block.

        Each ``<div class='list-item'>`` inside the matching section has an
        ``<a class='list-item--title' href='article.pl?...&cn_search=NUMAC&...'>``.
        We extract the ``cn_search`` query parameter as the NUMAC.

        The summary page exposes the NUMAC but not the promulgation date,
        so we look up each NUMAC in the ELI listing for the likely year(s)
        to rebuild the composite ``dt:yyyy:mm:dd:numac`` form that the
        client uses to build URLs.
        """
        assert isinstance(client, JustelClient)

        try:
            html_bytes = client.get_daily_summary()
        except Exception as exc:
            logger.warning("Failed to fetch daily summary for %s: %s", target_date, exc)
            return

        try:
            tree = lxml_html.fromstring(
                html_bytes,
                parser=lxml_html.HTMLParser(encoding="iso-8859-1"),
            )
        except Exception as exc:
            logger.warning("Failed to parse daily summary HTML: %s", exc)
            return

        target_numacs: list[str] = []
        day_sections = tree.xpath('//div[@id="list-title-1"]')
        for section in day_sections:
            header_nodes = section.xpath('.//h2[contains(@class, "list-title")]')
            if not header_nodes:
                continue
            header_text = "".join(header_nodes[0].itertext())
            section_date = _parse_fr_date_header(header_text)
            if section_date != target_date:
                continue
            for a in section.xpath('.//a[contains(@class, "list-item--title")]'):
                href = a.get("href", "") or ""
                match = re.search(r"cn_search=(\d+)", href)
                if match:
                    target_numacs.append(match.group(1))

        if not target_numacs:
            logger.info("No consolidations found for %s on Justel", target_date)
            return

        known_years_loaded: dict[tuple[str, int], list[str]] = {}

        def load_year(dt: str, year: int) -> list[str]:
            key = (dt, year)
            if key in known_years_loaded:
                return known_years_loaded[key]
            try:
                listing = client.get_listing(dt, year)
            except Exception as exc:
                logger.warning("Failed to load %s listing %d: %s", dt, year, exc)
                known_years_loaded[key] = []
                return []
            ids = list(extract_norm_ids_from_listing(listing))
            known_years_loaded[key] = ids
            return ids

        # Look up each NUMAC in the ELI listing(s) for the likely year(s).
        # The NUMAC usually starts with the promulgation year; we also try
        # the target date year +/- 1 as a fallback.
        seen: set[str] = set()
        for numac in target_numacs:
            guesses: list[int] = []
            if len(numac) >= 4 and numac[:4].isdigit():
                guesses.append(int(numac[:4]))
            for y in (target_date.year, target_date.year - 1, target_date.year - 2):
                if y not in guesses:
                    guesses.append(y)

            resolved_id: str | None = None
            for y in guesses:
                if y < 1800 or y > date.today().year + 1:
                    continue
                for dt in DOCUMENT_TYPES:
                    for nid in load_year(dt, y):
                        if nid.endswith(f":{numac}"):
                            resolved_id = nid
                            break
                    if resolved_id:
                        break
                if resolved_id:
                    break

            if resolved_id and resolved_id not in seen:
                seen.add(resolved_id)
                yield resolved_id
            elif not resolved_id:
                logger.warning("NUMAC %s on %s not found in any ELI listing", numac, target_date)
