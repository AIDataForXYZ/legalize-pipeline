"""Wayback Machine client for historical Justice Canada XML snapshots.

Between 2011-05 and 2021-02 the only source of consolidated Canadian
federal law XML is the Wayback Machine's archive of
``https://laws-lois.justice.gc.ca/{eng,fra}/XML/{ID}.xml``. This module
uses the CDX Server API to enumerate every distinct snapshot of each law
and downloads the original bytes for parsing through the same
``CATextParser._parse_root`` that handles upstream XML.

CDX flow
--------

1. ``GET https://web.archive.org/cdx/search/cdx?url={target}&collapse=digest&output=json``
   returns one row per distinct content digest — Wayback takes thousands of
   captures of popular files, but ``collapse=digest`` folds them to the
   unique ones we care about (typically 20-50 per heavily-amended act).

2. For each unique ``(timestamp, digest)`` row we download
   ``https://web.archive.org/web/{timestamp}id_/{original_url}`` — the
   ``id_`` suffix returns the raw archived bytes (no Wayback chrome).

3. Each downloaded XML is cached on disk at
   ``{data_dir}/wayback-xml/{lang}/{category}/{id_safe}/{timestamp}.xml``
   so subsequent runs skip the network entirely.

Rate limits & reliability
-------------------------

Wayback's CDX endpoint tolerates reasonable parallelism but returns 429
under heavy load. We cap at ``DEFAULT_RPS`` requests per second per
client instance (callers can parallelise multiple clients if they want
more throughput). Backoff on 429/503 with exponential delay.

Some early snapshots return ``warc/revisit`` entries (Wayback redirects
to an earlier identical capture). The CDX digest collapse already handles
most of these; the ones that slip through come back as XML syntax errors
and are skipped gracefully in the parser layer.
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_ORIGINAL_URL_TEMPLATE = "https://web.archive.org/web/{timestamp}id_/{url}"
DEFAULT_SLEEP_BETWEEN_REQUESTS = 0.5  # seconds — 2 req/s per worker
DEFAULT_TIMEOUT = 60

# Wayback only started archiving the structured XML endpoint in mid-2011;
# earlier captures are either HTML or the pre-unified laws.justice.gc.ca
# domain, which we don't parse.
EARLIEST_EXPECTED = "2011"
# Upstream git log starts 2021-02-26. After that, upstream is authoritative
# and Wayback duplicates just add noise, so we cap Wayback collection
# slightly before the handover (±14 days for safety).
LATEST_EXPECTED = "20210215000000"


class WaybackClient:
    """Reads Justice Canada XML snapshots from the Wayback Machine archive.

    The client is stateless aside from an on-disk cache rooted at
    ``cache_dir``. Instantiate once per bootstrap run and reuse for all
    norms — each call to :meth:`fetch_versions` is independent.
    """

    def __init__(
        self,
        cache_dir: Path,
        *,
        sleep_between_requests: float = DEFAULT_SLEEP_BETWEEN_REQUESTS,
        timeout: int = DEFAULT_TIMEOUT,
        session: requests.Session | None = None,
    ) -> None:
        self._cache_dir = Path(cache_dir)
        self._sleep = sleep_between_requests
        self._timeout = timeout
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "legalize-pipeline/1.0 (+https://legalize.dev)",
                "Accept": "application/json,application/xml,text/xml",
            }
        )

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def fetch_versions(self, norm_id: str) -> list[dict]:
        """Return every distinct archived XML snapshot for a norm, oldest first.

        Each returned entry has the same shape as the upstream-git / annual-
        statute entries consumed by :meth:`CATextParser.parse_suvestine`:

            {"source_type": "wayback-xml",
             "source_id": "wayback-20141215143022",
             "date": "2014-12-15",
             "xml": "<base64>",
             "wayback_digest": "YZXDKL...",
             "wayback_url": "https://web.archive.org/web/20141215143022id_/…"}
        """
        lang, category, file_id = _parse_norm_id(norm_id)
        if lang not in ("eng", "fra"):
            return []

        target_url = f"laws-lois.justice.gc.ca/{lang}/XML/{file_id}.xml"
        try:
            rows = self._cdx_query(target_url)
        except requests.RequestException as exc:
            logger.warning("Wayback CDX query failed for %s: %s", norm_id, exc)
            return []

        if not rows:
            return []

        out: list[dict] = []
        cache_root = self._cache_root_for(norm_id)
        cache_root.mkdir(parents=True, exist_ok=True)

        for ts, digest in rows:
            # Guard clause: skip rows outside the expected archive window.
            if ts < EARLIEST_EXPECTED or ts > LATEST_EXPECTED:
                continue
            xml_bytes = self._load_snapshot(cache_root, ts, digest, target_url)
            if xml_bytes is None:
                continue

            import base64  # local import — keep module import surface tight

            out.append(
                {
                    "source_type": "wayback-xml",
                    "source_id": f"wayback-{ts}",
                    "date": _timestamp_to_iso_date(ts),
                    "xml": base64.b64encode(xml_bytes).decode("ascii"),
                    "wayback_digest": digest,
                    "wayback_url": WAYBACK_ORIGINAL_URL_TEMPLATE.format(
                        timestamp=ts, url=target_url
                    ),
                }
            )

        # Already oldest-first because CDX returns chronological order, but
        # sort explicitly to be robust against future API changes.
        out.sort(key=lambda v: v["source_id"])
        return out

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _cdx_query(self, target_url: str) -> list[tuple[str, str]]:
        """Return ``[(timestamp, digest), …]`` rows from the CDX endpoint.

        The CDX response is a JSON array with a header row followed by data
        rows. ``collapse=digest`` keeps only the first row per unique content
        digest — which is exactly our dedup key.
        """
        params = {
            "url": target_url,
            "collapse": "digest",
            "output": "json",
            "fl": "timestamp,digest",
            # Only successful captures of XML content.
            "filter": ["statuscode:200", "mimetype:(text|application)/.*xml"],
        }
        resp = self._session.get(CDX_ENDPOINT, params=params, timeout=self._timeout)
        self._respect_rate_limit()
        if resp.status_code in (429, 503):
            # Exponential backoff — one retry is enough in practice.
            time.sleep(self._sleep * 10)
            resp = self._session.get(CDX_ENDPOINT, params=params, timeout=self._timeout)
            self._respect_rate_limit()
        resp.raise_for_status()

        try:
            payload = resp.json()
        except json.JSONDecodeError:
            logger.warning("Wayback CDX returned non-JSON for %s", target_url)
            return []
        if not payload or len(payload) < 2:
            return []

        # First row is the header. Data rows are 2-tuples here because of `fl`.
        out: list[tuple[str, str]] = []
        for row in payload[1:]:
            if len(row) < 2:
                continue
            ts, digest = row[0], row[1]
            if not ts or not digest:
                continue
            out.append((ts, digest))
        return out

    def _load_snapshot(
        self,
        cache_root: Path,
        timestamp: str,
        digest: str,
        target_url: str,
    ) -> bytes | None:
        """Return snapshot bytes, hitting the cache first, then Wayback."""
        # Cache file: ``{timestamp}.xml.gz`` compressed on disk to keep the
        # 1-5 GB total cache footprint manageable. Gzip is transparent to
        # callers — we gunzip on read.
        cache_path = cache_root / f"{timestamp}.xml.gz"
        if cache_path.exists():
            try:
                return gzip.decompress(cache_path.read_bytes())
            except (OSError, gzip.BadGzipFile) as exc:
                logger.warning("Invalid cache file %s (%s); redownloading", cache_path, exc)

        url = WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp=timestamp, url=target_url)
        try:
            resp = self._session.get(url, timeout=self._timeout)
            self._respect_rate_limit()
            if resp.status_code in (429, 503):
                time.sleep(self._sleep * 10)
                resp = self._session.get(url, timeout=self._timeout)
                self._respect_rate_limit()
            if resp.status_code >= 400:
                logger.debug("Wayback %s returned %d; skipping", url, resp.status_code)
                return None
        except requests.RequestException as exc:
            logger.warning("Wayback fetch failed for %s: %s", url, exc)
            return None

        body = resp.content
        # Defensive: some "archived" captures are HTML redirect pages.
        # A real Justice Canada XML starts with "<?xml" or "<Statute"/"<Regulation".
        head = body[:100].lstrip()
        if not head.startswith((b"<?xml", b"<Statute", b"<Regulation", b"<Bill")):
            logger.debug("Wayback %s returned non-XML content (%d bytes); skipping", url, len(body))
            return None

        try:
            cache_path.write_bytes(gzip.compress(body))
        except OSError as exc:
            logger.warning("Failed to cache %s: %s", cache_path, exc)
        return body

    def _cache_root_for(self, norm_id: str) -> Path:
        """Directory that holds all snapshots for one norm.

        Path:
            ``{cache_dir}/wayback-xml/{lang}/{category}/{id}``

        The ``id`` segment uses the norm_id's final component with ``/``
        replaced by ``-`` (matching what ``title_index`` already does on
        disk for InstrumentNumbers like ``SOR/99-129``).
        """
        lang, category, file_id = _parse_norm_id(norm_id)
        safe_id = file_id.replace("/", "-")
        return self._cache_dir / "wayback-xml" / lang / category / safe_id

    def _respect_rate_limit(self) -> None:
        if self._sleep > 0:
            time.sleep(self._sleep)


def _parse_norm_id(norm_id: str) -> tuple[str, str, str]:
    parts = norm_id.split("/", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid CA norm_id: {norm_id!r}")
    return parts[0], parts[1], parts[2]


def _timestamp_to_iso_date(ts: str) -> str:
    """Wayback timestamps are ``YYYYMMDDhhmmss`` strings."""
    if len(ts) < 8:
        return "1970-01-01"
    return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
