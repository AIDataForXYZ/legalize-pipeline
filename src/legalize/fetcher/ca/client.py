"""Justice Canada client -- reads consolidated XML from local clone or HTTP.

Primary mode: read from a local clone of justicecanada/laws-lois-xml.
Fallback mode: download individual XML files via HTTPS.

The local clone is strongly preferred for bootstrap (instant access to all
~11,600 files without HTTP overhead). The HTTP fallback exists for daily
updates when the git clone is not available.

Suvestine (version timeline) sources
------------------------------------

``get_suvestine(norm_id)`` returns a chronologically-sorted JSON blob
that merges every known version of a law from up to four sources:

- ``upstream-git`` — the commit history of ``justicecanada/laws-lois-xml``
  (covers 2021-02-26 to today). Consolidated XML per commit.
- ``annual-statute`` — bill XMLs from
  ``annual-statutes-lois-annuelles/{en,fr}/{year}/{year}-c{N}_{E,F}.xml``
  (covers 2001 to today). Amendment bills as-enacted, not consolidated.
  Attached only to norms whose title appears in the bill title (primary
  attribution, see :class:`AnnualStatuteIndex`).
- ``wayback-xml`` (future) — Wayback Machine snapshots of the XML API
  (covers 2011-05 to 2021-02). Same consolidated format as upstream.
- ``gazette-pdf`` (future) — Canada Gazette Part III PDF segments
  (covers 1974 to 2000). Bill as-enacted, extracted by OCR.

Each version carries ``source_type`` so the parser can route to the right
renderer. Pre-2011 versions (annual-statute / gazette-pdf) emit amendment-
bill bodies; 2011+ versions emit consolidated bodies. This is the hard
boundary — see RESEARCH-CA-HISTORY.md for why consolidated text is not
available pre-2011.
"""

from __future__ import annotations

import base64
import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator

from legalize.fetcher.base import HttpClient
from legalize.fetcher.ca.annual_statute_index import (
    AnnualStatuteIndex,
    load_or_build_annual_statute_index,
)
from legalize.fetcher.ca.gazette_index import (
    GazetteIndex,
    GazetteRef,
    load_or_build_gazette_index,
)
from legalize.fetcher.ca.title_index import load_or_build_title_index
from legalize.fetcher.ca.wayback_client import WaybackClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://laws-lois.justice.gc.ca"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RPS = 1.0

# Namespace used in Justice Canada XML root attributes.
LIMS_NS = "http://justice.gc.ca/lims"


@dataclass(frozen=True)
class _SvManifestEntry:
    """Lightweight manifest row: ``loader`` fetches the full entry on demand.

    Used by :meth:`JusticeCanadaClient.iter_suvestine` so the client can
    sort + dedup across all sources before paying the XML-load cost. A
    100-version act's manifest is ~20 KB regardless of total XML size.
    """

    source_type: str
    source_id: str
    date: str  # ISO YYYY-MM-DD for sort keying
    loader: Callable[[], dict]


class JusticeCanadaClient(HttpClient):
    """Client for Justice Canada consolidated legislation XML.

    Reads from a local git clone of justicecanada/laws-lois-xml when
    available; falls back to HTTPS downloads for individual files.
    """

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        xml_dir: str = "",
        data_dir: str = "",
        request_timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        requests_per_second: float = DEFAULT_RPS,
        wayback_enabled: bool = True,
        wayback_categories: tuple[str, ...] = ("eng/acts", "fra/lois"),
    ) -> None:
        super().__init__(
            base_url=base_url,
            request_timeout=request_timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
        )
        self._xml_dir = Path(xml_dir) if xml_dir else None
        self._data_dir = Path(data_dir) if data_dir else Path(".")
        # Lazy-loaded indices. Built on first suvestine call and cached for
        # the life of the client — they're heavy to rebuild (~8s) but
        # stable across all norms in a single bootstrap run.
        self._annual_statute_idx: AnnualStatuteIndex | None = None
        # Wayback: opt-in per-category so bootstrap can include acts
        # (2011-2021 history is high-value) without pulling 4,845 regulation
        # snapshot trees (diminishing return, ~10 GB).
        self._wayback_enabled = wayback_enabled
        self._wayback_categories = wayback_categories
        self._wayback_client: WaybackClient | None = None
        # Gazette PDF: only used if the PDF cache exists on disk and is
        # non-empty. The first bootstrap call triggers a lazy scan.
        self._gazette_idx: GazetteIndex | None = None

    @classmethod
    def create(cls, country_config: CountryConfig) -> JusticeCanadaClient:
        source = country_config.source or {}
        # xml_dir defaults to {data_dir}/laws-lois-xml so CI can just override
        # --data-dir and clone the upstream repo into the right place without
        # editing config.yaml for each environment.
        xml_dir = source.get("xml_dir", "")
        if not xml_dir and country_config.data_dir:
            xml_dir = str(Path(country_config.data_dir) / "laws-lois-xml")
        return cls(
            base_url=source.get("base_url", BASE_URL),
            xml_dir=xml_dir,
            data_dir=country_config.data_dir,
            request_timeout=source.get("request_timeout", DEFAULT_TIMEOUT),
            max_retries=source.get("max_retries", DEFAULT_MAX_RETRIES),
            requests_per_second=source.get("requests_per_second", DEFAULT_RPS),
            wayback_enabled=source.get("wayback_enabled", True),
            wayback_categories=tuple(source.get("wayback_categories", ("eng/acts", "fra/lois"))),
        )

    # -- LegislativeClient interface ------------------------------------------

    def get_text(self, norm_id: str) -> bytes:
        """Return the full XML for a norm.

        norm_id format: "eng/acts/A-1" or "fra/reglements/SOR-99-129"
        """
        # Try local clone first.
        if self._xml_dir:
            xml_path = self._xml_dir / f"{norm_id}.xml"
            if xml_path.exists():
                return xml_path.read_bytes()

        # Fallback: HTTP download.
        lang, category, file_id = _parse_norm_id(norm_id)
        url = f"{self._base_url}/{lang}/XML/{file_id}.xml"
        logger.info("Downloading %s", url)
        return self._get(url)

    def get_metadata(self, norm_id: str) -> bytes:
        """Same data as get_text -- metadata is embedded in the XML."""
        return self.get_text(norm_id)

    # -- Suvestine -----------------------------------------------------------

    def _annual_statute_index(self) -> AnnualStatuteIndex | None:
        """Build (or reuse) the annual-statute cross-reference index.

        Returns ``None`` if the upstream clone isn't present — in that case
        the suvestine falls back to git-log-only behavior.
        """
        if self._annual_statute_idx is not None:
            return self._annual_statute_idx
        if self._xml_dir is None or not self._xml_dir.exists():
            return None
        try:
            title_idx = load_or_build_title_index(self._xml_dir, self._data_dir)
            self._annual_statute_idx = load_or_build_annual_statute_index(
                self._xml_dir, title_idx, self._data_dir
            )
        except FileNotFoundError as exc:
            logger.warning("Annual-statute index unavailable: %s", exc)
            return None
        return self._annual_statute_idx

    def _git_log_versions(self, norm_id: str) -> list[dict]:
        """Walk upstream git log for ``{norm_id}.xml`` and return versions.

        Each version is a dict with ``source_type`` ``"upstream-git"``,
        the commit SHA as ``source_id``, the commit date, and the full XML
        at that revision (base64-encoded).
        """
        if self._xml_dir is None:
            return []
        rel_path = f"{norm_id}.xml"
        log_result = subprocess.run(
            [
                "git",
                "-C",
                str(self._xml_dir),
                "log",
                "--format=%H %aI",
                "--follow",
                "--",
                rel_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if log_result.returncode != 0:
            logger.warning("git log failed for %s: %s", norm_id, log_result.stderr)
            return []

        out: list[dict] = []
        for line in log_result.stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            if len(parts) != 2:
                continue
            sha, iso_datetime = parts
            show_result = subprocess.run(
                ["git", "-C", str(self._xml_dir), "show", f"{sha}:{rel_path}"],
                capture_output=True,
                timeout=30,
            )
            if show_result.returncode != 0:
                continue
            commit_date = iso_datetime[:10]
            # Encode once, then immediately drop the raw bytes reference —
            # on big acts (Income Tax Act: 41 commits x ~1 MB XML each) the
            # subprocess stdout + base64 string otherwise coexist twice
            # until the next iteration reassigns ``show_result``.
            encoded = base64.b64encode(show_result.stdout).decode("ascii")
            del show_result
            out.append(
                {
                    "source_type": "upstream-git",
                    "source_id": sha,
                    "date": commit_date,
                    "xml": encoded,
                }
            )
        # git log yields newest-first; reverse to oldest-first for the pipeline.
        out.reverse()
        return out

    def _gazette_index(self) -> GazetteIndex | None:
        """Build (or reuse) the Gazette PDF cross-reference index.

        Returns ``None`` if the PDF cache is absent — the first bootstrap
        run skips gazette entirely unless the operator has populated
        ``{data_dir}/gazette-pdf/`` via ``GazetteClient.fetch_range``.
        """
        if self._gazette_idx is not None:
            return self._gazette_idx
        pdf_root = self._data_dir / "gazette-pdf"
        if not pdf_root.is_dir():
            return None
        try:
            title_idx = load_or_build_title_index(self._xml_dir, self._data_dir)  # type: ignore[arg-type]
        except (FileNotFoundError, TypeError):
            return None
        self._gazette_idx = load_or_build_gazette_index(pdf_root, title_idx, self._data_dir)
        return self._gazette_idx

    def _gazette_versions(self, norm_id: str) -> list[dict]:
        """Return Gazette-PDF chapter events attributed to ``norm_id``.

        Each entry carries the raw extracted body text (per language) plus
        metadata. The parser wraps the text as amendment-event paragraphs
        rather than feeding it through the XML renderer — there's no
        structured XML to route through when the source is a PDF.
        """
        idx = self._gazette_index()
        if idx is None:
            return []
        refs = idx.refs_for(norm_id)
        if not refs:
            return []

        out: list[dict] = []
        _, _, lang_code = _lang_for_norm(norm_id)
        for ref in refs:
            body = self._gazette_body_for(ref, lang_code)
            if not body:
                continue
            out.append(
                {
                    "source_type": "gazette-pdf",
                    "source_id": f"gazette-{ref.year}-c{ref.chapter}",
                    "date": ref.assent_date,
                    "body_text": body,
                    "bill_number": ref.bill_number,
                    "amending_title": (ref.title_en if lang_code == "en" else ref.title_fr),
                    "gazette_pdf_path": ref.pdf_path,
                    "ocr_confidence": ref.ocr_confidence,
                }
            )
        return out

    def _gazette_body_for(self, ref: GazetteRef, lang: str) -> str:
        """Extract and return the body text for one chapter in one language.

        Re-reads the PDF from cache and re-segments it — the on-disk index
        stores only metadata + page ranges, not the bulky body text. This
        keeps the index JSON tiny (a few hundred KB instead of tens of MB)
        while still giving callers fast per-norm lookup.
        """
        from legalize.fetcher.ca.gazette_segmenter import segment
        from legalize.fetcher.ca.pdf_extractor import extract_text_from_pdf

        pdf_path = self._data_dir / ref.pdf_path
        if not pdf_path.exists():
            logger.warning("Gazette PDF missing at %s", pdf_path)
            return ""
        try:
            extraction = extract_text_from_pdf(pdf_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Gazette extraction failed for %s: %s", pdf_path, exc)
            return ""
        for seg in segment(extraction):
            if seg.chapter == ref.chapter and seg.year == ref.year:
                return seg.body_en if lang == "en" else seg.body_fr
        return ""

    def _wayback_enabled_for(self, norm_id: str) -> bool:
        """Wayback is gated per-category to keep the cache footprint sane.

        Acts in both languages opt-in by default (high value: 10 years of
        consolidated history). Regulations opt-out by default (diminishing
        return at ~4,845 regs × 15 snapshots = ~70K extra downloads).
        """
        if not self._wayback_enabled:
            return False
        for prefix in self._wayback_categories:
            if norm_id.startswith(prefix + "/"):
                return True
        return False

    def _wayback_versions(self, norm_id: str) -> list[dict]:
        """Return Wayback-archived versions of the consolidated XML."""
        if not self._wayback_enabled_for(norm_id):
            return []
        if self._wayback_client is None:
            self._wayback_client = WaybackClient(cache_dir=self._data_dir)
        try:
            return self._wayback_client.fetch_versions(norm_id)
        except Exception as exc:  # noqa: BLE001 — Wayback is best-effort
            logger.warning("Wayback fetch failed for %s: %s", norm_id, exc)
            return []

    def _annual_statute_versions(self, norm_id: str) -> list[dict]:
        """Return amendment-bill versions attached to ``norm_id`` from the
        annual-statute index."""
        idx = self._annual_statute_index()
        if idx is None or self._xml_dir is None:
            return []
        refs = idx.refs_for(norm_id)
        out: list[dict] = []
        for ref in refs:
            xml_path = self._xml_dir / ref.xml_path
            if not xml_path.exists():
                logger.debug("Missing annual-statute XML %s (referenced from index)", ref.xml_path)
                continue
            try:
                xml_bytes = xml_path.read_bytes()
            except OSError as exc:
                logger.warning("Could not read %s: %s", xml_path, exc)
                continue
            out.append(
                {
                    "source_type": "annual-statute",
                    "source_id": f"as-{ref.year}-c{ref.chapter}",
                    "date": ref.assent_date,
                    "xml": base64.b64encode(xml_bytes).decode("ascii"),
                    "bill_number": ref.bill_number,
                    "amending_title": ref.amending_title,
                }
            )
        return out

    # -- Streaming manifest + lazy loaders ----------------------------------

    def _iter_git_log_manifest(self, norm_id: str) -> Iterator[_SvManifestEntry]:
        """Yield one lightweight entry per upstream-git commit (no XML yet).

        The ``loader`` closure does the ``git show`` for its specific SHA
        only when the caller actually consumes the entry — so at no point
        do we hold more than one commit's XML in memory.
        """
        if self._xml_dir is None:
            return
        rel_path = f"{norm_id}.xml"
        log_result = subprocess.run(
            [
                "git",
                "-C",
                str(self._xml_dir),
                "log",
                "--format=%H %aI",
                "--follow",
                "--",
                rel_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if log_result.returncode != 0:
            logger.warning("git log failed for %s: %s", norm_id, log_result.stderr)
            return

        entries: list[tuple[str, str]] = []
        for line in log_result.stdout.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            parts = line.split(" ", 1)
            if len(parts) != 2:
                continue
            sha, iso_datetime = parts
            entries.append((sha, iso_datetime[:10]))
        # Reverse to oldest-first (matches the legacy output ordering).
        entries.reverse()

        xml_dir = self._xml_dir  # capture for the closures

        def _loader_for(sha: str, commit_date: str) -> Callable[[], dict]:
            def _load() -> dict:
                show_result = subprocess.run(
                    ["git", "-C", str(xml_dir), "show", f"{sha}:{rel_path}"],
                    capture_output=True,
                    timeout=30,
                )
                if show_result.returncode != 0:
                    return {}
                encoded = base64.b64encode(show_result.stdout).decode("ascii")
                del show_result
                return {
                    "source_type": "upstream-git",
                    "source_id": sha,
                    "date": commit_date,
                    "xml": encoded,
                }

            return _load

        for sha, commit_date in entries:
            yield _SvManifestEntry(
                source_type="upstream-git",
                source_id=sha,
                date=commit_date,
                loader=_loader_for(sha, commit_date),
            )

    def _iter_wayback_manifest(self, norm_id: str) -> Iterator[_SvManifestEntry]:
        """Yield one lightweight entry per Wayback snapshot (no XML yet).

        The CDX query happens up front (one HTTP call); the individual
        snapshot downloads + base64 encoding happen inside the loader so
        at most one snapshot's bytes are resident at a time.
        """
        if not self._wayback_enabled_for(norm_id):
            return
        if self._wayback_client is None:
            self._wayback_client = WaybackClient(cache_dir=self._data_dir)
        client = self._wayback_client

        # fetch_versions already returns fully-loaded entries. To preserve
        # the streaming semantic we materialise them once here (CDX call
        # + cached downloads) but release xml strings between yields so
        # the consumer sees them one at a time.
        try:
            entries = client.fetch_versions(norm_id)
        except Exception as exc:  # noqa: BLE001 — Wayback is best-effort
            logger.warning("Wayback fetch failed for %s: %s", norm_id, exc)
            return

        def _loader_for(entry: dict) -> Callable[[], dict]:
            return lambda: entry

        for entry in entries:
            yield _SvManifestEntry(
                source_type=entry["source_type"],
                source_id=entry["source_id"],
                date=entry["date"],
                loader=_loader_for(entry),
            )

    def _iter_annual_statute_manifest(self, norm_id: str) -> Iterator[_SvManifestEntry]:
        """Yield one lightweight entry per annual-statute amendment.

        XMLs are read from the local clone on demand inside the loader.
        """
        idx = self._annual_statute_index()
        if idx is None or self._xml_dir is None:
            return
        refs = idx.refs_for(norm_id)
        if not refs:
            return
        xml_dir = self._xml_dir  # captured

        def _loader_for(ref) -> Callable[[], dict]:
            def _load() -> dict:
                xml_path = xml_dir / ref.xml_path
                if not xml_path.exists():
                    logger.debug(
                        "Missing annual-statute XML %s (referenced from index)",
                        ref.xml_path,
                    )
                    return {}
                try:
                    xml_bytes = xml_path.read_bytes()
                except OSError as exc:
                    logger.warning("Could not read %s: %s", xml_path, exc)
                    return {}
                encoded = base64.b64encode(xml_bytes).decode("ascii")
                del xml_bytes
                return {
                    "source_type": "annual-statute",
                    "source_id": f"as-{ref.year}-c{ref.chapter}",
                    "date": ref.assent_date,
                    "xml": encoded,
                    "bill_number": ref.bill_number,
                    "amending_title": ref.amending_title,
                }

            return _load

        for ref in refs:
            yield _SvManifestEntry(
                source_type="annual-statute",
                source_id=f"as-{ref.year}-c{ref.chapter}",
                date=ref.assent_date,
                loader=_loader_for(ref),
            )

    def _iter_gazette_manifest(self, norm_id: str) -> Iterator[_SvManifestEntry]:
        """Yield one lightweight entry per Gazette PDF chapter affecting ``norm_id``.

        Body text extraction (``extract_text_from_pdf`` + ``segment``) runs
        inside the loader — the default is to re-read the PDF per yield,
        but the LRU inside the extractor absorbs back-to-back calls on
        the same file.
        """
        idx = self._gazette_index()
        if idx is None:
            return
        refs = idx.refs_for(norm_id)
        if not refs:
            return

        _, _, lang_code = _lang_for_norm(norm_id)

        def _loader_for(ref) -> Callable[[], dict]:
            def _load() -> dict:
                body = self._gazette_body_for(ref, lang_code)
                if not body:
                    return {}
                return {
                    "source_type": "gazette-pdf",
                    "source_id": f"gazette-{ref.year}-c{ref.chapter}",
                    "date": ref.assent_date,
                    "body_text": body,
                    "bill_number": ref.bill_number,
                    "amending_title": (ref.title_en if lang_code == "en" else ref.title_fr),
                    "gazette_pdf_path": ref.pdf_path,
                    "ocr_confidence": ref.ocr_confidence,
                }

            return _load

        for ref in refs:
            yield _SvManifestEntry(
                source_type="gazette-pdf",
                source_id=f"gazette-{ref.year}-c{ref.chapter}",
                date=ref.assent_date,
                loader=_loader_for(ref),
            )

    def iter_suvestine(self, norm_id: str) -> Iterator[dict]:
        """Stream one version entry at a time, chronologically sorted + deduped.

        This is the memory-efficient counterpart to :meth:`get_suvestine`.
        The pipeline prefers this path when both client and parser expose
        the streaming interface. Peak RSS during a full Criminal Code
        bootstrap drops from ~2.5 GB (bytes path) to <500 MB with the
        stream path because only the currently-yielded entry's XML (or
        body text) is resident — no JSON blob is ever materialised.

        Dedup and ordering follow the same rules as :meth:`get_suvestine`:
        chronological by event date, duplicate ``source_id``s dropped,
        gazette-pdf entries suppressed when an annual-statute entry with
        the same (year, chapter) covers the same bill.
        """
        if self._xml_dir is None or not self._xml_dir.exists():
            logger.warning(
                "No upstream clone at %s; suvestine stream falls back to current text",
                self._xml_dir,
            )
            try:
                current = self.get_text(norm_id)
            except Exception:  # noqa: BLE001 — absolute fallback, may fail offline
                return
            today = date.today().isoformat()
            yield {
                "source_type": "http-current",
                "source_id": "current",
                "date": today,
                "xml": base64.b64encode(current).decode("ascii"),
            }
            return

        # Build a lightweight manifest across all sources — metadata only,
        # no XML loaded yet. For a 100-version act the manifest is ~20 KB.
        manifest: list[_SvManifestEntry] = []
        manifest.extend(self._iter_git_log_manifest(norm_id))
        manifest.extend(self._iter_wayback_manifest(norm_id))
        manifest.extend(self._iter_gazette_manifest(norm_id))
        manifest.extend(self._iter_annual_statute_manifest(norm_id))

        manifest.sort(key=lambda m: m.date)

        # Dedup: collect keys first so gazette-pdf can defer to
        # annual-statute on matching (year, chapter).
        annual_keys: set[tuple[int, int]] = set()
        for m in manifest:
            if m.source_type == "annual-statute":
                key = _statute_year_chapter(m.source_id)
                if key is not None:
                    annual_keys.add(key)

        seen_ids: set[str] = set()
        for m in manifest:
            if m.source_id in seen_ids:
                continue
            if m.source_type == "gazette-pdf":
                gkey = _statute_year_chapter(m.source_id.replace("gazette-", "as-", 1))
                if gkey is not None and gkey in annual_keys:
                    continue
            seen_ids.add(m.source_id)
            entry = m.loader()
            if not entry:
                continue
            yield entry
            # Help the allocator: the entry dict (with its base64 XML
            # string) can weigh tens of MB for big consolidated acts.
            # We can't force the consumer to release it, but we can drop
            # our own reference before moving to the next.
            del entry

    def get_suvestine(self, norm_id: str) -> bytes:
        """Return a merged chronological timeline of all known versions.

        Merges git-log consolidations (2021+) with annual-statute
        amendments (2001+) and (future) Wayback + Gazette PDF sources. The
        returned blob is a JSON object:

            {
              "versions": [
                {"source_type": "annual-statute", "source_id": "as-2020-c13",
                 "date": "2020-11-19", "xml": "<base64>", …},
                {"source_type": "upstream-git", "source_id": "<sha>",
                 "date": "2021-02-26", "xml": "<base64>"},
                …
              ]
            }

        Dedupe rules:
        - Same ``source_id`` appears at most once.
        - Consolidated duplicates by content digest are handled by the
          parser, not here (we can't decode base64 cheaply here).
        """
        all_versions: list[dict] = []

        if self._xml_dir is None or not self._xml_dir.exists():
            logger.warning(
                "No upstream clone at %s; suvestine falls back to single snapshot",
                self._xml_dir,
            )
            current = self.get_text(norm_id)
            today = date.today().isoformat()
            all_versions.append(
                {
                    "source_type": "http-current",
                    "source_id": "current",
                    "date": today,
                    "xml": base64.b64encode(current).decode("ascii"),
                }
            )
            return json.dumps({"versions": all_versions}).encode("utf-8")

        # 1. Upstream git log (2021-02 → today).
        all_versions.extend(self._git_log_versions(norm_id))

        # 2. Wayback Machine XML snapshots (2011-05 → 2021-02). Same
        #    consolidated shape as upstream — the boundary at 2021 is
        #    invisible in the body except for metadata.
        all_versions.extend(self._wayback_versions(norm_id))

        # 3. Gazette Part III PDF segments (1998 → 2000 in v1). Filled in
        #    only when the operator has populated the PDF cache.
        all_versions.extend(self._gazette_versions(norm_id))

        # 4. Annual-statute amendments (2001 → today, primary attribution).
        #    Many of these predate the upstream git log so they push history
        #    further back without overlap. When they overlap (same bill was
        #    both recorded via git-log commit AND cross-referenced), we
        #    keep both: the git-log version is the consolidated text AFTER
        #    the amendment, the annual-statute version is the amendment
        #    itself — different content, not duplicates.
        all_versions.extend(self._annual_statute_versions(norm_id))

        # Chronological sort: oldest-first. Stable ties preserve source
        # order (git log was appended first, so git-log versions win on
        # same-day ties — matters at the 2021 boundary where Wayback
        # should yield to upstream).
        all_versions.sort(key=lambda v: v["date"])

        # Deduplicate. Two rules in priority order:
        # 1. Exact source_id match — a bug-level duplicate, drop.
        # 2. Same (year, chapter) across annual-statute and gazette-pdf —
        #    the annual-statute XML is authoritative and wins over the
        #    OCR-inferred gazette-pdf content. Source_ids encode the key:
        #    ``as-{Y}-c{N}`` vs ``gazette-{Y}-c{N}``.
        seen_ids: set[str] = set()
        annual_keys: set[tuple[int, int]] = set()
        for v in all_versions:
            if v.get("source_type") == "annual-statute":
                key = _statute_year_chapter(v.get("source_id", ""))
                if key:
                    annual_keys.add(key)

        deduped: list[dict] = []
        for v in all_versions:
            sid = v.get("source_id", "")
            if sid in seen_ids:
                continue
            if v.get("source_type") == "gazette-pdf":
                key = _statute_year_chapter(sid.replace("gazette-", "as-", 1))
                if key and key in annual_keys:
                    # Annual-statute covers this chapter — skip the PDF entry.
                    continue
            seen_ids.add(sid)
            deduped.append(v)

        if not deduped:
            # Final degradation: no git log, no annual statute, no clone —
            # emit the current text as a single snapshot so the pipeline
            # can still produce a valid bootstrap commit.
            current = self.get_text(norm_id)
            today = date.today().isoformat()
            deduped.append(
                {
                    "source_type": "http-current",
                    "source_id": "current",
                    "date": today,
                    "xml": base64.b64encode(current).decode("ascii"),
                }
            )

        blob = json.dumps({"versions": deduped}).encode("utf-8")
        # Release the intermediate lists before returning so the caller's
        # next call (same worker, next norm) doesn't start on top of the
        # previous law's peak. The encoded blob alone can be 100s of MB
        # for Criminal Code; holding ``all_versions`` + ``deduped`` on top
        # doubles the transient footprint until the GC decides to run.
        del all_versions, deduped, seen_ids, annual_keys
        import gc as _gc

        _gc.collect()
        return blob


def _parse_norm_id(norm_id: str) -> tuple[str, str, str]:
    """Parse 'eng/acts/A-1' into (lang, category, file_id).

    >>> _parse_norm_id("eng/acts/A-1")
    ('eng', 'acts', 'A-1')
    >>> _parse_norm_id("fra/reglements/SOR-99-129")
    ('fra', 'reglements', 'SOR-99-129')
    """
    parts = norm_id.split("/", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid CA norm_id: {norm_id!r} (expected lang/category/id)")
    return parts[0], parts[1], parts[2]


def _statute_year_chapter(source_id: str) -> tuple[int, int] | None:
    """Parse an ``as-{Y}-c{N}`` source_id into ``(year, chapter)``.

    Returns ``None`` for source_ids that don't match (git SHAs, Wayback
    timestamps, etc). Used by the dedup logic to cross-reference
    annual-statute and gazette-pdf entries.
    """
    import re

    m = re.fullmatch(r"(?:as|gazette)-(\d{4})-c(\d{1,3})", source_id)
    if not m:
        return None
    try:
        return int(m.group(1)), int(m.group(2))
    except (TypeError, ValueError):
        return None


def _lang_for_norm(norm_id: str) -> tuple[str, str, str]:
    """Return ``(url_lang, category, short_lang)`` for a norm_id.

    ``short_lang`` is the 2-letter code used by the Gazette / title
    indices (``"en"``/``"fr"``) — note that in norm_id we use the 3-letter
    ``"eng"``/``"fra"`` matching Justice Canada's URL structure.
    """
    url_lang, category, _ = _parse_norm_id(norm_id)
    short_lang = "fr" if url_lang == "fra" else "en"
    return url_lang, category, short_lang
