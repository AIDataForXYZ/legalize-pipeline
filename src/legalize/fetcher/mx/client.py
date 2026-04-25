"""Mexico HTTP client — multi-source.

Each Mexican legal-information portal has its own URL pattern, format and
quirks. This module hides that behind a per-source ``Adapter`` so the
generic pipeline only ever sees ``client.get_metadata(norm_id)`` and
``client.get_text(norm_id)``. Routing is by id prefix
(``DIP-…`` → Diputados, ``DOF-…`` → DOF, etc.).

Implemented today:
  - **Diputados** (LeyesBiblio) — federal laws, codes, Constitution.
    Index page lists 262 laws with PDF + DOC links and DOF dates;
    we cache the index, build a per-law row registry, and treat each
    PDF as a single-snapshot consolidated text. Historical reforms are
    not yet wired (Diputados only publishes current text — DOF holds the
    reform stream).

Stubbed (registry-only, NotImplementedError on fetch):
  - DOF, OJN, SJF, UNAM, Justia.

HTTP cache
----------
When ``cache_dir`` is supplied (taken from ``config.yaml::countries.mx.cache_dir``),
the client wraps its session in a ``requests_cache.CachedSession`` backed by SQLite
at ``<cache_dir>/http_cache.sqlite``.  Only GET requests are cached; POSTs bypass the
cache transparently.  Entries never expire (``NEVER_EXPIRE``) because Diputados PDFs
live at stable, immutable URLs.

Pass ``force=True`` (wired to the CLI ``--force`` flag) to bypass the cache so that
fresh bytes are fetched from the network and the cache entry is updated.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from lxml import html as lxml_html

from legalize.fetcher.base import HttpClient

if TYPE_CHECKING:
    from legalize.config import CountryConfig

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"

# Default seed sources — wire each one up in Step 0 research before flipping
# its `enabled` flag in config.yaml. `kind` documents whether the source
# yields primary legislation (fits the engine's norm model directly) or
# something else (case_law, doctrine, aggregator) that may need a custom
# data model before it can be ingested.
DEFAULT_SOURCES: dict[str, dict] = {
    "diputados": {
        "base_url": "https://www.diputados.gob.mx/LeyesBiblio",
        "id_prefix": "DIP",
        "kind": "primary_legislation",
    },
    "dof": {
        "base_url": "https://www.dof.gob.mx",
        "id_prefix": "DOF",
        "kind": "primary_legislation",
    },
    "ojn": {
        "base_url": "https://www.ordenjuridico.gob.mx",
        "id_prefix": "OJN",
        "kind": "primary_legislation",
    },
    "sjf": {
        "base_url": "https://sjf2.scjn.gob.mx",
        "id_prefix": "SJF",
        "kind": "case_law",
    },
    "unam": {
        "base_url": "https://biblio.juridicas.unam.mx/bjv",
        "id_prefix": "UNAM",
        "kind": "doctrine",
    },
    "justia": {
        "base_url": "https://mexico.justia.com",
        "id_prefix": "JUSTIA",
        "kind": "aggregator",
    },
}


# Diputados row patterns ---------------------------------------------------
# Index page row text after tag-stripping looks like:
#   "001 CONSTITUCIÓN Política de los Estados Unidos Mexicanos
#    DOF 05/02/1917 Nueva reforma DOF 10/04/2026"
_RANK_BY_KEYWORD: tuple[tuple[str, str], ...] = (
    ("constitución", "constitucion"),
    ("constitucion", "constitucion"),
    ("código", "codigo"),
    ("codigo", "codigo"),
    ("ley general", "ley_general"),
    ("ley federal", "ley_federal"),
    ("ley orgánica", "ley_organica"),
    ("ley reglamentaria", "ley_reglamentaria"),
    ("ley", "ley"),
    ("estatuto", "estatuto"),
    ("reglamento", "reglamento"),
    ("decreto", "decreto"),
)

_DOF_DATE_RE = re.compile(r"DOF\s+(\d{2}/\d{2}/\d{4})")


def _make_cached_session(
    cache_dir: str | Path,
    *,
    force: bool = False,
    user_agent: str = DEFAULT_USER_AGENT,
) -> "requests_cache.CachedSession":
    """Create a ``requests_cache.CachedSession`` backed by SQLite.

    The cache file is placed at ``<cache_dir>/http_cache.sqlite``.
    Only GET requests are cached; POSTs bypass the cache transparently.
    Entries never expire (``NEVER_EXPIRE``) — Diputados PDFs live at
    stable, immutable URLs so TTL-based eviction is not useful.

    When ``force=True`` every request is fetched fresh from the network and
    the cache entry is updated (``CachedSession.settings.disabled`` can't be
    used because it would prevent writes too; instead we delete the cached
    response before each GET so the next hit always goes to the network).
    """
    try:
        import requests_cache
    except ImportError as exc:
        raise ImportError(
            "requests-cache is required for the MX HTTP cache. "
            "Install it with: pip install requests-cache"
        ) from exc

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    db_path = cache_path / "http_cache.sqlite"

    session = requests_cache.CachedSession(
        cache_name=str(db_path),
        backend="sqlite",
        expire_after=requests_cache.NEVER_EXPIRE,
        allowable_methods=["GET"],
        # Disable conditional requests (ETag/Last-Modified) — we want straight
        # cache hits, not 304 round-trips that still consume rate-limit budget.
        always_revalidate=False,
        stale_while_revalidate=False,
    )
    session.headers["User-Agent"] = user_agent

    if force:
        # Monkey-patch _send so every GET removes its cached entry first,
        # guaranteeing a fresh network request that also updates the cache.
        _orig_send = session.send

        def _force_send(request, **kw):
            if request.method == "GET":
                session.cache.delete(urls=[request.url])
            return _orig_send(request, **kw)

        session.send = _force_send  # type: ignore[method-assign]

    return session


@dataclass(frozen=True)
class MXSource:
    """A single Mexican legislative source registered with MXClient.

    ``kind`` flags which engine model the source belongs to:
    - ``primary_legislation`` — fits NormMetadata/Block/Reform directly
    - ``case_law`` — court rulings (SJF); needs a separate model
    - ``doctrine`` — academic/secondary literature (UNAM); not a norm
    - ``aggregator`` — re-publishes content from other sources (Justia)
    """

    name: str
    base_url: str
    id_prefix: str
    kind: str = "primary_legislation"


@dataclass(frozen=True)
class DiputadosRow:
    """A single law as it appears on the LeyesBiblio index page."""

    abbrev: str           # canonical short id, e.g. "CPEUM"
    title: str            # e.g. "Constitución Política de los Estados Unidos Mexicanos"
    rank: str             # mapped Rank value: "constitucion", "codigo", "ley", ...
    publication_date: date    # first DOF date (original publication)
    last_reform_date: date | None  # most recent DOF date if listed
    pdf_url: str          # absolute URL to the consolidated PDF
    doc_url: str | None   # absolute URL to the .doc/.docx if present


class MXClient(HttpClient):
    """Multi-source HTTP client for Mexican legislation.

    Sources are passed as ``{name: {base_url, id_prefix, kind}}``. The
    single shared session (plain ``requests.Session`` or a
    ``requests_cache.CachedSession`` when ``cache_dir`` is given) is used
    across all sources; per-source URL construction lives in the matching
    adapter helper.

    Parameters
    ----------
    cache_dir:
        Directory for the SQLite HTTP cache.  When ``None`` (default) no
        caching is applied.  When set, a ``CachedSession`` is created at
        ``<cache_dir>/http_cache.sqlite`` with ``expire_after=NEVER_EXPIRE``
        and GET-only caching.
    force:
        When ``True`` the cache is bypassed for every request — the network
        is always hit and the cached entry is overwritten.  Maps to the CLI
        ``--force`` flag.
    """

    def __init__(
        self,
        sources: dict[str, dict] | None = None,
        *,
        cache_dir: str | Path | None = None,
        force: bool = False,
        **kwargs,
    ) -> None:
        kwargs.setdefault("user_agent", DEFAULT_USER_AGENT)
        kwargs.setdefault("requests_per_second", 1.0)
        super().__init__(**kwargs)

        # Replace the plain session with a cached one when cache_dir is given.
        if cache_dir is not None:
            self._session = _make_cached_session(
                cache_dir=cache_dir,
                force=force,
                user_agent=kwargs.get("user_agent", DEFAULT_USER_AGENT),
            )
            self._force = force
        else:
            self._force = False

        raw = sources or DEFAULT_SOURCES
        self._sources: dict[str, MXSource] = {}
        self._by_prefix: dict[str, MXSource] = {}
        for name, conf in raw.items():
            src = MXSource(
                name=name,
                base_url=str(conf["base_url"]).rstrip("/"),
                id_prefix=str(conf["id_prefix"]),
                kind=str(conf.get("kind", "primary_legislation")),
            )
            self._sources[name] = src
            self._by_prefix[src.id_prefix] = src

        # Lazily-built per-source caches.
        self._diputados_index: dict[str, DiputadosRow] | None = None

    @classmethod
    def create(cls, country_config: CountryConfig, *, force: bool = False) -> MXClient:
        source = country_config.source or {}
        return cls(
            sources=source.get("sources"),
            request_timeout=source.get("request_timeout", 30),
            max_retries=source.get("max_retries", 3),
            requests_per_second=source.get("requests_per_second", 1.0),
            cache_dir=country_config.cache_dir or None,
            force=force,
        )

    @property
    def sources(self) -> dict[str, MXSource]:
        return dict(self._sources)

    def source_for(self, norm_id: str) -> MXSource:
        prefix = norm_id.split("-", 1)[0] if "-" in norm_id else norm_id
        if prefix not in self._by_prefix:
            available = ", ".join(sorted(self._by_prefix)) or "<none>"
            raise ValueError(
                f"No MX source registered for id prefix '{prefix}' "
                f"(norm_id={norm_id!r}). Available prefixes: {available}"
            )
        return self._by_prefix[prefix]

    # ── Generic surface used by the pipeline ────────────────────────────

    def get_metadata(self, norm_id: str) -> bytes:
        source = self.source_for(norm_id)
        if source.name == "diputados":
            return self._diputados_metadata(norm_id)
        raise NotImplementedError(
            f"get_metadata not implemented for MX source '{source.name}'."
        )

    def get_text(self, norm_id: str, meta_data: bytes | None = None) -> bytes:
        source = self.source_for(norm_id)
        if source.name == "diputados":
            return self._diputados_text(norm_id, meta_data=meta_data)
        raise NotImplementedError(
            f"get_text not implemented for MX source '{source.name}'."
        )

    # ── Diputados adapter ───────────────────────────────────────────────

    def diputados_index(self) -> dict[str, DiputadosRow]:
        """Return ``{abbrev: DiputadosRow}`` for every law on LeyesBiblio.

        Lazily fetched and cached for the lifetime of the client.
        """
        if self._diputados_index is None:
            base = self._sources["diputados"].base_url
            html_bytes = self._get(f"{base}/index.htm")
            self._diputados_index = parse_diputados_index(html_bytes, base)
            logger.info("Diputados index loaded: %d laws", len(self._diputados_index))
        return self._diputados_index

    def _diputados_row(self, norm_id: str) -> DiputadosRow:
        abbrev = norm_id.split("-", 1)[1] if "-" in norm_id else norm_id
        index = self.diputados_index()
        if abbrev not in index:
            raise ValueError(
                f"Diputados has no law with abbrev '{abbrev}' "
                f"(norm_id={norm_id!r}). Sample: {sorted(index)[:5]}"
            )
        return index[abbrev]

    def _diputados_metadata(self, norm_id: str) -> bytes:
        row = self._diputados_row(norm_id)
        envelope = {
            "source": "diputados",
            "norm_id": norm_id,
            "abbrev": row.abbrev,
            "title": row.title,
            "rank": row.rank,
            "publication_date": row.publication_date.isoformat(),
            "last_reform_date": (
                row.last_reform_date.isoformat() if row.last_reform_date else None
            ),
            "pdf_url": row.pdf_url,
            "doc_url": row.doc_url,
        }
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    def _diputados_text(self, norm_id: str, meta_data: bytes | None) -> bytes:
        row = self._diputados_row(norm_id)
        pdf_bytes = self._get(row.pdf_url)
        envelope = {
            "source": "diputados",
            "norm_id": norm_id,
            "abbrev": row.abbrev,
            "title": row.title,
            "rank": row.rank,
            "publication_date": row.publication_date.isoformat(),
            "last_reform_date": (
                row.last_reform_date.isoformat() if row.last_reform_date else None
            ),
            "pdf_url": row.pdf_url,
            "pdf_b64": base64.b64encode(pdf_bytes).decode("ascii"),
        }
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")


# ── Module-level helpers (so they can be unit-tested without HTTP) ──────


def _decode_index_html(data: bytes) -> str:
    """LeyesBiblio is served as Windows-1252; decode explicitly."""
    return data.decode("windows-1252", errors="replace")


def _classify_rank(text: str) -> str:
    """Map an index-row title to a `Rank` slug. Falls back to ``ley``."""
    lower = text.lower()
    for keyword, rank in _RANK_BY_KEYWORD:
        if keyword in lower:
            return rank
    return "ley"


def _parse_dof_date(s: str) -> date:
    return datetime.strptime(s, "%d/%m/%Y").date()


def parse_diputados_index(
    html_bytes: bytes,
    base_url: str,
) -> dict[str, DiputadosRow]:
    """Parse the LeyesBiblio index page into ``{abbrev: DiputadosRow}``.

    Each table row carries a sequence number, the law's full title, two DOF
    dates (publication + most recent reform), and links to PDF and DOC
    forms. Rows without a ``pdf/{abbrev}.pdf`` link are skipped.
    """
    text = _decode_index_html(html_bytes)
    doc = lxml_html.fromstring(text)
    base = base_url.rstrip("/")
    rows: dict[str, DiputadosRow] = {}

    for tr in doc.xpath('//tr[.//a[contains(@href, "pdf/") and contains(@href, ".pdf")]]'):
        pdf_hrefs = [
            h for h in tr.xpath('.//a/@href')
            if h.startswith("pdf/") and h.endswith(".pdf")
        ]
        if not pdf_hrefs:
            continue
        pdf_href = pdf_hrefs[0]
        abbrev = pdf_href[len("pdf/"):-len(".pdf")]
        if not abbrev or abbrev in rows:
            continue

        doc_hrefs = [
            h for h in tr.xpath('.//a/@href')
            if h.startswith("doc/") and not h.startswith("doc_mov")
        ]
        doc_href = doc_hrefs[0] if doc_hrefs else None

        row_text = re.sub(r"\s+", " ", tr.text_content()).strip()
        # Extract the title — drop the leading row number, drop the trailing
        # DOF dates suffix.
        title = row_text
        m = re.match(r"^\d+\s+(.+)", title)
        if m:
            title = m.group(1)
        title = re.split(r"DOF\s+\d{2}/\d{2}/\d{4}", title, maxsplit=1)[0].strip()
        title = title.rstrip(".,;:")

        dof_dates = _DOF_DATE_RE.findall(row_text)
        if not dof_dates:
            continue
        publication_date = _parse_dof_date(dof_dates[0])
        last_reform_date = _parse_dof_date(dof_dates[-1]) if len(dof_dates) > 1 else None

        rows[abbrev] = DiputadosRow(
            abbrev=abbrev,
            title=title,
            rank=_classify_rank(title),
            publication_date=publication_date,
            last_reform_date=last_reform_date,
            pdf_url=f"{base}/{pdf_href}",
            doc_url=f"{base}/{doc_href}" if doc_href else None,
        )

    return rows
