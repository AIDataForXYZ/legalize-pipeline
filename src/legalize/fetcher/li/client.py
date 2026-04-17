"""HTTP client for the Lilex law database (gesetze.li, Liechtenstein).

Lilex has no API. We scrape three endpoints per law:
  - /konso/{LGBl-dotted}         → landing page with version dropdown
  - /konso/html/{lgblId}         → iframe content (current version)
  - /konso/html/{lgblId}?version → iframe content for a historical version

`get_text()` resolves the version list from the metadata page and fetches
every historical version in parallel (small pool), returning a JSON
envelope that embeds both the meta page and each version's HTML body.
The parser then walks that envelope to emit one `Version` per
historical entry.

The site sits behind a Citrix NetScaler that issues `citrix_ns_id` /
`citrix_ns_id_*` tracking cookies on the first hit and expects them on
follow-ups. The shared `requests.Session` keeps the cookies for the
whole bootstrap. Bursts of parallel requests trip the gateway into a
redirect loop, so we cap parallelism at a small per-law pool and use a
modest per-second rate.
"""

from __future__ import annotations

import json
import logging
import re

from legalize.fetcher.base import HttpClient

logger = logging.getLogger(__name__)

BASE_URL = "https://www.gesetze.li"

# 10-digit URL form is YEAR (4) + NUMBER (3, zero-padded) + 3-digit suffix.
# All consolidated laws use suffix "000".
_LGBL_RE = re.compile(r"^\d{10}$")
_DOTTED_RE = re.compile(r"^(\d{4})\.(\d{1,3})$")
# Error pages embed this marker; a real law page never references it.
_ERROR_MARKER = b"/error_pages/"


def to_url_id(lgbl: str) -> str:
    """Convert dotted LGBl (1921.015) to 10-digit URL form (1921015000)."""
    if _LGBL_RE.match(lgbl):
        return lgbl
    m = _DOTTED_RE.match(lgbl)
    if not m:
        raise ValueError(f"Unrecognized LGBl format: {lgbl!r}")
    year, num = m.group(1), int(m.group(2))
    return f"{year}{num:03d}000"


def to_dotted_id(lgbl: str) -> str:
    """Convert 10-digit URL form to dotted LGBl (1921015000 → 1921.015)."""
    if _DOTTED_RE.match(lgbl):
        return lgbl
    if not _LGBL_RE.match(lgbl):
        raise ValueError(f"Unrecognized LGBl format: {lgbl!r}")
    return f"{lgbl[:4]}.{int(lgbl[4:7]):03d}"


class LilexClient(HttpClient):
    """HTTP client for gesetze.li.

    Combines per-version HTML fetches into a single JSON envelope so the
    parser can iterate without making any network calls of its own.
    """

    @classmethod
    def create(cls, country_config) -> "LilexClient":
        return cls()

    def __init__(self) -> None:
        super().__init__(requests_per_second=2.0)
        # Lower the per-connection redirect cap. Citrix can otherwise send us
        # in a loop until the urllib3 default of 30 trips.
        self._session.max_redirects = 4
        self._warm_session()

    def _warm_session(self) -> None:
        """Hit the homepage once to collect Citrix tracking cookies.

        Wipes any existing cookies first — when Citrix puts us in a redirect
        loop, the only reliable recovery is a clean session.
        """
        self._session.cookies.clear()
        try:
            self._session.get(BASE_URL + "/", timeout=self._timeout)
        except Exception:  # noqa: BLE001
            pass

    def get_metadata(self, lgbl: str) -> bytes:
        """Fetch the consolidated landing page (HTML).

        The landing page truncates very long titles (treaties, EU decrees)
        with an ellipsis, so we also pull the current iframe HTML — the
        `<meta name="description">` there carries the full untruncated
        title. Returned as a JSON envelope so the metadata parser can read
        either source.
        """
        dotted = to_dotted_id(lgbl)
        url_id = to_url_id(lgbl)
        landing = self._fetch_with_recovery(f"{BASE_URL}/konso/{dotted}")
        if _ERROR_MARKER in landing:
            raise FileNotFoundError(f"LGBl {lgbl!r} not found in Lilex")
        try:
            current_html = self._fetch_with_recovery(f"{BASE_URL}/konso/html/{url_id}").decode(
                "utf-8", errors="replace"
            )
        except Exception:  # noqa: BLE001
            current_html = ""
        envelope = {
            "meta_html": landing.decode("utf-8", errors="replace"),
            "current_html": current_html,
        }
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    def _fetch_with_recovery(self, url: str, params: dict | None = None) -> bytes:
        """GET with one retry through a fresh session if Citrix redirects loop.

        Citrix flips into a redirect-loop watchdog after sustained crawling.
        The only practical recovery is to drop all cookies, hit the homepage
        again, and retry the original URL once.
        """
        try:
            return self._get(url, params=params)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "redirect" not in msg and "max retries" not in msg:
                raise
            logger.info("Citrix loop detected on %s — re-warming session", url)
            self._warm_session()
            return self._get(url, params=params)

    def get_text(self, lgbl: str, meta_data: bytes | None = None) -> bytes:
        """Fetch all historical versions of a law and return a JSON envelope.

        Envelope shape:
            {
              "lgbl": "1921.015",
              "url_id": "1921015000",
              "meta_html": "<html>...</html>",
              "versions": [
                {"version": 45, "date_text": "01.01.2026", "html": "..."},
                {"version": 44, "date_text": "01.02.2021 - 31.12.2025", ...},
                ...
              ]
            }

        Versions are sorted oldest-first so the committer writes commits
        in chronological order without further sorting.
        """
        url_id = to_url_id(lgbl)
        if meta_data is None:
            meta_data = self.get_metadata(lgbl)
        meta_text = _unwrap_meta_html(meta_data)
        version_options = _extract_version_options(meta_text)

        if not version_options:
            # No version dropdown — single snapshot. Fetch current HTML only.
            html = self._get(f"{BASE_URL}/konso/html/{url_id}")
            if _ERROR_MARKER in html:
                raise FileNotFoundError(f"LGBl {lgbl!r}: content not found")
            envelope = {
                "lgbl": to_dotted_id(lgbl),
                "url_id": url_id,
                "meta_html": meta_text,
                "versions": [
                    {
                        "version": 1,
                        "date_text": "",
                        "html": html.decode("utf-8", errors="replace"),
                    }
                ],
            }
            return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

        def _fetch_version(opt: tuple[int, str]) -> tuple[int, str, str | None]:
            version, date_text = opt
            try:
                body = self._fetch_with_recovery(
                    f"{BASE_URL}/konso/html/{url_id}", params={"version": version}
                )
                if _ERROR_MARKER in body:
                    return version, date_text, None
                return version, date_text, body.decode("utf-8", errors="replace")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to fetch %s version %d: %s", url_id, version, exc)
                return version, date_text, None

        results: list[dict] = []
        # Sequential per-law fetch. Citrix's bot heuristic flags any pair of
        # near-simultaneous requests from the same session and starts a
        # redirect loop, so a single in-flight request is the only reliable
        # mode. Outer parallelism (across laws) is similarly limited to 1
        # via config.yaml::max_workers.
        for opt in version_options:
            version, date_text, html = _fetch_version(opt)
            if html is not None:
                results.append({"version": version, "date_text": date_text, "html": html})

        if not results:
            raise FileNotFoundError(f"LGBl {lgbl!r}: no versions could be fetched")

        # Sort oldest first (lowest version number wins because the dropdown
        # numbers monotonically with publication order).
        results.sort(key=lambda v: v["version"])
        envelope = {
            "lgbl": to_dotted_id(lgbl),
            "url_id": url_id,
            "meta_html": meta_text,
            "versions": results,
        }
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    def get_page(self, path: str, **params: str | int) -> bytes:
        """Generic GET against the gesetze.li site. Used by discovery."""
        url = f"{BASE_URL}{path}" if path.startswith("/") else f"{BASE_URL}/{path}"
        return self._fetch_with_recovery(url, params=params or None)


# ── Helpers ──────────────────────────────────────────────────────────────

_VERSION_OPT_RE = re.compile(
    r'<option(?:\s+selected="selected")?\s+value="(\d+)">\s*([^<]+?)\s*</option>'
)


def _unwrap_meta_html(data: bytes) -> str:
    """Return the landing-page HTML, whether `data` is the raw HTML or a
    `{meta_html, current_html}` envelope from get_metadata."""
    if not data:
        return ""
    head = data.lstrip()[:1]
    if head == b"{":
        try:
            envelope = json.loads(data)
            return envelope.get("meta_html", "")
        except json.JSONDecodeError:
            pass
    return data.decode("utf-8", errors="replace")


def _extract_version_options(meta_html: str) -> list[tuple[int, str]]:
    """Extract (version_number, date_text) tuples from the version dropdown.

    The dropdown options look like:
        <option selected="selected" value="45">01.01.2026</option>
        <option value="44">01.02.2021 - 31.12.2025</option>
        ...

    Returned in the order they appear in the page (newest first).
    """
    out: list[tuple[int, str]] = []
    seen: set[int] = set()
    for m in _VERSION_OPT_RE.finditer(meta_html):
        version = int(m.group(1))
        if version in seen:
            continue
        seen.add(version)
        out.append((version, m.group(2).strip()))
    return out
