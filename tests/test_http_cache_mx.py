"""Tests for the Mexico HTTP response cache (requests-cache / SQLite).

All tests are offline — network calls are intercepted by ``responses``.

Coverage:
- Cache miss on first GET → network called, entry written to SQLite.
- Cache hit on second GET → network NOT called (zero additional request).
- force=True bypasses the cache → network called again even when entry exists.
- No cache when cache_dir is None — plain requests.Session used.
- cache_dir path is resolved and SQLite file is created at expected location.
- MXClient.create() reads cache_dir from CountryConfig and wires it in.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import responses as responses_lib
from responses import RequestsMock

from legalize.fetcher.mx.client import MXClient, _make_cached_session


# ── Helper fixtures ───────────────────────────────────────────────────────────

_INDEX_URL = "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
_FIXTURE_HTML = Path("tests/fixtures/mx/diputados-index.html").read_bytes()


@pytest.fixture()
def tmp_cache(tmp_path: Path) -> Path:
    """A throwaway directory for the SQLite cache."""
    return tmp_path / "cache"


# ── _make_cached_session unit tests ──────────────────────────────────────────


def test_make_cached_session_creates_directory(tmp_path: Path) -> None:
    """_make_cached_session must create cache_dir if it does not exist."""
    cache_dir = tmp_path / "new" / "nested"
    assert not cache_dir.exists()
    session = _make_cached_session(cache_dir=cache_dir)
    assert cache_dir.exists()
    session.close()


def test_make_cached_session_creates_correct_db_path(tmp_path: Path) -> None:
    """The SQLite file is placed at <cache_dir>/http_cache.sqlite."""
    import requests_cache

    cache_dir = tmp_path / "cache"
    session = _make_cached_session(cache_dir=cache_dir)

    assert isinstance(session, requests_cache.CachedSession)
    expected_db = cache_dir / "http_cache.sqlite"
    # db_path is a Path; compare directly.
    assert session.cache.db_path == expected_db
    session.close()


# ── MXClient cache integration tests (using responses mock) ──────────────────


def test_cache_miss_then_hit(tmp_cache: Path) -> None:
    """First request is a network miss; second is a cache hit (no extra network call)."""
    with RequestsMock() as rsps:
        rsps.add(
            responses_lib.GET,
            _INDEX_URL,
            body=_FIXTURE_HTML,
            status=200,
            content_type="text/html; charset=windows-1252",
        )

        client = MXClient(cache_dir=tmp_cache)

        # First call — network must be hit.
        result1 = client._get(_INDEX_URL)
        assert len(rsps.calls) == 1, "expected one network call on cache miss"
        assert result1 == _FIXTURE_HTML

        # Second call — cache must serve the response; no additional network call.
        result2 = client._get(_INDEX_URL)
        assert len(rsps.calls) == 1, "expected zero new network calls on cache hit"
        assert result2 == _FIXTURE_HTML

        client.close()


def test_force_bypasses_cache(tmp_cache: Path) -> None:
    """With force=True, every GET goes to the network even when cached."""
    with RequestsMock() as rsps:
        # Register the same URL twice — responses serves them in FIFO order.
        for _ in range(2):
            rsps.add(
                responses_lib.GET, _INDEX_URL, body=_FIXTURE_HTML, status=200,
                content_type="text/html; charset=windows-1252",
            )

        # Populate cache with a normal (non-force) client.
        client_normal = MXClient(cache_dir=tmp_cache)
        client_normal._get(_INDEX_URL)
        assert len(rsps.calls) == 1
        client_normal.close()

        # Force client must re-fetch from the network.
        client_force = MXClient(cache_dir=tmp_cache, force=True)
        client_force._get(_INDEX_URL)
        assert len(rsps.calls) == 2, "expected force=True to bypass cache and hit network"
        client_force.close()


def test_no_cache_when_cache_dir_is_none() -> None:
    """When cache_dir is None the client uses a plain requests.Session (no caching)."""
    import requests
    import requests_cache

    with RequestsMock() as rsps:
        rsps.add(responses_lib.GET, _INDEX_URL, body=_FIXTURE_HTML, status=200)
        rsps.add(responses_lib.GET, _INDEX_URL, body=_FIXTURE_HTML, status=200)

        client = MXClient(cache_dir=None)

        # Session must be a plain Session, not a CachedSession.
        assert type(client._session) is requests.Session
        assert not isinstance(client._session, requests_cache.CachedSession)

        client._get(_INDEX_URL)
        client._get(_INDEX_URL)
        # Both calls go to the network because there is no cache.
        assert len(rsps.calls) == 2, "expected two network calls with no cache"

        client.close()


def test_cache_dir_config_wires_through_create(tmp_path: Path) -> None:
    """MXClient.create() reads cache_dir from CountryConfig and wires it in."""
    import requests_cache

    from legalize.config import CountryConfig

    cc = CountryConfig(
        cache_dir=str(tmp_path / "http"),
        source={
            "request_timeout": 10,
            "max_retries": 1,
            "requests_per_second": 0,
        },
    )
    client = MXClient.create(cc)
    assert isinstance(client._session, requests_cache.CachedSession)
    client.close()


def test_force_create_bypasses_cache_via_pipeline(tmp_cache: Path) -> None:
    """MXClient.create(cc, force=True) configures a force-mode cached session."""
    import requests_cache

    from legalize.config import CountryConfig

    cc = CountryConfig(
        cache_dir=str(tmp_cache),
        source={
            "request_timeout": 10,
            "max_retries": 1,
            "requests_per_second": 0,
        },
    )

    with RequestsMock() as rsps:
        for _ in range(2):
            rsps.add(
                responses_lib.GET, _INDEX_URL, body=_FIXTURE_HTML, status=200,
                content_type="text/html; charset=windows-1252",
            )

        # First request populates the cache (force=False).
        client_normal = MXClient.create(cc, force=False)
        assert isinstance(client_normal._session, requests_cache.CachedSession)
        client_normal._get(_INDEX_URL)
        assert len(rsps.calls) == 1
        client_normal.close()

        # force=True client must still hit the network.
        client_force = MXClient.create(cc, force=True)
        client_force._get(_INDEX_URL)
        assert len(rsps.calls) == 2, "expected force=True to bypass cache"
        client_force.close()
