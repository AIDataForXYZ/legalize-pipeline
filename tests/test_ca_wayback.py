"""Tests for the Wayback Machine client.

Uses ``responses`` to mock the CDX and snapshot HTTP endpoints so the
tests run offline. The only live-network path (exercised by the
historical smoke against A-1) is not part of the test suite.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest
import responses

from legalize.fetcher.ca.wayback_client import (
    CDX_ENDPOINT,
    WAYBACK_ORIGINAL_URL_TEMPLATE,
    WaybackClient,
    _parse_norm_id,
    _timestamp_to_iso_date,
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


class TestTimestampToIsoDate:
    def test_typical_wayback_timestamp(self):
        assert _timestamp_to_iso_date("20141215143022") == "2014-12-15"

    def test_date_only_timestamp(self):
        # Date-truncated CDX responses occasionally return just YYYYMMDD.
        assert _timestamp_to_iso_date("20200101") == "2020-01-01"

    def test_too_short_returns_epoch(self):
        assert _timestamp_to_iso_date("2020") == "1970-01-01"
        assert _timestamp_to_iso_date("") == "1970-01-01"


class TestParseNormId:
    def test_valid(self):
        assert _parse_norm_id("eng/acts/A-1") == ("eng", "acts", "A-1")
        assert _parse_norm_id("fra/reglements/SOR-85-567") == (
            "fra",
            "reglements",
            "SOR-85-567",
        )

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_norm_id("not-a-norm-id")
        with pytest.raises(ValueError):
            _parse_norm_id("just/two")


# ─────────────────────────────────────────────
# fetch_versions end-to-end
# ─────────────────────────────────────────────


def _minimal_statute_xml(pit_date: str = "2014-06-01") -> bytes:
    return (
        b'<?xml version="1.0"?>'
        b'<Statute xmlns:lims="http://justice.gc.ca/lims" '
        b'lims:pit-date="' + pit_date.encode("ascii") + b'">'
        b"<Body/></Statute>"
    )


class TestFetchVersions:
    @responses.activate
    def test_returns_deduped_snapshots(self, tmp_path: Path):
        """CDX returns 2 distinct digests → we download and return 2 entries."""
        target = "laws-lois.justice.gc.ca/eng/XML/A-1.xml"
        cdx_payload = [
            ["timestamp", "digest"],
            ["20120115000000", "DIGEST1"],
            ["20140601000000", "DIGEST2"],
        ]
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=cdx_payload,
            status=200,
        )
        for ts in ("20120115000000", "20140601000000"):
            responses.add(
                responses.GET,
                WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp=ts, url=target),
                body=_minimal_statute_xml(),
                status=200,
            )

        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        versions = client.fetch_versions("eng/acts/A-1")

        assert len(versions) == 2
        assert versions[0]["source_type"] == "wayback-xml"
        assert versions[0]["source_id"] == "wayback-20120115000000"
        assert versions[0]["date"] == "2012-01-15"
        assert versions[0]["wayback_digest"] == "DIGEST1"
        assert versions[1]["source_id"] == "wayback-20140601000000"

    @responses.activate
    def test_skips_non_xml_snapshot(self, tmp_path: Path):
        """Snapshots that return HTML instead of XML are filtered out."""
        target = "laws-lois.justice.gc.ca/eng/XML/A-1.xml"
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=[["timestamp", "digest"], ["20140601000000", "D"]],
            status=200,
        )
        responses.add(
            responses.GET,
            WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp="20140601000000", url=target),
            body=b"<html><body>not xml</body></html>",
            status=200,
        )

        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        assert client.fetch_versions("eng/acts/A-1") == []

    @responses.activate
    def test_cdx_empty_returns_empty(self, tmp_path: Path):
        """CDX returning only the header row (no data) yields no versions."""
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=[["timestamp", "digest"]],
            status=200,
        )
        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        assert client.fetch_versions("eng/acts/A-1") == []

    @responses.activate
    def test_snapshot_404_is_skipped(self, tmp_path: Path):
        """Missing snapshots (rare but happens) don't break the enumeration."""
        target = "laws-lois.justice.gc.ca/eng/XML/A-1.xml"
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=[
                ["timestamp", "digest"],
                ["20120115000000", "D1"],
                ["20140601000000", "D2"],
            ],
            status=200,
        )
        # First snapshot 404s, second succeeds — we expect one surviving entry.
        responses.add(
            responses.GET,
            WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp="20120115000000", url=target),
            status=404,
        )
        responses.add(
            responses.GET,
            WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp="20140601000000", url=target),
            body=_minimal_statute_xml(),
            status=200,
        )

        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        versions = client.fetch_versions("eng/acts/A-1")
        assert len(versions) == 1
        assert versions[0]["source_id"] == "wayback-20140601000000"

    def test_cache_hit_skips_http(self, tmp_path: Path):
        """Pre-populated on-disk cache should return without any HTTP call.

        We don't activate ``responses`` here, so any outgoing HTTP request
        would raise ``ConnectionError``. The test passes iff the cached
        bytes are returned directly.
        """
        cache_dir = tmp_path
        snapshot_dir = cache_dir / "wayback-xml" / "eng" / "acts" / "A-1"
        snapshot_dir.mkdir(parents=True)

        # We also need to short-circuit the CDX query itself; do that by
        # stubbing the _cdx_query method to return the known timestamp.
        xml = _minimal_statute_xml()
        (snapshot_dir / "20140601000000.xml.gz").write_bytes(gzip.compress(xml))

        client = WaybackClient(cache_dir=cache_dir, sleep_between_requests=0)

        # Monkeypatch _cdx_query to avoid hitting the network.
        client._cdx_query = lambda target_url: [("20140601000000", "DIGEST")]

        versions = client.fetch_versions("eng/acts/A-1")
        assert len(versions) == 1
        # Decoded XML matches what we cached.
        import base64

        assert base64.b64decode(versions[0]["xml"]) == xml

    @responses.activate
    def test_skips_snapshots_outside_window(self, tmp_path: Path):
        """Timestamps before 2011 or after 2021-02-15 are filtered out."""
        target = "laws-lois.justice.gc.ca/eng/XML/A-1.xml"
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=[
                ["timestamp", "digest"],
                ["20050101000000", "OLD"],  # pre-2011 — skipped
                ["20140601000000", "MID"],  # accepted
                ["20250101000000", "NEW"],  # post-2021 — skipped
            ],
            status=200,
        )
        responses.add(
            responses.GET,
            WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp="20140601000000", url=target),
            body=_minimal_statute_xml(),
            status=200,
        )

        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        versions = client.fetch_versions("eng/acts/A-1")
        assert len(versions) == 1
        assert versions[0]["source_id"] == "wayback-20140601000000"

    @responses.activate
    def test_cdx_malformed_json_returns_empty(self, tmp_path: Path):
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            body=b"not-json",
            status=200,
        )
        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        assert client.fetch_versions("eng/acts/A-1") == []

    @responses.activate
    def test_invalid_norm_id_category_returns_empty(self, tmp_path: Path):
        """``xyz/foo/bar`` is not an eng/fra norm — we short-circuit."""
        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        # The call should not even issue an HTTP request. ``responses``
        # raises on unmatched calls, so a clean pass means zero HTTP.
        assert client.fetch_versions("xyz/foo/bar") == []

    @responses.activate
    def test_snapshot_is_cached_after_first_fetch(self, tmp_path: Path):
        """After a successful download the bytes are written to the gzip cache."""
        target = "laws-lois.justice.gc.ca/eng/XML/A-1.xml"
        responses.add(
            responses.GET,
            CDX_ENDPOINT,
            json=[["timestamp", "digest"], ["20140601000000", "D"]],
            status=200,
        )
        responses.add(
            responses.GET,
            WAYBACK_ORIGINAL_URL_TEMPLATE.format(timestamp="20140601000000", url=target),
            body=_minimal_statute_xml(),
            status=200,
        )
        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        client.fetch_versions("eng/acts/A-1")

        cache_file = tmp_path / "wayback-xml" / "eng" / "acts" / "A-1" / "20140601000000.xml.gz"
        assert cache_file.exists()
        assert gzip.decompress(cache_file.read_bytes()) == _minimal_statute_xml()


# ─────────────────────────────────────────────
# Cache layout and keying
# ─────────────────────────────────────────────


class TestCacheLayout:
    def test_regulation_id_sanitized(self, tmp_path: Path):
        """InstrumentNumbers with '/' get flattened to '-' in the cache path."""
        client = WaybackClient(cache_dir=tmp_path, sleep_between_requests=0)
        root = client._cache_root_for("fra/reglements/SOR-99-129")
        assert root.name == "SOR-99-129"
        assert root.parent.name == "reglements"
        assert root.parent.parent.name == "fra"
