"""Tests for the Italy bootstrap: ZIP graceful degradation, version-walking, reform extraction."""

from __future__ import annotations

import logging
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from legalize.fetcher.it.bootstrap import (
    FNAME_RE,
    _build_norm,
    _download_and_process_collections,
)
from legalize.fetcher.it.parser import NormattivaMetadataParser, NormattivaTextParser


class TestFnameRegex:
    def test_originale(self):
        m = FNAME_RE.match("1947-12-27_047U0001_ORIGINALE_V1.html")
        assert m
        assert m.group(1) == "1947-12-27"
        assert m.group(2) == "047U0001"
        assert m.group(3) == "ORIGINALE"
        assert m.group(4) is None
        assert m.group(5) == "1"

    def test_vigenza_with_date(self):
        m = FNAME_RE.match("1947-12-27_047U0001_VIGENZA_2001-10-30_V2.html")
        assert m
        assert m.group(2) == "047U0001"
        assert m.group(3) == "VIGENZA"
        assert m.group(4) == "2001-10-30"
        assert m.group(5) == "2"

    def test_vigenza_without_date(self):
        m = FNAME_RE.match("2020-01-15_090G0294_VIGENZA_V1.html")
        assert m
        assert m.group(2) == "090G0294"
        assert m.group(4) is None

    def test_no_match(self):
        assert FNAME_RE.match("README.md") is None
        assert FNAME_RE.match("some_random_file.html") is None


class TestBuildNorm:
    @pytest.fixture()
    def parsers(self):
        return NormattivaTextParser(), NormattivaMetadataParser()

    def _make_discovery_meta(self, codice="047U0001"):
        return {
            codice: {
                "codiceRedazionale": codice,
                "descrizioneAtto": "Test law title",
                "titoloAtto": "Test short title",
                "denominazioneAtto": "LEGGE COSTITUZIONALE",
                "annoProvvedimento": 2002,
                "meseProvvedimento": 10,
                "giornoProvvedimento": 18,
                "numeroProvvedimento": 1,
                "dataGU": "2002-10-23",
                "numeroGU": 252,
                "tipoSupplemento": "NO",
                "numeroSupplemento": 0,
            }
        }

    def test_builds_norm_with_reforms(self, parsers):
        tp, mp = parsers
        versions = [
            {"html": '<div class="bodyTesto">Version 1</div>', "date": "2002-10-23"},
            {"html": '<div class="bodyTesto">Version 2</div>', "date": "2022-04-01"},
        ]
        norm = _build_norm(
            "047U0001", "LEGGE COSTITUZIONALE_20021018_1", versions,
            self._make_discovery_meta(), tp, mp,
        )
        assert norm is not None
        assert norm.metadata.identifier == "047U0001"
        assert len(norm.reforms) == 2
        assert norm.reforms[0].date == date(2002, 10, 23)
        assert norm.reforms[1].date == date(2022, 4, 1)

    def test_builds_norm_without_discovery_meta(self, parsers):
        tp, mp = parsers
        versions = [
            {"html": '<div class="bodyTesto">Only version</div>', "date": "1990-08-07"},
        ]
        norm = _build_norm(
            "090G0294", "LEGGE_19900807_241", versions,
            {}, tp, mp,
        )
        assert norm is not None
        assert len(norm.reforms) == 1

    def test_malformed_dates_logged(self, parsers, caplog):
        tp, mp = parsers
        versions = [
            {"html": '<div class="bodyTesto">Ok</div>', "date": "2002-10-23"},
            {"html": '<div class="bodyTesto">Bad</div>', "date": "not-a-date"},
        ]
        with caplog.at_level(logging.WARNING):
            norm = _build_norm(
                "047U0001", "LEGGE COSTITUZIONALE_20021018_1", versions,
                self._make_discovery_meta(), tp, mp,
            )
        assert norm is not None
        assert len(norm.reforms) == 1
        assert any("Malformed version date" in r.message for r in caplog.records)

    def test_empty_versions_raises(self, parsers):
        tp, mp = parsers
        with pytest.raises(IndexError):
            _build_norm(
                "047U0001", "TEST_DIR", [],
                self._make_discovery_meta(), tp, mp,
            )


class TestPhase1GracefulDegradation:
    def test_zero_byte_download_warns_and_skips(self, tmp_path, caplog):
        json_dir = tmp_path / "json"
        json_dir.mkdir()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content = MagicMock(return_value=[])

        with (
            patch("legalize.fetcher.it.bootstrap.requests.get", return_value=mock_response),
            patch("legalize.fetcher.it.bootstrap.BULK_COLLECTIONS", ["TestCollection"]),
            caplog.at_level(logging.WARNING),
        ):
            total = _download_and_process_collections(tmp_path, json_dir, {})

        assert total == 0
        assert any("0 bytes" in r.message for r in caplog.records)
        assert not list(tmp_path.glob("*-multi.zip"))

    def test_stale_empty_zip_cleaned_up(self, tmp_path, caplog):
        json_dir = tmp_path / "json"
        json_dir.mkdir()
        stale = tmp_path / "OldCollection-multi.zip"
        stale.write_bytes(b"")

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content = MagicMock(return_value=[])

        with (
            patch("legalize.fetcher.it.bootstrap.requests.get", return_value=mock_response),
            patch("legalize.fetcher.it.bootstrap.BULK_COLLECTIONS", ["TestCollection"]),
            caplog.at_level(logging.INFO),
        ):
            _download_and_process_collections(tmp_path, json_dir, {})

        assert not stale.exists()
        assert any("Removed stale empty ZIP" in r.message for r in caplog.records)

    def test_phase1_zero_acts_logs_fallthrough(self, tmp_path, caplog):
        json_dir = tmp_path / "json"
        json_dir.mkdir()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content = MagicMock(return_value=[])

        with (
            patch("legalize.fetcher.it.bootstrap.requests.get", return_value=mock_response),
            patch("legalize.fetcher.it.bootstrap.BULK_COLLECTIONS", ["A"]),
            caplog.at_level(logging.WARNING),
        ):
            total = _download_and_process_collections(tmp_path, json_dir, {})

        assert total == 0
        assert any("Phase 1 yielded 0 acts" in r.message for r in caplog.records)
