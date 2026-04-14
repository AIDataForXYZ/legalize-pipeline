"""Tests for the Italy daily discovery pipeline."""

from __future__ import annotations

import logging
from datetime import date
from unittest.mock import MagicMock

from legalize.fetcher.it.client import NormattivaClient
from legalize.fetcher.it.discovery import NormattivaDiscovery


def _updated_response(acts: list[dict]) -> dict:
    """Build a fake ricerca/aggiornati response."""
    return {"listaAtti": acts}


def _make_act(codice: str, desc: str = "Test act") -> dict:
    return {
        "codiceRedazionale": codice,
        "descrizioneAtto": desc,
        "denominazioneAtto": "LEGGE",
        "annoProvvedimento": 2026,
        "meseProvvedimento": 4,
        "giornoProvvedimento": 1,
        "numeroProvvedimento": 42,
        "dataGU": "2026-04-05",
        "numeroGU": 80,
    }


class TestDailyDiscovery:
    def test_yields_updated_codices(self):
        mock_client = MagicMock(spec=NormattivaClient)
        mock_client.search_updated.return_value = _updated_response([
            _make_act("26G00042", "Legge 42/2026"),
            _make_act("26G00043", "Legge 43/2026"),
        ])

        discovery = NormattivaDiscovery.__new__(NormattivaDiscovery)
        ids = list(discovery.discover_daily(mock_client, date(2026, 4, 10)))

        assert ids == ["26G00042", "26G00043"]
        mock_client.search_updated.assert_called_once_with(
            "2026-04-10T00:00:00.000Z",
            "2026-04-10T23:59:59.000Z",
        )

    def test_empty_results_yields_nothing(self):
        mock_client = MagicMock(spec=NormattivaClient)
        mock_client.search_updated.return_value = _updated_response([])

        discovery = NormattivaDiscovery.__new__(NormattivaDiscovery)
        ids = list(discovery.discover_daily(mock_client, date(2026, 4, 14)))

        assert ids == []

    def test_api_error_yields_nothing(self, caplog):
        mock_client = MagicMock(spec=NormattivaClient)
        mock_client.search_updated.side_effect = ConnectionError("API down")

        discovery = NormattivaDiscovery.__new__(NormattivaDiscovery)
        with caplog.at_level(logging.ERROR):
            ids = list(discovery.discover_daily(mock_client, date(2026, 4, 14)))

        assert ids == []
        assert any("Failed to fetch updated acts" in r.message for r in caplog.records)

    def test_skips_acts_without_codice(self):
        mock_client = MagicMock(spec=NormattivaClient)
        mock_client.search_updated.return_value = _updated_response([
            _make_act("26G00042"),
            {"descrizioneAtto": "No codice"},
            _make_act("26G00044"),
        ])

        discovery = NormattivaDiscovery.__new__(NormattivaDiscovery)
        ids = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert ids == ["26G00042", "26G00044"]

    def test_duplicate_codices_both_yielded(self):
        mock_client = MagicMock(spec=NormattivaClient)
        mock_client.search_updated.return_value = _updated_response([
            _make_act("26G00042"),
            _make_act("26G00042"),
        ])

        discovery = NormattivaDiscovery.__new__(NormattivaDiscovery)
        ids = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert len(ids) == 2
        assert all(i == "26G00042" for i in ids)
