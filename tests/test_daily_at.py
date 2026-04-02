"""Tests for the Austria daily incremental pipeline.

Covers:
- RISDiscovery.discover_daily (API pagination, date filtering, dedup)
- daily() orchestration (dry run, no changes, error handling, state management)
"""

from __future__ import annotations

import json
import subprocess
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from legalize.fetcher.at.client import RISClient
from legalize.fetcher.at.daily import _infer_last_date_from_git, daily
from legalize.fetcher.at.discovery import RISDiscovery


# ─────────────────────────────────────────────
# Fixtures: RIS API responses
# ─────────────────────────────────────────────


def _ris_response(hits: int, refs: list[dict], geaendert: str | None = None) -> bytes:
    """Build a fake RIS OGD API response."""
    docs = []
    for ref in refs:
        doc = {
            "Data": {
                "Metadaten": {
                    "Allgemein": {"Geaendert": geaendert or ""},
                    "Bundesrecht": {"BrKons": {"Gesetzesnummer": ref["gesnr"]}},
                    "Technisch": {"ID": ref.get("nor_id", "NOR12345678")},
                },
            }
        }
        docs.append(doc)

    ref_value = docs if len(docs) != 1 else docs[0]
    return json.dumps(
        {
            "OgdSearchResult": {
                "OgdDocumentResults": {
                    "Hits": {"#text": str(hits)},
                    "OgdDocumentReference": ref_value,
                }
            }
        }
    ).encode("utf-8")


def _ris_empty_response() -> bytes:
    """RIS response with no results."""
    return json.dumps(
        {
            "OgdSearchResult": {
                "OgdDocumentResults": {
                    "Hits": {"#text": "0"},
                    "OgdDocumentReference": [],
                }
            }
        }
    ).encode("utf-8")


# ─────────────────────────────────────────────
# Tests: RISDiscovery.discover_daily
# ─────────────────────────────────────────────


class TestRISDiscoverDaily:
    def _mock_client(self):
        """Create a MagicMock that passes isinstance(client, RISClient)."""
        return MagicMock(spec=RISClient)

    def test_discovers_modified_gesetzesnummern(self):
        mock_client = self._mock_client()
        mock_client.get_page.return_value = _ris_response(
            hits=2,
            refs=[{"gesnr": "10002333"}, {"gesnr": "10001848"}],
            geaendert="2026-04-01",
        )

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert set(result) == {"10002333", "10001848"}

    def test_filters_by_date(self):
        mock_client = self._mock_client()
        resp = json.dumps(
            {
                "OgdSearchResult": {
                    "OgdDocumentResults": {
                        "Hits": {"#text": "2"},
                        "OgdDocumentReference": [
                            {
                                "Data": {
                                    "Metadaten": {
                                        "Allgemein": {"Geaendert": "2026-04-01"},
                                        "Bundesrecht": {"BrKons": {"Gesetzesnummer": "10002333"}},
                                    }
                                }
                            },
                            {
                                "Data": {
                                    "Metadaten": {
                                        "Allgemein": {"Geaendert": "2026-03-30"},
                                        "Bundesrecht": {"BrKons": {"Gesetzesnummer": "99999999"}},
                                    }
                                }
                            },
                        ],
                    }
                }
            }
        ).encode("utf-8")
        mock_client.get_page.return_value = resp

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert result == ["10002333"]
        assert "99999999" not in result

    def test_deduplicates(self):
        mock_client = self._mock_client()
        mock_client.get_page.return_value = _ris_response(
            hits=3,
            refs=[
                {"gesnr": "10002333"},
                {"gesnr": "10002333"},
                {"gesnr": "10002333"},
            ],
            geaendert="2026-04-01",
        )

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert result == ["10002333"]

    def test_returns_empty_when_no_changes(self):
        mock_client = self._mock_client()
        mock_client.get_page.return_value = _ris_empty_response()

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert result == []

    def test_handles_single_ref_as_dict(self):
        mock_client = self._mock_client()
        mock_client.get_page.return_value = _ris_response(
            hits=1,
            refs=[{"gesnr": "10002333"}],
            geaendert="2026-04-01",
        )

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert result == ["10002333"]

    def test_paginates_through_multiple_pages(self):
        page1 = _ris_response(hits=150, refs=[{"gesnr": "10002333"}], geaendert="2026-04-01")
        page2 = _ris_response(hits=150, refs=[{"gesnr": "20003456"}], geaendert="2026-04-01")
        page3 = _ris_empty_response()

        mock_client = self._mock_client()
        mock_client.get_page.side_effect = [page1, page2, page3]

        discovery = RISDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        assert "10002333" in result
        assert "20003456" in result

    def test_uses_imrisseit_filter(self):
        mock_client = self._mock_client()
        mock_client.get_page.return_value = _ris_empty_response()

        discovery = RISDiscovery()
        list(discovery.discover_daily(mock_client, date(2026, 4, 1)))

        mock_client.get_page.assert_called_with(page=1, page_size=100, ImRisSeit="EinerWoche")


# ─────────────────────────────────────────────
# Tests: _infer_last_date_from_git (AT)
# ─────────────────────────────────────────────


class TestInferLastDateFromGitAT:
    def test_infers_from_source_date(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        subprocess.run(["git", "init"], cwd=repo, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, capture_output=True)
        subprocess.run(["git", "config", "user.email", "t@t.com"], cwd=repo, capture_output=True)
        (repo / "test.md").write_text("hello")
        subprocess.run(["git", "add", "."], cwd=repo, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "test\n\nSource-Date: 2026-03-28"],
            cwd=repo,
            capture_output=True,
        )

        assert _infer_last_date_from_git(str(repo)) == date(2026, 3, 28)

    def test_returns_none_for_empty_repo(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        subprocess.run(["git", "init"], cwd=repo, capture_output=True)

        assert _infer_last_date_from_git(str(repo)) is None


# ─────────────────────────────────────────────
# Tests: daily() orchestration
# ─────────────────────────────────────────────


class TestDailyATOrchestration:
    def _make_config(self, tmp_path: Path):
        from legalize.config import Config, CountryConfig, GitConfig

        repo_path = tmp_path / "repo"
        repo_path.mkdir()
        subprocess.run(["git", "init"], cwd=repo_path, capture_output=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=repo_path, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "t@t.com"], cwd=repo_path, capture_output=True
        )

        return Config(
            git=GitConfig(committer_name="Legalize", committer_email="test@test.com"),
            countries={
                "at": CountryConfig(
                    repo_path=str(repo_path),
                    data_dir=str(tmp_path / "data"),
                    state_path=str(tmp_path / "state" / "state.json"),
                    source={},
                )
            },
        )

    @patch("legalize.fetcher.at.client.RISClient", autospec=True)
    @patch("legalize.fetcher.at.discovery.RISDiscovery", autospec=True)
    def test_dry_run_does_not_commit(self, mock_disc_cls, mock_client_cls, tmp_path):
        config = self._make_config(tmp_path)

        mock_discovery = mock_disc_cls.return_value
        mock_discovery.discover_daily.return_value = iter(["10002333", "10001848"])

        mock_client = mock_client_cls.return_value
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        result = daily(config, target_date=date(2026, 4, 1), dry_run=True)

        assert result == 0

    @patch("legalize.fetcher.at.client.RISClient", autospec=True)
    @patch("legalize.fetcher.at.discovery.RISDiscovery", autospec=True)
    def test_no_changes_returns_zero(self, mock_disc_cls, mock_client_cls, tmp_path):
        config = self._make_config(tmp_path)

        mock_discovery = mock_disc_cls.return_value
        mock_discovery.discover_daily.return_value = iter([])

        mock_client = mock_client_cls.return_value
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        result = daily(config, target_date=date(2026, 4, 1))

        assert result == 0

    @patch("legalize.fetcher.at.client.RISClient", autospec=True)
    @patch("legalize.fetcher.at.discovery.RISDiscovery", autospec=True)
    def test_discovery_error_continues(self, mock_disc_cls, mock_client_cls, tmp_path):
        config = self._make_config(tmp_path)

        mock_discovery = mock_disc_cls.return_value
        mock_discovery.discover_daily.side_effect = RuntimeError("API down")

        mock_client = mock_client_cls.return_value
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        result = daily(config, target_date=date(2026, 4, 1))

        assert result == 0
        state_path = Path(config.get_country("at").state_path)
        assert state_path.exists()

    @patch("legalize.fetcher.at.client.RISClient", autospec=True)
    @patch("legalize.fetcher.at.discovery.RISDiscovery", autospec=True)
    def test_state_saved_after_run(self, mock_disc_cls, mock_client_cls, tmp_path):
        config = self._make_config(tmp_path)

        mock_discovery = mock_disc_cls.return_value
        mock_discovery.discover_daily.return_value = iter([])

        mock_client = mock_client_cls.return_value
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        daily(config, target_date=date(2026, 4, 1))

        state_path = Path(config.get_country("at").state_path)
        state = json.loads(state_path.read_text())
        assert state["last_summary"] == "2026-04-01"
        assert len(state["runs"]) == 1
        assert state["runs"][0]["commits_created"] == 0
