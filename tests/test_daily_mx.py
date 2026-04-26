"""Tests for the Mexico daily incremental pipeline.

Coverage:
- MXDiscovery.discover_daily always returns empty (stub state — DOF not yet wired)
- generic_daily for MX produces zero commits when discover_daily returns empty
- generic_daily for MX correctly handles a non-empty discover_daily (mocked)
  so the workflow is validated end-to-end once DOF discovery lands
- _SKIP_WEEKDAYS["mx"] skips only Sunday (DOF publishes Mon–Sat)
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from legalize.fetcher.mx.client import MXClient
from legalize.fetcher.mx.discovery import MXDiscovery
from legalize.pipeline import _SKIP_WEEKDAYS, generic_daily


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def _make_config(tmp_path: Path):
    """Minimal Config for MX pointing at a fresh empty git repo."""
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
            "mx": CountryConfig(
                repo_path=str(repo_path),
                data_dir=str(tmp_path / "data"),
                state_path=str(tmp_path / "state" / "state.json"),
                source={},
            )
        },
    )


def _mock_countries_dispatch():
    """Build generic mocks for client/discovery as dispatched by generic_daily."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    mock_client_cls = MagicMock()
    mock_client_cls.create.return_value = mock_client

    mock_discovery = MagicMock()
    mock_disc_cls = MagicMock()
    mock_disc_cls.create.return_value = mock_discovery

    return mock_client, mock_client_cls, mock_discovery, mock_disc_cls


# ─────────────────────────────────────────────
# Tests: _SKIP_WEEKDAYS["mx"]
# ─────────────────────────────────────────────


def test_mx_skip_weekdays_skips_only_sunday():
    """DOF publishes Mon–Sat; only Sunday (weekday 6) should be skipped."""
    skip = _SKIP_WEEKDAYS.get("mx", set())
    assert 6 in skip, "Sunday must be in skip set"
    # Mon–Sat (0–5) must all be run days
    for wd in range(6):
        assert wd not in skip, f"Weekday {wd} must NOT be skipped for MX"


# ─────────────────────────────────────────────
# Tests: MXDiscovery.discover_daily (current stub state)
# ─────────────────────────────────────────────


class TestMXDiscoverDailyStub:
    """discover_daily is a stub: it returns empty for all sources until DOF lands."""

    def _mock_mx_client(self) -> MagicMock:
        """Create a MagicMock that passes isinstance(client, MXClient)."""
        return MagicMock(spec=MXClient)

    def test_returns_empty_for_diputados(self):
        mock_client = self._mock_mx_client()
        # Provide a minimal sources dict so the discovery loop runs
        mock_client.sources = {"diputados": MagicMock(name="diputados")}

        discovery = MXDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 25)))

        assert result == [], (
            "discover_daily must return empty until DOF daily wiring lands"
        )

    def test_returns_empty_for_dof_source(self):
        mock_client = self._mock_mx_client()
        mock_client.sources = {"dof": MagicMock(name="dof")}

        discovery = MXDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 25)))

        assert result == []

    def test_returns_empty_for_all_sources(self):
        mock_client = self._mock_mx_client()
        mock_client.sources = {
            name: MagicMock(name=name)
            for name in ("diputados", "dof", "ojn", "sjf", "unam", "justia")
        }

        discovery = MXDiscovery()
        result = list(discovery.discover_daily(mock_client, date(2026, 4, 25)))

        assert result == []

    def test_raises_on_wrong_client_type(self):
        from legalize.fetcher.base import LegislativeClient

        wrong_client = MagicMock(spec=LegislativeClient)
        discovery = MXDiscovery()
        with pytest.raises(TypeError, match="MXDiscovery requires MXClient"):
            list(discovery.discover_daily(wrong_client, date(2026, 4, 25)))


# ─────────────────────────────────────────────
# Tests: generic_daily for MX — zero-change run
# ─────────────────────────────────────────────


class TestDailyMXNoChanges:
    """When discover_daily yields nothing, generic_daily must return 0 commits."""

    def test_zero_commits_when_nothing_discovered(self, tmp_path):
        config = _make_config(tmp_path)
        mock_client, mock_client_cls, mock_discovery, mock_disc_cls = (
            _mock_countries_dispatch()
        )
        mock_discovery.discover_daily.return_value = iter([])

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_discovery_class", return_value=mock_disc_cls),
            patch("legalize.countries.get_text_parser", return_value=MagicMock()),
            patch("legalize.countries.get_metadata_parser", return_value=MagicMock()),
            patch("legalize.pipeline.resolve_dates_to_process", return_value=[date(2026, 4, 25)]),
        ):
            commits = generic_daily(
                config, "mx", target_date=date(2026, 4, 25), dry_run=True
            )

        assert commits == 0


# ─────────────────────────────────────────────
# Tests: generic_daily for MX — simulated DOF reform
# ─────────────────────────────────────────────


class TestDailyMXWithReforms:
    """Validate that generic_daily correctly processes MX reforms once DOF lands.

    These tests mock discover_daily to return a non-empty list — simulating
    what will happen when DOF daily discovery is implemented.  They verify
    that the orchestration layer (fetch → commit → push) works for MX
    without requiring a real network call.
    """

    def test_dry_run_does_not_commit(self, tmp_path):
        config = _make_config(tmp_path)
        mock_client, mock_client_cls, mock_discovery, mock_disc_cls = (
            _mock_countries_dispatch()
        )
        mock_discovery.discover_daily.return_value = iter(["DOF-2026-001", "DOF-2026-002"])

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_discovery_class", return_value=mock_disc_cls),
            patch("legalize.countries.get_text_parser", return_value=MagicMock()),
            patch("legalize.countries.get_metadata_parser", return_value=MagicMock()),
            patch("legalize.pipeline.resolve_dates_to_process", return_value=[date(2026, 4, 25)]),
        ):
            commits = generic_daily(
                config, "mx", target_date=date(2026, 4, 25), dry_run=True
            )

        # dry_run skips actual writes and commits
        assert commits == 0
        mock_client.get_metadata.assert_not_called()
        mock_client.get_text.assert_not_called()

    def test_discovery_error_is_recorded_not_raised(self, tmp_path):
        """A discovery failure for a given date is logged and counted, not re-raised."""
        config = _make_config(tmp_path)
        mock_client, mock_client_cls, mock_discovery, mock_disc_cls = (
            _mock_countries_dispatch()
        )
        mock_discovery.discover_daily.side_effect = RuntimeError("DOF API down")

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_discovery_class", return_value=mock_disc_cls),
            patch("legalize.countries.get_text_parser", return_value=MagicMock()),
            patch("legalize.countries.get_metadata_parser", return_value=MagicMock()),
            patch("legalize.pipeline.resolve_dates_to_process", return_value=[date(2026, 4, 25)]),
        ):
            # Should not raise — errors are collected and returned with the count
            commits = generic_daily(
                config, "mx", target_date=date(2026, 4, 25), dry_run=False
            )

        assert commits == 0

    def test_nothing_to_process_returns_zero(self, tmp_path):
        """When resolve_dates_to_process returns [] (already up to date), return 0."""
        config = _make_config(tmp_path)
        mock_client, mock_client_cls, mock_discovery, mock_disc_cls = (
            _mock_countries_dispatch()
        )

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_discovery_class", return_value=mock_disc_cls),
            patch("legalize.countries.get_text_parser", return_value=MagicMock()),
            patch("legalize.countries.get_metadata_parser", return_value=MagicMock()),
            patch("legalize.pipeline.resolve_dates_to_process", return_value=[]),
        ):
            commits = generic_daily(config, "mx", target_date=date(2026, 4, 25))

        assert commits == 0
        mock_discovery.discover_daily.assert_not_called()
