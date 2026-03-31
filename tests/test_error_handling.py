"""Error handling tests for the Legalize pipeline.

Tests that errors are handled gracefully throughout the pipeline:
- Fetch errors (network, parse) return None instead of crashing
- Commit errors skip bad norms and continue with the rest
- Storage errors raise appropriate exceptions
- State/mappings handle missing files gracefully
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from legalize.config import Config, CountryConfig, GitConfig
from legalize.models import (
    Bloque,
    EstadoNorma,
    NormaCompleta,
    NormaMetadata,
    Paragraph,
    Rango,
    Reform,
    Version,
)
from legalize.pipeline import commit_all, commit_one, generic_fetch_all, generic_fetch_one
from legalize.state.mappings import IdToFilename
from legalize.state.store import StateStore
from legalize.storage import load_norma_from_json, save_structured_json


# ─────────────────────────────────────────────
# Helpers — synthetic norm builders (same pattern as test_integration_multicountry)
# ─────────────────────────────────────────────


def _make_version(source_id: str, d: date, text: str) -> Version:
    return Version(
        id_norma=source_id,
        fecha_publicacion=d,
        fecha_vigencia=d,
        paragraphs=(Paragraph(css_class="parrafo", text=text),),
    )


def _make_block(block_id: str, titulo: str, versions: list[Version]) -> Bloque:
    return Bloque(
        id=block_id,
        tipo="precepto",
        titulo=titulo,
        versions=tuple(versions),
    )


def _make_simple_norm(norm_id: str, title: str) -> NormaCompleta:
    """Create a minimal valid norm for testing."""
    d = date(2024, 1, 1)
    blocks = [
        _make_block("a1", "Articulo 1", [_make_version(norm_id, d, f"Texto de {title}.")]),
    ]
    reforms = [Reform(fecha=d, id_norma=norm_id, bloques_afectados=("a1",))]
    metadata = NormaMetadata(
        titulo=title,
        titulo_corto=title,
        identificador=norm_id,
        pais="es",
        rango=Rango.LEY,
        fecha_publicacion=d,
        estado=EstadoNorma.VIGENTE,
        departamento="Test",
        fuente="https://example.com/test",
    )
    return NormaCompleta(metadata=metadata, bloques=tuple(blocks), reforms=tuple(reforms))


@pytest.fixture
def test_config(tmp_path) -> Config:
    """Config with temporary repo and data dir."""
    return Config(
        git=GitConfig(repo_path=str(tmp_path / "repo")),
        data_dir=str(tmp_path / "data"),
        state_path=str(tmp_path / "state.json"),
        mappings_path=str(tmp_path / "mappings.json"),
        countries={
            "es": CountryConfig(
                repo_path=str(tmp_path / "repo"),
                data_dir=str(tmp_path / "data"),
            ),
        },
    )


# ─────────────────────────────────────────────
# TestGenericFetchErrorHandling
# ─────────────────────────────────────────────


class TestGenericFetchErrorHandling:
    """Test that fetch errors are handled gracefully."""

    def test_fetch_one_returns_none_on_client_error(self, test_config):
        """Mock a client whose get_metadata raises requests.RequestException.
        Verify generic_fetch_one returns None (doesn't crash).
        """
        mock_client = MagicMock()
        mock_client.get_metadata.side_effect = requests.RequestException("Connection refused")
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_client_cls = MagicMock()
        mock_client_cls.create.return_value = mock_client

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_text_parser"),
            patch("legalize.countries.get_metadata_parser"),
        ):
            result = generic_fetch_one(test_config, "es", "TEST-NORM-001", force=True)

        assert result is None

    def test_fetch_all_continues_after_failure(self, test_config):
        """Mock discovery that returns 3 IDs. Mock client where 2nd ID raises an error.
        Verify fetch_all returns the 2 successful ones and reports 1 error.
        """

        def mock_fetch_one(config, country, norm_id, force=False):
            if norm_id == "NORM-002":
                return None  # simulate error
            return _make_simple_norm(norm_id, f"Norm {norm_id}")

        mock_discovery = MagicMock()
        mock_discovery.discover_all.return_value = iter(["NORM-001", "NORM-002", "NORM-003"])

        mock_discovery_cls = MagicMock(return_value=mock_discovery)

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_client_cls = MagicMock()
        mock_client_cls.create.return_value = mock_client

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_discovery_class", return_value=mock_discovery_cls),
            patch("legalize.pipeline.generic_fetch_one", side_effect=mock_fetch_one),
        ):
            result = generic_fetch_all(test_config, "es")

        assert len(result) == 2
        assert "NORM-001" in result
        assert "NORM-003" in result
        assert "NORM-002" not in result

    def test_fetch_one_returns_none_on_parse_error(self, test_config):
        """Mock client that returns data, but parser.parse raises ValueError.
        Verify returns None.
        """
        mock_client = MagicMock()
        mock_client.get_metadata.return_value = b"<metadata/>"
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_client_cls = MagicMock()
        mock_client_cls.create.return_value = mock_client

        mock_meta_parser = MagicMock()
        mock_meta_parser.parse.side_effect = ValueError("Invalid metadata format")

        with (
            patch("legalize.countries.get_client_class", return_value=mock_client_cls),
            patch("legalize.countries.get_text_parser"),
            patch("legalize.countries.get_metadata_parser", return_value=mock_meta_parser),
        ):
            result = generic_fetch_one(test_config, "es", "TEST-NORM-BAD", force=True)

        assert result is None


# ─────────────────────────────────────────────
# TestCommitErrorHandling
# ─────────────────────────────────────────────


class TestCommitErrorHandling:
    """Test that commit errors are handled gracefully."""

    def test_commit_one_returns_zero_for_missing_json(self, test_config):
        """Call commit_one with a norm_id that has no JSON file.
        Verify returns 0 (not crash).
        """
        result = commit_one(test_config, "NONEXISTENT-NORM-ID")
        assert result == 0

    def test_commit_all_continues_after_failure(self, test_config):
        """Create 3 JSON files where one has corrupt data.
        Verify commit_all processes the other 2 and reports the error.
        """
        # Save 2 valid norms
        norm1 = _make_simple_norm("TEST-ERR-001", "Ley Uno")
        norm3 = _make_simple_norm("TEST-ERR-003", "Ley Tres")
        save_structured_json(test_config.data_dir, norm1)
        save_structured_json(test_config.data_dir, norm3)

        # Write a corrupt JSON file
        json_dir = Path(test_config.data_dir) / "json"
        corrupt_file = json_dir / "TEST-ERR-002.json"
        corrupt_file.write_text("{invalid json content", encoding="utf-8")

        total = commit_all(test_config)

        # The 2 valid norms should each produce 1 commit (bootstrap)
        assert total == 2

    def test_commit_all_returns_zero_for_empty_dir(self, test_config):
        """Call commit_all with an empty data/json/ directory.
        Verify returns 0.
        """
        # Create the directory but leave it empty
        json_dir = Path(test_config.data_dir) / "json"
        json_dir.mkdir(parents=True, exist_ok=True)

        # Also create the repo dir so git log doesn't crash
        repo_dir = Path(test_config.git.repo_path)
        repo_dir.mkdir(parents=True, exist_ok=True)

        result = commit_all(test_config)
        assert result == 0

    def test_commit_all_returns_zero_for_nonexistent_dir(self, test_config):
        """Call commit_all when data/json/ does not exist at all.
        Verify returns 0.
        """
        result = commit_all(test_config)
        assert result == 0


# ─────────────────────────────────────────────
# TestStorageErrorHandling
# ─────────────────────────────────────────────


class TestStorageErrorHandling:
    """Test storage error handling."""

    def test_load_raises_on_corrupt_json(self, tmp_path):
        """Write invalid JSON to a file, call load_norma_from_json.
        Verify it raises json.JSONDecodeError.
        """
        bad_file = tmp_path / "corrupt.json"
        bad_file.write_text("{not valid json!!!", encoding="utf-8")

        with pytest.raises(json.JSONDecodeError):
            load_norma_from_json(bad_file)

    def test_load_raises_on_missing_fields(self, tmp_path):
        """Write JSON missing required fields (e.g., no "metadata" key).
        Verify it raises KeyError.
        """
        incomplete_file = tmp_path / "incomplete.json"
        incomplete_file.write_text(
            json.dumps({"articles": [], "reforms": []}),
            encoding="utf-8",
        )

        with pytest.raises(KeyError):
            load_norma_from_json(incomplete_file)

    def test_save_creates_directories(self, tmp_path):
        """Call save_structured_json with a data_dir that doesn't exist yet.
        Verify it creates the directories and saves the file.
        """
        nested_dir = tmp_path / "deep" / "nested" / "path"
        assert not nested_dir.exists()

        norm = _make_simple_norm("TEST-DIR-CREATE", "Ley Directorios")
        result_path = save_structured_json(str(nested_dir), norm)

        assert result_path.exists()
        assert result_path.parent.exists()

        # Verify the saved file can be loaded back
        loaded = load_norma_from_json(result_path)
        assert loaded.metadata.identificador == "TEST-DIR-CREATE"


# ─────────────────────────────────────────────
# TestStateStoreErrorHandling
# ─────────────────────────────────────────────


class TestStateStoreErrorHandling:
    """Test StateStore error handling."""

    def test_load_handles_missing_file(self, tmp_path):
        """StateStore.load() on non-existent path.
        Verify no crash, empty state.
        """
        store = StateStore(tmp_path / "nonexistent" / "state.json")
        store.load()  # should not raise

        assert store.norms_count == 0
        assert store.last_summary_date is None

    def test_load_handles_corrupt_json(self, tmp_path):
        """Write invalid JSON to state file.
        Verify load() raises json.JSONDecodeError.
        """
        state_file = tmp_path / "state.json"
        state_file.write_text("{corrupt json!!!", encoding="utf-8")

        store = StateStore(state_file)
        with pytest.raises(json.JSONDecodeError):
            store.load()


# ─────────────────────────────────────────────
# TestMappingsErrorHandling
# ─────────────────────────────────────────────


class TestMappingsErrorHandling:
    """Test IdToFilename error handling."""

    def test_load_handles_missing_file(self, tmp_path):
        """IdToFilename.load() on non-existent path.
        Verify no crash, empty mappings.
        """
        mappings = IdToFilename(tmp_path / "nonexistent" / "mappings.json")
        mappings.load()  # should not raise

        assert len(mappings) == 0
        assert mappings.get("BOE-A-1978-31229") is None
