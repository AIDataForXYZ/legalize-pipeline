"""Tests for the generic multi-country pipeline.

Covers: CountryConfig, countries dispatch, storage round-trip,
IdToFilename reverse index, StateStore persistence, and
generic pipeline helpers.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest

from legalize.config import Config, CountryConfig
from legalize.countries import (
    REGISTRY,
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)
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
from legalize.pipeline import _extract_reforms_generic
from legalize.state.mappings import IdToFilename
from legalize.state.store import StateStore
from legalize.storage import load_norma_from_json, save_structured_json


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


def _make_norma(
    identificador: str = "TEST-001",
    pais: str = "es",
    css_classes: list[str] | None = None,
) -> NormaCompleta:
    """Build a minimal NormaCompleta for testing."""
    paragraphs = []
    if css_classes:
        for i, cls in enumerate(css_classes):
            paragraphs.append(Paragraph(css_class=cls, text=f"Paragraph {i}"))
    else:
        paragraphs.append(Paragraph(css_class="parrafo", text="Texto del articulo 1."))

    version = Version(
        id_norma=identificador,
        fecha_publicacion=date(2024, 1, 1),
        fecha_vigencia=date(2024, 1, 1),
        paragraphs=tuple(paragraphs),
    )
    block = Bloque(
        id="a1",
        tipo="precepto",
        titulo="Articulo 1",
        versions=(version,),
    )
    metadata = NormaMetadata(
        titulo="Ley de Pruebas",
        titulo_corto="Ley Pruebas",
        identificador=identificador,
        pais=pais,
        rango=Rango("ley"),
        fecha_publicacion=date(2024, 1, 1),
        estado=EstadoNorma.VIGENTE,
        departamento="Test",
        fuente="https://example.com/test",
        fecha_ultima_modificacion=date(2024, 6, 1),
    )
    reform = Reform(
        fecha=date(2024, 1, 1),
        id_norma=identificador,
        bloques_afectados=("a1",),
    )
    return NormaCompleta(
        metadata=metadata,
        bloques=(block,),
        reforms=(reform,),
    )


# ─────────────────────────────────────────────
# TestCountryConfig
# ─────────────────────────────────────────────


class TestCountryConfig:
    def test_get_country_from_yaml(self):
        """Config with countries section returns correct CountryConfig."""
        cc_se = CountryConfig(repo_path="../se", data_dir="../data-se")
        config = Config(countries={"se": cc_se})
        result = config.get_country("se")
        assert result.repo_path == "../se"
        assert result.data_dir == "../data-se"

    def test_get_country_without_countries_section_raises(self):
        """Config without countries section raises ValueError."""
        config = Config()
        with pytest.raises(ValueError, match="not configured"):
            config.get_country("es")

    def test_get_country_unknown_raises(self):
        """Requesting unknown country raises ValueError."""
        config = Config()
        with pytest.raises(ValueError, match="not configured"):
            config.get_country("xx")

    def test_country_config_defaults_state_path(self):
        """Empty state_path gets default .pipeline/{code}/state.json."""
        cc = CountryConfig(repo_path="../se", data_dir="../data-se", state_path="")
        config = Config(countries={"se": cc})
        result = config.get_country("se")
        assert result.state_path == ".pipeline/se/state.json"
        assert result.mappings_path == ".pipeline/se/mappings.json"

    def test_supported_countries(self):
        """supported_countries() returns sorted list of registered codes."""
        codes = supported_countries()
        assert codes == sorted(codes)
        assert len(codes) >= 2  # at least es and fr
        assert "es" in codes
        assert "fr" in codes


# ─────────────────────────────────────────────
# TestCountriesDispatch
# ─────────────────────────────────────────────


class TestCountriesDispatch:
    REQUIRED_COMPONENTS = {"client", "discovery", "text_parser", "metadata_parser"}

    def test_all_countries_have_required_components(self):
        """Every country in REGISTRY has client, discovery, text_parser, metadata_parser."""
        for code, components in REGISTRY.items():
            missing = self.REQUIRED_COMPONENTS - set(components.keys())
            assert not missing, f"Country '{code}' missing components: {missing}"

    def test_all_clients_have_create(self):
        """Every client class has a create() classmethod."""
        for code in REGISTRY:
            cls = get_client_class(code)
            assert hasattr(cls, "create"), f"{code} client missing create()"
            assert callable(cls.create)

    def test_all_clients_are_context_managers(self):
        """Every client has __enter__ and __exit__."""
        for code in REGISTRY:
            cls = get_client_class(code)
            assert hasattr(cls, "__enter__"), f"{code} client missing __enter__"
            assert hasattr(cls, "__exit__"), f"{code} client missing __exit__"

    def test_all_parsers_have_required_methods(self):
        """TextParser has parse_text, extract_reforms; MetadataParser has parse."""
        for code in REGISTRY:
            tp = get_text_parser(code)
            assert hasattr(tp, "parse_text"), f"{code} text_parser missing parse_text"
            assert hasattr(tp, "extract_reforms"), f"{code} text_parser missing extract_reforms"

            mp = get_metadata_parser(code)
            assert hasattr(mp, "parse"), f"{code} metadata_parser missing parse"

    def test_all_discoveries_have_required_methods(self):
        """NormDiscovery has discover_all, discover_daily."""
        for code in REGISTRY:
            cls = get_discovery_class(code)
            # Check on the class (not an instance, since constructors may need args)
            assert hasattr(cls, "discover_all"), f"{code} discovery missing discover_all"
            assert hasattr(cls, "discover_daily"), f"{code} discovery missing discover_daily"


# ─────────────────────────────────────────────
# TestStorageRoundTrip
# ─────────────────────────────────────────────


class TestStorageRoundTrip:
    def test_save_and_load_norma(self, tmp_path):
        """Create a NormaCompleta, save to JSON, load back, verify all fields match."""
        norm = _make_norma()
        save_structured_json(str(tmp_path), norm)

        json_path = tmp_path / "json" / f"{norm.metadata.identificador}.json"
        assert json_path.exists()

        loaded = load_norma_from_json(json_path)
        assert loaded.metadata.identificador == norm.metadata.identificador
        assert loaded.metadata.titulo == norm.metadata.titulo.rstrip(". ")
        assert loaded.metadata.pais == norm.metadata.pais
        assert loaded.metadata.rango == norm.metadata.rango
        assert loaded.metadata.fecha_publicacion == norm.metadata.fecha_publicacion
        assert loaded.metadata.estado == norm.metadata.estado
        assert len(loaded.bloques) == len(norm.bloques)
        assert len(loaded.reforms) == len(norm.reforms)

        # Check block content round-trips
        orig_block = norm.bloques[0]
        loaded_block = loaded.bloques[0]
        assert loaded_block.id == orig_block.id
        assert loaded_block.tipo == orig_block.tipo
        assert loaded_block.titulo == orig_block.titulo
        assert len(loaded_block.versions) == len(orig_block.versions)

    def test_css_class_round_trip(self, tmp_path):
        """Save norm with non-parrafo CSS classes, load back, verify classes preserved."""
        norm = _make_norma(css_classes=["titulo_articulo", "parrafo", "lista_letra"])
        save_structured_json(str(tmp_path), norm)

        json_path = tmp_path / "json" / f"{norm.metadata.identificador}.json"
        loaded = load_norma_from_json(json_path)

        loaded_paragraphs = loaded.bloques[0].versions[0].paragraphs
        assert len(loaded_paragraphs) == 3
        assert loaded_paragraphs[0].css_class == "titulo_articulo"
        assert loaded_paragraphs[1].css_class == "parrafo"
        assert loaded_paragraphs[2].css_class == "lista_letra"

    def test_load_old_json_without_css_classes(self, tmp_path):
        """Load JSON without css_classes field, verify fallback to 'parrafo'."""
        # Simulate old JSON format (no css_classes key in versions)
        data = {
            "metadata": {
                "titulo": "Ley Antigua",
                "titulo_corto": "Ley Antigua",
                "identificador": "OLD-001",
                "pais": "es",
                "rango": "ley",
                "fecha_publicacion": "2020-01-01",
                "ultima_actualizacion": "2020-01-01",
                "estado": "vigente",
                "departamento": "Test",
                "fuente": "https://example.com",
            },
            "articles": [
                {
                    "block_id": "a1",
                    "block_type": "precepto",
                    "title": "Articulo 1",
                    "position": 0,
                    "current_text": "Line one\n\nLine two",
                    "versions": [
                        {
                            "date": "2020-01-01",
                            "source_id": "OLD-001",
                            "text": "Line one\n\nLine two",
                            # No css_classes key
                        }
                    ],
                }
            ],
            "reforms": [
                {
                    "date": "2020-01-01",
                    "source_id": "OLD-001",
                    "articles_affected": ["Articulo 1"],
                }
            ],
        }
        json_path = tmp_path / "old.json"
        json_path.write_text(json.dumps(data), encoding="utf-8")

        loaded = load_norma_from_json(json_path)
        paragraphs = loaded.bloques[0].versions[0].paragraphs
        assert len(paragraphs) == 2
        assert all(p.css_class == "parrafo" for p in paragraphs)


# ─────────────────────────────────────────────
# TestMappingsReverseIndex
# ─────────────────────────────────────────────


class TestMappingsReverseIndex:
    def test_set_and_reverse_lookup(self, tmp_path):
        """set(id, path) then get_by_filepath(path) returns id."""
        m = IdToFilename(tmp_path / "mappings.json")
        m.set("BOE-A-2024-001", "spain/BOE-A-2024-001.md")
        assert m.get_by_filepath("spain/BOE-A-2024-001.md") == "BOE-A-2024-001"

    def test_reverse_lookup_after_load(self, tmp_path):
        """save, reload, verify reverse lookup works."""
        path = tmp_path / "mappings.json"

        m1 = IdToFilename(path)
        m1.set("BOE-A-2024-002", "spain/BOE-A-2024-002.md")
        m1.save()

        m2 = IdToFilename(path)
        m2.load()
        assert m2.get_by_filepath("spain/BOE-A-2024-002.md") == "BOE-A-2024-002"
        assert m2.get("BOE-A-2024-002") == "spain/BOE-A-2024-002.md"

    def test_reverse_lookup_missing(self, tmp_path):
        """get_by_filepath for unknown path returns None."""
        m = IdToFilename(tmp_path / "mappings.json")
        assert m.get_by_filepath("nonexistent/path.md") is None


# ─────────────────────────────────────────────
# TestStateStorePersistence
# ─────────────────────────────────────────────


class TestStateStorePersistence:
    def test_load_state_from_json(self, tmp_path):
        """Load a state.json and verify all fields are read correctly."""
        state_path = tmp_path / "state.json"
        data = {
            "last_summary": "2024-03-15",
            "norms_processed": {
                "BOE-A-1978-31229": {
                    "last_version_applied": "2024-02-17",
                    "total_versions_applied": 12,
                }
            },
            "runs": [
                {
                    "timestamp": "2024-03-15T10:30:00",
                    "summaries_reviewed": ["2024-03-15"],
                    "commits_created": 5,
                    "errors": [],
                }
            ],
        }
        state_path.write_text(json.dumps(data), encoding="utf-8")

        store = StateStore(state_path)
        store.load()

        assert store.last_summary_date == date(2024, 3, 15)
        ns = store.get_norm_state("BOE-A-1978-31229")
        assert ns is not None
        assert ns.last_version_applied == "2024-02-17"
        assert ns.total_versions_applied == 12

    def test_save_json_structure(self, tmp_path):
        """Save state, read raw JSON, verify key structure."""
        state_path = tmp_path / "state.json"
        store = StateStore(state_path)
        store.last_summary_date = date(2024, 6, 1)
        store.mark_norma_processed("TEST-001", date(2024, 6, 1), 3)
        store.save()

        raw = json.loads(state_path.read_text(encoding="utf-8"))
        assert raw["last_summary"] == "2024-06-01"
        assert "TEST-001" in raw["norms_processed"]
        assert raw["norms_processed"]["TEST-001"]["last_version_applied"] == "2024-06-01"
        assert raw["norms_processed"]["TEST-001"]["total_versions_applied"] == 3
        assert isinstance(raw["runs"], list)

    def test_round_trip(self, tmp_path):
        """record_run + save + load, verify data preserved."""
        state_path = tmp_path / "state.json"

        store1 = StateStore(state_path)
        store1.last_summary_date = date(2024, 7, 1)
        store1.mark_norma_processed("ROUND-001", date(2024, 7, 1), 5)
        store1.record_run(summaries=["2024-07-01"], commits=10, errors=["minor issue"])
        store1.save()

        store2 = StateStore(state_path)
        store2.load()

        assert store2.last_summary_date == date(2024, 7, 1)
        assert store2.norms_count == 1
        ns = store2.get_norm_state("ROUND-001")
        assert ns is not None
        assert ns.total_versions_applied == 5


# ─────────────────────────────────────────────
# TestGenericPipeline
# ─────────────────────────────────────────────


class TestGenericPipeline:
    def test_extract_reforms_generic_fallback(self):
        """_extract_reforms_generic falls back to extract_reforms when parser has no SFSR method."""
        mock_parser = MagicMock(spec=["parse_text", "extract_reforms"])
        mock_client = MagicMock()
        # Ensure the parser does NOT have extract_reforms_from_sfsr
        assert not hasattr(mock_parser, "extract_reforms_from_sfsr")

        blocks = [
            Bloque(
                id="a1",
                tipo="precepto",
                titulo="Articulo 1",
                versions=(
                    Version(
                        id_norma="TEST-001",
                        fecha_publicacion=date(2024, 1, 1),
                        fecha_vigencia=date(2024, 1, 1),
                        paragraphs=(Paragraph(css_class="parrafo", text="text"),),
                    ),
                ),
            )
        ]

        result = _extract_reforms_generic(mock_parser, mock_client, "TEST-001", blocks)
        # Should have called the standard extract_reforms from xml_parser (not on the mock)
        # The result should be a list of Reform objects derived from the blocks
        assert isinstance(result, list)

    def test_extract_reforms_generic_with_sfsr(self):
        """Mock a parser with extract_reforms_from_sfsr and client with get_amendment_register."""
        mock_parser = MagicMock()
        mock_parser.extract_reforms_from_sfsr.return_value = [
            Reform(
                fecha=date(2024, 3, 1),
                id_norma="SFS-2024:100",
                bloques_afectados=(),
            )
        ]

        mock_client = MagicMock()
        mock_client.get_amendment_register.return_value = b"<html>sfsr data</html>"

        result = _extract_reforms_generic(mock_parser, mock_client, "SFS-2024:1", [])
        assert len(result) == 1
        assert result[0].id_norma == "SFS-2024:100"
        mock_client.get_amendment_register.assert_called_once_with("SFS-2024:1")
        mock_parser.extract_reforms_from_sfsr.assert_called_once_with(b"<html>sfsr data</html>")

    def test_extract_reforms_generic_sfsr_fallback_on_error(self):
        """If SFSR fetch fails, falls back to standard extract_reforms."""
        mock_parser = MagicMock()
        mock_parser.extract_reforms_from_sfsr.side_effect = Exception("Network error")

        mock_client = MagicMock()
        mock_client.get_amendment_register.side_effect = Exception("Network error")

        blocks = [
            Bloque(
                id="a1",
                tipo="precepto",
                titulo="Articulo 1",
                versions=(
                    Version(
                        id_norma="SFS-2024:1",
                        fecha_publicacion=date(2024, 1, 1),
                        fecha_vigencia=date(2024, 1, 1),
                        paragraphs=(Paragraph(css_class="parrafo", text="text"),),
                    ),
                ),
            )
        ]

        # Should not raise, falls back to extract_reforms
        result = _extract_reforms_generic(mock_parser, mock_client, "SFS-2024:1", blocks)
        assert isinstance(result, list)
