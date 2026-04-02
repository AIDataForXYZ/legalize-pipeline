"""Tests for the Uruguayan IMPO parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import (
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)
from legalize.fetcher.uy.client import IMPOClient
from legalize.fetcher.uy.discovery import IMPODiscovery, _estimate_year, _year_candidates
from legalize.fetcher.uy.parser import (
    IMPOMetadataParser,
    IMPOTextParser,
    _decode_json,
    _make_identifier,
    _parse_date,
    _strip_html,
)
from legalize.models import NormMetadata, NormStatus
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


# ─── Helpers ───


def _load_fixture(name: str) -> bytes:
    """Load a fixture as Latin-1 bytes (matching IMPO encoding)."""
    text = (FIXTURES / name).read_text(encoding="utf-8")
    return text.encode("latin-1")


# ─── Unit tests for parsing helpers ───


class TestParsingHelpers:
    def test_parse_date_dd_mm_yyyy(self):
        assert _parse_date("09/11/2021") == date(2021, 11, 9)

    def test_parse_date_iso_format(self):
        assert _parse_date("2021-11-09") == date(2021, 11, 9)

    def test_parse_date_empty(self):
        assert _parse_date("") is None
        assert _parse_date("  ") is None

    def test_parse_date_invalid(self):
        assert _parse_date("not-a-date") is None

    def test_strip_html_basic(self):
        assert _strip_html('<font color="#0000FF">Hello</font>') == "Hello"

    def test_strip_html_nested(self):
        assert _strip_html("<a href='x'><b>Text</b></a>") == "Text"

    def test_strip_html_empty(self):
        assert _strip_html("") == ""
        assert _strip_html(None) == ""

    def test_decode_json_latin1(self):
        raw = '{"key": "value"}'.encode("latin-1")
        assert _decode_json(raw) == {"key": "value"}

    def test_decode_json_utf8(self):
        raw = '{"key": "value"}'.encode("utf-8")
        assert _decode_json(raw) == {"key": "value"}

    def test_make_identifier_ley(self):
        doc = {"tipoNorma": "Ley", "nroNorma": "19996", "anioNorma": 2021}
        assert _make_identifier(doc, "leyes/19996-2021") == "UY-ley-19996"

    def test_make_identifier_decreto_ley(self):
        doc = {"tipoNorma": "Decreto Ley", "nroNorma": "14261", "anioNorma": 1974}
        assert _make_identifier(doc, "decretos-ley/14261-1974") == "UY-decreto-ley-14261"

    def test_make_identifier_constitucion(self):
        doc = {"tipoNorma": "CONSTITUCION DE LA REPUBLICA"}
        assert _make_identifier(doc, "constitucion/1967-1967") == "UY-constitucion-1967"

    def test_make_identifier_decreto(self):
        doc = {"tipoNorma": "Decreto", "nroNorma": "122", "anioNorma": 2021}
        assert _make_identifier(doc, "decretos/122-2021") == "UY-decreto-122-2021"

    def test_nombre_norma_strips_crlf(self):
        """nombreNorma from real IMPO responses can have trailing \\r\\n."""
        raw = "PRESUPUESTO NACIONAL\r\n"
        assert raw.strip() == "PRESUPUESTO NACIONAL"


# ─── IMPOTextParser tests ───


class TestIMPOTextParser:
    def setup_method(self):
        self.parser = IMPOTextParser()

    def test_parse_ley_blocks(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        # 4 articles + 2 section headings = 6 blocks
        assert len(blocks) >= 4

    def test_parse_ley_article_text(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        # Find the first article block (not heading)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) >= 3  # articles 1, 2, 3 (placeholder), 4

        first = article_blocks[0]
        assert first.id == "art-1"
        assert "plataformas digitales" in first.versions[0].paragraphs[0].text

    def test_parse_ley_section_headings(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        heading_blocks = [b for b in blocks if b.block_type == "heading"]
        assert len(heading_blocks) >= 1
        assert "SECCION I" in heading_blocks[0].title
        # Headings should use css_classes the markdown renderer knows
        assert heading_blocks[0].versions[0].paragraphs[0].css_class == "seccion"

    def test_parse_placeholder_article(self):
        """Articles with textoArticulo='(*)' should emit a (*) placeholder."""
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        art3 = [b for b in article_blocks if b.id == "art-3"]
        assert len(art3) == 1
        assert art3[0].versions[0].paragraphs[0].text == "(*)"

    def test_notes_excluded_from_article_text(self):
        """IMPO editorial notes (notasArticulo) should NOT appear in article paragraphs."""
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        for art in article_blocks:
            for p in art.versions[0].paragraphs:
                assert p.css_class != "nota", f"nota found in {art.id}"

    def test_parse_version_date(self):
        data = _load_fixture("impo-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert article_blocks[0].versions[0].publication_date == date(2021, 11, 9)

    def test_parse_empty_data(self):
        assert self.parser.parse_text(b"") == []

    def test_parse_constitucion(self):
        data = _load_fixture("impo-constitucion-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) == 3
        first = article_blocks[0]
        assert "Republica Oriental del Uruguay" in first.versions[0].paragraphs[0].text

    def test_parse_constitucion_nested_headings(self):
        """Constitution has nested section/chapter headings with correct css_classes."""
        data = _load_fixture("impo-constitucion-sample.json")
        blocks = self.parser.parse_text(data)
        heading_blocks = [b for b in blocks if b.block_type == "heading"]
        titles = [b.title for b in heading_blocks]
        assert any("SECCION I" in t for t in titles)
        assert any("CAPITULO I" in t for t in titles)
        # Verify correct CSS class mapping
        for hb in heading_blocks:
            css = hb.versions[0].paragraphs[0].css_class
            if "SECCION" in hb.title:
                assert css == "seccion"
            elif "CAPITULO" in hb.title:
                assert css == "capitulo_tit"

    def test_parse_decreto_ley(self):
        data = _load_fixture("impo-decreto-ley-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) == 2
        assert "tributos" in article_blocks[0].versions[0].paragraphs[0].text

    def test_parse_decreto(self):
        """Decreto (not decreto-ley) parses correctly."""
        data = _load_fixture("impo-decreto-sample.json")
        blocks = self.parser.parse_text(data)
        article_blocks = [b for b in blocks if b.block_type == "articulo"]
        assert len(article_blocks) == 2
        assert "reglamentacion" in article_blocks[0].versions[0].paragraphs[0].text

    def test_extract_reforms_returns_list(self):
        data = _load_fixture("impo-ley-sample.json")
        reforms = self.parser.extract_reforms(data)
        assert isinstance(reforms, list)
        assert len(reforms) == 1
        assert reforms[0].date == date(2021, 11, 9)

    def test_parse_html_returns_empty(self):
        """IMPO returns HTML (not JSON) for non-existent norms.

        The parser should handle this gracefully — _decode_json raises
        ValueError, and parse_text should return empty.
        """
        html = b"<html><head><title>Ingreso - IMPO</title></head><body>Login</body></html>"
        # This would be caught by the client (not starts with '{'),
        # but if it reaches the parser, it should not crash
        import pytest

        with pytest.raises(ValueError, match="Could not decode"):
            from legalize.fetcher.uy.parser import _decode_json

            _decode_json(html)


# ─── IMPOMetadataParser tests ───


class TestIMPOMetadataParser:
    def setup_method(self):
        self.parser = IMPOMetadataParser()

    def test_parse_ley_metadata(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert isinstance(meta, NormMetadata)
        assert meta.country == "uy"
        assert meta.identifier == "UY-ley-19996"

    def test_ley_rank(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert str(meta.rank) == "ley"

    def test_ley_title(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert "19996" in meta.title
        assert "PLATAFORMAS DIGITALES" in meta.title

    def test_ley_date(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.publication_date == date(2021, 11, 9)

    def test_ley_source(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.source == "https://www.impo.com.uy/bases/leyes/19996-2021"

    def test_ley_status(self):
        data = _load_fixture("impo-ley-sample.json")
        meta = self.parser.parse(data, "leyes/19996-2021")
        assert meta.status == NormStatus.IN_FORCE

    def test_constitucion_metadata(self):
        data = _load_fixture("impo-constitucion-sample.json")
        meta = self.parser.parse(data, "constitucion/1967-1967")
        assert meta.identifier == "UY-constitucion-1967"
        assert str(meta.rank) == "constitucion"
        assert meta.publication_date == date(1967, 2, 2)

    def test_decreto_ley_metadata(self):
        data = _load_fixture("impo-decreto-ley-sample.json")
        meta = self.parser.parse(data, "decretos-ley/14261-1974")
        assert meta.identifier == "UY-decreto-ley-14261"
        assert str(meta.rank) == "decreto_ley"
        assert meta.publication_date == date(1974, 9, 9)

    def test_decreto_metadata(self):
        """Decreto identifier includes year (numbers reset yearly)."""
        data = _load_fixture("impo-decreto-sample.json")
        meta = self.parser.parse(data, "decretos/122-2021")
        assert meta.identifier == "UY-decreto-122-2021"
        assert str(meta.rank) == "decreto"
        assert meta.publication_date == date(2021, 4, 30)

    def test_empty_data_raises(self):
        import pytest

        with pytest.raises(ValueError, match="Empty data"):
            self.parser.parse(b"", "leyes/1-0000")


# ─── Client tests ───


class TestIMPOClientNotFound:
    def test_html_response_detected_as_not_found(self):
        """Client should return empty bytes when IMPO returns HTML instead of JSON."""
        from unittest.mock import MagicMock, patch

        client = IMPOClient(base_url="https://www.impo.com.uy", requests_per_second=0)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"<html><head><title>Ingreso - IMPO</title></head></html>"
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.get_text("leyes/99999-2025")
            assert result == b""

    def test_valid_json_response_returned(self):
        """Client should return content when response is valid JSON."""
        from unittest.mock import MagicMock, patch

        client = IMPOClient(base_url="https://www.impo.com.uy", requests_per_second=0)

        json_bytes = b'{"tipoNorma": "Ley", "articulos": []}'
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = json_bytes
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_resp):
            result = client.get_text("leyes/19996-2021")
            assert result == json_bytes


# ─── Country dispatch tests ───


class TestCountriesDispatch:
    def test_uy_in_supported_countries(self):
        assert "uy" in supported_countries()

    def test_get_client_class_uy(self):
        cls = get_client_class("uy")
        assert cls is IMPOClient

    def test_get_discovery_class_uy(self):
        cls = get_discovery_class("uy")
        assert cls is IMPODiscovery

    def test_get_text_parser_uy(self):
        parser = get_text_parser("uy")
        assert isinstance(parser, IMPOTextParser)

    def test_get_metadata_parser_uy(self):
        parser = get_metadata_parser("uy")
        assert isinstance(parser, IMPOMetadataParser)


# ─── Slug tests ───


class TestSlugUruguay:
    def test_ley_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="UY-ley-19996",
            country="uy",
            rank="ley",
            publication_date=date(2021, 11, 9),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/leyes/19996-2021",
        )
        assert norm_to_filepath(meta) == "uy/UY-ley-19996.md"

    def test_constitucion_path(self):
        meta = NormMetadata(
            title="Constitucion",
            short_title="Constitucion",
            identifier="UY-constitucion-1967",
            country="uy",
            rank="constitucion",
            publication_date=date(1967, 2, 2),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/constitucion/1967-1967",
        )
        assert norm_to_filepath(meta) == "uy/UY-constitucion-1967.md"

    def test_decreto_ley_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="UY-decreto-ley-14261",
            country="uy",
            rank="decreto_ley",
            publication_date=date(1974, 9, 9),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/decretos-ley/14261-1974",
        )
        assert norm_to_filepath(meta) == "uy/UY-decreto-ley-14261.md"

    def test_decreto_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="UY-decreto-122-2021",
            country="uy",
            rank="decreto",
            publication_date=date(2021, 4, 30),
            status=NormStatus.IN_FORCE,
            department="",
            source="https://www.impo.com.uy/bases/decretos/122-2021",
        )
        assert norm_to_filepath(meta) == "uy/UY-decreto-122-2021.md"


# ─── Discovery tests ───


class TestYearEstimation:
    def test_known_laws_in_candidates(self):
        """All known law number→year pairs should appear in year candidates."""
        known = [
            (9155, 1933),
            (17000, 1998),
            (18000, 2006),
            (19000, 2012),
            (19996, 2021),
            (20468, 2026),
        ]
        for num, real_year in known:
            cands = _year_candidates(num)
            assert real_year in cands, f"Year {real_year} not in candidates for law {num}: {cands}"

    def test_estimate_boundary_low(self):
        assert _estimate_year(1) == 1826

    def test_estimate_boundary_high(self):
        assert _estimate_year(25000) == 2026

    def test_candidates_has_five_entries(self):
        cands = _year_candidates(19000)
        assert len(cands) == 5

    def test_estimate_interpolation(self):
        # Law 19500 should estimate to ~2017
        est = _estimate_year(19500)
        assert 2015 <= est <= 2019


class TestIMPODiscoveryDaily:
    def test_discover_daily_uses_correct_year(self):
        """discover_daily should try target_date.year and year-1."""
        from unittest.mock import MagicMock, call

        client = MagicMock(spec=IMPOClient)

        # Return data only for leyes/20461-2025 (second year candidate)
        def fake_get_text(norm_id):
            if norm_id == "leyes/20461-2025":
                return b'{"articulos": []}'
            return b""

        client.get_text.side_effect = fake_get_text

        discovery = IMPODiscovery(law_number_max=20460)
        results = list(discovery.discover_daily(client, date(2026, 4, 3), last_known_number=20460))

        assert "leyes/20461-2025" in results
        # Should have tried 2026 first, then 2025
        assert call("leyes/20461-2026") in client.get_text.call_args_list
        assert call("leyes/20461-2025") in client.get_text.call_args_list

    def test_discover_daily_finds_current_year(self):
        """discover_daily should find laws published in the current year."""
        from unittest.mock import MagicMock

        client = MagicMock(spec=IMPOClient)

        def fake_get_text(norm_id):
            if norm_id == "leyes/20461-2026":
                return b'{"articulos": []}'
            return b""

        client.get_text.side_effect = fake_get_text

        discovery = IMPODiscovery(law_number_max=20460)
        results = list(discovery.discover_daily(client, date(2026, 4, 3), last_known_number=20460))

        assert "leyes/20461-2026" in results
