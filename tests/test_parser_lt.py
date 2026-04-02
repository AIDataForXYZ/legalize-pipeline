"""Tests for the Lithuanian TAR parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.lt.parser import TARMetadataParser, TARTextParser
from legalize.models import NormMetadata, NormStatus
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


class TestTARTextParser:
    def setup_method(self):
        self.parser = TARTextParser()

    def test_parse_html_returns_blocks(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        assert len(blocks) > 0

    def test_articles_detected(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        assert len(article_blocks) == 3

    def test_article_ids(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        ids = [b.id for b in article_blocks]
        assert ids == ["str1", "str2", "str3"]

    def test_article_has_paragraphs(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        # First article should have the heading + 2 body paragraphs
        assert len(article_blocks[0].versions[0].paragraphs) >= 2

    def test_article_title(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        article_blocks = [b for b in blocks if b.block_type == "article"]
        assert "1 straipsnis" in article_blocks[0].title

    def test_extract_reforms_returns_list(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        reforms = self.parser.extract_reforms(html)
        assert isinstance(reforms, list)

    def test_empty_html_returns_empty(self):
        blocks = self.parser.parse_text(b"<html><body></body></html>")
        assert blocks == []

    def test_structural_headings_detected(self):
        html = (FIXTURES / "tar-text-sample.html").read_bytes()
        blocks = self.parser.parse_text(html)
        # First block (before first article) should contain structural headings
        first_block = blocks[0]
        para_classes = [p.css_class for p in first_block.versions[0].paragraphs]
        assert "chapter_heading" in para_classes or "part_heading" in para_classes


class TestTARMetadataParser:
    def setup_method(self):
        self.parser = TARMetadataParser()

    def test_parse_civil_code(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert isinstance(meta, NormMetadata)
        assert meta.country == "lt"
        assert meta.identifier == "TAR-2000-12345"

    def test_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert "civilinis kodeksas" in meta.title.lower()

    def test_short_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert meta.short_title == "Civilinis kodeksas"

    def test_rank_istatymas(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert str(meta.rank) == "istatymas"

    def test_rank_konstitucija(self):
        json_data = (FIXTURES / "tar-metadata-TAR-1992-00001.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-1992-00001")
        assert str(meta.rank) == "konstitucija"

    def test_publication_date(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert meta.publication_date == date(2000, 7, 18)

    def test_in_force_status(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert meta.status == NormStatus.IN_FORCE

    def test_repealed_status(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2020-99999.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2020-99999")
        assert meta.status == NormStatus.REPEALED

    def test_department(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert meta.department == "Seimas"

    def test_source_url(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert "e-tar.lt" in meta.source
        assert "TAR-2000-12345" in meta.source

    def test_last_modified(self):
        json_data = (FIXTURES / "tar-metadata-TAR-2000-12345.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-2000-12345")
        assert meta.last_modified == date(2001, 7, 1)

    def test_short_title_falls_back_to_title(self):
        json_data = (FIXTURES / "tar-metadata-TAR-1992-00001.json").read_bytes()
        meta = self.parser.parse(json_data, "TAR-1992-00001")
        # trumpas_pavadinimas is empty, should fall back to pavadinimas
        assert meta.short_title == meta.title

    def test_empty_data_raises(self):
        import pytest

        empty = b'{"_data": [], "_page": {"next": null}}'
        with pytest.raises(ValueError, match="No metadata found"):
            self.parser.parse(empty, "TAR-0000-00000")


class TestCountriesDispatchLT:
    def test_get_text_parser_lt(self):
        parser = get_text_parser("lt")
        assert isinstance(parser, TARTextParser)

    def test_get_metadata_parser_lt(self):
        parser = get_metadata_parser("lt")
        assert isinstance(parser, TARMetadataParser)


class TestSlugLithuania:
    def test_norm_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="TAR-2000-12345",
            country="lt",
            rank="istatymas",
            publication_date=date(2000, 7, 18),
            status=NormStatus.IN_FORCE,
            department="Seimas",
            source="https://www.e-tar.lt/portal/lt/legalAct/TAR-2000-12345",
        )
        assert norm_to_filepath(meta) == "lt/TAR-2000-12345.md"
