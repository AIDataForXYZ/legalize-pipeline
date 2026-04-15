"""Tests for the Canadian federal legislation parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.ca.client import _parse_norm_id
from legalize.fetcher.ca.parser import CAMetadataParser, CATextParser
from legalize.models import NormStatus
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures" / "ca"

ACT_SMALL = FIXTURES / "sample-act-small.xml"
ACT_MINIMAL = FIXTURES / "sample-act-minimal.xml"
ACT_WITH_TABLES = FIXTURES / "sample-act-with-tables.xml"
REGULATION = FIXTURES / "sample-regulation.xml"
REGULATION_WITH_TABLES = FIXTURES / "sample-regulation-with-tables.xml"


# ─────────────────────────────────────────────
# norm_id helpers
# ─────────────────────────────────────────────


class TestNormIdHelpers:
    def test_parse_norm_id_act(self):
        lang, cat, fid = _parse_norm_id("eng/acts/A-1")
        assert lang == "eng"
        assert cat == "acts"
        assert fid == "A-1"

    def test_parse_norm_id_regulation(self):
        lang, cat, fid = _parse_norm_id("fra/reglements/SOR-99-129")
        assert lang == "fra"
        assert cat == "reglements"
        assert fid == "SOR-99-129"

    def test_parse_norm_id_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid CA norm_id"):
            _parse_norm_id("invalid")


# ─────────────────────────────────────────────
# Text parser — acts
# ─────────────────────────────────────────────


class TestCATextParserActs:
    def setup_method(self):
        self.parser = CATextParser()

    def test_small_act_returns_one_block(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        assert len(blocks) == 1

    def test_small_act_block_structure(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        block = blocks[0]
        assert block.block_type == "article"
        assert block.id == "body"
        assert len(block.versions) == 1

    def test_small_act_has_sections(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        articles = [p for p in paras if p.css_class == "articulo"]
        assert len(articles) == 25  # B-9.8 has 25 sections

    def test_small_act_has_headings(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        headings = [p for p in paras if p.css_class in ("titulo_tit", "capitulo_tit")]
        assert len(headings) >= 5

    def test_small_act_has_paragraphs(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        assert len(paras) >= 50

    def test_small_act_publication_date(self):
        blocks = self.parser.parse_text(ACT_SMALL.read_bytes())
        pub_date = blocks[0].versions[0].publication_date
        assert isinstance(pub_date, date)
        assert pub_date == date(2003, 1, 1)

    def test_minimal_act_no_body_returns_empty(self):
        """Acts with no <Body> (e.g., repealed stubs) return no blocks."""
        blocks = self.parser.parse_text(ACT_MINIMAL.read_bytes())
        assert blocks == []

    def test_act_with_tables_has_tables(self):
        blocks = self.parser.parse_text(ACT_WITH_TABLES.read_bytes())
        assert len(blocks) == 1
        paras = blocks[0].versions[0].paragraphs
        tables = [p for p in paras if p.css_class == "table"]
        assert len(tables) == 2

    def test_act_with_tables_markdown_format(self):
        blocks = self.parser.parse_text(ACT_WITH_TABLES.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        tables = [p for p in paras if p.css_class == "table"]
        first_table = tables[0].text
        lines = first_table.split("\n")
        # Valid pipe table: header | separator | rows
        assert lines[0].startswith("|") and lines[0].endswith("|")
        assert lines[1].startswith("| ---")
        assert len(lines) >= 3

    def test_act_with_tables_has_schedules(self):
        """Schedules outside <Body> should be parsed."""
        blocks = self.parser.parse_text(ACT_WITH_TABLES.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        schedule_headings = [p for p in paras if p.css_class == "titulo_tit"]
        assert len(schedule_headings) >= 2


# ─────────────────────────────────────────────
# Text parser — regulations
# ─────────────────────────────────────────────


class TestCATextParserRegulations:
    def setup_method(self):
        self.parser = CATextParser()

    def test_regulation_returns_one_block(self):
        blocks = self.parser.parse_text(REGULATION.read_bytes())
        assert len(blocks) == 1

    def test_regulation_has_sections(self):
        blocks = self.parser.parse_text(REGULATION.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        articles = [p for p in paras if p.css_class == "articulo"]
        assert len(articles) == 4  # SOR/99-129 has 4 sections

    def test_regulation_has_defined_terms(self):
        """Defined terms in regulations should be preserved."""
        blocks = self.parser.parse_text(REGULATION.read_bytes())
        paras = blocks[0].versions[0].paragraphs
        # DefinedTermEn should render as italic
        italic_paras = [p for p in paras if "*" in p.text]
        assert len(italic_paras) >= 1

    def test_regulation_with_tables(self):
        blocks = self.parser.parse_text(REGULATION_WITH_TABLES.read_bytes())
        assert len(blocks) == 1
        paras = blocks[0].versions[0].paragraphs
        tables = [p for p in paras if p.css_class == "table"]
        assert len(tables) >= 1


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


class TestCAMetadataParser:
    def setup_method(self):
        self.parser = CAMetadataParser()

    def test_act_metadata_core_fields(self):
        meta = self.parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        assert meta.country == "ca"
        assert meta.rank == "act"
        assert meta.status == NormStatus.IN_FORCE
        assert "Budget Implementation Act" in meta.title

    def test_act_metadata_identifier(self):
        meta = self.parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        assert meta.identifier == "B-9.8"

    def test_act_metadata_source_url(self):
        meta = self.parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        assert "laws-lois.justice.gc.ca" in meta.source
        assert "eng/acts/B-9.8" in meta.source

    def test_act_metadata_publication_date(self):
        meta = self.parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        assert isinstance(meta.publication_date, date)
        assert meta.publication_date == date(2003, 1, 1)

    def test_act_metadata_extra_fields(self):
        meta = self.parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        extra = dict(meta.extra)
        assert "last_amended" in extra
        assert "inforce_start" in extra
        assert "consolidation_date" in extra
        assert extra.get("lang") == "en"
        assert extra.get("bill_origin") == "commons"

    def test_regulation_metadata(self):
        meta = self.parser.parse(REGULATION.read_bytes(), "eng/regulations/SOR-99-129")
        assert meta.country == "ca"
        assert meta.rank == "regulation"
        assert meta.identifier == "SOR-99-129"
        # Regulations have enabling authority as department.
        assert "INSURANCE COMPANIES ACT" in meta.department

    def test_regulation_metadata_source_url(self):
        meta = self.parser.parse(REGULATION.read_bytes(), "eng/regulations/SOR-99-129")
        assert "eng/regulations/SOR-99-129" in meta.source

    def test_minimal_act_metadata(self):
        """Minimal act (no body) should still have valid metadata."""
        meta = self.parser.parse(ACT_MINIMAL.read_bytes(), "eng/acts/C-0.4")
        assert meta.title == "Canada Agricultural Products Act"
        assert meta.identifier == "C-0.4"
        assert meta.status == NormStatus.IN_FORCE
        extra = dict(meta.extra)
        assert extra.get("has_previous_version") == "true"


# ─────────────────────────────────────────────
# Filepath / slug
# ─────────────────────────────────────────────


class TestSlug:
    def test_act_filepath(self):
        parser = CAMetadataParser()
        meta = parser.parse(ACT_SMALL.read_bytes(), "eng/acts/B-9.8")
        filepath = norm_to_filepath(meta)
        assert filepath == "ca/B-9.8.md"

    def test_regulation_filepath(self):
        parser = CAMetadataParser()
        meta = parser.parse(REGULATION.read_bytes(), "eng/regulations/SOR-99-129")
        filepath = norm_to_filepath(meta)
        assert filepath == "ca/SOR-99-129.md"


# ─────────────────────────────────────────────
# Registry hookup
# ─────────────────────────────────────────────


class TestRegistryHookup:
    def test_text_parser_lookup(self):
        parser = get_text_parser("ca")
        assert isinstance(parser, CATextParser)

    def test_metadata_parser_lookup(self):
        parser = get_metadata_parser("ca")
        assert isinstance(parser, CAMetadataParser)
