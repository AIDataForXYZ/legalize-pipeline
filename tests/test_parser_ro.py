"""Tests for Romanian parser (legislatie.just.ro)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.ro.parser import (
    RoMetadataParser,
    RoTextParser,
    _clean_text,
    _normalize_href,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ro"


# ── Helpers ──────────────────────────────────────────────


def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ── Registry dispatch ────────────────────────────────────


class TestCountryDispatch:
    def test_text_parser_registry(self):
        parser = get_text_parser("ro")
        assert isinstance(parser, RoTextParser)

    def test_metadata_parser_registry(self):
        parser = get_metadata_parser("ro")
        assert isinstance(parser, RoMetadataParser)


# ── Constitution ─────────────────────────────────────────


class TestConstitution:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.parser = RoTextParser()
        self.blocks = self.parser.parse_text(_read("sample-constitution.html"))

    def test_block_count(self):
        assert len(self.blocks) > 150

    def test_article_count(self):
        articles = [b for b in self.blocks if b.block_type == "article"]
        assert len(articles) == 156

    def test_title_count(self):
        titles = [b for b in self.blocks if b.block_type == "title"]
        assert len(titles) == 8

    def test_chapter_count(self):
        chapters = [b for b in self.blocks if b.block_type == "chapter"]
        assert len(chapters) == 10

    def test_section_count(self):
        sections = [b for b in self.blocks if b.block_type == "section"]
        assert len(sections) == 8

    def test_first_article_text(self):
        articles = [b for b in self.blocks if b.block_type == "article"]
        art1 = articles[0]
        assert art1.title == "Articolul 1"
        paras = art1.versions[0].paragraphs
        # Article heading
        assert paras[0].css_class == "articulo"
        # Article body text
        body_texts = [p.text for p in paras if p.css_class == "parrafo"]
        assert any("România este stat național" in t for t in body_texts)

    def test_no_html_tags_in_output(self):
        for block in self.blocks:
            for v in block.versions:
                for p in v.paragraphs:
                    assert "<span" not in p.text, f"HTML tag in: {p.text[:80]}"
                    assert "<div" not in p.text, f"HTML tag in: {p.text[:80]}"

    def test_no_control_chars(self):
        ctrl_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
        for block in self.blocks:
            for v in block.versions:
                for p in v.paragraphs:
                    assert not ctrl_re.search(p.text), f"Control char in: {p.text[:80]}"

    def test_romanian_diacritics_preserved(self):
        all_text = " ".join(p.text for b in self.blocks for v in b.versions for p in v.paragraphs)
        assert "ă" in all_text or "ș" in all_text or "ț" in all_text


# ── Companies Law ────────────────────────────────────────


class TestCompaniesLaw:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.parser = RoTextParser()
        self.blocks = self.parser.parse_text(_read("sample-ordinary-law.html"))

    def test_article_count(self):
        articles = [b for b in self.blocks if b.block_type == "article"]
        assert len(articles) == 397

    def test_alineat_format(self):
        """Alneats (numbered paragraphs) should be preserved as (1), (2), etc."""
        articles = [b for b in self.blocks if b.block_type == "article"]
        art1 = articles[0]
        body_texts = [p.text for p in art1.versions[0].paragraphs if p.css_class == "parrafo"]
        has_alineat = any(t.startswith("(1)") or t.startswith("(2)") for t in body_texts)
        assert has_alineat, "Alneats not preserved"


# ── Tax Code (tables) ───────────────────────────────────


class TestTaxCode:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.parser = RoTextParser()
        self.blocks = self.parser.parse_text(_read("sample-with-tables.html"))

    def test_article_count(self):
        articles = [b for b in self.blocks if b.block_type == "article"]
        assert len(articles) >= 90  # Fixture trimmed to ~100 articles

    def test_has_tables(self):
        table_blocks = [b for b in self.blocks if b.block_type == "table"]
        assert len(table_blocks) >= 1

    def test_table_is_pipe_format(self):
        table_blocks = [b for b in self.blocks if b.block_type == "table"]
        if table_blocks:
            text = table_blocks[0].versions[0].paragraphs[0].text
            assert text.startswith("| "), "Table should start with pipe"
            assert "---" in text, "Table should have separator row"


# ── Regulation (HG 611/2008) ─────────────────────────────


class TestRegulation:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.parser = RoTextParser()
        self.blocks = self.parser.parse_text(_read("sample-regulation.html"))

    def test_article_count(self):
        articles = [b for b in self.blocks if b.block_type == "article"]
        assert len(articles) == 180

    def test_has_tables(self):
        table_blocks = [b for b in self.blocks if b.block_type == "table"]
        assert len(table_blocks) >= 10


# ── Metadata ─────────────────────────────────────────────


class TestMetadata:
    def test_companies_law_metadata(self):
        parser = RoMetadataParser()
        meta = parser.parse(_read("sample-detail-versions.html"), "798")
        assert meta.identifier == "RO-798"
        assert meta.country == "ro"
        assert meta.rank == "lege"
        assert meta.department == "PARLAMENTUL"
        assert meta.publication_date == date(2004, 11, 17)
        assert "MONITORUL OFICIAL" in dict(meta.extra).get("publication_reference", "")

    def test_identifier_filesystem_safe(self):
        parser = RoMetadataParser()
        meta = parser.parse(_read("sample-detail-versions.html"), "798")
        assert ":" not in meta.identifier
        assert " " not in meta.identifier
        assert "/" not in meta.identifier


# ── Utility functions ────────────────────────────────────


class TestUtilities:
    def test_clean_text_strips_controls(self):
        assert _clean_text("hello\x00world") == "helloworld"
        assert _clean_text("no\x0bbreak") == "nobreak"

    def test_clean_text_normalizes_whitespace(self):
        assert _clean_text("  multiple   spaces  ") == "multiple spaces"

    def test_clean_text_replaces_nbsp(self):
        assert _clean_text("hello\xa0world") == "hello world"

    def test_normalize_href_relative(self):
        href = "~/../../../Public/DetaliiDocumentAfis/47355"
        result = _normalize_href(href)
        assert result == "https://legislatie.just.ro/Public/DetaliiDocumentAfis/47355"

    def test_normalize_href_double_dot(self):
        href = "../../Public/DetaliiDocument/1234"
        result = _normalize_href(href)
        assert result == "https://legislatie.just.ro/Public/DetaliiDocument/1234"

    def test_normalize_href_absolute(self):
        href = "https://example.com/page"
        result = _normalize_href(href)
        assert result == "https://example.com/page"
