"""Tests for the Finnish Finlex Akoma Ntoso parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from legalize.fetcher.fi.parser import FinlexMetadataParser, FinlexTextParser
from legalize.models import NormStatus
from legalize.transformer.markdown import render_norm_at_date


FIXTURES = Path(__file__).parent / "fixtures" / "fi"


def _read(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


# ─────────────────────────────────────────────
# Metadata — Constitution (731/1999)
# ─────────────────────────────────────────────


class TestMetadataConstitution:
    """Finnish Constitution — Suomen perustuslaki (731/1999)."""

    @pytest.fixture
    def metadata(self):
        return FinlexMetadataParser().parse(_read("sample-constitution.xml"), "1999/731")

    def test_identifier(self, metadata):
        assert metadata.identifier == "1999-731"

    def test_country(self, metadata):
        assert metadata.country == "fi"

    def test_title(self, metadata):
        assert metadata.title == "Suomen perustuslaki"

    def test_rank_is_act(self, metadata):
        assert metadata.rank == "laki"

    def test_publication_date(self, metadata):
        assert metadata.publication_date == date(1999, 6, 11)

    def test_status_in_force(self, metadata):
        assert metadata.status == NormStatus.IN_FORCE

    def test_department(self, metadata):
        assert metadata.department == "Oikeusministeriö"

    def test_source_url(self, metadata):
        assert "finlex.fi" in metadata.source
        assert "1999" in metadata.source

    def test_subjects_has_keywords(self, metadata):
        assert len(metadata.subjects) >= 1
        assert "Perustuslaki" in metadata.subjects

    def test_extra_has_eli(self, metadata):
        extra = dict(metadata.extra)
        assert "eli" in extra
        assert "731" in extra["eli"]

    def test_extra_has_entry_into_force(self, metadata):
        extra = dict(metadata.extra)
        assert extra["entry_into_force"] == "2000-03-01"

    def test_extra_has_citation(self, metadata):
        extra = dict(metadata.extra)
        assert extra["citation"] == "731/1999"

    def test_extra_has_amendments_count(self, metadata):
        extra = dict(metadata.extra)
        assert int(extra["amendments_count"]) == 4


# ─────────────────────────────────────────────
# Metadata — Regulation (decree, 51/2025)
# ─────────────────────────────────────────────


class TestMetadataRegulation:
    """Finnish decree — Valtioneuvoston asetus (51/2025)."""

    @pytest.fixture
    def metadata(self):
        return FinlexMetadataParser().parse(_read("sample-regulation.xml"), "2025/51")

    def test_identifier(self, metadata):
        assert metadata.identifier == "2025-51"

    def test_rank_is_decree(self, metadata):
        assert metadata.rank == "asetus"

    def test_department(self, metadata):
        assert "metsätalousministeriö" in metadata.department


# ─────────────────────────────────────────────
# Metadata — Ordinary law (224/2024)
# ─────────────────────────────────────────────


class TestMetadataOrdinaryLaw:
    """Ordinary Finnish act (224/2024)."""

    @pytest.fixture
    def metadata(self):
        return FinlexMetadataParser().parse(_read("sample-ordinary-law.xml"), "2024/224")

    def test_identifier(self, metadata):
        assert metadata.identifier == "2024-224"

    def test_rank_is_act(self, metadata):
        assert metadata.rank == "laki"

    def test_status_in_force(self, metadata):
        assert metadata.status == NormStatus.IN_FORCE


# ─────────────────────────────────────────────
# Metadata — Income Tax Act (1535/1992, with tables)
# ─────────────────────────────────────────────


class TestMetadataIncomeTax:
    """Income Tax Act — Tuloverolaki (1535/1992)."""

    @pytest.fixture
    def metadata(self):
        return FinlexMetadataParser().parse(_read("sample-with-tables.xml"), "1992/1535")

    def test_identifier(self, metadata):
        assert metadata.identifier == "1992-1535"

    def test_rank_is_act(self, metadata):
        assert metadata.rank == "laki"

    def test_extra_has_many_amendments(self, metadata):
        extra = dict(metadata.extra)
        assert int(extra["amendments_count"]) > 200


# ─────────────────────────────────────────────
# Text parser — Constitution
# ─────────────────────────────────────────────


class TestTextConstitution:
    """Text parsing of the Finnish Constitution."""

    @pytest.fixture
    def blocks(self):
        return FinlexTextParser().parse_text(_read("sample-constitution.xml"))

    def test_has_blocks(self, blocks):
        assert len(blocks) > 100

    def test_first_block_is_preamble(self, blocks):
        assert blocks[0].block_type == "preamble"
        assert blocks[0].versions[0].paragraphs[0].text
        assert "Eduskunnan" in blocks[0].versions[0].paragraphs[0].text

    def test_has_chapters(self, blocks):
        chapters = [b for b in blocks if b.block_type == "chapter"]
        assert len(chapters) == 13  # Constitution has 13 chapters

    def test_has_articles(self, blocks):
        articles = [b for b in blocks if b.block_type == "article"]
        assert len(articles) == 131  # 131 sections (§)

    def test_first_article_is_section_1(self, blocks):
        articles = [b for b in blocks if b.block_type == "article"]
        first = articles[0]
        paragraphs_text = " ".join(p.text for p in first.versions[0].paragraphs)
        assert "Valtiosääntö" in paragraphs_text
        assert "tasavalta" in paragraphs_text


# ─────────────────────────────────────────────
# Text parser — Income Tax Act (tables)
# ─────────────────────────────────────────────


class TestTextIncomeTax:
    """Text parsing of the Income Tax Act (with tables)."""

    @pytest.fixture
    def blocks(self):
        return FinlexTextParser().parse_text(_read("sample-with-tables.xml"))

    def test_has_parts(self, blocks):
        parts = [b for b in blocks if b.block_type == "part"]
        assert len(parts) >= 5  # At least 5 parts (OSA)

    def test_has_tables_in_paragraphs(self, blocks):
        table_paragraphs = []
        for b in blocks:
            for v in b.versions:
                for p in v.paragraphs:
                    if p.css_class == "table_row":
                        table_paragraphs.append(p)
        assert len(table_paragraphs) >= 1


# ─────────────────────────────────────────────
# Text parser — Regulation (annexes with tables)
# ─────────────────────────────────────────────


class TestTextRegulation:
    """Text parsing of the regulation with annex table."""

    @pytest.fixture
    def blocks(self):
        return FinlexTextParser().parse_text(_read("sample-regulation.xml"))

    def test_has_annex(self, blocks):
        annexes = [b for b in blocks if b.block_type == "annex"]
        assert len(annexes) >= 1

    def test_annex_has_table(self, blocks):
        table_found = False
        for b in blocks:
            for v in b.versions:
                for p in v.paragraphs:
                    if p.css_class == "table_row" and "|" in p.text:
                        table_found = True
        assert table_found, "Annex table not found"


# ─────────────────────────────────────────────
# Reforms
# ─────────────────────────────────────────────


class TestReforms:
    """Reform extraction from finlex:amendedBy metadata."""

    def test_constitution_has_4_reforms(self):
        reforms = FinlexTextParser().extract_reforms(_read("sample-constitution.xml"))
        assert len(reforms) == 4

    def test_reforms_are_sorted_chronologically(self):
        reforms = FinlexTextParser().extract_reforms(_read("sample-constitution.xml"))
        dates = [r.date for r in reforms]
        assert dates == sorted(dates)

    def test_income_tax_has_many_reforms(self):
        reforms = FinlexTextParser().extract_reforms(_read("sample-with-tables.xml"))
        assert len(reforms) > 200

    def test_regulation_has_no_reforms(self):
        reforms = FinlexTextParser().extract_reforms(_read("sample-regulation.xml"))
        assert len(reforms) == 0


# ─────────────────────────────────────────────
# Markdown rendering (integration)
# ─────────────────────────────────────────────


class TestMarkdownRendering:
    """End-to-end rendering of Finnish laws to Markdown."""

    def test_constitution_renders_with_frontmatter(self):
        data = _read("sample-constitution.xml")
        blocks = FinlexTextParser().parse_text(data)
        meta = FinlexMetadataParser().parse(data, "1999/731")
        md = render_norm_at_date(meta, blocks, date.today(), include_all=True)

        assert md.startswith("---\n")
        assert 'identifier: "1999-731"' in md
        assert 'country: "fi"' in md
        assert "# Suomen perustuslaki" in md
        assert "### 1 luku" in md
        assert "##### 1 §" in md
        assert "tasavalta" in md

    def test_regulation_renders_pipe_table(self):
        data = _read("sample-regulation.xml")
        blocks = FinlexTextParser().parse_text(data)
        meta = FinlexMetadataParser().parse(data, "2025/51")
        md = render_norm_at_date(meta, blocks, date.today(), include_all=True)

        assert "| Kuljetusmatka" in md
        assert "| ---" in md
        assert "17,46" in md

    def test_ordinary_law_renders_chapters_and_sections(self):
        data = _read("sample-ordinary-law.xml")
        blocks = FinlexTextParser().parse_text(data)
        meta = FinlexMetadataParser().parse(data, "2024/224")
        md = render_norm_at_date(meta, blocks, date.today(), include_all=True)

        assert "###" in md  # chapters
        assert "#####" in md  # sections

    def test_no_xml_tags_in_output(self):
        """Verify no leftover XML/HTML tags in any fixture."""
        import re

        for fixture in FIXTURES.glob("*.xml"):
            data = fixture.read_bytes()
            blocks = FinlexTextParser().parse_text(data)
            meta = FinlexMetadataParser().parse(data, "test/1")
            md = render_norm_at_date(meta, blocks, date.today(), include_all=True)

            # Skip frontmatter (contains URLs with angle brackets)
            body = md.split("---", 2)[-1] if "---" in md else md
            # Check for XML/HTML tags (allowing markdown links [text](url))
            tags = re.findall(r"<(?!http)[a-zA-Z][^>]*>", body)
            assert not tags, f"{fixture.name}: leftover tags: {tags[:5]}"

    def test_no_mojibake(self):
        """Verify no encoding artifacts in any fixture."""
        for fixture in FIXTURES.glob("*.xml"):
            data = fixture.read_bytes()
            blocks = FinlexTextParser().parse_text(data)
            meta = FinlexMetadataParser().parse(data, "test/1")
            md = render_norm_at_date(meta, blocks, date.today(), include_all=True)

            # Finnish special chars should be preserved
            assert "Ã¤" not in md, f"{fixture.name}: mojibake detected (Ã¤)"
            assert "Ã¶" not in md, f"{fixture.name}: mojibake detected (Ã¶)"
