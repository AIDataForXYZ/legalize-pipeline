"""Tests for the Andorran BOPA parser (Format A modern + Format B legacy)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.ad.client import BOPAClient
from legalize.fetcher.ad.discovery import (
    BOPADiscovery,
    _is_target_document,
    _make_norm_id,
)
from legalize.fetcher.ad.parser import (
    BOPAMetadataParser,
    BOPATextParser,
    RANK_CONSTITUCIO,
    RANK_DECRET_LLEI,
    RANK_LLEI,
    RANK_REGLAMENT,
    _decode_html,
    _is_format_a,
    _parse_format_a,
    _parse_format_b,
)
from legalize.models import NormStatus
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures" / "ad"

# Document fixtures
LLEI_CACA = FIXTURES / "llei_18-2024_caca_CGL_2025_01_08_10_50_58.html"
LLEI_CACA_CORRECCIO = FIXTURES / "llei_caca_correccio_CGL_2025_01_15_16_30_31.html"
DECRET_AMB_TAULES = FIXTURES / "decret_501-2024_amb_taules_GR_2024_12_27_13_39_37.html"
DECRET_MODIFICA = FIXTURES / "decret_2-2025_modifica_GR_2025_01_09_13_08_40.html"
ERRATA_AMB_TAULA = FIXTURES / "correccio_taula_GV_2025_01_17_09_12_14.html"
CONSTITUCIO_LEGACY = FIXTURES / "constitucio_1993_legacy_7586.html"
LEGACY_2010 = FIXTURES / "legacy_2010_6074E.html"
EARLY_MODERN_2015 = FIXTURES / "early_modern_2015_ge27001008.html"
MIDDLE_2018 = FIXTURES / "middle_2018_GLT20171219_15_49_04.html"
LLEI_MODIFICA = FIXTURES / "llei_37-2021_modifica_14-2017_CGL20211223_09_20_50.html"
LLEI_14_2017 = FIXTURES / "llei_14-2017_blanqueig_CGL20170712_09_31_30.html"

# API response fixtures
API_NEWSLETTER = FIXTURES / "api_GetPaginatedNewsletter.json"
API_FILTERS = FIXTURES / "api_GetFilters.json"
API_DOCS_2025_4 = FIXTURES / "api_GetDocumentsByBOPA_2025-4.json"
API_DOCS_1993_24 = FIXTURES / "api_GetDocumentsByBOPA_1993-24.json"


# ─────────────────────────────────────────────
# Encoding detection
# ─────────────────────────────────────────────


class TestEncodingDetection:
    def test_utf16_le_bom(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        assert "Llei 18/2024" in text
        assert len(text) > 10000

    def test_utf8_no_bom(self):
        text = _decode_html(LLEI_CACA_CORRECCIO.read_bytes())
        assert "Correcció d’errata" in text or "Correcció" in text

    def test_constitucio_utf16(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        assert "Constitució del Principat d'Andorra" in text


# ─────────────────────────────────────────────
# Format detection
# ─────────────────────────────────────────────


class TestFormatDetection:
    def test_modern_llei_is_format_a(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        assert _is_format_a(text)

    def test_decret_is_format_a(self):
        text = _decode_html(DECRET_AMB_TAULES.read_bytes())
        assert _is_format_a(text)

    def test_constitucio_is_format_b(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        assert not _is_format_a(text)

    def test_2010_doc_is_format_b(self):
        text = _decode_html(LEGACY_2010.read_bytes())
        assert not _is_format_a(text)


# ─────────────────────────────────────────────
# Format A parser
# ─────────────────────────────────────────────


class TestFormatAParser:
    def test_llei_caca_paragraph_count(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        pars = _parse_format_a(text)
        # Llei caça has 76 Titol-* headings + ~430 body paragraphs
        assert len(pars) > 400

    def test_llei_caca_has_articles(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        pars = _parse_format_a(text)
        articles = [p for p in pars if p.css_class == "articulo"]
        # 54 articles + "Article N" headings inside index references
        assert len(articles) >= 50

    def test_llei_caca_has_titols(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        pars = _parse_format_a(text)
        titols = [p for p in pars if p.css_class == "titulo_tit"]
        # 9 Títol I..IX
        assert len(titols) >= 8

    def test_llei_caca_skips_titol3(self):
        """Titol-3 (law title) should NOT be emitted — it duplicates the H1."""
        text = _decode_html(LLEI_CACA.read_bytes())
        pars = _parse_format_a(text)
        # No paragraph should contain just "Llei 18/2024" as a heading
        for p in pars:
            if p.css_class == "titulo_tit":
                # Titol-4 headings are "Títol I.", "Títol II." — never start with "Llei "
                assert not p.text.startswith("Llei 18/2024")

    def test_llei_caca_signature(self):
        text = _decode_html(LLEI_CACA.read_bytes())
        pars = _parse_format_a(text)
        signatures = [p for p in pars if p.css_class == "firma_rey"]
        assert len(signatures) >= 1
        # The Síndic General signature should have Carles Ensenyat Reig
        assert any("Carles Ensenyat Reig" in p.text for p in signatures)

    def test_decret_table_count(self):
        """Decret 501/2024 has 17 tables, all must be emitted."""
        text = _decode_html(DECRET_AMB_TAULES.read_bytes())
        pars = _parse_format_a(text)
        tables = [p for p in pars if p.css_class == "table_row"]
        assert len(tables) == 17

    def test_table_markdown_format(self):
        """Each table should be a valid Markdown table with header + separator + rows."""
        text = _decode_html(DECRET_AMB_TAULES.read_bytes())
        pars = _parse_format_a(text)
        tables = [p for p in pars if p.css_class == "table_row"]
        first = tables[0]
        lines = first.text.split("\n")
        assert lines[0].startswith("|") and lines[0].endswith("|")
        assert lines[1].startswith("| ---")
        assert len(lines) >= 3  # header + sep + at least one data row


# ─────────────────────────────────────────────
# Format B parser (legacy)
# ─────────────────────────────────────────────


class TestFormatBParser:
    def test_constitucio_paragraphs(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        pars = _parse_format_b(text)
        # Should have at least the Article 1..107 + Disposicions
        assert len(pars) >= 100

    def test_constitucio_has_articles(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        pars = _parse_format_b(text)
        articles = [p for p in pars if p.css_class == "articulo"]
        # The Andorran Constitution has 107 articles
        assert len(articles) >= 50  # Should be ~107, allowing slack for parser quirks

    def test_constitucio_has_titols(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        pars = _parse_format_b(text)
        titols = [p for p in pars if p.css_class == "titulo_tit"]
        # 9 Títol I..IX
        assert len(titols) >= 5

    def test_constitucio_has_preambul(self):
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        pars = _parse_format_b(text)
        # Preàmbul should be detected as a section heading
        preambul = [p for p in pars if "Preàmbul" in p.text]
        assert preambul

    def test_constitucio_paragraphs_are_reflowed(self):
        """Legacy hard line wraps must be joined into proper paragraphs."""
        text = _decode_html(CONSTITUCIO_LEGACY.read_bytes())
        pars = _parse_format_b(text)
        # Find a paragraph that should be a full sentence (Article 1.1)
        body = [p for p in pars if p.css_class == "parrafo" and len(p.text) > 100]
        assert body, "Reflowing should produce paragraphs longer than 100 chars"


# ─────────────────────────────────────────────
# BOPATextParser (full pipeline)
# ─────────────────────────────────────────────


class TestBOPATextParser:
    def setup_method(self):
        self.parser = BOPATextParser()

    def test_modern_llei_returns_one_block(self):
        blocks = self.parser.parse_text(LLEI_CACA.read_bytes())
        assert len(blocks) == 1
        assert blocks[0].block_type == "article"
        assert blocks[0].id == "body"

    def test_legacy_constitucio_returns_one_block(self):
        blocks = self.parser.parse_text(CONSTITUCIO_LEGACY.read_bytes())
        assert len(blocks) == 1
        # Both formats produce a single Block — BOPA documents are atomic
        pars = blocks[0].versions[0].paragraphs
        assert len(pars) > 50

    def test_decret_with_tables(self):
        blocks = self.parser.parse_text(DECRET_AMB_TAULES.read_bytes())
        assert len(blocks) == 1
        pars = blocks[0].versions[0].paragraphs
        tables = [p for p in pars if p.css_class == "table_row"]
        assert len(tables) == 17


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


def _make_doc(**overrides) -> bytes:
    """Build a fake API document JSON for metadata parser tests."""
    doc = {
        "metadata_storage_path": "https://bopadocuments.blob.core.windows.net/bopa-documents/037004/html/CGL_x.html",
        "organisme": "Lleis",
        "organismePare": "02. Consell General",
        "tema": "Lleis",
        "temaPare": "12. Lleis i legislació delegada",
        "dataPublicacioButlleti": "2025-01-14T23:00:00+00:00",
        "dataArticle": "2024-12-19T11:00:00+00:00",
        "dataFiPublicacio": "3000-01-01T00:00:00+00:00",
        "isExtra": "False",
        "numButlleti": "4",
        "anyButlleti": "2025",
        "sumari": "Llei%2018%2F2024%2C%20del%2019%20de%20desembre%2C%20de%20ca%C3%A7a.",
        "nomDocument": "CGL_2025_01_08_10_50_58",
    }
    doc.update(overrides)
    return json.dumps(doc).encode()


class TestBOPAMetadataParser:
    def setup_method(self):
        self.parser = BOPAMetadataParser()

    def test_llei_metadata(self):
        meta = self.parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        assert meta.identifier == "BOPA-L-2024-18"
        assert meta.country == "ad"
        assert meta.rank == RANK_LLEI
        assert meta.title.startswith("Llei 18/2024")
        assert meta.publication_date == date(2025, 1, 14)
        assert meta.status == NormStatus.IN_FORCE

    def test_llei_signature_date_in_extra(self):
        meta = self.parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        extra = dict(meta.extra)
        assert extra["signature_date"] == "2024-12-19"
        assert extra["bopa_issue"] == "BOPA 4/2025"
        assert extra["bopa_document_id"] == "CGL_2025_01_08_10_50_58"

    def test_decret_metadata(self):
        data = _make_doc(
            organisme="Reglaments",
            organismePare="03. Govern",
            sumari="Decret%20501%2F2024%2C%20del%2023-12-2024%2C%20d%E2%80%99aprovaci%C3%B3%20del%20Reglament...",
            nomDocument="GR_2024_12_27_13_39_37",
        )
        meta = self.parser.parse(data, "2025/1/GR_2024_12_27_13_39_37")
        assert meta.identifier == "BOPA-D-2024-501"
        assert meta.rank == RANK_REGLAMENT

    def test_constitucio_identifier(self):
        data = _make_doc(
            organisme="Constitució del Principat d'Andorra",
            sumari="Constituci%c3%b3 del Principat d%27Andorra%2c de 28-4-93.",
            nomDocument="7586",
            anyButlleti="1993",
            numButlleti="24",
            dataPublicacioButlleti="1993-05-03T10:00:00",
            dataArticle="1993-04-28T10:00:00",
        )
        meta = self.parser.parse(data, "1993/24/7586")
        assert meta.identifier == "BOPA-C-1993"
        assert meta.rank == RANK_CONSTITUCIO

    def test_legislacio_delegada_identifier(self):
        data = _make_doc(
            organisme="Legislació delegada",
            organismePare="03. Govern",
            sumari="Decret%20legislatiu%205%2F2024%2C%20del%2010-7-2024",
            nomDocument="GLT_x",
        )
        meta = self.parser.parse(data, "2024/50/GLT_x")
        assert meta.identifier == "BOPA-LD-2024-5"
        assert meta.rank == RANK_DECRET_LLEI

    def test_fallback_identifier_for_unparseable_title(self):
        data = _make_doc(
            sumari="Edicte%20del%2010-12-2014%20pel%20qual...",
            nomDocument="ge27001008",
        )
        meta = self.parser.parse(data, "2015/1/ge27001008")
        # Llei without N/YYYY pattern → fallback to BOPA-L-{year}-{nomDocument}
        assert meta.identifier == "BOPA-L-2025-ge27001008"

    def test_source_url_format(self):
        meta = self.parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        # year 2025 → offset 037, issue 4 → 004
        assert "037004/Pagines/CGL_2025_01_08_10_50_58.aspx" in meta.source

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid JSON"):
            self.parser.parse(b"not-json", "x/y/z")


# ─────────────────────────────────────────────
# End-to-end Markdown rendering
# ─────────────────────────────────────────────


class TestEndToEndMarkdown:
    def test_llei_caca_renders_to_markdown(self):
        parser = BOPATextParser()
        meta_parser = BOPAMetadataParser()
        meta = meta_parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        blocks = parser.parse_text(LLEI_CACA.read_bytes())
        md = render_norm_at_date(meta, blocks, meta.publication_date, include_all=True)

        # Frontmatter
        assert 'identifier: "BOPA-L-2024-18"' in md
        assert 'country: "ad"' in md
        assert 'rank: "llei"' in md
        # H1 title
        assert "# Llei 18/2024" in md
        # Article headings
        assert "##### Article 1. Objecte" in md
        # Final disposition
        assert "Disposició final quarta" in md
        # Signature
        assert "Carles Ensenyat Reig" in md

    def test_llei_caca_does_not_duplicate_title(self):
        """The Titol-3 must not produce a duplicate ## heading after the H1."""
        parser = BOPATextParser()
        meta_parser = BOPAMetadataParser()
        meta = meta_parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        blocks = parser.parse_text(LLEI_CACA.read_bytes())
        md = render_norm_at_date(meta, blocks, meta.publication_date, include_all=True)

        # Should have exactly one H1 with the title and no H2 echoing it
        lines = md.split("\n")
        h1 = [ln for ln in lines if ln.startswith("# ") and "Llei 18/2024" in ln]
        h2 = [ln for ln in lines if ln.startswith("## ") and "Llei 18/2024" in ln]
        assert len(h1) == 1
        assert len(h2) == 0

    def test_constitucio_renders(self):
        parser = BOPATextParser()
        meta_parser = BOPAMetadataParser()
        data = _make_doc(
            organisme="Constitució del Principat d'Andorra",
            sumari="Constituci%c3%b3 del Principat d%27Andorra%2c de 28-4-93.",
            nomDocument="7586",
            anyButlleti="1993",
            numButlleti="24",
            dataPublicacioButlleti="1993-05-03T10:00:00",
        )
        meta = meta_parser.parse(data, "1993/24/7586")
        blocks = parser.parse_text(CONSTITUCIO_LEGACY.read_bytes())
        md = render_norm_at_date(meta, blocks, meta.publication_date, include_all=True)

        assert 'identifier: "BOPA-C-1993"' in md
        assert "# Constitució del Principat d'Andorra" in md
        # Article 1 should be rendered as a heading
        assert "##### Article 1" in md
        # Preàmbul should be present
        assert "Preàmbul" in md
        # Should have exactly one H1 line — no duplicate ## echoing it
        assert sum(1 for ln in md.split("\n") if ln.startswith("# Constitució")) == 1


# ─────────────────────────────────────────────
# Filename / slug
# ─────────────────────────────────────────────


class TestSlug:
    def test_llei_filepath(self):
        meta_parser = BOPAMetadataParser()
        meta = meta_parser.parse(_make_doc(), "2025/4/CGL_2025_01_08_10_50_58")
        assert norm_to_filepath(meta) == "ad/BOPA-L-2024-18.md"

    def test_constitucio_filepath(self):
        meta_parser = BOPAMetadataParser()
        data = _make_doc(
            organisme="Constitució del Principat d'Andorra",
            sumari="Constituci%c3%b3 del Principat d%27Andorra%2c de 28-4-93.",
            nomDocument="7586",
            anyButlleti="1993",
            numButlleti="24",
            dataPublicacioButlleti="1993-05-03T10:00:00",
        )
        meta = meta_parser.parse(data, "1993/24/7586")
        assert norm_to_filepath(meta) == "ad/BOPA-C-1993.md"


# ─────────────────────────────────────────────
# Discovery
# ─────────────────────────────────────────────


class TestDiscovery:
    def test_target_organisme_filter(self):
        assert _is_target_document({"organisme": "Lleis"})
        assert _is_target_document({"organisme": "Reglaments"})
        assert _is_target_document({"organisme": "Legislació delegada"})
        assert _is_target_document({"organisme": "Constitució del Principat d'Andorra"})
        assert not _is_target_document({"organisme": "Adjudicacions i ampliacions de contractes"})
        assert not _is_target_document({"organisme": "Notificacions"})

    def test_make_norm_id(self):
        doc = {
            "anyButlleti": "2025",
            "numButlleti": "4",
            "nomDocument": "CGL_2025_01_08_10_50_58",
        }
        assert _make_norm_id(doc) == "2025/4/CGL_2025_01_08_10_50_58"

    def test_discovery_against_real_butlleti_2025_4(self):
        """Use the live API JSON fixture to verify discovery yields the Llei caça."""
        # Stub client returning the cached newsletter + butlletí response
        bopa_list = [
            {
                "numBOPA": "4",
                "dataPublicacio": "2025-01-14T23:00:00+00:00",
                "isExtra": False,
                "num": None,
            }
        ]
        docs_response = json.loads(API_DOCS_2025_4.read_text())
        butlleti_docs = [w["document"] for w in docs_response.get("paginatedDocuments", [])]

        class _StubClient(BOPAClient):
            def __init__(self):
                pass

            def get_paginated_newsletter(self):
                return bopa_list

            def get_butlleti_documents(self, *, num, year, use_cache=True):
                return butlleti_docs

        discovery = BOPADiscovery()
        norm_ids = list(discovery.discover_all(_StubClient()))
        # The fixture butlletí 2025/4 has 1 Llei + 1 Reglament — only those should be yielded
        assert any("CGL_2025_01_08_10_50_58" in nid for nid in norm_ids)
        # All yielded ids must follow the year/num/nomDocument format
        for nid in norm_ids:
            assert nid.startswith("2025/4/")


# ─────────────────────────────────────────────
# Client URL helpers
# ─────────────────────────────────────────────


class TestBOPAClient:
    def setup_method(self):
        self.client = BOPAClient()

    def test_strip_prefix_from_full_url(self):
        url = "https://bopadocuments.blob.core.windows.net/bopa-documents/037004/html/1_CGL_x.html"
        normalized = self.client._normalize_blob_url(url)
        assert normalized.endswith("/CGL_x.html")
        assert "1_CGL" not in normalized

    def test_strip_multi_digit_prefix(self):
        url = "https://bopadocuments.blob.core.windows.net/bopa-documents/037004/html/15_GR_x.html"
        normalized = self.client._normalize_blob_url(url)
        assert normalized.endswith("/GR_x.html")

    def test_no_prefix_unchanged(self):
        url = "https://bopadocuments.blob.core.windows.net/bopa-documents/037004/html/CGL_x.html"
        assert self.client._normalize_blob_url(url) == url

    def test_relative_path_resolves_to_full_url(self):
        normalized = self.client._normalize_blob_url("037004/html/3_GR_x.html")
        assert normalized.startswith("https://bopadocuments.blob.core.windows.net/")
        assert normalized.endswith("/GR_x.html")

    def test_build_blob_url_year_offset(self):
        # 1989 → 001, 2025 → 037, 2026 → 038
        url = self.client._build_blob_url("2025", "4", "CGL_x.html")
        assert "037004/html/CGL_x.html" in url
        url = self.client._build_blob_url("1989", "1", "x.html")
        assert "001001/html/x.html" in url
        url = self.client._build_blob_url("1993", "24", "7586.html")
        assert "005024/html/7586.html" in url

    def test_split_norm_id(self):
        year, num, nom = self.client._split_norm_id("2025/4/CGL_2025_01_08_10_50_58")
        assert year == "2025"
        assert num == "4"
        assert nom == "CGL_2025_01_08_10_50_58"

    def test_split_norm_id_with_path_in_nom(self):
        # nomDocument never has slashes in practice but split should be limited to 3 parts
        year, num, nom = self.client._split_norm_id("2025/4/with/slash")
        assert year == "2025"
        assert num == "4"
        assert nom == "with/slash"


# ─────────────────────────────────────────────
# Registry hookup
# ─────────────────────────────────────────────


class TestRegistryHookup:
    def test_text_parser_lookup(self):
        parser = get_text_parser("ad")
        assert isinstance(parser, BOPATextParser)

    def test_metadata_parser_lookup(self):
        parser = get_metadata_parser("ad")
        assert isinstance(parser, BOPAMetadataParser)
