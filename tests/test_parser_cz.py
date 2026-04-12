"""Tests for the Czech e-Sbírka fetcher (parser + metadata + client)."""

from __future__ import annotations

from pathlib import Path

import pytest

from legalize.countries import (
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)
from legalize.fetcher.cz.client import ESbirkaClient
from legalize.fetcher.cz.discovery import ESbirkaDiscovery
from legalize.fetcher.cz.parser import (
    ESbirkaMetadataParser,
    ESbirkaTextParser,
    _clean_text,
    _html_table_to_markdown,
    _parse_date,
    _stale_url_to_identifier,
)
from legalize.models import NormStatus, Rank

FIXTURES = Path(__file__).parent / "fixtures" / "cz"


# ─────────────────────────────────────────────
# Registry dispatch
# ─────────────────────────────────────────────


class TestCountryDispatch:
    def test_registry_has_cz(self):
        assert "cz" in supported_countries()

    def test_cz_text_parser_class(self):
        parser = get_text_parser("cz")
        assert isinstance(parser, ESbirkaTextParser)

    def test_cz_metadata_parser_class(self):
        parser = get_metadata_parser("cz")
        assert isinstance(parser, ESbirkaMetadataParser)

    def test_cz_client_class(self):
        cls = get_client_class("cz")
        assert cls is ESbirkaClient

    def test_cz_discovery_class(self):
        cls = get_discovery_class("cz")
        assert cls is ESbirkaDiscovery


# ─────────────────────────────────────────────
# Client URL encoding
# ─────────────────────────────────────────────


class TestESbirkaClient:
    def test_encode_url_simple(self):
        assert ESbirkaClient._encode_url("/sb/1993/1") == "%2Fsb%2F1993%2F1"

    def test_encode_url_with_date(self):
        encoded = ESbirkaClient._encode_url("/sb/1993/1/2026-01-01")
        assert "%2F2026-01-01" in encoded

    def test_encode_url_preserves_numbers(self):
        encoded = ESbirkaClient._encode_url("/sb/2009/40")
        assert "2009" in encoded
        assert "40" in encoded


# ─────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────


class TestHelpers:
    def test_clean_text_strips_var_tags(self):
        assert _clean_text("<var>Čl. 1</var>") == "Čl. 1"

    def test_clean_text_converts_em_to_italic(self):
        assert _clean_text("text <em>italic</em> end") == "text *italic* end"

    def test_clean_text_converts_strong_to_bold(self):
        assert _clean_text("text <strong>bold</strong> end") == "text **bold** end"

    def test_clean_text_strips_czechvoc(self):
        result = _clean_text('<czechvoc-termin koncept="foo">bar</czechvoc-termin>')
        assert result == "bar"

    def test_clean_text_strips_control_chars(self):
        assert _clean_text("hello\x00world\x7f") == "helloworld"

    def test_clean_text_normalizes_whitespace(self):
        assert _clean_text("  hello   world  ") == "hello world"

    def test_clean_text_bold_b_tag(self):
        assert _clean_text("<b>bold</b>") == "**bold**"

    def test_clean_text_italic_i_tag(self):
        assert _clean_text("<i>italic</i>") == "*italic*"

    def test_clean_text_subscript_stripped(self):
        assert _clean_text("CO<sub>2</sub>") == "CO2"

    def test_clean_text_underline_stripped(self):
        assert _clean_text("<u>16</u>") == "16"

    def test_clean_text_link_keeps_text(self):
        assert _clean_text('<a href="#" class="ext">Listina</a>') == "Listina"

    def test_clean_text_image_replaced(self):
        assert _clean_text('<img src="/foo"/>') == "[image omitted]"

    def test_clean_text_mathml_stripped(self):
        result = _clean_text("<math><mfrac><mi>α</mi><mi>β</mi></mfrac></math>")
        assert "α" in result
        assert "<" not in result

    def test_clean_text_br_to_space(self):
        assert _clean_text("line1<br/>line2") == "line1 line2"

    def test_clean_text_empty(self):
        assert _clean_text("") == ""
        assert _clean_text(None) == ""

    def test_parse_date_iso(self):
        assert _parse_date("2024-01-15") is not None
        assert _parse_date("2024-01-15").isoformat() == "2024-01-15"

    def test_parse_date_datetime(self):
        d = _parse_date("1992-12-28T00:00:00.000+01:00")
        assert d is not None
        assert d.isoformat() == "1992-12-28"

    def test_parse_date_none(self):
        assert _parse_date(None) is None
        assert _parse_date("") is None

    def test_stale_url_to_identifier(self):
        assert _stale_url_to_identifier("/sb/1993/1") == "SB-1993-1"
        assert _stale_url_to_identifier("/sb/2009/40") == "SB-2009-40"
        assert _stale_url_to_identifier("/sm/2004/100") == "SM-2004-100"


# ─────────────────────────────────────────────
# Metadata parser — Constitution
# ─────────────────────────────────────────────


class TestMetadataParserConstitution:
    @pytest.fixture()
    def meta(self):
        data = (FIXTURES / "sample-constitution-meta.json").read_bytes()
        return ESbirkaMetadataParser().parse(data, "/sb/1993/1")

    def test_title(self, meta):
        assert meta.title == "Ústava České republiky"

    def test_identifier(self, meta):
        assert meta.identifier == "SB-1993-1"

    def test_country(self, meta):
        assert meta.country == "cz"

    def test_rank(self, meta):
        assert meta.rank == Rank("constitutional_law")

    def test_publication_date(self, meta):
        assert meta.publication_date.isoformat() == "1992-12-28"

    def test_status_in_force(self, meta):
        assert meta.status == NormStatus.IN_FORCE

    def test_source_url(self, meta):
        assert "e-sbirka.gov.cz" in meta.source
        assert "/eli/" in meta.source

    def test_last_modified(self, meta):
        assert meta.last_modified is not None
        assert meta.last_modified.year >= 2024

    def test_extra_has_official_code(self, meta):
        extra_dict = dict(meta.extra)
        assert extra_dict["official_code"] == "1/1993 Sb."

    def test_extra_has_eli(self, meta):
        extra_dict = dict(meta.extra)
        assert "/eli/cz/sb/1993/1" in extra_dict["eli"]

    def test_extra_has_collection_code(self, meta):
        extra_dict = dict(meta.extra)
        assert extra_dict["collection_code"] == "sb"

    def test_identifier_is_filesystem_safe(self, meta):
        assert ":" not in meta.identifier
        assert " " not in meta.identifier
        assert "/" not in meta.identifier

    def test_extra_has_boolean_fields(self, meta):
        extra_dict = dict(meta.extra)
        for key in (
            "is_international_treaty",
            "has_editorial_correction",
            "never_effective",
            "provisions_never_effective",
            "has_explanatory_report",
            "inactive_temporal_version",
        ):
            assert key in extra_dict, f"Missing boolean field: {key}"
            assert extra_dict[key] in ("true", "false")

    def test_extra_has_full_citation_with_amendments(self, meta):
        extra_dict = dict(meta.extra)
        cit = extra_dict.get("full_citation_with_amendments", "")
        assert "347/1997 Sb." in cit
        assert "87/2024 Sb." in cit
        # Not truncated (used to be capped at 500 chars)
        assert not cit.endswith("...")

    def test_short_title_populated(self, meta):
        assert meta.short_title
        assert "1/1993" in meta.short_title


# ─────────────────────────────────────────────
# Metadata parser — Criminal Code
# ─────────────────────────────────────────────


class TestMetadataParserCriminalCode:
    @pytest.fixture()
    def meta(self):
        data = (FIXTURES / "sample-criminal-code-meta.json").read_bytes()
        return ESbirkaMetadataParser().parse(data, "/sb/2009/40")

    def test_title(self, meta):
        assert "trestní" in meta.title.lower() or "Trestní" in meta.title

    def test_identifier(self, meta):
        assert meta.identifier == "SB-2009-40"

    def test_rank_is_law(self, meta):
        assert meta.rank == Rank("law")

    def test_has_amendments(self, meta):
        extra_dict = dict(meta.extra)
        assert "amendments" in extra_dict


# ─────────────────────────────────────────────
# Metadata parser — Regulation (amending law)
# ─────────────────────────────────────────────


class TestMetadataParserRegulation:
    @pytest.fixture()
    def meta(self):
        data = (FIXTURES / "sample-regulation-meta.json").read_bytes()
        return ESbirkaMetadataParser().parse(data, "/sb/2024/1")

    def test_identifier(self, meta):
        assert meta.identifier == "SB-2024-1"

    def test_country(self, meta):
        assert meta.country == "cz"

    def test_rank_is_law(self, meta):
        # This is actually a "čistá novela" (pure amendment) but
        # the template SABL_CISTA_NOVELA_ZAKON contains ZAKON → law
        assert meta.rank == Rank("law")


# ─────────────────────────────────────────────
# Text parser — Constitution fragments
# ─────────────────────────────────────────────


class TestTextParserConstitution:
    @pytest.fixture()
    def blocks(self):
        data = (FIXTURES / "sample-constitution-fragments.json").read_bytes()
        return ESbirkaTextParser().parse_text(data)

    def test_returns_blocks(self, blocks):
        assert len(blocks) >= 1

    def test_block_has_versions(self, blocks):
        assert len(blocks[0].versions) == 1

    def test_paragraphs_not_empty(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        assert len(paragraphs) > 400  # Constitution has ~453 paragraphs

    def test_no_html_tags_in_text(self, blocks):
        for p in blocks[0].versions[0].paragraphs:
            assert "<var>" not in p.text
            assert "<em>" not in p.text
            assert "</" not in p.text

    def test_no_control_chars(self, blocks):
        import re

        ctrl = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
        for p in blocks[0].versions[0].paragraphs:
            assert not ctrl.search(p.text), f"Control chars in: {p.text[:50]}"

    def test_has_preamble_text(self, blocks):
        texts = [p.text for p in blocks[0].versions[0].paragraphs]
        joined = " ".join(texts)
        assert "občané České republiky" in joined

    def test_has_article_headings(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        article_headings = [p for p in paragraphs if p.css_class == "articulo"]
        assert len(article_headings) > 100  # Constitution has 113 articles

    def test_has_structural_headings(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        # 8 Hlavy rendered as capitulo_tit (### headings)
        chapters = [p for p in paragraphs if p.css_class == "capitulo_tit"]
        assert len(chapters) >= 8

    def test_has_signatory(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        signatories = [p for p in paragraphs if p.css_class == "firma_rey"]
        assert len(signatories) >= 1

    def test_list_items_prefixed(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        list_items = [p for p in paragraphs if p.css_class == "list_item"]
        assert len(list_items) > 0
        for item in list_items:
            assert item.text.startswith("- "), f"List item not prefixed: {item.text[:50]}"


# ─────────────────────────────────────────────
# Text parser — Criminal Code (multi-page)
# ─────────────────────────────────────────────


class TestTextParserCriminalCode:
    @pytest.fixture()
    def blocks(self):
        data = (FIXTURES / "sample-criminal-code-fragments-p0.json").read_bytes()
        return ESbirkaTextParser().parse_text(data)

    def test_returns_blocks(self, blocks):
        assert len(blocks) >= 1

    def test_has_many_paragraphs(self, blocks):
        paragraphs = blocks[0].versions[0].paragraphs
        # Fixture trimmed to 200 fragments; after filtering virtuals ~150+ remain
        assert len(paragraphs) > 100

    def test_hierarchy_cast_above_hlava(self, blocks):
        """CAST (Part) uses titulo_tit, HLAVA (Title) uses capitulo_tit."""
        paragraphs = blocks[0].versions[0].paragraphs
        cast_items = [p for p in paragraphs if p.css_class == "titulo_tit" and "ČÁST" in p.text]
        hlava_items = [p for p in paragraphs if p.css_class == "capitulo_tit" and "HLAVA" in p.text]
        assert len(cast_items) >= 1, "No CAST found as titulo_tit"
        assert len(hlava_items) >= 1, "No HLAVA found as capitulo_tit"


# ─────────────────────────────────────────────
# Text parser — edge cases
# ─────────────────────────────────────────────


class TestTextParserEdgeCases:
    def test_empty_fragments(self):
        data = b'{"seznam": [], "pocetStranek": 0}'
        blocks = ESbirkaTextParser().parse_text(data)
        assert blocks == []

    def test_virtual_only_fragments(self):
        import json

        frags = [
            {"kodTypuFragmentu": "Virtual_Document", "xhtml": ""},
            {"kodTypuFragmentu": "Virtual_Norma", "xhtml": ""},
        ]
        data = json.dumps({"seznam": frags, "pocetStranek": 1}).encode()
        blocks = ESbirkaTextParser().parse_text(data)
        assert blocks == []

    def test_list_format_input(self):
        import json

        frags = [
            {"kodTypuFragmentu": "Odstavec_Dc", "xhtml": "Hello world"},
        ]
        data = json.dumps(frags).encode()
        blocks = ESbirkaTextParser().parse_text(data)
        assert len(blocks) == 1
        assert blocks[0].versions[0].paragraphs[0].text == "Hello world"


# ─────────────────────────────────────────────
# Reform extraction
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
# Table conversion
# ─────────────────────────────────────────────


class TestTableConversion:
    def test_simple_table(self):
        html = (
            "<table><tbody>"
            "<tr><th>A</th><th>B</th></tr>"
            "<tr><td>1</td><td>2</td></tr>"
            "</tbody></table>"
        )
        md = _html_table_to_markdown(html)
        assert "| A | B |" in md
        assert "| --- | --- |" in md
        assert "| 1 | 2 |" in md

    def test_table_with_br(self):
        html = "<table><tbody><tr><th>Col<br/>1</th></tr><tr><td>val</td></tr></tbody></table>"
        md = _html_table_to_markdown(html)
        assert "Col 1" in md  # <br/> → space

    def test_empty_table(self):
        assert _html_table_to_markdown("") == ""
        assert _html_table_to_markdown("<table></table>") == ""

    def test_table_with_pipe_in_content(self):
        html = "<table><tbody><tr><td>a|b</td></tr></tbody></table>"
        md = _html_table_to_markdown(html)
        assert "a\\|b" in md  # pipe escaped

    def test_table_fragment_rendered(self):
        """Tabulka fragments should produce table paragraphs."""
        import json

        frags = [
            {
                "kodTypuFragmentu": "Tabulka",
                "xhtml": "<table><tbody><tr><th>X</th></tr><tr><td>1</td></tr></tbody></table>",
            },
        ]
        data = json.dumps(frags).encode()
        blocks = ESbirkaTextParser().parse_text(data)
        assert len(blocks) == 1
        text = blocks[0].versions[0].paragraphs[0].text
        assert "| X |" in text
        assert "| 1 |" in text


# ─────────────────────────────────────────────
# Reform extraction
# ─────────────────────────────────────────────


class TestReformExtraction:
    def test_extract_reforms_from_constitution(self):
        data = (FIXTURES / "sample-constitution-meta.json").read_bytes()
        reforms = ESbirkaTextParser().extract_reforms(data)
        assert len(reforms) == 9  # 9 amendments to the Constitution
        assert reforms[0]["kodDokumentuSbirky"] == "347/1997 Sb."

    def test_extract_reforms_empty(self):
        reforms = ESbirkaTextParser().extract_reforms(b"{}")
        assert reforms == []

    def test_extract_reforms_invalid_json(self):
        reforms = ESbirkaTextParser().extract_reforms(b"not json")
        assert reforms == []
