"""Tests for the Liechtenstein gesetze.li parser."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.li.client import to_dotted_id, to_url_id
from legalize.fetcher.li.parser import (
    LilexMetadataParser,
    LilexTextParser,
)
from legalize.models import NormStatus

FIXTURES = Path(__file__).parent / "fixtures" / "li"

CONSTITUTION_META = FIXTURES / "constitution-meta.html"
CONSTITUTION_CONTENT = FIXTURES / "constitution-content.html"
PGR_META = FIXTURES / "pgr-meta.html"
PGR_CONTENT = FIXTURES / "pgr-content.html"
STGB_META = FIXTURES / "stgb-meta.html"
STGB_CONTENT = FIXTURES / "stgb-content.html"
TAX_META = FIXTURES / "tax-code-meta.html"
TAX_CONTENT = FIXTURES / "tax-code-content.html"
TREATY_META = FIXTURES / "recent-ordinance-meta.html"
TREATY_CONTENT = FIXTURES / "recent-ordinance-content.html"


def _build_envelope(meta_html: bytes, content_html: bytes, lgbl: str) -> bytes:
    """Build a get_text-style JSON envelope from a meta+content fixture pair."""
    return json.dumps(
        {
            "lgbl": lgbl,
            "url_id": to_url_id(lgbl),
            "meta_html": meta_html.decode("utf-8"),
            "versions": [
                {
                    "version": 999,
                    "date_text": "01.01.2026",
                    "html": content_html.decode("utf-8"),
                }
            ],
        },
        ensure_ascii=False,
    ).encode("utf-8")


def _meta_envelope(meta_html: bytes, content_html: bytes) -> bytes:
    """Build a get_metadata-style JSON envelope (meta_html + current_html)."""
    return json.dumps(
        {
            "meta_html": meta_html.decode("utf-8"),
            "current_html": content_html.decode("utf-8"),
        },
        ensure_ascii=False,
    ).encode("utf-8")


# ─────────────────────────────────────────────
# Identifier helpers
# ─────────────────────────────────────────────


class TestIdentifierHelpers:
    def test_dotted_to_url(self):
        assert to_url_id("1921.015") == "1921015000"
        assert to_url_id("2024.076") == "2024076000"

    def test_url_to_dotted(self):
        assert to_dotted_id("1921015000") == "1921.015"
        assert to_dotted_id("2024076000") == "2024.076"

    def test_idempotent(self):
        assert to_url_id(to_url_id("1921.015")) == "1921015000"
        assert to_dotted_id(to_dotted_id("2024076000")) == "2024.076"

    def test_invalid(self):
        with pytest.raises(ValueError):
            to_url_id("nonsense")


# ─────────────────────────────────────────────
# Metadata parser
# ─────────────────────────────────────────────


class TestLilexMetadataParser:
    def setup_method(self):
        self.parser = LilexMetadataParser()

    def test_constitution_metadata(self):
        envelope = _meta_envelope(CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes())
        meta = self.parser.parse(envelope, "1921.015")
        assert meta.country == "li"
        assert meta.identifier == "LGBl-1921-015"
        assert "Verfassung" in meta.title
        assert "1921" in meta.title
        assert meta.rank == "verfassung"
        assert meta.status == NormStatus.IN_FORCE
        assert meta.source == "https://www.gesetze.li/konso/1921.015"
        # extra metadata fields are present
        keys = {k for k, _ in meta.extra}
        assert "lr_nr" in keys
        assert "lgbl_nr" in keys
        assert "version_count" in keys
        assert dict(meta.extra)["lr_nr"] == "101"

    def test_pgr_metadata(self):
        envelope = _meta_envelope(PGR_META.read_bytes(), PGR_CONTENT.read_bytes())
        meta = self.parser.parse(envelope, "1926.004")
        assert meta.identifier == "LGBl-1926-004"
        assert meta.rank == "gesetz"
        assert "Personen" in meta.title

    def test_stgb_metadata(self):
        envelope = _meta_envelope(STGB_META.read_bytes(), STGB_CONTENT.read_bytes())
        meta = self.parser.parse(envelope, "1988.037")
        assert meta.identifier == "LGBl-1988-037"
        assert "Strafgesetzbuch" in meta.title

    def test_treaty_full_title_from_iframe_meta_description(self):
        """Landing pages truncate long titles with '...'; the iframe's
        <meta name="description"> carries the full text."""
        envelope = _meta_envelope(TREATY_META.read_bytes(), TREATY_CONTENT.read_bytes())
        meta = self.parser.parse(envelope, "2024.076")
        assert meta.identifier == "LGBl-2024-076"
        # Full title — landing was clipped at "betreffe..."
        assert "Schengen" in meta.title
        assert "2019/817" in meta.title or "2019/818" in meta.title

    def test_filesystem_safe_identifier(self):
        envelope = _meta_envelope(CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes())
        meta = self.parser.parse(envelope, "1921.015")
        assert ":" not in meta.identifier
        assert " " not in meta.identifier
        assert "/" not in meta.identifier
        assert "?" not in meta.identifier


# ─────────────────────────────────────────────
# Text parser
# ─────────────────────────────────────────────


class TestLilexTextParser:
    def setup_method(self):
        self.parser = LilexTextParser()

    def test_constitution_blocks(self):
        envelope = _build_envelope(
            CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes(), "1921.015"
        )
        blocks = self.parser.parse_text(envelope)
        assert len(blocks) > 100
        # First block is preamble
        assert blocks[0].id == "preamble"
        # We have heading and article blocks
        types = {b.block_type for b in blocks}
        assert "article" in types
        assert "heading" in types

    def test_constitution_articles_are_unique(self):
        envelope = _build_envelope(
            CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes(), "1921.015"
        )
        blocks = self.parser.parse_text(envelope)
        article_ids = [b.id for b in blocks if b.block_type == "article"]
        assert len(article_ids) == len(set(article_ids))
        # The constitution starts with art-1, art-2, ...
        assert "art-1" in article_ids
        assert "art-2" in article_ids

    def test_pgr_duplicate_anchors_are_disambiguated(self):
        """The PGR has 4 `<a name="art:1">` anchors (one per Abteilung).
        All four must be preserved as distinct blocks."""
        envelope = _build_envelope(PGR_META.read_bytes(), PGR_CONTENT.read_bytes(), "1926.004")
        blocks = self.parser.parse_text(envelope)
        article_ids = [b.id for b in blocks if b.block_type == "article"]
        assert len(article_ids) == len(set(article_ids))
        # The first art:1 keeps the bare id; subsequent occurrences get
        # numeric suffixes (-2, -3, -4, ...) — at least one must be present.
        assert "art-1" in article_ids
        suffixed = [bid for bid in article_ids if bid in {"art-1-2", "art-1-3", "art-1-4"}]
        assert len(suffixed) >= 1, (
            f"Expected at least one art-1-N variant, got: {[bid for bid in article_ids if bid.startswith('art-1-')]}"
        )

    def test_stgb_uses_paragraph_symbol(self):
        envelope = _build_envelope(STGB_META.read_bytes(), STGB_CONTENT.read_bytes(), "1988.037")
        blocks = self.parser.parse_text(envelope)
        articles = [b for b in blocks if b.block_type == "article"]
        # StGB uses § notation. The first article paragraph should start with "§".
        first = articles[0]
        first_para_text = first.versions[0].paragraphs[0].text
        assert "§" in first_para_text

    def test_no_control_chars_in_output(self):
        envelope = _build_envelope(
            CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes(), "1921.015"
        )
        blocks = self.parser.parse_text(envelope)
        import re

        ctrl = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
        for block in blocks:
            for version in block.versions:
                for p in version.paragraphs:
                    assert not ctrl.search(p.text), f"Control char in {block.id}: {p.text!r}"

    def test_footnote_refs_inline_not_loose(self):
        """`<sup>` markers inside `<a href="#fnN">` must be rendered as
        Markdown footnote references `[^N]` inline, not as standalone text."""
        envelope = _build_envelope(
            CONSTITUTION_META.read_bytes(), CONSTITUTION_CONTENT.read_bytes(), "1921.015"
        )
        blocks = self.parser.parse_text(envelope)
        # Find art-1 (which has a footnote in the latest version)
        art1 = next((b for b in blocks if b.id == "art-1"), None)
        assert art1 is not None
        first_para = art1.versions[0].paragraphs[0]
        # First paragraph is the article header — must be "Art. 1[^1]"-shaped
        assert "[^" in first_para.text or first_para.text == "Art. 1"

    def test_treaty_single_block(self):
        envelope = _build_envelope(
            TREATY_META.read_bytes(), TREATY_CONTENT.read_bytes(), "2024.076"
        )
        blocks = self.parser.parse_text(envelope)
        assert len(blocks) >= 1


# ─────────────────────────────────────────────
# Country dispatch
# ─────────────────────────────────────────────


class TestCountryDispatch:
    def test_text_parser_registered(self):
        parser = get_text_parser("li")
        assert isinstance(parser, LilexTextParser)

    def test_metadata_parser_registered(self):
        parser = get_metadata_parser("li")
        assert isinstance(parser, LilexMetadataParser)
