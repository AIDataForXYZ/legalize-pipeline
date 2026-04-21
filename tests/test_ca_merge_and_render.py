"""Tests for the multi-source suvestine merge + new parser render branches.

Covers the surface that didn't yet have dedicated tests:

- ``JusticeCanadaClient._statute_year_chapter``, ``_lang_for_norm``
  helpers.
- ``get_suvestine`` merge rules: chronological sort + dedup between
  gazette-pdf and annual-statute on matching ``(year, chapter)``.
- ``CATextParser._render_gazette_body`` including the OCR-confidence
  disclaimer path.
- ``CATextParser`` handling a ``<Bill>`` root end-to-end (Introduction
  renders as blockquoted Summary/Recommendation; Body sections render
  through the shared ``_parse_body``).
- ``CATextParser.parse_suvestine`` dispatch on ``source_type``:
  ``gazette-pdf`` entries flow to ``_render_gazette_body``; XML entries
  flow through ``_parse_root``.
"""

from __future__ import annotations

import base64
import json
from datetime import date
from pathlib import Path


from legalize.fetcher.ca.client import (
    JusticeCanadaClient,
    _lang_for_norm,
    _statute_year_chapter,
)
from legalize.fetcher.ca.parser import CATextParser, _current_lang
from legalize.fetcher.ca.parser import _render_gazette_body as render_gazette


# ─────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────


class TestStatuteYearChapter:
    def test_annual_statute_source_id(self):
        assert _statute_year_chapter("as-2020-c13") == (2020, 13)

    def test_gazette_source_id(self):
        assert _statute_year_chapter("gazette-1998-c2") == (1998, 2)

    def test_missing_prefix_returns_none(self):
        assert _statute_year_chapter("2020-c13") is None

    def test_upstream_sha_returns_none(self):
        assert _statute_year_chapter("0123456789abcdef" * 2) is None

    def test_wayback_timestamp_returns_none(self):
        assert _statute_year_chapter("wayback-20141215143022") is None

    def test_malformed_returns_none(self):
        assert _statute_year_chapter("as-2020") is None
        assert _statute_year_chapter("as-abc-cXX") is None


class TestLangForNorm:
    def test_english_acts(self):
        assert _lang_for_norm("eng/acts/A-1") == ("eng", "acts", "en")

    def test_french_regulations(self):
        assert _lang_for_norm("fra/reglements/SOR-85-567") == (
            "fra",
            "reglements",
            "fr",
        )


# ─────────────────────────────────────────────
# _render_gazette_body
# ─────────────────────────────────────────────


class TestRenderGazetteBody:
    def test_lead_in_quote_with_bill_number(self):
        entry = {
            "body_text": "Section 1 text.",
            "amending_title": "An Act to amend the Income Tax Act",
            "bill_number": "C-4",
            "date": "2020-03-13",
            "ocr_confidence": 1.0,
        }
        paragraphs = render_gazette(entry)
        # First paragraph is the lead-in blockquote.
        assert paragraphs[0].text.startswith("> **Amendment bill.**")
        assert "Bill C-4" in paragraphs[0].text
        assert "2020-03-13" in paragraphs[0].text
        # Body follows.
        assert any("Section 1 text." in p.text for p in paragraphs)

    def test_lead_in_without_bill_number(self):
        entry = {
            "body_text": "Section 1 text.",
            "amending_title": "An Act respecting X",
            "bill_number": "",
            "date": "2020-03-13",
            "ocr_confidence": 1.0,
        }
        paragraphs = render_gazette(entry)
        assert paragraphs[0].text.startswith("> **Amendment bill.**")
        assert "Bill" not in paragraphs[0].text  # no bill marker
        assert "assented to 2020-03-13" in paragraphs[0].text

    def test_low_ocr_confidence_emits_disclaimer(self):
        entry = {
            "body_text": "Body.",
            "amending_title": "An Act to amend X",
            "bill_number": "C-1",
            "date": "1998-04-22",
            "ocr_confidence": 0.62,
        }
        paragraphs = render_gazette(entry)
        # The OCR disclaimer is a second blockquote between the lead-in
        # and the body.
        texts = [p.text for p in paragraphs]
        assert any("OCR quality" in t for t in texts)
        assert any("62%" in t for t in texts)

    def test_high_ocr_confidence_no_disclaimer(self):
        entry = {
            "body_text": "Body.",
            "amending_title": "An Act to amend X",
            "bill_number": "C-1",
            "date": "2020-03-13",
            "ocr_confidence": 1.0,
        }
        paragraphs = render_gazette(entry)
        assert not any("OCR quality" in p.text for p in paragraphs)

    def test_splits_body_on_blank_lines(self):
        entry = {
            "body_text": "First paragraph.\n\nSecond paragraph.\n\nThird.",
            "amending_title": "An Act X",
            "bill_number": "C-1",
            "date": "2020-01-01",
            "ocr_confidence": 1.0,
        }
        paragraphs = render_gazette(entry)
        body_paragraphs = [p.text for p in paragraphs if not p.text.startswith(">")]
        assert "First paragraph." in body_paragraphs
        assert "Second paragraph." in body_paragraphs
        assert "Third." in body_paragraphs

    def test_empty_body_returns_empty(self):
        entry = {
            "body_text": "",
            "amending_title": "X",
            "bill_number": "",
            "date": "",
            "ocr_confidence": 1.0,
        }
        assert render_gazette(entry) == ()


# ─────────────────────────────────────────────
# CATextParser on <Bill> root
# ─────────────────────────────────────────────


_BILL_XML = b"""<?xml version="1.0"?>
<Bill xml:lang="en" bill-origin="commons" bill-type="govt-public">
  <Identification>
    <BillNumber>C-9</BillNumber>
    <LongTitle>An Act to amend the Income Tax Act (Canada Emergency Rent Subsidy)</LongTitle>
    <ShortTitle>An Act to amend the Income Tax Act</ShortTitle>
    <BillHistory>
      <Stages stage="assented-to">
        <Date><YYYY>2020</YYYY><MM>11</MM><DD>19</DD></Date>
      </Stages>
    </BillHistory>
    <Chapter><AnnualStatuteId><AnnualStatuteNumber>13</AnnualStatuteNumber><YYYY>2020</YYYY></AnnualStatuteId></Chapter>
  </Identification>
  <Introduction>
    <Recommendation>
      <TitleText>RECOMMENDATION</TitleText>
      <Provision>
        <Text>Her Excellency recommends the appropriation of public revenue.</Text>
      </Provision>
    </Recommendation>
    <Summary>
      <TitleText>SUMMARY</TitleText>
      <Provision>
        <Text>This enactment amends the Income Tax Act to revise eligibility criteria.</Text>
      </Provision>
      <Provision>
        <Text>It also extends the CEWS to June 30, 2021.</Text>
      </Provision>
    </Summary>
  </Introduction>
  <Body>
    <Section>
      <Label>1</Label>
      <Text>Paragraph 87(2)(g.6) of the Income Tax Act is replaced by the following:</Text>
      <AmendedText>
        <Text>COVID-19 wage subsidy - new text.</Text>
      </AmendedText>
    </Section>
  </Body>
</Bill>"""


class TestParserOnBillRoot:
    def test_bill_renders_recommendation_and_summary(self):
        token = _current_lang.set("en")
        try:
            blocks = CATextParser().parse_text(_BILL_XML)
        finally:
            _current_lang.reset(token)

        assert len(blocks) == 1
        v = blocks[0].versions[0]
        texts = [p.text for p in v.paragraphs]
        # Recommendation is first — blockquoted.
        assert any(t.startswith("> **Recommendation.**") for t in texts)
        # Summary follows — first line carries the Summary label.
        summary_lines = [t for t in texts if t.startswith("> **Summary.**")]
        assert len(summary_lines) == 1
        # Subsequent Summary Provisions render as continuation quotes.
        assert any(t.startswith("> ") and "CEWS to June 30" in t for t in texts)

    def test_bill_pub_date_falls_back_to_assent(self):
        """With no lims:pit-date on the root, the parser should use the
        BillHistory/assented-to date."""
        token = _current_lang.set("en")
        try:
            blocks = CATextParser().parse_text(_BILL_XML)
        finally:
            _current_lang.reset(token)

        assert blocks[0].versions[0].publication_date == date(2020, 11, 19)

    def test_amended_text_renders_as_blockquote(self):
        token = _current_lang.set("en")
        try:
            blocks = CATextParser().parse_text(_BILL_XML)
        finally:
            _current_lang.reset(token)
        texts = [p.text for p in blocks[0].versions[0].paragraphs]
        # The AmendedText child of Section 1 renders with a quote prefix.
        assert any(t.startswith("> ") and "COVID-19" in t for t in texts)


# ─────────────────────────────────────────────
# parse_suvestine dispatch
# ─────────────────────────────────────────────


class TestParseSuvestineDispatch:
    def test_gazette_entry_routed_to_gazette_renderer(self):
        blob = json.dumps(
            {
                "versions": [
                    {
                        "source_type": "gazette-pdf",
                        "source_id": "gazette-1999-c5",
                        "date": "1999-06-14",
                        "body_text": "Section 1 text.",
                        "amending_title": "An Act to amend the X Act",
                        "bill_number": "C-5",
                        "ocr_confidence": 1.0,
                    }
                ]
            }
        ).encode("utf-8")

        blocks, reforms = CATextParser().parse_suvestine(blob, "eng/acts/X-1")

        assert len(blocks) == 1
        assert len(reforms) == 1
        assert reforms[0].norm_id == "gazette-1999-c5"
        assert reforms[0].date == date(1999, 6, 14)
        texts = [p.text for p in blocks[0].versions[0].paragraphs]
        # The gazette renderer emitted the Amendment-bill lead-in.
        assert any(t.startswith("> **Amendment bill.**") for t in texts)

    def test_xml_entry_routed_through_parse_root(self):
        blob = json.dumps(
            {
                "versions": [
                    {
                        "source_type": "annual-statute",
                        "source_id": "as-2020-c13",
                        "date": "2020-11-19",
                        "xml": base64.b64encode(_BILL_XML).decode("ascii"),
                    }
                ]
            }
        ).encode("utf-8")

        blocks, reforms = CATextParser().parse_suvestine(blob, "eng/acts/I-3.3")
        assert len(reforms) == 1
        texts = [p.text for p in blocks[0].versions[0].paragraphs]
        # XML route produced the Recommendation/Summary preamble.
        assert any(t.startswith("> **Recommendation.**") for t in texts)
        assert any(t.startswith("> **Summary.**") for t in texts)

    def test_mixed_sources_preserve_chronological_order(self):
        """A blob carrying gazette + annual-statute + upstream-git entries
        should parse all and order reforms oldest-first."""
        # Minimal Statute XML for the upstream-git entry.
        statute_xml = (
            b'<?xml version="1.0"?>'
            b'<Statute xmlns:lims="http://justice.gc.ca/lims" '
            b'lims:pit-date="2021-03-01">'
            b"<Body><Section><Label>1</Label><Text>consolidated text</Text></Section></Body>"
            b"</Statute>"
        )
        blob = json.dumps(
            {
                "versions": [
                    {
                        "source_type": "gazette-pdf",
                        "source_id": "gazette-1999-c5",
                        "date": "1999-06-14",
                        "body_text": "gazette body.",
                        "amending_title": "An Act X",
                        "bill_number": "C-5",
                        "ocr_confidence": 1.0,
                    },
                    {
                        "source_type": "annual-statute",
                        "source_id": "as-2020-c13",
                        "date": "2020-11-19",
                        "xml": base64.b64encode(_BILL_XML).decode("ascii"),
                    },
                    {
                        "source_type": "upstream-git",
                        "source_id": "deadbeef" * 5,
                        "date": "2021-03-01",
                        "xml": base64.b64encode(statute_xml).decode("ascii"),
                    },
                ]
            }
        ).encode("utf-8")

        blocks, reforms = CATextParser().parse_suvestine(blob, "eng/acts/I-3.3")
        assert len(reforms) == 3
        assert reforms[0].date == date(1999, 6, 14)
        assert reforms[1].date == date(2020, 11, 19)
        assert reforms[2].date == date(2021, 3, 1)

    def test_empty_blob_returns_empty(self):
        blocks, reforms = CATextParser().parse_suvestine(b"", "eng/acts/A-1")
        assert blocks == []
        assert reforms == []

    def test_malformed_json_returns_empty(self):
        blocks, reforms = CATextParser().parse_suvestine(b"{not json", "eng/acts/A-1")
        assert blocks == []
        assert reforms == []


# ─────────────────────────────────────────────
# Client merge + dedup
# ─────────────────────────────────────────────


class TestClientMergeDedup:
    def test_gazette_dedupes_against_annual_statute(self, tmp_path: Path):
        """When the same (year, chapter) appears in both gazette-pdf and
        annual-statute, the annual-statute entry wins."""
        client = JusticeCanadaClient(
            xml_dir=str(tmp_path / "no-such-clone"),  # forces fallback branches
            data_dir=str(tmp_path),
            wayback_enabled=False,
        )
        # Monkeypatch the private source enumerators so we don't need a
        # real upstream clone.
        client._git_log_versions = lambda norm_id: []  # type: ignore[assignment]
        client._wayback_versions = lambda norm_id: []  # type: ignore[assignment]
        client._annual_statute_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "annual-statute",
                "source_id": "as-2020-c13",
                "date": "2020-11-19",
                "xml": base64.b64encode(_BILL_XML).decode("ascii"),
            }
        ]
        client._gazette_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "gazette-pdf",
                "source_id": "gazette-2020-c13",
                "date": "2020-11-19",
                "body_text": "...",
                "amending_title": "An Act X",
                "bill_number": "C-9",
                "ocr_confidence": 1.0,
            }
        ]

        # Force the fallback-clone branch to return something non-empty.
        client._xml_dir = tmp_path  # make it pass the existence check
        (tmp_path).mkdir(exist_ok=True)

        blob = client.get_suvestine("eng/acts/I-3.3")
        data = json.loads(blob)
        types = [v["source_type"] for v in data["versions"]]
        # The gazette entry should have been dropped.
        assert "annual-statute" in types
        assert "gazette-pdf" not in types

    def test_gazette_kept_when_no_overlap(self, tmp_path: Path):
        """A gazette entry without a matching annual-statute chapter stays."""
        client = JusticeCanadaClient(
            xml_dir=str(tmp_path),
            data_dir=str(tmp_path),
            wayback_enabled=False,
        )
        client._git_log_versions = lambda norm_id: []  # type: ignore[assignment]
        client._wayback_versions = lambda norm_id: []  # type: ignore[assignment]
        client._annual_statute_versions = lambda norm_id: []  # type: ignore[assignment]
        client._gazette_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "gazette-pdf",
                "source_id": "gazette-1999-c5",
                "date": "1999-06-14",
                "body_text": "...",
                "amending_title": "An Act X",
                "bill_number": "C-5",
                "ocr_confidence": 1.0,
            }
        ]

        blob = client.get_suvestine("eng/acts/X-1")
        data = json.loads(blob)
        types = [v["source_type"] for v in data["versions"]]
        assert types == ["gazette-pdf"]

    def test_wayback_disabled_for_regulations(self, tmp_path: Path):
        """By default Wayback is opted-in only for ``eng/acts`` and ``fra/lois``."""
        client = JusticeCanadaClient(
            xml_dir=str(tmp_path),
            data_dir=str(tmp_path),
            wayback_enabled=True,
        )
        assert client._wayback_enabled_for("eng/acts/A-1") is True
        assert client._wayback_enabled_for("fra/lois/I-3.3") is True
        assert client._wayback_enabled_for("eng/regulations/SOR-85-567") is False
        assert client._wayback_enabled_for("fra/reglements/SOR-85-567") is False

    def test_wayback_fully_disabled_short_circuits(self, tmp_path: Path):
        client = JusticeCanadaClient(
            xml_dir=str(tmp_path),
            data_dir=str(tmp_path),
            wayback_enabled=False,
        )
        assert client._wayback_enabled_for("eng/acts/A-1") is False

    def test_versions_sorted_chronologically(self, tmp_path: Path):
        """Entries from all sources should come out oldest-first."""
        client = JusticeCanadaClient(
            xml_dir=str(tmp_path),
            data_dir=str(tmp_path),
            wayback_enabled=False,
        )
        client._git_log_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "upstream-git",
                "source_id": "abc123" * 6 + "abcd",
                "date": "2021-03-01",
                "xml": "PFN0YXR1dGUvPg==",  # "<Statute/>"
            }
        ]
        client._wayback_versions = lambda norm_id: []  # type: ignore[assignment]
        client._annual_statute_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "annual-statute",
                "source_id": "as-2020-c13",
                "date": "2020-11-19",
                "xml": "PEJpbGwvPg==",  # "<Bill/>"
            }
        ]
        client._gazette_versions = lambda norm_id: [  # type: ignore[assignment]
            {
                "source_type": "gazette-pdf",
                "source_id": "gazette-1999-c5",
                "date": "1999-06-14",
                "body_text": "body",
                "amending_title": "An Act X",
                "bill_number": "C-5",
                "ocr_confidence": 1.0,
            }
        ]

        blob = client.get_suvestine("eng/acts/I-3.3")
        data = json.loads(blob)
        dates = [v["date"] for v in data["versions"]]
        assert dates == sorted(dates)
