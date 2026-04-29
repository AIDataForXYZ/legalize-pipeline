"""Mexico fetcher tests.

Diputados (LeyesBiblio) is wired end-to-end against a saved index fixture.
The other five sources are still stubs and only the registry/routing
contract is exercised for them.
"""

import json
from pathlib import Path

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.mx.client import (
    DEFAULT_SOURCES,
    MXClient,
    parse_diputados_index,
)
from legalize.fetcher.mx.parser import MXMetadataParser, MXTextParser

FIXTURES = Path("tests/fixtures/mx")


# ── Registry / routing ────────────────────────────────────────────────


def test_registry_dispatch():
    text_parser = get_text_parser("mx")
    metadata_parser = get_metadata_parser("mx")
    assert isinstance(text_parser, MXTextParser)
    assert isinstance(metadata_parser, MXMetadataParser)


def test_default_sources_loaded():
    client = MXClient()
    assert set(client.sources) == {"diputados", "dof", "ojn", "sjf"}
    assert client.sources["dof"].id_prefix == "DOF"


def test_source_for_routes_by_prefix():
    client = MXClient()
    assert client.source_for("DOF-2024-001").name == "dof"
    assert client.source_for("DIP-CPEUM").name == "diputados"
    assert client.source_for("DIP-LFT").name == "diputados"
    assert client.source_for("OJN-CONST-1917").name == "ojn"
    assert client.source_for("SJF-TESIS-2024-12345").name == "sjf"


def test_source_for_unknown_prefix_raises():
    client = MXClient()
    with pytest.raises(ValueError, match="No MX source registered"):
        client.source_for("XYZ-123")


def test_source_kinds():
    client = MXClient()
    kinds = {name: src.kind for name, src in client.sources.items()}
    assert kinds["diputados"] == "primary_legislation"
    assert kinds["dof"] == "primary_legislation"
    assert kinds["ojn"] == "primary_legislation"
    assert kinds["sjf"] == "case_law"


def test_default_sources_have_required_fields():
    for name, conf in DEFAULT_SOURCES.items():
        assert "base_url" in conf, name
        assert "id_prefix" in conf, name


# ── Diputados index walker ────────────────────────────────────────────


def test_parse_diputados_index_against_fixture():
    html_bytes = (FIXTURES / "diputados-index.html").read_bytes()
    rows = parse_diputados_index(html_bytes, "https://www.diputados.gob.mx/LeyesBiblio")
    # The live index has ~260 federal laws. Allow some drift but require a sane floor.
    assert len(rows) > 200, f"unexpectedly few rows: {len(rows)}"
    assert "CPEUM" in rows
    cpeum = rows["CPEUM"]
    assert cpeum.publication_date.year == 1917
    assert "constituci" in cpeum.title.lower()
    assert cpeum.rank == "constitucion"
    assert cpeum.pdf_url.endswith("/pdf/CPEUM.pdf")
    assert cpeum.doc_url is not None and cpeum.doc_url.endswith("/doc/CPEUM.doc")


def test_parse_diputados_index_classifies_codigo():
    html_bytes = (FIXTURES / "diputados-index.html").read_bytes()
    rows = parse_diputados_index(html_bytes, "https://www.diputados.gob.mx/LeyesBiblio")
    assert "CCF" in rows  # Código Civil Federal
    assert rows["CCF"].rank == "codigo"


# ── Parsers (Diputados envelope) ──────────────────────────────────────


def test_metadata_parser_decodes_diputados_envelope():
    envelope = {
        "source": "diputados",
        "norm_id": "DIP-CPEUM",
        "abbrev": "CPEUM",
        "title": "Constitución Política de los Estados Unidos Mexicanos",
        "rank": "constitucion",
        "publication_date": "1917-02-05",
        "last_reform_date": "2026-04-10",
        "pdf_url": "https://www.diputados.gob.mx/LeyesBiblio/pdf/CPEUM.pdf",
        "doc_url": "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc",
    }
    meta = MXMetadataParser().parse(json.dumps(envelope).encode("utf-8"), "DIP-CPEUM")
    assert meta.country == "mx"
    assert meta.identifier == "DIP-CPEUM"
    assert meta.publication_date.year == 1917
    assert meta.last_modified is not None
    assert str(meta.rank) == "constitucion"
    # source now points to the DOC (primary format); pdf_url is preserved in extra.
    assert meta.source.endswith("/CPEUM.doc")
    extra = dict(meta.extra)
    assert extra["abbrev"] == "CPEUM"
    assert extra["last_reform_dof"] == "2026-04-10"
    assert extra["pdf_url"].endswith("/CPEUM.pdf")
    assert extra["doc_url"].endswith("/CPEUM.doc")


def test_text_parser_rejects_non_envelope():
    with pytest.raises(ValueError, match="JSON envelope"):
        MXTextParser().parse_text(b"not json")


def test_metadata_parser_unwired_source_raises():
    envelope = {
        "source": "dof",
        "norm_id": "DOF-2024-1",
        "title": "stub",
    }
    with pytest.raises(NotImplementedError):
        MXMetadataParser().parse(json.dumps(envelope).encode("utf-8"), "DOF-2024-1")


def test_get_metadata_unwired_source_raises():
    client = MXClient()
    with pytest.raises(NotImplementedError, match="dof"):
        client.get_metadata("DOF-2024-1")


# ── Diputados block builder against a synthetic PDF stream ────────────


def _diputados_block_run(text: str):
    """Helper: drive the line-stream block builder with synthetic PDF text.

    Skips the actual pdfplumber call by feeding a single 'page' string with
    blank-line separators. Returns the built blocks.
    """
    import base64

    from legalize.fetcher.mx import parser as mx_parser

    # Patch _extract_pdf_text so we can inject our own page stream.
    real = mx_parser._extract_pdf_text
    mx_parser._extract_pdf_text = lambda _b: [text]
    try:
        envelope = {
            "source": "diputados",
            "norm_id": "DIP-TEST",
            "abbrev": "TEST",
            "title": "Ley de Prueba",
            "rank": "ley",
            "publication_date": "2020-01-01",
            "last_reform_date": "2024-06-15",
            "pdf_url": "https://example.test/TEST.pdf",
            "pdf_b64": base64.b64encode(b"%PDF-stub").decode("ascii"),
        }
        return mx_parser._diputados_blocks(envelope)
    finally:
        mx_parser._extract_pdf_text = real


def test_article_heading_separates_from_body():
    blocks = _diputados_block_run(
        "Artículo 1o. Las personas son libres y tienen derecho a la dignidad.\n"
        "\n"
        "Artículo 2o.- Los derechos humanos son universales."
    )
    article_blocks = [b for b in blocks if b.block_type == "article"]
    assert len(article_blocks) == 2
    # Heading paragraph contains only the article number, not the body sentence
    first = article_blocks[0]
    head_para = first.versions[0].paragraphs[0]
    assert head_para.css_class == "articulo"
    assert head_para.text == "Artículo 1o."
    # Body sentence ends up in its own paragraph
    body_para = first.versions[0].paragraphs[1]
    assert body_para.css_class == "parrafo"
    assert "personas son libres" in body_para.text


def test_pdf_line_wraps_merge_into_one_paragraph():
    blocks = _diputados_block_run(
        "Artículo 1o.\n"
        "Esta es la primera línea visual\n"
        "que continúa en la siguiente sin un salto de párrafo real.\n"
        "\n"
        "Este es un párrafo distinto."
    )
    paragraphs = blocks[0].versions[0].paragraphs
    bodies = [p for p in paragraphs if p.css_class == "parrafo"]
    assert len(bodies) == 2
    assert bodies[0].text == (
        "Esta es la primera línea visual que continúa en la siguiente "
        "sin un salto de párrafo real."
    )
    assert bodies[1].text == "Este es un párrafo distinto."


def test_reform_stamps_are_tagged_nota_pie_and_isolated():
    blocks = _diputados_block_run(
        "Artículo 1o.\n"
        "Este es el cuerpo del artículo.\n"
        "Párrafo reformado DOF 04-12-2006\n"
        "Esta línea es texto de ley posterior."
    )
    paragraphs = blocks[0].versions[0].paragraphs
    classes = [p.css_class for p in paragraphs]
    # Expect: articulo, parrafo (body), nota_pie (stamp), parrafo (body)
    assert classes == ["articulo", "parrafo", "nota_pie", "parrafo"]
    stamp = paragraphs[2]
    assert "DOF 04-12-2006" in stamp.text
    # The stamp does NOT contain the law text that follows it.
    assert "texto de ley" not in stamp.text


def test_fracciones_force_paragraph_breaks():
    blocks = _diputados_block_run(
        "Artículo 1o.\n"
        "Las personas tienen derecho a:\n"
        "I. La libertad de expresión.\n"
        "II. La libertad de reunión.\n"
        "III. La libertad de asociación."
    )
    bodies = [p for p in blocks[0].versions[0].paragraphs if p.css_class == "parrafo"]
    # Each Roman-numeral fracción should be its own paragraph, not glued.
    assert len(bodies) == 4
    assert bodies[1].text.startswith("I.")
    assert bodies[2].text.startswith("II.")
    assert bodies[3].text.startswith("III.")


def test_apartado_marker_forces_paragraph_break():
    blocks = _diputados_block_run(
        "Artículo 1o.\n"
        "Los derechos se organizan en dos apartados.\n"
        "A. Derechos individuales.\n"
        "B. Derechos colectivos."
    )
    bodies = [p for p in blocks[0].versions[0].paragraphs if p.css_class == "parrafo"]
    assert len(bodies) == 3
    assert bodies[1].text.startswith("A.")
    assert bodies[2].text.startswith("B.")


def test_transitorios_emit_section_heading():
    blocks = _diputados_block_run(
        "Artículo 1o.\n"
        "Cuerpo del artículo principal.\n"
        "\n"
        "ARTÍCULOS TRANSITORIOS\n"
        "\n"
        "Artículo Primero. Esta ley entrará en vigor al día siguiente."
    )
    section_blocks = [b for b in blocks if b.block_type == "section"]
    assert any("TRANSITORIOS" in b.title.upper() for b in section_blocks)


# ── DOC path: unit tests against CPEUM.doc fixture ───────────────────────────


def test_doc_paragraph_extraction_from_cpeum_fixture():
    """_extract_doc_paragraphs parses the real CPEUM.doc into sane paragraphs."""
    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    paras = _extract_doc_paragraphs(doc_bytes)

    # The CPEUM is 406 pages; we expect thousands of paragraphs.
    assert len(paras) > 500, f"too few paragraphs: {len(paras)}"

    # Artículo 1 should be present.
    art1_paras = [p for p in paras if p.startswith("Artículo 1o.")]
    assert len(art1_paras) >= 1, "Artículo 1o. not found in extracted paragraphs"

    # At least one reform stamp for Artículo 1.
    reform_near_art1 = any(
        "Párrafo reformado DOF" in p or "Artículo reformado DOF" in p
        for p in paras[:50]
    )
    assert reform_near_art1, "Expected a reform stamp near the start of the document"


def test_doc_block_builder_artículo_1_paragraphs():
    """DOC block builder correctly parses Artículo 1 of CPEUM from the real fixture."""
    import base64

    from legalize.fetcher.mx.parser import _diputados_doc_blocks

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    envelope = {
        "source": "diputados",
        "source_format": "doc",
        "norm_id": "DIP-CPEUM",
        "abbrev": "CPEUM",
        "title": "Constitución Política de los Estados Unidos Mexicanos",
        "rank": "constitucion",
        "publication_date": "1917-02-05",
        "last_reform_date": "2026-04-10",
        "pdf_url": "https://www.diputados.gob.mx/LeyesBiblio/pdf/CPEUM.pdf",
        "doc_url": "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc",
        "doc_b64": base64.b64encode(doc_bytes).decode("ascii"),
    }

    blocks = _diputados_doc_blocks(envelope)

    article_blocks = [b for b in blocks if b.block_type == "article"]
    section_blocks = [b for b in blocks if b.block_type == "section"]

    # CPEUM has 136 constitutional articles + transitorios; allow some variance
    # from the DOC version (amendments add/remove articles).
    assert len(article_blocks) > 100, f"too few articles: {len(article_blocks)}"
    assert len(section_blocks) >= 5, f"too few sections (títulos/capítulos): {len(section_blocks)}"

    # First article must be Artículo 1o.
    art1 = article_blocks[0]
    assert art1.id.startswith("art-1o-")
    assert art1.title == "Artículo 1o."

    # Artículo 1 has a heading paragraph and at least one body paragraph.
    paras_art1 = art1.versions[0].paragraphs
    head = paras_art1[0]
    assert head.css_class == "articulo"
    assert "Artículo 1o." in head.text

    body_paras = [p for p in paras_art1 if p.css_class == "parrafo"]
    assert len(body_paras) >= 4, "Expected at least 4 body paragraphs in Artículo 1"
    # First body paragraph should be the rights enumeration opening.
    assert "Estados Unidos Mexicanos" in body_paras[0].text

    # Reform stamps must be present and tagged as nota_pie.
    stamp_paras = [p for p in paras_art1 if p.css_class == "nota_pie"]
    assert len(stamp_paras) >= 1, "Expected at least one reform stamp in Artículo 1"
    # All stamps must contain a DOF date.
    for stamp in stamp_paras:
        assert "DOF" in stamp.text, f"Stamp missing DOF date: {stamp.text}"


def test_doc_dispatch_via_text_parser_envelope():
    """MXTextParser dispatches source_format='doc' to the DOC block builder."""
    import base64

    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    envelope = {
        "source": "diputados",
        "source_format": "doc",
        "norm_id": "DIP-CPEUM",
        "abbrev": "CPEUM",
        "title": "Constitución Política de los Estados Unidos Mexicanos",
        "rank": "constitucion",
        "publication_date": "1917-02-05",
        "last_reform_date": "2026-04-10",
        "pdf_url": "https://www.diputados.gob.mx/LeyesBiblio/pdf/CPEUM.pdf",
        "doc_url": "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc",
        "doc_b64": base64.b64encode(doc_bytes).decode("ascii"),
    }
    payload = json.dumps(envelope).encode("utf-8")
    blocks = MXTextParser().parse_text(payload)
    assert len(blocks) > 100


def test_reform_stamp_regex_encabezado():
    """_REFORM_STAMP_RE must match 'Encabezado de inciso reformado DOF ...' stamps."""
    from legalize.fetcher.mx.parser import _REFORM_STAMP_RE

    assert _REFORM_STAMP_RE.match("Encabezado de inciso reformado DOF 27-06-1990")
    assert _REFORM_STAMP_RE.match("Encabezado del Capítulo reformado DOF 01-01-2000")
    # Sanity-check that existing patterns still work.
    assert _REFORM_STAMP_RE.match("Párrafo reformado DOF 04-12-2006, 10-06-2011")
    assert _REFORM_STAMP_RE.match("Reforma DOF 14-08-2001: Derogó del artículo")
    assert _REFORM_STAMP_RE.match("Denominación del Capítulo reformada DOF 10-06-2011")


# ── Mock-HTTP integration: DOC download path ─────────────────────────────────


def test_diputados_text_returns_doc_envelope_by_default():
    """_diputados_text downloads the DOC and embeds it as doc_b64 by default."""
    import base64

    import responses as responses_lib
    from responses import RequestsMock

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    index_html = (FIXTURES / "diputados-index.html").read_bytes()

    _INDEX_URL = "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
    _DOC_URL = "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc"

    with RequestsMock() as rsps:
        rsps.add(
            responses_lib.GET,
            _INDEX_URL,
            body=index_html,
            status=200,
            content_type="text/html; charset=windows-1252",
        )
        rsps.add(
            responses_lib.GET,
            _DOC_URL,
            body=doc_bytes,
            status=200,
            content_type="application/msword",
        )

        client = MXClient()
        raw = client.get_text("DIP-CPEUM")

    envelope = json.loads(raw.decode("utf-8"))
    assert envelope["source"] == "diputados"
    assert envelope["source_format"] == "doc"
    assert "doc_b64" in envelope
    assert "pdf_b64" not in envelope
    # Round-trip the bytes.
    assert base64.b64decode(envelope["doc_b64"]) == doc_bytes
    # Both URLs must be recorded.
    assert envelope["pdf_url"].endswith("/CPEUM.pdf")
    assert envelope["doc_url"].endswith("/CPEUM.doc")


def test_diputados_text_falls_back_to_pdf_when_use_pdf_true():
    """_diputados_text downloads the PDF and sets source_format='pdf' when use_pdf=True."""
    import base64

    import responses as responses_lib
    from responses import RequestsMock

    index_html = (FIXTURES / "diputados-index.html").read_bytes()

    _INDEX_URL = "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
    _PDF_URL = "https://www.diputados.gob.mx/LeyesBiblio/pdf/CPEUM.pdf"

    fake_pdf = b"%PDF-1.4 fake"

    with RequestsMock() as rsps:
        rsps.add(
            responses_lib.GET,
            _INDEX_URL,
            body=index_html,
            status=200,
            content_type="text/html; charset=windows-1252",
        )
        rsps.add(
            responses_lib.GET,
            _PDF_URL,
            body=fake_pdf,
            status=200,
            content_type="application/pdf",
        )

        client = MXClient()
        raw = client._diputados_text("DIP-CPEUM", meta_data=None, use_pdf=True)

    envelope = json.loads(raw.decode("utf-8"))
    assert envelope["source_format"] == "pdf"
    assert "pdf_b64" in envelope
    assert "doc_b64" not in envelope
    assert base64.b64decode(envelope["pdf_b64"]) == fake_pdf


def test_doc_get_text_then_parse_text_end_to_end():
    """Full pipeline: get_text returns a DOC envelope that parse_text can consume."""
    import responses as responses_lib
    from responses import RequestsMock

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    index_html = (FIXTURES / "diputados-index.html").read_bytes()

    _INDEX_URL = "https://www.diputados.gob.mx/LeyesBiblio/index.htm"
    _DOC_URL = "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc"

    with RequestsMock() as rsps:
        rsps.add(
            responses_lib.GET,
            _INDEX_URL,
            body=index_html,
            status=200,
            content_type="text/html; charset=windows-1252",
        )
        rsps.add(
            responses_lib.GET,
            _DOC_URL,
            body=doc_bytes,
            status=200,
            content_type="application/msword",
        )

        client = MXClient()
        raw = client.get_text("DIP-CPEUM")

    # parse_text must produce a non-empty list of blocks without raising.
    blocks = MXTextParser().parse_text(raw)
    assert len(blocks) > 100
    article_ids = [b.id for b in blocks if b.block_type == "article"]
    # Must include article 1.
    assert any(aid.startswith("art-1o-") for aid in article_ids)


# ── Field-code garbage filter tests ──────────────────────────────────────────


def test_field_code_garbage_filtered_from_dip109():
    """DIP-109.doc must produce no field-code garbage paragraphs.

    DIP-109 (Ley Federal de Juegos y Sorteos) is one of the DOC files that
    previously leaked Word conditional-format field codes (``$$IfF4``,
    ``Faöf4``, ``$If^``, ``Qkd…``) and OLE2 binary tail artifacts into the
    extracted text.  After the fix, none of those tokens should appear in
    any extracted paragraph, and the trailing single-char relics must be gone.
    """
    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    doc_bytes = (FIXTURES / "DIP-109.doc").read_bytes()
    paras = _extract_doc_paragraphs(doc_bytes)

    # Known field-code garbage substrings that must not survive.
    garbage_patterns = [
        "$$IfF4",
        "Faöf4",
        "$If^",
        "Qkd",
        "OJQJ",
        "bjbj",
        "mH\nsH",
    ]
    for pattern in garbage_patterns:
        offenders = [p for p in paras if pattern in p]
        assert not offenders, (
            f"Field-code garbage pattern {pattern!r} found in paragraph(s): "
            + "; ".join(repr(p[:80]) for p in offenders)
        )

    # Trailing single-char artifacts must be trimmed.
    if paras:
        last = paras[-1]
        assert len(last) > 1 or last in {".", ","}, (
            f"Trailing single-char artifact not trimmed: {last!r}"
        )

    # No non-table paragraph should contain an embedded newline (post-\r split).
    # Table paragraphs legitimately use \n as a line separator in pipe-table format.
    newline_paras = [p for p in paras if "\n" in p and not p.startswith("|")]
    assert not newline_paras, (
        "Non-table paragraphs with embedded \\n found: "
        + "; ".join(repr(p[:80]) for p in newline_paras)
    )

    # Main law text must still be present.
    assert any("JUEGOS Y SORTEOS" in p or "ARTICULO 1o" in p for p in paras), (
        "Main law text missing from DIP-109 extraction"
    )


def test_ascii_clean_paragraphs_not_dropped():
    """_extract_doc_paragraphs must preserve clean ASCII / Spanish prose.

    Regression guard: the garbage filter must not discard ordinary Spanish
    legislative text (accented chars, periods, commas, digits).
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    clean_paras = [
        "En los Estados Unidos Mexicanos todas las personas gozarán de los derechos humanos.",
        "Artículo 1o. En los Estados Unidos Mexicanos todas las personas gozarán.",
        "I. La libertad de expresión es inviolable.",
        "A. Derechos individuales.",
        "Párrafo reformado DOF 04-12-2006, 10-06-2011",
        "Se concede un plazo de diez días a los interesados.",
        "El importe podrá ser de $500 a $5,000 pesos.",  # dollar sign in prose
    ]
    for para in clean_paras:
        assert not _is_binary_garbage(para), (
            f"Clean paragraph incorrectly flagged as garbage: {para!r}"
        )


def test_embedded_newline_always_garbage():
    """_is_binary_garbage must flag any paragraph containing an embedded newline."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    # These mimic OLE2 binary paragraphs that bleed into the Word text stream.
    embedded_newline_cases = [
        "!/õ\tá\t\n\n\n\n\n¶\nº\n¸½#1_",          # DIP-109 artifact
        "6B*CJOJPJQJ]mH\nphÿsH\nhY>Ìhès",          # CPEUM tail garbage
        "sometext\nmore text",                         # generic embedded-NL
    ]
    for para in embedded_newline_cases:
        assert _is_binary_garbage(para), (
            f"Embedded-newline paragraph not flagged as garbage: {para!r}"
        )


def test_cpeum_doc_no_garbage_after_fix():
    """_extract_doc_paragraphs on CPEUM.doc must not produce field-code garbage.

    CPEUM previously leaked 35+ binary paragraphs containing embedded
    newlines and CJOJPJQJ style-sheet tokens.  After the fix all extracted
    paragraphs must be clean.
    """
    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    paras = _extract_doc_paragraphs(doc_bytes)

    # No non-table paragraph should contain an embedded newline.
    # (Table paragraphs legitimately contain \n as part of the pipe-table format.)
    newline_paras = [p for p in paras if "\n" in p and not p.startswith("|")]
    assert not newline_paras, (
        f"{len(newline_paras)} non-table paragraph(s) with embedded \\n found in CPEUM output"
    )

    # No paragraph should contain Word style-sheet tokens.
    for pattern in ["OJQJ", "bjbj", "Faöf4", "$If^"]:
        offenders = [p for p in paras if pattern in p]
        assert not offenders, (
            f"Garbage pattern {pattern!r} found in CPEUM output: "
            + repr(offenders[0][:80])
        )

    # Fracción paragraphs that use tab as indent separator must still be present.
    fraccion_paras = [p for p in paras if "\t" in p]
    assert len(fraccion_paras) > 100, (
        f"Expected >100 tab-formatted fracción paragraphs, got {len(fraccion_paras)}"
    )
    # Spot-check: Artículo 2o. apartado A with tab separator.
    tab_apartado = [p for p in fraccion_paras if p.startswith("A. \t") or p.startswith("A.\t")]
    assert tab_apartado, "Expected at least one 'A. \\t...' apartado paragraph"


# ── Bug 1 — mid-document field-code garbage ──────────────────────────────────


def test_toc_garbage_runs_filtered():
    """_is_binary_garbage must reject TOC-style field-code garbage mid-document.

    Covers the patterns that slipped through the original filter for DIP-218
    and similar files: $$IfF tokens, $%@A TOC delimiters, $!`!a$ cell refs,
    and long runs of identical or alternating non-ASCII characters.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    garbage_cases = [
        # $$IfF conditional-format field code (any suffix)
        "$$IfFÖ0óË×",
        # $%@A TOC-style delimiter
        "$%@Aª«ßà();<\\]°±ÅÆõöý" + "ô" * 20 + "$!`!a$ö,-AB¡¢",
        # 4+ identical non-ASCII chars (ôôôô run)
        "ãä®¯ÃÄÖ×çèRSop" + "ô" * 10 + "$!`!a$",
        # Alternating non-ASCII pair (ïáïáïáïá)
        "ïÙñèñJóYó" + "ïá" * 8 + "ÚÖáïáïáïá",
        # OJPJQJ Word style-sheet token (variant without Q)
        "6B*CJOJPJQJ]aJphÿPó_óGô_ôkôtô",
        # 4+ identical non-ASCII (ØØØ)
        "ØØØØØÙÙÙÙËÙŸÙÈÚÊÚÐÚÑÚ",
    ]
    for para in garbage_cases:
        assert _is_binary_garbage(para), (
            f"TOC/field-code garbage not flagged: {para[:80]!r}"
        )


def test_clean_spanish_not_dropped_by_new_signals():
    """_is_binary_garbage must NOT drop legitimate Spanish legislative text.

    Regression guard for the new signals (4 and 5): accented chars, ordinals,
    and dollar signs in prose must not trigger false positives.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    clean_paras = [
        # Normal accented Spanish prose
        "En los Estados Unidos Mexicanos todas las personas gozarán de derechos.",
        # Accented chars not repeated 4+ times
        "Artículo 1o.- La presente Ley tiene por objeto establecer las bases.",
        # Dollar sign in monetary context
        "El importe podrá ser de $500 a $5,000 pesos.",
        # Ellipsis with ASCII periods (multiple dots)
        "Se concede al C........... como titular de la dependencia.",
        # Sequence of different non-ASCII accented chars
        "áéíóúÁÉÍÓÚñÑçÇ — valid chars in Spanish law.",
    ]
    for para in clean_paras:
        assert not _is_binary_garbage(para), (
            f"Clean paragraph incorrectly flagged as garbage: {para!r}"
        )


# ── Bug 2 — issuing-decree articles at law start ──────────────────────────────


def test_issuing_decree_primero_not_article_heading():
    """Artículo PRIMERO/SEGUNDO at the START of a law (issuing decree) must not
    render as ###### article headings when numeric articles follow.

    BEFORE: PRIMERO/SEGUNDO appeared as ###### Artículo PRIMERO.-
    AFTER:  They appear as body text under a "Decreto que expide esta Ley" section.
    """
    from legalize.transformer.markdown import render_paragraphs

    blocks = _diputados_doc_block_run([
        "Artículo PRIMERO.- Se expide la Ley X para quedar como sigue:",
        "Artículo SEGUNDO.- Se deroga la norma anterior.",
        "LEY X",
        "Artículo 1o.- Las disposiciones de esta Ley son de orden público.",
        "Artículo 2o.- Para efectos de esta Ley se entiende por:",
    ])

    full_md = "".join(render_paragraphs(b.versions[0].paragraphs) for b in blocks)

    # The issuing-decree section heading must appear
    assert "Decreto que expide esta Ley" in full_md, (
        "Missing 'Decreto que expide esta Ley' section heading"
    )
    # PRIMERO and SEGUNDO must NOT be ###### headings
    assert "###### Artículo PRIMERO" not in full_md, (
        "Artículo PRIMERO still appears as a ###### heading"
    )
    assert "###### Artículo SEGUNDO" not in full_md, (
        "Artículo SEGUNDO still appears as a ###### heading"
    )
    # Their text must still be present as prose
    assert "Se expide la Ley X" in full_md, "Issuing decree text missing"
    assert "Se deroga la norma anterior" in full_md, "Issuing decree text missing"

    # The main-law articles MUST still be ###### headings
    assert "###### Artículo 1o." in full_md, "Main law article 1o. missing as heading"
    assert "###### Artículo 2o." in full_md, "Main law article 2o. missing as heading"


def test_issuing_decree_only_triggers_with_numeric_articles():
    """When a law's articles are ALL word-ordinal (rare), PRIMERO/SEGUNDO must remain
    as proper article headings (no issuing-decree detection).
    """
    blocks = _diputados_doc_block_run([
        "Artículo PRIMERO.- Primera disposición.",
        "Artículo SEGUNDO.- Segunda disposición.",
        "Artículo TERCERO.- Tercera disposición.",
    ])

    # No numeric articles → no issuing decree section
    section_blocks = [b for b in blocks if b.block_type == "section"]
    decreto_sections = [b for b in section_blocks if "Decreto" in b.title]
    assert not decreto_sections, (
        "Issuing-decree section should NOT appear when no numeric articles exist"
    )

    # PRIMERO/SEGUNDO/TERCERO must be regular article blocks
    art_blocks = [b for b in blocks if b.block_type == "article"]
    assert len(art_blocks) == 3, f"Expected 3 article blocks, got {len(art_blocks)}"


def test_issuing_decree_section_heading_emitted_once():
    """The 'Decreto que expide esta Ley' heading is emitted exactly once,
    before the first PRIMERO article.
    """
    blocks = _diputados_doc_block_run([
        "Artículo PRIMERO.- Se expide la Ley Y.",
        "Artículo SEGUNDO.- Disposición secundaria.",
        "Artículo 1o.- Texto principal.",
        "Artículo 2o.- Más texto.",
    ])

    decreto_sections = [
        b for b in blocks
        if b.block_type == "section" and "Decreto que expide" in b.title
    ]
    assert len(decreto_sections) == 1, (
        f"Expected exactly 1 'Decreto que expide' section, got {len(decreto_sections)}"
    )

    # Confirm ordering: decreto section comes before first main article
    decreto_idx = next(i for i, b in enumerate(blocks) if "Decreto que expide" in b.title)
    first_art_idx = next(i for i, b in enumerate(blocks) if b.id.startswith("art-1o-"))
    assert decreto_idx < first_art_idx, (
        "Decreto section must appear before the first main-law article"
    )


# ── Decreto-tail grouping tests ──────────────────────────────────────────────


def _diputados_doc_block_run(paragraphs: list[str]):
    """Helper: drive the DOC block builder with a synthetic paragraph list.

    Patches ``_extract_doc_paragraphs`` to return the given list so no real
    .doc file is needed.  Returns the built blocks.
    """
    import base64

    from legalize.fetcher.mx import parser as mx_parser

    real = mx_parser._extract_doc_paragraphs
    mx_parser._extract_doc_paragraphs = lambda _b: paragraphs
    try:
        envelope = {
            "source": "diputados",
            "source_format": "doc",
            "norm_id": "DIP-TEST",
            "abbrev": "TEST",
            "title": "Ley de Prueba",
            "rank": "ley",
            "publication_date": "2020-01-01",
            "last_reform_date": "2024-06-15",
            "doc_url": "https://example.test/TEST.doc",
            "doc_b64": base64.b64encode(b"\xd0\xcf\x11\xe0stub").decode("ascii"),
        }
        return mx_parser._diputados_doc_blocks(envelope)
    finally:
        mx_parser._extract_doc_paragraphs = real


def test_decreto_tail_trigger_switches_mode():
    """After ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA, no new art- blocks."""
    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Texto del artículo principal.",
        "ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA",
        "DECRETO por el que se reforma el artículo 1o.",
        "TRANSITORIOS",
        "Artículo Primero.- Este decreto entrará en vigor.",
        "Artículo Segundo.- Se abroga la norma anterior.",
    ])
    # Only the main article should be an art- block
    art_blocks = [b for b in blocks if b.block_type == "article" and b.id.startswith("art-")]
    assert len(art_blocks) == 1
    assert art_blocks[0].id.startswith("art-1o-")


def test_decreto_tail_articulo_not_an_article_heading():
    """Artículo PRIMERO/SEGUNDO inside decreto-tail must NOT become ###### headings."""
    from legalize.transformer.markdown import render_paragraphs

    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Cuerpo del artículo principal.",
        "ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA",
        "DECRETO por el que se reforma el artículo 1o.",
        "TRANSITORIOS",
        "Artículo Primero.- Este decreto entrará en vigor al día siguiente.",
        "Artículo Segundo.- Se abrogan las disposiciones contrarias.",
    ])

    # Render all blocks and check no ###### heading appears in the tail
    full_md = ""
    for b in blocks:
        full_md += render_paragraphs(b.versions[0].paragraphs)

    # The main article heading (###### Artículo 1o.) is expected
    assert "###### Artículo 1o." in full_md

    # Artículo Primero and Segundo from the decreto-tail must NOT be headings
    assert "###### Artículo Primero." not in full_md
    assert "###### Artículo Segundo." not in full_md

    # Their text must still appear as prose
    assert "Este decreto entrará en vigor" in full_md
    assert "Se abrogan las disposiciones contrarias" in full_md


def test_decreto_tail_decreto_lines_become_section_headings():
    """Each DECRETO line inside the tail renders as a #### section heading."""
    from legalize.transformer.markdown import render_paragraphs

    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Cuerpo del artículo principal.",
        "ARTÍCULOS TRANSITORIOS DE DECRETOS DE REFORMA",
        "DECRETO por el que se reforma el artículo 1o.",
        "TRANSITORIOS",
        "PRIMERO.- Este decreto entrará en vigor.",
        "DECRETO por el que se reforma el artículo 2o.",
        "TRANSITORIOS",
        "PRIMERO.- Este otro decreto también entrará en vigor.",
    ])

    section_blocks = [b for b in blocks if b.block_type == "section"]
    section_titles = [b.title for b in section_blocks]

    # The trigger heading
    assert any("DECRETOS DE REFORMA" in t for t in section_titles)
    # Both DECRETO lines become section headings
    assert any("artículo 1o" in t for t in section_titles)
    assert any("artículo 2o" in t for t in section_titles)

    # TRANSITORIOS inside decreto-tail become sub-section headings
    full_md = ""
    for b in blocks:
        full_md += render_paragraphs(b.versions[0].paragraphs)
    assert "##### TRANSITORIOS" in full_md


def test_decreto_tail_does_not_affect_main_transitorios():
    """Plain ARTÍCULOS TRANSITORIOS (without DE DECRETOS DE REFORMA) is unaffected."""
    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Cuerpo del artículo principal.",
        "ARTÍCULOS TRANSITORIOS",
        "Artículo Único.- Esta ley entrará en vigor.",
    ])

    # The transitorio must be an article block (not a decreto-body container)
    art_blocks = [b for b in blocks if b.block_type == "article"]
    unico_blocks = [b for b in art_blocks if "unico" in b.id]
    assert len(unico_blocks) == 1
    assert unico_blocks[0].id.startswith("art-unico-")


def test_decreto_tail_cpeum_fixture_no_spurious_headings():
    """On the real CPEUM.doc fixture, the decreto-tail produces no art- blocks past 136."""
    import base64

    from legalize.fetcher.mx.parser import _diputados_doc_blocks
    from legalize.transformer.markdown import render_paragraphs

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    envelope = {
        "source": "diputados",
        "source_format": "doc",
        "norm_id": "DIP-CPEUM",
        "abbrev": "CPEUM",
        "title": "Constitución Política de los Estados Unidos Mexicanos",
        "rank": "constitucion",
        "publication_date": "1917-02-05",
        "last_reform_date": "2026-04-10",
        "pdf_url": "https://www.diputados.gob.mx/LeyesBiblio/pdf/CPEUM.pdf",
        "doc_url": "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc",
        "doc_b64": base64.b64encode(doc_bytes).decode("ascii"),
    }
    blocks = _diputados_doc_blocks(envelope)

    # Main law articles only — no art- blocks should exist after the decreto-tail
    # trigger (which appears after the constitutional articles at ~block idx 132).
    main_law_art_seqs = [
        int(b.id.split("-")[-1])
        for b in blocks
        if b.block_type == "article" and b.id.startswith("art-")
    ]
    assert main_law_art_seqs, "No main law articles found"
    # All main law art- blocks must have been built before the decreto-tail trigger;
    # the maximum sequence number should be close to the real article count (~136).
    assert max(main_law_art_seqs) <= 150, (
        f"Suspiciously high max article sequence {max(main_law_art_seqs)} — "
        "decreto-tail articles may still be getting promoted to art- blocks"
    )

    # Render and confirm no ###### ordinal headings in the tail portion
    # (after the first DECRETO section heading).
    tail_start = next(
        i for i, b in enumerate(blocks)
        if b.block_type == "section" and "DECRETOS DE REFORMA" in b.title.upper()
    )
    tail_md = ""
    for b in blocks[tail_start:]:
        tail_md += render_paragraphs(b.versions[0].paragraphs)

    # Ordinal article headings must not appear in the decreto tail
    assert "###### Artículo Primero" not in tail_md
    assert "###### Artículo Segundo" not in tail_md
    assert "###### Artículo PRIMERO" not in tail_md
    assert "###### ARTICULO PRIMERO" not in tail_md


# ── Signal-6 garbage filter: short paragraph with no real Spanish word ───────


def test_binary_garbage_filters_ole2_fraccion_lookalike():
    """Signal 6: 'I J m!!!¦"È"É"k#l#w#¦#ïÝÝÕ…' is OLE2 garbage, not a fracción."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert _is_binary_garbage('I J m!!!¦“È“É“k#l#w#¦#ïÝÝÕË¿·°¿·¿¿·°°°¢¢')


def test_binary_garbage_filters_roman_numeral_field_code():
    """Signal 6: 'I A VIII.' is a Word field-code artifact, not a fracción body."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert _is_binary_garbage("I A VIII.")


def test_binary_garbage_passes_legitimate_fraccion_pudieren():
    """Signal 6 must NOT filter 'I Pudieren verse perjudicadas…' — real fracción text."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert not _is_binary_garbage(
        "I Pudieren verse perjudicadas en sus actividades por los actos u omisiones"
    )


def test_binary_garbage_passes_legitimate_fraccion_cualquier():
    """Signal 6 must NOT filter 'I Cualquier persona…' — real fracción text."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert not _is_binary_garbage(
        "I Cualquier persona que tenga conocimiento de los hechos denunciados"
    )


def test_binary_garbage_passes_section_heading():
    """Signal 6 must NOT filter section headings like 'ARTÍCULO 123' or 'CAPÍTULO I'."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert not _is_binary_garbage("ARTÍCULO 123")
    assert not _is_binary_garbage("CAPÍTULO I")
    assert not _is_binary_garbage("TÍTULO PRIMERO")
    assert not _is_binary_garbage("SECCIÓN PRIMERA")


# ── Word style-property revision strings (mH sH, CJPJaJ) ─────────────────────


def test_word_style_property_mH_sH_glued_to_CJ_is_garbage():
    """mH<TAB>sH glued to CJ style codes must be caught as field-code garbage.

    The old pattern used \\bmH\\s*sH\\b which requires a word boundary before 'mH'.
    In strings like 'CJmH\\tsH' the preceding 'J' is \\w so no \\b exists and the
    pattern silently failed.  These 5 strings are the exact values that leaked
    into exports/mx/ before the fix.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    # Exact strings from exports/mx/DIP-LDFEFM.md (lines 1041, 1043)
    assert _is_binary_garbage("Ä5CJmH\tsH\th1_h+"), "DIP-LDFEFM line 1041"
    assert _is_binary_garbage("ÄCJmH\tsH\th1_h+"), "DIP-LDFEFM line 1043"
    # Exact strings from exports/mx/DIP-LGS.md (lines 11963, 11965)
    assert _is_binary_garbage("G5CJPJaJmH\tsH\th)g×h"), "DIP-LGS line 11963"
    assert _is_binary_garbage("GCJPJaJmH\tsH\th"), "DIP-LGS line 11965"
    # Exact string from exports/mx/DIP-LVGC.md (line 5403)
    assert _is_binary_garbage("5CJmH\tsH\th/hÊ"), "DIP-LVGC line 5403"


def test_word_style_property_CJPJaJ_alone_is_garbage():
    """CJPJaJ is a Word character-style code that never appears in Spanish prose."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert _is_binary_garbage("CJPJaJ")
    assert _is_binary_garbage("6B*CJPJaJphÿ")


def test_mH_sH_with_space_separator_still_caught():
    """mH<SPACE>sH (space, not tab) must also be caught — the original test case."""
    from legalize.fetcher.mx.parser import _is_binary_garbage

    assert _is_binary_garbage("mH sH")
    assert _is_binary_garbage("SomeCJmH sH extra")


def test_legitimate_spanish_with_mH_letters_not_dropped():
    """Lowercase 'mh'/'sh' in ordinary Spanish must NOT be falsely flagged.

    The pattern mH\\s+sH is case-sensitive so words containing lowercase 'mh'
    or 'sh' sequences in authentic legislative text are safe.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    # 'flashback' contains 'sh' (lowercase) — should not match mH sH (CamelCase)
    assert not _is_binary_garbage(
        "Se realizó un flashback histórico del texto legislativo."
    )
    # Normal Spanish legislative prose
    assert not _is_binary_garbage(
        "En los Estados Unidos Mexicanos todas las personas gozarán de los derechos humanos."
    )
    # Reform stamp with no garbage tokens
    assert not _is_binary_garbage("Párrafo reformado DOF 04-12-2006, 10-06-2011")


# ── Bug 1 — trailing Word XML/CSS metadata tail (CCF/CFF/LFT) ────────────────


def test_cj_caret_j_garbage_filtered():
    """Bug 1: CJ^J / 5CJ^J / CJPJ^JaJ Word stylesheet tokens must be garbage.

    These appear in the binary tail of CCF.doc, LFT.doc, and similar files
    where the Word application appends character-style property strings after
    the last real text paragraph.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    # Exact strings from CCF.doc tail
    assert _is_binary_garbage(
        "óçóÚóÍóÚóÚÀ¶©¶Àynyaynya¶aynyah<X¢hÃCJ^JaJhAtÊ5CJ^JaJh<X¢hÃ5CJ^J"
    ), "CCF CJ^J tail garbage not filtered"

    # Exact strings from CFF.doc tail (B*CJ + CJPJ]aJ)
    assert _is_binary_garbage(
        "ìhPb6B*CJPJ]aJphÌ3!hPb6B*CJPJ]aJphÌ3"
    ), "CFF B*CJPJ]aJ tail garbage not filtered"

    # Exact strings from LFT.doc tail (CJPJ^JaJ)
    assert _is_binary_garbage(
        "h9(CJ^Jh9(h9(CJ^JaJh9(CJPJ^JaJh)^"
    ), "LFT CJPJ^JaJ tail garbage not filtered"


def test_cj_garbage_does_not_filter_spanish_prose():
    """Bug 1: The CJ^J/CJPJ filter must NOT drop legitimate Spanish text.

    The added patterns are highly specific to Word stylesheet tokens and must
    not produce false positives on ordinary accented Spanish text.
    """
    from legalize.fetcher.mx.parser import _is_binary_garbage

    clean_paras = [
        # Normal constitutional article
        "En los Estados Unidos Mexicanos todas las personas gozarán de los derechos humanos.",
        # Article heading with ordinal
        "Artículo 1o.- La presente Ley es de orden público.",
        # Reform stamp with DOF date
        "Párrafo reformado DOF 04-12-2006, 10-06-2011",
        # Signatory line
        "Ciudad de México, a 7 de abril de 2026.- Dip. Kenia López Rabadán, Presidenta.",
        # Promulgation line
        "En cumplimiento de lo dispuesto por la fracción I del Artículo 89 de la Constitución.",
    ]
    for para in clean_paras:
        assert not _is_binary_garbage(para), (
            f"Clean paragraph incorrectly flagged as garbage by CJ-filter: {para!r}"
        )


def test_tail_blob_truncation_removes_trailing_garbage():
    """Bug 1: _truncate_tail_blob must strip trailing binary-garbage paragraphs.

    The function should truncate at the last legitimate paragraph and drop
    everything after it when the tail blob is >= 1 paragraph long.
    """
    from legalize.fetcher.mx.parser import _truncate_tail_blob

    good_paras = [
        "Artículo 1o.- Esta ley es de orden público.",
        "Ciudad de México, a 01 de enero de 2025.- Secretaría.- Rúbrica.",
        "En cumplimiento de lo dispuesto por la fracción I del Artículo 89.",
    ]
    garbage_tail = [
        "h9(CJ^JaJh)^",
        "hAtÊ5CJ^JaJh",
        "ìhPb6B*CJPJ]aJphÌ3",
        "´5CJ\\aJh*8",
        "hJgD5CJ",
        "hD5CJ(`bbb",
    ]
    result = _truncate_tail_blob(good_paras + garbage_tail)
    # All garbage must be removed; the legitimate text must survive.
    assert result == good_paras, (
        f"Tail blob not truncated correctly: last para = {repr(result[-1][:80]) if result else 'EMPTY'}"
    )


def test_tail_blob_does_not_cut_legitimate_text():
    """Bug 1: _truncate_tail_blob must not cut a document that ends cleanly.

    A normal document whose last paragraph is a Spanish promulgation line
    must come out unchanged.
    """
    from legalize.fetcher.mx.parser import _truncate_tail_blob

    clean_document = [
        "Artículo 1o.- Esta ley es de orden público.",
        "Artículo 2o.- Las personas físicas y morales…",
        "TRANSITORIOS",
        "Único.- La presente ley entrará en vigor al día siguiente.",
        "Ciudad de México, a 01 de enero de 2025.- Sen. X, Presidenta.- Rúbrica.",
        "En cumplimiento de lo dispuesto por la fracción I del Artículo 89.",
    ]
    result = _truncate_tail_blob(clean_document)
    assert result == clean_document, "Tail truncation incorrectly cut a clean document"


def test_cff_and_lft_doc_no_cj_garbage_via_cache():
    """Bug 1: CCF.doc, CFF.doc, and LFT.doc must produce 0 CJ-style garbage paragraphs.

    Uses the HTTP cache (no network) to verify the fix against the real DOC files.
    Skipped when the HTTP cache is not available.
    """
    import pytest
    requests_cache = pytest.importorskip("requests_cache")

    cache_path = ".cache/http_cache.sqlite"
    import os
    if not os.path.exists(cache_path):
        pytest.skip("HTTP cache not available")

    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    session = requests_cache.CachedSession(
        cache_path, backend="sqlite", expire_after=requests_cache.NEVER_EXPIRE
    )

    for abbrev in ("CCF", "CFF", "LFT"):
        url = f"https://www.diputados.gob.mx/LeyesBiblio/doc/{abbrev}.doc"
        resp = session.get(url)
        if not getattr(resp, "from_cache", True):
            pytest.skip(f"{abbrev}.doc not in cache — skipping to avoid network")
        paras = _extract_doc_paragraphs(resp.content)
        cj_garbage = [
            p for p in paras
            if "CJ^J" in p or "CJPJ" in p or "B*CJ" in p or "CJaJ" in p
        ]
        assert not cj_garbage, (
            f"{abbrev}: {len(cj_garbage)} CJ-garbage paragraph(s) survived the filter. "
            f"First: {repr(cj_garbage[0][:100])}"
        )


# ── Bug 2 — Word table cell markers (BEL / \\x07) rendered as pipe tables ─────


def test_word_table_bel_produces_pipe_table():
    """Bug 2: A Word binary table segment with BEL cell markers becomes a pipe table.

    The synthetic input mirrors the exact byte pattern of the CFF.doc tax tarifa
    table: a header row followed by data rows, each row terminated by double-BEL.
    """
    from legalize.fetcher.mx.parser import _word_table_to_markdown

    # Exact pattern from CFF.doc: Ejercicio\x07Por ciento\x07\x071996\x0712.50\x07\x07...
    raw = "Ejercicio\x07Por ciento\x07\x071996\x0712.50\x07\x071997\x0712.50\x07\x071998\x0712.50\x07\x071999\x0710.00\x07\x07"
    result = _word_table_to_markdown(raw)
    assert result is not None, "_word_table_to_markdown returned None for valid table"
    lines = result.splitlines()
    assert lines[0] == "| Ejercicio | Por ciento |", f"Header row wrong: {lines[0]!r}"
    assert lines[1] == "| --- | --- |", f"Separator row wrong: {lines[1]!r}"
    assert "| 1996 | 12.50 |" in lines, "Data row 1996/12.50 missing"
    assert "| 1999 | 10.00 |" in lines, "Data row 1999/10.00 missing"
    assert len(lines) == 6, f"Expected 6 lines (header + sep + 4 data rows), got {len(lines)}"


def test_word_table_bel_garbage_not_emitted():
    """Bug 2: A BEL-containing segment that is OLE2 binary garbage must return None.

    Word drawing-object and style-sheet segments also contain BEL bytes.
    They must not be converted to pipe tables — the validity check must reject them.
    """
    from legalize.fetcher.mx.parser import _word_table_to_markdown

    garbage_segments = [
        # CJ^J style-sheet token in first cell (caught by field-code RE)
        "5CJ^JaJhAtÊ\x07h<X¢hÃCJ^JaJ\x07\x07",
        # Drawing-object coordinate data (only non-Latin chars in cells — garbage check)
        "ôôôô\x07ÖÖÖÖ\x07\x07ÎÎÎÎ\x07ÜÜÜÜ\x07\x07",
        # Empty segment
        "\x07\x07\x07",
    ]
    for seg in garbage_segments:
        result = _word_table_to_markdown(seg)
        assert result is None, (
            f"Binary garbage segment produced a table: {repr(seg[:60])!r}"
        )


def test_cff_tax_tarifa_table_rendered_as_pipe_table():
    """Bug 2: CFF.doc tax tarifa table at article 66 must render as a Markdown pipe table.

    The BEFORE state (pre-fix) collapsed cells into prose: "EjercicioPor
    ciento199612.50199712.50…".  The AFTER state must be a proper pipe table.
    Uses the HTTP cache (no network).
    """
    import pytest
    requests_cache = pytest.importorskip("requests_cache")

    cache_path = ".cache/http_cache.sqlite"
    import os
    if not os.path.exists(cache_path):
        pytest.skip("HTTP cache not available")

    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    session = requests_cache.CachedSession(
        cache_path, backend="sqlite", expire_after=requests_cache.NEVER_EXPIRE
    )
    resp = session.get("https://www.diputados.gob.mx/LeyesBiblio/doc/CFF.doc")
    if not getattr(resp, "from_cache", True):
        pytest.skip("CFF.doc not in cache — skipping to avoid network")

    paras = _extract_doc_paragraphs(resp.content)

    # BEFORE: "EjercicioPor ciento199612.50..." (collapsed prose) — must NOT appear.
    collapsed = [p for p in paras if "EjercicioPor ciento" in p]
    assert not collapsed, (
        "Tax tarifa table still collapsed into prose — Bug 2 not fixed"
    )

    # AFTER: a pipe table containing the ejercicio header and 1996/12.50 row.
    tax_table = [p for p in paras if "Ejercicio" in p and "Por ciento" in p]
    assert tax_table, "Tax tarifa table not found in CFF paragraphs"
    table_para = tax_table[0]
    assert table_para.startswith("|"), f"Tax table not a pipe table: {repr(table_para[:80])}"
    assert "| Ejercicio | Por ciento |" in table_para, "Header row missing from tax table"
    assert "| 1996 | 12.50 |" in table_para or "1996" in table_para, (
        "Data row 1996/12.50 missing from tax table"
    )


# ── DOF reform extraction ────────────────────────────────────────────────────


def test_extract_reforms_single_date():
    """extract_reforms returns one bootstrap + one reform for a law with one DOF stamp."""
    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Texto original.",
        "Párrafo reformado DOF 14-08-2001",
    ])
    from legalize.fetcher.mx.parser import _extract_dof_reforms_from_blocks
    from datetime import date

    pub_date = date(1980, 1, 1)
    reforms = _extract_dof_reforms_from_blocks(blocks, "DIP-TEST", pub_date)

    assert len(reforms) == 2, f"Expected 2 reforms, got {len(reforms)}"
    assert reforms[0].date == pub_date, "First reform must be the bootstrap/publication date"
    assert reforms[0].norm_id == "DIP-TEST", "Bootstrap norm_id must be the law's own ID"
    assert reforms[1].date == date(2001, 8, 14), "Second reform must be the DOF date"
    assert reforms[1].norm_id == "DIP-TEST-DOF-2001-08-14"


def test_extract_reforms_multi_date_stamp():
    """Multi-date stamp 'DOF 04-12-2006, 10-06-2011' produces two separate reforms."""
    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Texto original.",
        "Párrafo reformado DOF 04-12-2006, 10-06-2011",
    ])
    from legalize.fetcher.mx.parser import _extract_dof_reforms_from_blocks
    from datetime import date

    reforms = _extract_dof_reforms_from_blocks(blocks, "DIP-TEST", date(1980, 1, 1))

    assert len(reforms) == 3  # bootstrap + 2006 + 2011
    dates = [r.date for r in reforms]
    assert date(2006, 12, 4) in dates
    assert date(2011, 6, 10) in dates


def test_extract_reforms_no_stamps_returns_single_bootstrap():
    """A law with no reform stamps gets exactly one bootstrap reform."""
    blocks = _diputados_doc_block_run([
        "Artículo 1o.- Esta ley es nueva y nunca ha sido reformada.",
        "Artículo 2o.- Disposición adicional.",
    ])
    from legalize.fetcher.mx.parser import _extract_dof_reforms_from_blocks
    from datetime import date

    reforms = _extract_dof_reforms_from_blocks(blocks, "DIP-TEST", date(2020, 3, 15))
    assert len(reforms) == 1
    assert reforms[0].norm_id == "DIP-TEST"
    assert reforms[0].date == date(2020, 3, 15)


def test_mx_text_parser_extract_reforms_via_envelope():
    """MXTextParser.extract_reforms returns multi-reform list from a DOC envelope."""
    import base64
    from datetime import date

    from legalize.fetcher.mx.parser import MXTextParser, _extract_doc_paragraphs

    # Inject a synthetic paragraph list with two reform stamps on different dates.
    synthetic_paras = [
        "Artículo 1o.- Texto original del artículo primero.",
        "Párrafo reformado DOF 14-08-2001",
        "Artículo 2o.- Texto original del artículo segundo.",
        "Fracción adicionada DOF 12-04-2019",
    ]

    import legalize.fetcher.mx.parser as mx_parser
    real_extract = mx_parser._extract_doc_paragraphs
    mx_parser._extract_doc_paragraphs = lambda _b: synthetic_paras
    try:
        envelope = {
            "source": "diputados",
            "source_format": "doc",
            "norm_id": "DIP-SYNTH",
            "abbrev": "SYNTH",
            "title": "Ley Sintética de Prueba",
            "rank": "ley",
            "publication_date": "1970-01-01",
            "last_reform_date": "2019-04-12",
            "doc_url": "https://example.test/SYNTH.doc",
            "doc_b64": base64.b64encode(b"\xd0\xcf\x11\xe0stub").decode("ascii"),
        }
        data = json.dumps(envelope).encode("utf-8")
        reforms = MXTextParser().extract_reforms(data)
    finally:
        mx_parser._extract_doc_paragraphs = real_extract

    assert len(reforms) == 3, f"Expected 3 reforms (bootstrap + 2001 + 2019), got {len(reforms)}"
    assert reforms[0].date == date(1970, 1, 1), "First reform = bootstrap"
    assert reforms[0].norm_id == "DIP-SYNTH"
    assert reforms[1].date == date(2001, 8, 14)
    assert reforms[1].norm_id == "DIP-SYNTH-DOF-2001-08-14"
    assert reforms[2].date == date(2019, 4, 12)
    assert reforms[2].norm_id == "DIP-SYNTH-DOF-2019-04-12"
    # Affected blocks must be populated.
    assert len(reforms[1].affected_blocks) >= 1
    assert reforms[1].affected_blocks[0].startswith("art-")


def test_extract_reforms_cpeum_fixture():
    """On the real CPEUM.doc fixture, extract_reforms yields many distinct reforms.

    The exact count varies between fixture versions but must be well above 1
    (the fixture version has ~240 unique DOF dates).
    """
    import base64
    from datetime import date

    from legalize.fetcher.mx.parser import MXTextParser

    doc_bytes = (FIXTURES / "CPEUM.doc").read_bytes()
    envelope = {
        "source": "diputados",
        "source_format": "doc",
        "norm_id": "DIP-CPEUM",
        "abbrev": "CPEUM",
        "title": "Constitución Política de los Estados Unidos Mexicanos",
        "rank": "constitucion",
        "publication_date": "1917-02-05",
        "last_reform_date": "2026-04-10",
        "doc_url": "https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc",
        "doc_b64": base64.b64encode(doc_bytes).decode("ascii"),
    }
    data = json.dumps(envelope).encode("utf-8")
    reforms = MXTextParser().extract_reforms(data)

    # The CPEUM is a heavily amended law; any correct extraction must find many reforms.
    assert len(reforms) >= 150, f"Expected ≥150 reforms from CPEUM fixture, got {len(reforms)}"
    assert reforms[0].date == date(1917, 2, 5), "First reform must be 1917-02-05 (publication)"
    assert reforms[0].norm_id == "DIP-CPEUM", "Bootstrap norm_id must be the law's own ID"
    # Verify a known reform date is present.
    reform_dates = {r.date for r in reforms}
    assert date(2011, 6, 10) in reform_dates, "2011-06-10 (human rights reform) must be present"
    # All reform norm_ids (except bootstrap) must follow the DIP-CPEUM-DOF-YYYY-MM-DD pattern.
    for r in reforms[1:]:
        assert r.norm_id.startswith("DIP-CPEUM-DOF-"), f"Unexpected norm_id: {r.norm_id}"
    # All affected_blocks must be populated (except possibly the bootstrap).
    for r in reforms[1:]:
        assert len(r.affected_blocks) >= 1, f"Reform {r.norm_id} has no affected blocks"


# ── Bug: repeated-short-tail garbage (Jáh-style Word stylesheet handles) ─────


def test_truncate_repeated_short_tail_removes_jah_run():
    """_truncate_repeated_short_tail must strip trailing runs of ≥3 identical
    short (≤10 char) paragraphs that contain at least one non-ASCII character.

    'Jáh' is a Word stylesheet handle stub: J=style-ref letter, á=accent char,
    h=handle prefix.  It passes _is_binary_garbage because it has only one
    non-ASCII char (á) and 'Jáh' qualifies as a 'real word' under signal 6.
    """
    from legalize.fetcher.mx.parser import _truncate_repeated_short_tail

    good_paras = [
        "Artículo 1o.- Esta ley es de orden público.",
        "Ciudad de México, a 01 de enero de 2025.- Rúbrica.",
        "En cumplimiento de lo dispuesto por la fracción I del Artículo 89.",
    ]
    # 10 repetitions of 'Jáh' — mirrors the Reg_Senado tail before fix.
    jah_tail = ["Jáh"] * 10
    result = _truncate_repeated_short_tail(good_paras + jah_tail)
    assert result == good_paras, (
        f"Jáh tail not removed. Last para: {repr(result[-1]) if result else 'EMPTY'}"
    )


def test_truncate_repeated_short_tail_reg_senado_via_cache():
    """Reg_Senado.doc must produce NO trailing 'Jáh' run after _extract_doc_paragraphs.

    Uses the HTTP cache (no network).  Skipped when the cache is not available.
    """
    import os

    import pytest
    requests_cache = pytest.importorskip("requests_cache")

    cache_path = ".cache/http_cache.sqlite"
    if not os.path.exists(cache_path):
        pytest.skip("HTTP cache not available")

    from legalize.fetcher.mx.parser import _extract_doc_paragraphs

    session = requests_cache.CachedSession(
        cache_path, backend="sqlite", expire_after=requests_cache.NEVER_EXPIRE
    )
    url = "https://www.diputados.gob.mx/LeyesBiblio/doc/Reg_Senado.doc"
    resp = session.get(url)
    if not getattr(resp, "from_cache", True):
        pytest.skip("Reg_Senado.doc not in cache — skipping to avoid network")

    paras = _extract_doc_paragraphs(resp.content)

    # Inspect the last 30 paragraphs for repeated 'Jáh' or similar short garbage.
    tail_30 = paras[-30:] if len(paras) >= 30 else paras
    jah_count = tail_30.count("Jáh")
    assert jah_count == 0, (
        f"'Jáh' still appears {jah_count} time(s) in the last 30 paragraphs — "
        "repeated-short-tail truncation did not fire"
    )

    # The last paragraph must be recognisable legislative text (> 20 chars).
    assert paras, "No paragraphs extracted from Reg_Senado.doc"
    last = paras[-1]
    assert len(last) > 20, (
        f"Last paragraph is suspiciously short after fix: {repr(last)}"
    )
