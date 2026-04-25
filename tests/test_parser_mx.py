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
    assert set(client.sources) == {"diputados", "dof", "ojn", "sjf", "unam", "justia"}


def test_source_for_routes_by_prefix():
    client = MXClient()
    assert client.source_for("DOF-2024-001").name == "dof"
    assert client.source_for("DIP-CPEUM").name == "diputados"
    assert client.source_for("JUSTIA-CDMX-CIVIL").name == "justia"


def test_source_for_unknown_prefix_raises():
    client = MXClient()
    with pytest.raises(ValueError, match="No MX source registered"):
        client.source_for("XYZ-123")


def test_source_kinds():
    client = MXClient()
    kinds = {name: src.kind for name, src in client.sources.items()}
    assert kinds["sjf"] == "case_law"
    assert kinds["unam"] == "doctrine"
    assert kinds["justia"] == "aggregator"


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
    assert meta.source.endswith("/CPEUM.pdf")
    extra = dict(meta.extra)
    assert extra["abbrev"] == "CPEUM"
    assert extra["last_reform_dof"] == "2026-04-10"


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
