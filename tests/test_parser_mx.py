"""Mexico fetcher — multi-source scaffold tests.

Pin the registry contract, the multi-source routing, and the
NotImplementedError stubs while the per-source clients/parsers get wired up.
"""

import pytest

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.mx.client import DEFAULT_SOURCES, MXClient
from legalize.fetcher.mx.parser import MXMetadataParser, MXTextParser


def test_registry_dispatch():
    text_parser = get_text_parser("mx")
    metadata_parser = get_metadata_parser("mx")
    assert isinstance(text_parser, MXTextParser)
    assert isinstance(metadata_parser, MXMetadataParser)


def test_default_sources_loaded():
    client = MXClient()
    assert set(client.sources) == {"diputados", "dof", "ojn", "sjf", "unam", "justia"}
    assert client.sources["dof"].id_prefix == "DOF"


def test_source_for_routes_by_prefix():
    client = MXClient()
    assert client.source_for("DOF-2024-001").name == "dof"
    assert client.source_for("DIP-LFT").name == "diputados"
    assert client.source_for("OJN-CONST-1917").name == "ojn"
    assert client.source_for("SJF-TESIS-2024-12345").name == "sjf"
    assert client.source_for("UNAM-LIBRO-3421").name == "unam"
    assert client.source_for("JUSTIA-CDMX-CIVIL").name == "justia"


def test_source_kinds():
    client = MXClient()
    kinds = {name: src.kind for name, src in client.sources.items()}
    assert kinds["diputados"] == "primary_legislation"
    assert kinds["dof"] == "primary_legislation"
    assert kinds["ojn"] == "primary_legislation"
    assert kinds["sjf"] == "case_law"
    assert kinds["unam"] == "doctrine"
    assert kinds["justia"] == "aggregator"


def test_source_for_unknown_prefix_raises():
    client = MXClient()
    with pytest.raises(ValueError, match="No MX source registered"):
        client.source_for("XYZ-123")


def test_custom_sources_override_defaults():
    client = MXClient(sources={"local": {"base_url": "https://example.test", "id_prefix": "LCL"}})
    assert set(client.sources) == {"local"}
    assert client.source_for("LCL-1").base_url == "https://example.test"


def test_default_sources_have_required_fields():
    for name, conf in DEFAULT_SOURCES.items():
        assert "base_url" in conf, name
        assert "id_prefix" in conf, name


def test_text_parser_is_scaffold():
    with pytest.raises(NotImplementedError):
        MXTextParser().parse_text(b"", "DOF-2024-1")


def test_metadata_parser_is_scaffold():
    with pytest.raises(NotImplementedError):
        MXMetadataParser().parse(b"", "DOF-2024-1")
