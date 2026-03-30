"""Tests for the Austrian RIS parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.at.parser import RISMetadataParser, RISTextParser
from legalize.models import EstadoNorma, NormaMetadata
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


class TestRISTextParser:
    def setup_method(self):
        self.parser = RISTextParser()

    def test_parse_nor_xml(self):
        xml = (FIXTURES / "ris-nor-NOR12030057.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        assert len(blocks) == 1
        block = blocks[0]
        assert block.id == "NOR12030057"
        assert "§ 1" in block.titulo
        assert len(block.versions) == 1

    def test_version_has_paragraphs(self):
        xml = (FIXTURES / "ris-nor-NOR12030057.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        version = blocks[0].versions[0]
        assert len(version.paragraphs) > 0

    def test_version_date(self):
        xml = (FIXTURES / "ris-nor-NOR12030057.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        version = blocks[0].versions[0]
        assert version.fecha_publicacion == date(1975, 1, 17)

    def test_extract_reforms_returns_list(self):
        xml = (FIXTURES / "ris-nor-NOR12030057.xml").read_bytes()
        reforms = self.parser.extract_reforms(xml)
        assert isinstance(reforms, list)


class TestRISMetadataParser:
    def setup_method(self):
        self.parser = RISMetadataParser()

    def test_parse_metadata(self):
        json_data = (FIXTURES / "ris-metadata-10002333.json").read_bytes()
        meta = self.parser.parse(json_data, "10002333")
        assert isinstance(meta, NormaMetadata)
        assert meta.pais == "at"
        assert meta.identificador == "AT-10002333"

    def test_rank(self):
        json_data = (FIXTURES / "ris-metadata-10002333.json").read_bytes()
        meta = self.parser.parse(json_data, "10002333")
        assert str(meta.rango) == "verordnung"

    def test_short_title(self):
        json_data = (FIXTURES / "ris-metadata-10002333.json").read_bytes()
        meta = self.parser.parse(json_data, "10002333")
        assert "Produktdeklaration" in meta.titulo_corto

    def test_repealed_status(self):
        json_data = (FIXTURES / "ris-metadata-10002333.json").read_bytes()
        meta = self.parser.parse(json_data, "10002333")
        assert meta.estado == EstadoNorma.DEROGADA


class TestCountriesDispatch:
    def test_get_text_parser_at(self):
        parser = get_text_parser("at")
        assert isinstance(parser, RISTextParser)

    def test_get_metadata_parser_at(self):
        parser = get_metadata_parser("at")
        assert isinstance(parser, RISMetadataParser)


class TestSlugAustria:
    def test_norm_path(self):
        meta = NormaMetadata(
            titulo="Test",
            titulo_corto="Test",
            identificador="AT-10002333",
            pais="at",
            rango="verordnung",
            fecha_publicacion=date(1975, 1, 17),
            estado=EstadoNorma.VIGENTE,
            departamento="BKA",
            fuente="https://ris.bka.gv.at",
        )
        assert norm_to_filepath(meta) == "at/AT-10002333.md"
