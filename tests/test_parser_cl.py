"""Tests for the Chilean BCN parser."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from legalize.countries import get_metadata_parser, get_text_parser
from legalize.fetcher.cl.discovery import _parse_csv
from legalize.fetcher.cl.parser import CLMetadataParser, CLTextParser, _clean_text
from legalize.models import NormMetadata, NormStatus
from legalize.transformer.slug import norm_to_filepath

FIXTURES = Path(__file__).parent / "fixtures"


class TestCLTextParser:
    def setup_method(self):
        self.parser = CLTextParser()

    def test_parse_constitucion(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        # encabezado + chapter + 2 articles = 4 blocks
        assert len(blocks) == 4

    def test_encabezado_block(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        enc = blocks[0]
        assert enc.block_type == "encabezado"
        assert enc.id == "242302-encabezado"
        assert "CONSTITUCIÓN" in enc.versions[0].paragraphs[0].text

    def test_chapter_block(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        cap = blocks[1]
        assert cap.block_type == "capitulo"
        assert "BASES DE LA INSTITUCIONALIDAD" in cap.title

    def test_article_blocks(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        art1 = blocks[2]
        art2 = blocks[3]
        assert art1.block_type == "articulo"
        assert art1.title == "Artículo 1"
        assert art2.title == "Artículo 2"

    def test_article_has_paragraphs(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        art1 = blocks[2]
        assert len(art1.versions) == 1
        assert len(art1.versions[0].paragraphs) > 0
        assert "personas nacen libres" in art1.versions[0].paragraphs[0].text

    def test_version_date(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        art1 = blocks[2]
        assert art1.versions[0].publication_date == date(2005, 9, 22)

    def test_extract_reforms_returns_list(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        reforms = self.parser.extract_reforms(xml)
        assert isinstance(reforms, list)

    def test_derogado_law(self):
        xml = (FIXTURES / "bcn-dl-derogado.xml").read_bytes()
        blocks = self.parser.parse_text(xml)
        assert len(blocks) == 1
        assert blocks[0].block_type == "articulo"
        assert "único" in blocks[0].title


class TestCLMetadataParser:
    def setup_method(self):
        self.parser = CLMetadataParser()

    def test_parse_constitucion_metadata(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert isinstance(meta, NormMetadata)
        assert meta.country == "cl"
        assert meta.identifier == "CL-242302"

    def test_title(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert "CONSTITUCION POLITICA" in meta.title

    def test_short_title(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert "CONSTITUCION POLITICA" in meta.short_title

    def test_rank(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert str(meta.rank) == "decreto"

    def test_publication_date(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert meta.publication_date == date(2005, 9, 22)

    def test_in_force_status(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert meta.status == NormStatus.IN_FORCE

    def test_repealed_status(self):
        xml = (FIXTURES / "bcn-dl-derogado.xml").read_bytes()
        meta = self.parser.parse(xml, "6917")
        assert meta.status == NormStatus.REPEALED
        assert str(meta.rank) == "dl"

    def test_subjects(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert len(meta.subjects) == 2
        assert "Constitución 1980" in meta.subjects

    def test_department(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert "PRESIDENCIA" in meta.department

    def test_source_url(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert meta.source == "https://www.leychile.cl/Navegar?idNorma=242302"


class TestCountriesDispatch:
    def test_get_text_parser_cl(self):
        parser = get_text_parser("cl")
        assert isinstance(parser, CLTextParser)

    def test_get_metadata_parser_cl(self):
        parser = get_metadata_parser("cl")
        assert isinstance(parser, CLMetadataParser)


class TestCleanText:
    """Tests for margin annotation removal from BCN text."""

    def test_strips_cpr_margin_refs(self):
        raw = (
            "     La familia es el núcleo                  CPR Art. 1° D.O.\n"
            "fundamental de la sociedad.                                         24.10.1980"
        )
        cleaned = _clean_text(raw)
        assert "CPR Art." not in cleaned
        assert "24.10.1980" not in cleaned
        assert "familia" in cleaned
        assert "sociedad" in cleaned

    def test_strips_ley_margin_refs(self):
        raw = (
            "     El Estado reconoce y ampara a los grupos intermedios a         LEY N° 19.611 Art.\n"
            "través de los cuales se organiza y estructura la sociedad y         único\n"
            "les garantiza la adecuada autonomía para cumplir sus                Nº1 D.O. 16.06.1999"
        )
        cleaned = _clean_text(raw)
        assert "LEY N°" not in cleaned
        assert "16.06.1999" not in cleaned
        assert "Estado reconoce" in cleaned
        assert "autonomía" in cleaned

    def test_preserves_clean_text(self):
        raw = "     Artículo 1°.- Las personas nacen libres e iguales en\ndignidad y derechos."
        cleaned = _clean_text(raw)
        assert "personas nacen libres" in cleaned
        assert "dignidad y derechos" in cleaned

    def test_preserves_normal_law_text(self):
        raw = (
            '"Artículo 1°.- Introdúcense las siguientes modificaciones en la '
            "Ley N° 19.640, Orgánica Constitucional del Ministerio Público:"
        )
        cleaned = _clean_text(raw)
        assert cleaned.strip() == raw.strip()


class TestCSVParsing:
    """Tests for BCN CSV discovery response parsing."""

    def test_parse_csv_with_bom(self):
        # BCN returns UTF-8 BOM + quoted headers with semicolons.
        # The BOM is in the byte stream, not a character in the string.
        csv_bytes = (
            b'\xef\xbb\xbf"Identificaci\xc3\xb3n de la Norma";"Tipo Norma";"T\xc3\xadtulo de la Norma"\n'
            b'"1222840";"Ley";"MODIFICA DIVERSOS CUERPOS LEGALES"\n'
            b'"1222799";"Ley";"SOBRE CONVIVENCIA"'
        )
        rows = _parse_csv(csv_bytes)
        assert len(rows) == 2
        assert rows[0]["Identificación de la Norma"] == "1222840"
        assert rows[0]["Tipo Norma"] == "Ley"

    def test_parse_csv_empty(self):
        rows = _parse_csv(b"")
        assert rows == []

    def test_parse_csv_header_only(self):
        csv_bytes = '\ufeff"Identificación de la Norma";"Tipo Norma"\n'.encode("utf-8-sig")
        rows = _parse_csv(csv_bytes)
        assert rows == []

    def test_parse_csv_strips_bom_from_column_name(self):
        csv_bytes = ('\ufeff"Identificación de la Norma";"Tipo"\n"123";"Ley"').encode("utf-8")
        rows = _parse_csv(csv_bytes)
        assert "Identificación de la Norma" in rows[0]


class TestMetadataEdgeCases:
    def setup_method(self):
        self.parser = CLMetadataParser()

    def test_no_publication_date_falls_back_to_promulgation(self):
        xml = (FIXTURES / "bcn-dl-derogado.xml").read_bytes()
        meta = self.parser.parse(xml, "6917")
        # This fixture has fechaPromulgacion=1975-03-15 and fechaPublicacion=1975-04-01
        assert meta.publication_date == date(1975, 4, 1)

    def test_no_nombre_uso_comun(self):
        xml = (FIXTURES / "bcn-dl-derogado.xml").read_bytes()
        meta = self.parser.parse(xml, "6917")
        # DL fixture has no NombreUsoComun — short_title should fallback to title
        assert meta.short_title == meta.title

    def test_identifier_format(self):
        xml = (FIXTURES / "bcn-constitucion-sample.xml").read_bytes()
        meta = self.parser.parse(xml, "242302")
        assert meta.identifier == "CL-242302"
        assert meta.identifier.startswith("CL-")

    def test_source_url_format(self):
        xml = (FIXTURES / "bcn-dl-derogado.xml").read_bytes()
        meta = self.parser.parse(xml, "6917")
        assert "leychile.cl/Navegar?idNorma=6917" in meta.source


class TestSlugChile:
    def test_norm_path(self):
        meta = NormMetadata(
            title="Test",
            short_title="Test",
            identifier="CL-242302",
            country="cl",
            rank="decreto",
            publication_date=date(2005, 9, 22),
            status=NormStatus.IN_FORCE,
            department="BCN",
            source="https://www.leychile.cl/Navegar?idNorma=242302",
        )
        assert norm_to_filepath(meta) == "cl/CL-242302.md"
