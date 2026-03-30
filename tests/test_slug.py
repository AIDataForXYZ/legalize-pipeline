"""Tests for file path generation."""

from datetime import date

from legalize.models import EstadoNorma, NormaMetadata, Rango
from legalize.transformer.slug import norma_to_filepath


def _make_metadata(
    identificador: str = "BOE-A-2024-1",
    pais: str = "es",
    rango: Rango = Rango.LEY,
    jurisdiccion: str | None = None,
) -> NormaMetadata:
    return NormaMetadata(
        titulo="Test",
        titulo_corto="Test",
        identificador=identificador,
        pais=pais,
        rango=rango,
        fecha_publicacion=date(2024, 1, 1),
        estado=EstadoNorma.VIGENTE,
        departamento="Test",
        fuente="https://example.com",
        jurisdiccion=jurisdiccion,
    )


class TestNormaToFilepath:
    def test_state_level_uses_pais(self):
        meta = _make_metadata("BOE-A-2015-11430")
        assert norma_to_filepath(meta) == "es/BOE-A-2015-11430.md"

    def test_ccaa_uses_jurisdiccion(self):
        meta = _make_metadata("BOE-A-2020-615", jurisdiccion="es-pv")
        assert norma_to_filepath(meta) == "es-pv/BOE-A-2020-615.md"

    def test_france(self):
        meta = _make_metadata("JORF-001", pais="fr")
        assert norma_to_filepath(meta) == "fr/JORF-001.md"

    def test_filename_is_identificador(self):
        meta = _make_metadata("BOE-A-1978-31229")
        assert norma_to_filepath(meta).endswith("BOE-A-1978-31229.md")

    def test_no_rango_subfolder(self):
        """Rango does not affect the path — it's in the YAML frontmatter."""
        meta1 = _make_metadata("BOE-A-1978-31229", rango=Rango.CONSTITUCION)
        meta2 = _make_metadata("BOE-A-1978-31229", rango=Rango.LEY)
        assert norma_to_filepath(meta1) == norma_to_filepath(meta2)
