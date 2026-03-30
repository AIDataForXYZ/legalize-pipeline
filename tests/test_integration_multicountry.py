"""Integration tests for multi-country generic pipeline.

Tests that commit_one, commit_all, and storage round-trip work
correctly for norms from different countries (ES, FR, SE).
No HTTP calls — all data is synthetic NormaCompleta objects.
"""

import subprocess
from datetime import date
from pathlib import Path

import pytest

from legalize.config import Config, GitConfig
from legalize.models import (
    Bloque,
    EstadoNorma,
    NormaCompleta,
    NormaMetadata,
    Paragraph,
    Rango,
    Reform,
    Version,
)
from legalize.pipeline import commit_all, commit_one
from legalize.storage import load_norma_from_json, save_structured_json
from legalize.transformer.slug import norma_to_filepath


# ─────────────────────────────────────────────
# Helpers — synthetic norm builders
# ─────────────────────────────────────────────


def _make_version(source_id: str, d: date, text: str) -> Version:
    return Version(
        id_norma=source_id,
        fecha_publicacion=d,
        fecha_vigencia=d,
        paragraphs=(Paragraph(css_class="parrafo", text=text),),
    )


def _make_block(block_id: str, titulo: str, versions: list[Version]) -> Bloque:
    return Bloque(
        id=block_id,
        tipo="precepto",
        titulo=titulo,
        versions=tuple(versions),
    )


def _make_norm_es() -> NormaCompleta:
    """Spanish norm with 2 blocks, 2 reforms (original + amendment)."""
    d1 = date(2000, 1, 15)
    d2 = date(2010, 6, 20)
    src1 = "BOE-A-2000-100"
    src2 = "BOE-A-2010-500"

    blocks = [
        _make_block("a1", "Artículo 1", [
            _make_version(src1, d1, "Texto original del artículo 1."),
            _make_version(src2, d2, "Texto reformado del artículo 1."),
        ]),
        _make_block("a2", "Artículo 2", [
            _make_version(src1, d1, "Texto original del artículo 2."),
        ]),
        _make_block("a3", "Artículo 3", [
            _make_version(src1, d1, "Texto original del artículo 3."),
        ]),
    ]

    reforms = [
        Reform(fecha=d1, id_norma=src1, bloques_afectados=("a1", "a2", "a3")),
        Reform(fecha=d2, id_norma=src2, bloques_afectados=("a1",)),
    ]

    metadata = NormaMetadata(
        titulo="Ley de Prueba Española",
        titulo_corto="Ley de Prueba",
        identificador="BOE-A-2000-100",
        pais="es",
        rango=Rango.LEY,
        fecha_publicacion=d1,
        estado=EstadoNorma.VIGENTE,
        departamento="Ministerio de Justicia",
        fuente="https://www.boe.es/eli/es/l/2000/01/15/1",
    )

    return NormaCompleta(metadata=metadata, bloques=tuple(blocks), reforms=tuple(reforms))


def _make_norm_fr() -> NormaCompleta:
    """French norm with 2 blocks, 2 reforms."""
    d1 = date(1804, 3, 21)
    d2 = date(2016, 10, 1)
    src1 = "LEGITEXT000006070721"
    src2 = "JORFTEXT000033202746"

    blocks = [
        _make_block("art1", "Article 1", [
            _make_version(src1, d1, "Toute personne est capable de contracter."),
            _make_version(src2, d2, "Toute personne physique et morale est capable de contracter."),
        ]),
        _make_block("art2", "Article 2", [
            _make_version(src1, d1, "La loi ne dispose que pour l'avenir."),
        ]),
        _make_block("art3", "Article 3", [
            _make_version(src1, d1, "Les lois de police et de sûreté obligent tous."),
        ]),
    ]

    reforms = [
        Reform(fecha=d1, id_norma=src1, bloques_afectados=("art1", "art2", "art3")),
        Reform(fecha=d2, id_norma=src2, bloques_afectados=("art1",)),
    ]

    metadata = NormaMetadata(
        titulo="Code civil",
        titulo_corto="Code civil",
        identificador="LEGITEXT000006070721",
        pais="fr",
        rango=Rango.CODE,
        fecha_publicacion=d1,
        estado=EstadoNorma.VIGENTE,
        departamento="Ministère de la Justice",
        fuente="https://www.legifrance.gouv.fr/codes/texte_lc/LEGITEXT000006070721",
    )

    return NormaCompleta(metadata=metadata, bloques=tuple(blocks), reforms=tuple(reforms))


def _make_norm_se() -> NormaCompleta:
    """Swedish norm with 3 blocks, 2 reforms."""
    d1 = date(1962, 1, 1)
    d2 = date(2020, 7, 1)
    src1 = "SFS-1962-700"
    src2 = "SFS-2020-321"

    blocks = [
        _make_block("kap1p1", "1 kap. 1 §", [
            _make_version(src1, d1, "Brottsbalken gäller för brott som begås i Sverige."),
            _make_version(src2, d2, "Brottsbalken gäller för brott begångna inom riket."),
        ]),
        _make_block("kap1p2", "1 kap. 2 §", [
            _make_version(src1, d1, "Straff skall bestämmas efter lag."),
        ]),
        _make_block("kap1p3", "1 kap. 3 §", [
            _make_version(src1, d1, "Den som begår brott under påverkan av alkohol döms."),
        ]),
    ]

    reforms = [
        Reform(fecha=d1, id_norma=src1, bloques_afectados=("kap1p1", "kap1p2", "kap1p3")),
        Reform(fecha=d2, id_norma=src2, bloques_afectados=("kap1p1",)),
    ]

    metadata = NormaMetadata(
        titulo="Brottsbalk",
        titulo_corto="Brottsbalk",
        identificador="SFS-1962-700",
        pais="se",
        rango=Rango("lag"),
        fecha_publicacion=d1,
        estado=EstadoNorma.VIGENTE,
        departamento="Justitiedepartementet",
        fuente="https://www.riksdagen.se/sv/dokument-och-lagar/dokument/svensk-forfattningssamling/brottsbalk-1962700_sfs-1962-700/",
    )

    return NormaCompleta(metadata=metadata, bloques=tuple(blocks), reforms=tuple(reforms))


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────


@pytest.fixture
def test_config(tmp_path) -> Config:
    """Config with temporary repo and data dir."""
    return Config(
        git=GitConfig(repo_path=str(tmp_path / "repo")),
        data_dir=str(tmp_path / "data"),
        state_path=str(tmp_path / "state.json"),
        mappings_path=str(tmp_path / "mappings.json"),
    )


def _save_norm(config: Config, norm: NormaCompleta) -> Path:
    """Save a norm to JSON and return the path."""
    return save_structured_json(config.data_dir, norm)


def _git_log_dates(repo_path: str) -> list[str]:
    """Get commit dates in chronological order."""
    result = subprocess.run(
        ["git", "log", "--format=%ai", "--reverse"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    return [line.split()[0] for line in result.stdout.strip().splitlines() if line.strip()]


def _git_log_bodies(repo_path: str) -> list[str]:
    """Get all commit bodies."""
    result = subprocess.run(
        ["git", "log", "--format=%B---SEPARATOR---", "--reverse"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    bodies = result.stdout.split("---SEPARATOR---")
    return [b.strip() for b in bodies if b.strip()]


def _git_commit_count(repo_path: str) -> int:
    """Count total commits."""
    result = subprocess.run(
        ["git", "rev-list", "--count", "HEAD"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return 0
    return int(result.stdout.strip())


# ─────────────────────────────────────────────
# TestGenericCommitMultiCountry
# ─────────────────────────────────────────────


class TestGenericCommitMultiCountry:
    """Test that commit_one works correctly with JSON files from different countries."""

    @pytest.mark.parametrize(
        "make_norm,expected_dir",
        [
            (_make_norm_es, "es"),
            (_make_norm_fr, "fr"),
            (_make_norm_se, "se"),
        ],
        ids=["spain", "france", "sweden"],
    )
    def test_correct_number_of_commits(self, test_config, make_norm, expected_dir):
        norm = make_norm()
        _save_norm(test_config, norm)
        count = commit_one(test_config, norm.metadata.identificador)
        assert count == 2

    @pytest.mark.parametrize(
        "make_norm,expected_dir",
        [
            (_make_norm_es, "es"),
            (_make_norm_fr, "fr"),
            (_make_norm_se, "se"),
        ],
        ids=["spain", "france", "sweden"],
    )
    def test_markdown_file_at_correct_path(self, test_config, make_norm, expected_dir):
        norm = make_norm()
        _save_norm(test_config, norm)
        commit_one(test_config, norm.metadata.identificador)

        md_path = Path(test_config.git.repo_path) / expected_dir / f"{norm.metadata.identificador}.md"
        assert md_path.exists(), f"Expected {md_path} to exist"

        content = md_path.read_text(encoding="utf-8")
        assert norm.metadata.titulo_corto in content

    @pytest.mark.parametrize(
        "make_norm,expected_dir",
        [
            (_make_norm_es, "es"),
            (_make_norm_fr, "fr"),
            (_make_norm_se, "se"),
        ],
        ids=["spain", "france", "sweden"],
    )
    def test_frontmatter_has_correct_pais(self, test_config, make_norm, expected_dir):
        norm = make_norm()
        _save_norm(test_config, norm)
        commit_one(test_config, norm.metadata.identificador)

        md_path = Path(test_config.git.repo_path) / expected_dir / f"{norm.metadata.identificador}.md"
        content = md_path.read_text(encoding="utf-8")
        assert f'pais: "{norm.metadata.pais}"' in content

    @pytest.mark.parametrize(
        "make_norm,expected_dir",
        [
            (_make_norm_es, "es"),
            (_make_norm_fr, "fr"),
            (_make_norm_se, "se"),
        ],
        ids=["spain", "france", "sweden"],
    )
    def test_git_commits_have_correct_trailers(self, test_config, make_norm, expected_dir):
        norm = make_norm()
        _save_norm(test_config, norm)
        commit_one(test_config, norm.metadata.identificador)

        bodies = _git_log_bodies(test_config.git.repo_path)
        assert len(bodies) == 2

        # Last commit should have Source-Id, Source-Date, Norm-Id
        last_body = bodies[-1]
        assert "Source-Id:" in last_body
        assert "Source-Date:" in last_body
        assert f"Norm-Id: {norm.metadata.identificador}" in last_body


# ─────────────────────────────────────────────
# TestMultiVersionNorm
# ─────────────────────────────────────────────


class TestMultiVersionNorm:
    """Test a norm with 4+ versions (like the Constitution with 4 reforms)."""

    @staticmethod
    def _make_four_version_norm() -> NormaCompleta:
        dates = [date(1978, 12, 29), date(1992, 8, 28), date(2011, 9, 27), date(2024, 2, 17)]
        sources = ["SRC-ORIG", "SRC-1992", "SRC-2011", "SRC-2024"]

        blocks = [
            _make_block("a1", "Artículo 1", [
                _make_version(sources[0], dates[0], "Versión original art 1."),
                _make_version(sources[1], dates[1], "Versión 1992 art 1."),
            ]),
            _make_block("a2", "Artículo 2", [
                _make_version(sources[0], dates[0], "Versión original art 2."),
                _make_version(sources[2], dates[2], "Versión 2011 art 2."),
            ]),
            _make_block("a3", "Artículo 3", [
                _make_version(sources[0], dates[0], "Versión original art 3."),
                _make_version(sources[3], dates[3], "Versión 2024 art 3."),
            ]),
        ]

        reforms = [
            Reform(fecha=dates[0], id_norma=sources[0], bloques_afectados=("a1", "a2", "a3")),
            Reform(fecha=dates[1], id_norma=sources[1], bloques_afectados=("a1",)),
            Reform(fecha=dates[2], id_norma=sources[2], bloques_afectados=("a2",)),
            Reform(fecha=dates[3], id_norma=sources[3], bloques_afectados=("a3",)),
        ]

        metadata = NormaMetadata(
            titulo="Norma con Cuatro Versiones",
            titulo_corto="Norma Cuatro Versiones",
            identificador="TEST-FOUR-VERSIONS",
            pais="es",
            rango=Rango.CONSTITUCION,
            fecha_publicacion=dates[0],
            estado=EstadoNorma.VIGENTE,
            departamento="Test",
            fuente="https://example.com/test",
        )

        return NormaCompleta(metadata=metadata, bloques=tuple(blocks), reforms=tuple(reforms))

    def test_creates_four_commits(self, test_config):
        norm = self._make_four_version_norm()
        _save_norm(test_config, norm)
        count = commit_one(test_config, norm.metadata.identificador)
        assert count == 4

    def test_each_commit_has_correct_historical_date(self, test_config):
        norm = self._make_four_version_norm()
        _save_norm(test_config, norm)
        commit_one(test_config, norm.metadata.identificador)

        dates = _git_log_dates(test_config.git.repo_path)
        assert dates == ["1978-12-29", "1992-08-28", "2011-09-27", "2024-02-17"]

    def test_markdown_content_changes_between_versions(self, test_config):
        norm = self._make_four_version_norm()
        _save_norm(test_config, norm)
        commit_one(test_config, norm.metadata.identificador)

        # Get content at each commit
        result = subprocess.run(
            ["git", "log", "--format=%H", "--reverse"],
            cwd=test_config.git.repo_path,
            capture_output=True,
            text=True,
        )
        shas = result.stdout.strip().splitlines()
        assert len(shas) == 4

        contents = []
        for sha in shas:
            show = subprocess.run(
                ["git", "show", f"{sha}:es/TEST-FOUR-VERSIONS.md"],
                cwd=test_config.git.repo_path,
                capture_output=True,
                text=True,
            )
            contents.append(show.stdout)

        # First version has "original" text
        assert "Versión original art 1" in contents[0]
        # Second version updates art 1
        assert "Versión 1992 art 1" in contents[1]
        assert "Versión 1992 art 1" not in contents[0]
        # Third version updates art 2
        assert "Versión 2011 art 2" in contents[2]
        assert "Versión 2011 art 2" not in contents[1]
        # Fourth version updates art 3
        assert "Versión 2024 art 3" in contents[3]
        assert "Versión 2024 art 3" not in contents[2]

    def test_idempotent_rerun_creates_zero_commits(self, test_config):
        norm = self._make_four_version_norm()
        _save_norm(test_config, norm)
        count1 = commit_one(test_config, norm.metadata.identificador)
        count2 = commit_one(test_config, norm.metadata.identificador)
        assert count1 == 4
        assert count2 == 0


# ─────────────────────────────────────────────
# TestStorageGenericRoundTrip
# ─────────────────────────────────────────────


class TestStorageGenericRoundTrip:
    """Test that save + load works identically for norms from different countries."""

    @pytest.mark.parametrize(
        "make_norm",
        [_make_norm_es, _make_norm_fr, _make_norm_se],
        ids=["spain", "france", "sweden"],
    )
    def test_metadata_round_trips(self, test_config, make_norm):
        norm = make_norm()
        json_path = _save_norm(test_config, norm)
        loaded = load_norma_from_json(json_path)

        assert loaded.metadata.titulo == norm.metadata.titulo.rstrip(". ")
        assert loaded.metadata.titulo_corto == norm.metadata.titulo_corto
        assert loaded.metadata.identificador == norm.metadata.identificador
        assert loaded.metadata.pais == norm.metadata.pais
        assert loaded.metadata.rango == norm.metadata.rango
        assert loaded.metadata.fecha_publicacion == norm.metadata.fecha_publicacion
        assert loaded.metadata.estado == norm.metadata.estado
        assert loaded.metadata.departamento == norm.metadata.departamento
        assert loaded.metadata.fuente == norm.metadata.fuente

    @pytest.mark.parametrize(
        "make_norm",
        [_make_norm_es, _make_norm_fr, _make_norm_se],
        ids=["spain", "france", "sweden"],
    )
    def test_blocks_round_trip(self, test_config, make_norm):
        norm = make_norm()
        json_path = _save_norm(test_config, norm)
        loaded = load_norma_from_json(json_path)

        assert len(loaded.bloques) == len(norm.bloques)
        for orig, loaded_b in zip(norm.bloques, loaded.bloques):
            assert loaded_b.id == orig.id
            assert loaded_b.tipo == orig.tipo
            assert loaded_b.titulo == orig.titulo
            assert len(loaded_b.versions) == len(orig.versions)

            for orig_v, loaded_v in zip(orig.versions, loaded_b.versions):
                assert loaded_v.id_norma == orig_v.id_norma
                assert loaded_v.fecha_publicacion == orig_v.fecha_publicacion
                # Paragraph text should match
                orig_text = "\n\n".join(p.text for p in orig_v.paragraphs)
                loaded_text = "\n\n".join(p.text for p in loaded_v.paragraphs)
                assert loaded_text == orig_text

    @pytest.mark.parametrize(
        "make_norm",
        [_make_norm_es, _make_norm_fr, _make_norm_se],
        ids=["spain", "france", "sweden"],
    )
    def test_reforms_round_trip(self, test_config, make_norm):
        norm = make_norm()
        json_path = _save_norm(test_config, norm)
        loaded = load_norma_from_json(json_path)

        assert len(loaded.reforms) == len(norm.reforms)
        for orig, loaded_r in zip(norm.reforms, loaded.reforms):
            assert loaded_r.fecha == orig.fecha
            assert loaded_r.id_norma == orig.id_norma

    def test_different_pais_values_preserved(self, test_config):
        """All three countries have distinct pais values after round-trip."""
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        loaded_pais = []
        for norm in norms:
            json_path = _save_norm(test_config, norm)
            loaded = load_norma_from_json(json_path)
            loaded_pais.append(loaded.metadata.pais)

        assert loaded_pais == ["es", "fr", "se"]

    def test_different_rango_values_preserved(self, test_config):
        """Country-specific rango values survive round-trip."""
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        loaded_rangos = []
        for norm in norms:
            json_path = _save_norm(test_config, norm)
            loaded = load_norma_from_json(json_path)
            loaded_rangos.append(str(loaded.metadata.rango))

        assert loaded_rangos == ["ley", "code", "lag"]


# ─────────────────────────────────────────────
# TestCommitAllMultiCountry
# ─────────────────────────────────────────────


class TestCommitAllMultiCountry:
    """Test commit_all with a mix of countries in the same data_dir."""

    def test_all_three_norms_generate_commits(self, test_config):
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        for norm in norms:
            _save_norm(test_config, norm)

        total = commit_all(test_config)
        # Each norm has 2 reforms = 2 commits, so 6 total
        assert total == 6

    def test_each_markdown_in_correct_country_directory(self, test_config):
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        for norm in norms:
            _save_norm(test_config, norm)

        commit_all(test_config)

        repo = Path(test_config.git.repo_path)
        assert (repo / "es" / "BOE-A-2000-100.md").exists()
        assert (repo / "fr" / "LEGITEXT000006070721.md").exists()
        assert (repo / "se" / "SFS-1962-700.md").exists()

    def test_each_file_has_correct_frontmatter(self, test_config):
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        for norm in norms:
            _save_norm(test_config, norm)

        commit_all(test_config)

        repo = Path(test_config.git.repo_path)

        es_content = (repo / "es" / "BOE-A-2000-100.md").read_text(encoding="utf-8")
        assert 'pais: "es"' in es_content
        assert 'rango: "ley"' in es_content

        fr_content = (repo / "fr" / "LEGITEXT000006070721.md").read_text(encoding="utf-8")
        assert 'pais: "fr"' in fr_content
        assert 'rango: "code"' in fr_content

        se_content = (repo / "se" / "SFS-1962-700.md").read_text(encoding="utf-8")
        assert 'pais: "se"' in se_content
        assert 'rango: "lag"' in se_content

    def test_total_git_commits(self, test_config):
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        for norm in norms:
            _save_norm(test_config, norm)

        commit_all(test_config)

        count = _git_commit_count(test_config.git.repo_path)
        assert count == 6

    def test_idempotent_rerun(self, test_config):
        norms = [_make_norm_es(), _make_norm_fr(), _make_norm_se()]
        for norm in norms:
            _save_norm(test_config, norm)

        total1 = commit_all(test_config)
        total2 = commit_all(test_config)
        assert total1 == 6
        assert total2 == 0


# ─────────────────────────────────────────────
# TestSlugMultiCountry
# ─────────────────────────────────────────────


class TestSlugMultiCountry:
    """Test norma_to_filepath generates correct paths for each country."""

    def test_spanish_norm_path(self):
        norm = _make_norm_es()
        assert norma_to_filepath(norm.metadata) == "es/BOE-A-2000-100.md"

    def test_french_norm_path(self):
        norm = _make_norm_fr()
        assert norma_to_filepath(norm.metadata) == "fr/LEGITEXT000006070721.md"

    def test_swedish_norm_path(self):
        norm = _make_norm_se()
        assert norma_to_filepath(norm.metadata) == "se/SFS-1962-700.md"

    def test_jurisdiccion_overrides_pais(self):
        """Autonomous community norms use jurisdiccion as directory."""
        meta = NormaMetadata(
            titulo="Ley vasca",
            titulo_corto="Ley vasca",
            identificador="BOE-A-2020-615",
            pais="es",
            rango=Rango.LEY,
            fecha_publicacion=date(2020, 1, 1),
            estado=EstadoNorma.VIGENTE,
            departamento="Test",
            fuente="https://example.com",
            jurisdiccion="es-pv",
        )
        assert norma_to_filepath(meta) == "es-pv/BOE-A-2020-615.md"
