# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Legalize is a multi-country platform that converts official legislation into version-controlled Markdown. Each law is a file, each reform is a git commit. The public repo is the product; this repo is the pipeline that generates it.

**Repos:**
- `legalize-dev/legalize` -- public hub: README, index of countries, docs
- `legalize-dev/legalize-es` -- public: Spanish laws as Markdown + git history (8,642 laws)
- `legalize-dev/legalize-fr` -- public: French laws (80 codes)
- `legalize-dev/legalize-se` -- public: Swedish laws (in progress)
- `legalize-dev/legalize-pipeline` -- public: this repo. Python engine that generates the public repos.

**Local structure:**
```
~/autonomo/legalize/
├── engine/     ← this repo (legalize-pipeline)
├── es/         ← Spanish laws (legalize-es)
├── fr/         ← French laws (legalize-fr)
├── se/         ← Swedish laws (legalize-se)
├── hub/        ← hub repo (legalize)
└── data/       ← XML + JSON cache (no git)
```

**Website:** https://legalize.dev

Processing Spanish (BOE), French (LEGI), and Swedish (SFSR) legislation. Architecture is multi-country with a unified pipeline.

## Language & Stack

- **English only** — all code, comments, variable names, function names, and documentation must be in English. The only exceptions are dataclass field names in `models.py` that are part of the output data contract (e.g., `titulo`, `identificador`, `fecha_publicacion`) and string literals for XML element names or commit message content.
- **Python 3.12+** with `pyproject.toml` (hatchling build), src layout
- Dependencies: `lxml`, `requests`, `pyyaml`, `click`, `rich`
- Dev: `pytest`, `ruff`, `responses` (HTTP mocking)
- Git operations via `subprocess` (not GitPython) for full control over `GIT_AUTHOR_DATE`
- CI via GitHub App (Legalize Pipeline)

## Commands

```bash
# Install
pip install -e ".[dev]"

# Run tests (147 passing)
pytest tests/ -v

# Lint
ruff check src/ tests/

# Fetch laws to data/ (does not touch git)
legalize fetch -c es --catalog                 # Spain: full BOE catalog
legalize fetch -c fr --all --legi-dir /path    # France: LEGI dump
legalize fetch -c se --all                     # Sweden: SFSR

# Generate git commits from local data/
legalize commit -c es --all
legalize commit -c fr --all

# Full pipeline: fetch + commit
legalize bootstrap                             # Spain (default)
legalize bootstrap -c fr --legi-dir /path      # France
legalize bootstrap -c se                       # Sweden

# Daily incremental update
legalize daily -c es --date 2026-03-28

# Reprocess specific norms
legalize reprocess -c es --reason "bug fix" BOE-A-1978-31229

# Pipeline status
legalize status
```

## Architecture

Modular pipeline in `src/legalize/`:

### Fetcher (`fetcher/`)

Country-specific fetchers live in subpackages. Each implements the 4 interfaces from `fetcher/base.py`.

- `base.py` -- Abstract interfaces: `LegislativeClient`, `NormDiscovery`, `TextParser`, `MetadataParser`
- `cache.py` -- `FileCache`: local XML cache with TTL
- `es/` -- Spain (BOE API)
  - `client.py` -- `BOEClient`: HTTP with rate limiting (2 req/s), exponential backoff, ETag/Last-Modified cache
  - `discovery.py` -- `BOEDiscovery`: norm discovery via catalog + sumarios
  - `parser.py` -- `BOETextParser`, `BOEMetadataParser`: BOE XML parsing
  - `sumario.py` -- daily BOE summary parsing
  - `catalogo.py` -- catalog-based norm discovery
  - `metadata.py` -- BOE metadata extraction
  - `titulos.py` -- title normalization
- `fr/` -- France (LEGI XML dump)
  - `client.py` -- `LEGIClient`: local XML dump reader
  - `discovery.py` -- `LEGIDiscovery`: filesystem-based discovery
  - `parser.py` -- `LEGITextParser`, `LEGIMetadataParser`: LEGI XML parsing
- `se/` -- Sweden (SFSR / Riksdag)
  - `client.py` -- `SwedishClient`: Riksdag API client
  - `discovery.py` -- `SwedishDiscovery`: SFS catalog discovery
  - `parser.py` -- `SwedishTextParser`, `SwedishMetadataParser`: Swedish XML parsing

### Transformer (`transformer/`)
- `xml_parser.py` -- `parse_texto_xml(bytes) -> list[Bloque]`, `extract_reforms()`, `get_bloque_at_date()`
- `markdown.py` -- `render_norma_at_date(metadata, bloques, date) -> str`. CSS->MD mapping is data-driven
- `frontmatter.py` -- `render_frontmatter(NormaMetadata, date) -> str`
- `metadata.py` -- metadata parsing helpers
- `slug.py` -- `norma_to_filepath(metadata) -> str` (e.g., `spain/BOE-A-1978-31229.md`)

### Committer (`committer/`)
- `git_ops.py` -- `GitRepo`: init, write_and_add, commit (historical dates), push, idempotency via `git log --grep`
- `message.py` -- `build_commit_info()`, `format_commit_message()`. Six types: `[bootstrap]`, `[reforma]`, `[nueva]`, `[derogacion]`, `[correccion]`, `[fix-pipeline]`. Trailers: `Source-Id`, `Source-Date`, `Norm-Id`
- `author.py` -- All commits by `Legalize <legalize@legalize.es>`

### State (`state/`)
- `store.py` -- `StateStore`: state.json tracking processed norms and run history
- `mappings.py` -- `IdToFilename`: norm ID <-> filepath mapping

### Multi-country (`countries.py`, `config.py`)
- `countries.py` -- `REGISTRY` dict with lazy imports: maps country code to `(module, class)` tuples for client, discovery, text_parser, metadata_parser. Helper functions: `get_client_class()`, `get_discovery_class()`, `get_text_parser()`, `get_metadata_parser()`, `supported_countries()`
- `config.py` -- `Config` with `CountryConfig` per country. `config.yaml` has a `countries:` section with per-country `repo_path`, `data_dir`, `source` (passed to client `create()`)

### Orchestration
- `pipeline.py` -- Generic flows: `generic_fetch_all()`, `generic_fetch_one()`, `generic_bootstrap()`, `commit_all()`, `commit_one()`, `daily()`, `reprocess()`. All country-agnostic; dispatch via `countries.py`
- `cli.py` -- Click CLI with unified `--country` / `-c` flag: `fetch`, `commit`, `bootstrap`, `daily`, `reprocess`, `status`. Deprecated aliases (`fetch-fr`, `bootstrap-se`, etc.) are hidden but still work
- `config.py` -- `Config` from `config.yaml` with CLI overrides

## Data Model (`models.py`)

Multi-country ready. Key types:
- `Rango` -- free-form string for normative rank (each country defines its own values)
- `NormaMetadata` -- generic fields (`identificador`, `pais`, `fuente`)
- `Bloque` -- structural unit (article, chapter) with versioned content
- `Version` -- temporal version with `fecha_publicacion` and paragraphs
- `CommitInfo` -- generic trailers (`Source-Id`, `Source-Date`, `Norm-Id`)
- Filenames = official ID: `spain/BOE-A-1978-31229.md`

## Output Format (FINAL -- do not change without regenerating all commits)

**Filename:** `{country_dir}/{official_id}.md` -> `spain/BOE-A-1978-31229.md`

**Frontmatter:**
```yaml
---
titulo: "Constitucion Espanola"
identificador: "BOE-A-1978-31229"
pais: "es"
rango: "constitucion"
fecha_publicacion: "1978-12-29"
ultima_actualizacion: "2024-02-17"
estado: "vigente"
fuente: "https://www.boe.es/eli/es/c/1978/12/27/(1)"
---
```

**Commit messages:** `[reforma] Constitucion Espanola -- art. 49`
**Author:** `Legalize <legalize@legalize.es>` (always)
**Trailers:** `Source-Id`, `Source-Date`, `Norm-Id`

## Adding New Countries

To add a new country:
1. Create `fetcher/{code}/` with `client.py`, `discovery.py`, `parser.py`
2. Implement the 4 interfaces from `fetcher/base.py`
3. Register in `countries.py` REGISTRY
4. Add `countries:` section to `config.yaml` with `source` params for the client

See [ADDING_A_COUNTRY.md](ADDING_A_COUNTRY.md) for the full walkthrough.

## BOE API (Spain)

Base: `https://www.boe.es/datosabiertos/`
- `/api/boe/sumario/{YYYYMMDD}` -- daily publications
- `/api/legislacion-consolidada?limit=-1` -- full catalog (1065 norms in scope)
- `/api/legislacion-consolidada/id/{id}/texto` -- full XML with versioned `<bloque>` elements
- `/api/legislacion-consolidada/id/{id}/metadatos` -- norm metadata

## Key Conventions

- Dates as `datetime.date` internally; parse at XML boundary, format at output
- English for all code, comments, and variable names
- CI via GitHub App (Legalize Pipeline); daily runs are local for now
