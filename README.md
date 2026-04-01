# legalize-pipeline

[![CI](https://github.com/legalize-dev/legalize-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/legalize-dev/legalize-pipeline/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

The engine behind **[legalize.dev](https://legalize.dev)** -- converts official legislation into version-controlled Markdown in Git.

Each law is a file. Each reform is a commit. Every country is a repo.

## What it does

1. **Fetches** legislation from official open data sources (BOE, LEGI, SFSR, and more)
2. **Parses** XML into structured data (articles, versions, reforms)
3. **Generates** Markdown files with YAML frontmatter and git commits with historical dates

## Public repos (output)

| Country | Repo | Source |
|---------|------|--------|
| Spain | [legalize-es](https://github.com/legalize-dev/legalize-es) | BOE |
| France | [legalize-fr](https://github.com/legalize-dev/legalize-fr) | LEGI (Legifrance) |
| Austria | [legalize-at](https://github.com/legalize-dev/legalize-at) | RIS (Bundeskanzleramt) |
| Sweden | [legalize-se](https://github.com/legalize-dev/legalize-se) | SFSR (Riksdag) |

## Architecture

```
src/legalize/
  fetcher/              # Country-specific data fetching
    base.py               Abstract interfaces (LegislativeClient, NormDiscovery, TextParser, MetadataParser)
    es/                   Spain (BOE API)
      client.py             HTTP client with rate limiting, caching
      discovery.py          Norm discovery via catalog + sumarios
      parser.py             BOE XML -> Bloque/NormaMetadata
    fr/                   France (LEGI XML dump)
      client.py             Local XML dump reader
      discovery.py          Filesystem-based discovery
      parser.py             LEGI XML -> Bloque/NormaMetadata
    se/                   Sweden (SFSR)
      client.py             Riksdag API client
      discovery.py          SFS catalog discovery
      parser.py             Swedish XML -> Bloque/NormaMetadata
  transformer/          # Generic: XML -> Markdown
    xml_parser.py         Bloque/Version extraction, reform timeline
    markdown.py           Bloque -> Markdown (CSS class mapping)
    frontmatter.py        YAML frontmatter rendering
    slug.py               norm_to_filepath() -> {country_dir}/{id}.md
  committer/            # Generic: Markdown -> git commits
    git_ops.py            Git operations with historical dates
    message.py            Commit message formatting (6 types)
    author.py             Author from git config (whoever runs the pipeline)
  state/                # Pipeline state tracking
    store.py              Last processed summary, run history
  countries.py          # Country registry (lazy import dispatch)
  config.py             # Config + CountryConfig from config.yaml
  models.py             # Domain models (generic, multi-country)
  storage.py            # Save XML + JSON to data/ (intermediate cache)
  pipeline.py           # Generic orchestration (fetch, commit, bootstrap, daily, reprocess)
```

## Prerequisites

- Python 3.12+
- Git

## Quick start

```bash
git clone https://github.com/legalize-dev/legalize-pipeline.git
cd legalize-pipeline

pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/
```

## CLI

All commands use a unified `--country` / `-c` flag:

```bash
# Fetch laws to data/ (does not touch git)
legalize fetch -c es --catalog             # Spain: full BOE catalog
legalize fetch -c fr --all --legi-dir /path # France: all codes from LEGI dump
legalize fetch -c se --all                  # Sweden: all statutes from SFSR
legalize fetch BOE-A-1978-31229             # Single law by ID

# Generate git commits from local data/ (does not download)
legalize commit -c es --all
legalize commit -c fr --all

# Full pipeline: fetch + commit
legalize bootstrap                          # Spain (default)
legalize bootstrap -c fr --legi-dir /path   # France
legalize bootstrap -c se                    # Sweden

# Daily incremental update
legalize daily -c es --date 2026-03-28

# Reprocess specific norms
legalize reprocess -c es --reason "bug fix" BOE-A-1978-31229

# Pipeline status
legalize status
```

## Adding a new country

1. Create `fetcher/{code}/` with `client.py`, `discovery.py`, `parser.py`
2. Implement the 4 interfaces from `fetcher/base.py`:
   - `LegislativeClient` -- fetch raw data
   - `NormDiscovery` -- discover all laws in catalog
   - `TextParser` -- parse into `Bloque` objects
   - `MetadataParser` -- parse into `NormaMetadata`
3. Register in `countries.py` REGISTRY
4. Add `countries:` section to `config.yaml`

See [ADDING_A_COUNTRY.md](ADDING_A_COUNTRY.md) for the full walkthrough.

## Countries

| Country | Status | Source | Repo |
|---------|--------|--------|------|
| Spain | Live | [BOE](https://www.boe.es/) | [legalize-es](https://github.com/legalize-dev/legalize-es) |
| France | Live | [Legifrance](https://www.legifrance.gouv.fr/) | [legalize-fr](https://github.com/legalize-dev/legalize-fr) |
| Austria | Live | [RIS](https://www.ris.bka.gv.at/) | [legalize-at](https://github.com/legalize-dev/legalize-at) |
| Sweden | Beta | [Riksdag](https://www.riksdagen.se/) | [legalize-se](https://github.com/legalize-dev/legalize-se) |
| Germany | Wanted | [BGBL](https://www.bgbl.de/) | Help wanted! |
| Portugal | Wanted | [DRE](https://dre.pt/) | Help wanted! |
| Netherlands | Wanted | [Overheid.nl](https://www.overheid.nl/) | Help wanted! |
| Brazil | Wanted | [LeXML](https://www.lexml.gov.br/) | Help wanted! |

Want to add your country? See [ADDING_A_COUNTRY.md](ADDING_A_COUNTRY.md).

## Contributing

We welcome contributions, especially new country parsers. See [CONTRIBUTING.md](CONTRIBUTING.md) and [ADDING_A_COUNTRY.md](ADDING_A_COUNTRY.md).

## License

MIT
