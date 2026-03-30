# legalize-pipeline

The engine behind [legalize.dev](https://legalize.dev). Converts official legislation into version-controlled Markdown in Git.

Each law is a file. Each reform is a commit. Every country is a repo.

## What it does

1. **Fetches** legislation from official open data sources (BOE, LEGI, SFSR, and more)
2. **Parses** XML into structured data (articles, versions, reforms)
3. **Generates** Markdown files with YAML frontmatter and git commits with historical dates

## Public repos (output)

| Country | Repo | Laws | Source |
|---------|------|------|--------|
| Spain | [legalize-es](https://github.com/legalize-dev/legalize-es) | 8,642 | BOE |
| France | [legalize-fr](https://github.com/legalize-dev/legalize-fr) | 80 codes | LEGI (Legifrance) |
| Sweden | [legalize-se](https://github.com/legalize-dev/legalize-se) | In progress | SFSR (Riksdag) |

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
    metadata.py           Metadata parsing helpers
    slug.py               norma_to_filepath() -> {country_dir}/{id}.md
  committer/            # Generic: Markdown -> git commits
    git_ops.py            Git operations with historical dates
    message.py            Commit message formatting (6 types)
    author.py             Legalize <legalize@legalize.es>
  state/                # Pipeline state tracking
    store.py              Last processed summary, run history
    mappings.py           ID <-> filepath mapping
  countries.py          # Country registry (lazy import dispatch)
  config.py             # Config + CountryConfig from config.yaml
  models.py             # Domain models (generic, multi-country)
  storage.py            # Save XML + JSON to data/ (intermediate cache)
  pipeline.py           # Generic orchestration (fetch, commit, bootstrap, daily, reprocess)
```

## Quick start

```bash
# Install
pip install -e ".[dev]"

# Run tests (111 passing)
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

See [docs/ADDING_A_COUNTRY.md](docs/ADDING_A_COUNTRY.md) for the full walkthrough.

## Countries

| Country | Status | Source | Laws | Repo |
|---------|--------|--------|------|------|
| Spain | Live | [BOE](https://www.boe.es/) | 8,642 | [legalize-es](https://github.com/legalize-dev/legalize-es) |
| France | Beta | [Legifrance](https://www.legifrance.gouv.fr/) | 80 codes | [legalize-fr](https://github.com/legalize-dev/legalize-fr) |
| Sweden | Beta | [Riksdag](https://www.riksdagen.se/) | In progress | [legalize-se](https://github.com/legalize-dev/legalize-se) |
| Germany | Wanted | [BGBL](https://www.bgbl.de/) | -- | Help wanted! |
| Portugal | Wanted | [DRE](https://dre.pt/) | -- | Help wanted! |

Want to add your country? See [docs/ADDING_A_COUNTRY.md](docs/ADDING_A_COUNTRY.md).

## Contributing

We welcome contributions, especially new country parsers. See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/ADDING_A_COUNTRY.md](docs/ADDING_A_COUNTRY.md).

## License

MIT
