# Contributing to Legalize

Thanks for your interest in Legalize! We especially welcome contributions that add new countries to the platform.

## The #1 contribution: add your country

The highest-impact contribution is adding a new country's legislation to the pipeline. This means writing a fetcher for your country's official gazette API or XML dump.

**Full guide:** [docs/ADDING_A_COUNTRY.md](docs/ADDING_A_COUNTRY.md)

**Reference implementation:** France (`fetcher/fr/`)

**What you need to know:**
- Your country's official open data source for legislation (API, XML dump, or scraping)
- Python (the pipeline is Python 3.12+)
- Basic understanding of your country's legal hierarchy (types of laws, how reforms work)

**What you produce:**
- A `fetcher/{code}/` package with:
  - `__init__.py` -- re-export your classes
  - `client.py` -- fetch raw data from the source (`LegislativeClient`)
  - `discovery.py` -- find all laws in the catalog (`NormDiscovery`)
  - `parser.py` -- convert into the generic data model (`TextParser`, `MetadataParser`)
- A registration entry in `countries.py` REGISTRY
- A `countries:` section in `config.yaml` with source-specific params
- Tests with fixture data

The generic layers (markdown rendering, git committing, CLI) work automatically once your fetcher produces the right data structures. The unified CLI commands (`legalize fetch -c {code}`, `legalize bootstrap -c {code}`) are available immediately after registration.

## Quick test

```bash
# Test your fetcher with just 5 laws
legalize fetch -c {code} --all --limit 5

# Dry-run: see what commits it would create
legalize bootstrap -c {code} --dry-run
```

## Development setup

```bash
# Clone
git clone https://github.com/legalize-dev/legalize-pipeline.git
cd legalize-pipeline

# Install
pip install -e ".[dev]"

# Run tests (111 passing)
pytest tests/ -v

# Lint
ruff check src/ tests/
```

## Code conventions

- **Python 3.12+**, type hints encouraged
- **ruff** for linting (`ruff check src/ tests/`)
- **pytest** for tests
- **Language:** English for all code, comments, and variable names
- **No frameworks for git:** we use `subprocess` for full control over `GIT_AUTHOR_DATE`

## Project structure

```
src/legalize/
  fetcher/
    base.py             Abstract interfaces (implement these)
    es/                 Spain (BOE) -- reference for API-based sources
    fr/                 France (LEGI) -- reference for XML dump sources
    se/                 Sweden (SFSR) -- reference for amendment-register sources
  transformer/          Generic markdown rendering
  committer/            Generic git commit generation
  countries.py          Country registry (add your entry here)
  config.py             CountryConfig with per-country source params
  pipeline.py           Generic orchestration (all countries share this)
```

## Pull request process

1. Fork the repo
2. Create a branch (`git checkout -b add-country-de`)
3. Make your changes
4. Run tests and lint: `pytest tests/ -v && ruff check src/ tests/`
5. Submit a PR with a clear description of what the country fetcher does and what data source it uses

For new country PRs, include:
- Sample fixture data (a few XML/JSON files from the source)
- Tests that parse the fixtures
- A note on the data source's license/terms of use

## Questions?

Open an issue or start a discussion. We're happy to help you get started with a new country fetcher.
