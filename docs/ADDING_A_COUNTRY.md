# Adding a New Country to Legalize

This guide walks through adding a new country to the pipeline. Use France (`fr`) as the reference implementation.

## Prerequisites

Before starting, you need:
- An open data source for the country's legislation (API, XML dump, or HTML)
- Understanding of the source's data format
- Knowledge of the country's legal hierarchy (types of laws, reform process)
- Whether the source provides version history (amendments) or only current text

## Architecture overview

The pipeline has two layers:

**Country-specific (you write this):** fetcher that downloads and parses raw data into generic models.

**Generic (provided for free):** markdown rendering, YAML frontmatter, git commits with historical dates, CLI commands, state management, web app integration. These work automatically once your fetcher produces the right data structures.

```
Official data source
  |
  v
fetcher/{code}/          <-- you implement this
  client.py              LegislativeClient: fetch raw data
  discovery.py           NormDiscovery: find all laws
  parser.py              TextParser + MetadataParser: parse into Bloque/NormaMetadata
  |
  v
Generic pipeline         <-- provided for free
  transformer/           Markdown rendering, frontmatter
  committer/             Git commits with historical dates
  cli.py                 Unified CLI (legalize fetch -c xx, legalize bootstrap -c xx)
  pipeline.py            Orchestration (fetch, commit, bootstrap, daily)
```

## Step 1: Create the fetcher package

Create `src/legalize/fetcher/{code}/` with four files.

### `__init__.py`

Re-export your classes:

```python
"""Country Name ({CODE}) -- legislative fetcher components."""

from legalize.fetcher.{code}.client import MyClient
from legalize.fetcher.{code}.discovery import MyDiscovery
from legalize.fetcher.{code}.parser import MyMetadataParser, MyTextParser

__all__ = ["MyClient", "MyDiscovery", "MyTextParser", "MyMetadataParser"]
```

### `client.py` -- LegislativeClient

Fetches raw data (XML, JSON, HTML) from the source. Three methods to implement:

```python
from legalize.fetcher.base import LegislativeClient

class MyClient(LegislativeClient):

    @classmethod
    def create(cls, country_config):
        """Create from CountryConfig. Read source-specific params here.

        country_config.source is a dict from config.yaml:
            countries:
              xx:
                source:
                  base_url: "https://..."
                  api_key: "..."
        """
        base_url = country_config.source.get("base_url", "https://default.api/")
        return cls(base_url)

    def __init__(self, base_url: str):
        self._session = requests.Session()
        self._session.headers["User-Agent"] = (
            "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)"
        )
        self._base_url = base_url

    def get_texto(self, norm_id: str) -> bytes:
        """Fetch the consolidated text of a law. Returns raw bytes."""
        resp = self._session.get(f"{self._base_url}/text/{norm_id}")
        resp.raise_for_status()
        return resp.content

    def get_metadatos(self, norm_id: str) -> bytes:
        """Fetch metadata. Can return same data as get_texto if metadata is embedded."""
        resp = self._session.get(f"{self._base_url}/metadata/{norm_id}")
        resp.raise_for_status()
        return resp.content

    def close(self) -> None:
        self._session.close()
```

The `create()` classmethod is how the pipeline instantiates your client. It receives a `CountryConfig` whose `.source` dict comes from `config.yaml`. Override it to read source-specific parameters. The default implementation calls `cls()` with no arguments.

**Important:**
- Add rate limiting (respect the source -- typically 500ms-1s between requests)
- Add retry with backoff for 429/503 errors
- Set a descriptive `User-Agent`
- The client is a context manager (`with MyClient.create(cfg) as client:`)

**Reference:** `fetcher/fr/client.py` (reads from local XML dump), `fetcher/es/client.py` (HTTP API with caching)

### `discovery.py` -- NormDiscovery

Finds all law IDs in the catalog:

```python
from collections.abc import Iterator
from datetime import date
from legalize.fetcher.base import LegislativeClient, NormDiscovery

class MyDiscovery(NormDiscovery):

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield all norm IDs in the catalog.
        Filter OUT amendment documents -- only yield base laws."""
        # Example: paginate through an API
        page = 1
        while True:
            resp = client._session.get(f"https://api/laws?page={page}")
            data = resp.json()
            for item in data["results"]:
                yield item["id"]
            if not data.get("next"):
                break
            page += 1

    def discover_daily(self, client: LegislativeClient, target_date: date, **kwargs) -> Iterator[str]:
        """Yield norm IDs published/updated on a specific date."""
        # For amendments: yield the BASE law's ID, not the amendment's
        ...
```

**Reference:** `fetcher/fr/discovery.py` (scans filesystem), `fetcher/es/discovery.py` (paginates BOE API)

### `parser.py` -- TextParser + MetadataParser

Parses raw bytes into the generic data model:

```python
from typing import Any
from legalize.fetcher.base import MetadataParser, TextParser
from legalize.models import Bloque, EstadoNorma, NormaMetadata, Paragraph, Rango, Version

class MyTextParser(TextParser):

    def parse_texto(self, data: bytes) -> list[Any]:
        """Parse raw text into Bloque objects.

        Each structural unit (chapter, section, article) becomes a Bloque.
        Each Bloque has one or more Versions with paragraphs.
        """
        # Parse your format (XML, JSON, HTML) into:
        return [
            Bloque(
                id="art-1",
                tipo="article",
                titulo="Article 1",
                versions=(
                    Version(
                        id_norma="LAW-2024-1",
                        fecha_publicacion=date(2024, 1, 15),
                        fecha_vigencia=date(2024, 1, 15),
                        paragraphs=(
                            Paragraph(css_class="articulo", text="Article 1"),
                            Paragraph(css_class="parrafo", text="Everyone has the right to..."),
                        ),
                    ),
                ),
            ),
        ]

    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from the text data."""
        blocks = self.parse_texto(data)
        from legalize.transformer.xml_parser import extract_reforms
        return extract_reforms(blocks)


class MyMetadataParser(MetadataParser):

    def parse(self, data: bytes, norm_id: str) -> NormaMetadata:
        """Parse raw metadata into NormaMetadata."""
        # Parse your format, then return:
        return NormaMetadata(
            titulo="Full Title of the Law",
            titulo_corto="Short Title",
            identificador=norm_id,     # must be filesystem-safe
            pais="xx",                 # ISO 3166-1 alpha-2
            rango=Rango("act"),        # free-form string
            fecha_publicacion=date(2024, 1, 15),
            estado=EstadoNorma.VIGENTE,
            departamento="Ministry of Justice",
            fuente="https://official-source.gov/law/123",
        )
```

**Key rules for the output models:**

- `Bloque` -- structural unit (article, chapter, section) with versioned content
- `Version` -- a temporal version with `fecha_publicacion` and `paragraphs`
- `Paragraph` -- text + `css_class` (controls markdown rendering: `"articulo"` for article headings, `"parrafo"` for body text, `"titulo_tit"` for title headings, `"capitulo_tit"` for chapter headings)
- `NormaMetadata` -- title, id, country, rango, dates, status
- `identificador` must be filesystem-safe: no `:`, no spaces, no `/\*?"<>|`. Use `-` as separator. Example: SFS `1962:700` becomes `SFS-1962-700`
- `pais` must be the ISO 3166-1 alpha-2 code (e.g., `"se"`, `"fr"`, `"es"`)
- `rango` is a free-form string (`Rango("act")`, `Rango("code")`, `Rango("lag")`). Goes in YAML frontmatter, not in the file path
- You can reuse `extract_reforms()` from `transformer/xml_parser.py` -- it works with any list of Bloques

**Reference:** `fetcher/fr/parser.py` (XML), `fetcher/es/parser.py` (XML)

## Step 2: Register in `countries.py`

Add your country to the `REGISTRY` dict in `src/legalize/countries.py`:

```python
REGISTRY: dict[str, dict[str, tuple[str, str]]] = {
    # ... existing ...
    "xx": {
        "client": ("legalize.fetcher.xx.client", "MyClient"),
        "discovery": ("legalize.fetcher.xx.discovery", "MyDiscovery"),
        "text_parser": ("legalize.fetcher.xx.parser", "MyTextParser"),
        "metadata_parser": ("legalize.fetcher.xx.parser", "MyMetadataParser"),
    },
}
```

The registry uses lazy imports -- your module is only loaded when the country is selected. This keeps startup fast and avoids importing dependencies for countries that aren't being used.

Once registered, the unified CLI commands work automatically:
- `legalize fetch -c xx`
- `legalize bootstrap -c xx`
- `legalize commit -c xx`

## Step 3: Add config.yaml section

Add your country's configuration:

```yaml
countries:
  xx:
    repo_path: "../xx"           # output git repo
    data_dir: "../data-xx"       # raw data + parsed JSON
    cache_dir: ".cache"
    max_workers: 1
    source:                      # passed to client.create() as country_config.source
      base_url: "https://api.example.gov/legislation"
      api_key: "optional"
      # any key-value pairs your client needs
```

The `source` dict is passed through to your client's `create()` classmethod via `country_config.source`. Put any source-specific configuration there.

## Step 4: Create the output repo

```bash
gh repo create legalize-dev/legalize-{code} --public \
  --description "Legislation from [Country] in Markdown, version-controlled with Git"

git init ../xx/
mkdir -p ../xx/{code}
git -C ../xx commit --allow-empty --author="Legalize <legalize@legalize.es>" \
  -m "[bootstrap] Init legalize-{code}"
git -C ../xx remote add origin git@github.com:legalize-dev/legalize-{code}.git
git -C ../xx push -u origin main
```

Output structure is flat -- all laws in `{country_dir}/`, rango goes in YAML frontmatter:

```
legalize-{code}/
  {code}/
    ID-2024-123.md
    ID-2024-456.md
  README.md       # in the country's language
  LICENSE         # MIT
```

The `norma_to_filepath()` function generates `{pais}/{identificador}.md` automatically.

## Step 5: Write tests

Create `tests/test_parser_{code}.py` with fixture data:

```python
import pytest
from legalize.fetcher.{code}.parser import MyTextParser, MyMetadataParser
from legalize.countries import get_text_parser, get_metadata_parser

# Save sample data from your source in tests/fixtures/

class TestParser:
    def test_parse_texto(self):
        data = Path("tests/fixtures/sample_{code}.xml").read_bytes()
        parser = MyTextParser()
        blocks = parser.parse_texto(data)
        assert len(blocks) > 0
        assert blocks[0].versions  # has at least one version

    def test_metadata(self):
        data = Path("tests/fixtures/sample_{code}_meta.xml").read_bytes()
        parser = MyMetadataParser()
        meta = parser.parse(data, "NORM-ID-123")
        assert meta.pais == "xx"
        assert meta.identificador == "NORM-ID-123"

    def test_filesystem_safe_id(self):
        # Ensure no colons, spaces, or special chars
        meta = ...
        assert ":" not in meta.identificador
        assert " " not in meta.identificador

class TestCountryDispatch:
    def test_registry(self):
        parser = get_text_parser("xx")
        assert isinstance(parser, MyTextParser)
```

## Step 6: Test end-to-end

```bash
# Fetch 5 laws to verify the client and discovery work
legalize fetch -c xx --all --limit 5

# Dry-run bootstrap to see what commits it would create
legalize bootstrap -c xx --dry-run

# Full bootstrap (creates git commits in the output repo)
legalize bootstrap -c xx
```

## Checklist

- [ ] `fetcher/{code}/__init__.py` -- re-exports all classes
- [ ] `fetcher/{code}/client.py` -- with `create()`, rate limiting, retry
- [ ] `fetcher/{code}/discovery.py` -- `discover_all()` and `discover_daily()`
- [ ] `fetcher/{code}/parser.py` -- `TextParser` and `MetadataParser`
- [ ] `countries.py` -- registry entry added
- [ ] `config.yaml` -- country section with source params
- [ ] `tests/test_parser_{code}.py` -- passing
- [ ] GitHub repo `legalize-dev/legalize-{code}` -- with README in local language
- [ ] Tested with `legalize fetch -c {code} --all --limit 5`
- [ ] Tested with `legalize bootstrap -c {code} --dry-run`
- [ ] Full bootstrap run

## Version history strategies

Different countries provide different levels of historical data:

| Strategy | Example | What you get |
|----------|---------|-------------|
| **Embedded versions** | Spain (BOE), France (LEGI) | Full text at every point in time. Best case. |
| **Amendment register** | Sweden (SFSR) | Timeline of which sections changed when, but only current text. |
| **Snapshots over time** | Germany (gesetze-im-internet) | Only current text. Build history by re-downloading periodically. |
| **Point-in-time API** | UK (legislation.gov.uk) | Request any law at any date via URL parameter. |

Choose the strategy that matches your data source. The pipeline supports all of them -- the `Reform` model is flexible enough for any.

## Subnational jurisdictions

If a country has subnational legislation (e.g., Spain's Comunidades Autonomas, Germany's Bundeslaender), use the `jurisdiccion` field in `NormaMetadata`.

We follow the [ELI (European Legislation Identifier)](https://eur-lex.europa.eu/eli-register/what_is_eli.html) standard: `{country}` for national, `{country}-{region}` for subnational.

```
legalize-es/
  es/              # national
  es-pv/           # Pais Vasco
  es-ct/           # Catalunya
```

The `norma_to_filepath()` function handles this automatically based on `metadata.jurisdiccion`.

All subnational laws live in the same repo as national laws.
