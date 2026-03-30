# Adding a New Country to Legalize

This guide walks through adding a new country to the pipeline. Use Sweden (`se`) as the latest reference implementation, and France (`fr`) for XML dump-based sources.

## Prerequisites

Before starting, you need:
- An open data source for the country's legislation (API, XML dump, or HTML)
- Understanding of the source's data format (XML, JSON, HTML, plain text)
- Knowledge of the country's legal hierarchy (types of laws, reform process)
- Whether the source provides version history (amendments) or only current text

## Step 1: Implement the fetcher modules

Create 3 files in `src/legalize/fetcher/`:

### a) Client — `client_{code}.py`

Implements `LegislativeClient` from `fetcher/base.py`:

```python
from legalize.fetcher.base import LegislativeClient

class MyClient(LegislativeClient):
    def get_texto(self, norm_id: str) -> bytes:
        """Fetch the consolidated text of a law. Returns raw bytes (XML/JSON/HTML)."""

    def get_metadatos(self, norm_id: str) -> bytes:
        """Fetch metadata for a law. Returns raw bytes.
        Can return the same data as get_texto if metadata is embedded."""

    def close(self) -> None:
        """Clean up (close HTTP sessions, etc.)."""
```

If the data source has a separate amendment register (like Sweden's SFSR), add an extra method:

```python
    def get_amendment_register(self, norm_id: str) -> bytes:
        """Fetch the amendment history register (optional).
        Not part of the base interface — country-specific."""
```

**Important:**
- Add rate limiting (respect the data source — typically 500ms-1s between requests)
- Add retry with backoff for 429/503 errors
- Set a descriptive User-Agent: `legalize-bot/1.0 (+https://github.com/legalize-dev/legalize-pipeline)`

**Before writing the client, test the API rate limits:**

```python
# Run this FIRST to understand the API's tolerance
import time, requests
times = []
for i in range(20):
    t0 = time.time()
    r = requests.get(f"https://API_URL/?page={i+1}", timeout=10)
    times.append(time.time() - t0)
    print(f"  req {i+1}: {r.status_code} ({times[-1]:.2f}s)")
    if r.status_code == 429:
        print(f"  → Rate limited at request {i+1}. Add delay.")
        break
print(f"Avg: {sum(times)/len(times):.2f}s")
# If no 429: safe to go fast. If 429 at N: add delay of ~1s/request.
```

Document the results in your client's docstring so future maintainers know the limits.

**Reference implementations:**
- `client_se.py` — HTTP API (Riksdagen, JSON, no strict rate limit)
- `client_legi.py` — local XML dump (LEGI, reads from filesystem)
- `client.py` — HTTP API with caching (BOE, XML, 2 req/s limit)

### b) Discovery — `discovery_{code}.py`

Implements `NormDiscovery` from `fetcher/base.py`:

```python
from legalize.fetcher.base import NormDiscovery

class MyDiscovery(NormDiscovery):
    def discover_all(self, client, **kwargs) -> Iterator[str]:
        """Yield all norm IDs in the catalog.
        Filter OUT amendment documents — only yield base laws."""

    def discover_daily(self, client, target_date, **kwargs) -> Iterator[str]:
        """Yield norm IDs published/updated on a specific date.
        For amendments: yield the BASE law's ID (not the amendment's)."""
```

**Reference:** `discovery_se.py` (paginates API), `discovery_legi.py` (scans filesystem)

### c) Text Parser + Metadata Parser — `parser_{code}.py`

Implements `TextParser` and `MetadataParser` from `fetcher/base.py`:

```python
from legalize.fetcher.base import TextParser, MetadataParser

class MyTextParser(TextParser):
    def parse_texto(self, data: bytes) -> list[Bloque]:
        """Parse raw text into Bloque objects.
        Each structural unit (chapter, section, article) becomes a Bloque.
        Each Bloque has one or more Versions with paragraphs."""

    def extract_reforms(self, data: bytes) -> list[Reform]:
        """Extract reform timeline.
        Each Reform = a point in time where the law changed."""
```

The output models (in `models.py`):
- `Bloque` — structural unit (article, chapter, section)
- `Version` — a temporal version with `fecha_publicacion` and `paragraphs`
- `Paragraph` — text + `css_class` (for markdown rendering: `"articulo"`, `"parrafo"`, `"titulo_tit"`)
- `NormaMetadata` — title, id, country, rango, dates, status
- `Reform` — fecha, id_norma, bloques_afectados

**Key rules:**
- Your parser MUST produce these generic objects. The markdown renderer, git committer, and web app work with them regardless of country.
- `identificador` must be filesystem-safe: no `:`, no spaces, no special chars. Use `-` instead. Example: SFS `1962:700` → `SFS-1962-700`.
- `pais` must be the ISO 3166-1 alpha-2 country code (e.g., `"se"`, `"fr"`, `"es"`).
- `rango` is a free-form string describing the law type. It goes in the YAML frontmatter, not in the file path. Define country-specific constants as `Rango("lag")`, `Rango("balk")`, etc.

**Rango detection from title** — common pattern:
```python
def _detect_rango(title: str) -> Rango:
    lower = title.lower()
    if "grundlag" in lower:
        return Rango("grundlag")
    if "balk" in lower:
        return Rango("balk")
    if "förordning" in lower:
        return Rango("forordning")
    return Rango("lag")  # default
```

**Reference:** `parser_se.py` (plain text + HTML), `parser_legi.py` (XML), `parser_boe.py` (XML)

## Step 2: Register in the pipeline

### a) `src/legalize/countries.py`

Add to the `REGISTRY` dict:

```python
REGISTRY = {
    # ... existing ...
    "xx": {
        "client": ("legalize.fetcher.client_xx", "MyClient"),
        "discovery": ("legalize.fetcher.discovery_xx", "MyDiscovery"),
        "text_parser": ("legalize.fetcher.parser_xx", "MyTextParser"),
        "metadata_parser": ("legalize.fetcher.parser_xx", "MyMetadataParser"),
    },
}
```

### b) `web/src/legalize/web/countries.py` (private repo)

Add the web config with ALL required UI strings:

```python
COUNTRIES = {
    # ... existing ...
    "xx": {
        "name": "Country Name",          # in the country's language
        "lang": "xx",                     # ISO 639-1
        "source": "Data Source Name",
        "source_url": "https://...",
        "github_repo": "legalize-dev/legalize-xx",
        "cta_enabled": False,
        "rangos": {                       # display names for law types
            "act": "Act",
            "ordinance": "Ordinance",
        },
        "strings": {                      # ALL UI strings in the country's language
            "search": "...",
            "catalog": "...",
            "dashboard": "...",
            "search_placeholder": "...",
            "results": "...",
            "result": "...",
            "no_results": "...",
            "reforms": "...",
            "reform": "...",
            "articles": "...",
            "article": "...",
            "expand": "...",
            "collapse": "...",
            "before": "...",
            "after": "...",
            "reform_history": "...",
            "view_changes": "...",
            "current_text": "...",
            "load_more": "...",
            "remaining": "...",
            "reforms_by_month": "...",
            "laws_by_decade": "...",
            "by_type": "...",
            "no_reforms": "...",
            "json_api": "JSON API",
            # ... copy all keys from an existing country config
        },
    },
}
```

**Tip:** Copy all `strings` keys from the `"es"` or `"se"` config and translate them.

## Step 3: Create the pipeline orchestrator

Create `src/legalize/pipeline_{code}.py`:

```python
def fetch_one_xx(config, norm_id, force=False) -> NormaCompleta | None:
    """Fetch and parse one law."""

def fetch_all_xx(config, force=False) -> list[str]:
    """Discover and fetch all laws."""

def bootstrap_xx(config, dry_run=False) -> int:
    """Full bootstrap: discover + fetch + commit."""
```

The commit phase reuses `pipeline.commit_all()` which is generic.

**Reference:** `pipeline_se.py` (Sweden), `pipeline_fr.py` (France)

## Step 4: Add CLI commands

In `src/legalize/cli.py`:

```python
@cli.command("fetch-xx")
@click.argument("norm_ids", nargs=-1)
@click.option("--discover", is_flag=True)
@click.option("--force", is_flag=True)
@click.option("--data-dir", default=None)
@click.pass_context
def fetch_xx(ctx, norm_ids, discover, force, data_dir):
    """Fetch legislation from [Country]."""

@cli.command("bootstrap-xx")
@click.option("--repo-path", default="../xx")
@click.option("--data-dir", default="../data-xx")
@click.option("--dry-run", is_flag=True)
@click.pass_context
def bootstrap_xx_cmd(ctx, repo_path, data_dir, dry_run):
    """Full bootstrap for [Country]."""
```

## Step 5: Create the output repo

### Structure

Flat — all laws in `{country_code}/`, rango in YAML frontmatter:

```
legalize-{code}/
  {code}/              ← all laws here, flat
    ID-2024-123.md
    ID-2024-456.md
  README.md            ← in the country's language
  LICENSE              ← MIT
```

The `norma_to_filepath()` function generates `{pais}/{identificador}.md`.

### Create on GitHub

```bash
gh repo create legalize-dev/legalize-{code} --public \
  --description "Legislation from [Country] in Markdown, version-controlled with Git"

git init ../xx/
mkdir -p ../xx/{code}
touch ../xx/{code}/.gitkeep
# Create README.md and LICENSE
git -C ../xx add -A
git -C ../xx commit --author="Legalize <legalize@legalize.es>" \
  -m "[bootstrap] Init legalize-{code}"
git -C ../xx remote add origin git@github.com:legalize-dev/legalize-{code}.git
git -C ../xx push -u origin main
```

### README

Write the README **in the country's language**. Include:
- What the repo is (legislation as Markdown + Git)
- File structure
- Data sources with links
- **Credits** — thank every project and data source used:
  - The official data provider (government API, parliament, etc.)
  - Any open-source projects whose code or approach you referenced
  - Any data standards or prior art that informed the parser
- Format explanation (YAML frontmatter + Markdown body)
- Author (Legalize pipeline)
- License (MIT for formatting, legislation is public domain)

### Subnational jurisdictions (autonomous communities, states, provinces)

If a country has subnational legislation (e.g., Spain's Comunidades Autónomas, Germany's Bundesländer), use the `jurisdiccion` field in `NormaMetadata`.

We follow the [ELI (European Legislation Identifier)](https://eur-lex.europa.eu/eli-register/what_is_eli.html) standard for jurisdiction codes. ELI is the EU standard for identifying legislation across borders.

**ELI jurisdiction format:** `{country}` for national, `{country}-{region}` for subnational:

```
legalize-es/
  es/                  ← national (ELI: /eli/es/...)
    BOE-A-1978-31229.md
  es-pv/               ← País Vasco (ELI: /eli/es-pv/...)
    BOPV-2024-123.md
  es-ct/               ← Catalunya (ELI: /eli/es-ct/...)
    DOGC-2024-456.md
  es-an/               ← Andalucía (ELI: /eli/es-an/...)
    BOJA-2024-789.md
```

The `norma_to_filepath()` function handles this automatically:
- If `metadata.jurisdiccion` is set → `{jurisdiccion}/{identificador}.md`
- If not → `{pais}/{identificador}.md`

**ELI jurisdiction codes by country:**

| Country | National | Subnational examples |
|---------|----------|---------------------|
| Spain | `es` | `es-pv` (Basque), `es-ct` (Catalonia), `es-an` (Andalusia), `es-nc` (Navarre) |
| France | `fr` | `fr-idf` (Île-de-France), `fr-bre` (Bretagne) |
| Germany | `de` | `de-by` (Bavaria), `de-nw` (North Rhine-Westphalia) |
| Italy | `it` | `it-lom` (Lombardy), `it-tos` (Tuscany) |

For non-EU countries, use ISO 3166-2 subdivision codes following the same pattern:
- `us-ca` (California), `us-ny` (New York)
- `br-sp` (São Paulo), `br-rj` (Rio de Janeiro)

All subnational laws live in the same repo as national laws (e.g., `legalize-es`), just in different folders.

**Reference:** [Spain's ELI implementation](https://www.boe.es/legislacion/eli.php) | [ELI standard](https://eur-lex.europa.eu/eli-register/what_is_eli.html)

### Filesystem-safe identifiers

The `identificador` field is used as the filename. It MUST be safe for all operating systems:
- No `:` (invalid on Windows)
- No spaces (causes issues with shell commands)
- No `/`, `\`, `*`, `?`, `"`, `<`, `>`, `|`
- Use `-` as separator

Examples:
- Spain: `BOE-A-1978-31229` (already safe)
- France: `LEGITEXT000006069414` (already safe)
- Sweden: `1962:700` → normalize to `SFS-1962-700`

## Step 6: Write tests

Create `tests/test_parser_{code}.py` with:

```python
class TestParser:
    def test_parse_texto(self): ...           # Parse sample text → Bloques
    def test_metadata(self): ...              # Parse metadata → NormaMetadata
    def test_date_parsing(self): ...          # Country-specific date formats
    def test_rango_detection(self): ...       # Title → correct rango
    def test_filesystem_safe_id(self): ...    # No colons/spaces in identificador
    def test_reforms_from_register(self): ... # Amendment register → Reform list

class TestCountriesDispatch:
    def test_get_text_parser(self):
        from legalize.countries import get_text_parser
        parser = get_text_parser("{code}")
        assert isinstance(parser, MyTextParser)

class TestFilepath:
    def test_norma_to_filepath(self):
        from legalize.transformer.slug import norma_to_filepath
        # ... assert path == "{code}/ID-XXXX.md"
```

## Checklist

- [ ] `fetcher/client_{code}.py` — with rate limiting and retry
- [ ] `fetcher/discovery_{code}.py` — filters out amendments
- [ ] `fetcher/parser_{code}.py` — text parser + metadata parser
- [ ] `countries.py` — registry entry
- [ ] `web/countries.py` — full config with ALL UI strings translated
- [ ] `pipeline_{code}.py` — fetch + bootstrap orchestration
- [ ] `cli.py` — `fetch-{code}` and `bootstrap-{code}` commands
- [ ] GitHub repo `legalize-dev/legalize-{code}` — with README in local language + credits
- [ ] `tests/test_parser_{code}.py` — passing
- [ ] Test with 3 laws before full bootstrap
- [ ] Full bootstrap run

## Architecture reference

```
User request → FastAPI route → DB (metadata) + Blob (content)
                                    ↑                ↑
                              ingest.py          ingest.py
                                    ↑                ↑
                              pipeline_{code}.py (fetch + parse + save)
                                    ↑
                              fetcher/{code} (client, discovery, parser)
                                    ↑
                              Official open data source
```

The generic layers (markdown, frontmatter, git, web) never change. Only the fetcher layer is country-specific.

## Version history strategies

Different countries provide different levels of historical data:

| Strategy | Example | What you get |
|----------|---------|-------------|
| **Embedded versions** | Spain (BOE), France (LEGI) | Full text at every point in time. Best case. |
| **Amendment register** | Sweden (SFSR) | Timeline of which sections changed when, but only current text. |
| **Snapshots over time** | Germany (gesetze-im-internet) | Only current text. Build history by re-downloading periodically. |
| **Point-in-time API** | UK (legislation.gov.uk) | Request any law at any date via URL parameter. |

Choose the strategy that matches your data source. The pipeline supports all of them — the `Reform` model is flexible enough for any.
