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
  parser.py              TextParser + MetadataParser: parse into Block/NormMetadata
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

    def get_text(self, norm_id: str) -> bytes:
        """Fetch the consolidated text of a law. Returns raw bytes."""
        resp = self._session.get(f"{self._base_url}/text/{norm_id}")
        resp.raise_for_status()
        return resp.content

    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata. Can return same data as get_text if metadata is embedded."""
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
from legalize.models import Block, NormStatus, NormMetadata, Paragraph, Rank, Version

class MyTextParser(TextParser):

    def parse_text(self, data: bytes) -> list[Any]:
        """Parse raw text into Block objects.

        Each structural unit (chapter, section, article) becomes a Block.
        Each Block has one or more Versions with paragraphs.
        """
        return [
            Block(
                id="art-1",
                block_type="article",
                title="Article 1",
                versions=(
                    Version(
                        norm_id="LAW-2024-1",
                        publication_date=date(2024, 1, 15),
                        effective_date=date(2024, 1, 15),
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
        blocks = self.parse_text(data)
        from legalize.transformer.xml_parser import extract_reforms
        return extract_reforms(blocks)


class MyMetadataParser(MetadataParser):

    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse raw metadata into NormMetadata."""
        return NormMetadata(
            title="Full Title of the Law",
            short_title="Short Title",
            identifier=norm_id,        # must be filesystem-safe
            country="xx",              # ISO 3166-1 alpha-2
            rank=Rank("act"),          # free-form string
            publication_date=date(2024, 1, 15),
            status=NormStatus.IN_FORCE,
            department="Ministry of Justice",
            source="https://official-source.gov/law/123",
            extra=(                    # country-specific fields (optional)
                ("department", "Ministry of Justice"),
                ("summary", "Establishes the legal framework for..."),
                ("eli", "https://data.example.gov/eli/act/1/2024"),
            ),
        )
```

**Key rules for the output models:**

- `Block` -- structural unit (article, chapter, section) with versioned content
- `Version` -- a temporal version with `publication_date` and `paragraphs`
- `Paragraph` -- text + `css_class` (controls markdown rendering: `"articulo"` for article headings, `"parrafo"` for body text, `"titulo_tit"` for title headings, `"capitulo_tit"` for chapter headings)
- `NormMetadata` -- title, id, country, rank, dates, status
- `identifier` must be filesystem-safe: no `:`, no spaces, no `/\*?"<>|`. Use `-` as separator. Example: SFS `1962:700` becomes `SFS-1962-700`
- `country` must be the ISO 3166-1 alpha-2 code (e.g., `"se"`, `"fr"`, `"es"`)
- `rank` is a free-form string (`Rank("act")`, `Rank("code")`, `Rank("lag")`). Goes in YAML frontmatter, not in the file path
- `extra` is a tuple of `(key, value)` pairs for country-specific metadata. These are rendered as additional YAML fields in the frontmatter, after the generic fields. Use English keys. Only countries that populate `extra` get these fields -- other countries are unaffected
- You can reuse `extract_reforms()` from `transformer/xml_parser.py` -- it works with any list of Blocks

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
    repo_path: "../countries/xx"           # output git repo
    data_dir: "../countries/data-xx"       # raw data + parsed JSON
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

git init ../countries/xx/
mkdir -p ../countries/xx/{code}
git -C ../countries/xx commit --allow-empty \
  -m "[bootstrap] Init legalize-{code}"
git -C ../countries/xx remote add origin git@github.com:legalize-dev/legalize-{code}.git
git -C ../countries/xx push -u origin main
```

Output structure is flat -- all laws in `{country_dir}/`, rank goes in YAML frontmatter:

```
legalize-{code}/
  {code}/
    ID-2024-123.md
    ID-2024-456.md
  README.md       # in the country's language
  LICENSE         # MIT
```

The `norm_to_filepath()` function generates `{country}/{identifier}.md` automatically.

## Step 5: Implement daily processing

Create `src/legalize/fetcher/{code}/daily.py` with a `daily()` function. The CLI dispatches to this file via dynamic import (`legalize.fetcher.{code}.daily`). Without it, `legalize daily -c {code}` will print "not yet implemented".

```python
from datetime import date
from legalize.config import Config
from legalize.state.store import StateStore, resolve_dates_to_process

def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing for {country}: discover + fetch + commit new norms."""
    from legalize.fetcher.{code}.client import MyClient
    from legalize.fetcher.{code}.discovery import MyDiscovery
    from legalize.fetcher.{code}.parser import MyMetadataParser, MyTextParser

    cc = config.get_country("{code}")
    state = StateStore(cc.state_path)
    state.load()

    # Determine dates to process (includes safety cap + weekday filter)
    dates_to_process = resolve_dates_to_process(
        state, cc.repo_path, target_date,
        skip_weekdays={6},  # adapt to source's schedule
    )
    if dates_to_process is None:
        console.print("[yellow]No last date found. Use --date or run bootstrap.[/yellow]")
        return 0
    if not dates_to_process:
        console.print("[green]Nothing to process — up to date[/green]")
        return 0

    # For each date: discover → fetch → commit
    with MyClient.create(cc) as client:
        for current_date in dates_to_process:
            norm_ids = list(discovery.discover_daily(client, current_date))
            for norm_id in norm_ids:
                # fetch metadata + text
                # render markdown
                # write_and_add + commit
                ...
            state.last_summary_date = current_date

    state.save()
    return commits_created
```

The flow is always the same — the country-specific part is how you discover and fetch. See `fetcher/at/daily.py` (API-based) and `fetcher/es/daily.py` (sumario-based) for complete examples.

**Key responsibilities:**
- Determine which dates need processing (state tracking via `StateStore`)
- Call `discover_daily()` for each date
- Fetch + parse + render markdown for each norm
- Create git commits with appropriate `CommitType` (NEW, REFORM, CORRECTION)
- Update `state.last_summary_date` after each date
- Handle `--dry-run` (print what would happen, don't commit)
- Handle `config.git.push` (push to remote after commits)

### Date resolution (centralized)

Use `resolve_dates_to_process()` from `state/store.py` instead of writing the date logic by hand. It handles state inference, git fallback, the 10-day safety cap, and weekday filtering:

```python
from legalize.state.store import StateStore, resolve_dates_to_process

state = StateStore(cc.state_path)
state.load()

dates_to_process = resolve_dates_to_process(
    state, cc.repo_path, target_date,
    skip_weekdays={6},  # skip Sunday (Mon-Sat schedule)
)
if dates_to_process is None:
    console.print("[yellow]No last date found. Use --date or run bootstrap.[/yellow]")
    return 0
if not dates_to_process:
    console.print("[green]Nothing to process — up to date[/green]")
    return 0
```

The safety cap (10 days) prevents accidentally processing months of history when no `--date` is given (e.g., first CI run after setup, or after a long outage). Users can still process older dates explicitly with `--date`.

Common `skip_weekdays` values:
- `{6}` — Mon-Sat (ES, FR, CL)
- `{5, 6}` — Mon-Fri (AT, PT)
- `None` — all days (LT)

### Handling reforms (affected norms pattern)

Many data sources publish reform dispositions (amendments) before updating the consolidated text of the affected law. This means fetching the reform disposition itself may return 404 or stale data. The solution: **process the affected (reformed) norms instead of the reform disposition**.

The pattern:

1. **Classify** each daily entry as NEW, CORRECTION, or REFORM. How you detect this depends on the source — it could be a field in the metadata, a keyword in the title, or a document type code.
2. **New/Correction** → try to download the entry itself, skip on 404 (not consolidated yet)
3. **Reform** → resolve which existing laws it modifies, then re-download those:

```python
# 1. Resolve affected norm IDs.
#    How: fetch the raw entry document (not consolidated text) and parse its
#    analysis/reference section. Each source has its own format — the key is
#    extracting the IDs of the laws being modified.
affected_ids = resolve_affected_norms(client, entry)

# 2. For each affected norm already in the repo:
for affected_id in affected_ids:
    # Idempotency: use 2-arg form (Source-Id + Norm-Id pair).
    # One reform can affect multiple norms — checking only source_id
    # would block processing after the first one.
    if repo.has_commit_with_source_id(entry.id, affected_id):
        continue

    # Re-download the consolidated text (bypass cache — we need the updated version)
    meta_xml = client.get_metadata(affected_id)
    text_xml = client.get_text(affected_id, bypass_cache=True)

    # Skip norms we don't track (lower-rank regulations, etc.)
    if not (repo_root / file_path).exists():
        continue

    # Render, compare, and commit as REFORM.
    # Source-Id = the reform entry (what caused the change)
    # Norm-Id = the affected law (what changed)
    reform = Reform(date=current_date, norm_id=entry.id, affected_blocks=())
    info = build_commit_info(CommitType.REFORM, metadata, reform, ...)
```

**Key details:**
- `bypass_cache=True` forces a fresh download — the source may have updated the consolidated text since our last fetch
- Idempotency uses the 2-arg `has_commit_with_source_id(source_id, norm_id)` — one reform can affect multiple norms
- The commit's `Source-Id` trailer is the reform entry (what caused the change), `Norm-Id` is the affected law (what changed)
- Norms not in the repo are silently skipped
- If the source hasn't updated the consolidated text yet, `write_and_add()` detects no change — no commit is created

**Data source latency:** Some sources populate the analysis/reference metadata asynchronously — fresh entries may not list affected norms for 1-2 days. In normal daily operation this is fine: today's run processes dates from a few days ago, when references are already populated. For backfill runs (processing months of past data), all references will be available.

**Reference:** `fetcher/es/daily.py` implements this pattern for Spain's BOE, resolving affected norms from the raw disposition XML's `<analisis>` section.

## Step 6: Write tests

Create `tests/test_parser_{code}.py` with fixture data (and optionally `tests/test_daily_{code}.py`):

```python
import pytest
from legalize.fetcher.{code}.parser import MyTextParser, MyMetadataParser
from legalize.countries import get_text_parser, get_metadata_parser

# Save sample data from your source in tests/fixtures/

class TestParser:
    def test_parse_text(self):
        data = Path("tests/fixtures/sample_{code}.xml").read_bytes()
        parser = MyTextParser()
        blocks = parser.parse_text(data)
        assert len(blocks) > 0
        assert blocks[0].versions  # has at least one version

    def test_metadata(self):
        data = Path("tests/fixtures/sample_{code}_meta.xml").read_bytes()
        parser = MyMetadataParser()
        meta = parser.parse(data, "NORM-ID-123")
        assert meta.country == "xx"
        assert meta.identifier == "NORM-ID-123"

    def test_filesystem_safe_id(self):
        # Ensure no colons, spaces, or special chars
        meta = ...
        assert ":" not in meta.identifier
        assert " " not in meta.identifier

class TestCountryDispatch:
    def test_registry(self):
        parser = get_text_parser("xx")
        assert isinstance(parser, MyTextParser)
```

## Step 7: Test end-to-end

```bash
# Fetch 5 laws to verify the client and discovery work
legalize fetch -c xx --all --limit 5

# Dry-run bootstrap to see what commits it would create
legalize bootstrap -c xx --dry-run

# Full bootstrap (creates git commits in the output repo)
legalize bootstrap -c xx

# Daily dry-run to verify daily processing
legalize daily -c xx --date 2026-03-28 --dry-run
```

## Checklist

- [ ] `fetcher/{code}/__init__.py` -- re-exports all classes
- [ ] `fetcher/{code}/client.py` -- with `create()`, rate limiting, retry
- [ ] `fetcher/{code}/discovery.py` -- `discover_all()` and `discover_daily()`
- [ ] `fetcher/{code}/parser.py` -- `TextParser` and `MetadataParser`
- [ ] `fetcher/{code}/daily.py` -- `daily()` function for incremental updates
- [ ] `countries.py` -- registry entry added
- [ ] `config.yaml` -- country section with source params
- [ ] `tests/test_parser_{code}.py` -- passing
- [ ] GitHub repo `legalize-dev/legalize-{code}` -- with README in local language
- [ ] Tested with `legalize fetch -c {code} --all --limit 5`
- [ ] Tested with `legalize bootstrap -c {code} --dry-run`
- [ ] Tested with `legalize daily -c {code} --date YYYY-MM-DD --dry-run`
- [ ] Tested daily reform path: run with a date that has reform dispositions, verify affected norms are resolved and commits created
- [ ] Tested daily idempotency: re-run the same date, verify 0 duplicate commits
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

If a country has subnational legislation (e.g., Spain's autonomous communities, Germany's Bundesländer), use the `jurisdiction` field in `NormMetadata`.

We follow the [ELI (European Legislation Identifier)](https://eur-lex.europa.eu/eli-register/what_is_eli.html) standard: `{country}` for national, `{country}-{region}` for subnational.

```
legalize-es/
  es/              # national
  es-pv/           # País Vasco
  es-ct/           # Catalunya
```

The `norm_to_filepath()` function handles this automatically based on `metadata.jurisdiction`.

All subnational laws live in the same repo as national laws.
