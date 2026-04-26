# Caching layers

The pipeline maintains several distinct caches. Knowing what each one does — and how to invalidate it — is essential when iterating on parser code, debugging stale data, or operating an agent that should not waste rate-limit budget.

The layers, from network-edge inward:

| # | Layer | Storage | Scope | Invalidates with |
|---|---|---|---|---|
| 1 | **HTTP response cache** | `.cache/http_cache.sqlite` (SQLite) | per-URL, all GETs, currently MX only | `--force` (per-URL), `rm .cache/http_cache.sqlite` (whole) |
| 2 | **Discovery cache** | `<data_dir>/discovery_ids.txt` | per-country list of norm IDs | `--force` (CLI), or delete the file |
| 3 | **Parsed JSON cache** | `<data_dir>/json/{id}.json` | per-norm parsed output | `--force` (CLI), or `rm <data_dir>/json/*.json` |
| 4 | **In-process index cache** | RAM in `MXClient` | process lifetime only | terminate the process |

`<data_dir>` defaults to `../countries/data-{cc}/` per `config.yaml::countries.{cc}.data_dir`.

---

## 1. HTTP response cache

Wrapping `requests.Session` with `requests_cache.CachedSession`, SQLite-backed. Every GET response (HTML, PDF, DOC — content-type agnostic) is stored keyed by URL.

- **Path:** `.cache/http_cache.sqlite` (relative to the repo root). Set via `config.yaml::countries.mx.cache_dir = ".cache"`.
- **TTL:** `requests_cache.NEVER_EXPIRE`. Entries do not auto-expire — diputados publishes immutable URLs (each reform PDF/DOC has a stable URL), so cache invalidation is opt-in.
- **Rate-limit interaction:** cache hits **skip the rate-limit wait entirely**. Re-running a fetch that's 100% cached takes seconds, not the rate-limit-bound wall time.
- **Currently wired:** MX only. Other country fetchers don't yet use it.

### How to clear

| Goal | Command |
|---|---|
| Wipe everything | `rm .cache/http_cache.sqlite` |
| Refresh a single law | `wg-exec env HOME=$HOME uv run legalize fetch -c mx <ID> --force` |
| Refresh all of MX | `wg-exec env HOME=$HOME uv run legalize fetch -c mx --all --force` |
| Inspect contents | `uv run python -c "import requests_cache; s=requests_cache.CachedSession('.cache/http_cache.sqlite'); print(len(list(s.cache.urls())))"` |

The `--force` flag patches `session.send` to delete the cached entry on each GET, so the call goes through the network *and* the fresh response replaces the stale entry.

---

## 2. Discovery cache

The list of norm IDs returned by a country's `Discovery` step (e.g. all 316 laws on the Diputados index) is written to `<data_dir>/discovery_ids.txt` so re-runs don't re-enumerate the source.

- **Path:** e.g. `/home/dev/countries/data-mx/discovery_ids.txt`
- **Format:** one ID per line.
- **Bypass:** `--force` re-runs discovery and overwrites the file.

### How to clear

```sh
rm /home/dev/countries/data-mx/discovery_ids.txt
```

---

## 3. Parsed JSON cache

The output of the parser. One JSON file per norm.

- **Path:** `<data_dir>/json/{id}.json` (e.g. `/home/dev/countries/data-mx/json/DIP-CPEUM.json`)
- **Behavior:** if `{id}.json` exists and `--force` is not set, the pipeline **skips parsing** — it loads the existing JSON. If `--force` is set, parsing runs again (and the HTTP layer is bypassed too via the MX `--force` patch).
- **Implication:** when a parser bug is fixed, the JSONs need to be regenerated. They will not change on their own.

### How to clear

```sh
# Whole country
rm -rf /home/dev/countries/data-mx/json/

# Single law
rm /home/dev/countries/data-mx/json/DIP-CPEUM.json
```

---

## 4. In-process index cache

The MX client lazily loads the Diputados master index once per process and reuses it for every law. Lives in RAM only.

- Not user-managed. Restart the process to invalidate.

---

## Common workflows

### Fix a parser bug, regenerate without re-hitting the network

This is the canonical "I changed the parser code; make all outputs reflect the fix" flow:

```sh
# 1. Wipe the parsed JSONs (forces re-parse)
rm /home/dev/countries/data-mx/json/*.json

# 2. Fetch — but DON'T pass --force.
#    --force would also bypass the HTTP cache; we want cache hits.
wg-exec env HOME=$HOME uv run legalize fetch -c mx --all

# 3. Re-render
uv run python scripts/export_mx.py
```

What happens:
- HTTP cache **hits** for every URL (no network, no rate-limit waits).
- Parser runs against cached `.doc` bytes with the new code.
- Fresh JSON written, fresh Markdown rendered.

### Force a fresh pull from the source for one law

The source actually changed (rare — usually only on reform). Bypass the cache for just that law:

```sh
wg-exec env HOME=$HOME uv run legalize fetch -c mx DIP-CPEUM --force
```

### Force a fresh pull for everything

Most expensive option — every URL hits the network.

```sh
wg-exec env HOME=$HOME uv run legalize fetch -c mx --all --force
```

At the configured rate limit (`config.yaml::countries.mx.source.requests_per_second`), expect this to take a long time. Use sparingly.

---

## Using the caches from an agent

Agents working on parser changes should follow these patterns to avoid burning rate-limit budget.

### Don't invoke the CLI fetch unless you need to

When testing parser code on cached source bytes, read directly from the HTTP cache instead of running `legalize fetch`:

```python
import requests_cache

session = requests_cache.CachedSession(
    ".cache/http_cache.sqlite",
    backend="sqlite",
    expire_after=requests_cache.NEVER_EXPIRE,
)

# Pull cached .doc bytes for one law
resp = session.get("https://www.diputados.gob.mx/LeyesBiblio/doc/CPEUM.doc")
doc_bytes = resp.content  # may have come from cache; may have hit the network

# Confirm it was a cache hit (no network was used)
assert getattr(resp, "from_cache", False), "expected cache hit"
```

If `from_cache` is `False`, the URL wasn't cached and the agent just consumed rate-limit budget. Decide whether that's acceptable or fail loudly.

### Check what's cached before fetching

```python
import requests_cache
s = requests_cache.CachedSession(".cache/http_cache.sqlite", backend="sqlite")
urls = list(s.cache.urls())
print(f"{len(urls)} URLs cached")
print(f"  .doc: {sum(1 for u in urls if u.endswith('.doc'))}")
print(f"  .pdf: {sum(1 for u in urls if u.endswith('.pdf'))}")
```

### When an agent must run a fetch

- **Don't compete** with another in-flight `legalize fetch` for the same country — they'll both throttle each other and double the rate-limit cost.
- **Use `wg-exec`** for any MX fetch (diputados.gob.mx is firewalled from this VM otherwise).
- **Pass `env HOME=$HOME`** when wrapping `wg-exec` around `uv` so uv finds its cache directory:
  ```sh
  wg-exec env HOME=$HOME uv run legalize fetch -c mx <ID>
  ```
- **Prefer one law over many** for smoke tests (1 fetch vs 316).
- **Don't pass `--force`** unless you genuinely need to bypass the cache. The default behavior (cache hit on already-fetched URLs) is almost always what you want.

### Don't write to caches the user owns

- Parser test fixtures should live in `tests/fixtures/`, not the cache. Don't read `.cache/http_cache.sqlite` and emit a copy of the bytes you find — extract them into a checked-in fixture file.
- Don't `rm -rf .cache/` as a "clean state" step. It throws away the user's work; let them decide.
