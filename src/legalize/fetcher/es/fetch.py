"""Spain-specific fetch operations.

Downloads XML + metadata from the BOE API and saves to data/.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console

from legalize.config import Config
from legalize.models import NormaCompleta
from legalize.storage import load_norma_from_json, save_raw_xml, save_structured_json
from legalize.transformer.xml_parser import extract_reforms, parse_text_xml

console = Console()
logger = logging.getLogger(__name__)


def fetch_one(config: Config, boe_id: str, force: bool = False) -> NormaCompleta | None:
    """Download XML + metadata of a law and save to data/.

    If already exists in data/ and force=False, does not re-download.
    Returns NormaCompleta or None on error.
    """
    from legalize.fetcher.cache import FileCache
    from legalize.fetcher.es.client import BOEClient
    from legalize.fetcher.es.metadata import parse_metadata

    json_path = Path(config.data_dir) / "json" / f"{boe_id}.json"
    if json_path.exists() and not force:
        console.print(f"  [dim]{boe_id} already downloaded, skipping[/dim]")
        return load_norma_from_json(json_path)

    cache = FileCache(config.cache_dir)
    with BOEClient(config.boe, cache) as client:
        try:
            console.print(f"  Downloading [bold]{boe_id}[/bold]...")
            meta_xml = client.get_metadata(boe_id)
            metadata = parse_metadata(meta_xml, boe_id)
            text_xml = client.get_consolidated_text(boe_id, bypass_cache=force)

            blocks = parse_text_xml(text_xml)
            reforms = extract_reforms(blocks)

            norm = NormaCompleta(
                metadata=metadata,
                bloques=tuple(blocks),
                reforms=tuple(reforms),
            )

            save_raw_xml(config.data_dir, boe_id, text_xml)
            save_structured_json(config.data_dir, norm)

            console.print(
                f"  [green]✓[/green] {metadata.titulo_corto}: "
                f"{len(blocks)} bloques, {len(reforms)} versiones"
            )
            return norm

        except Exception:
            logger.error("Error downloading %s", boe_id, exc_info=True)
            console.print(f"  [red]✗ Error downloading {boe_id}[/red]")
            return None


def fetch_all(config: Config, force: bool = False) -> list[str]:
    """Download all norms from config.normas_fijas.

    Returns list of successfully downloaded BOE-IDs.
    """
    console.print("[bold]Fetch — downloading norms from BOE[/bold]\n")
    fetched = []
    for boe_id in config.scope.normas_fijas:
        norm = fetch_one(config, boe_id, force=force)
        if norm is not None:
            fetched.append(boe_id)
    console.print(f"\n[bold green]✓ {len(fetched)} norms downloaded[/bold green]")
    return fetched


def fetch_catalog(config: Config, force: bool = False) -> list[str]:
    """Download ALL state-level norms from the BOE catalog.

    Paginates correctly (the API has a 10,000 per request limit).
    Downloads all state-level norms, regardless of rango.
    Skips those already in data/.
    """
    import requests

    console.print("[bold]Fetch catalog — downloading full BOE catalog[/bold]\n")

    # Paginate full catalog
    base_url = f"{config.boe.base_url}/api/legislacion-consolidada"
    all_items: list[dict] = []
    offset = 0
    batch = 1000

    console.print("  Querying catalog (paginated)...")
    while True:
        resp = requests.get(
            base_url,
            headers={"Accept": "application/json"},
            params={"limit": batch, "offset": offset},
            timeout=60,
        )
        resp.raise_for_status()
        items = resp.json().get("data", [])
        if not items:
            break
        all_items.extend(items)
        offset += batch

    # Filter state-level only
    in_scope = [
        item["identificador"] for item in all_items if item.get("ambito", {}).get("codigo") == "1"
    ]

    console.print(f"  {len(in_scope)} state-level norms found\n")

    # Download each one (skips those that already exist)
    fetched = []
    errors = 0
    skipped = 0
    for i, boe_id in enumerate(in_scope, 1):
        json_path = Path(config.data_dir) / "json" / f"{boe_id}.json"
        if json_path.exists() and not force:
            skipped += 1
            continue

        norm = fetch_one(config, boe_id, force=force)
        if norm is not None:
            fetched.append(boe_id)
        else:
            errors += 1

        # Progress every 100
        if (len(fetched) + errors) % 100 == 0:
            console.print(
                f"  [{i}/{len(in_scope)}] {len(fetched)} new, {skipped} existing, {errors} errors"
            )

    console.print(f"\n[bold green]✓ {len(fetched)} new norms downloaded[/bold green]")
    console.print(f"  {skipped} already existed, {errors} errors")

    total = len(list((Path(config.data_dir) / "json").glob("*.json")))
    console.print(f"  Total in data/: {total} norms")

    return fetched


def fetch_catalog_ccaa(config: Config, jurisdiction: str, force: bool = False) -> list[str]:
    """Download all norms for an autonomous community from the BOE catalog.

    Filters by ambito=2 (Autonomico) and the ELI jurisdiction code.
    Uses the same BOE API — CCAA laws published in BOE have full consolidated text.

    Args:
        config: Pipeline configuration.
        jurisdiction: ELI code (e.g., "es-pv", "es-ct", "es-an").
        force: Re-download even if already cached.

    Returns:
        List of successfully downloaded BOE-IDs.
    """
    import requests

    from legalize.fetcher.es.metadata import _DEPT_TO_JURISDICTION

    # Reverse lookup: jurisdiction -> all matching departamento codes
    dept_codes = [code for code, jur in _DEPT_TO_JURISDICTION.items() if jur == jurisdiction]

    if not dept_codes:
        console.print(f"[red]Unknown jurisdiction: {jurisdiction}[/red]")
        return []

    console.print(f"[bold]Fetch CCAA catalog — {jurisdiction} (depts={dept_codes})[/bold]\n")

    # Paginate full catalog
    base_url = f"{config.boe.base_url}/api/legislacion-consolidada"
    all_items: list[dict] = []
    offset = 0
    batch = 1000

    console.print("  Querying catalog (paginated)...")
    while True:
        resp = requests.get(
            base_url,
            headers={"Accept": "application/json"},
            params={"limit": batch, "offset": offset},
            timeout=60,
        )
        resp.raise_for_status()
        items = resp.json().get("data", [])
        if not items:
            break
        all_items.extend(items)
        offset += batch

    # Filter by CCAA departamento (may have multiple codes)
    dept_code_set = set(dept_codes)
    in_scope = [
        item["identificador"]
        for item in all_items
        if item.get("ambito", {}).get("codigo") == "2"
        and item.get("departamento", {}).get("codigo") in dept_code_set
    ]

    console.print(f"  {len(in_scope)} norms found for {jurisdiction}\n")

    # Download each one
    fetched = []
    errors = 0
    skipped = 0
    for i, boe_id in enumerate(in_scope, 1):
        json_path = Path(config.data_dir) / "json" / f"{boe_id}.json"
        if json_path.exists() and not force:
            skipped += 1
            continue

        norm = fetch_one(config, boe_id, force=force)
        if norm is not None:
            fetched.append(boe_id)
        else:
            errors += 1

        if (len(fetched) + errors) % 50 == 0:
            console.print(
                f"  [{i}/{len(in_scope)}] {len(fetched)} new, {skipped} existing, {errors} errors"
            )

    console.print(f"\n[bold green]✓ {len(fetched)} new norms downloaded[/bold green]")
    console.print(f"  {skipped} already existed, {errors} errors")

    return fetched
