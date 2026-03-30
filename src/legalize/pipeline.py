"""Legalize pipeline orchestrator.

Incremental flows:
- fetch: download XML+JSON of one or more laws to data/ (does not touch git)
- commit: read JSON from data/ and generate commits for one or more laws (does not download anything)
- bootstrap: fetch --all + commit --all (shortcut)
- daily: process daily summary, incremental
- reprocess: re-download and regenerate a specific law
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.models import (
    CommitType,
    NormaCompleta,
    NormaMetadata,
    Reform,
)
from legalize.state.mappings import IdToFilename
from legalize.state.store import StateStore
from legalize.storage import load_norma_from_json, save_raw_xml, save_structured_json
from legalize.transformer.markdown import render_norma_at_date
from legalize.transformer.slug import norma_to_filepath
from legalize.transformer.xml_parser import extract_reforms, parse_texto_xml

console = Console()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# FETCH — download data from the API, does not touch git
# ─────────────────────────────────────────────


def fetch_one(config: Config, boe_id: str, force: bool = False) -> NormaCompleta | None:
    """Download XML + metadata of a law and save to data/.

    If already exists in data/ and force=False, does not re-download.
    Returns NormaCompleta or None on error.
    """
    from legalize.fetcher.cache import FileCache
    from legalize.fetcher.client import BOEClient
    from legalize.transformer.metadata import parse_metadatos

    json_path = Path(config.data_dir) / "json" / f"{boe_id}.json"
    if json_path.exists() and not force:
        console.print(f"  [dim]{boe_id} already downloaded, skipping[/dim]")
        return load_norma_from_json(json_path)

    cache = FileCache(config.cache_dir)
    with BOEClient(config.boe, cache) as client:
        try:
            console.print(f"  Downloading [bold]{boe_id}[/bold]...")
            meta_xml = client.get_metadatos(boe_id)
            metadata = parse_metadatos(meta_xml, boe_id)
            text_xml = client.get_texto_consolidado(boe_id, bypass_cache=force)

            blocks = parse_texto_xml(text_xml)
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
        item["identificador"]
        for item in all_items
        if item.get("ambito", {}).get("codigo") == "1"
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
                f"  [{i}/{len(in_scope)}] {len(fetched)} new, "
                f"{skipped} existing, {errors} errors"
            )

    console.print(f"\n[bold green]✓ {len(fetched)} new norms downloaded[/bold green]")
    console.print(f"  {skipped} already existed, {errors} errors")

    total = len(list((Path(config.data_dir) / "json").glob("*.json")))
    console.print(f"  Total in data/: {total} norms")

    return fetched


def fetch_catalog_ccaa(config: Config, jurisdiccion: str, force: bool = False) -> list[str]:
    """Download all norms for an autonomous community from the BOE catalog.

    Filters by ambito=2 (Autonómico) and the ELI jurisdiction code.
    Uses the same BOE API — CCAA laws published in BOE have full consolidated text.

    Args:
        config: Pipeline configuration.
        jurisdiccion: ELI code (e.g., "es-pv", "es-ct", "es-an").
        force: Re-download even if already cached.

    Returns:
        List of successfully downloaded BOE-IDs.
    """
    import requests

    from legalize.transformer.metadata import _DEPT_TO_JURISDICCION

    # Reverse lookup: jurisdiccion → all matching departamento codes
    dept_codes = [code for code, jur in _DEPT_TO_JURISDICCION.items() if jur == jurisdiccion]

    if not dept_codes:
        console.print(f"[red]Unknown jurisdiction: {jurisdiccion}[/red]")
        return []

    console.print(f"[bold]Fetch CCAA catalog — {jurisdiccion} (depts={dept_codes})[/bold]\n")

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

    console.print(f"  {len(in_scope)} norms found for {jurisdiccion}\n")

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
                f"  [{i}/{len(in_scope)}] {len(fetched)} new, "
                f"{skipped} existing, {errors} errors"
            )

    console.print(f"\n[bold green]✓ {len(fetched)} new norms downloaded[/bold green]")
    console.print(f"  {skipped} already existed, {errors} errors")

    return fetched


# ─────────────────────────────────────────────
# GENERIC FETCH — works for any country via dispatch
# ─────────────────────────────────────────────


def generic_fetch_one(
    config: Config,
    country: str,
    norm_id: str,
    force: bool = False,
) -> NormaCompleta | None:
    """Fetch one norm for any country using countries.py dispatch.

    Uses the country's client, text_parser, and metadata_parser.
    Saves structured JSON to data_dir.
    """
    from legalize.countries import get_client_class, get_metadata_parser, get_text_parser

    cc = config.get_country(country)
    safe_id = norm_id.replace(":", "-").replace("/", "-")
    json_path = Path(cc.data_dir) / "json" / f"{safe_id}.json"

    if json_path.exists() and not force:
        console.print(f"  [dim]{norm_id} already processed, skipping[/dim]")
        return load_norma_from_json(json_path)

    client_cls = get_client_class(country)
    text_parser = get_text_parser(country)
    meta_parser = get_metadata_parser(country)

    with client_cls.create(cc) as client:
        try:
            console.print(f"  Processing [bold]{norm_id}[/bold]...")

            meta_data = client.get_metadatos(norm_id)
            metadata = meta_parser.parse(meta_data, norm_id)

            text_data = client.get_texto(norm_id)
            blocks = text_parser.parse_texto(text_data)
            reforms = _extract_reforms_generic(text_parser, client, norm_id, blocks)

            norm = NormaCompleta(
                metadata=metadata,
                bloques=tuple(blocks),
                reforms=tuple(reforms),
            )

            save_structured_json(cc.data_dir, norm)

            console.print(
                f"  [green]✓[/green] {metadata.titulo_corto}: "
                f"{len(blocks)} bloques, {len(reforms)} versiones"
            )
            return norm

        except Exception:
            logger.error("Error processing %s", norm_id, exc_info=True)
            console.print(f"  [red]✗ Error processing {norm_id}[/red]")
            return None


def generic_fetch_all(
    config: Config,
    country: str,
    force: bool = False,
    limit: int | None = None,
) -> list[str]:
    """Fetch all norms for any country using discovery + dispatch.

    Uses NormDiscovery.discover_all() then fetches each norm.
    Supports --limit for testing (fetch only N norms).
    """
    from legalize.countries import get_client_class, get_discovery_class

    cc = config.get_country(country)
    client_cls = get_client_class(country)
    discovery_cls = get_discovery_class(country)

    # Discover all norm IDs
    with client_cls.create(cc) as client:
        discovery = discovery_cls()
        norm_ids = list(discovery.discover_all(client))

    if limit:
        norm_ids = norm_ids[:limit]

    console.print(f"[bold]Fetch — {len(norm_ids)} norms for {country.upper()}[/bold]\n")

    fetched = []
    errors = 0
    for i, norm_id in enumerate(norm_ids, 1):
        norm = generic_fetch_one(config, country, norm_id, force=force)
        if norm is not None:
            fetched.append(norm_id)
        else:
            errors += 1

        if i % 50 == 0:
            console.print(
                f"  [dim][{i}/{len(norm_ids)}] {len(fetched)} OK, {errors} errors[/dim]"
            )

    console.print(f"\n[bold green]✓ {len(fetched)} norms fetched[/bold green]")
    if errors:
        console.print(f"[yellow]⚠ {errors} errors[/yellow]")

    return fetched


def generic_bootstrap(
    config: Config,
    country: str,
    dry_run: bool = False,
    limit: int | None = None,
) -> int:
    """Full bootstrap for any country: discover + fetch + commit."""
    cc = config.get_country(country)

    console.print(f"[bold]Bootstrap {country.upper()}[/bold]\n")
    console.print(f"  Data dir: {cc.data_dir}")
    console.print(f"  Repo output: {config.git.repo_path}\n")

    fetched = generic_fetch_all(config, country, force=False, limit=limit)
    if not fetched:
        console.print("[yellow]No norms found.[/yellow]")
        return 0

    console.print("\n[bold]Commit — generating git history[/bold]\n")
    total_commits = commit_all(config, dry_run=dry_run)

    console.print(f"\n[bold green]✓ Bootstrap {country.upper()} completed[/bold green]")
    console.print(f"  {len(fetched)} norms fetched, {total_commits} commits created")

    return total_commits


def _extract_reforms_generic(text_parser, client, norm_id, blocks):
    """Extract reforms, with country-specific hooks.

    Swedish parser has extract_reforms_from_sfsr for the SFSR amendment register.
    Falls back to generic extract_reforms() from blocks.
    """
    if hasattr(text_parser, "extract_reforms_from_sfsr") and hasattr(
        client, "get_amendment_register"
    ):
        try:
            sfsr_html = client.get_amendment_register(norm_id)
            return text_parser.extract_reforms_from_sfsr(sfsr_html)
        except Exception:
            logger.warning(
                "Amendment register unavailable for %s, using text-based reforms",
                norm_id,
            )
    return extract_reforms(blocks)


# ─────────────────────────────────────────────
# COMMIT — generate git commits from local data/
# ─────────────────────────────────────────────


def commit_one(config: Config, norm_id: str, dry_run: bool = False) -> int:
    """Generate commits for ONE law from its JSON in data/.

    Does not download anything. Reads data/json/{norm_id}.json.
    Commits for this law are added to the repo without touching other laws.

    Returns number of commits created.
    """
    json_path = Path(config.data_dir) / "json" / f"{norm_id}.json"
    if not json_path.exists():
        console.print(f"  [red]{json_path} does not exist. Run fetch first.[/red]")
        return 0

    norm = load_norma_from_json(json_path)
    metadata = norm.metadata
    blocks = norm.bloques
    reforms = norm.reforms

    console.print(
        f"  [bold]{metadata.titulo_corto}[/bold]: "
        f"{len(blocks)} bloques, {len(reforms)} versiones"
    )

    if dry_run:
        for reform in reforms:
            is_first = reform == reforms[0]
            label = "bootstrap" if is_first else "reforma"
            console.print(f"    [dim]{reform.fecha} [{label}][/dim]")
        return 0

    repo = GitRepo(config.git.repo_path, config.git.committer_name, config.git.committer_email)
    repo.init()

    mappings = IdToFilename(config.mappings_path)
    mappings.load()

    commits_created = 0
    file_path = norma_to_filepath(metadata)

    for reform in reforms:
        # Idempotency check: Source-Id + Norm-Id (a single Source-Id can be both its own norm AND a reform of another)
        if repo.has_commit_with_source_id(reform.id_norma, metadata.identificador):
            continue

        is_first = reform == reforms[0]
        commit_type = CommitType.BOOTSTRAP if is_first else CommitType.REFORMA

        markdown = render_norma_at_date(metadata, blocks, reform.fecha)
        changed = repo.write_and_add(file_path, markdown)

        if not changed and not is_first:
            continue

        info = build_commit_info(commit_type, metadata, reform, blocks, file_path, markdown)
        sha = repo.commit(info)

        if sha:
            commits_created += 1
            console.print(f"    [green]✓[/green] {reform.fecha} — {info.subject}")

    mappings.set(metadata.identificador, file_path)
    mappings.save()

    return commits_created


def commit_all(config: Config, dry_run: bool = False) -> int:
    """Generate commits for ALL laws in data/json/.

    Processes each law independently — does not interleave commits.
    """
    json_dir = Path(config.data_dir) / "json"
    if not json_dir.exists():
        console.print("[red]No data in data/json/. Run fetch first.[/red]")
        return 0

    json_files = sorted(json_dir.glob("*.json"))
    console.print(f"[bold]Commit — generating commits for {len(json_files)} laws[/bold]\n")

    state = StateStore(config.state_path)
    state.load()

    total = 0
    errors = 0
    for i, json_file in enumerate(json_files, 1):
        norm_id = json_file.stem
        try:
            commits = commit_one(config, norm_id, dry_run=dry_run)
            total += commits

            if not dry_run and commits > 0:
                norm = load_norma_from_json(json_file)
                if norm.reforms:
                    state.mark_norma_processed(
                        norm.metadata.identificador,
                        norm.reforms[-1].fecha,
                        len(norm.reforms),
                    )
        except Exception:
            errors += 1
            logger.error("Error committing %s, continuing", norm_id, exc_info=True)
            console.print(f"  [red]✗ {norm_id} — error, continuing[/red]")

        # Save state periodically (every 50 laws)
        if not dry_run and i % 50 == 0:
            state.record_run(commits=total)
            state.save()
            console.print(f"  [dim][{i}/{len(json_files)}] {total} commits, {errors} errors[/dim]")

    if not dry_run:
        state.record_run(commits=total)
        state.save()

    console.print(f"\n[bold green]✓ {total} commits created[/bold green]")

    repo = GitRepo(config.git.repo_path, config.git.committer_name, config.git.committer_email)
    log_output = repo.log()
    if log_output and not dry_run:
        lines = log_output.strip().splitlines()
        console.print(f"\n[bold]Git log ({len(lines)} commits):[/bold]")
        for line in lines[-10:]:
            console.print(f"  {line}")
        if len(lines) > 10:
            console.print(f"  ... ({len(lines) - 10} more)")

    return total


# ─────────────────────────────────────────────
# BOOTSTRAP — shortcut: fetch all + commit all
# ─────────────────────────────────────────────


def bootstrap(config: Config, dry_run: bool = False) -> int:
    """Fetch + commit all norms in config."""
    fetch_all(config)
    return commit_all(config, dry_run=dry_run)


def bootstrap_from_local_xml(
    config: Config,
    metadata: NormaMetadata,
    xml_path: str | Path,
    dry_run: bool = False,
) -> int:
    """Bootstrap from a local XML (pilot/tests)."""
    xml_bytes = Path(xml_path).read_bytes()
    blocks = parse_texto_xml(xml_bytes)
    reforms = extract_reforms(blocks)

    norm = NormaCompleta(
        metadata=metadata,
        bloques=tuple(blocks),
        reforms=tuple(reforms),
    )

    save_raw_xml(config.data_dir, metadata.identificador, xml_bytes)
    save_structured_json(config.data_dir, norm)

    return commit_one(config, metadata.identificador, dry_run=dry_run)


# ─────────────────────────────────────────────
# DAILY — incremental, new reforms only
# ─────────────────────────────────────────────


def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing: process BOE summary/summaries."""
    from datetime import timedelta

    from legalize.fetcher.cache import FileCache
    from legalize.fetcher.client import BOEClient
    from legalize.fetcher.sumario import parse_sumario
    from legalize.transformer.metadata import parse_metadatos

    cache = FileCache(config.cache_dir)
    state = StateStore(config.state_path)
    state.load()
    mappings = IdToFilename(config.mappings_path)
    mappings.load()

    if target_date:
        dates_to_process = [target_date]
    else:
        start = state.ultimo_sumario
        if start is None:
            console.print("[yellow]No last summary found. Use --date or run bootstrap.[/yellow]")
            return 0
        start = start + timedelta(days=1)
        end = date.today()
        dates_to_process = []
        current = start
        while current <= end:
            if current.weekday() != 6:
                dates_to_process.append(current)
            current += timedelta(days=1)

    if not dates_to_process:
        console.print("[green]Nothing to process — up to date[/green]")
        return 0

    console.print(f"[bold]Daily — processing {len(dates_to_process)} day(s)[/bold]")

    repo = GitRepo(config.git.repo_path, config.git.committer_name, config.git.committer_email)
    commits_created = 0
    errors: list[str] = []

    with BOEClient(config.boe, cache) as client:
        for current_date in dates_to_process:
            console.print(f"\n  [bold]{current_date}[/bold]")

            try:
                xml_data = client.get_sumario(current_date)
                dispositions = parse_sumario(xml_data, config.scope)
            except Exception:
                msg = f"Error fetching summary for {current_date}"
                logger.error(msg, exc_info=True)
                errors.append(msg)
                continue

            if not dispositions:
                console.print("    No dispositions in scope")
                continue

            console.print(f"    {len(dispositions)} dispositions in scope")

            for disp in dispositions:
                if dry_run:
                    console.print(f"    [dim]{disp.id_boe} — {disp.titulo[:60]}...[/dim]")
                    continue

                try:
                    meta_xml = client.get_metadatos(disp.id_boe)
                    metadata = parse_metadatos(meta_xml, disp.id_boe)
                    text_xml = client.get_texto_consolidado(metadata.identificador)
                    blocks = parse_texto_xml(text_xml)

                    file_path = norma_to_filepath(metadata)
                    markdown = render_norma_at_date(metadata, blocks, current_date)

                    if repo.has_commit_with_source_id(disp.id_boe):
                        continue

                    changed = repo.write_and_add(file_path, markdown)
                    if not changed:
                        continue

                    if disp.es_correccion:
                        commit_type = CommitType.CORRECCION
                    elif disp.es_nueva:
                        commit_type = CommitType.NUEVA
                    else:
                        commit_type = CommitType.REFORMA

                    reform = Reform(fecha=current_date, id_norma=disp.id_boe, bloques_afectados=())
                    info = build_commit_info(
                        commit_type, metadata, reform, blocks, file_path, markdown
                    )
                    sha = repo.commit(info)

                    if sha:
                        commits_created += 1
                        console.print(f"    [green]✓[/green] {info.subject}")

                    mappings.set(metadata.identificador, file_path)

                except Exception:
                    msg = f"Error processing {disp.id_boe}"
                    logger.error(msg, exc_info=True)
                    errors.append(msg)

            state.ultimo_sumario = current_date

    if not dry_run and config.git.push and commits_created > 0:
        try:
            repo.push(branch=config.git.branch)
        except Exception:
            logger.error("Error pushing", exc_info=True)
            errors.append("Error pushing")

    state.record_run(
        sumarios=[d.isoformat() for d in dates_to_process],
        commits=commits_created,
        errores=errors,
    )
    state.save()
    mappings.save()

    console.print(f"\n[bold green]✓ {commits_created} commits[/bold green]")
    if errors:
        console.print(f"[yellow]⚠ {len(errors)} errors[/yellow]")

    return commits_created


# ─────────────────────────────────────────────
# REPROCESS — re-download and regenerate a law
# ─────────────────────────────────────────────


def reprocess(
    config: Config,
    boe_ids: list[str],
    reason: str,
    dry_run: bool = False,
) -> int:
    """Re-download and regenerate specific norms as [fix-pipeline]."""
    console.print(f"[bold]Reprocess — {reason}[/bold]\n")
    commits = 0
    for boe_id in boe_ids:
        fetch_one(config, boe_id, force=True)
        # TODO: commit as fix-pipeline instead of bootstrap/reforma
        commits += commit_one(config, boe_id, dry_run=dry_run)
    return commits


