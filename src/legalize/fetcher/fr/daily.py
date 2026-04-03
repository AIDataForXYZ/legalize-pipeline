"""France-specific daily processing.

Downloads LEGI daily increments from DILA and processes modified texts.
Increments are tar.gz files published Mon-Sat at:
  https://echanges.dila.gouv.fr/OPENDATA/LEGI/LEGI_YYYYMMDD-HHMMSS.tar.gz

Unlike Spain (BOE), France does NOT have a reform/404 problem:
LEGI increments contain the updated consolidated texts directly.
When a law is reformed, the increment delivers the modified LEGITEXT
XML with the new article versions already merged. There is no need
to resolve "affected norms" from a reforming disposition.
"""

from __future__ import annotations

import logging
import re
import tarfile
import tempfile
from datetime import date
from pathlib import Path

import requests
from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.models import CommitType, Reform
from legalize.pipeline import finalize_daily
from legalize.state.store import StateStore, resolve_dates_to_process
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath

console = Console()
logger = logging.getLogger(__name__)

DILA_LEGI_URL = "https://echanges.dila.gouv.fr/OPENDATA/LEGI/"
USER_AGENT = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)"

_INCREMENT_RE = re.compile(r'href="(LEGI_(\d{8}-\d{6})\.tar\.gz)"')


def _list_increments(session: requests.Session) -> list[tuple[str, str]]:
    """List available LEGI increment files from DILA directory listing.

    Returns sorted list of (filename, url).
    """
    resp = session.get(DILA_LEGI_URL, timeout=30)
    resp.raise_for_status()

    results = []
    for filename, _ts in sorted(_INCREMENT_RE.findall(resp.text)):
        results.append((filename, f"{DILA_LEGI_URL}{filename}"))
    return results


def _find_increment_for_date(
    increments: list[tuple[str, str]], target_date: date
) -> tuple[str, str] | None:
    """Find the increment file matching target_date (LEGI_YYYYMMDD-*.tar.gz)."""
    date_str = target_date.strftime("%Y%m%d")
    for filename, url in increments:
        if f"LEGI_{date_str}" in filename:
            return filename, url
    return None


def _download_increment(session: requests.Session, url: str, dest_path: Path) -> None:
    """Download a LEGI increment tar.gz file with streaming."""
    logger.info("Downloading %s ...", url)
    resp = session.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    size_mb = dest_path.stat().st_size / (1024 * 1024)
    logger.info("Downloaded %s (%.1f MB)", dest_path.name, size_mb)


def _extract_increment(tar_path: Path, legi_dir: Path, increment_dir: Path) -> None:
    """Extract increment tar.gz to both legi_dir and increment_dir.

    - legi_dir: merge into existing dump (overwrites modified files)
    - increment_dir: separate copy for discover_daily() to scan
    """
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=increment_dir, filter="data")
        tar.extractall(path=legi_dir, filter="data")

    logger.info("Extracted to %s and %s", increment_dir, legi_dir)


def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing for France: download LEGI increment and process changes."""
    from legalize.fetcher.fr.client import LEGIClient
    from legalize.fetcher.fr.discovery import LEGIDiscovery
    from legalize.fetcher.fr.parser import LEGIMetadataParser, LEGITextParser

    cc = config.get_country("fr")
    legi_dir = cc.source.get("legi_dir", "")
    if not legi_dir:
        console.print(
            "[red]legi_dir not configured for France. "
            "Set it in config.yaml or use --legi-dir.[/red]"
        )
        return 0

    legi_path = Path(legi_dir)
    legi_path.mkdir(parents=True, exist_ok=True)

    state = StateStore(cc.state_path)
    state.load()

    dates_to_process = resolve_dates_to_process(
        state,
        cc.repo_path,
        target_date,
        skip_weekdays={6},
    )
    if dates_to_process is None:
        console.print("[yellow]No last date found. Use --date or run bootstrap.[/yellow]")
        return 0
    if not dates_to_process:
        console.print("[green]Nothing to process — up to date[/green]")
        return 0

    console.print(f"[bold]Daily FR — processing {len(dates_to_process)} day(s)[/bold]")

    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    commits_created = 0
    errors: list[str] = []

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    try:
        increments = _list_increments(session)
    except requests.RequestException:
        logger.error("Could not list DILA increments", exc_info=True)
        console.print("[red]Error listing DILA increments[/red]")
        return 0

    text_parser = LEGITextParser()
    meta_parser = LEGIMetadataParser()

    for current_date in dates_to_process:
        console.print(f"\n  [bold]{current_date}[/bold]")

        match = _find_increment_for_date(increments, current_date)
        if match is None:
            console.print("    No increment available (holiday/no changes)")
            state.last_summary_date = current_date
            continue

        filename, url = match
        increment_name = filename.replace(".tar.gz", "").replace("LEGI_", "")
        increment_dir = legi_path / increment_name

        if dry_run:
            console.print(f"    [dim]Would download {filename}[/dim]")
            state.last_summary_date = current_date
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = Path(tmpdir) / filename
            try:
                _download_increment(session, url, tar_path)
                _extract_increment(tar_path, legi_path, increment_dir)
            except (requests.RequestException, tarfile.TarError, OSError) as e:
                msg = f"Error downloading/extracting {filename}: {e}"
                logger.error(msg, exc_info=True)
                errors.append(msg)
                continue

        discovery = LEGIDiscovery(legi_path)
        client = LEGIClient(legi_path)

        try:
            modified_ids = list(discovery.discover_daily(client, current_date))
        except Exception:
            msg = f"Error discovering changes for {current_date}"
            logger.error(msg, exc_info=True)
            errors.append(msg)
            continue

        if not modified_ids:
            console.print("    No texts modified in scope")
            state.last_summary_date = current_date
            continue

        console.print(f"    {len(modified_ids)} text(s) modified")

        for norm_id in modified_ids:
            try:
                meta_data = client.get_metadata(norm_id)
                metadata = meta_parser.parse(meta_data, norm_id)

                text_data = client.get_text(norm_id)
                blocks = text_parser.parse_text(text_data)

                file_path = norm_to_filepath(metadata)
                markdown = render_norm_at_date(metadata, blocks, current_date)

                # Safety: skip if generated markdown is suspiciously short
                existing_path = Path(cc.repo_path) / file_path
                if existing_path.exists():
                    existing_size = existing_path.stat().st_size
                    if len(markdown) < existing_size * 0.5:
                        logger.warning(
                            "Skipping %s: markdown too short (%d vs %d bytes)",
                            norm_id,
                            len(markdown),
                            existing_size,
                        )
                        continue

                changed = repo.write_and_add(file_path, markdown)
                if not changed:
                    console.print(f"    [dim]⏭ {metadata.short_title} — no changes[/dim]")
                    continue

                source_id = f"LEGI-DAILY-{current_date.isoformat()}-{norm_id}"
                reform = Reform(
                    date=current_date,
                    norm_id=source_id,
                    affected_blocks=(),
                )
                info = build_commit_info(
                    CommitType.REFORM,
                    metadata,
                    reform,
                    blocks,
                    file_path,
                    markdown,
                )
                sha = repo.commit(info)

                if sha:
                    commits_created += 1
                    console.print(f"    [green]✓[/green] {info.subject}")

            except (ValueError, FileNotFoundError, OSError) as e:
                msg = f"Error processing {norm_id}: {e}"
                logger.error(msg, exc_info=True)
                errors.append(msg)

        state.last_summary_date = current_date

    session.close()

    return finalize_daily(
        repo,
        state,
        dates_to_process,
        commits_created,
        errors,
        dry_run=dry_run,
        push=config.git.push,
    )
