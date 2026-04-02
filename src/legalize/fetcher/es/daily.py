"""Spain-specific daily processing.

Processes BOE daily summaries (sumarios) and generates commits for new legislation.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date

import requests

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.models import CommitType, Reform
from legalize.state.store import StateStore
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath
from legalize.transformer.xml_parser import parse_text_xml

console = Console()
logger = logging.getLogger(__name__)


def _infer_last_date_from_git(repo_path: str) -> date | None:
    """Infer the last processed date from the most recent Source-Date trailer in git log.

    Scans the last 20 commits for a Source-Date trailer (pipeline commits have these).
    Falls back to the author date of the most recent commit.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-20", "--format=%B%x00"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            for body in result.stdout.split("\0"):
                for line in body.splitlines():
                    if line.startswith("Source-Date: "):
                        inferred = date.fromisoformat(line[len("Source-Date: ") :].strip())
                        logger.info("Inferred last date from git: %s", inferred)
                        return inferred
    except (OSError, ValueError):
        pass
    # Fallback: author date of most recent commit
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%aI"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            inferred = date.fromisoformat(result.stdout.strip()[:10])
            logger.info("Inferred last date from author date: %s", inferred)
            return inferred
    except (OSError, ValueError):
        pass
    return None


def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing: process BOE summary/summaries."""
    from datetime import timedelta

    from legalize.fetcher.cache import FileCache
    from legalize.fetcher.es.client import BOEClient
    from legalize.fetcher.es.config import BOEConfig, ScopeConfig
    from legalize.fetcher.es.metadata import parse_metadata
    from legalize.fetcher.es.sumario import parse_summary

    cc = config.get_country("es")
    source = cc.source
    boe_config = BOEConfig(
        base_url=source.get("base_url", BOEConfig.base_url),
        requests_per_second=source.get("requests_per_second", BOEConfig.requests_per_second),
        request_timeout=source.get("request_timeout", BOEConfig.request_timeout),
        max_retries=source.get("max_retries", BOEConfig.max_retries),
    )
    scope = ScopeConfig(
        rangos=source.get("rangos", []),
        normas_fijas=source.get("normas_fijas", []),
    )
    cache = FileCache(cc.cache_dir)
    state = StateStore(cc.state_path)
    state.load()

    if target_date:
        dates_to_process = [target_date]
    else:
        start = state.last_summary_date
        if start is None:
            # Infer from the most recent commit's Source-Date trailer
            start = _infer_last_date_from_git(cc.repo_path)
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

    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    commits_created = 0
    errors: list[str] = []

    with BOEClient(boe_config, cache) as client:
        for current_date in dates_to_process:
            console.print(f"\n  [bold]{current_date}[/bold]")

            try:
                xml_data = client.get_sumario(current_date)
                dispositions = parse_summary(xml_data, scope)
            except requests.RequestException:
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
                    meta_xml = client.get_metadata(disp.id_boe)
                    metadata = parse_metadata(meta_xml, disp.id_boe)
                    text_xml = client.get_consolidated_text(metadata.identificador)
                    blocks = parse_text_xml(text_xml)

                    file_path = norm_to_filepath(metadata)
                    markdown = render_norm_at_date(metadata, blocks, current_date)

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

                except (requests.RequestException, ValueError, OSError):
                    msg = f"Error processing {disp.id_boe}"
                    logger.error(msg, exc_info=True)
                    errors.append(msg)

            state.last_summary_date = current_date

    if not dry_run and config.git.push and commits_created > 0:
        try:
            repo.push()
        except subprocess.CalledProcessError:
            logger.error("Error pushing", exc_info=True)
            errors.append("Error pushing")

    state.record_run(
        summaries=[d.isoformat() for d in dates_to_process],
        commits=commits_created,
        errors=errors,
    )
    state.save()

    console.print(f"\n[bold green]✓ {commits_created} commits[/bold green]")
    if errors:
        console.print(f"[yellow]⚠ {len(errors)} errors[/yellow]")

    return commits_created
