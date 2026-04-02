"""Chile-specific daily processing.

Queries the BCN Ley Chile API for norms published on a target date
and generates commits for new/changed legislation.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date, timedelta

from rich.console import Console

from legalize.committer.git_ops import GitRepo
from legalize.committer.message import build_commit_info
from legalize.config import Config
from legalize.models import CommitType, Reform
from legalize.state.store import StateStore
from legalize.transformer.markdown import render_norm_at_date
from legalize.transformer.slug import norm_to_filepath

console = Console()
logger = logging.getLogger(__name__)


def _infer_last_date_from_git(repo_path: str) -> date | None:
    """Infer the last processed date from git log Source-Date trailers."""
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
                        return date.fromisoformat(line[len("Source-Date: ") :].strip())
    except (OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%aI"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return date.fromisoformat(result.stdout.strip()[:10])
    except (OSError, ValueError):
        pass
    return None


def daily(
    config: Config,
    target_date: date | None = None,
    dry_run: bool = False,
) -> int:
    """Daily processing for Chile: query BCN API for published norms."""
    from legalize.fetcher.cl.client import BCNClient
    from legalize.fetcher.cl.discovery import BCNDiscovery
    from legalize.fetcher.cl.parser import CLMetadataParser, CLTextParser

    cc = config.get_country("cl")
    state = StateStore(cc.state_path)
    state.load()

    if target_date:
        dates_to_process = [target_date]
    else:
        start = state.last_summary_date
        if start is None:
            start = _infer_last_date_from_git(cc.repo_path)
        if start is None:
            console.print("[yellow]No last date found. Use --date or run bootstrap.[/yellow]")
            return 0
        start = start + timedelta(days=1)
        end = date.today()
        dates_to_process = []
        current = start
        while current <= end:
            # Diario Oficial publishes Mon-Sat
            if current.weekday() < 6:
                dates_to_process.append(current)
            current += timedelta(days=1)

    if not dates_to_process:
        console.print("[green]Nothing to process — up to date[/green]")
        return 0

    console.print(f"[bold]Daily CL — processing {len(dates_to_process)} day(s)[/bold]")

    repo = GitRepo(cc.repo_path, config.git.committer_name, config.git.committer_email)
    commits_created = 0
    errors: list[str] = []

    text_parser = CLTextParser()
    meta_parser = CLMetadataParser()
    discovery = BCNDiscovery.create(cc.source or {})

    with BCNClient.create(cc) as client:
        for current_date in dates_to_process:
            console.print(f"\n  [bold]{current_date}[/bold]")

            try:
                published_ids = list(discovery.discover_daily(client, current_date))
            except Exception:
                msg = f"Error discovering publications for {current_date}"
                logger.error(msg, exc_info=True)
                errors.append(msg)
                continue

            if not published_ids:
                console.print("    No publications found")
                state.last_summary_date = current_date
                continue

            console.print(f"    {len(published_ids)} norm(s) published")

            for norm_id in published_ids:
                if dry_run:
                    console.print(f"    [dim]CL-{norm_id} — would process[/dim]")
                    continue

                try:
                    xml_data = client.get_text(norm_id)
                    metadata = meta_parser.parse(xml_data, norm_id)
                    blocks = text_parser.parse_text(xml_data)

                    file_path = norm_to_filepath(metadata)
                    markdown = render_norm_at_date(metadata, blocks, current_date)

                    changed = repo.write_and_add(file_path, markdown)
                    if not changed:
                        console.print(f"    [dim]⏭ {metadata.short_title[:50]} — no changes[/dim]")
                        continue

                    reform = Reform(
                        date=current_date,
                        norm_id=f"BCN-DAILY-{current_date.isoformat()}",
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

                except Exception as e:
                    msg = f"Error processing CL-{norm_id}: {e}"
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
