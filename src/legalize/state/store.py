"""State Store — pipeline state tracking.

Persists in state.json: last summary date and run history.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default safety cap for automatic lookback (no explicit --date)
MAX_LOOKBACK_DAYS = 10


def resolve_dates_to_process(
    state: "StateStore",
    repo_path: str,
    target_date: date | None = None,
    *,
    skip_weekdays: set[int] | None = None,
) -> list[date] | None:
    """Determine which dates need processing for a daily run.

    Centralizes the date resolution logic shared by all country dailies:
    1. If ``target_date`` is given, returns ``[target_date]``.
    2. Otherwise infers start from state or git, applies the safety cap,
       and generates the date range up to today.

    Args:
        state: Loaded StateStore for the country.
        repo_path: Path to the country git repo (for git-based inference).
        target_date: Explicit date from ``--date`` CLI flag, or None.
        skip_weekdays: Set of ``date.weekday()`` values to exclude.
            Common values: ``{6}`` (skip Sunday = Mon-Sat schedule),
            ``{5, 6}`` (skip Sat+Sun = Mon-Fri schedule).
            None means include all days.

    Returns:
        List of dates to process, or None if no start date could be
        determined (caller should print a warning and return 0).
    """
    if target_date:
        return [target_date]

    start = state.last_summary_date
    if start is None:
        start = infer_last_date_from_git(repo_path)
    if start is None:
        return None

    start = start + timedelta(days=1)
    end = date.today()

    # Safety cap: without an explicit --date, limit automatic lookback
    # to avoid processing months of history by accident
    # (e.g., first CI run after setup, or after a long outage).
    max_lookback = end - timedelta(days=MAX_LOOKBACK_DAYS)
    if start < max_lookback:
        logger.warning(
            "Clamping start from %s to %s (max %d days)",
            start,
            max_lookback,
            MAX_LOOKBACK_DAYS,
        )
        start = max_lookback

    skip = skip_weekdays or set()
    dates: list[date] = []
    current = start
    while current <= end:
        if current.weekday() not in skip:
            dates.append(current)
        current += timedelta(days=1)

    return dates


def infer_last_date_from_git(repo_path: str) -> date | None:
    """Infer the last processed date from the most recent Source-Date trailer.

    Uses ``git log --grep`` to find only pipeline commits (which carry a
    Source-Date trailer), ignoring manual commits like README updates.
    Works for any country — all pipeline commits use the same trailer.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--grep=Source-Date:", "--format=%B"],
            cwd=repo_path,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.splitlines():
                if line.startswith("Source-Date: "):
                    inferred = date.fromisoformat(line[len("Source-Date: ") :].strip())
                    logger.info("Inferred last date from git: %s", inferred)
                    return inferred
    except (OSError, ValueError):
        pass
    return None


@dataclass
class RunRecord:
    """Record of a pipeline run."""

    timestamp: str  # ISO datetime
    summaries_reviewed: list[str] = field(default_factory=list)
    commits_created: int = 0
    errors: list[str] = field(default_factory=list)


class StateStore:
    """Manages the pipeline's state.json file."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._last_summary: Optional[str] = None
        self._runs: list[RunRecord] = []

    def load(self) -> None:
        """Load state from disk."""
        if not self._path.exists():
            return

        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)

        self._last_summary = data.get("last_summary")

        for r in data.get("runs", []):
            self._runs.append(
                RunRecord(
                    timestamp=r["timestamp"],
                    summaries_reviewed=r.get("summaries_reviewed", []),
                    commits_created=r.get("commits_created", 0),
                    errors=r.get("errors", []),
                )
            )

    def save(self) -> None:
        """Persist state to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "last_summary": self._last_summary,
            "runs": [asdict(r) for r in self._runs],
        }

        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.debug("State saved to %s", self._path)

    @property
    def last_summary_date(self) -> Optional[date]:
        """Date of the last processed summary."""
        if self._last_summary:
            return date.fromisoformat(self._last_summary)
        return None

    @last_summary_date.setter
    def last_summary_date(self, value: date) -> None:
        self._last_summary = value.isoformat()

    def record_run(
        self,
        summaries: list[str] | None = None,
        commits: int = 0,
        errors: list[str] | None = None,
    ) -> None:
        """Record a pipeline run."""
        self._runs.append(
            RunRecord(
                timestamp=datetime.now().isoformat(),
                summaries_reviewed=summaries or [],
                commits_created=commits,
                errors=errors or [],
            )
        )
