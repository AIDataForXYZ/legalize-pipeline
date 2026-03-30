"""State Store — pipeline state tracking.

Persists in state.json which norms have been processed,
enabling idempotent re-runs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class NormState:
    """Processing state of an individual norm."""

    last_version_applied: str  # ISO date
    total_versions_applied: int


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
        self._norms: dict[str, NormState] = {}
        self._runs: list[RunRecord] = []

    def load(self) -> None:
        """Load state from disk. Handles both old and new key names."""
        if not self._path.exists():
            return

        with open(self._path, encoding="utf-8") as f:
            data = json.load(f)

        # Support both old (Spanish) and new (English) key names
        self._last_summary = data.get("last_summary") or data.get("ultimo_sumario_procesado")

        norms_raw = data.get("norms_processed") or data.get("normas_procesadas", {})
        for k, v in norms_raw.items():
            self._norms[k] = NormState(
                last_version_applied=v.get("last_version_applied") or v.get("ultima_version_aplicada", ""),
                total_versions_applied=v.get("total_versions_applied") or v.get("total_versiones_aplicadas", 0),
            )

        runs_raw = data.get("runs") or data.get("ejecuciones", [])
        for r in runs_raw:
            self._runs.append(RunRecord(
                timestamp=r.get("timestamp") or r.get("fecha", ""),
                summaries_reviewed=r.get("summaries_reviewed") or r.get("sumarios_revisados", []),
                commits_created=r.get("commits_created") or r.get("commits_generados", 0),
                errors=r.get("errors") or r.get("errores", []),
            ))

    def save(self) -> None:
        """Persist state to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "last_summary": self._last_summary,
            "norms_processed": {k: asdict(v) for k, v in self._norms.items()},
            "runs": [asdict(r) for r in self._runs],
        }

        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.debug("State saved to %s", self._path)

    @property
    def ultimo_sumario(self) -> Optional[date]:
        """Date of the last processed summary. Legacy property name kept for compat."""
        if self._last_summary:
            return date.fromisoformat(self._last_summary)
        return None

    @ultimo_sumario.setter
    def ultimo_sumario(self, value: date) -> None:
        self._last_summary = value.isoformat()

    def is_norma_processed(self, norm_id: str, target_date: date) -> bool:
        """Check whether a specific version of a norm has been processed."""
        state = self._norms.get(norm_id)
        if state is None:
            return False
        return state.last_version_applied >= target_date.isoformat()

    def mark_norma_processed(self, norm_id: str, target_date: date, total_versions: int) -> None:
        """Mark a norm as processed up to a given date."""
        self._norms[norm_id] = NormState(
            last_version_applied=target_date.isoformat(),
            total_versions_applied=total_versions,
        )

    def record_run(
        self,
        sumarios: list[str] | None = None,
        commits: int = 0,
        errores: list[str] | None = None,
    ) -> None:
        """Record a pipeline run. Parameter names kept for caller compat."""
        self._runs.append(RunRecord(
            timestamp=datetime.now().isoformat(),
            summaries_reviewed=sumarios or [],
            commits_created=commits,
            errors=errores or [],
        ))

    def get_norma_state(self, norm_id: str) -> Optional[NormState]:
        return self._norms.get(norm_id)

    @property
    def normas_count(self) -> int:
        return len(self._norms)
