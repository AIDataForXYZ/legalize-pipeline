"""Git commit authorship.

Author = whoever runs the pipeline (from git config).
Committer = the pipeline tool (from config.yaml).
"""

from __future__ import annotations

import subprocess


def resolve_author() -> tuple[str, str]:
    """Returns (name, email) for the commit author.

    Reads from git config (user.name / user.email), same as any
    open source project. Falls back to "Legalize" if not configured.
    """
    name = _git_config("user.name") or "Legalize"
    email = _git_config("user.email") or "legalize@legalize.dev"
    return name, email


def _git_config(key: str) -> str | None:
    """Read a value from git config."""
    try:
        result = subprocess.run(
            ["git", "config", "--get", key],
            capture_output=True,
            text=True,
        )
        value = result.stdout.strip()
        return value if value else None
    except OSError:
        return None
