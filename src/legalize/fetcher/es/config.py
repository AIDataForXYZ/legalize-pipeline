"""Spain-specific configuration types.

BOEConfig and ScopeConfig define settings for the BOE API client
and the scope of norms to process. These are Spain-specific and
should not be in the generic pipeline Config.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class BOEConfig:
    """BOE API connection configuration."""

    base_url: str = "https://www.boe.es/datosabiertos"
    request_timeout: int = 30
    max_retries: int = 5
    retry_backoff_base: float = 2.0
    retry_backoff_multiplier: float = 2.0
    retry_jitter: float = 0.25
    requests_per_second: float = 2.0
    user_agent: str = "legalize-bot/1.0 (+https://github.com/legalize-dev/legalize)"


@dataclass
class ScopeConfig:
    """Pipeline scope: which norms to process."""

    rangos: list[str] = field(default_factory=list)  # Empty = all rangos accepted
    fecha_desde: Optional[date] = None
    fecha_hasta: Optional[date] = None
    normas_fijas: list[str] = field(default_factory=list)  # BOE IDs always included
