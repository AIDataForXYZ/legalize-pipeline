"""Abstract base for legislative API clients and norm discovery."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import date
from typing import TYPE_CHECKING, Any

from legalize.models import NormMetadata

if TYPE_CHECKING:
    from legalize.config import CountryConfig


class LegislativeClient(ABC):
    """Base class for country-specific legislative API clients.

    Each country implements its own client with endpoints for:
    - Fetching consolidated text (XML/HTML)
    - Fetching metadata
    - Rate limiting and caching
    """

    @classmethod
    def create(cls, country_config: CountryConfig) -> LegislativeClient:
        """Create a client instance from country config.

        Override in subclass to read source-specific params.
        Default: no-args constructor.
        """
        return cls()

    @abstractmethod
    def get_text(self, norm_id: str) -> bytes:
        """Fetch the consolidated text of a norm (XML or HTML)."""

    @abstractmethod
    def get_metadata(self, norm_id: str) -> bytes:
        """Fetch metadata for a norm."""

    @abstractmethod
    def close(self) -> None:
        """Clean up resources."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class NormDiscovery(ABC):
    """Base class for discovering norms in a country's catalog.

    Each country publishes legislation differently:
    - Spain: daily BOE sumario XML
    - France: LEGI XML dumps with versioning
    - UK: Atom publication feed
    - Germany: static XML with HTTP header change detection
    """

    @classmethod
    def create(cls, source: dict) -> NormDiscovery:
        """Create a discovery instance from source config.

        Override in subclass to read source-specific params.
        Default: no-args constructor.
        """
        return cls()

    @abstractmethod
    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Discover all norm IDs in the catalog."""

    @abstractmethod
    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Discover norms published/updated on a specific date."""


class TextParser(ABC):
    """Base class for parsing consolidated text into structured blocks.

    Each country's XML/HTML format is different, but the output
    is always a list of Block objects with version history.
    """

    @abstractmethod
    def parse_text(self, data: bytes) -> list[Any]:
        """Parse consolidated text into a list of Block objects."""

    @abstractmethod
    def extract_reforms(self, data: bytes) -> list[Any]:
        """Extract reform timeline from consolidated text."""


class MetadataParser(ABC):
    """Base class for parsing norm metadata.

    Each country has different metadata fields, rank hierarchies,
    and status flags, but the output is always NormMetadata.
    """

    @abstractmethod
    def parse(self, data: bytes, norm_id: str) -> NormMetadata:
        """Parse raw metadata into NormMetadata."""
