"""Canada norm discovery -- enumerates laws from local XML clone or git.

Primary mode: scan a local clone of justicecanada/laws-lois-xml to yield
all act and regulation IDs. Daily mode: use git log to find files changed
since a given date.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Iterator
from datetime import date

from legalize.fetcher.base import LegislativeClient, NormDiscovery
from legalize.fetcher.ca.client import JusticeCanadaClient

logger = logging.getLogger(__name__)

# Directories to scan in the laws-lois-xml clone.
_SCAN_DIRS: list[tuple[str, str]] = [
    ("eng/acts", "eng/acts"),
    ("eng/regulations", "eng/regulations"),
    ("fra/lois", "fra/lois"),
    ("fra/reglements", "fra/reglements"),
]


class CADiscovery(NormDiscovery):
    """Discovers Canadian federal acts and regulations."""

    def discover_all(self, client: LegislativeClient, **kwargs) -> Iterator[str]:
        """Yield norm_ids for every act and regulation.

        Scans the local XML directory (from JusticeCanadaClient.xml_dir).
        Yields identifiers like "eng/acts/A-1", "fra/reglements/SOR-99-129".
        """
        assert isinstance(client, JusticeCanadaClient)
        xml_dir = client._xml_dir

        if xml_dir is None or not xml_dir.exists():
            logger.error(
                "No xml_dir configured or directory does not exist. "
                "Set source.xml_dir in config.yaml to a local clone of "
                "justicecanada/laws-lois-xml."
            )
            return

        for subdir, norm_prefix in _SCAN_DIRS:
            scan_path = xml_dir / subdir
            if not scan_path.exists():
                logger.debug("Skipping %s (not found)", scan_path)
                continue

            xml_files = sorted(scan_path.glob("*.xml"))
            logger.info("Found %d files in %s", len(xml_files), subdir)

            for xml_file in xml_files:
                # norm_id = "eng/acts/A-1" (without .xml extension)
                yield f"{norm_prefix}/{xml_file.stem}"

    def discover_daily(
        self, client: LegislativeClient, target_date: date, **kwargs
    ) -> Iterator[str]:
        """Yield norm_ids for files changed on or after target_date.

        Uses git log on the local clone to find changed XML files.
        Falls back to discover_all if git is not available.
        """
        assert isinstance(client, JusticeCanadaClient)
        xml_dir = client._xml_dir

        if xml_dir is None or not xml_dir.exists():
            logger.warning("No xml_dir for daily discovery; falling back to discover_all")
            yield from self.discover_all(client, **kwargs)
            return

        # Use git diff to find changed files since target_date.
        date_str = target_date.isoformat()
        try:
            result = subprocess.run(
                [
                    "git",
                    "-C",
                    str(xml_dir),
                    "log",
                    f"--since={date_str}",
                    "--name-only",
                    "--pretty=format:",
                    "--diff-filter=ACMR",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                logger.warning("git log failed: %s", result.stderr)
                return

            seen: set[str] = set()
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if not line or not line.endswith(".xml"):
                    continue
                # Convert file path to norm_id: "eng/acts/A-1.xml" → "eng/acts/A-1"
                norm_id = line.removesuffix(".xml")
                # Only yield files in the scan directories.
                if any(norm_id.startswith(prefix) for _, prefix in _SCAN_DIRS):
                    if norm_id not in seen:
                        seen.add(norm_id)
                        yield norm_id

            logger.info("Daily discovery found %d changed norms since %s", len(seen), date_str)

        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            logger.warning("git not available for daily discovery: %s", exc)
