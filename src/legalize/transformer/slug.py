"""File path generation for norms.

Structure: {pais}/{identificador}.md
The rango goes in the YAML frontmatter, not in the directory structure.

Example: es/BOE-A-1978-31229.md
         se/SFS-1962-700.md
"""

from __future__ import annotations

from legalize.models import NormaMetadata


def norma_to_filepath(metadata: NormaMetadata) -> str:
    """Generates the path for a norm file.

    State-level: '{pais}/{identificador}.md'
      Example: 'es/BOE-A-2015-11430.md'

    Autonomous community: '{jurisdiccion}/{identificador}.md'
      Example: 'es-pv/BOE-A-2020-615.md'
    """
    filename = f"{metadata.identificador}.md"
    if metadata.jurisdiccion:
        return f"{metadata.jurisdiccion}/{filename}"
    return f"{metadata.pais}/{filename}"
