"""Structured commit message construction.

Generic multi-country format:
    [type] Title — affected articles

    Norma: BOE-A-1978-31229
    Disposición: BOE-A-2024-3099
    Fecha: 2024-02-17
    URL: https://www.boe.es/...
    Artículos afectados: Artículo 49

    Source-Id: BOE-A-2024-3099
    Source-Date: 2024-02-17
    Norm-Id: BOE-A-1978-31229
"""

from __future__ import annotations

import re

from legalize.committer.author import resolve_author
from legalize.models import (
    Bloque,
    CommitInfo,
    CommitType,
    NormaMetadata,
    Reform,
)


def build_commit_info(
    commit_type: CommitType,
    norm_metadata: NormaMetadata,
    reform: Reform,
    blocks: list[Bloque] | tuple[Bloque, ...],
    file_path: str,
    content: str,
) -> CommitInfo:
    """Build a complete CommitInfo from domain data."""
    affected = _get_affected_articles(reform, blocks)
    affected_str = ", ".join(affected) if affected else "N/A"

    subject = _build_subject(commit_type, norm_metadata, reform, affected)
    body = _build_body(commit_type, norm_metadata, reform, affected_str)

    trailers = {
        "Source-Id": reform.id_norma,
        "Source-Date": reform.fecha.isoformat(),
        "Norm-Id": norm_metadata.identificador,
    }

    author_name, author_email = resolve_author()

    return CommitInfo(
        commit_type=commit_type,
        subject=subject,
        body=body,
        trailers=trailers,
        author_name=author_name,
        author_email=author_email,
        author_date=reform.fecha,
        file_path=file_path,
        content=content,
    )


def format_commit_message(info: CommitInfo) -> str:
    """Format CommitInfo as a complete git commit message."""
    parts = [info.subject, "", info.body]

    if info.trailers:
        parts.append("")
        for key, value in info.trailers.items():
            parts.append(f"{key}: {value}")

    return "\n".join(parts)


def _build_subject(
    commit_type: CommitType,
    metadata: NormaMetadata,
    reform: Reform,
    affected: list[str] | None = None,
) -> str:
    """Build the first line of the commit message.

    [reforma] Constitución Española — art. 49
    """
    prefix = f"[{commit_type.value}]"
    title = metadata.titulo_corto

    if commit_type == CommitType.BOOTSTRAP:
        return f"{prefix} {title} — versión original {reform.fecha.year}"

    if commit_type == CommitType.FIX_PIPELINE:
        return f"{prefix} Regenerar {title}"

    if affected:
        arts_brief = _abbreviate_articles(affected)
        if arts_brief:
            return f"{prefix} {title} — {arts_brief}"

    return f"{prefix} {title}"


def _build_body(
    commit_type: CommitType,
    metadata: NormaMetadata,
    reform: Reform,
    affected_str: str,
) -> str:
    """Build the commit message body."""
    date_str = reform.fecha.isoformat()

    if commit_type == CommitType.BOOTSTRAP:
        return (
            f"Publicación original de {metadata.titulo_corto}.\n"
            f"\n"
            f"Norma: {metadata.identificador}\n"
            f"Fecha: {date_str}\n"
            f"Fuente: {metadata.fuente}"
        )

    return (
        f"Norma: {metadata.identificador}\n"
        f"Disposición: {reform.id_norma}\n"
        f"Fecha: {date_str}\n"
        f"Fuente: {metadata.fuente}\n"
        f"\n"
        f"Artículos afectados: {affected_str}"
    )


def _abbreviate_articles(articles: list[str]) -> str:
    """Abbreviate list of articles for the commit subject.

    ['Artículo 49'] → 'art. 49'
    ['Artículo 13', 'Artículo 14'] → 'arts. 13, 14'
    """
    nums = []
    for art in articles:
        match = re.search(r"(\d+)", art)
        if match:
            nums.append(match.group(1))

    if not nums:
        return ""

    if len(nums) == 1:
        return f"art. {nums[0]}"

    if len(nums) <= 4:
        return f"arts. {', '.join(nums)}"

    shown = ", ".join(nums[:3])
    return f"arts. {shown} y {len(nums) - 3} más"


def _get_affected_articles(
    reform: Reform, blocks: list[Bloque] | tuple[Bloque, ...]
) -> list[str]:
    """Get titles of articles affected by a reform."""
    titles = []
    block_map = {b.id: b for b in blocks}
    for bid in reform.bloques_afectados:
        block = block_map.get(bid)
        if block and block.titulo:
            titles.append(block.titulo)
    return titles
