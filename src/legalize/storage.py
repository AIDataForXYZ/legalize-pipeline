"""Local storage for raw and structured data.

Saves intermediate data for the pipeline:
- data/xml/{id}.xml     — Raw source XML
- data/json/{id}.json   — Structured data for downstream consumers

The JSON contains all information needed to generate commits
without re-downloading or re-parsing anything.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from legalize.models import (
    Bloque,
    EstadoNorma,
    NormaCompleta,
    NormaMetadata,
    Paragraph,
    Rango,
    Reform,
    Version,
)

logger = logging.getLogger(__name__)


def save_raw_xml(data_dir: str | Path, norm_id: str, xml_bytes: bytes) -> Path:
    """Save the raw BOE XML."""
    path = Path(data_dir) / "xml" / f"{norm_id}.xml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(xml_bytes)
    logger.debug("XML saved: %s", path)
    return path


def save_structured_json(data_dir: str | Path, norm: NormaCompleta) -> Path:
    """Save structured data as DB-ready JSON.

    JSON structure:
    {
        "metadata": { titulo, identificador, pais, rango, ... },
        "articles": [
            {
                "block_id": "a135",
                "block_type": "precepto",
                "title": "Artículo 135",
                "position": 42,
                "current_text": "...",
                "versions": [
                    {
                        "date": "1978-12-29",
                        "source_id": "BOE-A-1978-31229",
                        "text": "..."
                    },
                    ...
                ]
            }
        ],
        "reforms": [
            {
                "date": "1992-08-28",
                "source_id": "BOE-A-1992-20403",
                "articles_affected": ["Artículo 13"]
            },
            ...
        ]
    }
    """
    data = _norma_to_dict(norm)
    path = Path(data_dir) / "json" / f"{norm.metadata.identificador}.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.debug("JSON saved: %s", path)
    return path


def _norma_to_dict(norm: NormaCompleta) -> dict:
    """Convert a NormaCompleta to a serializable dict."""
    meta = norm.metadata

    # Metadata
    metadata_dict = {
        "titulo": meta.titulo.rstrip(". "),
        "titulo_corto": meta.titulo_corto,
        "identificador": meta.identificador,
        "pais": meta.pais,
        "rango": str(meta.rango),
        "fecha_publicacion": meta.fecha_publicacion.isoformat(),
        "ultima_actualizacion": (
            meta.fecha_ultima_modificacion.isoformat()
            if meta.fecha_ultima_modificacion
            else meta.fecha_publicacion.isoformat()
        ),
        "estado": meta.estado.value,
        "departamento": meta.departamento,
        "fuente": meta.fuente,
    }

    if meta.jurisdiccion:
        metadata_dict["jurisdiccion"] = meta.jurisdiccion
    if meta.url_pdf:
        metadata_dict["url_pdf"] = meta.url_pdf
    if meta.materias:
        metadata_dict["materias"] = list(meta.materias)

    # Articles with all their versions
    articles = []
    for i, block in enumerate(norm.bloques):
        article = {
            "block_id": block.id,
            "block_type": block.tipo,
            "title": block.titulo,
            "position": i,
            "versions": [],
        }

        for version in block.versions:
            text = "\n\n".join(p.text for p in version.paragraphs)
            version_dict: dict = {
                "date": version.fecha_publicacion.isoformat(),
                "source_id": version.id_norma,
                "text": text,
            }
            # Preserve CSS classes for lossless round-trip
            css_classes = [p.css_class for p in version.paragraphs]
            if css_classes and any(c != "parrafo" for c in css_classes):
                version_dict["css_classes"] = css_classes
            article["versions"].append(version_dict)

        # current_text = latest version
        if block.versions:
            last = max(block.versions, key=lambda v: v.fecha_publicacion)
            article["current_text"] = "\n\n".join(p.text for p in last.paragraphs)
        else:
            article["current_text"] = ""

        articles.append(article)

    # Reforms
    block_map = {b.id: b for b in norm.bloques}
    reforms = []
    for reform in norm.reforms:
        affected = []
        for bid in reform.bloques_afectados:
            b = block_map.get(bid)
            if b and b.titulo:
                affected.append(b.titulo)

        reforms.append({
            "date": reform.fecha.isoformat(),
            "source_id": reform.id_norma,
            "articles_affected": affected,
        })

    return {
        "metadata": metadata_dict,
        "articles": articles,
        "reforms": reforms,
    }


def load_norma_from_json(json_path: Path) -> NormaCompleta:
    """Load a NormaCompleta from a structured JSON file.

    Inverse of save_structured_json(). Handles backward compatibility
    with older JSONs that don't have css_classes.
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    meta = data["metadata"]
    metadata = NormaMetadata(
        titulo=meta["titulo"],
        titulo_corto=meta["titulo_corto"],
        identificador=meta["identificador"],
        pais=meta["pais"],
        rango=Rango(meta["rango"]),
        fecha_publicacion=date.fromisoformat(meta["fecha_publicacion"]),
        estado=EstadoNorma(meta["estado"]),
        departamento=meta["departamento"],
        fuente=meta["fuente"],
        jurisdiccion=meta.get("jurisdiccion"),
        fecha_ultima_modificacion=date.fromisoformat(meta["ultima_actualizacion"]),
    )

    blocks = []
    for art in data["articles"]:
        versions = []
        for v in art["versions"]:
            paragraphs = []
            css_classes = v.get("css_classes")
            if v["text"].strip():
                lines = [line.strip() for line in v["text"].split("\n\n") if line.strip()]
                for i, line in enumerate(lines):
                    css = css_classes[i] if css_classes and i < len(css_classes) else "parrafo"
                    paragraphs.append(Paragraph(css_class=css, text=line))
            versions.append(Version(
                id_norma=v["source_id"],
                fecha_publicacion=date.fromisoformat(v["date"]),
                fecha_vigencia=date.fromisoformat(v["date"]),
                paragraphs=tuple(paragraphs),
            ))
        blocks.append(Bloque(
            id=art["block_id"],
            tipo=art["block_type"],
            titulo=art["title"],
            versions=tuple(versions),
        ))

    reforms = []
    for r in data["reforms"]:
        reforms.append(Reform(
            fecha=date.fromisoformat(r["date"]),
            id_norma=r["source_id"],
            bloques_afectados=tuple(
                art["block_id"]
                for art in data["articles"]
                for v in art["versions"]
                if v["source_id"] == r["source_id"]
                and v["date"] == r["date"]
            ),
        ))

    return NormaCompleta(
        metadata=metadata,
        bloques=tuple(blocks),
        reforms=tuple(reforms),
    )
