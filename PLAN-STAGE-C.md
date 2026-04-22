# Stage C — Reconstrucción de versiones para normas no consolidadas

**Status**: planificación — aprobado cerrar Stage A primero (2026-04-22).

Este documento es el plan de trabajo para la siguiente fase del refactor ES
tras finalizar Stage A (parser + reproceso de las 12 245 consolidadas).

## Objetivo

Reconstruir la historia de reformas para normas **no consolidadas por BOE**
(Circulares, Instrucciones, Órdenes puntuales…), aprovechando que el XML del
diario BOE ya trae las modificaciones analizadas estructuradamente en
`<analisis><referencias><anteriores>`. Resultado esperado: commit
`[bootstrap]` + N commits `[reforma]` por norma, con el texto del día
correcto en cada commit.

## Hallazgo clave que habilita el Stage

El BOE incluye en cada norma modificadora un bloque estructurado:

```xml
<anterior referencia="BOE-A-2017-14334" orden="2015">
  <palabra codigo="270">MODIFICA</palabra>
  <texto>Norma 3, apartado 2; añade apartado 3</texto>
</anterior>
```

Esto nos da el `target_id`, la operación (códigos fijos: 270 MODIFICA, 407
AÑADE, 235 SUPRIME, 210 DEROGA) y el anchor textual. Lo que falta — el texto
nuevo literal — se extrae del cuerpo de la modificadora, donde aparece entre
comillas angulares `«…»` tras intros predecibles ("queda redactado como
sigue:", "con la siguiente redacción:").

Cobertura medida sobre muestra de 6 modificadoras reales (BOE-A-2021-21666,
BOE-A-2023-5481, BOE-A-2025-26847, y 3 más): **65-92 % de match automático
según la norma**. Media ponderada estimada: **~70 %**.

## Arquitectura

Pipeline híbrido determinista + LLM:

```
  Modificadora BOE-A-Y-N
         │
         ▼
  [1] parse <analisis>/<anteriores>
         │  (lista de patches estructurados brutos)
         ▼
  [2] extract cuerpo «…» con regex
         │  (rellena new_text para ~70 % de patches)
         ▼
  ┌──────┴──────┐
  │ confidence  │
  │   ≥ 0.9 ?   │
  └──┬──────┬───┘
     │ no   │ sí
     ▼      ▼
  [3] LLM   [5] apply patch
     │      │    ├─ hash-check sobre texto base
     ▼      │    ├─ si fallo → commit pointer fallback
  [4] merge │    └─ si ok → commit [reforma]
     └──────┤
            ▼
        output: per-file commit stream
```

### Módulos

| # | Módulo | Ruta propuesta | Tiempo |
|---|---|---|---|
| 1 | Parser de `<anteriores>` | `src/legalize/fetcher/es/amendments.py` | 2 días |
| 2 | Extractor de bloques `«…»` | mismo fichero | 3 días |
| 3 | Cliente LLM (Groq + fallback Ollama) | `src/legalize/llm/amendment_parser.py` | 3 días |
| 4 | Anchor resolver sobre Markdown base | `src/legalize/transformer/anchor.py` | 4 días |
| 5 | Motor de aplicación + hash-check + dry-run | `src/legalize/transformer/patcher.py` | 3 días |
| 6 | Fidelity loop Stage C | `scripts/es_fidelity_c/` | 5 días |
| 7 | Wiring al commit pipeline | `src/legalize/committer/` | 3 días |
| 8 | Tests + docs | `tests/test_stage_c_*.py` | 3 días |

**Total MVP: ~26 días-persona (~5 semanas calendar).**

## Estrategia LLM

### Por qué híbrido, no LLM-only

- **Determinismo**: el parser de regex + `<anteriores>` es reproducible bit-a-bit. Para commits legales queremos esto donde sea posible — evita que el mismo texto genere historias distintas entre ejecuciones.
- **Coste**: 70 % de casos resueltos sin llamar al LLM. Reducir llamadas = reducir latencia y gasto.
- **Auditabilidad**: cada patch tiene `source: "regex" | "llm"` registrado. Si un LLM alucina, queda trazado y auditable.

### Proveedor recomendado: Groq (Llama 3.3 70B Versatile)

- **Coste**: $0.59/M tokens input, $0.79/M output. Input típico por llamada: ~3k tokens (texto base + modificadora). Output: ~500 tokens (JSON). ≈ **$0.002/modificación**.
- **Latencia**: ~800 tokens/s. Una modificación ≈ 1 segundo.
- **Volumen estimado** Stage C sobre Circulares BdE MVP: ~500 circulares × ~8 modificaciones medias × ~30 % que caen al LLM = ~1 200 llamadas = **$2.40 total**.
- **Calidad**: Llama 3.3 70B maneja español técnico bien. Alternativas: `deepseek-r1-distill-llama-70b` (razonamiento, más caro), `llama-3.1-8b-instant` (10x más barato, OK para casos fáciles).

### Fallback local: Ollama con Qwen 2.5 32B o Llama 3.3 70B

- Gratis, sin dependencias externas, pero requiere GPU/Mac Apple Silicon con ≥ 32 GB RAM unificada.
- Velocidad: ~30 tokens/s en M2 Max. Latencia por modificación: ~20 s.
- Útil para:
  - CI (evitar llamadas de red en bootstrap workflows)
  - Desarrollo (iteración rápida sin gastar API)
  - Soberanía de datos (nothing leaves local)

### Interfaz del cliente LLM

```python
# src/legalize/llm/amendment_parser.py

from dataclasses import dataclass
from typing import Literal

@dataclass
class AmendmentPatch:
    target_id: str
    operation: Literal["replace", "insert", "delete"]
    anchor: dict  # {article, section, subsection, letter}
    new_text: list[str] | None
    source_boe_id: str
    source_date: str
    confidence: float
    source: Literal["regex", "llm"]

class AmendmentLLM:
    def __init__(self, backend: Literal["groq", "ollama"], model: str): ...
    
    def parse_difficult_case(
        self,
        base_markdown: str,      # texto base de la norma afectada
        modifier_text: str,      # cuerpo de la modificadora
        anchor_hint: str,        # el <texto> del <anterior>
        operation_hint: str,     # "MODIFICA" / "AÑADE" / etc.
    ) -> AmendmentPatch: ...
```

El prompt del LLM es **structured-output JSON** (Groq soporta tool-use): el
modelo tiene que devolver un dict con `operation`, `anchor`, `new_text`,
`confidence`. No se permite output libre. Si el modelo no puede, devuelve
`confidence=0` y el pipeline cae al commit-pointer fallback.

## Política de fallback

**Nunca inventar texto.** Si el patch no se puede aplicar con `confidence ≥
0.95`:

1. Dry-run hash-check falla (texto de anchor no existe en base) → **commit
   pointer** con trailer `Source-Id: BOE-A-Y-N`, sin modificar texto.
2. LLM también falla → **commit pointer**.
3. Anchor ambiguo (múltiples matches) → **commit pointer**.

El commit pointer es un commit `[reforma]` con body:

```
[reforma] Modificada por BOE-A-2021-21666 (texto no reconstruible
automáticamente). Ver referencias en frontmatter.

Source-Id: BOE-A-2021-21666
Source-Date: 2021-12-22
Norm-Id: BOE-A-2017-14334
```

Esto preserva la historia visible en `git log` sin corromper el contenido
del fichero. El usuario ve que existió una reforma y dónde está el texto
oficial; si quiere, va al BOE a leerla.

## Criterios de éxito del MVP

Sobre la muestra de **~500 Circulares BdE/CNMV** (subset de Stage C MVP):

- ≥ 90 % de operaciones MODIFICA + AÑADE + SUPRIME aplicadas con hash-check OK.
- ≥ 0.95 text_ratio medio contra BOE HTML cuando exista consolidación oficial de algún test case (Circulares del BdE de antes de 2010 a veces están en `/legislacion-consolidada`).
- 0 corrupciones silenciosas (todo fallo visible en logs + commit-pointer fallback).
- Tiempo de procesado aceptable: ≤ 2 segundos por patch de media (incluido LLM).

Métrica alcanzada con la muestra del subagente:
- Circular BdE 1/2025: 92 % match regex-only.
- Circular BdE 6/2021: 64 % regex-only (sube con LLM).

## Fases y dependencias

```
[FASE 0] Cerrar Stage A
  ├─ Reproceso 12 245 consolidadas
  ├─ Verify fidelity 50 leyes
  ├─ Push --force-with-lease a legalize-es
  └─ Web sync (next cron)

[FASE 1] Stage C MVP — Circulares BdE/CNMV  (5 semanas)
  ├─ Semana 1: amendments.py (módulos 1 + 2)
  ├─ Semana 2: amendment_parser.py (módulo 3) + anchor.py (módulo 4, parte 1)
  ├─ Semana 3: anchor.py (parte 2) + patcher.py (módulo 5)
  ├─ Semana 4: fidelity loop (módulo 6) + iteración
  └─ Semana 5: wiring (módulo 7) + tests (módulo 8) + push

[FASE 2] Decisión go/no-go
  └─ Si cobertura ≥ 90 % sobre Circulares → seguir
     Si < 90 % → iterar parser antes de extender

[FASE 3] Extensión — Órdenes, Instrucciones, Convenios  (3-4 semanas)
  ├─ Adapt patterns: cada rango tiene variantes de redacción
  ├─ Fidelity loop sobre 50 normas nuevas por rango
  └─ Si éxito → bootstrap masivo de ~10 k normas

[FASE 4] Stage B clásico para el resto  (después)
  └─ Las que NO son candidatas a Stage C (correcciones, indultos,
     comunicaciones, nombramientos) — single-commit + metadata.
```

## Riesgos

| # | Riesgo | Mitigación |
|---|---|---|
| 1 | Aplicar mal un patch → corromper texto de una Circular importante | Hash-check pre-aplicación. Si el fragmento que vamos a reemplazar no está exactamente en el texto base, abortar. |
| 2 | LLM alucina `new_text` | Solo aceptar si `confidence ≥ 0.95`. JSON structured output. Post-check: el `new_text` no puede contener referencias que no estén en la modificadora. |
| 3 | Circular modificada por norma que es Ley (no circular) — ¿procesamos Leyes como modificadoras? | Sí. El patrón es el mismo. Solo hay que extender la discovery: seguir el grafo `<anteriores>` hacia atrás desde cada Circular, descubriendo qué leyes la han modificado. |
| 4 | Orden de aplicación de patches importa (si A modifica el artículo 3 y luego B modifica el mismo artículo 3, hay que aplicar en orden) | Sort por `source_date` ascendente antes de aplicar. Si dos patches tienen la misma fecha, orden estable por BOE-ID. |
| 5 | Base text cambia tras nuestro parser refactor (Stage A) y los anchors textuales dejan de matchear | Anchors son por estructura lógica (artículo X, apartado N), no por posición textual. El refactor cambia markdown formatting, no la estructura semántica. |
| 6 | Coste LLM escala mal si extendemos a 87 k | Groq a $0.002/modificación × 87k × 5 mods/norma media = $870. Caro pero no prohibitivo. Con Ollama local = $0. Decisión se revisita en Fase 3. |

## Cosas que NO hace Stage C

- No reintenta "consolidar" normas que BOE ha decidido no consolidar. Lo que hacemos es **reconstruir nosotros** la historia; el output está en nuestro repo, no en BOE.
- No intenta resolver modificaciones indirectas ("se actualizarán las cuantías en función del IPC") que requieren contexto externo. Esas caen a commit-pointer.
- No sustituye el commit `[bootstrap]` por un único commit `[reforma]` final — cada versión histórica es un commit, preservando la cronología completa.
- No rehace las 12 245 consolidadas: Stage A ya lo hizo.

## Preguntas abiertas

1. ¿El repo legalize-es debe mezclar normas Stage A (consolidadas) y Stage C (reconstruidas) en el mismo directorio `es/`? → **Sí**, misma estructura filename = BOE-ID. El frontmatter lleva un flag `reconstruction: boe-consolidada | legalize-rebuilt | single-commit`.
2. ¿Quién ejecuta Stage C — local o CI? → Local para bootstrap (requiere un par de días de fetch + procesado). CI `daily-update` incremental: cuando llega una modificadora nueva, aplicar su patch sobre la norma afectada y hacer commit.
3. ¿Qué hacemos con modificadoras que afectan a MÚLTIPLES normas (una Ley que modifica 8 cosas diferentes)? → El `<analisis>/<anteriores>` lista los 8 targets por separado. El pipeline itera.

## Arranque

Cuando acabe Stage A, abrir una sesión nueva con este plan en la mano.
Primer commit sobre la rama `feat/stage-c-amendments`:

```python
# src/legalize/fetcher/es/amendments.py
"""Stage C — reconstruct reform history for non-consolidated BOE norms."""

# MVP scope: Circulares BdE + CNMV
# Reference: PLAN-STAGE-C.md
```

Y adelante con el módulo 1.
