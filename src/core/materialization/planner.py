"""Lightweight materialization planner (Phase 2 placeholder)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from common.models import FileBlock, MappingConfig, SchemaDefinition


@dataclass(slots=True)
class PlanEntry:
    schema_id: str
    schema_name: str
    block_count: int
    estimated_rows: int
    output_path: str
    source_files: List[str]


class MaterializationPlanner:
    """Builds offline plans for per-schema dataset generation."""

    def __init__(self, chunk_rows: int) -> None:
        self.chunk_rows = chunk_rows

    def build_plan(self, mapping: MappingConfig, output_dir: Path) -> List[PlanEntry]:
        schemas_by_id: Dict[str, SchemaDefinition] = {
            str(schema.id): schema for schema in mapping.schemas
        }
        grouped: Dict[str, List[FileBlock]] = {}
        for block in mapping.blocks:
            if not block.schema_id:
                continue
            grouped.setdefault(str(block.schema_id), []).append(block)

        plan: List[PlanEntry] = []
        for schema_id, blocks in grouped.items():
            schema = schemas_by_id.get(schema_id)
            schema_name = schema.name if schema else schema_id
            output_path = sanitize_output_path(output_dir, schema_name or schema_id)
            estimated_rows = sum(block.end_line - block.start_line + 1 for block in blocks)
            plan.append(
                PlanEntry(
                    schema_id=schema_id,
                    schema_name=schema_name,
                    block_count=len(blocks),
                    estimated_rows=estimated_rows,
                    output_path=str(output_path),
                    source_files=sorted({str(block.file_path) for block in blocks}),
                )
            )
        plan.sort(key=lambda entry: entry.schema_name)
        return plan

    @staticmethod
    def write_plan(plan: Iterable[PlanEntry], path: Path) -> None:
        from dataclasses import asdict
        payload = [asdict(entry) for entry in plan]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def sanitize_output_path(base: Path, name: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in name.lower())
    if not safe:
        safe = "dataset"
    return base / f"{safe}.csv"
