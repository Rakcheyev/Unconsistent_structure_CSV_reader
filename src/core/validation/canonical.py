"""Helpers for loading and resolving canonical schema contracts."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Tuple

from common.models import CanonicalSchema, CanonicalSchemaRegistry, SchemaDefinition


def load_canonical_registry(path: Path | str | None) -> CanonicalSchemaRegistry:
    """Load canonical schema contracts from a JSON file.

    The format accepts either {"schemas": [...]} or a bare list of schema objects.
    Missing files return an empty registry so callers can treat the feature as optional.
    """

    registry = CanonicalSchemaRegistry()
    if path is None:
        return registry
    schema_path = Path(path)
    if not schema_path.exists():
        return registry
    with schema_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    for payload in _iter_schema_payloads(raw):
        registry.register(CanonicalSchema.from_dict(payload))
    return registry


def resolve_canonical_schema(
    schema: SchemaDefinition,
    registry: CanonicalSchemaRegistry | None,
) -> CanonicalSchema | None:
    """Find the canonical schema contract associated with a logical schema."""

    if registry is None:
        return None
    candidates: list[Tuple[str, str | None]] = []
    if schema.canonical_schema_id:
        candidates.append((schema.canonical_schema_id, schema.canonical_namespace))
    if schema.name:
        candidates.append((schema.name, schema.canonical_namespace))

    tried: set[Tuple[str, str | None]] = set()
    for schema_id, namespace in candidates:
        if not schema_id:
            continue
        key = (schema_id, namespace)
        if key in tried:
            continue
        tried.add(key)
        resolved = registry.get(schema_id, namespace)
        if resolved:
            return resolved
    # Fallback: search by schema_id across all namespaces when none provided
    if candidates:
        schema_ids = {schema_id for schema_id, _ in candidates if schema_id}
        for registered in registry.schemas.values():
            if registered.schema_id in schema_ids:
                return registered
    return None


def _iter_schema_payloads(raw: object) -> Iterable[dict]:
    if isinstance(raw, dict):
        items = raw.get("schemas")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    yield item
        return
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                yield item
