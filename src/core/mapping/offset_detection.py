from typing import Dict, List, Optional, Sequence
from pathlib import Path
from collections import Counter

from common.models import ColumnProfileResult, HeaderCluster, SchemaMappingEntry


def detect_offsets(
    header_clusters: List[HeaderCluster],
    column_profiles: Sequence[ColumnProfileResult] | None = None,
) -> List[SchemaMappingEntry]:
    # Build mapping: canonical_name -> list of (file_path, column_index)
    col_map: Dict[str, List[tuple[Path, int]]] = {}
    cluster_profiles: Dict[str, Counter] = {}
    for cluster in header_clusters:
        profile_counter = cluster_profiles.setdefault(cluster.canonical_name, Counter())
        for variant in cluster.variants:
            col_map.setdefault(cluster.canonical_name, []).append((variant.file_path, variant.column_index))
            profile_counter.update(variant.detected_types)
    # For each canonical_name, check if column_index is stable or offset
    mapping_entries: List[SchemaMappingEntry] = []
    profile_lookup = _build_profile_lookup(column_profiles)
    for canonical, positions in col_map.items():
        # Find most common index for this canonical_name
        index_counts: Dict[int, int] = {}
        for _, idx in positions:
            index_counts[idx] = index_counts.get(idx, 0) + 1
        target_index = max(index_counts, key=index_counts.get)
        canonical_profile = cluster_profiles.get(canonical, Counter())
        for file_path, source_index in positions:
            offset = source_index - target_index
            profile = profile_lookup.get((file_path.as_posix(), source_index))
            confidence = _type_confidence(profile, canonical_profile)
            entry = SchemaMappingEntry(
                file_path=file_path,
                source_index=source_index,
                canonical_name=canonical,
                target_index=target_index,
                offset_from_index=offset if offset != 0 else None,
                offset_reason="auto-detected" if offset != 0 else None,
                offset_confidence=confidence if confidence is not None else (1.0 if offset != 0 else None),
            )
            mapping_entries.append(entry)
    return mapping_entries


def _build_profile_lookup(
    column_profiles: Sequence[ColumnProfileResult] | None,
) -> Dict[tuple[str, int], ColumnProfileResult]:
    lookup: Dict[tuple[str, int], ColumnProfileResult] = {}
    if not column_profiles:
        return lookup
    for profile in column_profiles:
        lookup[(profile.file_id, profile.column_index)] = profile
    return lookup


def _type_confidence(
    profile: ColumnProfileResult | None,
    canonical: Counter,
) -> Optional[float]:
    if profile is None or not canonical:
        return None
    canonical_total = sum(canonical.values())
    if canonical_total == 0:
        return None
    canonical_norm = _normalize_counts(dict(canonical))
    observed_norm = _normalize_counts(profile.type_distribution)
    keys = set(canonical_norm) | set(observed_norm)
    if not keys:
        return None
    distance = 0.0
    for key in keys:
        distance += abs(canonical_norm.get(key, 0.0) - observed_norm.get(key, 0.0))
    score = max(0.0, 1.0 - (distance / len(keys)))
    return round(score, 2)


def _normalize_counts(counts: Dict[str, int]) -> Dict[str, float]:
    mapping = {
        "null": "empty",
        "empty": "empty",
        "integer": "integer",
        "float": "float",
        "text": "text",
        "date": "date",
    }
    normalized: Dict[str, int] = {}
    for bucket, value in counts.items():
        key = mapping.get(bucket)
        if not key:
            key = bucket
        normalized[key] = normalized.get(key, 0) + int(value)
    total = float(sum(normalized.values()))
    if total <= 0:
        return {}
    return {key: value / total for key, value in normalized.items()}
