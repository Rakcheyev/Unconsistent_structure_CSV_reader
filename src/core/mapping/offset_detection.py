from typing import List, Dict, Optional
from pathlib import Path
from common.models import HeaderCluster, SchemaMappingEntry

def detect_offsets(header_clusters: List[HeaderCluster]) -> List[SchemaMappingEntry]:
    # Build mapping: canonical_name -> list of (file_path, column_index)
    col_map: Dict[str, List[tuple[Path, int]]] = {}
    for cluster in header_clusters:
        for variant in cluster.variants:
            col_map.setdefault(cluster.canonical_name, []).append((variant.file_path, variant.column_index))
    # For each canonical_name, check if column_index is stable or offset
    mapping_entries: List[SchemaMappingEntry] = []
    for canonical, positions in col_map.items():
        # Find most common index for this canonical_name
        index_counts: Dict[int, int] = {}
        for _, idx in positions:
            index_counts[idx] = index_counts.get(idx, 0) + 1
        target_index = max(index_counts, key=index_counts.get)
        for file_path, source_index in positions:
            offset = source_index - target_index
            entry = SchemaMappingEntry(
                file_path=file_path,
                source_index=source_index,
                canonical_name=canonical,
                target_index=target_index,
                offset_from_index=offset if offset != 0 else None,
                offset_reason="auto-detected" if offset != 0 else None,
                offset_confidence=1.0 if offset != 0 else None,
            )
            mapping_entries.append(entry)
    return mapping_entries
