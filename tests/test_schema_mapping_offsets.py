from __future__ import annotations

from pathlib import Path

from common.models import HeaderCluster, HeaderVariant
from core.mapping.offset_detection import detect_offsets


def _variant(file_path: Path, column_index: int, raw: str) -> HeaderVariant:
    return HeaderVariant(
        file_path=file_path,
        column_index=column_index,
        raw_name=raw,
        normalized_name=raw,
        detected_types={},
        sample_values=set(),
        row_count=3,
    )


def test_detect_offsets_handles_swapped_columns(tmp_path) -> None:
    file_a = tmp_path / "customers_a.csv"
    file_b = tmp_path / "customers_b.csv"
    clusters = [
        HeaderCluster(
            canonical_name="name",
            variants=[
                _variant(file_a, 0, "name"),
                _variant(file_b, 1, "name"),
            ],
        ),
        HeaderCluster(
            canonical_name="email",
            variants=[
                _variant(file_a, 1, "email"),
                _variant(file_b, 0, "email"),
            ],
        ),
    ]
    entries = detect_offsets(clusters)
    assert len(entries) == 4

    by_file = {(entry.file_path, entry.canonical_name): entry for entry in entries}
    assert by_file[(file_a, "name")].target_index == 0
    assert by_file[(file_a, "name")].offset_from_index is None
    assert by_file[(file_b, "name")].target_index == 0
    assert by_file[(file_b, "name")].offset_from_index == 1
    assert by_file[(file_b, "name")].offset_confidence == 1.0

    assert by_file[(file_b, "email")].target_index == 1
    assert by_file[(file_b, "email")].offset_from_index == -1