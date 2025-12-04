from pathlib import Path
from common.models import HeaderCluster, HeaderVariant
from core.mapping.offset_detection import detect_offsets

def test_detect_offsets_simple():
    # Two files, city column at index 0 and index 1
    clusters = [
        HeaderCluster(
            canonical_name="city",
            variants=[
                HeaderVariant(file_path=Path("a.csv"), column_index=0, raw_name="city", normalized_name="city", detected_types={}, sample_values={"Kyiv"}, row_count=1),
                HeaderVariant(file_path=Path("b.csv"), column_index=1, raw_name="city", normalized_name="city", detected_types={}, sample_values={"Lviv"}, row_count=1),
            ],
        )
    ]
    mapping_entries = detect_offsets(clusters)
    # Should detect target_index=0 (most common), offset=0 for a.csv, offset=1 for b.csv
    entries = {str(e.file_path): e for e in mapping_entries}
    assert entries["a.csv"].offset_from_index is None
    assert entries["b.csv"].offset_from_index == 1
    assert entries["b.csv"].offset_reason == "auto-detected"
    assert entries["b.csv"].offset_confidence == 1.0
