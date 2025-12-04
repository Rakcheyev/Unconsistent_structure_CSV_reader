from pathlib import Path

from common.models import HeaderCluster, HeaderVariant
from core.mapping.header_clustering import HeaderClusteringService


def test_header_clustering_groups_by_lowercased_name():
    service = HeaderClusteringService()
    clusters = [
        HeaderCluster(
            canonical_name="City",
            variants=[
                HeaderVariant(
                    file_path=Path("a.csv"),
                    column_index=0,
                    raw_name="City",
                    normalized_name="City",
                    detected_types={},
                    sample_values={"Kyiv"},
                    row_count=1,
                )
            ],
        ),
        HeaderCluster(
            canonical_name="city",
            variants=[
                HeaderVariant(
                    file_path=Path("b.csv"),
                    column_index=0,
                    raw_name="city",
                    normalized_name="city",
                    detected_types={},
                    sample_values={"Lviv"},
                    row_count=1,
                )
            ],
        ),
    ]
    merged = service.cluster(clusters)
    assert len(merged) == 1
    merged_cluster = merged[0]
    assert merged_cluster.canonical_name == "city"
    names = sorted(v.raw_name for v in merged_cluster.variants)
    assert names == ["City", "city"]
