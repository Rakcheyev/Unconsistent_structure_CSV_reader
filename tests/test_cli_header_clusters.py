import tempfile
from pathlib import Path
import json
from common.mapping_serialization import mapping_from_dict
from ui.cli import command_analyze

CSV_CONTENT = "city,age\nKyiv,30\nLviv,25\n"

def test_cli_analyze_header_clusters(tmp_path):
    # Prepare minimal CSV fixture
    csv_path = tmp_path / "input.csv"
    csv_path.write_text(CSV_CONTENT, encoding="utf-8")
    output_path = tmp_path / "mapping.json"
    class Args:
        inputs = [str(csv_path)]
        profile = "low_memory"
        output = str(output_path)
        include_samples = True
        progress_log = None
        sqlite_db = None
    # Run CLI analyze
    command_analyze(Args)
    # Load mapping.json
    mapping_dict = json.loads(output_path.read_text(encoding="utf-8"))
    mapping = mapping_from_dict(mapping_dict)
    # Check header_clusters
    clusters = mapping.header_clusters
    assert clusters, "No header_clusters found in MappingConfig"
    # There should be two clusters: city and age
    names = sorted([c.canonical_name for c in clusters])
    assert names == ["age", "city"], f"Unexpected canonical_names: {names}"
    # Each cluster should have at least one variant
    for cluster in clusters:
        assert cluster.variants, f"Cluster {cluster.canonical_name} has no variants"
        # Check that sample_values are present for city
        if cluster.canonical_name == "city":
            variant = cluster.variants[0]
            assert "Kyiv" in variant.sample_values and "Lviv" in variant.sample_values
