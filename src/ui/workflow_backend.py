"""Backend workflow for batch processing and convenience helpers."""
from pathlib import Path
from typing import List, Sequence

from common.config import load_runtime_config
from common.models import FileAnalysisResult, HeaderCluster, MappingConfig
from core.analysis import AnalysisEngine
from core.headers.cluster_builder import HeaderClusterizer
from core.headers.metadata import build_header_metadata
from core.mapping.offset_detection import detect_offsets
from core.materialization import MaterializationJobRunner
from storage import save_mapping_config

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt"}

def collect_csv_files(input_folder: str) -> List[Path]:
    folder = Path(input_folder or "input_data/")
    return [p for p in folder.glob("**/*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]


def build_header_clusters(results: Sequence[FileAnalysisResult]) -> List[HeaderCluster]:
    """Convenience wrapper that runs the core HeaderClusterizer."""

    metadata = build_header_metadata(results)
    clusterizer = HeaderClusterizer()
    return clusterizer.build(results, metadata=metadata)

def run_batch_workflow(input_folder: str, output_folder: str, output_format: str, memory_cap: int, chunk_size: int):
    files = collect_csv_files(input_folder or "input_data/")
    if not files:
        print("No CSV files found in input folder.")
        return
    print(f"Found {len(files)} files for processing.")

    # Load runtime config with memory cap and chunk size, using an existing profile
    runtime = load_runtime_config(profile="low_memory", overrides={
        "profile": {
            "block_size": chunk_size,
            "max_parallel_files": 2,
            "sample_values_cap": 24,
            "writer_chunk_rows": chunk_size,
        },
        "global_settings": {
            "memory_cap": memory_cap * 1024 * 1024,  # Convert MB to bytes
        }
    })

    # Analyze files
    engine = AnalysisEngine(runtime)
    results = engine.analyze_files(files)
    all_blocks = []
    all_column_profiles = []
    for result in results:
        all_blocks.extend(result.blocks)
        all_column_profiles.extend(result.column_profiles)

    # Build and save mapping config with header clusters
    header_clusters = build_header_clusters(results)
    schema_mapping = detect_offsets(header_clusters, column_profiles=all_column_profiles)
    mapping = MappingConfig(
        blocks=all_blocks,
        schemas=[],
        header_clusters=header_clusters,
        schema_mapping=schema_mapping,
        column_profiles=all_column_profiles,
    )
    mapping_path = Path(output_folder or "output_data/") / "mapping.json"
    save_mapping_config(mapping, mapping_path)

    # Materialize output
    job_runner = MaterializationJobRunner(
        runtime,
        writer_format=output_format.lower(),
        spill_threshold=chunk_size,
    )
    job_runner.run(mapping, Path(output_folder or "output_data/"))
    print(f"Materialization complete. Output saved to {output_folder}")
