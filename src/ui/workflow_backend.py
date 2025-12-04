"""Backend workflow for batch processing and convenience helpers."""
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from common.config import load_runtime_config
from common.models import (
    FileAnalysisResult,
    HeaderCluster,
    HeaderVariant,
    MappingConfig,
)
from core.analysis import AnalysisEngine
from core.materialization import MaterializationJobRunner
from storage import save_mapping_config

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt"}

def collect_csv_files(input_folder: str) -> List[Path]:
    folder = Path(input_folder or "input_data/")
    return [p for p in folder.glob("**/*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]


def build_header_clusters(results: Sequence[FileAnalysisResult]) -> List[HeaderCluster]:
    """Build simple per-file header clusters from analysis signatures.

    This is intentionally lightweight; richer clustering happens in core.mapping.
    """
    header_groups: Dict[Tuple[Path, int, str], List[HeaderVariant]] = {}
    for result in results:
        for block in result.blocks:
            signature = block.signature
            if not signature.header_sample:
                continue
            delimiter = signature.delimiter or ","
            raw_headers = signature.header_sample.rstrip("\n\r").split(delimiter)
            row_count = max(0, block.end_line - block.start_line + 1)
            for index, stats in signature.columns.items():
                raw_name = raw_headers[index] if index < len(raw_headers) else ""
                key = (block.file_path, index, raw_name)
                detected_types: Dict[str, int] = {}
                if stats.maybe_numeric:
                    detected_types["numeric"] = stats.sample_count
                if stats.maybe_bool:
                    detected_types["bool"] = stats.sample_count
                if stats.maybe_date:
                    detected_types["date"] = stats.sample_count
                variant = HeaderVariant(
                    file_path=block.file_path,
                    column_index=index,
                    raw_name=raw_name,
                    normalized_name=raw_name,
                    detected_types=detected_types,
                    sample_values=set(stats.sample_values),
                    row_count=row_count,
                )
                header_groups.setdefault(key, []).append(variant)

    header_clusters: List[HeaderCluster] = []
    for (_file_path, idx, raw_name), variants in header_groups.items():
        canonical = raw_name or f"column_{idx}"
        header_clusters.append(
            HeaderCluster(
                canonical_name=canonical,
                variants=variants,
            )
        )
    return header_clusters

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
    for result in results:
        all_blocks.extend(result.blocks)

    # Build and save mapping config with header clusters
    header_clusters = build_header_clusters(results)
    mapping = MappingConfig(blocks=all_blocks, schemas=[], header_clusters=header_clusters)
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
