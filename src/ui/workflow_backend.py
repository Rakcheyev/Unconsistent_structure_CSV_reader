"""
Backend workflow function for batch processing, output format selection, memory cap, chunk size, and localization.
"""
from pathlib import Path
from typing import List
from common.config import load_runtime_config
from core.analysis import AnalysisEngine
from core.materialization import MaterializationJobRunner
from storage import save_mapping_config
from common.models import MappingConfig

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt"}

def collect_csv_files(input_folder: str) -> List[Path]:
    folder = Path(input_folder or "input_data/")
    return [p for p in folder.glob("**/*") if p.suffix.lower() in SUPPORTED_EXTENSIONS]

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

    # Save mapping config
    mapping = MappingConfig(blocks=all_blocks, schemas=[])
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
