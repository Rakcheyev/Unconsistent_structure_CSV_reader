from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from common.models import FileBlock, MappingConfig, ProfileSettings, RuntimeConfig, GlobalSettings
from core.analysis import AnalysisEngine
from core.mapping import MappingService
from core.normalization import NormalizationService, SynonymDictionary
from core.materialization import MaterializationJobRunner
from core.materialization.runner import SpillBuffer
from storage import fetch_job_progress_events, record_job_metrics, record_progress_event


def build_runtime(chunk_rows: int = 2) -> RuntimeConfig:
    return RuntimeConfig(
        global_settings=GlobalSettings(
            encoding="utf-8",
            error_policy="replace",
            synonym_dictionary="storage/synonyms.json",
        ),
        profile=ProfileSettings(
            description="test",
            block_size=16,
            min_gap_lines=1,
            max_parallel_files=1,
            sample_values_cap=8,
            writer_chunk_rows=chunk_rows,
        ),
    )


def materialize_mapping(runtime: RuntimeConfig, csv_path: Path, tmp_path: Path) -> MappingConfig:
    engine = AnalysisEngine(runtime)
    results = engine.analyze_files([csv_path])
    blocks: list[FileBlock] = []
    for result in results:
        blocks.extend(result.blocks)
    # use synonyms to keep deterministic normalized names
    synonyms_data = {"customer_name": ["name"]}
    synonyms_path = tmp_path / "synonyms.json"
    synonyms_path.write_text(json.dumps(synonyms_data), encoding="utf-8")
    synonyms = SynonymDictionary.from_file(synonyms_path)
    mapping = MappingService(synonyms).cluster(blocks)
    NormalizationService(synonyms).apply(mapping)
    return mapping


def test_pipeline_resume_and_progress_history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_path = tmp_path / "customers.csv"
    csv_path.write_text(
        "customer_name,email\nAlice,a@example.com\nBob,b@example.com\nCara,c@example.com\n",
        encoding="utf-8",
    )
    runtime = build_runtime(chunk_rows=2)
    mapping = materialize_mapping(runtime, csv_path, tmp_path)

    checkpoint_path = tmp_path / "checkpoint.json"
    dest_dir = tmp_path / "out"
    db_path = tmp_path / "history.db"

    runner = MaterializationJobRunner(
        runtime,
        checkpoint_path=checkpoint_path,
        writer_format="csv",
        spill_threshold=2,
    )
    runner.progress_granularity = 1

    crash_state = {"raised": False}
    original_flush = SpillBuffer.flush

    def flaky_flush(self):  # type: ignore[override]
        original_flush(self)
        if not crash_state["raised"]:
            crash_state["raised"] = True
            raise RuntimeError("simulated crash")

    monkeypatch.setattr(SpillBuffer, "flush", flaky_flush)

    def persist_progress(progress):
        record_progress_event(db_path, progress)

    with pytest.raises(RuntimeError):
        runner.run(mapping, dest_dir, max_jobs=1, progress_callback=persist_progress)

    assert crash_state["raised"], "Crash hook did not fire"
    history = fetch_job_progress_events(db_path, limit=5)
    assert history, "Progress events were not persisted before crash"

    monkeypatch.setattr(SpillBuffer, "flush", original_flush)

    runner_after = MaterializationJobRunner(
        runtime,
        checkpoint_path=checkpoint_path,
        writer_format="csv",
        spill_threshold=2,
    )
    runner_after.progress_granularity = 1
    summaries = runner_after.run(mapping, dest_dir, max_jobs=1, progress_callback=persist_progress)

    if checkpoint_path.exists():
        remaining = checkpoint_path.read_text(encoding="utf-8").strip()
        assert remaining in ("", "{}"), "Checkpoint file should be empty after resume"
    summary = summaries[0]
    assert summary.rows_written == 3
    record_job_metrics(db_path, summary.to_job_metrics())

    with sqlite3.connect(db_path) as conn:
        job_metric_rows = conn.execute("SELECT COUNT(*) FROM job_metrics").fetchone()[0]
    assert job_metric_rows == 1

    csv_outputs = sorted(dest_dir.glob("*.csv"))
    assert csv_outputs, "Materialization did not produce any CSV chunks"
    merged = "\n".join(path.read_text(encoding="utf-8").strip() for path in csv_outputs)
    assert merged.count("Alice,a@example.com") == 1
    assert merged.count("Cara,c@example.com") == 1

    schema_id = str(mapping.schemas[0].id)
    replay = fetch_job_progress_events(db_path, schema_id=schema_id, limit=50)
    assert replay, "History lookup returned no events for schema"
    assert replay[0].processed_rows <= replay[0].total_rows
    assert replay[0].file_path.name.endswith(".materialize")
