"""CLI workflow shell covering Analyze → Review → Normalize → Materialize."""
from __future__ import annotations

import argparse
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Iterable, List

from common.config import load_runtime_config
from common.models import FileProgress, MappingConfig, RuntimeConfig
from common.progress import BenchmarkRecorder
from core.analysis import AnalysisEngine
from core.mapping import MappingService
from core.materialization import MaterializationPlanner, MaterializationJobRunner
from core.normalization import NormalizationService, SynonymDictionary
from storage import (
    init_sqlite,
    load_mapping_config,
    persist_mapping_sqlite,
    record_audit_event,
    record_job_metrics,
    record_progress_event,
    save_mapping_config,
)

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt"}


def collect_input_files(targets: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for target in targets:
        if target.is_dir():
            files.extend(
                sorted(
                    p
                    for p in target.rglob("*")
                    if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
                )
            )
        elif target.is_file():
            files.append(target)
    deduped = []
    seen = set()
    for path in files:
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    return deduped


def render_progress(progress: FileProgress) -> None:
    print(
        f"[analysis] {progress.file_path} rows={progress.processed_rows} phase={progress.current_phase}"
    )


def render_materialization_progress(progress: FileProgress) -> None:
    total = progress.total_rows if progress.total_rows else "?"
    eta = f" eta={progress.eta_seconds:.1f}s" if progress.eta_seconds is not None else ""
    print(
        f"[materialize/progress] {progress.file_path.name} rows={progress.processed_rows}/{total}{eta}"
    )


def command_analyze(args: argparse.Namespace) -> None:
    inputs = [Path(p) for p in args.inputs]
    files = collect_input_files(inputs)
    if not files:
        raise SystemExit("No input files found. Provide files or directories containing CSV/TSV data.")

    runtime = load_runtime_config(profile=args.profile)
    progress_log = Path(args.progress_log) if args.progress_log else None
    engine = AnalysisEngine(runtime, progress_log=progress_log)

    print(
        f"Starting analysis for {len(files)} file(s) using profile '{args.profile}' "
        f"(block_size={runtime.profile.block_size}, parallel={runtime.profile.max_parallel_files})"
    )

    results = engine.analyze_files(files, progress_callback=render_progress)

    all_blocks = []
    for result in results:
        all_blocks.extend(result.blocks)

    mapping = MappingConfig(blocks=all_blocks, schemas=[])
    output_path = Path(args.output)
    save_mapping_config(mapping, output_path, include_samples=args.include_samples)
    maybe_persist_sqlite(mapping, args.sqlite_db, "analyze")

    print(f"Wrote mapping with {len(all_blocks)} blocks to {output_path}")

    delimiter_counts = Counter(block.signature.delimiter for block in all_blocks)
    if delimiter_counts:
        top_delim, count = delimiter_counts.most_common(1)[0]
        print(f"Most common delimiter: '{top_delim}' ({count} blocks)")

    print(
        "Workflow complete: Import → Analyze summary available. Continue with mapping/normalization next."
    )


def command_benchmark(args: argparse.Namespace) -> None:
    inputs = [Path(p) for p in args.inputs]
    files = collect_input_files(inputs)
    if not files:
        raise SystemExit("No input files found for benchmark.")

    runtime = load_runtime_config(profile=args.profile)
    engine = AnalysisEngine(runtime)
    recorder = BenchmarkRecorder(Path(args.log))

    start = time.perf_counter()
    results = engine.analyze_files(files)
    duration = time.perf_counter() - start
    total_rows = sum(result.total_lines for result in results)
    throughput = total_rows / duration if duration else 0.0
    recorder.record(
        dataset=",".join(args.inputs),
        metrics={"seconds": duration, "rows": total_rows, "rows_per_second": throughput},
    )
    print(
        f"Benchmark complete: {len(files)} file(s) in {duration:.2f}s, throughput {throughput:,.0f} rows/s"
    )


def command_review(args: argparse.Namespace) -> None:
    runtime = load_runtime_config(profile=args.profile)
    mapping = load_mapping_config(Path(args.mapping))
    synonyms = load_synonyms(args.synonyms, runtime)

    service = MappingService(synonyms)
    updated = service.cluster(mapping.blocks)

    output_path = Path(args.output)
    save_mapping_config(updated, output_path, include_samples=args.include_samples)
    maybe_persist_sqlite(updated, args.sqlite_db, "review")

    print(
        f"Clustered {len(updated.blocks)} blocks into {len(updated.schemas)} schema(s). Output: {output_path}"
    )


def command_normalize(args: argparse.Namespace) -> None:
    runtime = load_runtime_config(profile=args.profile)
    mapping = load_mapping_config(Path(args.mapping))
    synonyms = load_synonyms(args.synonyms, runtime)

    service = NormalizationService(synonyms)
    service.apply(mapping)

    output_path = Path(args.output)
    save_mapping_config(mapping, output_path, include_samples=args.include_samples)
    maybe_persist_sqlite(mapping, args.sqlite_db, "normalize")
    print(f"Applied synonym dictionary to {len(mapping.schemas)} schema(s). Output: {output_path}")


def command_materialize(args: argparse.Namespace) -> None:
    runtime = load_runtime_config(profile=args.profile)
    mapping = load_mapping_config(Path(args.mapping))

    if args.writer_format == "database" and not args.db_url:
        raise SystemExit("--db-url is required when --writer-format=database")

    checkpoint_path = Path(args.checkpoint) if args.checkpoint else None
    dest_dir = Path(args.dest)
    telemetry_log = Path(args.telemetry_log) if args.telemetry_log else None
    progress_callbacks: List[Callable[[FileProgress], None]] = [render_materialization_progress]
    progress_db_path: Path | None = Path(args.sqlite_db) if args.sqlite_db else None
    if progress_db_path:

        def persist_progress(progress: FileProgress) -> None:
            record_progress_event(progress_db_path, progress)

        progress_callbacks.append(persist_progress)

    def combined_progress(progress: FileProgress) -> None:
        for callback in progress_callbacks:
            callback(progress)

    runner = MaterializationJobRunner(
        runtime,
        checkpoint_path=checkpoint_path,
        writer_format=args.writer_format,
        spill_threshold=args.spill_threshold,
        telemetry_log=telemetry_log,
        db_url=args.db_url,
    )
    summaries = runner.run(mapping, dest_dir, progress_callback=combined_progress)

    total_rows = sum(summary.rows_written for summary in summaries)
    for summary in summaries:
        validation = summary.validation
        spill = summary.spill_metrics
        print(
            f"[materialize] schema={summary.schema_name or summary.schema_id} blocks={summary.blocks_processed}"
            f" rows={summary.rows_written} rows/s={summary.rows_per_second:,.0f}"
            f" files={len(summary.output_files)} short_rows={validation.short_rows}"
            f" long_rows={validation.long_rows} spills={spill.spills}"
        )
        maybe_record_job_metrics(args.sqlite_db, summary)

    plan_path = Path(args.plan)
    planner = MaterializationPlanner(runtime.profile.writer_chunk_rows)
    plan = planner.build_plan(mapping, dest_dir)
    planner.write_plan(plan, plan_path)
    maybe_record_audit(args.sqlite_db, "materialize", f"schemas={len(summaries)}, rows={total_rows}")
    print(
        f"Materialized {len(summaries)} schema job(s) → {dest_dir}. Rows written: {total_rows}. "
        f"Plan saved to {plan_path}."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="uscsv", description="Resource-aware CSV analysis and normalization helper"
    )
    subparsers = parser.add_subparsers(dest="command")

    analyze = subparsers.add_parser("analyze", help="Analyze files and emit mapping config JSON")
    analyze.add_argument("inputs", nargs="+", help="Files or directories to process")
    analyze.add_argument(
        "--profile",
        default="low_memory",
        help="Profile from config/defaults.json (e.g., low_memory, workstation)",
    )
    analyze.add_argument(
        "--output",
        default="output_data/mapping.json",
        help="Path to write mapping configuration JSON",
    )
    analyze.add_argument(
        "--include-samples",
        action="store_true",
        help="Include per-column sample values in serialized signatures",
    )
    analyze.add_argument(
        "--progress-log",
        help="Path to JSONL file for structured progress events",
    )
    analyze.add_argument(
        "--sqlite-db",
        help="Optional SQLite file to persist mapping + audit log",
    )
    analyze.set_defaults(func=command_analyze)

    benchmark = subparsers.add_parser("benchmark", help="Measure Phase 1 throughput")
    benchmark.add_argument("inputs", nargs="+", help="Files or directories to process")
    benchmark.add_argument(
        "--profile",
        default="low_memory",
        help="Profile from config/defaults.json",
    )
    benchmark.add_argument(
        "--log",
        default="artifacts/benchmarks.jsonl",
        help="Where to append benchmark metrics",
    )
    benchmark.set_defaults(func=command_benchmark)

    review = subparsers.add_parser(
        "review", help="Cluster blocks into schemas and persist updated mapping"
    )
    review.add_argument("mapping", help="Input mapping JSON from analyze phase")
    review.add_argument(
        "--profile",
        default="low_memory",
        help="Profile for defaults (synonym path, future heuristics)",
    )
    review.add_argument(
        "--synonyms",
        help="Override path to synonyms JSON (defaults to profile global settings)",
    )
    review.add_argument(
        "--output",
        default="mapping.review.json",
        help="Destination mapping JSON with schema definitions",
    )
    review.add_argument(
        "--include-samples",
        action="store_true",
        help="Carry sample values forward when serializing signatures",
    )
    review.add_argument(
        "--sqlite-db",
        help="Optional SQLite file to persist mapping + audit log",
    )
    review.set_defaults(func=command_review)

    normalize = subparsers.add_parser(
        "normalize", help="Apply synonym dictionary to schema columns"
    )
    normalize.add_argument("mapping", help="Mapping JSON with schemas")
    normalize.add_argument(
        "--profile",
        default="low_memory",
        help="Profile for defaults (synonym path)",
    )
    normalize.add_argument(
        "--synonyms",
        help="Override path to synonyms JSON",
    )
    normalize.add_argument(
        "--output",
        default="mapping.normalized.json",
        help="Destination mapping JSON",
    )
    normalize.add_argument(
        "--include-samples",
        action="store_true",
        help="Include per-column sample values when writing",
    )
    normalize.add_argument(
        "--sqlite-db",
        help="Optional SQLite file to persist mapping + audit log",
    )
    normalize.set_defaults(func=command_normalize)

    materialize = subparsers.add_parser(
        "materialize", help="Write normalized datasets with resume support"
    )
    materialize.add_argument("mapping", help="Mapping JSON with schemas and block assignments")
    materialize.add_argument(
        "--profile",
        default="low_memory",
        help="Profile to inherit writer chunk sizes",
    )
    materialize.add_argument(
        "--dest",
        default="output_data/",
        help="Base directory where future dataset files will be written",
    )
    materialize.add_argument(
        "--plan",
        default="artifacts/materialization_plan.json",
        help="Path to materialization plan JSON",
    )
    materialize.add_argument(
        "--checkpoint",
        default="artifacts/materialize_checkpoint.json",
        help="Checkpoint file for resume support",
    )
    materialize.add_argument(
        "--sqlite-db",
        help="Optional SQLite file for audit events",
    )
    materialize.add_argument(
        "--writer-format",
        choices=["csv", "parquet", "database"],
        default="csv",
        help="Output format for writers (CSV default, Parquet/DB produce NDJSON/SQL stubs)",
    )
    materialize.add_argument(
        "--spill-threshold",
        type=int,
        default=50_000,
        help="Rows to buffer before spilling to temp/back-pressure telemetry (min 1)",
    )
    materialize.add_argument(
        "--telemetry-log",
        help="Optional JSONL file capturing per-schema throughput/validation",
    )
    materialize.add_argument(
        "--db-url",
        help="Database target for --writer-format database (e.g., sqlite:///artifacts/output.db)",
    )
    materialize.set_defaults(func=command_materialize)

    return parser


def load_synonyms(arg_path: str | None, runtime_config: RuntimeConfig) -> SynonymDictionary:
    path = Path(arg_path) if arg_path else Path(runtime_config.global_settings.synonym_dictionary)
    return SynonymDictionary.from_file(path)


def maybe_persist_sqlite(mapping: MappingConfig, sqlite_arg: str | None, action: str) -> None:
    if not sqlite_arg:
        return
    db_path = Path(sqlite_arg)
    persist_mapping_sqlite(mapping, db_path)
    record_audit_event(
        db_path,
        entity="mapping",
        action=action,
        detail=f"schemas={len(mapping.schemas)} blocks={len(mapping.blocks)}",
    )


def maybe_record_audit(sqlite_arg: str | None, action: str, detail: str) -> None:
    if not sqlite_arg:
        return
    record_audit_event(Path(sqlite_arg), entity="materialization", action=action, detail=detail)


def maybe_record_job_metrics(sqlite_arg: str | None, summary) -> None:
    if not sqlite_arg:
        return
    record_job_metrics(Path(sqlite_arg), summary.to_job_metrics())


def maybe_initialize_sqlite(sqlite_arg: str | None) -> None:
    if not sqlite_arg:
        return
    init_sqlite(Path(sqlite_arg))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return
    maybe_initialize_sqlite(getattr(args, "sqlite_db", None))
    args.func(args)


if __name__ == "__main__":
    main()
