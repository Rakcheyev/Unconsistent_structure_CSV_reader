"""Helpers for collecting header metadata during Phase 1."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import List, Sequence

from common.models import (
    ColumnProfileResult,
    FileAnalysisResult,
    FileHeaderSummary,
    HeaderOccurrence,
    HeaderTypeProfile,
)
from core.headers.type_inference import TYPE_BUCKETS, classify_value, ensure_type_buckets


@dataclass(slots=True)
class HeaderMetadata:
    file_headers: List[FileHeaderSummary]
    occurrences: List[HeaderOccurrence]
    profiles: List[HeaderTypeProfile]


def build_header_metadata(results: Sequence[FileAnalysisResult]) -> HeaderMetadata:
    file_headers: List[FileHeaderSummary] = []
    occurrences: List[HeaderOccurrence] = []
    profile_accumulator: dict[str, Counter] = defaultdict(Counter)

    for result in results:
        file_id = result.file_path.as_posix()
        max_columns = _max_columns(result)
        headers = _prepare_headers(result.raw_headers, max_columns)
        file_headers.append(FileHeaderSummary(file_id=file_id, headers=headers))
        column_profiles = {
            profile.column_index: profile
            for profile in result.column_profiles
        }
        for idx, header in enumerate(headers):
            normalized = header.strip() or f"column_{idx + 1}"
            occurrences.append(
                HeaderOccurrence(
                    raw_header=normalized,
                    file_id=file_id,
                    column_index=idx,
                )
            )
            type_counts = _aggregate_column_type_counts(result.blocks, idx)
            profile = column_profiles.get(idx)
            if profile:
                _merge_column_profile_counts(type_counts, profile)
            profile_accumulator[normalized].update(type_counts)

    profiles = [
        HeaderTypeProfile(raw_header=raw, type_profile=ensure_type_buckets(dict(counter)))
        for raw, counter in sorted(profile_accumulator.items())
    ]
    return HeaderMetadata(file_headers=file_headers, occurrences=occurrences, profiles=profiles)


def _max_columns(result: FileAnalysisResult) -> int:
    counts = [block.signature.column_count for block in result.blocks if block.signature.column_count]
    if counts:
        return max(counts)
    return len(result.raw_headers)


def _prepare_headers(raw_headers: Sequence[str], target_length: int) -> List[str]:
    headers = [h.strip() for h in raw_headers if h is not None]
    while len(headers) < target_length:
        headers.append(f"column_{len(headers) + 1}")
    return headers or ["column_1"]


def _aggregate_column_type_counts(blocks, column_index: int) -> dict[str, int]:
    counter: Counter = Counter()
    for block in blocks:
        stats = block.signature.columns.get(column_index)
        if not stats:
            continue
        if stats.type_counts:
            counter.update(stats.type_counts)
        elif stats.sample_values:
            inferred = Counter(classify_value(value) for value in stats.sample_values)
            counter.update(inferred)
    return ensure_type_buckets(dict(counter))


def _merge_column_profile_counts(counter: dict[str, int], profile: ColumnProfileResult) -> None:
    mapping = {
        "null": "empty",
        "integer": "integer",
        "float": "float",
        "text": "text",
        "date": "date",
    }
    for bucket, count in profile.type_distribution.items():
        key = mapping.get(bucket)
        if not key:
            continue
        counter[key] = counter.get(key, 0) + int(count)
