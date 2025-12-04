from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Sequence

from common.models import ColumnStats, FileAnalysisResult, FileBlock, SchemaSignature
from core.headers.cluster_builder import HeaderClusterizer


def _build_stats(index: int, samples: Iterable[str], type_counts: Dict[str, int]) -> ColumnStats:
    stats = ColumnStats(index=index)
    stats.sample_values.update(str(value) for value in samples)
    stats.sample_count = len(stats.sample_values)
    stats.type_counts = dict(type_counts)
    stats.maybe_numeric = "numeric" in type_counts
    stats.maybe_bool = "bool" in type_counts
    stats.maybe_date = "date" in type_counts
    return stats


def _make_result(
    tmp_path: Path,
    file_name: str,
    headers: Sequence[str],
    samples: Sequence[Iterable[str]],
    type_counts: Sequence[Dict[str, int]],
) -> FileAnalysisResult:
    columns = {
        idx: _build_stats(idx, samples[idx], type_counts[idx])
        for idx in range(len(headers))
    }
    signature = SchemaSignature(
        delimiter=",",
        column_count=len(headers),
        header_sample=",".join(headers),
        columns=columns,
    )
    file_path = tmp_path / file_name
    block = FileBlock(
        file_path=file_path,
        block_id=0,
        start_line=0,
        end_line=2,
        signature=signature,
    )
    return FileAnalysisResult(
        file_path=file_path,
        total_lines=3,
        blocks=[block],
        raw_headers=list(headers),
    )


def test_clusterizer_basic_headers(tmp_path) -> None:
    results = [
        _make_result(
            tmp_path,
            "cities.csv",
            ["city", "age"],
            samples=[["Kyiv", "Lviv"], ["30", "25"]],
            type_counts=[{"string": 2}, {"numeric": 2}],
        )
    ]
    clusters = HeaderClusterizer().build(results)
    names = sorted(cluster.canonical_name for cluster in clusters)
    assert names == ["age", "city"]
    city_cluster = next(cluster for cluster in clusters if cluster.canonical_name == "city")
    variant = city_cluster.variants[0]
    assert "Kyiv" in variant.sample_values and "Lviv" in variant.sample_values
    assert city_cluster.confidence_score >= 0.7


def test_clusterizer_merges_month_synonyms(tmp_path) -> None:
    results = [
        _make_result(
            tmp_path,
            "month_en.csv",
            ["month", "value"],
            samples=[["april"], ["10"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
        _make_result(
            tmp_path,
            "month_short.csv",
            ["mon", "value"],
            samples=[["may"], ["12"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
        _make_result(
            tmp_path,
            "month_ua.csv",
            ["місяць", "value"],
            samples=[["червень"], ["15"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
    ]
    clusters = HeaderClusterizer().build(results)
    month_clusters = [
        cluster
        for cluster in clusters
        if {variant.raw_name.lower() for variant in cluster.variants}.issuperset({"month", "mon", "місяць"})
    ]
    assert len(month_clusters) == 1
    names = {variant.raw_name for variant in month_clusters[0].variants}
    assert {"month", "mon", "місяць"}.issubset(names)
    assert month_clusters[0].needs_review is False
    assert month_clusters[0].confidence_score >= 0.7


def test_clusterizer_handles_localized_city_synonyms(tmp_path) -> None:
    results = [
        _make_result(
            tmp_path,
            "city_en.csv",
            ["City", "population"],
            samples=[["Kyiv"], ["100"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
        _make_result(
            tmp_path,
            "city_local.csv",
            ["місто", "населення"],
            samples=[["Львів"], ["80"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
        _make_result(
            tmp_path,
            "city_alt.csv",
            ["town", "population"],
            samples=[["Odessa"], ["150"]],
            type_counts=[{"string": 1}, {"numeric": 1}],
        ),
    ]
    clusters = HeaderClusterizer().build(results)
    city_cluster = None
    for cluster in clusters:
        variant_names = {variant.raw_name.lower() for variant in cluster.variants}
        if {"city", "місто", "town"}.intersection(variant_names):
            city_cluster = cluster
            break
    assert city_cluster is not None, "City-related cluster not found"
    variant_names = {variant.raw_name.lower() for variant in city_cluster.variants}
    assert {"city", "місто", "town"}.issubset(variant_names)
    assert city_cluster.needs_review is False
