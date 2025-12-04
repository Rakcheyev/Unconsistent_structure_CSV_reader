"""Graph-based header cluster builder for Phase 1.5."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
import re
from typing import Dict, Iterable, List, Sequence
import unicodedata

from common.models import (
    ColumnStats,
    FileAnalysisResult,
    FileBlock,
    HeaderCluster,
    HeaderVariant,
)
from core.headers.metadata import HeaderMetadata, build_header_metadata
from core.headers.type_inference import ensure_type_buckets

_SLUG_CLEANUP = re.compile(r"[^a-z0-9]+")
_VOWEL_TABLE = str.maketrans("", "", "aeiouy")

_CYRILLIC_LATIN = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "ґ": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "є": "ye",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "і": "i",
    "ї": "yi",
    "й": "i",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}
_TRANLIT_TABLE = str.maketrans({**_CYRILLIC_LATIN})

_DEFAULT_SYNONYM_SETS: Sequence[Sequence[str]] = (
    ("month", "months", "mon", "mth", "місяць", "міс"),
    ("city", "city_name", "town", "місто"),
    ("age", "years", "yrs"),
)


def _transliterate(value: str) -> str:
    return value.translate(_TRANLIT_TABLE)


def _canonical_slug(text: str) -> str:
    lowered = text.lower()
    transliterated = _transliterate(lowered)
    normalized = unicodedata.normalize("NFKD", transliterated)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = _SLUG_CLEANUP.sub(" ", stripped)
    return " ".join(cleaned.split())


def _skeleton(slug: str) -> str:
    return slug.replace(" ", "").translate(_VOWEL_TABLE)


def _metadata_key(raw: str, column_index: int) -> str:
    text = raw.strip()
    if text:
        return text
    return f"column_{column_index + 1}"


def _block_row_count(block: FileBlock) -> int:
    if block.end_line < block.start_line:
        return 0
    return (block.end_line - block.start_line) + 1


def _merge_counts(target: Dict[str, int], source: Dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + int(value)


@dataclass(slots=True)
class VariantAccumulator:
    file_path: Path
    column_index: int
    raw_name: str
    sample_values: set[str] = field(default_factory=set)
    detected_types: Dict[str, int] = field(default_factory=dict)
    row_count: int = 0

    def update(self, stats: ColumnStats | None, rows: int) -> None:
        if stats:
            self.sample_values.update(stats.sample_values)
            _merge_counts(self.detected_types, stats.type_counts)
        self.row_count += max(0, rows)


@dataclass(slots=True)
class HeaderNode:
    key: str
    display_name: str
    slug: str
    alias: str
    translit: str
    skeleton: str
    type_profile: Dict[str, int]
    variants: List[HeaderVariant] = field(default_factory=list)
    total_rows: int = 0

    def add_variant(self, variant: HeaderVariant) -> None:
        self.variants.append(variant)
        self.total_rows += max(0, variant.row_count)

    @property
    def dominant_type(self) -> str | None:
        filtered = {k: v for k, v in self.type_profile.items() if v > 0}
        if not filtered:
            return None
        return max(filtered.items(), key=lambda item: item[1])[0]


class HeaderClusterizer:
    """Builds header clusters by combining metadata and fuzzy similarity."""

    def __init__(
        self,
        *,
        similarity_threshold: float = 0.78,
        review_threshold: float = 0.7,
        synonym_sets: Sequence[Sequence[str]] | None = None,
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.review_threshold = review_threshold
        self.synonym_map = self._build_synonym_map(synonym_sets or _DEFAULT_SYNONYM_SETS)
        self.sample_clip = 32

    def build(
        self,
        results: Sequence[FileAnalysisResult],
        *,
        metadata: HeaderMetadata | None = None,
    ) -> List[HeaderCluster]:
        if not results:
            return []
        metadata = metadata or build_header_metadata(results)
        variants = self._accumulate_variants(results)
        if not variants:
            return []
        nodes = self._build_nodes(variants, metadata)
        if not nodes:
            return []
        groups = self._link_nodes(nodes)
        clusters = [self._build_cluster(group) for group in groups]
        clusters.sort(key=lambda cluster: cluster.canonical_name.lower() if cluster.canonical_name else "")
        return clusters

    def _accumulate_variants(self, results: Sequence[FileAnalysisResult]) -> List[HeaderVariant]:
        accumulators: Dict[tuple[Path, int], VariantAccumulator] = {}
        for result in results:
            headers = self._resolved_headers(result)
            max_columns = len(headers)
            for block in result.blocks:
                column_count = block.signature.column_count or max_columns
                row_count = _block_row_count(block)
                for idx in range(max(column_count, max_columns)):
                    raw_name = headers[idx] if idx < len(headers) else f"column_{idx + 1}"
                    key = (block.file_path, idx)
                    accumulator = accumulators.get(key)
                    if accumulator is None:
                        accumulator = VariantAccumulator(block.file_path, idx, raw_name)
                        accumulators[key] = accumulator
                    elif not accumulator.raw_name.strip() and raw_name.strip():
                        accumulator.raw_name = raw_name
                    stats = block.signature.columns.get(idx)
                    accumulator.update(stats, row_count)
        variants: List[HeaderVariant] = []
        for accumulator in accumulators.values():
            normalized = _canonical_slug(accumulator.raw_name) or accumulator.raw_name.strip() or f"column_{accumulator.column_index + 1}"
            sample_values = accumulator.sample_values
            if len(sample_values) > self.sample_clip:
                sample_values = set(sorted(sample_values)[: self.sample_clip])
            detected_types = ensure_type_buckets(dict(accumulator.detected_types))
            variants.append(
                HeaderVariant(
                    file_path=accumulator.file_path,
                    column_index=accumulator.column_index,
                    raw_name=accumulator.raw_name.strip() or f"column_{accumulator.column_index + 1}",
                    normalized_name=normalized,
                    detected_types=detected_types,
                    sample_values=sample_values,
                    row_count=accumulator.row_count,
                )
            )
        return variants

    def _build_nodes(self, variants: Iterable[HeaderVariant], metadata: HeaderMetadata) -> List[HeaderNode]:
        profile_lookup = {
            item.raw_header.strip(): ensure_type_buckets(dict(item.type_profile))
            for item in metadata.profiles
        }
        nodes: Dict[str, HeaderNode] = {}
        for variant in variants:
            key = _metadata_key(variant.raw_name, variant.column_index)
            slug = _canonical_slug(variant.raw_name)
            alias = self.synonym_map.get(slug, slug)
            translit = slug.replace(" ", "")
            skeleton = _skeleton(slug)
            type_profile = profile_lookup.get(key)
            if type_profile is None:
                type_profile = ensure_type_buckets(dict(variant.detected_types))
            node = nodes.get(key)
            if node is None:
                node = HeaderNode(
                    key=key,
                    display_name=variant.raw_name,
                    slug=slug,
                    alias=alias,
                    translit=translit,
                    skeleton=skeleton,
                    type_profile=type_profile,
                )
                nodes[key] = node
            node.add_variant(variant)
        return list(nodes.values())

    def _link_nodes(self, nodes: List[HeaderNode]) -> List[List[HeaderNode]]:
        if not nodes:
            return []
        parent = {node.key: node.key for node in nodes}

        def find(key: str) -> str:
            while parent[key] != key:
                parent[key] = parent[parent[key]]
                key = parent[key]
            return key

        def union(a: str, b: str) -> None:
            root_a = find(a)
            root_b = find(b)
            if root_a == root_b:
                return
            parent[root_b] = root_a

        alias_buckets: Dict[str, List[str]] = defaultdict(list)
        for node in nodes:
            if node.alias:
                alias_buckets[node.alias].append(node.key)
        for keys in alias_buckets.values():
            base = keys[0]
            for other in keys[1:]:
                union(base, other)

        total = len(nodes)
        for idx in range(total):
            for jdx in range(idx + 1, total):
                left = nodes[idx]
                right = nodes[jdx]
                if self._should_link(left, right):
                    union(left.key, right.key)

        grouped: Dict[str, List[HeaderNode]] = defaultdict(list)
        for node in nodes:
            grouped[find(node.key)].append(node)
        return list(grouped.values())

    def _should_link(self, left: HeaderNode, right: HeaderNode) -> bool:
        if left.alias and left.alias == right.alias:
            return True
        if not left.slug or not right.slug:
            return False
        if left.dominant_type and right.dominant_type and left.dominant_type != right.dominant_type:
            return False
        similarity = SequenceMatcher(None, left.slug, right.slug).ratio()
        if similarity >= self.similarity_threshold:
            return True
        translit_match = bool(left.translit and left.translit == right.translit)
        if translit_match:
            return True
        skeleton_match = bool(left.skeleton and left.skeleton == right.skeleton and len(left.skeleton) >= 3)
        if skeleton_match:
            return True
        short_hand = len(left.slug) <= 4 or len(right.slug) <= 4
        prefix_match = left.slug.startswith(right.slug) or right.slug.startswith(left.slug)
        if short_hand and prefix_match:
            return True
        return False

    def _build_cluster(self, nodes: Sequence[HeaderNode]) -> HeaderCluster:
        type_counter: Counter[str] = Counter()
        variants: List[HeaderVariant] = []
        for node in nodes:
            type_counter.update(node.type_profile)
            variants.extend(node.variants)
        variants.sort(key=lambda variant: (variant.file_path.as_posix(), variant.column_index))
        canonical_name = self._select_canonical_name(nodes)
        confidence = self._compute_confidence(type_counter, variants)
        needs_review = confidence < self.review_threshold or len(nodes) == 1
        return HeaderCluster(
            canonical_name=canonical_name,
            variants=variants,
            confidence_score=confidence,
            needs_review=needs_review,
        )

    def _select_canonical_name(self, nodes: Sequence[HeaderNode]) -> str:
        def score(node: HeaderNode) -> float:
            penalty = 0.25 if node.display_name.lower().startswith("column_") else 0.0
            return node.total_rows * (1.0 - penalty)

        best = max(nodes, key=score)
        return best.display_name

    def _compute_confidence(self, type_counter: Counter[str], variants: Sequence[HeaderVariant]) -> float:
        total_types = sum(type_counter.values())
        purity = max(type_counter.values()) / total_types if total_types else 1.0
        unique_sources = { (variant.file_path, variant.column_index) for variant in variants }
        coverage = min(1.0, len(unique_sources) / 4)
        confidence = 0.35 + 0.4 * purity + 0.25 * coverage
        confidence = max(0.35, min(confidence, 1.0))
        return round(confidence, 2)

    def _build_synonym_map(self, synonym_sets: Sequence[Sequence[str]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for group in synonym_sets:
            canonical_slug = ""
            for token in group:
                slug = _canonical_slug(token)
                if not slug:
                    continue
                if not canonical_slug:
                    canonical_slug = slug
                mapping[slug] = canonical_slug
        return mapping

    def _resolved_headers(self, result: FileAnalysisResult) -> List[str]:
        headers = [header.strip() for header in result.raw_headers if header is not None]
        max_columns = max(
            [len(headers)] + [block.signature.column_count for block in result.blocks if block.signature.column_count]
        )
        while len(headers) < max_columns:
            headers.append(f"column_{len(headers) + 1}")
        return headers or ["column_1"]
