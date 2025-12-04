"""Data models shared across UI, core engine, and storage layers."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4


@dataclass(slots=True)
class ColumnStats:
    """Lightweight profiler for a single column inside a sampled block."""

    index: int
    sample_values: Set[str] = field(default_factory=set)
    sample_count: int = 0
    maybe_numeric: bool = True
    maybe_date: bool = True
    maybe_bool: bool = True
    type_counts: Dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class SchemaSignature:
    """Signature inferred from a block of rows during Phase 1."""

    delimiter: str = ","
    column_count: int = 0
    header_sample: Optional[str] = None
    columns: Dict[int, ColumnStats] = field(default_factory=dict)


@dataclass(slots=True)
class FileBlock:
    """Chunk of a file with homogeneous structure guess."""

    file_path: Path
    block_id: int
    start_line: int
    end_line: int
    signature: SchemaSignature = field(default_factory=SchemaSignature)
    schema_id: Optional[UUID] = None


@dataclass(slots=True)
class SchemaColumn:
    """Column definition after manual/automatic review."""

    index: int
    raw_name: str = ""
    normalized_name: str = ""
    data_type: str = "string"  # string,int,decimal,date,bool
    known_variants: List[str] = field(default_factory=list)


@dataclass(slots=True)
class SchemaDefinition:
    """Normalized schema derived from clustered signatures."""

    id: UUID = field(default_factory=uuid4)
    name: str = ""
    columns: List[SchemaColumn] = field(default_factory=list)


@dataclass(slots=True)
class HeaderVariant:
    """Observed header for a specific file/column with a light type profile.

    This is populated during Phase 1 and reused by header clustering/offset detection.
    """

    file_path: Path
    column_index: int
    raw_name: str
    normalized_name: str
    detected_types: Dict[str, int] = field(default_factory=dict)
    sample_values: Set[str] = field(default_factory=set)
    row_count: int = 0


@dataclass(slots=True)
class HeaderCluster:
    """Cluster of semantically equivalent headers (synonyms, fuzzy matches)."""

    cluster_id: UUID = field(default_factory=uuid4)
    canonical_name: str = ""
    variants: List[HeaderVariant] = field(default_factory=list)
    confidence_score: float = 1.0
    needs_review: bool = False


@dataclass(slots=True)
class SchemaMappingEntry:
    """Mapping from a concrete file/column to a canonical header position.

    Used by Phase 2 to realign rows when headers shift between files.
    """

    file_path: Path
    source_index: int
    canonical_name: str
    target_index: int
    offset_from_index: Optional[int] = None
    offset_reason: Optional[str] = None
    offset_confidence: Optional[float] = None


@dataclass(slots=True)
class FileHeaderSummary:
    """Raw header snapshot for a single file."""

    file_id: str
    headers: List[str] = field(default_factory=list)


@dataclass(slots=True)
class HeaderOccurrence:
    """Single (file, column) header occurrence."""

    raw_header: str
    file_id: str
    column_index: int


@dataclass(slots=True)
class HeaderTypeProfile:
    """Aggregated type profile counts for a raw header."""

    raw_header: str
    type_profile: Dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class MappingConfig:
    """Serializable mapping between file blocks and schemas."""

    blocks: List[FileBlock] = field(default_factory=list)
    schemas: List[SchemaDefinition] = field(default_factory=list)
    header_clusters: List[HeaderCluster] = field(default_factory=list)
    schema_mapping: List[SchemaMappingEntry] = field(default_factory=list)
    file_headers: List[FileHeaderSummary] = field(default_factory=list)
    header_occurrences: List[HeaderOccurrence] = field(default_factory=list)
    header_profiles: List[HeaderTypeProfile] = field(default_factory=list)

    def to_dict(self, *, include_samples: bool = False) -> Dict[str, object]:
        """Return a JSON-ready dictionary without copying sample payloads unless requested."""

        from . import mapping_serialization

        return mapping_serialization.mapping_to_dict(self, include_samples=include_samples)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "MappingConfig":
        from . import mapping_serialization

        return mapping_serialization.mapping_from_dict(data)


@dataclass(slots=True)
class FileProgress:
    """Progress payload reported back to UI during heavy jobs."""

    file_path: Path
    processed_rows: int
    total_rows: int
    current_phase: str
    eta_seconds: Optional[float] = None
    schema_id: Optional[str] = None
    schema_name: Optional[str] = None
    rows_per_second: Optional[float] = None
    spill_rows: Optional[int] = None


@dataclass(slots=True)
class FileAnalysisResult:
    """Outcome of analyzing a single file in Phase 1."""

    file_path: Path
    total_lines: int
    blocks: List[FileBlock] = field(default_factory=list)
    raw_headers: List[str] = field(default_factory=list)


@dataclass(slots=True)
class GlobalSettings:
    """Global knobs that apply across profiles."""

    encoding: str = "utf-8"
    error_policy: str = "fail-fast"  # fail-fast | replace
    synonym_dictionary: str = "storage/synonyms.json"


@dataclass(slots=True)
class ProfileSettings:
    """Profile-specific resource limits."""

    description: str
    block_size: int
    min_gap_lines: int
    max_parallel_files: int
    sample_values_cap: int
    writer_chunk_rows: int = 10000  # Recommended for CSV: 10k rows per chunk


@dataclass(slots=True)
class RuntimeConfig:
    """Resolved configuration for a single run."""

    global_settings: GlobalSettings
    profile: ProfileSettings


@dataclass(slots=True)
class ColumnProfile:
    """Lightweight summary metrics for normalized datasets."""

    name: str
    unique_count_estimate: Optional[int] = None
    top_values: List[str] = field(default_factory=list)


@dataclass(slots=True)
class SchemaStats:
    """Aggregated statistics per schema for audit/export."""

    schema_id: UUID
    row_count: int = 0
    columns: List[ColumnProfile] = field(default_factory=list)


@dataclass(slots=True)
class ValidationSummary:
    """Row-level validation counts emitted during materialization."""

    total_rows: int = 0
    short_rows: int = 0
    long_rows: int = 0
    empty_rows: int = 0


@dataclass(slots=True)
class SpillMetrics:
    """Telemetry for spill-to-temp/back-pressure events."""

    spills: int = 0
    rows_spilled: int = 0
    bytes_spilled: int = 0
    max_buffer_rows: int = 0


@dataclass(slots=True)
class JobMetrics:
    """Per-schema materialization metrics persisted to SQLite."""

    schema_id: str
    schema_name: str
    rows_written: int
    duration_seconds: float
    rows_per_second: float
    validation: ValidationSummary = field(default_factory=ValidationSummary)
    spill_metrics: SpillMetrics = field(default_factory=SpillMetrics)


@dataclass(slots=True)
class JobProgressEvent:
    """Stored history of FileProgress ticks for UI history panels."""

    schema_id: str
    schema_name: Optional[str]
    file_path: Path
    processed_rows: int
    total_rows: int
    eta_seconds: Optional[float] = None
    rows_per_second: Optional[float] = None
    spill_rows: Optional[int] = None
    created_at: float = 0.0
