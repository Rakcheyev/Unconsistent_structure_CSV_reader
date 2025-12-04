"""Storage and configuration providers (JSON / SQLite)."""

from .json_store import (
	load_mapping_config,
	load_schema_stats,
	save_mapping_config,
	save_schema_stats,
)
from .sqlite_store import initialize as init_sqlite
from .sqlite_store import persist_mapping as persist_mapping_sqlite
from .sqlite_store import (
	fetch_file_headers,
	fetch_header_occurrences,
	fetch_header_profiles,
	fetch_column_profiles,
	fetch_job_progress_events,
	fetch_job_status,
	persist_header_metadata,
	persist_column_profiles,
	prune_progress_history,
	record_audit_event,
	record_job_event,
	record_job_metrics,
	record_progress_event,
	upsert_job_status,
)

__all__ = [
	"load_mapping_config",
	"save_mapping_config",
	"load_schema_stats",
	"save_schema_stats",
	"init_sqlite",
	"persist_mapping_sqlite",
	"persist_header_metadata",
	"persist_column_profiles",
	"record_audit_event",
	"record_job_metrics",
	"record_progress_event",
	"fetch_job_progress_events",
	"fetch_job_status",
	"prune_progress_history",
	"fetch_file_headers",
	"fetch_header_occurrences",
	"fetch_header_profiles",
	"fetch_column_profiles",
	"record_job_event",
	"upsert_job_status",
]
