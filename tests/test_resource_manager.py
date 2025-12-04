from __future__ import annotations

import pytest

from common.models import ResourceLimits
from core.resources import ResourceLimitError, ResourceManager


def test_plan_workers_respects_limit() -> None:
    manager = ResourceManager(ResourceLimits(max_workers=2))
    assert manager.plan_workers(4) == 2
    assert manager.plan_workers(1) == 1


def test_reserve_and_release_updates_counters() -> None:
    manager = ResourceManager(ResourceLimits(memory_mb=10, spill_mb=20, max_workers=3))
    lease = manager.reserve(memory_mb=4, disk_mb=5, workers=2)
    assert manager.available_memory_mb() == 6
    assert manager.available_disk_mb() == 15
    assert manager.available_workers() == 1
    lease.release()
    assert manager.available_memory_mb() == 10
    assert manager.available_disk_mb() == 20
    assert manager.available_workers() == 3


def test_reserve_raises_when_budget_exceeded() -> None:
    manager = ResourceManager(ResourceLimits(memory_mb=1))
    with pytest.raises(ResourceLimitError):
        manager.reserve(memory_mb=2)


def test_scratch_dir_and_cleanup(tmp_path) -> None:
    manager = ResourceManager(ResourceLimits(temp_dir=str(tmp_path / "scratch")))
    path = manager.scratch_dir("Job#1", "phase", "schema")
    assert path.exists()
    assert path.parent.exists()
    manager.cleanup("Job#1")
    assert not path.exists()
