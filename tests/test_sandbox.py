"""Tests for filesystem sandbox enforcement."""
from __future__ import annotations

from pathlib import Path

import pytest

from common.sandbox import Sandbox, SandboxViolation


def test_resolve_within_root(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    root.mkdir()
    sandbox = Sandbox(root)

    resolved = sandbox.resolve("nested", "file.csv")

    assert resolved == root / "nested" / "file.csv"
    assert resolved.is_absolute()


def test_resolve_rejects_escape_attempt(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    root.mkdir()
    sandbox = Sandbox(root)

    with pytest.raises(SandboxViolation) as exc:
        sandbox.resolve("..", "outside.csv")

    assert "outside sandbox" in str(exc.value)


def test_allowlist_permits_external_paths(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    root.mkdir()
    allowed = (tmp_path / "shared").resolve()
    allowed.mkdir()
    sandbox = Sandbox(root, allowlist=(allowed,))

    external_file = allowed / "state.sqlite"
    resolved = sandbox.resolve(str(external_file))

    assert resolved == external_file


def test_ensure_dir_creates_nested_directories(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    root.mkdir()
    sandbox = Sandbox(root)

    created = sandbox.ensure_dir("artifacts", "reports")

    assert created == root / "artifacts" / "reports"
    assert created.is_dir()


def test_must_exist_enforced(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    root.mkdir()
    sandbox = Sandbox(root)

    with pytest.raises(SandboxViolation) as exc:
        sandbox.resolve("missing.txt", must_exist=True)

    assert "does not exist" in str(exc.value)


def test_child_inherits_allowlist(tmp_path: Path) -> None:
    root = (tmp_path / "job").resolve()
    allowed = (tmp_path / "shared").resolve()
    root.mkdir()
    allowed.mkdir()
    sandbox = Sandbox(root, allowlist=(allowed,))

    child = sandbox.child("sub")
    inner = child.resolve("output.csv")
    external = child.resolve(str(allowed / "aux.log"))

    assert child.root == root / "sub"
    assert inner == root / "sub" / "output.csv"
    assert external == allowed / "aux.log"
