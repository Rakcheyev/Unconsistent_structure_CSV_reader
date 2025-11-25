from __future__ import annotations

from pathlib import Path

from ui import cli


def test_cli_main_initializes_sqlite(monkeypatch, tmp_path):
    db_path = tmp_path / "cli.db"
    init_calls: list[Path] = []

    def fake_init_sqlite(path: Path) -> None:
        init_calls.append(path)

    monkeypatch.setattr(cli, "init_sqlite", fake_init_sqlite)

    invoked: dict[str, str | None] = {}

    def fake_command(args) -> None:  # type: ignore[override]
        invoked["sqlite_db"] = args.sqlite_db

    monkeypatch.setattr(cli, "command_analyze", fake_command)

    cli.main(
        [
            "analyze",
            str(tmp_path / "input.csv"),
            "--output",
            str(tmp_path / "mapping.json"),
            "--sqlite-db",
            str(db_path),
        ]
    )

    assert init_calls == [db_path]
    assert invoked["sqlite_db"] == str(db_path)
