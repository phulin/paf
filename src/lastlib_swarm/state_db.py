from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from lastlib_swarm import json_codec as json

DATABASE_NAME = "state.sqlite3"
LEGACY_BACKUP_NAME = "state.legacy-v6.json"
SCHEMA_VERSION = 1


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS checkpoint (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            updated_at TEXT NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            task_key TEXT NOT NULL,
            chapter_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            status TEXT NOT NULL,
            summary BLOB NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS runs_task_started
            ON runs(task_key, started_at, id);
        CREATE INDEX IF NOT EXISTS runs_chapter_started
            ON runs(chapter_id, started_at, id);
        CREATE TABLE IF NOT EXISTS source_issues (
            id TEXT PRIMARY KEY,
            payload BLOB NOT NULL
        );
        """
    )
    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _run_summary(run: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in run.items() if key not in {"report", "validation", "isolation"}
    }


def _clean_report(run: dict[str, Any], issue_ids_by_run: dict[str, list[str]]) -> dict[str, Any]:
    cleaned = dict(run)
    report = cleaned.get("report")
    if isinstance(report, dict):
        report = {key: value for key, value in report.items() if key != "source_issues"}
        issue_ids = issue_ids_by_run.get(str(cleaned.get("id", "")), [])
        if issue_ids:
            report["source_issue_ids"] = issue_ids
        cleaned["report"] = report
    return cleaned


def _legacy_payloads(
    state_path: Path, source_issues_path: Path
) -> tuple[dict[str, Any] | None, list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    raw: dict[str, Any] | None = None
    if state_path.is_file():
        value = json.loads(state_path.read_bytes())
        if not isinstance(value, dict):
            raise ValueError(f"invalid state file: {state_path}")
        if int(value.get("version", 0)) >= 7:
            raise ValueError(
                f"{state_path} references SQLite history, but {DATABASE_NAME} is missing"
            )
        raw = value

    issues_by_id: dict[str, dict[str, Any]] = {}
    if source_issues_path.is_file():
        ledger = json.loads(source_issues_path.read_bytes())
        if not isinstance(ledger, dict) or not isinstance(ledger.get("issues"), list):
            raise ValueError(f"invalid source-issue ledger: {source_issues_path}")
        for issue in ledger["issues"]:
            if isinstance(issue, dict) and isinstance(issue.get("id"), str):
                issues_by_id[issue["id"]] = issue
    if raw is not None and isinstance(raw.get("source_issues"), list):
        for issue in raw["source_issues"]:
            if isinstance(issue, dict) and isinstance(issue.get("id"), str):
                issues_by_id[issue["id"]] = issue

    issue_ids_by_run: dict[str, list[str]] = {}
    for issue_id, issue in issues_by_id.items():
        run_ids = issue.get("run_ids")
        if isinstance(run_ids, list):
            for run_id in run_ids:
                if isinstance(run_id, str):
                    issue_ids_by_run.setdefault(run_id, []).append(issue_id)

    runs: list[tuple[str, dict[str, Any]]] = []
    if raw is not None and isinstance(raw.get("tasks"), dict):
        for task_key, task in raw["tasks"].items():
            if not isinstance(task_key, str) or not isinstance(task, dict):
                continue
            normalized_key = task_key
            if normalized_key.endswith(":repair"):
                normalized_key = f"{normalized_key[: -len(':repair')]}:fixup"
            task_runs = task.get("runs")
            if not isinstance(task_runs, list):
                continue
            for value in task_runs:
                if not isinstance(value, dict) or not isinstance(value.get("id"), str):
                    continue
                run = dict(value)
                if run.get("stage") == "repair":
                    run["stage"] = "fixup"
                runs.append((normalized_key, _clean_report(run, issue_ids_by_run)))

    return raw, runs, list(issues_by_id.values())


def initialize_database(state_dir: Path) -> Path:
    """Create the database, transactionally importing a legacy JSON state once."""

    state_dir.mkdir(parents=True, exist_ok=True)
    database_path = state_dir / DATABASE_NAME
    if database_path.is_file():
        with _connect(database_path) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != SCHEMA_VERSION:
                raise ValueError(
                    f"unsupported swarm database schema {version}; expected {SCHEMA_VERSION}"
                )
            _create_schema(connection)
        return database_path

    state_path = state_dir / "state.json"
    source_issues_path = state_dir / "source-issues.json"
    raw, runs, issues = _legacy_payloads(state_path, source_issues_path)
    temporary = database_path.with_name(f".{database_path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        with _connect(temporary) as connection:
            _create_schema(connection)
            with connection:
                if raw is not None:
                    checkpoint = dict(raw)
                    checkpoint["version"] = 8
                    checkpoint["history_database"] = DATABASE_NAME
                    checkpoint.pop("source_issues", None)
                    raw_tasks = checkpoint.get("tasks")
                    if isinstance(raw_tasks, dict):
                        tasks: dict[str, Any] = {}
                        for task_key, value in raw_tasks.items():
                            if not isinstance(task_key, str) or not isinstance(value, dict):
                                continue
                            normalized_key = task_key
                            task = dict(value)
                            if normalized_key.endswith(":repair"):
                                normalized_key = f"{normalized_key[: -len(':repair')]}:fixup"
                            if task.get("stage") == "repair":
                                task["stage"] = "fixup"
                            task_runs = task.pop("runs", [])
                            task["run_count"] = len(task_runs) if isinstance(task_runs, list) else 0
                            task["latest_run_id"] = (
                                task_runs[-1].get("id")
                                if isinstance(task_runs, list)
                                and task_runs
                                and isinstance(task_runs[-1], dict)
                                else None
                            )
                            tasks[normalized_key] = task
                        checkpoint["tasks"] = tasks
                    connection.execute(
                        "INSERT INTO checkpoint(singleton, updated_at, payload) VALUES(1, ?, ?)",
                        (
                            str(checkpoint.get("updated_at", "")),
                            json.dumpb(checkpoint),
                        ),
                    )
                for task_key, run in runs:
                    summary = _run_summary(run)
                    connection.execute(
                        """
                        INSERT INTO runs(
                            id, task_key, chapter_id, started_at, status, summary, payload
                        ) VALUES(?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(run["id"]),
                            task_key,
                            str(run.get("chapter_id", "")),
                            str(run.get("started_at", "")),
                            str(run.get("status", "pending")),
                            json.dumpb(summary),
                            json.dumpb(run),
                        ),
                    )
                for issue in issues:
                    connection.execute(
                        "INSERT INTO source_issues(id, payload) VALUES(?, ?)",
                        (str(issue["id"]), json.dumpb(issue)),
                    )
            migrated_runs = int(connection.execute("SELECT count(*) FROM runs").fetchone()[0])
            migrated_issues = int(
                connection.execute("SELECT count(*) FROM source_issues").fetchone()[0]
            )
            checkpoint_count = int(
                connection.execute("SELECT count(*) FROM checkpoint").fetchone()[0]
            )
            if migrated_runs != len(runs) or migrated_issues != len(issues):
                raise ValueError(
                    "legacy state migration did not preserve every run and source issue"
                )
            if raw is not None and checkpoint_count != 1:
                raise ValueError("legacy state migration did not preserve its checkpoint")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        if raw is not None:
            backup = state_dir / LEGACY_BACKUP_NAME
            if not backup.exists():
                shutil.copy2(state_path, backup)
        os.replace(temporary, database_path)
    finally:
        temporary.unlink(missing_ok=True)
        temporary.with_name(f"{temporary.name}-wal").unlink(missing_ok=True)
        temporary.with_name(f"{temporary.name}-shm").unlink(missing_ok=True)

    return database_path


class StateDatabase:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / DATABASE_NAME

    def initialize(self) -> None:
        initialize_database(self.state_dir)

    def load(self) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
        with _connect(self.path) as connection:
            checkpoint_row = connection.execute(
                "SELECT payload FROM checkpoint WHERE singleton = 1"
            ).fetchone()
            checkpoint = json.loads(checkpoint_row[0]) if checkpoint_row is not None else None
            summaries = [
                json.loads(row[0])
                for row in connection.execute("SELECT summary FROM runs ORDER BY started_at, id")
            ]
            issues = [
                json.loads(row[0])
                for row in connection.execute("SELECT payload FROM source_issues ORDER BY id")
            ]
        if checkpoint is not None and not isinstance(checkpoint, dict):
            raise ValueError(f"invalid checkpoint in {self.path}")
        return checkpoint, summaries, issues

    def write_batch(
        self,
        checkpoint: dict[str, Any],
        runs: list[tuple[str, dict[str, Any]]],
        issues: list[dict[str, Any]] | None,
    ) -> None:
        with _connect(self.path) as connection, connection:
            connection.execute(
                """
                INSERT INTO checkpoint(singleton, updated_at, payload) VALUES(1, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    payload = excluded.payload
                """,
                (str(checkpoint["updated_at"]), json.dumpb(checkpoint)),
            )
            for task_key, run in runs:
                summary = _run_summary(run)
                payload = run
                if not any(name in run for name in ("report", "validation", "isolation")):
                    existing = connection.execute(
                        "SELECT payload FROM runs WHERE id = ?", (str(run["id"]),)
                    ).fetchone()
                    if existing is not None:
                        existing_payload = json.loads(existing[0])
                        if isinstance(existing_payload, dict):
                            payload = existing_payload | run
                connection.execute(
                    """
                    INSERT INTO runs(
                        id, task_key, chapter_id, started_at, status, summary, payload
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        task_key = excluded.task_key,
                        chapter_id = excluded.chapter_id,
                        started_at = excluded.started_at,
                        status = excluded.status,
                        summary = excluded.summary,
                        payload = excluded.payload
                    """,
                    (
                        str(run["id"]),
                        task_key,
                        str(run.get("chapter_id", "")),
                        str(run.get("started_at", "")),
                        str(run.get("status", "pending")),
                        json.dumpb(summary),
                        json.dumpb(payload),
                    ),
                )
            if issues is not None:
                connection.execute("DELETE FROM source_issues")
                connection.executemany(
                    "INSERT INTO source_issues(id, payload) VALUES(?, ?)",
                    ((str(issue["id"]), json.dumpb(issue)) for issue in issues),
                )

        self._write_export(self.state_dir / "state.json", checkpoint)
        if issues is not None:
            ledger = {
                "version": 1,
                "updated_at": checkpoint["updated_at"],
                "issues": issues,
            }
            self._write_export(self.state_dir / "source-issues.json", ledger, indent=True)

    @staticmethod
    def _write_export(path: Path, value: dict[str, Any], *, indent: bool = False) -> None:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(json.dumpb(value, indent=indent, sort_keys=indent))
        os.replace(temporary, path)

    def run_payload(self, run_id: str) -> dict[str, Any] | None:
        with _connect(self.path) as connection:
            row = connection.execute("SELECT payload FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        value = json.loads(row[0])
        return value if isinstance(value, dict) else None

    def run_payloads(self) -> dict[str, dict[str, Any]]:
        with _connect(self.path) as connection:
            rows = connection.execute("SELECT id, payload FROM runs").fetchall()
        return {
            str(run_id): value
            for run_id, payload in rows
            if isinstance((value := json.loads(payload)), dict)
        }

    def full_snapshot(self) -> dict[str, Any] | None:
        checkpoint, _, issues = self.load()
        if checkpoint is None:
            return None
        tasks_value = checkpoint.get("tasks")
        tasks: dict[str, Any] = {}
        if isinstance(tasks_value, dict):
            for key, value in tasks_value.items():
                if not isinstance(key, str) or not isinstance(value, dict):
                    continue
                task = {
                    name: item
                    for name, item in value.items()
                    if name not in {"run_count", "latest_run_id"}
                }
                task["runs"] = []
                tasks[key] = task
        with _connect(self.path) as connection:
            rows = connection.execute(
                "SELECT task_key, payload FROM runs ORDER BY started_at, id"
            ).fetchall()
        for task_key, payload in rows:
            run = json.loads(payload)
            if isinstance(task_key, str) and isinstance(run, dict) and task_key in tasks:
                tasks[task_key]["runs"].append(run)
        snapshot = dict(checkpoint)
        snapshot["tasks"] = tasks
        snapshot["source_issues"] = issues
        return snapshot


def read_checkpoint(state_dir: Path) -> dict[str, Any] | None:
    database = StateDatabase(state_dir)
    if database.path.is_file():
        checkpoint, _, _ = database.load()
        return checkpoint
    state_path = state_dir / "state.json"
    if not state_path.is_file():
        return None
    value = json.loads(state_path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"invalid state file: {state_path}")
    return value


def read_full_snapshot(state_dir: Path) -> dict[str, Any] | None:
    database = StateDatabase(state_dir)
    if database.path.is_file():
        return database.full_snapshot()
    return read_checkpoint(state_dir)
