from __future__ import annotations

import os
import queue
import shutil
import sqlite3
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paf import json_codec as json

DATABASE_NAME = "state.sqlite3"
LEGACY_BACKUP_NAME = "state.legacy-v6.json"
SCHEMA_VERSION = 2
CHANGE_RETENTION = 10_000


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _create_v1_schema(connection: sqlite3.Connection) -> None:
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
    connection.execute("PRAGMA user_version=1")


def _create_schema(connection: sqlite3.Connection) -> None:
    """Create the normalized current-state schema.

    The v1 checkpoint table is intentionally retained as a migration aid. New
    writes never update it; normalized rows are authoritative in schema v2.
    """

    _create_v1_schema(connection)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            config_fingerprint TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            ordinal INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_units (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS work_units_document_ordinal
            ON work_units(document_id, ordinal, id);
        CREATE TABLE IF NOT EXISTS tasks (
            task_key TEXT PRIMARY KEY,
            work_unit_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            queued INTEGER NOT NULL,
            detail TEXT NOT NULL,
            rounds INTEGER NOT NULL,
            source_digest TEXT,
            updated_at TEXT NOT NULL,
            latest_run_id TEXT,
            run_count INTEGER NOT NULL,
            payload BLOB NOT NULL,
            UNIQUE(work_unit_id, stage)
        );
        CREATE INDEX IF NOT EXISTS tasks_stage_status
            ON tasks(stage, status, queued);
        CREATE TABLE IF NOT EXISTS globals (
            key TEXT PRIMARY KEY,
            revision INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS changes (
            revision INTEGER NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            PRIMARY KEY(revision, entity_type, entity_id)
        );
        CREATE INDEX IF NOT EXISTS changes_entity
            ON changes(entity_type, entity_id, revision);
        """
    )
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(runs)").fetchall()}
    for name, declaration in (
        ("work_unit_id", "TEXT NOT NULL DEFAULT ''"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("role", "TEXT NOT NULL DEFAULT ''"),
        ("finished_at", "TEXT"),
        ("usage", "BLOB NOT NULL DEFAULT '{}'"),
    ):
        if name not in columns:
            connection.execute(f"ALTER TABLE runs ADD COLUMN {name} {declaration}")
    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _split_checkpoint(
    checkpoint: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    header = {
        key: value
        for key, value in checkpoint.items()
        if key not in {"documents", "work_units", "tasks", "source_issues"}
    }
    documents = [value for value in checkpoint.get("documents", []) if isinstance(value, dict)]
    work_units = [value for value in checkpoint.get("work_units", []) if isinstance(value, dict)]
    raw_tasks = checkpoint.get("tasks", {})
    tasks = (
        {str(key): value for key, value in raw_tasks.items() if isinstance(value, dict)}
        if isinstance(raw_tasks, dict)
        else {}
    )
    return header, documents, work_units, tasks


def _upsert_normalized_checkpoint(
    connection: sqlite3.Connection,
    checkpoint: dict[str, Any],
    *,
    revision: int,
) -> None:
    header, documents, work_units, tasks = _split_checkpoint(checkpoint)
    connection.execute(
        """
        INSERT INTO globals(key, revision, payload) VALUES('state', ?, ?)
        ON CONFLICT(key) DO UPDATE SET revision=excluded.revision, payload=excluded.payload
        """,
        (revision, json.dumpb(header)),
    )
    connection.executemany(
        """
        INSERT INTO documents(id, ordinal, payload) VALUES(?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET ordinal=excluded.ordinal, payload=excluded.payload
        """,
        (
            (str(value.get("id", "")), ordinal, json.dumpb(value))
            for ordinal, value in enumerate(documents)
        ),
    )
    connection.executemany(
        """
        INSERT INTO work_units(id, document_id, ordinal, title, source, payload)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            document_id=excluded.document_id,
            ordinal=excluded.ordinal,
            title=excluded.title,
            source=excluded.source,
            payload=excluded.payload
        """,
        (
            (
                str(value.get("id", "")),
                str(value.get("document_id", value.get("book_id", ""))),
                int(value.get("ordinal", value.get("chapter_number", 0))),
                str(value.get("title", value.get("unit_title", ""))),
                str(value.get("source", "")),
                json.dumpb(value),
            )
            for value in work_units
        ),
    )
    connection.executemany(
        """
        INSERT INTO tasks(
            task_key, work_unit_id, stage, status, queued, detail, rounds,
            source_digest, updated_at, latest_run_id, run_count, payload
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(task_key) DO UPDATE SET
            work_unit_id=excluded.work_unit_id,
            stage=excluded.stage,
            status=excluded.status,
            queued=excluded.queued,
            detail=excluded.detail,
            rounds=excluded.rounds,
            source_digest=excluded.source_digest,
            updated_at=excluded.updated_at,
            latest_run_id=excluded.latest_run_id,
            run_count=excluded.run_count,
            payload=excluded.payload
        """,
        (_task_row(key, value) for key, value in tasks.items()),
    )


def _task_row(task_key: str, task: dict[str, Any]) -> tuple[Any, ...]:
    return (
        task_key,
        str(task.get("work_unit_id", task.get("chapter_id", ""))),
        str(task.get("stage", "")),
        str(task.get("status", "pending")),
        int(bool(task.get("queued", False))),
        str(task.get("detail", "")),
        int(task.get("rounds", 0)),
        task.get("source_digest"),
        str(task.get("updated_at", "")),
        task.get("latest_run_id"),
        int(task.get("run_count", 0)),
        json.dumpb(task),
    )


def _migrate_v1(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT payload FROM checkpoint WHERE singleton=1").fetchone()
    checkpoint = json.loads(row[0]) if row is not None else None
    _create_schema(connection)
    now = str(checkpoint.get("updated_at", "")) if isinstance(checkpoint, dict) else ""
    created = str(checkpoint.get("created_at", now)) if isinstance(checkpoint, dict) else now
    connection.execute(
        """
        INSERT OR IGNORE INTO meta(
            singleton, schema_version, revision, created_at, updated_at, config_fingerprint
        ) VALUES(1, ?, 0, ?, ?, '')
        """,
        (SCHEMA_VERSION, created, now),
    )
    if isinstance(checkpoint, dict):
        _upsert_normalized_checkpoint(connection, checkpoint, revision=0)
    connection.execute(
        """
        UPDATE runs SET
            work_unit_id=chapter_id,
            stage=CASE
                WHEN instr(task_key, ':') > 0
                THEN substr(task_key, instr(task_key, ':') + 1)
                ELSE ''
            END
        WHERE work_unit_id=''
        """
    )


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
            if version == 1:
                with connection:
                    _migrate_v1(connection)
            elif version != SCHEMA_VERSION:
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
            _create_v1_schema(connection)
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
                _migrate_v1(connection)
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


@dataclass(frozen=True)
class DatabaseWrite:
    """A serialized, immutable delta ready for the writer thread."""

    updated_at: str
    globals: dict[str, bytes] = field(default_factory=dict)
    tasks: dict[str, bytes] = field(default_factory=dict)
    runs: dict[str, tuple[str, bytes]] = field(default_factory=dict)
    source_issues: dict[str, bytes] = field(default_factory=dict)
    replace_source_issues: bool = False
    documents: dict[str, tuple[int, bytes]] = field(default_factory=dict)
    work_units: dict[str, tuple[str, int, str, str, bytes]] = field(default_factory=dict)
    config_fingerprint: str | None = None
    replace_static: bool = False
    changes: frozenset[tuple[str, str]] = frozenset()


def _coalesce_writes(writes: list[DatabaseWrite]) -> DatabaseWrite:
    globals_: dict[str, bytes] = {}
    tasks: dict[str, bytes] = {}
    runs: dict[str, tuple[str, bytes]] = {}
    issues: dict[str, bytes] = {}
    documents: dict[str, tuple[int, bytes]] = {}
    work_units: dict[str, tuple[str, int, str, str, bytes]] = {}
    changes: set[tuple[str, str]] = set()
    replace_issues = False
    replace_static = False
    config_fingerprint: str | None = None
    for write in writes:
        globals_.update(write.globals)
        tasks.update(write.tasks)
        runs.update(write.runs)
        if write.replace_source_issues:
            issues.clear()
            replace_issues = True
        issues.update(write.source_issues)
        documents.update(write.documents)
        work_units.update(write.work_units)
        if write.config_fingerprint is not None:
            config_fingerprint = write.config_fingerprint
        replace_static = replace_static or write.replace_static
        changes.update(write.changes)
    return DatabaseWrite(
        updated_at=writes[-1].updated_at,
        globals=globals_,
        tasks=tasks,
        runs=runs,
        source_issues=issues,
        replace_source_issues=replace_issues,
        documents=documents,
        work_units=work_units,
        config_fingerprint=config_fingerprint,
        replace_static=replace_static,
        changes=frozenset(changes),
    )


class StateWriter:
    """Single-connection delta writer with short micro-batching."""

    def __init__(self, database: StateDatabase, *, batch_seconds: float = 0.02) -> None:
        self.database = database
        self.batch_seconds = batch_seconds
        self._queue: queue.Queue[tuple[DatabaseWrite | None, Future[int | None]]] = queue.Queue()
        self._thread = threading.Thread(target=self._run, name="paf-state-writer", daemon=True)
        self._started = False
        self._stop_future: Future[int | None] | None = None

    def start(self) -> None:
        if not self._started:
            self._started = True
            self._thread.start()

    def submit(self, write: DatabaseWrite) -> Future[int | None]:
        if self._stop_future is not None:
            raise RuntimeError("state writer is closed")
        if not self._started:
            self.start()
        future: Future[int | None] = Future()
        self._queue.put((write, future))
        return future

    def stop(self) -> Future[int | None]:
        if self._stop_future is not None:
            return self._stop_future
        future: Future[int | None] = Future()
        self._stop_future = future
        if not self._started:
            future.set_result(None)
            return future
        self._queue.put((None, future))
        return future

    def _run(self) -> None:
        connection = self.database.connect_writer()
        try:
            while True:
                write, future = self._queue.get()
                if write is None:
                    future.set_result(None)
                    return
                writes = [write]
                futures = [future]
                deadline = time.monotonic() + self.batch_seconds
                stopping: Future[int | None] | None = None
                while (remaining := deadline - time.monotonic()) > 0:
                    try:
                        next_write, next_future = self._queue.get(timeout=remaining)
                    except queue.Empty:
                        break
                    if next_write is None:
                        stopping = next_future
                        break
                    writes.append(next_write)
                    futures.append(next_future)
                try:
                    revision = self.database.write_delta(
                        _coalesce_writes(writes), connection=connection
                    )
                except BaseException as error:
                    for item in futures:
                        if not item.cancelled():
                            item.set_exception(error)
                    if stopping is not None:
                        if not stopping.cancelled():
                            stopping.set_exception(error)
                    if stopping is not None:
                        return
                    continue
                for item in futures:
                    if not item.cancelled():
                        item.set_result(revision)
                if stopping is not None:
                    if not stopping.cancelled():
                        stopping.set_result(None)
                    return
        finally:
            connection.close()


class StateDatabase:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / DATABASE_NAME

    def initialize(self) -> None:
        initialize_database(self.state_dir)

    def connect_writer(self) -> sqlite3.Connection:
        """Return the long-lived connection owned by a StateWriter thread."""

        return _connect(self.path)

    def write_delta(
        self, write: DatabaseWrite, *, connection: sqlite3.Connection | None = None
    ) -> int:
        owned = connection is None
        connection = connection or _connect(self.path)
        try:
            with connection:
                revision = self._next_revision(connection, write.updated_at)
                if write.config_fingerprint is not None:
                    connection.execute(
                        "UPDATE meta SET config_fingerprint=? WHERE singleton=1",
                        (write.config_fingerprint,),
                    )
                if write.replace_static:
                    connection.execute("DELETE FROM documents")
                    connection.execute("DELETE FROM work_units")
                    connection.execute("DELETE FROM tasks")
                for key, payload in write.globals.items():
                    connection.execute(
                        """
                        INSERT INTO globals(key, revision, payload) VALUES(?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            revision=excluded.revision, payload=excluded.payload
                        """,
                        (key, revision, payload),
                    )
                for key, payload in write.tasks.items():
                    value = json.loads(payload)
                    if not isinstance(value, dict):
                        raise ValueError(f"invalid task delta for {key}")
                    connection.execute(
                        """
                        INSERT INTO tasks(
                            task_key, work_unit_id, stage, status, queued, detail, rounds,
                            source_digest, updated_at, latest_run_id, run_count, payload
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(task_key) DO UPDATE SET
                            work_unit_id=excluded.work_unit_id,
                            stage=excluded.stage,
                            status=excluded.status,
                            queued=excluded.queued,
                            detail=excluded.detail,
                            rounds=excluded.rounds,
                            source_digest=excluded.source_digest,
                            updated_at=excluded.updated_at,
                            latest_run_id=excluded.latest_run_id,
                            run_count=excluded.run_count,
                            payload=excluded.payload
                        """,
                        _task_row(key, value),
                    )
                for run_id, (task_key, serialized) in write.runs.items():
                    run = json.loads(serialized)
                    if not isinstance(run, dict):
                        raise ValueError(f"invalid run delta for {run_id}")
                    existing = connection.execute(
                        "SELECT payload FROM runs WHERE id=?", (run_id,)
                    ).fetchone()
                    payload = run
                    if existing is not None and not any(
                        name in run for name in ("report", "validation", "isolation")
                    ):
                        existing_payload = json.loads(existing[0])
                        if isinstance(existing_payload, dict):
                            payload = existing_payload | run
                    connection.execute(
                        """
                        INSERT INTO runs(
                            id, task_key, chapter_id, started_at, status, summary, payload,
                            work_unit_id, stage, role, finished_at, usage
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            task_key=excluded.task_key,
                            chapter_id=excluded.chapter_id,
                            started_at=excluded.started_at,
                            status=excluded.status,
                            summary=excluded.summary,
                            payload=excluded.payload,
                            work_unit_id=excluded.work_unit_id,
                            stage=excluded.stage,
                            role=excluded.role,
                            finished_at=excluded.finished_at,
                            usage=excluded.usage
                        """,
                        (
                            run_id,
                            task_key,
                            str(run.get("chapter_id", run.get("work_unit_id", ""))),
                            str(run.get("started_at", "")),
                            str(run.get("status", "pending")),
                            json.dumpb(_run_summary(run)),
                            json.dumpb(payload),
                            str(run.get("work_unit_id", run.get("chapter_id", ""))),
                            str(run.get("stage", "")),
                            str(run.get("role", "")),
                            run.get("finished_at"),
                            json.dumpb(run.get("usage", {})),
                        ),
                    )
                if write.replace_source_issues:
                    connection.execute("DELETE FROM source_issues")
                for issue_id, payload in write.source_issues.items():
                    connection.execute(
                        """
                        INSERT INTO source_issues(id, payload) VALUES(?, ?)
                        ON CONFLICT(id) DO UPDATE SET payload=excluded.payload
                        """,
                        (issue_id, payload),
                    )
                for document_id, (ordinal, payload) in write.documents.items():
                    connection.execute(
                        """
                        INSERT INTO documents(id, ordinal, payload) VALUES(?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            ordinal=excluded.ordinal, payload=excluded.payload
                        """,
                        (document_id, ordinal, payload),
                    )
                for unit_id, row in write.work_units.items():
                    connection.execute(
                        """
                        INSERT INTO work_units(
                            id, document_id, ordinal, title, source, payload
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            document_id=excluded.document_id,
                            ordinal=excluded.ordinal,
                            title=excluded.title,
                            source=excluded.source,
                            payload=excluded.payload
                        """,
                        (unit_id, *row),
                    )
                for entity_type, entity_id in write.changes:
                    connection.execute(
                        "INSERT OR IGNORE INTO changes VALUES(?, ?, ?)",
                        (revision, entity_type, entity_id),
                    )
                self._prune_changes(connection, revision)
            return revision
        finally:
            if owned:
                connection.close()

    def revision(self) -> int:
        with _connect(self.path) as connection:
            row = connection.execute("SELECT revision FROM meta WHERE singleton=1").fetchone()
        return int(row[0]) if row is not None else 0

    def config_fingerprint(self) -> str:
        with _connect(self.path) as connection:
            row = connection.execute(
                "SELECT config_fingerprint FROM meta WHERE singleton=1"
            ).fetchone()
        return str(row[0]) if row is not None else ""

    def status_view(self) -> dict[str, Any] | None:
        with _connect(self.path) as connection:
            row = connection.execute("SELECT payload FROM globals WHERE key='state'").fetchone()
            if row is None:
                return None
            value = json.loads(row[0])
            if not isinstance(value, dict):
                raise ValueError(f"invalid global state in {self.path}")
            counts = {
                str(status): int(count)
                for status, count in connection.execute(
                    "SELECT status, count(*) FROM tasks GROUP BY status"
                )
            }
            revision_row = connection.execute(
                "SELECT revision FROM meta WHERE singleton=1"
            ).fetchone()
            task_metrics = connection.execute(
                """
                SELECT count(*),
                    sum(CASE WHEN status='running' THEN 1 ELSE 0 END),
                    count(DISTINCT work_unit_id)
                FROM tasks
                """
            ).fetchone()
            document_count = int(connection.execute("SELECT count(*) FROM documents").fetchone()[0])
        return dict(value) | {
            "revision": int(revision_row[0]) if revision_row is not None else 0,
            "task_counts": counts,
            "projection_metrics": {
                "task_count": int(task_metrics[0] or 0),
                "running_tasks": int(task_metrics[1] or 0),
                "work_unit_count": int(task_metrics[2] or 0),
                "document_count": document_count,
            },
        }

    def changes_since(self, revision: int) -> dict[str, Any]:
        with _connect(self.path) as connection:
            current_row = connection.execute(
                "SELECT revision FROM meta WHERE singleton=1"
            ).fetchone()
            current = int(current_row[0]) if current_row is not None else 0
            oldest_row = connection.execute("SELECT min(revision) FROM changes").fetchone()
            oldest = int(oldest_row[0]) if oldest_row and oldest_row[0] is not None else current
            if revision < oldest - 1:
                return {"revision": current, "resync_required": True, "changes": []}
            rows = connection.execute(
                """
                SELECT revision, entity_type, entity_id
                FROM changes WHERE revision > ? ORDER BY revision, entity_type, entity_id
                """,
                (revision,),
            ).fetchall()
        return {
            "revision": current,
            "resync_required": False,
            "changes": [
                {"revision": int(item[0]), "entity_type": str(item[1]), "entity_id": str(item[2])}
                for item in rows
            ],
        }

    def load(self) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
        with _connect(self.path) as connection:
            global_row = connection.execute(
                "SELECT payload FROM globals WHERE key='state'"
            ).fetchone()
            checkpoint = json.loads(global_row[0]) if global_row is not None else None
            if isinstance(checkpoint, dict):
                checkpoint = dict(checkpoint)
                revision_row = connection.execute(
                    "SELECT revision FROM meta WHERE singleton=1"
                ).fetchone()
                checkpoint["revision"] = int(revision_row[0]) if revision_row is not None else 0
                checkpoint["documents"] = [
                    json.loads(row[0])
                    for row in connection.execute(
                        "SELECT payload FROM documents ORDER BY ordinal, id"
                    )
                ]
                checkpoint["work_units"] = [
                    json.loads(row[0])
                    for row in connection.execute(
                        "SELECT payload FROM work_units ORDER BY document_id, ordinal, id"
                    )
                ]
                checkpoint["tasks"] = {
                    str(key): json.loads(payload)
                    for key, payload in connection.execute(
                        "SELECT task_key, payload FROM tasks ORDER BY task_key"
                    )
                }
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
            revision = self._next_revision(connection, str(checkpoint["updated_at"]))
            _upsert_normalized_checkpoint(connection, checkpoint, revision=revision)
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
                        id, task_key, chapter_id, started_at, status, summary, payload,
                        work_unit_id, stage, role, finished_at, usage
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        task_key = excluded.task_key,
                        chapter_id = excluded.chapter_id,
                        started_at = excluded.started_at,
                        status = excluded.status,
                        summary = excluded.summary,
                        payload = excluded.payload,
                        work_unit_id = excluded.work_unit_id,
                        stage = excluded.stage,
                        role = excluded.role,
                        finished_at = excluded.finished_at,
                        usage = excluded.usage
                    """,
                    (
                        str(run["id"]),
                        task_key,
                        str(run.get("chapter_id", "")),
                        str(run.get("started_at", "")),
                        str(run.get("status", "pending")),
                        json.dumpb(summary),
                        json.dumpb(payload),
                        str(run.get("work_unit_id", run.get("chapter_id", ""))),
                        str(run.get("stage", "")),
                        str(run.get("role", "")),
                        run.get("finished_at"),
                        json.dumpb(run.get("usage", {})),
                    ),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO changes VALUES(?, 'run', ?)",
                    (revision, str(run["id"])),
                )
            if issues is not None:
                connection.execute("DELETE FROM source_issues")
                connection.executemany(
                    "INSERT INTO source_issues(id, payload) VALUES(?, ?)",
                    ((str(issue["id"]), json.dumpb(issue)) for issue in issues),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO changes VALUES(?, 'source_issues', '*')",
                    (revision,),
                )
            connection.execute(
                "INSERT OR IGNORE INTO changes VALUES(?, 'global', 'state')", (revision,)
            )
            self._prune_changes(connection, revision)

        self._write_export(self.state_dir / "state.json", checkpoint)
        if issues is not None:
            ledger = {
                "version": 1,
                "updated_at": checkpoint["updated_at"],
                "issues": issues,
            }
            self._write_export(self.state_dir / "source-issues.json", ledger, indent=True)

    @staticmethod
    def _next_revision(connection: sqlite3.Connection, updated_at: str) -> int:
        row = connection.execute("SELECT revision FROM meta WHERE singleton=1").fetchone()
        revision = (int(row[0]) if row is not None else 0) + 1
        if row is None:
            connection.execute(
                """
                INSERT INTO meta(
                    singleton, schema_version, revision, created_at, updated_at,
                    config_fingerprint
                ) VALUES(1, ?, ?, ?, ?, '')
                """,
                (SCHEMA_VERSION, revision, updated_at, updated_at),
            )
        else:
            connection.execute(
                "UPDATE meta SET revision=?, updated_at=? WHERE singleton=1",
                (revision, updated_at),
            )
        return revision

    @staticmethod
    def _prune_changes(connection: sqlite3.Connection, revision: int) -> None:
        cutoff = revision - CHANGE_RETENTION
        if cutoff > 0:
            connection.execute("DELETE FROM changes WHERE revision <= ?", (cutoff,))

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

    def export_snapshot(self) -> Path | None:
        """Write legacy JSON artifacts from one explicit normalized read."""

        snapshot = self.full_snapshot()
        if snapshot is None:
            return None
        hot = dict(snapshot)
        issues = hot.pop("source_issues", [])
        raw_tasks = hot.get("tasks", {})
        if isinstance(raw_tasks, dict):
            hot["tasks"] = {
                key: {name: value for name, value in task.items() if name != "runs"}
                | {
                    "run_count": len(task.get("runs", [])),
                    "latest_run_id": (
                        task["runs"][-1].get("id")
                        if isinstance(task.get("runs"), list) and task["runs"]
                        else None
                    ),
                }
                for key, task in raw_tasks.items()
                if isinstance(task, dict)
            }
        state_path = self.state_dir / "state.json"
        self._write_export(state_path, hot)
        self._write_export(
            self.state_dir / "source-issues.json",
            {
                "version": 1,
                "updated_at": hot.get("updated_at", ""),
                "issues": issues,
            },
            indent=True,
        )
        return state_path


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


def read_status_view(state_dir: Path) -> dict[str, Any] | None:
    database = StateDatabase(state_dir)
    if database.path.is_file():
        return database.status_view()
    checkpoint = read_checkpoint(state_dir)
    if checkpoint is None:
        return None
    counts: dict[str, int] = {}
    tasks = checkpoint.get("tasks", {})
    if isinstance(tasks, dict):
        for task in tasks.values():
            if isinstance(task, dict):
                status = str(task.get("status", "pending"))
                counts[status] = counts.get(status, 0) + 1
    return dict(checkpoint) | {"revision": 0, "task_counts": counts}


def read_full_snapshot(state_dir: Path) -> dict[str, Any] | None:
    database = StateDatabase(state_dir)
    if database.path.is_file():
        return database.full_snapshot()
    return read_checkpoint(state_dir)


def read_source_issues(state_dir: Path) -> list[dict[str, Any]] | None:
    database = StateDatabase(state_dir)
    if database.path.is_file():
        _, _, issues = database.load()
        return issues
    path = state_dir / "source-issues.json"
    if not path.is_file():
        return None
    ledger = json.loads(path.read_bytes())
    if not isinstance(ledger, dict) or not isinstance(ledger.get("issues"), list):
        raise ValueError(f"invalid source-issue ledger: {path}")
    return [value for value in ledger["issues"] if isinstance(value, dict)]
