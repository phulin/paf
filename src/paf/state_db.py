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
SCHEMA_VERSION = 3
CHANGE_RETENTION = 10_000

COLLECTION_SECTIONS = frozenset(
    {
        "scheduling",
        "fixup_requests",
        "proof_review_requests",
        "upstream_requests",
        "proof_blockers",
        "thread_cumulative_usage",
        "repair_cases",
        "repair_sweeps",
        "repair_work_units",
        "coordinator_targets",
    }
)
GRAPH_SECTIONS = frozenset({"source_dependency_tree", "formalize_graph"})
NORMALIZED_STATE_KEYS = COLLECTION_SECTIONS.difference({"coordinator_targets"}) | GRAPH_SECTIONS


@dataclass(frozen=True)
class CollectionWrite:
    upserts: dict[str, tuple[int, bytes]] = field(default_factory=dict)
    deletes: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GraphSnapshot:
    metadata: dict[str, Any]
    nodes: dict[tuple[str, str], tuple[int, Any]]
    edges: dict[tuple[str, str, str], int]


@dataclass(frozen=True)
class GraphWrite:
    metadata_upserts: dict[str, bytes] = field(default_factory=dict)
    metadata_deletes: frozenset[str] = frozenset()
    node_upserts: dict[tuple[str, str], tuple[int, bytes]] = field(default_factory=dict)
    node_deletes: frozenset[tuple[str, str]] = frozenset()
    edge_upserts: dict[tuple[str, str, str], int] = field(default_factory=dict)
    edge_deletes: frozenset[tuple[str, str, str]] = frozenset()


def bounded_global_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return the O(1)-sized header retained in ``globals.state``."""

    header = {
        key: value
        for key, value in snapshot.items()
        if key
        not in NORMALIZED_STATE_KEYS
        | {
            "documents",
            "work_units",
            "tasks",
            "source_issues",
            "upstream_request_batches",
            "coordinator_build",
        }
    }
    shepherd = header.get("shepherd")
    if isinstance(shepherd, dict) and "agents" in shepherd:
        header["shepherd"] = {key: value for key, value in shepherd.items() if key != "agents"}
    return header


def collection_snapshot(section: str, value: Any) -> dict[str, tuple[int, Any]]:
    """Split an unbounded state collection into independently writable rows."""

    if section == "scheduling":
        scheduling = value if isinstance(value, dict) else {}
        rows: dict[str, tuple[int, Any]] = {
            "@meta": (
                0,
                {
                    key: item
                    for key, item in scheduling.items()
                    if key not in {"statements", "proofs"}
                },
            )
        }
        ordinal = 1
        for phase in ("statements", "proofs"):
            raw_phase = scheduling.get(phase)
            if not isinstance(raw_phase, dict):
                continue
            order = [item for item in raw_phase.get("order", []) if isinstance(item, str)]
            critical = [
                item for item in raw_phase.get("critical_path", []) if isinstance(item, str)
            ]
            rank = raw_phase.get("rank", {})
            effort = raw_phase.get("effort", {})
            rank = rank if isinstance(rank, dict) else {}
            effort = effort if isinstance(effort, dict) else {}
            ids = dict.fromkeys((*order, *critical, *map(str, rank), *map(str, effort)))
            rows[f"@phase:{phase}"] = (
                ordinal,
                {
                    key: item
                    for key, item in raw_phase.items()
                    if key not in {"order", "critical_path", "rank", "effort"}
                },
            )
            ordinal += 1
            order_positions = {item: index for index, item in enumerate(order)}
            critical_positions = {item: index for index, item in enumerate(critical)}
            for item_id in ids:
                rows[f"{phase}\0{item_id}"] = (
                    ordinal,
                    {
                        "order": order_positions.get(item_id),
                        "critical_path": critical_positions.get(item_id),
                        "rank": rank.get(item_id),
                        "effort": effort.get(item_id),
                    },
                )
                ordinal += 1
        return rows
    if section == "coordinator_targets":
        values = value if isinstance(value, (list, tuple)) else ()
        return {str(item): (ordinal, None) for ordinal, item in enumerate(values)}
    if not isinstance(value, dict):
        return {}
    return {
        str(key): (ordinal, item)
        for ordinal, (key, item) in enumerate(sorted(value.items(), key=lambda pair: str(pair[0])))
    }


def restore_collection(section: str, rows: list[tuple[str, int, Any]]) -> Any:
    if section == "scheduling":
        by_key = {key: value for key, _, value in rows}
        meta = by_key.get("@meta", {})
        scheduling = dict(meta) if isinstance(meta, dict) else {}
        for phase in ("statements", "proofs"):
            phase_meta = by_key.get(f"@phase:{phase}", {})
            phase_value = dict(phase_meta) if isinstance(phase_meta, dict) else {}
            items: list[tuple[str, dict[str, Any]]] = []
            for key, _, payload in rows:
                prefix = f"{phase}\0"
                if key.startswith(prefix) and isinstance(payload, dict):
                    items.append((key[len(prefix) :], payload))
            phase_value["order"] = [
                item_id
                for item_id, _ in sorted(
                    (item for item in items if item[1].get("order") is not None),
                    key=lambda item: int(item[1]["order"]),
                )
            ]
            phase_value["critical_path"] = [
                item_id
                for item_id, _ in sorted(
                    (item for item in items if item[1].get("critical_path") is not None),
                    key=lambda item: int(item[1]["critical_path"]),
                )
            ]
            phase_value["rank"] = {
                item_id: payload["rank"]
                for item_id, payload in items
                if payload.get("rank") is not None
            }
            phase_value["effort"] = {
                item_id: payload["effort"]
                for item_id, payload in items
                if payload.get("effort") is not None
            }
            if items or f"@phase:{phase}" in by_key:
                scheduling[phase] = phase_value
        return scheduling
    if section == "coordinator_targets":
        return [key for key, _, _ in sorted(rows, key=lambda row: row[1])]
    return {key: value for key, _, value in rows}


def graph_snapshot(section: str, value: Any) -> GraphSnapshot:
    graph = value if isinstance(value, dict) else {}
    if section == "source_dependency_tree":
        excluded = {"order", "edges", "dependencies", "nodes"}
        metadata = {key: item for key, item in graph.items() if key not in excluded}
        metadata["__paf_shape"] = sorted(excluded.intersection(graph))
        order = [item for item in graph.get("order", []) if isinstance(item, str)]
        dependencies = graph.get("dependencies", {})
        dependencies = dependencies if isinstance(dependencies, dict) else {}
        records = graph.get("nodes", {})
        records = records if isinstance(records, dict) else {}
        node_ids = dict.fromkeys((*order, *map(str, dependencies), *map(str, records)))
        positions = {item: index for index, item in enumerate(order)}
        nodes = {
            ("dependency", node_id): (
                positions.get(node_id, len(positions)),
                (
                    {key: item for key, item in record.items() if key != "dependencies"}
                    if isinstance((record := records.get(node_id)), dict)
                    else None
                ),
            )
            for node_id in node_ids
        }
        edge_values = [
            ("dependency", str(edge[0]), str(edge[1]))
            for edge in graph.get("edges", [])
            if isinstance(edge, list) and len(edge) == 2
        ]
        if not edge_values:
            edge_values = [
                ("dependency", str(prerequisite), str(dependent))
                for dependent, required in dependencies.items()
                if isinstance(required, list)
                for prerequisite in required
                if isinstance(prerequisite, str)
            ]
        edges = {edge: ordinal for ordinal, edge in enumerate(edge_values)}
        return GraphSnapshot(metadata, nodes, edges)

    excluded = {
        "order",
        "edges",
        "dependencies",
        "clean",
        "dirty",
        "interfaces",
        "interface_imports",
        "interface_import_graph",
        "interface_stale",
    }
    metadata = {key: item for key, item in graph.items() if key not in excluded}
    metadata["__paf_shape"] = sorted(excluded.intersection(graph))
    clean = graph.get("clean", {})
    clean = clean if isinstance(clean, dict) else {}
    interfaces = graph.get("interfaces", {})
    interfaces = interfaces if isinstance(interfaces, dict) else {}
    imports = graph.get("interface_imports", {})
    imports = imports if isinstance(imports, dict) else {}
    dirty = {str(item) for item in graph.get("dirty", [])}
    stale = {str(item) for item in graph.get("interface_stale", [])}
    dependencies = graph.get("dependencies", {})
    dependencies = dependencies if isinstance(dependencies, dict) else {}
    order = [item for item in graph.get("order", []) if isinstance(item, str)]
    node_ids = dict.fromkeys(
        (
            *order,
            *map(str, dependencies),
            *map(str, clean),
            *map(str, interfaces),
            *map(str, imports),
            *dirty,
            *stale,
        )
    )
    positions = {item: index for index, item in enumerate(order)}
    nodes: dict[tuple[str, str], tuple[int, Any]] = {
        ("dependency", node_id): (
            positions.get(node_id, len(positions)),
            {
                "clean": clean.get(node_id),
                "interface": interfaces.get(node_id),
                "dirty": node_id in dirty,
                "stale": node_id in stale,
                "interface_imports": node_id in imports,
            },
        )
        for node_id in node_ids
    }
    dependency_edges = [
        ("dependency", str(edge[0]), str(edge[1]))
        for edge in graph.get("edges", [])
        if isinstance(edge, list) and len(edge) == 2
    ]
    if not dependency_edges:
        dependency_edges = [
            ("dependency", str(prerequisite), str(dependent))
            for dependent, required in dependencies.items()
            if isinstance(required, list)
            for prerequisite in required
            if isinstance(prerequisite, str)
        ]
    edges = {edge: ordinal for ordinal, edge in enumerate(dependency_edges)}
    next_edge_ordinal = len(edges)
    for dependent, required in imports.items():
        if not isinstance(required, list):
            continue
        for prerequisite in required:
            if not isinstance(prerequisite, str):
                continue
            edges[("interface_raw", prerequisite, str(dependent))] = next_edge_ordinal
            next_edge_ordinal += 1
    interface_graph = graph.get("interface_import_graph", {})
    interface_graph = interface_graph if isinstance(interface_graph, dict) else {}
    interface_order = [item for item in interface_graph.get("order", []) if isinstance(item, str)]
    interface_dependencies = interface_graph.get("dependencies", {})
    interface_dependencies = (
        interface_dependencies if isinstance(interface_dependencies, dict) else {}
    )
    interface_ids = dict.fromkeys((*interface_order, *map(str, interface_dependencies)))
    interface_positions = {item: index for index, item in enumerate(interface_order)}
    nodes.update(
        {
            ("interface", node_id): (
                interface_positions.get(node_id, len(interface_positions)),
                None,
            )
            for node_id in interface_ids
        }
    )
    interface_edges = [
        ("interface", str(edge[0]), str(edge[1]))
        for edge in interface_graph.get("edges", [])
        if isinstance(edge, list) and len(edge) == 2
    ]
    if not interface_edges:
        interface_edges = [
            ("interface", str(prerequisite), str(dependent))
            for dependent, required in interface_dependencies.items()
            if isinstance(required, list)
            for prerequisite in required
            if isinstance(prerequisite, str)
        ]
    edges.update(
        {edge: next_edge_ordinal + ordinal for ordinal, edge in enumerate(interface_edges)}
    )
    metadata["interface_graph_algorithm"] = interface_graph.get(
        "algorithm", "observed-lean-imports"
    )
    metadata["interface_graph_coverage"] = interface_graph.get("coverage", len(imports))
    return GraphSnapshot(metadata, nodes, edges)


def restore_graph(snapshot: GraphSnapshot, section: str) -> dict[str, Any]:
    metadata = dict(snapshot.metadata)
    raw_shape = metadata.pop("__paf_shape", ())
    shape = {str(key) for key in raw_shape} if isinstance(raw_shape, list) else set()

    def projection(kind: str) -> tuple[list[str], list[list[str]], dict[str, list[str]]]:
        ordered = sorted(
            (
                (node_id, ordinal)
                for (node_kind, node_id), (ordinal, _) in snapshot.nodes.items()
                if node_kind == kind
            ),
            key=lambda item: (item[1], item[0]),
        )
        order = [node_id for node_id, _ in ordered]
        dependencies = {node_id: [] for node_id in order}
        edges = [
            (source, target)
            for (edge_kind, source, target), _ in sorted(
                snapshot.edges.items(), key=lambda item: (item[1], item[0])
            )
            if edge_kind == kind
        ]
        for source, target in edges:
            dependencies.setdefault(source, [])
            dependencies.setdefault(target, []).append(source)
        return order, [[source, target] for source, target in edges], dependencies

    order, edges, dependencies = projection("dependency")
    if section == "source_dependency_tree":
        records: dict[str, Any] = {}
        for (kind, node_id), (_, payload) in snapshot.nodes.items():
            if kind != "dependency" or not isinstance(payload, dict):
                continue
            records[node_id] = dict(payload) | {"dependencies": dependencies.get(node_id, [])}
        projections = {
            "order": order,
            "edges": edges,
            "dependencies": dependencies,
            "nodes": records,
        }
        return metadata | {key: value for key, value in projections.items() if key in shape}

    clean: dict[str, Any] = {}
    interfaces: dict[str, Any] = {}
    dirty: list[str] = []
    stale: list[str] = []
    imports_present: set[str] = set()
    for (kind, node_id), (_, payload) in snapshot.nodes.items():
        if kind != "dependency" or not isinstance(payload, dict):
            continue
        if isinstance(payload.get("clean"), dict):
            clean[node_id] = payload["clean"]
        if isinstance(payload.get("interface"), dict):
            interfaces[node_id] = payload["interface"]
        if payload.get("dirty"):
            dirty.append(node_id)
        if payload.get("stale"):
            stale.append(node_id)
        if payload.get("interface_imports"):
            imports_present.add(node_id)
    interface_order, interface_edges, interface_dependencies = projection("interface")
    raw_imports: dict[str, list[str]] = {node_id: [] for node_id in imports_present}
    for (kind, source, target), _ in sorted(
        snapshot.edges.items(), key=lambda item: (item[1], item[0])
    ):
        if kind == "interface_raw" and target in raw_imports:
            raw_imports[target].append(source)
    interface_imports = {node_id: raw_imports[node_id] for node_id in sorted(imports_present)}
    interface_algorithm = metadata.pop("interface_graph_algorithm", "observed-lean-imports")
    interface_coverage = metadata.pop("interface_graph_coverage", len(interface_imports))
    projections = {
        "order": order,
        "edges": edges,
        "dependencies": dependencies,
        "clean": clean,
        "dirty": sorted(dirty),
        "interfaces": interfaces,
        "interface_imports": interface_imports,
        "interface_import_graph": {
            "algorithm": interface_algorithm,
            "order": interface_order,
            "edges": interface_edges,
            "dependencies": interface_dependencies,
            "coverage": interface_coverage,
        },
        "interface_stale": sorted(stale),
    }
    return metadata | {key: value for key, value in projections.items() if key in shape}


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

    previous_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
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
        CREATE TABLE IF NOT EXISTS state_items (
            section TEXT NOT NULL,
            item_key TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0,
            payload BLOB NOT NULL,
            PRIMARY KEY(section, item_key)
        );
        CREATE INDEX IF NOT EXISTS state_items_order
            ON state_items(section, ordinal, item_key);
        CREATE TABLE IF NOT EXISTS graph_metadata (
            graph TEXT NOT NULL,
            key TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0,
            payload BLOB NOT NULL,
            PRIMARY KEY(graph, key)
        );
        CREATE TABLE IF NOT EXISTS graph_nodes (
            graph TEXT NOT NULL,
            kind TEXT NOT NULL,
            node_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0,
            payload BLOB NOT NULL,
            PRIMARY KEY(graph, kind, node_id)
        );
        CREATE INDEX IF NOT EXISTS graph_nodes_order
            ON graph_nodes(graph, kind, ordinal, node_id);
        CREATE TABLE IF NOT EXISTS graph_edges (
            graph TEXT NOT NULL,
            kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL DEFAULT 0,
            revision INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(graph, kind, source_id, target_id)
        );
        CREATE INDEX IF NOT EXISTS graph_edges_target
            ON graph_edges(graph, kind, target_id, source_id);
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
    if previous_version == 2:
        _migrate_v2_globals(connection)
    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _split_checkpoint(
    checkpoint: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    header = bounded_global_snapshot(checkpoint)
    documents = [value for value in checkpoint.get("documents", []) if isinstance(value, dict)]
    work_units = [value for value in checkpoint.get("work_units", []) if isinstance(value, dict)]
    raw_tasks = checkpoint.get("tasks", {})
    tasks = (
        {str(key): value for key, value in raw_tasks.items() if isinstance(value, dict)}
        if isinstance(raw_tasks, dict)
        else {}
    )
    return header, documents, work_units, tasks


def _replace_collection(connection: sqlite3.Connection, section: str, value: Any) -> None:
    connection.execute("DELETE FROM state_items WHERE section=?", (section,))
    rows = collection_snapshot(section, value)
    connection.executemany(
        "INSERT INTO state_items(section, item_key, ordinal, payload) VALUES(?, ?, ?, ?)",
        ((section, key, ordinal, json.dumpb(payload)) for key, (ordinal, payload) in rows.items()),
    )


def _replace_graph(connection: sqlite3.Connection, section: str, value: Any) -> None:
    snapshot = graph_snapshot(section, value)
    connection.execute("DELETE FROM graph_metadata WHERE graph=?", (section,))
    connection.execute("DELETE FROM graph_nodes WHERE graph=?", (section,))
    connection.execute("DELETE FROM graph_edges WHERE graph=?", (section,))
    connection.executemany(
        "INSERT INTO graph_metadata(graph, key, payload) VALUES(?, ?, ?)",
        ((section, key, json.dumpb(payload)) for key, payload in snapshot.metadata.items()),
    )
    connection.executemany(
        "INSERT INTO graph_nodes(graph, kind, node_id, ordinal, payload) VALUES(?, ?, ?, ?, ?)",
        (
            (section, kind, node_id, ordinal, json.dumpb(payload))
            for (kind, node_id), (ordinal, payload) in snapshot.nodes.items()
        ),
    )
    connection.executemany(
        """
        INSERT INTO graph_edges(graph, kind, source_id, target_id, ordinal)
        VALUES(?, ?, ?, ?, ?)
        """,
        (
            (section, kind, source, target, ordinal)
            for (kind, source, target), ordinal in snapshot.edges.items()
        ),
    )


def _migrate_v2_globals(connection: sqlite3.Connection) -> None:
    row = connection.execute("SELECT payload FROM globals WHERE key='state'").fetchone()
    if row is None:
        return
    checkpoint = json.loads(row[0])
    if not isinstance(checkpoint, dict):
        raise ValueError("invalid global state while migrating schema v2")
    thread_row = connection.execute(
        "SELECT payload FROM globals WHERE key='thread_cumulative_usage'"
    ).fetchone()
    if thread_row is not None:
        thread_usage = json.loads(thread_row[0])
        if isinstance(thread_usage, dict):
            checkpoint["thread_cumulative_usage"] = thread_usage
    for section in COLLECTION_SECTIONS.difference({"coordinator_targets"}):
        _replace_collection(connection, section, checkpoint.get(section, {}))
    for section in GRAPH_SECTIONS:
        _replace_graph(connection, section, checkpoint.get(section, {}))
    coordinator_row = connection.execute(
        "SELECT payload FROM globals WHERE key='coordinator_build'"
    ).fetchone()
    coordinator = (
        json.loads(coordinator_row[0])
        if coordinator_row is not None
        else checkpoint.get("coordinator_build")
    )
    if isinstance(coordinator, dict):
        coordinator = dict(coordinator)
        targets = coordinator.pop("target_work_unit_ids", coordinator.pop("target_chapter_ids", []))
        _replace_collection(connection, "coordinator_targets", targets)
        connection.execute(
            """
            INSERT INTO globals(key, revision, payload)
            SELECT 'coordinator_build', revision, ? FROM meta WHERE singleton=1
            ON CONFLICT(key) DO UPDATE SET payload=excluded.payload
            """,
            (json.dumpb(coordinator),),
        )
    connection.execute(
        "UPDATE globals SET payload=? WHERE key='state'",
        (json.dumpb(bounded_global_snapshot(checkpoint)),),
    )
    connection.execute("DELETE FROM globals WHERE key='thread_cumulative_usage'")


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
    for section in COLLECTION_SECTIONS.difference({"coordinator_targets"}):
        _replace_collection(connection, section, checkpoint.get(section, {}))
    for section in GRAPH_SECTIONS:
        _replace_graph(connection, section, checkpoint.get(section, {}))
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


def _agent_summary(connection: sqlite3.Connection, base_agents: dict[str, Any]) -> dict[str, Any]:
    """Project live agent counters from normalized task and run rows."""

    stage_names = set(base_agents.get("by_stage", {}))
    stage_names.update(
        str(row[0]) for row in connection.execute("SELECT DISTINCT stage FROM tasks")
    )
    by_stage = {stage: 0 for stage in stage_names}
    by_role: dict[str, int] = {}
    for stage, role, count in connection.execute(
        """
        SELECT stage, role, count(*)
        FROM runs WHERE status='running'
        GROUP BY stage, role
        """
    ):
        stage_name = str(stage)
        active = int(count)
        by_stage[stage_name] = by_stage.get(stage_name, 0) + active
        role_name = str(role or stage)
        by_role[role_name] = by_role.get(role_name, 0) + active

    postprocessing_by_stage = {
        stage: 0 for stage in set(base_agents.get("postprocessing_by_stage", {})).union(stage_names)
    }
    queued = 0
    postprocessing = 0
    for stage, queued_count, postprocess_count in connection.execute(
        """
        SELECT stage,
            sum(CASE WHEN queued != 0 THEN 1 ELSE 0 END),
            sum(CASE WHEN status='running'
                AND json_extract(payload, '$.phase')='postprocess' THEN 1 ELSE 0 END)
        FROM tasks GROUP BY stage
        """
    ):
        stage_name = str(stage)
        queued += int(queued_count or 0)
        current_postprocessing = int(postprocess_count or 0)
        postprocessing += current_postprocessing
        postprocessing_by_stage[stage_name] = current_postprocessing

    return base_agents | {
        "active": sum(by_stage.values()),
        "queued": queued,
        "postprocessing": postprocessing,
        "postprocessing_by_stage": postprocessing_by_stage,
        "by_stage": by_stage,
        "by_role": by_role,
    }


def _load_collection(connection: sqlite3.Connection, section: str) -> Any:
    rows = [
        (str(key), int(ordinal), json.loads(payload))
        for key, ordinal, payload in connection.execute(
            """
            SELECT item_key, ordinal, payload FROM state_items
            WHERE section=? ORDER BY ordinal, item_key
            """,
            (section,),
        )
    ]
    return restore_collection(section, rows)


def _load_graph(connection: sqlite3.Connection, section: str) -> dict[str, Any]:
    metadata = {
        str(key): json.loads(payload)
        for key, payload in connection.execute(
            "SELECT key, payload FROM graph_metadata WHERE graph=?", (section,)
        )
    }
    nodes = {
        (str(kind), str(node_id)): (int(ordinal), json.loads(payload))
        for kind, node_id, ordinal, payload in connection.execute(
            """
            SELECT kind, node_id, ordinal, payload FROM graph_nodes
            WHERE graph=? ORDER BY kind, ordinal, node_id
            """,
            (section,),
        )
    }
    edges = {
        (str(kind), str(source), str(target)): int(ordinal)
        for kind, source, target, ordinal in connection.execute(
            """
            SELECT kind, source_id, target_id, ordinal FROM graph_edges
            WHERE graph=? ORDER BY kind, ordinal, source_id, target_id
            """,
            (section,),
        )
    }
    return restore_graph(GraphSnapshot(metadata, nodes, edges), section)


def _upstream_request_batches(requests: Any) -> dict[str, list[str]]:
    batches: dict[str, list[str]] = {}
    if not isinstance(requests, dict):
        return batches
    for request_id, request in sorted(requests.items()):
        if not isinstance(request, dict) or request.get("status") != "requested":
            continue
        owner = request.get("owner_chapter_id")
        if isinstance(owner, str) and owner:
            batches.setdefault(owner, []).append(str(request_id))
    return batches


def _hydrate_normalized_state(
    connection: sqlite3.Connection,
    checkpoint: dict[str, Any],
    *,
    sections: set[str] | frozenset[str] | None = None,
) -> None:
    selected = NORMALIZED_STATE_KEYS if sections is None else sections
    for section in COLLECTION_SECTIONS.difference({"coordinator_targets"}):
        if section in selected:
            checkpoint[section] = _load_collection(connection, section)
    for section in GRAPH_SECTIONS:
        if section in selected:
            checkpoint[section] = _load_graph(connection, section)
    if "upstream_requests" in selected:
        checkpoint["upstream_request_batches"] = _upstream_request_batches(
            checkpoint.get("upstream_requests")
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
            elif version == 2:
                with connection:
                    _create_schema(connection)
            elif version != SCHEMA_VERSION:
                raise ValueError(
                    f"unsupported swarm database schema {version}; expected {SCHEMA_VERSION}"
                )
            else:
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
    collections: dict[str, CollectionWrite] = field(default_factory=dict)
    graphs: dict[str, GraphWrite] = field(default_factory=dict)
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
    collections: dict[str, CollectionWrite] = {}
    graphs: dict[str, GraphWrite] = {}
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
        for section, delta in write.collections.items():
            previous = collections.get(section, CollectionWrite())
            upserts = dict(previous.upserts)
            deletes = set(previous.deletes)
            for key in delta.deletes:
                upserts.pop(key, None)
                deletes.add(key)
            for key, value in delta.upserts.items():
                deletes.discard(key)
                upserts[key] = value
            collections[section] = CollectionWrite(upserts, frozenset(deletes))
        for section, delta in write.graphs.items():
            previous = graphs.get(section, GraphWrite())
            metadata_upserts = dict(previous.metadata_upserts)
            metadata_deletes = set(previous.metadata_deletes)
            node_upserts = dict(previous.node_upserts)
            node_deletes = set(previous.node_deletes)
            edge_upserts = dict(previous.edge_upserts)
            edge_deletes = set(previous.edge_deletes)
            for key in delta.metadata_deletes:
                metadata_upserts.pop(key, None)
                metadata_deletes.add(key)
            for key, value in delta.metadata_upserts.items():
                metadata_deletes.discard(key)
                metadata_upserts[key] = value
            for key in delta.node_deletes:
                node_upserts.pop(key, None)
                node_deletes.add(key)
            for key, value in delta.node_upserts.items():
                node_deletes.discard(key)
                node_upserts[key] = value
            for edge in delta.edge_deletes:
                edge_upserts.pop(edge, None)
                edge_deletes.add(edge)
            for edge, ordinal in delta.edge_upserts.items():
                edge_deletes.discard(edge)
                edge_upserts[edge] = ordinal
            graphs[section] = GraphWrite(
                metadata_upserts,
                frozenset(metadata_deletes),
                node_upserts,
                frozenset(node_deletes),
                edge_upserts,
                frozenset(edge_deletes),
            )
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
        collections=collections,
        graphs=graphs,
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

    def __init__(self, database: StateDatabase, *, batch_seconds: float = 0.01) -> None:
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
                    if not future.cancelled():
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
                    if stopping is not None and not stopping.cancelled():
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
                for section, delta in write.collections.items():
                    connection.executemany(
                        "DELETE FROM state_items WHERE section=? AND item_key=?",
                        ((section, key) for key in delta.deletes),
                    )
                    connection.executemany(
                        """
                        INSERT INTO state_items(section, item_key, ordinal, revision, payload)
                        VALUES(?, ?, ?, ?, ?)
                        ON CONFLICT(section, item_key) DO UPDATE SET
                            ordinal=excluded.ordinal,
                            revision=excluded.revision,
                            payload=excluded.payload
                        """,
                        (
                            (section, key, ordinal, revision, payload)
                            for key, (ordinal, payload) in delta.upserts.items()
                        ),
                    )
                for section, delta in write.graphs.items():
                    connection.executemany(
                        "DELETE FROM graph_metadata WHERE graph=? AND key=?",
                        ((section, key) for key in delta.metadata_deletes),
                    )
                    connection.executemany(
                        """
                        INSERT INTO graph_metadata(graph, key, revision, payload)
                        VALUES(?, ?, ?, ?)
                        ON CONFLICT(graph, key) DO UPDATE SET
                            revision=excluded.revision, payload=excluded.payload
                        """,
                        (
                            (section, key, revision, payload)
                            for key, payload in delta.metadata_upserts.items()
                        ),
                    )
                    connection.executemany(
                        "DELETE FROM graph_nodes WHERE graph=? AND kind=? AND node_id=?",
                        ((section, kind, node_id) for kind, node_id in delta.node_deletes),
                    )
                    connection.executemany(
                        """
                        INSERT INTO graph_nodes(graph, kind, node_id, ordinal, revision, payload)
                        VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(graph, kind, node_id) DO UPDATE SET
                            ordinal=excluded.ordinal,
                            revision=excluded.revision,
                            payload=excluded.payload
                        """,
                        (
                            (section, kind, node_id, ordinal, revision, payload)
                            for (kind, node_id), (ordinal, payload) in delta.node_upserts.items()
                        ),
                    )
                    connection.executemany(
                        """
                        DELETE FROM graph_edges
                        WHERE graph=? AND kind=? AND source_id=? AND target_id=?
                        """,
                        (
                            (section, kind, source, target)
                            for kind, source, target in delta.edge_deletes
                        ),
                    )
                    connection.executemany(
                        """
                        INSERT INTO graph_edges(
                            graph, kind, source_id, target_id, ordinal, revision
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(graph, kind, source_id, target_id) DO UPDATE SET
                            ordinal=excluded.ordinal, revision=excluded.revision
                        """,
                        (
                            (section, kind, source, target, ordinal, revision)
                            for (kind, source, target), ordinal in delta.edge_upserts.items()
                        ),
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
            section_row = connection.execute(
                "SELECT payload FROM globals WHERE key='coordinator_build'"
            ).fetchone()
            if section_row is not None:
                section = json.loads(section_row[0])
                if isinstance(section, dict):
                    value["coordinator_build"] = section | {
                        "target_work_unit_ids": _load_collection(connection, "coordinator_targets")
                    }
            _hydrate_normalized_state(connection, value, sections={"scheduling"})
            base_agents = value.get("agents", {})
            value["agents"] = _agent_summary(
                connection, dict(base_agents) if isinstance(base_agents, dict) else {}
            )
            counts = {
                str(status): int(count)
                for status, count in connection.execute(
                    "SELECT status, count(*) FROM tasks GROUP BY status"
                )
            }
            revision_row = connection.execute(
                "SELECT revision, updated_at FROM meta WHERE singleton=1"
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
            "updated_at": (
                str(revision_row[1])
                if revision_row is not None
                else str(value.get("updated_at", ""))
            ),
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
            if revision > current or revision < oldest - 1:
                return {"revision": current, "resync_required": True, "changes": []}
            rows = connection.execute(
                """
                SELECT revision, entity_type, entity_id
                FROM changes WHERE revision > ? ORDER BY revision, entity_type, entity_id
                """,
                (revision,),
            ).fetchall()
        changes = [
            {
                "revision": int(item[0]),
                "entity_type": str(item[1]),
                "entity_id": str(item[2]),
            }
            for item in rows
        ]
        return {
            "revision": current,
            "resync_required": any(change["entity_type"] == "resync" for change in changes),
            "changes": changes,
        }

    def dashboard_delta(self, revision: int) -> dict[str, Any]:
        """Load changed dashboard projections from one consistent read transaction."""

        with _connect(self.path) as connection:
            connection.execute("BEGIN")
            current_row = connection.execute(
                "SELECT revision, updated_at FROM meta WHERE singleton=1"
            ).fetchone()
            current = int(current_row[0]) if current_row is not None else 0
            oldest_row = connection.execute("SELECT min(revision) FROM changes").fetchone()
            oldest = int(oldest_row[0]) if oldest_row and oldest_row[0] is not None else current
            if revision > current or revision < oldest - 1:
                return {
                    "revision": current,
                    "resync_required": True,
                    "changes": [],
                    "tasks": {},
                    "removed_task_ids": [],
                    "globals": {},
                    "run_ids": [],
                    "active_run_ids": [],
                }
            rows = connection.execute(
                """
                SELECT revision, entity_type, entity_id
                FROM changes WHERE revision > ? ORDER BY revision, entity_type, entity_id
                """,
                (revision,),
            ).fetchall()
            changes = [
                {
                    "revision": int(item[0]),
                    "entity_type": str(item[1]),
                    "entity_id": str(item[2]),
                }
                for item in rows
            ]
            if any(change["entity_type"] == "resync" for change in changes):
                return {
                    "revision": current,
                    "resync_required": True,
                    "changes": changes,
                    "tasks": {},
                    "removed_task_ids": [],
                    "globals": {},
                    "run_ids": [],
                    "active_run_ids": [],
                }

            task_keys = sorted(
                {str(change["entity_id"]) for change in changes if change["entity_type"] == "task"}
            )
            work_unit_ids = sorted(
                {
                    str(change["entity_id"])
                    for change in changes
                    if change["entity_type"] == "work_unit"
                }
            )
            clauses: list[str] = []
            parameters: list[str] = []
            if task_keys:
                clauses.append(f"task_key IN ({','.join('?' for _ in task_keys)})")
                parameters.extend(task_keys)
            if work_unit_ids:
                clauses.append(f"work_unit_id IN ({','.join('?' for _ in work_unit_ids)})")
                parameters.extend(work_unit_ids)
            task_rows = (
                connection.execute(
                    f"SELECT task_key, payload FROM tasks WHERE {' OR '.join(clauses)}",
                    parameters,
                ).fetchall()
                if clauses
                else []
            )
            tasks = {str(key): json.loads(payload) for key, payload in task_rows}
            global_ids = {
                str(change["entity_id"]) for change in changes if change["entity_type"] == "global"
            }
            globals_: dict[str, Any] = {}
            if global_ids:
                global_row = connection.execute(
                    "SELECT payload FROM globals WHERE key='state'"
                ).fetchone()
                if "state" in global_ids and global_row is not None:
                    value = json.loads(global_row[0])
                    if not isinstance(value, dict):
                        raise ValueError(f"invalid global state in {self.path}")
                    globals_ = dict(value)
                    globals_["revision"] = current
                    if current_row is not None:
                        globals_["updated_at"] = str(current_row[1])
                normalized_ids = global_ids.intersection(NORMALIZED_STATE_KEYS)
                _hydrate_normalized_state(connection, globals_, sections=normalized_ids)
                if "coordinator_build" in global_ids:
                    section_row = connection.execute(
                        "SELECT payload FROM globals WHERE key='coordinator_build'"
                    ).fetchone()
                    if section_row is not None:
                        section = json.loads(section_row[0])
                        if isinstance(section, dict):
                            globals_["coordinator_build"] = section | {
                                "target_work_unit_ids": _load_collection(
                                    connection, "coordinator_targets"
                                )
                            }
            task_or_run_changed = any(
                change["entity_type"] in {"task", "run", "work_unit"} for change in changes
            )
            if task_or_run_changed and "agents" not in globals_:
                global_row = connection.execute(
                    "SELECT payload FROM globals WHERE key='state'"
                ).fetchone()
                base_agents: dict[str, Any] = {}
                if global_row is not None:
                    base = json.loads(global_row[0])
                    if isinstance(base, dict) and isinstance(base.get("agents"), dict):
                        base_agents = dict(base["agents"])
                globals_["agents"] = _agent_summary(connection, base_agents)
            active_run_ids = [
                str(row[0])
                for row in connection.execute(
                    "SELECT id FROM runs WHERE status='running' ORDER BY started_at, id"
                )
            ]
        loaded_task_keys = set(tasks)
        return {
            "revision": current,
            "resync_required": False,
            "changes": changes,
            "tasks": tasks,
            "removed_task_ids": sorted(set(task_keys).difference(loaded_task_keys)),
            "globals": globals_,
            "run_ids": sorted(
                {str(change["entity_id"]) for change in changes if change["entity_type"] == "run"}
            ),
            "active_run_ids": active_run_ids,
        }

    def load(self) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
        with _connect(self.path) as connection:
            global_row = connection.execute(
                "SELECT payload FROM globals WHERE key='state'"
            ).fetchone()
            checkpoint = json.loads(global_row[0]) if global_row is not None else None
            if isinstance(checkpoint, dict):
                checkpoint = dict(checkpoint)
                section_row = connection.execute(
                    "SELECT payload FROM globals WHERE key='coordinator_build'"
                ).fetchone()
                if section_row is not None:
                    section = json.loads(section_row[0])
                    if isinstance(section, dict):
                        checkpoint["coordinator_build"] = section | {
                            "target_work_unit_ids": _load_collection(
                                connection, "coordinator_targets"
                            )
                        }
                _hydrate_normalized_state(connection, checkpoint)
                revision_row = connection.execute(
                    "SELECT revision, updated_at FROM meta WHERE singleton=1"
                ).fetchone()
                checkpoint["revision"] = int(revision_row[0]) if revision_row is not None else 0
                if revision_row is not None:
                    checkpoint["updated_at"] = str(revision_row[1])
                checkpoint["documents"] = [
                    json.loads(row[0])
                    for row in connection.execute(
                        "SELECT payload FROM documents ORDER BY ordinal, id"
                    )
                ]
                checkpoint["work_units"] = [
                    json.loads(row[0])
                    for row in connection.execute(
                        """
                        SELECT work_units.payload
                        FROM work_units
                        LEFT JOIN documents ON documents.id = work_units.document_id
                        ORDER BY
                            documents.ordinal,
                            coalesce(
                                cast(
                                    json_extract(work_units.payload, '$.source_start_line') AS INT
                                ),
                                work_units.ordinal
                            ),
                            work_units.id
                        """
                    )
                ]
                checkpoint["tasks"] = {
                    str(key): json.loads(payload)
                    for key, payload in connection.execute(
                        "SELECT task_key, payload FROM tasks ORDER BY task_key"
                    )
                }
                base_agents = checkpoint.get("agents", {})
                checkpoint["agents"] = _agent_summary(
                    connection, dict(base_agents) if isinstance(base_agents, dict) else {}
                )
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

    def write_snapshot(self, output: Path, snapshot: dict[str, Any]) -> Path:
        """Atomically write an explicitly requested JSON snapshot."""

        output = output.resolve()
        database_path = self.path.resolve()
        protected = {
            database_path,
            database_path.with_name(f"{database_path.name}-wal"),
            database_path.with_name(f"{database_path.name}-shm"),
        }
        if output in protected:
            raise ValueError(f"snapshot output cannot overwrite the state database: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        self._write_export(output, snapshot)
        return output

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

    def export_snapshot(self, output: Path) -> Path | None:
        """Explicitly export one complete normalized snapshot as JSON."""

        snapshot = self.full_snapshot()
        if snapshot is None:
            return None
        return self.write_snapshot(output, snapshot)


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
