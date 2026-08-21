from __future__ import annotations

import os
import queue
import shutil
import sqlite3
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from paf import json_codec as json
from paf.package_model import (
    PACKAGE_SNAPSHOT_KEYS,
    CapabilityPackage,
    ConsumerStatus,
    EvidenceKind,
    GlobalPathReservation,
    IntegrationJournal,
    IntegrationPhase,
    PackageConsumer,
    PackageDependency,
    PackageDisposition,
    PackageEvidence,
    PackageRecovery,
    PackageState,
    PackageStatus,
    PackageStep,
    PackageStepKind,
    PackageStepStatus,
    PathReservation,
    RelevantReadInterface,
    ReservationConflict,
    ReservationDecision,
    ReservationMode,
    ReservationOwnerKind,
    ReservationResult,
    ReservationSpec,
    StewardLease,
    canonical_reservation_specs,
    normalize_capability_key,
    normalize_repository_path,
    package_step_key,
)

DATABASE_NAME = "state.sqlite3"
LEGACY_BACKUP_NAME = "state.legacy-v6.json"
SCHEMA_VERSION = 9
CHANGE_RETENTION = 10_000

COLLECTION_SECTIONS = frozenset(
    {
        "scheduling",
        "fixup_requests",
        "proof_review_requests",
        "proof_blockers",
        "thread_cumulative_usage",
        "coordinator_targets",
    }
)
GRAPH_SECTIONS = frozenset({"source_dependency_tree", "formalize_graph"})
NORMALIZED_STATE_KEYS = COLLECTION_SECTIONS.difference({"coordinator_targets"}) | GRAPH_SECTIONS
LEGACY_RUNTIME_KEYS = frozenset(
    {
        "shepherd",
        "upstream_requests",
        "upstream_request_batches",
        "failure_records",
        "repair_cases",
        "repair_sweeps",
        "repair_work_units",
    }
)


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
        | LEGACY_RUNTIME_KEYS
        | PACKAGE_SNAPSHOT_KEYS
        | {
            "documents",
            "work_units",
            "tasks",
            "source_issues",
            "coordinator_build",
        }
    }
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
    connection.execute("PRAGMA foreign_keys=ON")
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


def _migrate_path_reservations_v6(connection: sqlite3.Connection) -> None:
    """Make the historical package table the one global reservation authority."""

    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(path_reservations)")}
    if "owner_kind" in columns:
        return
    connection.execute("ALTER TABLE path_reservations RENAME TO package_path_reservations_v5")
    connection.executescript(
        """
        CREATE TABLE path_reservations (
            normalized_path TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            owner_kind TEXT NOT NULL CHECK(owner_kind IN ('package', 'ordinary_task')),
            owner_id TEXT NOT NULL,
            fence_generation INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT,
            package_id TEXT REFERENCES capability_packages(id) ON DELETE CASCADE,
            CHECK((owner_kind = 'package' AND package_id = owner_id) OR
                  (owner_kind = 'ordinary_task' AND package_id IS NULL))
        );
        CREATE INDEX path_reservations_owner
            ON path_reservations(owner_kind, owner_id, normalized_path);
        INSERT INTO path_reservations(
            normalized_path, mode, owner_kind, owner_id, fence_generation,
            acquired_at, expires_at, package_id
        )
        SELECT normalized_path, mode, 'package', package_id, lease_generation,
            acquired_at, NULL, package_id
        FROM package_path_reservations_v5;
        DROP TABLE package_path_reservations_v5;
        """
    )


def _migrate_package_steps_v8(connection: sqlite3.Connection) -> None:
    """Scope plan-step identities and relationships to their owning package."""

    columns = connection.execute("PRAGMA table_info(package_steps)").fetchall()
    primary_key = tuple(
        str(row[1]) for row in sorted(columns, key=lambda row: int(row[5])) if int(row[5])
    )
    if primary_key == ("package_id", "id"):
        return
    connection.executescript(
        """
        CREATE TABLE package_steps_v8 (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            id TEXT NOT NULL,
            objective TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            assigned_worker_id TEXT,
            validation_contract BLOB NOT NULL,
            remaining_gap TEXT NOT NULL,
            plan_revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(package_id, id)
        );
        CREATE TABLE package_step_items_v8 (
            package_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            item_kind TEXT NOT NULL CHECK(item_kind IN ('declaration', 'path', 'commit')),
            item_value TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(package_id, step_id, item_kind, item_value),
            FOREIGN KEY(package_id, step_id)
                REFERENCES package_steps_v8(package_id, id) ON DELETE CASCADE
        );
        CREATE TABLE package_step_dependencies_v8 (
            package_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            depends_on_step_id TEXT NOT NULL,
            PRIMARY KEY(package_id, step_id, depends_on_step_id),
            FOREIGN KEY(package_id, step_id)
                REFERENCES package_steps_v8(package_id, id) ON DELETE CASCADE,
            FOREIGN KEY(package_id, depends_on_step_id)
                REFERENCES package_steps_v8(package_id, id)
        );
        INSERT INTO package_steps_v8
        SELECT package_id, id, objective, kind, status, assigned_worker_id,
            validation_contract, remaining_gap, plan_revision, created_at, updated_at
        FROM package_steps;
        INSERT INTO package_step_items_v8
        SELECT steps.package_id, items.step_id, items.item_kind, items.item_value, items.ordinal
        FROM package_step_items AS items
        JOIN package_steps AS steps ON steps.id=items.step_id;
        INSERT INTO package_step_dependencies_v8
        SELECT steps.package_id, dependencies.step_id,
            dependencies.depends_on_step_id
        FROM package_step_dependencies AS dependencies
        JOIN package_steps AS steps ON steps.id=dependencies.step_id;
        DELETE FROM package_steps_v8
        WHERE plan_revision > (
            SELECT packages.plan_revision FROM capability_packages AS packages
            WHERE packages.id=package_steps_v8.package_id
        );
        DROP TABLE package_step_dependencies;
        DROP TABLE package_step_items;
        DROP TABLE package_steps;
        ALTER TABLE package_steps_v8 RENAME TO package_steps;
        ALTER TABLE package_step_items_v8 RENAME TO package_step_items;
        ALTER TABLE package_step_dependencies_v8 RENAME TO package_step_dependencies;
        CREATE INDEX package_steps_package_status
            ON package_steps(package_id, status, id);
        """
    )


def _migrate_capability_packages_v9(connection: sqlite3.Connection) -> None:
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(capability_packages)")}
    if "worktree" in columns:
        connection.execute("ALTER TABLE capability_packages DROP COLUMN worktree")
    recovery_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(package_recoveries)")
    }
    if "worktree_head" in recovery_columns:
        connection.execute(
            "ALTER TABLE package_recoveries RENAME COLUMN worktree_head TO candidate_revision"
        )
    if "dirty_digest" in recovery_columns:
        connection.execute(
            "ALTER TABLE package_recoveries RENAME COLUMN dirty_digest TO candidate_digest"
        )
    if "worktree_status" in recovery_columns:
        connection.execute("ALTER TABLE package_recoveries DROP COLUMN worktree_status")


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
        CREATE TABLE IF NOT EXISTS interface_invalidation_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT NOT NULL,
            work_unit_id TEXT NOT NULL,
            source_file TEXT NOT NULL,
            old_digest TEXT,
            new_digest TEXT,
            invalidated_work_unit_ids BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS interface_invalidation_events_source
            ON interface_invalidation_events(source_file, id);
        CREATE TABLE IF NOT EXISTS capability_packages (
            id TEXT PRIMARY KEY,
            capability_key TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            mathematical_objective TEXT NOT NULL,
            status TEXT NOT NULL,
            disposition TEXT,
            base_revision TEXT NOT NULL DEFAULT '',
            branch TEXT NOT NULL DEFAULT '',
            parent_package_id TEXT REFERENCES capability_packages(id),
            plan_revision INTEGER NOT NULL DEFAULT 0,
            integrated_revision TEXT,
            revision INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS capability_packages_status
            ON capability_packages(status, updated_at, id);
        CREATE TABLE IF NOT EXISTS capability_aliases (
            alias_key TEXT PRIMARY KEY,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS capability_aliases_package
            ON capability_aliases(package_id, alias_key);
        CREATE TABLE IF NOT EXISTS package_textbook_refs (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            textbook_ref TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(package_id, textbook_ref)
        );
        CREATE TABLE IF NOT EXISTS package_scopes (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            scope_kind TEXT NOT NULL CHECK(scope_kind IN ('write', 'expansion')),
            normalized_path TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(package_id, scope_kind, normalized_path)
        );
        CREATE TABLE IF NOT EXISTS package_consumers (
            id TEXT PRIMARY KEY,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            work_unit_id TEXT NOT NULL,
            path TEXT NOT NULL,
            declaration TEXT NOT NULL,
            stage TEXT NOT NULL,
            residual_goal TEXT NOT NULL,
            source_digest TEXT,
            acceptance_contract BLOB NOT NULL,
            status TEXT NOT NULL,
            accepted_revision TEXT,
            detached_package_id TEXT REFERENCES capability_packages(id),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(package_id, work_unit_id, path, declaration, stage)
        );
        CREATE INDEX IF NOT EXISTS package_consumers_package_status
            ON package_consumers(package_id, status, id);
        CREATE TABLE IF NOT EXISTS package_consumer_blockers (
            consumer_id TEXT NOT NULL REFERENCES package_consumers(id) ON DELETE CASCADE,
            blocker_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(consumer_id, blocker_id)
        );
        CREATE TABLE IF NOT EXISTS package_consumer_routes (
            consumer_id TEXT NOT NULL REFERENCES package_consumers(id) ON DELETE CASCADE,
            attempted_route TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(consumer_id, attempted_route)
        );
        CREATE TABLE IF NOT EXISTS package_steps (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            id TEXT NOT NULL,
            objective TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            assigned_worker_id TEXT,
            validation_contract BLOB NOT NULL,
            remaining_gap TEXT NOT NULL,
            plan_revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(package_id, id)
        );
        CREATE INDEX IF NOT EXISTS package_steps_package_status
            ON package_steps(package_id, status, id);
        CREATE TABLE IF NOT EXISTS package_step_items (
            package_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            item_kind TEXT NOT NULL CHECK(item_kind IN ('declaration', 'path', 'commit')),
            item_value TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(package_id, step_id, item_kind, item_value),
            FOREIGN KEY(package_id, step_id)
                REFERENCES package_steps(package_id, id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS package_step_dependencies (
            package_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            depends_on_step_id TEXT NOT NULL,
            PRIMARY KEY(package_id, step_id, depends_on_step_id),
            FOREIGN KEY(package_id, step_id)
                REFERENCES package_steps(package_id, id) ON DELETE CASCADE,
            FOREIGN KEY(package_id, depends_on_step_id)
                REFERENCES package_steps(package_id, id)
        );
        CREATE TABLE IF NOT EXISTS package_evidence (
            id TEXT PRIMARY KEY,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            producer TEXT NOT NULL,
            kind TEXT NOT NULL,
            source_revision TEXT NOT NULL,
            payload BLOB NOT NULL,
            digest TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS package_evidence_package_created
            ON package_evidence(package_id, created_at, id);
        CREATE TABLE IF NOT EXISTS package_evidence_items (
            evidence_id TEXT NOT NULL REFERENCES package_evidence(id) ON DELETE CASCADE,
            item_kind TEXT NOT NULL CHECK(item_kind IN ('path', 'declaration')),
            item_value TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(evidence_id, item_kind, item_value)
        );
        CREATE TABLE IF NOT EXISTS steward_leases (
            package_id TEXT PRIMARY KEY REFERENCES capability_packages(id) ON DELETE CASCADE,
            agent_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS path_reservations (
            normalized_path TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            lease_generation INTEGER NOT NULL,
            acquired_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS path_reservations_package
            ON path_reservations(package_id, normalized_path);
        CREATE TABLE IF NOT EXISTS package_dependencies (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            depends_on_package_id TEXT NOT NULL REFERENCES capability_packages(id),
            required_revision TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(package_id, depends_on_package_id),
            CHECK(package_id != depends_on_package_id)
        );
        CREATE TABLE IF NOT EXISTS package_read_interfaces (
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            interface_id TEXT NOT NULL,
            digest TEXT NOT NULL,
            source_revision TEXT NOT NULL,
            PRIMARY KEY(package_id, interface_id)
        );
        CREATE TABLE IF NOT EXISTS integration_journal (
            id TEXT PRIMARY KEY,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            lease_generation INTEGER NOT NULL,
            base_revision TEXT NOT NULL,
            candidate_revision TEXT NOT NULL,
            canonical_revision_before TEXT NOT NULL,
            phase TEXT NOT NULL,
            validation_digest TEXT NOT NULL,
            canonical_revision_after TEXT,
            provisional_consumer_ids BLOB NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS integration_journal_package
            ON integration_journal(package_id, updated_at, id);
        CREATE TABLE IF NOT EXISTS upstream_request_imports (
            request_id TEXT PRIMARY KEY,
            package_id TEXT NOT NULL REFERENCES capability_packages(id),
            evidence_id TEXT NOT NULL REFERENCES package_evidence(id),
            source_digest TEXT NOT NULL,
            imported_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS steward_lease_fences (
            package_id TEXT PRIMARY KEY REFERENCES capability_packages(id) ON DELETE CASCADE,
            generation INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ordinary_reservation_leases (
            owner_id TEXT PRIMARY KEY,
            generation INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS path_reservation_queue (
            id TEXT PRIMARY KEY,
            owner_kind TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            fence_generation INTEGER NOT NULL,
            requested BLOB NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT
        );
        CREATE INDEX IF NOT EXISTS path_reservation_queue_owner
            ON path_reservation_queue(owner_kind, owner_id, created_at, id);
        CREATE TABLE IF NOT EXISTS package_recoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
            prior_generation INTEGER NOT NULL,
            recovered_generation INTEGER NOT NULL,
            candidate_revision TEXT NOT NULL,
            candidate_digest TEXT NOT NULL,
            active_child_workers BLOB NOT NULL,
            journal_phase TEXT,
            recovered_at TEXT NOT NULL
        );
        """
    )
    _migrate_path_reservations_v6(connection)
    _migrate_package_steps_v8(connection)
    _migrate_capability_packages_v9(connection)
    journal_columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(integration_journal)").fetchall()
    }
    if "provisional_consumer_ids" not in journal_columns:
        connection.execute(
            "ALTER TABLE integration_journal ADD COLUMN "
            "provisional_consumer_ids BLOB NOT NULL DEFAULT '[]'"
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
    _import_persisted_upstream_requests(connection)
    connection.execute(
        "DELETE FROM state_items WHERE section IN "
        "('upstream_requests', 'failure_records', 'repair_cases', 'repair_sweeps', "
        "'repair_work_units')"
    )
    connection.execute(
        "UPDATE meta SET schema_version=? WHERE singleton=1",
        (SCHEMA_VERSION,),
    )
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
    _import_legacy_request_mapping(connection, checkpoint.get("upstream_requests"))
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
    _import_legacy_request_mapping(connection, checkpoint.get("upstream_requests"))
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


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _as_utc(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _expires(now: str, ttl_seconds: float) -> str:
    if ttl_seconds <= 0:
        raise ValueError("lease ttl must be positive")
    return (_as_utc(now) + timedelta(seconds=ttl_seconds)).isoformat()


def _content_digest(value: Any) -> str:
    return sha256(json.dumpb(value, sort_keys=True)).hexdigest()


def _stable_record_id(prefix: str, *parts: str) -> str:
    digest = sha256("\0".join(parts).encode()).hexdigest()[:20]
    return f"{prefix}-{digest}"


def _clean_import_paths(values: Any) -> tuple[str, ...]:
    if not isinstance(values, list):
        return ()
    clean: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            path = normalize_repository_path(value)
        except ValueError:
            continue
        if path not in clean:
            clean.append(path)
    return tuple(clean)


def _package_id_for_key(connection: sqlite3.Connection, capability_key: str) -> str | None:
    row = connection.execute(
        """
        SELECT id FROM capability_packages WHERE capability_key=? AND status != 'superseded'
        UNION ALL
        SELECT package_id FROM capability_aliases WHERE alias_key=?
        LIMIT 1
        """,
        (capability_key, capability_key),
    ).fetchone()
    return str(row[0]) if row is not None else None


def _insert_package(connection: sqlite3.Connection, package: CapabilityPackage) -> None:
    connection.execute(
        """
        INSERT INTO capability_packages(
            id, capability_key, title, mathematical_objective, status, disposition,
            base_revision, branch, parent_package_id, plan_revision,
            integrated_revision, revision, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            package.id,
            package.capability_key,
            package.title,
            package.mathematical_objective,
            str(package.status),
            str(package.disposition) if package.disposition is not None else None,
            package.base_revision,
            package.branch,
            package.parent_package_id,
            package.plan_revision,
            package.integrated_revision,
            package.revision,
            package.created_at,
            package.updated_at,
        ),
    )
    aliases = tuple(dict.fromkeys((package.capability_key, *package.aliases)))
    connection.executemany(
        "INSERT INTO capability_aliases(alias_key, package_id) VALUES(?, ?)",
        ((alias, package.id) for alias in aliases),
    )
    connection.executemany(
        "INSERT INTO package_textbook_refs VALUES(?, ?, ?)",
        ((package.id, value, ordinal) for ordinal, value in enumerate(package.textbook_refs)),
    )
    connection.executemany(
        "INSERT INTO package_scopes VALUES(?, 'write', ?, ?)",
        ((package.id, value, ordinal) for ordinal, value in enumerate(package.write_scope)),
    )
    connection.executemany(
        "INSERT INTO package_scopes VALUES(?, 'expansion', ?, ?)",
        ((package.id, value, ordinal) for ordinal, value in enumerate(package.expansion_scope)),
    )


def _insert_consumer(connection: sqlite3.Connection, consumer: PackageConsumer) -> str:
    existing = connection.execute(
        """
        SELECT id FROM package_consumers
        WHERE package_id=? AND work_unit_id=? AND path=? AND declaration=? AND stage=?
        """,
        (
            consumer.package_id,
            consumer.work_unit_id,
            consumer.path,
            consumer.declaration,
            consumer.stage,
        ),
    ).fetchone()
    if existing is not None:
        return str(existing[0])
    connection.execute(
        """
        INSERT INTO package_consumers(
            id, package_id, work_unit_id, path, declaration, stage, residual_goal,
            source_digest, acceptance_contract, status, accepted_revision,
            detached_package_id, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            consumer.id,
            consumer.package_id,
            consumer.work_unit_id,
            consumer.path,
            consumer.declaration,
            consumer.stage,
            consumer.residual_goal,
            consumer.source_digest,
            json.dumpb(consumer.acceptance_contract),
            str(consumer.status),
            consumer.accepted_revision,
            consumer.detached_package_id,
            consumer.created_at,
            consumer.updated_at,
        ),
    )
    connection.executemany(
        "INSERT INTO package_consumer_blockers VALUES(?, ?, ?)",
        ((consumer.id, value, ordinal) for ordinal, value in enumerate(consumer.blocker_ids)),
    )
    connection.executemany(
        "INSERT INTO package_consumer_routes VALUES(?, ?, ?)",
        ((consumer.id, value, ordinal) for ordinal, value in enumerate(consumer.attempted_routes)),
    )
    return consumer.id


def _insert_evidence(connection: sqlite3.Connection, evidence: PackageEvidence) -> None:
    connection.execute(
        """
        INSERT INTO package_evidence(
            id, package_id, producer, kind, source_revision, payload, digest, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence.id,
            evidence.package_id,
            evidence.producer,
            str(evidence.kind),
            evidence.source_revision,
            json.dumpb(evidence.payload),
            evidence.digest,
            evidence.created_at,
        ),
    )
    connection.executemany(
        "INSERT INTO package_evidence_items VALUES(?, 'path', ?, ?)",
        ((evidence.id, value, ordinal) for ordinal, value in enumerate(evidence.paths)),
    )
    connection.executemany(
        "INSERT INTO package_evidence_items VALUES(?, 'declaration', ?, ?)",
        ((evidence.id, value, ordinal) for ordinal, value in enumerate(evidence.declarations)),
    )


def _import_upstream_request(
    connection: sqlite3.Connection, request_id: str, request: dict[str, Any]
) -> str:
    imported = connection.execute(
        "SELECT package_id FROM upstream_request_imports WHERE request_id=?", (request_id,)
    ).fetchone()
    if imported is not None:
        return str(imported[0])

    raw_key = str(request.get("capability_key", "")).strip()
    if not raw_key:
        raw_key = str(request.get("fingerprint", request_id)).strip()
    capability_key = normalize_capability_key(raw_key) or f"legacy-upstream:{request_id}"
    package_id = _package_id_for_key(connection, capability_key)
    now = str(request.get("created_at", "")).strip() or _utc_now()
    owner_paths = _clean_import_paths(request.get("owner_paths"))
    consumer_path = _clean_import_paths([request.get("consumer_path")])
    write_scope = tuple(dict.fromkeys((*owner_paths, *consumer_path)))
    raw_answer = request.get("answer")
    answer: dict[str, Any] = raw_answer if isinstance(raw_answer, dict) else {}
    disposition = str(answer.get("disposition", ""))
    # The old proof agent's owner kind was only a placement hypothesis.  In
    # particular, ``consumer`` and ``shared`` never meant that the capability
    # was unavailable, and even an ``external`` hypothesis still required an
    # upstream answer before it became a terminal fact.
    external = disposition == "external"
    if package_id is None:
        package_id = _stable_record_id("package", capability_key)
        title = str(
            request.get("needed_result")
            or request.get("candidate_signature")
            or request.get("blocked_declaration")
            or capability_key
        ).strip()
        package = CapabilityPackage(
            id=package_id,
            capability_key=capability_key,
            title=title,
            mathematical_objective=str(request.get("needed_result", title)).strip(),
            status=PackageStatus.EXTERNAL if external else PackageStatus.OBSERVED,
            disposition=PackageDisposition.EXTERNAL if external else None,
            write_scope=write_scope,
            expansion_scope=owner_paths,
            created_at=now,
            updated_at=str(request.get("updated_at", now)),
        )
        _insert_package(connection, package)
    package_status = str(
        connection.execute(
            "SELECT status FROM capability_packages WHERE id=?", (package_id,)
        ).fetchone()[0]
    )

    attachments = request.get("consumers")
    if not isinstance(attachments, list) or not attachments:
        attachments = [request]
    for ordinal, raw in enumerate(attachments):
        if not isinstance(raw, dict):
            continue
        work_unit_id = str(raw.get("consumer_chapter_id", request.get("consumer_chapter_id", "")))
        declaration = str(
            raw.get("blocked_declaration", request.get("blocked_declaration", ""))
        ).strip()
        path_value = str(raw.get("consumer_path", request.get("consumer_path", ""))).strip()
        try:
            path_value = normalize_repository_path(path_value) if path_value else ""
        except ValueError:
            path_value = ""
        old_status = str(raw.get("status", request.get("status", "requested")))
        consumer_status = (
            ConsumerStatus.ACCEPTED
            if old_status == "closed" and bool(raw.get("closed_by_run_id"))
            else ConsumerStatus.TERMINAL
            if old_status == "closed" or package_status == str(PackageStatus.EXTERNAL)
            else ConsumerStatus.OPEN
        )
        consumer_id = _stable_record_id(
            "consumer", request_id, str(ordinal), work_unit_id, path_value, declaration
        )
        attempted = raw.get("attempted_alternatives", request.get("attempted_alternatives", []))
        blocker_ids = raw.get("blocker_ids", request.get("blocker_ids", []))
        _insert_consumer(
            connection,
            PackageConsumer(
                id=consumer_id,
                package_id=package_id,
                work_unit_id=work_unit_id,
                path=path_value,
                declaration=declaration,
                stage="prove",
                residual_goal=str(raw.get("residual_goal", request.get("residual_goal", ""))),
                blocker_ids=tuple(str(value) for value in blocker_ids)
                if isinstance(blocker_ids, list)
                else (),
                attempted_routes=tuple(str(value) for value in attempted)
                if isinstance(attempted, list)
                else (),
                acceptance_contract={
                    "tests": [
                        str(value)
                        for value in raw.get(
                            "acceptance_tests", request.get("acceptance_tests", [])
                        )
                        if isinstance(value, str)
                    ],
                    "migrated": True,
                },
                status=consumer_status,
                accepted_revision=(
                    str(raw.get("closed_by_run_id"))
                    if consumer_status is ConsumerStatus.ACCEPTED
                    else None
                ),
                created_at=now,
                updated_at=str(request.get("updated_at", now)),
            ),
        )

    evidence_id = _stable_record_id("evidence", "upstream-request", request_id)
    evidence = PackageEvidence(
        id=evidence_id,
        package_id=package_id,
        producer="legacy-state-importer",
        kind=EvidenceKind.MIGRATION,
        paths=write_scope,
        declarations=tuple(
            value
            for value in (
                str(request.get("blocked_declaration", "")).strip(),
                *(str(item) for item in answer.get("declarations", []) if isinstance(item, str)),
            )
            if value
        ),
        payload={"migration_source": "legacy_state"},
        digest=_content_digest(request),
        created_at=now,
    )
    _insert_evidence(connection, evidence)
    connection.execute(
        "INSERT INTO upstream_request_imports VALUES(?, ?, ?, ?, ?)",
        (request_id, package_id, evidence_id, evidence.digest, _utc_now()),
    )
    connection.execute(
        "UPDATE capability_packages SET revision=revision+1, updated_at=? WHERE id=?",
        (_utc_now(), package_id),
    )
    return package_id


def _import_persisted_upstream_requests(connection: sqlite3.Connection) -> dict[str, str]:
    imported: dict[str, str] = {}
    rows = connection.execute(
        "SELECT item_key, payload FROM state_items WHERE section='upstream_requests'"
    ).fetchall()
    for request_id, payload in rows:
        value = json.loads(payload)
        if isinstance(value, dict):
            imported[str(request_id)] = _import_upstream_request(connection, str(request_id), value)
    return imported


def _import_legacy_request_mapping(connection: sqlite3.Connection, requests: Any) -> dict[str, str]:
    """Import once at the persistence boundary without hydrating legacy runtime state."""

    if not isinstance(requests, dict):
        return {}
    return {
        str(request_id): _import_upstream_request(connection, str(request_id), request)
        for request_id, request in sorted(requests.items(), key=lambda item: str(item[0]))
        if isinstance(request_id, str) and isinstance(request, dict)
    }


def _group_ordered_items(
    connection: sqlite3.Connection,
    query: str,
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for owner_id, value in connection.execute(query):
        grouped.setdefault(str(owner_id), []).append(str(value))
    return {key: tuple(values) for key, values in grouped.items()}


def _group_package_step_items(
    connection: sqlite3.Connection,
    query: str,
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for package_id, step_id, value in connection.execute(query):
        key = package_step_key(str(package_id), str(step_id))
        grouped.setdefault(key, []).append(str(value))
    return {key: tuple(values) for key, values in grouped.items()}


def _load_package_state(connection: sqlite3.Connection) -> PackageState:
    aliases = _group_ordered_items(
        connection,
        "SELECT package_id, alias_key FROM capability_aliases ORDER BY package_id, alias_key",
    )
    textbook_refs = _group_ordered_items(
        connection,
        """SELECT package_id, textbook_ref FROM package_textbook_refs
        ORDER BY package_id, ordinal, textbook_ref""",
    )
    write_scopes = _group_ordered_items(
        connection,
        """SELECT package_id, normalized_path FROM package_scopes
        WHERE scope_kind='write' ORDER BY package_id, ordinal, normalized_path""",
    )
    expansion_scopes = _group_ordered_items(
        connection,
        """SELECT package_id, normalized_path FROM package_scopes
        WHERE scope_kind='expansion' ORDER BY package_id, ordinal, normalized_path""",
    )
    packages: dict[str, CapabilityPackage] = {}
    for row in connection.execute(
        """
        SELECT id, capability_key, title, mathematical_objective, status, disposition,
            base_revision, branch, parent_package_id, plan_revision,
            integrated_revision, revision, created_at, updated_at
        FROM capability_packages ORDER BY created_at, id
        """
    ):
        package_id = str(row[0])
        key = str(row[1])
        packages[package_id] = CapabilityPackage(
            id=package_id,
            capability_key=key,
            title=str(row[2]),
            mathematical_objective=str(row[3]),
            status=PackageStatus(str(row[4])),
            disposition=PackageDisposition(str(row[5])) if row[5] is not None else None,
            aliases=tuple(value for value in aliases.get(package_id, ()) if value != key),
            textbook_refs=textbook_refs.get(package_id, ()),
            write_scope=write_scopes.get(package_id, ()),
            expansion_scope=expansion_scopes.get(package_id, ()),
            base_revision=str(row[6]),
            branch=str(row[7]),
            parent_package_id=str(row[8]) if row[8] is not None else None,
            plan_revision=int(row[9]),
            integrated_revision=str(row[10]) if row[10] is not None else None,
            revision=int(row[11]),
            created_at=str(row[12]),
            updated_at=str(row[13]),
        )

    blockers = _group_ordered_items(
        connection,
        """SELECT consumer_id, blocker_id FROM package_consumer_blockers
        ORDER BY consumer_id, ordinal, blocker_id""",
    )
    routes = _group_ordered_items(
        connection,
        """SELECT consumer_id, attempted_route FROM package_consumer_routes
        ORDER BY consumer_id, ordinal, attempted_route""",
    )
    consumers: dict[str, PackageConsumer] = {}
    for row in connection.execute(
        """
        SELECT id, package_id, work_unit_id, path, declaration, stage, residual_goal,
            source_digest, acceptance_contract, status, accepted_revision,
            detached_package_id, created_at, updated_at
        FROM package_consumers ORDER BY created_at, id
        """
    ):
        consumer_id = str(row[0])
        contract = json.loads(row[8])
        consumers[consumer_id] = PackageConsumer(
            id=consumer_id,
            package_id=str(row[1]),
            work_unit_id=str(row[2]),
            path=str(row[3]),
            declaration=str(row[4]),
            stage=str(row[5]),
            residual_goal=str(row[6]),
            source_digest=str(row[7]) if row[7] is not None else None,
            blocker_ids=blockers.get(consumer_id, ()),
            attempted_routes=routes.get(consumer_id, ()),
            acceptance_contract=contract if isinstance(contract, dict) else {},
            status=ConsumerStatus(str(row[9])),
            accepted_revision=str(row[10]) if row[10] is not None else None,
            detached_package_id=str(row[11]) if row[11] is not None else None,
            created_at=str(row[12]),
            updated_at=str(row[13]),
        )

    step_declarations = _group_package_step_items(
        connection,
        """SELECT package_id, step_id, item_value FROM package_step_items
        WHERE item_kind='declaration' ORDER BY package_id, step_id, ordinal, item_value""",
    )
    step_paths = _group_package_step_items(
        connection,
        """SELECT package_id, step_id, item_value FROM package_step_items
        WHERE item_kind='path' ORDER BY package_id, step_id, ordinal, item_value""",
    )
    step_commits = _group_package_step_items(
        connection,
        """SELECT package_id, step_id, item_value FROM package_step_items
        WHERE item_kind='commit' ORDER BY package_id, step_id, ordinal, item_value""",
    )
    step_dependencies = _group_package_step_items(
        connection,
        """SELECT package_id, step_id, depends_on_step_id FROM package_step_dependencies
        ORDER BY package_id, step_id, depends_on_step_id""",
    )
    steps: dict[str, PackageStep] = {}
    for row in connection.execute(
        """SELECT id, package_id, objective, kind, status, assigned_worker_id,
            validation_contract, remaining_gap, plan_revision, created_at, updated_at
        FROM package_steps ORDER BY created_at, id"""
    ):
        step_id = str(row[0])
        contract = json.loads(row[6])
        package_id = str(row[1])
        key = package_step_key(package_id, step_id)
        steps[key] = PackageStep(
            id=step_id,
            package_id=package_id,
            objective=str(row[2]),
            kind=PackageStepKind(str(row[3])),
            status=PackageStepStatus(str(row[4])),
            assigned_worker_id=str(row[5]) if row[5] is not None else None,
            validation_contract=contract if isinstance(contract, dict) else {},
            remaining_gap=str(row[7]),
            plan_revision=int(row[8]),
            intended_declarations=step_declarations.get(key, ()),
            intended_paths=step_paths.get(key, ()),
            depends_on_step_ids=step_dependencies.get(key, ()),
            commit_ids=step_commits.get(key, ()),
            created_at=str(row[9]),
            updated_at=str(row[10]),
        )

    evidence_paths = _group_ordered_items(
        connection,
        """SELECT evidence_id, item_value FROM package_evidence_items WHERE item_kind='path'
        ORDER BY evidence_id, ordinal, item_value""",
    )
    evidence_declarations = _group_ordered_items(
        connection,
        """SELECT evidence_id, item_value FROM package_evidence_items
        WHERE item_kind='declaration' ORDER BY evidence_id, ordinal, item_value""",
    )
    evidence: dict[str, PackageEvidence] = {}
    for row in connection.execute(
        """SELECT id, package_id, producer, kind, source_revision, payload, digest, created_at
        FROM package_evidence ORDER BY created_at, id"""
    ):
        evidence_id = str(row[0])
        payload = json.loads(row[5])
        evidence[evidence_id] = PackageEvidence(
            id=evidence_id,
            package_id=str(row[1]),
            producer=str(row[2]),
            kind=EvidenceKind(str(row[3])),
            source_revision=str(row[4]),
            payload=payload if isinstance(payload, dict) else {},
            digest=str(row[6]),
            created_at=str(row[7]),
            paths=evidence_paths.get(evidence_id, ()),
            declarations=evidence_declarations.get(evidence_id, ()),
        )

    leases = {
        str(row[0]): StewardLease(
            package_id=str(row[0]),
            agent_id=str(row[1]),
            generation=int(row[2]),
            acquired_at=str(row[3]),
            heartbeat_at=str(row[4]),
            expires_at=str(row[5]),
        )
        for row in connection.execute(
            "SELECT package_id, agent_id, generation, acquired_at, heartbeat_at, expires_at "
            "FROM steward_leases ORDER BY package_id"
        )
    }
    reservations = {
        str(row[0]): PathReservation(
            normalized_path=str(row[0]),
            mode=ReservationMode(str(row[1])),
            package_id=str(row[2]),
            lease_generation=int(row[3]),
            acquired_at=str(row[4]),
        )
        for row in connection.execute(
            """SELECT normalized_path, mode, owner_id, fence_generation, acquired_at
            FROM path_reservations WHERE owner_kind='package' ORDER BY normalized_path"""
        )
    }
    dependencies = tuple(
        PackageDependency(
            package_id=str(row[0]),
            depends_on_package_id=str(row[1]),
            required_revision=str(row[2]) if row[2] is not None else None,
            created_at=str(row[3]),
        )
        for row in connection.execute(
            """SELECT package_id, depends_on_package_id, required_revision, created_at
            FROM package_dependencies ORDER BY package_id, depends_on_package_id"""
        )
    )
    read_interfaces = tuple(
        RelevantReadInterface(
            package_id=str(row[0]),
            interface_id=str(row[1]),
            digest=str(row[2]),
            source_revision=str(row[3]),
        )
        for row in connection.execute(
            """SELECT package_id, interface_id, digest, source_revision
            FROM package_read_interfaces ORDER BY package_id, interface_id"""
        )
    )
    journals = {
        str(row[0]): IntegrationJournal(
            id=str(row[0]),
            package_id=str(row[1]),
            lease_generation=int(row[2]),
            base_revision=str(row[3]),
            candidate_revision=str(row[4]),
            canonical_revision_before=str(row[5]),
            phase=IntegrationPhase(str(row[6])),
            validation_digest=str(row[7]),
            canonical_revision_after=str(row[8]) if row[8] is not None else None,
            provisional_consumer_ids=tuple(str(value) for value in json.loads(row[9])),
            created_at=str(row[10]),
            updated_at=str(row[11]),
        )
        for row in connection.execute(
            """SELECT id, package_id, lease_generation, base_revision,
                candidate_revision, canonical_revision_before, phase, validation_digest,
                canonical_revision_after, provisional_consumer_ids, created_at, updated_at
            FROM integration_journal ORDER BY created_at, id"""
        )
    }
    return PackageState(
        packages=packages,
        consumers=consumers,
        steps=steps,
        evidence=evidence,
        leases=leases,
        reservations=reservations,
        dependencies=dependencies,
        relevant_read_interfaces=read_interfaces,
        integration_journal=journals,
    )


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
            elif version in {2, 3, 4, 5, 6, 7, 8}:
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


def _upsert_package_step_rows(connection: sqlite3.Connection, step: PackageStep) -> None:
    now = _utc_now()
    connection.execute(
        """INSERT INTO package_steps(
            package_id, id, objective, kind, status, assigned_worker_id,
            validation_contract, remaining_gap, plan_revision, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(package_id, id) DO UPDATE SET
            objective=excluded.objective, kind=excluded.kind, status=excluded.status,
            assigned_worker_id=excluded.assigned_worker_id,
            validation_contract=excluded.validation_contract,
            remaining_gap=excluded.remaining_gap,
            plan_revision=excluded.plan_revision, updated_at=excluded.updated_at""",
        (
            step.package_id,
            step.id,
            step.objective,
            str(step.kind),
            str(step.status),
            step.assigned_worker_id,
            json.dumpb(step.validation_contract),
            step.remaining_gap,
            step.plan_revision,
            step.created_at or now,
            step.updated_at or now,
        ),
    )
    connection.execute(
        "DELETE FROM package_step_items WHERE package_id=? AND step_id=?",
        (step.package_id, step.id),
    )
    connection.execute(
        "DELETE FROM package_step_dependencies WHERE package_id=? AND step_id=?",
        (step.package_id, step.id),
    )
    for kind, values in (
        ("declaration", step.intended_declarations),
        ("path", step.intended_paths),
        ("commit", step.commit_ids),
    ):
        connection.executemany(
            "INSERT INTO package_step_items VALUES(?, ?, ?, ?, ?)",
            (
                (step.package_id, step.id, kind, value, ordinal)
                for ordinal, value in enumerate(values)
            ),
        )
    connection.executemany(
        "INSERT INTO package_step_dependencies VALUES(?, ?, ?)",
        ((step.package_id, step.id, value) for value in step.depends_on_step_ids),
    )


class StateDatabase:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / DATABASE_NAME

    def initialize(self) -> None:
        initialize_database(self.state_dir)

    def connect_writer(self) -> sqlite3.Connection:
        """Return the long-lived connection owned by a StateWriter thread."""

        return _connect(self.path)

    def record_interface_invalidation(
        self,
        *,
        occurred_at: str,
        work_unit_id: str,
        source_file: str,
        old_digest: str | None,
        new_digest: str | None,
        invalidated_work_unit_ids: tuple[str, ...],
    ) -> None:
        """Append analysis-only provenance for one changed file interface."""

        with _connect(self.path) as connection, connection:
            connection.execute(
                """
                INSERT INTO interface_invalidation_events(
                    occurred_at, work_unit_id, source_file, old_digest, new_digest,
                    invalidated_work_unit_ids
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    occurred_at,
                    work_unit_id,
                    source_file,
                    old_digest,
                    new_digest,
                    json.dumpb(list(invalidated_work_unit_ids)),
                ),
            )

    def load_package_state(self) -> PackageState:
        """Load the authoritative normalized capability-package aggregate."""

        with _connect(self.path) as connection:
            return _load_package_state(connection)

    def import_legacy_upstream_state(self, requests: dict[str, dict[str, Any]]) -> dict[str, str]:
        """Import legacy requests once; later legacy mutations are deliberately ignored."""

        with _connect(self.path) as connection, connection:
            return _import_legacy_request_mapping(connection, requests)

    @staticmethod
    def _assert_package_revision(
        connection: sqlite3.Connection, package_id: str, expected_revision: int
    ) -> None:
        row = connection.execute(
            "SELECT revision FROM capability_packages WHERE id=?", (package_id,)
        ).fetchone()
        if row is None:
            raise KeyError(package_id)
        if int(row[0]) != expected_revision:
            raise ValueError(
                f"stale package revision for {package_id}: "
                f"expected {expected_revision}, found {int(row[0])}"
            )

    @staticmethod
    def _assert_lease_generation(
        connection: sqlite3.Connection, package_id: str, generation: int
    ) -> None:
        row = connection.execute(
            "SELECT generation, expires_at FROM steward_leases WHERE package_id=?", (package_id,)
        ).fetchone()
        if row is None or int(row[0]) != generation:
            actual = int(row[0]) if row is not None else None
            raise ValueError(
                f"stale lease generation for {package_id}: expected {generation}, found {actual}"
            )
        if _as_utc(str(row[1])) <= datetime.now(UTC):
            raise ValueError(f"steward lease for {package_id} has expired")

    @staticmethod
    def _touch_package(connection: sqlite3.Connection, package_id: str) -> None:
        connection.execute(
            "UPDATE capability_packages SET revision=revision+1, updated_at=? WHERE id=?",
            (_utc_now(), package_id),
        )

    def create_or_attach_capability_package(
        self,
        package: CapabilityPackage,
        *,
        consumer: PackageConsumer | None = None,
        evidence: tuple[PackageEvidence, ...] = (),
        expected_revision: int | None = None,
    ) -> tuple[CapabilityPackage, bool]:
        """Create one capability owner or attach records to its existing key/alias."""

        with _connect(self.path) as connection, connection:
            package_id = _package_id_for_key(connection, package.capability_key)
            created = package_id is None
            now = _utc_now()
            if package_id is None:
                package_id = package.id
                _insert_package(
                    connection,
                    replace(
                        package,
                        created_at=package.created_at or now,
                        updated_at=package.updated_at or now,
                    ),
                )
            elif expected_revision is not None:
                self._assert_package_revision(connection, package_id, expected_revision)
            changed = created
            if consumer is not None:
                attached = replace(
                    consumer,
                    package_id=package_id,
                    created_at=consumer.created_at or now,
                    updated_at=consumer.updated_at or now,
                )
                prior = connection.execute(
                    """SELECT id FROM package_consumers WHERE package_id=? AND work_unit_id=?
                    AND path=? AND declaration=? AND stage=?""",
                    (
                        package_id,
                        attached.work_unit_id,
                        attached.path,
                        attached.declaration,
                        attached.stage,
                    ),
                ).fetchone()
                _insert_consumer(connection, attached)
                if prior is None and not created:
                    current_status = PackageStatus(
                        str(
                            connection.execute(
                                "SELECT status FROM capability_packages WHERE id=?", (package_id,)
                            ).fetchone()[0]
                        )
                    )
                    if current_status in {
                        PackageStatus.COMPLETE,
                        PackageStatus.DECOMPOSED,
                        PackageStatus.EXTERNAL,
                    }:
                        # New current evidence invalidates a terminal root's claim that all
                        # consumers have been classified.  Reopen the same capability owner
                        # instead of stranding an open consumer on an unschedulable package.
                        connection.execute(
                            """UPDATE capability_packages SET status=?, disposition=NULL
                            WHERE id=?""",
                            (str(PackageStatus.OBSERVED), package_id),
                        )
                changed = changed or prior is None
            for item in evidence:
                evidence_id = item.id
                if (
                    connection.execute(
                        "SELECT 1 FROM package_evidence WHERE id=?", (evidence_id,)
                    ).fetchone()
                    is not None
                ):
                    continue
                _insert_evidence(
                    connection,
                    replace(
                        item,
                        package_id=package_id,
                        created_at=item.created_at or now,
                    ),
                )
                changed = True
            if changed and not created:
                self._touch_package(connection, package_id)
            result = _load_package_state(connection).packages[package_id]
        return result, created

    def attach_package_consumer(
        self,
        package_id: str,
        consumer: PackageConsumer,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> PackageConsumer:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, package_id, lease_generation)
            attached = replace(
                consumer,
                package_id=package_id,
                created_at=consumer.created_at or _utc_now(),
                updated_at=consumer.updated_at or _utc_now(),
            )
            consumer_id = _insert_consumer(connection, attached)
            self._touch_package(connection, package_id)
            return _load_package_state(connection).consumers[consumer_id]

    def update_package_consumer(
        self,
        consumer: PackageConsumer,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> PackageConsumer:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, consumer.package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, consumer.package_id, lease_generation)
            cursor = connection.execute(
                """UPDATE package_consumers SET residual_goal=?, source_digest=?,
                    acceptance_contract=?, status=?, accepted_revision=?,
                    detached_package_id=?, updated_at=?
                WHERE id=? AND package_id=?""",
                (
                    consumer.residual_goal,
                    consumer.source_digest,
                    json.dumpb(consumer.acceptance_contract),
                    str(consumer.status),
                    consumer.accepted_revision,
                    consumer.detached_package_id,
                    consumer.updated_at or _utc_now(),
                    consumer.id,
                    consumer.package_id,
                ),
            )
            if cursor.rowcount != 1:
                raise KeyError(consumer.id)
            connection.execute(
                "DELETE FROM package_consumer_blockers WHERE consumer_id=?", (consumer.id,)
            )
            connection.execute(
                "DELETE FROM package_consumer_routes WHERE consumer_id=?", (consumer.id,)
            )
            connection.executemany(
                "INSERT INTO package_consumer_blockers VALUES(?, ?, ?)",
                (
                    (consumer.id, value, ordinal)
                    for ordinal, value in enumerate(consumer.blocker_ids)
                ),
            )
            connection.executemany(
                "INSERT INTO package_consumer_routes VALUES(?, ?, ?)",
                (
                    (consumer.id, value, ordinal)
                    for ordinal, value in enumerate(consumer.attempted_routes)
                ),
            )
            self._touch_package(connection, consumer.package_id)
            return _load_package_state(connection).consumers[consumer.id]

    def update_package_lifecycle(
        self,
        package_id: str,
        status: PackageStatus,
        *,
        expected_revision: int,
        disposition: PackageDisposition | None = None,
        plan_revision: int | None = None,
        integrated_revision: str | None = None,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, package_id, lease_generation)
            current = connection.execute(
                "SELECT plan_revision, integrated_revision FROM capability_packages WHERE id=?",
                (package_id,),
            ).fetchone()
            assert current is not None
            connection.execute(
                """UPDATE capability_packages SET status=?, disposition=?, plan_revision=?,
                    integrated_revision=?, revision=revision+1, updated_at=? WHERE id=?""",
                (
                    str(status),
                    str(disposition) if disposition is not None else None,
                    int(current[0]) if plan_revision is None else plan_revision,
                    current[1] if integrated_revision is None else integrated_revision,
                    _utc_now(),
                    package_id,
                ),
            )
            return _load_package_state(connection).packages[package_id]

    def park_capability_package(self, package_id: str, *, reason: str) -> CapabilityPackage:
        """Fence active work, release paths, and park a package by operator decision."""

        reason = reason.strip()
        if not reason:
            raise ValueError("parking a package requires a reason")
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = _load_package_state(connection)
            package = state.packages.get(package_id)
            if package is None:
                raise KeyError(package_id)
            if package.status in {
                PackageStatus.COMPLETE,
                PackageStatus.DECOMPOSED,
                PackageStatus.EXTERNAL,
                PackageStatus.SUPERSEDED,
            }:
                raise ValueError(f"cannot park terminal package {package_id}")
            lease = state.leases.get(package_id)
            fence_generation = (lease.generation if lease is not None else 0) + 1
            connection.execute("DELETE FROM steward_leases WHERE package_id=?", (package_id,))
            connection.execute(
                """INSERT INTO steward_lease_fences VALUES(?, ?)
                ON CONFLICT(package_id) DO UPDATE SET
                    generation=max(generation, excluded.generation)""",
                (package_id, fence_generation),
            )
            connection.execute(
                "DELETE FROM path_reservations WHERE owner_kind='package' AND owner_id=?",
                (package_id,),
            )
            connection.execute(
                "DELETE FROM path_reservation_queue WHERE owner_kind='package' AND owner_id=?",
                (package_id,),
            )
            now = _utc_now()
            evidence = PackageEvidence(
                id=_stable_record_id("operator-park", package_id, now),
                package_id=package_id,
                producer="operator",
                kind=EvidenceKind.OPERATOR_DECISION,
                payload={"action": "park", "reason": reason},
                digest=_content_digest({"action": "park", "reason": reason}),
                created_at=now,
            )
            _insert_evidence(connection, evidence)
            connection.execute(
                """UPDATE capability_packages SET status=?, disposition=?, revision=revision+1,
                    updated_at=? WHERE id=?""",
                (str(PackageStatus.PARKED), str(PackageDisposition.PARKED), now, package_id),
            )
            self._wake_waiting_reservation_packages(connection, now)
            connection.commit()
        return self.load_package_state().packages[package_id]

    def resume_capability_package(self, package_id: str, *, reason: str = "") -> CapabilityPackage:
        """Return an operator-parked package to the runnable lifecycle."""

        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = _load_package_state(connection)
            package = state.packages.get(package_id)
            if package is None:
                raise KeyError(package_id)
            if package.status is not PackageStatus.PARKED:
                raise ValueError(f"package {package_id} is not parked")
            if package_id in state.leases:
                raise ValueError(f"parked package {package_id} unexpectedly has a Steward lease")
            now = _utc_now()
            target = PackageStatus.PLANNED if package.plan_revision else PackageStatus.OBSERVED
            payload = {"action": "resume", "reason": reason.strip()}
            _insert_evidence(
                connection,
                PackageEvidence(
                    id=_stable_record_id("operator-resume", package_id, now),
                    package_id=package_id,
                    producer="operator",
                    kind=EvidenceKind.OPERATOR_DECISION,
                    payload=payload,
                    digest=_content_digest(payload),
                    created_at=now,
                ),
            )
            connection.execute(
                """UPDATE capability_packages SET status=?, disposition=NULL,
                    revision=revision+1, updated_at=? WHERE id=?""",
                (str(target), now, package_id),
            )
            connection.commit()
        return self.load_package_state().packages[package_id]

    def add_capability_alias(
        self,
        package_id: str,
        alias: str,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        normalized = normalize_capability_key(alias)
        if not normalized:
            raise ValueError("capability alias must not be empty")
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, package_id, lease_generation)
            owner = _package_id_for_key(connection, normalized)
            if owner is not None and owner != package_id:
                raise ValueError(f"capability alias is owned by package {owner}")
            connection.execute(
                "INSERT OR IGNORE INTO capability_aliases VALUES(?, ?)",
                (normalized, package_id),
            )
            self._touch_package(connection, package_id)
            return _load_package_state(connection).packages[package_id]

    def append_package_evidence(
        self,
        evidence: PackageEvidence,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, evidence.package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, evidence.package_id, lease_generation)
            _insert_evidence(
                connection,
                replace(evidence, created_at=evidence.created_at or _utc_now()),
            )
            self._touch_package(connection, evidence.package_id)
            return _load_package_state(connection).packages[evidence.package_id]

    def upsert_package_step(
        self,
        step: PackageStep,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, step.package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, step.package_id, lease_generation)
            for dependency_id in step.depends_on_step_ids:
                row = connection.execute(
                    "SELECT 1 FROM package_steps WHERE package_id=? AND id=?",
                    (step.package_id, dependency_id),
                ).fetchone()
                if row is None:
                    raise ValueError(
                        f"step dependency {dependency_id} does not belong to {step.package_id}"
                    )
            _upsert_package_step_rows(connection, step)
            step_graph: dict[str, set[str]] = {
                str(row[0]): set()
                for row in connection.execute(
                    "SELECT id FROM package_steps WHERE package_id=?", (step.package_id,)
                )
            }
            for step_id, depends_on in connection.execute(
                """SELECT step_id, depends_on_step_id FROM package_step_dependencies
                WHERE package_id=?""",
                (step.package_id,),
            ):
                step_graph.setdefault(str(step_id), set()).add(str(depends_on))
            self._assert_dependency_dag(step_graph)
            self._touch_package(connection, step.package_id)
            return _load_package_state(connection).packages[step.package_id]

    def replace_package_plan(
        self,
        package_id: str,
        steps: tuple[PackageStep, ...],
        *,
        plan_revision: int,
        expected_revision: int,
        lease_generation: int,
    ) -> CapabilityPackage:
        """Atomically publish one complete revision of a package plan."""

        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_package_revision(connection, package_id, expected_revision)
            self._assert_lease_generation(connection, package_id, lease_generation)
            if any(step.package_id != package_id for step in steps):
                raise ValueError("replacement plan contains a step from another package")
            ids = {step.id for step in steps}
            if len(ids) != len(steps):
                raise ValueError("replacement plan repeats a step id")
            existing_complete = {
                str(row[0])
                for row in connection.execute(
                    "SELECT id FROM package_steps WHERE package_id=? AND status=?",
                    (package_id, str(PackageStepStatus.COMPLETE)),
                )
            }
            known = ids | existing_complete
            if any(not set(step.depends_on_step_ids).issubset(known) for step in steps):
                raise ValueError("replacement plan contains an unknown dependency")
            for step in steps:
                _upsert_package_step_rows(connection, step)
            if ids:
                placeholders = ",".join("?" for _ in ids)
                connection.execute(
                    f"""UPDATE package_steps SET status=?, assigned_worker_id=NULL,
                        plan_revision=?, updated_at=? WHERE package_id=?
                        AND id NOT IN ({placeholders}) AND status NOT IN (?, ?)""",
                    (
                        str(PackageStepStatus.SUPERSEDED),
                        plan_revision,
                        _utc_now(),
                        package_id,
                        *sorted(ids),
                        str(PackageStepStatus.COMPLETE),
                        str(PackageStepStatus.SUPERSEDED),
                    ),
                )
            else:
                connection.execute(
                    """UPDATE package_steps SET status=?, assigned_worker_id=NULL,
                        plan_revision=?, updated_at=? WHERE package_id=?
                        AND status NOT IN (?, ?)""",
                    (
                        str(PackageStepStatus.SUPERSEDED),
                        plan_revision,
                        _utc_now(),
                        package_id,
                        str(PackageStepStatus.COMPLETE),
                        str(PackageStepStatus.SUPERSEDED),
                    ),
                )
            graph = {step.id: set(step.depends_on_step_ids) for step in steps}
            self._assert_dependency_dag(graph)
            now = _utc_now()
            connection.execute(
                """UPDATE capability_packages SET status=?, plan_revision=?,
                    revision=revision+1, updated_at=? WHERE id=?""",
                (str(PackageStatus.PLANNED), plan_revision, now, package_id),
            )
            connection.commit()
        return self.load_package_state().packages[package_id]

    def put_steward_lease(
        self, lease: StewardLease, *, expected_revision: int
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, lease.package_id, expected_revision)
            row = connection.execute(
                "SELECT agent_id, generation FROM steward_leases WHERE package_id=?",
                (lease.package_id,),
            ).fetchone()
            if row is not None and (
                lease.generation < int(row[1])
                or (lease.agent_id != str(row[0]) and lease.generation <= int(row[1]))
            ):
                raise ValueError("a replacement steward must use a newer lease generation")
            connection.execute(
                """INSERT INTO steward_leases VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(package_id) DO UPDATE SET agent_id=excluded.agent_id,
                    generation=excluded.generation, acquired_at=excluded.acquired_at,
                    heartbeat_at=excluded.heartbeat_at, expires_at=excluded.expires_at""",
                (
                    lease.package_id,
                    lease.agent_id,
                    lease.generation,
                    lease.acquired_at,
                    lease.heartbeat_at,
                    lease.expires_at,
                ),
            )
            connection.execute(
                """INSERT INTO steward_lease_fences VALUES(?, ?)
                ON CONFLICT(package_id) DO UPDATE SET
                    generation=max(generation, excluded.generation)""",
                (lease.package_id, lease.generation),
            )
            connection.execute(
                """UPDATE path_reservations SET fence_generation=?
                WHERE owner_kind='package' AND owner_id=?""",
                (lease.generation, lease.package_id),
            )
            self._refence_package_reservation_queue(connection, lease.package_id, lease.generation)
            self._touch_package(connection, lease.package_id)
            return _load_package_state(connection).packages[lease.package_id]

    @staticmethod
    def _assert_live_lease(
        connection: sqlite3.Connection,
        package_id: str,
        generation: int,
        *,
        agent_id: str | None = None,
        now: str | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT agent_id, generation, expires_at FROM steward_leases WHERE package_id=?",
            (package_id,),
        ).fetchone()
        if row is None or int(row[1]) != generation:
            actual = int(row[1]) if row is not None else None
            raise ValueError(
                f"stale lease generation for {package_id}: expected {generation}, found {actual}"
            )
        if agent_id is not None and str(row[0]) != agent_id:
            raise ValueError(f"steward lease for {package_id} belongs to another agent")
        if _as_utc(str(row[2])) <= _as_utc(now):
            raise ValueError(f"steward lease for {package_id} has expired")

    def claim_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        *,
        expected_revision: int,
        ttl_seconds: float,
        now: str | None = None,
    ) -> StewardLease:
        claimed_at = now or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_package_revision(connection, package_id, expected_revision)
            row = connection.execute(
                "SELECT agent_id, generation, acquired_at, expires_at FROM steward_leases "
                "WHERE package_id=?",
                (package_id,),
            ).fetchone()
            fence = connection.execute(
                "SELECT generation FROM steward_lease_fences WHERE package_id=?", (package_id,)
            ).fetchone()
            last_generation = max(
                int(row[1]) if row is not None else 0,
                int(fence[0]) if fence is not None else 0,
            )
            if row is not None and _as_utc(str(row[3])) > _as_utc(claimed_at):
                if str(row[0]) != agent_id:
                    raise ValueError(f"package {package_id} already has a live steward")
                generation = int(row[1])
                acquired_at = str(row[2])
            else:
                generation = last_generation + 1
                acquired_at = claimed_at
            lease = StewardLease(
                package_id,
                agent_id,
                generation,
                acquired_at,
                claimed_at,
                _expires(claimed_at, ttl_seconds),
            )
            connection.execute(
                """INSERT INTO steward_leases VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(package_id) DO UPDATE SET agent_id=excluded.agent_id,
                    generation=excluded.generation, acquired_at=excluded.acquired_at,
                    heartbeat_at=excluded.heartbeat_at, expires_at=excluded.expires_at""",
                (
                    lease.package_id,
                    lease.agent_id,
                    lease.generation,
                    lease.acquired_at,
                    lease.heartbeat_at,
                    lease.expires_at,
                ),
            )
            connection.execute(
                """INSERT INTO steward_lease_fences VALUES(?, ?)
                ON CONFLICT(package_id) DO UPDATE SET generation=excluded.generation""",
                (package_id, generation),
            )
            connection.execute(
                """UPDATE path_reservations SET fence_generation=?
                WHERE owner_kind='package' AND owner_id=?""",
                (generation, package_id),
            )
            self._refence_package_reservation_queue(connection, package_id, generation)
            self._touch_package(connection, package_id)
            connection.commit()
            return lease

    def heartbeat_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        generation: int,
        *,
        ttl_seconds: float,
        now: str | None = None,
    ) -> StewardLease:
        heartbeat_at = now or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_live_lease(
                connection, package_id, generation, agent_id=agent_id, now=heartbeat_at
            )
            expires_at = _expires(heartbeat_at, ttl_seconds)
            connection.execute(
                "UPDATE steward_leases SET heartbeat_at=?, expires_at=? WHERE package_id=?",
                (heartbeat_at, expires_at, package_id),
            )
            row = connection.execute(
                """SELECT package_id, agent_id, generation, acquired_at, heartbeat_at, expires_at
                FROM steward_leases WHERE package_id=?""",
                (package_id,),
            ).fetchone()
            connection.commit()
            assert row is not None
            return StewardLease(str(row[0]), str(row[1]), int(row[2]), *map(str, row[3:]))

    def assert_live_steward_lease(
        self,
        package_id: str,
        generation: int,
        *,
        agent_id: str | None = None,
        now: str | None = None,
    ) -> None:
        with _connect(self.path) as connection:
            self._assert_live_lease(connection, package_id, generation, agent_id=agent_id, now=now)

    def release_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        generation: int,
        *,
        release_reservations: bool = False,
        now: str | None = None,
    ) -> None:
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_live_lease(connection, package_id, generation, agent_id=agent_id, now=now)
            connection.execute("DELETE FROM steward_leases WHERE package_id=?", (package_id,))
            if release_reservations:
                connection.execute(
                    "DELETE FROM path_reservations WHERE owner_kind='package' AND owner_id=?",
                    (package_id,),
                )
                self._wake_waiting_reservation_packages(connection, now or _utc_now())
            self._touch_package(connection, package_id)
            connection.commit()

    def recover_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        *,
        expected_revision: int,
        ttl_seconds: float,
        candidate_revision: str,
        candidate_digest: str,
        active_child_workers: tuple[str, ...] = (),
        journal_phase: IntegrationPhase | None = None,
        now: str | None = None,
    ) -> tuple[StewardLease, PackageRecovery]:
        recovered_at = now or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_package_revision(connection, package_id, expected_revision)
            row = connection.execute(
                "SELECT generation, expires_at FROM steward_leases WHERE package_id=?",
                (package_id,),
            ).fetchone()
            if row is not None and _as_utc(str(row[1])) > _as_utc(recovered_at):
                raise ValueError(f"cannot recover package {package_id} with a live steward")
            fence = connection.execute(
                "SELECT generation FROM steward_lease_fences WHERE package_id=?", (package_id,)
            ).fetchone()
            prior_generation = max(
                int(row[0]) if row is not None else 0,
                int(fence[0]) if fence is not None else 0,
            )
            generation = prior_generation + 1
            lease = StewardLease(
                package_id,
                agent_id,
                generation,
                recovered_at,
                recovered_at,
                _expires(recovered_at, ttl_seconds),
            )
            connection.execute(
                """INSERT INTO steward_leases VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(package_id) DO UPDATE SET agent_id=excluded.agent_id,
                    generation=excluded.generation, acquired_at=excluded.acquired_at,
                    heartbeat_at=excluded.heartbeat_at, expires_at=excluded.expires_at""",
                (
                    lease.package_id,
                    lease.agent_id,
                    lease.generation,
                    lease.acquired_at,
                    lease.heartbeat_at,
                    lease.expires_at,
                ),
            )
            connection.execute(
                """INSERT INTO steward_lease_fences VALUES(?, ?)
                ON CONFLICT(package_id) DO UPDATE SET generation=excluded.generation""",
                (package_id, generation),
            )
            connection.execute(
                """UPDATE path_reservations SET fence_generation=?
                WHERE owner_kind='package' AND owner_id=?""",
                (generation, package_id),
            )
            self._refence_package_reservation_queue(connection, package_id, generation)
            connection.execute(
                """INSERT INTO package_recoveries(
                    package_id, prior_generation, recovered_generation, candidate_revision,
                    candidate_digest, active_child_workers, journal_phase, recovered_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    package_id,
                    prior_generation,
                    generation,
                    candidate_revision,
                    candidate_digest,
                    json.dumpb(list(active_child_workers)),
                    str(journal_phase) if journal_phase is not None else None,
                    recovered_at,
                ),
            )
            self._touch_package(connection, package_id)
            connection.commit()
        recovery = PackageRecovery(
            package_id,
            prior_generation,
            generation,
            candidate_revision,
            candidate_digest,
            active_child_workers,
            journal_phase,
            recovered_at,
        )
        return lease, recovery

    def update_package_candidate(
        self,
        package_id: str,
        *,
        expected_revision: int,
        lease_generation: int,
        base_revision: str,
        branch: str,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, package_id, expected_revision)
            self._assert_lease_generation(connection, package_id, lease_generation)
            connection.execute(
                """UPDATE capability_packages SET base_revision=?, branch=?,
                    revision=revision+1, updated_at=? WHERE id=?""",
                (base_revision, branch, _utc_now(), package_id),
            )
            return _load_package_state(connection).packages[package_id]

    @staticmethod
    def _reservation_conflicts(
        left_path: str, left_mode: str, right_path: str, right_mode: str
    ) -> bool:
        if left_path == right_path:
            return True
        left_prefix = f"{left_path.rstrip('/')}/"
        right_prefix = f"{right_path.rstrip('/')}/"
        return (
            left_mode == ReservationMode.EXCLUSIVE_SUBTREE and right_path.startswith(left_prefix)
        ) or (
            right_mode == ReservationMode.EXCLUSIVE_SUBTREE and left_path.startswith(right_prefix)
        )

    def reserve_package_paths(
        self,
        reservations: tuple[PathReservation, ...],
        *,
        expected_revision: int,
    ) -> CapabilityPackage:
        if not reservations:
            raise ValueError("at least one path reservation is required")
        package_ids = {item.package_id for item in reservations}
        generations = {item.lease_generation for item in reservations}
        if len(package_ids) != 1 or len(generations) != 1:
            raise ValueError("one atomic reservation set must share a package and generation")
        package_id = next(iter(package_ids))
        generation = next(iter(generations))
        result = self.acquire_path_reservations(
            ReservationOwnerKind.PACKAGE,
            package_id,
            generation,
            tuple(ReservationSpec(item.normalized_path, item.mode) for item in reservations),
            acquired_at=min(item.acquired_at for item in reservations),
            expected_revision=expected_revision,
        )
        if not result.granted:
            conflict = result.conflicts[0]
            raise ValueError(
                f"path {conflict.requested_path} conflicts with "
                f"{conflict.owner_kind} {conflict.owner_id}"
            )
        return self.load_package_state().packages[package_id]

    def expand_package_write_scope(
        self,
        package_id: str,
        lease_generation: int,
        requested: tuple[ReservationSpec, ...],
        *,
        expected_revision: int,
        acquired_at: str | None = None,
        queue_on_conflict: bool = True,
    ) -> ReservationResult:
        """Atomically grant a bounded scope expansion and its path reservations."""

        requested = canonical_reservation_specs(requested)
        if not requested:
            raise ValueError("at least one scope expansion path is required")
        now = acquired_at or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_package_revision(connection, package_id, expected_revision)
            self._assert_live_lease(connection, package_id, lease_generation)
            allowed = tuple(
                str(row[0])
                for row in connection.execute(
                    """SELECT normalized_path FROM package_scopes
                    WHERE package_id=? AND scope_kind='expansion'""",
                    (package_id,),
                )
            )
            invalid = tuple(
                item.normalized_path
                for item in requested
                if not any(
                    item.normalized_path == root
                    or item.normalized_path.startswith(f"{root.rstrip('/')}/")
                    for root in allowed
                )
            )
            if invalid:
                raise ValueError(
                    "scope expansion is outside the configured expansion scope: "
                    + ", ".join(invalid)
                )
            conflicts = self._reservation_conflict_rows(
                connection, ReservationOwnerKind.PACKAGE, package_id, requested
            )
            if conflicts:
                queue_id = None
                decision = ReservationDecision.CONFLICT
                if queue_on_conflict:
                    decision = ReservationDecision.QUEUED
                    queue_id = _stable_record_id("reservation", "package", package_id)
                    connection.execute(
                        """INSERT INTO path_reservation_queue VALUES(?, 'package', ?, ?, ?, ?, NULL)
                        ON CONFLICT(id) DO UPDATE SET
                            fence_generation=excluded.fence_generation,
                            requested=excluded.requested, created_at=excluded.created_at""",
                        (
                            queue_id,
                            package_id,
                            lease_generation,
                            json.dumpb(
                                [
                                    {"path": item.normalized_path, "mode": str(item.mode)}
                                    for item in requested
                                ]
                            ),
                            now,
                        ),
                    )
                connection.commit()
                return ReservationResult(
                    decision,
                    ReservationOwnerKind.PACKAGE,
                    package_id,
                    lease_generation,
                    requested,
                    conflicts,
                    queue_id,
                )
            connection.executemany(
                """INSERT INTO path_reservations(
                    normalized_path, mode, owner_kind, owner_id, fence_generation,
                    acquired_at, expires_at, package_id
                ) VALUES(?, ?, 'package', ?, ?, ?, NULL, ?)
                ON CONFLICT(normalized_path) DO UPDATE SET mode=excluded.mode,
                    fence_generation=excluded.fence_generation,
                    acquired_at=excluded.acquired_at
                WHERE path_reservations.owner_kind='package'
                    AND path_reservations.owner_id=excluded.owner_id""",
                (
                    (
                        item.normalized_path,
                        str(item.mode),
                        package_id,
                        lease_generation,
                        now,
                        package_id,
                    )
                    for item in requested
                ),
            )
            next_ordinal = int(
                connection.execute(
                    """SELECT coalesce(max(ordinal), -1) + 1 FROM package_scopes
                    WHERE package_id=? AND scope_kind='write'""",
                    (package_id,),
                ).fetchone()[0]
            )
            connection.executemany(
                "INSERT OR IGNORE INTO package_scopes VALUES(?, 'write', ?, ?)",
                (
                    (package_id, item.normalized_path, next_ordinal + index)
                    for index, item in enumerate(requested)
                ),
            )
            connection.execute(
                "DELETE FROM path_reservation_queue WHERE owner_kind='package' AND owner_id=?",
                (package_id,),
            )
            self._touch_package(connection, package_id)
            connection.commit()
        return ReservationResult(
            ReservationDecision.GRANTED,
            ReservationOwnerKind.PACKAGE,
            package_id,
            lease_generation,
            requested,
        )

    @staticmethod
    def _clean_expired_ordinary_reservations(connection: sqlite3.Connection, now: str) -> None:
        connection.execute(
            """DELETE FROM path_reservations WHERE owner_kind='ordinary_task'
            AND expires_at IS NOT NULL AND expires_at <= ?""",
            (now,),
        )
        connection.execute("DELETE FROM ordinary_reservation_leases WHERE expires_at <= ?", (now,))
        connection.execute(
            "DELETE FROM path_reservation_queue WHERE expires_at IS NOT NULL AND expires_at <= ?",
            (now,),
        )

    @staticmethod
    def _refence_package_reservation_queue(
        connection: sqlite3.Connection, package_id: str, generation: int
    ) -> None:
        rows = connection.execute(
            """SELECT requested, created_at FROM path_reservation_queue
            WHERE owner_kind='package' AND owner_id=? ORDER BY created_at DESC, id DESC""",
            (package_id,),
        ).fetchall()
        if not rows:
            return
        connection.execute(
            "DELETE FROM path_reservation_queue WHERE owner_kind='package' AND owner_id=?",
            (package_id,),
        )
        connection.execute(
            """INSERT INTO path_reservation_queue
            VALUES(?, 'package', ?, ?, ?, ?, NULL)""",
            (
                _stable_record_id("reservation", "package", package_id),
                package_id,
                generation,
                rows[0][0],
                str(rows[0][1]),
            ),
        )

    @classmethod
    def _wake_waiting_reservation_packages(
        cls, connection: sqlite3.Connection, now: str
    ) -> tuple[str, ...]:
        cls._clean_expired_ordinary_reservations(connection, now)
        waiting = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT id FROM capability_packages WHERE status=? ORDER BY created_at, id",
                (str(PackageStatus.WAITING_RESERVATION),),
            )
        )
        woken: list[str] = []
        for package_id in waiting:
            fence = connection.execute(
                "SELECT generation FROM steward_lease_fences WHERE package_id=?",
                (package_id,),
            ).fetchone()
            rows = connection.execute(
                """SELECT requested, created_at FROM path_reservation_queue
                WHERE owner_kind='package' AND owner_id=?
                ORDER BY created_at DESC, id DESC""",
                (package_id,),
            ).fetchall()
            if fence is None or not rows:
                continue
            generation = int(fence[0])
            cls._refence_package_reservation_queue(connection, package_id, generation)
            requested = canonical_reservation_specs(
                tuple(
                    ReservationSpec(str(item["path"]), ReservationMode(str(item["mode"])))
                    for item in json.loads(rows[0][0])
                )
            )
            if cls._reservation_conflict_rows(
                connection, ReservationOwnerKind.PACKAGE, package_id, requested
            ):
                continue
            connection.execute(
                "DELETE FROM path_reservation_queue WHERE owner_kind='package' AND owner_id=?",
                (package_id,),
            )
            connection.execute(
                """UPDATE capability_packages SET status=?, revision=revision+1,
                    updated_at=? WHERE id=? AND status=?""",
                (
                    str(PackageStatus.OBSERVED),
                    now,
                    package_id,
                    str(PackageStatus.WAITING_RESERVATION),
                ),
            )
            woken.append(package_id)
        return tuple(woken)

    def wake_waiting_reservation_packages(self, *, now: str | None = None) -> tuple[str, ...]:
        """Make conflict-free queued packages schedulable without granting an old fence."""

        observed_at = now or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            woken = self._wake_waiting_reservation_packages(connection, observed_at)
            connection.commit()
            return woken

    @classmethod
    def _reservation_conflict_rows(
        cls,
        connection: sqlite3.Connection,
        owner_kind: ReservationOwnerKind,
        owner_id: str,
        requested: tuple[ReservationSpec, ...],
    ) -> tuple[ReservationConflict, ...]:
        held = tuple(
            GlobalPathReservation(
                str(row[0]),
                ReservationMode(str(row[1])),
                ReservationOwnerKind(str(row[2])),
                str(row[3]),
                int(row[4]),
                str(row[5]),
                str(row[6]) if row[6] is not None else None,
            )
            for row in connection.execute(
                """SELECT normalized_path, mode, owner_kind, owner_id, fence_generation,
                    acquired_at, expires_at FROM path_reservations ORDER BY normalized_path"""
            )
            if (str(row[2]), str(row[3])) != (str(owner_kind), owner_id)
        )
        return tuple(
            ReservationConflict(
                item.normalized_path,
                item.mode,
                current.normalized_path,
                current.mode,
                current.owner_kind,
                current.owner_id,
            )
            for item in requested
            for current in held
            if cls._reservation_conflicts(
                item.normalized_path,
                str(item.mode),
                current.normalized_path,
                str(current.mode),
            )
        )

    def acquire_path_reservations(
        self,
        owner_kind: ReservationOwnerKind,
        owner_id: str,
        fence_generation: int,
        requested: tuple[ReservationSpec, ...],
        *,
        acquired_at: str | None = None,
        expires_at: str | None = None,
        expected_revision: int | None = None,
        queue_on_conflict: bool = False,
    ) -> ReservationResult:
        if not owner_id or fence_generation <= 0 or not requested:
            raise ValueError("reservation owner, positive fence, and paths are required")
        requested = canonical_reservation_specs(requested)
        for index, item in enumerate(requested):
            for other in requested[index + 1 :]:
                if self._reservation_conflicts(
                    item.normalized_path, str(item.mode), other.normalized_path, str(other.mode)
                ):
                    raise ValueError("requested reservation set overlaps itself")
        now = acquired_at or _utc_now()
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._clean_expired_ordinary_reservations(connection, now)
            if owner_kind is ReservationOwnerKind.PACKAGE:
                if expected_revision is None:
                    raise ValueError("package reservations require an expected revision")
                self._assert_package_revision(connection, owner_id, expected_revision)
                self._assert_lease_generation(connection, owner_id, fence_generation)
            else:
                row = connection.execute(
                    """SELECT generation, expires_at FROM ordinary_reservation_leases
                    WHERE owner_id=?""",
                    (owner_id,),
                ).fetchone()
                if row is None or int(row[0]) != fence_generation or str(row[1]) <= now:
                    raise ValueError(f"stale ordinary-task reservation fence for {owner_id}")
            conflicts = self._reservation_conflict_rows(connection, owner_kind, owner_id, requested)
            queue_id: str | None = None
            if conflicts:
                decision = (
                    ReservationDecision.QUEUED
                    if queue_on_conflict
                    else ReservationDecision.CONFLICT
                )
                if queue_on_conflict:
                    queue_id = _stable_record_id("reservation", str(owner_kind), owner_id)
                    connection.execute(
                        """INSERT INTO path_reservation_queue VALUES(?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            fence_generation=excluded.fence_generation,
                            requested=excluded.requested,
                            created_at=excluded.created_at, expires_at=excluded.expires_at""",
                        (
                            queue_id,
                            str(owner_kind),
                            owner_id,
                            fence_generation,
                            json.dumpb(
                                [
                                    {"path": item.normalized_path, "mode": str(item.mode)}
                                    for item in requested
                                ]
                            ),
                            now,
                            expires_at,
                        ),
                    )
                connection.commit()
                return ReservationResult(
                    decision,
                    owner_kind,
                    owner_id,
                    fence_generation,
                    requested,
                    conflicts,
                    queue_id,
                )
            connection.executemany(
                """INSERT INTO path_reservations(
                    normalized_path, mode, owner_kind, owner_id, fence_generation,
                    acquired_at, expires_at, package_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(normalized_path) DO UPDATE SET mode=excluded.mode,
                    fence_generation=excluded.fence_generation,
                    acquired_at=excluded.acquired_at, expires_at=excluded.expires_at
                WHERE path_reservations.owner_kind=excluded.owner_kind
                    AND path_reservations.owner_id=excluded.owner_id""",
                (
                    (
                        item.normalized_path,
                        str(item.mode),
                        str(owner_kind),
                        owner_id,
                        fence_generation,
                        now,
                        expires_at,
                        owner_id if owner_kind is ReservationOwnerKind.PACKAGE else None,
                    )
                    for item in requested
                ),
            )
            connection.execute(
                "DELETE FROM path_reservation_queue WHERE owner_kind=? AND owner_id=?",
                (str(owner_kind), owner_id),
            )
            if owner_kind is ReservationOwnerKind.PACKAGE:
                self._touch_package(connection, owner_id)
            connection.commit()
            return ReservationResult(
                ReservationDecision.GRANTED,
                owner_kind,
                owner_id,
                fence_generation,
                requested,
            )

    def claim_ordinary_path_reservations(
        self,
        owner_id: str,
        requested: tuple[ReservationSpec, ...],
        *,
        ttl_seconds: float,
        now: str | None = None,
        queue_on_conflict: bool = True,
    ) -> ReservationResult:
        acquired_at = now or _utc_now()
        expires_at = _expires(acquired_at, ttl_seconds)
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._clean_expired_ordinary_reservations(connection, acquired_at)
            row = connection.execute(
                "SELECT generation FROM ordinary_reservation_leases WHERE owner_id=?", (owner_id,)
            ).fetchone()
            generation = int(row[0]) if row is not None else 1
            connection.execute(
                """INSERT INTO ordinary_reservation_leases VALUES(?, ?, ?, ?)
                ON CONFLICT(owner_id) DO UPDATE SET expires_at=excluded.expires_at""",
                (owner_id, generation, acquired_at, expires_at),
            )
            connection.commit()
        return self.acquire_path_reservations(
            ReservationOwnerKind.ORDINARY_TASK,
            owner_id,
            generation,
            requested,
            acquired_at=acquired_at,
            expires_at=expires_at,
            queue_on_conflict=queue_on_conflict,
        )

    def release_path_reservations(
        self,
        owner_kind: ReservationOwnerKind,
        owner_id: str,
        fence_generation: int,
    ) -> None:
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if owner_kind is ReservationOwnerKind.PACKAGE:
                self._assert_lease_generation(connection, owner_id, fence_generation)
            else:
                row = connection.execute(
                    "SELECT generation FROM ordinary_reservation_leases WHERE owner_id=?",
                    (owner_id,),
                ).fetchone()
                if row is None or int(row[0]) != fence_generation:
                    raise ValueError(f"stale ordinary-task reservation fence for {owner_id}")
            connection.execute(
                "DELETE FROM path_reservations WHERE owner_kind=? AND owner_id=?",
                (str(owner_kind), owner_id),
            )
            connection.execute(
                "DELETE FROM path_reservation_queue WHERE owner_kind=? AND owner_id=?",
                (str(owner_kind), owner_id),
            )
            if owner_kind is ReservationOwnerKind.ORDINARY_TASK:
                connection.execute(
                    "DELETE FROM ordinary_reservation_leases WHERE owner_id=?", (owner_id,)
                )
            else:
                self._touch_package(connection, owner_id)
            self._wake_waiting_reservation_packages(connection, _utc_now())
            connection.commit()

    def load_path_reservations(
        self, *, now: str | None = None
    ) -> tuple[GlobalPathReservation, ...]:
        observed_at = now or _utc_now()
        with _connect(self.path) as connection, connection:
            self._clean_expired_ordinary_reservations(connection, observed_at)
            return tuple(
                GlobalPathReservation(
                    str(row[0]),
                    ReservationMode(str(row[1])),
                    ReservationOwnerKind(str(row[2])),
                    str(row[3]),
                    int(row[4]),
                    str(row[5]),
                    str(row[6]) if row[6] is not None else None,
                )
                for row in connection.execute(
                    """SELECT normalized_path, mode, owner_kind, owner_id,
                        fence_generation, acquired_at, expires_at
                    FROM path_reservations ORDER BY normalized_path"""
                )
            )

    @staticmethod
    def _dependency_graph(connection: sqlite3.Connection) -> dict[str, set[str]]:
        graph = {
            str(row[0]): set() for row in connection.execute("SELECT id FROM capability_packages")
        }
        for package_id, depends_on in connection.execute(
            "SELECT package_id, depends_on_package_id FROM package_dependencies"
        ):
            graph.setdefault(str(package_id), set()).add(str(depends_on))
        return graph

    @staticmethod
    def _assert_dependency_dag(graph: dict[str, set[str]]) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(package_id: str) -> None:
            if package_id in visiting:
                raise ValueError("package dependency would create a cycle; merge instead")
            if package_id in visited:
                return
            visiting.add(package_id)
            for dependency in graph.get(package_id, ()):
                visit(dependency)
            visiting.remove(package_id)
            visited.add(package_id)

        for package_id in graph:
            visit(package_id)

    def add_package_dependency(
        self,
        dependency: PackageDependency,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, dependency.package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, dependency.package_id, lease_generation)
            if (
                connection.execute(
                    "SELECT 1 FROM capability_packages WHERE id=?",
                    (dependency.depends_on_package_id,),
                ).fetchone()
                is None
            ):
                raise KeyError(dependency.depends_on_package_id)
            graph = self._dependency_graph(connection)
            graph.setdefault(dependency.package_id, set()).add(dependency.depends_on_package_id)
            self._assert_dependency_dag(graph)
            connection.execute(
                """INSERT INTO package_dependencies VALUES(?, ?, ?, ?)
                ON CONFLICT(package_id, depends_on_package_id) DO UPDATE SET
                    required_revision=excluded.required_revision""",
                (
                    dependency.package_id,
                    dependency.depends_on_package_id,
                    dependency.required_revision,
                    dependency.created_at or _utc_now(),
                ),
            )
            self._touch_package(connection, dependency.package_id)
            return _load_package_state(connection).packages[dependency.package_id]

    def merge_capability_packages(
        self,
        survivor_id: str,
        merged_id: str,
        *,
        expected_survivor_revision: int,
        expected_merged_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        """Atomically transfer package-owned state and fence the superseded owner."""

        if survivor_id == merged_id:
            raise ValueError("cannot merge a package into itself")
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, survivor_id, expected_survivor_revision)
            self._assert_package_revision(connection, merged_id, expected_merged_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, survivor_id, lease_generation)

            for row in connection.execute(
                """SELECT id, work_unit_id, path, declaration, stage
                FROM package_consumers WHERE package_id=? ORDER BY id""",
                (merged_id,),
            ).fetchall():
                consumer_id = str(row[0])
                duplicate = connection.execute(
                    """SELECT id FROM package_consumers WHERE package_id=? AND work_unit_id=?
                    AND path=? AND declaration=? AND stage=?""",
                    (survivor_id, *row[1:]),
                ).fetchone()
                if duplicate is None:
                    connection.execute(
                        "UPDATE package_consumers SET package_id=? WHERE id=?",
                        (survivor_id, consumer_id),
                    )
                    continue
                kept_id = str(duplicate[0])
                connection.execute(
                    """INSERT OR IGNORE INTO package_consumer_blockers
                    SELECT ?, blocker_id, ordinal FROM package_consumer_blockers
                    WHERE consumer_id=?""",
                    (kept_id, consumer_id),
                )
                connection.execute(
                    """INSERT OR IGNORE INTO package_consumer_routes
                    SELECT ?, attempted_route, ordinal FROM package_consumer_routes
                    WHERE consumer_id=?""",
                    (kept_id, consumer_id),
                )
                connection.execute("DELETE FROM package_consumers WHERE id=?", (consumer_id,))

            for table in ("package_steps", "package_evidence", "integration_journal"):
                connection.execute(
                    f"UPDATE {table} SET package_id=? WHERE package_id=?",
                    (survivor_id, merged_id),
                )
            connection.execute(
                "UPDATE upstream_request_imports SET package_id=? WHERE package_id=?",
                (survivor_id, merged_id),
            )
            connection.execute(
                "UPDATE capability_aliases SET package_id=? WHERE package_id=?",
                (survivor_id, merged_id),
            )
            for table, columns in (
                ("package_textbook_refs", "textbook_ref, ordinal"),
                ("package_scopes", "scope_kind, normalized_path, ordinal"),
                ("package_read_interfaces", "interface_id, digest, source_revision"),
            ):
                connection.execute(
                    f"INSERT OR IGNORE INTO {table} SELECT ?, {columns} FROM {table} "
                    "WHERE package_id=?",
                    (survivor_id, merged_id),
                )
                connection.execute(f"DELETE FROM {table} WHERE package_id=?", (merged_id,))
            connection.execute(
                """UPDATE path_reservations SET package_id=?, owner_id=?
                WHERE owner_kind='package' AND owner_id=?""",
                (survivor_id, survivor_id, merged_id),
            )
            connection.execute(
                "UPDATE capability_packages SET parent_package_id=? WHERE parent_package_id=?",
                (survivor_id, merged_id),
            )

            transformed: dict[tuple[str, str], tuple[str | None, str]] = {}
            for package_id, depends_on, required, created in connection.execute(
                """SELECT package_id, depends_on_package_id, required_revision, created_at
                FROM package_dependencies"""
            ):
                source = survivor_id if str(package_id) == merged_id else str(package_id)
                target = survivor_id if str(depends_on) == merged_id else str(depends_on)
                if source != target:
                    transformed.setdefault(
                        (source, target),
                        (str(required) if required is not None else None, str(created)),
                    )
            connection.execute("DELETE FROM package_dependencies")
            connection.executemany(
                "INSERT INTO package_dependencies VALUES(?, ?, ?, ?)",
                (
                    (source, target, required, created)
                    for (source, target), (required, created) in transformed.items()
                ),
            )
            self._assert_dependency_dag(self._dependency_graph(connection))
            connection.execute("DELETE FROM steward_leases WHERE package_id=?", (merged_id,))
            now = _utc_now()
            connection.execute(
                """UPDATE capability_packages SET status=?, disposition=?, revision=revision+1,
                    updated_at=? WHERE id=?""",
                (
                    str(PackageStatus.SUPERSEDED),
                    str(PackageDisposition.SUPERSEDED),
                    now,
                    merged_id,
                ),
            )
            self._touch_package(connection, survivor_id)
            return _load_package_state(connection).packages[survivor_id]

    def split_capability_package(
        self,
        parent_id: str,
        children: tuple[CapabilityPackage, ...],
        consumer_assignments: dict[str, tuple[str, ...]],
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> tuple[CapabilityPackage, ...]:
        """Atomically decompose a package and transfer all open consumers and locks."""

        if not children:
            raise ValueError("a split requires at least one child package")
        child_ids = {child.id for child in children}
        if len(child_ids) != len(children) or set(consumer_assignments) - child_ids:
            raise ValueError("consumer assignments must name distinct child packages")
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, parent_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, parent_id, lease_generation)
            assigned_ids = [value for values in consumer_assignments.values() for value in values]
            if len(assigned_ids) != len(set(assigned_ids)):
                raise ValueError("a consumer may be assigned to only one child")
            open_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT id FROM package_consumers WHERE package_id=? AND status='open'",
                    (parent_id,),
                )
            }
            if not open_ids.issubset(assigned_ids):
                raise ValueError("every open parent consumer must be assigned to a child")
            actual_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT id FROM package_consumers WHERE package_id=?", (parent_id,)
                )
            }
            if not set(assigned_ids).issubset(actual_ids):
                raise ValueError("consumer assignment does not belong to the parent package")
            now = _utc_now()
            for child in children:
                if child.parent_package_id not in {None, parent_id}:
                    raise ValueError("split child has a different parent")
                if _package_id_for_key(connection, child.capability_key) is not None:
                    raise ValueError(f"child capability already exists: {child.capability_key}")
                _insert_package(
                    connection,
                    replace(
                        child,
                        parent_package_id=parent_id,
                        created_at=child.created_at or now,
                        updated_at=child.updated_at or now,
                    ),
                )
            for child_id, consumer_ids in consumer_assignments.items():
                connection.executemany(
                    "UPDATE package_consumers SET package_id=?, updated_at=? WHERE id=?",
                    ((child_id, now, consumer_id) for consumer_id in consumer_ids),
                )

            child_scopes = {child.id: child.write_scope for child in children}
            for path, _mode in connection.execute(
                """SELECT normalized_path, mode FROM path_reservations
                WHERE owner_kind='package' AND owner_id=?""",
                (parent_id,),
            ).fetchall():
                owners = [
                    child_id
                    for child_id, scopes in child_scopes.items()
                    if any(
                        str(path) == scope or str(path).startswith(f"{scope}/") for scope in scopes
                    )
                ]
                if len(owners) != 1:
                    raise ValueError(
                        f"reservation {path} must belong to exactly one split child scope"
                    )
                connection.execute(
                    """UPDATE path_reservations SET package_id=?, owner_id=?
                    WHERE normalized_path=?""",
                    (owners[0], owners[0], str(path)),
                )

            incoming: list[tuple[str, str | None, str]] = []
            outgoing: list[tuple[str, str | None, str]] = []
            for package_id, depends_on, required, created in connection.execute(
                """SELECT package_id, depends_on_package_id, required_revision, created_at
                FROM package_dependencies"""
            ):
                if str(depends_on) == parent_id:
                    incoming.append(
                        (
                            str(package_id),
                            str(required) if required is not None else None,
                            str(created),
                        )
                    )
                if str(package_id) == parent_id:
                    outgoing.append(
                        (
                            str(depends_on),
                            str(required) if required is not None else None,
                            str(created),
                        )
                    )
            connection.execute(
                "DELETE FROM package_dependencies WHERE package_id=? OR depends_on_package_id=?",
                (parent_id, parent_id),
            )
            edges = {
                (source, child.id, required, created)
                for source, required, created in incoming
                for child in children
                if source != child.id
            } | {
                (child.id, target, required, created)
                for target, required, created in outgoing
                for child in children
                if target != child.id
            }
            connection.executemany(
                "INSERT OR IGNORE INTO package_dependencies VALUES(?, ?, ?, ?)", edges
            )
            self._assert_dependency_dag(self._dependency_graph(connection))
            connection.execute("DELETE FROM steward_leases WHERE package_id=?", (parent_id,))
            connection.execute(
                """UPDATE capability_packages SET status=?, disposition=?, revision=revision+1,
                    updated_at=? WHERE id=?""",
                (
                    str(PackageStatus.DECOMPOSED),
                    str(PackageDisposition.DECOMPOSED),
                    now,
                    parent_id,
                ),
            )
            state = _load_package_state(connection)
            return tuple(state.packages[child.id] for child in children)

    def replace_relevant_read_interfaces(
        self,
        package_id: str,
        interfaces: tuple[RelevantReadInterface, ...],
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, package_id, expected_revision)
            if lease_generation is not None:
                self._assert_lease_generation(connection, package_id, lease_generation)
            if any(item.package_id != package_id for item in interfaces):
                raise ValueError("read interfaces must belong to the selected package")
            connection.execute(
                "DELETE FROM package_read_interfaces WHERE package_id=?", (package_id,)
            )
            connection.executemany(
                "INSERT INTO package_read_interfaces VALUES(?, ?, ?, ?)",
                (
                    (
                        item.package_id,
                        item.interface_id,
                        item.digest,
                        item.source_revision,
                    )
                    for item in interfaces
                ),
            )
            self._touch_package(connection, package_id)
            return _load_package_state(connection).packages[package_id]

    def record_integration_journal(
        self,
        journal: IntegrationJournal,
        *,
        expected_revision: int,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection, connection:
            self._assert_package_revision(connection, journal.package_id, expected_revision)
            self._assert_lease_generation(connection, journal.package_id, journal.lease_generation)
            existing = connection.execute(
                "SELECT package_id, lease_generation FROM integration_journal WHERE id=?",
                (journal.id,),
            ).fetchone()
            if existing is not None and (
                str(existing[0]) != journal.package_id
                or int(existing[1]) != journal.lease_generation
            ):
                raise ValueError("integration journal id belongs to another fenced generation")
            now = _utc_now()
            connection.execute(
                """INSERT INTO integration_journal(
                    id, package_id, lease_generation, base_revision, candidate_revision,
                    canonical_revision_before, phase, validation_digest,
                    canonical_revision_after, provisional_consumer_ids, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET phase=excluded.phase,
                    validation_digest=excluded.validation_digest,
                    canonical_revision_after=excluded.canonical_revision_after,
                    provisional_consumer_ids=excluded.provisional_consumer_ids,
                    updated_at=excluded.updated_at
                WHERE integration_journal.package_id=excluded.package_id
                    AND integration_journal.lease_generation=excluded.lease_generation""",
                (
                    journal.id,
                    journal.package_id,
                    journal.lease_generation,
                    journal.base_revision,
                    journal.candidate_revision,
                    journal.canonical_revision_before,
                    str(journal.phase),
                    journal.validation_digest,
                    journal.canonical_revision_after,
                    json.dumpb(list(journal.provisional_consumer_ids)),
                    journal.created_at or now,
                    journal.updated_at or now,
                ),
            )
            self._touch_package(connection, journal.package_id)
            return _load_package_state(connection).packages[journal.package_id]

    @staticmethod
    def _accept_integration_consumers(
        connection: sqlite3.Connection,
        package_id: str,
        consumer_ids: tuple[str, ...],
        canonical_revision: str,
        now: str,
    ) -> None:
        for consumer_id in consumer_ids:
            row = connection.execute(
                """SELECT package_id, status, accepted_revision
                FROM package_consumers WHERE id=?""",
                (consumer_id,),
            ).fetchone()
            if row is None or str(row[0]) != package_id:
                raise ValueError(
                    f"provisional consumer {consumer_id} does not belong to package {package_id}"
                )
            status = ConsumerStatus(str(row[1]))
            accepted_revision = str(row[2]) if row[2] is not None else None
            if status is ConsumerStatus.ACCEPTED:
                if accepted_revision != canonical_revision:
                    raise ValueError(
                        f"consumer {consumer_id} was accepted at another canonical revision"
                    )
                continue
            if status is not ConsumerStatus.OPEN:
                raise ValueError(
                    f"provisional consumer {consumer_id} is no longer open for acceptance"
                )
            connection.execute(
                """UPDATE package_consumers SET status=?, accepted_revision=?,
                    residual_goal='', updated_at=? WHERE id=? AND package_id=?""",
                (
                    str(ConsumerStatus.ACCEPTED),
                    canonical_revision,
                    now,
                    consumer_id,
                    package_id,
                ),
            )

    def finalize_package_integration(
        self,
        journal_id: str,
        *,
        expected_revision: int,
        lease_generation: int,
        canonical_revision_after: str,
        release_reservations: bool = True,
    ) -> CapabilityPackage:
        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT package_id, lease_generation, candidate_revision, phase,
                    provisional_consumer_ids
                FROM integration_journal WHERE id=?""",
                (journal_id,),
            ).fetchone()
            if row is None:
                raise KeyError(journal_id)
            package_id = str(row[0])
            self._assert_package_revision(connection, package_id, expected_revision)
            self._assert_live_lease(connection, package_id, lease_generation)
            if int(row[1]) != lease_generation:
                raise ValueError("integration journal belongs to another fenced generation")
            if str(row[2]) != canonical_revision_after:
                raise ValueError("canonical revision does not equal the validated candidate")
            if str(row[3]) not in {
                str(IntegrationPhase.VALIDATED),
                str(IntegrationPhase.IMPORTING),
            }:
                raise ValueError("integration journal is not ready to finalize")
            now = _utc_now()
            provisional_consumer_ids = tuple(str(value) for value in json.loads(row[4]))
            self._accept_integration_consumers(
                connection,
                package_id,
                provisional_consumer_ids,
                canonical_revision_after,
                now,
            )
            connection.execute(
                """UPDATE integration_journal SET phase=?, canonical_revision_after=?,
                    updated_at=? WHERE id=?""",
                (str(IntegrationPhase.FINALIZED), canonical_revision_after, now, journal_id),
            )
            connection.execute(
                """UPDATE capability_packages SET integrated_revision=?, revision=revision+1,
                    updated_at=? WHERE id=?""",
                (canonical_revision_after, now, package_id),
            )
            if release_reservations:
                connection.execute(
                    "DELETE FROM path_reservations WHERE owner_kind='package' AND owner_id=?",
                    (package_id,),
                )
                self._wake_waiting_reservation_packages(connection, now)
            connection.commit()
        return self.load_package_state().packages[package_id]

    def reconcile_imported_integration(
        self,
        journal_id: str,
        *,
        canonical_revision_after: str,
        validation_digest: str,
    ) -> CapabilityPackage:
        """Finish durable state after Git advanced but the final transaction was interrupted."""

        with _connect(self.path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT package_id, candidate_revision, phase, validation_digest,
                    provisional_consumer_ids
                FROM integration_journal WHERE id=?""",
                (journal_id,),
            ).fetchone()
            if row is None:
                raise KeyError(journal_id)
            if str(row[1]) != canonical_revision_after:
                raise ValueError("canonical revision does not equal journal candidate")
            if str(row[2]) not in {
                str(IntegrationPhase.IMPORTING),
                str(IntegrationPhase.FINALIZED),
            }:
                raise ValueError("journal does not describe an interrupted import")
            if not validation_digest or str(row[3]) != validation_digest:
                raise ValueError("validation digest does not match interrupted import")
            package_id = str(row[0])
            now = _utc_now()
            provisional_consumer_ids = tuple(str(value) for value in json.loads(row[4]))
            self._accept_integration_consumers(
                connection,
                package_id,
                provisional_consumer_ids,
                canonical_revision_after,
                now,
            )
            connection.execute(
                """UPDATE integration_journal SET phase=?, canonical_revision_after=?,
                    updated_at=? WHERE id=?""",
                (str(IntegrationPhase.FINALIZED), canonical_revision_after, now, journal_id),
            )
            connection.execute(
                """UPDATE capability_packages SET integrated_revision=?, revision=revision+1,
                    updated_at=? WHERE id=? AND integrated_revision IS NOT ?""",
                (canonical_revision_after, now, package_id, canonical_revision_after),
            )
            connection.execute(
                "DELETE FROM path_reservations WHERE owner_kind='package' AND owner_id=?",
                (package_id,),
            )
            self._wake_waiting_reservation_packages(connection, now)
            connection.commit()
        return self.load_package_state().packages[package_id]

    def abort_integration_reconciliation(self, journal_id: str) -> CapabilityPackage:
        """Record that an interrupted import cannot be replayed onto current history."""

        with _connect(self.path) as connection, connection:
            row = connection.execute(
                "SELECT package_id, phase FROM integration_journal WHERE id=?", (journal_id,)
            ).fetchone()
            if row is None:
                raise KeyError(journal_id)
            if str(row[1]) != str(IntegrationPhase.IMPORTING):
                raise ValueError("only an interrupted import may be reconciled as aborted")
            package_id = str(row[0])
            connection.execute(
                "UPDATE integration_journal SET phase=?, updated_at=? WHERE id=?",
                (str(IntegrationPhase.ABORTED), _utc_now(), journal_id),
            )
            self._touch_package(connection, package_id)
            return _load_package_state(connection).packages[package_id]

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
                    globals_.update(_load_package_state(connection).as_dict())
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
                checkpoint.update(_load_package_state(connection).as_dict())
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
