from __future__ import annotations

import asyncio
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from paf.package_model import (
    CapabilityPackage,
    ConsumerStatus,
    PackageConsumer,
    PackageDisposition,
    PackageStatus,
    PathReservation,
    ReservationDecision,
    ReservationMode,
    ReservationOwnerKind,
    ReservationSpec,
)
from paf.package_runtime import (
    ConsumerValidation,
    PackageExecutionLayer,
    PackageImport,
    PackageValidation,
    PackageWorkspace,
)
from paf.state_db import StateDatabase

EARLY = "2030-01-01T00:00:00+00:00"
LATE = "2030-01-01T02:00:00+00:00"


def run_git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return result.stdout.decode().strip()


def git_repo(tmp_path: Path) -> Path:
    run_git(tmp_path, "init", "-b", "main")
    run_git(tmp_path, "config", "user.email", "paf@example.invalid")
    run_git(tmp_path, "config", "user.name", "PAF Tests")
    source = tmp_path / "lean" / "Book.lean"
    source.parent.mkdir()
    source.write_text("def base := 1\n", encoding="utf-8")
    run_git(tmp_path, "add", ".")
    run_git(tmp_path, "commit", "-m", "chore: initialize")
    return tmp_path


def package(store: StateDatabase, package_id: str = "P42") -> CapabilityPackage:
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            package_id,
            f"capability.{package_id}",
            f"Capability {package_id}",
            f"Implement {package_id}",
            write_scope=("lean/Book.lean",),
            expansion_scope=("lean/Book.lean",),
        )
    )
    return current


def claim(store: StateDatabase, current: CapabilityPackage, agent: str = "steward"):
    return store.claim_steward_lease(
        current.id,
        agent,
        expected_revision=current.revision,
        ttl_seconds=3600,
        now=EARLY,
    )


def reserve(store: StateDatabase, current: CapabilityPackage, generation: int):
    return store.reserve_package_paths(
        (
            PathReservation(
                "lean/Book.lean",
                ReservationMode.EXCLUSIVE_FILE,
                current.id,
                generation,
                EARLY,
            ),
        ),
        expected_revision=current.revision,
    )


def test_steward_claim_heartbeat_expiry_recovery_and_release_are_fenced(
    tmp_path: Path,
) -> None:
    store = StateDatabase(tmp_path / ".paf")
    store.initialize()
    current = package(store)
    first = claim(store, current, "first")
    with pytest.raises(ValueError, match="live steward"):
        store.claim_steward_lease(
            current.id,
            "second",
            expected_revision=store.load_package_state().packages[current.id].revision,
            ttl_seconds=3600,
            now="2030-01-01T00:30:00+00:00",
        )
    heartbeat = store.heartbeat_steward_lease(
        current.id,
        "first",
        first.generation,
        ttl_seconds=60,
        now="2030-01-01T00:10:00+00:00",
    )
    assert heartbeat.generation == first.generation
    current = store.load_package_state().packages[current.id]
    current = reserve(store, current, first.generation)

    recovered, record = store.recover_steward_lease(
        current.id,
        "second",
        expected_revision=current.revision,
        ttl_seconds=3600,
        now=LATE,
    )
    assert recovered.generation == first.generation + 1
    assert record.prior_generation == first.generation
    assert store.load_package_state().reservations["lean/Book.lean"].lease_generation == (
        recovered.generation
    )
    with pytest.raises(ValueError, match="stale lease generation"):
        store.heartbeat_steward_lease(
            current.id, "first", first.generation, ttl_seconds=60, now=LATE
        )
    with pytest.raises(ValueError, match="stale lease generation"):
        store.release_steward_lease(current.id, "first", first.generation, now=LATE)
    store.release_steward_lease(
        current.id,
        "second",
        recovered.generation,
        release_reservations=True,
        now="2030-01-01T02:30:00+00:00",
    )
    assert not store.load_package_state().reservations


def test_global_reservations_are_atomic_and_cover_package_vs_ordinary_conflicts(
    tmp_path: Path,
) -> None:
    store = StateDatabase(tmp_path / ".paf")
    store.initialize()
    current = package(store)
    lease = claim(store, current)
    current = store.load_package_state().packages[current.id]
    result = store.acquire_path_reservations(
        ReservationOwnerKind.PACKAGE,
        current.id,
        lease.generation,
        (ReservationSpec("lean", ReservationMode.EXCLUSIVE_SUBTREE),),
        acquired_at=EARLY,
        expected_revision=current.revision,
    )
    assert result.granted

    ordinary = store.claim_ordinary_path_reservations(
        "ordinary-run",
        (
            ReservationSpec("lean/Other.lean"),
            ReservationSpec("docs/free.md"),
        ),
        ttl_seconds=3600,
        now=EARLY,
    )
    assert ordinary.decision is ReservationDecision.QUEUED
    assert ordinary.queue_id
    assert {value.owner_id for value in store.load_path_reservations(now=EARLY)} == {current.id}

    store.release_path_reservations(ReservationOwnerKind.PACKAGE, current.id, lease.generation)
    granted = store.claim_ordinary_path_reservations(
        "ordinary-run",
        ordinary.requested,
        ttl_seconds=3600,
        now=EARLY,
    )
    assert granted.granted
    assert {value.normalized_path for value in store.load_path_reservations(now=EARLY)} == {
        "docs/free.md",
        "lean/Other.lean",
    }


def workspace_provider(repo: Path, _store: StateDatabase):
    async def acquire(
        package: CapabilityPackage, _generation: int, _scope: tuple[str, ...]
    ) -> PackageWorkspace:
        parent = repo / ".paf" / "runtime-overlays"
        sequence = len(list(parent.glob("*"))) if parent.exists() else 0
        root = parent / f"{package.id}-{sequence}"
        root.mkdir(parents=True)
        shutil.copytree(repo / "lean", root / "lean")

        async def integrate(scope: tuple[str, ...], message: str) -> PackageImport:
            tracked = set(run_git(repo, "ls-files").splitlines())
            present = {
                path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()
            }
            changed = tuple(
                sorted(
                    path
                    for path in tracked | present
                    if not (repo / path).exists()
                    or not (root / path).exists()
                    or (repo / path).read_bytes() != (root / path).read_bytes()
                )
            )
            outside = tuple(
                path
                for path in changed
                if not any(
                    path == allowed or path.startswith(f"{allowed.rstrip('/')}/")
                    for allowed in scope
                )
            )
            if outside:
                raise ValueError("overlay changed paths outside scope: " + ", ".join(outside))
            for path in changed:
                source_path = root / path
                destination = repo / path
                if source_path.exists():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_path, destination)
                else:
                    destination.unlink(missing_ok=True)
            commit = ""
            if changed:
                run_git(repo, "add", "-A", "--", *changed)
                run_git(repo, "commit", "-m", message)
                commit = run_git(repo, "rev-parse", "HEAD")
            return PackageImport(changed, commit, commit or run_git(repo, "rev-parse", "HEAD"))

        async def close() -> None:
            return

        return PackageWorkspace(root, integrate, close)

    return acquire


def test_recovered_lease_has_no_private_git_candidate(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store)
    first = claim(store, current, "first")
    current = store.load_package_state().packages[current.id]

    recovered, record = store.recover_steward_lease(
        current.id,
        "replacement",
        expected_revision=current.revision,
        ttl_seconds=3600,
        now=LATE,
    )

    assert recovered.generation == first.generation + 1
    assert record.active_child_workers == ()
    assert not run_git(repo, "for-each-ref", "--format=%(refname)", "refs/heads/paf")


def test_ordinary_reservation_expiry_cannot_block_a_package_forever(
    tmp_path: Path,
) -> None:
    store = StateDatabase(tmp_path / ".paf")
    store.initialize()
    ordinary = store.claim_ordinary_path_reservations(
        "ordinary",
        (ReservationSpec("lean/Book.lean"),),
        ttl_seconds=60,
        now=EARLY,
    )
    assert ordinary.granted
    current = package(store)
    lease = claim(store, current)
    current = store.load_package_state().packages[current.id]
    result = store.acquire_path_reservations(
        ReservationOwnerKind.PACKAGE,
        current.id,
        lease.generation,
        (ReservationSpec("lean/Book.lean"),),
        acquired_at=LATE,
        expected_revision=current.revision,
    )
    assert result.granted


def test_schema_v5_package_locks_migrate_into_global_authority(tmp_path: Path) -> None:
    store = StateDatabase(tmp_path / ".paf")
    store.initialize()
    current = package(store)
    lease = claim(store, current)
    current = store.load_package_state().packages[current.id]
    reserve(store, current, lease.generation)
    with sqlite3.connect(store.path) as connection, connection:
        connection.execute("ALTER TABLE path_reservations RENAME TO path_reservations_v6")
        connection.executescript(
            """
            CREATE TABLE path_reservations (
                normalized_path TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                package_id TEXT NOT NULL REFERENCES capability_packages(id) ON DELETE CASCADE,
                lease_generation INTEGER NOT NULL,
                acquired_at TEXT NOT NULL
            );
            INSERT INTO path_reservations
            SELECT normalized_path, mode, package_id, fence_generation, acquired_at
            FROM path_reservations_v6;
            DROP TABLE path_reservations_v6;
            UPDATE meta SET schema_version=5 WHERE singleton=1;
            PRAGMA user_version=5;
            """
        )

    store.initialize()

    migrated = store.load_path_reservations(now=EARLY)
    assert len(migrated) == 1
    assert migrated[0].owner_kind is ReservationOwnerKind.PACKAGE
    assert migrated[0].owner_id == current.id


def steward_report(
    *,
    disposition: str = "complete",
    complete: bool = True,
    consumers: tuple[str, ...] = ("consumer-a", "consumer-b"),
) -> dict[str, object]:
    return {
        "complete": complete,
        "summary": "implemented the shared interface and both consumers",
        "issues": [],
        "diagnosis": "A shared bridge belongs in the earlier support file.",
        "placement_decision": {
            "kind": "shared",
            "paths": ["lean/Support.lean"],
            "declarations": ["supportBridge"],
            "rationale": "Both consumers use the same source-neutral fact.",
        },
        "scope_expansion_requests": [],
        "plan_revision": {
            "base_revision": 0,
            "revision_reason": "Initial small-lemma plan.",
            "steps": [
                {
                    "step_id": "interface",
                    "objective": "Add supportBridge",
                    "kind": "interface",
                    "intended_declarations": ["supportBridge"],
                    "intended_paths": ["lean/Support.lean"],
                    "depends_on_step_ids": [],
                    "validation_commands": ["check support"],
                },
                {
                    "step_id": "consumer",
                    "objective": "Use supportBridge",
                    "kind": "consumer_integration",
                    "intended_declarations": ["base"],
                    "intended_paths": ["lean/Book.lean"],
                    "depends_on_step_ids": ["interface"],
                    "validation_commands": ["check consumer"],
                },
            ],
        },
        "completed_step_assessments": [
            {
                "step_id": "interface",
                "accepted": True,
                "commit_ids": [],
                "validation_evidence": "support file checks",
                "remaining_gap": "",
            }
        ],
        "worker_assignments": [
            {"step_id": "consumer", "worker_id": "worker-1", "objective": "wire consumer"}
        ],
        "package_dependency_requests": [],
        "child_packages": [],
        "consumer_assessments": [
            {
                "consumer_id": consumer_id,
                "disposition": "accepted",
                "acceptance_evidence": "focused declaration check",
                "detached_package_id": None,
                "remaining_obstruction": "",
            }
            for consumer_id in consumers
        ],
        "disposition": disposition,
        "remaining_work": "",
    }


def disposition_report(
    disposition: str,
    consumer_ids: tuple[str, ...],
    *,
    child_packages: list[dict[str, object]] | None = None,
    dependencies: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "complete": disposition in {"complete", "external", "decomposed"},
        "summary": f"classified package as {disposition}",
        "issues": [],
        "diagnosis": "The current source and consumers were checked.",
        "placement_decision": {
            "kind": "external" if disposition == "external" else "consumer_local",
            "paths": ["lean/Book.lean"],
            "declarations": ["base"],
            "rationale": "The classification follows the current source boundary.",
        },
        "scope_expansion_requests": [],
        "plan_revision": {
            "base_revision": 0,
            "revision_reason": "Classification requires no implementation steps.",
            "steps": [],
        },
        "completed_step_assessments": [],
        "worker_assignments": [],
        "package_dependency_requests": dependencies or [],
        "child_packages": child_packages or [],
        "consumer_assessments": [
            {
                "consumer_id": consumer_id,
                "disposition": "terminal" if disposition == "external" else "open",
                "acceptance_evidence": (
                    "Capability belongs to an unavailable dependency."
                    if disposition == "external"
                    else ""
                ),
                "detached_package_id": None,
                "remaining_obstruction": "external gap" if disposition == "external" else "",
            }
            for consumer_id in consumer_ids
        ],
        "disposition": disposition,
        "remaining_work": "exact remaining work",
    }


def test_decomposition_report_rejects_consumer_outside_child_scope(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    consumer_path = repo / "lean" / "Consumer.lean"
    consumer_path.write_text("theorem consumer : True := by sorry\n", encoding="utf-8")
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "test: add consumer")
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "parent",
            "parent.key",
            "Parent",
            "Implement the parent",
            write_scope=("lean/Book.lean", "lean/Consumer.lean"),
            expansion_scope=("lean/Book.lean", "lean/Consumer.lean"),
        ),
        consumer=PackageConsumer(
            "consumer-a",
            "parent",
            "chapter-a",
            "lean/Consumer.lean",
            "consumer",
            "prove",
        ),
    )
    report = disposition_report(
        "decomposed",
        ("consumer-a",),
        child_packages=[
            {
                "capability_key": "child.key",
                "title": "Child",
                "mathematical_objective": "Implement the child",
                "write_scope": ["lean/Book.lean"],
                "consumer_ids": ["consumer-a"],
            }
        ],
    )
    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "still blocked", consumer.id
        ),
    )

    with pytest.raises(
        ValueError,
        match="consumer consumer-a path is outside its decomposition child scope",
    ):
        runtime._validate_report(current, report)

    state = store.load_package_state()
    assert state.packages[current.id].status is PackageStatus.OBSERVED
    assert state.consumers["consumer-a"].package_id == current.id


@pytest.mark.asyncio
async def test_initial_scope_conflict_waits_durably_without_steward_ping_pong(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    ordinary = store.claim_ordinary_path_reservations(
        "ordinary",
        (ReservationSpec("lean/Book.lean"),),
        ttl_seconds=3600,
        now=EARLY,
    )
    current = package(store, "P-initial-wait")
    steward_calls = 0

    async def run_steward(*_args):
        nonlocal steward_calls
        steward_calls += 1
        return disposition_report("complete", ())

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=run_steward,
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
    )

    first = await runtime.execute(current.id)
    second = await runtime.execute(current.id)

    assert first.status is PackageStatus.WAITING_RESERVATION
    assert second.status is PackageStatus.WAITING_RESERVATION
    assert second.generation == 0
    assert steward_calls == 0
    assert runtime.ready_packages() == ()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM path_reservation_queue WHERE owner_id=?", (current.id,)
        ).fetchone() == (1,)

    store.release_path_reservations(
        ReservationOwnerKind.ORDINARY_TASK, ordinary.owner_id, ordinary.fence_generation
    )

    assert store.load_package_state().packages[current.id].status is PackageStatus.OBSERVED
    assert tuple(value.id for value in runtime.ready_packages()) == (current.id,)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM path_reservation_queue WHERE owner_id=?", (current.id,)
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_scope_expansion_wait_refences_queue_and_recovers_retained_edits(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    support = repo / "lean" / "Support.lean"
    support.write_text("def support := 1\n", encoding="utf-8")
    run_git(repo, "add", "lean/Support.lean")
    run_git(repo, "commit", "-m", "feat: add support")
    store = StateDatabase(repo / ".paf")
    store.initialize()
    ordinary = store.claim_ordinary_path_reservations(
        "ordinary-support",
        (ReservationSpec("lean/Support.lean"),),
        ttl_seconds=3600,
        now=EARLY,
    )
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "P-expansion-wait",
            "expansion.wait",
            "Expansion wait",
            "Wait safely for an expanded support path",
            write_scope=("lean/Book.lean",),
            expansion_scope=("lean/Book.lean", "lean/Support.lean"),
        )
    )
    report = steward_report(consumers=())
    report["scope_expansion_requests"] = [
        {
            "path": "lean/Support.lean",
            "mode": "exclusive_file",
            "reason": "the shared interface belongs here",
        }
    ]
    report["plan_revision"] = {
        "base_revision": 0,
        "revision_reason": "the Steward owns this bounded interface edit",
        "steps": [],
    }
    report["completed_step_assessments"] = []
    report["worker_assignments"] = []
    steward_calls = 0

    async def run_steward(_package, _dossier, worktree):
        nonlocal steward_calls
        steward_calls += 1
        (worktree / "lean" / "Support.lean").write_text(
            "def support := 1\ndef sharedBridge := support\n", encoding="utf-8"
        )
        return report

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=run_steward,
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
    )

    first = await runtime.execute(current.id)

    assert first.status is PackageStatus.WAITING_RESERVATION
    assert steward_calls == 1
    waiting = store.load_package_state().packages[current.id]
    replacement = store.claim_steward_lease(
        current.id,
        "replacement",
        expected_revision=waiting.revision,
        ttl_seconds=3600,
    )
    with sqlite3.connect(store.path) as connection:
        rows = connection.execute(
            """SELECT fence_generation FROM path_reservation_queue
            WHERE owner_kind='package' AND owner_id=?""",
            (current.id,),
        ).fetchall()
    assert rows == [(replacement.generation,)]
    store.release_steward_lease(current.id, "replacement", replacement.generation)
    store.release_path_reservations(
        ReservationOwnerKind.ORDINARY_TASK, ordinary.owner_id, ordinary.fence_generation
    )

    assert store.load_package_state().packages[current.id].status is PackageStatus.OBSERVED
    second = await runtime.execute(current.id)

    assert second.status is PackageStatus.COMPLETE
    assert steward_calls == 2
    assert "sharedBridge" in support.read_text(encoding="utf-8")
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM path_reservation_queue WHERE owner_id=?", (current.id,)
        ).fetchone() == (0,)


@pytest.mark.asyncio
async def test_package_execution_integrates_multifile_steward_and_small_worker_steps(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    support = repo / "lean" / "Support.lean"
    support.write_text("def support := 1\n", encoding="utf-8")
    run_git(repo, "add", "lean/Support.lean")
    run_git(repo, "commit", "-m", "feat: add support")
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "P-multi",
            "shared.bridge",
            "Shared bridge",
            "Implement and use a shared bridge",
            write_scope=("lean/Support.lean", "lean/Book.lean"),
            expansion_scope=("lean/Support.lean", "lean/Book.lean"),
        ),
        consumer=PackageConsumer(
            "consumer-a", "P-multi", "chapter-a", "lean/Book.lean", "base", "prove"
        ),
    )
    store.attach_package_consumer(
        current.id,
        PackageConsumer("consumer-b", "P-multi", "chapter-b", "lean/Book.lean", "base", "prove"),
        expected_revision=current.revision,
    )
    woken: list[str] = []

    async def run_steward(_package, _dossier, worktree):
        (worktree / "lean" / "Support.lean").write_text(
            "def support := 1\ndef supportBridge := support\n", encoding="utf-8"
        )
        return steward_report()

    async def run_worker(_package, step, _packet, worktree):
        (worktree / "lean" / "Book.lean").write_text(
            "def base := supportBridge\n", encoding="utf-8"
        )
        return {
            "complete": True,
            "summary": "wired the bounded consumer",
            "issues": [],
            "step_id": step.id,
            "changed_declarations": ["base"],
            "changed_paths": ["lean/Book.lean"],
            "focused_validation": "consumer checks",
            "remaining_gap": "",
            "new_evidence": [],
        }

    async def wake(ids):
        woken.extend(ids)

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=run_steward,
        run_worker=run_worker,
        validate_step=lambda _path, _step: PackageValidation(True, "step", "step checks"),
        validate_package=lambda _path, _package: PackageValidation(
            True, "package", "package checks"
        ),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            True,
            f"consumer-{consumer.id}",
            "consumer checks",
            consumer.id,
            (consumer.work_unit_id,),
        ),
        wake_consumers=wake,
    )

    result = await runtime.execute(current.id)

    assert result.status.value == "complete"
    assert result.worker_ids == ("worker-1",)
    assert set(result.accepted_consumer_ids) == {"consumer-a", "consumer-b"}
    assert set(woken) == {"chapter-a", "chapter-b"}
    assert "supportBridge" in support.read_text(encoding="utf-8")
    state = store.load_package_state()
    assert all(value.status.value == "complete" for value in state.steps.values())
    assert all(value.status is ConsumerStatus.ACCEPTED for value in state.consumers.values())


@pytest.mark.asyncio
async def test_package_execution_rejects_model_path_outside_expansion_scope(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store, "P-invalid")
    report = steward_report(consumers=())
    report["scope_expansion_requests"] = [
        {"path": "README.md", "mode": "exclusive_file", "reason": "wrong scope"}
    ]
    report["plan_revision"] = {
        "base_revision": 0,
        "revision_reason": "invalid",
        "steps": [],
    }
    report["completed_step_assessments"] = []
    report["worker_assignments"] = []

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
    )

    result = await runtime.execute(current.id)

    assert result.status.value == "parked"
    assert "invalid package path" in result.detail
    assert not (repo / "README.md").exists()
    assert not store.load_package_state().reservations


@pytest.mark.asyncio
async def test_package_execution_accepts_consumers_independently_and_wakes_only_accepted(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "P-partial",
            "partial.consumers",
            "Partial consumers",
            "Validate consumers independently",
            write_scope=("lean/Book.lean",),
            expansion_scope=("lean/Book.lean",),
        ),
        consumer=PackageConsumer(
            "consumer-a", "P-partial", "chapter-a", "lean/Book.lean", "base", "prove"
        ),
    )
    store.attach_package_consumer(
        current.id,
        PackageConsumer("consumer-b", "P-partial", "chapter-b", "lean/Book.lean", "base", "prove"),
        expected_revision=current.revision,
    )
    report = disposition_report("continue", ("consumer-a", "consumer-b"))
    woken: list[str] = []

    async def wake(ids):
        woken.extend(ids)

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            consumer.id == "consumer-a",
            consumer.id,
            "accepted" if consumer.id == "consumer-a" else "new local obstruction",
            consumer.id,
            (consumer.work_unit_id,),
        ),
        wake_consumers=wake,
    )

    result = await runtime.execute(current.id)

    assert result.status.value == "implementing"
    assert result.accepted_consumer_ids == ("consumer-a",)
    assert woken == ["chapter-a"]
    state = store.load_package_state()
    assert state.consumers["consumer-a"].status is ConsumerStatus.ACCEPTED
    assert state.consumers["consumer-b"].status is ConsumerStatus.OPEN


@pytest.mark.asyncio
async def test_overlay_agent_result_is_canonical_before_package_validation(
    tmp_path: Path,
) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store, "P-prevalidated")
    report = disposition_report("complete", ())

    async def run_steward(_package, _dossier, worktree):
        (worktree / "lean" / "Book.lean").write_text("def base := 2\n", encoding="utf-8")
        return report

    async def validate_package(_worktree, _package):
        (repo / "README.md").write_text("concurrent canonical edit\n", encoding="utf-8")
        run_git(repo, "add", "README.md")
        run_git(repo, "commit", "-m", "docs: concurrent validation edit")
        return PackageValidation(True, "package", "validated before canonical changed")

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=run_steward,
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=validate_package,
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
    )

    result = await runtime.execute(current.id)

    assert result.status is PackageStatus.COMPLETE
    assert (repo / "lean" / "Book.lean").read_text(encoding="utf-8") == "def base := 2\n"
    assert (repo / "README.md").read_text(encoding="utf-8") == "concurrent canonical edit\n"


@pytest.mark.asyncio
async def test_package_lease_is_renewed_during_validation(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store, "P-heartbeat")
    report = disposition_report("complete", ())

    async def validate_package(_worktree, _package):
        await asyncio.sleep(0.7)
        return PackageValidation(True, "package", "slow validation completed")

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=validate_package,
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
        lease_ttl_seconds=0.3,
    )

    result = await runtime.execute(current.id)

    assert result.status is PackageStatus.COMPLETE


@pytest.mark.asyncio
async def test_replan_supersedes_abandoned_incomplete_steps(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store, "P-replan")
    first_report = disposition_report("continue", ())
    first_report["plan_revision"] = {
        "base_revision": 0,
        "revision_reason": "Try the initial interface plan.",
        "steps": [
            {
                "step_id": "abandoned",
                "objective": "Try an interface that evidence later disproves",
                "kind": "interface",
                "intended_declarations": ["abandonedBridge"],
                "intended_paths": ["lean/Book.lean"],
                "depends_on_step_ids": [],
                "validation_commands": ["check abandonedBridge"],
            }
        ],
    }
    second_report = disposition_report("complete", ())
    second_report["plan_revision"] = {
        "base_revision": 1,
        "revision_reason": "Evidence disproved the abandoned interface plan.",
        "steps": [],
    }
    reports = [first_report, second_report]
    validations = 0

    async def validate_package(_worktree, _package):
        nonlocal validations
        validations += 1
        return PackageValidation(
            validations > 1,
            f"package-{validations}",
            "first plan disproved" if validations == 1 else "revised plan validated",
        )

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=reports.pop(0)),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=validate_package,
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "not checked", consumer.id
        ),
    )

    first = await runtime.execute(current.id)
    second = await runtime.execute(current.id)

    assert first.status is PackageStatus.IMPLEMENTING
    assert second.status is PackageStatus.COMPLETE
    assert store.load_package_state().step(current.id, "abandoned").status.value == "superseded"


@pytest.mark.asyncio
async def test_runtime_keeps_consumer_open_when_package_validation_fails(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "P-stale-consumer",
            "stale.consumer",
            "Stale consumer",
            "Do not publish acceptance before integration",
            write_scope=("lean/Book.lean",),
            expansion_scope=("lean/Book.lean",),
        ),
        consumer=PackageConsumer(
            "consumer-a",
            "P-stale-consumer",
            "chapter-a",
            "lean/Book.lean",
            "base",
            "prove",
        ),
    )
    report = disposition_report("continue", ("consumer-a",))
    consumer_validations: list[str] = []
    woken: list[str] = []

    async def wake(ids):
        woken.extend(ids)

    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(False, "package", "failed"),
        validate_consumer=lambda _path, consumer: (
            consumer_validations.append(consumer.id)
            or ConsumerValidation(
                True,
                "consumer",
                "focused consumer check",
                consumer.id,
                (consumer.work_unit_id,),
            )
        ),
        wake_consumers=wake,
    )

    result = await runtime.execute(current.id)

    assert consumer_validations == []
    assert result.status is PackageStatus.IMPLEMENTING
    assert result.accepted_consumer_ids == ()
    assert woken == []
    consumer = store.load_package_state().consumers["consumer-a"]
    assert consumer.status is ConsumerStatus.OPEN
    assert consumer.accepted_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["external", "waiting_dependency", "decomposed"])
async def test_package_execution_persists_nonimplementation_dispositions(
    tmp_path: Path, disposition: str
) -> None:
    repo = git_repo(tmp_path)
    store = StateDatabase(repo / ".paf")
    store.initialize()
    dependency, _ = store.create_or_attach_capability_package(
        CapabilityPackage("dependency", "dependency.key", "Dependency", "Dependency objective")
    )
    dependency = store.update_package_lifecycle(
        dependency.id,
        PackageStatus.COMPLETE,
        disposition=PackageDisposition.IMPLEMENTED,
        integrated_revision="dependency-revision",
        expected_revision=dependency.revision,
    )
    current, _ = store.create_or_attach_capability_package(
        CapabilityPackage(
            "P-disposition",
            f"disposition.{disposition}",
            "Disposition package",
            "Classify remaining work",
            write_scope=("lean/Book.lean",),
            expansion_scope=("lean/Book.lean",),
        ),
        consumer=PackageConsumer(
            "consumer-a",
            "P-disposition",
            "chapter-a",
            "lean/Book.lean",
            "base",
            "prove",
        ),
    )
    children = (
        [
            {
                "capability_key": "child.key",
                "title": "Child capability",
                "mathematical_objective": "Complete the independent child",
                "write_scope": ["lean/Book.lean"],
                "consumer_ids": ["consumer-a"],
            }
        ]
        if disposition == "decomposed"
        else []
    )
    dependencies = (
        [
            {
                "package_id": dependency.id,
                "required_revision": "newer-revision",
                "reason": "The newer dependency interface is required.",
            }
        ]
        if disposition == "waiting_dependency"
        else []
    )
    report = disposition_report(
        disposition,
        ("consumer-a",),
        child_packages=children,
        dependencies=dependencies,
    )
    runtime = PackageExecutionLayer(
        repo,
        repo / ".paf",
        store,
        acquire_workspace=workspace_provider(repo, store),
        run_steward=lambda *_args: asyncio.sleep(0, result=report),
        run_worker=lambda *_args: asyncio.sleep(0, result={}),
        validate_step=lambda *_args: PackageValidation(True, "step", "ok"),
        validate_package=lambda *_args: PackageValidation(True, "package", "ok"),
        validate_consumer=lambda _path, consumer: ConsumerValidation(
            False, "consumer", "still blocked", consumer.id
        ),
    )

    result = await runtime.execute(current.id)

    assert result.status.value == disposition
    state = store.load_package_state()
    if disposition == "decomposed":
        assert state.packages[current.id].status.value == "decomposed"
        assert state.consumers["consumer-a"].package_id != current.id
    elif disposition == "waiting_dependency":
        assert state.dependencies_of(current.id)[0].required_revision == "newer-revision"
        assert current.id not in {package.id for package in runtime.ready_packages()}
    else:
        assert state.consumers["consumer-a"].status is ConsumerStatus.TERMINAL
    if disposition == "decomposed":
        assert {value.package_id for value in state.reservations.values()} == {
            state.consumers["consumer-a"].package_id
        }
    elif disposition != "waiting_dependency":
        assert not state.reservations
