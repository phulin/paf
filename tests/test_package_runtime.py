from __future__ import annotations

import asyncio
import sqlite3
import subprocess
from pathlib import Path

import pytest

from paf.package_model import (
    CapabilityPackage,
    ConsumerStatus,
    IntegrationJournal,
    IntegrationPhase,
    PackageConsumer,
    PathReservation,
    ReservationDecision,
    ReservationMode,
    ReservationOwnerKind,
    ReservationSpec,
)
from paf.package_runtime import (
    ConsumerValidation,
    PackageExecutionLayer,
    PackageGitError,
    PackageIntegrator,
    PackageValidation,
    PackageWorktreeManager,
    RelevantInterfaceGuard,
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


def claim(store: StateDatabase, current: CapabilityPackage, agent: str = "shepherd"):
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
        worktree_head="candidate",
        worktree_status=" M lean/Book.lean",
        dirty_digest="dirty",
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


def prepared_package(repo: Path) -> tuple[StateDatabase, CapabilityPackage, int, Path]:
    store = StateDatabase(repo / ".paf")
    store.initialize()
    current = package(store)
    lease = store.claim_steward_lease(
        current.id,
        "shepherd",
        expected_revision=current.revision,
        ttl_seconds=10**9,
    )
    current = store.load_package_state().packages[current.id]
    current = store.reserve_package_paths(
        (
            PathReservation(
                "lean/Book.lean",
                ReservationMode.EXCLUSIVE_FILE,
                current.id,
                lease.generation,
                EARLY,
            ),
        ),
        expected_revision=current.revision,
    )
    manager = PackageWorktreeManager(repo, repo / ".paf", store)
    current = manager.create(
        current.id,
        lease.generation,
        expected_revision=current.revision,
        base_revision=run_git(repo, "rev-parse", "HEAD"),
    )
    return store, current, lease.generation, Path(current.worktree)


def commit_candidate(worktree: Path, value: int = 2) -> str:
    (worktree / "lean" / "Book.lean").write_text(f"def base := {value}\n", encoding="utf-8")
    run_git(worktree, "add", "lean/Book.lean")
    run_git(worktree, "commit", "-m", f"feat: package value {value}")
    return run_git(worktree, "rev-parse", "HEAD")


def test_package_worktree_recovery_preserves_dirty_inherited_work(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    dirty = worktree / "lean" / "Book.lean"
    dirty.write_text("def base := 99\n", encoding="utf-8")
    manager = PackageWorktreeManager(repo, repo / ".paf", store)

    lease, recovery, snapshot = manager.recover(
        current,
        "replacement",
        expected_revision=current.revision,
        ttl_seconds=10**9,
        now="2100-01-01T00:00:00+00:00",
    )
    assert lease.generation == generation + 1
    assert snapshot is not None and snapshot.dirty_paths == ("lean/Book.lean",)
    assert recovery.dirty_digest == snapshot.dirty_digest
    assert dirty.read_text(encoding="utf-8") == "def base := 99\n"
    with manager.sequential_worker(current.id, lease.generation, "worker") as selected:
        assert selected == worktree


def test_two_phase_integration_retries_stale_base_and_checks_interfaces(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    commit_candidate(worktree)
    guard = RelevantInterfaceGuard(store)
    current = guard.capture(
        current.id,
        generation,
        expected_revision=current.revision,
        interface_ids=("Book.input",),
        source_revision=current.base_revision,
        digest=lambda _: "interface-v1",
    )
    integrator = PackageIntegrator(repo, repo / ".paf", store)
    advanced = False

    def validate(_: Path) -> str:
        nonlocal advanced
        if not advanced:
            advanced = True
            note = repo / "README.md"
            note.write_text("concurrent canonical change\n", encoding="utf-8")
            run_git(repo, "add", "README.md")
            run_git(repo, "commit", "-m", "docs: concurrent change")
        return "validation-v1"

    result = integrator.integrate(
        current.id,
        generation,
        validate=validate,
        interface_digest=lambda _: "interface-v1",
    )
    assert result.integrated
    assert run_git(repo, "rev-parse", "HEAD") == result.canonical_revision
    assert (repo / "lean" / "Book.lean").read_text(encoding="utf-8") == "def base := 2\n"
    assert not store.load_package_state().reservations
    assert any(
        item.phase is IntegrationPhase.ABORTED
        for item in store.load_package_state().integration_journal.values()
    )


def test_integration_rejects_dirty_worktree_and_changed_interface(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    commit_candidate(worktree)
    guard = RelevantInterfaceGuard(store)
    current = guard.capture(
        current.id,
        generation,
        expected_revision=current.revision,
        interface_ids=("Book.input",),
        source_revision=current.base_revision,
        digest=lambda _: "before",
    )
    integrator = PackageIntegrator(repo, repo / ".paf", store)
    dirty = repo / "uncommitted.txt"
    dirty.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(PackageGitError, match="canonical worktree is dirty"):
        integrator.integrate(
            current.id,
            generation,
            validate=lambda _: "validation",
            interface_digest=lambda _: "before",
        )
    dirty.unlink()
    interface = "before"

    def validate(_: Path) -> str:
        nonlocal interface
        interface = "after"
        return "validation"

    result = integrator.integrate(
        current.id,
        generation,
        validate=validate,
        interface_digest=lambda _: interface,
    )
    assert not result.integrated
    assert result.stale_reason.startswith("relevant read interface changed")
    assert run_git(repo, "rev-parse", "HEAD") == current.base_revision


def test_restart_reconciles_canonical_commit_without_reapplying_it(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    candidate = commit_candidate(worktree)
    canonical_before = run_git(repo, "rev-parse", "HEAD")
    journal = IntegrationJournal(
        "interrupted-import",
        current.id,
        generation,
        canonical_before,
        candidate,
        canonical_before,
        IntegrationPhase.IMPORTING,
        validation_digest="validated",
    )
    store.record_integration_journal(journal, expected_revision=current.revision)
    run_git(repo, "merge", "--ff-only", candidate)

    results = PackageIntegrator(repo, repo / ".paf", store).reconcile()

    assert len(results) == 1 and results[0].integrated
    state = store.load_package_state()
    assert state.integration_journal[journal.id].phase is IntegrationPhase.FINALIZED
    assert state.packages[current.id].integrated_revision == candidate
    assert run_git(repo, "rev-list", "--count", "HEAD") == "2"


def test_ordinary_reservation_expiry_cannot_block_a_package_forever(tmp_path: Path) -> None:
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


def test_expired_generation_cannot_integrate_after_recovery(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    commit_candidate(worktree)
    replacement, _ = store.recover_steward_lease(
        current.id,
        "replacement",
        expected_revision=current.revision,
        ttl_seconds=10**9,
        worktree_head=run_git(worktree, "rev-parse", "HEAD"),
        worktree_status="",
        dirty_digest="clean",
        now="2100-01-01T00:00:00+00:00",
    )
    assert replacement.generation > generation
    with pytest.raises(ValueError, match="stale lease generation"):
        PackageIntegrator(repo, repo / ".paf", store).integrate(
            current.id,
            generation,
            validate=lambda _: "validation",
            interface_digest=lambda _: None,
        )


def test_reconciliation_aborts_a_diverged_interrupted_import(tmp_path: Path) -> None:
    repo = git_repo(tmp_path)
    store, current, generation, worktree = prepared_package(repo)
    candidate = commit_candidate(worktree)
    canonical_before = run_git(repo, "rev-parse", "HEAD")
    journal = IntegrationJournal(
        "diverged-import",
        current.id,
        generation,
        canonical_before,
        candidate,
        canonical_before,
        IntegrationPhase.IMPORTING,
        validation_digest="validated",
    )
    store.record_integration_journal(journal, expected_revision=current.revision)
    (repo / "README.md").write_text("diverged\n", encoding="utf-8")
    run_git(repo, "add", "README.md")
    run_git(repo, "commit", "-m", "docs: diverge")

    result = PackageIntegrator(repo, repo / ".paf", store).reconcile()[0]

    assert not result.integrated
    assert store.load_package_state().integration_journal[journal.id].phase is (
        IntegrationPhase.ABORTED
    )


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
        run_git(worktree, "add", "lean/Book.lean")
        run_git(worktree, "commit", "-m", "feat: wire package consumer")
        return {
            "complete": True,
            "summary": "wired the bounded consumer",
            "issues": [],
            "step_id": step.id,
            "changed_declarations": ["base"],
            "changed_paths": ["lean/Book.lean"],
            "commit_id": run_git(worktree, "rev-parse", "HEAD"),
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
