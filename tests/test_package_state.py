from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from paf.config import load_config
from paf.package_model import (
    CapabilityPackage,
    ConsumerStatus,
    EvidenceKind,
    IntegrationJournal,
    IntegrationPhase,
    PackageConsumer,
    PackageDependency,
    PackageDisposition,
    PackageEvidence,
    PackageStatus,
    PackageStep,
    PackageStepKind,
    PackageStepStatus,
    PathReservation,
    RelevantReadInterface,
    ReservationMode,
    StewardLease,
    normalize_capability_key,
    normalize_repository_path,
)
from paf.state import StateStore
from paf.state_db import StateDatabase, read_checkpoint
from tests.support import write_project

NOW = "2100-08-21T00:00:00+00:00"
LATER = "2100-08-21T01:00:00+00:00"


def package(package_id: str, key: str, *paths: str) -> CapabilityPackage:
    return CapabilityPackage(
        id=package_id,
        capability_key=key,
        title=f"Capability {key}",
        mathematical_objective=f"Implement {key}",
        write_scope=paths,
        expansion_scope=paths,
        created_at=NOW,
        updated_at=NOW,
    )


def consumer(consumer_id: str, package_id: str, declaration: str) -> PackageConsumer:
    return PackageConsumer(
        id=consumer_id,
        package_id=package_id,
        work_unit_id="book/chapter-02",
        path="lean/Book/Chapter02.lean",
        declaration=declaration,
        stage="prove",
        residual_goal="⊢ Result x",
        blocker_ids=(f"blocker-{consumer_id}",),
        attempted_routes=("simp", "exact candidate"),
        acceptance_contract={"tests": ["lake build +Book.Chapter02"]},
        created_at=NOW,
        updated_at=NOW,
    )


def database(tmp_path: Path) -> StateDatabase:
    result = StateDatabase(tmp_path / ".paf")
    result.initialize()
    return result


def test_package_model_normalizes_identity_and_repository_paths() -> None:
    value = package("package-a", "  Book.Transport\n Result  ", "lean\\Book\\Chapter01.lean")
    assert value.capability_key == "book.transport result"
    assert value.write_scope == ("lean/Book/Chapter01.lean",)


def test_package_model_rejects_escaping_paths() -> None:
    assert normalize_capability_key("  A\tB  ") == "a b"
    assert normalize_repository_path("lean\\Book\\Chapter01.lean") == ("lean/Book/Chapter01.lean")
    with pytest.raises(ValueError, match="repository-relative"):
        normalize_repository_path("../outside")
    with pytest.raises(ValueError, match="repository-relative"):
        package("package-a", "key", "/tmp/outside")


def test_plan_step_ids_are_local_to_their_package(tmp_path: Path) -> None:
    store = database(tmp_path)
    first, _ = store.create_or_attach_capability_package(package("package-a", "key.a"))
    second, _ = store.create_or_attach_capability_package(package("package-b", "key.b"))

    for current in (first, second):
        store.upsert_package_step(
            PackageStep(
                id="validate-package",
                package_id=current.id,
                objective=f"Validate {current.id}",
                kind=PackageStepKind.VALIDATION,
                status=PackageStepStatus.READY,
            ),
            expected_revision=current.revision,
        )

    state = store.load_package_state()
    assert state.step("package-a", "validate-package").objective == "Validate package-a"
    assert state.step("package-b", "validate-package").objective == "Validate package-b"


@pytest.mark.asyncio
async def test_package_mutations_publish_dashboard_projection(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    baseline = state.revision

    created, _ = await state.create_or_attach_capability_package(
        package("package-live", "Book.LiveCapability", "lean/Book/Chapter01.lean")
    )

    delta = state._database.dashboard_delta(baseline)
    assert delta["revision"] > baseline
    assert delta["globals"]["capability_packages"][created.id]["status"] == "observed"
    assert delta["globals"]["package_consumers"] == {}
    await state.close()


def test_create_deduplicates_capability_and_persists_normalized_records(
    tmp_path: Path,
) -> None:
    store = database(tmp_path)
    first, created = store.create_or_attach_capability_package(
        package("package-a", "Book.Transport Result", "lean/Book/Chapter01.lean"),
        consumer=consumer("consumer-a", "package-a", "Book.consumerA"),
        evidence=(
            PackageEvidence(
                id="evidence-a",
                package_id="package-a",
                producer="proof-review",
                kind=EvidenceKind.RESIDUAL_GOAL,
                paths=("lean/Book/Chapter02.lean",),
                declarations=("Book.consumerA",),
                payload={"goal": "⊢ Result x"},
                digest="digest-a",
                created_at=NOW,
            ),
        ),
    )
    second, created_again = store.create_or_attach_capability_package(
        package("ignored-id", " book.transport\tresult "),
        consumer=consumer("consumer-b", "ignored-id", "Book.consumerB"),
        expected_revision=first.revision,
    )

    state = store.load_package_state()
    assert created and not created_again
    assert second.id == first.id == "package-a"
    assert set(state.packages) == {"package-a"}
    assert set(state.consumers) == {"consumer-a", "consumer-b"}
    assert state.evidence["evidence-a"].payload == {"goal": "⊢ Result x"}
    assert state.packages["package-a"].write_scope == ("lean/Book/Chapter01.lean",)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT count(*) FROM capability_packages").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM package_consumers").fetchone() == (2,)


@pytest.mark.parametrize("terminal", [PackageStatus.COMPLETE, PackageStatus.EXTERNAL])
def test_new_consumer_reopens_a_terminal_capability_owner(
    tmp_path: Path, terminal: PackageStatus
) -> None:
    store = database(tmp_path)
    current, _ = store.create_or_attach_capability_package(package("package-a", "key.a"))
    current = store.update_package_lifecycle(
        current.id,
        terminal,
        disposition=(
            PackageDisposition.IMPLEMENTED
            if terminal is PackageStatus.COMPLETE
            else PackageDisposition.EXTERNAL
        ),
        expected_revision=current.revision,
    )

    reopened, created = store.create_or_attach_capability_package(
        package("ignored", "key.a"),
        consumer=consumer("consumer-new", "ignored", "Book.newConsumer"),
        expected_revision=current.revision,
    )

    assert not created
    assert reopened.status is PackageStatus.OBSERVED
    assert reopened.disposition is None
    assert store.load_package_state().consumers["consumer-new"].status is ConsumerStatus.OPEN


def test_package_mutations_are_revisioned_and_lease_fenced(tmp_path: Path) -> None:
    store = database(tmp_path)
    current, _ = store.create_or_attach_capability_package(package("package-a", "key.a"))
    current = store.put_steward_lease(
        StewardLease("package-a", "agent-a", 3, NOW, NOW, LATER),
        expected_revision=current.revision,
    )
    current = store.reserve_package_paths(
        (
            PathReservation(
                "lean/Book/Chapter01.lean",
                ReservationMode.EXCLUSIVE_FILE,
                "package-a",
                3,
                NOW,
            ),
        ),
        expected_revision=current.revision,
    )
    current = store.upsert_package_step(
        PackageStep(
            id="step-a",
            package_id="package-a",
            objective="Prove a small bridge",
            kind=PackageStepKind.SUPPORTING_LEMMA,
            status=PackageStepStatus.READY,
            intended_declarations=("Book.bridge",),
            intended_paths=("lean/Book/Chapter01.lean",),
            validation_contract={"commands": ["lake build +Book.Chapter01"]},
            created_at=NOW,
            updated_at=NOW,
        ),
        expected_revision=current.revision,
        lease_generation=3,
    )
    current = store.replace_relevant_read_interfaces(
        "package-a",
        (RelevantReadInterface("package-a", "Book.input", "api-digest", "base"),),
        expected_revision=current.revision,
        lease_generation=3,
    )
    current = store.record_integration_journal(
        IntegrationJournal(
            "journal-a",
            "package-a",
            3,
            "base",
            "candidate",
            "canonical",
            IntegrationPhase.PREPARED,
            created_at=NOW,
            updated_at=NOW,
        ),
        expected_revision=current.revision,
    )

    state = store.load_package_state()
    assert state.step("package-a", "step-a").intended_declarations == ("Book.bridge",)
    assert state.leases["package-a"].generation == 3
    assert state.reservations["lean/Book/Chapter01.lean"].package_id == "package-a"
    assert state.relevant_read_interfaces[0].digest == "api-digest"
    assert state.integration_journal["journal-a"].phase is IntegrationPhase.PREPARED
    current = store.upsert_package_step(
        PackageStep(
            id="step-b",
            package_id="package-a",
            objective="Use the bridge",
            kind=PackageStepKind.CONSUMER_INTEGRATION,
            depends_on_step_ids=("step-a",),
            created_at=NOW,
            updated_at=NOW,
        ),
        expected_revision=current.revision,
        lease_generation=3,
    )
    with pytest.raises(ValueError, match="cycle"):
        store.upsert_package_step(
            replace(state.step("package-a", "step-a"), depends_on_step_ids=("step-b",)),
            expected_revision=current.revision,
            lease_generation=3,
        )
    assert store.load_package_state().step("package-a", "step-a").depends_on_step_ids == ()
    with pytest.raises(ValueError, match="stale package revision"):
        store.append_package_evidence(
            PackageEvidence(
                "late", "package-a", "agent-a", EvidenceKind.DIAGNOSTIC, created_at=NOW
            ),
            expected_revision=current.revision - 1,
            lease_generation=3,
        )
    with pytest.raises(ValueError, match="stale lease generation"):
        store.append_package_evidence(
            PackageEvidence(
                "fenced", "package-a", "agent-a", EvidenceKind.DIAGNOSTIC, created_at=NOW
            ),
            expected_revision=current.revision,
            lease_generation=2,
        )


def test_dependencies_reject_cycles_and_merge_transfers_ownership(tmp_path: Path) -> None:
    store = database(tmp_path)
    first, _ = store.create_or_attach_capability_package(
        package("package-a", "key.a", "lean/Book/A.lean"),
        consumer=consumer("consumer-a", "package-a", "Book.a"),
    )
    second, _ = store.create_or_attach_capability_package(
        package("package-b", "key.b", "lean/Book/B.lean"),
        consumer=consumer("consumer-b", "package-b", "Book.b"),
    )
    first = store.add_package_dependency(
        PackageDependency("package-a", "package-b", created_at=NOW),
        expected_revision=first.revision,
    )
    with pytest.raises(ValueError, match="cycle"):
        store.add_package_dependency(
            PackageDependency("package-b", "package-a", created_at=NOW),
            expected_revision=second.revision,
        )

    merged = store.merge_capability_packages(
        "package-a",
        "package-b",
        expected_survivor_revision=first.revision,
        expected_merged_revision=second.revision,
    )
    state = store.load_package_state()
    assert merged.status is PackageStatus.OBSERVED
    assert state.packages["package-b"].status is PackageStatus.SUPERSEDED
    assert {value.package_id for value in state.consumers.values()} == {"package-a"}
    assert "key.b" in state.packages["package-a"].aliases
    deduplicated, created = store.create_or_attach_capability_package(
        package("not-created", "key.b")
    )
    assert not created and deduplicated.id == "package-a"


def test_split_moves_open_consumers_dependencies_and_reservations(tmp_path: Path) -> None:
    store = database(tmp_path)
    parent, _ = store.create_or_attach_capability_package(
        package("parent", "parent.key", "lean/Book/A.lean", "lean/Book/B.lean"),
        consumer=consumer("consumer-a", "parent", "Book.a"),
    )
    attached = store.attach_package_consumer(
        "parent",
        replace(consumer("consumer-b", "parent", "Book.b"), path="lean/Book/B.lean"),
        expected_revision=parent.revision,
    )
    parent = store.load_package_state().packages["parent"]
    parent = store.put_steward_lease(
        StewardLease("parent", "agent", 1, NOW, NOW, LATER),
        expected_revision=parent.revision,
    )
    parent = store.reserve_package_paths(
        (
            PathReservation("lean/Book/A.lean", ReservationMode.EXCLUSIVE_FILE, "parent", 1, NOW),
            PathReservation("lean/Book/B.lean", ReservationMode.EXCLUSIVE_FILE, "parent", 1, NOW),
        ),
        expected_revision=parent.revision,
    )
    children = store.split_capability_package(
        "parent",
        (
            package("child-a", "child.a", "lean/Book/A.lean"),
            package("child-b", "child.b", "lean/Book/B.lean"),
        ),
        {"child-a": ("consumer-a",), "child-b": (attached.id,)},
        expected_revision=parent.revision,
        lease_generation=1,
    )
    state = store.load_package_state()
    assert {value.id for value in children} == {"child-a", "child-b"}
    assert state.packages["parent"].status is PackageStatus.DECOMPOSED
    assert state.consumers["consumer-a"].package_id == "child-a"
    assert state.consumers["consumer-b"].package_id == "child-b"
    assert state.reservations["lean/Book/A.lean"].package_id == "child-a"
    assert "parent" not in state.leases


@pytest.mark.asyncio
async def test_state_store_package_api_refreshes_derived_checkpoint_view(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    await state.load_or_create()
    created, is_new = await state.create_or_attach_capability_package(
        package("package-state", "state.key"),
        consumer=consumer("consumer-state", "package-state", "Book.stateConsumer"),
    )
    assert is_new
    planned = await state.update_package_lifecycle(
        created.id,
        PackageStatus.PLANNED,
        expected_revision=created.revision,
        plan_revision=1,
    )
    assert planned.plan_revision == 1
    assert state.hot_snapshot()["capability_packages"][created.id]["status"] == "planned"
    await state.close()

    restored = StateStore(config)
    await restored.load_or_create()
    assert restored.package_state.packages[created.id].status is PackageStatus.PLANNED
    assert restored.package_state.consumers_for(created.id)[0].status is ConsumerStatus.OPEN
    await restored.close()


@pytest.mark.asyncio
async def test_legacy_upstream_state_imports_once_without_runtime_projection(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    request_id = "legacy-request"
    imported_ids = state._database.import_legacy_upstream_state(
        {
            request_id: {
                "capability_key": "Book.SharedBridge",
                "consumer_chapter_id": config.chapters[1].id,
                "blocked_declaration": "Book.consumer",
                "consumer_path": "lean/Book/Chapter02.lean",
                "residual_goal": "⊢ Result x",
                "needed_result": "A shared bridge",
                "owner_paths": ["lean/Book/Chapter01.lean"],
                "acceptance_tests": ["lake build +Book.Chapter02"],
            }
        }
    )
    await state.refresh_package_state()
    package_id = imported_ids[request_id]
    assert imported_ids == {request_id: package_id}
    original = state.package_state.packages[package_id]

    assert state._database.import_legacy_upstream_state({request_id: {}}) == {
        request_id: package_id
    }
    durable = state._database.load_package_state()
    assert durable.packages[package_id].status == original.status
    assert durable.packages[package_id].revision == original.revision
    assert len(durable.evidence) == 1
    await state.close()

    checkpoint = read_checkpoint(config.settings.state_dir)
    assert checkpoint is not None
    assert "upstream_requests" not in checkpoint
    assert "upstream_request_imports" not in checkpoint
    assert checkpoint["capability_packages"][package_id]["status"] == "observed"
    assert checkpoint["package_consumers"]


def test_legacy_owner_hypotheses_do_not_bypass_steward_disposition(tmp_path: Path) -> None:
    store = database(tmp_path)
    requests = {
        "shared": {
            "capability_key": "Book.Shared",
            "owner_kind": "shared",
            "consumer_chapter_id": "book/chapter-02",
            "consumer_path": "lean/Book/Chapter02.lean",
            "blocked_declaration": "Book.sharedConsumer",
            "needed_result": "A shared bridge",
        },
        "external-proposal": {
            "capability_key": "Book.ProposedExternal",
            "owner_kind": "external",
            "consumer_chapter_id": "book/chapter-02",
            "consumer_path": "lean/Book/Chapter02.lean",
            "blocked_declaration": "Book.externalConsumer",
            "needed_result": "A possibly external bridge",
        },
        "confirmed-external": {
            "capability_key": "Book.ConfirmedExternal",
            "owner_kind": "external",
            "consumer_chapter_id": "book/chapter-02",
            "consumer_path": "lean/Book/Chapter02.lean",
            "blocked_declaration": "Book.confirmedExternalConsumer",
            "needed_result": "An unavailable external theorem",
            "answer": {
                "disposition": "external",
                "rejection_reason": "The dependency is unavailable.",
            },
        },
    }

    imported = store.import_legacy_upstream_state(requests)
    state = store.load_package_state()

    assert state.packages[imported["shared"]].status is PackageStatus.OBSERVED
    assert state.packages[imported["external-proposal"]].status is PackageStatus.OBSERVED
    confirmed = state.packages[imported["confirmed-external"]]
    assert confirmed.status is PackageStatus.EXTERNAL
    assert confirmed.disposition is PackageDisposition.EXTERNAL
    assert state.consumers_for(confirmed.id)[0].status is ConsumerStatus.TERMINAL
