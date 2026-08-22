import asyncio
import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import replace
from pathlib import Path
from threading import Event, get_ident
from typing import Any

import pytest

import paf.scheduler as scheduler_module
import paf.state as state_module
from paf.codex import (
    DIAGNOSTIC_REVIEW_ROLE,
    PROOF_REVIEW_ROLE,
    WARNING_REVIEW_ROLE,
    AgentResult,
    CodexExecutor,
    ValidationResult,
    ValidationStatus,
    scope_digest,
    scoped_files,
)
from paf.config import load_config
from paf.corpus import WorkUnitImportGraph
from paf.git import GitCommitError
from paf.hashing import stable_digest_text
from paf.models import Chapter, PipelineConfig, ProofTarget, Stage
from paf.package_model import CapabilityPackage, PackageStatus, ReservationResult, ReservationSpec
from paf.package_runtime import PackageExecutionResult
from paf.scheduler import (
    BUILD_ERROR_REVIEW_KIND,
    BUILD_WARNING_REVIEW_KIND,
    COORDINATOR_VERIFICATION_RETRY_DETAIL,
    Attempt,
    ExecutionDisposition,
    Orchestrator,
    StageOutcome,
)
from paf.state import (
    ProofBlockerStatus,
    RunRecord,
    StateStore,
    TaskPhase,
    TaskStatus,
    TokenUsage,
    UpstreamRequestStatus,
)
from paf.state_db import read_checkpoint
from tests.support import write_project


class FakeExecutor(CodexExecutor):
    def __init__(self, state: StateStore, results: list[AgentResult]) -> None:
        self.state = state
        self.results = results
        self.feedbacks: list[str] = []
        self.resume_thread_ids: list[str | None] = []
        self.resume_prompts: list[str] = []

    async def run(
        self,
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        del chapter, stage, workspace_root
        self.feedbacks.append(feedback)
        self.resume_thread_ids.append(None)
        self.resume_prompts.append("")
        result = self.results.pop(0)
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            changed=result.changed,
            placeholders=result.placeholders,
            report=result.report,
            usage=result.usage,
            thread_id=result.thread_id,
        )
        return result

    async def resume(
        self,
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        thread_id: str,
        previous_run_id: str,
        reminder: str,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        del chapter, stage, workspace_root
        self.feedbacks.append(feedback)
        self.resume_thread_ids.append(thread_id)
        self.resume_prompts.append(reminder)
        result = self.results.pop(0)
        await self.state.update_run(
            run,
            thread_id=thread_id,
            resumed_from_run_id=previous_run_id,
        )
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            changed=result.changed,
            placeholders=result.placeholders,
            report=result.report,
            usage=result.usage,
            thread_id=result.thread_id or thread_id,
        )
        return result


@pytest.mark.asyncio
async def test_package_validation_deduplicates_real_work_units_by_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = object.__new__(Orchestrator)
    orchestrator.config = config
    unit = config.work_units[0]
    calls: list[str] = []

    async def validation(
        _config: PipelineConfig,
        current: Any,
        **_kwargs: object,
    ) -> ValidationResult:
        calls.append(current.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    result = await orchestrator._validate_package_units(
        tmp_path,
        (unit, unit),
        scope=("Book/Chapter01/Target.lean",),
    )

    assert result.succeeded
    assert calls == [unit.id]


@pytest.mark.asyncio
async def test_package_validation_ignores_inherited_warnings_outside_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = object.__new__(Orchestrator)
    orchestrator.config = config
    unit = config.work_units[0]

    async def validation(
        _config: PipelineConfig,
        _current: Any,
        **_kwargs: object,
    ) -> ValidationResult:
        return ValidationResult(
            False,
            1,
            "warning: Book/Chapter01/Unrelated.lean:10:2: unused variable `h`\n\n"
            "Coordinator rejected 1 non-sorry Lean warning(s):\n"
            "warning: Book/Chapter01/Unrelated.lean:10:2: unused variable `h`",
            process_exit_code=0,
        )

    monkeypatch.setattr(scheduler_module, "validate", validation)

    result = await orchestrator._validate_package_units(
        tmp_path,
        (unit,),
        scope=("Book/Chapter01/Target.lean",),
    )

    assert result.succeeded
    assert "inherited warnings outside package scope" in result.evidence


@pytest.mark.asyncio
async def test_package_validation_rejects_warnings_inside_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = object.__new__(Orchestrator)
    orchestrator.config = config
    unit = config.work_units[0]

    async def validation(
        _config: PipelineConfig,
        _current: Any,
        **_kwargs: object,
    ) -> ValidationResult:
        return ValidationResult(
            False,
            1,
            "warning: Book/Chapter01/Target.lean:10:2: unused variable `h`\n\n"
            "Coordinator rejected 1 non-sorry Lean warning(s):\n"
            "warning: Book/Chapter01/Target.lean:10:2: unused variable `h`",
            process_exit_code=0,
        )

    monkeypatch.setattr(scheduler_module, "validate", validation)

    result = await orchestrator._validate_package_units(
        tmp_path,
        (unit,),
        scope=("Book/Chapter01/Target.lean",),
    )

    assert not result.succeeded
    assert result.evidence.count("unused variable `h`") == 1
    assert "within package scope" in result.evidence


@pytest.mark.asyncio
async def test_package_drain_schedules_newly_unblocked_dependencies(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.state.load_or_create()
    orchestrator.git.enabled = True
    prerequisite = CapabilityPackage("prerequisite", "key.a", "A", "Implement A")
    dependent = CapabilityPackage("dependent", "key.b", "B", "Implement B")
    executed: list[str] = []

    class FakePackageExecution:
        def ready_packages(self):
            if not executed:
                return (prerequisite,)
            if executed == [prerequisite.id]:
                return (dependent,)
            return ()

        async def execute(self, package_id: str) -> PackageExecutionResult:
            executed.append(package_id)
            return PackageExecutionResult(package_id, 1, PackageStatus.COMPLETE)

    orchestrator.package_execution = FakePackageExecution()  # ty: ignore[invalid-assignment]

    assert not await orchestrator._schedule_ready_packages()
    results = await orchestrator._drain_active_packages()

    assert executed == []
    assert results == ()
    await orchestrator.state.close()


@pytest.mark.asyncio
async def test_package_drain_continues_nonterminal_package_turns(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.state.load_or_create()
    orchestrator.git.enabled = True
    package = CapabilityPackage("package", "key", "Package", "Implement package")
    executions = 0

    class FakePackageExecution:
        def ready_packages(self):
            return (package,) if executions < 2 else ()

        async def execute(self, package_id: str) -> PackageExecutionResult:
            nonlocal executions
            executions += 1
            return PackageExecutionResult(
                package_id,
                executions,
                PackageStatus.IMPLEMENTING if executions == 1 else PackageStatus.COMPLETE,
            )

    orchestrator.package_execution = FakePackageExecution()  # ty: ignore[invalid-assignment]

    results = await orchestrator._drain_active_packages()

    assert executions == 0
    assert results == ()
    assert not orchestrator._package_tasks
    await orchestrator.state.close()


@pytest.mark.asyncio
async def test_package_scheduler_limits_concurrency_per_work_unit(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.state.load_or_create()
    orchestrator.git.enabled = True
    packages = tuple(
        CapabilityPackage(f"package-{index}", f"key.{index}", f"Package {index}", "Implement")
        for index in range(3)
    )
    for package in packages:
        orchestrator.state._database.create_or_attach_capability_package(package)
    completed: set[str] = set()

    class FakePackageExecution:
        def ready_packages(self):
            return tuple(package for package in packages if package.id not in completed)

        async def execute(self, package_id: str) -> PackageExecutionResult:
            completed.add(package_id)
            return PackageExecutionResult(package_id, 1, PackageStatus.COMPLETE)

    orchestrator.package_execution = FakePackageExecution()  # ty: ignore[invalid-assignment]

    assert not await orchestrator._schedule_ready_packages()
    assert not orchestrator._package_tasks
    results = await orchestrator._drain_active_packages()

    assert results == ()
    assert completed == set()
    assert not orchestrator._package_tasks
    await orchestrator.state.close()


def test_coordinator_build_output_shortens_diagnostic_paths(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)

    state.append_coordinator_build_output(
        "error: lean/LastLib/Book05LocalClassFieldTheory/Chapter07/"
        "Section07WhyFrobeniusIsCanonical.lean:12:3: broken\n"
        "warning: lean/LastLib/Book05LocalClassFieldTheory/Chapter07/"
        "Section07WhyFrobeniusIsCanonical.lean:13:3: declaration uses `sorry`\n"
        "warning: lean/LastLib/Book05LocalClassFieldTheory/Chapter07/"
        "Section07WhyFrobeniusIsCanonical.lean:14:3: unused variable"
    )

    assert state.coordinator_build.output_tail == [
        "error: [Book 5 Chap 7 Sec 7: Why Frobenius Is Canonical]:12:3: broken",
        "warning: [Book 5 Chap 7 Sec 7: Why Frobenius Is Canonical]:13:3: declaration uses `sorry`",
        "warning: [Book 5 Chap 7 Sec 7: Why Frobenius Is Canonical]:14:3: unused variable",
    ]
    assert state.coordinator_build.error_count == 1
    assert state.coordinator_build.warning_count == 1


@pytest.mark.asyncio
async def test_coordinator_build_output_tracks_lake_progress(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    await state.start_coordinator_build(
        mode="optimistic",
        stage=Stage.FORMALIZE,
        iteration=1,
        maximum_iterations=10,
        total=2,
    )

    state.append_coordinator_build_output(
        "\x1b[32m✔ [7/12] Built LastLib.Book.Chapter01.Section\x1b[0m\n"
    )
    assert state.coordinator_build.completed == 7
    assert state.coordinator_build.total == 12
    assert state.coordinator_build.current_chapter_id == "LastLib.Book.Chapter01.Section"

    state.append_coordinator_build_output("✔ [5/12] Built LastLib.Book.Chapter02.Section\n")
    assert state.coordinator_build.completed == 7


def failed_attempt(
    obstruction: str,
    *,
    path: str = "lean/Book/Chapter01.lean",
    declaration: str = "Book.target",
) -> dict[str, Any]:
    return {
        "path": path,
        "declaration": declaration,
        "attempts": ["Tried the source proof with exact lemmas.", "Unfolded the definition."],
        "remaining_goal": "⊢ True",
        "obstruction": obstruction,
    }


def unresolved_proof(
    obstruction: str,
    *,
    path: str = "lean/Book/Chapter01.lean",
    declaration: str = "Book.target",
    kind: str = "local_proof_failure",
    upstream_hypothesis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "path": path,
        "declaration": declaration,
        "attempts": [
            {
                "strategy": "Apply the source argument through the available exact lemmas",
                "probe": "by\n  exact source_route",
                "outcome": "Lean leaves the stated residual goal.",
            }
        ],
        "remaining_goal": "⊢ True",
        "obstruction": obstruction,
        "evidence": "A focused Lean probe reproduced this obstruction.",
        "kind": kind,
        "upstream_hypothesis": upstream_hypothesis,
    }


def test_proof_observations_are_normalized_for_coordinator_routing() -> None:
    local = unresolved_proof("the construction remains unfinished")
    suspected = unresolved_proof(
        "the source requires an omitted hypothesis",
        kind="suspected_statement_defect",
    )

    local_record, suspected_record = scheduler_module._proof_blocker_records(
        {"unresolved_proofs": [local, suspected]}
    )

    assert local_record["disposition"] == "retry"
    assert suspected_record["disposition"] == "statement_review"
    assert "Strategy:" in local_record["attempts"][0]
    assert "Probe:" in local_record["attempts"][0]
    assert "Observed outcome:" in local_record["attempts"][0]


def test_same_scope_upstream_observation_is_rejected_as_local_failure() -> None:
    claimed_upstream = unresolved_proof(
        "the assigned construction needs a helper lemma",
        kind="suspected_upstream_gap",
        upstream_hypothesis={
            "capability_key": "Book.localHelper",
            "owner_kind": "chapter",
            "owner_paths": ["lean/Book/Chapter01.lean"],
            "needed_result": "A helper for the assigned construction",
        },
    )

    [record] = scheduler_module._proof_blocker_records(
        {"unresolved_proofs": [claimed_upstream]},
        upstream_owner_is_local=lambda _blocked, owner: owner == "lean/Book/Chapter01.lean",
    )

    assert record["disposition"] == "retry"
    assert record["capability"] is None
    assert "Coordinator rejected the upstream classification" in record["obstruction"]


@pytest.mark.asyncio
async def test_same_scope_upstream_blocker_reference_is_downgraded(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    capability = {
        "capability_key": "Book.localHelper",
        "blocked_declaration": "Book.target",
        "consumer_path": "lean/Book/Chapter01.lean",
        "residual_goal": "⊢ True",
        "needed_result": "A helper in the assigned chapter",
        "owner_paths": ["lean/Book/Chapter01.lean"],
    }
    [blocker] = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="run-1",
        failed_attempts=[
            failed_attempt(
                "the local helper is missing",
                path="lean/Book/Chapter01.lean",
                declaration="Book.target",
            )
            | {"disposition": "missing_capability"}
        ],
        capability_candidates=[capability],
    )
    run = await state.start_run(chapter.id, Stage.PROVE)

    [updated] = await orchestrator._record_proof_blocker_deltas(
        chapter,
        run,
        {"unresolved_proofs": [], "blocker_refs": [blocker["id"]]},
    )

    assert updated["disposition"] == "retry"
    assert "capability" not in updated
    assert await orchestrator._request_upstream_for_blockers(chapter, [updated]) == ()
    assert state.upstream_requests == {}
    await orchestrator.shutdown()


def result(
    *,
    changed: bool,
    placeholders: int = 2,
    complete: bool = True,
    issues: list[str] | None = None,
    failed_attempts: list[dict[str, Any]] | None = None,
    unresolved_proofs: list[dict[str, Any]] | None = None,
    finding_assessments: list[dict[str, Any]] | None = None,
) -> AgentResult:
    report: dict[str, Any] = {
        "complete": complete,
        "summary": "reviewed",
        "issues": issues or [],
    }
    if failed_attempts is not None:
        report["failed_attempts"] = failed_attempts
    if unresolved_proofs is not None:
        report["unresolved_proofs"] = unresolved_proofs
    if finding_assessments is not None:
        report["finding_assessments"] = finding_assessments
    return AgentResult(
        succeeded=True,
        exit_code=0,
        changed=changed,
        placeholders=placeholders,
        usage=TokenUsage(input_tokens=10, output_tokens=5, measured=True),
        report=report,
    )


def finding_resolution(
    finding_id: str,
    *,
    action: str,
    diagnosis: str = "consumer_local_proof",
    explanation: str = "checked against the source",
    retry_contract: dict[str, Any] | None = None,
    capability: dict[str, Any] | None = None,
    dependency_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "finding_id": finding_id,
        "finding": "Book.target remains unproved",
        "diagnosis": diagnosis,
        "action": action,
        "explanation": explanation,
        "retry_contract": retry_contract,
        "upstream_request": capability,
        "dependency_ids": dependency_ids or [],
    }


def executable_retry_contract() -> dict[str, Any]:
    return {
        "new_information": "A checked finite-presentation induction route is now available.",
        "declarations": ["Book.induction_step"],
        "intermediate_claims": ["reduce to the finite presentation"],
        "critical_probe": "lean_goal accepted `apply Book.induction_step`",
        "known_remaining_gap": "",
    }


async def mark_discovered(
    orchestrator: Orchestrator,
    dependencies: dict[str, tuple[str, ...]] | None = None,
) -> None:
    """Persist a current source tree when a test starts at formalization."""

    dependencies = dependencies or {}
    await asyncio.gather(
        *(
            orchestrator._persist_source_dependencies(
                chapter,
                tuple(dict.fromkeys((*chapter.depends_on, *dependencies.get(chapter.id, ())))),
                {"summary": "test dependency tree", "issues": []},
            )
            for chapter in orchestrator.work_units
        )
    )


async def mark_formalized(
    orchestrator: Orchestrator,
    dependencies: dict[str, tuple[str, ...]] | None = None,
) -> None:
    """Start a review-focused test from a valid formalized frontier."""

    await mark_discovered(orchestrator, dependencies)
    await orchestrator.state.set_tasks(
        (chapter.id for chapter in orchestrator.work_units),
        Stage.FORMALIZE,
        TaskStatus.SUCCEEDED,
        "clean formalization",
    )


async def mark_clean_formalization(
    orchestrator: Orchestrator,
    dependencies: dict[str, tuple[str, ...]] | None = None,
) -> None:
    """Persist clean build records for tests focused on review or proof work."""

    await mark_formalized(orchestrator, dependencies)
    graph = orchestrator._observed_work_unit_graph()
    orchestrator.state.formalize_graph = graph.snapshot() | {
        "algorithm": "source-dependency-tree",
        "revision": 1,
        "build_generation": len(orchestrator.work_units),
        "clean": {
            chapter.id: {"source_digest": scope_digest(orchestrator.config.settings.repo, chapter)}
            for chapter in orchestrator.work_units
        },
        "dirty": [],
    }
    await orchestrator.state.save()


def legacy_scope_digest(root: Path, chapter: Chapter) -> str:
    digest = hashlib.sha256()
    for path in scoped_files(root, chapter):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def test_source_input_digests_read_shared_document_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    source = tmp_path / "books" / "book.md"
    original_read_text = Path.read_text
    reads: list[Path] = []

    def counted_read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == source:
            reads.append(path)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted_read_text)

    digests = orchestrator._source_input_digests(config.chapters)

    assert set(digests) == {chapter.id for chapter in config.chapters}
    assert reads == [source]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lean_source", "expected_proof_status", "expected_sorries"),
    [
        ("theorem target : True := by sorry\n", TaskStatus.PENDING, 1),
        ("theorem target : True := by trivial\n", TaskStatus.SUCCEEDED, 0),
    ],
)
async def test_changed_textbook_source_reopens_review_and_only_retries_incomplete_proofs(
    tmp_path: Path,
    lean_source: str,
    expected_proof_status: TaskStatus,
    expected_sorries: int,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    target = tmp_path / "lean" / "Book" / "Chapter01.lean"
    target.parent.mkdir(parents=True)
    target.write_text(lean_source, encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    await orchestrator.state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    await orchestrator.state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "proved",
        source_digest=scope_digest(config.settings.repo, chapter),
    )

    textbook = tmp_path / chapter.source
    textbook.write_text(
        textbook.read_text(encoding="utf-8").replace("Text.", "Revised text."),
        encoding="utf-8",
    )

    assert await orchestrator._reconcile_changed_source_inputs() == {chapter.id}
    assert orchestrator.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.PENDING
    assert orchestrator.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert proof.status == expected_proof_status
    assert proof.sorry_count == expected_sorries
    assert await orchestrator._reconcile_changed_source_inputs() == set()
    await orchestrator.shutdown()


def test_observed_graph_is_reused_until_dependency_state_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    original_build = scheduler_module.build_source_dependency_graph
    builds = 0

    def counted_build(*args: Any, **kwargs: Any) -> Any:
        nonlocal builds
        builds += 1
        return original_build(*args, **kwargs)

    monkeypatch.setattr(scheduler_module, "build_source_dependency_graph", counted_build)

    first = orchestrator._observed_work_unit_graph()
    second = orchestrator._observed_work_unit_graph()
    assert first is second
    assert builds == 1

    orchestrator.state.source_dependency_tree = dict(orchestrator.state.source_dependency_tree)
    third = orchestrator._observed_work_unit_graph()
    assert third is not second
    assert builds == 2


def test_current_interface_graph_is_reused_for_freshness_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = orchestrator._observed_work_unit_graph()
    orchestrator.state.formalize_graph = {
        "interface_stale": [first.id],
        "interface_imports": {first.id: [], second.id: [first.id]},
    }
    original_build = scheduler_module.build_compiled_import_graph
    builds = 0

    def counted_build(*args: Any, **kwargs: Any) -> Any:
        nonlocal builds
        builds += 1
        return original_build(*args, **kwargs)

    monkeypatch.setattr(scheduler_module, "build_compiled_import_graph", counted_build)

    assert not orchestrator._interface_dependencies_are_current(graph, first.id)
    assert not orchestrator._interface_dependencies_are_current(graph, second.id)
    assert builds == 1


@pytest.mark.asyncio
async def test_prepare_migrates_legacy_discovery_and_build_digests(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem complete : True := by trivial\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()

    lines = (tmp_path / chapter.source).read_text(encoding="utf-8").splitlines()
    selected = "\n".join(lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line])
    legacy_discovery = stable_digest_text(f"{chapter.id}\0{selected}")
    legacy_scope = legacy_scope_digest(tmp_path, chapter)
    orchestrator.state.source_dependency_tree = {
        "nodes": {chapter.id: {"dependencies": [], "source_digest": legacy_discovery}}
    }
    orchestrator.state.formalize_graph = {
        "clean": {chapter.id: {"source_digest": legacy_scope, "build_generation": 1}}
    }
    await orchestrator.state.set_task(
        chapter.id,
        Stage.DISCOVER,
        TaskStatus.PENDING,
        "queued by the unversioned hash transition",
    )
    await orchestrator.state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "proved before the hash transition",
        source_digest=legacy_scope,
    )
    await orchestrator.state.save()
    await orchestrator.shutdown()

    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    migrated_discovery = restarted.state.source_dependency_tree["nodes"][chapter.id][
        "source_digest"
    ]
    migrated_scope = restarted.state.formalize_graph["clean"][chapter.id]["source_digest"]
    assert migrated_discovery.startswith("xxh3-64:")
    assert migrated_scope == scope_digest(tmp_path, chapter)
    assert restarted.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
    assert restarted.state.task(chapter.id, Stage.PROVE).source_digest == migrated_scope
    assert restarted._discovery_is_current(chapter)
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_prepare_does_not_migrate_changed_legacy_discovery_digest(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    source = tmp_path / chapter.source
    lines = source.read_text(encoding="utf-8").splitlines()
    selected = "\n".join(lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line])
    legacy_discovery = stable_digest_text(f"{chapter.id}\0{selected}")
    orchestrator.state.source_dependency_tree = {
        "nodes": {chapter.id: {"dependencies": [], "source_digest": legacy_discovery}}
    }
    await orchestrator.state.set_task(
        chapter.id,
        Stage.DISCOVER,
        TaskStatus.PENDING,
        "awaiting freshness check",
    )
    await orchestrator.state.save()
    await orchestrator.shutdown()

    source.write_text(
        source.read_text(encoding="utf-8").replace("Text.", "Changed.", 1),
        encoding="utf-8",
    )
    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    assert (
        restarted.state.source_dependency_tree["nodes"][chapter.id]["source_digest"]
        == legacy_discovery
    )
    assert restarted.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.PENDING
    assert not restarted._discovery_is_current(chapter)
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_discovery_results_batch_graph_rebuild_and_task_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3, 4, 5, 6]")
    source = tmp_path / "books" / "book.md"
    source.write_text(
        source.read_text(encoding="utf-8")
        + "\n".join(f"\n## {number}. Chapter {number}\n" for number in range(3, 7)),
        encoding="utf-8",
    )
    config = load_config(config_path)
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    graph_calls = 0
    writes = 0
    original_graph = scheduler_module.build_source_dependency_graph
    original_write = state._database.write_delta

    def build_graph(*args: Any, **kwargs: Any) -> Any:
        nonlocal graph_calls
        graph_calls += 1
        return original_graph(*args, **kwargs)

    def write_delta(write: Any, *, connection: Any = None) -> int:
        nonlocal writes
        writes += 1
        return original_write(write, connection=connection)

    monkeypatch.setattr(scheduler_module, "build_source_dependency_graph", build_graph)
    monkeypatch.setattr(state._database, "write_delta", write_delta)

    await asyncio.gather(
        *(
            orchestrator._persist_source_dependencies(
                chapter,
                (),
                {"summary": f"discovered {chapter.id}", "issues": []},
            )
            for chapter in config.chapters
        )
    )

    assert graph_calls == 1
    assert writes == 1
    assert set(state.source_dependency_tree["nodes"]) == {chapter.id for chapter in config.chapters}
    assert all(
        state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
        for chapter in config.chapters
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_discovery_scheduler_bounds_created_coroutines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3, 4, 5, 6]")
    source = tmp_path / "books" / "book.md"
    source.write_text(
        source.read_text(encoding="utf-8")
        + "\n".join(f"\n## {number}. Chapter {number}\n" for number in range(3, 7)),
        encoding="utf-8",
    )
    config = load_config(config_path)
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.DISCOVER: replace(config.stages[Stage.DISCOVER], max_agents=1),
        },
    )
    orchestrator = Orchestrator(config, StateStore(config))
    release = asyncio.Event()
    window_full = asyncio.Event()
    started: list[str] = []

    async def discover(chapter: Chapter, *, rerun: bool = False) -> StageOutcome:
        del rerun
        started.append(chapter.id)
        if len(started) == 2:
            window_full.set()
        await release.wait()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_discover", discover)
    operation = asyncio.create_task(orchestrator._discover_all())
    await asyncio.wait_for(window_full.wait(), timeout=1)
    await asyncio.sleep(0)

    assert len(started) == 2
    release.set()
    assert await operation
    assert len(started) == 6


@pytest.mark.asyncio
async def test_invocation_usage_excludes_persisted_attempts(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    first = StateStore(config)
    await first.load_or_create()
    old_run = await first.start_run(config.chapters[0].id, Stage.FORMALIZE)
    await first.finish_run(
        old_run,
        status=TaskStatus.SUCCEEDED,
        usage=TokenUsage(input_tokens=100, output_tokens=20, measured=True),
    )

    second = StateStore(config)
    await second.load_or_create()

    assert second.total_usage().total_tokens == 120
    assert second.invocation_usage().total_tokens == 0
    assert not second.invocation_usage().measured

    new_run = await second.start_run(config.chapters[0].id, Stage.REVIEW)
    await second.update_run(
        new_run,
        usage=TokenUsage(input_tokens=30, output_tokens=5, measured=True),
    )

    assert second.total_usage().total_tokens == 155
    assert second.invocation_usage().total_tokens == 35
    assert second.invocation_usage(config.chapters[0].id).total_tokens == 35
    assert old_run.model == "gpt-5.6-luna"
    assert new_run.model == "gpt-5.6-luna"
    assert second.total_cost().estimated_usd == pytest.approx(0.000056)
    assert second.invocation_cost().estimated_usd == pytest.approx(0.000012)
    snapshot = second.snapshot()
    assert snapshot["cost"]["estimated_usd"] == pytest.approx(0.000056)
    assert snapshot["invocation_cost"]["estimated_usd"] == pytest.approx(0.000012)


@pytest.mark.asyncio
async def test_reload_migrates_cumulative_thread_usage_to_per_run_deltas(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    first = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
    await state.finish_run(
        first,
        status=TaskStatus.SUCCEEDED,
        thread_id="shared-thread",
        usage=TokenUsage(input_tokens=100, output_tokens=20, measured=True),
    )
    second = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    await state.finish_run(
        second,
        status=TaskStatus.SUCCEEDED,
        thread_id="shared-thread",
        usage=TokenUsage(input_tokens=150, output_tokens=30, measured=True),
    )
    await state.close()

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    runs = (
        reloaded.task(config.chapters[0].id, Stage.FORMALIZE).runs
        + reloaded.task(config.chapters[0].id, Stage.REVIEW).runs
    )

    assert [run.usage.total_tokens for run in runs] == [120, 60]
    assert [run.cumulative_usage.total_tokens for run in runs if run.cumulative_usage] == [
        120,
        180,
    ]
    assert reloaded.total_usage().total_tokens == 180
    assert reloaded.thread_cumulative_usage["shared-thread"].total_tokens == 180
    await reloaded.close()


@pytest.mark.asyncio
async def test_proof_blockers_merge_delta_reports_and_persist_ids(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter_id = config.chapters[0].id
    attempt = failed_attempt("no reusable bridge is available") | {"disposition": "genuine_blocker"}

    first = await state.record_proof_blockers(
        chapter_id,
        origin_run_id="run-1",
        failed_attempts=[attempt],
    )
    second = await state.record_proof_blockers(
        chapter_id,
        origin_run_id="run-2",
        failed_attempts=[],
        unchanged_ids=[first[0]["id"]],
    )

    assert first[0]["id"].startswith("B")
    assert second[0]["id"] == first[0]["id"]
    assert second[0]["sightings"] == 2
    assert len(second[0]["attempts"]) == 2
    await state.close()


@pytest.mark.asyncio
async def test_proof_blockers_deduplicate_obstruction_wording_by_residual_goal(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter_id = config.chapters[0].id

    first = await state.record_proof_blockers(
        chapter_id,
        origin_run_id="run-1",
        failed_attempts=[failed_attempt("no bridge was found")],
    )
    second = await state.record_proof_blockers(
        chapter_id,
        origin_run_id="run-2",
        failed_attempts=[failed_attempt("the imported API does not expose a bridge")],
    )

    assert second[0]["id"] == first[0]["id"]
    assert len(state.proof_blockers) == 1
    assert second[0]["sightings"] == 2
    await state.close()


@pytest.mark.asyncio
async def test_usage_and_cost_aggregates_cache_all_chapters_in_one_pass(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.chapters
    first_run = await state.start_run(first.id, Stage.FORMALIZE)
    second_run = await state.start_run(second.id, Stage.FORMALIZE)
    await state.update_run(
        first_run,
        usage=TokenUsage(input_tokens=100, output_tokens=20, measured=True),
    )
    await state.update_run(
        second_run,
        usage=TokenUsage(input_tokens=200, output_tokens=40, measured=True),
    )

    assert state.invocation_usage().total_tokens == 360
    assert state.invocation_usage(first.id).total_tokens == 120
    assert state.invocation_usage(second.id).total_tokens == 240
    total_cost = state.invocation_cost().estimated_usd
    assert state.invocation_cost(first.id).estimated_usd == pytest.approx(total_cost / 3)
    assert state.invocation_cost(second.id).estimated_usd == pytest.approx(2 * total_cost / 3)

    await state.update_run(
        first_run,
        usage=TokenUsage(input_tokens=150, output_tokens=30, measured=True),
    )

    assert state.invocation_usage().total_tokens == 420
    assert state.invocation_usage(first.id).total_tokens == 180
    assert state.invocation_usage(second.id).total_tokens == 240


@pytest.mark.asyncio
async def test_state_migrates_legacy_repair_tasks_to_formalize(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    payload = {
        "version": 4,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:01+00:00",
        "tasks": {
            f"{chapter.id}:repair": {
                "chapter_id": chapter.id,
                "book_id": chapter.book_id,
                "chapter_number": chapter.number,
                "chapter_title": chapter.title,
                "stage": "repair",
                "status": "succeeded",
                "detail": "legacy repair",
                "rounds": 1,
                "updated_at": "2026-01-01T00:00:01+00:00",
                "runs": [
                    {
                        "id": "legacy-run",
                        "chapter_id": chapter.id,
                        "stage": "repair",
                        "round": 1,
                        "status": "succeeded",
                        "usage": {"input_tokens": 10, "output_tokens": 2, "measured": True},
                    }
                ],
            }
        },
    }
    legacy_path = config.settings.state_dir / "state.json"
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(json.dumps(payload), encoding="utf-8")

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    migrated = reloaded.task(chapter.id, Stage.FORMALIZE)
    assert migrated.stage == "formalize"
    assert migrated.runs[0].stage == "formalize"
    assert not reloaded.coordinator_build.active
    assert reloaded.database_path.is_file()
    assert (config.settings.state_dir / "state.legacy-v6.json").is_file()
    assert json.loads(legacy_path.read_text(encoding="utf-8")) == payload
    assert reloaded.snapshot()["tasks"][f"{chapter.id}:formalize"]["runs"][0]["id"] == (
        "legacy-run"
    )


@pytest.mark.asyncio
async def test_state_persists_fixup_graph(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    state.fixup_graph = {
        "algorithm": "observed-lean-imports",
        "revision": 3,
        "edges": [],
        "clean": {config.chapters[0].id: {"source_digest": "abc"}},
    }
    await state.save()

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    assert reloaded.fixup_graph == state.fixup_graph
    assert reloaded.snapshot()["version"] == 18


@pytest.mark.asyncio
async def test_review_progress_does_not_imply_formalization_success(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()

    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.RUNNING, "reviewing")

    fixup = state.task(chapter.id, Stage.FORMALIZE)
    assert fixup.status == TaskStatus.PENDING

    with pytest.raises(RuntimeError, match="after review or proof has begun"):
        await state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.RUNNING,
            "late coordinator rebuild",
        )
    with pytest.raises(RuntimeError, match="after review or proof has begun"):
        await state.start_run(chapter.id, Stage.FORMALIZE)

    fixup.status = TaskStatus.RUNNING
    fixup.detail = "legacy coordinator build state"
    await state.save()
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    recovered_fixup = recovered.task(chapter.id, Stage.FORMALIZE)
    assert recovered_fixup.status == TaskStatus.SUCCEEDED
    assert recovered_fixup.detail == "formalization completed before review"
    await recovered.close()


@pytest.mark.asyncio
async def test_proof_review_requests_persist_and_acknowledge_exact_findings(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    first_id, first_created = await state.enqueue_proof_review_request(
        {chapter.id: "first failed-proof finding"},
        origin_run_id="proof-one",
    )
    second_id, second_created = await state.enqueue_proof_review_request(
        {chapter.id: "new finding that arrived during review"},
        origin_run_id="proof-two",
    )
    assert first_created and second_created
    await state.finish_proof_review_requests(chapter.id, (first_id,))
    await state.close()

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    assert first_id not in reloaded.proof_review_requests
    assert second_id in reloaded.proof_review_requests
    assert reloaded.proof_review_requests[second_id]["feedback"] == {
        chapter.id: "new finding that arrived during review"
    }
    assert reloaded.proof_review_requests[second_id]["kind"] == "proof_finding"
    await reloaded.close()


@pytest.mark.asyncio
async def test_proof_review_requests_discard_only_stale_source_owners(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    request_id, created = await state.enqueue_proof_review_request(
        {first.id: "first diagnostic", second.id: "second diagnostic"},
        origin_run_id="shared-build",
        source_digests={first.id: "first-old", second.id: "second-current"},
    )

    affected = await state.discard_stale_proof_review_requests(
        {first.id: "first-new", second.id: "second-current"}
    )

    assert created
    assert affected == {first.id}
    assert state.proof_review_requests[request_id]["feedback"] == {second.id: "second diagnostic"}
    assert state.proof_review_requests[request_id]["source_digests"] == {
        second.id: "second-current"
    }
    await state.close()


@pytest.mark.asyncio
async def test_proof_review_request_origin_is_reusable_after_source_changes(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    first_id, first_created = await state.enqueue_proof_review_request(
        {chapter.id: "same diagnostic"},
        origin_run_id="same-build-fingerprint",
        source_digests={chapter.id: "old-source"},
    )
    second_id, second_created = await state.enqueue_proof_review_request(
        {chapter.id: "same diagnostic"},
        origin_run_id="same-build-fingerprint",
        source_digests={chapter.id: "new-source"},
    )

    assert first_created and second_created
    assert first_id != second_id
    await state.close()


@pytest.mark.asyncio
async def test_stale_snapshot_review_requests_are_migrated_to_verification(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    retry, genuine = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    for chapter in config.chapters:
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.PENDING, "needs review")
    stale_id, _ = await state.enqueue_proof_review_request(
        {
            retry.id: (
                "Coordinator build failed without a source-located diagnostic:\n"
                "Source dependency scope changed during the coordinator build; retry required."
            ),
            genuine.id: (
                "Coordinator build failed without a source-located diagnostic:\n"
                "Source dependency scope changed during the coordinator build; retry required."
            ),
        },
        origin_run_id="stale-build",
        kind=BUILD_ERROR_REVIEW_KIND,
    )
    warning_id, _ = await state.enqueue_proof_review_request(
        {genuine.id: "warning: Book/Chapter02.lean:3:1: unused variable"},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
    )

    affected = await state.migrate_stale_snapshot_review_requests()

    assert affected == {retry.id, genuine.id}
    assert stale_id not in state.proof_review_requests
    assert warning_id in state.proof_review_requests
    assert state.task(retry.id, Stage.REVIEW).detail == "coordinator verification retry queued"
    assert state.task(genuine.id, Stage.REVIEW).detail == "needs review"
    await state.close()


@pytest.mark.asyncio
async def test_changed_owner_scope_discards_queued_diagnostics_before_review(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("theorem target : True := by trivial\n", encoding="utf-8")
    old_digest = scope_digest(config.settings.repo, chapter)
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "error: Book/Chapter01.lean:3:1: unknown identifier"},
        origin_run_id="old-coordinator-build",
        kind=BUILD_ERROR_REVIEW_KIND,
        source_digests={chapter.id: old_digest},
    )
    source.write_text(source.read_text(encoding="utf-8") + "\n-- replacement snapshot\n")
    await state.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.FAILED,
        "stale diagnostic review failed",
    )

    affected = await orchestrator._discard_stale_proof_review_requests()

    assert affected == {chapter.id}
    assert request_id not in state.proof_review_requests
    review = state.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.PENDING
    assert review.detail == COORDINATOR_VERIFICATION_RETRY_DETAIL
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_queued_build_diagnostics_capture_each_owner_source_digest(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    request_id, _ = await orchestrator._queue_review_feedback(
        {first.id: "first diagnostic", second.id: "second diagnostic"},
        origin="coordinator-build",
    )

    assert state.proof_review_requests[request_id]["source_digests"] == {
        first.id: scope_digest(config.settings.repo, first),
        second.id: scope_digest(config.settings.repo, second),
    }
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_migrated_stale_review_verifies_without_rerunning_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    await orchestrator.state.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.PENDING,
        "coordinator verification retry queued",
    )

    async def clean_build(_chapter: Chapter) -> dict[str, str]:
        return {}

    async def unexpected_review(*_args: object, **_kwargs: object) -> StageOutcome:
        raise AssertionError("stale coordinator retry must not launch a review agent")

    monkeypatch.setattr(orchestrator, "_review_build", clean_build)
    monkeypatch.setattr(orchestrator, "_review_once", unexpected_review)

    outcome = await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        verification_retry=True,
    )

    assert outcome.succeeded
    review = orchestrator.state.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.SUCCEEDED
    assert review.detail == "coordinator verification completed after stale snapshot"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_diagnostic_requests_select_targeted_review_role(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "warning: Book/Chapter01.lean:3:1: unused variable"},
        origin_run_id="coordinator-build",
        kind="diagnostic",
    )

    _, request_ids = orchestrator._proof_review_feedback(chapter.id)

    assert request_ids == (request_id,)
    assert orchestrator._proof_review_role(request_ids) == DIAGNOSTIC_REVIEW_ROLE
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_feedback_records_warning_and_error_reasons(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    warning_id, _ = await orchestrator._queue_review_feedback(
        {chapter.id: "warning: Book/Chapter01.lean:3:1: unused variable"},
        origin="warning-build",
    )
    assert state.proof_review_requests[warning_id]["kind"] == BUILD_WARNING_REVIEW_KIND
    _, warning_ids = orchestrator._proof_review_feedback(chapter.id)
    assert warning_ids == ()
    warning_feedback, warning_ids = orchestrator._warning_cleanup_feedback(chapter.id)
    assert warning_ids == (warning_id,)
    assert "unused variable" in warning_feedback

    await state.finish_proof_review_requests(chapter.id, (warning_id,))
    error_id, _ = await orchestrator._queue_review_feedback(
        {chapter.id: "error: Book/Chapter01.lean:4:2: unknown identifier"},
        origin="error-build",
    )
    assert state.proof_review_requests[error_id]["kind"] == BUILD_ERROR_REVIEW_KIND
    _, error_ids = orchestrator._proof_review_feedback(chapter.id)
    assert error_ids == (error_id,)
    assert orchestrator._proof_review_role(error_ids) == DIAGNOSTIC_REVIEW_ROLE
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_warning_cleanup_is_separate_from_proof_findings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    proof_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`: blocked"},
        origin_run_id="proof-run",
    )
    warning_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "warning: Book/Chapter01.lean:3:1: unused variable"},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
    )

    _, selected_ids = orchestrator._proof_review_feedback(chapter.id)
    assert selected_ids == (proof_id,)
    _, warning_ids = orchestrator._warning_cleanup_feedback(chapter.id)
    assert warning_ids == (warning_id,)

    await state.finish_proof_review_requests(chapter.id, warning_ids)
    _, selected_ids = orchestrator._proof_review_feedback(chapter.id)
    assert selected_ids == (proof_id,)
    assert orchestrator._proof_review_role(selected_ids) == PROOF_REVIEW_ROLE
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_tree_ignores_auxiliary_warning_cleanup_obligations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    proof_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`: blocked"},
        origin_run_id="proof-run",
    )
    warning_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "warning: Book/Chapter01.lean:3:1: unused variable"},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
    )
    await state.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.PENDING,
        "reason-specific re-review required",
    )
    calls: list[tuple[str, tuple[str, ...]]] = []

    async def review(
        _chapter: Chapter,
        _rounds_used: dict[str, int],
        **options: Any,
    ) -> StageOutcome:
        calls.append(
            (
                str(options.get("role", "")),
                tuple(options.get("proof_request_ids", ())),
            )
        )
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)

    assert await orchestrator._review_tree()
    assert calls == [(PROOF_REVIEW_ROLE, (proof_id,))]
    assert set(state.proof_review_requests) == {warning_id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_build_warning_switches_follow_up_to_warning_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    roles: list[str] = []
    builds = 0

    async def review_once(_chapter: Chapter, **options: Any) -> StageOutcome:
        roles.append(str(options.get("role", "")))
        if len(roles) == 1:
            source.write_text(
                "theorem target : True := by\n  sorry\n",
                encoding="utf-8",
            )
            return StageOutcome(
                ExecutionDisposition.SUCCEEDED,
                changed=True,
                complete=True,
                run_id="initial-review",
            )
        return StageOutcome(
            ExecutionDisposition.SUCCEEDED,
            changed=False,
            complete=True,
            run_id="warning-review",
        )

    async def review_build(_chapter: Chapter) -> dict[str, str]:
        nonlocal builds
        builds += 1
        return (
            {chapter.id: "warning: Book/Chapter01.lean:1:1: unused variable"} if builds == 1 else {}
        )

    monkeypatch.setattr(orchestrator, "_review_once", review_once)
    monkeypatch.setattr(orchestrator, "_review_build", review_build)

    assert await orchestrator._review_chapter_to_clean(chapter, {chapter.id: 0})
    assert roles == ["", WARNING_REVIEW_ROLE]
    assert builds == 2
    assert state.proof_review_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_diagnostic_review_records_distinct_run_role_and_schema(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [result(changed=False)])

    outcome = await orchestrator._review_once(
        chapter,
        rerun=True,
        feedback="error: Book/Chapter01.lean:3:1: unknown identifier",
        role=DIAGNOSTIC_REVIEW_ROLE,
        request_ids=("diagnostic-request",),
    )

    assert outcome.succeeded
    run = state.task(chapter.id, Stage.REVIEW).runs[-1]
    assert run.role == DIAGNOSTIC_REVIEW_ROLE
    assert run.prompt_kind == DIAGNOSTIC_REVIEW_ROLE
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_no_change_diagnostic_review_reuses_clean_source_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "error: Book/Chapter01.lean:3:1: unknown identifier"},
        origin_run_id="coordinator-build",
        kind="diagnostic",
    )
    orchestrator.executor = FakeExecutor(state, [result(changed=False)])

    async def forbidden_build(_chapter: Chapter) -> dict[str, str]:
        raise AssertionError("an unchanged certified digest must not be rebuilt")

    monkeypatch.setattr(orchestrator, "_review_build", forbidden_build)

    assert await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        rerun=True,
        feedback="error: Book/Chapter01.lean:3:1: unknown identifier",
        role=DIAGNOSTIC_REVIEW_ROLE,
        proof_request_ids=(request_id,),
    )
    assert request_id not in state.proof_review_requests
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_no_change_diagnostic_review_rebuilds_uncertified_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "error: Book/Chapter01.lean:3:1: unknown identifier"},
        origin_run_id="coordinator-build",
        kind="diagnostic",
    )
    orchestrator.executor = FakeExecutor(state, [result(changed=False)] * 3)
    builds = 0

    async def review_build(_chapter: Chapter) -> dict[str, str]:
        nonlocal builds
        builds += 1
        return {}

    async def forbidden_freshness_check(_chapter: Chapter) -> bool:
        raise AssertionError("a completed coordinator verification must be retained locally")

    monkeypatch.setattr(orchestrator, "_review_build", review_build)
    monkeypatch.setattr(orchestrator, "_proof_build_is_fresh", forbidden_freshness_check)

    assert await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        rerun=True,
        feedback="error: Book/Chapter01.lean:3:1: unknown identifier",
        role=DIAGNOSTIC_REVIEW_ROLE,
        proof_request_ids=(request_id,),
    )
    assert builds == 1
    assert request_id not in state.proof_review_requests
    assert state.task(chapter.id, Stage.REVIEW).rounds == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_diagnostic_review_waits_for_upstream_certification_feedback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator, {consumer.id: (owner.id,)})
    request_id, _ = await state.enqueue_proof_review_request(
        {consumer.id: "error: Book/Chapter02.lean:3:1: unknown identifier"},
        origin_run_id="coordinator-build",
        kind="diagnostic",
    )
    orchestrator.executor = FakeExecutor(state, [result(changed=False)])

    async def review_build(_chapter: Chapter) -> dict[str, str]:
        return {owner.id: "error: Book/Chapter01.lean:3:1: unknown identifier"}

    monkeypatch.setattr(orchestrator, "_review_build", review_build)

    outcome = await orchestrator._review_chapter_to_clean(
        consumer,
        {consumer.id: 0},
        rerun=True,
        feedback="error: Book/Chapter02.lean:3:1: unknown identifier",
        role=DIAGNOSTIC_REVIEW_ROLE,
        proof_request_ids=(request_id,),
    )

    assert outcome.waiting
    assert request_id in state.proof_review_requests
    consumer_review = state.task(consumer.id, Stage.REVIEW)
    assert consumer_review.status == TaskStatus.PENDING
    assert [requirement.owner_task_key for requirement in consumer_review.waiting_on] == [
        state.key(owner.id, Stage.REVIEW)
    ]
    assert not state.readiness(consumer_review).ready
    assert any(
        owner.id in request["feedback"]
        for candidate_id, request in state.proof_review_requests.items()
        if candidate_id != request_id
    )
    await orchestrator.shutdown()


def test_proof_review_feedback_tags_each_finding_with_a_stable_id(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    request_id = "request-one"
    orchestrator.state.proof_review_requests[request_id] = {
        "feedback": {
            chapter.id: "Proof findings:\n\nFailed proof `first` in `one.lean`:\n...\n\n"
            "Failed proof `second` in `two.lean`:\n..."
        },
        "origin_run_id": "proof-run",
    }

    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)

    assert request_ids == (request_id,)
    assert "Finding ID: `request-one:1`" in feedback
    assert "Finding ID: `request-one:2`" in feedback
    assert orchestrator._expected_proof_finding_ids(chapter.id, request_ids) == (
        "request-one:1",
        "request-one:2",
    )


@pytest.mark.asyncio
async def test_proof_review_retries_missing_finding_assessments_in_same_session(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {
            chapter.id: (
                "Proof work left checked failures.\n\n"
                "Failed proof `Book.target` in `lean/Book/Chapter01.lean`:\n..."
            )
        },
        origin_run_id="proof-run",
    )
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    executor = FakeExecutor(
        state,
        [
            replace(
                result(changed=False, finding_assessments=[]),
                thread_id="review-session",
            ),
            result(
                changed=False,
                finding_assessments=[
                    {
                        "finding_id": f"{request_id}:1",
                        "finding": "Book.target",
                        "assessment": "rejected",
                        "explanation": "The statement is sound.",
                    }
                ],
            ),
        ],
    )
    orchestrator.executor = executor

    succeeded = await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        rerun=True,
        feedback=feedback,
        proof_request_ids=request_ids,
    )

    assert succeeded
    assert request_id not in state.proof_review_requests
    review = state.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.SUCCEEDED
    assert review.rounds == 2
    assert executor.resume_thread_ids == [None, "review-session"]
    assert f"missing: {request_id}:1" in executor.resume_prompts[-1]
    assert all(run.request_ids == [request_id] for run in review.runs)
    assert all(run.prompt_kind == "proof_review" for run in review.runs)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_zero_work_malformed_review_report_starts_fresh_session(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    executor = FakeExecutor(
        state,
        [
            AgentResult(
                succeeded=False,
                exit_code=0,
                changed=False,
                placeholders=0,
                usage=TokenUsage(),
                report={},
                thread_id="malformed-session",
                error="Codex returned no structured final report",
            ),
            result(changed=False),
        ],
    )
    orchestrator.executor = executor

    assert await orchestrator._review_chapter_to_clean(chapter, {chapter.id: 0})
    assert state.task(chapter.id, Stage.REVIEW).rounds == 2
    assert executor.resume_thread_ids == [None, None]
    assert state.routing_metrics["zero_work_review_report"] == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_incomplete_review_report_respects_three_round_cap(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    incomplete = replace(result(changed=False, complete=False), thread_id="review-session")
    executor = FakeExecutor(state, [incomplete] * 5)
    orchestrator.executor = executor

    assert not await orchestrator._review_chapter_to_clean(chapter, {chapter.id: 0})
    review = state.task(chapter.id, Stage.REVIEW)
    assert review.rounds == 3
    assert review.status == TaskStatus.FAILED
    assert "retry cap reached after 3 cycles" in review.detail
    assert executor.resume_thread_ids == [None, "review-session", "review-session"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_review_acknowledges_exact_finding_assessments(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {
            chapter.id: (
                "Proof work left checked failures.\n\n"
                "Failed proof `Book.target` in `lean/Book/Chapter01.lean`:\n..."
            )
        },
        origin_run_id="proof-run",
    )
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(
                changed=False,
                finding_assessments=[
                    {
                        "finding_id": f"{request_id}:1",
                        "finding": "Book.target",
                        "assessment": "rejected",
                        "explanation": "The statement is sound.",
                    }
                ],
            )
        ],
    )

    succeeded = await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        rerun=True,
        feedback=feedback,
        proof_request_ids=request_ids,
    )

    assert succeeded
    assert request_id not in state.proof_review_requests
    assert state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_state_persists_successful_review_status(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    assert reloaded.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    hot = read_checkpoint(config.settings.state_dir)
    assert hot is not None
    assert "review_green" not in hot["tasks"][f"{chapter.id}:review"]


@pytest.mark.asyncio
async def test_state_recovers_orphan_run_even_when_task_is_not_running(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.FORMALIZE)
    await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "newer work completed")
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()

    recovered_run = recovered.task(chapter.id, Stage.FORMALIZE).runs[-1]
    assert recovered_run.id == run.id
    assert recovered_run.status == TaskStatus.INTERRUPTED
    assert recovered_run.finished_at is not None
    assert recovered.agent_summary()["active"] == 0


@pytest.mark.asyncio
async def test_interrupted_review_persists_until_startup_requeues_it(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    await state.start_run(chapter.id, Stage.REVIEW)

    recovered = StateStore(config)
    await recovered.load_or_create()
    task = recovered.task(chapter.id, Stage.REVIEW)

    assert task.status == TaskStatus.INTERRUPTED
    assert task.detail == "agent interrupted with the orchestrator"

    await recovered.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.BLOCKED,
        "blocked by cancellation fallout",
    )

    assert task.status == TaskStatus.INTERRUPTED
    assert task.detail == "agent interrupted with the orchestrator"

    changed = await recovered.requeue_interrupted(resume_agents=True)

    assert changed == [f"{chapter.id}:review"]
    assert task.status == TaskStatus.PENDING
    assert task.detail == "interrupted agent queued for session resume"


@pytest.mark.asyncio
async def test_interrupted_run_does_not_overwrite_newer_pending_task_state(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.REVIEW)
    task = state.task(chapter.id, Stage.REVIEW)

    # Review invalidation can make the task schedulable again before cancellation
    # cleanup for the superseded agent has finished.
    task.status = TaskStatus.PENDING
    task.detail = "review invalidated while its agent was stopping"
    await state.finish_run(run, status=TaskStatus.INTERRUPTED)

    assert run.status == TaskStatus.INTERRUPTED
    assert task.status == TaskStatus.PENDING
    assert task.detail == "review invalidated while its agent was stopping"
    await state.close()


@pytest.mark.asyncio
async def test_orchestrator_requeues_interrupted_tasks_for_fresh_retry_by_default(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.REVIEW)
    await state.finish_run(
        run,
        status=TaskStatus.INTERRUPTED,
        thread_id="saved-but-not-requested",
    )
    await state.close()

    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()

    task = orchestrator.state.task(chapter.id, Stage.REVIEW)
    assert task.status == TaskStatus.PENDING
    assert task.detail == "interrupted agent queued for a fresh retry"
    assert orchestrator.executor.resume_agents is False
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_hot_checkpoint_does_not_grow_with_run_payload_history(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()

    async with state.batch():
        for index in range(25):
            run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
            await state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED,
                report={"summary": f"payload-{index}-" + "x" * 10_000, "issues": []},
                validation={"succeeded": True, "output": "y" * 2_000},
                isolation={"accepted": True, "changed_paths": [f"file-{index}.lean"]},
            )

    hot = read_checkpoint(config.settings.state_dir)
    assert hot is not None
    task = hot["tasks"][f"{config.chapters[0].id}:formalize"]
    assert hot["version"] == 18
    assert "source_issues" not in hot
    assert "runs" not in task
    assert task["run_count"] == 25
    assert state.path == state.database_path
    with sqlite3.connect(state.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM runs").fetchone()[0] == 25

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    historical = reloaded.task(config.chapters[0].id, Stage.FORMALIZE).runs[0]
    assert historical.report is None
    reloaded.load_run_details(historical)
    assert historical.report is not None
    assert historical.report["summary"].startswith("payload-0-")
    assert len(reloaded.snapshot()["tasks"][f"{config.chapters[0].id}:formalize"]["runs"]) == 25


@pytest.mark.asyncio
async def test_json_snapshot_is_written_only_to_an_explicit_output(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    legacy_path = config.settings.state_dir / "state.json"
    assert not legacy_path.exists()

    run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    assert not legacy_path.exists()
    checkpoint = read_checkpoint(config.settings.state_dir)
    assert checkpoint is not None
    assert checkpoint["tasks"][f"{config.chapters[0].id}:formalize"]["run_count"] == 1

    output = tmp_path / "exports" / "snapshot.json"
    assert await state.export(output) == output.resolve()
    exported = json.loads(output.read_text(encoding="utf-8"))
    runs = exported["tasks"][f"{config.chapters[0].id}:formalize"]["runs"]
    assert [value["id"] for value in runs] == [run.id]
    await state.close()
    assert not legacy_path.exists()


@pytest.mark.asyncio
async def test_deferred_telemetry_is_durable_after_coalescing_window(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    usage = TokenUsage(input_tokens=80, output_tokens=20, measured=True)
    await state.update_run(run, usage=usage, deferred=True)
    await asyncio.sleep(0.6)

    persisted = state._database.run_payload(run.id)
    assert persisted is not None
    assert persisted["usage"]["input_tokens"] == 80
    assert persisted["usage"]["output_tokens"] == 20
    await state.close()


@pytest.mark.asyncio
async def test_concurrent_run_updates_coalesce_into_one_database_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    async with state.batch():
        runs = [await state.start_run(config.chapters[0].id, stage) for stage in Stage]

    calls = 0
    original = state._database.write_delta

    def tracked(write: Any, *, connection: Any = None) -> int:
        nonlocal calls
        calls += 1
        return original(write, connection=connection)

    monkeypatch.setattr(state._database, "write_delta", tracked)
    await asyncio.gather(
        *(state.update_run(run, pid=index) for index, run in enumerate(runs, start=1))
    )

    assert calls == 1


@pytest.mark.asyncio
async def test_run_records_use_the_stage_model(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, model="gpt-5.6-sol"))
    state = StateStore(config)
    await state.load_or_create()

    discovery = await state.start_run(config.chapters[0].id, Stage.DISCOVER)
    formalize = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    assert discovery.model == "gpt-5.6-luna"
    assert formalize.model == "gpt-5.6-sol"


@pytest.mark.asyncio
async def test_recovering_interrupted_run_preserves_lazy_payload(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    first = StateStore(config)
    await first.load_or_create()
    run = await first.start_run(config.chapters[0].id, Stage.REVIEW)
    await first.update_run(
        run,
        report={"summary": "preserve me", "issues": []},
        validation={"succeeded": False, "output": "interrupted"},
    )

    recovered = StateStore(config)
    await recovered.load_or_create()
    recovered_task = recovered.task(config.chapters[0].id, Stage.REVIEW)
    recovered_run = recovered_task.runs[-1]

    assert recovered_task.status == TaskStatus.INTERRUPTED
    assert recovered_run.status == TaskStatus.INTERRUPTED
    assert recovered_run.report is None
    recovered.load_run_details(recovered_run)
    assert recovered_run.report == {"summary": "preserve me", "issues": []}
    assert recovered_run.validation == {"succeeded": False, "output": "interrupted"}


@pytest.mark.asyncio
async def test_state_accumulates_and_deduplicates_source_issue_ledger(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    source_issue = {
        "location": "Chapter 1, paragraph 2",
        "source_excerpt": "Every local ring is a field.",
        "description": "The assertion omits the zero-dimensional hypothesis.",
        "suggested_correction": "Every zero-dimensional reduced local ring is a field.",
    }
    for stage in (Stage.FORMALIZE, Stage.REVIEW):
        run = await state.start_run(config.chapters[0].id, stage)
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            report={"source_issues": [source_issue]},
        )

    assert len(state.source_issues) == 1
    recorded = next(iter(state.source_issues.values()))
    assert recorded.source == "books/book.md"
    assert recorded.sightings == 2
    assert recorded.stages == ["formalize", "review"]
    assert len(recorded.run_ids) == 2
    latest = state.task(config.chapters[0].id, Stage.REVIEW).runs[-1]
    assert latest.report is not None
    assert "source_issues" not in latest.report
    assert latest.report["source_issue_ids"] == [recorded.id]

    _, _, issues = state._database.load()
    assert issues[0]["id"] == recorded.id
    assert issues[0]["suggested_correction"] == source_issue["suggested_correction"]

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    assert reloaded.source_issues[recorded.id].sightings == 2
    assert not reloaded.source_issues_path.exists()


def test_legacy_attempt_cost_is_always_luna(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, model="gpt-5.6-sol"))
    state = StateStore(config)
    legacy = RunRecord(
        id="legacy",
        chapter_id=config.chapters[0].id,
        stage=Stage.FORMALIZE,
        round=1,
        model=None,
        usage=TokenUsage(input_tokens=100, output_tokens=20, measured=True),
    )

    cost = state.run_cost(legacy)

    assert cost.estimated_usd == pytest.approx(0.000044)
    assert cost.inferred_runs == 1


@pytest.mark.asyncio
async def test_prepare_scaffolds_directories_without_lean_files(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_discovered(orchestrator)

    await orchestrator.prepare()

    for chapter in config.chapters:
        directory = tmp_path / chapter.lean_root / chapter.chapter_path
        assert directory.is_dir()
    assert not list((tmp_path / "lean").rglob("*.lean"))
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_skips_agent_for_an_existing_clean_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    directory = tmp_path / chapter.lean_root / chapter.chapter_path
    directory.mkdir(parents=True, exist_ok=True)
    (tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean").write_text(
        "import Book.Chapter01.Section\n", encoding="utf-8"
    )
    (directory / "Section.lean").write_text("theorem drafted : True := by sorry\n")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    orchestrator.executor = FakeExecutor(state, [])

    async def clean_build(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", clean_build)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert state.task(chapter.id, Stage.FORMALIZE).rounds == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_retries_after_diagnostics_and_builds_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    chapter = config.chapters[0]
    directory = tmp_path / chapter.lean_root / chapter.chapter_path
    directory.mkdir(parents=True, exist_ok=True)
    (tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean").write_text(
        "import Book.Chapter01.Section\n", encoding="utf-8"
    )
    (directory / "Section.lean").write_text("theorem drafted : True := by sorry\n")
    orchestrator.executor = FakeExecutor(state, [result(changed=True)])
    builds = iter(
        (
            ValidationResult(False, 1, "error: needs repair"),
            ValidationResult(True, 0, "ok"),
        )
    )

    async def tracked_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return next(builds)

    monkeypatch.setattr(scheduler_module, "validate", tracked_validation)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert state.task(config.chapters[0].id, Stage.FORMALIZE).rounds == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_retries_an_incomplete_draft(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    executor = FakeExecutor(
        state,
        [
            result(changed=True, complete=False, issues=["coverage audit unfinished"]),
            result(changed=True),
        ],
    )
    orchestrator.executor = executor
    scope_checks = iter((False, False, True))

    async def scope_exists(_chapter: Chapter) -> bool:
        return next(scope_checks)

    async def clean_build(
        *_args: object, snapshots: dict[str, object], **_kwargs: object
    ) -> dict[str, ValidationResult]:
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "ok")}

    async def publish(_chapter: Chapter, _snapshot: object) -> bool:
        return True

    monkeypatch.setattr(orchestrator, "_scope_exists", scope_exists)
    monkeypatch.setattr(orchestrator, "_build_chapters", clean_build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    task = state.task(chapter.id, Stage.FORMALIZE)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.rounds == 2
    assert len(executor.feedbacks) == 2
    assert "coverage audit unfinished" in executor.feedbacks[1]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_repeats_coordinator_build_and_hands_back_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    chapter = config.chapters[0]
    directory = tmp_path / chapter.lean_root / chapter.chapter_path
    directory.mkdir(parents=True, exist_ok=True)
    (tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean").write_text(
        "import Book.Chapter01.Section\n", encoding="utf-8"
    )
    (directory / "Section.lean").write_text("theorem drafted : True := by sorry\n")
    orchestrator.executor = FakeExecutor(state, [result(changed=True)])
    feedback_seen: list[str] = []
    original_run = orchestrator.executor.run

    async def tracked_run(
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        assert stage is Stage.FORMALIZE
        feedback_seen.append(feedback)
        return await original_run(
            chapter,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )

    builds = iter(
        (
            ValidationResult(False, 1, "error: Book/Chapter01/Section.lean:4:1: broken"),
            ValidationResult(True, 0, "ok"),
            ValidationResult(True, 0, "ok"),
        )
    )

    async def tracked_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return next(builds)

    monkeypatch.setattr(orchestrator.executor, "run", tracked_run)
    monkeypatch.setattr(scheduler_module, "validate", tracked_validation)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert len(feedback_seen) == 1
    assert "Book/Chapter01/Section.lean:4:1: broken" in feedback_seen[0]
    assert state.task(config.chapters[0].id, Stage.FORMALIZE).rounds == 1
    await orchestrator.shutdown()


def with_example_modules(config: PipelineConfig) -> PipelineConfig:
    return replace(
        config,
        chapters=tuple(
            replace(
                chapter,
                chapter_module=f"LastLib.Book.Chapter{chapter.number:02d}",
            )
            for chapter in config.chapters
        ),
    )


@pytest.mark.asyncio
async def test_formalize_builds_in_discovered_source_dependency_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    (tmp_path / "lean" / "Book").mkdir(parents=True)
    (tmp_path / "lean" / "Book" / "Chapter01.lean").write_text(
        "import LastLib.Book.Chapter02\n",
        encoding="utf-8",
    )
    for chapter in (second,):
        (tmp_path / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    for chapter in (first, second):
        section = tmp_path / chapter.lean_root / chapter.chapter_path / "Section.lean"
        section.parent.mkdir(parents=True, exist_ok=True)
        section.write_text(f"def section{chapter.number} := {chapter.number}\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator, {first.id: (second.id,)})
    builds: list[tuple[str, str]] = []

    async def successful_validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append((chapter.id, chapter.build_command))
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert builds == [
        (second.id, "cd lean && lake build +Book.Chapter02"),
        (
            first.id,
            "cd lean && lake build +Book.Chapter01",
        ),
    ]
    assert orchestrator.state.task(first.id, Stage.FORMALIZE).rounds == 0
    assert orchestrator.state.task(second.id, Stage.FORMALIZE).rounds == 0
    assert state.fixup_graph["edges"] == [[second.id, first.id]]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_post_review_fixup_request_is_migrated_back_to_review(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    request_id = await state.enqueue_fixup_request(
        {chapter.id: "missing scalar tower"},
        {chapter.id},
        origin_run_id="proof-run",
    )
    await state.close()

    recovered = StateStore(config)
    orchestrator = Orchestrator(config, recovered)
    await orchestrator.prepare()

    assert request_id not in recovered.fixup_requests
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    assert feedback == "missing scalar tower"
    assert request_ids == (request_id,)
    assert recovered.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
    assert recovered.task(chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_single_unqueued_proof_finding_is_recovered_as_open_blocker(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.PROVE)
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={"failed_attempts": [failed_attempt("add the missing scalar tower")]},
    )
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.PENDING, "needs review")
    await state.close()

    recovered = StateStore(config)
    orchestrator = Orchestrator(config, recovered)
    await orchestrator.prepare()
    await orchestrator._recover_proof_review_requests()

    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    assert feedback == ""
    assert request_ids == ()
    assert recovered.fixup_requests == {}
    assert recovered.proof_review_requests == {}
    blockers = recovered.proof_blockers_for_consumer(chapter.id)
    assert len(blockers) == 1
    assert "missing scalar tower" in blockers[0]["obstruction"]
    assert blockers[0]["sightings"] == 1
    assert recovered.task(chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_recovery_restores_green_reviews_without_direct_findings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, downstream = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(downstream.id, Stage.REVIEW)
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        changed=False,
        report={"complete": True},
    )
    await state.set_task(downstream.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    await state.set_task(
        downstream.id,
        Stage.REVIEW,
        TaskStatus.PENDING,
        "review invalidated by the former closure-wide policy",
    )
    await state.set_task(owner.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    await state.enqueue_proof_review_request(
        {owner.id: "repair the owner statement"},
        origin_run_id="proof-finding",
    )
    await state.close()

    recovered = StateStore(config)
    orchestrator = Orchestrator(config, recovered)
    await orchestrator.prepare()
    await orchestrator._recover_proof_review_requests()

    owner_review = recovered.task(owner.id, Stage.REVIEW)
    assert owner_review.status == TaskStatus.SUCCEEDED
    assert owner_review.detail == "reviewed"
    restored = recovered.task(downstream.id, Stage.REVIEW)
    assert restored.status == TaskStatus.SUCCEEDED
    assert restored.detail == "durable review remains green; no pending findings for this chapter"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_later_review_build_waits_for_running_proof_certification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    proof_chapter, review_chapter = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    proof_started = asyncio.Event()
    release_proof = asyncio.Event()
    events: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        *,
        workspace_root: Path | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ValidationResult:
        assert workspace_root == config.settings.repo
        assert on_output is not None
        if chapter.id == proof_chapter.id:
            events.append("proof-started")
            proof_started.set()
            await release_proof.wait()
            events.append("proof-built")
        else:
            events.append("review-built")
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    proof = asyncio.create_task(
        orchestrator._build_chapters(
            (proof_chapter,),
            publish_if_clean=False,
            mode="proof-certification",
            stage=Stage.PROVE,
        )
    )
    await proof_started.wait()
    review = asyncio.create_task(
        orchestrator._build_chapters(
            (review_chapter,),
            publish_if_clean=False,
            mode="review-verification",
            stage=Stage.REVIEW,
        )
    )

    await asyncio.sleep(0)
    assert not review.done()
    assert events == ["proof-started"]
    release_proof.set()
    assert (await review)[review_chapter.id].succeeded
    assert (await proof)[proof_chapter.id].succeeded
    assert events == ["proof-started", "proof-built", "review-built"]
    assert orchestrator.build_queue.snapshot() == {
        "owner": "",
        "owner_stage": "",
        "queued": 0,
        "queued_jobs": [],
    }
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_concurrent_review_builds_share_commands_and_partition_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3, 4]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n\n## 4. Fourth chapter\n")
    config = load_config(project)
    first, second, third, fourth = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    commands: list[str] = []
    first_build_started = asyncio.Event()
    release_first_build = asyncio.Event()

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        if len(commands) == 1:
            first_build_started.set()
            await release_first_build.wait()
            return ValidationResult(
                False,
                1,
                "error: Book/Chapter01/Section.lean:1:1: broken review output",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    initial = asyncio.gather(
        orchestrator._review_build(first),
        orchestrator._review_build(second),
        orchestrator._review_build(third),
    )
    await first_build_started.wait()
    fourth_build = asyncio.create_task(orchestrator._review_build(fourth))
    release_first_build.set()
    (first_feedback, second_feedback, third_feedback), fourth_feedback = await asyncio.gather(
        initial, fourth_build
    )

    def target(chapter: Chapter) -> str:
        return chapter.build_command.rpartition(" ")[2]

    assert commands == [
        f"cd lean && lake build {target(first)} {target(second)} {target(third)}",
        f"cd lean && lake build {target(second)} {target(third)} {target(fourth)}",
    ]
    assert set(first_feedback) == {first.id}
    assert "broken review output" in first_feedback[first.id]
    assert second_feedback == {}
    assert third_feedback == {}
    assert fourth_feedback == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_known_broken_prerequisite_short_circuits_dependent_builds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(project)
    owner, consumer, independent = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    graph = WorkUnitImportGraph(
        dependencies={
            owner.id: frozenset(),
            consumer.id: frozenset({owner.id}),
            independent.id: frozenset(),
        },
        successors={
            owner.id: frozenset({consumer.id}),
            consumer.id: frozenset(),
            independent.id: frozenset(),
        },
        order=(owner.id, consumer.id, independent.id),
        edges=((owner.id, consumer.id),),
    )
    attempts: list[tuple[str, ...]] = []

    async def build(
        chapters: Iterable[Chapter],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        selected = tuple(chapters)
        attempts.append(tuple(chapter.id for chapter in selected))
        if len(attempts) == 1:
            return {
                consumer.id: ValidationResult(
                    False,
                    1,
                    "error: Book/Chapter01/Section.lean:1:1: broken prerequisite",
                    process_exit_code=1,
                    status=ValidationStatus.DEPENDENCY_FAILED,
                    blocked_by=(owner.id,),
                ),
                independent.id: ValidationResult(
                    False,
                    1,
                    "combined build stopped before this target",
                    process_exit_code=1,
                    status=ValidationStatus.UNATTRIBUTED_BUILD_FAILURE,
                ),
            }
        return {
            chapter.id: ValidationResult(True, 0, "ok", process_exit_code=0) for chapter in selected
        }

    monkeypatch.setattr(orchestrator, "_observed_work_unit_graph", lambda: graph)
    monkeypatch.setattr(orchestrator, "_execute_build_chapters", build)

    consumer_result, independent_result = await asyncio.gather(
        orchestrator._build_chapters((consumer,), publish_if_clean=False),
        orchestrator._build_chapters((independent,), publish_if_clean=False),
    )
    cached_result = await orchestrator._build_chapters((consumer,), publish_if_clean=False)

    assert consumer_result[consumer.id].status is ValidationStatus.DEPENDENCY_FAILED
    assert independent_result[independent.id].succeeded
    assert cached_result[consumer.id].status is ValidationStatus.DEPENDENCY_FAILED
    assert attempts == [
        (consumer.id, independent.id),
        (independent.id,),
    ]

    orchestrator._mark_source_changed((owner.id,))
    rebuilt = await orchestrator._build_chapters((consumer,), publish_if_clean=False)
    assert rebuilt[consumer.id].succeeded
    assert attempts[-1] == (consumer.id,)

    feedback = {owner.id: "error: Book/Chapter01/Section.lean:1:1: broken prerequisite"}
    first_request, first_created = await orchestrator._queue_review_feedback(
        feedback,
        origin="consumer-one",
    )
    second_request, second_created = await orchestrator._queue_review_feedback(
        feedback,
        origin="consumer-two",
    )
    assert first_created
    assert not second_created
    assert second_request == first_request
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_stale_build_batch_retries_without_returning_feedback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    attempts = 0

    async def build(
        chapters: Iterable[Chapter],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        nonlocal attempts
        attempts += 1
        selected = tuple(chapters)
        status = ValidationStatus.STALE_SNAPSHOT if attempts == 1 else ValidationStatus.CLEAN
        return {
            item.id: ValidationResult(
                status is ValidationStatus.CLEAN,
                0 if status is ValidationStatus.CLEAN else 1,
                "ok" if status is ValidationStatus.CLEAN else "source changed",
                status=status,
            )
            for item in selected
        }

    monkeypatch.setattr(orchestrator, "_execute_build_chapters", build)

    result = await orchestrator._build_chapters((chapter,), publish_if_clean=False)

    assert attempts == 2
    assert result[chapter.id].succeeded
    assert orchestrator.state.proof_review_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_wholly_unattributed_batch_failure_is_not_probed_by_subsets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    commands: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        return ValidationResult(False, 1, "error: coordinator process failed")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    first_result, second_result = await asyncio.gather(
        orchestrator._build_chapters((first,), publish_if_clean=False),
        orchestrator._build_chapters((second,), publish_if_clean=False),
    )

    assert len(commands) == 1
    assert first_result[first.id].status is ValidationStatus.UNATTRIBUTED_BUILD_FAILURE
    assert second_result[second.id].status is ValidationStatus.UNATTRIBUTED_BUILD_FAILURE
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coordinator_queue_routes_one_bounded_slice_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3, 4]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n\n## 4. Fourth chapter\n")
    config = load_config(project)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    commands: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "MAXIMUM_COORDINATOR_BUILD_TARGETS", 2)
    monkeypatch.setattr(scheduler_module, "validate", validation)

    results = await asyncio.gather(
        *(
            orchestrator._build_chapters((chapter,), publish_if_clean=False)
            for chapter in config.chapters
        )
    )

    targets = [chapter.build_command.rpartition(" ")[2] for chapter in config.chapters]
    assert commands == [
        f"cd lean && lake build {targets[0]} {targets[1]}",
        f"cd lean && lake build {targets[2]} {targets[3]}",
    ]
    assert all(
        result[chapter.id].succeeded
        for result, chapter in zip(results, config.chapters, strict=True)
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_overlapping_multi_target_callers_retain_partial_results_across_slices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(project)
    first, second, third = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    commands: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "MAXIMUM_COORDINATOR_BUILD_TARGETS", 1)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    left_snapshots: dict[str, scheduler_module.ValidatedBuildSnapshot] = {}

    left, right = await asyncio.gather(
        orchestrator._build_chapters(
            (first, second), publish_if_clean=False, snapshots=left_snapshots
        ),
        orchestrator._build_chapters((second, third), publish_if_clean=False),
    )

    assert [command.rpartition(" ")[2] for command in commands] == [
        first.build_command.rpartition(" ")[2],
        second.build_command.rpartition(" ")[2],
        third.build_command.rpartition(" ")[2],
    ]
    assert set(left) == {first.id, second.id}
    assert set(right) == {second.id, third.id}
    assert set(left_snapshots) == {first.id, second.id}
    assert all(result.succeeded for result in (*left.values(), *right.values()))
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_cancelling_build_dispatcher_cancels_callers_and_stops_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    started = asyncio.Event()
    validation_cancelled = asyncio.Event()

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            validation_cancelled.set()
            raise
        raise AssertionError("unreachable validation completion")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    caller = asyncio.create_task(
        orchestrator._build_chapters((config.chapters[0],), publish_if_clean=False)
    )
    await started.wait()
    assert orchestrator._build_dispatch_task is not None

    orchestrator._build_dispatch_task.cancel()
    await asyncio.gather(orchestrator._build_dispatch_task, return_exceptions=True)

    with pytest.raises(asyncio.CancelledError):
        await caller
    assert validation_cancelled.is_set()
    assert orchestrator._build_dispatch_task is None
    assert orchestrator._pending_build_requests == []
    await orchestrator.shutdown()


def test_failed_batch_diagnostics_are_partitioned_by_target_closure(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = WorkUnitImportGraph(
        dependencies={owner.id: frozenset(), consumer.id: frozenset({owner.id})},
        successors={owner.id: frozenset({consumer.id}), consumer.id: frozenset()},
        order=(owner.id, consumer.id),
        edges=((owner.id, consumer.id),),
    )
    result = ValidationResult(
        False,
        1,
        "error: Book/Chapter01/Section.lean:3:2: rejected declaration",
        process_exit_code=1,
    )

    partitioned = orchestrator._partition_build_diagnostics(result, (owner.id, consumer.id), graph)

    assert partitioned[owner.id].status is ValidationStatus.TARGET_FAILED
    assert partitioned[consumer.id].status is ValidationStatus.DEPENDENCY_FAILED
    assert partitioned[consumer.id].blocked_by == (owner.id,)


def test_failed_batch_uses_structured_diagnostics_hidden_from_display_output(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = WorkUnitImportGraph(
        dependencies={owner.id: frozenset(), consumer.id: frozenset({owner.id})},
        successors={owner.id: frozenset({consumer.id}), consumer.id: frozenset()},
        order=(owner.id, consumer.id),
        edges=((owner.id, consumer.id),),
    )
    hidden = scheduler_module.LeanDiagnostic(
        "error",
        "error: Book/Chapter01/Section.lean:3:2: hidden failure",
        "error: Book/Chapter01/Section.lean:3:2: hidden failure",
    )
    result = ValidationResult(
        False,
        1,
        "warning: Book/Unrelated.lean:9:1: trailing warning",
        process_exit_code=1,
        diagnostics=(hidden,),
        failed_modules=("Book.Chapter01",),
        raw_log_path="/tmp/coordinator-build.log",
    )

    partitioned = orchestrator._partition_build_diagnostics(result, (owner.id, consumer.id), graph)

    assert partitioned[owner.id].status is ValidationStatus.TARGET_FAILED
    assert partitioned[consumer.id].status is ValidationStatus.DEPENDENCY_FAILED
    assert partitioned[consumer.id].blocked_by == (owner.id,)
    assert partitioned[owner.id].diagnostics == (hidden,)
    assert partitioned[owner.id].raw_log_path == "/tmp/coordinator-build.log"


def test_upstream_warning_does_not_fail_dependent_build(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = WorkUnitImportGraph(
        dependencies={owner.id: frozenset(), consumer.id: frozenset({owner.id})},
        successors={owner.id: frozenset({consumer.id}), consumer.id: frozenset()},
        order=(owner.id, consumer.id),
        edges=((owner.id, consumer.id),),
    )
    result = ValidationResult(
        False,
        1,
        "warning: Book/Chapter01/Section.lean:3:2: unused variable `h`",
        process_exit_code=0,
    )

    partitioned = orchestrator._partition_build_diagnostics(result, (owner.id, consumer.id), graph)

    assert partitioned[owner.id].status is ValidationStatus.TARGET_WARNINGS
    assert partitioned[owner.id].warnings_only
    assert partitioned[consumer.id].status is ValidationStatus.CLEAN
    assert partitioned[consumer.id].succeeded


def test_warning_does_not_become_upstream_failure_when_batch_process_fails(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = WorkUnitImportGraph(
        dependencies={owner.id: frozenset(), consumer.id: frozenset({owner.id})},
        successors={owner.id: frozenset({consumer.id}), consumer.id: frozenset()},
        order=(owner.id, consumer.id),
        edges=((owner.id, consumer.id),),
    )
    result = ValidationResult(
        False,
        1,
        "warning: Book/Chapter01/Section.lean:3:2: unused variable `h`",
        process_exit_code=1,
    )

    partitioned = orchestrator._partition_build_diagnostics(result, (owner.id, consumer.id), graph)

    assert partitioned[owner.id].status is ValidationStatus.TARGET_WARNINGS
    assert not partitioned[owner.id].warnings_only
    assert partitioned[consumer.id].status is ValidationStatus.UNATTRIBUTED_BUILD_FAILURE
    assert partitioned[consumer.id].blocked_by == ()


def test_cached_broken_result_is_relabelled_for_each_consumer(tmp_path: Path) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(project)
    owner, first_consumer, second_consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    graph = WorkUnitImportGraph(
        dependencies={
            owner.id: frozenset(),
            first_consumer.id: frozenset({owner.id}),
            second_consumer.id: frozenset({first_consumer.id}),
        },
        successors={
            owner.id: frozenset({first_consumer.id}),
            first_consumer.id: frozenset({second_consumer.id}),
            second_consumer.id: frozenset(),
        },
        order=(owner.id, first_consumer.id, second_consumer.id),
        edges=((owner.id, first_consumer.id), (first_consumer.id, second_consumer.id)),
    )
    diagnostic = "error: Book/Chapter01/Section.lean:3:2: rejected declaration"
    first_result = ValidationResult(
        False,
        1,
        f"{diagnostic}\n\nCoordinator found 1 Lean diagnostic(s) relevant to {first_consumer.id}.",
        process_exit_code=1,
        status=ValidationStatus.DEPENDENCY_FAILED,
        blocked_by=(owner.id,),
    )

    orchestrator._remember_broken_builds({first_consumer.id: first_result}, graph)
    cached = orchestrator._cached_broken_results((second_consumer.id,), graph)[second_consumer.id]

    assert f"relevant to {second_consumer.id}." in cached.output
    assert f"relevant to {first_consumer.id}." not in cached.output


@pytest.mark.asyncio
async def test_formalize_warning_publishes_artifact_and_queues_auxiliary_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.DISCOVER, TaskStatus.SUCCEEDED, "discovered")
    monkeypatch.setattr(orchestrator, "_scope_exists", lambda _chapter: asyncio.sleep(0, True))

    async def warning_build(
        _chapters: Iterable[Chapter],
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        snapshots[chapter.id] = object()
        return {
            chapter.id: ValidationResult(
                False,
                1,
                "warning: Book/Chapter01.lean:3:1: unused variable `h`",
                process_exit_code=0,
                status=ValidationStatus.TARGET_WARNINGS,
            )
        }

    async def publish(_chapter: Chapter, _snapshot: object) -> bool:
        return True

    monkeypatch.setattr(orchestrator, "_build_chapters", warning_build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)

    outcome = await orchestrator._formalize(chapter)

    assert outcome.succeeded
    formalize = state.task(chapter.id, Stage.FORMALIZE)
    assert formalize.status == TaskStatus.SUCCEEDED
    assert "warning cleanup queued" in formalize.detail
    feedback, request_ids = orchestrator._warning_cleanup_feedback(chapter.id)
    assert len(request_ids) == 1
    assert "unused variable" in feedback
    assert state.task(chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    await state.close()


@pytest.mark.asyncio
async def test_formalize_requeues_clean_build_if_source_changes_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.DISCOVER, TaskStatus.SUCCEEDED, "discovered")
    monkeypatch.setattr(orchestrator, "_scope_exists", lambda _chapter: asyncio.sleep(0, True))

    async def clean_build(
        _chapters: Iterable[Chapter],
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "ok")}

    async def stale_publication(_chapter: Chapter, _snapshot: object) -> bool:
        return False

    async def no_agent(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a publication race must not spend a formalize agent round")

    monkeypatch.setattr(orchestrator, "_build_chapters", clean_build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", stale_publication)
    monkeypatch.setattr(orchestrator, "_attempt", no_agent)

    outcome = await orchestrator._formalize(chapter)

    assert outcome.disposition is ExecutionDisposition.WAITING
    formalize = state.task(chapter.id, Stage.FORMALIZE)
    assert formalize.status == TaskStatus.PENDING
    assert formalize.rounds == 0
    assert "revalidation queued" in formalize.detail
    await state.close()


@pytest.mark.asyncio
async def test_auxiliary_warning_cleanup_preserves_stage_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    for stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
        await state.set_task(chapter.id, stage, TaskStatus.SUCCEEDED, "complete")
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "warning: Book/Chapter01.lean:3:1: unused variable `h`"},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
        stage=Stage.PROVE,
    )

    async def cleanup_attempt(*_args: object, **_kwargs: object) -> Attempt:
        run = await state.start_auxiliary_run(
            chapter.id,
            Stage.REVIEW,
            role=WARNING_REVIEW_ROLE,
            request_ids=(request_id,),
        )
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            report={"complete": True, "summary": "removed warning", "issues": []},
        )
        return Attempt(
            AgentResult(
                succeeded=True,
                exit_code=0,
                changed=True,
                placeholders=0,
                usage=TokenUsage(),
                report={"complete": True, "summary": "removed warning", "issues": []},
            ),
            ValidationResult(True, 0, "deferred"),
            run,
        )

    async def clean_build(
        _chapters: Iterable[Chapter],
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "clean", process_exit_code=0)}

    monkeypatch.setattr(orchestrator, "_attempt", cleanup_attempt)
    monkeypatch.setattr(orchestrator, "_build_chapters", clean_build)
    monkeypatch.setattr(
        orchestrator,
        "_publish_validated_build",
        lambda *_args: asyncio.sleep(0, result=True),
    )

    outcome = await orchestrator._drain_warning_cleanups()

    assert outcome.clean
    assert outcome.changed
    assert request_id not in state.proof_review_requests
    assert all(
        state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED
        for stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE)
    )
    assert state.task(chapter.id, Stage.REVIEW).runs[-1].auxiliary
    assert state.task(chapter.id, Stage.REVIEW).runs[-1].role == WARNING_REVIEW_ROLE
    await state.close()


@pytest.mark.asyncio
async def test_deterministic_warning_cleanup_skips_agent_after_clean_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    target = tmp_path / "lean" / "Book" / "Chapter01.lean"
    target.parent.mkdir(parents=True)
    target.write_text("  simpa using h\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: ("warning: Book/Chapter01.lean:1:2: try 'simp' instead of 'simpa'")},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
        stage=Stage.PROVE,
    )

    async def clean_build(
        _chapters: Iterable[Chapter],
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "clean", process_exit_code=0)}

    monkeypatch.setattr(orchestrator, "_build_chapters", clean_build)
    monkeypatch.setattr(
        orchestrator,
        "_publish_validated_build",
        lambda *_args: asyncio.sleep(0, result=True),
    )

    async def unexpected_agent(*_args: object, **_kwargs: object) -> Attempt:
        raise AssertionError("deterministic warning cleanup should not assign an agent")

    monkeypatch.setattr(orchestrator, "_attempt", unexpected_agent)
    feedback, request_ids = orchestrator._warning_cleanup_feedback(chapter.id)

    outcome = await orchestrator._clean_warnings_for_chapter(chapter, feedback, request_ids)

    assert outcome.clean
    assert outcome.changed
    assert target.read_text(encoding="utf-8") == "  simp using h\n"
    assert request_id not in state.proof_review_requests
    run = state.task(chapter.id, Stage.REVIEW).runs[-1]
    assert run.auxiliary
    assert run.role == scheduler_module.DETERMINISTIC_WARNING_CLEANUP_ROLE
    assert run.report is not None
    assert run.report["complete"] is True
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_failed_deterministic_build_falls_back_to_warning_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    target = tmp_path / "lean" / "Book" / "Chapter01.lean"
    target.parent.mkdir(parents=True)
    target.write_text("  simpa using h\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: ("warning: Book/Chapter01.lean:1:2: try 'simp' instead of 'simpa'")},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
        stage=Stage.PROVE,
    )
    build_count = 0

    async def builds(
        _chapters: Iterable[Chapter],
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        nonlocal build_count
        build_count += 1
        if build_count == 1:
            return {
                chapter.id: ValidationResult(
                    False,
                    1,
                    "error: Book/Chapter01.lean:1:2: deterministic edit failed",
                    process_exit_code=1,
                )
            }
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "clean", process_exit_code=0)}

    agent_feedback: list[str] = []

    async def cleanup_attempt(
        _chapter: Chapter,
        _stage: Stage,
        *,
        feedback: str,
        **_kwargs: object,
    ) -> Attempt:
        agent_feedback.append(feedback)
        run = await state.start_auxiliary_run(
            chapter.id,
            Stage.REVIEW,
            role=WARNING_REVIEW_ROLE,
            request_ids=(request_id,),
        )
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            report={"complete": True, "summary": "checked fallback", "issues": []},
        )
        return Attempt(result(changed=False, placeholders=0), ValidationResult(True, 0, "ok"), run)

    monkeypatch.setattr(orchestrator, "_build_chapters", builds)
    monkeypatch.setattr(orchestrator, "_attempt", cleanup_attempt)
    monkeypatch.setattr(
        orchestrator,
        "_publish_validated_build",
        lambda *_args: asyncio.sleep(0, result=True),
    )
    feedback, request_ids = orchestrator._warning_cleanup_feedback(chapter.id)

    outcome = await orchestrator._clean_warnings_for_chapter(chapter, feedback, request_ids)

    assert outcome.clean
    assert not outcome.changed
    assert build_count == 2
    assert len(agent_feedback) == 1
    assert agent_feedback == [feedback]
    assert request_id not in state.proof_review_requests
    assert target.read_text(encoding="utf-8") == "  simpa using h\n"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_warning_obligation_does_not_invalidate_review_dependency(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    state.source_dependency_tree = {
        "dependencies": {owner.id: [], consumer.id: [owner.id]},
    }
    for chapter in config.chapters:
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "built")
    await state.set_task(owner.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")

    await orchestrator._queue_review_feedback(
        {owner.id: "warning: Book/Chapter01.lean:3:1: unused variable `h`"},
        origin="warning-build",
        stage=Stage.REVIEW,
    )

    assert state.task(owner.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    waiting = state.task_requirements(state.task(consumer.id, Stage.REVIEW))
    assert all(
        requirement.owner_task_key != state.key(owner.id, Stage.REVIEW) for requirement in waiting
    )
    await state.close()


@pytest.mark.asyncio
async def test_formalizer_blocks_on_upstream_diagnostics_without_running_consumer_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    monkeypatch.setattr(orchestrator, "_scope_exists", lambda _chapter: asyncio.sleep(0, True))

    async def build(*_args: object, **_kwargs: object) -> dict[str, ValidationResult]:
        return {
            consumer.id: ValidationResult(
                False,
                1,
                "dependency diagnostic",
                status=ValidationStatus.DEPENDENCY_FAILED,
                blocked_by=(owner.id,),
            )
        }

    async def no_agent(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("consumer agent must not run for an upstream diagnostic")

    async def invalidate(ids: Iterable[str]) -> set[str]:
        assert tuple(ids) == (owner.id,)
        return {owner.id, consumer.id}

    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_attempt", no_agent)
    monkeypatch.setattr(orchestrator, "_invalidate_build_records", invalidate)

    outcome = await orchestrator._formalize(consumer)

    assert [requirement.owner_task_key for requirement in outcome.waiting_on] == [
        state.key(owner.id, Stage.FORMALIZE)
    ]
    assert outcome.disposition is ExecutionDisposition.WAITING
    assert state.task(owner.id, Stage.FORMALIZE).status == TaskStatus.PENDING
    consumer_task = state.task(consumer.id, Stage.FORMALIZE)
    assert consumer_task.status == TaskStatus.PENDING
    assert [requirement.owner_task_key for requirement in consumer_task.waiting_on] == [
        state.key(owner.id, Stage.FORMALIZE)
    ]
    assert state.task(consumer.id, Stage.FORMALIZE).rounds == 0
    await state.close()


@pytest.mark.asyncio
async def test_formalizer_reopens_late_upstream_diagnostic_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    review_run = await state.start_run(owner.id, Stage.REVIEW)
    await state.finish_run(review_run, status=TaskStatus.SUCCEEDED)
    await state.set_task(owner.id, Stage.PROVE, TaskStatus.SUCCEEDED, "proved")
    monkeypatch.setattr(orchestrator, "_scope_exists", lambda _chapter: asyncio.sleep(0, True))

    async def build(*_args: object, **_kwargs: object) -> dict[str, ValidationResult]:
        return {
            consumer.id: ValidationResult(
                False,
                1,
                "dependency diagnostic without a source location",
                status=ValidationStatus.DEPENDENCY_FAILED,
                blocked_by=(owner.id,),
            )
        }

    async def no_agent(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("consumer agent must not run for an upstream diagnostic")

    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_attempt", no_agent)
    monkeypatch.setattr(
        orchestrator,
        "_invalidate_build_records",
        lambda _ids: asyncio.sleep(0, {owner.id, consumer.id}),
    )

    outcome = await orchestrator._formalize(consumer)

    owner_formalize = state.task(owner.id, Stage.FORMALIZE)
    assert owner_formalize.status == TaskStatus.PENDING
    assert owner_formalize.recovering_failure
    owner_review = state.task(owner.id, Stage.REVIEW)
    assert owner_review.status == TaskStatus.PENDING
    assert owner_review.detail == "review invalidated by reopened formalization"
    owner_prove = state.task(owner.id, Stage.PROVE)
    assert owner_prove.status == TaskStatus.PENDING
    assert owner_prove.detail == "proof invalidated by reopened formalization"
    assert orchestrator._proof_review_feedback(owner.id) == ("", ())
    requirement = outcome.waiting_on[0]
    assert requirement.owner_task_key == state.key(owner.id, Stage.FORMALIZE)
    assert state.task(consumer.id, Stage.FORMALIZE).waiting_on == (requirement,)
    recovery_run = await state.start_run(owner.id, Stage.FORMALIZE)
    assert recovery_run.round == 1
    await state.close()


@pytest.mark.asyncio
async def test_pending_review_and_proof_builds_share_a_cross_stage_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    review_chapter, proof_chapter = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    commands: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    review, proof = await asyncio.gather(
        orchestrator._build_chapters(
            (review_chapter,),
            publish_if_clean=False,
            mode="review-verification",
            stage=Stage.REVIEW,
        ),
        orchestrator._build_chapters(
            (proof_chapter,),
            publish_if_clean=False,
            mode="proof-certification",
            stage=Stage.PROVE,
        ),
    )

    review_target = review_chapter.build_command.rpartition(" ")[2]
    proof_target = proof_chapter.build_command.rpartition(" ")[2]
    assert commands == [f"cd lean && lake build {review_target} {proof_target}"]
    assert review[review_chapter.id].succeeded
    assert proof[proof_chapter.id].succeeded
    assert state.task(review_chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    assert state.task(proof_chapter.id, Stage.PROVE).status == TaskStatus.PENDING
    assert state.task(review_chapter.id, Stage.PROVE).status == TaskStatus.PENDING
    assert state.task(proof_chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_successful_batch_warning_fails_only_its_owned_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    commands: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        commands.append(chapter.build_command)
        return ValidationResult(
            False,
            1,
            "warning: Book/Chapter02/Section.lean:10:2: unused variable `h`\n\n"
            "Coordinator rejected 1 non-sorry Lean warning(s):\n"
            "warning: Book/Chapter02/Section.lean:10:2: unused variable `h`",
            process_exit_code=0,
        )

    monkeypatch.setattr(scheduler_module, "validate", validation)

    first_result, second_result = await asyncio.gather(
        orchestrator._build_chapters(
            (first,),
            publish_if_clean=True,
            mode="review-verification",
            stage=Stage.REVIEW,
        ),
        orchestrator._build_chapters(
            (second,),
            publish_if_clean=True,
            mode="proof-certification",
            stage=Stage.PROVE,
        ),
    )

    assert len(commands) == 1
    assert first_result[first.id].succeeded
    assert not second_result[second.id].succeeded
    assert "Chapter02" not in first_result[first.id].output
    assert second_result[second.id].output.count("unused variable `h`") == 1
    await orchestrator.shutdown()


def test_build_feedback_routes_only_source_located_non_sorry_diagnostics(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters
    output = """⚠ [1/3] Replayed Book.Chapter01.Section
warning: Book/Chapter01/Section.lean:4:1: declaration uses `sorry`
✖ [2/3] Building Book.Chapter02.Section
error: Book/Chapter02/Section.lean:8:3: unknown identifier `missing`
warning: Book/Chapter02/Section.lean:10:2: unused variable `h`

Coordinator rejected 1 non-sorry Lean warning(s):
warning: Book/Chapter02/Section.lean:10:2: unused variable `h`
"""

    diagnostics = orchestrator._build_feedback({first.id: ValidationResult(False, 1, output)})

    assert set(diagnostics.actionable) == {second.id}
    feedback = diagnostics.actionable[second.id]
    assert "unknown identifier `missing`" in feedback
    assert feedback.count("unused variable `h`") == 1
    assert "Chapter01" not in feedback
    assert "declaration uses `sorry`" not in feedback
    assert diagnostics.deferred_owner_ids == ()


def test_build_feedback_defers_only_blocked_diagnostic_owner(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters
    output = """error: Book/Chapter01/Section.lean:4:1: first failure
error: Book/Chapter02/Section.lean:7:2: second failure
"""

    diagnostics = orchestrator._build_feedback(
        {first.id: ValidationResult(False, 1, output)},
        blocked_owner_ids={second.id},
    )

    assert set(diagnostics.actionable) == {first.id}
    assert "first failure" in diagnostics.actionable[first.id]
    assert "second failure" not in diagnostics.actionable[first.id]
    assert diagnostics.deferred_owner_ids == (second.id,)


def test_build_feedback_uses_failed_module_when_diagnostic_was_truncated(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters
    output = """Some required targets logged failures:
- Book.Chapter02.Section
error: build failed
"""

    diagnostics = orchestrator._build_feedback({first.id: ValidationResult(False, 1, output)})

    assert set(diagnostics.actionable) == {second.id}
    assert "Book.Chapter02.Section" in diagnostics.actionable[second.id]


def test_build_feedback_keeps_failed_module_alongside_another_owned_warning(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters
    output = """warning: Book/Chapter01/Section.lean:3:1: unused variable `h`
Some required targets logged failures:
- Book.Chapter02.Section
error: build failed
"""

    diagnostics = orchestrator._build_feedback({first.id: ValidationResult(False, 1, output)})

    assert set(diagnostics.actionable) == {first.id, second.id}
    assert "unused variable `h`" in diagnostics.actionable[first.id]
    assert "Book.Chapter02.Section" in diagnostics.actionable[second.id]


def test_build_feedback_falls_back_to_build_target_for_unlocated_failure(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, _ = config.chapters

    diagnostics = orchestrator._build_feedback(
        {first.id: ValidationResult(False, 124, "validation timed out", timed_out=True)}
    )

    assert set(diagnostics.actionable) == {first.id}
    assert "failed without a source-located diagnostic" in diagnostics.actionable[first.id]
    assert "validation timed out" in diagnostics.actionable[first.id]


def test_build_feedback_parses_shared_batch_output_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters
    result = ValidationResult(
        False,
        1,
        "error: Book/Chapter02/Section.lean:8:3: unknown identifier `missing`\n",
    )
    parse_calls = 0
    original = scheduler_module._lean_diagnostics

    def tracked_diagnostics(output: str) -> tuple[scheduler_module.LeanDiagnostic, ...]:
        nonlocal parse_calls
        parse_calls += 1
        return original(output)

    monkeypatch.setattr(scheduler_module, "_lean_diagnostics", tracked_diagnostics)

    diagnostics = orchestrator._build_feedback({first.id: result, second.id: result})

    assert parse_calls == 1
    assert set(diagnostics.actionable) == {second.id}


def test_diagnostic_owner_lookup_uses_identifier_index_and_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    _, second = config.chapters
    diagnostic = scheduler_module.LeanDiagnostic(
        "error",
        "error: build failed",
        "error: build failed while compiling Book.Chapter02.Section",
    )

    assert orchestrator._diagnostic_owner_ids(diagnostic) == (second.id,)

    def unexpected_lookup(_text: str) -> tuple[str, ...]:
        raise AssertionError("cached diagnostic performed another identifier lookup")

    monkeypatch.setattr(orchestrator, "_identifier_owner_ids", unexpected_lookup)
    assert orchestrator._diagnostic_owner_ids(diagnostic) == (second.id,)


@pytest.mark.asyncio
async def test_async_build_feedback_runs_off_event_loop_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    chapter = config.chapters[0]
    result = ValidationResult(False, 1, "error: build failed")
    event_loop_thread = get_ident()
    routing_thread = event_loop_thread
    original = orchestrator._build_feedback

    def tracked_feedback(
        results: dict[str, ValidationResult],
        *,
        blocked_owner_ids: set[str] | frozenset[str] = frozenset(),
    ) -> scheduler_module.BuildDiagnostics:
        nonlocal routing_thread
        routing_thread = get_ident()
        return original(results, blocked_owner_ids=blocked_owner_ids)

    monkeypatch.setattr(orchestrator, "_build_feedback", tracked_feedback)

    diagnostics = await orchestrator._build_feedback_async({chapter.id: result})

    assert diagnostics.actionable
    assert routing_thread != event_loop_thread


@pytest.mark.asyncio
async def test_capacity_exhaustion_is_a_bounded_formalizer_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    attempts: dict[int, int] = {1: 0, 2: 0}
    order: list[tuple[int, bool]] = []

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> StageOutcome:
        attempts[chapter.number] += 1
        order.append((chapter.number, rerun))
        if chapter.number == 1:
            return StageOutcome(ExecutionDisposition.FAILED)
        await orchestrator.state.set_task(
            chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized"
        )
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert not await orchestrator._formalize_all()
    assert attempts == {1: 1, 2: 1}
    assert order == [(1, False), (2, False)]
    assert (
        orchestrator.state.task(config.chapters[0].id, Stage.FORMALIZE).status == TaskStatus.FAILED
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalizer_failure_does_not_cancel_healthy_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    healthy_finished = False

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> StageOutcome:
        nonlocal healthy_finished
        assert not rerun
        if chapter.number == 1:
            return StageOutcome(ExecutionDisposition.FAILED)
        await asyncio.sleep(0.01)
        healthy_finished = True
        await orchestrator.state.set_task(
            chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized"
        )
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert not await orchestrator._formalize_all()
    assert healthy_finished
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_streaming_build_does_not_publish_partial_cache_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    published = 0

    async def successful_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    original_acquire_build = orchestrator.isolation.acquire_build

    async def track_build(build_id: str) -> object:
        workspace = await original_acquire_build(build_id)

        class TrackedBuild:
            root = workspace.root

            async def finish(
                self,
                *,
                succeeded: bool,
                publish: bool,
                retain: bool = False,
            ) -> tuple[str, ...]:
                nonlocal published
                published += int(publish)
                return await workspace.finish(
                    succeeded=succeeded,
                    publish=publish,
                    retain=retain,
                )

            async def close(self) -> None:
                await workspace.close()

        return TrackedBuild()

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)
    monkeypatch.setattr(orchestrator.isolation, "acquire_build", track_build)

    await orchestrator._build_chapters((config.chapters[0],), publish_if_clean=False)
    assert published == 0

    await orchestrator._build_all()
    assert published == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("validation_result", "expected_retain"),
    (
        (
            ValidationResult(
                False,
                1,
                "error: Book/Chapter01.lean:1:1: broken",
                process_exit_code=1,
            ),
            True,
        ),
        (
            ValidationResult(
                False,
                124,
                "coordinator validation timed out",
                timed_out=True,
                process_exit_code=-15,
            ),
            False,
        ),
    ),
)
async def test_coordinator_retains_only_completed_failed_build_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    validation_result: ValidationResult,
    expected_retain: bool,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    finished: list[tuple[bool, bool, bool]] = []

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return validation_result

    original_acquire_build = orchestrator.isolation.acquire_build

    async def track_build(build_id: str) -> object:
        workspace = await original_acquire_build(build_id)

        class TrackedBuild:
            root = workspace.root

            async def finish(
                self,
                *,
                succeeded: bool,
                publish: bool,
                retain: bool = False,
            ) -> tuple[str, ...]:
                finished.append((succeeded, publish, retain))
                return await workspace.finish(
                    succeeded=succeeded,
                    publish=publish,
                    retain=retain,
                )

            async def close(self) -> None:
                await workspace.close()

        return TrackedBuild()

    monkeypatch.setattr(scheduler_module, "validate", validation)
    monkeypatch.setattr(orchestrator.isolation, "acquire_build", track_build)

    result = await orchestrator._build_chapters(
        (config.chapters[0],),
        publish_if_clean=True,
    )

    assert not result[config.chapters[0].id].succeeded
    assert finished == [(False, False, expected_retain)]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coordinator_starts_validation_while_capturing_source_digests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    digest_started = Event()
    release_digest = Event()

    def held_scope_digest(_root: Path, _chapter: Chapter) -> str:
        digest_started.set()
        if not release_digest.wait(timeout=5):
            raise AssertionError("validation did not start while source digest capture was running")
        return "source-digest"

    async def successful_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        assert digest_started.is_set()
        assert not release_digest.is_set()
        release_digest.set()
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "scope_digest", held_scope_digest)
    monkeypatch.setattr(scheduler_module, "validate", successful_validation)

    results = await orchestrator._build_chapters((config.chapters[0],), publish_if_clean=False)

    assert results[config.chapters[0].id].succeeded
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coordinator_build_does_not_count_as_an_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    validation_started = asyncio.Event()
    release_validation = asyncio.Event()

    async def gated_validation(
        _config: object,
        chapter: Chapter,
        *,
        workspace_root: Path | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ValidationResult:
        assert workspace_root == config.settings.repo
        assert on_output is not None
        targeted = tuple(
            item
            for item in config.chapters
            if item.build_command.rpartition(" ")[2] in chapter.build_command
        )
        for index, item in enumerate(targeted):
            on_output(f"building {item.id}\n")
            on_output(f"error: Book/Chapter{item.number:02d}.lean:1:1: broken\n")
            if index == 0:
                validation_started.set()
                await release_validation.wait()
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", gated_validation)
    build = asyncio.create_task(
        orchestrator._build_chapters(
            config.chapters,
            publish_if_clean=False,
            mode="streaming",
            iteration=2,
            maximum_iterations=6,
        )
    )
    await validation_started.wait()

    assert state.coordinator_build.active
    assert state.coordinator_build.mode == "streaming"
    assert state.coordinator_build.completed == 0
    assert state.coordinator_build.total == 2
    assert state.coordinator_build.output_tail[-1] == "error: Book/Chapter01.lean:1:1: broken"
    assert state.coordinator_build.error_count == 1
    assert state.agent_summary()["active"] == 0
    assert all(
        state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.PENDING
        for chapter in config.chapters
    )

    release_validation.set()
    assert all(result.succeeded for result in (await build).values())
    assert not state.coordinator_build.active
    assert state.coordinator_build.completed == 2
    assert state.coordinator_build.output_tail == [
        "building book/chapter-01",
        "error: Book/Chapter01.lean:1:1: broken",
        "building book/chapter-02",
        "error: Book/Chapter02.lean:1:1: broken",
    ]
    assert state.coordinator_build.error_count == 2
    assert all(
        state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.PENDING
        for chapter in config.chapters
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coordinator_build_counts_only_errors_owned_by_each_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    async def validation_with_replayed_dependency_error(
        _config: object,
        chapter: Chapter,
        *,
        workspace_root: Path | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ValidationResult:
        assert workspace_root == config.settings.repo
        assert on_output is not None
        on_output("error: Book/Chapter01/Section.lean:1:1: shared dependency failure\n")
        if config.chapters[1].build_command.rpartition(" ")[2] in chapter.build_command:
            on_output("error: Book/Chapter02/Section.lean:2:1: target failure\n")
        return ValidationResult(
            False,
            1,
            "error: Book/Chapter01/Section.lean:1:1: shared dependency failure\n"
            "error: Book/Chapter02/Section.lean:2:1: target failure",
        )

    monkeypatch.setattr(scheduler_module, "validate", validation_with_replayed_dependency_error)

    await orchestrator._build_chapters(config.chapters, publish_if_clean=False)

    assert state.coordinator_build.error_count == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_validated_build_refuses_to_certify_a_newer_source_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("def built := 1\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    snapshots = {}
    results = await orchestrator._build_chapters(
        (chapter,),
        publish_if_clean=True,
        snapshots=snapshots,
    )
    assert results[chapter.id].succeeded

    source.write_text("def built := 2\n", encoding="utf-8")

    assert not await orchestrator._publish_validated_build(chapter, snapshots[chapter.id])
    assert chapter.id not in orchestrator.state.fixup_graph.get("clean", {})
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_discovery_graph_change_does_not_invalidate_validated_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    for chapter in config.chapters:
        source = tmp_path / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"def value{chapter.number} := {chapter.number}\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    initial_graph = WorkUnitImportGraph(
        dependencies={first.id: frozenset(), second.id: frozenset()},
        successors={first.id: frozenset(), second.id: frozenset()},
        order=(first.id, second.id),
        edges=(),
    )
    refined_graph = WorkUnitImportGraph(
        dependencies={first.id: frozenset(), second.id: frozenset({first.id})},
        successors={first.id: frozenset({second.id}), second.id: frozenset()},
        order=(first.id, second.id),
        edges=((first.id, second.id),),
    )
    observed_graph = initial_graph

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        nonlocal observed_graph
        observed_graph = refined_graph
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(orchestrator, "_observed_work_unit_graph", lambda: observed_graph)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    snapshots: dict[str, scheduler_module.ValidatedBuildSnapshot] = {}

    results = await orchestrator._build_chapters(
        (second,), publish_if_clean=True, snapshots=snapshots
    )

    assert results[second.id].succeeded
    assert snapshots[second.id].graph is initial_graph
    assert await orchestrator._publish_validated_build(second, snapshots[second.id])
    assert second.id in orchestrator.state.formalize_graph["clean"]
    assert orchestrator.state.formalize_graph["edges"] == [list(refined_graph.edges[0])]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_overlay_build_releases_source_barrier_and_retries_concurrent_edit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("def built := 1\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    monkeypatch.setattr(orchestrator.isolation, "name", "fuse-overlay")
    validation_started = asyncio.Event()
    release_validation = asyncio.Event()
    validations = 0

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        nonlocal validations
        validations += 1
        validation_started.set()
        await release_validation.wait()
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    build = asyncio.create_task(orchestrator._build_chapters((chapter,), publish_if_clean=True))
    await validation_started.wait()

    await asyncio.wait_for(orchestrator.source_lock.acquire(), timeout=0.5)
    source.write_text("def built := 2\n", encoding="utf-8")
    orchestrator.source_lock.release()
    release_validation.set()

    result = (await build)[chapter.id]
    assert result.succeeded
    assert validations == 2
    assert orchestrator.state.proof_review_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_build_dispatch_coalesces_one_batch_behind_coordinator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(project)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    release = asyncio.Event()
    first_started = asyncio.Event()
    maximum_active = 0
    active = 0
    batch_sizes: list[int] = []

    async def held_batch(requests: tuple[scheduler_module.PendingBuildRequest, ...]) -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        batch_sizes.append(len(requests))
        first_started.set()
        try:
            await release.wait()
            for request in requests:
                request.future.set_result(
                    {chapter.id: ValidationResult(True, 0, "ok") for chapter in request.chapters}
                )
        finally:
            active -= 1

    monkeypatch.setattr(orchestrator, "_run_build_batch", held_batch)
    builds = [
        asyncio.create_task(
            orchestrator._build_chapters(
                (config.chapters[0],),
                publish_if_clean=False,
            )
        )
    ]
    await first_started.wait()
    builds.extend(
        asyncio.create_task(
            orchestrator._build_chapters(
                (config.chapters[index % len(config.chapters)],),
                publish_if_clean=False,
            )
        )
        for index in range(1, 6)
    )
    builds.extend(
        asyncio.create_task(
            orchestrator._build_chapters(
                (config.chapters[index % len(config.chapters)],),
                publish_if_clean=False,
            )
        )
        for index in range(6, 12)
    )
    for _ in range(10):
        if orchestrator._pending_build_requests:
            break
        await asyncio.sleep(0)
    assert len(orchestrator._pending_build_requests) == 11
    release.set()
    await asyncio.gather(*builds)
    assert maximum_active == 1
    assert batch_sizes == [1, 11]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_agent_summary_separates_started_and_queued_runs(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    config = replace(config, settings=replace(config.settings, max_agents=1))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    class GatedExecutor(CodexExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            del stage, feedback, workspace_root
            if chapter.number == 1:
                first_started.set()
                await release_first.wait()
            agent = result(changed=False)
            await state.finish_run(run, status=TaskStatus.SUCCEEDED)
            return agent

    orchestrator.executor = GatedExecutor(config, state)
    first = asyncio.create_task(orchestrator._attempt(config.chapters[0], Stage.FORMALIZE))
    await first_started.wait()
    second = asyncio.create_task(orchestrator._attempt(config.chapters[1], Stage.FORMALIZE))
    await asyncio.sleep(0.05)

    summary = state.agent_summary()
    assert summary["active"] == 1
    assert summary["queued"] == 1
    assert summary["by_stage"]["formalize"] == 1
    queued_task = state.task(config.chapters[1].id, Stage.FORMALIZE)
    assert queued_task.status == TaskStatus.PENDING
    assert queued_task.queued is True

    release_first.set()
    await asyncio.gather(first, second)
    assert state.agent_summary()["queued"] == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_completed_agent_transitions_to_visible_postprocessing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    entered_postprocess = asyncio.Event()
    release_postprocess = asyncio.Event()
    original_set_phase = state.set_task_phase

    class ImmediateExecutor(CodexExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            del chapter, stage, feedback, workspace_root
            await state.finish_run(run, status=TaskStatus.SUCCEEDED)
            return result(changed=False)

    async def hold_postprocess(
        chapter_id: str, stage: Stage, phase: TaskPhase, detail: str
    ) -> None:
        await original_set_phase(chapter_id, stage, phase, detail)
        entered_postprocess.set()
        await release_postprocess.wait()

    orchestrator.executor = ImmediateExecutor(config, state)
    monkeypatch.setattr(state, "set_task_phase", hold_postprocess)
    attempt = asyncio.create_task(orchestrator._attempt(config.chapters[0], Stage.FORMALIZE))
    await entered_postprocess.wait()

    task = state.task(config.chapters[0].id, Stage.FORMALIZE)
    assert task.status == TaskStatus.RUNNING
    assert task.phase == TaskPhase.POSTPROCESS
    assert task.detail == "postprocessing completed formalize agent result"
    assert state.agent_summary()["active"] == 0
    assert state.agent_summary()["postprocessing"] == 1

    release_postprocess.set()
    await attempt
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_discovery_uses_a_separate_agent_pool(tmp_path: Path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    source = tmp_path / "books" / "book.md"
    source.write_text(
        source.read_text(encoding="utf-8") + "\n## 3. Third chapter\n",
        encoding="utf-8",
    )
    config = load_config(config_path)
    config = replace(
        config,
        settings=replace(config.settings, max_agents=1),
        stages={
            **config.stages,
            Stage.DISCOVER: replace(config.stages[Stage.DISCOVER], max_agents=1),
        },
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    discovery_started = asyncio.Event()
    formalize_started = asyncio.Event()
    release_discovery = asyncio.Event()
    release_formalize = asyncio.Event()
    starts: list[tuple[Stage, int]] = []

    class GatedExecutor(CodexExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            del feedback, workspace_root
            starts.append((stage, chapter.number))
            if stage is Stage.DISCOVER and chapter.number == 1:
                discovery_started.set()
                await release_discovery.wait()
            elif stage is Stage.FORMALIZE:
                formalize_started.set()
                await release_formalize.wait()
            agent = result(changed=False)
            await state.finish_run(run, status=TaskStatus.SUCCEEDED)
            return agent

    orchestrator.executor = GatedExecutor(config, state)
    first_discovery = asyncio.create_task(orchestrator._attempt(config.chapters[0], Stage.DISCOVER))
    await discovery_started.wait()
    second_discovery = asyncio.create_task(
        orchestrator._attempt(config.chapters[1], Stage.DISCOVER)
    )
    formalize = asyncio.create_task(orchestrator._attempt(config.chapters[2], Stage.FORMALIZE))
    await formalize_started.wait()
    await asyncio.sleep(0)

    assert (Stage.DISCOVER, 2) not in starts
    summary = state.agent_summary()
    assert summary["by_stage"]["discover"] == 1
    assert summary["by_stage"]["formalize"] == 1
    assert summary["maximum_by_pool"] == {"discover": 1, "mutating": 1}

    release_formalize.set()
    release_discovery.set()
    await asyncio.gather(first_discovery, second_discovery, formalize)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_changed_review_is_rebuilt_fixed_and_reviewed_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.set_task(
        config.chapters[0].id,
        Stage.FORMALIZE,
        TaskStatus.SUCCEEDED,
        "clean formalization",
    )
    review_path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text("def beforeReview := 1\n", encoding="utf-8")
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(
                changed=True,
            ),
            result(changed=True),
        ],
    )
    stages_seen: list[Stage] = []
    original_run = orchestrator.executor.run

    async def tracked_run(
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        stages_seen.append(stage)
        if stage is Stage.REVIEW and stages_seen.count(Stage.REVIEW) == 1:
            assert workspace_root is not None
            target = workspace_root / "lean" / "Book" / "Chapter01.lean"
            target.write_text("def afterReview := 1\n", encoding="utf-8")
        elif stage is Stage.REVIEW and stages_seen.count(Stage.REVIEW) == 2:
            assert workspace_root is not None
            target = workspace_root / "lean" / "Book" / "Chapter01.lean"
            target.write_text("def afterRepair := 1\n", encoding="utf-8")
        return await original_run(
            chapter,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )

    builds = iter(
        (
            ValidationResult(True, 0, "ok"),
            ValidationResult(False, 1, "error: Book/Chapter01.lean:1:1: unknown declaration"),
            ValidationResult(True, 0, "ok"),
        )
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return next(builds)

    monkeypatch.setattr(orchestrator.executor, "run", tracked_run)
    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator._review_until_clean()
    assert stages_seen == [Stage.REVIEW, Stage.REVIEW]
    assert review_path.read_text(encoding="utf-8") == "def afterRepair := 1\n"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_incomplete_changed_review_gets_another_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.set_task(
        config.chapters[0].id,
        Stage.FORMALIZE,
        TaskStatus.SUCCEEDED,
        "formalization complete",
    )
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(
                changed=True,
                complete=False,
                issues=["statement needs a hypothesis"],
            ),
            result(changed=False),
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator._review_until_clean()
    review_task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert review_task.rounds == 2
    assert review_task.status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_changed_review_remains_active_until_rebuild_finishes(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [result(changed=True)])

    outcome = await orchestrator._review_once(config.chapters[0])

    assert outcome.succeeded
    assert outcome.changed
    review_task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert review_task.status == TaskStatus.RUNNING
    assert review_task.detail == "review changes merged; coordinator verification queued"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_classifies_stale_isolation_as_a_fresh_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    chapter = config.chapters[0]
    run = await state.start_run(chapter.id, Stage.REVIEW)
    stale_agent = replace(
        result(changed=True),
        succeeded=False,
        error="assigned scope changed after this agent started; retry on a fresh generation",
    )

    async def attempt(*_args: object, **_kwargs: object) -> Attempt:
        return Attempt(
            stale_agent,
            ValidationResult(
                False,
                1,
                "Isolation rejected the agent result",
                status=ValidationStatus.STALE_SNAPSHOT,
            ),
            run,
        )

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    outcome = await orchestrator._review_once(chapter)

    assert outcome.waiting
    assert outcome.retry_fresh
    assert not outcome.changed
    task = state.task(chapter.id, Stage.REVIEW)
    assert task.status == TaskStatus.RUNNING
    assert task.detail == "review snapshot became stale; fresh isolation retry queued"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_snapshot_and_merge_do_not_acquire_coordinator_build_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [result(changed=False)])

    async def forbidden_build_queue(**_kwargs: object) -> object:
        raise AssertionError("agent snapshot and merge must not acquire the Lake-build queue")

    monkeypatch.setattr(orchestrator.build_queue, "acquire", forbidden_build_queue)

    outcome = await orchestrator._review_once(config.chapters[0])
    assert outcome.succeeded
    assert not outcome.changed
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_failure_quarantines_branch_without_cancelling_unrelated_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = with_example_modules(load_config(project))
    first, second, third = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    (source_root / "Chapter03.lean").write_text("def third := 3\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator, {second.id: (first.id,)})
    third_started = asyncio.Event()
    healthy_finished = False
    healthy_proved = False
    reviewed: list[str] = []

    async def review(
        chapter: Chapter,
        _rounds_used: dict[str, int],
        *,
        rerun: bool = False,
    ) -> StageOutcome:
        nonlocal healthy_finished
        assert not rerun
        reviewed.append(chapter.id)
        if chapter.id == first.id:
            await third_started.wait()
            return StageOutcome(ExecutionDisposition.FAILED)
        if chapter.id == third.id:
            third_started.set()
            await asyncio.sleep(0.01)
            healthy_finished = True
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        raise AssertionError("a dependent of the failed review must not start")

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> StageOutcome:
        assert defer_review
        nonlocal healthy_proved
        assert chapter.id == third.id
        healthy_proved = True
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert not await orchestrator._review_tree(prove=True)
    assert healthy_finished
    assert healthy_proved
    assert set(reviewed) == {first.id, third.id}
    assert state.task(first.id, Stage.REVIEW).status == TaskStatus.FAILED
    dependent_review = state.task(second.id, Stage.REVIEW)
    assert dependent_review.status == TaskStatus.PENDING
    assert state.failure_roots(dependent_review) == (state.key(first.id, Stage.REVIEW),)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_dirty_proof_scope_defers_only_that_chapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    orchestrator.force = True

    async def dirty_attempt(*_args: object, **_kwargs: object) -> object:
        raise GitCommitError(
            "cannot start book/chapter-01 with uncommitted files in its exclusive scope"
        )

    monkeypatch.setattr(orchestrator, "_attempt", dirty_attempt)

    assert not await orchestrator._prove(chapter, defer_review=True)
    task = state.task(chapter.id, Stage.PROVE)
    assert task.status == TaskStatus.PENDING
    assert "dirty exclusive scope" in task.detail
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_is_capped_at_three_edit_rebuild_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    reviews = 0
    rebuilds = 0

    async def changed_review(
        _chapter: Chapter, *, rerun: bool = False, feedback: str = ""
    ) -> StageOutcome:
        del feedback
        nonlocal reviews
        assert rerun == (reviews > 0)
        reviews += 1
        path = tmp_path / "lean" / "Book" / "Chapter01.lean"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            (path.read_text(encoding="utf-8") if path.exists() else "")
            + f"\n-- review pass {reviews}\n",
            encoding="utf-8",
        )
        return StageOutcome(ExecutionDisposition.SUCCEEDED, changed=True)

    async def review_build(_chapter: Chapter) -> dict[str, str]:
        nonlocal rebuilds
        rebuilds += 1
        return {}

    monkeypatch.setattr(orchestrator, "_review_once", changed_review)
    monkeypatch.setattr(orchestrator, "_review_build", review_build)

    assert await orchestrator._review_until_clean()
    assert reviews == 3
    assert rebuilds == 4
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_failure_consumes_a_review_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    outcomes = iter((StageOutcome(ExecutionDisposition.FAILED, changed=False, complete=False),))

    async def clean(*_args: object, **_kwargs: object) -> dict[str, str]:
        return {}

    async def review(_chapter: Chapter, *, rerun: bool = False, feedback: str = "") -> StageOutcome:
        del rerun, feedback
        return next(outcomes)

    monkeypatch.setattr(orchestrator, "_review_build", clean)
    monkeypatch.setattr(orchestrator, "_review_once", review)
    rounds = {chapter.id: 0}

    assert not await orchestrator._review_chapter_to_clean(chapter, rounds)
    assert rounds[chapter.id] == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_stale_review_snapshot_retries_fresh_without_consuming_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    outcomes = iter(
        (
            StageOutcome(
                ExecutionDisposition.WAITING,
                changed=False,
                complete=False,
                retry_fresh=True,
            ),
            StageOutcome(ExecutionDisposition.SUCCEEDED, changed=False, complete=True),
        )
    )
    reruns: list[bool] = []

    async def clean(*_args: object, **_kwargs: object) -> dict[str, str]:
        return {}

    async def review(_chapter: Chapter, *, rerun: bool = False, feedback: str = "") -> StageOutcome:
        del feedback
        reruns.append(rerun)
        return next(outcomes)

    monkeypatch.setattr(orchestrator, "_review_build", clean)
    monkeypatch.setattr(orchestrator, "_review_once", review)
    rounds = {chapter.id: 0}

    assert await orchestrator._review_chapter_to_clean(chapter, rounds)
    assert rounds[chapter.id] == 1
    assert reruns == [False, False]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_failure_stops_proof_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    run = await orchestrator.state.start_run(chapter.id, Stage.PROVE)
    capacity = replace(
        result(changed=False, placeholders=1),
        succeeded=False,
        exit_code=1,
        capacity_exhausted=True,
    )
    agents = iter((capacity,))
    calls = 0

    async def attempt(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return scheduler_module.Attempt(next(agents), ValidationResult(True, 0, "ok"), run)

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    assert not await orchestrator._prove(chapter)
    assert calls == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_releases_agent_slot_before_coordinator_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, max_agents=1))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(orchestrator.state, [result(changed=True, placeholders=0)])

    async def build(
        _chapters: object,
        *,
        snapshots: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, ValidationResult]:
        assert orchestrator.agent_slots.available == 1
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "ok")}

    async def publish(_chapter: Chapter, _snapshot: object) -> bool:
        return True

    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)

    attempt = await orchestrator._attempt(chapter, Stage.PROVE)

    assert attempt.validation.succeeded
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_stale_assigned_target_is_rescanned_and_certified_without_agent_edits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by trivial\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        orchestrator.state, [result(changed=False, placeholders=1)]
    )
    builds = 0

    async def build(
        _chapters: object, *, snapshots: dict[str, object], **_kwargs: object
    ) -> dict[str, ValidationResult]:
        nonlocal builds
        builds += 1
        snapshots[chapter.id] = object()
        return {chapter.id: ValidationResult(True, 0, "ok")}

    async def publish(_chapter: Chapter, _snapshot: object) -> bool:
        return True

    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)
    stale = ProofTarget(
        path="lean/Book/Chapter01.lean",
        declaration="target",
        line=1,
        end_line=1,
        placeholder_count=1,
        fingerprint="stale-target",
    )

    attempt = await orchestrator._attempt(chapter, Stage.PROVE, proof_targets=(stale,))

    assert attempt.validation.succeeded
    assert builds == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_targeted_live_retry_resumes_only_selected_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    executor = FakeExecutor(state, [result(changed=False)])
    started = asyncio.Event()

    async def blocked_run(
        _chapter: Chapter,
        _stage: Stage,
        run: RunRecord,
        **_kwargs: Any,
    ) -> AgentResult:
        await state.update_run(run, thread_id="live-session")
        started.set()
        try:
            await asyncio.Future()
            raise AssertionError("blocked agent unexpectedly continued without cancellation")
        except asyncio.CancelledError:
            await state.finish_run(
                run,
                status=TaskStatus.INTERRUPTED,
                thread_id="live-session",
            )
            raise

    monkeypatch.setattr(executor, "run", blocked_run)
    orchestrator.executor = executor
    attempt_task = asyncio.create_task(orchestrator._attempt(chapter, Stage.REVIEW))
    await started.wait()
    first_run = state.task(chapter.id, Stage.REVIEW).runs[-1]

    response = orchestrator.retry_live_agent("1")
    attempt = await attempt_task

    assert response == {
        "accepted": True,
        "chapter_id": chapter.id,
        "stage": "review",
        "interrupted_run_id": first_run.id,
    }
    assert first_run.status == TaskStatus.INTERRUPTED
    assert attempt.run.id != first_run.id
    assert attempt.run.resumed_from_run_id == first_run.id
    assert executor.resume_thread_ids == ["live-session"]
    assert "operator requested a targeted retry" in executor.resume_prompts[0]
    assert not orchestrator._live_agent_tasks
    await orchestrator.shutdown()


def test_proof_feedback_is_bounded_and_retains_latest_diagnostics() -> None:
    feedback = scheduler_module._bounded_proof_feedback(("old" * 20_000, "latest diagnostic"))

    assert len(feedback) == scheduler_module.PROOF_FEEDBACK_MAX_CHARS
    assert "older proof feedback omitted" in feedback
    assert feedback.endswith("latest diagnostic")


def test_proof_feedback_is_target_filtered_and_deduplicated(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    state.proof_blockers = {
        "B1": {
            "id": "B1",
            "consumer_chapter_id": config.chapters[0].id,
            "path": "lean/Book/Chapter01.lean",
            "declaration": "Book.assigned",
            "remaining_goal": "⊢ Nonempty (True)",
            "obstruction": "old evidence",
            "status": ProofBlockerStatus.OPEN.value,
            "updated_at": "2026-01-01T00:00:00+00:00",
        },
        "B2": {
            "id": "B2",
            "consumer_chapter_id": config.chapters[0].id,
            "path": "lean/Book/Chapter01.lean",
            "declaration": "Book.assigned",
            "remaining_goal": "  ⊢   True ",
            "obstruction": "latest evidence",
            "status": ProofBlockerStatus.OPEN.value,
            "updated_at": "2026-01-02T00:00:00+00:00",
        },
        "B3": {
            "id": "B3",
            "consumer_chapter_id": config.chapters[0].id,
            "path": "lean/Book/Chapter01.lean",
            "declaration": "Book.reserved",
            "remaining_goal": "⊢ False",
            "obstruction": "unrelated",
            "status": ProofBlockerStatus.OPEN.value,
            "updated_at": "2026-01-03T00:00:00+00:00",
        },
    }
    orchestrator = Orchestrator(config, state)
    target = ProofTarget(
        path="lean/Book/Chapter01.lean",
        declaration="assigned",
        line=1,
        end_line=2,
        placeholder_count=1,
        fingerprint="target",
    )

    feedback = orchestrator._durable_blocker_feedback(config.chapters[0].id, (target,))

    assert "B2" in feedback
    assert "latest evidence" in feedback
    assert "B1" not in feedback
    assert "B3" not in feedback
    assert Orchestrator._obsolete_dependency_blocker(
        {"obstruction": "The imported dependency failed before the target was checked."}
    )


@pytest.mark.parametrize("severity", ["error", "warning"])
def test_proof_chunk_validation_only_rejects_diagnostics_in_assigned_spans(
    severity: str,
) -> None:
    target = ProofTarget(
        path="lean/Book/Chapter01.lean",
        declaration="assigned",
        line=10,
        end_line=20,
        placeholder_count=1,
        fingerprint="assigned-target",
    )
    outside = ValidationResult(
        False,
        1,
        f"{severity}: lean/Book/Chapter01.lean:25:3: diagnostic outside the chunk",
        process_exit_code=1 if severity == "error" else 0,
    )
    inside = ValidationResult(
        False,
        1,
        f"{severity}: /workspace/lean/Book/Chapter01.lean:15:3: diagnostic in the chunk",
        process_exit_code=1 if severity == "error" else 0,
    )
    unlocated = ValidationResult(False, 1, f"{severity}: dependency build failed")

    scoped_outside = Orchestrator._proof_chunk_validation(outside, (target,))
    scoped_inside = Orchestrator._proof_chunk_validation(inside, (target,))
    scoped_unlocated = Orchestrator._proof_chunk_validation(unlocated, (target,))

    assert scoped_outside.succeeded
    assert "belonged to the assigned proof chunk" in scoped_outside.output
    assert not scoped_inside.succeeded
    assert "diagnostic in the chunk" in scoped_inside.output
    assert scoped_unlocated.succeeded


@pytest.mark.asyncio
async def test_unrelated_diagnostic_does_not_consume_chunk_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.PROVE: replace(config.stages[Stage.PROVE], max_rounds=2, chunk_size=1),
        },
    )
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "theorem first : True := by sorry\n"
        "theorem second : True := by sorry\n"
        "theorem broken : True := by sorry\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    attempted: list[str] = []

    async def attempt(*_args: object, **kwargs: object) -> scheduler_module.Attempt:
        targets = kwargs["proof_targets"]
        assert isinstance(targets, tuple)
        target = targets[0]
        attempted.append(target.declaration)
        changed = target.declaration != "broken"
        if changed:
            source.write_text(
                source.read_text(encoding="utf-8").replace(
                    f"theorem {target.declaration} : True := by sorry",
                    f"theorem {target.declaration} : True := by trivial",
                ),
                encoding="utf-8",
            )
        run = await state.start_run(chapter.id, Stage.PROVE)
        run.proof_targets = [item.as_dict() for item in targets]
        agent = result(changed=changed, placeholders=1)
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            changed=changed,
            placeholders=1,
            report=agent.report,
        )
        validation = ValidationResult(
            False,
            1,
            "error: lean/Book/Chapter01.lean:3:1: diagnostic in another declaration",
            process_exit_code=1,
        )
        return scheduler_module.Attempt(agent, validation, run)

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    assert not await orchestrator._prove(chapter)
    assert attempted == ["first", "second", "broken", "broken"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_stale_build_is_refreshed_before_proof_agent_with_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    events: list[str] = []

    async def validation(
        _config: object,
        _chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        events.append("build")
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    fake = FakeExecutor(orchestrator.state, [result(changed=True, placeholders=0)])
    original_run = fake.run

    async def tracked_agent(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        events.append("agent")
        agent = await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        assert workspace_root is not None
        target = workspace_root / "lean" / "Book" / "Chapter01.lean"
        target.write_text(
            target.read_text(encoding="utf-8").replace("by sorry", "by trivial"),
            encoding="utf-8",
        )
        return agent

    monkeypatch.setattr(fake, "run", tracked_agent)
    orchestrator.executor = fake

    assert await orchestrator._prove(chapter)
    assert events[:2] == ["build", "agent"]
    assert events.count("build") == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_placeholder_free_proof_does_not_run_agent_after_failed_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by trivial\n", encoding="utf-8")
    builds = 0

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        nonlocal builds
        builds += 1
        return ValidationResult(False, 1, "error: dependency is broken")

    async def forbidden_agent(*_args: object, **_kwargs: object) -> AgentResult:
        raise AssertionError("a placeholder-free failed refresh must not launch a proof agent")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    monkeypatch.setattr(orchestrator.executor, "run", forbidden_agent)

    assert not await orchestrator._prove(chapter)
    assert builds == 1
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.PENDING
    assert proof.detail == "pre-existing coordinator diagnostics routed before proof work"
    assert proof.rounds == 0
    request = next(iter(orchestrator.state.proof_review_requests.values()))
    assert request["kind"] == BUILD_ERROR_REVIEW_KIND
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coordinator_proof_scan_replaces_stale_agent_sorry_count(
    tmp_path: Path,
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by trivial\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    old_run = await orchestrator.state.start_run(chapter.id, Stage.REVIEW)
    await orchestrator.state.finish_run(
        old_run,
        status=TaskStatus.SUCCEEDED,
        placeholders=106,
    )
    await mark_clean_formalization(orchestrator)

    assert await orchestrator._prove(chapter)
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.SUCCEEDED
    assert (
        orchestrator.state.snapshot()["tasks"][orchestrator.state.key(chapter.id, Stage.PROVE)][
            "sorry_count"
        ]
        == 0
    )
    await orchestrator.shutdown()

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    assert reloaded.snapshot()["tasks"][reloaded.key(chapter.id, Stage.PROVE)]["sorry_count"] == 0
    await reloaded.close()


@pytest.mark.asyncio
async def test_dirty_proof_baseline_routes_diagnostics_before_placeholder_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    builds = 0

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        nonlocal builds
        builds += 1
        return ValidationResult(False, 1, "error: dependency is broken")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        orchestrator.state,
        [
            result(changed=True, placeholders=0),
            result(changed=False, placeholders=0),
            result(changed=False, placeholders=0),
        ],
    )

    assert not await orchestrator._prove(chapter)
    assert builds == 1
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.PENDING
    assert proof.rounds == 0
    assert len(orchestrator.executor.results) == 3
    request = next(iter(orchestrator.state.proof_review_requests.values()))
    assert request["kind"] == BUILD_ERROR_REVIEW_KIND
    assert chapter.id in request["feedback"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_standalone_failed_proof_attempt_queues_durable_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")

    async def validation(
        _config: object,
        _chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    assert (await orchestrator._refresh_stale_proof_build(chapter)).succeeded
    await orchestrator._complete_review(chapter, "reviewed")
    orchestrator.executor = FakeExecutor(
        orchestrator.state,
        [
            result(
                changed=False,
                failed_attempts=[
                    failed_attempt("the statement needs another hypothesis")
                    | {"disposition": "statement_review"}
                ],
            ),
            result(changed=False, failed_attempts=[]),
        ],
    )
    orchestrator.executor.results[1].report["blocker_refs"] = ["B1"]

    assert not await orchestrator._prove(chapter)
    review = orchestrator.state.task(chapter.id, Stage.REVIEW)
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert review.status == TaskStatus.SUCCEEDED
    assert proof.status == TaskStatus.BLOCKED, (
        proof.detail,
        orchestrator.state.proof_blockers,
        orchestrator.state.package_state.as_dict(),
    )
    assert proof.source_digest is None
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    assert feedback == ""
    assert request_ids == ()
    assert len(orchestrator.state.upstream_requests) == 1
    assert orchestrator.state.fixup_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_structural_blockers_create_one_upstream_request_without_ping_pong(
    tmp_path: Path,
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    first, second = config.chapters

    async def request(chapter: Chapter, blocker_id: str) -> str:
        path = f"lean/Book/Chapter{chapter.number:02d}.lean"
        attempt = failed_attempt(
            "the shared bridge is missing",
            path=path,
            declaration=f"Book.consumer{chapter.number}",
        ) | {"disposition": "missing_capability"}
        candidate = {
            "capability_key": "Book.sharedBridge",
            "blocked_declaration": attempt["declaration"],
            "consumer_path": path,
            "residual_goal": attempt["remaining_goal"],
            "needed_result": "A shared bridge theorem",
            "candidate_signature": "sharedBridge : True",
            "owner_kind": "shared",
            "owner_chapter_id": first.id,
            "owner_paths": ["lean/Book/Chapter01.lean"],
            "attempted_alternatives": attempt["attempts"],
            "acceptance_tests": [chapter.build_command],
        }
        blockers = await orchestrator.state.record_proof_blockers(
            chapter.id,
            origin_run_id=f"run-{blocker_id}",
            failed_attempts=(attempt,),
            capability_candidates=(candidate,),
        )
        request_ids = await orchestrator._request_upstream_for_blockers(chapter, blockers)
        return request_ids[0]

    first_request = await request(second, "second")
    repeated_request = await request(second, "second-repeat")

    assert first_request == repeated_request
    assert len(orchestrator.state.upstream_requests) == 1
    assert orchestrator.state.upstream_requests[first_request]["owner_chapter_id"] == first.id
    assert orchestrator.state.package_state.packages == {}
    assert not orchestrator._package_tasks
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_requests_launch_one_global_steward(tmp_path: Path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3, 4]")
    source = tmp_path / "books" / "book.md"
    source.write_text(
        source.read_text(encoding="utf-8")
        + "\n## 3. Third chapter\n\nThird.\n\n## 4. Fourth chapter\n\nFourth.\n",
        encoding="utf-8",
    )
    config = with_example_modules(load_config(config_path))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.state.load_or_create()
    consumer = config.chapters[3]
    for owner in config.chapters[:3]:
        await orchestrator.state.enqueue_upstream_request(
            {
                "consumer_path": "lean/Book/Chapter04.lean",
                "blocked_declaration": f"Book.consumer{owner.number}",
                "residual_goal": "True",
                "needed_result": f"support from chapter {owner.number}",
                "owner_paths": [f"lean/Book/Chapter{owner.number:02d}.lean"],
                "attempted_alternatives": ["simp", "exact existing"],
            },
            consumer_chapter_id=consumer.id,
            owner_chapter_id=owner.id,
            blocker_ids=(f"missing-{owner.number}",),
        )

    await orchestrator._route_migrated_upstream_requests()

    assert orchestrator._upstream_steward_task is not None
    assert not orchestrator.state.proof_review_requests
    assert set(orchestrator._upstream_steward_dossier()["requests"]) == set(
        orchestrator.state.upstream_requests
    )
    orchestrator._upstream_steward_task.cancel()
    await asyncio.gather(orchestrator._upstream_steward_task, return_exceptions=True)
    orchestrator._upstream_steward_task = None
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_global_steward_resumes_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "consumer_path": "lean/Book/Chapter01.lean",
            "blocked_declaration": "Book.target",
            "residual_goal": "True",
            "needed_result": "a shared bridge",
            "owner_paths": ["lean/Book/Chapter01.lean"],
        },
        consumer_chapter_id=chapter.id,
        owner_chapter_id=chapter.id,
    )
    interrupted = await state.start_auxiliary_run(
        chapter.id,
        Stage.DISCOVER,
        role="upstream_steward",
        request_ids=(request_id,),
    )
    await state.finish_run(
        interrupted,
        status=TaskStatus.INTERRUPTED,
        thread_id="global-steward-session",
    )
    orchestrator = Orchestrator(config, state, resume_agents=True)
    observed: dict[str, object] = {}

    async def run_steward(
        _anchor: Chapter,
        run: RunRecord,
        _dossier: dict[str, Any],
        **kwargs: object,
    ) -> AgentResult:
        observed.update(run=run, **kwargs)
        report = {
            "complete": True,
            "summary": "placed request",
            "issues": [],
            "cases": [
                {
                    "case_id": "case-a",
                    "disposition": "implement",
                    "request_ids": [request_id],
                    "context_work_unit_ids": [chapter.id],
                }
            ],
        }
        await state.finish_run(run, status=TaskStatus.SUCCEEDED, report=report)
        return AgentResult(
            succeeded=True,
            exit_code=0,
            changed=False,
            placeholders=0,
            usage=TokenUsage(),
            report=report,
        )

    monkeypatch.setattr(orchestrator.executor, "run_upstream_steward", run_steward)

    await orchestrator._run_upstream_steward()

    assert observed["run"] is interrupted
    assert observed["resume_thread_id"] == "global-steward-session"
    assert observed["resume_run_id"] == interrupted.id
    steward_runs = [run for run in state.chapter_runs(chapter.id) if run.role == "upstream_steward"]
    assert steward_runs == [interrupted]
    assert state.steward_cases["case-a"]["steward_run_id"] == interrupted.id
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_implementation_resumes_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "consumer_path": "lean/Book/Chapter01.lean",
            "blocked_declaration": "Book.target",
            "residual_goal": "True",
            "needed_result": "a shared bridge",
            "owner_paths": ["lean/Book/Chapter01.lean"],
        },
        consumer_chapter_id=chapter.id,
        owner_chapter_id=chapter.id,
    )
    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [request_id],
                "context_work_unit_ids": [chapter.id],
            }
        ]
    )
    interrupted = await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="upstream_implementation",
        request_ids=("case-a", request_id),
    )
    await state.finish_run(
        interrupted,
        status=TaskStatus.INTERRUPTED,
        thread_id="implementation-session",
    )
    orchestrator = Orchestrator(config, state, resume_agents=True)
    observed: dict[str, object] = {}

    async def attempt(
        _chapter: Chapter,
        _stage: Stage,
        **kwargs: object,
    ) -> Attempt:
        observed.update(kwargs)
        resumed = kwargs["resume_run"]
        assert isinstance(resumed, RunRecord)
        await state.resume_auxiliary_run(resumed)
        report = {
            "complete": True,
            "summary": "consumer already has the needed interface",
            "issues": [],
            "disposition": "consumer_local",
        }
        agent = AgentResult(
            succeeded=True,
            exit_code=0,
            changed=False,
            placeholders=0,
            usage=TokenUsage(),
            report=report,
        )
        await state.finish_run(resumed, status=TaskStatus.SUCCEEDED, report=report)
        return Attempt(agent, ValidationResult(True, 0, "ok"), resumed)

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    await orchestrator._run_upstream_implementation("case-a")

    assert observed["resume_run"] is interrupted
    assert observed["resume_thread_id"] == "implementation-session"
    assert observed["resume_run_id"] == interrupted.id
    implementation_runs = [
        run for run in state.chapter_runs(chapter.id) if run.role == "upstream_implementation"
    ]
    assert implementation_runs == [interrupted]
    assert state.steward_cases["case-a"]["implementation_run_ids"] == [interrupted.id]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_implementation_infrastructure_failure_fails_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "consumer_path": "lean/Book/Chapter01.lean",
            "blocked_declaration": "Book.target",
            "residual_goal": "True",
            "needed_result": "a shared bridge",
            "owner_paths": ["lean/Book/Chapter01.lean"],
        },
        consumer_chapter_id=chapter.id,
        owner_chapter_id=chapter.id,
    )
    await state.update_upstream_request(request_id, UpstreamRequestStatus.EVALUATING)
    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [request_id],
                "context_work_unit_ids": [chapter.id],
            }
        ]
    )
    run = await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="upstream_implementation",
        request_ids=("case-a", request_id),
    )
    orchestrator = Orchestrator(config, state)

    async def attempt(
        _chapter: Chapter,
        _stage: Stage,
        **_kwargs: object,
    ) -> Attempt:
        agent = AgentResult(
            succeeded=False,
            exit_code=1,
            changed=False,
            placeholders=0,
            usage=TokenUsage(),
            error="required MCP servers failed to initialize",
            infrastructure_failed=True,
        )
        await state.finish_run(
            run,
            status=TaskStatus.FAILED,
            failure_kind="infrastructure",
            error=agent.error,
        )
        return Attempt(agent, ValidationResult(False, 1, "not run"), run)

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    await orchestrator._run_upstream_implementation("case-a")

    case = state.steward_cases["case-a"]
    assert case["status"] == "failed"
    assert case["decision"]["disposition"] == "failed"
    assert case["decision"]["summary"] == "required MCP servers failed to initialize"
    assert case["implementation_run_ids"] == [run.id]
    assert state.upstream_requests[request_id]["status"] == "failed"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_global_steward_cases_deduplicate_requests_and_include_consumers(
    tmp_path: Path,
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    request_ids = []
    for declaration in ("Book.first", "Book.second"):
        request_id, _ = await state.enqueue_upstream_request(
            {
                "consumer_path": "lean/Book/Chapter02.lean",
                "blocked_declaration": declaration,
                "residual_goal": "True",
                "needed_result": "one shared bridge",
                "owner_paths": ["lean/Book/Chapter01.lean"],
            },
            consumer_chapter_id=consumer.id,
            owner_chapter_id=owner.id,
        )
        request_ids.append(request_id)

    await state.replace_steward_cases(
        [
            {
                "case_id": "shared-bridge",
                "status": "ready",
                "title": "Shared bridge",
                "disposition": "implement",
                "needed_result": "one shared bridge",
                "request_ids": request_ids,
                "context_work_unit_ids": [owner.id, consumer.id],
                "acceptance_tests": ["both consumers elaborate"],
                "rationale": "The observations request the same mathematical interface.",
            }
        ]
    )

    case = state.steward_cases["shared-bridge"]
    assert case["request_ids"] == request_ids
    assert all(
        state.upstream_requests[request_id]["steward_case_id"] == "shared-bridge"
        for request_id in request_ids
    )
    assert all(
        state.upstream_requests[request_id]["status"] == "evaluating" for request_id in request_ids
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_steward_rewrite_preserves_active_unchanged_case(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    original = {
        "case_id": "case-a",
        "status": "ready",
        "disposition": "implement",
        "request_ids": [],
        "context_work_unit_ids": [chapter.id],
        "title": "Original title",
    }
    await state.replace_steward_cases([original])
    await state.update_steward_case_generation(
        "case-a", 1, status="implementing", active_implementation_generation=1
    )

    await state.replace_steward_cases([{**original, "title": "Improved title"}])

    case = state.steward_cases["case-a"]
    assert case["generation"] == 1
    assert case["status"] == "implementing"
    assert case["active_implementation_generation"] == 1
    assert case["title"] == "Improved title"


@pytest.mark.asyncio
async def test_steward_scope_change_advances_implementation_generation(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [first.id],
            }
        ]
    )
    await state.update_steward_case_generation(
        "case-a", 1, status="needs_scope", implementation_run_ids=["old-run"]
    )

    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [first.id, second.id],
            }
        ]
    )

    case = state.steward_cases["case-a"]
    assert case["generation"] == 2
    assert case["status"] == "ready"
    assert case["implementation_run_ids"] == ["old-run"]
    assert "active_implementation_generation" not in case


@pytest.mark.asyncio
async def test_stale_steward_generation_cannot_claim_or_update_case(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [chapter.id],
            }
        ]
    )
    state.steward_cases["case-a"]["generation"] = 2

    updated = await state.update_steward_case_generation("case-a", 1, status="verified")

    assert not updated
    assert state.steward_cases["case-a"]["status"] == "ready"


@pytest.mark.asyncio
async def test_scope_revision_waits_for_prior_case_task_to_finish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [first.id],
            }
        ]
    )
    started = [asyncio.Event(), asyncio.Event()]
    releases = [asyncio.Event(), asyncio.Event()]
    launches: list[int] = []

    async def implement(_case_id: str) -> None:
        launch = len(launches)
        launches.append(state.steward_cases["case-a"]["generation"])
        started[launch].set()
        await releases[launch].wait()

    monkeypatch.setattr(orchestrator, "_run_upstream_implementation", implement)
    await orchestrator._schedule_upstream_coordination()
    await asyncio.wait_for(started[0].wait(), timeout=2)

    await state.replace_steward_cases(
        [
            {
                "case_id": "case-a",
                "status": "ready",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [first.id, second.id],
            }
        ]
    )
    await orchestrator._schedule_upstream_coordination()
    await asyncio.sleep(0)
    assert launches == [1]

    releases[0].set()
    await asyncio.wait_for(orchestrator._upstream_case_tasks["case-a"], timeout=2)
    await orchestrator._schedule_upstream_coordination()
    await asyncio.wait_for(started[1].wait(), timeout=2)
    assert launches == [1, 2]
    releases[1].set()
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_implementation_excludes_simultaneous_chapter_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    await state.replace_steward_cases(
        [
            {
                "case_id": "shared-chapter-repair",
                "status": "ready",
                "title": "Shared chapter repair",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [chapter.id],
            }
        ]
    )
    implementation_started = asyncio.Event()
    release_implementation = asyncio.Event()
    review_started = asyncio.Event()

    async def implement(case_id: str) -> None:
        implementation_started.set()
        await release_implementation.wait()
        await state.update_steward_case(case_id, status="verified")

    async def review(
        current: Chapter,
        _rounds_used: dict[str, int],
        **_kwargs: object,
    ) -> StageOutcome:
        assert current.id == chapter.id
        review_started.set()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_run_upstream_implementation", implement)
    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)

    review_tree = asyncio.create_task(orchestrator._review_tree())
    await asyncio.wait_for(implementation_started.wait(), timeout=2)
    await asyncio.sleep(0)
    task = state.task(chapter.id, Stage.REVIEW)
    assert task.status == TaskStatus.PENDING
    assert task.phase == TaskPhase.IDLE
    assert not review_started.is_set()

    release_implementation.set()
    await asyncio.wait_for(review_started.wait(), timeout=2)
    assert await asyncio.wait_for(review_tree, timeout=2)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_active_chapter_work_excludes_upstream_implementation(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.replace_steward_cases(
        [
            {
                "case_id": "shared-chapter-repair",
                "status": "ready",
                "title": "Shared chapter repair",
                "disposition": "implement",
                "request_ids": [],
                "context_work_unit_ids": [chapter.id],
            }
        ]
    )

    chapter_lock = orchestrator._chapter_agent_locks[chapter.id]
    await chapter_lock.acquire()
    try:
        await orchestrator._schedule_upstream_coordination()
    finally:
        chapter_lock.release()

    assert not orchestrator._upstream_case_tasks
    assert state.steward_cases["shared-chapter-repair"]["status"] == "ready"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_legacy_upstream_review_route_is_removed_during_normalization(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    consumer = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "consumer_path": "lean/Book/Chapter01.lean",
            "blocked_declaration": "Book.target",
            "residual_goal": "True",
            "needed_result": "a bridge",
            "owner_paths": [],
        },
        consumer_chapter_id=consumer.id,
        owner_chapter_id=consumer.id,
    )
    state.proof_review_requests[request_id] = {
        "feedback": {consumer.id: "legacy upstream review"},
        "origin_run_id": request_id,
        "kind": "upstream_request",
    }
    state.upstream_requests[request_id]["status"] = "needs_human"
    state.steward_cases["legacy-case"] = {
        "id": "legacy-case",
        "status": "needs_human",
        "disposition": "needs_human",
    }

    state._normalize_upstream_request_state()

    assert request_id not in state.proof_review_requests
    assert state.upstream_requests[request_id]["status"] == "failed"
    assert state.steward_cases["legacy-case"]["status"] == "failed"
    assert state.steward_cases["legacy-case"]["disposition"] == "failed"

    with pytest.raises(ValueError, match="no longer supported"):
        await state.enqueue_proof_review_request(
            {consumer.id: "do not recreate the legacy route"},
            origin_run_id=request_id,
            kind="upstream_request",
        )


@pytest.mark.asyncio
async def test_external_candidate_still_requires_steward_disposition(tmp_path: Path) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    attempt = failed_attempt(
        "the result may belong to an unavailable dependency",
        path="lean/Book/Chapter01.lean",
        declaration="Book.target",
    ) | {"disposition": "missing_capability"}
    candidate = {
        "capability_key": "external:book.target",
        "blocked_declaration": "Book.target",
        "consumer_path": "lean/Book/Chapter01.lean",
        "residual_goal": attempt["remaining_goal"],
        "needed_result": "A theorem currently believed to be external",
        "candidate_signature": "",
        "owner_kind": "external",
        "owner_chapter_id": chapter.id,
        "owner_paths": ["lean/Book/Chapter01.lean"],
        "attempted_alternatives": attempt["attempts"],
        "acceptance_tests": [chapter.build_command],
    }
    blockers = await orchestrator.state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-run",
        failed_attempts=(attempt,),
        capability_candidates=(candidate,),
    )

    request_ids = await orchestrator._request_upstream_for_blockers(chapter, blockers)

    assert len(request_ids) == 1
    assert orchestrator.state.upstream_requests[request_ids[0]]["owner_chapter_id"] == chapter.id
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_structural_blocker_consumer_path_must_belong_to_work_unit(
    tmp_path: Path,
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    attempt = failed_attempt(
        "the report named a path outside the assigned source scope",
        path="pyproject.toml",
        declaration="Book.target",
    ) | {"disposition": "missing_capability"}
    candidate = {
        "capability_key": "invalid.path",
        "blocked_declaration": "Book.target",
        "consumer_path": "pyproject.toml",
        "residual_goal": attempt["remaining_goal"],
        "needed_result": "Do not reserve this path",
        "candidate_signature": "",
        "owner_kind": "consumer",
        "owner_chapter_id": chapter.id,
        "owner_paths": ["pyproject.toml"],
        "attempted_alternatives": attempt["attempts"],
        "acceptance_tests": [chapter.build_command],
    }
    blockers = await orchestrator.state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-run",
        failed_attempts=(attempt,),
        capability_candidates=(candidate,),
    )

    request_ids = await orchestrator._request_upstream_for_blockers(chapter, blockers)

    assert request_ids == ()
    assert orchestrator.state.package_state.packages == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "review_changed", "expected"),
    [
        ("repair_and_retry", True, ProofBlockerStatus.OPEN),
        ("retry_with_route", False, ProofBlockerStatus.OPEN),
        ("request_upstream", False, ProofBlockerStatus.UPSTREAM_REQUESTED),
    ],
)
async def test_completed_review_routes_blocker_by_structured_action(
    tmp_path: Path,
    action: str,
    review_changed: bool,
    expected: ProofBlockerStatus,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    blockers = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-run",
        failed_attempts=[
            failed_attempt("the statement may be too strong") | {"disposition": "statement_review"}
        ],
    )
    blocker_id = str(blockers[0]["id"])
    await state.set_proof_blocker_status((blocker_id,), ProofBlockerStatus.REVIEW_REQUESTED)
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`"},
        origin_run_id="proof-run",
        blocker_ids=(blocker_id,),
    )
    run = await state.start_run(chapter.id, Stage.REVIEW)
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        changed=review_changed,
        report={
            "complete": True,
            "finding_assessments": [
                finding_resolution(
                    f"{request_id}:1",
                    action=action,
                    retry_contract=(
                        executable_retry_contract() if action == "retry_with_route" else None
                    ),
                )
            ],
        },
    )

    assert await orchestrator._complete_review(chapter, "reviewed", proof_request_ids=(request_id,))
    assert state.proof_blockers[blocker_id]["status"] == expected.value
    if expected is ProofBlockerStatus.OPEN:
        assert state.proof_blockers[blocker_id]["retry_sighting_baseline"] == 1
    elif expected is ProofBlockerStatus.BLOCKED:
        assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.BLOCKED
    else:
        assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.PENDING
    assert state.proof_blockers[blocker_id]["review_exchange_count"] == 1
    assert request_id not in state.proof_review_requests
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_review_request_is_auxiliary_to_green_review(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "canonical review")
    blockers = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-run",
        failed_attempts=[failed_attempt("proof strategy stalled")],
    )
    blocker_id = str(blockers[0]["id"])
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`"},
        origin_run_id="proof-run",
        blocker_ids=(blocker_id,),
    )
    await state.set_proof_blocker_status((blocker_id,), ProofBlockerStatus.REVIEW_REQUESTED)
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(
                changed=False,
                finding_assessments=[
                    finding_resolution(
                        f"{request_id}:1",
                        action="retry_with_route",
                        explanation="Try induction on the finite presentation next.",
                        retry_contract=executable_retry_contract(),
                    )
                ],
            )
        ],
    )

    outcome = await orchestrator._review_chapter_to_clean(
        chapter,
        {chapter.id: 0},
        rerun=True,
        feedback=feedback,
        role=PROOF_REVIEW_ROLE,
        proof_request_ids=request_ids,
    )

    assert outcome.succeeded
    review = state.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.SUCCEEDED
    assert review.detail == "canonical review"
    assert review.runs[-1].auxiliary
    assert review.runs[-1].role == PROOF_REVIEW_ROLE
    assert request_id not in state.proof_review_requests
    blocker = state.proof_blockers[blocker_id]
    assert blocker["status"] == ProofBlockerStatus.OPEN.value
    assert blocker["review_exchange_count"] == 1
    assert blocker["review_responses"] == ["Try induction on the finite presentation next."]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_green_review_does_not_satisfy_pending_proof_review_request(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized")
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "review this failed proof"},
        origin_run_id="proof-run",
    )

    proof = state.task(chapter.id, Stage.PROVE)
    readiness = state.readiness(proof)
    assert not readiness.ready
    assert [requirement.request_id for requirement in readiness.waiting] == [request_id]

    await state.finish_proof_review_requests(chapter.id, (request_id,))
    assert state.readiness(proof).ready
    await state.close()


@pytest.mark.asyncio
async def test_build_warning_review_request_does_not_block_proof_readiness(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized")
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    warning_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "clean up this warning"},
        origin_run_id="warning-build",
        kind=BUILD_WARNING_REVIEW_KIND,
    )

    proof = state.task(chapter.id, Stage.PROVE)
    assert state.readiness(proof).ready
    assert warning_id in state.proof_review_requests
    await state.close()


@pytest.mark.asyncio
async def test_noop_proof_review_routes_once_to_upstream_steward(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "canonical review")
    blockers = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-run-0",
        failed_attempts=[failed_attempt("statement/interface strategy stalled")],
    )
    blocker_id = str(blockers[0]["id"])
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`"},
        origin_run_id="proof-run-1",
        blocker_ids=(blocker_id,),
    )
    await state.set_proof_blocker_status((blocker_id,), ProofBlockerStatus.REVIEW_REQUESTED)
    run = await state.start_auxiliary_run(
        chapter.id,
        Stage.REVIEW,
        role=PROOF_REVIEW_ROLE,
        request_ids=(request_id,),
    )
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={
            "complete": True,
            "finding_assessments": [
                finding_resolution(
                    f"{request_id}:1",
                    action="request_upstream",
                    diagnosis="genuine_blocker",
                )
            ],
        },
    )
    await orchestrator._apply_proof_review_outcomes(chapter, (request_id,))
    await state.finish_proof_review_requests(chapter.id, (request_id,))

    blocker = state.proof_blockers[blocker_id]
    assert blocker["status"] == ProofBlockerStatus.UPSTREAM_REQUESTED.value
    assert blocker["review_exchange_count"] == 1
    assert len(blocker["review_responses"]) == 1
    proof = state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.PENDING
    assert len(state.upstream_requests) == 1
    assert state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_review_can_open_upstream_request(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    root = tmp_path / "lean" / "Book"
    root.mkdir(parents=True)
    (root / "Chapter01.lean").write_text("theorem support : True := by trivial\n")
    (root / "Chapter02.lean").write_text("theorem target : True := by sorry\n")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    blockers = await state.record_proof_blockers(
        consumer.id,
        origin_run_id="proof-run",
        failed_attempts=[
            failed_attempt(
                "a reusable transport result is missing",
                path="lean/Book/Chapter02.lean",
            )
        ],
    )
    blocker_id = str(blockers[0]["id"])
    request_id, _ = await state.enqueue_proof_review_request(
        {consumer.id: "Failed proof `Book.target` in `lean/Book/Chapter02.lean`"},
        origin_run_id="proof-run",
        blocker_ids=(blocker_id,),
    )
    run = await state.start_auxiliary_run(
        consumer.id,
        Stage.REVIEW,
        role=PROOF_REVIEW_ROLE,
        request_ids=(request_id,),
    )
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={
            "complete": True,
            "finding_assessments": [
                finding_resolution(
                    f"{request_id}:1",
                    diagnosis="missing_capability",
                    action="request_upstream",
                    capability={
                        "capability_key": "book.transport",
                        "blocked_declaration": "Book.target",
                        "consumer_path": "lean/Book/Chapter02.lean",
                        "residual_goal": "⊢ True",
                        "needed_result": "a reusable transport theorem",
                        "candidate_signature": "theorem transport : True",
                        "owner_kind": "chapter",
                        "owner_chapter_id": owner.id,
                        "owner_paths": ["lean/Book/Chapter01.lean"],
                        "attempted_alternatives": ["simp", "exact support"],
                        "acceptance_tests": ["Book.target accepts `exact Book.transport`"],
                    },
                )
            ],
        },
    )

    assert await orchestrator._complete_review(
        consumer, "reviewed", proof_request_ids=(request_id,)
    )
    blocker = state.proof_blockers[blocker_id]
    assert blocker["status"] == ProofBlockerStatus.UPSTREAM_REQUESTED
    upstream = state.upstream_requests[blocker["upstream_request_id"]]
    assert upstream["capability_key"] == "book.transport"
    assert upstream["owner_chapter_id"] == owner.id
    assert upstream["consumer_chapter_id"] == consumer.id
    assert state.package_state.packages == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_review_dependency_wait_does_not_consume_exchange(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    dependency, consumer = config.chapters
    root = tmp_path / "lean" / "Book"
    root.mkdir(parents=True)
    (root / "Chapter02.lean").write_text("theorem target : True := by sorry\n")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    blocker = (
        await state.record_proof_blockers(
            consumer.id,
            origin_run_id="proof-run",
            failed_attempts=[failed_attempt("an unrelated prerequisite failed to build")],
        )
    )[0]
    blocker_id = str(blocker["id"])
    request_id, _ = await state.enqueue_proof_review_request(
        {consumer.id: "Failed proof `Book.target` in `lean/Book/Chapter02.lean`"},
        origin_run_id="proof-run",
        blocker_ids=(blocker_id,),
    )
    run = await state.start_auxiliary_run(
        consumer.id, Stage.REVIEW, role=PROOF_REVIEW_ROLE, request_ids=(request_id,)
    )
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={
            "complete": True,
            "finding_assessments": [
                finding_resolution(
                    f"{request_id}:1",
                    diagnosis="validation_noise",
                    action="wait_for_dependency",
                    dependency_ids=[dependency.id],
                )
            ],
        },
    )

    await orchestrator._complete_review(consumer, "reviewed", proof_request_ids=(request_id,))
    blocker = state.proof_blockers[blocker_id]
    assert blocker["status"] == ProofBlockerStatus.WAITING_DEPENDENCY
    assert blocker.get("review_exchange_count", 0) == 0
    proof = state.task(consumer.id, Stage.PROVE)
    assert proof.status == TaskStatus.PENDING
    assert any(
        requirement.owner_task_key == state.key(dependency.id, Stage.PROVE)
        for requirement in proof.waiting_on
    )
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_review_drops_confirmed_stale_target(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text("theorem replacement : True := by trivial\n")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    blocker = (
        await state.record_proof_blockers(
            chapter.id,
            origin_run_id="proof-run",
            failed_attempts=[failed_attempt("captured target was removed")],
        )
    )[0]
    blocker_id = str(blocker["id"])
    request_id, _ = await state.enqueue_proof_review_request(
        {chapter.id: "Failed proof `Book.target` in `lean/Book/Chapter01.lean`"},
        origin_run_id="proof-run",
        blocker_ids=(blocker_id,),
    )
    run = await state.start_auxiliary_run(
        chapter.id, Stage.REVIEW, role=PROOF_REVIEW_ROLE, request_ids=(request_id,)
    )
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={
            "complete": True,
            "finding_assessments": [
                finding_resolution(
                    f"{request_id}:1",
                    diagnosis="stale_target",
                    action="drop_stale_target",
                )
            ],
        },
    )

    await orchestrator._complete_review(chapter, "reviewed", proof_request_ids=(request_id,))
    assert state.proof_blockers[blocker_id]["status"] == ProofBlockerStatus.RESOLVED
    assert state.routing_metrics["stale_target_dropped"] == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_exhausted_retry_contract_requires_package_without_second_review(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    blocker = (
        await state.record_proof_blockers(
            chapter.id,
            origin_run_id="initial-proof",
            failed_attempts=[failed_attempt("the original strategy failed")],
        )
    )[0]
    blocker["retry_cause_digest"] = "checked-route-v1"
    blocker["status"] = ProofBlockerStatus.OPEN.value

    retained = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="contract-retry",
        failed_attempts=[failed_attempt("the original strategy failed")],
    )

    assert retained[0]["status"] == ProofBlockerStatus.OPEN
    assert retained[0]["last_attempted_retry_cause_digest"] == "checked-route-v1"
    assert state.routing_metrics["unchanged_retry_suppressed"] == 1
    await state.close()


@pytest.mark.asyncio
async def test_pending_review_is_not_recovered_from_historical_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(load_config(write_project(tmp_path, chapters="chapters = [1]")))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem finished : True := by trivial\n", encoding="utf-8")
    state = StateStore(config)
    await state.load_or_create()
    review = await state.start_run(chapter.id, Stage.REVIEW)
    await state.finish_run(
        review,
        status=TaskStatus.SUCCEEDED,
        changed=False,
        report={"complete": True},
        validation={"succeeded": True, "output": "ok"},
    )
    proof = await state.start_run(chapter.id, Stage.PROVE)
    await state.finish_run(
        proof,
        status=TaskStatus.SUCCEEDED,
        changed=True,
        placeholders=0,
        report={"complete": True, "failed_attempts": []},
        validation={"succeeded": True, "output": "ok"},
    )
    await state.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.PENDING,
        "review checkpoint invalidated by source or dependency changes",
    )
    await state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.PENDING,
        "waiting for invalidated statement review",
    )
    state.task(chapter.id, Stage.PROVE).source_digest = None
    await state.save()
    await state.close()

    builds: list[str] = []

    async def validation(
        _config: object,
        built: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(built.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    async def review_again(*_args: object, **_kwargs: object) -> StageOutcome:
        return StageOutcome(ExecutionDisposition.SUCCEEDED, changed=False)

    async def forbidden_agent(*_args: object, **_kwargs: object) -> AgentResult:
        raise AssertionError("current placeholder-free proof must not rerun an agent")

    monkeypatch.setattr(restarted, "_review_once", review_again)
    monkeypatch.setattr(restarted.executor, "run", forbidden_agent)

    assert await restarted._review_tree(prove=True)
    assert builds == [chapter.id]
    assert restarted.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    proved = restarted.state.task(chapter.id, Stage.PROVE)
    assert proved.status == TaskStatus.SUCCEEDED
    assert proved.source_digest == scope_digest(config.settings.repo, chapter)
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_statement_finding_invalidates_only_its_review(tmp_path: Path) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    for chapter in config.chapters:
        await orchestrator.state.set_task(
            chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed"
        )
        await orchestrator.state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.SUCCEEDED,
            "proved",
            source_digest=f"proof-source-{chapter.number}",
        )

    invalidated = await orchestrator._invalidate_reviews(
        {first.id}, detail="upstream statement changed"
    )

    assert invalidated == {first.id}
    assert orchestrator.state.task(first.id, Stage.REVIEW).status == TaskStatus.PENDING
    assert orchestrator.state.task(second.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    for chapter in config.chapters:
        proof = orchestrator.state.task(chapter.id, Stage.PROVE)
        assert proof.status == TaskStatus.SUCCEEDED
        assert proof.source_digest == f"proof-source-{chapter.number}"
    await orchestrator.shutdown()


def interface_record(digest: str) -> dict[str, object]:
    return {
        "artifact_digest": f"artifact-{digest}",
        "interface_digest": digest,
        "fingerprint_schema": "olean-proof-erased-v1",
        "lean_version": "4.33.0:test",
        "modules": [
            {
                "module": "Book.Chapter",
                "source": "lean/Book/Chapter.lean",
                "artifact": "lean/.lake/build/lib/lean/Book/Chapter.olean",
                "imports": [],
                "artifact_digest": f"artifact-{digest}",
                "interface_digest": digest,
                "declaration_count": 1,
            }
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_compiled_interface_controls_downstream_build_invalidation(
    tmp_path: Path,
    changed: bool,
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(project)
    first, imported_successor, textbook_only = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def value{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(
        orchestrator,
        {
            imported_successor.id: (first.id,),
            textbook_only.id: (first.id,),
        },
    )
    old = interface_record("old-interface")
    orchestrator.state.formalize_graph.update(
        {
            "interfaces": {
                chapter.id: interface_record(f"interface-{chapter.number}")
                for chapter in config.chapters
            }
            | {first.id: old},
            "interface_imports": {
                first.id: [],
                imported_successor.id: [first.id],
                textbook_only.id: [],
            },
        }
    )
    await orchestrator.state.set_task(
        imported_successor.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "proved",
        source_digest="proved-successor",
    )
    await orchestrator.state.save()

    first_source = source_root / "Chapter01.lean"
    first_source.write_text("def value1 := 10\n", encoding="utf-8")
    initially_invalidated = await orchestrator._invalidate_build_records((first.id,))

    assert initially_invalidated == {first.id}
    assert set(orchestrator.state.formalize_graph["clean"]) == {
        imported_successor.id,
        textbook_only.id,
    }
    assert orchestrator.state.formalize_graph.get("interface_stale", []) == []
    pending_successor_view = orchestrator.state.snapshot()["tasks"][
        orchestrator.state.key(imported_successor.id, Stage.PROVE)
    ]
    assert pending_successor_view["interface_current"] is True
    assert pending_successor_view["dependencies_current"] is True
    assert pending_successor_view["head_build_status"] == "clean"
    assert pending_successor_view["fully_certified"] is True

    graph = orchestrator._observed_work_unit_graph()
    new = interface_record("new-interface" if changed else "old-interface")
    snapshot = scheduler_module.ValidatedBuildSnapshot(
        graph=graph,
        source_digests={
            first.id: scope_digest(config.settings.repo, first),
        },
        fingerprint=new,
        import_dependencies=(),
    )

    assert await orchestrator._publish_validated_build(first, snapshot)
    clean = set(orchestrator.state.formalize_graph["clean"])
    if changed:
        assert clean == {first.id, textbook_only.id}
        assert orchestrator.state.formalize_graph["dirty"] == [imported_successor.id]
        assert orchestrator.state.formalize_graph["interface_stale"] == [imported_successor.id]
    else:
        assert clean == {first.id, imported_successor.id, textbook_only.id}
        assert orchestrator.state.formalize_graph["dirty"] == []
        assert orchestrator.state.formalize_graph["interface_stale"] == []
    proof = orchestrator.state.task(imported_successor.id, Stage.PROVE)
    assert proof.status == TaskStatus.SUCCEEDED
    assert proof.source_digest == "proved-successor"
    proof_view = orchestrator.state.snapshot()["tasks"][
        orchestrator.state.key(imported_successor.id, Stage.PROVE)
    ]
    assert proof_view["proof_complete"] is True
    assert proof_view["interface_current"] is (not changed)
    assert proof_view["dependencies_current"] is (not changed)
    assert proof_view["head_build_status"] == ("pending" if changed else "clean")
    assert proof_view["fully_certified"] is (not changed)
    with sqlite3.connect(orchestrator.state.database_path) as connection:
        events = connection.execute(
            """
            SELECT source_file, old_digest, new_digest, invalidated_work_unit_ids
            FROM interface_invalidation_events
            """
        ).fetchall()
    if changed:
        assert events == [
            (
                "lean/Book/Chapter.lean",
                "old-interface",
                "new-interface",
                json.dumps([imported_successor.id], separators=(",", ":")).encode(),
            )
        ]
    else:
        assert events == []
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_only_file_signature_change_does_not_invalidate_descendants(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator, {second.id: (first.id,)})
    old = interface_record("same-file-interface")
    old["interface_digest"] = "old-work-unit-aggregate"
    orchestrator.state.formalize_graph["interfaces"] = {first.id: old}
    await orchestrator._invalidate_build_records((first.id,))

    new = interface_record("same-file-interface")
    new["interface_digest"] = "new-work-unit-aggregate"
    graph = orchestrator._observed_work_unit_graph()
    snapshot = scheduler_module.ValidatedBuildSnapshot(
        graph=graph,
        source_digests={first.id: scope_digest(config.settings.repo, first)},
        fingerprint=new,
        import_dependencies=(),
    )

    assert await orchestrator._publish_validated_build(first, snapshot)
    assert set(orchestrator.state.formalize_graph["clean"]) == {first.id, second.id}
    assert orchestrator.state.formalize_graph["interface_stale"] == []
    with sqlite3.connect(orchestrator.state.database_path) as connection:
        count = connection.execute("SELECT count(*) FROM interface_invalidation_events").fetchone()[
            0
        ]
    assert count == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_first_file_signature_establishes_baseline_without_invalidation(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator, {second.id: (first.id,)})

    assert await orchestrator._invalidate_build_records((first.id,)) == {first.id}
    assert second.id in orchestrator.state.formalize_graph["clean"]

    graph = orchestrator._observed_work_unit_graph()
    snapshot = scheduler_module.ValidatedBuildSnapshot(
        graph=graph,
        source_digests={first.id: scope_digest(config.settings.repo, first)},
        fingerprint=interface_record("initial-file-interface"),
        import_dependencies=(),
    )
    assert await orchestrator._publish_validated_build(first, snapshot)
    assert set(orchestrator.state.formalize_graph["clean"]) == {first.id, second.id}
    assert orchestrator.state.formalize_graph["interface_stale"] == []
    assert orchestrator.state.formalize_graph["fingerprint_metrics"] == {
        "interface_baselines_initialized": 1
    }
    with sqlite3.connect(orchestrator.state.database_path) as connection:
        count = connection.execute("SELECT count(*) FROM interface_invalidation_events").fetchone()[
            0
        ]
    assert count == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_new_file_signature_extends_baseline_without_invalidation(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator, {second.id: (first.id,)})
    old = interface_record("existing-interface")
    orchestrator.state.formalize_graph["interfaces"] = {first.id: old}
    await orchestrator._invalidate_build_records((first.id,))

    old_modules = old["modules"]
    assert isinstance(old_modules, list)
    assert isinstance(old_modules[0], dict)
    added_module = dict(old_modules[0])
    added_module.update(
        {
            "module": "Book.Chapter.Extra",
            "source": "lean/Book/Chapter/Extra.lean",
            "artifact": "lean/.lake/build/lib/lean/Book/Chapter/Extra.olean",
            "artifact_digest": "artifact-extra",
            "interface_digest": "extra-interface",
        }
    )
    new = dict(old)
    new["modules"] = [*old_modules, added_module]
    graph = orchestrator._observed_work_unit_graph()
    snapshot = scheduler_module.ValidatedBuildSnapshot(
        graph=graph,
        source_digests={first.id: scope_digest(config.settings.repo, first)},
        fingerprint=new,
        import_dependencies=(),
    )

    assert await orchestrator._publish_validated_build(first, snapshot)
    assert set(orchestrator.state.formalize_graph["clean"]) == {first.id, second.id}
    assert orchestrator.state.formalize_graph["interface_stale"] == []
    assert orchestrator.state.formalize_graph["fingerprint_metrics"] == {
        "interface_baselines_initialized": 1,
        "interface_preserving_edits": 1,
    }
    with sqlite3.connect(orchestrator.state.database_path) as connection:
        count = connection.execute("SELECT count(*) FROM interface_invalidation_events").fetchone()[
            0
        ]
    assert count == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_build_invalidation_does_not_rescan_untouched_clean_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    first, second = config.chapters

    def unexpected_digest(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("known changed owners must be invalidated without a corpus scan")

    monkeypatch.setattr(scheduler_module, "scope_digest", unexpected_digest)

    assert await orchestrator._invalidate_build_records((first.id,)) == {first.id}
    assert set(orchestrator.state.formalize_graph["clean"]) == {second.id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_repeated_build_invalidation_reuses_persisted_graphs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    first, second = config.chapters

    await orchestrator._invalidate_build_records((first.id,))

    def unexpected_graph_build(*_args: object, **_kwargs: object) -> WorkUnitImportGraph:
        raise AssertionError("unchanged compiled imports should reuse their graph")

    def unexpected_snapshot(_graph: WorkUnitImportGraph) -> dict[str, object]:
        raise AssertionError("unchanged graph structure should reuse its snapshot")

    monkeypatch.setattr(scheduler_module, "build_compiled_import_graph", unexpected_graph_build)
    monkeypatch.setattr(WorkUnitImportGraph, "snapshot", unexpected_snapshot)

    assert await orchestrator._invalidate_build_records((second.id,)) == {second.id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_build_graph_publications_coalesce_normalized_database_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    first, second = config.chapters
    calls = 0
    original = state_module.graph_snapshot

    def counted_snapshot(section: str, value: Any):
        nonlocal calls
        calls += section == "formalize_graph"
        return original(section, value)

    monkeypatch.setattr(state_module, "graph_snapshot", counted_snapshot)

    await orchestrator._invalidate_build_records((first.id,))
    await orchestrator._invalidate_build_records((second.id,))
    assert calls == 0

    await orchestrator.state.flush()
    assert calls == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_build_publication_hashes_only_its_dependency_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator)
    first, second = config.chapters
    graph = orchestrator._observed_work_unit_graph()
    expected = scope_digest(config.settings.repo, first)
    digested: list[str] = []

    def counted_digest(root: Path, chapter: Chapter) -> str:
        digested.append(chapter.id)
        return scope_digest(root, chapter)

    monkeypatch.setattr(scheduler_module, "scope_digest", counted_digest)
    snapshot = scheduler_module.ValidatedBuildSnapshot(
        graph=graph,
        source_digests={first.id: expected},
    )

    assert await orchestrator._publish_validated_build(first, snapshot)
    assert digested == [first.id]
    assert second.id in orchestrator.state.formalize_graph["clean"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_build_snapshot_hashes_only_targets_without_modified_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    for chapter in config.chapters:
        source = tmp_path / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"def value{chapter.number} := {chapter.number}\n", encoding="utf-8")
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_clean_formalization(orchestrator, {second.id: (first.id,)})
    orchestrator.git.enabled = True
    digested: list[str] = []
    original_digest = scheduler_module.scope_digest

    def counted_digest(root: Path, chapter: Chapter) -> str:
        digested.append(chapter.id)
        return original_digest(root, chapter)

    async def no_dirty_paths() -> tuple[str, ...]:
        return ()

    async def successful_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "scope_digest", counted_digest)
    monkeypatch.setattr(orchestrator.git, "working_tree_paths", no_dirty_paths)
    monkeypatch.setattr(scheduler_module, "validate", successful_validation)
    snapshots: dict[str, scheduler_module.ValidatedBuildSnapshot] = {}

    result = await orchestrator._build_chapters(
        (second,), publish_if_clean=False, snapshots=snapshots
    )

    assert result[second.id].succeeded
    assert digested == [second.id]
    assert set(snapshots[second.id].source_digests) == {first.id, second.id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_completion_cannot_resurrect_an_invalidated_generation(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    started_generation = orchestrator._review_invalidation_generation(chapter.id)

    await orchestrator._invalidate_reviews(
        {chapter.id}, detail="statement changed while review was running"
    )

    assert not await orchestrator._complete_review(
        chapter,
        "obsolete review finished",
        expected_generation=started_generation,
    )
    review = orchestrator.state.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.PENDING
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_background_build_recertifies_successful_stale_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "proved",
        source_digest="proved-source",
    )

    async def refresh(_chapter: Chapter) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(orchestrator, "_refresh_stale_proof_build", refresh)

    assert await orchestrator._rebuild_dirty_chapter(chapter)
    proof = state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.SUCCEEDED
    assert proof.detail == "proved"
    assert proof.source_digest == scope_digest(config.settings.repo, chapter)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_background_build_reopens_successful_proof_only_after_build_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "proved",
        source_digest="proved-source",
    )

    async def refresh(_chapter: Chapter) -> ValidationResult:
        return ValidationResult(False, 1, "build failed")

    monkeypatch.setattr(orchestrator, "_refresh_stale_proof_build", refresh)

    assert not await orchestrator._rebuild_dirty_chapter(chapter)
    proof = state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.PENDING
    assert proof.detail == "stale proof no longer builds"
    assert proof.source_digest is None
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_proof_starts_before_downstream_review_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_formalized(orchestrator, {second.id: (first.id,)})
    upstream_proof_started = asyncio.Event()
    finish_upstream_proof = asyncio.Event()
    events: list[str] = []

    async def review(
        chapter: Chapter,
        _rounds_used: dict[str, int],
        *,
        rerun: bool = False,
    ) -> StageOutcome:
        assert not rerun
        if chapter.id == second.id:
            assert orchestrator.state.task(second.id, Stage.REVIEW).status == TaskStatus.RUNNING
            await upstream_proof_started.wait()
            events.append(f"review:{chapter.id}")
            finish_upstream_proof.set()
        else:
            events.append(f"review:{chapter.id}")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> StageOutcome:
        assert defer_review
        events.append(f"prove:{chapter.id}")
        if chapter.id == first.id:
            upstream_proof_started.set()
            await finish_upstream_proof.wait()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert await orchestrator._review_tree(prove=True)
    assert events.index(f"prove:{first.id}") < events.index(f"review:{second.id}")
    await orchestrator.shutdown()
    assert events.index(f"review:{first.id}") < events.index(f"review:{second.id}")


@pytest.mark.asyncio
async def test_invalidated_review_allows_descendant_proofs_to_run_optimistically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    book = tmp_path / "books" / "book.md"
    book.write_text(
        book.read_text(encoding="utf-8") + "\n## 3. Third chapter\n",
        encoding="utf-8",
    )
    config = with_example_modules(load_config(project))
    first, second, third = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    (source_root / "Chapter03.lean").write_text(
        "import LastLib.Book.Chapter02\ndef third := second + 1\n",
        encoding="utf-8",
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    for chapter in config.chapters:
        await orchestrator.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.SUCCEEDED,
            "reviewed",
        )
    await orchestrator._invalidate_reviews(
        {first.id},
        detail="upstream review invalidated",
    )

    upstream_review_started = asyncio.Event()
    release_upstream_review = asyncio.Event()
    proofs_started: list[str] = []
    descendant_proofs_started = asyncio.Event()

    async def review(
        chapter: Chapter,
        _rounds_used: dict[str, int],
        **_kwargs: object,
    ) -> StageOutcome:
        assert chapter.id == first.id
        upstream_review_started.set()
        await release_upstream_review.wait()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> StageOutcome:
        assert defer_review
        proofs_started.append(chapter.id)
        if {second.id, third.id}.issubset(proofs_started):
            descendant_proofs_started.set()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    review_tree = asyncio.create_task(orchestrator._review_tree(prove=True))
    await asyncio.wait_for(upstream_review_started.wait(), timeout=2)
    await asyncio.wait_for(descendant_proofs_started.wait(), timeout=2)
    assert set(proofs_started) == {second.id, third.id}

    release_upstream_review.set()
    assert await asyncio.wait_for(review_tree, timeout=2)
    assert set(proofs_started) == {first.id, second.id, third.id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_dirty_rebuilds_wait_only_for_an_agent_on_the_same_chapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    for chapter in config.chapters:
        await orchestrator.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.SUCCEEDED,
            "reviewed",
        )
    orchestrator.state.task(first.id, Stage.REVIEW).rounds = 1
    await orchestrator.state.set_task(
        first.id,
        Stage.REVIEW,
        TaskStatus.PENDING,
        "re-review requested",
    )
    orchestrator.state.fixup_graph["dirty"] = [first.id, second.id]
    await orchestrator.state.save()

    rereview_started = asyncio.Event()
    release_rereview = asyncio.Event()
    second_rebuilt = asyncio.Event()
    rebuilt: list[str] = []

    async def review(
        chapter: Chapter,
        _rounds_used: dict[str, int],
        **_kwargs: object,
    ) -> StageOutcome:
        assert chapter.id == first.id
        rereview_started.set()
        await release_rereview.wait()
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def rebuild(chapter: Chapter) -> bool:
        rebuilt.append(chapter.id)
        dirty = set(orchestrator.state.fixup_graph.get("dirty", ()))
        dirty.discard(chapter.id)
        orchestrator.state.fixup_graph["dirty"] = sorted(dirty)
        await orchestrator.state.save()
        if chapter.id == second.id:
            second_rebuilt.set()
        return True

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_rebuild_dirty_chapter", rebuild)

    review_tree = asyncio.create_task(orchestrator._review_tree())
    await asyncio.wait_for(rereview_started.wait(), timeout=2)
    await asyncio.wait_for(second_rebuilt.wait(), timeout=2)
    assert rebuilt == [second.id]

    release_rereview.set()
    assert await asyncio.wait_for(review_tree, timeout=2)
    assert rebuilt == [second.id, first.id]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_finding_requeues_only_its_review_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await state.set_task(
        chapter.id,
        Stage.FORMALIZE,
        TaskStatus.SUCCEEDED,
        "formalization complete",
    )
    reviews = 0
    proofs = 0
    review_feedback: list[str] = []
    build_invalidations: list[tuple[str, ...]] = []
    original_invalidate_build_records = orchestrator._invalidate_build_records

    async def invalidate_build_records(chapter_ids: Iterable[str]) -> set[str]:
        targets = tuple(chapter_ids)
        build_invalidations.append(targets)
        return await original_invalidate_build_records(targets)

    async def rebuild_dirty_chapter(rebuilt: Chapter) -> bool:
        dirty = set(state.fixup_graph.get("dirty", ()))
        dirty.discard(rebuilt.id)
        state.fixup_graph["dirty"] = sorted(dirty)
        await state.save()
        return True

    async def review(
        _chapter: Chapter,
        _rounds_used: dict[str, int],
        *,
        rerun: bool = False,
        feedback: str = "",
        role: str = "",
        proof_request_ids: tuple[str, ...] = (),
    ) -> StageOutcome:
        nonlocal reviews
        assert rerun == (reviews > 0)
        if feedback:
            assert role == PROOF_REVIEW_ROLE
        review_feedback.append(feedback)
        assert bool(proof_request_ids) == bool(feedback)
        if proof_request_ids:
            await state.finish_proof_review_requests(chapter.id, proof_request_ids)
        reviews += 1
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def prove(_chapter: Chapter, *, defer_review: bool = False) -> StageOutcome:
        assert defer_review
        nonlocal proofs
        proofs += 1
        if proofs > 1:
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        run = await state.start_run(chapter.id, Stage.PROVE)
        await state.finish_run(
            run,
            status=TaskStatus.FAILED,
            report={
                "issues": ["statement needs a hypothesis"],
                "failed_attempts": [failed_attempt("statement needs a hypothesis")],
            },
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)
    monkeypatch.setattr(orchestrator, "_invalidate_build_records", invalidate_build_records)
    monkeypatch.setattr(orchestrator, "_rebuild_dirty_chapter", rebuild_dirty_chapter)

    assert await orchestrator._review_tree(prove=True)
    assert (reviews, proofs) == (2, 2)
    assert review_feedback[0] == ""
    assert "statement needs a hypothesis" in review_feedback[1]
    assert build_invalidations == []
    assert state.fixup_requests == {}
    assert state.proof_review_requests == {}
    assert state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


def test_failed_attempt_feedback_preserves_proof_evidence(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    feedback = orchestrator._failed_attempt_feedback(
        {"failed_attempts": [failed_attempt("the statement needs another hypothesis")]}
    )

    assert "Book.target" in feedback
    assert "⊢ True" in feedback
    assert "the statement needs another hypothesis" in feedback


@pytest.mark.asyncio
async def test_review_is_one_no_change_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    orchestrator.executor = FakeExecutor(state, [result(changed=False)])

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator.run_stage(Stage.REVIEW)
    task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.rounds == 1
    assert state.total_usage().total_tokens == 15


@pytest.mark.asyncio
async def test_prove_builds_run_after_agents_and_are_serialized_in_main_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    for chapter in config.chapters:
        target = tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"theorem proof{chapter.number} : True := by sorry\n", encoding="utf-8")
    await mark_clean_formalization(orchestrator)
    await state.set_tasks(
        (chapter.id for chapter in config.chapters),
        Stage.REVIEW,
        TaskStatus.SUCCEEDED,
        "reviewed",
    )
    orchestrator.executor = FakeExecutor(
        state,
        [result(changed=True, placeholders=0), result(changed=True, placeholders=0)],
    )
    completed_agents: set[str] = set()
    original_run = orchestrator.executor.run
    active_builds = 0
    maximum_active_builds = 0

    async def tracked_run(
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        agent = await original_run(
            chapter,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        assert workspace_root is not None
        target = workspace_root / chapter.lean_root / f"{chapter.chapter_path}.lean"
        target.write_text(
            target.read_text(encoding="utf-8").replace("by sorry", "by trivial"),
            encoding="utf-8",
        )
        completed_agents.add(chapter.id)
        return agent

    async def tracked_validation(
        _config: object,
        chapter: Chapter,
        *,
        workspace_root: Path | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ValidationResult:
        nonlocal active_builds, maximum_active_builds
        assert on_output is not None
        assert chapter.id in completed_agents
        assert workspace_root == config.settings.repo
        active_builds += 1
        maximum_active_builds = max(maximum_active_builds, active_builds)
        await asyncio.sleep(0)
        active_builds -= 1
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(orchestrator.executor, "run", tracked_run)
    monkeypatch.setattr(scheduler_module, "validate", tracked_validation)

    assert await orchestrator.run_stage(Stage.PROVE)
    assert maximum_active_builds == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_assigns_source_ordered_chunks_of_six_and_persists_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(f"theorem target{i} : True := by sorry" for i in range(1, 10)) + "\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    fake = FakeExecutor(
        state,
        [
            result(changed=True, placeholders=3),
            result(changed=True, placeholders=0),
        ],
    )
    original_run = fake.run

    async def solve_assigned(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        agent = await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        assert workspace_root is not None
        target_path = workspace_root / "lean" / "Book" / "Chapter01.lean"
        text = target_path.read_text(encoding="utf-8")
        for target in run.proof_targets:
            declaration = target["declaration"]
            text = text.replace(
                f"theorem {declaration} : True := by sorry",
                f"theorem {declaration} : True := by trivial",
            )
        target_path.write_text(text, encoding="utf-8")
        return agent

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(fake, "run", solve_assigned)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator.executor = fake

    assert await orchestrator._prove(chapter)
    runs = state.task(chapter.id, Stage.PROVE).runs
    assert [sum(target["placeholder_count"] for target in run.proof_targets) for run in runs] == [
        6,
        3,
    ]
    assert [[target["declaration"] for target in run.proof_targets] for run in runs] == [
        ["target1", "target2", "target3", "target4", "target5", "target6"],
        ["target7", "target8", "target9"],
    ]
    await orchestrator.shutdown()

    recovered = StateStore(config)
    await recovered.load_or_create()
    persisted = recovered.task(chapter.id, Stage.PROVE).runs
    assert [run.proof_targets for run in persisted] == [run.proof_targets for run in runs]
    await recovered.close()


@pytest.mark.asyncio
async def test_proof_task_failure_does_not_fail_or_block_the_review_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")

    async def failed_proof(attempted: Chapter, *, defer_review: bool = False) -> StageOutcome:
        assert attempted.id == chapter.id
        assert defer_review
        await state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.FAILED,
            "proof chunks exhausted retries",
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    monkeypatch.setattr(orchestrator, "_prove", failed_proof)

    assert await orchestrator._review_tree(prove=True)
    assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.FAILED
    assert all(task.status != TaskStatus.BLOCKED for task in state.tasks.values())
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_pending_proof_reopened_during_review_tree_is_scheduled_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_formalized(orchestrator)
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    attempts = 0

    async def prove(attempted: Chapter, *, defer_review: bool = False) -> StageOutcome:
        nonlocal attempts
        assert attempted.id == chapter.id
        assert defer_review
        attempts += 1
        if attempts == 1:
            await state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.FAILED,
                "proof chunks exhausted retries",
            )
            await state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.PENDING,
                "manually set to pending",
            )
            return StageOutcome(ExecutionDisposition.FAILED)
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert await orchestrator._review_tree(prove=True)
    assert attempts == 2
    assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_retries_the_same_chunk_before_advancing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.PROVE: replace(config.stages[Stage.PROVE], max_rounds=2, chunk_size=4),
        },
    )
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(f"theorem target{i} : True := by sorry" for i in range(1, 6)) + "\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    fake = FakeExecutor(
        state,
        [
            result(changed=False, placeholders=5),
            result(changed=True, placeholders=1),
            result(changed=True, placeholders=0),
        ],
    )
    original_run = fake.run
    calls = 0

    async def solve_after_first_attempt(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        nonlocal calls
        calls += 1
        agent = await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        if calls == 1:
            return agent
        assert workspace_root is not None
        target_path = workspace_root / "lean" / "Book" / "Chapter01.lean"
        text = target_path.read_text(encoding="utf-8")
        for target in run.proof_targets:
            declaration = target["declaration"]
            text = text.replace(
                f"theorem {declaration} : True := by sorry",
                f"theorem {declaration} : True := by trivial",
            )
        target_path.write_text(text, encoding="utf-8")
        return agent

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(fake, "run", solve_after_first_attempt)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator.executor = fake

    assert await orchestrator._prove(chapter)
    runs = state.task(chapter.id, Stage.PROVE).runs
    assert runs[0].proof_targets == runs[1].proof_targets
    assert runs[2].proof_targets != runs[1].proof_targets
    assert [target["declaration"] for target in runs[2].proof_targets] == ["target5"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_infrastructure_failure_does_not_consume_or_skip_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    orchestrator.executor = FakeExecutor(
        state,
        [
            AgentResult(
                succeeded=False,
                exit_code=1,
                changed=False,
                placeholders=1,
                usage=TokenUsage(),
                error="required MCP servers failed to initialize",
                infrastructure_failed=True,
            )
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    outcome = await orchestrator._prove(chapter)
    assert outcome.disposition is ExecutionDisposition.WAITING
    task = state.task(chapter.id, Stage.PROVE)
    assert task.status == TaskStatus.INTERRUPTED
    assert task.rounds == 1
    assert len(task.runs) == 1
    assert task.runs[0].proof_targets[0]["declaration"] == "target"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_waits_for_persisted_evaluating_upstream_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    blockers = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="prior-proof-run",
        failed_attempts=(
            {
                "path": "lean/Book/Chapter01.lean",
                "declaration": "target",
                "remaining_goal": "True",
                "obstruction": "an earlier construction is missing",
                "disposition": "missing_capability",
                "attempts": ["checked the current imported API"],
            },
        ),
    )
    request_id, _ = await state.enqueue_upstream_request(
        {
            "consumer_path": "lean/Book/Chapter01.lean",
            "blocked_declaration": "target",
            "residual_goal": "True",
            "needed_result": "an earlier construction",
            "owner_paths": ["lean/Book/Chapter01.lean"],
        },
        consumer_chapter_id=chapter.id,
        owner_chapter_id=chapter.id,
        blocker_ids=(str(blockers[0]["id"]),),
    )
    await state.update_upstream_request(request_id, UpstreamRequestStatus.EVALUATING)

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    outcome = await orchestrator._prove(chapter)
    assert outcome.disposition is ExecutionDisposition.WAITING
    task = state.task(chapter.id, Stage.PROVE)
    assert task.status == TaskStatus.BLOCKED
    assert request_id in task.detail
    assert task.rounds == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_resume_requeues_legacy_mcp_startup_failure(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.PROVE)
    log = tmp_path / "legacy-mcp-failure.jsonl"
    log.write_text(
        "required MCP servers failed to initialize: paf_lean: connection closed: "
        "initialize response\n",
        encoding="utf-8",
    )
    await state.finish_run(
        run,
        status=TaskStatus.FAILED,
        exit_code=1,
        changed=False,
        placeholders=1,
        log_path=str(log),
    )
    await state.set_task(
        chapter.id,
        Stage.PROVE,
        TaskStatus.FAILED,
        "proof chunks exhausted retries with 1 placeholder remaining",
    )
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    changed = await recovered.requeue_interrupted(resume_agents=True)
    task = recovered.task(chapter.id, Stage.PROVE)
    assert changed == [f"{chapter.id}:prove"]
    assert task.status == TaskStatus.PENDING
    assert task.detail == "infrastructure-failed agent queued for a fresh retry"
    await recovered.close()


@pytest.mark.asyncio
async def test_proof_validation_routes_foreign_diagnostic_to_its_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    first_path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    second_path = tmp_path / "lean" / "Book" / "Chapter02.lean"
    first_path.parent.mkdir(parents=True)
    first_path.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    second_path.write_text("theorem other : True := by trivial\n", encoding="utf-8")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    executor = FakeExecutor(state, [result(changed=True, placeholders=0)])
    orchestrator.executor = executor

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(
            False,
            1,
            "error: Book/Chapter02.lean:1:1: unknown identifier `missing`",
        )

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert not await orchestrator._prove(first)
    assert state.task(first.id, Stage.PROVE).rounds == 1
    request = next(iter(state.proof_review_requests.values()))
    assert request["kind"] == BUILD_ERROR_REVIEW_KIND
    assert set(request["feedback"]) == {second.id}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_proof_warning_queues_cleanup_without_resuming_completed_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "theorem target : True := by sorry\ntheorem downstream : True := by trivial\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    executor = FakeExecutor(
        state,
        [
            replace(result(changed=True, placeholders=0), thread_id="proof-session"),
            result(changed=True, placeholders=0),
        ],
    )
    original_run = executor.run

    async def solve_target(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        agent = await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        assert workspace_root is not None
        target = workspace_root / "lean" / "Book" / "Chapter01.lean"
        target.write_text(
            target.read_text(encoding="utf-8").replace("by sorry", "by trivial", 1),
            encoding="utf-8",
        )
        return agent

    validations = iter(
        (
            ValidationResult(
                False,
                1,
                "warning: Book/Chapter01.lean:1:1: target diagnostic",
                process_exit_code=0,
            ),
            ValidationResult(True, 0, "ok"),
        )
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return next(validations)

    monkeypatch.setattr(executor, "run", solve_target)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator.executor = executor

    assert await orchestrator._prove(chapter)
    assert executor.resume_thread_ids == [None]
    feedback, request_ids = orchestrator._warning_cleanup_feedback(chapter.id)
    assert len(request_ids) == 1
    assert "target diagnostic" in feedback
    assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.SUCCEEDED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_obstructed_proof_chunk_does_not_prevent_independent_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.PROVE: replace(config.stages[Stage.PROVE], chunk_size=2),
        },
    )
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "theorem first : True := by sorry\ntheorem second : True := by sorry\n",
        encoding="utf-8",
    )
    obstruction = unresolved_proof(
        "no proof can be constructed from the current interface",
        path="lean/Book/Chapter01.lean",
        declaration="first",
        kind="suspected_statement_defect",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    await state.record_proof_blockers(
        chapter.id,
        origin_run_id="earlier-proof-run",
        failed_attempts=[
            failed_attempt(
                "an earlier pass left this target unresolved",
                path="lean/Book/Chapter01.lean",
                declaration="first",
            )
            | {"disposition": "retry"}
        ],
    )
    fake = FakeExecutor(
        state,
        [
            result(changed=False, placeholders=2, unresolved_proofs=[obstruction]),
            result(changed=True, placeholders=1, unresolved_proofs=[]),
        ],
    )
    original_run = fake.run

    async def solve_second(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        agent = await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        if run.proof_targets[0]["declaration"] == "second":
            assert workspace_root is not None
            target_path = workspace_root / "lean" / "Book" / "Chapter01.lean"
            target_path.write_text(
                target_path.read_text(encoding="utf-8").replace(
                    "theorem second : True := by sorry",
                    "theorem second : True := by trivial",
                ),
                encoding="utf-8",
            )
        return agent

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(fake, "run", solve_second)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator.executor = fake

    assert not await orchestrator._prove(chapter)
    runs = state.task(chapter.id, Stage.PROVE).runs
    assert [[target["declaration"] for target in run.proof_targets] for run in runs] == [
        ["first", "second"],
        ["second"],
    ]
    assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.BLOCKED
    assert "theorem second : True := by trivial" in source.read_text(encoding="utf-8")
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_stops_after_two_repeated_no_progress_rounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(changed=True, placeholders=4),
            result(changed=False, placeholders=4),
            result(changed=False, placeholders=4),
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert not await orchestrator._prove(chapter)
    task = state.task(chapter.id, Stage.PROVE)
    assert task.rounds == 3
    assert task.detail == "proof pass stalled with 4 placeholders"
    assert orchestrator.executor.results == []
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_counts_edits_without_fewer_placeholders_as_no_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(changed=True, placeholders=4),
            result(changed=True, placeholders=4),
            result(changed=True, placeholders=4),
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert not await orchestrator._prove(chapter)
    task = state.task(chapter.id, Stage.PROVE)
    assert task.rounds == 3
    assert task.detail == "proof pass stalled with 4 placeholders"
    assert orchestrator.executor.results == []
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_prove_retries_receive_only_latest_attempt_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(changed=True, placeholders=4, issues=["tried route one"]),
            result(changed=True, placeholders=3, issues=["tried route two"]),
            result(changed=False, placeholders=3, issues=["tried route three"]),
            result(changed=False, placeholders=3),
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert not await orchestrator._prove(chapter)
    feedbacks = orchestrator.executor.feedbacks
    assert feedbacks[0] == ""
    assert "Previous proof result (round 1):" in feedbacks[1]
    assert "tried route one" in feedbacks[1]
    assert "Previous proof result (round 1):" not in feedbacks[2]
    assert "Previous proof result (round 2):" in feedbacks[2]
    assert "tried route one" not in feedbacks[2]
    assert "tried route two" in feedbacks[2]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_waits_for_upstream_book_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    (tmp_path / "books" / "second.md").write_text(
        "# Second\n\n## 1. Consequence\n", encoding="utf-8"
    )
    with config_path.open("a", encoding="utf-8") as handle:
        handle.write(
            """
[[books]]
id = "second"
title = "Second"
source = "books/second.md"
lean_root = "lean/Second"
module = "Second"
depends_on = ["book"]
chapters = [1]
"""
        )
    config = load_config(config_path)
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.chapters
    await mark_discovered(orchestrator, {second.id: (first.id,)})
    events: list[str] = []

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> StageOutcome:
        assert not rerun
        events.append(f"start:{chapter.book_id}")
        await asyncio.sleep(0)
        events.append(f"end:{chapter.book_id}")
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert events == ["start:book", "end:book", "start:second", "end:second"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_chapter_failure_cancels_and_drains_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    sibling_started = asyncio.Event()
    sibling_cleaned = asyncio.Event()

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> StageOutcome:
        assert not rerun
        if chapter.number == 1:
            await sibling_started.wait()
            raise RuntimeError("primary failure")
        sibling_started.set()
        try:
            await asyncio.Future()
        finally:
            sibling_cleaned.set()
        raise AssertionError("unreachable")

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    with pytest.raises(RuntimeError, match="primary failure"):
        await orchestrator.run_stage(Stage.FORMALIZE)
    assert sibling_cleaned.is_set()
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_workspace_acquisition_failure_finishes_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    async def fail_acquire(_run_id: str) -> object:
        raise RuntimeError("workspace unavailable")

    monkeypatch.setattr(orchestrator.isolation, "acquire", fail_acquire)

    with pytest.raises(RuntimeError, match="workspace unavailable"):
        await orchestrator._attempt(config.chapters[0], Stage.FORMALIZE)

    run = state.task(config.chapters[0].id, Stage.FORMALIZE).runs[-1]
    assert run.status == TaskStatus.FAILED
    assert run.finished_at is not None
    assert run.isolation is not None
    assert "workspace unavailable" in run.isolation["error"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_discovery_uses_live_repo_without_acquiring_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    reservation_claims: list[tuple[str, tuple[ReservationSpec, ...]]] = []
    released_reservations: list[tuple[str, int]] = []
    original_claim = state.claim_ordinary_path_reservations
    original_release = state.release_ordinary_path_reservations

    async def claim_reservation(
        owner_id: str,
        requested: tuple[ReservationSpec, ...],
        *,
        ttl_seconds: float,
        queue_on_conflict: bool = True,
    ) -> ReservationResult:
        reservation_claims.append((owner_id, requested))
        return await original_claim(
            owner_id,
            requested,
            ttl_seconds=ttl_seconds,
            queue_on_conflict=queue_on_conflict,
        )

    async def release_reservation(owner_id: str, generation: int) -> None:
        released_reservations.append((owner_id, generation))
        await original_release(owner_id, generation)

    async def fail_acquire(_run_id: str) -> object:
        raise AssertionError("discovery must not acquire an isolated workspace")

    async def discover(
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        del chapter, feedback
        assert stage is Stage.DISCOVER
        assert workspace_root == config.settings.repo
        agent = result(changed=False, placeholders=0)
        agent.report["source_dependencies"] = []
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            changed=False,
            report=agent.report,
        )
        return agent

    monkeypatch.setattr(orchestrator.isolation, "acquire", fail_acquire)
    monkeypatch.setattr(orchestrator.executor, "run", discover)
    monkeypatch.setattr(state, "claim_ordinary_path_reservations", claim_reservation)
    monkeypatch.setattr(state, "release_ordinary_path_reservations", release_reservation)

    attempt = await orchestrator._attempt(config.chapters[0], Stage.DISCOVER)

    assert attempt.validation.succeeded
    assert attempt.run.isolation == {
        "accepted": True,
        "generation": 0,
        "cache_generation": 0,
        "changed_paths": [],
        "promoted_cache_paths": [],
        "out_of_scope_paths": [],
        "error": "",
        "commit": "",
    }
    assert len(reservation_claims) == 1
    owner_id, requested = reservation_claims[0]
    assert owner_id.startswith(f"ordinary-{config.chapters[0].id}-discover-")
    assert {item.normalized_path for item in requested} == {
        "lean/Book/Chapter01.lean",
        "lean/Book/Chapter01",
    }
    assert released_reservations == [(owner_id, 1)]
    await orchestrator.shutdown()
