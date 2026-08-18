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
from paf.scheduler import (
    BUILD_ERROR_REVIEW_KIND,
    BUILD_WARNING_REVIEW_KIND,
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


def result(
    *,
    changed: bool,
    placeholders: int = 2,
    complete: bool = True,
    issues: list[str] | None = None,
    failed_attempts: list[dict[str, Any]] | None = None,
    finding_assessments: list[dict[str, str]] | None = None,
) -> AgentResult:
    report: dict[str, Any] = {
        "complete": complete,
        "summary": "reviewed",
        "issues": issues or [],
    }
    if failed_attempts is not None:
        report["failed_attempts"] = failed_attempts
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
    assert warning_ids == (warning_id,)
    assert orchestrator._proof_review_role(warning_ids) == WARNING_REVIEW_ROLE

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
async def test_diagnostics_are_selected_before_proof_findings(tmp_path: Path) -> None:
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
    assert selected_ids == (warning_id,)
    assert orchestrator._proof_review_role(selected_ids) == WARNING_REVIEW_ROLE

    await state.finish_proof_review_requests(chapter.id, selected_ids)
    _, selected_ids = orchestrator._proof_review_feedback(chapter.id)
    assert selected_ids == (proof_id,)
    assert orchestrator._proof_review_role(selected_ids) == ""
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_tree_runs_diagnostics_before_pending_proof_review(
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
    assert calls == [
        (WARNING_REVIEW_ROLE, (warning_id,)),
        ("", (proof_id,)),
    ]
    assert state.proof_review_requests == {}
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
async def test_malformed_review_report_resumes_same_session(tmp_path: Path) -> None:
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
    assert executor.resume_thread_ids == [None, "malformed-session"]
    assert "structured final report" in executor.resume_prompts[-1]
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
async def test_upstream_requests_persist_answers_and_batch_by_owner(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    request = {
        "blocked_declaration": "consumerTarget",
        "consumer_path": "lean/Book/Chapter02.lean",
        "residual_goal": "⊢ Result x",
        "needed_result": "A transport lemma from Input x to Result x",
        "owner_chapter_id": owner.id,
        "owner_paths": ["lean/Book/Chapter01.lean"],
        "attempted_alternatives": ["simp [Result]", "exact existingCandidate x"],
    }
    request_id, created = await state.enqueue_upstream_request(
        request,
        consumer_chapter_id=consumer.id,
        origin_run_id="proof-run",
        owner_chapter_id=owner.id,
        previous_attempts="Proof attempt 1 left ⊢ Result x.",
    )

    assert created
    assert state.upstream_requests[request_id]["status"] == UpstreamRequestStatus.REQUESTED
    assert state.upstream_request_batches() == {owner.id: [request_id]}
    assert state.hot_snapshot()["upstream_request_batches"] == {owner.id: [request_id]}

    answer = {
        "disposition": "existing",
        "declarations": ["Book.transport_input_result"],
        "usage_guidance": "Apply `Book.transport_input_result x`.",
        "rejection_reason": "",
    }
    await state.record_upstream_answers(
        (request_id,),
        run_id="repair-run",
        answers={request_id: answer},
    )
    assert state.upstream_requests[request_id]["status"] == UpstreamRequestStatus.ANSWERED
    await state.finish_upstream_requests(
        (request_id,),
        run_id="retry-run",
        succeeded_ids=(),
        error="the residual goal remained",
    )
    await state.set_task(
        consumer.id,
        Stage.PROVE,
        TaskStatus.BLOCKED,
        "upstream request requires manual escalation",
    )
    await state.close()

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    persisted = reloaded.upstream_requests[request_id]
    assert persisted["status"] == UpstreamRequestStatus.ESCALATED
    assert persisted["answer"]["declarations"] == ["Book.transport_input_result"]
    assert persisted["repair_run_id"] == "repair-run"
    assert persisted["retry_run_id"] == "retry-run"
    assert persisted["escalation_reason"] == "the residual goal remained"
    assert not {"answers", "repair_attempts", "retry_attempts", "history"}.intersection(persisted)
    assert reloaded.upstream_request_batches() == {}

    assert await reloaded.unblock() == [f"{consumer.id}:prove"]
    assert reloaded.upstream_requests[request_id]["status"] == UpstreamRequestStatus.ANSWERED
    assert reloaded.upstream_requests[request_id]["answer"] == persisted["answer"]
    await reloaded.close()


@pytest.mark.asyncio
async def test_clear_upstream_requests_closes_requests_and_reopens_linked_blockers(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "blocked_declaration": "consumerTarget",
            "consumer_path": "lean/Book/Chapter02.lean",
            "residual_goal": "⊢ Result x",
            "needed_result": "A transport lemma from Input x to Result x",
            "owner_chapter_id": owner.id,
            "owner_paths": ["lean/Book/Chapter01.lean"],
            "attempted_alternatives": ["simp", "exact candidate"],
        },
        consumer_chapter_id=consumer.id,
        origin_run_id="proof-run",
        owner_chapter_id=owner.id,
        previous_attempts="attempt one",
    )
    state.proof_blockers["B1"] = {
        "id": "B1",
        "status": ProofBlockerStatus.UPSTREAM_REQUESTED.value,
        "consumer_chapter_id": consumer.id,
        "request_id": request_id,
    }

    assert await state.clear_upstream_requests() == [request_id]
    assert state.upstream_request_batches() == {}
    assert state.upstream_requests[request_id]["status"] == UpstreamRequestStatus.CLOSED
    assert state.upstream_requests[request_id]["closed_reason"] == "manually cleared"
    assert state.proof_blockers["B1"]["status"] == ProofBlockerStatus.OPEN
    assert "request_id" not in state.proof_blockers["B1"]
    assert await state.clear_upstream_requests() == []
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    assert recovered.upstream_requests[request_id]["status"] == UpstreamRequestStatus.CLOSED
    assert recovered.proof_blockers["B1"]["status"] == ProofBlockerStatus.OPEN
    await recovered.close()


def test_upstream_request_uses_unique_path_owner_over_agent_label(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))

    request, owner_id, error = orchestrator._normalize_upstream_request(
        consumer,
        {
            "blocked_declaration": "consumerTarget",
            "consumer_path": "lean/Book/Chapter02.lean",
            "residual_goal": "⊢ Result x",
            "needed_result": "A transport lemma from Input x to Result x",
            "owner_chapter_id": "chapter 1",
            "owner_paths": ["lean/Book/Chapter01.lean"],
            "attempted_alternatives": ["simp [Result]", "exact existingCandidate x"],
        },
    )

    assert error == ""
    assert owner_id == owner.id
    assert request["owner_chapter_id"] == owner.id


@pytest.mark.asyncio
async def test_interrupted_runs_leave_requests_at_last_completed_fact(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    base = {
        "blocked_declaration": "consumerTarget",
        "consumer_path": "lean/Book/Chapter02.lean",
        "residual_goal": "⊢ Result x",
        "needed_result": "transport Input to Result",
        "owner_chapter_id": owner.id,
        "owner_paths": ["lean/Book/Chapter01.lean"],
        "attempted_alternatives": ["simp", "exact candidate"],
    }
    repair_id, _ = await state.enqueue_upstream_request(
        base,
        consumer_chapter_id=consumer.id,
        origin_run_id="proof-one",
        owner_chapter_id=owner.id,
        previous_attempts="attempt one",
    )
    repair_run = await state.start_auxiliary_run(
        owner.id,
        Stage.PROVE,
        role="upstream_repair",
        request_ids=(repair_id,),
    )

    retry_request = dict(base)
    retry_request["blocked_declaration"] = "secondTarget"
    retry_id, _ = await state.enqueue_upstream_request(
        retry_request,
        consumer_chapter_id=consumer.id,
        origin_run_id="proof-two",
        owner_chapter_id=owner.id,
        previous_attempts="attempt two",
    )
    await state.record_upstream_answers(
        (retry_id,),
        run_id="repair-run",
        answers={
            retry_id: {
                "disposition": "downstream",
                "declarations": [],
                "usage_guidance": "Prove the bridge in the consumer.",
                "rejection_reason": "The construction depends on consumer-only hypotheses.",
            }
        },
    )
    retry_run = await state.start_run(consumer.id, Stage.PROVE)
    await state.update_run(
        retry_run,
        role="downstream_retry",
        request_ids=[retry_id],
    )
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()

    assert recovered.upstream_requests[repair_id]["status"] == UpstreamRequestStatus.REQUESTED
    assert recovered.upstream_requests[retry_id]["status"] == UpstreamRequestStatus.ANSWERED
    assert recovered.upstream_request_batches() == {owner.id: [repair_id]}
    assert recovered.task(owner.id, Stage.PROVE).runs[-1].id == repair_run.id
    assert recovered.task(owner.id, Stage.PROVE).runs[-1].status == TaskStatus.INTERRUPTED
    assert recovered.task(consumer.id, Stage.PROVE).runs[-1].id == retry_run.id
    assert recovered.task(consumer.id, Stage.PROVE).runs[-1].status == TaskStatus.INTERRUPTED
    await recovered.close()


@pytest.mark.asyncio
async def test_upstream_request_recovery_normalizes_malformed_collection_fields(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    state.upstream_requests["damaged-request"] = {
        "id": "damaged-request",
        "status": "repairing",
        "consumer_chapter_id": chapter.id,
        "history": {"not": "a list"},
        "origin_run_ids": "not a list",
        "repair_attempts": None,
        "answer": "not an answer object",
        "previous_attempts": ["not text"],
    }
    state.upstream_requests["interrupted-retry"] = {
        "id": "interrupted-retry",
        "status": "retrying",
        "consumer_chapter_id": chapter.id,
        "answer": {
            "disposition": "existing",
            "declarations": ["Book.bridge"],
            "usage_guidance": "Apply the bridge.",
            "rejection_reason": "",
        },
        "history": [{"status": "retrying"}],
    }
    state.upstream_requests["manual-escalation"] = {
        "id": "manual-escalation",
        "status": "manual_escalation",
        "consumer_chapter_id": chapter.id,
        "escalation_reason": "legacy failure",
    }
    await state.save()
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    request = recovered.upstream_requests["damaged-request"]

    assert request["status"] == UpstreamRequestStatus.REQUESTED
    assert request["origin_run_ids"] == []
    assert request["answer"] is None
    assert request["previous_attempts"] == ""
    assert "repair_attempts" not in request
    assert "history" not in request
    retry = recovered.upstream_requests["interrupted-retry"]
    assert retry["status"] == UpstreamRequestStatus.ANSWERED
    assert "history" not in retry
    assert (
        recovered.upstream_requests["manual-escalation"]["status"]
        == UpstreamRequestStatus.ESCALATED
    )
    await recovered.close()


@pytest.mark.asyncio
async def test_validated_external_proof_closes_request_without_false_escalation(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    request_id, _ = await state.enqueue_upstream_request(
        {
            "blocked_declaration": "consumerTarget",
            "consumer_path": "lean/Book/Chapter02.lean",
            "residual_goal": "⊢ True",
            "needed_result": "a truth bridge",
            "owner_chapter_id": owner.id,
            "owner_paths": ["lean/Book/Chapter01.lean"],
            "attempted_alternatives": ["simp", "constructor"],
        },
        consumer_chapter_id=consumer.id,
        origin_run_id="proof-run",
        owner_chapter_id=owner.id,
        previous_attempts="attempt one",
    )

    assert await state.finish_upstream_requests(
        (request_id,),
        run_id="concurrent-proof",
        succeeded_ids=(request_id,),
        success_detail="a concurrent validated proof solved the declaration",
    ) == (request_id,)
    request = state.upstream_requests[request_id]
    assert request["status"] == UpstreamRequestStatus.CLOSED
    assert request["closed_by_run_id"] == "concurrent-proof"
    assert request["closed_reason"] == "a concurrent validated proof solved the declaration"
    assert "history" not in request
    await state.close()


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
async def test_successful_failure_retry_releases_only_its_blocked_dependents(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    state = StateStore(config)
    await state.load_or_create()
    state.source_dependency_tree = {
        "dependencies": {first.id: [], second.id: [first.id]},
        "nodes": {},
    }
    await state.set_tasks(
        (first.id, second.id),
        Stage.FORMALIZE,
        TaskStatus.SUCCEEDED,
        "formalized",
    )
    await state.set_task(first.id, Stage.REVIEW, TaskStatus.FAILED, "review failed")
    await state.set_task(
        second.id,
        Stage.REVIEW,
        TaskStatus.BLOCKED,
        "blocked by a failed prerequisite review; unrelated branches completed",
    )
    await state.set_task(
        second.id,
        Stage.PROVE,
        TaskStatus.BLOCKED,
        "blocked because statement review did not complete",
    )
    await state.set_task(
        first.id,
        Stage.PROVE,
        TaskStatus.BLOCKED,
        "upstream request requires manual escalation: request-one",
    )

    assert await state.retry_failed() == [f"{first.id}:review"]
    assert state.shepherd_failure_records() == []
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    first_review = recovered.task(first.id, Stage.REVIEW)
    assert first_review.recovering_failure

    await recovered.set_task(first.id, Stage.REVIEW, TaskStatus.RUNNING, "retrying")
    await recovered.set_task(first.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "review recovered")

    second_review = recovered.task(second.id, Stage.REVIEW)
    assert second_review.status == TaskStatus.PENDING
    assert recovered.readiness(second_review).ready
    assert recovered.task(second.id, Stage.PROVE).status == TaskStatus.PENDING
    assert recovered.task(first.id, Stage.PROVE).waiting_on

    await recovered.set_task(second.id, Stage.REVIEW, TaskStatus.RUNNING, "reviewing")
    await recovered.set_task(second.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")

    second_proof = recovered.task(second.id, Stage.PROVE)
    assert second_proof.status == TaskStatus.PENDING
    assert recovered.readiness(second_proof).ready
    assert recovered.task(first.id, Stage.PROVE).waiting_on
    await recovered.close()


@pytest.mark.asyncio
async def test_failed_proof_retry_reopens_durable_execution_gates(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.PROVE, TaskStatus.FAILED, "proof stalled")
    first = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-one",
        failed_attempts=(failed_attempt("missing bridge"),),
    )
    blockers = await state.record_proof_blockers(
        chapter.id,
        origin_run_id="proof-two",
        failed_attempts=(failed_attempt("missing bridge"),),
    )
    blocker_id = str(first[0]["id"])
    assert blockers[0]["sightings"] == 2
    await state.set_proof_blocker_status((blocker_id,), ProofBlockerStatus.BLOCKED)
    request_id, _ = await state.enqueue_upstream_request(
        {
            "blocked_declaration": "Book.target",
            "consumer_path": "lean/Book/Chapter01.lean",
            "residual_goal": "⊢ True",
            "needed_result": "A reusable proof of True.",
            "attempted_alternatives": ["exact True.intro"],
        },
        consumer_chapter_id=chapter.id,
        origin_run_id="proof-two",
        owner_chapter_id=chapter.id,
        previous_attempts="two failed attempts",
        escalation_reason="manual evaluation required",
    )

    assert await state.retry_failed() == [f"{chapter.id}:prove"]

    task = state.task(chapter.id, Stage.PROVE)
    assert task.status == TaskStatus.PENDING
    assert task.detail == "manually retried"
    blocker = state.proof_blockers[blocker_id]
    assert blocker["status"] == ProofBlockerStatus.OPEN
    assert blocker["sightings"] == 2
    assert blocker["retry_sighting_baseline"] == 2
    assert blocker["origin_run_ids"] == ["proof-one", "proof-two"]
    request = state.upstream_requests[request_id]
    assert request["status"] == UpstreamRequestStatus.REQUESTED
    assert "escalation_reason" not in request
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    assert recovered.proof_blockers[blocker_id]["status"] == ProofBlockerStatus.OPEN
    assert recovered.upstream_requests[request_id]["status"] == UpstreamRequestStatus.REQUESTED
    await recovered.close()


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

    assert recovered.task(owner.id, Stage.REVIEW).status == TaskStatus.PENDING
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
        if len(commands) == 1:
            return ValidationResult(
                False,
                1,
                "error: Book/Chapter01/Section.lean:1:1: broken review output",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    first_feedback, second_feedback, third_feedback = await asyncio.gather(
        orchestrator._review_build(first),
        orchestrator._review_build(second),
        orchestrator._review_build(third),
    )

    def target(chapter: Chapter) -> str:
        return chapter.build_command.rpartition(" ")[2]

    assert commands == [
        f"cd lean && lake build {target(first)} {target(second)} {target(third)}",
        f"cd lean && lake build {target(second)} {target(third)}",
    ]
    assert set(first_feedback) == {first.id}
    assert "broken review output" in first_feedback[first.id]
    assert second_feedback == {}
    assert third_feedback == {}
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
    assert partitioned[consumer.id].status is ValidationStatus.UPSTREAM_FAILED
    assert partitioned[consumer.id].blocked_by == (owner.id,)


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
                "upstream diagnostic",
                status=ValidationStatus.UPSTREAM_FAILED,
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
async def test_formalizer_routes_late_upstream_diagnostics_to_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, consumer = config.chapters
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    await state.set_task(owner.id, Stage.REVIEW, TaskStatus.RUNNING, "reviewing")
    monkeypatch.setattr(orchestrator, "_scope_exists", lambda _chapter: asyncio.sleep(0, True))

    async def build(*_args: object, **_kwargs: object) -> dict[str, ValidationResult]:
        return {
            consumer.id: ValidationResult(
                False,
                1,
                "upstream diagnostic without a source location",
                status=ValidationStatus.UPSTREAM_FAILED,
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
    assert owner_formalize.status == TaskStatus.SUCCEEDED
    owner_review = state.task(owner.id, Stage.REVIEW)
    assert owner_review.status == TaskStatus.PENDING
    assert (
        "upstream diagnostic without a source location"
        in orchestrator._proof_review_feedback(owner.id)[0]
    )
    requirement = outcome.waiting_on[0]
    assert requirement.owner_task_key == state.key(owner.id, Stage.REVIEW)
    assert state.task(consumer.id, Stage.FORMALIZE).waiting_on == (requirement,)
    await state.close()


@pytest.mark.asyncio
async def test_shepherd_reconciliation_accepts_matching_clean_build_without_agent(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    digest = scope_digest(config.settings.repo, chapter)
    state.formalize_graph = {
        "clean": {chapter.id: {"source_digest": digest, "build_generation": 1}}
    }
    await state.save("formalize_graph")
    await state.set_task(
        chapter.id,
        Stage.FORMALIZE,
        TaskStatus.FAILED,
        "stale coordinator result",
    )

    assert await orchestrator._reconcile_stale_formalizations(
        (state.key(chapter.id, Stage.FORMALIZE),)
    )
    assert state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
    assert state.task(chapter.id, Stage.FORMALIZE).rounds == 0
    await state.close()


@pytest.mark.asyncio
async def test_shepherd_reconciliation_queues_completed_unchanged_agent_for_normal_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await state.load_or_create()
    digest = scope_digest(config.settings.repo, chapter)
    run = await state.start_run(chapter.id, Stage.FORMALIZE)
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        changed=True,
        report={"complete": True},
        source_digest=digest,
    )
    await state.set_task(
        chapter.id,
        Stage.FORMALIZE,
        TaskStatus.FAILED,
        "coordinator result was interrupted",
    )

    async def build(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("shepherd reconciliation must not run Lean")

    monkeypatch.setattr(orchestrator, "_build_chapters", build)

    assert await orchestrator._reconcile_stale_formalizations(
        (state.key(chapter.id, Stage.FORMALIZE),)
    )
    assert state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.PENDING
    assert run.validation is None
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
    assert [record.task_key for record in orchestrator.state.shepherd_failure_records()] == [
        orchestrator.state.key(config.chapters[0].id, Stage.FORMALIZE)
    ]
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

            async def finish(self, *, succeeded: bool, publish: bool) -> tuple[str, ...]:
                nonlocal published
                published += int(publish)
                return await workspace.finish(succeeded=succeeded, publish=publish)

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
async def test_overlay_build_releases_source_barrier_and_rejects_concurrent_edit(
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

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
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
    assert not result.succeeded
    assert "changed during the coordinator build" in result.output
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
    assert review.status == TaskStatus.PENDING
    assert proof.status == TaskStatus.PENDING
    assert proof.source_digest is None
    feedback, request_ids = orchestrator._proof_review_feedback(chapter.id)
    assert "the statement needs another hypothesis" in feedback
    assert len(request_ids) == 1
    assert orchestrator.state.fixup_requests == {}
    await orchestrator.shutdown()


def test_upstream_answers_validate_reported_declarations_against_sources(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    owner, _consumer = config.chapters
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem provedBridge : True := by trivial\n\ntheorem unprovedBridge : True := by sorry\n",
        encoding="utf-8",
    )
    orchestrator = Orchestrator(config, StateStore(config))
    base = {
        "usage_guidance": "Apply the named bridge.",
        "rejection_reason": "",
    }
    answers = {
        "good-added": base | {"disposition": "added", "declarations": ["Book.provedBridge"]},
        "missing-added": base | {"disposition": "added", "declarations": ["Book.missingBridge"]},
        "unproved-added": base | {"disposition": "added", "declarations": ["Book.unprovedBridge"]},
        "unproved-existing": base
        | {"disposition": "existing", "declarations": ["Book.unprovedBridge"]},
        "external-existing": base
        | {"disposition": "existing", "declarations": ["Mathlib.externalBridge"]},
    }

    accepted, error = orchestrator._validate_upstream_answer_declarations(
        owner,
        answers,
        agent_changed=True,
    )

    assert set(accepted) == {"good-added", "external-existing"}
    assert "Book.missingBridge" in error
    assert "Book.unprovedBridge" in error
    accepted, error = orchestrator._validate_upstream_answer_declarations(
        owner,
        {"good-added": answers["good-added"]},
        agent_changed=False,
    )
    assert accepted == {}
    assert "without an integrated source edit" in error


@pytest.mark.asyncio
async def test_proof_upstream_request_runs_repair_then_targeted_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    owner, consumer = config.chapters
    root = tmp_path / "lean" / "Book"
    root.mkdir(parents=True)
    owner_path = root / "Chapter01.lean"
    consumer_path = root / "Chapter02.lean"
    owner_path.write_text("def ownerInput : True := trivial\n", encoding="utf-8")
    consumer_path.write_text(
        "import LastLib.Book.Chapter01\ntheorem blockedTarget : True := by sorry\n",
        encoding="utf-8",
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    repair_batches: list[tuple[str, ...]] = []
    proof_roles: list[str] = []

    async def finish_agent(
        run: RunRecord,
        *,
        changed: bool,
        placeholders: int,
        report: dict[str, Any],
    ) -> AgentResult:
        await orchestrator.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            changed=changed,
            placeholders=placeholders,
            report=report,
            usage=TokenUsage(),
        )
        return AgentResult(
            succeeded=True,
            exit_code=0,
            changed=changed,
            placeholders=placeholders,
            usage=TokenUsage(),
            report=report,
        )

    async def proof_agent(
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        assert stage is Stage.PROVE
        assert workspace_root is not None
        proof_roles.append(run.role)
        if run.role == "downstream_retry":
            request = next(iter(orchestrator.state.upstream_requests.values()))
            assert request["status"] == UpstreamRequestStatus.ANSWERED
            assert "earlierBridge" in feedback
            assert "blockedTarget" in feedback
            target = workspace_root / "lean" / "Book" / "Chapter02.lean"
            target.write_text(
                target.read_text(encoding="utf-8").replace("by sorry", "by exact earlierBridge"),
                encoding="utf-8",
            )
            return await finish_agent(
                run,
                changed=True,
                placeholders=0,
                report={
                    "complete": True,
                    "summary": "Used the repaired upstream bridge.",
                    "issues": [],
                    "failed_attempts": [],
                    "upstream_requests": [],
                },
            )
        assert chapter.id == consumer.id
        return await finish_agent(
            run,
            changed=False,
            placeholders=1,
            report={
                "complete": False,
                "summary": "Preserved the blocked proof after exhausting local routes.",
                "issues": [],
                "upstream_requests": [
                    {
                        "blocked_declaration": "blockedTarget",
                        "consumer_path": "lean/Book/Chapter02.lean",
                        "residual_goal": "⊢ True",
                        "needed_result": "A proved upstream truth bridge",
                        "owner_chapter_id": owner.id,
                        "owner_paths": ["lean/Book/Chapter01.lean"],
                        "attempted_alternatives": [
                            "exact ownerInput",
                            "constructor followed by simp",
                        ],
                    }
                ],
            },
        )

    async def repair_agent(
        chapter: Chapter,
        run: RunRecord,
        requests: Iterable[dict[str, Any]],
        *,
        workspace_root: Path | None = None,
    ) -> AgentResult:
        assert chapter.id == owner.id
        assert workspace_root is not None
        selected = tuple(requests)
        assert all(request["status"] == UpstreamRequestStatus.REQUESTED for request in selected)
        repair_batches.append(tuple(str(request["id"]) for request in selected))
        target = workspace_root / "lean" / "Book" / "Chapter01.lean"
        target.write_text(
            target.read_text(encoding="utf-8") + "\ntheorem earlierBridge : True := by trivial\n",
            encoding="utf-8",
        )
        request_ids = [str(request["id"]) for request in selected]
        return await finish_agent(
            run,
            changed=True,
            placeholders=0,
            report={
                "complete": True,
                "summary": "Added and proved the requested upstream truth bridge.",
                "issues": [],
                "failed_attempts": [],
                "upstream_answers": [
                    {
                        "request_ids": request_ids,
                        "disposition": "added",
                        "declarations": ["earlierBridge"],
                        "usage_guidance": "Apply `earlierBridge` directly.",
                        "rejection_reason": "",
                    }
                ],
            },
        )

    monkeypatch.setattr(orchestrator.executor, "run", proof_agent)
    monkeypatch.setattr(orchestrator.executor, "run_upstream_repair", repair_agent)

    assert await orchestrator._prove(consumer)
    assert len(repair_batches) == 1
    assert proof_roles == ["prove", "downstream_retry"]
    assert orchestrator.state.task(owner.id, Stage.PROVE).rounds == 0
    owner_runs = orchestrator.state.task(owner.id, Stage.PROVE).runs
    assert len(owner_runs) == 1
    assert owner_runs[0].auxiliary
    request = next(iter(orchestrator.state.upstream_requests.values()))
    assert request["status"] == UpstreamRequestStatus.CLOSED
    assert request["answer"]["declarations"] == ["earlierBridge"]
    assert (
        request["closed_by_run_id"] == orchestrator.state.task(consumer.id, Stage.PROVE).runs[-1].id
    )
    assert "by exact earlierBridge" in consumer_path.read_text(encoding="utf-8")
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_upstream_request_escalates_only_after_failed_targeted_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = with_example_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    owner, consumer = config.chapters
    root = tmp_path / "lean" / "Book"
    root.mkdir(parents=True)
    (root / "Chapter01.lean").write_text("def input := 1\n", encoding="utf-8")
    (root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ntheorem blockedTarget : True := by sorry\n",
        encoding="utf-8",
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    proof_attempts = 0

    async def persist(
        run: RunRecord,
        report: dict[str, Any],
        *,
        placeholders: int,
    ) -> AgentResult:
        await orchestrator.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            changed=False,
            placeholders=placeholders,
            report=report,
        )
        return AgentResult(
            True,
            0,
            False,
            placeholders,
            TokenUsage(),
            report=report,
        )

    async def proof_agent(
        _chapter: Chapter,
        _stage: Stage,
        run: RunRecord,
        **_kwargs: Any,
    ) -> AgentResult:
        nonlocal proof_attempts
        proof_attempts += 1
        requests = []
        if proof_attempts == 1:
            requests = [
                {
                    "blocked_declaration": "blockedTarget",
                    "consumer_path": "lean/Book/Chapter02.lean",
                    "residual_goal": "⊢ True",
                    "needed_result": "consumer-specific bridge",
                    "owner_chapter_id": owner.id,
                    "owner_paths": ["lean/Book/Chapter01.lean"],
                    "attempted_alternatives": ["simp", "exact input"],
                }
            ]
        return await persist(
            run,
            {
                "complete": False,
                "summary": "The target remains blocked.",
                "issues": [],
                "failed_attempts": [],
                "upstream_requests": requests,
            },
            placeholders=1,
        )

    async def repair_agent(
        _chapter: Chapter,
        run: RunRecord,
        requests: Iterable[dict[str, Any]],
        **_kwargs: Any,
    ) -> AgentResult:
        request_ids = [str(request["id"]) for request in requests]
        return await persist(
            run,
            {
                "complete": True,
                "summary": "The bridge belongs in the consumer.",
                "issues": [],
                "failed_attempts": [],
                "upstream_answers": [
                    {
                        "request_ids": request_ids,
                        "disposition": "downstream",
                        "declarations": [],
                        "usage_guidance": "Construct the result from the consumer hypothesis.",
                        "rejection_reason": "The needed hypothesis exists only downstream.",
                    }
                ],
            },
            placeholders=0,
        )

    monkeypatch.setattr(orchestrator.executor, "run", proof_agent)
    monkeypatch.setattr(orchestrator.executor, "run_upstream_repair", repair_agent)

    assert not await orchestrator._prove(consumer)
    assert proof_attempts == 2
    request = next(iter(orchestrator.state.upstream_requests.values()))
    assert request["status"] == UpstreamRequestStatus.ESCALATED
    assert request["answer"]["disposition"] == "downstream"
    assert request["retry_run_id"] == orchestrator.state.task(consumer.id, Stage.PROVE).runs[-1].id
    assert "blocked declaration remained unresolved" in request["escalation_reason"]
    assert orchestrator.state.task(consumer.id, Stage.PROVE).status == TaskStatus.FAILED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_requested_upstream_requests_for_one_owner_share_one_repair_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    (tmp_path / "books" / "book.md").write_text(
        "# Book\n\n## 1. First chapter\n\nText.\n\n"
        "## 2. Second chapter\n\nText.\n\n## 3. Third chapter\n\nText.\n",
        encoding="utf-8",
    )
    config = with_example_modules(load_config(config_path))
    owner, *consumers = config.chapters
    root = tmp_path / "lean" / "Book"
    root.mkdir(parents=True)
    (root / "Chapter01.lean").write_text(
        "theorem sharedBridge : True := by trivial\n",
        encoding="utf-8",
    )
    for consumer in consumers:
        (root / f"Chapter{consumer.number:02d}.lean").write_text(
            f"import LastLib.Book.Chapter01\ntheorem blocked{consumer.number} : True := by sorry\n",
            encoding="utf-8",
        )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    request_ids: list[str] = []
    for consumer in consumers:
        request_id, _ = await orchestrator.state.enqueue_upstream_request(
            {
                "blocked_declaration": f"blocked{consumer.number}",
                "consumer_path": f"lean/Book/Chapter{consumer.number:02d}.lean",
                "residual_goal": "⊢ True",
                "needed_result": "the shared truth bridge",
                "owner_chapter_id": owner.id,
                "owner_paths": ["lean/Book/Chapter01.lean"],
                "attempted_alternatives": ["simp", "constructor"],
            },
            consumer_chapter_id=consumer.id,
            origin_run_id=f"proof-{consumer.number}",
            owner_chapter_id=owner.id,
            previous_attempts=f"attempt {consumer.number}",
        )
        request_ids.append(request_id)

    batches: list[tuple[str, ...]] = []

    async def repair_agent(
        _chapter: Chapter,
        run: RunRecord,
        requests: Iterable[dict[str, Any]],
        **_kwargs: Any,
    ) -> AgentResult:
        selected = tuple(str(request["id"]) for request in requests)
        batches.append(selected)
        report = {
            "complete": True,
            "summary": "Identified the existing shared bridge.",
            "issues": [],
            "failed_attempts": [],
            "upstream_answers": [
                {
                    "request_ids": list(selected),
                    "disposition": "existing",
                    "declarations": ["sharedBridge"],
                    "usage_guidance": "Apply `sharedBridge` directly.",
                    "rejection_reason": "",
                }
            ],
        }
        await orchestrator.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            changed=False,
            placeholders=0,
            report=report,
        )
        return AgentResult(True, 0, False, 0, TokenUsage(), report=report)

    monkeypatch.setattr(orchestrator.executor, "run_upstream_repair", repair_agent)

    answered = await asyncio.gather(
        *(orchestrator._ensure_upstream_answers((request_id,)) for request_id in request_ids)
    )

    assert len(batches) == 1
    assert set(batches[0]) == set(request_ids)
    assert answered == [(request_ids[0],), (request_ids[1],)]
    assert all(
        orchestrator.state.upstream_requests[request_id]["status"] == UpstreamRequestStatus.ANSWERED
        for request_id in request_ids
    )
    repair_runs = orchestrator.state.task(owner.id, Stage.PROVE).runs
    assert len(repair_runs) == 1
    assert set(repair_runs[0].request_ids) == set(request_ids)
    await orchestrator.shutdown()


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
        "modules": [],
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
    config = replace(
        config,
        settings=replace(config.settings, interface_invalidation="interface"),
    )
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
async def test_background_build_does_not_change_proof_stage(
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
    assert proof.source_digest == "proved-source"
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
        proof_request_ids: tuple[str, ...] = (),
    ) -> StageOutcome:
        nonlocal reviews
        assert rerun == (reviews > 0)
        review_feedback.append(feedback)
        assert bool(proof_request_ids) == bool(feedback)
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
async def test_prove_assigns_source_ordered_chunks_of_four_and_persists_them(
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
            result(changed=True, placeholders=5),
            result(changed=True, placeholders=1),
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
        4,
        4,
        1,
    ]
    assert [[target["declaration"] for target in run.proof_targets] for run in runs] == [
        ["target1", "target2", "target3", "target4"],
        ["target5", "target6", "target7", "target8"],
        ["target9"],
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
async def test_prove_retries_the_same_chunk_before_advancing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.PROVE: replace(config.stages[Stage.PROVE], max_rounds=2),
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
async def test_proof_validation_resumes_originating_chunk_for_local_diagnostic(
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
    assert executor.resume_thread_ids == [None, "proof-session"]
    assert "target diagnostic" in executor.feedbacks[1]
    assert state.proof_review_requests == {}
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
            Stage.PROVE: replace(config.stages[Stage.PROVE], chunk_size=1),
        },
    )
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text(
        "theorem first : True := by sorry\ntheorem second : True := by sorry\n",
        encoding="utf-8",
    )
    obstruction = failed_attempt(
        "no proof can be constructed from the current interface",
        path="lean/Book/Chapter01.lean",
        declaration="first",
    ) | {"disposition": "genuine_blocker"}
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.force = True
    fake = FakeExecutor(
        state,
        [
            result(changed=False, placeholders=2, failed_attempts=[obstruction]),
            result(changed=False, placeholders=2, failed_attempts=[obstruction]),
            result(changed=True, placeholders=1, failed_attempts=[]),
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
    assert [run.proof_targets[0]["declaration"] for run in runs] == [
        "first",
        "first",
        "second",
    ]
    assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.FAILED
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
    assert "Proof attempt 1:" in feedbacks[1]
    assert "tried route one" in feedbacks[1]
    assert "Proof attempt 1:" not in feedbacks[2]
    assert "Proof attempt 2:" in feedbacks[2]
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
    await orchestrator.shutdown()
