import asyncio
import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import paf.scheduler as scheduler_module
from paf.codex import (
    AgentResult,
    CodexExecutor,
    ValidationResult,
    scope_digest,
)
from paf.config import load_config
from paf.models import Chapter, PipelineConfig, Stage
from paf.scheduler import FormalizeOutcome, Orchestrator, ReviewOutcome
from paf.state import (
    RunRecord,
    StateStore,
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
        result = self.results.pop(0)
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            exit_code=0,
            changed=result.changed,
            placeholders=result.placeholders,
            report=result.report,
            usage=result.usage,
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
) -> AgentResult:
    report: dict[str, Any] = {
        "changed": changed,
        "complete": complete,
        "summary": "reviewed",
        "issues": issues or [],
    }
    if failed_attempts is not None:
        report["failed_attempts"] = failed_attempts
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

    async def discover(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        del rerun
        started.append(chapter.id)
        if len(started) == 2:
            window_full.set()
        await release.wait()
        return FormalizeOutcome(True)

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
    assert reloaded.snapshot()["version"] == 14


@pytest.mark.asyncio
async def test_review_progress_reconciles_pending_initial_fixup(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()

    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.RUNNING, "reviewing")

    fixup = state.task(chapter.id, Stage.FORMALIZE)
    assert fixup.status == TaskStatus.SUCCEEDED
    assert fixup.detail == "formalization completed before review"

    await state.set_task(
        chapter.id,
        Stage.FORMALIZE,
        TaskStatus.RUNNING,
        "late coordinator rebuild",
    )
    assert fixup.status == TaskStatus.SUCCEEDED
    assert fixup.detail == "formalization completed before review"
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
    await reloaded.close()


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
    assert hot["version"] == 14
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
async def test_formalize_rejects_an_incomplete_draft(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    await mark_discovered(orchestrator)
    orchestrator.executor = FakeExecutor(
        state,
        [result(changed=True, complete=False, issues=["coverage audit unfinished"])],
    )

    assert not await orchestrator.run_stage(Stage.FORMALIZE)
    task = state.task(chapter.id, Stage.FORMALIZE)
    assert task.status == TaskStatus.FAILED
    assert task.detail == "formalizer failed or reported incomplete coverage and diagnostics"
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
async def test_unqueued_proof_finding_is_recovered_as_durable_review(
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
    assert "missing scalar tower" in feedback
    assert len(request_ids) == 1
    assert recovered.fixup_requests == {}
    assert len(recovered.proof_review_requests) == 1
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
async def test_review_build_preempts_running_proof_certification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    proof_chapter, review_chapter = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    proof_started = asyncio.Event()
    events: list[str] = []
    proof_attempts = 0

    async def validation(
        _config: object,
        chapter: Chapter,
        *,
        workspace_root: Path | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> ValidationResult:
        nonlocal proof_attempts
        assert workspace_root == config.settings.repo
        assert on_output is not None
        if chapter.id == proof_chapter.id:
            proof_attempts += 1
            if proof_attempts == 1:
                events.append("proof-started")
                proof_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    events.append("proof-preempted")
                    raise
            events.append("proof-retried")
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
            priority=0.0,
            preemptible=True,
        )
    )
    await proof_started.wait()
    review = asyncio.create_task(
        orchestrator._build_chapters(
            (review_chapter,),
            publish_if_clean=False,
            mode="review-verification",
            stage=Stage.REVIEW,
            priority=200.0,
        )
    )

    assert (await review)[review_chapter.id].succeeded
    assert (await proof)[proof_chapter.id].succeeded
    assert events == ["proof-started", "proof-preempted", "review-built", "proof-retried"]
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
            priority=200.0,
        ),
        orchestrator._build_chapters(
            (proof_chapter,),
            publish_if_clean=False,
            mode="proof-certification",
            stage=Stage.PROVE,
            priority=0.0,
            preemptible=True,
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

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        attempts[chapter.number] += 1
        order.append((chapter.number, rerun))
        if chapter.number == 1:
            return FormalizeOutcome(False)
        await orchestrator.state.set_task(
            chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized"
        )
        return FormalizeOutcome(True)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert not await orchestrator._formalize_all()
    assert attempts == {1: 1, 2: 1}
    assert order == [(1, False), (2, False)]
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

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        nonlocal healthy_finished
        assert not rerun
        if chapter.number == 1:
            return FormalizeOutcome(False)
        await asyncio.sleep(0.01)
        healthy_finished = True
        await orchestrator.state.set_task(
            chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized"
        )
        return FormalizeOutcome(True)

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
        return ValidationResult(False, 1, "build failed")

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
    assert review_path.read_text(encoding="utf-8") == "def afterReview := 1\n"
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
    ) -> bool:
        nonlocal healthy_finished
        assert not rerun
        reviewed.append(chapter.id)
        if chapter.id == first.id:
            await third_started.wait()
            return False
        if chapter.id == third.id:
            third_started.set()
            await asyncio.sleep(0.01)
            healthy_finished = True
            return True
        raise AssertionError("a dependent of the failed review must not start")

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> bool:
        assert defer_review
        nonlocal healthy_proved
        assert chapter.id == third.id
        healthy_proved = True
        return True

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert not await orchestrator._review_tree(prove=True)
    assert healthy_finished
    assert healthy_proved
    assert set(reviewed) == {first.id, third.id}
    assert state.task(first.id, Stage.REVIEW).status == TaskStatus.FAILED
    assert state.task(second.id, Stage.REVIEW).status == TaskStatus.BLOCKED
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_is_capped_at_five_edit_rebuild_cycles(
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
    ) -> ReviewOutcome:
        del feedback
        nonlocal reviews
        assert rerun == (reviews > 0)
        reviews += 1
        return ReviewOutcome(True, True)

    async def review_build(_chapter: Chapter) -> dict[str, str]:
        nonlocal rebuilds
        rebuilds += 1
        return {}

    monkeypatch.setattr(orchestrator, "_review_once", changed_review)
    monkeypatch.setattr(orchestrator, "_review_build", review_build)

    assert await orchestrator._review_until_clean()
    assert reviews == 5
    assert rebuilds == 6
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_failure_consumes_a_review_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    outcomes = iter((ReviewOutcome(False, False, complete=False),))

    async def clean(*_args: object, **_kwargs: object) -> dict[str, str]:
        return {}

    async def review(
        _chapter: Chapter, *, rerun: bool = False, feedback: str = ""
    ) -> ReviewOutcome:
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


def test_proof_feedback_is_bounded_and_retains_latest_diagnostics() -> None:
    feedback = scheduler_module._bounded_proof_feedback(("old" * 20_000, "latest diagnostic"))

    assert len(feedback) == scheduler_module.PROOF_FEEDBACK_MAX_CHARS
    assert "older proof feedback omitted" in feedback
    assert feedback.endswith("latest diagnostic")


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
    fake = FakeExecutor(orchestrator.state, [result(changed=False, placeholders=0)])
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
        return await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )

    monkeypatch.setattr(fake, "run", tracked_agent)
    orchestrator.executor = fake

    assert await orchestrator._prove(chapter)
    assert events[:2] == ["build", "agent"]
    assert events.count("build") == 1
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
    assert proof.detail == "current sources failed coordinator build refresh"
    assert proof.rounds == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_noop_proof_cannot_reuse_failed_certification(
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
    assert builds == 2
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert proof.status == TaskStatus.FAILED
    assert proof.detail == "proof pass stalled with 0 placeholders"
    assert all(run.validation is not None for run in proof.runs)
    assert [run.validation["succeeded"] for run in proof.runs if run.validation is not None] == [
        False,
        False,
        False,
    ]
    final_validation = proof.runs[-1].validation
    assert final_validation is not None
    assert final_validation["output"] == ("unchanged proof source has no clean coordinator build")
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
                failed_attempts=[failed_attempt("the statement needs another hypothesis")],
            )
        ],
    )

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
                    "changed": True,
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
                "changed": False,
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
                "changed": True,
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
                "changed": False,
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
                "changed": False,
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
    assert orchestrator.state.task(consumer.id, Stage.PROVE).status == TaskStatus.BLOCKED
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
            "changed": False,
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

    async def review_again(*_args: object, **_kwargs: object) -> ReviewOutcome:
        return ReviewOutcome(True, False)

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
    ) -> bool:
        assert not rerun
        if chapter.id == second.id:
            assert orchestrator.state.task(second.id, Stage.REVIEW).status == TaskStatus.RUNNING
            await upstream_proof_started.wait()
            events.append(f"review:{chapter.id}")
            finish_upstream_proof.set()
        else:
            events.append(f"review:{chapter.id}")
        return True

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> bool:
        assert defer_review
        events.append(f"prove:{chapter.id}")
        if chapter.id == first.id:
            upstream_proof_started.set()
            await finish_upstream_proof.wait()
        return True

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
    ) -> bool:
        assert chapter.id == first.id
        upstream_review_started.set()
        await release_upstream_review.wait()
        return True

    async def prove(chapter: Chapter, *, defer_review: bool = False) -> bool:
        assert defer_review
        proofs_started.append(chapter.id)
        if {second.id, third.id}.issubset(proofs_started):
            descendant_proofs_started.set()
        return True

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
    ) -> bool:
        assert chapter.id == first.id
        rereview_started.set()
        await release_rereview.wait()
        return True

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
    ) -> bool:
        nonlocal reviews
        assert rerun == (reviews > 0)
        review_feedback.append(feedback)
        assert bool(proof_request_ids) == bool(feedback)
        reviews += 1
        return True

    async def prove(_chapter: Chapter, *, defer_review: bool = False) -> bool:
        assert defer_review
        nonlocal proofs
        proofs += 1
        if proofs > 1:
            return True
        run = await state.start_run(chapter.id, Stage.PROVE)
        await state.finish_run(
            run,
            status=TaskStatus.FAILED,
            report={
                "issues": ["statement needs a hypothesis"],
                "failed_attempts": [failed_attempt("statement needs a hypothesis")],
            },
        )
        return False

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
async def test_prove_retries_receive_a_cumulative_attempt_ledger(
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
    assert "Proof attempt 1:" in feedbacks[2]
    assert "Proof attempt 2:" in feedbacks[2]
    assert "tried route one" in feedbacks[2]
    assert "tried route two" in feedbacks[2]
    assert "Proof attempt 3:" in feedbacks[3]
    assert "tried route three" in feedbacks[3]
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

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        assert not rerun
        events.append(f"start:{chapter.book_id}")
        await asyncio.sleep(0)
        events.append(f"end:{chapter.book_id}")
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "formalized")
        return FormalizeOutcome(True)

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

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
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
