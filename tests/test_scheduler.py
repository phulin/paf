import asyncio
import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import lastlib_swarm.scheduler as scheduler_module
from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult, scope_digest
from lastlib_swarm.config import load_config
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.scheduler import FormalizeOutcome, Orchestrator, ReviewOutcome
from lastlib_swarm.state import RunRecord, StateStore, TaskPhase, TaskStatus, TokenUsage
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


def result(
    *,
    changed: bool,
    placeholders: int = 2,
    complete: bool = True,
    issues: list[str] | None = None,
    fixup_findings: list[dict[str, Any]] | None = None,
) -> AgentResult:
    return AgentResult(
        succeeded=True,
        exit_code=0,
        changed=changed,
        placeholders=placeholders,
        usage=TokenUsage(input_tokens=10, output_tokens=5, measured=True),
        report={
            "changed": changed,
            "complete": complete,
            "summary": "reviewed",
            "issues": issues or [],
            "fixup_findings": fixup_findings or [],
        },
    )


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
async def test_state_migrates_legacy_repair_tasks_to_fixup(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
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
    state.path.parent.mkdir(parents=True)
    state.path.write_text(json.dumps(payload), encoding="utf-8")

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    migrated = reloaded.task(chapter.id, Stage.FIXUP)
    assert migrated.stage == "fixup"
    assert migrated.phase == TaskPhase.IDLE
    assert migrated.runs[0].stage == "fixup"
    assert not reloaded.coordinator_build.active
    assert reloaded.database_path.is_file()
    assert (config.settings.state_dir / "state.legacy-v6.json").is_file()
    hot = json.loads(reloaded.path.read_text(encoding="utf-8"))
    assert "runs" not in hot["tasks"][f"{chapter.id}:fixup"]
    assert reloaded.snapshot()["tasks"][f"{chapter.id}:fixup"]["runs"][0]["id"] == ("legacy-run")


@pytest.mark.asyncio
async def test_state_persists_fixup_graph(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    state.fixup_graph = {
        "algorithm": "observed-lean-imports",
        "revision": 3,
        "edges": [],
        "clean": {config.chapters[0].id: {"certificate": "abc"}},
    }
    await state.save()

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    assert reloaded.fixup_graph == state.fixup_graph
    assert reloaded.snapshot()["version"] == 9


@pytest.mark.asyncio
async def test_state_persists_durable_review_green(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_review_green((chapter.id,), True)
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")

    reloaded = StateStore(config)
    await reloaded.load_or_create()

    assert reloaded.task(chapter.id, Stage.REVIEW).review_green is True
    hot = json.loads(reloaded.path.read_text(encoding="utf-8"))
    assert hot["tasks"][f"{chapter.id}:review"]["review_green"] is True


@pytest.mark.asyncio
async def test_state_recovers_orphan_run_even_when_task_is_not_running(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.FIXUP)
    await state.set_task(chapter.id, Stage.FIXUP, TaskStatus.SUCCEEDED, "newer work completed")
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()

    recovered_run = recovered.task(chapter.id, Stage.FIXUP).runs[-1]
    assert recovered_run.id == run.id
    assert recovered_run.status == TaskStatus.FAILED
    assert recovered_run.finished_at is not None
    assert recovered.agent_summary()["active"] == 0


@pytest.mark.asyncio
async def test_interrupted_review_does_not_clear_durable_green(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_review_green((chapter.id,), True)
    await state.start_run(chapter.id, Stage.REVIEW)

    recovered = StateStore(config)
    await recovered.load_or_create()
    task = recovered.task(chapter.id, Stage.REVIEW)

    assert task.status == TaskStatus.SUCCEEDED
    assert task.phase == TaskPhase.IDLE
    assert task.review_green is True
    assert task.detail == "durable review remains green after restart"


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

    hot = json.loads(state.path.read_text(encoding="utf-8"))
    task = hot["tasks"][f"{config.chapters[0].id}:formalize"]
    assert hot["version"] == 9
    assert "source_issues" not in hot
    assert "runs" not in task
    assert task["run_count"] == 25
    assert state.path.stat().st_size < 10_000
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
async def test_concurrent_run_updates_coalesce_into_one_database_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    async with state.batch():
        runs = [await state.start_run(config.chapters[0].id, stage) for stage in Stage]

    calls = 0
    original = state._database.write_batch

    def tracked(
        checkpoint: dict[str, Any],
        dirty_runs: list[tuple[str, dict[str, Any]]],
        issues: list[dict[str, Any]] | None,
    ) -> None:
        nonlocal calls
        calls += 1
        original(checkpoint, dirty_runs, issues)

    monkeypatch.setattr(state._database, "write_batch", tracked)
    await asyncio.gather(
        *(state.update_run(run, pid=index) for index, run in enumerate(runs, start=1))
    )

    assert calls == 1


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

    assert recovered_task.status == TaskStatus.PENDING
    assert recovered_task.phase == TaskPhase.RECOVERING
    assert recovered_run.status == TaskStatus.FAILED
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

    ledger = json.loads(state.source_issues_path.read_text(encoding="utf-8"))
    assert ledger["version"] == 1
    assert ledger["issues"][0]["id"] == recorded.id
    assert ledger["issues"][0]["suggested_correction"] == source_issue["suggested_correction"]

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    assert reloaded.source_issues[recorded.id].sightings == 2

    reloaded.path.unlink()
    reloaded.database_path.unlink()
    ledger_only = StateStore(config)
    await ledger_only.load_or_create()
    assert ledger_only.source_issues[recorded.id].sightings == 2


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

    for chapter in config.chapters:
        directory = tmp_path / chapter.lean_root / chapter.chapter_path
        assert directory.is_dir()
    assert not list((tmp_path / "lean").rglob("*.lean"))
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_skips_an_existing_materialized_scope(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    directory = tmp_path / chapter.lean_root / chapter.chapter_path
    directory.mkdir(parents=True)
    (tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean").write_text(
        "import Book.Chapter01.Section\n", encoding="utf-8"
    )
    (directory / "Section.lean").write_text("theorem drafted : True := by sorry\n")
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [])

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert state.task(chapter.id, Stage.FORMALIZE).rounds == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_formalize_is_one_pass_and_does_not_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [result(changed=True)])

    async def forbidden_validation(*_args: object, **_kwargs: object) -> ValidationResult:
        raise AssertionError("formalization must not build")

    monkeypatch.setattr(scheduler_module, "validate", forbidden_validation)

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
    orchestrator.executor = FakeExecutor(
        state,
        [result(changed=True, complete=False, issues=["coverage audit unfinished"])],
    )

    assert not await orchestrator.run_stage(Stage.FORMALIZE)
    task = state.task(chapter.id, Stage.FORMALIZE)
    assert task.status == TaskStatus.FAILED
    assert task.detail == "formalizer reported an incomplete chapter draft"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_repeats_coordinator_build_and_hands_back_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
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
        assert stage is Stage.FIXUP
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

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert len(feedback_seen) == 1
    assert "Book/Chapter01/Section.lean:4:1: broken" in feedback_seen[0]
    assert state.task(config.chapters[0].id, Stage.FIXUP).rounds == 1
    await orchestrator.shutdown()


def with_lastlib_modules(config: PipelineConfig) -> PipelineConfig:
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
async def test_fixup_builds_in_observed_chapter_import_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
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
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    builds: list[str] = []

    async def successful_validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(chapter.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert builds == [
        second.id,
        first.id,
        second.id,
        first.id,
    ]
    assert state.fixup_graph["edges"] == [[second.id, first.id]]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_targeted_fixup_does_not_build_cleanliness_descendants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
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
    builds: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(chapter.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator._fixup_to_clean(target_ids={first.id})
    assert builds == [first.id]
    clean = orchestrator.state.fixup_graph["clean"]
    assert first.id in clean
    assert second.id not in clean
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_concurrent_fixup_requests_share_one_repair_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    calls: list[tuple[dict[str, str] | None, set[str]]] = []

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        assert isinstance(target_ids, set)
        calls.append((feedback, target_ids))
        return True

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)

    assert all(
        await asyncio.gather(
            orchestrator._request_fixup({first.id: "first"}, target_ids={first.id}),
            orchestrator._request_fixup({second.id: "second"}, target_ids={second.id}),
        )
    )
    assert calls == [({first.id: "first", second.id: "second"}, {first.id, second.id})]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_batched_fixup_requests_resolve_from_their_own_goal_closures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()

    async def partially_failed_fixup(
        _feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        assert target_ids == {first.id, second.id}
        return False

    def goals_are_clean(goal_ids: Iterable[str]) -> bool:
        return set(goal_ids) == {first.id}

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", partially_failed_fixup)
    monkeypatch.setattr(orchestrator, "_fixup_goals_are_clean", goals_are_clean)

    first_result, second_result = await asyncio.gather(
        orchestrator._request_fixup({first.id: "first"}, target_ids={first.id}),
        orchestrator._request_fixup({second.id: "second"}, target_ids={second.id}),
    )

    assert first_result is True
    assert second_result is False
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_request_is_visible_and_durable_while_another_batch_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls = 0

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_started.set()
            await release_first.wait()
        return True

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)
    first_request = asyncio.create_task(
        orchestrator._request_fixup({first.id: "first"}, target_ids={first.id})
    )
    await first_started.wait()
    second_request = asyncio.create_task(
        orchestrator._request_fixup({second.id: "second"}, target_ids={second.id})
    )
    while len(orchestrator.state.fixup_requests) < 2:
        await asyncio.sleep(0)

    queued = orchestrator.state.task(second.id, Stage.FIXUP)
    assert queued.status == TaskStatus.RUNNING
    assert queued.phase == TaskPhase.WAITING_FIXUP
    assert "queued behind the active repair batch" in queued.detail
    assert len(orchestrator.state.fixup_requests) == 2
    while True:
        persisted = json.loads(orchestrator.state.path.read_text(encoding="utf-8"))
        if len(persisted["fixup_requests"]) == 2:
            break
        await asyncio.sleep(0)
    assert len(persisted["fixup_requests"]) == 2

    release_first.set()
    assert all(await asyncio.gather(first_request, second_request))
    assert orchestrator.state.fixup_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_interrupted_fixup_request_is_restored_from_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    calls: list[tuple[dict[str, str] | None, set[str]]] = []

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        assert isinstance(target_ids, set)
        calls.append((feedback, target_ids))
        return True

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)
    futures = await orchestrator._recover_fixup_requests()

    assert all(await asyncio.gather(*futures))
    assert calls == [({chapter.id: "missing scalar tower"}, {chapter.id})]
    assert request_id not in recovered.fixup_requests
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_unqueued_proof_finding_is_recovered_from_run_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    await state.load_or_create()
    run = await state.start_run(chapter.id, Stage.PROVE)
    await state.finish_run(
        run,
        status=TaskStatus.SUCCEEDED,
        report={
            "fixup_findings": [
                {
                    "description": "add the missing scalar tower",
                    "owner_paths": ["lean/Book/Chapter01.lean"],
                }
            ]
        },
    )
    await state.set_review_green((chapter.id,), False)
    await state.close()

    recovered = StateStore(config)
    orchestrator = Orchestrator(config, recovered)
    await orchestrator.prepare()
    calls: list[dict[str, str] | None] = []

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        calls.append(feedback)
        assert target_ids == {chapter.id}
        return True

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)
    futures = await orchestrator._recover_fixup_requests()

    assert all(await asyncio.gather(*futures))
    assert len(calls) == 1
    assert calls[0] is not None
    assert "missing scalar tower" in calls[0][chapter.id]
    assert recovered.fixup_requests == {}
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_clean_review_verification_bypasses_active_fixup_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    repair_started = asyncio.Event()
    release_repair = asyncio.Event()
    calls: list[tuple[dict[str, str] | None, set[str], dict[str, Stage]]] = []

    async def converge(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
        verification_stages: dict[str, Stage] | None = None,
    ) -> bool:
        assert isinstance(target_ids, set)
        calls.append((feedback, target_ids, dict(verification_stages or {})))
        if feedback:
            repair_started.set()
            await release_repair.wait()
        return True

    monkeypatch.setattr(orchestrator, "_fixup_to_clean", converge)
    repair = asyncio.create_task(
        orchestrator._request_fixup({first.id: "repair first"}, target_ids={first.id})
    )
    await repair_started.wait()

    assert await orchestrator._request_clean_build(target_ids={second.id}, stage=Stage.REVIEW)
    assert not repair.done()
    assert calls[-1] == (None, {second.id}, {second.id: Stage.REVIEW})

    release_repair.set()
    assert await repair
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
async def test_fixup_rescans_new_import_before_rebuilding_edited_chapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    events: list[str] = []
    first_failed = False

    class ImportingExecutor(FakeExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            assert chapter.id == first.id
            assert stage is Stage.FIXUP
            assert workspace_root is not None
            assert "broken" in feedback
            events.append(f"agent:{chapter.id}")
            (workspace_root / "lean" / "Book" / "Chapter01.lean").write_text(
                "import LastLib.Book.Chapter02\ndef chapter1 := 1\n",
                encoding="utf-8",
            )
            agent = result(changed=True)
            await state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED,
                changed=True,
                placeholders=agent.placeholders,
                report=agent.report,
                usage=agent.usage,
            )
            return agent

    orchestrator.executor = ImportingExecutor(state, [])

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        nonlocal first_failed
        events.append(f"build:{chapter.id}")
        if chapter.id == first.id and not first_failed:
            first_failed = True
            return ValidationResult(
                False,
                1,
                "error: Book/Chapter01.lean:1:1: broken",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert events[:4] == [
        f"build:{first.id}",
        f"agent:{first.id}",
        f"build:{second.id}",
        f"build:{first.id}",
    ]
    assert state.fixup_graph["edges"] == [[second.id, first.id]]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_runs_dependency_ready_agent_frontier_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.isolation.name = "fuse-overlay"
    fixed: set[str] = set()
    active = 0
    maximum_active = 0
    wave_started = asyncio.Event()

    class FixingExecutor(FakeExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            nonlocal active, maximum_active
            assert stage is Stage.FIXUP
            assert workspace_root is not None
            assert "broken" in feedback
            active += 1
            maximum_active = max(maximum_active, active)
            if active == len(config.chapters):
                wave_started.set()
            await wave_started.wait()
            fixed.add(chapter.id)
            path = workspace_root / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean"
            path.write_text(path.read_text(encoding="utf-8") + "-- fixed\n", encoding="utf-8")
            agent = result(changed=True)
            await state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED,
                changed=True,
                placeholders=agent.placeholders,
                report=agent.report,
                usage=agent.usage,
            )
            active -= 1
            return agent

    orchestrator.executor = FixingExecutor(state, [])

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        if chapter.id not in fixed:
            return ValidationResult(
                False,
                1,
                f"error: Book/Chapter{chapter.number:02d}.lean:1:1: broken",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert maximum_active == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_does_not_launch_agent_before_observed_predecessor_is_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def chapter1 := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef chapter2 := 2\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.isolation.name = "fuse-overlay"
    fixed: set[str] = set()
    agent_order: list[str] = []

    class FixingExecutor(FakeExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            assert stage is Stage.FIXUP
            assert workspace_root is not None
            assert "broken" in feedback
            agent_order.append(chapter.id)
            fixed.add(chapter.id)
            path = workspace_root / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean"
            path.write_text(path.read_text(encoding="utf-8") + "-- fixed\n", encoding="utf-8")
            agent = result(changed=True)
            await state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED,
                changed=True,
                placeholders=agent.placeholders,
                report=agent.report,
                usage=agent.usage,
            )
            return agent

    orchestrator.executor = FixingExecutor(state, [])

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        if chapter.id not in fixed:
            return ValidationResult(
                False,
                1,
                f"error: Book/Chapter{chapter.number:02d}.lean:1:1: broken",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert agent_order == [first.id, second.id]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_unlocks_descendant_before_slow_independent_agent_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2]")
    book_text = (tmp_path / "books" / "book.md").read_text(encoding="utf-8")
    (tmp_path / "books" / "book.md").write_text(
        book_text + "\n## 3. Third chapter\n",
        encoding="utf-8",
    )
    config_text = config_path.read_text(encoding="utf-8").replace(
        "chapters = [1, 2]",
        "chapters = [1, 2, 3]",
    )
    config_path.write_text(config_text, encoding="utf-8")
    config = with_lastlib_modules(load_config(config_path))
    first, second, third = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    (source_root / "Chapter01.lean").write_text("def chapter1 := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text("def chapter2 := 2\n", encoding="utf-8")
    (source_root / "Chapter03.lean").write_text(
        "import LastLib.Book.Chapter01\ndef chapter3 := 3\n",
        encoding="utf-8",
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.isolation.name = "fuse-overlay"
    fixed: set[str] = set()
    slow_release = asyncio.Event()
    descendant_started = asyncio.Event()
    slow_finished = False

    class OpportunisticExecutor(FakeExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            nonlocal slow_finished
            assert stage is Stage.FIXUP
            assert workspace_root is not None
            assert "broken" in feedback
            if chapter.id == second.id:
                await slow_release.wait()
                slow_finished = True
            elif chapter.id == third.id:
                assert not slow_finished
                assert state.task(first.id, Stage.FIXUP).status == TaskStatus.SUCCEEDED
                assert state.task(first.id, Stage.FIXUP).phase == TaskPhase.IDLE
                descendant_started.set()
                slow_release.set()
            fixed.add(chapter.id)
            path = workspace_root / "lean" / "Book" / f"Chapter{chapter.number:02d}.lean"
            path.write_text(path.read_text(encoding="utf-8") + "-- fixed\n", encoding="utf-8")
            agent = result(changed=True)
            await state.finish_run(
                run,
                status=TaskStatus.SUCCEEDED,
                changed=True,
                placeholders=agent.placeholders,
                report=agent.report,
                usage=agent.usage,
            )
            return agent

    orchestrator.executor = OpportunisticExecutor(state, [])

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        if chapter.id not in fixed:
            return ValidationResult(
                False,
                1,
                f"error: Book/Chapter{chapter.number:02d}.lean:1:1: broken",
            )
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert descendant_started.is_set()
    assert slow_finished
    assert state.fixup_graph["edges"] == [[first.id, third.id]]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_reuses_valid_persisted_build_certificates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    builds: list[str] = []

    async def successful_validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(chapter.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)
    first_state = StateStore(config)
    first = Orchestrator(config, first_state)
    await first.prepare()
    assert await first.run_stage(Stage.FIXUP)
    await first.shutdown()
    assert len(builds) == 4

    builds.clear()
    second_state = StateStore(config)
    second = Orchestrator(config, second_state)
    await second.prepare()
    assert await second.run_stage(Stage.FIXUP)

    assert builds == [chapter.id for chapter in config.chapters]
    await second.shutdown()


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
async def test_capacity_exhaustion_requeues_formalizer_at_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    attempts: dict[int, int] = {1: 0, 2: 0}
    order: list[tuple[int, bool]] = []

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        attempts[chapter.number] += 1
        order.append((chapter.number, rerun))
        if chapter.number == 1 and attempts[chapter.number] == 1:
            return FormalizeOutcome(False, capacity_deferred=True)
        return FormalizeOutcome(True)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert await orchestrator._formalize_all()
    assert attempts == {1: 2, 2: 1}
    assert order == [(1, False), (2, False), (1, True)]


@pytest.mark.asyncio
async def test_formalizer_failure_does_not_cancel_healthy_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    healthy_finished = False

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        nonlocal healthy_finished
        assert not rerun
        if chapter.number == 1:
            return FormalizeOutcome(False)
        await asyncio.sleep(0.01)
        healthy_finished = True
        return FormalizeOutcome(True)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert not await orchestrator._formalize_all()
    assert healthy_finished


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
async def test_coordinator_build_uses_build_phases_without_counting_agents(
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
        on_output(f"building {chapter.id}\n")
        on_output(f"error: Book/Chapter{chapter.number:02d}.lean:1:1: broken\n")
        if chapter.number == 1:
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
        state.task(chapter.id, Stage.FIXUP).phase == TaskPhase.BUILDING
        for chapter in config.chapters
    )

    release_validation.set()
    assert all(result.succeeded for result in (await build).values())
    assert not state.coordinator_build.active
    assert state.coordinator_build.completed == 2
    assert state.coordinator_build.output_tail == [
        "error: Book/Chapter01.lean:1:1: broken",
        f"$ {config.chapters[1].build_command}",
        "building book/chapter-02",
        "error: Book/Chapter02.lean:1:1: broken",
    ]
    assert state.coordinator_build.error_count == 2
    assert all(
        state.task(chapter.id, Stage.FIXUP).phase == TaskPhase.IDLE
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
        if chapter.number == 2:
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
async def test_agent_limiter_distinguishes_live_and_queued_runs(tmp_path: Path) -> None:
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
    assert state.task(config.chapters[0].id, Stage.FORMALIZE).phase == TaskPhase.AGENT
    assert state.task(config.chapters[1].id, Stage.FORMALIZE).phase == TaskPhase.QUEUED

    release_first.set()
    await asyncio.gather(first, second)
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_changed_review_is_rebuilt_fixed_and_reviewed_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
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
            result(changed=False),
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
        if stage is Stage.FIXUP:
            assert "unknown declaration" in feedback
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
    assert stages_seen == [Stage.REVIEW, Stage.FIXUP, Stage.REVIEW]
    assert review_path.read_text(encoding="utf-8") == "def afterReview := 1\n"
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_no_change_review_stops_after_one_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        state,
        [
            result(
                changed=False,
                issues=["statement needs a hypothesis"],
                fixup_findings=[
                    {
                        "description": "statement needs a hypothesis",
                        "owner_paths": ["lean/Book/Chapter01.lean"],
                    }
                ],
            ),
            result(changed=True),
        ],
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator._review_until_clean()
    review_task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert review_task.rounds == 1
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
    assert review_task.phase == TaskPhase.VERIFICATION_QUEUED
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
async def test_incomplete_review_routes_fixup_and_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    reviews = iter(
        (
            ReviewOutcome(
                True,
                True,
                {chapter.id: "repair the remaining statement interface"},
                complete=False,
            ),
            ReviewOutcome(True, False, {}, complete=True),
        )
    )
    fixups: list[tuple[dict[str, str] | None, object]] = []
    review_calls = 0

    async def review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        nonlocal review_calls
        outcome = next(reviews)
        assert rerun == (review_calls > 0)
        review_calls += 1
        return outcome

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
        verification_stages: object = None,
    ) -> bool:
        del verification_stages
        fixups.append((feedback, target_ids))
        state.fixup_graph["clean"] = {chapter.id: {"certificate": "test-green-certificate"}}
        return True

    monkeypatch.setattr(orchestrator, "_review_once", review)
    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)

    assert await orchestrator._review_until_clean()
    assert fixups == [
        (None, {chapter.id}),
        ({chapter.id: "repair the remaining statement interface"}, {chapter.id}),
    ]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_failure_quarantines_branch_without_cancelling_unrelated_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = with_lastlib_modules(load_config(project))
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

    async def prove(chapter: Chapter) -> bool:
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
async def test_fixup_failure_does_not_cancel_independent_fixup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    config = replace(
        config,
        stages=config.stages | {Stage.FIXUP: replace(config.stages[Stage.FIXUP], max_rounds=1)},
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n",
            encoding="utf-8",
        )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    failed_agent = replace(result(changed=False), succeeded=False, error="local agent failure")
    orchestrator.executor = FakeExecutor(state, [failed_agent, result(changed=False)])

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert not await orchestrator._fixup_to_clean(
        {first.id: "repair first", second.id: "repair second"},
        target_ids={first.id, second.id},
    )
    assert state.task(first.id, Stage.FIXUP).status == TaskStatus.FAILED
    assert state.task(second.id, Stage.FIXUP).status == TaskStatus.SUCCEEDED
    assert not orchestrator.executor.results
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_review_is_capped_at_five_edit_rebuild_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    reviews = 0
    rebuilds = 0

    async def changed_review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        nonlocal reviews
        assert rerun == (reviews > 0)
        reviews += 1
        return ReviewOutcome(True, True, {})

    async def clean_target(
        _feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
        verification_stages: object = None,
    ) -> bool:
        del verification_stages
        nonlocal rebuilds
        assert target_ids == {config.chapters[0].id}
        rebuilds += 1
        orchestrator.state.fixup_graph["clean"] = {
            config.chapters[0].id: {"certificate": "test-green-certificate"}
        }
        return True

    monkeypatch.setattr(orchestrator, "_review_once", changed_review)
    monkeypatch.setattr(orchestrator, "_fixup_to_clean", clean_target)

    assert await orchestrator._review_until_clean()
    assert reviews == 5
    assert rebuilds == 6
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_deferred_review_does_not_consume_a_review_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    outcomes = iter(
        (
            ReviewOutcome(False, False, {}, complete=False, capacity_deferred=True),
            ReviewOutcome(True, False, {}),
        )
    )

    async def clean(*_args: object, **_kwargs: object) -> bool:
        return True

    async def review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        del rerun
        return next(outcomes)

    monkeypatch.setattr(orchestrator, "_request_clean_build", clean)
    monkeypatch.setattr(orchestrator, "_review_once", review)
    rounds = {chapter.id: 0}

    assert await orchestrator._review_chapter_to_clean(chapter, rounds)
    assert rounds[chapter.id] == 1
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fixup_cancellation_releases_shared_source_lock(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, isolation="shared"))
    chapter = config.chapters[0]
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    started = asyncio.Event()

    class BlockingExecutor(CodexExecutor):
        async def run(
            self,
            chapter: Chapter,
            stage: Stage,
            run: RunRecord,
            *,
            feedback: str = "",
            workspace_root: Path | None = None,
        ) -> AgentResult:
            del chapter, stage, run, feedback, workspace_root
            started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    orchestrator.executor = BlockingExecutor(config, orchestrator.state)
    task = asyncio.create_task(
        orchestrator._fixup_to_clean(
            {chapter.id: "repair"}, target_ids={chapter.id}
        )
    )
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not orchestrator.source_lock.locked()
    assert orchestrator.agent_slots.available == orchestrator.agent_slots.capacity
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_deferred_fixup_does_not_consume_the_repair_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        settings=replace(config.settings, isolation="shared"),
        stages={
            **config.stages,
            Stage.FIXUP: replace(config.stages[Stage.FIXUP], max_rounds=1),
        },
    )
    chapter = config.chapters[0]
    capacity = replace(
        result(changed=False),
        succeeded=False,
        exit_code=1,
        capacity_exhausted=True,
    )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(
        orchestrator.state, [capacity, result(changed=False)]
    )

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)

    assert await orchestrator._fixup_to_clean(
        {chapter.id: "repair"}, target_ids={chapter.id}
    )
    assert len(orchestrator.executor.feedbacks) == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_capacity_deferred_proof_does_not_consume_a_proof_round(
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
    success = result(changed=False, placeholders=0)
    agents = iter((capacity, success))
    calls = 0

    async def attempt(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return scheduler_module.Attempt(
            next(agents), ValidationResult(True, 0, "ok"), run
        )

    monkeypatch.setattr(orchestrator, "_attempt", attempt)

    assert await orchestrator._prove(chapter)
    assert calls == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_pipeline_reviews_each_chapter_as_soon_as_its_build_is_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
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
    events: list[str] = []

    async def formalize_all() -> bool:
        return True

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        events.append(f"build:{chapter.id}")
        return ValidationResult(True, 0, "ok")

    async def review(chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        events.append(f"review:{chapter.id}")
        return ReviewOutcome(True, False, {})

    async def prove(chapter: Chapter) -> bool:
        events.append(f"prove:{chapter.id}")
        return True

    monkeypatch.setattr(orchestrator, "_formalize_all", formalize_all)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    monkeypatch.setattr(orchestrator, "_review_once", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert await orchestrator.run_pipeline()
    assert [event for event in events if event.startswith("build:")] == [
        f"build:{first.id}",
        f"build:{second.id}",
    ]
    assert events.index(f"build:{first.id}") < events.index(f"review:{first.id}")
    assert events.index(f"review:{first.id}") < events.index(f"build:{second.id}")
    assert events.index(f"build:{second.id}") < events.index(f"review:{second.id}")
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_pipeline_quarantines_failed_formalization_but_reviews_independent_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    for chapter in config.chapters:
        (source_root / f"Chapter{chapter.number:02d}.lean").write_text(
            f"def chapter{chapter.number} := {chapter.number}\n", encoding="utf-8"
        )
    orchestrator = Orchestrator(config, StateStore(config))
    await orchestrator.prepare()
    reviewed: list[str] = []
    proved: list[str] = []

    async def formalize_all() -> bool:
        await orchestrator.state.set_task(
            first.id, Stage.FORMALIZE, TaskStatus.FAILED, "draft failed"
        )
        await orchestrator.state.set_task(
            second.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "drafted"
        )
        return False

    async def validation(*_args: object, **_kwargs: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    async def review(chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        reviewed.append(chapter.id)
        return ReviewOutcome(True, False, {})

    async def prove(chapter: Chapter) -> bool:
        proved.append(chapter.id)
        return True

    monkeypatch.setattr(orchestrator, "_formalize_all", formalize_all)
    monkeypatch.setattr(scheduler_module, "validate", validation)
    monkeypatch.setattr(orchestrator, "_review_once", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    assert not await orchestrator.run_pipeline()
    assert reviewed == [second.id]
    assert proved == [second.id]
    assert orchestrator.state.task(first.id, Stage.REVIEW).status == TaskStatus.FAILED
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
    orchestrator.executor = FakeExecutor(
        orchestrator.state, [result(changed=False, placeholders=0)]
    )

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
    feedback = scheduler_module._bounded_proof_feedback(
        ("old" * 20_000, "latest diagnostic")
    )

    assert len(feedback) == scheduler_module.PROOF_FEEDBACK_MAX_CHARS
    assert "older proof feedback omitted" in feedback
    assert feedback.endswith("latest diagnostic")


@pytest.mark.asyncio
async def test_review_restart_seeds_proofs_from_durable_green_without_rebuilding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
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
    builds: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(chapter.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    first_run = Orchestrator(config, StateStore(config))
    await first_run.prepare()
    assert await first_run._fixup_to_clean(target_ids={first.id, second.id})
    first_reviews: list[str] = []

    async def first_review(chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        first_reviews.append(chapter.id)
        return ReviewOutcome(True, False, {})

    monkeypatch.setattr(first_run, "_review_once", first_review)
    assert await first_run._review_tree()
    assert first_reviews == [first.id, second.id]
    assert all(
        first_run.state.task(chapter.id, Stage.REVIEW).review_green is True
        for chapter in config.chapters
    )
    await first_run.shutdown()
    assert builds == [first.id, second.id]

    builds.clear()
    proofs: list[str] = []
    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    async def review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        raise AssertionError("durable green reviews must not rerun")

    async def prove(chapter: Chapter) -> bool:
        proofs.append(chapter.id)
        return True

    monkeypatch.setattr(restarted, "_review_once", review)
    monkeypatch.setattr(restarted, "_prove", prove)

    assert await restarted._review_tree(prove=True)
    assert builds == []
    assert set(proofs) == {first.id, second.id}
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_restart_after_proof_adds_lemma_preserves_review_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1]"))
    )
    chapter = config.chapters[0]
    source = tmp_path / "lean" / "Book" / "Chapter01.lean"
    source.parent.mkdir(parents=True)
    source.write_text("theorem target : True := by sorry\n", encoding="utf-8")
    builds: list[str] = []

    async def validation(
        _config: object,
        built: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(built.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    first_run = Orchestrator(config, StateStore(config))
    await first_run.prepare()
    assert await first_run._fixup_to_clean(target_ids={chapter.id})

    async def review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        return ReviewOutcome(True, False, {})

    monkeypatch.setattr(first_run, "_review_once", review)
    assert await first_run._review_tree()
    first_run.executor = FakeExecutor(
        first_run.state,
        [result(changed=True, placeholders=0)],
    )
    original_run = first_run.executor.run

    async def add_helper(
        attempted: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        assert stage is Stage.PROVE
        assert workspace_root is not None
        target = workspace_root / "lean" / "Book" / "Chapter01.lean"
        target.write_text(
            "theorem target : True := by trivial\n"
            "theorem helper_added_by_proof : True := by trivial\n",
            encoding="utf-8",
        )
        return await original_run(
            attempted,
            stage,
            run,
            feedback=feedback,
            workspace_root=workspace_root,
        )

    monkeypatch.setattr(first_run.executor, "run", add_helper)
    assert await first_run._prove(chapter)
    proof_digest = first_run.state.task(chapter.id, Stage.PROVE).source_digest
    assert proof_digest == scope_digest(config.settings.repo, chapter)
    await first_run.shutdown()

    builds.clear()
    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    async def forbidden_review(*_args: object, **_kwargs: object) -> ReviewOutcome:
        raise AssertionError("durable green review must not rerun")

    async def forbidden_agent(*_args: object, **_kwargs: object) -> AgentResult:
        raise AssertionError("validated proof must not rerun an agent")

    monkeypatch.setattr(restarted, "_review_once", forbidden_review)
    monkeypatch.setattr(restarted.executor, "run", forbidden_agent)

    assert await restarted._review_tree(prove=True)
    assert builds == []
    assert restarted.state.task(chapter.id, Stage.REVIEW).review_green is True
    assert restarted.state.task(chapter.id, Stage.PROVE).source_digest == proof_digest
    assert "helper_added_by_proof" in source.read_text(encoding="utf-8")
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_stale_build_is_refreshed_before_proof_agent_with_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1]"))
    )
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
    assert events.count("build") == 2
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_standalone_proof_fixup_finding_clears_durable_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1]"))
    )
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
    assert await orchestrator._fixup_to_clean(target_ids={chapter.id})
    await orchestrator._complete_review(chapter, "reviewed")
    orchestrator.executor = FakeExecutor(
        orchestrator.state,
        [
            result(
                changed=False,
                fixup_findings=[
                    {
                        "description": "the statement needs another hypothesis",
                        "owner_paths": ["lean/Book/Chapter01.lean"],
                    }
                ],
            )
        ],
    )

    assert not await orchestrator._prove(chapter)
    review = orchestrator.state.task(chapter.id, Stage.REVIEW)
    proof = orchestrator.state.task(chapter.id, Stage.PROVE)
    assert review.review_green is False
    assert review.status == TaskStatus.PENDING
    assert proof.status == TaskStatus.PENDING
    assert proof.source_digest is None
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_legacy_reset_recovers_green_review_and_proof_without_agents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1]"))
    )
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
        report={"complete": True, "fixup_findings": []},
        validation={"succeeded": True, "output": "ok"},
    )
    proof = await state.start_run(chapter.id, Stage.PROVE)
    await state.finish_run(
        proof,
        status=TaskStatus.SUCCEEDED,
        changed=True,
        placeholders=0,
        report={"complete": True, "fixup_findings": []},
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
    state.task(chapter.id, Stage.REVIEW).review_green = None
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

    async def forbidden_review(*_args: object, **_kwargs: object) -> ReviewOutcome:
        raise AssertionError("historical green review must not rerun")

    async def forbidden_agent(*_args: object, **_kwargs: object) -> AgentResult:
        raise AssertionError("current placeholder-free proof must not rerun an agent")

    monkeypatch.setattr(restarted, "_review_once", forbidden_review)
    monkeypatch.setattr(restarted.executor, "run", forbidden_agent)

    assert await restarted._review_tree(prove=True)
    assert builds == [chapter.id]
    assert restarted.state.task(chapter.id, Stage.REVIEW).review_green is True
    proved = restarted.state.task(chapter.id, Stage.PROVE)
    assert proved.status == TaskStatus.SUCCEEDED
    assert proved.source_digest == scope_digest(config.settings.repo, chapter)
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_statement_repair_invalidates_review_and_proof_closure(tmp_path: Path) -> None:
    config = with_lastlib_modules(
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
        await orchestrator.state.set_review_green((chapter.id,), True)
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

    invalidated = await orchestrator._invalidate_review_closure(
        {first.id}, detail="upstream statement changed"
    )

    assert invalidated == {first.id, second.id}
    for chapter in config.chapters:
        review = orchestrator.state.task(chapter.id, Stage.REVIEW)
        proof = orchestrator.state.task(chapter.id, Stage.PROVE)
        assert review.status == TaskStatus.PENDING
        assert review.phase == TaskPhase.RECOVERING
        assert review.review_green is False
        assert proof.status == TaskStatus.PENDING
        assert proof.phase == TaskPhase.WAITING_PREREQUISITES
        assert proof.source_digest is None
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

    await orchestrator._invalidate_review_closure(
        {chapter.id}, detail="statement changed while review was running"
    )

    assert not await orchestrator._complete_review(
        chapter,
        "obsolete review finished",
        expected_generation=started_generation,
    )
    review = orchestrator.state.task(chapter.id, Stage.REVIEW)
    assert review.review_green is False
    assert review.status == TaskStatus.PENDING
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_changed_source_revalidates_proofs_without_clearing_review_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
        load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    )
    first, second = config.chapters
    source_root = tmp_path / "lean" / "Book"
    source_root.mkdir(parents=True)
    first_path = source_root / "Chapter01.lean"
    first_path.write_text("def first := 1\n", encoding="utf-8")
    (source_root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\ndef second := first + 1\n",
        encoding="utf-8",
    )
    builds: list[str] = []

    async def validation(
        _config: object,
        chapter: Chapter,
        **_kwargs: object,
    ) -> ValidationResult:
        builds.append(chapter.id)
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", validation)
    first_run = Orchestrator(config, StateStore(config))
    await first_run.prepare()
    assert await first_run._fixup_to_clean(target_ids={first.id, second.id})

    async def first_review(_chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        return ReviewOutcome(True, False, {})

    monkeypatch.setattr(first_run, "_review_once", first_review)
    assert await first_run._review_tree()
    await first_run.state.set_task(
        second.id,
        Stage.PROVE,
        TaskStatus.SUCCEEDED,
        "no placeholders and chapter elaborates",
        source_digest=scope_digest(config.settings.repo, second),
    )
    await first_run.shutdown()

    first_path.write_text("def first := 2\n", encoding="utf-8")
    builds.clear()
    reviews: list[str] = []
    restarted = Orchestrator(config, StateStore(config))
    await restarted.prepare()

    async def review(chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        assert not rerun
        reviews.append(chapter.id)
        return ReviewOutcome(True, False, {})

    monkeypatch.setattr(restarted, "_review_once", review)

    assert await restarted._review_tree(prove=True)
    assert set(builds) == {first.id, second.id}
    assert reviews == []
    assert all(
        restarted.state.task(chapter.id, Stage.REVIEW).review_green is True
        for chapter in config.chapters
    )
    assert restarted.state.task(second.id, Stage.PROVE).status == TaskStatus.SUCCEEDED
    await restarted.shutdown()


@pytest.mark.asyncio
async def test_upstream_proof_starts_before_downstream_review_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = with_lastlib_modules(
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
            assert orchestrator.state.task(second.id, Stage.REVIEW).phase == (
                TaskPhase.WAITING_BUILD
            )
            await upstream_proof_started.wait()
            events.append(f"review:{chapter.id}")
            finish_upstream_proof.set()
        else:
            events.append(f"review:{chapter.id}")
        return True

    async def prove(chapter: Chapter) -> bool:
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
async def test_proof_fixup_requeues_only_its_review_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    reviews = 0
    proofs = 0
    fixups = 0

    async def review(
        _chapter: Chapter,
        _rounds_used: dict[str, int],
        *,
        rerun: bool = False,
    ) -> bool:
        nonlocal reviews
        assert rerun == (reviews > 0)
        reviews += 1
        return True

    async def prove(_chapter: Chapter) -> bool:
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
                "fixup_findings": [
                    {
                        "description": "statement needs a hypothesis",
                        "owner_paths": ["lean/Book/Chapter01.lean"],
                    }
                ],
            },
        )
        return False

    async def fixup(
        feedback: dict[str, str] | None = None,
        *,
        target_ids: object = None,
    ) -> bool:
        nonlocal fixups
        assert feedback is not None
        assert "statement needs a hypothesis" in feedback[chapter.id]
        assert target_ids == {chapter.id}
        assert state.task(chapter.id, Stage.REVIEW).review_green is False
        assert state.task(chapter.id, Stage.PROVE).status == TaskStatus.PENDING
        assert state.task(chapter.id, Stage.FIXUP).status == TaskStatus.RUNNING
        assert state.task(chapter.id, Stage.FIXUP).phase == TaskPhase.WAITING_FIXUP
        fixups += 1
        return True

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)
    monkeypatch.setattr(orchestrator, "_fixup_to_clean", fixup)

    assert await orchestrator._review_tree(prove=True)
    assert (reviews, proofs, fixups) == (2, 2, 1)
    assert state.task(chapter.id, Stage.REVIEW).review_green is True
    await orchestrator.shutdown()


def test_review_findings_route_to_requested_chapter_owners(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    first, second = config.chapters

    routed = orchestrator._route_review_findings(
        first,
        {
            "fixup_findings": [
                {
                    "description": "Add the missing bridge in its actual dependency owner.",
                    "owner_paths": [
                        "lean/Book/Chapter01/Section02Consumer.lean",
                        "lean/Book/Chapter02/Section01Bridge.lean",
                    ],
                }
            ]
        },
    )

    assert set(routed) == {first.id, second.id}
    assert f"auditing `{first.id}`" in routed[second.id]
    assert "Add the missing bridge" in routed[second.id]
    assert "lean/Book/Chapter02/Section01Bridge.lean" in routed[second.id]
    assert "Chapter01/Section02Consumer.lean" not in routed[second.id]
    assert "lean/Book/Chapter01/Section02Consumer.lean" in routed[first.id]
    assert "Chapter02/Section01Bridge.lean" not in routed[first.id]


def test_unowned_review_findings_fall_back_to_reviewed_chapter(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    chapter = config.chapters[0]

    routed = orchestrator._route_review_findings(
        chapter,
        {
            "fixup_findings": [
                {
                    "description": "Repository-wide infrastructure is missing.",
                    "owner_paths": ["src/tooling.py"],
                }
            ]
        },
    )

    assert set(routed) == {chapter.id}
    assert "Repository-wide infrastructure is missing" in routed[chapter.id]


@pytest.mark.asyncio
async def test_review_is_one_no_change_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
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
async def test_formalize_runs_upstream_and_downstream_books_optimistically(
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
    events: list[str] = []

    async def formalize(chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        assert not rerun
        events.append(f"start:{chapter.book_id}")
        await asyncio.sleep(0)
        events.append(f"end:{chapter.book_id}")
        return FormalizeOutcome(True)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert events[:2] == ["start:book", "start:second"]
    assert set(events[2:]) == {"end:book", "end:second"}


@pytest.mark.asyncio
async def test_chapter_failure_cancels_and_drains_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    orchestrator = Orchestrator(config, StateStore(config))
    sibling_started = asyncio.Event()
    sibling_cleaned = asyncio.Event()

    async def formalize(chapter: Chapter) -> bool:
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
