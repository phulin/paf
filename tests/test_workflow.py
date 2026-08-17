import asyncio
from types import SimpleNamespace

import pytest

from paf.codex import ValidationResult
from paf.config import load_config
from paf.models import Stage
from paf.scheduler import FormalizeOutcome, Orchestrator
from paf.state import StateStore, TaskStatus
from tests.support import write_project


@pytest.mark.asyncio
async def test_discovery_streams_into_dependency_ready_formalization(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units
    release_second_discovery = asyncio.Event()
    first_formalized = asyncio.Event()

    async def discover(chapter, *, rerun=False):
        del rerun
        if chapter.id == second.id:
            await release_second_discovery.wait()
        dependencies = () if chapter.id == first.id else (first.id,)
        await orchestrator._persist_source_dependencies(
            chapter,
            dependencies,
            {"summary": "discovered", "issues": []},
        )
        await state.set_task(
            chapter.id,
            Stage.DISCOVER,
            TaskStatus.SUCCEEDED,
            "source dependency tree persisted",
        )
        return FormalizeOutcome(True)

    async def formalize(chapter, *, rerun=False):
        del rerun
        if chapter.id == first.id:
            first_formalized.set()
        else:
            assert state.task(first.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
        await state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.SUCCEEDED,
            "clean",
        )
        return FormalizeOutcome(True)

    monkeypatch.setattr(orchestrator, "_discover", discover)
    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    pipeline = asyncio.create_task(orchestrator._discover_and_formalize())
    await asyncio.wait_for(first_formalized.wait(), timeout=1)
    assert not release_second_discovery.is_set()
    release_second_discovery.set()

    assert await pipeline
    assert state.source_dependency_tree["dependencies"][second.id] == [first.id]


@pytest.mark.asyncio
async def test_source_dependency_tree_survives_restart(tmp_path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units

    await orchestrator._persist_source_dependencies(first, (), {"summary": "root", "issues": []})
    await orchestrator._persist_source_dependencies(
        second, (first.id,), {"summary": "dependent", "issues": []}
    )

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    assert reloaded.source_dependency_tree["edges"] == [[first.id, second.id]]
    assert reloaded.source_dependency_tree["nodes"][second.id]["dependencies"] == [first.id]


@pytest.mark.asyncio
async def test_source_dependency_cycle_drops_forward_edge_instead_of_failing(tmp_path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units

    await orchestrator._persist_source_dependencies(
        first, (second.id,), {"summary": "incorrect forward edge", "issues": []}
    )
    await orchestrator._persist_source_dependencies(
        second, (first.id,), {"summary": "valid earlier edge", "issues": []}
    )

    assert state.source_dependency_tree["edges"] == [[first.id, second.id]]
    assert state.source_dependency_tree["nodes"][first.id]["dependencies"] == []
    assert state.source_dependency_tree["nodes"][second.id]["dependencies"] == [first.id]


@pytest.mark.asyncio
async def test_rereview_does_not_wait_for_dependency_review(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units
    await orchestrator._persist_source_dependencies(first, (), {"summary": "root", "issues": []})
    await orchestrator._persist_source_dependencies(
        second, (first.id,), {"summary": "dependent", "issues": []}
    )
    await state.set_tasks((first.id, second.id), Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    state.task(second.id, Stage.REVIEW).rounds = 1
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def review(chapter, rounds_used, **kwargs):
        del rounds_used, kwargs
        if chapter.id == first.id:
            await release_first.wait()
        else:
            second_started.set()
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
        return True

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    run = asyncio.create_task(orchestrator._review_tree())
    await asyncio.wait_for(second_started.wait(), timeout=1)
    release_first.set()
    assert await run


@pytest.mark.asyncio
async def test_first_review_waits_for_dependency_review(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units
    await orchestrator._persist_source_dependencies(first, (), {"summary": "root", "issues": []})
    await orchestrator._persist_source_dependencies(
        second, (first.id,), {"summary": "dependent", "issues": []}
    )
    await state.set_tasks((first.id, second.id), Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def review(chapter, rounds_used, **kwargs):
        del rounds_used, kwargs
        if chapter.id == first.id:
            await release_first.wait()
        else:
            second_started.set()
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
        return True

    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    # Even a forced pipeline invocation must preserve ordering for a node's
    # genuinely first review.
    run = asyncio.create_task(orchestrator._review_tree(rerun=True))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(second_started.wait(), timeout=0.05)
    release_first.set()
    assert await run
    assert second_started.is_set()


@pytest.mark.asyncio
async def test_proof_release_does_not_wait_for_dependency_proof(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units
    await orchestrator._persist_source_dependencies(first, (), {"summary": "root", "issues": []})
    await orchestrator._persist_source_dependencies(
        second, (first.id,), {"summary": "dependent", "issues": []}
    )
    await state.set_tasks((first.id, second.id), Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
    await state.set_tasks((first.id, second.id), Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def prove(chapter, *, defer_review=False):
        del defer_review
        if chapter.id == first.id:
            await release_first.wait()
        else:
            second_started.set()
        await state.set_task(chapter.id, Stage.PROVE, TaskStatus.SUCCEEDED, "proved")
        return True

    monkeypatch.setattr(orchestrator, "_prove", prove)
    run = asyncio.create_task(orchestrator._review_tree(prove=True))
    await asyncio.wait_for(second_started.wait(), timeout=1)
    release_first.set()
    assert await run


@pytest.mark.asyncio
async def test_pipeline_has_no_full_stage_barriers(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    first, second = config.work_units
    release_second_formalize = asyncio.Event()
    first_review_started = asyncio.Event()

    async def discover(chapter, *, rerun=False):
        del rerun
        await orchestrator._persist_source_dependencies(
            chapter, (), {"summary": "independent", "issues": []}
        )
        await state.set_task(chapter.id, Stage.DISCOVER, TaskStatus.SUCCEEDED, "discovered")
        return FormalizeOutcome(True)

    async def formalize(chapter, *, rerun=False):
        del rerun
        if chapter.id == second.id:
            await release_second_formalize.wait()
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
        return FormalizeOutcome(True)

    async def review(chapter, rounds_used, **kwargs):
        del rounds_used, kwargs
        if chapter.id == first.id:
            first_review_started.set()
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
        return True

    async def prove(chapter, *, defer_review=False):
        del defer_review
        await state.set_task(chapter.id, Stage.PROVE, TaskStatus.SUCCEEDED, "proved")
        return True

    monkeypatch.setattr(orchestrator, "_discover", discover)
    monkeypatch.setattr(orchestrator, "_formalize", formalize)
    monkeypatch.setattr(orchestrator, "_review_chapter_to_clean", review)
    monkeypatch.setattr(orchestrator, "_prove", prove)

    pipeline = asyncio.create_task(orchestrator.run_pipeline())
    await asyncio.wait_for(first_review_started.wait(), timeout=1)
    assert not release_second_formalize.is_set()
    release_second_formalize.set()
    assert await pipeline


@pytest.mark.asyncio
async def test_formalize_retries_diagnostics_and_finishes_with_clean_build(
    tmp_path, monkeypatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    chapter = config.work_units[0]
    target = tmp_path / chapter.lean_root / f"{chapter.chapter_path}.lean"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("theorem openGoal : True := by sorry\n", encoding="utf-8")
    section = tmp_path / chapter.lean_root / chapter.chapter_path / "Section.lean"
    section.parent.mkdir(parents=True, exist_ok=True)
    section.write_text("theorem sectionGoal : True := by sorry\n", encoding="utf-8")
    builds = 0
    feedbacks = []

    async def build(*args, snapshots, **kwargs):
        nonlocal builds
        del args, kwargs
        builds += 1
        snapshots[chapter.id] = SimpleNamespace()
        if builds == 1:
            return {
                chapter.id: ValidationResult(
                    False, 1, "error: Book/Chapter01.lean:1:1: unknown identifier"
                )
            }
        return {chapter.id: ValidationResult(True, 0, "warning: declaration uses `sorry`")}

    async def attempt(_chapter, _stage, *, feedback="", **kwargs):
        del kwargs
        feedbacks.append(feedback)
        agent = SimpleNamespace(
            succeeded=True,
            capacity_exhausted=False,
            report={"complete": True},
        )
        return SimpleNamespace(
            agent=agent,
            validation=ValidationResult(True, 0, "MCP diagnostics clean except sorry"),
        )

    async def publish(_chapter, _snapshot):
        return True

    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_attempt", attempt)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)

    outcome = await orchestrator._formalize(chapter)

    assert outcome.succeeded
    assert builds == 2
    assert "unknown identifier" in feedbacks[0]
    assert state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_code", [1, 124])
async def test_formalize_retries_nonfatal_agent_failure_instead_of_failing_chapter(
    tmp_path, monkeypatch, exit_code
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    chapter = config.work_units[0]
    scope_checks = 0
    attempts = 0

    async def scope_exists(_chapter):
        nonlocal scope_checks
        scope_checks += 1
        return scope_checks >= 3

    async def attempt(_chapter, _stage, **kwargs):
        nonlocal attempts
        del kwargs
        attempts += 1
        failed = attempts == 1
        agent = SimpleNamespace(
            succeeded=not failed,
            exit_code=exit_code if failed else 0,
            capacity_exhausted=False,
            report={"complete": not failed},
        )
        return SimpleNamespace(
            agent=agent,
            validation=ValidationResult(not failed, exit_code if failed else 0, ""),
            feedback=lambda: "agent attempt failed",
        )

    async def build(*args, snapshots, **kwargs):
        del args, kwargs
        snapshots[chapter.id] = SimpleNamespace()
        return {chapter.id: ValidationResult(True, 0, "")}

    async def publish(_chapter, _snapshot):
        return True

    monkeypatch.setattr(orchestrator, "_scope_exists", scope_exists)
    monkeypatch.setattr(orchestrator, "_attempt", attempt)
    monkeypatch.setattr(orchestrator, "_build_chapters", build)
    monkeypatch.setattr(orchestrator, "_publish_validated_build", publish)

    outcome = await orchestrator._formalize(chapter)

    assert outcome.succeeded
    assert attempts == 2
    assert state.task(chapter.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
