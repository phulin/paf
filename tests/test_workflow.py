import asyncio
import json
import sqlite3
from types import SimpleNamespace

import pytest

from paf.codex import ValidationResult
from paf.config import load_config
from paf.models import Stage
from paf.scheduler import ExecutionDisposition, Orchestrator, StageOutcome
from paf.state import ChangeSet, Requirement, RequirementKind, StateStore, TaskStatus
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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_discover", discover)
    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    pipeline = asyncio.create_task(orchestrator._discover_and_formalize())
    await asyncio.wait_for(first_formalized.wait(), timeout=1)
    assert not release_second_discovery.is_set()
    release_second_discovery.set()

    assert await pipeline
    assert state.source_dependency_tree["dependencies"][second.id] == [first.id]


@pytest.mark.asyncio
async def test_waiting_owner_recovery_retries_consumer_in_same_run(tmp_path, monkeypatch) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    owner, consumer = config.work_units
    await orchestrator._persist_source_dependencies(owner, (), {"summary": "root", "issues": []})
    await orchestrator._persist_source_dependencies(
        consumer, (owner.id,), {"summary": "dependent", "issues": []}
    )
    await state.set_tasks(
        (owner.id, consumer.id), Stage.DISCOVER, TaskStatus.SUCCEEDED, "discovered"
    )
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "initially clean")
    events: list[str] = []

    async def formalize(chapter, *, rerun=False):
        del rerun
        events.append(chapter.id)
        if chapter.id == consumer.id and events.count(consumer.id) == 1:
            requirement = Requirement(
                RequirementKind.COORDINATOR_OWNER,
                owner_task_key=state.key(owner.id, Stage.FORMALIZE),
                detail="coordinator diagnostic owner",
            )
            await state.set_task(
                owner.id,
                Stage.FORMALIZE,
                TaskStatus.PENDING,
                "requeued diagnostic owner",
            )
            await state.set_task_waiting(
                consumer.id,
                Stage.FORMALIZE,
                (requirement,),
                "waiting for diagnostic owner",
            )
            return StageOutcome(ExecutionDisposition.WAITING, (requirement,))
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert await orchestrator._discover_and_formalize(discover=False)
    assert events == [consumer.id, owner.id, consumer.id]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_transient_wait_does_not_write_descendant_task_rows(tmp_path, monkeypatch) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    with (tmp_path / "books" / "book.md").open("a", encoding="utf-8") as source:
        source.write("\n## 3. Third chapter\n")
    config = load_config(config_path)
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    owner, consumer, descendant = config.work_units
    dependencies = {owner.id: (), consumer.id: (owner.id,), descendant.id: (consumer.id,)}
    for chapter in config.work_units:
        await orchestrator._persist_source_dependencies(
            chapter,
            dependencies[chapter.id],
            {"summary": "dependency graph", "issues": []},
        )
    await state.set_tasks(
        (chapter.id for chapter in config.work_units),
        Stage.DISCOVER,
        TaskStatus.SUCCEEDED,
        "discovered",
    )
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "initially clean")
    baseline_revision = state.revision

    async def formalize(chapter, *, rerun=False):
        del rerun
        if chapter.id == consumer.id:
            requirement = Requirement(
                RequirementKind.COORDINATOR_OWNER,
                owner_task_key=state.key(owner.id, Stage.FORMALIZE),
                detail="coordinator diagnostic owner",
            )
            await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.PENDING, "owner requeued")
            await state.set_task_waiting(
                consumer.id,
                Stage.FORMALIZE,
                (requirement,),
                "waiting for diagnostic owner",
            )
            return StageOutcome(ExecutionDisposition.WAITING, (requirement,))
        await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.FAILED, "owner failed")
        return StageOutcome(ExecutionDisposition.FAILED)

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert not await orchestrator._discover_and_formalize(discover=False)
    descendant_task = state.task(descendant.id, Stage.FORMALIZE)
    assert descendant_task.status == TaskStatus.PENDING
    assert state.failure_roots(descendant_task) == (state.key(owner.id, Stage.FORMALIZE),)
    assert (
        state.hot_snapshot()["tasks"][state.key(descendant.id, Stage.FORMALIZE)][
            "scheduling_status"
        ]
        == "blocked"
    )
    assert [record.task_key for record in state.shepherd_failure_records()] == [
        state.key(owner.id, Stage.FORMALIZE)
    ]
    with sqlite3.connect(state.database_path) as connection:
        descendant_writes = connection.execute(
            """
            SELECT count(*) FROM changes
            WHERE revision > ? AND entity_type = 'task' AND entity_id = ?
            """,
            (baseline_revision, state.key(descendant.id, Stage.FORMALIZE)),
        ).fetchone()[0]
    assert descendant_writes == 0
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_hot_snapshot_batches_failure_roots_without_recursive_walks(
    tmp_path, monkeypatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.work_units
    state.source_dependency_tree = {
        "dependencies": {
            first.id: [],
            second.id: [first.id],
        }
    }
    state.task(first.id, Stage.FORMALIZE).status = TaskStatus.FAILED

    expected = {key: state.failure_roots(task) for key, task in state.tasks.items()}
    calls = 0
    original = state.task_requirements

    def counted(task):
        nonlocal calls
        calls += 1
        return original(task)

    monkeypatch.setattr(state, "task_requirements", counted)
    snapshot = state.hot_snapshot()

    assert calls == 0
    for key, task in snapshot["tasks"].items():
        assert tuple(task["blocked_by"]) == expected[key]

    calls = 0
    delta = state.dashboard_delta(
        ChangeSet(revision=state.revision, work_units=frozenset({second.id}))
    )
    assert calls == 0
    assert tuple(delta["tasks"][state.key(second.id, Stage.FORMALIZE)]["blocked_by"]) == (
        state.key(first.id, Stage.FORMALIZE),
    )
    await state.close()


@pytest.mark.asyncio
async def test_dashboard_delta_does_not_reserialize_unchanged_active_activities(tmp_path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.work_units
    first_run = await state.start_run(first.id, Stage.REVIEW)
    second_run = await state.start_run(second.id, Stage.REVIEW)
    for run in (first_run, second_run):
        activity = state.activities.start(run.id, run.chapter_id, Stage.REVIEW.value)
        activity.current = f"working on {run.chapter_id}"
        state.activities.save(activity)

    delta = state.dashboard_delta(
        ChangeSet(
            revision=state.revision,
            work_units=frozenset({first.id}),
            runs=frozenset({first_run.id}),
        )
    )

    assert set(delta["active_run_ids"]) == {first_run.id, second_run.id}
    assert set(delta["activities"]) == {first_run.id}
    await state.close()


@pytest.mark.asyncio
async def test_subset_failure_routing_matches_full_walk_for_shared_roots_and_recovery(
    tmp_path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.work_units
    state.source_dependency_tree = {
        "dependencies": {
            first.id: [],
            second.id: [first.id],
        }
    }
    first_failure = state.key(first.id, Stage.DISCOVER)
    second_failure = state.key(second.id, Stage.DISCOVER)
    state.tasks[first_failure].status = TaskStatus.FAILED
    state.tasks[second_failure].status = TaskStatus.FAILED
    requested = (
        state.key(second.id, Stage.FORMALIZE),
        state.key(second.id, Stage.REVIEW),
        state.key(second.id, Stage.PROVE),
    )

    def assert_matches_full_walk() -> None:
        expected = {key: state.failure_roots(state.tasks[key]) for key in requested}
        assert state._failure_roots_subset(requested) == expected

    assert_matches_full_walk()
    assert state._failure_roots_subset(requested)[requested[0]] == (
        first_failure,
        second_failure,
    )

    state.tasks[first_failure].status = TaskStatus.SUCCEEDED
    assert_matches_full_walk()
    assert state._failure_roots_subset(requested)[requested[0]] == (second_failure,)
    await state.close()


@pytest.mark.asyncio
async def test_structured_wait_and_direct_failure_survive_restart(tmp_path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2]")
    config = load_config(config_path)
    state = StateStore(config)
    await state.load_or_create()
    owner, consumer = config.work_units
    requirement = Requirement(
        RequirementKind.COORDINATOR_OWNER,
        owner_task_key=state.key(owner.id, Stage.FORMALIZE),
        detail="coordinator diagnostic owner",
    )
    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.FAILED, "owner failed")
    await state.set_task_waiting(
        consumer.id,
        Stage.FORMALIZE,
        (requirement,),
        "waiting for diagnostic owner",
    )
    failure_id = state.shepherd_failure_records()[0].id
    await state.close()

    recovered = StateStore(load_config(config_path))
    await recovered.load_or_create()
    consumer_task = recovered.task(consumer.id, Stage.FORMALIZE)
    assert consumer_task.status == TaskStatus.PENDING
    assert consumer_task.waiting_on == (requirement,)
    assert recovered.shepherd_failure_records()[0].id == failure_id
    assert recovered.failure_roots(consumer_task) == (recovered.key(owner.id, Stage.FORMALIZE),)
    await recovered.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_tasks", [False, True])
async def test_wait_recovery_is_independent_of_task_insertion_order(
    tmp_path, reverse_tasks
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    owner, consumer = config.work_units
    owner_key = state.key(owner.id, Stage.FORMALIZE)
    requirement = Requirement(
        RequirementKind.COORDINATOR_OWNER,
        owner_task_key=owner_key,
        detail="coordinator diagnostic owner",
    )
    await state.set_task_waiting(
        consumer.id,
        Stage.FORMALIZE,
        (requirement,),
        "waiting for diagnostic owner",
    )
    await state.set_task_waiting(
        consumer.id,
        Stage.REVIEW,
        (requirement,),
        "waiting for diagnostic owner",
    )
    if reverse_tasks:
        state.tasks = dict(reversed(tuple(state.tasks.items())))

    await state.set_task(owner.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "owner recovered")

    assert state.task(consumer.id, Stage.FORMALIZE).waiting_on == ()
    assert state.task(consumer.id, Stage.REVIEW).waiting_on == ()
    await state.close()


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

    with sqlite3.connect(state.database_path) as connection:
        header = connection.execute("SELECT payload FROM globals WHERE key='state'").fetchone()[0]
        assert "source_dependency_tree" not in json.loads(header)
        assert (
            connection.execute(
                """
            SELECT count(*) FROM graph_edges
            WHERE graph='source_dependency_tree' AND kind='dependency'
                AND source_id=? AND target_id=?
            """,
                (first.id, second.id),
            ).fetchone()[0]
            == 1
        )
        revisions = dict(
            connection.execute(
                """
                SELECT node_id, revision FROM graph_nodes
                WHERE graph='source_dependency_tree' AND kind='dependency'
                """
            )
        )

    await orchestrator._persist_source_dependencies(
        first, (), {"summary": "updated root", "issues": []}
    )
    with sqlite3.connect(state.database_path) as connection:
        updated_revisions = dict(
            connection.execute(
                """
                SELECT node_id, revision FROM graph_nodes
                WHERE graph='source_dependency_tree' AND kind='dependency'
                """
            )
        )
    assert updated_revisions[first.id] > revisions[first.id]
    assert updated_revisions[second.id] == revisions[second.id]

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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

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
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def formalize(chapter, *, rerun=False):
        del rerun
        if chapter.id == second.id:
            await release_second_formalize.wait()
        await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "clean")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def review(chapter, rounds_used, **kwargs):
        del rounds_used, kwargs
        if chapter.id == first.id:
            first_review_started.set()
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.SUCCEEDED, "reviewed")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

    async def prove(chapter, *, defer_review=False):
        del defer_review
        await state.set_task(chapter.id, Stage.PROVE, TaskStatus.SUCCEEDED, "proved")
        return StageOutcome(ExecutionDisposition.SUCCEEDED)

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
