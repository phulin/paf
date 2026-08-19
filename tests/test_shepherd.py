import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from paf.codex import (
    REPAIR_WORKER_ROLE,
    SHEPHERD_ROLE,
    AgentResult,
    CodexExecutor,
    ValidationResult,
    ValidationStatus,
)
from paf.config import load_config
from paf.models import Stage
from paf.scheduler import Orchestrator
from paf.state import (
    ChangeSet,
    RepairCaseStatus,
    RepairWorkUnitRecord,
    RepairWorkUnitStatus,
    StateStore,
    TaskStatus,
    TokenUsage,
)
from tests.support import write_project


@pytest.mark.asyncio
async def test_repair_work_unit_overlays_existing_stage_and_persists(tmp_path: Path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(config_path)
    chapter = config.work_units[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "review build failed")
    task_key = state.key(chapter.id, Stage.REVIEW)
    sweep = await state.start_repair_sweep(trigger="failure-threshold", task_keys=[task_key])
    case = state.repair_cases[sweep.case_ids[0]]
    unit = RepairWorkUnitRecord(
        id="repair-1",
        sweep_id=sweep.id,
        case_ids=[case.id],
        task_keys=[task_key],
        owner_chapter_id=chapter.id,
        target_stage=Stage.REVIEW,
        objective="repair the review blocker",
        priority=7.0,
    )
    await state.install_repair_plan(sweep.id, [unit], summary="one repair", run_id="plan-run")
    assert [key for key, _task in state.repairable_tasks()] == [task_key]

    await state.start_repair_work_unit(unit.id)

    task = state.task(chapter.id, Stage.REVIEW)
    assert task.status == TaskStatus.FAILED
    assert task.repairing is True
    assert task.repair_work_unit_id == unit.id
    assert state.hot_snapshot()["tasks"][task_key]["repairing"] is True
    assert case.status == RepairCaseStatus.REPAIRING
    assert state.repairable_tasks() == []

    await state.finish_repair_work_unit(
        unit.id,
        status=RepairWorkUnitStatus.SUCCEEDED,
        detail="validated",
        run_id="worker-run",
    )
    assert [key for key, _task in state.repairable_tasks()] == [task_key]
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.PENDING, "repair accepted")
    await state.finish_repair_sweep(sweep.id)
    await state.close()

    recovered = StateStore(load_config(config_path))
    await recovered.load_or_create()
    recovered_task = recovered.task(chapter.id, Stage.REVIEW)
    assert recovered_task.status == TaskStatus.PENDING
    assert recovered_task.repairing is False
    assert recovered.repair_cases[case.id].status == RepairCaseStatus.RESOLVED
    assert recovered.repair_work_units[unit.id].status == RepairWorkUnitStatus.SUCCEEDED
    assert recovered.hot_snapshot()["shepherd"]["last_summary"] == "one repair"
    await recovered.close()


@pytest.mark.asyncio
async def test_shepherd_receives_root_failures_not_causal_blockers(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    root, consumer = config.work_units
    await state.set_task(root.id, Stage.FORMALIZE, TaskStatus.FAILED, "local Lean error")
    await state.set_task(
        consumer.id,
        Stage.FORMALIZE,
        TaskStatus.BLOCKED,
        "a newly worded causal blocker that Shepherd does not know",
    )
    await state.set_task(
        consumer.id,
        Stage.REVIEW,
        TaskStatus.BLOCKED,
        "formalization did not complete",
    )
    await state.set_task(
        consumer.id,
        Stage.PROVE,
        TaskStatus.BLOCKED,
        "blocked because statement review did not complete",
    )

    assert [key for key, _task in state.shepherd_repairable_tasks()] == [
        state.key(root.id, Stage.FORMALIZE)
    ]
    await state.close()


@pytest.mark.asyncio
async def test_persisted_repair_plan_reopens_only_root_failures(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    root, consumer = config.work_units
    await state.set_task(root.id, Stage.FORMALIZE, TaskStatus.FAILED, "local Lean error")
    await state.set_task(
        consumer.id,
        Stage.FORMALIZE,
        TaskStatus.BLOCKED,
        "blocked by a failed source dependency formalization",
    )
    root_key = state.key(root.id, Stage.FORMALIZE)
    consumer_key = state.key(consumer.id, Stage.FORMALIZE)
    sweep = await state.start_repair_sweep(
        trigger="test",
        task_keys=[root_key, consumer_key],
    )
    root_case, consumer_case = (state.repair_cases[value] for value in sweep.case_ids)
    root_unit = RepairWorkUnitRecord(
        id="resume-root",
        sweep_id=sweep.id,
        # Legacy planners could mix a derived blocked case into an otherwise valid root unit.
        case_ids=[root_case.id, consumer_case.id],
        task_keys=[root_key],
        owner_chapter_id=root.id,
        target_stage=Stage.FORMALIZE,
        objective="repair the root failure",
        status=RepairWorkUnitStatus.INTERRUPTED,
    )
    downstream_unit = RepairWorkUnitRecord(
        id="do-not-resume-downstream",
        sweep_id=sweep.id,
        case_ids=[consumer_case.id],
        task_keys=[consumer_key],
        owner_chapter_id=consumer.id,
        target_stage=Stage.FORMALIZE,
        objective="repair the downstream symptom",
        status=RepairWorkUnitStatus.INTERRUPTED,
    )
    await state.install_repair_plan(
        sweep.id,
        [root_unit, downstream_unit],
        summary="legacy plan",
        run_id="planner",
    )
    root_unit.status = RepairWorkUnitStatus.INTERRUPTED
    downstream_unit.status = RepairWorkUnitStatus.INTERRUPTED
    await state.save("repair_work_units")
    reopened = await Orchestrator(config, state)._discard_persisted_repair_plans()

    assert [case.id for case in reopened] == [root_case.id]
    assert state.repair_work_units == {}
    assert state.repair_sweeps == {}
    assert root_case.status == RepairCaseStatus.OPEN
    assert consumer_case.status == RepairCaseStatus.RESOLVED
    await state.close()


@pytest.mark.asyncio
async def test_legacy_derived_review_failure_migrates_to_structured_wait(tmp_path: Path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(config_path)
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.work_units[0]
    await state.set_task(
        chapter.id,
        Stage.REVIEW,
        TaskStatus.FAILED,
        "formalization did not complete",
    )
    await state.close()

    recovered = StateStore(load_config(config_path))
    await recovered.load_or_create()

    review = recovered.task(chapter.id, Stage.REVIEW)
    assert review.status == TaskStatus.PENDING
    assert review.waiting_on
    assert recovered.shepherd_repairable_tasks() == []
    await recovered.close()


@pytest.mark.asyncio
async def test_interrupted_repair_dag_is_discarded_for_fresh_planning(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.work_units[0]
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "root review error")
    task_key = state.key(chapter.id, Stage.REVIEW)
    sweep = await state.start_repair_sweep(trigger="test", task_keys=[task_key])
    case = state.repair_cases[sweep.case_ids[0]]
    unit = RepairWorkUnitRecord(
        id="resume-me",
        sweep_id=sweep.id,
        case_ids=[case.id],
        task_keys=[task_key],
        owner_chapter_id=chapter.id,
        target_stage=Stage.REVIEW,
        objective="resume the existing repair",
        status=RepairWorkUnitStatus.INTERRUPTED,
    )
    await state.install_repair_plan(sweep.id, [unit], summary="existing plan", run_id="planner")
    unit.status = RepairWorkUnitStatus.INTERRUPTED
    await state.save("repair_work_units")
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    orchestrator = Orchestrator(config, recovered)
    reopened = await orchestrator._discard_persisted_repair_plans()

    assert [item.id for item in reopened] == [case.id]
    assert recovered.repair_work_units == {}
    assert recovered.repair_sweeps == {}
    assert recovered.repair_cases[case.id].status == RepairCaseStatus.OPEN
    await recovered.close()

    verified = StateStore(config)
    await verified.load_or_create()
    assert verified.repair_work_units == {}
    assert verified.repair_sweeps == {}
    assert verified.repair_cases[case.id].status == RepairCaseStatus.OPEN
    await verified.close()


@pytest.mark.asyncio
async def test_shepherd_loop_replans_discarded_cases_on_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.work_units[0]
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "root review error")
    case = state.ensure_repair_case(state.key(chapter.id, Stage.REVIEW))
    orchestrator = Orchestrator(config, state)
    replanned: list[tuple[str, list[str]]] = []

    class Replanned(RuntimeError):
        pass

    async def run_sweep(*, trigger: str, cases: Any) -> bool:
        replanned.append((trigger, [item.id for item in cases]))
        raise Replanned

    monkeypatch.setattr(orchestrator, "_run_shepherd_sweep", run_sweep)

    with pytest.raises(Replanned):
        await orchestrator._shepherd_loop([case])
    assert replanned == [("restart", [case.id])]
    await state.close()


@pytest.mark.asyncio
async def test_repair_worker_returns_integrated_edit_to_normal_stage_scheduler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.work_units[0]
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "root review error")
    task_key = state.key(chapter.id, Stage.REVIEW)
    sweep = await state.start_repair_sweep(trigger="test", task_keys=[task_key])
    case = state.repair_cases[sweep.case_ids[0]]
    unit = RepairWorkUnitRecord(
        id="normal-build",
        sweep_id=sweep.id,
        case_ids=[case.id],
        task_keys=[task_key],
        owner_chapter_id=chapter.id,
        target_stage=Stage.REVIEW,
        objective="repair the statement",
    )
    await state.install_repair_plan(sweep.id, [unit], summary="repair", run_id="planner")
    orchestrator = Orchestrator(config, state)

    async def attempt(*_args: object, **kwargs: object) -> object:
        assert kwargs["role"] == REPAIR_WORKER_ROLE
        await state.start_repair_work_unit(unit.id)
        return SimpleNamespace(
            agent=SimpleNamespace(
                succeeded=True,
                report={"complete": True},
            ),
            validation=ValidationResult(
                True,
                0,
                "deferred",
                status=ValidationStatus.DEFERRED,
            ),
            run=SimpleNamespace(id="worker"),
        )

    async def no_build(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("repair worker must not launch a coordinator build")

    monkeypatch.setattr(orchestrator, "_attempt", attempt)
    monkeypatch.setattr(orchestrator, "_build_chapters", no_build)

    assert await orchestrator._run_repair_work_unit(unit)
    assert unit.status == RepairWorkUnitStatus.SUCCEEDED
    assert state.task(chapter.id, Stage.REVIEW).status == TaskStatus.PENDING
    assert "normal stage validation" in unit.detail
    await state.close()


@pytest.mark.asyncio
async def test_dashboard_exposes_live_shepherd_and_repair_worker_runs(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.work_units[0]
    state = StateStore(config)
    await state.load_or_create()
    await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "review build failed")
    task_key = state.key(chapter.id, Stage.REVIEW)
    sweep = await state.start_repair_sweep(trigger="test", task_keys=[task_key])
    planner = await state.start_auxiliary_run(
        chapter.id,
        Stage.DISCOVER,
        role=SHEPHERD_ROLE,
        request_ids=sweep.case_ids,
    )
    state.shepherd.current_run_id = planner.id
    planner_activity = state.activities.start(planner.id, chapter.id, SHEPHERD_ROLE)
    planner_activity.current = "ranking repair candidates"
    state.activities.save(planner_activity)
    case = state.repair_cases[sweep.case_ids[0]]
    unit = RepairWorkUnitRecord(
        id="repair-live",
        sweep_id=sweep.id,
        case_ids=[case.id],
        task_keys=[task_key],
        owner_chapter_id=chapter.id,
        target_stage=Stage.REVIEW,
        objective="repair the review blocker",
    )
    await state.install_repair_plan(sweep.id, [unit], summary="one repair", run_id=planner.id)
    await state.start_repair_work_unit(unit.id)
    worker = await state.start_auxiliary_run(
        chapter.id,
        Stage.REVIEW,
        role=REPAIR_WORKER_ROLE,
        request_ids=[unit.id, case.id],
    )
    await state.link_repair_work_unit_run(unit.id, worker.id)
    worker_activity = state.activities.start(worker.id, chapter.id, REPAIR_WORKER_ROLE)
    worker_activity.current = "editing the failed declaration"
    state.activities.save(worker_activity)

    snapshot = state.dashboard_snapshot()
    assert snapshot["shepherd"]["agents"] == [
        {
            "run_id": planner.id,
            "role": SHEPHERD_ROLE,
            "work_unit_id": chapter.id,
            "stage": Stage.DISCOVER,
            "status": TaskStatus.RUNNING,
            "label": "Shepherd planner",
            "repair_work_unit_id": "",
            "objective": "one repair",
            "document_id": "book",
            "document_title": "A Book",
            "ordinal": 1,
            "unit_title": "First chapter",
        },
        {
            "run_id": worker.id,
            "role": REPAIR_WORKER_ROLE,
            "work_unit_id": chapter.id,
            "stage": Stage.REVIEW,
            "status": RepairWorkUnitStatus.RUNNING,
            "label": "Repair review",
            "repair_work_unit_id": unit.id,
            "objective": unit.objective,
            "document_id": "book",
            "document_title": "A Book",
            "ordinal": 1,
            "unit_title": "First chapter",
        },
    ]
    assert snapshot["activities"][planner.id]["current"] == "ranking repair candidates"
    assert snapshot["activities"][worker.id]["current"] == "editing the failed declaration"
    assert snapshot["tasks"][task_key]["active_auxiliary_role"] == REPAIR_WORKER_ROLE

    await state.finish_run(worker, status=TaskStatus.SUCCEEDED)
    snapshot = state.dashboard_snapshot()
    assert snapshot["tasks"][task_key]["active_auxiliary_role"] == ""
    assert (
        snapshot["tasks"][state.key(chapter.id, Stage.DISCOVER)]["active_auxiliary_role"]
        == SHEPHERD_ROLE
    )
    await state.close()


@pytest.mark.asyncio
async def test_dashboard_tracks_lifetime_shepherd_cost_and_live_updates(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.work_units[0]
    state = StateStore(config)
    await state.load_or_create()

    planner = await state.start_auxiliary_run(
        chapter.id,
        Stage.DISCOVER,
        role=SHEPHERD_ROLE,
        request_ids=["case-1"],
        model="gpt-5.6-sol",
    )
    assert state.shepherd_cost().estimated_usd == 0
    await state.update_run(
        planner,
        usage=TokenUsage(input_tokens=100, output_tokens=20, measured=True),
    )
    planner_cost = state.run_cost(planner).estimated_usd
    assert state.shepherd_cost().estimated_usd == pytest.approx(planner_cost)

    live_delta = state.dashboard_delta(
        ChangeSet(revision=state.revision, runs=frozenset({planner.id}))
    )
    assert live_delta["globals"]["shepherd"]["cost"]["estimated_usd"] == pytest.approx(planner_cost)
    await state.finish_run(planner, status=TaskStatus.SUCCEEDED)

    repair = await state.start_auxiliary_run(
        chapter.id,
        Stage.REVIEW,
        role=REPAIR_WORKER_ROLE,
        request_ids=["repair-1"],
        model="gpt-5.6-sol",
    )
    await state.finish_run(
        repair,
        status=TaskStatus.SUCCEEDED,
        usage=TokenUsage(input_tokens=200, output_tokens=40, measured=True),
    )
    ordinary = await state.start_run(chapter.id, Stage.FORMALIZE)
    await state.finish_run(
        ordinary,
        status=TaskStatus.SUCCEEDED,
        usage=TokenUsage(input_tokens=500, output_tokens=100, measured=True),
    )

    expected = planner_cost + state.run_cost(repair).estimated_usd
    snapshot = state.dashboard_snapshot()
    assert state.total_cost().estimated_usd > expected
    assert snapshot["shepherd"]["cost"]["estimated_usd"] == pytest.approx(expected)
    await state.close()

    recovered = StateStore(config)
    await recovered.load_or_create()
    assert recovered.shepherd_cost().estimated_usd == pytest.approx(expected)
    await recovered.close()


@pytest.mark.asyncio
async def test_shepherd_plan_is_validated_and_ranked_as_independent_work(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.work_units
    await state.set_task(first.id, Stage.REVIEW, TaskStatus.FAILED, "bad interface")
    await state.set_task(second.id, Stage.PROVE, TaskStatus.FAILED, "proof stalled")
    keys = [state.key(first.id, Stage.REVIEW), state.key(second.id, Stage.PROVE)]
    sweep = await state.start_repair_sweep(trigger="test", task_keys=keys)
    cases = [state.repair_cases[case_id] for case_id in sweep.case_ids]
    orchestrator = Orchestrator(config, state)
    report: dict[str, Any] = {
        "complete": True,
        "summary": "repair two independent failures",
        "issues": [],
        "dispositions": [
            {"case_id": case.id, "disposition": "repair", "reason": "actionable"} for case in cases
        ],
        "work_units": [
            {
                "key": "interface",
                "case_ids": [cases[0].id],
                "owner_chapter_id": first.id,
                "target_stage": "review",
                "objective": "repair the malformed declaration",
                "effort": "small",
            },
            {
                "key": "proof",
                "case_ids": [cases[1].id],
                "owner_chapter_id": second.id,
                "target_stage": "prove",
                "objective": "repair the stalled proof",
                "effort": "large",
            },
        ],
    }

    units = orchestrator._validate_shepherd_plan(sweep.id, cases, report)

    interface, proof = units
    ordinary_ceiling = max(
        [
            *orchestrator.statement_schedule.rank.values(),
            *orchestrator.proof_schedule.rank.values(),
        ]
    )
    assert interface.priority == (
        orchestrator._repair_priority_offset
        + orchestrator.statement_schedule.priority(first.document_id)
        + 1.0
    )
    assert interface.priority > ordinary_ceiling
    assert interface.task_keys == [state.key(first.id, Stage.REVIEW)]
    assert proof.target_stage == Stage.PROVE
    assert proof.priority == (
        orchestrator._repair_priority_offset
        + orchestrator.proof_schedule.priority(second.document_id)
        + 8.0
    )
    await state.close()


@pytest.mark.asyncio
async def test_failure_threshold_launches_strong_shepherd_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    config = replace(
        config,
        shepherd=replace(config.shepherd, enabled=True, failure_threshold=2),
    )
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.work_units
    await state.set_task(first.id, Stage.REVIEW, TaskStatus.FAILED, "first blocker")
    await state.set_task(second.id, Stage.REVIEW, TaskStatus.FAILED, "second blocker")
    orchestrator = Orchestrator(config, state)
    planned: list[RepairWorkUnitRecord] = []

    class FakeShepherd(CodexExecutor):
        async def run_shepherd(
            self,
            anchor: Any,
            run: Any,
            failures: Any,
            *,
            scheduling: dict[str, Any],
        ) -> AgentResult:
            del anchor, scheduling
            cases = list(failures)
            report = {
                "complete": True,
                "summary": "one shared repair",
                "issues": [],
                "dispositions": [
                    {
                        "case_id": case["case_id"],
                        "disposition": "repair",
                        "reason": "same blocker",
                    }
                    for case in cases
                ],
                "work_units": [
                    {
                        "key": "shared",
                        "case_ids": [case["case_id"] for case in cases],
                        "owner_chapter_id": first.id,
                        "target_stage": "review",
                        "objective": "repair the shared review blocker",
                        "effort": "medium",
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

    async def capture_plan(units: Any) -> bool:
        planned.extend(units)
        return True

    orchestrator.executor = FakeShepherd(config, state)
    monkeypatch.setattr(orchestrator, "_run_repair_units", capture_plan)

    assert await orchestrator._trigger_threshold_shepherd() is True
    assert len(planned) == 1
    assert planned[0].task_keys == [state.key(first.id, Stage.REVIEW)]
    sweep = next(iter(state.repair_sweeps.values()))
    assert sweep.trigger == "failure-threshold"
    assert state.task(first.id, Stage.DISCOVER).runs[-1].model == "gpt-5.6-sol"
    await state.close()


@pytest.mark.asyncio
async def test_discovery_repair_uses_the_global_agent_pool(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    orchestrator = Orchestrator(config, state)
    selected: list[str] = []

    class StopAcquire(RuntimeError):
        pass

    class RecordingLimiter:
        def __init__(self, name: str) -> None:
            self.name = name

        async def acquire(self, priority: float) -> None:
            del priority
            selected.append(self.name)
            raise StopAcquire

        def release(self) -> None:
            raise AssertionError("an unacquired test slot must not be released")

    orchestrator.discovery_slots = cast(Any, RecordingLimiter("discover"))
    orchestrator.agent_slots = cast(Any, RecordingLimiter("mutating"))

    with pytest.raises(StopAcquire):
        await orchestrator._attempt(
            config.work_units[0],
            Stage.DISCOVER,
            role=REPAIR_WORKER_ROLE,
        )
    assert selected == ["mutating"]
    await state.close()


@pytest.mark.asyncio
async def test_all_repair_units_enter_the_global_priority_pool(tmp_path: Path) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1, 2, 3]")
    source_path = tmp_path / "books" / "book.md"
    source_path.write_text(
        source_path.read_text(encoding="utf-8") + "\n\n## 3. Third chapter\n",
        encoding="utf-8",
    )
    config = load_config(config_path)
    state = StateStore(config)
    await state.load_or_create()
    task_keys: list[str] = []
    for chapter in config.work_units:
        await state.set_task(chapter.id, Stage.REVIEW, TaskStatus.FAILED, "review failed")
        task_keys.append(state.key(chapter.id, Stage.REVIEW))
    sweep = await state.start_repair_sweep(trigger="test", task_keys=task_keys)
    units = [
        RepairWorkUnitRecord(
            id=f"repair-{index}",
            sweep_id=sweep.id,
            case_ids=[case_id],
            task_keys=[task_key],
            owner_chapter_id=chapter.id,
            target_stage=Stage.REVIEW,
            objective=f"repair chapter {index}",
            priority=float(index),
        )
        for index, (chapter, task_key, case_id) in enumerate(
            zip(config.work_units, task_keys, sweep.case_ids, strict=True), start=1
        )
    ]
    await state.install_repair_plan(sweep.id, units, summary="repairs", run_id="planner")
    orchestrator = Orchestrator(config, state)
    acquired: list[float] = []
    all_waiting = asyncio.Event()
    never = asyncio.Event()

    class RecordingLimiter:
        async def acquire(self, priority: float) -> None:
            acquired.append(priority)
            if len(acquired) == len(units):
                all_waiting.set()
            await never.wait()

        def release(self) -> None:
            raise AssertionError("no global slot was granted")

    orchestrator.agent_slots = cast(Any, RecordingLimiter())
    running = asyncio.create_task(orchestrator._run_repair_units(units))
    await asyncio.wait_for(all_waiting.wait(), timeout=1)

    assert sorted(acquired) == [1.0, 2.0, 3.0]
    assert all(unit.queued for unit in units)
    assert state.shepherd.running_units == 0

    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    await state.close()
