from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from paf.codex import REPAIR_WORKER_ROLE, SHEPHERD_ROLE, AgentResult, CodexExecutor
from paf.config import load_config
from paf.models import Stage
from paf.scheduler import Orchestrator, ShepherdPlanError
from paf.state import (
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
        "blocked by a failed source dependency formalization",
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
async def test_legacy_derived_review_failure_migrates_to_blocked(tmp_path: Path) -> None:
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

    assert recovered.task(chapter.id, Stage.REVIEW).status == TaskStatus.BLOCKED
    assert recovered.shepherd_repairable_tasks() == []
    await recovered.close()


@pytest.mark.asyncio
async def test_interrupted_repair_dag_resumes_without_replanning(
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
    resumed: list[str] = []

    async def execute(units: Any) -> bool:
        resumed.extend(item.id for item in units)
        return False

    monkeypatch.setattr(orchestrator, "_execute_repair_plan", execute)

    assert not await orchestrator._resume_repair_dags(include_certification=True)
    assert resumed == [unit.id]
    assert len(recovered.repair_sweeps) == 1
    await recovered.close()


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
        },
    ]
    assert snapshot["activities"][planner.id]["current"] == "ranking repair candidates"
    assert snapshot["activities"][worker.id]["current"] == "editing the failed declaration"
    await state.close()


@pytest.mark.asyncio
async def test_shepherd_plan_is_validated_and_ranked_in_the_four_stage_dag(
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
        "summary": "repair the shared interface before retrying the proof",
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
                "depends_on": [],
                "effort": "small",
            },
            {
                "key": "proof",
                "case_ids": [cases[1].id],
                "owner_chapter_id": second.id,
                "target_stage": "prove",
                "objective": "retry the proof against the repaired declaration",
                "depends_on": ["interface"],
                "effort": "large",
            },
        ],
    }

    units = orchestrator._validate_shepherd_plan(sweep.id, cases, report)

    by_id = {unit.id: unit for unit in units}
    interface, proof = units
    assert proof.depends_on == [interface.id]
    assert interface.priority > orchestrator.statement_schedule.priority(first.document_id)
    assert interface.task_keys == [state.key(first.id, Stage.REVIEW)]
    assert by_id[proof.id].target_stage == Stage.PROVE

    report["work_units"][0]["depends_on"] = ["proof"]
    with pytest.raises(ShepherdPlanError, match="cycle"):
        orchestrator._validate_shepherd_plan(sweep.id, cases, report)
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
                        "depends_on": [],
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
    monkeypatch.setattr(orchestrator, "_execute_repair_plan", capture_plan)

    assert await orchestrator._trigger_threshold_shepherd() is True
    assert len(planned) == 1
    assert planned[0].task_keys == [state.key(first.id, Stage.REVIEW)]
    sweep = next(iter(state.repair_sweeps.values()))
    assert sweep.trigger == "failure-threshold"
    assert state.task(first.id, Stage.DISCOVER).runs[-1].model == "gpt-5.6-sol"
    await state.close()


@pytest.mark.asyncio
async def test_discovery_repair_uses_the_discovery_pool(tmp_path: Path) -> None:
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
    assert selected == ["discover"]
    await state.close()
