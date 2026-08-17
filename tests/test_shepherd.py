from pathlib import Path
from dataclasses import replace
from typing import Any

import pytest

from paf.config import load_config
from paf.codex import AgentResult, CodexExecutor
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

    await state.start_repair_work_unit(unit.id)

    task = state.task(chapter.id, Stage.REVIEW)
    assert task.status == TaskStatus.FAILED
    assert task.repairing is True
    assert task.repair_work_unit_id == unit.id
    assert state.hot_snapshot()["tasks"][task_key]["repairing"] is True
    assert case.status == RepairCaseStatus.REPAIRING

    await state.finish_repair_work_unit(
        unit.id,
        status=RepairWorkUnitStatus.SUCCEEDED,
        detail="validated",
        run_id="worker-run",
    )
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
