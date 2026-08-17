from pathlib import Path

import pytest

from paf.config import load_config
from paf.models import Stage
from paf.state import (
    RepairCaseStatus,
    RepairWorkUnitRecord,
    RepairWorkUnitStatus,
    StateStore,
    TaskStatus,
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
