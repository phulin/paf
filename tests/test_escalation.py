from pathlib import Path

import pytest

from paf.config import load_config
from paf.escalation import collect_coordination_signals, coordination_case_proposals
from paf.models import Stage
from paf.state import SourceIssueRecord, StateStore, TaskStatus, UpstreamRequestStatus
from tests.support import write_project


@pytest.mark.asyncio
async def test_detectors_reduce_related_upstream_and_source_evidence(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    for request_id, declaration in (("upstream-a", "first"), ("upstream-b", "second")):
        state.upstream_requests[request_id] = {
            "id": request_id,
            "status": UpstreamRequestStatus.OPEN.value,
            "consumer_chapter_id": chapter.id,
            "consumer_path": chapter.scope[0],
            "blocked_declaration": declaration,
            "needed_result": "A shared canonical lifting theorem",
            "capability_key": "canonical-lift",
            "owner_paths": [chapter.scope[0]],
            "attempted_alternatives": [],
            "acceptance_tests": [],
        }
    state.source_issues["source-a"] = SourceIssueRecord(
        id="source-a",
        chapter_id=chapter.id,
        book_id=chapter.book_id,
        chapter_number=chapter.number,
        chapter_title=chapter.title,
        source=chapter.source.as_posix(),
        location="the displayed degree formula",
        source_excerpt="[L:K] = e + f",
        description="The degree formula should be multiplicative.",
        suggested_correction="Replace the sum with a product.",
        sightings=2,
        stages=[Stage.REVIEW.value, Stage.PROVE.value],
        run_ids=["review-run", "proof-run"],
    )

    signals = collect_coordination_signals(state, config.escalation)
    proposals = coordination_case_proposals(signals)

    upstream = [value for value in proposals if value["kind"] == "upstream_request"]
    source = [value for value in proposals if value["kind"] == "source_issue"]
    assert len(upstream) == 1
    assert len(upstream[0]["signal_ids"]) == 2
    assert len(source) == 1
    assert source[0]["work_unit_ids"] == [chapter.id]
    source_signal = next(value for value in signals if value["kind"] == "source_issue")
    assert source_signal["evidence"]["source_excerpt_check"] == "missing"
    await state.close()


@pytest.mark.asyncio
async def test_detector_opens_one_signal_for_repeated_identical_failures(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    for _ in range(config.escalation.persistent_failure_threshold):
        run = await state.start_run(chapter.id, Stage.PROVE)
        await state.finish_run(
            run,
            status=TaskStatus.FAILED,
            error="agent repeatedly failed the same focused probe",
        )

    signals = collect_coordination_signals(state, config.escalation)
    failures = [value for value in signals if value["kind"] == "persistent_failure"]

    assert len(failures) == 1
    assert failures[0]["work_unit_ids"] == [chapter.id]
    assert len(failures[0]["evidence"]["run_ids"]) == 3
    await state.close()


@pytest.mark.asyncio
async def test_coordination_state_is_durable_and_generation_fenced(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    signal = {
        "id": "signal-a",
        "kind": "persistent_failure",
        "group_key": "failure-a",
        "evidence_digest": "evidence-a",
        "evidence": {"run_ids": ["run-a"]},
        "work_unit_ids": [config.chapters[0].id],
    }
    assert await state.upsert_coordination_signals((signal,)) == ("signal-a",)
    proposal = {
        "id": "case-a",
        "kind": "persistent_failure",
        "evidence_digest": "case-evidence-a",
        "signal_ids": ["signal-a"],
        "work_unit_ids": [config.chapters[0].id],
    }
    assert await state.sync_coordination_cases((proposal,)) == ("case-a",)
    assert not await state.update_coordination_case_generation("case-a", 2, status="running")
    assert await state.update_coordination_case_generation("case-a", 1, status="running")
    assert await state.upsert_coordination_signals(
        (signal | {"evidence_digest": "evidence-b", "evidence": {"run_ids": ["run-b"]}},)
    ) == ("signal-a",)
    assert await state.sync_coordination_cases(
        (proposal | {"evidence_digest": "case-evidence-b"},)
    ) == ("case-a",)
    assert state.coordination_cases["case-a"]["status"] == "running"
    assert await state.update_coordination_case_generation("case-a", 1, status="parked")
    assert state.coordination_cases["case-a"]["status"] == "open"
    assert state.coordination_cases["case-a"]["generation"] == 2
    await state.close()

    reloaded = StateStore(config)
    await reloaded.load_or_create()
    assert reloaded.coordination_signals["signal-a"]["evidence_digest"] == "evidence-b"
    assert reloaded.coordination_cases["case-a"]["status"] == "open"
    await reloaded.close()


@pytest.mark.asyncio
async def test_interrupted_legacy_decision_recovers_to_simple_incident_state(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    state.coordination_cases["case-a"] = {
        "id": "case-a",
        "kind": "persistent_failure",
        "status": "deciding",
        "generation": 1,
        "investigation_attempts": 1,
        "planner_attempts": 1,
        "scope_expansions": 1,
    }

    assert await state.recover_interrupted_coordination_cases() == ["case-a"]
    case = state.coordination_cases["case-a"]
    assert case["status"] == "open"
    assert case["attempts"] == 2
    assert case["strong_used"] is False
    assert case["force_strong"] is True
    assert "planner_attempts" not in case
    assert "scope_expansions" not in case
    await state.close()
