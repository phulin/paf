from pathlib import Path

import pytest

import lastlib_swarm.scheduler as scheduler_module
from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult
from lastlib_swarm.config import load_config
from lastlib_swarm.models import Chapter, Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus, TokenUsage
from tests.support import write_project


class FakeExecutor(CodexExecutor):
    def __init__(self, state: StateStore, results: list[AgentResult]) -> None:
        self.state = state
        self.results = results

    async def run(
        self,
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
    ) -> AgentResult:
        del chapter, stage, feedback
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


def result(*, changed: bool, placeholders: int = 2) -> AgentResult:
    return AgentResult(
        succeeded=True,
        exit_code=0,
        changed=changed,
        placeholders=placeholders,
        usage=TokenUsage(input_tokens=10, output_tokens=5, measured=True),
        report={
            "changed": changed,
            "complete": not changed,
            "needs_repair": False,
            "summary": "reviewed",
            "issues": [],
        },
    )


@pytest.mark.asyncio
async def test_review_requires_a_no_change_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = FakeExecutor(state, [result(changed=True), result(changed=False)])

    async def successful_validation(_config: object, _chapter: object) -> ValidationResult:
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)

    assert await orchestrator.run_stage(Stage.REVIEW)
    task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.rounds == 2
    assert state.total_usage().api_tokens == 30
