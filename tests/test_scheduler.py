import asyncio
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
        workspace_root: Path | None = None,
    ) -> AgentResult:
        del chapter, stage, feedback, workspace_root
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

    async def successful_validation(
        _config: object, _chapter: object, *, workspace_root: Path | None = None
    ) -> ValidationResult:
        del workspace_root
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", successful_validation)

    assert await orchestrator.run_stage(Stage.REVIEW)
    task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.rounds == 2
    assert state.total_usage().api_tokens == 30


@pytest.mark.asyncio
async def test_individual_stage_waits_for_upstream_book(
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

    async def formalize(chapter: Chapter) -> bool:
        events.append(f"start:{chapter.book_id}")
        await asyncio.sleep(0)
        events.append(f"end:{chapter.book_id}")
        return True

    monkeypatch.setattr(orchestrator, "_formalize", formalize)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert events == ["start:book", "end:book", "start:second", "end:second"]
