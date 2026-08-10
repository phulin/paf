import asyncio
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import DataTable, RichLog, Static

from lastlib_swarm.config import load_config
from lastlib_swarm.models import Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TaskStatus, TokenUsage
from lastlib_swarm.tui import AgentDetailScreen, SwarmApp, format_count, format_usage
from tests.support import write_project


def test_formats_measured_token_spend_without_double_counting_cache() -> None:
    usage = TokenUsage(
        input_tokens=1_250_000,
        cached_input_tokens=1_000_000,
        output_tokens=50_000,
        reasoning_output_tokens=20_000,
        measured=True,
    )

    rendered = format_usage(usage)

    assert "1.30m" in rendered
    assert "cached 1.00m" in rendered
    assert format_count(999) == "999"


@pytest.mark.asyncio
async def test_dashboard_runs_an_operation_and_exits(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)

    async def operation() -> bool:
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test() as pilot:
        await pilot.pause(1.2)
        usage = app.query_one("#usage", Static).content

    assert app.result
    assert "API-equivalent cost" in str(usage)
    assert "lifetime tokens" in str(usage)
    assert "Lean MCP: on" in str(usage)
    assert "Codex access: full" in str(usage)


@pytest.mark.asyncio
async def test_dashboard_keeps_startup_warning_visible(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))

    async def operation() -> bool:
        return True

    app = SwarmApp(
        orchestrator,
        operation,
        label="test",
        startup_warning="ripgrep (`rg`) was not found on PATH",
    )
    async with app.run_test() as pilot:
        await pilot.pause(0.2)
        warning = app.query_one("#startup-warning", Static).content

    assert "ripgrep (`rg`) was not found" in str(warning)


@pytest.mark.asyncio
async def test_quit_drains_pipeline_before_app_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    started = asyncio.Event()
    operation_cleaned = asyncio.Event()
    shutdown_finished = asyncio.Event()
    original_shutdown = orchestrator.shutdown

    async def operation() -> bool:
        started.set()
        try:
            await asyncio.Future()
        finally:
            operation_cleaned.set()
        return False

    async def shutdown() -> None:
        await original_shutdown()
        shutdown_finished.set()

    monkeypatch.setattr(orchestrator, "shutdown", shutdown)
    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test() as pilot:
        await started.wait()
        assert str(app.query_one("#status", Static).content) == "Running test…"
        await pilot.press("q")

    assert operation_cleaned.is_set()
    assert shutdown_finished.is_set()


@pytest.mark.asyncio
async def test_selected_chapter_opens_live_agent_detail(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    ready = asyncio.Event()
    finish = asyncio.Event()

    async def operation() -> bool:
        old_run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
        await state.finish_run(
            old_run,
            status=TaskStatus.SUCCEEDED,
            usage=TokenUsage(input_tokens=1_000_000, output_tokens=100_000, measured=True),
        )
        run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
        await state.update_run(
            run,
            usage=TokenUsage(
                input_tokens=1_000,
                cached_input_tokens=200,
                output_tokens=50,
                measured=True,
            ),
        )
        activity = state.activities.start(run.id, run.chapter_id, run.stage)
        activity.consume(
            {
                "type": "item.completed",
                "item": {
                    "id": "mcp",
                    "type": "mcp_tool_call",
                    "server": "lastlib_lean",
                    "tool": "lean_goal",
                    "status": "failed",
                    "result": {"content": [{"type": "text", "text": "diagnostic failed"}]},
                },
            },
            workspace_root=tmp_path,
        )
        activity.consume(
            {
                "type": "item.completed",
                "item": {
                    "id": "message",
                    "type": "agent_message",
                    "text": "complete update\n" + "x" * 1_200 + "\nEND OF UPDATE",
                },
            },
            workspace_root=tmp_path,
        )
        state.activities.save(activity)
        ready.set()
        await finish.wait()
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test(size=(160, 50)) as pilot:
        await ready.wait()
        app.action_inspect_agent()
        await pilot.pause(0.6)

        assert isinstance(app.screen, AgentDetailScreen)
        assert app.check_action("inspect_agent", ()) is False
        assert "✗ 1" in str(app.screen.query_one("#agent-heading", Static).content)
        assert "diagnostic failed" in str(app.screen.query_one("#agent-error", Static).content)
        spend = str(app.screen.query_one("#agent-spend", Static).content)
        assert "tokens 1.1k" in spend
        assert "API-equivalent cost $0.0002" in spend
        assert "1.10m" not in spend
        summary = app.screen.query_one("#agent-summary", RichLog)
        assert summary.max_lines is None
        assert "END OF UPDATE" in "".join(line.text for line in summary.lines)

        await pilot.press("escape")
        assert app.check_action("inspect_agent", ()) is True
        finish.set()
        await pilot.pause(1.2)


@pytest.mark.asyncio
async def test_unchanged_dashboard_does_not_update_table_cells(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))
    ready = asyncio.Event()
    finish = asyncio.Event()

    async def operation() -> bool:
        ready.set()
        await finish.wait()
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test() as pilot:
        await ready.wait()
        app.refresh_dashboard()
        table = app.query_one("#tasks", DataTable)
        original = table.update_cell
        updates = 0

        def update_cell(*args: Any, **kwargs: Any) -> None:
            nonlocal updates
            updates += 1
            original(*args, **kwargs)

        monkeypatch.setattr(table, "update_cell", update_cell)

        app.refresh_dashboard()
        app.refresh_dashboard()

        assert updates == 0
        finish.set()
        await pilot.pause(1.2)
