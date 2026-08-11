import asyncio
from pathlib import Path
from typing import Any

import pytest
from textual.widgets import DataTable, RichLog, Static, Tab, Tabs

from lastlib_swarm.config import load_config
from lastlib_swarm.models import Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TaskPhase, TaskStatus, TokenUsage
from lastlib_swarm.tui import (
    TUI_THEME,
    AgentDetailScreen,
    SwarmApp,
    format_count,
    format_usage,
)
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
    assert "Proof LSP: on" in str(usage)
    assert "Codex access: full" in str(usage)


def test_dashboard_inherits_terminal_theme(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    orchestrator = Orchestrator(config, StateStore(config))

    async def operation() -> bool:
        return True

    app = SwarmApp(orchestrator, operation, label="test")

    assert app.theme == TUI_THEME
    assert TUI_THEME in app.available_themes


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
        assert not app.screen.query("#agent-state")
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
async def test_agent_detail_can_switch_between_chapter_steps(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    ready = asyncio.Event()
    finish = asyncio.Event()

    async def record_update(run_id: str, text: str) -> None:
        activity = state.activities.start(run_id, config.chapters[0].id, Stage.FORMALIZE.value)
        activity.consume(
            {
                "type": "item.completed",
                "item": {"id": "message", "type": "agent_message", "text": text},
            },
            workspace_root=tmp_path,
        )
        state.activities.save(activity)

    async def operation() -> bool:
        first = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
        await state.update_run(
            first,
            usage=TokenUsage(input_tokens=1_000, output_tokens=100, measured=True),
        )
        await record_update(first.id, "formalization update")
        await state.finish_run(first, status=TaskStatus.SUCCEEDED)

        second = await state.start_run(config.chapters[0].id, Stage.REVIEW)
        await state.update_run(
            second,
            usage=TokenUsage(input_tokens=2_000, output_tokens=200, measured=True),
        )
        await record_update(second.id, "review update")
        ready.set()
        await finish.wait()
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test(size=(160, 50)) as pilot:
        await ready.wait()
        app.action_inspect_agent()
        await pilot.pause(0.6)

        screen = app.screen
        assert isinstance(screen, AgentDetailScreen)
        run_tabs = screen.query_one("#run-tabs", Tabs)
        assert run_tabs.tab_count == 2
        assert [tab.label_text for tab in run_tabs.query(Tab)] == [
            "✓ Step 1 · Formalize round 1",
            "▶ Step 2 · Review round 1",
        ]
        assert run_tabs.active == "agent-step-2"
        assert "review round 1" in str(screen.query_one("#agent-heading", Static).content)
        summary = screen.query_one("#agent-summary", RichLog)
        assert "review update" in "".join(line.text for line in summary.lines)

        run_tabs.active = "agent-step-1"
        await pilot.pause(0.2)

        heading = str(screen.query_one("#agent-heading", Static).content)
        assert "step 1 of 2" in heading
        assert "formalize round 1" in heading
        assert "formalization update" in "".join(line.text for line in summary.lines)
        assert "tokens 1.1k" in str(screen.query_one("#agent-spend", Static).content)

        await pilot.press("escape")
        finish.set()
        await pilot.pause(1.2)


@pytest.mark.asyncio
async def test_agent_detail_adds_a_step_that_starts_while_open(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    ready = asyncio.Event()
    start = asyncio.Event()
    run_ready = asyncio.Event()
    finish = asyncio.Event()

    async def operation() -> bool:
        ready.set()
        await start.wait()
        await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
        run_ready.set()
        await finish.wait()
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test(size=(160, 50)) as pilot:
        await ready.wait()
        app.push_screen(AgentDetailScreen(state, config.chapters[0]))
        await pilot.pause(0.2)
        run_tabs = app.screen.query_one("#run-tabs", Tabs)
        assert run_tabs.tab_count == 0

        start.set()
        await run_ready.wait()
        await pilot.pause(1.1)

        assert run_tabs.tab_count == 1
        assert run_tabs.active == "agent-step-1"
        heading = str(app.screen.query_one("#agent-heading", Static).content)
        assert "step 1 of 1" in heading
        assert "formalize round 1" in heading

        await pilot.press("escape")
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


@pytest.mark.asyncio
@pytest.mark.parametrize("build_mode", ["streaming", "global"])
async def test_dashboard_separates_agents_queues_and_coordinator_builds(
    tmp_path: Path, build_mode: str
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    ready = asyncio.Event()
    finish_build = asyncio.Event()
    build_finished = asyncio.Event()
    finish = asyncio.Event()

    async def operation() -> bool:
        await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
        await state.set_task(
            config.chapters[1].id,
            Stage.FIXUP,
            TaskStatus.RUNNING,
            "streaming coordinator build",
            phase=TaskPhase.BUILDING,
        )
        await state.set_task(
            config.chapters[1].id,
            Stage.REVIEW,
            TaskStatus.RUNNING,
            "queued for review",
            phase=TaskPhase.QUEUED,
        )
        await state.start_coordinator_build(
            mode=build_mode,
            stage=Stage.FIXUP,
            iteration=2,
            maximum_iterations=6,
            total=81,
        )
        await state.advance_coordinator_build(
            chapter_id=config.chapters[1].id,
            completed=20,
        )
        state.append_coordinator_build_output("Building dependency graph\n")
        state.append_coordinator_build_output("Compiling Book.Chapter02\n")
        state.append_coordinator_build_output("error: Book/Chapter02.lean:3:1: broken\n")
        state.append_coordinator_build_output("warning: Book/Chapter02.lean:4:1: unused\n")
        ready.set()
        await finish_build.wait()
        await state.finish_coordinator_build()
        build_finished.set()
        await finish.wait()
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test(size=(180, 50)) as pilot:
        await ready.wait()
        app.refresh_dashboard()

        usage = str(app.query_one("#usage", Static).content)
        assert "Agents 1/4 · formalize 1 · queued 1" in usage
        assert f"Coordinator {build_mode} build 20/81 · iter 2/6" in usage
        assert "err 1 · warn 1" in usage
        status = str(app.query_one("#status", Static).content)
        build_label = "GLOBAL BUILD" if build_mode == "global" else "BUILD"
        assert f"{build_label} [" in status
        assert "20/81 (25%) · iter 2/6" in status
        assert "err 1 · warn 1" in status
        assert "book/chapter-02" in status
        assert "Building dependency graph" in status
        assert "Compiling Book.Chapter02" in status
        fixup = str(app.query_one("#stage-fixup", Static).content)
        assert "agent 0 · queue 0 · build 1 · rebuild 0" in fixup
        review = str(app.query_one("#stage-review", Static).content)
        assert "agent 0 · queue 1 · build 0 · rebuild 0" in review

        finish_build.set()
        await build_finished.wait()
        app.refresh_dashboard()
        assert str(app.query_one("#status", Static).content) == "Running test…"

        finish.set()
        await pilot.pause(1.2)
