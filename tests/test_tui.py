import asyncio
import fcntl
import json
import os
import pty
import struct
import subprocess
import sys
import termios
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

import paf.tui as tui_module
from paf.config import load_config
from paf.control import ControlServer, control_socket
from paf.display import (
    ACTIVITY_KIND_ALIASES,
    ACTIVITY_KIND_DISPLAYS,
    activity_kind_badge,
    activity_kind_display,
    format_count,
    format_usage,
)
from paf.models import Stage
from paf.scheduler import Orchestrator
from paf.state import StateStore, TaskStatus, TokenUsage
from tests.support import write_project


class FakeRustTui:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, ...]] = []

    def run(self, *args: object) -> object:
        self.calls.append(args)
        return self.result


def test_activity_kinds_have_short_unique_labels_and_colors() -> None:
    expected_labels = {
        "agent": "agent",
        "usage": "tokens",
        "todo": "plan",
        "message": "msg",
        "reasoning": "think",
        "command_execution": "bash",
        "file_change": "edit",
        "mcp_tool_call": "mcp",
        "collab_tool_call": "swarm",
        "web_search": "web",
        "error": "error",
        "context_compaction": "compact",
    }

    assert {kind: activity_kind_display(kind).label for kind in expected_labels} == expected_labels
    assert len({value.label for value in ACTIVITY_KIND_DISPLAYS.values()}) == len(
        ACTIVITY_KIND_DISPLAYS
    )
    assert len({value.color for value in ACTIVITY_KIND_DISPLAYS.values()}) == len(
        ACTIVITY_KIND_DISPLAYS
    )
    assert set(ACTIVITY_KIND_ALIASES.values()) <= set(ACTIVITY_KIND_DISPLAYS)
    assert activity_kind_display("compaction") == activity_kind_display("context_compaction")
    badge = activity_kind_badge("command_execution")
    assert badge.plain == "[bash]"
    assert "#2ac3de" in str(badge.style)


def test_unknown_activity_kind_gets_a_bounded_neutral_badge() -> None:
    display = activity_kind_display("future_extremely_long_event_kind")
    assert display.label == "future-extr…"
    assert display.color == "#a9b1d6"


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


def test_native_entry_point_forwards_connection_options(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    received: list[tuple[str, str, str, str | None, str | None]] = []

    def native_run(
        socket_path: str,
        label: str,
        warning: str,
        agent_view: str | None,
        detail_tab: str | None,
    ) -> tui_module.TuiOutcome:
        received.append((socket_path, label, warning, agent_view, detail_tab))
        return tui_module.TuiOutcome("success")

    monkeypatch.setattr(tui_module, "_native_run", native_run)
    assert (
        tui_module.main(
            ["--socket", "/tmp/paf.sock", "--label", "review", "--startup-warning", "rg"]
        )
        == 0
    )
    assert received == [("/tmp/paf.sock", "review", "rg", None, None)]


def test_native_reload_result_carries_the_open_agent_view(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    native = FakeRustTui('{"action":"reload","agent_view":"book/chapter-01","detail_tab":"files"}')
    monkeypatch.setattr(tui_module.importlib, "import_module", lambda _: native)

    outcome = tui_module._native_run("/tmp/live.sock", "review", "", None, None)

    assert outcome == tui_module.TuiOutcome("reload", "book/chapter-01", "files")
    assert native.calls == [("/tmp/live.sock", "review", "")]


def test_native_restart_receives_the_agent_view_to_restore(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    native = FakeRustTui("success")
    monkeypatch.setattr(tui_module.importlib, "import_module", lambda _: native)

    outcome = tui_module._native_run("/tmp/live.sock", "review", "", "book/chapter-01", "summary")

    assert outcome == tui_module.TuiOutcome("success")
    assert native.calls == [("/tmp/live.sock", "review", "", "book/chapter-01", "summary")]


def test_native_detach_returns_success_without_reloading(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    native = FakeRustTui("detach")
    monkeypatch.setattr(tui_module.importlib, "import_module", lambda _: native)

    outcome = tui_module._native_run("/tmp/live.sock", "review", "", None, None)

    assert outcome == tui_module.TuiOutcome("detach")
    monkeypatch.setattr(tui_module, "_native_run", lambda *_: outcome)
    assert tui_module.main(["--socket", "/tmp/live.sock", "--label", "review"]) == 0


def test_reload_rebuilds_and_restarts_with_the_same_agent_view(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    rebuilt: list[bool] = []
    restarted: list[tuple[str, str, str, str | None, str | None]] = []

    class Restarted(Exception):
        pass

    def restart(
        socket_path: str,
        label: str,
        warning: str,
        agent_view: str | None,
        detail_tab: str | None,
    ) -> None:
        restarted.append((socket_path, label, warning, agent_view, detail_tab))
        raise Restarted

    monkeypatch.setattr(
        tui_module,
        "_native_run",
        lambda *_: tui_module.TuiOutcome("reload", "book/chapter-01", "plan"),
    )
    monkeypatch.setattr(tui_module, "_rebuild_native_tui", lambda: rebuilt.append(True))
    monkeypatch.setattr(tui_module, "_restart_native_tui", restart)

    try:
        tui_module.main(["--socket", "/tmp/live.sock", "--label", "review"])
    except Restarted:
        pass
    else:
        raise AssertionError("reload did not restart the TUI process")

    assert rebuilt == [True]
    assert restarted == [("/tmp/live.sock", "review", "", "book/chapter-01", "plan")]


def test_reload_builds_an_optimized_locked_native_extension(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    calls: list[tuple[list[str], Path, bool]] = []

    def which(command: str) -> str | None:
        return f"/tools/{command}" if command in {"maturin", "uv"} else None

    def run(command: list[str], *, cwd: Path, check: bool) -> None:
        calls.append((command, cwd, check))

    monkeypatch.setattr(tui_module, "_source_root", lambda: tmp_path)
    monkeypatch.setattr(tui_module.shutil, "which", which)
    monkeypatch.setattr(tui_module.subprocess, "run", run)

    tui_module._rebuild_native_tui()

    assert calls == [
        (
            ["/tools/maturin", "develop", "--release", "--locked", "--uv"],
            tmp_path,
            True,
        )
    ]


async def test_run_tui_waits_for_server_readiness_without_a_fixed_deadline(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    launched: list[tuple[object, ...]] = []

    class FakeServer:
        def __init__(self, orchestrator: object, operation: object) -> None:
            self.ready = asyncio.Event()
            self.stopped = asyncio.Event()
            # Deliberately never create this path: readiness is an in-process contract,
            # not a racy filesystem observation.
            self.socket_path = tmp_path / "control.sock"

        async def run(self) -> bool:
            for _ in range(201):
                await asyncio.sleep(0)
            self.ready.set()
            await self.stopped.wait()
            return True

        def request_stop(self) -> None:
            self.stopped.set()

    class FakeProcess:
        async def wait(self) -> int:
            return 0

    async def create_subprocess_exec(*args: object) -> FakeProcess:
        launched.append(args)
        return FakeProcess()

    monkeypatch.setattr(tui_module, "ControlServer", FakeServer)
    monkeypatch.setattr(tui_module.asyncio, "create_subprocess_exec", create_subprocess_exec)

    async def operation() -> bool:
        return True

    assert await tui_module._run_tui(
        cast(Orchestrator, object()),
        operation,
        label="readiness test",
        startup_warning="",
    )
    assert launched


async def test_dashboard_projection_includes_ordered_units_and_bounded_activity(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    run = await state.start_run(chapter.id, Stage.REVIEW)
    activity = state.activities.start(run.id, chapter.id, Stage.REVIEW.value)
    activity.current = "checking theorem signatures"
    state.activities.save(activity)

    snapshot = state.dashboard_snapshot()

    assert [unit["id"] for unit in snapshot["work_units"]] == [
        chapter.id for chapter in config.chapters
    ]
    assert snapshot["activities"][run.id]["current"] == "checking theorem signatures"
    task = snapshot["tasks"][f"{chapter.id}:review"]
    assert "work_unit_usage" in task
    assert "work_unit_cost" in task


async def test_chapter_run_projection_lists_history_and_selected_recent_activity(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    formalize = await state.start_run(chapter.id, Stage.FORMALIZE)
    review = await state.start_run(chapter.id, Stage.REVIEW)
    activity = state.activities.start(formalize.id, chapter.id, Stage.FORMALIZE.value)
    activity.current = "historical formalization"
    state.activities.save(activity)

    details = state.dashboard_chapter_runs(chapter.id, selected_run_id=formalize.id)

    assert [(run["stage"], run["round"]) for run in details["runs"]] == [
        ("formalize", 1),
        ("review", 1),
    ]
    assert details["selected_run_id"] == formalize.id
    assert details["activity"]["current"] == "historical formalization"
    assert review.id != details["selected_run_id"]

    prompt_path = state.logs_dir / f"{formalize.id}.prompt.md"
    prompt_path.write_text("Formalize this chapter.", encoding="utf-8")
    assert state.dashboard_run_prompt(formalize.id) == "Formalize this chapter."
    assert state.dashboard_run_prompt("missing-run") is None

    transcript_path = state.logs_dir / f"{formalize.id}.jsonl"
    transcript_path.write_text(
        "".join(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": f"event-{index}",
                        "type": "reasoning",
                        "status": "completed",
                    },
                }
            )
            + "\n"
            for index in range(150)
        ),
        encoding="utf-8",
    )
    formalize.log_path = str(transcript_path)
    formalize.project_root = str(tmp_path)
    full_timeline = state.dashboard_run_timeline(formalize.id)
    assert full_timeline is not None
    assert len(full_timeline["recent"]) == 150


async def test_package_run_projection_lists_only_its_steward_and_workers(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    steward = await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="package_steward",
        request_ids=("package-a",),
        model="test-model",
    )
    worker = await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="package_worker",
        request_ids=("package-a", "S1"),
        model="test-model",
    )
    await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="package_steward",
        request_ids=("package-b",),
        model="test-model",
    )
    activity = state.activities.start(steward.id, chapter.id, "package_steward")
    activity.current = "reviewing the package plan"
    state.activities.save(activity)

    details = state.dashboard_package_runs("package-a", selected_run_id=steward.id)

    assert [(run["id"], run["role"]) for run in details["runs"]] == [
        (steward.id, "package_steward"),
        (worker.id, "package_worker"),
    ]
    assert details["selected_run_id"] == steward.id
    assert details["activity"]["current"] == "reviewing the package plan"


@pytest.mark.asyncio
async def test_resumed_steward_auxiliary_run_remains_one_tui_run(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    chapter = config.chapters[0]
    run = await state.start_auxiliary_run(
        chapter.id,
        Stage.PROVE,
        role="upstream_implementation",
        request_ids=("case-a", "request-a"),
    )
    await state.finish_run(
        run,
        status=TaskStatus.INTERRUPTED,
        thread_id="steward-session",
    )
    state.steward_cases["case-a"] = {
        "id": "case-a",
        "status": "implementing",
        "request_ids": ["request-a"],
        "implementation_run_ids": [run.id],
    }

    assert (
        state.interrupted_auxiliary_run(
            role="upstream_implementation",
            request_ids=("case-a", "request-a"),
        )
        is run
    )
    await state.resume_auxiliary_run(run)
    details = state.dashboard_steward_case_runs("case-a")

    assert [item["id"] for item in details["runs"]] == [run.id]
    assert details["runs"][0]["status"] == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_orphaned_implementing_steward_case_is_requeued(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    state.steward_cases["case-a"] = {
        "id": "case-a",
        "status": "implementing",
        "request_ids": [],
    }

    recovered = await state.recover_interrupted_steward_cases()

    assert recovered == ["case-a"]
    assert state.steward_cases["case-a"]["status"] == "ready"


@pytest.mark.asyncio
async def test_dashboard_projects_steward_cases_without_legacy_package_state(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    await state.load_or_create()
    state.steward_cases["case-a"] = {
        "id": "case-a",
        "status": "ready",
        "title": "Shared bridge",
        "request_ids": ["request-a"],
    }
    snapshot = state.dashboard_snapshot()

    assert snapshot["steward_cases"]["case-a"]["title"] == "Shared bridge"
    assert "capability_packages" not in snapshot
    assert "package_consumers" not in snapshot
    assert "upstream_requests" not in snapshot
    await state.close()


async def test_native_tui_connects_renders_and_exits_with_pipeline(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)

    async def operation() -> bool:
        # Leave enough time for the native child to subscribe and render its first frame.
        await asyncio.sleep(0.5)
        return True

    prepare = orchestrator.prepare
    continue_preparation = asyncio.Event()

    async def delayed_prepare(
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        if progress is not None:
            progress("Preparing isolated workspaces and Lean caches", 7, 9)
        await continue_preparation.wait()
        await prepare(progress)

    monkeypatch.setattr(orchestrator, "prepare", delayed_prepare)
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    await server.ready.wait()

    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 180, 0, 0))
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "paf.tui",
                "--socket",
                str(control_socket(config.settings.state_dir)),
                "--label",
                "native smoke test",
            ],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            close_fds=True,
        )
    finally:
        os.close(slave)
    await asyncio.wait_for(server.dashboard_attached.wait(), timeout=2)
    await asyncio.sleep(0.05)
    continue_preparation.set()
    output = b""
    try:
        return_code = await asyncio.to_thread(process.wait, 5)
        os.set_blocking(master, False)
        while True:
            try:
                chunk = os.read(master, 65_536)
            except BlockingIOError:
                break
            except OSError:
                break
            if not chunk:
                break
            output += chunk
    finally:
        os.close(master)
        if process.poll() is None:
            process.kill()
            process.wait()

    decoded = output.decode(errors="replace")
    assert return_code == 0, decoded
    assert "Preparing PAF" in decoded
    assert "7 / 9" in decoded
    assert await server_task
