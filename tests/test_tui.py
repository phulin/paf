import asyncio
import fcntl
import os
import pty
import struct
import subprocess
import sys
import termios
from pathlib import Path
from typing import cast

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
from paf.state import StateStore, TokenUsage
from tests.support import write_project


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
    received: list[tuple[str, str, str]] = []

    def native_run(socket_path: str, label: str, warning: str) -> bool:
        received.append((socket_path, label, warning))
        return True

    monkeypatch.setattr(tui_module, "_native_run", native_run)
    assert (
        tui_module.main(
            ["--socket", "/tmp/paf.sock", "--label", "review", "--startup-warning", "rg"]
        )
        == 0
    )
    assert received == [("/tmp/paf.sock", "review", "rg")]


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


async def test_native_tui_connects_renders_and_exits_with_pipeline(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)

    async def operation() -> bool:
        # Leave enough time for the native child to subscribe and render its first frame.
        await asyncio.sleep(0.5)
        return True

    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    for _ in range(100):
        if control_socket(config.settings.state_dir).exists():
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("control socket was not created")

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

    assert return_code == 0, output.decode(errors="replace")
    assert await server_task
