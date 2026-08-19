import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from paf.config import load_config
from paf.control import ControlServer, control_socket, offline_status, send_command
from paf.models import Stage
from paf.scheduler import Orchestrator, RunControl
from paf.state import StateStore, TaskStatus
from tests.support import write_project


async def _read_stream_event(reader: asyncio.StreamReader) -> dict[str, object]:
    line = await asyncio.wait_for(reader.readline(), timeout=2)
    assert line
    value = json.loads(line)
    assert isinstance(value, dict)
    return value


@pytest.mark.asyncio
async def test_run_control_pauses_and_stops_checkpoints() -> None:
    control = RunControl()
    control.pause()
    checkpoint = asyncio.create_task(control.checkpoint())
    await asyncio.sleep(0)
    assert not checkpoint.done()

    control.resume()
    await checkpoint
    control.stop()
    with pytest.raises(asyncio.CancelledError):
        await control.checkpoint()


@pytest.mark.asyncio
async def test_control_server_accepts_bash_friendly_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    control = RunControl()
    orchestrator = Orchestrator(config, state, control=control)

    async def operation() -> bool:
        while True:
            await control.checkpoint()
            await asyncio.sleep(0.05)

    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    for _ in range(100):
        if control_socket(config.settings.state_dir).exists():
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("control socket was not created")
    await server.prepared.wait()

    status = await asyncio.to_thread(send_command, config.settings.state_dir, "status")
    assert status["status"] == "running"
    assert Path(status["state_path"]) == state.database_path
    await state.set_task(
        config.chapters[0].id,
        Stage.REVIEW,
        TaskStatus.BLOCKED,
        "formalization failed",
    )
    unblocked = await asyncio.to_thread(send_command, config.settings.state_dir, "unblock")
    assert unblocked["unblocked"] == 1
    assert unblocked["unblocked_tasks"] == ["book/chapter-01:review"]
    assert unblocked["tasks"]["blocked"] == 0
    assert state.task(config.chapters[0].id, Stage.REVIEW).status == TaskStatus.PENDING
    request_id, _ = await state.enqueue_upstream_request(
        {
            "blocked_declaration": "target",
            "consumer_path": "lean/Book/Chapter01.lean",
            "needed_result": "a helper lemma",
        },
        consumer_chapter_id=config.chapters[0].id,
        origin_run_id="proof-run",
        owner_chapter_id=config.chapters[0].id,
        previous_attempts="attempt one",
    )
    cleared = await asyncio.to_thread(
        send_command, config.settings.state_dir, "clear-upstream-requests"
    )
    assert cleared["cleared"] == 1
    assert cleared["cleared_upstream_requests"] == [request_id]
    assert state.upstream_requests[request_id]["status"] == "closed"
    paused = await asyncio.to_thread(send_command, config.settings.state_dir, "pause")
    assert paused["status"] == "paused"
    resumed = await asyncio.to_thread(send_command, config.settings.state_dir, "resume")
    assert resumed["status"] == "running"
    await state.set_task(
        config.chapters[0].id,
        Stage.REVIEW,
        TaskStatus.FAILED,
        "review failed",
    )
    retried_failed = await asyncio.to_thread(send_command, config.settings.state_dir, "retry")
    assert retried_failed["retried"] == 1
    assert retried_failed["retried_tasks"] == ["book/chapter-01:review"]
    assert state.task(config.chapters[0].id, Stage.REVIEW).status == TaskStatus.PENDING
    retried_targets: list[tuple[str, Stage]] = []

    def retry_live_agent(chapter: str) -> dict[str, object]:
        retried_targets.append((chapter, Stage.REVIEW))
        return {
            "accepted": True,
            "chapter_id": "book/chapter-01",
            "stage": "review",
            "interrupted_run_id": "live-run",
        }

    monkeypatch.setattr(orchestrator, "retry_live_agent", retry_live_agent)
    retried = await asyncio.to_thread(
        send_command,
        config.settings.state_dir,
        "retry",
        parameters={"chapter": "book/chapter-01"},
    )
    assert retried["accepted"] is True
    assert retried["interrupted_run_id"] == "live-run"
    assert retried_targets == [("book/chapter-01", Stage.REVIEW)]
    await state.set_task(
        config.chapters[0].id,
        Stage.REVIEW,
        TaskStatus.INTERRUPTED,
        "agent was interrupted",
    )
    changed = await asyncio.to_thread(
        send_command,
        config.settings.state_dir,
        "state",
        parameters={
            "chapter": "book/chapter-01",
            "stage": "review",
            "state": "pending",
            "detail": "operator override",
        },
    )
    assert changed["changed"] is True
    assert changed["task"] == "book/chapter-01:review"
    assert changed["previous_state"] == "interrupted"
    assert changed["state"] == "pending"
    changed_task = state.task(config.chapters[0].id, Stage.REVIEW)
    assert changed_task.status == TaskStatus.PENDING
    assert changed_task.detail == "operator override"
    stopped = await asyncio.to_thread(send_command, config.settings.state_dir, "stop")
    assert stopped["accepted"]

    assert not await server_task
    completed = offline_status(config.settings.state_dir)
    assert completed["status"] == "failed"
    assert Path(completed["state_path"]) == state.database_path


@pytest.mark.asyncio
async def test_control_server_can_stop_during_preparation(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    prepare = orchestrator.prepare
    continue_preparation = asyncio.Event()
    operation_started = False

    async def delayed_prepare(
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        await continue_preparation.wait()
        await prepare(progress)

    async def operation() -> bool:
        nonlocal operation_started
        operation_started = True
        return True

    monkeypatch.setattr(orchestrator, "prepare", delayed_prepare)
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    await server.ready.wait()

    status = await asyncio.to_thread(send_command, config.settings.state_dir, "status")
    assert status["status"] == "preparing"
    stopped = await asyncio.to_thread(send_command, config.settings.state_dir, "stop")
    assert stopped["accepted"] is True
    assert stopped["status"] == "stopping"

    continue_preparation.set()
    assert not await server_task
    assert not operation_started


@pytest.mark.asyncio
async def test_dashboard_subscription_pushes_preparation_errors(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    fail_preparation = asyncio.Event()

    async def prepare(
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        if progress is not None:
            progress("Checking the Lean project", 0, 9)
        await fail_preparation.wait()
        raise RuntimeError("Lean project preparation failed")

    async def operation() -> bool:
        raise AssertionError("operation must not start after a preparation failure")

    monkeypatch.setattr(orchestrator, "prepare", prepare)
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    await server.ready.wait()
    reader, writer = await asyncio.open_unix_connection(server.socket_path)
    writer.write(b'{"command":"subscribe","view":"dashboard"}\n')
    await writer.drain()

    progress = await _read_stream_event(reader)
    assert progress["event"] == "preparation"
    fail_preparation.set()
    failure = await _read_stream_event(reader)
    assert failure == {
        "protocol_version": 4,
        "event": "error",
        "status": "failed",
        "message": "RuntimeError: Lean project preparation failed",
    }
    writer.close()
    await writer.wait_closed()
    with pytest.raises(RuntimeError, match="Lean project preparation failed"):
        await server_task


@pytest.mark.asyncio
async def test_dashboard_subscription_pushes_preparation_snapshot_and_live_deltas(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    control = RunControl()
    orchestrator = Orchestrator(config, state, control=control)
    finish = asyncio.Event()

    async def operation() -> bool:
        await finish.wait()
        return True

    prepare = orchestrator.prepare
    continue_preparation = asyncio.Event()

    async def delayed_prepare(
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        if progress is not None:
            progress("Inspecting a large Lean project", 2, 9)
        await continue_preparation.wait()
        await prepare(progress)

    monkeypatch.setattr(orchestrator, "prepare", delayed_prepare)
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    await server.ready.wait()

    reader, writer = await asyncio.open_unix_connection(control_socket(config.settings.state_dir))
    writer.write(b'{"command":"subscribe","view":"dashboard"}\n')
    await writer.drain()

    initial = await _read_stream_event(reader)
    assert initial["protocol_version"] == 4
    assert initial["event"] == "preparation"
    assert initial["status"] == "preparing"
    assert initial["preparation"] == {
        "phase": "Inspecting a large Lean project",
        "completed": 2,
        "total": 9,
    }

    continue_preparation.set()
    event = await _read_stream_event(reader)
    while event["event"] != "snapshot":
        assert event["event"] == "preparation"
        event = await _read_stream_event(reader)
    snapshot = event["snapshot"]
    assert isinstance(snapshot, dict)
    assert snapshot["source"] == str(state.database_path)
    assert snapshot["tasks"]

    chapter = config.chapters[0]
    run = await state.start_run(chapter.id, Stage.REVIEW)
    activity = state.activities.start(run.id, chapter.id, Stage.REVIEW.value)
    activity.current = "checking theorem signatures"
    state.activities.save(activity)

    delta_event = await _read_stream_event(reader)
    assert delta_event["event"] == "delta"
    delta = delta_event["delta"]
    assert isinstance(delta, dict)
    assert delta["tasks"][f"{chapter.id}:review"]["latest_run_id"] == run.id
    assert delta["activities"][run.id]["current"] == "checking theorem signatures"

    # Activity notifications are push events even when no durable state revision changes.
    revision = delta["revision"]
    activity.current = "proving the final goal"
    state.activities.save(activity)
    activity_event = await _read_stream_event(reader)
    assert activity_event["event"] == "delta"
    activity_delta = activity_event["delta"]
    assert isinstance(activity_delta, dict)
    assert activity_delta["revision"] == revision
    assert activity_delta["activities"][run.id]["current"] == "proving the final goal"

    finish.set()
    completed = await _read_stream_event(reader)
    assert completed["event"] == "complete"
    assert completed["result"] is True
    writer.close()
    await writer.wait_closed()
    assert await server_task
