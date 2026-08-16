import asyncio
import json
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
async def test_control_server_accepts_bash_friendly_commands(tmp_path: Path) -> None:
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
    paused = await asyncio.to_thread(send_command, config.settings.state_dir, "pause")
    assert paused["status"] == "paused"
    resumed = await asyncio.to_thread(send_command, config.settings.state_dir, "resume")
    assert resumed["status"] == "running"
    stopped = await asyncio.to_thread(send_command, config.settings.state_dir, "stop")
    assert stopped["accepted"]

    assert not await server_task
    completed = offline_status(config.settings.state_dir)
    assert completed["status"] == "failed"
    assert Path(completed["state_path"]) == state.database_path


@pytest.mark.asyncio
async def test_dashboard_subscription_pushes_snapshot_and_live_deltas(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    control = RunControl()
    orchestrator = Orchestrator(config, state, control=control)
    finish = asyncio.Event()

    async def operation() -> bool:
        await finish.wait()
        return True

    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    for _ in range(100):
        if control_socket(config.settings.state_dir).exists():
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("control socket was not created")

    reader, writer = await asyncio.open_unix_connection(control_socket(config.settings.state_dir))
    writer.write(b'{"command":"subscribe","view":"dashboard"}\n')
    await writer.drain()

    initial = await _read_stream_event(reader)
    assert initial["protocol_version"] == 3
    assert initial["event"] == "snapshot"
    snapshot = initial["snapshot"]
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
    await asyncio.sleep(0.11)
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
