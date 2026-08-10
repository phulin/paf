import asyncio
from pathlib import Path

import pytest

from lastlib_swarm.config import load_config
from lastlib_swarm.control import ControlServer, control_socket, offline_status, send_command
from lastlib_swarm.scheduler import Orchestrator, RunControl
from lastlib_swarm.state import StateStore
from tests.support import write_project


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
    paused = await asyncio.to_thread(send_command, config.settings.state_dir, "pause")
    assert paused["status"] == "paused"
    resumed = await asyncio.to_thread(send_command, config.settings.state_dir, "resume")
    assert resumed["status"] == "running"
    stopped = await asyncio.to_thread(send_command, config.settings.state_dir, "stop")
    assert stopped["accepted"]

    assert not await server_task
    assert offline_status(config.settings.state_dir)["status"] == "failed"
