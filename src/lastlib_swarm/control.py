from __future__ import annotations

import asyncio
import os
import signal
import socket
import stat
from collections.abc import Callable, Coroutine
from contextlib import suppress
from pathlib import Path
from typing import Any

from lastlib_swarm import json_codec as json
from lastlib_swarm.corpus import scheduling_summary
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import TaskStatus, timestamp

PROTOCOL_VERSION = 2
SOCKET_NAME = "control.sock"
PID_NAME = "daemon.pid"
RESULT_NAME = "daemon-result.json"
LOG_NAME = "daemon.log"


def control_socket(state_dir: Path) -> Path:
    return state_dir / SOCKET_NAME


def _task_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts = {status.value: 0 for status in TaskStatus}
    tasks = snapshot.get("tasks", {})
    if isinstance(tasks, dict):
        for task in tasks.values():
            if isinstance(task, dict) and task.get("status") in counts:
                counts[str(task["status"])] += 1
    return counts


def state_summary(
    orchestrator: Orchestrator,
    *,
    daemon_status: str,
    result: bool | None,
    full: bool = False,
) -> dict[str, Any]:
    snapshot = orchestrator.state.snapshot()
    response: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "status": daemon_status,
        "paused": orchestrator.control.paused,
        "stopping": orchestrator.control.stopping,
        "pid": os.getpid(),
        "result": result,
        "state_path": str(orchestrator.state.path),
        "updated_at": snapshot["updated_at"],
        "usage": snapshot["invocation_usage"],
        "lifetime_usage": snapshot["usage"],
        "cost": snapshot["invocation_cost"],
        "lifetime_cost": snapshot["cost"],
        "isolation": snapshot["isolation"],
        "scheduling": scheduling_summary(snapshot["scheduling"]),
        "agents": snapshot["agents"],
        "coordinator_build": snapshot["coordinator_build"],
        "tasks": _task_counts(snapshot),
    }
    if full:
        response["snapshot"] = snapshot
    return response


class ControlServer:
    def __init__(
        self,
        orchestrator: Orchestrator,
        operation: Callable[[], Coroutine[Any, Any, bool]],
    ) -> None:
        self.orchestrator = orchestrator
        self.operation = operation
        self.state_dir = orchestrator.config.settings.state_dir
        self.socket_path = control_socket(self.state_dir)
        self.result_path = self.state_dir / RESULT_NAME
        self.pid_path = self.state_dir / PID_NAME
        self.pipeline_task: asyncio.Task[bool] | None = None
        self.done = asyncio.Event()
        self.result: bool | None = None
        self._server: asyncio.Server | None = None

    async def run(self) -> bool:
        claimed_pid = False
        try:
            await self.orchestrator.prepare()
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self._claim_pid()
            claimed_pid = True
            self._remove_stale_socket()
            self.result_path.unlink(missing_ok=True)
            self._server = await asyncio.start_unix_server(self._handle, path=self.socket_path)
            self.socket_path.chmod(0o600)
            self.pipeline_task = asyncio.create_task(self.operation())
            loop = asyncio.get_running_loop()
            installed_signals: list[signal.Signals] = []
            for item in (signal.SIGINT, signal.SIGTERM):
                with suppress(NotImplementedError):
                    loop.add_signal_handler(item, self.request_stop)
                    installed_signals.append(item)
            try:
                self.result = await self.pipeline_task
            except asyncio.CancelledError:
                self.result = False
            finally:
                self.done.set()
                self._write_result()
                await asyncio.sleep(0.1)
                self._server.close()
                await self._server.wait_closed()
                for item in installed_signals:
                    loop.remove_signal_handler(item)
                self.socket_path.unlink(missing_ok=True)
        finally:
            await self.orchestrator.shutdown()
            if claimed_pid:
                self.pid_path.unlink(missing_ok=True)
        return bool(self.result)

    def request_stop(self) -> None:
        self.orchestrator.control.stop()
        if self.pipeline_task is not None and not self.pipeline_task.done():
            self.pipeline_task.cancel()

    def _remove_stale_socket(self) -> None:
        try:
            mode = self.socket_path.lstat().st_mode
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(mode):
            raise ValueError(f"refusing to replace non-socket control path: {self.socket_path}")
        self.socket_path.unlink()

    def _claim_pid(self) -> None:
        for _ in range(2):
            try:
                descriptor = os.open(
                    self.pid_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                try:
                    existing_pid = int(self.pid_path.read_text(encoding="utf-8").strip())
                except (OSError, ValueError):
                    existing_pid = -1
                if _pid_is_alive(existing_pid):
                    raise ValueError(
                        f"a managed pipeline already owns {self.state_dir} (pid {existing_pid})"
                    ) from None
                self.pid_path.unlink(missing_ok=True)
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(f"{os.getpid()}\n")
            return
        raise ValueError(f"could not claim managed pipeline state: {self.state_dir}")

    def _write_result(self) -> None:
        payload = state_summary(
            self.orchestrator,
            daemon_status="completed" if self.result else "failed",
            result=self.result,
        )
        payload["finished_at"] = timestamp()
        self.result_path.write_bytes(json.dumpb(payload, indent=True, sort_keys=True))

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            request = json.loads(line)
            if not isinstance(request, dict) or not isinstance(request.get("command"), str):
                raise ValueError("request must contain a string command")
            command = request["command"]
            if command == "pause":
                self.orchestrator.control.pause()
                response = self._status()
            elif command == "resume":
                self.orchestrator.control.resume()
                response = self._status()
            elif command == "unblock":
                tasks = await self.orchestrator.state.unblock()
                response = self._status() | {
                    "unblocked": len(tasks),
                    "unblocked_tasks": tasks,
                }
            elif command == "stop":
                self.request_stop()
                response = self._status() | {"accepted": True}
            elif command == "wait":
                await self.done.wait()
                response = self._status()
            elif command == "snapshot":
                response = self._status(full=True)
            elif command == "status":
                response = self._status()
            else:
                raise ValueError(f"unknown command: {command}")
        except (json.JSONDecodeError, ValueError) as error:
            response = {"protocol_version": PROTOCOL_VERSION, "error": str(error)}
        writer.write(json.dumpb(response, sort_keys=True) + b"\n")
        with suppress(ConnectionError):
            await writer.drain()
        writer.close()
        with suppress(ConnectionError):
            await writer.wait_closed()

    def _status(self, *, full: bool = False) -> dict[str, Any]:
        if self.done.is_set():
            status = "completed" if self.result else "failed"
        elif self.orchestrator.control.stopping:
            status = "stopping"
        elif self.orchestrator.control.paused:
            status = "paused"
        else:
            status = "running"
        return state_summary(
            self.orchestrator,
            daemon_status=status,
            result=self.result,
            full=full,
        )


def send_command(state_dir: Path, command: str, *, timeout: float | None = 10.0) -> dict[str, Any]:
    path = control_socket(state_dir)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(path))
        client.sendall(json.dumpb({"command": command}) + b"\n")
        chunks = bytearray()
        while not chunks.endswith(b"\n"):
            block = client.recv(65536)
            if not block:
                break
            chunks.extend(block)
    response = json.loads(chunks)
    if not isinstance(response, dict):
        raise ValueError("control server returned a non-object response")
    return response


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def offline_status(state_dir: Path) -> dict[str, Any]:
    result_path = state_dir / RESULT_NAME
    if result_path.is_file():
        value = json.loads(result_path.read_text(encoding="utf-8"))
        if isinstance(value, dict):
            return value
    state_path = state_dir / "state.json"
    if state_path.is_file():
        snapshot = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(snapshot, dict):
            return {
                "protocol_version": PROTOCOL_VERSION,
                "status": "offline",
                "result": None,
                "state_path": str(state_path),
                "updated_at": snapshot.get("updated_at"),
                "usage": snapshot.get("invocation_usage", snapshot.get("usage", {})),
                "lifetime_usage": snapshot.get("usage", {}),
                "cost": snapshot.get("invocation_cost", {}),
                "lifetime_cost": snapshot.get("cost", {}),
                "isolation": snapshot.get("isolation", {}),
                "scheduling": scheduling_summary(snapshot.get("scheduling", {})),
                "agents": snapshot.get("agents", {}),
                "coordinator_build": snapshot.get("coordinator_build", {}),
                "tasks": _task_counts(snapshot),
            }
    return {
        "protocol_version": PROTOCOL_VERSION,
        "status": "not-started",
        "result": None,
        "state_path": str(state_path),
        "usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "measured": False,
            "total_tokens": 0,
        },
        "cost": {
            "estimated_usd": 0.0,
            "priced_tokens": 0,
            "unpriced_tokens": 0,
            "inferred_runs": 0,
            "unknown_models": [],
            "measured": False,
        },
        "lifetime_usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "measured": False,
            "total_tokens": 0,
        },
        "lifetime_cost": {
            "estimated_usd": 0.0,
            "priced_tokens": 0,
            "unpriced_tokens": 0,
            "inferred_runs": 0,
            "unknown_models": [],
            "measured": False,
        },
        "scheduling": {},
        "isolation": {},
        "agents": {},
        "coordinator_build": {},
        "tasks": {status.value: 0 for status in TaskStatus},
    }
