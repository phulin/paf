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

from paf import json_codec as json
from paf.corpus import scheduling_summary
from paf.scheduler import Orchestrator
from paf.state import TaskStatus, timestamp
from paf.state_db import DATABASE_NAME, read_status_view

PROTOCOL_VERSION = 4
SOCKET_NAME = "control.sock"
PID_NAME = "daemon.pid"
RESULT_NAME = "daemon-result.json"
LOG_NAME = "daemon.log"
DASHBOARD_FRAME_INTERVAL_SECONDS = 1 / 60


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
    snapshot = orchestrator.state.snapshot() if full else orchestrator.state.status_view()
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
        "tasks": (_task_counts(snapshot) if full else dict(snapshot.get("task_counts", {}))),
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
        self.ready = asyncio.Event()
        self.prepared = asyncio.Event()
        self.dashboard_attached = asyncio.Event()
        self._startup_done = asyncio.Event()
        self._startup_error: BaseException | None = None
        self._preparation = {
            "phase": "Starting the orchestrator",
            "completed": 0,
            "total": 9,
        }
        self._preparation_subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    async def run(self) -> bool:
        claimed_pid = False
        installed_signals: list[signal.Signals] = []
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self._claim_pid()
            claimed_pid = True
            self._remove_stale_socket()
            self.result_path.unlink(missing_ok=True)
            self._server = await asyncio.start_unix_server(self._handle, path=self.socket_path)
            self.socket_path.chmod(0o600)
            self.ready.set()
            loop = asyncio.get_running_loop()
            for item in (signal.SIGINT, signal.SIGTERM):
                with suppress(NotImplementedError):
                    loop.add_signal_handler(item, self.request_stop)
                    installed_signals.append(item)
            try:
                await self.orchestrator.prepare(self._publish_preparation)
            except BaseException as error:
                self._startup_error = error
                self.done.set()
                self._startup_done.set()
                await asyncio.sleep(0.1)
                raise
            self.prepared.set()
            self._startup_done.set()
            try:
                if self.orchestrator.control.stopping:
                    self.result = False
                else:
                    self.pipeline_task = asyncio.create_task(self.operation())
                    self.result = await self.pipeline_task
            except asyncio.CancelledError:
                self.result = False
            finally:
                self.done.set()
                self._write_result()
                await asyncio.sleep(0.1)
            return bool(self.result)
        finally:
            self._startup_done.set()
            if self._server is not None:
                self._server.close()
                await self._server.wait_closed()
            loop = asyncio.get_running_loop()
            for item in installed_signals:
                loop.remove_signal_handler(item)
            self.socket_path.unlink(missing_ok=True)
            await self.orchestrator.shutdown()
            if claimed_pid:
                self.pid_path.unlink(missing_ok=True)

    def _publish_preparation(self, phase: str, completed: int, total: int) -> None:
        self._preparation = {"phase": phase, "completed": completed, "total": total}
        for queue in tuple(self._preparation_subscribers):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(dict(self._preparation))

    def request_stop(self) -> None:
        self.orchestrator.control.stop(integrate_interrupted_workspaces=True)
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
            if command == "subscribe":
                if request.get("view", "dashboard") != "dashboard":
                    raise ValueError("only the dashboard subscription view is supported")
                await self._subscribe_dashboard(writer)
                return
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
            elif command == "retry":
                chapter = request.get("chapter")
                if chapter is None:
                    tasks = await self.orchestrator.state.retry_failed()
                    response = self._status() | {
                        "retried": len(tasks),
                        "retried_tasks": tasks,
                    }
                elif isinstance(chapter, str) and chapter:
                    retried = self.orchestrator.retry_live_agent(chapter)
                    response = self._status() | retried
                else:
                    raise ValueError("retry chapter must be a non-empty string")
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

    async def _subscribe_dashboard(self, writer: asyncio.StreamWriter) -> None:
        """Push one snapshot and subsequent dashboard deltas as newline-delimited JSON."""

        changes = self.orchestrator.state.change_bus.subscribe()
        preparations: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self._preparation_subscribers.add(preparations)
        self.dashboard_attached.set()

        async def send(event: dict[str, Any]) -> None:
            writer.write(json.dumpb({"protocol_version": PROTOCOL_VERSION} | event) + b"\n")
            await writer.drain()

        try:
            if not self._startup_done.is_set():
                await send(
                    {
                        "event": "preparation",
                        "status": self._status_name(),
                        "preparation": dict(self._preparation),
                    }
                )
            while not self._startup_done.is_set():
                update = asyncio.create_task(preparations.get())
                finished = asyncio.create_task(self._startup_done.wait())
                ready, pending = await asyncio.wait(
                    {update, finished}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                if update in ready:
                    await send(
                        {
                            "event": "preparation",
                            "status": self._status_name(),
                            "preparation": update.result(),
                        }
                    )
            if self._startup_error is not None:
                await send(
                    {
                        "event": "error",
                        "status": "failed",
                        "message": (f"{type(self._startup_error).__name__}: {self._startup_error}"),
                    }
                )
                return
            # State loading may have published resync notifications before this client had a
            # valid baseline. The snapshot below already contains all of them.
            while not changes.empty():
                changes.get_nowait()
            await send(
                {
                    "event": "snapshot",
                    "status": self._status_name(),
                    "snapshot": self.orchestrator.state.dashboard_snapshot(),
                }
            )
            while not self.done.is_set():
                next_change = asyncio.create_task(changes.get())
                completed = asyncio.create_task(self.done.wait())
                done, pending = await asyncio.wait(
                    {next_change, completed}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                if completed in done:
                    break
                change = next_change.result()
                work_units = set(change.work_units)
                runs = set(change.runs)
                globals_ = set(change.globals)
                stages = set(change.stages)
                full_resync = change.full_resync
                # Cap stream traffic at one update per terminal frame while retaining the
                # trailing event in a burst. This is push debouncing, not state polling.
                deadline = asyncio.get_running_loop().time() + DASHBOARD_FRAME_INTERVAL_SECONDS
                while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
                    try:
                        item = await asyncio.wait_for(changes.get(), remaining)
                    except TimeoutError:
                        break
                    work_units.update(item.work_units)
                    runs.update(item.runs)
                    globals_.update(item.globals)
                    stages.update(item.stages)
                    full_resync = full_resync or item.full_resync
                combined = type(change)(
                    revision=self.orchestrator.state.revision,
                    work_units=frozenset(work_units),
                    runs=frozenset(runs),
                    globals=frozenset(globals_),
                    stages=frozenset(stages),
                    full_resync=full_resync,
                )
                if full_resync:
                    await send(
                        {
                            "event": "snapshot",
                            "status": self._status_name(),
                            "snapshot": self.orchestrator.state.dashboard_snapshot(),
                        }
                    )
                else:
                    await send(
                        {
                            "event": "delta",
                            "status": self._status_name(),
                            "delta": self.orchestrator.state.dashboard_delta(combined),
                        }
                    )
            await send(
                {
                    "event": "complete",
                    "status": self._status_name(),
                    "result": self.result,
                }
            )
        except (BrokenPipeError, ConnectionError):
            pass
        finally:
            self._preparation_subscribers.discard(preparations)
            self.orchestrator.state.change_bus.unsubscribe(changes)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    def _status(self, *, full: bool = False) -> dict[str, Any]:
        return state_summary(
            self.orchestrator,
            daemon_status=self._status_name(),
            result=self.result,
            full=full,
        )

    def _status_name(self) -> str:
        if self.done.is_set():
            return "completed" if self.result else "failed"
        if self.orchestrator.control.stopping:
            return "stopping"
        if not self.prepared.is_set():
            return "preparing"
        if self.orchestrator.control.paused:
            return "paused"
        return "running"


def send_command(
    state_dir: Path,
    command: str,
    *,
    timeout: float | None = 10.0,
    parameters: dict[str, object] | None = None,
) -> dict[str, Any]:
    path = control_socket(state_dir)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(path))
        request: dict[str, object] = {"command": command}
        if parameters is not None:
            request.update(parameters)
        client.sendall(json.dumpb(request) + b"\n")
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
    state_path = state_dir / DATABASE_NAME
    snapshot = read_status_view(state_dir)
    if snapshot is not None:
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
            "tasks": dict(snapshot.get("task_counts", {})),
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
