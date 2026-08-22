from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
from dataclasses import dataclass
from pathlib import Path


@dataclass
class BeamSession:
    """One PAF-owned Lean Beam daemon bound to an agent workspace."""

    command: str
    project: Path
    control_dir: Path
    bundle_dir: Path
    log_path: Path
    environment: dict[str, str]
    process: asyncio.subprocess.Process
    _drain_tasks: tuple[asyncio.Task[None], ...]

    @classmethod
    async def start(
        cls,
        *,
        command: str,
        project: Path,
        state_dir: Path,
        run_id: str,
        timeout_seconds: float,
    ) -> BeamSession:
        resolved = shutil.which(command)
        if resolved is None and Path(command).is_file():
            resolved = str(Path(command).resolve())
        if resolved is None:
            raise RuntimeError(f"Lean Beam executable was not found: {command}")

        beam_root = state_dir / "beam"
        control_dir = beam_root / "control" / run_id
        bundle_dir = beam_root / "bundles"
        log_path = state_dir / "logs" / f"{run_id}.beam.log"
        control_dir.mkdir(parents=True, exist_ok=True)
        bundle_dir.mkdir(parents=True, exist_ok=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        environment = os.environ.copy()
        environment["BEAM_CONTROL_DIR"] = str(control_dir)
        environment["BEAM_BUNDLE_DIR"] = str(bundle_dir)
        environment["PAF_BEAM_COMMAND"] = resolved
        command_parent = str(Path(resolved).parent)
        environment["PATH"] = os.pathsep.join(
            dict.fromkeys((command_parent, *environment.get("PATH", "").split(os.pathsep)))
        )

        process = await asyncio.create_subprocess_exec(
            resolved,
            "--root",
            str(project),
            "ensure",
            "--hold",
            cwd=project,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        assert process.stdout is not None
        try:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=timeout_seconds)
            if not line:
                exit_code = await process.wait()
                raise RuntimeError(f"Lean Beam exited during startup with status {exit_code}")
            try:
                response = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise RuntimeError(
                    "Lean Beam returned an invalid startup response: "
                    + line.decode(errors="replace").strip()[:1000]
                ) from error
            if not isinstance(response, dict) or response.get("error") is not None:
                raise RuntimeError(f"Lean Beam startup failed: {response}")
        except BaseException:
            await _terminate_process_group(process)
            shutil.rmtree(control_dir, ignore_errors=True)
            raise

        log_path.write_bytes(line)
        assert process.stderr is not None
        drain_tasks = (
            asyncio.create_task(_drain_output(process.stdout, log_path)),
            asyncio.create_task(_drain_output(process.stderr, log_path)),
        )
        return cls(
            command=resolved,
            project=project,
            control_dir=control_dir,
            bundle_dir=bundle_dir,
            log_path=log_path,
            environment=environment,
            process=process,
            _drain_tasks=drain_tasks,
        )

    async def close(self) -> None:
        await self._shutdown_daemon()
        await _terminate_process_group(self.process)
        await asyncio.gather(*self._drain_tasks, return_exceptions=True)
        shutil.rmtree(self.control_dir, ignore_errors=True)

    async def _shutdown_daemon(self) -> None:
        try:
            process = await asyncio.create_subprocess_exec(
                self.command,
                "--root",
                str(self.project),
                "shutdown",
                cwd=self.project,
                env=self.environment,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
            await asyncio.wait_for(process.communicate(), timeout=10)
        except (OSError, TimeoutError):
            if "process" in locals():
                await _terminate_process_group(process)


async def _drain_output(stream: asyncio.StreamReader, log_path: Path) -> None:
    with log_path.open("ab", buffering=0) as handle:
        while chunk := await stream.read(64 * 1024):
            handle.write(chunk)


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=2)
        return
    except TimeoutError:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    await process.wait()
