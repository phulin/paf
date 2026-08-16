from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Coroutine, Sequence
from pathlib import Path
from typing import Any, Literal, NoReturn

from paf.control import ControlServer
from paf.display import (
    ACTIVITY_KIND_ALIASES,
    ACTIVITY_KIND_DISPLAYS,
    ActivityKindDisplay,
    activity_kind_badge,
    activity_kind_display,
    format_count,
    format_usage,
)
from paf.scheduler import Orchestrator

__all__ = [
    "ACTIVITY_KIND_ALIASES",
    "ACTIVITY_KIND_DISPLAYS",
    "ActivityKindDisplay",
    "activity_kind_badge",
    "activity_kind_display",
    "format_count",
    "format_usage",
    "run_tui",
]


TuiAction = Literal["success", "failure", "reload"]


def _native_run(socket_path: str, label: str, startup_warning: str) -> TuiAction:
    try:
        _rust_tui = importlib.import_module("paf._rust_tui")
    except ImportError as error:
        raise RuntimeError(
            "the native PAF TUI is not installed; reinstall PAF from a wheel or with `uv sync`"
        ) from error
    result = _rust_tui.run(socket_path, label, startup_warning)
    # Accept the pre-reload extension during editable source upgrades. Fresh wheels return the
    # explicit action strings below.
    if isinstance(result, bool):
        return "success" if result else "failure"
    if result not in ("success", "failure", "reload"):
        raise RuntimeError(f"native PAF TUI returned an unknown action: {result!r}")
    return result


def _source_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "Cargo.toml").is_file() and (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("TUI reload requires a PAF source checkout")


def _rebuild_native_tui() -> None:
    root = _source_root()
    maturin = shutil.which("maturin")
    uv = shutil.which("uv")
    if maturin is not None:
        command = [maturin]
    elif uv is not None:
        command = [uv, "tool", "run", "--from", "maturin>=1.11,<2.0", "maturin"]
    else:
        raise RuntimeError("TUI reload requires `maturin` or `uv` on PATH")
    print("Rebuilding the native PAF TUI…", file=sys.stderr, flush=True)
    options = ["develop", "--release", "--locked"]
    if uv is not None:
        options.append("--uv")
    subprocess.run(
        [*command, *options],
        cwd=root,
        check=True,
    )


def _restart_native_tui(
    socket_path: str,
    label: str,
    startup_warning: str,
) -> NoReturn:
    arguments = [
        sys.executable,
        "-m",
        "paf.tui",
        "--socket",
        socket_path,
        "--label",
        label,
        "--startup-warning",
        startup_warning,
    ]
    os.execv(sys.executable, arguments)
    raise AssertionError("os.execv returned unexpectedly")


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="attach the native TUI to a managed PAF run")
    parser.add_argument("--socket", required=True, help="orchestrator control socket")
    parser.add_argument("--label", default="managed pipeline")
    parser.add_argument("--startup-warning", default="")
    args = parser.parse_args(arguments)
    action = _native_run(args.socket, args.label, args.startup_warning)
    if action == "reload":
        warning = args.startup_warning
        try:
            _rebuild_native_tui()
        except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
            detail = f"TUI reload failed: {error}"
            print(detail, file=sys.stderr, flush=True)
            warning = f"{warning}\n{detail}" if warning else detail
        _restart_native_tui(args.socket, args.label, warning)
    return 0 if action == "success" else 1


async def _run_tui(
    orchestrator: Orchestrator,
    operation: Callable[[], Coroutine[Any, Any, bool]],
    *,
    label: str,
    startup_warning: str,
) -> bool:
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    ready_task = asyncio.create_task(server.ready.wait())
    try:
        done, _ = await asyncio.wait({ready_task, server_task}, return_when=asyncio.FIRST_COMPLETED)
        if server_task in done:
            return await server_task

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "paf.tui",
            "--socket",
            str(server.socket_path),
            "--label",
            label,
            "--startup-warning",
            startup_warning,
        )
        return_code = await process.wait()
        if not server_task.done():
            server.request_stop()
        result = await server_task
        if return_code not in (0, 1):
            raise RuntimeError(f"native TUI exited unexpectedly with status {return_code}")
        return bool(result)
    finally:
        if not ready_task.done():
            ready_task.cancel()
        if not server_task.done():
            server.request_stop()
        await asyncio.gather(ready_task, server_task, return_exceptions=True)


def run_tui(
    orchestrator: Orchestrator,
    operation: Callable[[], Coroutine[Any, Any, bool]],
    *,
    label: str,
    startup_warning: str = "",
) -> bool:
    """Run orchestration and the native client as decoupled local processes."""

    return asyncio.run(
        _run_tui(
            orchestrator,
            operation,
            label=label,
            startup_warning=startup_warning,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
