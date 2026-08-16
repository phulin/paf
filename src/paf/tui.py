from __future__ import annotations

import argparse
import asyncio
import importlib
import sys
from collections.abc import Callable, Coroutine, Sequence
from typing import Any

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


def _native_run(socket_path: str, label: str, startup_warning: str) -> bool:
    try:
        _rust_tui = importlib.import_module("paf._rust_tui")
    except ImportError as error:
        raise RuntimeError(
            "the native PAF TUI is not installed; reinstall PAF from a wheel or with `uv sync`"
        ) from error
    return bool(_rust_tui.run(socket_path, label, startup_warning))


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="attach the native TUI to a managed PAF run")
    parser.add_argument("--socket", required=True, help="orchestrator control socket")
    parser.add_argument("--label", default="managed pipeline")
    parser.add_argument("--startup-warning", default="")
    args = parser.parse_args(arguments)
    return 0 if _native_run(args.socket, args.label, args.startup_warning) else 1


async def _run_tui(
    orchestrator: Orchestrator,
    operation: Callable[[], Coroutine[Any, Any, bool]],
    *,
    label: str,
    startup_warning: str,
) -> bool:
    server = ControlServer(orchestrator, operation)
    server_task = asyncio.create_task(server.run())
    try:
        for _ in range(200):
            if server.socket_path.exists():
                break
            if server_task.done():
                return await server_task
            await asyncio.sleep(0.01)
        else:
            server.request_stop()
            raise RuntimeError("orchestrator dashboard socket did not become ready")

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
        if not server_task.done():
            server.request_stop()
        await asyncio.gather(server_task, return_exceptions=True)


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
