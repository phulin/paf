from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from weakref import WeakKeyDictionary

from lean_lsp_mcp import main as upstream_main
from leanclient.aio import AsyncLeanLSPClient

Reload = Callable[[AsyncLeanLSPClient, str, bool], Awaitable[Any]]
_ORIGINAL_RELOAD = AsyncLeanLSPClient.reload_from_disk
_SWITCH_LOCKS: WeakKeyDictionary[Any, asyncio.Lock] = WeakKeyDictionary()
_ACTIVE_PATHS: WeakKeyDictionary[Any, str] = WeakKeyDictionary()


async def reload_with_dependencies_on_switch(
    self: Any,
    path: str,
    wait: bool = False,
    *,
    original: Reload = _ORIGINAL_RELOAD,
) -> Any:
    """Reopen a newly selected file with one Lean dependency-build pass."""

    lock = _SWITCH_LOCKS.get(self)
    if lock is None:
        lock = asyncio.Lock()
        _SWITCH_LOCKS[self] = lock
    async with lock:
        previous = _ACTIVE_PATHS.get(self)
        document = self._docs.get(path)
        stale = document is not None and document.stale_imports
        if previous != path or stale:
            await self.close_file(path)
            result = await self.open(path, wait=wait, dependency_build_mode="once")
            _ACTIVE_PATHS[self] = path
            return result
        return await original(self, path, wait)


def install_dependency_reopens() -> None:
    client_type: Any = AsyncLeanLSPClient
    client_type.reload_from_disk = reload_with_dependencies_on_switch


def main() -> int:
    install_dependency_reopens()
    return upstream_main()


if __name__ == "__main__":
    raise SystemExit(main())
