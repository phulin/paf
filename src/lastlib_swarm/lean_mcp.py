from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from weakref import WeakKeyDictionary

from lean_lsp_mcp import main as upstream_main
from leanclient.aio import AsyncLeanLSPClient

Reload = Callable[[AsyncLeanLSPClient, str, bool], Awaitable[Any]]
_ORIGINAL_RELOAD = AsyncLeanLSPClient.reload_from_disk
_RELOAD_LOCKS: WeakKeyDictionary[Any, asyncio.Lock] = WeakKeyDictionary()


async def reload_with_dependencies_when_stale(
    self: Any,
    path: str,
    wait: bool = False,
    *,
    original: Reload = _ORIGINAL_RELOAD,
) -> Any:
    """Reopen a file with dependency preparation only after Lean marks its imports stale."""

    lock = _RELOAD_LOCKS.get(self)
    if lock is None:
        lock = asyncio.Lock()
        _RELOAD_LOCKS[self] = lock
    async with lock:
        document = self._docs.get(path)
        stale = document is not None and document.stale_imports
        if stale:
            await self.close_file(path)
            return await self.open(path, wait=wait, dependency_build_mode="once")
        return await original(self, path, wait)


# Preserve imports from before stale-only reload semantics were introduced.
reload_with_dependencies_on_switch = reload_with_dependencies_when_stale


def install_dependency_reopens() -> None:
    client_type: Any = AsyncLeanLSPClient
    client_type.reload_from_disk = reload_with_dependencies_when_stale


def main() -> int:
    install_dependency_reopens()
    return upstream_main()


if __name__ == "__main__":
    raise SystemExit(main())
