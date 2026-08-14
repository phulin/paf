from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from weakref import WeakKeyDictionary

from lean_lsp_mcp import main as upstream_main
from lean_lsp_mcp import server
from lean_lsp_mcp.client_utils import open_synced
from leanclient.aio import AsyncLeanLSPClient, LeanClientError
from mcp.server.fastmcp import Context
from mcp.types import ToolAnnotations

Reload = Callable[[AsyncLeanLSPClient, str, bool], Awaitable[Any]]
Barrier = Callable[[AsyncLeanLSPClient, str, float | None], Awaitable[None]]
Notification = Callable[[AsyncLeanLSPClient, str, dict[str, Any]], None]

_ORIGINAL_RELOAD = AsyncLeanLSPClient.reload_from_disk
_ORIGINAL_BARRIER = AsyncLeanLSPClient.barrier
_ORIGINAL_NOTIFICATION = AsyncLeanLSPClient._on_notification

_DEPENDENCY_LOCKS: WeakKeyDictionary[Any, asyncio.Lock] = WeakKeyDictionary()
_DOCUMENT_LOCKS: WeakKeyDictionary[Any, dict[str, asyncio.Lock]] = WeakKeyDictionary()
_BUILD_GENERATIONS: WeakKeyDictionary[Any, int] = WeakKeyDictionary()
_STALE_EPOCHS: WeakKeyDictionary[Any, dict[str, int]] = WeakKeyDictionary()
_REFRESH_ATTEMPTS: WeakKeyDictionary[Any, dict[str, tuple[str, int, int]]] = WeakKeyDictionary()

_IMPORT_RE = re.compile(
    r"^[ \t]*(?:(?:public|private)[ \t]+)?(?:meta[ \t]+)?import(?:[ \t]+all)?"
    r"[ \t]+(?P<modules>[^\r\n]+)",
    re.MULTILINE,
)
_STALE_IMPORT_TEXT = "Imports are out of date"


def _client_lock(client: Any) -> asyncio.Lock:
    lock = _DEPENDENCY_LOCKS.get(client)
    if lock is None:
        lock = asyncio.Lock()
        _DEPENDENCY_LOCKS[client] = lock
    return lock


def _document_lock(client: Any, path: str) -> asyncio.Lock:
    locks = _DOCUMENT_LOCKS.setdefault(client, {})
    lock = locks.get(path)
    if lock is None:
        lock = asyncio.Lock()
        locks[path] = lock
    return lock


def _build_generation(client: Any) -> int:
    return _BUILD_GENERATIONS.get(client, 0)


def _stale_epoch(client: Any, path: str) -> int:
    return _STALE_EPOCHS.get(client, {}).get(path, 0)


def _source_digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _imports(text: str) -> tuple[str, ...]:
    imports: list[str] = []
    for match in _IMPORT_RE.finditer(text):
        modules = match.group("modules").split("--", 1)[0]
        imports.extend(modules.split())
    return tuple(imports)


def _has_stale_diagnostic(document: Any) -> bool:
    return any(
        _STALE_IMPORT_TEXT in str(diagnostic.get("message", ""))
        for diagnostic in document.diagnostics
        if isinstance(diagnostic, dict)
    )


def _needs_dependency_refresh(document: Any) -> bool:
    return bool(document.stale_imports or _has_stale_diagnostic(document))


def _require_fresh_dependencies(document: Any, path: str) -> Any:
    if _needs_dependency_refresh(document):
        raise LeanClientError(
            f"Lean could not prepare a usable dependency snapshot for {path}."
        )
    return document


def _preserve_document_identity(client: Any, path: str, previous: Any, current: Any) -> Any:
    """Move reopened state onto the object already held by an in-flight query."""

    if previous is current:
        return current
    current_state = vars(current).copy()
    previous_uri = previous.uri
    current_uri = current.uri
    vars(previous).clear()
    vars(previous).update(current_state)
    client._docs[path] = previous
    for uri in {previous_uri, current_uri}:
        mapped = client._docs_by_uri.get(uri)
        if mapped is previous or mapped is current:
            client._docs_by_uri.pop(uri, None)
    client._docs_by_uri[previous.uri] = previous
    return previous


def record_stale_dependency(
    self: Any,
    method: str,
    params: dict[str, Any],
    *,
    original: Notification = _ORIGINAL_NOTIFICATION,
) -> None:
    """Retain a generation for each newly published stale-import condition."""

    uri = None
    if "uri" in params:
        uri = params["uri"]
    elif "textDocument" in params:
        uri = params["textDocument"].get("uri")
    document = self._docs_by_uri.get(uri) if isinstance(uri, str) else None
    original(self, method, params)
    if document is None:
        return
    stale_was_published = method == "textDocument/publishDiagnostics" and any(
        _STALE_IMPORT_TEXT in str(diagnostic.get("message", ""))
        for diagnostic in params.get("diagnostics", [])
        if isinstance(diagnostic, dict)
    )
    if method != "$/lean/staleDependency" and not stale_was_published:
        return
    epochs = _STALE_EPOCHS.setdefault(self, {})
    epochs[document.path] = epochs.get(document.path, 0) + 1


async def _open_and_finish_dependency_setup(
    self: Any,
    path: str,
    mode: str,
    timeout: float | None,
    *,
    original_barrier: Barrier,
) -> Any:
    previous = self._docs.get(path)
    await self.close_file(path)
    document = await self.open(path, wait=False, dependency_build_mode=mode)
    await original_barrier(self, path, timeout)
    if previous is not None:
        document = _preserve_document_identity(self, path, previous, document)
    return document


async def _refresh_dependencies(
    self: Any,
    path: str,
    *,
    observed_generation: int,
    timeout: float | None,
    force_for_header_change: bool = False,
    original_barrier: Barrier = _ORIGINAL_BARRIER,
) -> Any:
    """Prepare one document's imports, coalescing concurrent Lake work."""

    async with _client_lock(self):
        document = self._docs.get(path)
        disk = (Path(self.project_path) / path).read_text(encoding="utf-8")
        header_changed = document is not None and _imports(document.text) != _imports(disk)
        needs_refresh = document is not None and _needs_dependency_refresh(document)
        if document is not None and disk != document.text and not header_changed:
            document = await _ORIGINAL_RELOAD(self, path, False)
            needs_refresh = _needs_dependency_refresh(document)
        if not (needs_refresh or header_changed or force_for_header_change):
            if document is None:
                return await self.open(path, wait=False, dependency_build_mode="never")
            return document

        generation = _build_generation(self)
        epoch = _stale_epoch(self, path)
        attempt_key = (_source_digest(disk), epoch, generation)
        attempts = _REFRESH_ATTEMPTS.setdefault(self, {})
        if not header_changed and attempts.get(path) == attempt_key:
            return _require_fresh_dependencies(document, path)

        # A dependency preparation that completed while this request waited may
        # already have repaired the cache. Reopen without a build first in that
        # case; otherwise perform exactly one dependency-build pass.
        mode = "never" if generation > observed_generation else "once"
        document = await _open_and_finish_dependency_setup(
            self,
            path,
            mode,
            timeout,
            original_barrier=original_barrier,
        )
        if mode == "never" and _needs_dependency_refresh(document):
            mode = "once"
            document = await _open_and_finish_dependency_setup(
                self,
                path,
                mode,
                timeout,
                original_barrier=original_barrier,
            )

        if mode == "once":
            generation += 1
            _BUILD_GENERATIONS[self] = generation
        attempts[path] = (_source_digest(document.text), _stale_epoch(self, path), generation)
        return _require_fresh_dependencies(document, path)


async def reload_with_dependencies_when_stale(
    self: Any,
    path: str,
    wait: bool = False,
    *,
    original: Reload = _ORIGINAL_RELOAD,
    original_barrier: Barrier = _ORIGINAL_BARRIER,
) -> Any:
    """Sync cheaply, rebuilding only for stale imports or an import-header edit."""

    document = self._docs.get(path)
    if document is None:
        return await self.open(path, wait=wait, dependency_build_mode="never")

    disk = (Path(self.project_path) / path).read_text(encoding="utf-8")
    header_changed = disk != document.text and _imports(disk) != _imports(document.text)
    if header_changed or _needs_dependency_refresh(document):
        async with _document_lock(self, path):
            return await _refresh_dependencies(
                self,
                path,
                observed_generation=_build_generation(self),
                timeout=None,
                force_for_header_change=header_changed,
                original_barrier=original_barrier,
            )
    return await original(self, path, wait)


async def barrier_with_dependency_refresh(
    self: Any,
    path: str,
    timeout: float | None = None,
    *,
    original: Barrier = _ORIGINAL_BARRIER,
) -> None:
    """Wait for fresh results, then repair and retry stale imports once."""

    async with _document_lock(self, path):
        observed_generation = _build_generation(self)
        await original(self, path, timeout)
        document = self._docs.get(path)
        if document is None or not _needs_dependency_refresh(document):
            return
        await _refresh_dependencies(
            self,
            path,
            observed_generation=observed_generation,
            timeout=timeout,
            original_barrier=original,
        )


# Preserve the earlier adapter name for downstream imports.
reload_with_dependencies_on_switch = reload_with_dependencies_when_stale


@server.mcp.tool(
    "lean_prepare_dependencies",
    annotations=ToolAnnotations(
        title="Prepare Lean Dependencies",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def prepare_dependencies(ctx: Context, file_paths: list[str]) -> dict[str, list[str]]:
    """Warm maximal affected files and build their stale imported closure once."""

    prepared: list[str] = []
    stale: list[str] = []
    for file_path in dict.fromkeys(file_paths):
        rel_path = await server.setup_client_for_file(ctx, file_path)
        if not rel_path:
            server._raise_invalid_path(file_path)
        client: AsyncLeanLSPClient = ctx.request_context.lifespan_context.client
        await open_synced(ctx, rel_path)
        await client.barrier(rel_path)
        prepared.append(rel_path)
        document = client._docs.get(rel_path)
        if document is not None and _needs_dependency_refresh(document):
            stale.append(rel_path)
    return {"prepared": prepared, "stale": stale}


def install_dependency_reopens() -> None:
    client_type: Any = AsyncLeanLSPClient
    client_type._on_notification = record_stale_dependency
    client_type.reload_from_disk = reload_with_dependencies_when_stale
    client_type.barrier = barrier_with_dependency_refresh


def main() -> int:
    install_dependency_reopens()
    return upstream_main()


if __name__ == "__main__":
    raise SystemExit(main())
