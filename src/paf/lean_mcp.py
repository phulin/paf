from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from weakref import WeakKeyDictionary

from leanclient.aio import AsyncLeanLSPClient, LeanClientError
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.server import Settings as FastMCPSettings
from mcp.types import ToolAnnotations

from paf.diagnostics import LEAN_WARNING_RE, lean_diagnostic_counts

# pydantic-settings 2.15 detects the forward reference to FastMCP in the MCP
# SDK's generic settings model.  Rebuild it after its module has finished
# defining FastMCP, but before lean-lsp-mcp constructs its server at import time.
FastMCPSettings.model_rebuild()

from lean_lsp_mcp import main as upstream_main  # noqa: E402
from lean_lsp_mcp import server  # noqa: E402
from lean_lsp_mcp.utils import (  # noqa: E402
    extract_failed_dependency_paths,
    is_build_stderr,
)

Reload = Callable[[AsyncLeanLSPClient, str, bool], Awaitable[Any]]
Barrier = Callable[[AsyncLeanLSPClient, str, float | None], Awaitable[None]]
Notification = Callable[[AsyncLeanLSPClient, str, dict[str, Any]], None]

_ORIGINAL_RELOAD = AsyncLeanLSPClient.reload_from_disk
_ORIGINAL_BARRIER = AsyncLeanLSPClient.barrier
_ORIGINAL_NOTIFICATION = AsyncLeanLSPClient._on_notification

_DEPENDENCY_LOCKS: WeakKeyDictionary[Any, asyncio.Lock] = WeakKeyDictionary()
_DOCUMENT_LOCKS: WeakKeyDictionary[Any, dict[str, asyncio.Lock]] = WeakKeyDictionary()
_BUILD_GENERATIONS: WeakKeyDictionary[Any, int] = WeakKeyDictionary()
_DEPENDENCY_EPOCHS: WeakKeyDictionary[Any, dict[str, int]] = WeakKeyDictionary()
_REFRESH_ATTEMPTS: WeakKeyDictionary[Any, dict[str, tuple[str, int, int]]] = WeakKeyDictionary()

_IMPORT_RE = re.compile(
    r"^[ \t]*(?:(?:public|private)[ \t]+)?(?:meta[ \t]+)?import(?:[ \t]+all)?"
    r"[ \t]+(?P<modules>[^\r\n]+)",
    re.MULTILINE,
)
_STALE_IMPORT_TEXT = "Imports are out of date"
_OBJECT_FILE_TEXT = "object file"
_OLEAN_TEXT = ".olean"
_RECOVERABLE_OBJECT_FILE_FAILURES = ("does not exist", "is out of date")


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


def _dependency_epoch(client: Any, path: str) -> int:
    return _DEPENDENCY_EPOCHS.get(client, {}).get(path, 0)


def _source_digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _imports(text: str) -> tuple[str, ...]:
    imports: list[str] = []
    for match in _IMPORT_RE.finditer(text):
        modules = match.group("modules").split("--", 1)[0]
        imports.extend(modules.split())
    return tuple(imports)


def _is_recoverable_dependency_build_failure(message: str) -> bool:
    return (
        is_build_stderr(message)
        and _OBJECT_FILE_TEXT in message
        and _OLEAN_TEXT in message
        and any(reason in message for reason in _RECOVERABLE_OBJECT_FILE_FAILURES)
    )


def _diagnostics_need_dependency_refresh(diagnostics: Any) -> bool:
    return any(
        _STALE_IMPORT_TEXT in (message := str(diagnostic.get("message", "")))
        or _is_recoverable_dependency_build_failure(message)
        for diagnostic in diagnostics
        if isinstance(diagnostic, dict)
    )


def _has_dependency_refresh_diagnostic(document: Any) -> bool:
    return _diagnostics_need_dependency_refresh(document.diagnostics)


def _needs_dependency_refresh(document: Any) -> bool:
    return bool(document.stale_imports or _has_dependency_refresh_diagnostic(document))


def _require_fresh_dependencies(document: Any, path: str) -> Any:
    if _needs_dependency_refresh(document):
        raise LeanClientError(f"Lean could not prepare a usable dependency snapshot for {path}.")
    return document


def _dependency_preparation_errors(document: Any, path: str) -> list[dict[str, Any]]:
    """Return dependency failures without hiding Lake's original diagnostics."""

    errors: list[dict[str, Any]] = []
    for diagnostic in document.diagnostics:
        if not isinstance(diagnostic, dict):
            continue
        message = str(diagnostic.get("message", ""))
        if not (is_build_stderr(message) or _STALE_IMPORT_TEXT in message):
            continue
        if is_build_stderr(message):
            error_count, warning_count = lean_diagnostic_counts(message)
            if not error_count and not warning_count and LEAN_WARNING_RE.search(message):
                # Lake writes permitted ``sorry`` warnings to stderr.  The Lean
                # server wraps that output as a build diagnostic even though the
                # dependency artifact is usable.
                continue
        errors.append(
            {
                "file_path": path,
                "message": message,
                "failed_dependencies": extract_failed_dependency_paths(message),
            }
        )
    if document.stale_imports and not errors:
        errors.append(
            {
                "file_path": path,
                "message": f"Lean reported stale dependencies for {path}.",
                "failed_dependencies": [],
            }
        )
    return errors


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
    """Retain a generation for each newly published dependency-refresh condition."""

    uri = None
    if "uri" in params:
        uri = params["uri"]
    elif "textDocument" in params:
        uri = params["textDocument"].get("uri")
    document = self._docs_by_uri.get(uri) if isinstance(uri, str) else None
    original(self, method, params)
    if document is None:
        return
    refresh_was_published = (
        method == "textDocument/publishDiagnostics"
        and _diagnostics_need_dependency_refresh(params.get("diagnostics", []))
    )
    if method != "$/lean/staleDependency" and not refresh_was_published:
        return
    epochs = _DEPENDENCY_EPOCHS.setdefault(self, {})
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
        epoch = _dependency_epoch(self, path)
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
        attempts[path] = (
            _source_digest(document.text),
            _dependency_epoch(self, path),
            generation,
        )
        return _require_fresh_dependencies(document, path)


async def _force_prepare_dependencies(
    client: Any,
    path: str,
    *,
    timeout: float | None = None,
    original_barrier: Barrier = _ORIGINAL_BARRIER,
) -> tuple[Any, list[dict[str, Any]]]:
    """Perform one real dependency build, bypassing automatic-refresh deduplication."""

    async with _document_lock(client, path), _client_lock(client):
        document = await _open_and_finish_dependency_setup(
            client,
            path,
            "once",
            timeout,
            original_barrier=original_barrier,
        )
        _BUILD_GENERATIONS[client] = _build_generation(client) + 1
    return document, _dependency_preparation_errors(document, path)


async def reload_with_dependencies_when_stale(
    self: Any,
    path: str,
    wait: bool = False,
    *,
    original: Reload = _ORIGINAL_RELOAD,
    original_barrier: Barrier = _ORIGINAL_BARRIER,
) -> Any:
    """Sync cheaply, rebuilding only for recoverable imports or an import-header edit."""

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
    """Wait for fresh results, then repair and retry recoverable import failures once."""

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
async def prepare_dependencies(ctx: Context, file_paths: list[str]) -> dict[str, Any]:
    """Build maximal affected files' imported closures once and report failures."""

    attempted: list[str] = []
    ready: list[str] = []
    failed: list[str] = []
    errors: list[dict[str, Any]] = []
    for file_path in dict.fromkeys(file_paths):
        rel_path = await server.setup_client_for_file(ctx, file_path)
        if not rel_path:
            server._raise_invalid_path(file_path)
        client: AsyncLeanLSPClient = ctx.request_context.lifespan_context.client
        attempted.append(rel_path)
        try:
            _, preparation_errors = await _force_prepare_dependencies(client, rel_path)
        except LeanClientError as exc:
            preparation_errors = [
                {
                    "file_path": rel_path,
                    "message": str(exc),
                    "failed_dependencies": [],
                }
            ]
        if preparation_errors:
            failed.append(rel_path)
            errors.extend(preparation_errors)
        else:
            ready.append(rel_path)
    return {
        "attempted": attempted,
        "ready": ready,
        "failed": failed,
        "errors": errors,
    }


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
