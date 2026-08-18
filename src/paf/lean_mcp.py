from __future__ import annotations

import asyncio
import hashlib
import inspect
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Annotated, Any, Literal
from weakref import WeakKeyDictionary

from leanclient.aio import AsyncLeanLSPClient, LeanClientError, LeanRequestTimeout
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.server import Settings as FastMCPSettings
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from paf.diagnostics import LEAN_WARNING_RE, lean_diagnostic_counts

# pydantic-settings 2.15 detects the forward reference to FastMCP in the MCP
# SDK's generic settings model.  Rebuild it after its module has finished
# defining FastMCP, but before lean-lsp-mcp constructs its server at import time.
FastMCPSettings.model_rebuild()

from lean_lsp_mcp import main as upstream_main  # noqa: E402
from lean_lsp_mcp import server  # noqa: E402
from lean_lsp_mcp.client_utils import (  # noqa: E402
    bind_lean_project_path,
    build_lean_path_policy,
    open_synced,
)
from lean_lsp_mcp.file_utils import (  # noqa: E402
    LeanPathPolicy,
    valid_lean_project_path,
)
from lean_lsp_mcp.file_utils import (  # noqa: E402
    require_lean_project_path as upstream_require_lean_project_path,
)
from lean_lsp_mcp.models import (  # noqa: E402
    DeclarationInfo,
    DiagnosticMessage,
    HoverInfo,
    LocalSearchResult,
    LocalSearchResults,
)
from lean_lsp_mcp.tools import analysis as analysis_tools  # noqa: E402
from lean_lsp_mcp.tools import diagnostics as diagnostics_tools  # noqa: E402
from lean_lsp_mcp.tools import navigation as navigation_tools  # noqa: E402
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
_ORIGINAL_PROCESS_DIAGNOSTICS = server._process_diagnostics
_ORIGINAL_SETUP_CLIENT_FOR_FILE = server.setup_client_for_file
_ORIGINAL_RESOLVE_FILE_PATH = server.resolve_file_path
_ORIGINAL_LOCAL_SEARCH = server.lean_local_search
_ORIGINAL_DECLARATION_FILE = navigation_tools.declaration_file
_ORIGINAL_HOVER = navigation_tools.hover
_ORIGINAL_DIAGNOSTICS = diagnostics_tools.diagnostic_messages
_ORIGINAL_MULTI_ATTEMPT = analysis_tools.multi_attempt

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
_TOOL_OUTPUT_MAX_CHARS = 12 * 1024
_BOUNDED_TOOL_NAMES: set[str] = set()
_LEAN_DECLARATION_RE = re.compile(
    r"^[ \t]*(?:@\[[^\]]*\][ \t]*)*"
    r"(?:(?:public|protected|private|noncomputable|partial|unsafe|scoped|local)[ \t]+)*"
    r"(?:theorem|lemma|def|axiom|class|instance|structure|inductive|abbrev|opaque)[ \t]+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_'.]*)",
    re.MULTILINE,
)
_LEAN_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_']*(?:\.[A-Za-z_][A-Za-z0-9_']*)*")
_TACTIC_KEYWORDS = {
    "apply",
    "assumption",
    "by",
    "case",
    "constructor",
    "exact",
    "intro",
    "rfl",
    "simp",
    "simpa",
    "rw",
}


def normalize_lean_project_root(path: Path | str) -> Path:
    """Accept either a Lean root or a PAF merged workspace containing ``lean/``."""

    candidate = Path(path).expanduser().resolve(strict=False)
    choices = (candidate, candidate / "lean")
    for choice in choices:
        if valid_lean_project_path(choice):
            return choice.resolve(strict=True)
    return upstream_require_lean_project_path(candidate)


def require_normalized_lean_project_path(path: Path | str) -> Path:
    """Drop-in replacement used by upstream's lifespan configuration."""

    return normalize_lean_project_root(path)


def _set_normalized_lifespan_root(ctx: Context) -> Path | None:
    lifespan = ctx.request_context.lifespan_context
    root = getattr(lifespan, "lean_project_path", None)
    if root is None:
        return None
    normalized = normalize_lean_project_root(root)
    if normalized != Path(root).resolve(strict=False):
        lifespan.lean_project_path = normalized
    return normalized


def resolve_lean_input_path(ctx: Context, file_path: str, *, require_exists: bool = True) -> Path:
    """Resolve project, display, and module-style dependency paths consistently."""

    path = Path(file_path).expanduser()
    if path.is_absolute():
        return path.resolve(strict=require_exists)

    root = _set_normalized_lifespan_root(ctx)
    if root is None:
        return _ORIGINAL_RESOLVE_FILE_PATH(ctx, file_path, require_exists=require_exists)

    policy = build_lean_path_policy(root)
    candidates = [root / path]

    parts = path.parts
    if len(parts) >= 3 and parts[:2] == (".lake", "packages"):
        package_name = parts[2]
        suffix = Path(*parts[3:])
        candidates.extend(
            allowed.root / suffix
            for allowed in policy.allowed_roots
            if allowed.display_prefix == f".lake/packages/{package_name}"
        )
    elif parts and parts[0] == ".lean-stdlib" and policy.stdlib_root is not None:
        candidates.append(policy.stdlib_root / Path(*parts[1:]))
    else:
        # Agents naturally spell dependency files as ``Mathlib/Foo.lean``.
        # Try that path under every allowed dependency source root.
        candidates.extend(allowed.root / path for allowed in policy.allowed_roots[1:])

    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=require_exists)
        except (FileNotFoundError, OSError):
            continue
        if not require_exists or resolved.exists():
            return resolved

    if require_exists:
        raise FileNotFoundError(
            f"Lean file '{file_path}' was not found under project '{root}' or its dependencies."
        )
    return candidates[0].resolve(strict=False)


async def setup_client_for_resolved_file(ctx: Context, file_path: str) -> str | None:
    """Normalize PAF roots and dependency display paths before upstream setup."""

    try:
        resolved = resolve_lean_input_path(ctx, file_path)
    except (FileNotFoundError, OSError, ValueError):
        return None
    return await _ORIGINAL_SETUP_CLIENT_FOR_FILE(ctx, str(resolved))


def local_search_with_dependencies(
    query: str,
    limit: int = 32,
    project_root: Path | None = None,
    path_policy: LeanPathPolicy | None = None,
) -> list[dict[str, str]]:
    """Search the project, each dependency source root, and stdlib explicitly.

    PAF isolation shares ``.lake/packages`` through a symlink. Ripgrep does not
    follow that symlink by default, so upstream's single-root scan silently
    omitted Mathlib even though the path policy allowed it.
    """

    policy = path_policy or build_lean_path_policy(
        normalize_lean_project_root(project_root or Path.cwd())
    )
    matches: list[dict[str, str]] = []
    for allowed in policy.allowed_roots:
        subpolicy = LeanPathPolicy(
            project_root=allowed.root,
            allowed_roots=(allowed,),
            stdlib_root=None,
        )
        matches.extend(
            _ORIGINAL_LOCAL_SEARCH(
                query=query,
                limit=max(limit, 1),
                project_root=allowed.root,
                path_policy=subpolicy,
            )
        )

    normalized = query.casefold()

    def sort_key(match: dict[str, str]) -> tuple[int, int, int, str]:
        name = match["name"]
        base = name.rsplit(".", 1)[-1]
        folded = name.casefold()
        base_folded = base.casefold()
        relevance = (
            0
            if folded == normalized or base_folded == normalized
            else 1
            if base_folded.startswith(normalized)
            else 2
            if normalized in base_folded
            else 3
        )
        external = int(match["file"].startswith((".lake/packages/", ".lean-stdlib/")))
        return relevance, external, len(base), name

    matches.sort(key=sort_key)
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for match in matches:
        key = (match["name"], match["kind"], match["file"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(match)
        if len(deduped) >= limit:
            break
    return deduped


def compact_target_diagnostics(
    diagnostics: list[dict[str, Any]],
    build_success: bool,
    severity: str | None = None,
    timed_out: bool = False,
    partial: bool = False,
    processing_lines: list[list[int]] | None = None,
) -> Any:
    """Return target errors plus a count, suppressing warning bodies and large payloads."""

    warning_count = sum(
        1
        for diagnostic in diagnostics
        if isinstance(diagnostic, dict) and diagnostic.get("severity", 1) == 2
    )
    errors: list[dict[str, Any]] = []
    remaining = _TOOL_OUTPUT_MAX_CHARS
    for diagnostic in diagnostics:
        if not isinstance(diagnostic, dict):
            continue
        # Keep dependency build stderr long enough for the upstream parser to
        # extract failed paths. It omits that imported-file blob from `items`.
        message = str(diagnostic.get("message", ""))
        is_dependency_build = is_build_stderr(message)
        if diagnostic.get("severity", 1) != 1 and not is_dependency_build:
            continue
        compact = dict(diagnostic)
        if not is_dependency_build and len(message) > remaining:
            compact["message"] = message[: max(0, remaining)] + "… [truncated]"
        remaining -= min(len(message), max(0, remaining))
        errors.append(compact)
        if remaining <= 0:
            break
    result = _ORIGINAL_PROCESS_DIAGNOSTICS(
        errors,
        build_success,
        severity="error",
        timed_out=timed_out,
        partial=partial,
        processing_lines=processing_lines,
    )
    dependency_paths = list(
        dict.fromkeys(
            path
            for diagnostic in errors
            if is_build_stderr(str(diagnostic.get("message", "")))
            for path in extract_failed_dependency_paths(str(diagnostic.get("message", "")))
        )
    )
    if dependency_paths:
        result.failed_dependencies = dependency_paths
    result.items.append(
        DiagnosticMessage(
            severity="info",
            message=f"{warning_count} target-file warning(s) suppressed",
            line=1,
            column=1,
        )
    )
    return result


def _bounded_tool_value(value: Any, remaining: list[int]) -> Any:
    """Copy a structured MCP result while enforcing one shared string budget."""

    if isinstance(value, str):
        if len(value) <= remaining[0]:
            remaining[0] -= len(value)
            return value
        kept = max(0, remaining[0] - 16)
        remaining[0] = 0
        return value[:kept] + "… [truncated]"
    if isinstance(value, BaseModel):
        updates = {
            name: _bounded_tool_value(item, remaining) for name, item in value.__dict__.items()
        }
        return value.model_copy(update=updates)
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        priority = (
            "status",
            "reason",
            "failed_dependencies",
            "partial",
            "success",
            "timed_out",
        )
        keys = [key for key in priority if key in value]
        keys.extend(key for key in value if key not in priority)
        for key in keys:
            if remaining[0] <= 0:
                break
            result[key] = _bounded_tool_value(value[key], remaining)
        return result
    if isinstance(value, (list, tuple)):
        items = []
        for item in value:
            if remaining[0] <= 0:
                break
            items.append(_bounded_tool_value(item, remaining))
        return type(value)(items)
    return value


def install_tool_output_bounds() -> None:
    """Bound every tool exposed to proof agents before FastMCP serializes it."""

    def wrap(original: Any) -> Any:
        async def bounded(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            return _bounded_tool_value(result, [_TOOL_OUTPUT_MAX_CHARS])

        return bounded

    for name, tool in server.mcp._tool_manager._tools.items():
        original = tool.fn
        if name in _BOUNDED_TOOL_NAMES:
            continue
        tool.fn = wrap(original)
        _BOUNDED_TOOL_NAMES.add(name)


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
        failures = _dependency_preparation_errors(document, path)
        failed_paths = list(
            dict.fromkeys(
                dependency
                for failure in failures
                for dependency in failure.get("failed_dependencies", [])
            )
        )
        detail = f" Failed dependencies: {', '.join(failed_paths)}." if failed_paths else ""
        raise LeanClientError(
            f"Lean could not prepare a usable dependency snapshot for {path}.{detail}"
        )
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
        # ScratchPool documents are virtual and intentionally have no file on
        # disk. Reopening one through Lake turns a useful dependency diagnostic
        # into a misleading ``_mcp_scratch_0.lean`` ENOENT.
        if (
            document is None
            or getattr(document, "virtual", False)
            or not _needs_dependency_refresh(document)
        ):
            return
        await _refresh_dependencies(
            self,
            path,
            observed_generation=observed_generation,
            timeout=timeout,
            original_barrier=original,
        )


server.mcp.remove_tool("lean_local_search")


@server.mcp.tool(
    "lean_local_search",
    annotations=ToolAnnotations(
        title="Local Project and Dependency Search",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def local_search(
    ctx: Context,
    query: Annotated[str, Field(description="Declaration name or prefix")],
    limit: Annotated[int, Field(description="Max matches", ge=1)] = 10,
    project_root: Annotated[
        str | None,
        Field(description="Lean root or PAF merged workspace (inferred if omitted)"),
    ] = None,
) -> LocalSearchResults:
    """Search declarations in the project, dependencies (including Mathlib), and stdlib."""

    lifespan = ctx.request_context.lifespan_context
    root_value: Path | str | None = project_root or getattr(lifespan, "lean_project_path", None)
    if root_value is None:
        raise server.LeanToolError("Lean project path not set. Call a file-based tool first.")
    try:
        root = normalize_lean_project_root(root_value)
        previous = getattr(lifespan, "lean_project_path", None)
        resolved_root = bind_lean_project_path(ctx, root)
        if previous is not None and Path(previous).resolve(strict=False) != resolved_root:
            await server._close_repl_for_project_switch(lifespan)
        policy = build_lean_path_policy(resolved_root)
        raw = await asyncio.to_thread(
            local_search_with_dependencies,
            query.strip(),
            limit,
            resolved_root,
            policy,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise server.LeanToolError(f"Search failed: {exc}") from exc
    return LocalSearchResults(
        items=[LocalSearchResult(name=r["name"], kind=r["kind"], file=r["file"]) for r in raw]
    )


async def _find_declaration_source(
    ctx: Context, symbol: str, file_path: str | None
) -> tuple[str, str]:
    """Return a resolvable source path and the spelling to locate within it."""

    if file_path:
        try:
            return str(resolve_lean_input_path(ctx, file_path)), symbol
        except (FileNotFoundError, OSError, ValueError) as exc:
            raise server.LeanToolError(str(exc)) from exc

    root = _set_normalized_lifespan_root(ctx)
    if root is None:
        raise server.LeanToolError(
            "A file_path is required until a Lean project has been selected."
        )
    policy = build_lean_path_policy(root)
    matches = await asyncio.to_thread(
        local_search_with_dependencies,
        symbol,
        32,
        root,
        policy,
    )
    exact = [
        match
        for match in matches
        if match["name"] == symbol or match["name"].rsplit(".", 1)[-1] == symbol
    ]
    if not exact:
        raise server.LeanToolError(
            f"No local project, dependency, or stdlib declaration matched `{symbol}`."
        )
    return str(resolve_lean_input_path(ctx, exact[0]["file"])), symbol.rsplit(".", 1)[-1]


server.mcp.remove_tool("lean_declaration_file")


@server.mcp.tool(
    "lean_declaration_file",
    annotations=ToolAnnotations(
        title="Declaration Source",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def declaration_file(
    ctx: Context,
    symbol: Annotated[str | None, Field(description="Fully qualified or bare symbol")] = None,
    file_path: Annotated[
        str | None,
        Field(description="Optional source/usage file; module-style Mathlib paths are accepted"),
    ] = None,
    declaration: Annotated[str | None, Field(description="Alias for symbol")] = None,
    declaration_name: Annotated[str | None, Field(description="Alias for symbol")] = None,
    context_lines: Annotated[
        int, Field(description="Lines of context around the declaration", ge=0)
    ] = 20,
    full_file: Annotated[
        bool, Field(description="Return the entire declaration file (large!)")
    ] = False,
) -> DeclarationInfo:
    """Find declaration source by symbol; a usage file is optional."""

    requested = symbol or declaration or declaration_name
    if not requested:
        raise server.LeanToolError(
            "Provide `symbol` (the aliases `declaration` and `declaration_name` also work)."
        )
    source_path, source_spelling = await _find_declaration_source(ctx, requested, file_path)
    spellings = dict.fromkeys((requested, source_spelling, requested.rsplit(".", 1)[-1]))
    last_error: Exception | None = None
    for spelling in spellings:
        try:
            return await _ORIGINAL_DECLARATION_FILE(
                ctx,
                source_path,
                spelling,
                context_lines=context_lines,
                full_file=full_file,
            )
        except server.LeanToolError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def _hover_columns(line_context: str, column: int) -> list[int]:
    """Return identifier starts ordered by proximity to the requested column."""

    requested = max(column - 1, 0)
    matches = list(_LEAN_IDENTIFIER_RE.finditer(line_context))
    matches.sort(
        key=lambda match: (
            0 if match.start() <= requested < match.end() else 1,
            min(abs(requested - match.start()), abs(requested - max(match.end() - 1, 0))),
        )
    )
    return [match.start() + 1 for match in matches if match.group() not in _TACTIC_KEYWORDS]


server.mcp.remove_tool("lean_hover_info")


@server.mcp.tool(
    "lean_hover_info",
    annotations=ToolAnnotations(
        title="Hover Info",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def hover(
    ctx: Context,
    file_path: Annotated[
        str, Field(description="Absolute or project-root-relative path to Lean file")
    ],
    line: Annotated[int, Field(description="Line number (1-indexed)", ge=1)],
    column: Annotated[
        int,
        Field(description="Approximate identifier column (1-indexed characters)", ge=1),
    ],
) -> HoverInfo:
    """Get symbol information, snapping an imprecise cursor to nearby identifiers."""

    rel_path = await setup_client_for_resolved_file(ctx, file_path)
    if not rel_path:
        server._raise_invalid_path(file_path)
    client: AsyncLeanLSPClient = ctx.request_context.lifespan_context.client
    await open_synced(ctx, rel_path)
    lines = client.content(rel_path).splitlines()
    if not 0 < line <= len(lines):
        raise server.LeanToolError(f"Line {line} out of range (file has {len(lines)} lines)")
    line_context = lines[line - 1]
    requested_token = next(
        (
            match.group()
            for match in _LEAN_IDENTIFIER_RE.finditer(line_context)
            if match.start() <= column - 1 < match.end()
        ),
        "",
    )
    candidates = _hover_columns(line_context, column)
    if requested_token not in _TACTIC_KEYWORDS:
        candidates.insert(0, column)

    last_error: Exception | None = None
    for candidate in dict.fromkeys(candidates):
        try:
            return await _ORIGINAL_HOVER(
                ctx, str(resolve_lean_input_path(ctx, file_path)), line, candidate
            )
        except server.LeanToolError as exc:
            last_error = exc
    raise server.LeanToolError(
        f"No hover information near line {line}, column {column}; "
        f"tried {len(set(candidates))} identifier position(s)."
    ) from last_error


def _unavailable_payload(error: Exception | str) -> dict[str, Any]:
    message = str(error)
    failed = extract_failed_dependency_paths(message)
    if not failed and "Failed dependencies:" in message:
        rendered = message.partition("Failed dependencies:")[2].strip().removesuffix(".")
        failed = [path.strip() for path in rendered.split(",") if path.strip()]
    status = (
        "dependency_unavailable"
        if failed or is_build_stderr(message)
        else "elaboration_unavailable"
    )
    return {"status": status, "reason": message, "failed_dependencies": failed}


server.mcp.remove_tool("lean_diagnostic_messages")


@server.mcp.tool(
    "lean_diagnostic_messages",
    annotations=ToolAnnotations(
        title="Diagnostics",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def diagnostic_messages(
    ctx: Context,
    file_path: str,
    start_line: int | None = None,
    end_line: int | None = None,
    declaration_name: str | None = None,
    interactive: bool = False,
    timeout_s: float | None = None,
    severity: Literal["error", "warning", "info", "hint"] | None = None,
) -> dict[str, Any]:
    """Return diagnostics with an explicit readiness status and failure reason."""

    try:
        result = await _ORIGINAL_DIAGNOSTICS(
            ctx,
            str(resolve_lean_input_path(ctx, file_path)),
            start_line=start_line,
            end_line=end_line,
            declaration_name=declaration_name,
            interactive=interactive,
            timeout_s=timeout_s,
            severity=severity,
        )
    except (LeanClientError, OSError, ValueError) as exc:
        unavailable = _unavailable_payload(exc)
        return {
            "partial": False,
            "success": False,
            "timed_out": False,
            "items": [],
            **unavailable,
        }

    payload = result.model_dump(mode="json")
    if interactive:
        return {**payload, "status": "ready", "reason": None}
    if payload.get("partial"):
        status, reason = "still_elaborating", "Lean is still elaborating the requested file."
    elif payload.get("failed_dependencies"):
        status = "dependency_unavailable"
        reason = "One or more imported Lean files failed to build."
    elif any(
        str(item.get("message", "")).startswith("diagnostics_unavailable:")
        for item in payload.get("items", [])
    ):
        status = "elaboration_unavailable"
        reason = "Lean did not produce a complete diagnostic snapshot."
    else:
        status, reason = "ready", None
    return {**payload, "status": status, "reason": reason}


async def _goal_unavailable_reason(client: Any, rel_path: str) -> dict[str, Any] | None:
    try:
        report = await client.diagnostics(rel_path, fresh=False)
    except LeanClientError as exc:
        return _unavailable_payload(exc)
    errors = [item for item in report.items if item.get("severity", 1) == 1]
    if not (report.fatal_error or report.has_errors or errors):
        return None
    messages = [str(item.get("message", "")) for item in errors]
    failed = [path for message in messages for path in extract_failed_dependency_paths(message)]
    return {
        "status": "dependency_unavailable" if failed else "elaboration_unavailable",
        "reason": next((message for message in messages if message), "Lean elaboration failed."),
        "failed_dependencies": list(dict.fromkeys(failed)),
    }


server.mcp.remove_tool("lean_goal")


@server.mcp.tool(
    "lean_goal",
    annotations=ToolAnnotations(
        title="Proof Goals",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def goal(
    ctx: Context,
    file_path: str,
    line: int,
    column: int | None = None,
    format: Literal["text", "structured"] = "text",
    timeout_s: float | None = None,
) -> dict[str, Any]:
    """Get goals while distinguishing no goal from unavailable elaboration."""

    rel_path = await setup_client_for_resolved_file(ctx, file_path)
    if not rel_path:
        server._raise_invalid_path(file_path)
    client: AsyncLeanLSPClient = ctx.request_context.lifespan_context.client
    try:
        await open_synced(ctx, rel_path)
    except LeanClientError as exc:
        return {"line_context": "", "goals": [], **_unavailable_payload(exc)}
    lines = client.content(rel_path).splitlines()
    if not 0 < line <= len(lines):
        raise server.LeanToolError(f"Line {line} out of range (file has {len(lines)} lines)")
    line_context = lines[line - 1]

    try:
        await client.barrier(rel_path, timeout=timeout_s)
    except LeanRequestTimeout:
        return {
            "line_context": line_context,
            "goals": [],
            "status": "still_elaborating",
            "reason": "Lean is still elaborating the requested file.",
        }

    def render(result: Any) -> list[Any]:
        goals = server._goal_strings(result)
        if format == "structured":
            return [server._goal_to_structured(item).model_dump(mode="json") for item in goals]
        return goals

    if column is not None:
        result = await client.goal(rel_path, line - 1, column - 1, fresh=False)
        if result.status == "goals":
            return {"line_context": line_context, "goals": render(result), "status": "goals"}
        unavailable = await _goal_unavailable_reason(client, rel_path)
        if unavailable is not None:
            return {"line_context": line_context, "goals": [], **unavailable}
        status = "no_goal_at_position" if result.status == "no_goal" else result.status
        return {"line_context": line_context, "goals": [], "status": status, "reason": None}

    start = next((index for index, char in enumerate(line_context) if not char.isspace()), 0)
    before = await client.goal(rel_path, line - 1, start, fresh=False)
    after = await client.goal(rel_path, line - 1, len(line_context), fresh=False)
    payload = {
        "line_context": line_context,
        "goals_before": render(before),
        "goals_after": render(after),
    }
    if "goals" in {before.status, after.status}:
        return {**payload, "status": "goals", "reason": None}
    unavailable = await _goal_unavailable_reason(client, rel_path)
    if unavailable is not None:
        return {**payload, **unavailable}
    status = "complete" if "complete" in {before.status, after.status} else "no_goal_at_position"
    return {**payload, "status": status, "reason": None}


def resolve_attempt_line(
    source: str,
    line: int | None,
    declaration_name: str | None,
    placeholder: str | None,
) -> int:
    """Resolve a stable declaration/placeholder anchor to a 1-indexed source line."""

    if placeholder:
        positions = [match.start() for match in re.finditer(re.escape(placeholder), source)]
        if len(positions) != 1:
            raise server.LeanToolError(
                f"Placeholder anchor must occur exactly once; found {len(positions)} occurrences."
            )
        return source.count("\n", 0, positions[0]) + 1
    if declaration_name:
        declarations = list(_LEAN_DECLARATION_RE.finditer(source))
        target_index = next(
            (
                index
                for index, match in enumerate(declarations)
                if match.group("name") == declaration_name
                or match.group("name").rsplit(".", 1)[-1] == declaration_name.rsplit(".", 1)[-1]
            ),
            None,
        )
        if target_index is None:
            raise server.LeanToolError(f"Declaration `{declaration_name}` was not found.")
        start = declarations[target_index].start()
        end = (
            declarations[target_index + 1].start()
            if target_index + 1 < len(declarations)
            else len(source)
        )
        body = source[start:end]
        sorry = re.search(r"\bsorry\b", body)
        if sorry is None:
            raise server.LeanToolError(
                f"Declaration `{declaration_name}` has no `sorry`; provide `line` or `placeholder`."
            )
        return source.count("\n", 0, start + sorry.start()) + 1
    if line is None:
        raise server.LeanToolError("Provide `line`, `declaration_name`, or a unique `placeholder`.")
    return line


server.mcp.remove_tool("lean_multi_attempt")


@server.mcp.tool(
    "lean_multi_attempt",
    annotations=ToolAnnotations(
        title="Multi-Attempt",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def multi_attempt(
    ctx: Context,
    file_path: str,
    snippets: list[str],
    line: int | None = None,
    column: int | None = None,
    declaration_name: str | None = None,
    placeholder: str | None = None,
) -> dict[str, Any]:
    """Try tactics using a line or a stable declaration/placeholder anchor."""

    try:
        resolved = resolve_lean_input_path(ctx, file_path)
        source = resolved.read_text(encoding="utf-8")
        target_line = resolve_attempt_line(source, line, declaration_name, placeholder)
        result = await _ORIGINAL_MULTI_ATTEMPT(
            ctx,
            str(resolved),
            target_line,
            snippets,
            column=column,
        )
    except (LeanClientError, FileNotFoundError, OSError, ValueError) as exc:
        return {"items": [], **_unavailable_payload(exc)}
    return {**result.model_dump(mode="json"), "status": "ready", "reason": None}


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
    server_any: Any = server
    server_any._process_diagnostics = compact_target_diagnostics
    server_any.require_lean_project_path = require_normalized_lean_project_path
    server_any.resolve_file_path = resolve_lean_input_path
    server_any.setup_client_for_file = setup_client_for_resolved_file
    server_any.lean_local_search = local_search_with_dependencies
    install_tool_output_bounds()


def main() -> int:
    install_dependency_reopens()
    return upstream_main()


if __name__ == "__main__":
    raise SystemExit(main())
