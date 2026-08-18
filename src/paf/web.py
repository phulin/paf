from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response
from starlette.routing import Route

from paf import json_codec as json
from paf.config import resolve_config
from paf.hashing import digest_text
from paf.models import PipelineConfig
from paf.project import Project, ProjectResolver
from paf.state_db import DATABASE_NAME, StateDatabase, read_checkpoint, read_status_view

_DECLARATION = re.compile(
    r"^\s*(?:(?:noncomputable|private|protected|unsafe|opaque)\s+)*"
    r"(theorem|lemma|def|abbrev|structure|class|instance)\s+([^\s([{:=]+)"
)
_HASHED_ASSET = re.compile(r"^assets/.+-[A-Za-z0-9_-]{8,}\.[^.]+$")
_MAX_BROWSE_BYTES = 2 * 1024 * 1024


class WebPathError(ValueError):
    """A browser or state path escaped its explicitly configured root."""


@dataclass(frozen=True)
class StateCandidate:
    id: str
    directory: Path
    modified: float
    state: dict[str, Any]


@dataclass
class CpuSampler:
    previous: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    @staticmethod
    def _times() -> tuple[int, int] | None:
        try:
            first = Path("/proc/stat").read_text().splitlines()[0]
            values = [int(value) for value in first.split()[1:]]
        except (OSError, ValueError, IndexError):
            return None
        if len(values) < 4:
            return None
        return values[3] + (values[4] if len(values) > 4 else 0), sum(values)

    def percent(self) -> float | None:
        current = self._times()
        if current is None:
            return None
        with self._lock:
            previous, self.previous = self.previous, current
        if previous is None:
            return None
        idle = current[0] - previous[0]
        total = current[1] - previous[1]
        if total <= 0:
            return None
        return max(0.0, min(100.0, (1 - idle / total) * 100))


def _contained(root: Path, supplied: str, *, must_exist: bool = True) -> Path:
    """Resolve a URL-derived relative path without permitting traversal or symlink escapes."""

    if "\x00" in supplied:
        raise WebPathError("path contains a null byte")
    path = PurePosixPath(supplied or ".")
    if path.is_absolute() or any(part == ".." for part in path.parts):
        raise WebPathError("path must be relative and remain inside its configured root")
    root = root.resolve()
    candidate = root.joinpath(*path.parts).resolve(strict=False)
    if not candidate.is_relative_to(root):
        raise WebPathError("path escapes its configured root")
    if must_exist and not candidate.exists():
        raise FileNotFoundError(supplied)
    # resolve() also catches a final symlink; reject all symlinked ancestors explicitly so
    # browser results cannot change meaning between validation and read.
    cursor = root
    for part in path.parts:
        if part in {"", "."}:
            continue
        cursor /= part
        if cursor.is_symlink():
            raise WebPathError("symlinked paths are not browsable")
    return candidate


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _state_candidates(state_root: Path, *, dashboard: bool = False) -> list[StateCandidate]:
    state_root = state_root.resolve()
    directories = [state_root]
    try:
        directories.extend(
            entry for entry in state_root.iterdir() if entry.is_dir() and not entry.is_symlink()
        )
    except FileNotFoundError:
        return []
    result: list[StateCandidate] = []
    for directory in directories:
        try:
            resolved = directory.resolve()
            if not resolved.is_relative_to(state_root):
                continue
            state_path = resolved / "state.json"
            database_path = resolved / DATABASE_NAME
            if not state_path.is_file() and not database_path.is_file():
                continue
            state = read_checkpoint(resolved) if dashboard else read_status_view(resolved)
            if not isinstance(state, dict):
                continue
            modified = max(
                path.stat().st_mtime for path in (state_path, database_path) if path.is_file()
            )
            raw_id = state.get("swarm_id")
            candidate_id = str(raw_id) if isinstance(raw_id, str) and raw_id else resolved.name
            if any(item.id == candidate_id for item in result):
                candidate_id = _relative(resolved, state_root) or state_root.name
            result.append(StateCandidate(candidate_id, resolved, modified, state))
        except (OSError, ValueError):
            continue
    return sorted(result, key=lambda item: item.modified, reverse=True)


def _summary(candidate: StateCandidate) -> dict[str, Any]:
    tasks = candidate.state.get("tasks", {})
    tasks = tasks if isinstance(tasks, dict) else {}
    task_values = [task for task in tasks.values() if isinstance(task, dict)]
    agents = candidate.state.get("agents", {})
    agents = agents if isinstance(agents, dict) else {}
    build = candidate.state.get("coordinator_build", {})
    build = build if isinstance(build, dict) else {}
    metrics = candidate.state.get("projection_metrics", {})
    metrics = metrics if isinstance(metrics, dict) else {}

    def number(value: object) -> int:
        if not isinstance(value, bool | int | float | str):
            return 0
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    active_agents = number(agents.get("active", 0))
    document_ids = {
        task.get("document_id", task.get("book_id"))
        for task in task_values
        if task.get("document_id", task.get("book_id"))
    }
    book_ids = {task.get("book_id") for task in task_values if task.get("book_id")}
    return {
        "id": candidate.id,
        "active": active_agents > 0 or build.get("active") is True,
        "updated_at": str(
            candidate.state.get(
                "updated_at", datetime.fromtimestamp(candidate.modified, UTC).isoformat()
            )
        ),
        "active_agents": active_agents,
        "maximum_agents": number(agents.get("maximum", 0)),
        "queued_agents": number(agents.get("queued", 0)),
        "running_tasks": number(
            metrics.get(
                "running_tasks",
                sum(task.get("status") == "running" for task in task_values),
            )
        ),
        "task_count": number(metrics.get("task_count", len(tasks))),
        "document_count": number(metrics.get("document_count", len(document_ids))),
        "book_count": number(metrics.get("document_count", len(book_ids))),
        "revision": number(candidate.state.get("revision", 0)),
    }


def _activity(candidate: StateCandidate, run_id: str) -> dict[str, Any] | None:
    try:
        path = _contained(candidate.directory / "logs", f"{run_id}.activity.json")
        value = json.loads(path.read_bytes())
    except (FileNotFoundError, OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _snapshot(candidate: StateCandidate, project_root: Path) -> dict[str, Any]:
    state = dict(candidate.state)
    tasks = state.get("tasks", {})
    recent: list[str] = []
    if isinstance(tasks, dict):
        task_values = [task for task in tasks.values() if isinstance(task, dict)]
        task_values.sort(key=lambda task: str(task.get("updated_at", "")), reverse=True)
        for task in task_values:
            run_id = task.get("latest_run_id")
            if isinstance(run_id, str) and run_id not in recent:
                recent.append(run_id)
            if len(recent) >= 36:
                break
    shepherd = state.get("shepherd", {})
    if isinstance(shepherd, dict):
        for agent in shepherd.get("agents", []):
            run_id = agent.get("run_id") if isinstance(agent, dict) else None
            if isinstance(run_id, str) and run_id and run_id not in recent:
                recent.append(run_id)
    activities = {
        run_id: activity
        for run_id in recent
        if (activity := _activity(candidate, run_id)) is not None
    }
    database_path = candidate.directory / DATABASE_NAME
    source_path = database_path if database_path.is_file() else candidate.directory / "state.json"
    try:
        source = source_path.relative_to(project_root).as_posix()
    except ValueError:
        source = str(source_path)
    return state | {"swarm_id": candidate.id, "source": source, "activities": activities}


def _words(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    value = re.sub(r"([A-Z])([A-Z][a-z])", r"\1 \2", value)
    return value.replace("_", " ").strip()


def _doc(lines: list[str], index: int) -> str:
    collected: list[str] = []
    cursor = index - 1
    while cursor >= 0 and "/--" not in lines[cursor]:
        if lines[cursor].strip() and not lines[cursor].strip().startswith("@["):
            collected.insert(0, lines[cursor])
        if index - cursor > 12:
            return ""
        cursor -= 1
    if cursor < 0:
        return ""
    collected.insert(0, lines[cursor])
    value = "\n".join(collected)
    value = re.sub(r"^\s*/--?", "", value)
    value = re.sub(r"-/\s*$", "", value)
    value = re.sub(r"^\s*\*\s?", "", value, flags=re.MULTILINE)
    value = re.sub(r"`([^`]+)`", r"\1", value)
    return re.sub(r"\s+", " ", value).strip()


def _signature(lines: list[str], start: int, stop: int) -> str:
    header: list[str] = []
    for line in lines[start : min(stop, start + 30)]:
        if ":=" in line:
            header.append(f"{line.split(':=', 1)[0].rstrip()} := …")
            break
        if re.search(r"\bwhere\s*$", line):
            header.append(f"{re.sub(r'\bwhere\s*$', '', line).rstrip()} where …")
            break
        header.append(line.rstrip())
        if re.match(r"^\s*(structure|class)\b", lines[start]) and "where" in line:
            break
    return "\n".join(header).strip()


def _parse_declarations(path: Path, target_root: Path, project_root: Path) -> list[dict[str, Any]]:
    try:
        relative = path.relative_to(project_root).as_posix()
    except ValueError:
        relative = path.relative_to(target_root).as_posix()
    source = path.read_text(encoding="utf-8")
    lines = source.split("\n")
    starts: list[tuple[int, str, str]] = []
    for index, line in enumerate(lines):
        if match := _DECLARATION.match(line):
            starts.append((index, match.group(1), match.group(2)))
    parts = relative.split("/")
    book_part = next((part for part in parts if re.match(r"^Book\d+", part)), "Book00Unknown")
    book_match = re.match(r"^Book(\d+)(.*)$", book_part)
    chapter_part = next((part for part in parts if re.match(r"^Chapter\d+", part)), "Chapter00")
    chapter_match = re.match(r"^Chapter(\d+)", chapter_part)
    section_match = re.match(r"^Section(\d+)(.*)$", path.stem)
    book_number = int(book_match.group(1)) if book_match else 0
    book = _words(book_match.group(2) if book_match else "Unknown")
    chapter = int(chapter_match.group(1)) if chapter_match else 0
    section = (
        f"{int(section_match.group(1))}.{_words(section_match.group(2))}"
        if section_match
        else _words(path.stem)
    )
    result: list[dict[str, Any]] = []
    for position, (start, kind, name) in enumerate(starts):
        stop = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
        doc = _doc(lines, start)
        excerpt_end = min(stop, start + 30)
        signature = _signature(lines, start, stop)
        body = "\n".join(lines[start:stop])
        identifier = digest_text(f"{relative}:{start + 1}:{name}")[:12]
        result.append(
            {
                "id": identifier,
                "name": name,
                "kind": kind,
                "signature": signature,
                "excerpt": "\n".join(
                    lines[max(0, start - (1 if doc else 0)) : excerpt_end]
                ).rstrip(),
                "doc": doc,
                "path": relative,
                "line": start + 1,
                "endLine": excerpt_end,
                "book": book,
                "bookNumber": book_number,
                "chapter": chapter,
                "section": section,
                "status": "sorry" if re.search(r"\bsorry\b", body) else "proved",
                "search": f"{name} {signature} {doc} {book} {section}".casefold(),
            }
        )
    return result


def _declarations(target_root: Path, project_root: Path) -> list[dict[str, Any]]:
    if not target_root.is_dir():
        return []
    result: list[dict[str, Any]] = []
    root = target_root.resolve()
    for path in sorted(target_root.rglob("*.lean")):
        try:
            resolved = path.resolve()
            if path.is_symlink() or not resolved.is_relative_to(root) or not resolved.is_file():
                continue
            result.extend(_parse_declarations(resolved, root, project_root))
        except (OSError, UnicodeError):
            continue
    return sorted(
        result,
        key=lambda item: (item["bookNumber"], item["chapter"], item["path"], item["line"]),
    )


def _memory() -> tuple[int, int]:
    try:
        values: dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            name, raw = line.split(":", 1)
            values[name] = int(raw.strip().split()[0]) * 1024
        total = values["MemTotal"]
        available = values.get("MemAvailable", values.get("MemFree", 0))
        return total - available, total
    except (OSError, ValueError, KeyError):
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        total = page_size * int(os.sysconf("SC_PHYS_PAGES"))
        available = page_size * int(os.sysconf("SC_AVPHYS_PAGES"))
        return total - available, total


def _json_error(message: str, status: int) -> JSONResponse:
    return JSONResponse(
        {"error": message}, status_code=status, headers={"Cache-Control": "no-store"}
    )


def _query_path(request: Request) -> str:
    values = request.query_params.getlist("path")
    if len(values) > 1:
        raise WebPathError("pass path only once")
    return values[0] if values else "."


def _allowed_source(path: Path, source_roots: tuple[Path, ...]) -> bool:
    for root in source_roots:
        resolved = root.resolve()
        if resolved.is_dir() and path.is_relative_to(resolved):
            return True
        if path == resolved:
            return True
    return False


def _source_visible(path: Path, source_roots: tuple[Path, ...]) -> bool:
    return _allowed_source(path, source_roots) or any(
        source.is_relative_to(path) for source in source_roots
    )


def _browser_payload(
    path: Path, root: Path, *, allowed_sources: tuple[Path, ...] = ()
) -> dict[str, Any]:
    if allowed_sources and not _source_visible(path, allowed_sources):
        raise WebPathError("path is outside the configured source roots")
    relative = _relative(path, root)
    if path.is_dir():
        entries: list[dict[str, Any]] = []
        children = sorted(
            path.iterdir(), key=lambda item: (not item.is_dir(), item.name.casefold())
        )
        for child in children:
            try:
                resolved = child.resolve()
                if child.is_symlink() or not resolved.is_relative_to(root):
                    continue
                if allowed_sources and not (_source_visible(resolved, allowed_sources)):
                    continue
                stat = child.stat()
                entries.append(
                    {
                        "name": child.name,
                        "path": _relative(resolved, root),
                        "type": "directory" if child.is_dir() else "file",
                        "size": stat.st_size,
                        "modified": stat.st_mtime,
                    }
                )
            except OSError:
                continue
        return {"path": relative, "type": "directory", "entries": entries}
    if not path.is_file():
        raise WebPathError("only regular files and directories are browsable")
    stat = path.stat()
    if stat.st_size > _MAX_BROWSE_BYTES:
        raise OverflowError(f"file exceeds {_MAX_BROWSE_BYTES} byte browse limit")
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as error:
        raise WebPathError("file is not UTF-8 text") from error
    return {"path": relative, "type": "file", "size": stat.st_size, "content": content}


def create_app(
    project: Project | PipelineConfig | str | Path | None = None,
    *,
    static_dir: str | Path | None = None,
) -> Starlette:
    """Create the project-scoped ASGI application used by both CLI and tests."""

    if isinstance(project, PipelineConfig):
        config = project
    else:
        resolved = (
            project if isinstance(project, Project) else ProjectResolver().resolve(project=project)
        )
        config = resolve_config(config=None, target=None, dependency_file=None, project=resolved)
    if config.project is None:
        raise ValueError("resolved configuration has no project metadata")
    project_root = config.project.root.resolve()
    source_roots = tuple(path.resolve() for path in config.project.source_paths)
    target_root = (
        config.project.target_dir or (project_root / config.settings.lean_project)
    ).resolve()
    state_root = (config.project.state_dir or config.settings.state_dir).resolve()
    cpu = CpuSampler()

    async def list_runs(_request: Request) -> Response:
        summaries = [_summary(candidate) for candidate in _state_candidates(state_root)]
        summaries.sort(
            key=lambda item: (bool(item["active"]), str(item["updated_at"])),
            reverse=True,
        )
        return JSONResponse(
            {"swarms": summaries, "runs": summaries},
            headers={"Cache-Control": "no-store"},
        )

    async def snapshot(request: Request) -> Response:
        candidates = _state_candidates(state_root, dashboard=True)
        requested = request.query_params.get("swarm") or request.path_params.get("run_id")
        if requested:
            candidate = next((item for item in candidates if item.id == requested), None)
            if candidate is None:
                return _json_error(f"Unknown swarm: {requested}", 404)
        else:
            candidate = next((item for item in candidates if _summary(item)["active"]), None)
            candidate = candidate or (candidates[0] if candidates else None)
        if candidate is None:
            return _json_error("No PAF state found", 404)
        return JSONResponse(
            _snapshot(candidate, project_root), headers={"Cache-Control": "no-store"}
        )

    async def changes(request: Request) -> Response:
        candidates = _state_candidates(state_root)
        requested = request.query_params.get("swarm")
        candidate = next((item for item in candidates if item.id == requested), None)
        candidate = candidate or (candidates[0] if not requested and candidates else None)
        if candidate is None:
            return _json_error(
                f"Unknown swarm: {requested}" if requested else "No PAF state found", 404
            )
        try:
            after = int(request.query_params.get("after", "0"))
        except ValueError:
            return _json_error("after must be an integer revision", 400)
        database = StateDatabase(candidate.directory)
        if not database.path.is_file():
            result: dict[str, Any] = {
                "revision": 0,
                "resync_required": True,
                "changes": [],
            }
        elif request.query_params.get("view") == "dashboard":
            result = database.dashboard_delta(after)
            run_ids = {
                *result.get("run_ids", []),
                *result.get("active_run_ids", []),
            }
            tasks = result.get("tasks", {})
            if isinstance(tasks, dict):
                run_ids.update(
                    run_id
                    for task in tasks.values()
                    if isinstance(task, dict)
                    and isinstance((run_id := task.get("latest_run_id")), str)
                )
            globals_ = result.get("globals", {})
            shepherd = globals_.get("shepherd", {}) if isinstance(globals_, dict) else {}
            if isinstance(shepherd, dict):
                run_ids.update(
                    run_id
                    for agent in shepherd.get("agents", [])
                    if isinstance(agent, dict)
                    and isinstance((run_id := agent.get("run_id")), str)
                    and run_id
                )
            result["activities"] = {
                run_id: activity
                for run_id in sorted(run_ids)
                if (activity := _activity(candidate, run_id)) is not None
            }
        else:
            result = database.changes_since(after)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    async def system(_request: Request) -> Response:
        used, total = _memory()
        return JSONResponse(
            {
                "cpu_percent": cpu.percent(),
                "memory_used_bytes": used,
                "memory_total_bytes": total,
                "memory_percent": used / total * 100 if total else 0,
            },
            headers={"Cache-Control": "no-store"},
        )

    async def statements(request: Request) -> Response:
        query = request.query_params.get("q", "").strip().casefold()
        book = request.query_params.get("book", "all")
        kind = request.query_params.get("kind", "all")
        status = request.query_params.get("status", "all")
        try:
            limit = min(250, max(1, int(request.query_params.get("limit", "120"))))
        except ValueError:
            return _json_error("limit must be an integer", 400)
        all_items = _declarations(target_root, project_root)
        filtered = [
            item
            for item in all_items
            if (not query or query in item["search"])
            and (book == "all" or str(item["bookNumber"]) == book)
            and (kind == "all" or item["kind"] == kind)
            and (status == "all" or item["status"] == status)
        ]
        books: dict[int, dict[str, Any]] = {}
        kinds: dict[str, int] = {}
        statuses = {"proved": 0, "sorry": 0}
        for item in all_items:
            current = books.setdefault(item["bookNumber"], {"label": item["book"], "count": 0})
            current["count"] += 1
            kinds[item["kind"]] = kinds.get(item["kind"], 0) + 1
            statuses[item["status"]] += 1
        declarations = [
            {key: value for key, value in item.items() if key != "search"}
            for item in filtered[:limit]
        ]
        return JSONResponse(
            {
                "source": "repository",
                "total": len(filtered),
                "declarations": declarations,
                "facets": {
                    "books": [
                        {"id": str(number), "number": number, **value}
                        for number, value in sorted(books.items())
                    ],
                    "kinds": kinds,
                    "statuses": statuses,
                },
            },
            headers={"Cache-Control": "no-store"},
        )

    async def browse(request: Request) -> Response:
        kind = request.path_params.get("kind") or request.query_params.get("root")
        if kind == "sources":
            kind = "source"
        elif kind == "targets":
            kind = "target"
        if kind not in {"source", "target"}:
            return _json_error("root must be source or target", 400)
        root = project_root if kind == "source" else target_root
        try:
            path = _contained(root, _query_path(request))
            payload = _browser_payload(
                path,
                root,
                allowed_sources=source_roots if kind == "source" else (),
            )
        except FileNotFoundError:
            return _json_error("path not found", 404)
        except WebPathError as error:
            return _json_error(str(error), 403)
        except OverflowError as error:
            return _json_error(str(error), 413)
        return JSONResponse({"root": kind, **payload}, headers={"Cache-Control": "no-store"})

    if static_dir is None:
        static_root = Path(str(resources.files("paf").joinpath("web_dist"))).resolve()
    else:
        static_root = Path(static_dir).resolve()
    if not (static_root / "index.html").is_file():
        raise ValueError(f"packaged web UI is missing index.html: {static_root}")

    async def static(request: Request) -> Response:
        raw = request.path_params.get("path", "")
        try:
            candidate = _contained(static_root, raw)
        except WebPathError as error:
            return _json_error(str(error), 403)
        except FileNotFoundError:
            if raw.startswith("assets/"):
                return _json_error("asset not found", 404)
            candidate = static_root / "index.html"
        if not candidate.is_file():
            candidate = static_root / "index.html"
        headers: dict[str, str] = {}
        relative = candidate.relative_to(static_root).as_posix()
        if _HASHED_ASSET.fullmatch(relative):
            headers["Cache-Control"] = "public, max-age=31536000, immutable"
        elif relative == "index.html":
            headers["Cache-Control"] = "no-cache"
        return FileResponse(candidate, headers=headers)

    async def unknown_api(_request: Request) -> Response:
        return _json_error("API endpoint not found", 404)

    async def server_error(_request: Request, _error: Exception) -> Response:
        return _json_error("Internal server error", 500)

    routes = [
        Route("/api/swarms", list_runs),
        Route("/api/runs", list_runs),
        Route("/api/swarm", snapshot),
        Route("/api/changes", changes),
        Route("/api/snapshots", snapshot),
        Route("/api/snapshots/{run_id:str}", snapshot),
        Route("/api/system", system),
        Route("/api/statements", statements),
        Route("/api/declarations", statements),
        Route("/api/files", browse),
        Route("/api/{kind:str}", browse),
        Route("/api/{path:path}", unknown_api),
        Route("/{path:path}", static),
    ]
    return Starlette(routes=routes, exception_handlers={Exception: server_error})


def run_web(config: PipelineConfig, *, host: str, port: int) -> None:
    """Run the installed web service until interrupted."""

    import uvicorn

    if not 1 <= port <= 65535:
        raise ValueError("--port must be between 1 and 65535")
    uvicorn.run(create_app(config), host=host, port=port, log_level="info")
