from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lastlib_swarm import json_codec as json

MAX_RECENT_EVENTS = 80
MAX_DETAIL_CHARS = 800
EXIT_STATUS_ONLY = re.compile(r"^exit\s+\d+$", re.IGNORECASE)
SHELL_COMMAND_WRAPPER = re.compile(r"^/bin/bash\s+-lc\s+(['\"])(.*)\1$", re.DOTALL)
LEAN_BOOK_PATH = re.compile(
    r"""
    (?:(?:[^\s'"`|;,()\[\]{}]+/)*lean/)?
    LastLib/
    Book(?P<book>\d+)[A-Za-z0-9_]*/
    Chapter(?P<chapter>\d+)
    (?:
        \.lean
        |
        /
        (?:
            Section(?P<section>\d+)(?P<title>[A-Za-z0-9_]*)
            |
            (?P<special>Dependencies|Core)
        )
        \.lean
    )
    """,
    re.VERBOSE,
)


def activity_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _compact(value: str, *, limit: int = MAX_DETAIL_CHARS) -> str:
    value = " ".join(value.split())
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _words(identifier: str) -> str:
    identifier = identifier.replace("_", " ")
    identifier = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", identifier)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", identifier).strip()


def _shorten_book_paths(value: str) -> str:
    def label(match: re.Match[str]) -> str:
        book = int(match.group("book"))
        chapter = int(match.group("chapter"))
        description = f"Book {book} Chap {chapter}"
        if section := match.group("section"):
            description += f" Sec {int(section)}"
            if title := _words(match.group("title")):
                description += f": {title}"
        elif special := match.group("special"):
            description += f" {special}"
        return f"[{description}]"

    return LEAN_BOOK_PATH.sub(label, value)


def _display_command(command: str) -> str:
    command = command.strip()
    if match := SHELL_COMMAND_WRAPPER.fullmatch(command):
        command = match.group(2)
    return _shorten_book_paths(command)


def _result_text(value: Any) -> str:
    texts: list[str] = []

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                texts.append(item["text"])
            else:
                for child in item.values():
                    visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return _compact(" ".join(texts))


def error_signature(message: str) -> str:
    """Collapse path-dependent errors into a useful cross-agent alert."""

    if "No such file or directory: 'lake'" in message:
        return "Lean MCP cannot find lake"
    message = re.sub(r"/tmp/lastlib-swarm-[^\s:'\"]+", "<workspace>", message)
    return _compact(message, limit=180)


def reportable_error(message: str) -> str:
    """Return meaningful diagnostic text, excluding a bare process exit status."""

    stripped = message.strip()
    return "" if not stripped or EXIT_STATUS_ONLY.fullmatch(stripped) else stripped


@dataclass
class ActivityEntry:
    sequence: int
    at: str
    kind: str
    status: str
    title: str
    detail: str = ""


@dataclass
class AgentActivity:
    run_id: str
    chapter_id: str
    stage: str
    started_at: str = field(default_factory=activity_timestamp)
    updated_at: str = field(default_factory=activity_timestamp)
    finished_at: str | None = None
    sequence: int = 0
    current: str = "starting Codex"
    current_kind: str = "agent"
    commands: int = 0
    command_failures: int = 0
    mcp_calls: int = 0
    mcp_failures: int = 0
    file_changes: int = 0
    latest_summary: str = ""
    latest_error: str = ""
    todos: list[dict[str, Any]] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    recent: list[ActivityEntry] = field(default_factory=list)
    active_items: dict[str, dict[str, str]] = field(default_factory=dict, repr=False)

    @property
    def failures(self) -> int:
        return self.command_failures + self.mcp_failures

    @property
    def todo_progress(self) -> tuple[int, int]:
        return sum(bool(item.get("completed")) for item in self.todos), len(self.todos)

    def _append(self, kind: str, status: str, title: str, detail: str = "") -> None:
        self.sequence += 1
        self.updated_at = activity_timestamp()
        self.recent.append(
            ActivityEntry(
                sequence=self.sequence,
                at=self.updated_at,
                kind=kind,
                status=status,
                title=_compact(title, limit=240),
                detail=_compact(detail),
            )
        )
        del self.recent[:-MAX_RECENT_EVENTS]

    def _set_current(self) -> None:
        if self.active_items:
            latest = next(reversed(self.active_items.values()))
            self.current = latest["title"]
            self.current_kind = latest["kind"]
        elif self.finished_at is None:
            self.current = "thinking / preparing the next action"
            self.current_kind = "agent"

    def consume(self, event: Any, *, workspace_root: Path) -> None:
        if not isinstance(event, dict):
            return
        event_type = event.get("type")
        if event_type == "thread.started":
            self.current = "Codex thread started"
            self._append("agent", "started", self.current)
            return
        if event_type == "turn.started":
            self.current = "reasoning"
            self.current_kind = "agent"
            self._append("agent", "started", "turn started")
            return
        if event_type == "turn.completed":
            usage = event.get("usage")
            detail = ""
            if isinstance(usage, dict):
                input_tokens = int(usage.get("input_tokens", 0))
                output_tokens = int(usage.get("output_tokens", 0))
                detail = f"{input_tokens + output_tokens:,} tokens"
            self._append("usage", "completed", "turn completed", detail)
            self._set_current()
            return

        item = event.get("item")
        if not isinstance(item, dict) or not isinstance(item.get("type"), str):
            return
        item_type = item["type"]
        item_id = str(item.get("id", f"anonymous-{self.sequence}"))
        status = str(item.get("status", "completed" if event_type == "item.completed" else ""))

        if item_type == "todo_list":
            items = item.get("items")
            if isinstance(items, list):
                self.todos = [value for value in items if isinstance(value, dict)]
            done, total = self.todo_progress
            self._append("todo", "updated", f"plan updated ({done}/{total} complete)")
            return

        if item_type == "agent_message":
            text = item.get("text")
            self.latest_summary = text.strip() if isinstance(text, str) else ""
            self._append("message", "completed", "agent update", self.latest_summary)
            self._set_current()
            return

        title, detail = self._describe_item(item_type, item, workspace_root)
        if event_type == "item.started":
            self.active_items[item_id] = {"kind": item_type, "title": title}
            self.current = title
            self.current_kind = item_type
            self._append(item_type, "started", title)
            return
        if event_type != "item.completed":
            return

        self.active_items.pop(item_id, None)
        failed = status == "failed"
        error_detail = detail
        if item_type == "command_execution":
            self.commands += 1
            failed = failed or item.get("exit_code") not in (None, 0)
            self.command_failures += int(failed)
            output = item.get("aggregated_output")
            error_detail = _compact(output) if failed and isinstance(output, str) else ""
            if error_detail:
                detail = f"{detail} · {error_detail}" if detail else error_detail
        elif item_type == "mcp_tool_call":
            self.mcp_calls += 1
            failed = failed or bool(item.get("error"))
            self.mcp_failures += int(failed)
            if failed:
                detail = _result_text(item.get("result")) or _compact(str(item.get("error", "")))
                error_detail = detail
        elif item_type == "file_change":
            changes = item.get("changes")
            if isinstance(changes, list):
                self.file_changes += len(changes)
                for change in changes:
                    if isinstance(change, dict) and isinstance(change.get("path"), str):
                        path = self._relative_path(change["path"], workspace_root)
                        if path not in self.files:
                            self.files.append(path)

        result_status = "failed" if failed else "completed"
        if failed and (candidate := reportable_error(error_detail)):
            self.latest_error = candidate
        completed_title = "done" if item_type == "command_execution" and not failed else title
        self._append(item_type, result_status, completed_title, detail)
        self._set_current()

    def _describe_item(
        self, item_type: str, item: dict[str, Any], workspace_root: Path
    ) -> tuple[str, str]:
        if item_type == "command_execution":
            command = str(item.get("command", "shell command"))
            exit_code = item.get("exit_code")
            detail = f"exit {exit_code}" if exit_code not in (None, 0) else ""
            return f"shell: {_compact(_display_command(command), limit=180)}", detail
        if item_type == "mcp_tool_call":
            server = str(item.get("server", "MCP"))
            tool = str(item.get("tool", "tool"))
            return f"MCP {server}.{tool}", ""
        if item_type == "file_change":
            changes = item.get("changes")
            paths: list[str] = []
            if isinstance(changes, list):
                for change in changes:
                    if isinstance(change, dict) and isinstance(change.get("path"), str):
                        paths.append(self._relative_path(change["path"], workspace_root))
            title = f"editing {paths[0]}" if len(paths) == 1 else f"editing {len(paths)} files"
            return title, ", ".join(paths)
        return item_type.replace("_", " "), ""

    @staticmethod
    def _relative_path(value: str, workspace_root: Path) -> str:
        path = Path(value)
        try:
            relative = path.relative_to(workspace_root).as_posix()
        except ValueError:
            relative = path.as_posix()
        return _shorten_book_paths(relative)

    def finish(self, status: str, error: str = "") -> None:
        self.finished_at = activity_timestamp()
        self.current = f"agent {status}"
        self.current_kind = "agent"
        if candidate := reportable_error(error):
            self.latest_error = _compact(candidate)
        self._append("agent", status, self.current, error)
        self.finished_at = self.updated_at

    def retry(self, message: str) -> None:
        self.current = message
        self.current_kind = "agent"
        self._append("agent", "retrying", message)

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["failures"] = self.failures
        value["todo_completed"], value["todo_total"] = self.todo_progress
        value.pop("active_items", None)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentActivity:
        fields = dict(value)
        fields.pop("failures", None)
        fields.pop("todo_completed", None)
        fields.pop("todo_total", None)
        fields["recent"] = [ActivityEntry(**entry) for entry in fields.get("recent", [])]
        return cls(**fields)


class ActivityStore:
    def __init__(self, logs_dir: Path) -> None:
        self.logs_dir = logs_dir
        self._cache: dict[str, AgentActivity] = {}

    def path(self, run_id: str) -> Path:
        return self.logs_dir / f"{run_id}.activity.json"

    def start(self, run_id: str, chapter_id: str, stage: str) -> AgentActivity:
        activity = AgentActivity(run_id=run_id, chapter_id=chapter_id, stage=stage)
        self._cache[run_id] = activity
        self.save(activity)
        return activity

    def save(self, activity: AgentActivity) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        path = self.path(activity.run_id)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(json.dumpb(activity.as_dict(), sort_keys=True))
        os.replace(temporary, path)
        self._cache[activity.run_id] = activity

    def get(self, run_id: str) -> AgentActivity | None:
        if run_id in self._cache:
            return self._cache[run_id]
        path = self.path(run_id)
        if not path.is_file():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            activity = AgentActivity.from_dict(value)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        self._cache[run_id] = activity
        return activity

    def read_fresh(self, run_id: str) -> AgentActivity | None:
        self._cache.pop(run_id, None)
        return self.get(run_id)

    def replay(
        self,
        run_id: str,
        chapter_id: str,
        stage: str,
        log_path: Path,
        *,
        workspace_root: Path,
    ) -> AgentActivity | None:
        if not log_path.is_file():
            return None
        activity = AgentActivity(run_id=run_id, chapter_id=chapter_id, stage=stage)
        with log_path.open("rb") as log:
            for line in log:
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                activity.consume(event, workspace_root=workspace_root)
        self._cache[run_id] = activity
        return activity


def systemic_errors(activities: list[AgentActivity]) -> list[tuple[int, str]]:
    counts: dict[str, int] = {}
    for activity in activities:
        if message := reportable_error(activity.latest_error):
            signature = error_signature(message)
            counts[signature] = counts.get(signature, 0) + 1
    return sorted(((count, message) for message, count in counts.items()), reverse=True)
