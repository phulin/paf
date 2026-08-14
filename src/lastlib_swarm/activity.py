from __future__ import annotations

import os
import re
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lastlib_swarm import json_codec as json

MAX_RECENT_EVENTS = 80
MAX_DETAIL_CHARS = 800
MAX_MCP_SUMMARY_CHARS = 500
EVENT_TIMESTAMP_FIELD = "_lastlib_received_at"
LEAN_MCP_TITLES = {
    "lean_prepare_dependencies": "Lean deps",
    "lean_diagnostic_messages": "Lean diagnostics",
    "lean_hover_info": "Lean hover",
    "lean_declaration_file": "Lean declaration",
    "lean_local_search": "Lean search",
    "lean_goal": "Lean goal",
    "lean_term_goal": "Lean term goal",
    "lean_completions": "Lean completions",
    "lean_multi_attempt": "Lean attempts",
    "lean_code_actions": "Lean actions",
}
EXIT_STATUS_ONLY = re.compile(r"^exit\s+\d+$", re.IGNORECASE)
SHELL_COMMAND_WRAPPER = re.compile(r"^/(?:usr/)?bin/bash\s+-lc\s+(['\"])(.*)\1$", re.DOTALL)
LEAN_BOOK_PATH = re.compile(
    r"""
    /?(?:(?:[^/\s'"`|;,()\[\]{}]+/)*lean/)?
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


def _compact_block(value: str, *, limit: int = MAX_DETAIL_CHARS) -> str:
    """Bound display text without destroying its line-oriented formatting."""

    value = value.strip()
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _words(identifier: str) -> str:
    identifier = identifier.replace("_", " ")
    identifier = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", identifier)
    return re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", identifier).strip()


def shorten_book_paths(value: str) -> str:
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
    return shorten_book_paths(command)


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


def _mcp_text(value: Any, *, limit: int = MAX_MCP_SUMMARY_CHARS) -> str:
    """Render an arbitrary MCP value as bounded single-line text."""

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return _compact(shorten_book_paths(value), limit=limit)
    try:
        rendered = json.dumps(value, sort_keys=True)
    except (TypeError, ValueError):
        rendered = str(value)
    return _compact(shorten_book_paths(rendered), limit=limit)


def _lean_location(arguments: dict[str, Any]) -> str:
    path = shorten_book_paths(str(arguments.get("file_path", "Lean file")))
    line = arguments.get("line")
    column = arguments.get("column")
    if line is not None:
        path += f":{line}"
        if column is not None:
            path += f":{column}"
    elif arguments.get("start_line") is not None or arguments.get("end_line") is not None:
        start = arguments.get("start_line", "…")
        end = arguments.get("end_line", "…")
        path += f":{start}-{end}"
    return path


def _sample(values: list[str], *, maximum: int = 3, limit: int = 180) -> str:
    shown = [_compact(value, limit=60) for value in values[:maximum]]
    if len(values) > maximum:
        shown.append(f"+{len(values) - maximum} more")
    return _compact(", ".join(shown), limit=limit)


def _counted_items(
    value: Any,
    *,
    singular: str,
    plural: str | None = None,
    names: list[str] | None = None,
) -> str:
    items = value if isinstance(value, list) else []
    noun = singular if len(items) == 1 else (plural or f"{singular}s")
    summary = f"{len(items)} {noun}"
    if names:
        summary += f": {_sample(names)}"
    return summary


def _lean_mcp_query(item: dict[str, Any]) -> str:
    if item.get("server") != "lastlib_lean" or "arguments" not in item:
        return ""
    arguments = item["arguments"]
    if not isinstance(arguments, dict):
        return f"Q {_mcp_text(arguments)}"

    tool = item.get("tool")
    location = _lean_location(arguments)
    if tool == "lean_prepare_dependencies":
        paths = arguments.get("file_paths")
        files = [shorten_book_paths(str(path)) for path in paths] if isinstance(paths, list) else []
        summary = _counted_items(paths, singular="file", names=files)
        return f"Q prepare {summary}"
    if tool == "lean_diagnostic_messages":
        target = location
        if declaration := arguments.get("declaration_name"):
            target += f" ({declaration})"
        modifiers = [str(arguments["severity"])] if arguments.get("severity") else []
        if arguments.get("interactive"):
            modifiers.append("interactive")
        suffix = f" · {', '.join(modifiers)}" if modifiers else ""
        return f"Q diagnostics {target}{suffix}"
    if tool == "lean_hover_info":
        return f"Q hover {location}"
    if tool == "lean_declaration_file":
        symbol = arguments.get("symbol", "symbol")
        context = (
            "full file"
            if arguments.get("full_file")
            else f"±{arguments.get('context_lines', 20)} lines"
        )
        return f"Q declaration {symbol} in {location} · {context}"
    if tool == "lean_local_search":
        query = _compact(str(arguments.get("query", "")), limit=180)
        return f"Q local search “{query}” · max {arguments.get('limit', 10)}"
    if tool == "lean_goal":
        return f"Q goal {location}"
    if tool == "lean_term_goal":
        return f"Q term goal {location}"
    if tool == "lean_completions":
        return f"Q completions {location} · max {arguments.get('max_completions', 32)}"
    if tool == "lean_multi_attempt":
        snippets = arguments.get("snippets")
        attempts = [str(value) for value in snippets] if isinstance(snippets, list) else []
        return f"Q {_counted_items(snippets, singular='attempt', names=attempts)} at {location}"
    if tool == "lean_code_actions":
        return f"Q code actions {location}"
    return f"Q {_mcp_text(arguments)}"


def _lean_mcp_result_value(item: dict[str, Any]) -> Any:
    result = item.get("result")
    if isinstance(result, dict) and result.get("structured_content") is not None:
        value = result["structured_content"]
        if isinstance(value, dict) and set(value) == {"result"}:
            return value["result"]
        return value
    if result is not None:
        texts: list[str] = []
        content = result.get("content") if isinstance(result, dict) else None
        if isinstance(content, list):
            texts = [
                block["text"]
                for block in content
                if isinstance(block, dict)
                and block.get("type") == "text"
                and isinstance(block.get("text"), str)
            ]
        return "\n".join(texts) if texts else result
    if item.get("error") is not None:
        return {"error": item["error"]}
    return None


def _diagnostics_result(value: dict[str, Any]) -> str:
    items = value.get("items")
    if not isinstance(items, list):
        interactive = value.get("diagnostics")
        if isinstance(interactive, list):
            return _counted_items(interactive, singular="interactive diagnostic")
        return _mcp_text(value)

    counts = Counter(
        str(diagnostic.get("severity", "diagnostic"))
        for diagnostic in items
        if isinstance(diagnostic, dict)
    )
    labels = []
    for severity in ("error", "warning", "info", "hint", "diagnostic"):
        count = counts[severity]
        if count:
            plurals = {"info": "info"}
            labels.append(
                f"{count} {severity if count == 1 else plurals.get(severity, severity + 's')}"
            )
    if not labels:
        labels.append("clean")
    if value.get("partial") or value.get("timed_out"):
        labels.append("partial")
    if dependencies := value.get("failed_dependencies"):
        labels.append(f"{len(dependencies)} failed deps")

    first = next((entry for entry in items if isinstance(entry, dict)), None)
    if first is not None and first.get("message"):
        position = f"L{first.get('line', '?')}"
        if first.get("column") is not None:
            position += f":{first['column']}"
        labels.append(f"{position} {_compact(str(first['message']), limit=180)}")
    return " · ".join(labels)


def _goal_result(value: dict[str, Any]) -> str:
    status = value.get("status")
    if status == "still_elaborating":
        return "still elaborating"
    if status == "complete":
        return "complete"
    if status == "no_goal_at_position":
        return "no goal"
    if isinstance(value.get("goals"), list):
        count = len(value["goals"])
        return f"{count} {'goal' if count == 1 else 'goals'}"
    before = value.get("goals_before")
    after = value.get("goals_after")
    if isinstance(before, list) or isinstance(after, list):
        return f"{len(before or [])}→{len(after or [])} goals"
    return _mcp_text(value)


def _multi_attempt_result(value: dict[str, Any]) -> str:
    items = value.get("items")
    if not isinstance(items, list):
        return _mcp_text(value)
    outcomes: Counter[str] = Counter()
    completed: list[str] = []
    for attempt in items:
        if not isinstance(attempt, dict):
            outcomes["unknown"] += 1
            continue
        proof_status = str(attempt.get("proof_status", "")).lower()
        diagnostics = attempt.get("diagnostics")
        has_error = isinstance(diagnostics, list) and any(
            isinstance(diagnostic, dict) and diagnostic.get("severity") == "error"
            for diagnostic in diagnostics
        )
        if attempt.get("timed_out"):
            outcome = "timed out"
        elif proof_status in {"complete", "completed"}:
            outcome = "complete"
        elif proof_status.startswith("incomplete"):
            outcome = "incomplete"
        elif has_error:
            outcome = "failed"
        elif attempt.get("goals"):
            outcome = "open"
        else:
            outcome = "complete"
        outcomes[outcome] += 1
        if outcome == "complete" and attempt.get("snippet"):
            completed.append(str(attempt["snippet"]))

    labels = [
        f"{count} {outcome}"
        for outcome in ("complete", "open", "incomplete", "failed", "timed out", "unknown")
        if (count := outcomes[outcome])
    ]
    summary = f"{len(items)} attempts: {', '.join(labels) or 'no outcomes'}"
    if completed:
        summary += f" · worked: {_sample(completed, maximum=1, limit=120)}"
    return summary


def _lean_mcp_result(item: dict[str, Any]) -> str:
    if item.get("server") != "lastlib_lean":
        return ""
    value = _lean_mcp_result_value(item)
    if value is None:
        return ""

    tool = item.get("tool")
    summary: str
    if not isinstance(value, dict):
        summary = _mcp_text(value)
    elif "error" in value:
        summary = f"error: {_mcp_text(value['error'])}"
    elif tool == "lean_prepare_dependencies":
        prepared = value.get("prepared")
        stale = value.get("stale")
        summary = f"{len(prepared or [])} prepared · {len(stale or [])} stale"
    elif tool == "lean_diagnostic_messages":
        summary = _diagnostics_result(value)
    elif tool == "lean_hover_info":
        symbol = value.get("symbol", "symbol")
        summary = f"{symbol}: {_compact(str(value.get('info', 'no info')), limit=300)}"
    elif tool == "lean_declaration_file":
        path = shorten_book_paths(str(value.get("file_path", "declaration file")))
        start, end, total = value.get("start_line"), value.get("end_line"), value.get("total_lines")
        span = f" L{start}-{end}" if start is not None and end is not None else ""
        total_text = f"/{total}" if total is not None else ""
        summary = f"{path}{span}{total_text}"
    elif tool in {"lean_local_search", "lean_completions"}:
        values = value.get("items")
        names = (
            [
                str(entry.get("name") or entry.get("label"))
                for entry in values
                if isinstance(entry, dict) and (entry.get("name") or entry.get("label"))
            ]
            if isinstance(values, list)
            else []
        )
        singular = "match" if tool == "lean_local_search" else "completion"
        plural = "matches" if singular == "match" else None
        summary = _counted_items(values, singular=singular, plural=plural, names=names)
    elif tool == "lean_goal":
        summary = _goal_result(value)
    elif tool == "lean_term_goal":
        expected = value.get("expected_type")
        summary = (
            f"expected {_compact(str(expected), limit=350)}" if expected else "no expected type"
        )
    elif tool == "lean_multi_attempt":
        summary = _multi_attempt_result(value)
    elif tool == "lean_code_actions":
        actions = value.get("actions")
        titles = (
            [
                str(action["title"])
                for action in actions
                if isinstance(action, dict) and action.get("title")
            ]
            if isinstance(actions, list)
            else []
        )
        summary = _counted_items(actions, singular="action", names=titles)
    else:
        summary = _mcp_text(value)
    return f"R {_compact(summary, limit=MAX_MCP_SUMMARY_CHARS)}"


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
    recent_limit: int | None = field(default=MAX_RECENT_EVENTS, repr=False, compare=False)

    @property
    def failures(self) -> int:
        return self.command_failures + self.mcp_failures

    @property
    def todo_progress(self) -> tuple[int, int]:
        return sum(bool(item.get("completed")) for item in self.todos), len(self.todos)

    def _append(
        self,
        kind: str,
        status: str,
        title: str,
        detail: str = "",
        *,
        at: str | None = None,
        preserve_detail_layout: bool = False,
    ) -> None:
        self.sequence += 1
        self.updated_at = at or activity_timestamp()
        self.recent.append(
            ActivityEntry(
                sequence=self.sequence,
                at=self.updated_at,
                kind=kind,
                status=status,
                title=_compact(title, limit=240),
                detail=_compact_block(detail) if preserve_detail_layout else _compact(detail),
            )
        )
        if self.recent_limit is not None:
            del self.recent[: -self.recent_limit]

    def _set_current(self) -> None:
        if self.active_items:
            latest = next(reversed(self.active_items.values()))
            self.current = latest["title"]
            self.current_kind = latest["kind"]
        elif self.finished_at is None:
            self.current = "thinking / preparing the next action"
            self.current_kind = "agent"

    def consume(self, event: Any, *, workspace_root: Path, at: str | None = None) -> None:
        if not isinstance(event, dict):
            return
        if at is None and isinstance(event.get(EVENT_TIMESTAMP_FIELD), str):
            at = event[EVENT_TIMESTAMP_FIELD]
        event_type = event.get("type")
        if event_type == "thread.started":
            self.current = "Codex thread started"
            self._append("agent", "started", self.current, at=at)
            return
        if event_type == "turn.started":
            self.current = "reasoning"
            self.current_kind = "agent"
            self._append("agent", "started", "turn started", at=at)
            return
        if event_type == "turn.completed":
            usage = event.get("usage")
            detail = ""
            if isinstance(usage, dict):
                input_tokens = int(usage.get("input_tokens", 0))
                output_tokens = int(usage.get("output_tokens", 0))
                detail = f"{input_tokens + output_tokens:,} tokens"
            self._append("usage", "completed", "turn completed", detail, at=at)
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
            self._append("todo", "updated", f"plan updated ({done}/{total} complete)", at=at)
            return

        if item_type == "agent_message":
            text = item.get("text")
            self.latest_summary = text.strip() if isinstance(text, str) else ""
            self._append("message", "completed", "agent update", self.latest_summary, at=at)
            self._set_current()
            return

        title, detail = self._describe_item(item_type, item, workspace_root)
        if event_type == "item.started":
            self.active_items[item_id] = {"kind": item_type, "title": title}
            self.current = title
            self.current_kind = item_type
            self._append(
                item_type,
                "started",
                title,
                detail,
                at=at,
                preserve_detail_layout=item_type == "mcp_tool_call" and bool(detail),
            )
            return
        if event_type != "item.completed":
            return

        was_active = item_id in self.active_items
        self.active_items.pop(item_id, None)
        failed = status == "failed"
        error_detail = detail
        result_detail = ""
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
            result_detail = _lean_mcp_result(item)
            if result_detail:
                if detail and not was_active:
                    query_detail = _compact(detail, limit=360)
                    result_detail = _compact(result_detail, limit=430)
                    detail = f"{query_detail} · {result_detail}"
                else:
                    detail = result_detail
            if failed:
                error_detail = _result_text(item.get("result")) or _compact(
                    str(item.get("error", ""))
                )
                if not detail:
                    detail = error_detail
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
        if not failed and item_type == "command_execution":
            completed_title = "done"
        elif not failed and item_type == "file_change":
            completed_title = "success"
            detail = ""
        else:
            completed_title = title
        self._append(
            item_type,
            result_status,
            completed_title,
            detail,
            at=at,
            preserve_detail_layout=item_type == "mcp_tool_call" and bool(result_detail),
        )
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
            title = LEAN_MCP_TITLES.get(tool) if server == "lastlib_lean" else None
            return title or f"MCP {server}.{tool}", _lean_mcp_query(item)
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
        return shorten_book_paths(relative)

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
        value.pop("recent_limit", None)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentActivity:
        fields = dict(value)
        fields.pop("failures", None)
        fields.pop("todo_completed", None)
        fields.pop("todo_total", None)
        fields.pop("recent_limit", None)
        fields["recent"] = [ActivityEntry(**entry) for entry in fields.get("recent", [])]
        return cls(**fields)


class ActivityStore:
    def __init__(self, logs_dir: Path) -> None:
        self.logs_dir = logs_dir
        self._cache: dict[str, AgentActivity] = {}
        self._last_saved: dict[str, float] = {}

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
        self._last_saved[activity.run_id] = time.monotonic()

    def save_throttled(self, activity: AgentActivity, *, interval: float = 1.0) -> None:
        """Persist a derived activity summary at most once per interval."""

        self._cache[activity.run_id] = activity
        last_saved = self._last_saved.get(activity.run_id, 0.0)
        if time.monotonic() - last_saved >= interval:
            self.save(activity)

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
        maximum_events: int | None = MAX_RECENT_EVENTS,
        cache: bool = True,
    ) -> AgentActivity | None:
        if not log_path.is_file():
            return None
        persisted = self.get(run_id)
        activity = AgentActivity(
            run_id=run_id,
            chapter_id=chapter_id,
            stage=stage,
            recent_limit=maximum_events,
        )
        with log_path.open("rb") as log:
            for line in log:
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                activity.consume(event, workspace_root=workspace_root)
        if persisted is not None:
            self._restore_persisted_timestamps(activity, persisted)
        if cache:
            self._cache[run_id] = activity
        return activity

    @staticmethod
    def _restore_persisted_timestamps(replayed: AgentActivity, persisted: AgentActivity) -> None:
        """Recover timestamps for legacy JSONL events from the compact sidecar."""

        def signature(entry: ActivityEntry) -> tuple[str, str, str, str]:
            return entry.kind, entry.status, entry.title, entry.detail

        replay_index = len(replayed.recent) - 1
        for known in reversed(persisted.recent):
            match = next(
                (
                    index
                    for index in range(replay_index, -1, -1)
                    if signature(replayed.recent[index]) == signature(known)
                ),
                None,
            )
            if match is None:
                continue
            replayed.recent[match].at = known.at
            replay_index = match - 1
        if replayed.recent:
            replayed.updated_at = replayed.recent[-1].at


def systemic_errors(activities: list[AgentActivity]) -> list[tuple[int, str]]:
    counts: dict[str, int] = {}
    for activity in activities:
        if message := reportable_error(activity.latest_error):
            signature = error_signature(message)
            counts[signature] = counts.get(signature, 0) + 1
    return sorted(((count, message) for message, count in counts.items()), reverse=True)
