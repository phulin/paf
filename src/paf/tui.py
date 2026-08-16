from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.geometry import Size
from textual.screen import Screen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    RichLog,
    Static,
    Tab,
    TabbedContent,
    TabPane,
    Tabs,
    TextArea,
)
from textual.worker import WorkerCancelled

from paf import json_codec as json
from paf.activity import ActivityEntry, AgentActivity, reportable_error, systemic_errors
from paf.models import Stage, WorkUnitLike
from paf.pricing import format_usd
from paf.scheduler import Orchestrator
from paf.state import RunRecord, StateStore, TaskPhase, TaskRecord, TaskStatus, TokenUsage

TUI_THEME = "ansi-dark"
MAX_TIMELINE_EVENTS = 10_000
REFRESH_INTERVAL_SECONDS = 1.0
DASHBOARD_FRAME_INTERVAL_SECONDS = 0.2
SUCCESS_EXIT_DELAY_SECONDS = 1.0
FAILURE_EXIT_DELAY_SECONDS = 2.0


class FixedGridDataTable(DataTable[Any]):
    """A fixed-size table that avoids measuring every cell as it is inserted.

    Textual's DataTable measures new cell contents even when every column has an
    explicit width.  That work is useful for auto-sized grids, but expensive for
    the large, deliberately fixed dashboard below.  Retain the standard path if
    another caller introduces an auto-sized column or row.
    """

    def _update_dimensions(self, new_rows: Iterable[Any]) -> None:
        rows = tuple(new_rows)
        if any(column.auto_width for column in self.columns.values()) or any(
            self.rows[row_key].auto_height for row_key in rows if row_key in self.rows
        ):
            super()._update_dimensions(rows)
            return

        data_cells_width = sum(column.get_render_width(self) for column in self.columns.values())
        header_height = self.header_height if self.show_header else 0
        self.virtual_size = Size(
            data_cells_width + self._row_label_column_width,
            self._total_row_height + header_height,
        )


@dataclass(frozen=True)
class ActivityKindDisplay:
    label: str
    color: str


# Keep labels narrow enough for the timeline and colors distinct so a scan down
# the log makes changes in activity immediately visible.  This includes the
# current Codex item kinds plus newer protocol kinds that older sidecars may not
# contain yet.
ACTIVITY_KIND_DISPLAYS = {
    "agent": ActivityKindDisplay("agent", "#7aa2f7"),
    "usage": ActivityKindDisplay("tokens", "#bb9af7"),
    "todo": ActivityKindDisplay("plan", "#e0af68"),
    "message": ActivityKindDisplay("msg", "#7dcfff"),
    "reasoning": ActivityKindDisplay("think", "#9d7cd8"),
    "command_execution": ActivityKindDisplay("bash", "#2ac3de"),
    "file_change": ActivityKindDisplay("edit", "#9ece6a"),
    "mcp_tool_call": ActivityKindDisplay("mcp", "#f7768e"),
    "collab_tool_call": ActivityKindDisplay("swarm", "#ff9e64"),
    "web_search": ActivityKindDisplay("web", "#73daca"),
    "error": ActivityKindDisplay("error", "#db4b4b"),
    "context_compaction": ActivityKindDisplay("compact", "#c0caf5"),
    "dynamic_tool_call": ActivityKindDisplay("tool", "#b4f9f8"),
    "image_generation": ActivityKindDisplay("image", "#e0aaff"),
    "image_view": ActivityKindDisplay("view", "#fca7ea"),
    "sub_agent_activity": ActivityKindDisplay("subagent", "#c3e88d"),
    "hook_prompt": ActivityKindDisplay("hook", "#ffc777"),
    "entered_review_mode": ActivityKindDisplay("review+", "#89ddff"),
    "exited_review_mode": ActivityKindDisplay("review-", "#82aaff"),
    "user_message": ActivityKindDisplay("user", "#f78c6c"),
    "extension": ActivityKindDisplay("ext", "#c792ea"),
}

# Protocol versions have used both of these spellings for the same concepts.
ACTIVITY_KIND_ALIASES = {
    "collab_agent_tool_call": "collab_tool_call",
    "compaction": "context_compaction",
}


def activity_kind_display(kind: str) -> ActivityKindDisplay:
    canonical = ACTIVITY_KIND_ALIASES.get(kind, kind)
    if display := ACTIVITY_KIND_DISPLAYS.get(canonical):
        return display
    label = kind.replace("_", "-")
    if len(label) > 12:
        label = f"{label[:11]}…"
    return ActivityKindDisplay(label or "event", "#a9b1d6")


def activity_kind_badge(kind: str) -> Text:
    display = activity_kind_display(kind)
    return Text(f"[{display.label}]", style=f"bold {display.color}")


STATUS_MARKS = {
    TaskStatus.PENDING: "· pending",
    TaskStatus.RUNNING: "▶ running",
    TaskStatus.SUCCEEDED: "✓ done",
    TaskStatus.FAILED: "✗ failed",
    TaskStatus.BLOCKED: "! blocked",
    TaskStatus.INTERRUPTED: "Ⅱ interrupted",
}


def format_count(value: int) -> str:
    if value < 1_000:
        return str(value)
    if value < 1_000_000:
        return f"{value / 1_000:.1f}k"
    if value < 1_000_000_000:
        return f"{value / 1_000_000:.2f}m"
    return f"{value / 1_000_000_000:.2f}b"


def format_usage(usage: TokenUsage, *, label: str = "Tokens") -> str:
    if not usage.measured:
        return f"{label}: awaiting measured usage"
    return (
        f"{label}: {format_count(usage.total_tokens)}  "
        f"input {format_count(usage.input_tokens)} "
        f"(cached {format_count(usage.cached_input_tokens)})  "
        f"output {format_count(usage.output_tokens)}  "
        f"reasoning {format_count(usage.reasoning_output_tokens)}"
    )


def chapter_usage(state: StateStore, chapter: WorkUnitLike) -> TokenUsage:
    return state.invocation_usage(chapter.id)


def stage_counts(state: StateStore, stage: Stage) -> dict[str, int]:
    return state.stage_counts(stage)


def running_agent_counts(state: StateStore) -> dict[str, int]:
    summary = state.agent_summary()
    by_stage = summary.get("by_stage", {})
    return {
        stage.value: int(by_stage.get(stage.value, 0)) if isinstance(by_stage, dict) else 0
        for stage in Stage
    }


def task_mark(task: TaskRecord, *, building: bool = False) -> str:
    if building:
        return "◆ building"
    if task.queued:
        return "· queued"
    if task.status == TaskStatus.RUNNING and task.phase == TaskPhase.POSTPROCESS:
        return "◇ postprocess"
    return STATUS_MARKS[TaskStatus(task.status)]


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def progress_meter(completed: int, total: int, *, width: int = 24) -> str:
    fraction = min(1.0, max(0.0, completed / total)) if total else 0.0
    filled = int(fraction * width)
    return "[" + "█" * filled + "░" * (width - filled) + "]"


def seconds_since(value: str) -> float:
    try:
        return (datetime.now(UTC) - datetime.fromisoformat(value)).total_seconds()
    except ValueError:
        return 0


def latest_run(state: StateStore, chapter: WorkUnitLike) -> RunRecord | None:
    return state.latest_run(chapter.id)


def chapter_runs(state: StateStore, chapter: WorkUnitLike) -> list[RunRecord]:
    """Return every agent step for a chapter in chronological order."""

    return list(state.chapter_runs(chapter.id))


def run_tab_label(run: RunRecord, step: int) -> str:
    marks = {
        TaskStatus.RUNNING: "▶",
        TaskStatus.SUCCEEDED: "✓",
        TaskStatus.FAILED: "✗",
        TaskStatus.BLOCKED: "!",
        TaskStatus.INTERRUPTED: "Ⅱ",
        TaskStatus.PENDING: "·",
    }
    mark = marks.get(TaskStatus(run.status), "·")
    return f"{mark} Step {step} · {run.stage.title()} round {run.round}"


def activity_label(activity: AgentActivity | None, run: RunRecord | None) -> str:
    if run is None:
        return "—"
    if activity is None:
        return "awaiting events" if run.status == TaskStatus.RUNNING else "no activity summary"
    age = seconds_since(activity.updated_at)
    prefix = f"✗ {activity.failures}  " if activity.failures else ""
    idle = f"  idle {int(age // 60)}m" if run.status == TaskStatus.RUNNING and age >= 60 else ""
    return f"{prefix}{activity.current}{idle}"


@dataclass(frozen=True)
class AgentUpdate:
    changed: bool
    summary: str
    issues: tuple[str, ...]


def parse_agent_update(value: str) -> AgentUpdate | None:
    """Decode the structured report emitted as an agent message, when present."""

    try:
        report = json.loads(value)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(report, dict):
        return None
    changed = report.get("changed")
    summary = report.get("summary")
    issues = report.get("issues")
    if (
        not isinstance(changed, bool)
        or not isinstance(summary, str)
        or not isinstance(issues, list)
        or not all(isinstance(issue, str) for issue in issues)
    ):
        return None
    return AgentUpdate(changed=changed, summary=summary, issues=tuple(issues))


def _right_aligned_status(label: str, status: str, width: int) -> str:
    available_label = width - len(status) - 1
    if available_label > 0 and len(label) > available_label:
        label = f"{label[: available_label - 1]}…" if available_label > 1 else "…"
    gap = max(1, width - len(label) - len(status))
    return f"{label}{' ' * gap}{status}"


def format_agent_update(value: str, *, heading: str, width: int, indent: str = "") -> str:
    """Render a structured agent report without exposing its JSON encoding."""

    update = parse_agent_update(value)
    if update is None:
        return f"{heading}\n{value}" if value else f"{heading}\nNo update yet."

    changed = f"CHANGED {'✓' if update.changed else '✗'}"
    lines = [_right_aligned_status(heading, changed, width)]
    summary_lines = update.summary.splitlines() or ["No summary provided."]
    lines.extend(f"{indent}{line}" for line in summary_lines)
    lines.append(f"{indent}ISSUES")
    if update.issues:
        for issue in update.issues:
            issue_lines = issue.splitlines() or [""]
            lines.append(f"{indent}  • {issue_lines[0]}")
            lines.extend(f"{indent}    {line}" for line in issue_lines[1:])
    else:
        lines.append(f"{indent}  • None reported")
    return "\n".join(lines)


def _log_render_width(log: RichLog) -> int:
    """Return the usable log width, excluding its reserved scrollbar gutter."""

    return max(1, log.scrollable_size.width)


def _compact_json(value: Any, *, limit: int = 400) -> Any:
    if isinstance(value, str):
        return value if len(value) <= limit else value[: limit - 1] + "…"
    if isinstance(value, list):
        return [_compact_json(item, limit=limit) for item in value]
    if isinstance(value, dict):
        return {key: _compact_json(item, limit=limit) for key, item in value.items()}
    return value


def recent_raw_events(path: Path, *, maximum: int = 30) -> list[str]:
    if not path.is_file():
        return []
    # A single Codex command result may span several MiB. Read enough for useful
    # recent context but never feed its unbounded output to Textual.
    size = path.stat().st_size
    with path.open("rb") as handle:
        handle.seek(max(0, size - 8 * 1024 * 1024))
        data = handle.read()
    lines = data.splitlines()
    if size > len(data) and lines:
        lines = lines[1:]
    rendered: list[str] = []
    for line in lines[-maximum:]:
        try:
            value = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue
        rendered.append(json.dumps(_compact_json(value), sort_keys=True))
    return rendered


class AgentDetailScreen(Screen[None]):
    BINDINGS: ClassVar = [("escape", "close", "Back to swarm"), ("q", "close", "Back to swarm")]
    CSS = """
    AgentDetailScreen { background: $background; }
    #run-tabs { height: 3; }
    #agent-heading {
        height: 4;
        padding: 1 2;
        background: $surface;
        border-bottom: solid $primary-muted;
        color: $text;
        text-style: bold;
    }
    #agent-metrics { height: 5; }
    .agent-card {
        width: 1fr;
        height: 5;
        border: round $panel-lighten-1;
        background: $surface;
        padding: 0 1;
    }
    #agent-summary {
        height: 8;
        padding: 1 2;
        border: round $primary-muted;
        background: $surface;
        scrollbar-gutter: stable;
    }
    #agent-error {
        height: auto;
        max-height: 5;
        padding: 0 2;
        color: $text-error;
    }
    #agent-tabs { height: 1fr; }
    RichLog { height: 1fr; }
    #agent-timeline { scrollbar-gutter: stable; }
    #agent-path { height: 3; padding: 1 2; color: $text-muted; }
    """

    def __init__(self, state: StateStore, chapter: WorkUnitLike) -> None:
        super().__init__()
        self.state = state
        self.chapter = chapter
        selected = latest_run(state, chapter)
        self._selected_run_id = selected.id if selected is not None else None
        self._rendered_activity: tuple[str, int | None, int] | None = None
        self._static_cache: dict[str, str] = {}
        self._tab_runs: dict[str, str] = {}
        self._tab_labels: dict[str, str] = {}
        self._raw_path: Path | None = None
        self._raw_offset = 0
        self._raw_pending = bytearray()
        self._raw_lines: deque[str] = deque(maxlen=30)
        self._rendered_plan: tuple[str, tuple[tuple[str, bool], ...]] | None = None
        self._rendered_files: tuple[str, tuple[str, ...]] | None = None
        self._rendered_prompt: tuple[str, str, int | None, int | None] | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        runs = chapter_runs(self.state, self.chapter)
        tabs: list[Tab] = []
        active: str | None = None
        for step, run in enumerate(runs, start=1):
            tab_id = f"agent-step-{step}"
            label = run_tab_label(run, step)
            self._tab_runs[tab_id] = run.id
            self._tab_labels[tab_id] = label
            tabs.append(Tab(label, id=tab_id))
            if run.id == self._selected_run_id:
                active = tab_id
        yield Tabs(*tabs, active=active, id="run-tabs")
        yield Static("Loading agent activity…", id="agent-heading", markup=False)
        with Horizontal(id="agent-metrics"):
            yield Static(id="agent-work", classes="agent-card", markup=False)
            yield Static(id="agent-spend", classes="agent-card", markup=False)
        yield RichLog(
            id="agent-summary",
            wrap=True,
            markup=False,
            auto_scroll=False,
            max_lines=None,
        )
        yield Static(id="agent-error", markup=False)
        with TabbedContent(id="agent-tabs"):
            with TabPane("Timeline", id="timeline-pane"):
                yield RichLog(
                    id="agent-timeline",
                    wrap=True,
                    markup=False,
                    max_lines=MAX_TIMELINE_EVENTS,
                )
            with TabPane("Prompt", id="prompt-pane"):
                yield TextArea(
                    id="agent-prompt",
                    soft_wrap=True,
                    read_only=True,
                    show_cursor=False,
                    highlight_cursor_line=False,
                )
            with TabPane("Plan", id="plan-pane"):
                yield RichLog(id="agent-plan", wrap=True, markup=False)
            with TabPane("Files", id="files-pane"):
                yield RichLog(id="agent-files", wrap=True, markup=False)
            with TabPane("Raw events", id="raw-pane"):
                yield RichLog(id="agent-raw", wrap=True, markup=False)
        yield Static(id="agent-path", markup=False)
        yield Footer()

    def on_mount(self) -> None:
        self.refresh_agent()
        self.call_after_refresh(self.refresh_agent)
        self.set_interval(REFRESH_INTERVAL_SECONDS, self.refresh_agent)

    def action_close(self) -> None:
        self.app.pop_screen()

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if event.tabs.id != "run-tabs" or event.tab.id is None:
            return
        run_id = self._tab_runs.get(event.tab.id)
        if run_id is None or run_id == self._selected_run_id:
            return
        self._selected_run_id = run_id
        self._rendered_activity = None
        self._rendered_prompt = None
        self.refresh_agent()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if event.tabbed_content.id != "agent-tabs" or event.pane.id != "prompt-pane":
            return
        run = next(
            (
                item
                for item in chapter_runs(self.state, self.chapter)
                if item.id == self._selected_run_id
            ),
            None,
        )
        if run is not None:
            self._refresh_prompt(run)

    def refresh_agent(self) -> None:
        runs = chapter_runs(self.state, self.chapter)
        run = next((item for item in runs if item.id == self._selected_run_id), None)
        if run is None:
            run = latest_run(self.state, self.chapter)
            self._selected_run_id = run.id if run is not None else None
        self._sync_run_tabs(runs)
        if run is None:
            self._update_static("#agent-heading", f"{self.chapter.id} — no agent run recorded")
            return
        step = next(index for index, item in enumerate(runs, start=1) if item.id == run.id)
        activity = self.state.activities.get(run.id)
        elapsed = seconds_since(run.started_at)
        if run.finished_at is not None:
            with suppress(ValueError):
                elapsed = (
                    datetime.fromisoformat(run.finished_at) - datetime.fromisoformat(run.started_at)
                ).total_seconds()
        last_event = format_duration(seconds_since(activity.updated_at)) if activity else "—"
        self._update_static(
            "#agent-heading",
            f"{self.chapter.id} · step {step} of {len(runs)} · "
            f"{run.stage} round {run.round} · {run.status}\n"
            f"{activity_label(activity, run)}",
        )
        if activity:
            done, total = activity.todo_progress
            work = (
                f"WORK\n{activity.commands} shell · {activity.mcp_calls} MCP · "
                f"{activity.file_changes} edits\nplan {done}/{total} · {activity.failures} failures"
            )
        else:
            work = "WORK\nAwaiting compact events"
        self._update_static("#agent-work", work)
        usage = format_count(run.usage.total_tokens) if run.usage.measured else "pending"
        cost = format_usd(self.state.run_cost(run))
        self._update_static(
            "#agent-spend",
            f"TIME / SPEND\nelapsed {format_duration(elapsed)} · last event {last_event}\n"
            f"tokens {usage} · API-equivalent cost {cost}",
        )
        summary = activity.latest_summary if activity else ""
        summary_log = self.query_one("#agent-summary", RichLog)
        self._update_log(
            "#agent-summary",
            format_agent_update(
                summary,
                heading="LATEST AGENT UPDATE",
                width=_log_render_width(summary_log),
            ),
        )
        error = reportable_error(activity.latest_error) if activity else ""
        self._update_static("#agent-error", f"LATEST ERROR\n{error}" if error else "")
        path = Path(run.log_path) if run.log_path else self.state.logs_dir / f"{run.id}.jsonl"
        self._update_static("#agent-path", f"Raw JSONL: {path}")
        timeline_width = _log_render_width(self.query_one("#agent-timeline", RichLog))
        self._refresh_activity(run.id, activity, timeline_width)
        tabs = self.query_one("#agent-tabs", TabbedContent)
        if tabs.active == "prompt-pane":
            self._refresh_prompt(run)
        if tabs.active == "raw-pane":
            self._refresh_raw_events(path)

    def _refresh_prompt(self, run: RunRecord) -> None:
        path = self.state.logs_dir / f"{run.id}.prompt.md"
        size: int | None = None
        modified: int | None = None
        with suppress(OSError):
            stat = path.stat()
            size = stat.st_size
            modified = stat.st_mtime_ns
        rendered = (run.id, str(path), size, modified)
        if rendered == self._rendered_prompt:
            return
        if size is None:
            content = "Prompt was not recorded for this run."
        else:
            try:
                content = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as error:
                content = f"Could not read prompt file {path}: {error}"
        prompt = self.query_one("#agent-prompt", TextArea)
        prompt.load_text(content)
        prompt.scroll_home(animate=False, immediate=True)
        self._rendered_prompt = rendered

    def _refresh_activity(
        self,
        run_id: str,
        activity: AgentActivity | None,
        timeline_width: int,
    ) -> None:
        rendered = self._rendered_activity
        sequence = activity.sequence if activity is not None else None
        target = (run_id, sequence, timeline_width)
        if target == rendered:
            return
        if rendered is not None and rendered[:2] == target[:2]:
            # RichLog handles viewport changes; rebuilding thousands of already
            # wrapped lines solely because a scrollbar changed the measured
            # width is much more disruptive than retaining their old wrapping.
            self._rendered_activity = target
            return

        can_append = (
            activity is not None
            and rendered is not None
            and rendered[0] == run_id
            and rendered[1] is not None
            and sequence is not None
            and sequence > rendered[1]
        )
        if can_append:
            assert activity is not None and rendered is not None and rendered[1] is not None
            additions = [entry for entry in activity.recent if entry.sequence > rendered[1]]
        else:
            additions = []
        if can_append and additions and additions[0].sequence == rendered[1] + 1:
            assert activity is not None
            self._append_activity(activity, additions, timeline_width)
        else:
            # The compact activity sidecar already retains a bounded recent
            # window. Rebuilding from it avoids parsing an unbounded JSONL and
            # is also the safe recovery path after missed events or a resize.
            self._render_activity(activity)
        self._render_activity_tabs(activity)
        self._rendered_activity = target

    def _sync_run_tabs(self, runs: list[RunRecord]) -> None:
        tabs = self.query_one("#run-tabs", Tabs)
        known_run_ids = set(self._tab_runs.values())
        for step, run in enumerate(runs, start=1):
            if run.id not in known_run_ids:
                tab_id = f"agent-step-{len(self._tab_runs) + 1}"
                self._tab_runs[tab_id] = run.id
                label = run_tab_label(run, step)
                self._tab_labels[tab_id] = label
                tabs.add_tab(Tab(label, id=tab_id))
            else:
                tab_id = next(key for key, run_id in self._tab_runs.items() if run_id == run.id)
            label = run_tab_label(run, step)
            if self._tab_labels.get(tab_id) != label:
                tab = tabs.get_tab(tab_id)
                if tab is not None:
                    tab.label = label
                self._tab_labels[tab_id] = label

        active = next(
            (
                tab_id
                for tab_id, run_id in self._tab_runs.items()
                if run_id == self._selected_run_id
            ),
            None,
        )
        if active is not None and tabs.active != active:
            tabs.active = active

    def _update_static(self, selector: str, content: str) -> None:
        if self._static_cache.get(selector) == content:
            return
        self._static_cache[selector] = content
        self.query_one(selector, Static).update(content)

    def _update_log(self, selector: str, content: str) -> None:
        if self._static_cache.get(selector) == content:
            return
        self._static_cache[selector] = content
        log = self.query_one(selector, RichLog)
        log.clear()
        width = _log_render_width(log) if log.scrollable_size.width else None
        log.write(content, width=width)
        log.scroll_home(animate=False)

    def _refresh_raw_events(self, path: Path) -> None:
        if not path.is_file():
            return
        size = path.stat().st_size
        reset = path != self._raw_path or size < self._raw_offset
        if reset:
            self._raw_path = path
            self._raw_pending.clear()
            self._raw_lines.clear()
            self._raw_offset = max(0, size - 8 * 1024 * 1024)
        if size == self._raw_offset:
            return
        with path.open("rb") as handle:
            handle.seek(self._raw_offset)
            data = handle.read()
            self._raw_offset = handle.tell()
        if reset and self._raw_offset - len(data) > 0:
            _, _, data = data.partition(b"\n")
        self._raw_pending.extend(data)
        changed = False
        while (newline := self._raw_pending.find(b"\n")) >= 0:
            line = bytes(self._raw_pending[:newline])
            del self._raw_pending[: newline + 1]
            try:
                value = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                continue
            self._raw_lines.append(json.dumps(_compact_json(value), sort_keys=True))
            changed = True
        if not changed:
            return
        raw = self.query_one("#agent-raw", RichLog)
        raw.clear()
        for line in self._raw_lines:
            raw.write(line)

    def _render_activity(self, activity: AgentActivity | None) -> None:
        timeline = self.query_one("#agent-timeline", RichLog)
        timeline.clear()
        timeline_width = _log_render_width(timeline)
        write_width = timeline_width if timeline.scrollable_size.width else None
        if activity and activity.recent and activity.recent[0].sequence > 1:
            omitted = activity.recent[0].sequence - 1
            timeline.write(
                f"… {omitted:,} earlier timeline events available in Raw events",
                width=write_width,
            )
        if activity is not None:
            self._append_activity(activity, activity.recent, timeline_width)
        else:
            timeline.write("No activity events recorded for this step.", width=write_width)

    def _append_activity(
        self,
        activity: AgentActivity,
        entries: Iterable[ActivityEntry],
        timeline_width: int,
    ) -> None:
        timeline = self.query_one("#agent-timeline", RichLog)
        write_width = timeline_width if timeline.scrollable_size.width else None
        marks = {"started": "▶", "completed": "✓", "failed": "✗", "updated": "•"}
        latest_message = max(
            (entry.sequence for entry in activity.recent if entry.kind == "message"),
            default=None,
        )
        for entry in entries:
            clock = entry.at[11:19] if len(entry.at) >= 19 else entry.at
            prefix = f"{clock} {marks.get(entry.status, '•')} "
            badge = activity_kind_badge(entry.kind)
            heading = f"{prefix}{badge.plain} {entry.title}"
            detail = (
                activity.latest_summary
                if activity is not None
                and entry.kind == "message"
                and entry.sequence == latest_message
                else entry.detail
            )
            if entry.kind == "message" and parse_agent_update(detail) is not None:
                rendered = format_agent_update(
                    detail,
                    heading=heading,
                    width=timeline_width,
                    indent="    ",
                )
                line = Text(rendered)
                line.stylize(badge.style, len(prefix), len(prefix) + len(badge))
            else:
                line = Text(prefix)
                line.append(badge)
                line.append(f" {entry.title}")
                if detail:
                    if entry.kind == "mcp_tool_call" and "\n" not in detail:
                        line.append(f" · {detail}")
                    else:
                        line.append("\n" + "\n".join(f"    {part}" for part in detail.splitlines()))
            timeline.write(line, width=write_width)

    def _render_activity_tabs(self, activity: AgentActivity | None) -> None:
        run_id = activity.run_id if activity is not None else ""
        plan_key = (
            run_id,
            tuple(
                (str(item.get("text", "")), bool(item.get("completed"))) for item in activity.todos
            )
            if activity is not None
            else (),
        )
        plan = self.query_one("#agent-plan", RichLog)
        if plan_key != self._rendered_plan:
            plan.clear()
            for text, completed in plan_key[1]:
                plan.write(f"{'✓' if completed else '·'} {text}")
            if not plan_key[1]:
                plan.write("No todo list emitted yet.")
            self._rendered_plan = plan_key

        files_key = (run_id, tuple(activity.files) if activity is not None else ())
        files = self.query_one("#agent-files", RichLog)
        if files_key != self._rendered_files:
            files.clear()
            for path in files_key[1]:
                files.write(path)
            if not files_key[1]:
                files.write("No file-change event emitted yet.")
            self._rendered_files = files_key


class SwarmApp(App[bool]):
    TITLE = "LastLib Formalization Swarm"
    SUB_TITLE = "Codex agent pipeline"
    BINDINGS: ClassVar = [("q", "quit", "Stop and quit"), ("i", "inspect_agent", "Inspect")]
    CSS = """
    Screen { background: $background; }
    #usage {
        height: auto;
        min-height: 4;
        max-height: 7;
        padding: 1 2;
        background: $surface;
        border-bottom: solid $primary-muted;
        color: $text;
        text-style: bold;
    }
    #startup-warning {
        height: auto;
        max-height: 4;
        padding: 1 2;
        color: $text-warning;
        text-style: bold;
    }
    #stages { height: 5; }
    #alerts {
        height: auto;
        max-height: 3;
        padding: 0 2;
        color: $text-error;
    }
    .stage-card {
        width: 1fr;
        height: 5;
        border: round $panel-lighten-1;
        background: $surface;
        padding: 0 1;
    }
    #tasks { height: 1fr; }
    #status {
        height: auto;
        min-height: 3;
        max-height: 8;
        padding: 1 2;
        background: $surface;
        color: $text;
    }
    """

    def __init__(
        self,
        orchestrator: Orchestrator,
        operation: Callable[[], Awaitable[bool]],
        *,
        label: str,
        startup_warning: str = "",
    ) -> None:
        super().__init__()
        self.theme = TUI_THEME
        self.orchestrator = orchestrator
        self.state = orchestrator.state
        self.operation = operation
        self.label = label
        self.startup_warning = startup_warning
        self.result = False
        self.fatal_error: BaseException | None = None
        self._quit_requested = False
        self._status_message = f"Starting {self.label}…"
        self._show_build_progress = False
        self._rows_added: set[str] = set()
        self._row_cache: dict[str, tuple[str, ...]] = {}
        self._static_cache: dict[str, str] = {}
        self._active_work_units: set[str] = set()
        self._active_activities: dict[str, AgentActivity] = {}
        self._change_queue: asyncio.Queue[Any] | None = None
        document_order = {
            document.id: index for index, document in enumerate(orchestrator.config.documents)
        }
        self.work_units = tuple(
            sorted(
                orchestrator.work_units,
                key=lambda unit: (
                    document_order[unit.document_id],
                    unit.source_span.start_line,
                    unit.source_span.end_line,
                ),
            )
        )
        self._work_units_by_id = {unit.id: unit for unit in self.work_units}
        self._work_unit_order = {unit.id: index for index, unit in enumerate(self.work_units)}

    @property
    def chapters(self) -> tuple[WorkUnitLike, ...]:
        """Compatibility view for integrations using the previous TUI name."""

        return self.work_units

    def compose(self) -> ComposeResult:
        yield Header()
        if self.startup_warning:
            yield Static(f"⚠ {self.startup_warning}", id="startup-warning", markup=False)
        yield Static("Preparing swarm…", id="usage")
        yield Static(id="alerts", markup=False)
        with Horizontal(id="stages"):
            for stage in Stage:
                yield Static(stage.value.title(), id=f"stage-{stage.value}", classes="stage-card")
        yield FixedGridDataTable(id="tasks", zebra_stripes=True, cursor_type="row")
        yield Static(self._status_message, id="status")
        yield Footer()

    def on_mount(self) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        table.add_column("Book", key="book", width=28)
        table.add_column("S/P rank", key="rank", width=10)
        table.add_column("Chapter", key="chapter", width=40)
        for stage in Stage:
            table.add_column(stage.value.title(), key=stage.value, width=18)
        table.add_column("Build", key="build", width=12)
        table.add_column("Current agent activity", key="activity", width=52)
        table.add_column("Tokens · API $", key="tokens", width=22)
        self.set_interval(REFRESH_INTERVAL_SECONDS, self._refresh_active_rows)
        self.run_worker(self.execute(), exclusive=True, group="pipeline")

    def action_inspect_agent(self) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        if not self.work_units or not table.row_count:
            return
        row = table.cursor_row
        if 0 <= row < len(self.work_units):
            self.push_screen(AgentDetailScreen(self.state, self.work_units[row]))

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "inspect_agent" and isinstance(self.screen, AgentDetailScreen):
            return False
        return super().check_action(action, parameters)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "tasks":
            return
        chapter_id = str(event.row_key.value)
        chapter = next((item for item in self.work_units if item.id == chapter_id), None)
        if chapter is not None:
            self.push_screen(AgentDetailScreen(self.state, chapter))

    async def action_quit(self) -> None:
        """Cancel and drain the pipeline before closing the terminal app."""

        self._quit_requested = True
        self.orchestrator.control.stop(integrate_interrupted_workspaces=True)
        self._set_status("Stopping workers and integrating workspace changes…")
        workers = self.workers.cancel_group(self, "pipeline")
        with suppress(WorkerCancelled):
            await self.workers.wait_for_complete(workers)
        self.exit(False)

    async def execute(self) -> None:
        error: BaseException | None = None
        try:
            await self.orchestrator.prepare()
            self._set_status(f"Running {self.label}…", show_build_progress=True)
            await self._bootstrap_dashboard()
            self._change_queue = self.state.change_bus.subscribe()
            self.run_worker(self._consume_changes(), exclusive=True, group="dashboard")
            self.result = await self.operation()
        except BaseException as caught:
            error = caught
        finally:
            try:
                await self.orchestrator.shutdown()
            except BaseException as shutdown_error:
                if error is None:
                    error = shutdown_error

        if error is not None:
            if isinstance(error, asyncio.CancelledError) and self._quit_requested:
                return
            self.fatal_error = error
            self._set_status(f"Fatal orchestrator error: {error}")
            self.set_timer(FAILURE_EXIT_DELAY_SECONDS, lambda: self.exit(False))
            return
        message = (
            "Pipeline completed successfully" if self.result else "Pipeline finished with failures"
        )
        self._set_status(message + " — returning to the shell")
        self.refresh_dashboard((), globals_changed=True)
        self.set_timer(SUCCESS_EXIT_DELAY_SECONDS, lambda: self.exit(self.result))

    async def _bootstrap_dashboard(self, *, chunk_size: int = 200) -> None:
        self.refresh_dashboard((), globals_changed=True)
        for offset in range(0, len(self.work_units), chunk_size):
            self.refresh_dashboard(
                self.work_units[offset : offset + chunk_size], globals_changed=False
            )
            await asyncio.sleep(0)
        self.refresh_dashboard((), globals_changed=True)

    async def _consume_changes(self) -> None:
        assert self._change_queue is not None
        try:
            while True:
                change = await self._change_queue.get()
                work_unit_ids = set(change.work_units)
                globals_changed = bool(change.globals)
                full_resync = change.full_resync
                deadline = asyncio.get_running_loop().time() + DASHBOARD_FRAME_INTERVAL_SECONDS
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(self._change_queue.get(), remaining)
                    except TimeoutError:
                        break
                    work_unit_ids.update(item.work_units)
                    globals_changed = globals_changed or bool(item.globals)
                    full_resync = full_resync or item.full_resync
                selected = None if full_resync else work_unit_ids
                self.refresh_dashboard(selected, globals_changed=globals_changed or full_resync)
        finally:
            self.state.change_bus.unsubscribe(self._change_queue)

    def _refresh_active_rows(self) -> None:
        if self._active_work_units:
            self.refresh_dashboard(self._active_work_units, globals_changed=False)

    def refresh_dashboard(
        self,
        work_units: Iterable[WorkUnitLike] | Iterable[str] | None = None,
        *,
        globals_changed: bool = True,
    ) -> None:
        if not self.state.tasks:
            return
        if work_units is None:
            selected = self.work_units
        else:
            requested = {item if isinstance(item, str) else item.id for item in work_units}
            selected = tuple(
                self._work_units_by_id[item]
                for item in sorted(
                    requested.intersection(self._work_units_by_id),
                    key=self._work_unit_order.__getitem__,
                )
            )
        self._refresh_rows(selected)
        if globals_changed:
            self._refresh_globals()

    def _refresh_globals(self) -> None:
        usage = self.state.invocation_usage()
        lifetime_usage = self.state.total_usage()
        cost = self.state.invocation_cost()
        lifetime_cost = self.state.total_cost()
        agents = running_agent_counts(self.state)
        active_agents = sum(agents.values())
        queued_agents = int(self.state.agent_summary().get("queued", 0))
        discovery_agents = agents[Stage.DISCOVER]
        mutating_agents = active_agents - discovery_agents
        discovery_maximum = self.orchestrator.config.stages[Stage.DISCOVER].max_agents
        assert discovery_maximum is not None
        mutating_maximum = self.orchestrator.config.settings.max_agents
        maximum = discovery_maximum + mutating_maximum
        codex_access = (
            "full"
            if self.orchestrator.config.settings.bypass_approvals_and_sandbox
            else self.orchestrator.config.settings.sandbox
        )
        isolation = self.orchestrator.isolation.name
        critical = " → ".join(self.orchestrator.statement_schedule.critical_path) or "—"
        agent_breakdown = (
            " · ".join(
                f"{stage.value} {agents[stage.value]}" for stage in Stage if agents[stage.value]
            )
            or "none"
        )
        build = self.state.coordinator_build
        build_queue = self.orchestrator.build_queue.snapshot()
        if build.active:
            build_status = (
                f"Coordinator {build.mode} build {build.completed}/{build.total} · "
                f"iter {build.iteration}/{build.maximum_iterations} · "
                f"err {build.error_count} · warn {build.warning_count}"
            )
            if build.current_work_unit_id:
                build_status += f" · {build.current_work_unit_id}"
        else:
            owner = str(build_queue["owner"])
            build_status = (
                f"Coordinator build reserved by {owner}" if owner else "Coordinator build idle"
            )
        if int(build_queue["queued"]):
            build_status += f" · queued {int(build_queue['queued'])}"
        if self._show_build_progress and build.active:
            percent = 100 * build.completed / build.total if build.total else 0
            build_label = "GLOBAL BUILD" if build.mode == "global" else "BUILD"
            footer_status = (
                f"{build_label} {progress_meter(build.completed, build.total)} "
                f"{build.completed}/{build.total} ({percent:.0f}%) · "
                f"iter {build.iteration}/{build.maximum_iterations} · "
                f"err {build.error_count} · warn {build.warning_count}"
            )
            if build.current_work_unit_id:
                footer_status += f" · {build.current_work_unit_id}"
            if build.output_tail:
                status_width = self.query_one("#status", Static).size.width
                line_width = max(20, status_width - 6)
                output_tail = (
                    line if len(line) <= line_width else "…" + line[-line_width + 1 :]
                    for line in build.output_tail
                )
                footer_status += "\n" + "\n".join(f"  {line}" for line in output_tail)
        else:
            footer_status = self._status_message
        self._update_static("#status", footer_status)
        self._update_static(
            "#usage",
            f"{format_usage(usage, label='This invocation')}    "
            f"API-equivalent cost: {format_usd(cost)}    "
            f"lifetime tokens: {format_count(lifetime_usage.total_tokens)}    "
            f"lifetime API-equivalent cost: {format_usd(lifetime_cost)}\n"
            f"Agents {active_agents}/{maximum} · discovery {discovery_agents}/{discovery_maximum} "
            f"· mutating {mutating_agents}/{mutating_maximum} · {agent_breakdown} "
            f"· queued {queued_agents}    "
            f"{build_status}\n"
            f"Statement critical path: {critical}    isolation: {isolation}  "
            f"Lean MCP: on    Codex access: {codex_access}",
        )
        activities = list(self._active_activities.values())
        systemic = [
            (count, message) for count, message in systemic_errors(activities) if count >= 2
        ]
        alert = ""
        if systemic:
            count, message = systemic[0]
            alert = f"SYSTEMIC AGENT ALERT · {count} agents · {message}"
        self._update_static("#alerts", alert)
        for stage in Stage:
            counts = stage_counts(self.state, stage)
            build_targets = set(build.target_work_unit_ids) or (
                {build.current_work_unit_id} if build.current_work_unit_id else set()
            )
            building = len(build_targets) if build.active and build.stage == stage.value else 0
            building_postprocess = sum(
                self.state.task(chapter_id, stage).phase == TaskPhase.POSTPROCESS
                for chapter_id in build_targets
                if build.active and build.stage == stage.value
            )
            postprocessing = max(0, counts["postprocess"] - building_postprocess)
            self._update_static(
                f"#stage-{stage.value}",
                f"[b]{stage.value.title()} chapters[/b]\n"
                f"agent {agents[stage.value]} · postprocess {postprocessing} · "
                f"building {building}\n"
                f"✓ {counts['succeeded']}  "
                f"✗ {counts['failed']}  · {counts['pending']} pending  "
                f"· {counts['queued']} queued  ! {counts['blocked']}  "
                f"Ⅱ {counts['interrupted']}",
            )

    def _refresh_rows(self, work_units: Iterable[WorkUnitLike]) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        for chapter in work_units:
            values = self._row_values(chapter)
            if chapter.id not in self._rows_added:
                table.add_row(*values, key=chapter.id)
                self._rows_added.add(chapter.id)
                self._row_cache[chapter.id] = values
            else:
                previous = self._row_cache[chapter.id]
                for column, old_value, value in zip(
                    (
                        "book",
                        "rank",
                        "chapter",
                        *(stage.value for stage in Stage),
                        "build",
                        "activity",
                        "tokens",
                    ),
                    previous,
                    values,
                    strict=True,
                ):
                    if old_value != value:
                        table.update_cell(chapter.id, column, value)
                self._row_cache[chapter.id] = values
            active_run = self.state.active_run(chapter.id)
            if active_run is None:
                self._active_work_units.discard(chapter.id)
                self._active_activities.pop(chapter.id, None)
            else:
                self._active_work_units.add(chapter.id)
                activity = self.state.activities.get(active_run.id)
                if activity is None:
                    self._active_activities.pop(chapter.id, None)
                else:
                    self._active_activities[chapter.id] = activity

    def _update_static(self, selector: str, content: str) -> None:
        if self._static_cache.get(selector) == content:
            return
        self._static_cache[selector] = content
        self.query_one(selector, Static).update(content)

    def _set_status(self, content: str, *, show_build_progress: bool = False) -> None:
        self._status_message = content
        self._show_build_progress = show_build_progress
        self._update_static("#status", content)

    def _row_values(self, chapter: WorkUnitLike) -> tuple[str, ...]:
        statuses = []
        build = self.state.coordinator_build
        build_targets = set(build.target_work_unit_ids)
        if not build_targets and build.current_work_unit_id:
            build_targets.add(build.current_work_unit_id)
        for stage in Stage:
            task = self.state.task(chapter.id, stage)
            mark = task_mark(
                task,
                building=(
                    build.active and chapter.id in build_targets and build.stage == stage.value
                ),
            )
            statuses.append(f"{mark} ({task.rounds})" if task.rounds else mark)
        usage = chapter_usage(self.state, chapter)
        tokens = format_count(usage.total_tokens) if usage.measured else "—"
        cost = format_usd(self.state.invocation_cost(chapter.id))
        run = latest_run(self.state, chapter)
        active_run = self.state.active_run(chapter.id)
        activity = self.state.activities.get(active_run.id) if active_run is not None else None
        if build.active and chapter.id in build_targets:
            current_activity = f"{build.mode} coordinator build"
        elif active_run is not None:
            current_activity = activity_label(activity, active_run)
        else:
            active_task = next(
                (
                    self.state.task(chapter.id, stage)
                    for stage in Stage
                    if self.state.task(chapter.id, stage).status
                    in (TaskStatus.PENDING, TaskStatus.RUNNING)
                    and self.state.task(chapter.id, stage).detail
                ),
                None,
            )
            if active_task is not None:
                current_activity = active_task.detail
            else:
                prior_activity = self.state.activities.get(run.id) if run is not None else None
                current_activity = activity_label(prior_activity, run)
        statement_rank = self.orchestrator.statement_schedule.rank[chapter.document_id]
        proof_rank = self.orchestrator.proof_schedule.rank[chapter.document_id]
        critical = chapter.document_id in self.orchestrator.statement_schedule.critical_path
        book = f"★ {chapter.document_id}" if critical else chapter.document_id
        clean = self.state.formalize_graph.get("clean", {})
        build_freshness = (
            "✓ fresh" if isinstance(clean, dict) and chapter.id in clean else "○ stale"
        )
        return (
            book,
            f"{statement_rank:g}/{proof_rank:g}",
            f"{chapter.ordinal:02d} {chapter.title}",
            *statuses,
            build_freshness,
            current_activity,
            f"{tokens} · {cost}",
        )


def run_tui(
    orchestrator: Orchestrator,
    operation: Callable[[], Awaitable[bool]],
    *,
    label: str,
    startup_warning: str = "",
) -> bool:
    app = SwarmApp(
        orchestrator,
        operation,
        label=label,
        startup_warning=startup_warning,
    )
    result = app.run()
    if app.fatal_error is not None:
        raise app.fatal_error
    return bool(result)
