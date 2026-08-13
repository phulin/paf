from __future__ import annotations

import re
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal
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
)
from textual.worker import WorkerCancelled

from lastlib_swarm import json_codec as json
from lastlib_swarm.activity import AgentActivity, reportable_error, systemic_errors
from lastlib_swarm.models import Chapter, Stage
from lastlib_swarm.pricing import format_usd
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import RunRecord, StateStore, TaskPhase, TaskRecord, TaskStatus, TokenUsage

TUI_THEME = "ansi-dark"
MAX_TIMELINE_EVENTS = 10_000


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
}

PHASE_MARKS = {
    TaskPhase.IDLE: "▶ in progress",
    TaskPhase.WAITING_PREREQUISITES: "⌛ prerequisites",
    TaskPhase.RECOVERING: "↺ recovering",
    TaskPhase.WAITING_BUILD: "⌛ build queue",
    TaskPhase.QUEUED: "… queued",
    TaskPhase.BUILDING: "◆ building",
    TaskPhase.AGENT: "▶ agent",
    TaskPhase.VERIFICATION_QUEUED: "⌛ verify queue",
    TaskPhase.VERIFYING: "◆ verifying",
    TaskPhase.WAITING_FIXUP: "⌛ fixup",
    TaskPhase.AWAITING_REBUILD: "↻ rebuild",
}

BOOK_ID = re.compile(r"^book(?P<number>\d+)$", re.IGNORECASE)


def chapter_display_sort_key(chapter: Chapter) -> tuple[tuple[int, int | str], int]:
    """Sort canonical book ids numerically, then sort chapters numerically."""

    match = BOOK_ID.fullmatch(chapter.book_id)
    if match is None:
        return (1, chapter.book_id.casefold()), chapter.number
    return (0, int(match.group("number"))), chapter.number


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


def chapter_usage(state: StateStore, chapter: Chapter) -> TokenUsage:
    return state.invocation_usage(chapter.id)


def stage_counts(state: StateStore, stage: Stage) -> dict[str, int]:
    return state.stage_counts(stage)[0]


def stage_phase_counts(state: StateStore, stage: Stage) -> dict[str, int]:
    return state.stage_counts(stage)[1]


def running_agent_counts(state: StateStore) -> dict[str, int]:
    summary = state.agent_summary()
    by_stage = summary.get("by_stage", {})
    return {
        stage.value: int(by_stage.get(stage.value, 0)) if isinstance(by_stage, dict) else 0
        for stage in Stage
    }


def task_mark(task: TaskRecord) -> str:
    if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING) and task.phase != TaskPhase.IDLE:
        return PHASE_MARKS.get(TaskPhase(task.phase), STATUS_MARKS[TaskStatus.RUNNING])
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


def latest_run(state: StateStore, chapter: Chapter) -> RunRecord | None:
    return state.latest_run(chapter.id)


def chapter_runs(state: StateStore, chapter: Chapter) -> list[RunRecord]:
    """Return every agent step for a chapter in chronological order."""

    return list(state.chapter_runs(chapter.id))


def run_tab_label(run: RunRecord, step: int) -> str:
    marks = {
        TaskStatus.RUNNING: "▶",
        TaskStatus.SUCCEEDED: "✓",
        TaskStatus.FAILED: "✗",
        TaskStatus.BLOCKED: "!",
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

    def __init__(self, state: StateStore, chapter: Chapter) -> None:
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
        self._timeline_activities: dict[str, AgentActivity] = {}

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
                    max_lines=None,
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
        self.set_interval(1.0, self.refresh_agent)

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
        self.refresh_agent()

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
        timeline_activity = self._timeline_activity(run, activity, path)
        timeline_width = _log_render_width(self.query_one("#agent-timeline", RichLog))
        rendered_activity = (
            run.id,
            timeline_activity.sequence if timeline_activity else None,
            timeline_width,
        )
        if rendered_activity != self._rendered_activity:
            self._render_activity(timeline_activity)
            self._rendered_activity = rendered_activity
        tabs = self.query_one("#agent-tabs", TabbedContent)
        if tabs.active == "raw-pane":
            self._refresh_raw_events(path)

    def _timeline_activity(
        self,
        run: RunRecord,
        compact: AgentActivity | None,
        path: Path,
    ) -> AgentActivity | None:
        """Replay a run once, then extend that full timeline from its live sidecar."""

        timeline = self._timeline_activities.get(run.id)
        if timeline is None:
            replayed = self.state.activities.replay(
                run.id,
                run.chapter_id,
                run.stage,
                path,
                workspace_root=self.state.config.settings.repo,
                maximum_events=MAX_TIMELINE_EVENTS,
                cache=False,
            )
            if replayed is None:
                # A newly started run may not have created its JSONL yet. Do not
                # cache the compact fallback, so a later refresh can replay it.
                return compact
            timeline = replayed
            self._timeline_activities[run.id] = timeline

        if compact is None or timeline is None or compact.sequence <= timeline.sequence:
            return timeline

        additions = [entry for entry in compact.recent if entry.sequence > timeline.sequence]
        if not additions or additions[0].sequence != timeline.sequence + 1:
            replayed = self.state.activities.replay(
                run.id,
                run.chapter_id,
                run.stage,
                path,
                workspace_root=self.state.config.settings.repo,
                maximum_events=MAX_TIMELINE_EVENTS,
                cache=False,
            )
            if replayed is not None:
                self._timeline_activities[run.id] = replayed
                return replayed
            return timeline

        timeline.recent.extend(additions)
        del timeline.recent[:-MAX_TIMELINE_EVENTS]
        timeline.sequence = compact.sequence
        timeline.updated_at = compact.updated_at
        timeline.latest_summary = compact.latest_summary
        timeline.latest_error = compact.latest_error
        return timeline

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
        marks = {"started": "▶", "completed": "✓", "failed": "✗", "updated": "•"}
        if activity and activity.recent and activity.recent[0].sequence > 1:
            omitted = activity.recent[0].sequence - 1
            timeline.write(
                f"… {omitted:,} earlier timeline events omitted (extreme-length safeguard)",
                width=write_width,
            )
        latest_message = (
            max(
                (entry.sequence for entry in activity.recent if entry.kind == "message"),
                default=None,
            )
            if activity
            else None
        )
        for entry in activity.recent if activity else ():
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
                    line.append("\n" + "\n".join(f"    {part}" for part in detail.splitlines()))
            timeline.write(line, width=write_width)
        if activity is None:
            timeline.write("No activity events recorded for this step.", width=write_width)

        plan = self.query_one("#agent-plan", RichLog)
        plan.clear()
        for item in activity.todos if activity else ():
            mark = "✓" if item.get("completed") else "·"
            plan.write(f"{mark} {item.get('text', '')}")
        if activity is None or not activity.todos:
            plan.write("No todo list emitted yet.")

        files = self.query_one("#agent-files", RichLog)
        files.clear()
        for path in activity.files if activity else ():
            files.write(path)
        if activity is None or not activity.files:
            files.write("No file-change event emitted yet.")


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
        self._status_message = f"Starting {self.label}…"
        self._show_build_progress = False
        self._rows_added: set[str] = set()
        self._row_cache: dict[str, tuple[str, ...]] = {}
        self._static_cache: dict[str, str] = {}
        self.chapters = tuple(sorted(orchestrator.chapters, key=chapter_display_sort_key))

    def compose(self) -> ComposeResult:
        yield Header()
        if self.startup_warning:
            yield Static(f"⚠ {self.startup_warning}", id="startup-warning", markup=False)
        yield Static("Preparing swarm…", id="usage")
        yield Static(id="alerts", markup=False)
        with Horizontal(id="stages"):
            for stage in Stage:
                yield Static(stage.value.title(), id=f"stage-{stage.value}", classes="stage-card")
        yield DataTable(id="tasks", zebra_stripes=True, cursor_type="row")
        yield Static(self._status_message, id="status")
        yield Footer()

    def on_mount(self) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        table.add_column("Book", key="book")
        table.add_column("S/P rank", key="rank")
        table.add_column("Chapter", key="chapter", width=40)
        for stage in Stage:
            table.add_column(stage.value.title(), key=stage.value)
        table.add_column("Build", key="build")
        table.add_column("Current agent activity", key="activity")
        table.add_column("Tokens · API $", key="tokens")
        self.set_interval(1.0, self.refresh_dashboard)
        self.run_worker(self.execute(), exclusive=True, group="pipeline")

    def action_inspect_agent(self) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        if not self.chapters or not table.row_count:
            return
        row = table.cursor_row
        if 0 <= row < len(self.chapters):
            self.push_screen(AgentDetailScreen(self.state, self.chapters[row]))

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "inspect_agent" and isinstance(self.screen, AgentDetailScreen):
            return False
        return super().check_action(action, parameters)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "tasks":
            return
        chapter_id = str(event.row_key.value)
        chapter = next((item for item in self.chapters if item.id == chapter_id), None)
        if chapter is not None:
            self.push_screen(AgentDetailScreen(self.state, chapter))

    async def action_quit(self) -> None:
        """Cancel and drain the pipeline before closing the terminal app."""

        self.orchestrator.control.stop()
        self._set_status("Stopping workers and cleaning workspaces…")
        workers = self.workers.cancel_group(self, "pipeline")
        with suppress(WorkerCancelled):
            await self.workers.wait_for_complete(workers)
        self.exit(False)

    async def execute(self) -> None:
        error: Exception | None = None
        try:
            await self.orchestrator.prepare()
            self._set_status(f"Running {self.label}…", show_build_progress=True)
            self.refresh_dashboard()
            self.result = await self.operation()
        except Exception as caught:
            error = caught
        finally:
            try:
                await self.orchestrator.shutdown()
            except Exception as shutdown_error:
                if error is None:
                    error = shutdown_error

        if error is not None:
            self._set_status(f"Fatal orchestrator error: {error}")
            self.set_timer(2.0, lambda: self.exit(False))
            return
        message = (
            "Pipeline completed successfully" if self.result else "Pipeline finished with failures"
        )
        self._set_status(message + " — returning to the shell")
        self.refresh_dashboard()
        self.set_timer(1.0, lambda: self.exit(self.result))

    def refresh_dashboard(self) -> None:
        if not self.state.tasks:
            return
        usage = self.state.invocation_usage()
        lifetime_usage = self.state.total_usage()
        cost = self.state.invocation_cost()
        lifetime_cost = self.state.total_cost()
        agents = running_agent_counts(self.state)
        active_agents = sum(agents.values())
        queued_agents = sum(
            task.status == TaskStatus.RUNNING and task.phase == TaskPhase.QUEUED
            for task in self.state.tasks.values()
        )
        maximum = self.orchestrator.config.settings.max_agents
        lean_mcp = "on" if self.orchestrator.config.settings.lean_mcp else "off"
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
            if build.current_chapter_id:
                build_status += f" · {build.current_chapter_id}"
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
            if build.current_chapter_id:
                footer_status += f" · {build.current_chapter_id}"
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
            f"Agents {active_agents}/{maximum} · {agent_breakdown} · queued {queued_agents}    "
            f"{build_status}\n"
            f"Statement critical path: {critical}    isolation: {isolation}  "
            f"Lean MCP: {lean_mcp}    Codex access: {codex_access}",
        )
        activities: list[AgentActivity] = []
        for chapter in self.chapters:
            run = latest_run(self.state, chapter)
            if (
                run is not None
                and run.status == TaskStatus.RUNNING
                and (activity := self.state.activities.get(run.id)) is not None
            ):
                activities.append(activity)
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
            phases = stage_phase_counts(self.state, stage)
            self._update_static(
                f"#stage-{stage.value}",
                f"[b]{stage.value.title()} chapters[/b]\n"
                f"agent {agents[stage.value]} · queue {phases['queued']} · "
                f"buildq {phases['waiting_build']} · build {phases['building']} · "
                f"verifyq {phases['verification_queued']} · verify {phases['verifying']} · "
                f"fixwait {phases['waiting_fixup']} · "
                f"deps {phases['waiting_prerequisites']} · recover {phases['recovering']}\n"
                f"✓ {counts['succeeded']}  "
                f"✗ {counts['failed']}  · {counts['pending']}  ! {counts['blocked']}",
            )
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        for chapter in self.chapters:
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

    def _update_static(self, selector: str, content: str) -> None:
        if self._static_cache.get(selector) == content:
            return
        self._static_cache[selector] = content
        self.query_one(selector, Static).update(content)

    def _set_status(self, content: str, *, show_build_progress: bool = False) -> None:
        self._status_message = content
        self._show_build_progress = show_build_progress
        self._update_static("#status", content)

    def _row_values(self, chapter: Chapter) -> tuple[str, ...]:
        statuses = []
        for stage in Stage:
            task = self.state.task(chapter.id, stage)
            mark = task_mark(task)
            statuses.append(f"{mark} ({task.rounds})" if task.rounds else mark)
        usage = chapter_usage(self.state, chapter)
        tokens = format_count(usage.total_tokens) if usage.measured else "—"
        cost = format_usd(self.state.invocation_cost(chapter.id))
        run = latest_run(self.state, chapter)
        active_run = self.state.active_run(chapter.id)
        activity = self.state.activities.get(active_run.id) if active_run is not None else None
        if active_run is not None:
            current_activity = activity_label(activity, active_run)
        else:
            active_task = next(
                (
                    self.state.task(chapter.id, stage)
                    for stage in Stage
                    if self.state.task(chapter.id, stage).status
                    in (TaskStatus.PENDING, TaskStatus.RUNNING)
                    and self.state.task(chapter.id, stage).phase != TaskPhase.IDLE
                ),
                None,
            )
            phase_activity = {
                TaskPhase.QUEUED: "queued for agent",
                TaskPhase.WAITING_PREREQUISITES: "waiting for prerequisites",
                TaskPhase.RECOVERING: "recovering interrupted work",
                TaskPhase.WAITING_BUILD: "waiting for coordinator build",
                TaskPhase.BUILDING: "coordinator building",
                TaskPhase.VERIFICATION_QUEUED: "queued for coordinator verification",
                TaskPhase.VERIFYING: "coordinator verifying",
                TaskPhase.WAITING_FIXUP: "waiting for targeted fixup",
                TaskPhase.AWAITING_REBUILD: "awaiting coordinator rebuild",
            }
            if active_task is not None and active_task.phase in phase_activity:
                current_activity = phase_activity[TaskPhase(active_task.phase)]
            else:
                prior_activity = self.state.activities.get(run.id) if run is not None else None
                current_activity = activity_label(prior_activity, run)
        statement_rank = self.orchestrator.statement_schedule.rank[chapter.book_id]
        proof_rank = self.orchestrator.proof_schedule.rank[chapter.book_id]
        critical = chapter.book_id in self.orchestrator.statement_schedule.critical_path
        book = f"★ {chapter.book_id}" if critical else chapter.book_id
        clean = self.state.fixup_graph.get("clean", {})
        build_freshness = (
            "✓ fresh" if isinstance(clean, dict) and chapter.id in clean else "○ stale"
        )
        return (
            book,
            f"{statement_rank:g}/{proof_rank:g}",
            f"{chapter.number:02d} {chapter.title}",
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
    result = SwarmApp(
        orchestrator,
        operation,
        label=label,
        startup_warning=startup_warning,
    ).run()
    return bool(result)
