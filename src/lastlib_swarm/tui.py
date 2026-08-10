from __future__ import annotations

from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, RichLog, Static, TabbedContent, TabPane
from textual.worker import WorkerCancelled

from lastlib_swarm import json_codec as json
from lastlib_swarm.activity import AgentActivity, reportable_error, systemic_errors
from lastlib_swarm.models import Chapter, Stage
from lastlib_swarm.pricing import format_usd
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus, TokenUsage

STATUS_MARKS = {
    TaskStatus.PENDING: "· pending",
    TaskStatus.RUNNING: "▶ running",
    TaskStatus.SUCCEEDED: "✓ done",
    TaskStatus.FAILED: "✗ failed",
    TaskStatus.BLOCKED: "! blocked",
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


def chapter_usage(state: StateStore, chapter: Chapter) -> TokenUsage:
    return state.invocation_usage(chapter.id)


def stage_counts(state: StateStore, stage: Stage) -> dict[str, int]:
    counts = {status.value: 0 for status in TaskStatus}
    for task in state.tasks.values():
        if task.stage == stage.value:
            counts[task.status] += 1
    return counts


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def seconds_since(value: str) -> float:
    try:
        return (datetime.now(UTC) - datetime.fromisoformat(value)).total_seconds()
    except ValueError:
        return 0


def latest_run(state: StateStore, chapter: Chapter) -> RunRecord | None:
    runs = [run for stage in Stage for run in state.task(chapter.id, stage).runs]
    if not runs:
        return None
    running = [run for run in runs if run.status == TaskStatus.RUNNING]
    return max(running or runs, key=lambda run: run.started_at)


def activity_label(activity: AgentActivity | None, run: RunRecord | None) -> str:
    if run is None:
        return "—"
    if activity is None:
        return "awaiting events" if run.status == TaskStatus.RUNNING else "no activity summary"
    age = seconds_since(activity.updated_at)
    prefix = f"✗ {activity.failures}  " if activity.failures else ""
    idle = f"  idle {int(age // 60)}m" if run.status == TaskStatus.RUNNING and age >= 60 else ""
    return f"{prefix}{activity.current}{idle}"


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
    #agent-heading { height: 4; padding: 1 2; background: $primary-background; text-style: bold; }
    #agent-metrics { height: 5; }
    .agent-card { width: 1fr; height: 5; border: round $primary; padding: 0 1; }
    #agent-summary { height: 8; padding: 1 2; border: round $primary; }
    #agent-error { height: auto; max-height: 5; padding: 0 2; color: $error; }
    #agent-tabs { height: 1fr; }
    RichLog { height: 1fr; }
    #agent-path { height: 3; padding: 1 2; }
    """

    def __init__(self, state: StateStore, chapter: Chapter) -> None:
        super().__init__()
        self.state = state
        self.chapter = chapter
        self._rendered_sequence = -1
        self._static_cache: dict[str, str] = {}
        self._raw_path: Path | None = None
        self._raw_offset = 0
        self._raw_pending = bytearray()
        self._raw_lines: deque[str] = deque(maxlen=30)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Loading agent activity…", id="agent-heading", markup=False)
        with Horizontal(id="agent-metrics"):
            yield Static(id="agent-state", classes="agent-card", markup=False)
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
                yield RichLog(id="agent-timeline", wrap=True, markup=False)
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
        self.set_interval(1.0, self.refresh_agent)

    def action_close(self) -> None:
        self.app.pop_screen()

    def refresh_agent(self) -> None:
        run = latest_run(self.state, self.chapter)
        if run is None:
            self._update_static("#agent-heading", f"{self.chapter.id} — no agent run recorded")
            return
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
            f"{self.chapter.id} · {run.stage} round {run.round} · {run.status}\n"
            f"{activity_label(activity, run)}",
        )
        thread = run.thread_id or "awaiting thread id"
        self._update_static("#agent-state", f"PROCESS\nPID {run.pid or '—'}\n{thread}")
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
        summary = (
            activity.latest_summary if activity and activity.latest_summary else "No update yet."
        )
        self._update_log("#agent-summary", f"LATEST AGENT UPDATE\n{summary}")
        error = reportable_error(activity.latest_error) if activity else ""
        self._update_static("#agent-error", f"LATEST ERROR\n{error}" if error else "")
        path = Path(run.log_path) if run.log_path else self.state.logs_dir / f"{run.id}.jsonl"
        self._update_static("#agent-path", f"Raw JSONL: {path}")
        if activity and activity.sequence != self._rendered_sequence:
            self._render_activity(activity)
            self._rendered_sequence = activity.sequence
        tabs = self.query_one("#agent-tabs", TabbedContent)
        if tabs.active == "raw-pane":
            self._refresh_raw_events(path)

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
        log.write(content)
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

    def _render_activity(self, activity: AgentActivity) -> None:
        timeline = self.query_one("#agent-timeline", RichLog)
        timeline.clear()
        marks = {"started": "▶", "completed": "✓", "failed": "✗", "updated": "•"}
        for entry in activity.recent:
            clock = entry.at[11:19] if len(entry.at) >= 19 else entry.at
            line = f"{clock} {marks.get(entry.status, '•')} [{entry.kind}] {entry.title}"
            if entry.detail:
                line += f"\n    {entry.detail}"
            timeline.write(line)

        plan = self.query_one("#agent-plan", RichLog)
        plan.clear()
        for item in activity.todos:
            mark = "✓" if item.get("completed") else "·"
            plan.write(f"{mark} {item.get('text', '')}")
        if not activity.todos:
            plan.write("No todo list emitted yet.")

        files = self.query_one("#agent-files", RichLog)
        files.clear()
        for path in activity.files:
            files.write(path)
        if not activity.files:
            files.write("No file-change event emitted yet.")


class SwarmApp(App[bool]):
    TITLE = "LastLib Formalization Swarm"
    SUB_TITLE = "Codex agent pipeline"
    BINDINGS: ClassVar = [("q", "quit", "Stop and quit"), ("i", "inspect_agent", "Inspect")]
    CSS = """
    #usage {
        height: auto;
        min-height: 4;
        max-height: 7;
        padding: 1 2;
        background: $primary-background;
        color: $text;
        text-style: bold;
    }
    #startup-warning {
        height: auto;
        max-height: 4;
        padding: 1 2;
        background: $warning;
        color: $text;
        text-style: bold;
    }
    #stages { height: 5; }
    #alerts { height: auto; max-height: 3; padding: 0 2; color: $error; }
    .stage-card {
        width: 1fr;
        height: 5;
        border: round $primary;
        padding: 0 1;
    }
    #tasks { height: 1fr; }
    #status { height: 3; padding: 1 2; }
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
        self.orchestrator = orchestrator
        self.state = orchestrator.state
        self.operation = operation
        self.label = label
        self.startup_warning = startup_warning
        self.result = False
        self._rows_added: set[str] = set()
        self._row_cache: dict[str, tuple[str, ...]] = {}
        self._static_cache: dict[str, str] = {}
        position = {
            book_id: index for index, book_id in enumerate(orchestrator.statement_schedule.order)
        }
        self.chapters = tuple(
            sorted(
                orchestrator.chapters,
                key=lambda chapter: (position[chapter.book_id], chapter.number),
            )
        )

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
        yield Static(f"Starting {self.label}…", id="status")
        yield Footer()

    def on_mount(self) -> None:
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        table.add_column("Book", key="book")
        table.add_column("S/P rank", key="rank")
        table.add_column("Chapter", key="chapter")
        for stage in Stage:
            table.add_column(stage.value.title(), key=stage.value)
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
        self.query_one("#status", Static).update("Stopping workers and cleaning workspaces…")
        workers = self.workers.cancel_group(self, "pipeline")
        with suppress(WorkerCancelled):
            await self.workers.wait_for_complete(workers)
        self.exit(False)

    async def execute(self) -> None:
        error: Exception | None = None
        try:
            await self.orchestrator.prepare()
            self.query_one("#status", Static).update(f"Running {self.label}…")
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
            self.query_one("#status", Static).update(f"Fatal orchestrator error: {error}")
            self.set_timer(2.0, lambda: self.exit(False))
            return
        message = (
            "Pipeline completed successfully" if self.result else "Pipeline finished with failures"
        )
        self.query_one("#status", Static).update(message + " — returning to the shell")
        self.refresh_dashboard()
        self.set_timer(1.0, lambda: self.exit(self.result))

    def refresh_dashboard(self) -> None:
        if not self.state.tasks:
            return
        usage = self.state.invocation_usage()
        lifetime_usage = self.state.total_usage()
        cost = self.state.invocation_cost()
        lifetime_cost = self.state.total_cost()
        active = sum(task.status == TaskStatus.RUNNING for task in self.state.tasks.values())
        maximum = self.orchestrator.config.settings.max_agents
        lean_mcp = "on" if self.orchestrator.config.settings.lean_mcp else "off"
        codex_access = (
            "full"
            if self.orchestrator.config.settings.bypass_approvals_and_sandbox
            else self.orchestrator.config.settings.sandbox
        )
        isolation = self.orchestrator.isolation.name
        critical = " → ".join(self.orchestrator.statement_schedule.critical_path) or "—"
        self._update_static(
            "#usage",
            f"{format_usage(usage, label='This invocation')}    "
            f"API-equivalent cost: {format_usd(cost)}    "
            f"lifetime tokens: {format_count(lifetime_usage.total_tokens)}    "
            f"lifetime API-equivalent cost: {format_usd(lifetime_cost)}    "
            f"active stage records: {active}  concurrency cap: {maximum}\n"
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
            self._update_static(
                f"#stage-{stage.value}",
                f"[b]{stage.value.title()}[/b]\n"
                f"▶ {counts['running']}  ✓ {counts['succeeded']}  "
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

    def _row_values(self, chapter: Chapter) -> tuple[str, ...]:
        statuses = []
        for stage in Stage:
            task = self.state.task(chapter.id, stage)
            mark = STATUS_MARKS[TaskStatus(task.status)]
            statuses.append(f"{mark} ({task.rounds})" if task.rounds else mark)
        usage = chapter_usage(self.state, chapter)
        tokens = format_count(usage.total_tokens) if usage.measured else "—"
        cost = format_usd(self.state.invocation_cost(chapter.id))
        run = latest_run(self.state, chapter)
        activity = self.state.activities.get(run.id) if run is not None else None
        statement_rank = self.orchestrator.statement_schedule.rank[chapter.book_id]
        proof_rank = self.orchestrator.proof_schedule.rank[chapter.book_id]
        critical = chapter.book_id in self.orchestrator.statement_schedule.critical_path
        book = f"★ {chapter.book_id}" if critical else chapter.book_id
        return (
            book,
            f"{statement_rank:g}/{proof_rank:g}",
            f"{chapter.number:02d} {chapter.title}",
            *statuses,
            activity_label(activity, run),
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
