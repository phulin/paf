from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, ClassVar

from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import DataTable, Footer, Header, Static

from lastlib_swarm.models import Chapter, Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TaskStatus, TokenUsage

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


def format_usage(usage: TokenUsage) -> str:
    if not usage.measured:
        return "API-equivalent tokens: awaiting measured usage"
    return (
        f"API-equivalent tokens: {format_count(usage.api_tokens)}  "
        f"input {format_count(usage.input_tokens)} "
        f"(cached {format_count(usage.cached_input_tokens)})  "
        f"output {format_count(usage.output_tokens)}  "
        f"reasoning {format_count(usage.reasoning_output_tokens)}"
    )


def chapter_usage(state: StateStore, chapter: Chapter) -> TokenUsage:
    usage = TokenUsage()
    for stage in Stage:
        for run in state.task(chapter.id, stage).runs:
            usage += run.usage
    return usage


def stage_counts(state: StateStore, stage: Stage) -> dict[str, int]:
    counts = {status.value: 0 for status in TaskStatus}
    for task in state.tasks.values():
        if task.stage == stage.value:
            counts[task.status] += 1
    return counts


class SwarmApp(App[bool]):
    TITLE = "LastLib Formalization Swarm"
    SUB_TITLE = "Codex agent pipeline"
    BINDINGS: ClassVar = [("q", "quit", "Stop and quit")]
    CSS = """
    #usage {
        height: 4;
        padding: 1 2;
        background: $primary-background;
        color: $text;
        text-style: bold;
    }
    #stages { height: 5; }
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
    ) -> None:
        super().__init__()
        self.orchestrator = orchestrator
        self.state = orchestrator.state
        self.operation = operation
        self.label = label
        self.result = False
        self._rows_added: set[str] = set()
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
        yield Static("Preparing swarm…", id="usage")
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
        table.add_column("Tokens", key="tokens")
        self.set_interval(0.5, self.refresh_dashboard)
        self.run_worker(self.execute(), exclusive=True, group="pipeline")

    async def execute(self) -> None:
        error: Exception | None = None
        try:
            await self.orchestrator.prepare()
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
        usage = self.state.total_usage()
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
        self.query_one("#usage", Static).update(
            f"{format_usage(usage)}    active stage records: {active}  concurrency cap: {maximum}\n"
            f"Statement critical path: {critical}    isolation: {isolation}  "
            f"Lean MCP: {lean_mcp}    Codex access: {codex_access}"
        )
        for stage in Stage:
            counts = stage_counts(self.state, stage)
            self.query_one(f"#stage-{stage.value}", Static).update(
                f"[b]{stage.value.title()}[/b]\n"
                f"▶ {counts['running']}  ✓ {counts['succeeded']}  "
                f"✗ {counts['failed']}  · {counts['pending']}  ! {counts['blocked']}"
            )
        table: DataTable[Any] = self.query_one("#tasks", DataTable)
        for chapter in self.chapters:
            values = self._row_values(chapter)
            if chapter.id not in self._rows_added:
                table.add_row(*values, key=chapter.id)
                self._rows_added.add(chapter.id)
            else:
                for column, value in zip(
                    ("book", "rank", "chapter", *(stage.value for stage in Stage), "tokens"),
                    values,
                    strict=True,
                ):
                    table.update_cell(chapter.id, column, value)

    def _row_values(self, chapter: Chapter) -> tuple[str, ...]:
        statuses = []
        for stage in Stage:
            task = self.state.task(chapter.id, stage)
            mark = STATUS_MARKS[TaskStatus(task.status)]
            statuses.append(f"{mark} ({task.rounds})" if task.rounds else mark)
        usage = chapter_usage(self.state, chapter)
        tokens = format_count(usage.api_tokens) if usage.measured else "—"
        statement_rank = self.orchestrator.statement_schedule.rank[chapter.book_id]
        proof_rank = self.orchestrator.proof_schedule.rank[chapter.book_id]
        critical = chapter.book_id in self.orchestrator.statement_schedule.critical_path
        book = f"★ {chapter.book_id}" if critical else chapter.book_id
        return (
            book,
            f"{statement_rank:g}/{proof_rank:g}",
            f"{chapter.number:02d} {chapter.title}",
            *statuses,
            tokens,
        )


def run_tui(
    orchestrator: Orchestrator,
    operation: Callable[[], Awaitable[bool]],
    *,
    label: str,
) -> bool:
    result = SwarmApp(orchestrator, operation, label=label).run()
    return bool(result)
