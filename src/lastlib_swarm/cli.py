from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lastlib_swarm import json_codec as json
from lastlib_swarm.activity import ActivityStore, reportable_error
from lastlib_swarm.config import infer_corpus, resolve_config
from lastlib_swarm.control import (
    LOG_NAME,
    ControlServer,
    control_socket,
    offline_status,
    send_command,
)
from lastlib_swarm.corpus import (
    build_corpus_schedule,
    scheduling_snapshot,
    scheduling_summary,
)
from lastlib_swarm.isolation import fuse_overlay_available
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.pricing import LEGACY_MODEL, CostEstimate, estimate_cost, format_usd
from lastlib_swarm.scheduler import Orchestrator, scaffold_directories
from lastlib_swarm.state import StateStore, TaskStatus
from lastlib_swarm.tui import format_count, format_usage, run_tui

RIPGREP_WARNING = (
    "ripgrep (`rg`) was not found on PATH. Swarm agents rely on fast source search and may be "
    "substantially slower. Install ripgrep and ensure `rg` is on PATH before launching a large run."
)


def _starts_workers(args: argparse.Namespace) -> bool:
    return args.command in {"stage", "pipeline", "corpus"} or (
        args.command == "agent" and args.agent_command in {"start", "serve"}
    )


def _warn_missing_ripgrep() -> None:
    Console(stderr=True).print(
        Panel(
            RIPGREP_WARNING,
            title="[bold]⚠ MISSING RECOMMENDED TOOL[/bold]",
            border_style="bold red",
        )
    )


def _add_source(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "target",
        nargs="?",
        help="informal book Markdown file or corpus directory; inferred without --config",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="optional pipeline TOML; defaults to target inference or ./swarm.toml",
    )
    parser.add_argument(
        "--dependencies",
        help="Mermaid book DAG; defaults to BOOK_DEPENDENCIES.md for inferred corpora",
    )


def _add_overrides(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--model", help="override the configured Codex model")
    parser.add_argument("--reasoning-effort", help="override the configured reasoning effort")
    parser.add_argument("--max-agents", type=int, help="override the concurrency cap")
    parser.add_argument(
        "--isolation",
        choices=("auto", "fuse-overlay", "shared"),
        help="execution isolation backend",
    )
    parser.add_argument(
        "--lean-mcp",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="provide proof attempts with an isolated Lean LSP MCP (default: enabled)",
    )


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    _add_source(parser)
    _add_overrides(parser)
    parser.add_argument(
        "--book", action="append", default=[], help="book id; repeat to select several"
    )
    parser.add_argument(
        "--chapter",
        action="append",
        default=[],
        help="chapter id (book/chapter-NN) or number; repeat to select several",
    )
    parser.add_argument(
        "--force", action="store_true", help="rerun stages already marked successful"
    )
    parser.add_argument("--no-tui", action="store_true", help="run headlessly for automation")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="lastlib-swarm", description=__doc__)
    root.add_argument("--version", action="version", version="lastlib-swarm 0.7.0")
    commands = root.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="show discovered books, chapters, and stage settings")
    _add_source(plan)
    _add_overrides(plan)

    status = commands.add_parser("status", help="show persisted pipeline state and token usage")
    _add_source(status)
    status.add_argument("--json", action="store_true", help="print the raw persisted state")

    source_issues = commands.add_parser(
        "source-issues", help="show the accumulated informal-textbook issue ledger"
    )
    _add_source(source_issues)
    source_issues.add_argument("--json", action="store_true", help="print the raw ledger")

    scaffold = commands.add_parser(
        "scaffold", help="deterministically create configured chapter directories"
    )
    _add_source(scaffold)
    scaffold.add_argument("--book", action="append", default=[], help="book id")
    scaffold.add_argument("--chapter", action="append", default=[], help="chapter id or number")

    stage = commands.add_parser("stage", help="run one stage across selected chapters")
    stage.add_argument("stage", choices=[item.value for item in Stage])
    _add_run_options(stage)

    pipeline = commands.add_parser(
        "pipeline", help="run scaffold/draft/fixup/review/prove to convergence"
    )
    _add_run_options(pipeline)

    corpus = commands.add_parser(
        "corpus", help="run a dependency-scheduled collection of Markdown books"
    )
    corpus.add_argument(
        "targets",
        nargs="*",
        help="Markdown files and/or directories (directories expand to their direct *.md files)",
    )
    corpus.add_argument("--config", help="use an explicit multi-book TOML configuration")
    corpus.add_argument(
        "--dependencies",
        help="Mermaid book DAG; defaults to BOOK_DEPENDENCIES.md in the repository",
    )
    _add_overrides(corpus)
    corpus.add_argument("--book", action="append", default=[])
    corpus.add_argument("--chapter", action="append", default=[])
    corpus.add_argument("--force", action="store_true")
    corpus.add_argument("--no-tui", action="store_true")

    agent = commands.add_parser("agent", help="machine-friendly background pipeline control")
    agent_commands = agent.add_subparsers(dest="agent_command", required=True)
    for name, help_text in (
        ("start", "start a detached managed pipeline"),
        ("serve", "run the managed pipeline server in the foreground"),
    ):
        command = agent_commands.add_parser(name, help=help_text)
        _add_source(command)
        _add_overrides(command)
        command.add_argument(
            "--stage",
            choices=["pipeline", *(stage.value for stage in Stage)],
            default="pipeline",
            help="managed operation (default: pipeline)",
        )
        command.add_argument("--force", action="store_true")
    for name, help_text in (
        ("status", "return a compact JSON status"),
        ("snapshot", "return status plus the complete persisted state"),
        ("pause", "pause before new chapter attempts"),
        ("resume", "release paused chapter attempts"),
        ("unblock", "reset blocked tasks to pending"),
        ("stop", "cancel the pipeline and active subprocesses"),
        ("wait", "block until the managed pipeline exits"),
    ):
        command = agent_commands.add_parser(name, help=help_text)
        _add_source(command)
    rpc = agent_commands.add_parser("rpc", help="read control commands as JSONL from stdin")
    _add_source(rpc)
    inspect = agent_commands.add_parser("inspect", help="show one agent's live activity")
    _add_source(inspect)
    inspect.add_argument(
        "--chapter", help="chapter id or number; defaults to the most recent active agent"
    )
    inspect.add_argument("--run", help="exact run id")
    inspect.add_argument("--follow", action="store_true", help="refresh until the run finishes")
    inspect.add_argument("--json", action="store_true", help="emit JSON (JSONL with --follow)")
    inspect.add_argument("--interval", type=float, default=0.5, help="follow refresh interval")
    return root


def _config_from_args(args: argparse.Namespace) -> PipelineConfig:
    if args.command == "corpus":
        if args.config is not None:
            if args.targets:
                raise ValueError("pass either --config or corpus targets, not both")
            config = resolve_config(
                config=args.config,
                target=None,
                dependency_file=args.dependencies,
            )
        else:
            config = infer_corpus(tuple(args.targets), dependency_file=args.dependencies)
    else:
        config = resolve_config(
            config=args.config,
            target=args.target,
            dependency_file=getattr(args, "dependencies", None),
        )
    model = getattr(args, "model", None)
    reasoning_effort = getattr(args, "reasoning_effort", None)
    max_agents = getattr(args, "max_agents", None)
    isolation = getattr(args, "isolation", None)
    lean_mcp = getattr(args, "lean_mcp", None)
    if max_agents is not None and max_agents < 1:
        raise ValueError("--max-agents must be positive")
    if (
        model is not None
        or reasoning_effort is not None
        or max_agents is not None
        or isolation is not None
        or lean_mcp is not None
    ):
        settings = replace(
            config.settings,
            model=model or config.settings.model,
            reasoning_effort=reasoning_effort or config.settings.reasoning_effort,
            max_agents=max_agents or config.settings.max_agents,
            isolation=isolation or config.settings.isolation,
            lean_mcp=config.settings.lean_mcp if lean_mcp is None else lean_mcp,
        )
        config = replace(config, settings=settings)
    return config


def select_chapters(
    config: PipelineConfig,
    *,
    books: Sequence[str],
    chapter_selectors: Sequence[str],
) -> tuple[Chapter, ...]:
    chapters = list(config.chapters)
    if books:
        unknown = set(books) - {book.id for book in config.books}
        if unknown:
            raise ValueError(f"unknown book ids: {', '.join(sorted(unknown))}")
        chapters = [chapter for chapter in chapters if chapter.book_id in books]
    if chapter_selectors:
        selected: list[Chapter] = []
        for selector in chapter_selectors:
            matches = [chapter for chapter in chapters if chapter.id == selector]
            if not matches and selector.isdigit():
                matches = [chapter for chapter in chapters if chapter.number == int(selector)]
            if not matches:
                raise ValueError(f"chapter selector matched nothing: {selector}")
            selected.extend(matches)
        selected_ids = {chapter.id for chapter in selected}
        chapters = [chapter for chapter in chapters if chapter.id in selected_ids]
    if not chapters:
        raise ValueError("selection contains no chapters")
    return tuple(chapters)


def print_plan(config: PipelineConfig, console: Console) -> None:
    isolation = config.settings.isolation
    if isolation == "auto":
        isolation = "fuse-overlay" if fuse_overlay_available() else "shared"
    console.print(f"[bold]Repository:[/bold] {config.settings.repo}")
    console.print(
        f"[bold]Concurrency:[/bold] {config.settings.max_agents} agents/builds  "
        f"[bold]State:[/bold] {config.settings.state_dir}"
    )
    console.print(
        f"[bold]Model:[/bold] {config.settings.model}  "
        f"[bold]Reasoning:[/bold] {config.settings.reasoning_effort}  "
        f"[bold]Isolation:[/bold] {config.settings.isolation} → {isolation}"
    )
    access = (
        "full (approval and sandbox bypass)"
        if config.settings.bypass_approvals_and_sandbox
        else config.settings.sandbox
    )
    console.print(
        f"[bold]Codex access:[/bold] {access}  "
        f"[bold]Proof LSP:[/bold] {'enabled' if config.settings.lean_mcp else 'disabled'}  "
        f"[bold]Project:[/bold] {config.settings.lean_project}  "
        f"[bold]Tool timeout:[/bold] {config.settings.lean_mcp_tool_timeout_seconds:g}s"
    )
    stages = Table(title="Stages")
    stages.add_column("Stage")
    stages.add_column("Prompt")
    stages.add_column("Max rounds", justify="right")
    for stage in Stage:
        settings = config.stages[stage]
        stages.add_row(stage.value, str(settings.prompt), str(settings.max_rounds))
    console.print(stages)
    statement_schedule = build_corpus_schedule(config.books, config.chapters, phase="statements")
    proof_schedule = build_corpus_schedule(config.books, config.chapters, phase="proofs")
    books = Table(title="Corpus (critical-path priority order)")
    books.add_column("Book")
    books.add_column("Chapters", justify="right")
    books.add_column("Depends on")
    books.add_column("Statement rank", justify="right")
    books.add_column("Proof rank", justify="right")
    books.add_column("Source")
    by_id = {book.id: book for book in config.books}
    critical = set(statement_schedule.critical_path) | set(proof_schedule.critical_path)
    for book_id in statement_schedule.order:
        book = by_id[book_id]
        count = sum(chapter.book_id == book.id for chapter in config.chapters)
        label = f"★ {book.id}" if book.id in critical else book.id
        books.add_row(
            label,
            str(count),
            ", ".join(book.depends_on) or "—",
            f"{statement_schedule.rank[book.id]:g}",
            f"{proof_schedule.rank[book.id]:g}",
            str(book.source),
        )
    console.print(books)
    console.print(
        "Completed drafts stream into targeted coordinator builds and fixup batches while other "
        "formalizers run. A final global clean build precedes read-only review and proof."
    )


def _read_status(config: PipelineConfig) -> dict[str, object] | None:
    path = config.settings.state_dir / "state.json"
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"invalid state file: {path}")
    return value


def print_status(config: PipelineConfig, console: Console, *, raw_json: bool) -> int:
    snapshot = _read_status(config)
    if snapshot is None:
        console.print(f"No swarm state exists at {config.settings.state_dir}")
        return 0
    if raw_json:
        console.print_json(json.dumps(snapshot))
        return 0
    usage = snapshot.get("usage", {})
    if isinstance(usage, dict):
        from lastlib_swarm.state import TokenUsage

        measured = bool(usage.get("measured"))
        token_usage = TokenUsage(
            input_tokens=int(usage.get("input_tokens", 0)),
            cached_input_tokens=int(usage.get("cached_input_tokens", 0)),
            output_tokens=int(usage.get("output_tokens", 0)),
            reasoning_output_tokens=int(usage.get("reasoning_output_tokens", 0)),
            measured=measured,
        )
        console.print(f"[bold]{format_usage(token_usage)}[/bold]")
    cost = snapshot.get("cost")
    if isinstance(cost, dict) and cost.get("measured"):
        console.print(
            f"[bold]API-equivalent cost: {format_usd(CostEstimate.from_dict(cost))}[/bold]"
        )
    tasks = snapshot.get("tasks", {})
    counts = {status.value: 0 for status in TaskStatus}
    if isinstance(tasks, dict):
        for task in tasks.values():
            if isinstance(task, dict) and task.get("status") in counts:
                counts[str(task["status"])] += 1
    console.print("  ".join(f"{name}: {count}" for name, count in counts.items()))
    console.print(f"State: {config.settings.state_dir / 'state.json'}")
    return 0


def print_source_issues(config: PipelineConfig, console: Console, *, raw_json: bool) -> int:
    path = config.settings.state_dir / "source-issues.json"
    if not path.is_file():
        console.print(f"No source-issue ledger exists at {path}")
        return 0
    ledger = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(ledger, dict) or not isinstance(ledger.get("issues"), list):
        raise ValueError(f"invalid source-issue ledger: {path}")
    if raw_json:
        console.print_json(json.dumps(ledger))
        return 0
    table = Table(title="Source issues", box=None)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Chapter", style="bold")
    table.add_column("Location")
    table.add_column("Issue")
    table.add_column("Suggested correction")
    for issue in ledger["issues"]:
        if not isinstance(issue, dict):
            continue
        table.add_row(
            str(issue.get("id", "")),
            str(issue.get("chapter_id", "")),
            str(issue.get("location", "")),
            str(issue.get("description", "")),
            str(issue.get("suggested_correction", "")),
        )
    console.print(table)
    console.print(f"Ledger: {path}")
    return 0


async def _headless(
    orchestrator: Orchestrator,
    operation: Callable[[], Awaitable[bool]],
) -> bool:
    try:
        await orchestrator.prepare()
        return await operation()
    finally:
        await orchestrator.shutdown()


def _run(args: argparse.Namespace, config: PipelineConfig, console: Console) -> int:
    chapters = select_chapters(
        config,
        books=args.book,
        chapter_selectors=args.chapter,
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state, chapters=chapters, force=args.force)
    if args.command == "stage":
        stage = Stage(args.stage)

        async def operation() -> bool:
            return await orchestrator.run_stage(stage)

        label = f"{stage.value} stage"
    else:
        operation = orchestrator.run_pipeline
        label = "full pipeline"
    if args.no_tui:
        succeeded = asyncio.run(_headless(orchestrator, operation))
    else:
        succeeded = run_tui(
            orchestrator,
            operation,
            label=label,
            startup_warning=getattr(args, "startup_warning", ""),
        )
    usage = state.invocation_usage()
    lifetime_usage = state.total_usage()
    cost = state.invocation_cost()
    lifetime_cost = state.total_cost()
    console.print(format_usage(usage, label="This invocation"))
    console.print(f"API-equivalent cost: {format_usd(cost)}")
    console.print(f"Lifetime tokens: {format_count(lifetime_usage.total_tokens)}")
    console.print(f"Lifetime API-equivalent cost: {format_usd(lifetime_cost)}")
    console.print("Completed successfully" if succeeded else "Finished with failures")
    return 0 if succeeded else 1


def _managed_operation(
    orchestrator: Orchestrator, stage_name: str
) -> Callable[[], Coroutine[Any, Any, bool]]:
    if stage_name == "pipeline":
        return orchestrator.run_pipeline
    stage = Stage(stage_name)

    async def operation() -> bool:
        return await orchestrator.run_stage(stage)

    return operation


def _serve_agent(args: argparse.Namespace, config: PipelineConfig) -> int:
    chapters = select_chapters(config, books=[], chapter_selectors=[])
    state = StateStore(config)
    orchestrator = Orchestrator(config, state, chapters=chapters, force=args.force)
    operation = _managed_operation(orchestrator, args.stage)
    succeeded = asyncio.run(ControlServer(orchestrator, operation).run())
    return 0 if succeeded else 1


def _agent_source_args(args: argparse.Namespace) -> list[str]:
    if args.config is not None:
        values = ["--config", str(Path(args.config).resolve())]
    elif args.target is not None:
        values = [str(Path(args.target).resolve())]
    else:
        values = []
    if args.dependencies is not None:
        values.extend(["--dependencies", str(Path(args.dependencies).resolve())])
    if args.model is not None:
        values.extend(["--model", args.model])
    if args.reasoning_effort is not None:
        values.extend(["--reasoning-effort", args.reasoning_effort])
    if args.max_agents is not None:
        values.extend(["--max-agents", str(args.max_agents)])
    if args.isolation is not None:
        values.extend(["--isolation", args.isolation])
    return values


def _start_agent(args: argparse.Namespace, config: PipelineConfig) -> int:
    state_dir = config.settings.state_dir
    try:
        running = send_command(state_dir, "status", timeout=0.5)
    except OSError:
        running = None
    if running is not None:
        raise ValueError(f"a managed pipeline is already {running.get('status')}: {state_dir}")
    state_dir.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "lastlib_swarm.cli",
        "agent",
        "serve",
        *_agent_source_args(args),
        "--stage",
        args.stage,
    ]
    if args.force:
        command.append("--force")
    log_path = state_dir / LOG_NAME
    log_handle = log_path.open("ab")
    try:
        process = subprocess.Popen(
            command,
            cwd=config.settings.repo,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        log_handle.close()
    deadline = time.monotonic() + 10
    response: dict[str, object] | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            completed = offline_status(state_dir)
            if completed.get("status") in {"completed", "failed"}:
                completed["log_path"] = str(log_path)
                print(json.dumps(completed, sort_keys=True))
                return 0 if completed.get("result") else 1
            raise ValueError(f"managed pipeline exited during startup; inspect {log_path}")
        if control_socket(state_dir).exists():
            try:
                response = send_command(state_dir, "status", timeout=0.5)
                break
            except OSError:
                pass
        time.sleep(0.05)
    if response is None:
        process.terminate()
        raise ValueError(f"managed pipeline did not become ready; inspect {log_path}")
    response["log_path"] = str(log_path)
    print(json.dumps(response, sort_keys=True))
    return 0


def _offline_snapshot(config: PipelineConfig) -> dict[str, object]:
    response = offline_status(config.settings.state_dir)
    state_path = config.settings.state_dir / "state.json"
    if state_path.is_file():
        value = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(value, dict):
            response["snapshot"] = value
    return response


def _inspect_snapshot(
    config: PipelineConfig, *, chapter_selector: str | None, run_id: str | None
) -> dict[str, Any]:
    snapshot = _read_status(config)
    if snapshot is None:
        raise ValueError(f"no swarm state exists at {config.settings.state_dir}")
    raw_tasks = snapshot.get("tasks")
    if not isinstance(raw_tasks, dict):
        raise ValueError("swarm state contains no task records")

    candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
    matched_chapters: set[str] = set()
    for value in raw_tasks.values():
        if not isinstance(value, dict):
            continue
        chapter_id = str(value.get("chapter_id", ""))
        chapter_number = str(value.get("chapter_number", ""))
        if chapter_selector and chapter_selector not in (chapter_id, chapter_number):
            continue
        matched_chapters.add(chapter_id)
        runs = value.get("runs")
        if not isinstance(runs, list):
            continue
        for run in runs:
            if isinstance(run, dict) and (run_id is None or run.get("id") == run_id):
                candidates.append((value, run))
    if chapter_selector and len(matched_chapters) > 1:
        raise ValueError(
            f"chapter number {chapter_selector} is ambiguous; pass a complete book/chapter-NN id"
        )
    if not candidates:
        requested = run_id or chapter_selector or "any chapter"
        raise ValueError(f"no agent run matches {requested}")

    task, run = max(
        candidates,
        key=lambda pair: (
            pair[1].get("status") == TaskStatus.RUNNING,
            str(pair[1].get("started_at", "")),
        ),
    )
    selected_run_id = str(run.get("id"))
    activities = ActivityStore(config.settings.state_dir / "logs")
    activity = activities.read_fresh(selected_run_id)
    raw_log_path = run.get("log_path")
    log_path = (
        Path(raw_log_path)
        if isinstance(raw_log_path, str)
        else activities.logs_dir / f"{selected_run_id}.jsonl"
    )
    if activity is None:
        activity = activities.replay(
            selected_run_id,
            str(task.get("chapter_id", "")),
            str(run.get("stage", task.get("stage", ""))),
            log_path,
            workspace_root=config.settings.repo,
        )
    raw_usage = run.get("usage")
    usage: dict[str, Any] = dict(raw_usage) if isinstance(raw_usage, dict) else {}
    usage["total_tokens"] = int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0))
    model = str(run.get("model") or LEGACY_MODEL)
    cost = estimate_cost(
        model=model,
        input_tokens=int(usage.get("input_tokens", 0)),
        cached_input_tokens=int(usage.get("cached_input_tokens", 0)),
        output_tokens=int(usage.get("output_tokens", 0)),
        inferred=run.get("model") is None,
    )
    return {
        "chapter_id": task.get("chapter_id"),
        "chapter_title": task.get("chapter_title"),
        "stage": run.get("stage", task.get("stage")),
        "round": run.get("round"),
        "task_status": task.get("status"),
        "task_detail": task.get("detail"),
        "run_id": selected_run_id,
        "run_status": run.get("status"),
        "pid": run.get("pid"),
        "thread_id": run.get("thread_id"),
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "usage": usage,
        "cost": cost.as_dict(),
        "log_path": str(log_path),
        "activity": activity.as_dict() if activity else None,
    }


def _print_inspection(value: dict[str, Any], console: Console) -> None:
    activity = value.get("activity")
    activity = activity if isinstance(activity, dict) else {}
    current = str(activity.get("current", "no compact activity available"))
    failures = int(activity.get("failures", 0))
    console.print(
        f"[bold]{value['chapter_id']}[/bold] · {value['stage']} round {value['round']} · "
        f"{value['run_status']}"
    )
    console.print(f"Current: {current}")
    console.print(
        f"PID {value['pid'] or '—'} · thread {value['thread_id'] or '—'} · "
        f"commands {activity.get('commands', 0)} · MCP {activity.get('mcp_calls', 0)} · "
        f"edits {activity.get('file_changes', 0)} · failures {failures}"
    )
    usage = value.get("usage")
    if isinstance(usage, dict) and usage.get("measured"):
        console.print(f"Tokens: {int(usage.get('total_tokens', 0)):,}")
    else:
        console.print("Tokens: pending measured usage")
    cost = value.get("cost")
    if isinstance(cost, dict) and cost.get("measured"):
        console.print(f"API-equivalent cost: {format_usd(CostEstimate.from_dict(cost))}")
    if summary := activity.get("latest_summary"):
        console.print(f"Latest update: {summary}")
    if error := reportable_error(str(activity.get("latest_error", ""))):
        console.print(f"[red]Latest error: {error}[/red]")
    recent = activity.get("recent")
    if isinstance(recent, list):
        console.print("\n[bold]Recent timeline[/bold]")
        for entry in recent[-20:]:
            if not isinstance(entry, dict):
                continue
            detail = f" — {entry.get('detail')}" if entry.get("detail") else ""
            console.print(
                f"{str(entry.get('at', ''))[11:19]} {entry.get('status', '•')} "
                f"[{entry.get('kind', 'event')}] {entry.get('title', '')}{detail}"
            )
    console.print(f"Raw JSONL: {value['log_path']}")


def _inspect_agent(args: argparse.Namespace, config: PipelineConfig) -> int:
    if args.interval <= 0:
        raise ValueError("--interval must be positive")
    console = Console()
    last_marker: tuple[object, object] | None = None
    while True:
        value = _inspect_snapshot(
            config,
            chapter_selector=args.chapter,
            run_id=args.run,
        )
        activity = value.get("activity")
        sequence = activity.get("sequence") if isinstance(activity, dict) else None
        marker = (value.get("run_status"), sequence)
        if marker != last_marker:
            if args.json:
                print(json.dumps(value, sort_keys=True), flush=True)
            else:
                if args.follow and last_marker is not None:
                    console.clear()
                _print_inspection(value, console)
            last_marker = marker
        if not args.follow:
            return 0
        if value.get("run_status") != TaskStatus.RUNNING:
            return 0 if value.get("run_status") == TaskStatus.SUCCEEDED else 1
        time.sleep(args.interval)


def _control_response(command: str, config: PipelineConfig) -> dict[str, object]:
    timeout = None if command == "wait" else 10.0
    try:
        response = send_command(config.settings.state_dir, command, timeout=timeout)
    except OSError:
        if command == "snapshot":
            response = _offline_snapshot(config)
        elif command == "unblock":
            state_path = config.settings.state_dir / "state.json"
            if not state_path.is_file():
                response = offline_status(config.settings.state_dir) | {
                    "unblocked": 0,
                    "unblocked_tasks": [],
                }
            else:
                state = StateStore(config)

                async def unblock() -> list[str]:
                    await state.load_or_create()
                    return await state.unblock()

                tasks = asyncio.run(unblock())
                counts = {status.value: 0 for status in TaskStatus}
                for task in state.tasks.values():
                    counts[str(task.status)] += 1
                response = offline_status(config.settings.state_dir) | {
                    "unblocked": len(tasks),
                    "unblocked_tasks": tasks,
                    "updated_at": state.updated_at,
                    "tasks": counts,
                }
        elif command in {"status", "wait"}:
            response = offline_status(config.settings.state_dir)
        else:
            raise ValueError(
                f"no managed pipeline is running at {config.settings.state_dir}"
            ) from None
    if not response.get("scheduling"):
        statements = build_corpus_schedule(config.books, config.chapters, phase="statements")
        proofs = build_corpus_schedule(config.books, config.chapters, phase="proofs")
        schedule = scheduling_snapshot(statements, proofs)
        response["scheduling"] = schedule if command == "snapshot" else scheduling_summary(schedule)
    return response


def _agent_rpc(config: PipelineConfig) -> int:
    allowed = {
        "status",
        "snapshot",
        "pause",
        "resume",
        "unblock",
        "stop",
        "wait",
        "inspect",
    }
    failed = False
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if not isinstance(request, dict) or request.get("command") not in allowed:
                raise ValueError(f"command must be one of: {', '.join(sorted(allowed))}")
            if request["command"] == "inspect":
                response = _inspect_snapshot(
                    config,
                    chapter_selector=(
                        str(request["chapter"]) if request.get("chapter") is not None else None
                    ),
                    run_id=str(request["run"]) if request.get("run") is not None else None,
                )
            else:
                response = _control_response(str(request["command"]), config)
        except (json.JSONDecodeError, ValueError) as error:
            response = {"error": str(error)}
            failed = True
        print(json.dumps(response, sort_keys=True), flush=True)
    return 1 if failed else 0


def _agent_command(args: argparse.Namespace, config: PipelineConfig) -> int:
    command = args.agent_command
    if command == "start":
        return _start_agent(args, config)
    if command == "serve":
        return _serve_agent(args, config)
    if command == "rpc":
        return _agent_rpc(config)
    if command == "inspect":
        return _inspect_agent(args, config)
    response = _control_response(command, config)
    print(json.dumps(response, sort_keys=True))
    if command == "wait":
        if response.get("result") is None:
            return 2
        return 0 if response["result"] else 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    raw_arguments = list(argv) if argv is not None else sys.argv[1:]
    if raw_arguments and (
        raw_arguments[0].lower().endswith(".md") or Path(raw_arguments[0]).is_dir()
    ):
        raw_arguments.insert(0, "pipeline")
    arguments = parser().parse_args(raw_arguments)
    console = Console()
    try:
        missing_ripgrep = _starts_workers(arguments) and shutil.which("rg") is None
        arguments.startup_warning = RIPGREP_WARNING if missing_ripgrep else ""
        if missing_ripgrep and getattr(arguments, "no_tui", True):
            _warn_missing_ripgrep()
        config = _config_from_args(arguments)
        if arguments.command == "agent":
            return _agent_command(arguments, config)
        if arguments.command == "plan":
            print_plan(config, console)
            return 0
        if arguments.command == "status":
            return print_status(config, console, raw_json=arguments.json)
        if arguments.command == "source-issues":
            return print_source_issues(config, console, raw_json=arguments.json)
        if arguments.command == "scaffold":
            chapters = select_chapters(
                config,
                books=arguments.book,
                chapter_selectors=arguments.chapter,
            )
            created = scaffold_directories(config, chapters)
            noun = "directory" if len(created) == 1 else "directories"
            console.print(f"Scaffolded {len(created)} chapter {noun}")
            return 0
        return _run(arguments, config, console)
    except (OSError, ValueError) as error:
        Console(stderr=True).print(f"[red]error:[/red] {error}")
        return 2
    except KeyboardInterrupt:
        Console(stderr=True).print(
            "[yellow]Interrupted; running work will resume as pending.[/yellow]"
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
