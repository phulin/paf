from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

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
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TaskStatus
from lastlib_swarm.tui import format_usage, run_tui


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
    root.add_argument("--version", action="version", version="lastlib-swarm 0.3.0")
    commands = root.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="show discovered books, chapters, and stage settings")
    _add_source(plan)
    _add_overrides(plan)

    status = commands.add_parser("status", help="show persisted pipeline state and token usage")
    _add_source(status)
    status.add_argument("--json", action="store_true", help="print the raw persisted state")

    stage = commands.add_parser("stage", help="run one stage across selected chapters")
    stage.add_argument("stage", choices=[item.value for item in Stage])
    _add_run_options(stage)

    pipeline = commands.add_parser(
        "pipeline", help="run the full formalize/review/prove/repair flow"
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
        ("stop", "cancel the pipeline and active subprocesses"),
        ("wait", "block until the managed pipeline exits"),
    ):
        command = agent_commands.add_parser(name, help=help_text)
        _add_source(command)
    rpc = agent_commands.add_parser("rpc", help="read control commands as JSONL from stdin")
    _add_source(rpc)
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
    if max_agents is not None and max_agents < 1:
        raise ValueError("--max-agents must be positive")
    if model is not None or reasoning_effort is not None or max_agents is not None:
        settings = replace(
            config.settings,
            model=model or config.settings.model,
            reasoning_effort=reasoning_effort or config.settings.reasoning_effort,
            max_agents=max_agents or config.settings.max_agents,
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
    console.print(f"[bold]Repository:[/bold] {config.settings.repo}")
    console.print(
        f"[bold]Concurrency:[/bold] {config.settings.max_agents} agents/builds  "
        f"[bold]State:[/bold] {config.settings.state_dir}"
    )
    console.print(
        f"[bold]Model:[/bold] {config.settings.model}  "
        f"[bold]Reasoning:[/bold] {config.settings.reasoning_effort}"
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
        "Books are dependency-gated in both phases. Ready books compete by weighted downstream "
        "critical-path rank; chapters pipeline formalize → review and prove ↔ repair."
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
    tasks = snapshot.get("tasks", {})
    counts = {status.value: 0 for status in TaskStatus}
    if isinstance(tasks, dict):
        for task in tasks.values():
            if isinstance(task, dict) and task.get("status") in counts:
                counts[str(task["status"])] += 1
    console.print("  ".join(f"{name}: {count}" for name, count in counts.items()))
    console.print(f"State: {config.settings.state_dir / 'state.json'}")
    return 0


async def _headless(
    orchestrator: Orchestrator,
    operation: Callable[[], Awaitable[bool]],
) -> bool:
    await orchestrator.prepare()
    return await operation()


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
        succeeded = run_tui(orchestrator, operation, label=label)
    usage = state.total_usage()
    console.print(format_usage(usage))
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


def _control_response(command: str, config: PipelineConfig) -> dict[str, object]:
    timeout = None if command == "wait" else 10.0
    try:
        response = send_command(config.settings.state_dir, command, timeout=timeout)
    except OSError:
        if command == "snapshot":
            response = _offline_snapshot(config)
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
    allowed = {"status", "snapshot", "pause", "resume", "stop", "wait"}
    failed = False
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if not isinstance(request, dict) or request.get("command") not in allowed:
                raise ValueError(f"command must be one of: {', '.join(sorted(allowed))}")
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
        config = _config_from_args(arguments)
        if arguments.command == "agent":
            return _agent_command(arguments, config)
        if arguments.command == "plan":
            print_plan(config, console)
            return 0
        if arguments.command == "status":
            return print_status(config, console, raw_json=arguments.json)
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
