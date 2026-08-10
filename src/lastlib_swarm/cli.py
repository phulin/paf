from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

from rich.console import Console
from rich.table import Table

from lastlib_swarm.config import load_config
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TaskStatus
from lastlib_swarm.tui import format_usage, run_tui


def _add_config(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config", default="swarm.toml", help="pipeline TOML (default: swarm.toml)"
    )


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    _add_config(parser)
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
    root.add_argument("--version", action="version", version="lastlib-swarm 0.1.0")
    commands = root.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="show discovered books, chapters, and stage settings")
    _add_config(plan)

    status = commands.add_parser("status", help="show persisted pipeline state and token usage")
    _add_config(status)
    status.add_argument("--json", action="store_true", help="print the raw persisted state")

    stage = commands.add_parser("stage", help="run one stage across selected chapters")
    stage.add_argument("stage", choices=[item.value for item in Stage])
    _add_run_options(stage)

    pipeline = commands.add_parser(
        "pipeline", help="run the full formalize/review/prove/repair flow"
    )
    _add_run_options(pipeline)
    return root


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
    stages = Table(title="Stages")
    stages.add_column("Stage")
    stages.add_column("Prompt")
    stages.add_column("Max rounds", justify="right")
    for stage in Stage:
        settings = config.stages[stage]
        stages.add_row(stage.value, str(settings.prompt), str(settings.max_rounds))
    console.print(stages)
    books = Table(title="Corpus")
    books.add_column("Book")
    books.add_column("Chapters", justify="right")
    books.add_column("Depends on")
    books.add_column("Source")
    for book in config.books:
        count = sum(chapter.book_id == book.id for chapter in config.chapters)
        books.add_row(book.id, str(count), ", ".join(book.depends_on) or "—", str(book.source))
    console.print(books)
    console.print(
        "Statement work is chapter-pipelined (formalize → review fixed point); proof work begins "
        "after the selected corpus reaches statement review fixed points."
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


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    console = Console()
    try:
        config = load_config(Path(arguments.config))
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
