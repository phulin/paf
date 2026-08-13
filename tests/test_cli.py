import argparse
import asyncio
import json
import sys
from io import StringIO
from pathlib import Path

import pytest
from rich.console import Console

import lastlib_swarm.cli as cli_module
from lastlib_swarm.cli import main, select_chapters
from lastlib_swarm.config import load_config
from lastlib_swarm.models import PipelineConfig, Stage
from lastlib_swarm.state import StateStore, TaskStatus
from lastlib_swarm.state_db import read_full_snapshot
from tests.support import write_project


def test_failure_summary_prints_task_build_and_blocker_details(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    chapter = config.chapters[0]

    async def populate() -> None:
        await state.load_or_create()
        run = await state.start_run(chapter.id, Stage.REVIEW)
        activity = state.activities.start(run.id, run.chapter_id, run.stage)
        activity.latest_error = "Codex returned no structured final report"
        state.activities.save(activity)
        await state.finish_run(
            run,
            status=TaskStatus.FAILED,
            report={
                "summary": "Review found a remaining interface defect.",
                "issues": ["The theorem needs a positivity hypothesis."],
                "fixup_findings": [{"description": "Add the missing positivity hypothesis."}],
            },
            validation={
                "succeeded": False,
                "output": "error: Book/Chapter01.lean:7:3: type mismatch",
            },
        )
        await state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            "review left fixup findings",
        )
        await state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.BLOCKED,
            "blocked because statement review did not complete",
        )
        await state.start_coordinator_build(
            mode="targeted",
            stage=Stage.FIXUP,
            iteration=1,
            maximum_iterations=1,
            total=1,
        )
        state.append_coordinator_build_output(
            "error: Book/Chapter01.lean:7:3: type mismatch",
            error_count=1,
        )
        await state.finish_coordinator_build()

    asyncio.run(populate())
    output = StringIO()
    console = Console(file=output, force_terminal=False, color_system=None, width=160)

    cli_module._print_failure_summary(state, console)

    rendered = output.getvalue()
    assert "Failure details" in rendered
    assert "book/chapter-01 [review]" in rendered
    assert "The theorem needs a positivity hypothesis." in rendered
    assert "Codex returned no structured final report" in rendered
    assert "type mismatch" in rendered
    assert "Coordinator build" in rendered
    assert "Blocked dependents (1)" in rendered
    assert str(state.path) in rendered


def test_graph_failure_does_not_print_historical_agent_findings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    chapter = config.chapters[0]
    graph_error = (
        "observed chapter import graph contains a cycle: "
        "book/chapter-01 -> book/chapter-02 -> book/chapter-01"
    )

    async def populate() -> None:
        await state.load_or_create()
        run = await state.start_run(chapter.id, Stage.REVIEW)
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            report={
                "summary": "Historical review summary.",
                "issues": ["Historical review issue."],
                "fixup_findings": [{"description": "Historical fixup instruction."}],
            },
        )
        state.fixup_graph = {"algorithm": "observed-lean-imports", "error": graph_error}
        await state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            graph_error,
        )

    asyncio.run(populate())
    output = StringIO()
    console = Console(file=output, force_terminal=False, color_system=None, width=160)

    cli_module._print_failure_summary(state, console)

    rendered = output.getvalue()
    assert rendered.count(graph_error) == 1
    assert "Coordinator [import graph]" in rendered
    assert "1 task could not be scheduled." in rendered
    assert "Historical review summary." not in rendered
    assert "Historical review issue." not in rendered
    assert "Historical fixup instruction." not in rendered


def test_run_prints_failure_summary_after_tui_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    events: list[str] = []

    def run_tui(*_args: object, **_kwargs: object) -> bool:
        events.append("tui-closed")
        return False

    def print_summary(
        _state: StateStore,
        _console: Console,
        *,
        chapter_ids: set[str] | None = None,
    ) -> None:
        assert chapter_ids == {config.chapters[0].id}
        events.append("summary-printed")

    monkeypatch.setattr(cli_module, "run_tui", run_tui)
    monkeypatch.setattr(cli_module, "_print_failure_summary", print_summary)
    args = argparse.Namespace(
        book=[],
        chapter=[],
        force=False,
        command="pipeline",
        no_tui=False,
        startup_warning="",
    )
    console = Console(file=StringIO(), force_terminal=False, color_system=None)

    assert cli_module._run(args, config, console) == 1
    assert events == ["tui-closed", "summary-printed"]


def test_run_prints_abort_error_after_tui_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))

    def run_tui(*_args: object, **_kwargs: object) -> bool:
        raise RuntimeError("coordinator cache promotion failed")

    monkeypatch.setattr(cli_module, "run_tui", run_tui)
    args = argparse.Namespace(
        book=[],
        chapter=[],
        force=False,
        command="pipeline",
        no_tui=False,
        startup_warning="",
    )
    output = StringIO()
    console = Console(file=output, force_terminal=False, color_system=None)

    assert cli_module._run(args, config, console) == 1
    rendered = output.getvalue()
    assert "Abort error" in rendered
    assert "RuntimeError: coordinator cache promotion failed" in rendered
    assert "Failure details" in rendered


def test_selects_book_and_chapter_number(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    chapters = select_chapters(config, books=["book"], chapter_selectors=["2"])

    assert [chapter.id for chapter in chapters] == ["book/chapter-02"]


def test_plan_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_project(tmp_path)

    assert main(["plan", "--config", str(path)]) == 0
    output = capsys.readouterr().out
    assert "fixup" in output
    assert "Lean MCP: enabled" in output


def test_scaffold_creates_only_chapter_directories(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = write_project(tmp_path, chapters="chapters = [1, 2]")

    assert main(["scaffold", "--config", str(path)]) == 0

    assert (tmp_path / "lean" / "Book" / "Chapter01").is_dir()
    assert (tmp_path / "lean" / "Book" / "Chapter02").is_dir()
    assert not list((tmp_path / "lean").rglob("*.lean"))
    assert "Scaffolded 2 chapter directories" in capsys.readouterr().out


def test_source_issues_command_shows_persisted_ledger(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(path)
    state = StateStore(config)

    async def populate() -> None:
        await state.load_or_create()
        run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
        await state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            report={
                "source_issues": [
                    {
                        "location": "Chapter 1, theorem statement",
                        "source_excerpt": "Every ring is a field.",
                        "description": "The claim is false without additional hypotheses.",
                        "suggested_correction": "Every division ring is a field when commutative.",
                    }
                ]
            },
        )

    asyncio.run(populate())

    assert main(["source-issues", "--config", str(path), "--json"]) == 0
    ledger = json.loads(capsys.readouterr().out)
    assert ledger["issues"][0]["location"] == "Chapter 1, theorem statement"
    assert (
        ledger["issues"][0]["suggested_correction"]
        == "Every division ring is a field when commutative."
    )


def test_worker_startup_warns_prominently_when_ripgrep_is_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = write_project(tmp_path)
    monkeypatch.setattr(cli_module.shutil, "which", lambda _name: None)
    monkeypatch.setattr(cli_module, "_run", lambda *_args, **_kwargs: 0)

    assert main(["pipeline", "--config", str(path), "--no-tui"]) == 0

    captured = capsys.readouterr()
    assert "MISSING RECOMMENDED TOOL" in captured.err
    assert "ripgrep" in captured.err
    assert "substantially slower" in captured.err
    assert captured.out == ""


def test_plan_can_disable_lean_mcp(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_project(tmp_path)

    assert main(["plan", "--config", str(path), "--no-lean-mcp"]) == 0

    assert "Lean MCP: disabled" in capsys.readouterr().out


def test_plan_accepts_just_a_markdown_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / ".git").mkdir()
    target = tmp_path / "04-sample-book.md"
    target.write_text("# Sample Book\n\n## 1. Start\n", encoding="utf-8")

    assert main(["plan", str(target)]) == 0
    output = capsys.readouterr().out
    assert "gpt-5.6-luna" in output
    assert "book04" in output


def test_agent_can_start_and_wait_for_detached_pipeline(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    fake_codex = tmp_path / "fake-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys
import time

sys.stdin.read()
time.sleep(0.15)
print(json.dumps({"type": "thread.started", "thread_id": "managed-thread"}))
print(json.dumps({"type": "turn.completed", "usage": {
    "input_tokens": 10, "cached_input_tokens": 5, "output_tokens": 2}}))
report = {"changed": False, "complete": True,
          "summary": "done", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config_text = config_path.read_text(encoding="utf-8")
    config_text = config_text.replace(
        "max_agents = 4", f'max_agents = 4\ncodex_bin = "{fake_codex}"'
    ).replace('module = "Book"', 'module = "Book"\nbuild_command = "true"')
    config_path.write_text(config_text, encoding="utf-8")

    assert main(["agent", "start", "--config", str(config_path)]) == 0
    started = json.loads(capsys.readouterr().out)
    assert started["status"] == "running"
    assert main(["agent", "wait", "--config", str(config_path)]) == 0
    finished = json.loads(capsys.readouterr().out)
    assert finished["status"] == "completed"
    assert finished["usage"]["total_tokens"] == 36
    assert finished["cost"]["estimated_usd"] == pytest.approx(0.0000105)


def test_agent_rpc_reads_jsonl_from_stdin(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    monkeypatch.setattr(sys, "stdin", StringIO('{"command":"status"}\n'))

    assert main(["agent", "rpc", "--config", str(config_path)]) == 0
    response = json.loads(capsys.readouterr().out)
    assert response["status"] == "not-started"
    assert response["scheduling"]["algorithm"] == "weighted-critical-path-list-scheduling"


def test_agent_unblock_updates_offline_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(config_path)
    state = StateStore(config)

    async def populate() -> None:
        await state.load_or_create()
        state.scheduling = {"algorithm": "saved-schedule"}
        state.isolation = {"backend": "saved-isolation"}
        run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
        await state.finish_run(run, status=TaskStatus.FAILED)
        await state.set_task(
            config.chapters[0].id,
            Stage.REVIEW,
            TaskStatus.BLOCKED,
            "formalization failed",
        )

    asyncio.run(populate())

    assert main(["agent", "unblock", "--config", str(config_path)]) == 0
    response = json.loads(capsys.readouterr().out)
    assert response["status"] == "offline"
    assert response["unblocked"] == 1
    assert response["unblocked_tasks"] == ["book/chapter-01:review"]
    assert response["tasks"]["blocked"] == 0

    snapshot = read_full_snapshot(config.settings.state_dir)
    assert snapshot is not None
    task = snapshot["tasks"]["book/chapter-01:review"]
    assert task["status"] == "pending"
    assert task["detail"] == "manually unblocked"
    assert len(task["runs"]) == 1
    assert snapshot["scheduling"] == {"algorithm": "saved-schedule"}
    assert snapshot["isolation"] == {"backend": "saved-isolation"}


def test_agent_inspect_reports_compact_live_activity(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(config_path)
    state = StateStore(config)

    async def populate() -> None:
        await state.load_or_create()
        run = await state.start_run(config.chapters[0].id, Stage.PROVE)
        activity = state.activities.start(run.id, run.chapter_id, run.stage)
        activity.consume(
            {
                "type": "item.started",
                "item": {
                    "id": "command",
                    "type": "command_execution",
                    "command": "cd lean && lake build +Book.Chapter01",
                    "status": "in_progress",
                },
            },
            workspace_root=tmp_path,
        )
        state.activities.save(activity)

    asyncio.run(populate())

    assert (
        main(
            [
                "agent",
                "inspect",
                "--config",
                str(config_path),
                "--chapter",
                "1",
                "--json",
            ]
        )
        == 0
    )
    response = json.loads(capsys.readouterr().out)
    assert response["chapter_id"] == "book/chapter-01"
    assert response["activity"]["current"] == "shell: cd lean && lake build +Book.Chapter01"
    assert response["run_status"] == "running"


def test_agent_inspect_replays_legacy_jsonl_without_sidecar(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    config = load_config(config_path)
    state = StateStore(config)

    async def populate() -> None:
        await state.load_or_create()
        run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
        log_path = state.logs_dir / f"{run.id}.jsonl"
        log_path.write_text(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "message",
                        "type": "agent_message",
                        "text": "working through declarations",
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        await state.update_run(run, log_path=str(log_path))

    asyncio.run(populate())

    assert main(["agent", "inspect", "--config", str(config_path), "--chapter", "1", "--json"]) == 0
    response = json.loads(capsys.readouterr().out)
    assert response["activity"]["latest_summary"] == "working through declarations"


def test_markdown_as_first_argument_is_pipeline_shorthand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".git").mkdir()
    target = tmp_path / "08-shortcut.md"
    target.write_text("# Shortcut\n\n## 1. Start\n", encoding="utf-8")

    def fake_run(*_args: object, **_kwargs: object) -> int:
        return 0

    monkeypatch.setattr(cli_module, "_run", fake_run)

    assert main([str(target)]) == 0


def test_corpus_command_infers_a_directory_and_dependency_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".git").mkdir()
    books = tmp_path / "books"
    books.mkdir()
    (books / "01-foundation.md").write_text("# Foundation\n\n## 1. Start\n", encoding="utf-8")
    (books / "02-consequence.md").write_text("# Consequence\n\n## 1. Finish\n", encoding="utf-8")
    (tmp_path / "BOOK_DEPENDENCIES.md").write_text(
        "```mermaid\nflowchart LR\nB01 --> B02\n```\n", encoding="utf-8"
    )
    captured: dict[str, object] = {}

    def fake_run(_args: object, config: object, _console: object) -> int:
        captured["config"] = config
        return 0

    monkeypatch.setattr(cli_module, "_run", fake_run)

    assert main(["corpus", str(books), "--no-tui"]) == 0
    config = captured["config"]
    assert isinstance(config, PipelineConfig)
    assert config.books[1].depends_on == ("book01",)
