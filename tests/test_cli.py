import asyncio
import json
import sys
from io import StringIO
from pathlib import Path

import pytest

import lastlib_swarm.cli as cli_module
from lastlib_swarm.cli import main, select_chapters
from lastlib_swarm.config import load_config
from lastlib_swarm.models import PipelineConfig, Stage
from lastlib_swarm.state import StateStore
from tests.support import write_project


def test_selects_book_and_chapter_number(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    chapters = select_chapters(config, books=["book"], chapter_selectors=["2"])

    assert [chapter.id for chapter in chapters] == ["book/chapter-02"]


def test_plan_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_project(tmp_path)

    assert main(["plan", "--config", str(path)]) == 0
    output = capsys.readouterr().out
    assert "critical-path rank" in output
    assert "Lean MCP: enabled" in output


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
report = {"changed": False, "complete": True, "needs_repair": False,
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
    assert finished["usage"]["api_tokens"] == 36


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
