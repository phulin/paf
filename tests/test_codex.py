import asyncio
import json
import os
import signal
from dataclasses import replace
from pathlib import Path

import pytest

from lastlib_swarm.codex import (
    CodexExecutor,
    _rollout_usage,
    count_placeholders,
    lean_mcp_executable,
    lean_mcp_path,
    render_prompt,
    unexpected_lean_warnings,
    validate,
)
from lastlib_swarm.config import load_config, standard_prompt_path
from lastlib_swarm.models import Stage
from lastlib_swarm.state import StateStore, TokenUsage
from tests.support import write_project


def test_extracts_api_equivalent_usage() -> None:
    usage = TokenUsage.from_event(
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 120,
                "cached_input_tokens": 40,
                "output_tokens": 30,
                "reasoning_output_tokens": 12,
            },
        }
    )

    assert usage is not None
    assert usage.total_tokens == 150
    assert usage.cached_input_tokens == 40
    assert usage.reasoning_output_tokens == 12
    assert usage.measured


def test_extracts_live_rollout_usage() -> None:
    usage = _rollout_usage(
        {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 400,
                        "cached_input_tokens": 300,
                        "output_tokens": 25,
                        "reasoning_output_tokens": 10,
                        "total_tokens": 425,
                    }
                },
            },
        }
    )

    assert usage is not None
    assert usage.total_tokens == 425
    assert usage.cached_input_tokens == 300


def test_counts_only_lean_code_placeholders(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    chapter_dir = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_dir.mkdir(parents=True)
    (chapter_dir / "Section.lean").write_text(
        """
-- sorry
/- admit /- sorry -/ -/
def message := "sorry"
theorem first : True := by sorry
theorem second : True := by admit
""",
        encoding="utf-8",
    )

    assert count_placeholders(tmp_path, chapter) == 2


def test_executor_uses_machine_readable_codex_mode(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.REVIEW)

    assert command[:2] == ["codex", "exec"]
    assert "--json" in command
    assert "--output-schema" in command
    assert "--dangerously-bypass-approvals-and-sandbox" in command
    assert "--approve-for-me" not in command
    assert "--sandbox" not in command
    assert "--skip-git-repo-check" not in command
    isolated = tmp_path / "isolated"
    isolated_command = executor.command(Stage.PROVE, isolated)
    assert "--skip-git-repo-check" in isolated_command
    overrides = {
        isolated_command[index + 1].split("=", 1)[0]: isolated_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(isolated_command[:-1])
        if item == "--config"
    }
    assert overrides["mcp_servers.lastlib_lean.command"] == f'"{lean_mcp_executable()}"'
    assert overrides["mcp_servers.lastlib_lean.cwd"] == f'"{isolated / "lean"}"'
    assert json.loads(overrides["mcp_servers.lastlib_lean.env.PATH"]) == lean_mcp_path()
    assert "lean_diagnostic_messages" in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_multi_attempt" in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_build" not in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert render_prompt(
        "Chapter {chapter_number_padded}: {chapter_title}", config.chapters[0]
    ) == ("Chapter 01: First chapter")


def test_lean_mcp_path_finds_elan_outside_inherited_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    elan_bin = tmp_path / "elan" / "bin"
    elan_bin.mkdir(parents=True)
    (elan_bin / "lake").touch()
    monkeypatch.setenv("ELAN_HOME", str(tmp_path / "elan"))
    monkeypatch.setenv("PATH", "/usr/bin")

    assert lean_mcp_path().split(os.pathsep)[0] == str(elan_bin)


def test_approve_for_me_is_not_combined_with_explicit_sandbox(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        settings=replace(
            config.settings,
            bypass_approvals_and_sandbox=False,
            approve_for_me=True,
        ),
    )
    command = CodexExecutor(config, StateStore(config)).command(Stage.REVIEW)

    assert "--approve-for-me" in command
    assert "--sandbox" not in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


def test_executor_can_disable_lean_mcp(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, lean_mcp=False))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.PROVE)
    prompt = executor.build_prompt(config.chapters[0], Stage.PROVE)

    assert not any("mcp_servers.lastlib_lean" in item for item in command)
    assert "Attached Lean MCP" not in prompt


def test_proof_prompt_requires_whole_file_pass_before_diagnostics(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            Stage.PROVE: replace(
                config.stages[Stage.PROVE], prompt=standard_prompt_path(Stage.PROVE)
            ),
        },
    )
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(config.chapters[0], Stage.PROVE)

    normalized = " ".join(prompt.split())
    whole_pass = normalized.index(
        "one coherent proof-writing pass over the entire assigned file set"
    )
    diagnostics = normalized.index("After that whole-file pass")
    assert whole_pass < diagnostics
    assert "iterate only over proofs and dependent declarations that fail" in prompt


@pytest.mark.parametrize("stage", list(Stage))
def test_rendered_prompts_compose_each_layer_once(tmp_path: Path, stage: Stage) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        stages={
            **config.stages,
            stage: replace(config.stages[stage], prompt=standard_prompt_path(stage)),
        },
    )
    prompt = CodexExecutor(config, StateStore(config)).build_prompt(config.chapters[0], stage)

    mission = prompt.index("## Mission")
    policy = prompt.index("## Common Lean policy")
    runtime = prompt.index("## Runtime contract")
    assert mission < policy < runtime
    assert prompt.count("## Common Lean policy") == 1
    assert prompt.count("## Runtime contract") == 1
    assert prompt.count("import Mathlib") == 1
    assert prompt.count("Do not run `lake build`") == 1
    assert prompt.count("single writable build cache") == 1
    assert prompt.count("declaration uses `sorry`") == 1


def test_warning_filter_allows_only_declaration_uses_sorry() -> None:
    output = """warning: Book/Chapter.lean:12:8: declaration uses `sorry`
warning: Book/Chapter.lean:18:5: Variable name `h` is not explicitly referenced.
warning: declaration uses 'sorry'
warning: Book/Chapter.lean:24:2: a warning that merely mentions sorry
"""

    assert unexpected_lean_warnings(output) == (
        "warning: Book/Chapter.lean:18:5: Variable name `h` is not explicitly referenced.",
        "warning: Book/Chapter.lean:24:2: a warning that merely mentions sorry",
    )


@pytest.mark.asyncio
async def test_validation_rejects_non_sorry_warnings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command=(
            "printf '%s\\n' 'warning: Book/Chapter.lean:1:1: declaration uses `sorry`' "
            "'warning: Book/Chapter.lean:2:1: unused variable'"
        ),
    )

    validation = await validate(config, chapter)

    assert not validation.succeeded
    assert validation.exit_code == 1
    assert "Coordinator rejected 1 non-sorry Lean warning(s)" in validation.output
    assert "unused variable" in validation.output


@pytest.mark.asyncio
async def test_validation_accepts_sorry_warnings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command=(
            "printf '%s\\n' 'warning: Book/Chapter.lean:1:1: declaration uses `sorry`'"
        ),
    )

    validation = await validate(config, chapter)

    assert validation.succeeded
    assert validation.exit_code == 0


@pytest.mark.asyncio
async def test_executor_consumes_jsonl_report_and_usage(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "fake-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "thread-123"}))
print(json.dumps({"type": "turn.completed", "usage": {
    "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
    "reasoning_output_tokens": 10}}))
report = {"changed": False, "complete": True, "needs_repair": False,
          "summary": "done", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    assert result.succeeded
    assert result.thread_id == "thread-123"
    assert result.usage.total_tokens == 125
    assert result.report["summary"] == "done"
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent succeeded"
    assert json.loads(activity.latest_summary)["summary"] == "done"


@pytest.mark.asyncio
async def test_executor_consumes_jsonl_records_larger_than_stream_limit(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "large-event-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

sys.stdin.read()
print(json.dumps({"type": "item.completed", "item": {
    "type": "command_execution", "aggregated_output": "x" * (2 * 1024 * 1024)}}))
print(json.dumps({"type": "turn.completed", "usage": {
    "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
    "reasoning_output_tokens": 10}}))
report = {"changed": False, "complete": True, "needs_repair": False,
          "summary": "large event drained", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    assert result.succeeded
    assert result.usage.total_tokens == 125
    assert result.report["summary"] == "large event drained"
    assert (state.logs_dir / f"{run.id}.jsonl").stat().st_size > 2 * 1024 * 1024


@pytest.mark.asyncio
async def test_executor_flushes_jsonl_while_agent_is_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "slow-codex"
    codex_home = tmp_path / "codex-home"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.stdin.read()
thread_id = "visible-now"
session = Path(os.environ["CODEX_HOME"]) / "sessions" / datetime.now(UTC).strftime("%Y/%m/%d")
session.mkdir(parents=True)
rollout = session / f"rollout-test-{thread_id}.jsonl"
rollout.write_text(json.dumps({"type": "event_msg", "payload": {
    "type": "token_count", "info": {"total_token_usage": {
        "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
        "reasoning_output_tokens": 10, "total_tokens": 125}}}}) + "\\n")
print(json.dumps({"type": "thread.started", "thread_id": thread_id}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    task = asyncio.create_task(executor.run(config.chapters[0], Stage.REVIEW, run))
    log_path = state.logs_dir / f"{run.id}.jsonl"
    try:
        for _ in range(100):
            if log_path.is_file() and log_path.stat().st_size and run.usage.measured:
                break
            await asyncio.sleep(0.01)
        assert "visible-now" in log_path.read_text(encoding="utf-8")
        assert run.usage.total_tokens == 125
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent cancelled"
    reloaded = json.loads(state.path.read_text(encoding="utf-8"))
    persisted = reloaded["tasks"][f"{run.chapter_id}:review"]["runs"][-1]["usage"]
    assert persisted["input_tokens"] == 100
    assert persisted["output_tokens"] == 25


@pytest.mark.asyncio
async def test_cancellation_kills_surviving_mcp_descendants(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    child_pid_path = tmp_path / "child.pid"
    child = tmp_path / "term-resistant-child"
    child.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    fake_codex = tmp_path / "codex-with-mcp-child"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import subprocess
import sys
import time

sys.stdin.read()
subprocess.Popen([{str(child)!r}, {str(child_pid_path)!r}])
print(json.dumps({{"type": "thread.started", "thread_id": "descendant-test"}}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    task = asyncio.create_task(executor.run(config.chapters[0], Stage.REVIEW, run))

    child_pid = 0
    try:
        for _ in range(200):
            if child_pid_path.is_file():
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                break
            await asyncio.sleep(0.01)
        assert child_pid
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not _process_is_running(child_pid)
    finally:
        task.cancel()
        if child_pid and _process_is_running(child_pid):
            os.kill(child_pid, signal.SIGKILL)


@pytest.mark.asyncio
async def test_successful_agent_exit_kills_surviving_mcp_descendants(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    child_pid_path = tmp_path / "successful-child.pid"
    child = tmp_path / "successful-term-resistant-child"
    child.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    fake_codex = tmp_path / "successful-codex-with-mcp-child"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import subprocess
import sys
import time
from pathlib import Path

sys.stdin.read()
subprocess.Popen([{str(child)!r}, {str(child_pid_path)!r}])
for _ in range(200):
    if Path({str(child_pid_path)!r}).is_file():
        break
    time.sleep(0.01)
report = {{"changed": False, "complete": True, "needs_repair": False,
          "summary": "done", "issues": []}}
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}), flush=True)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    child_pid = 0
    try:
        result = await executor.run(config.chapters[0], Stage.REVIEW, run)
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        assert result.succeeded
        assert not _process_is_running(child_pid)
    finally:
        if child_pid and _process_is_running(child_pid):
            os.kill(child_pid, signal.SIGKILL)


def _process_is_running(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()[2]
    except (FileNotFoundError, IndexError):
        return False
    return state != "Z"
