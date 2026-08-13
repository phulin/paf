import asyncio
import json
import os
import signal
from dataclasses import replace
from pathlib import Path

import pytest

from lastlib_swarm.codex import (
    REPORT_SCHEMA,
    REVIEW_SOURCE_BUNDLE_MAX_CHARS,
    CodexExecutor,
    _bounded_feedback,
    _capacity_resume_delay,
    _complete_lines,
    _is_capacity_failure,
    _review_source_bundle,
    _rollout_usage,
    _tail_rollout_usage,
    count_placeholders,
    lean_mcp_executable,
    lean_mcp_path,
    render_prompt,
    unexpected_lean_warnings,
    validate,
)
from lastlib_swarm.config import load_config
from lastlib_swarm.models import Stage
from lastlib_swarm.state import StateStore, TokenUsage
from lastlib_swarm.state_db import read_full_snapshot
from tests.support import write_project


def test_report_schema_uses_structured_fixup_findings() -> None:
    assert "needs_fixup" not in REPORT_SCHEMA["properties"]
    assert "needs_fixup" not in REPORT_SCHEMA["required"]
    assert "fixup_findings" in REPORT_SCHEMA["required"]


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


def test_frames_complete_lines_without_discarding_a_partial_record() -> None:
    pending = bytearray(b'{"first":')

    lines = _complete_lines(pending, b'1}\n{"second":2}\n{"third":')

    assert lines == (b'{"first":1}', b'{"second":2}')
    assert pending == b'{"third":'


@pytest.mark.asyncio
async def test_rollout_tail_skips_large_non_usage_records_before_json_decode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from lastlib_swarm import codex as codex_module

    rollout = tmp_path / "rollout.jsonl"
    irrelevant = b'{"type":"response_item","payload":"' + b"x" * (2 * 1024 * 1024) + b'"}\n'
    token_count = json.dumps(
        {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 400,
                        "cached_input_tokens": 300,
                        "output_tokens": 25,
                    }
                },
            },
        }
    ).encode()
    rollout.write_bytes(irrelevant + token_count + b"\n")
    decoded_sizes: list[int] = []
    original_loads = codex_module.json.loads

    def recording_loads(value: bytes) -> object:
        decoded_sizes.append(len(value))
        return original_loads(value)

    monkeypatch.setattr(codex_module, "_codex_rollout", lambda _thread_id: rollout)
    monkeypatch.setattr(codex_module.json, "loads", recording_loads)
    monkeypatch.setattr(codex_module, "USAGE_POLL_SECONDS", 0)
    stop = asyncio.Event()
    updates: list[TokenUsage] = []

    async def update(usage: TokenUsage) -> None:
        updates.append(usage)
        stop.set()

    await _tail_rollout_usage("thread", stop, update)

    assert [usage.total_tokens for usage in updates] == [425]
    assert decoded_sizes == [len(token_count)]


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

    command = executor.command(Stage.PROVE)

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
    assert json.loads(overrides["mcp_servers.lastlib_lean.args"]) == [
        "-m",
        "lastlib_swarm.lean_mcp",
    ]
    assert overrides["mcp_servers.lastlib_lean.cwd"] == f'"{isolated / "lean"}"'
    assert json.loads(overrides["mcp_servers.lastlib_lean.env.PATH"]) == lean_mcp_path()
    assert json.loads(
        overrides["mcp_servers.lastlib_lean.env.LEAN_MCP_SCRATCH_SLOTS"]
    ) == "1"
    assert "mcp_servers.lastlib_lean.env.LEAN_MCP_PREWARM_FILES" not in overrides
    assert "lean_diagnostic_messages" in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_prepare_dependencies" in overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_multi_attempt" in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_file_outline" not in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_build" not in overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert render_prompt(
        "Chapter {chapter_number_padded}: {chapter_title}", config.chapters[0]
    ) == ("Chapter 01: First chapter")

    review_command = executor.command(Stage.REVIEW)
    assert "--dangerously-bypass-approvals-and-sandbox" in review_command
    assert "--sandbox" not in review_command
    review_overrides = {
        review_command[index + 1].split("=", 1)[0]: review_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(review_command[:-1])
        if item == "--config"
    }
    assert "lean_diagnostic_messages" in review_overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_prepare_dependencies" in review_overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_code_actions" in review_overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_file_outline" not in review_overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_multi_attempt" not in review_overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_build" not in review_overrides["mcp_servers.lastlib_lean.enabled_tools"]
    fixup_command = executor.command(Stage.FIXUP, isolated)
    fixup_overrides = {
        fixup_command[index + 1].split("=", 1)[0]: fixup_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(fixup_command[:-1])
        if item == "--config"
    }
    assert "lean_diagnostic_messages" in fixup_overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_code_actions" in fixup_overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_file_outline" not in fixup_overrides[
        "mcp_servers.lastlib_lean.enabled_tools"
    ]
    assert "lean_multi_attempt" not in fixup_overrides["mcp_servers.lastlib_lean.enabled_tools"]
    assert "lean_goal" not in fixup_overrides["mcp_servers.lastlib_lean.enabled_tools"]

    assert not any(
        "mcp_servers.lastlib_lean" in item for item in executor.command(Stage.FORMALIZE)
    )

    resumed = executor.command(Stage.FORMALIZE, isolated, resume_thread_id="capacity-thread")
    assert resumed[:3] == ["codex", "exec", "resume"]
    assert "capacity-thread" in resumed
    assert "--json" in resumed
    assert "--output-schema" in resumed
    assert "--cd" not in resumed
    resumed_review = executor.command(Stage.REVIEW, isolated, resume_thread_id="review-thread")
    assert "--dangerously-bypass-approvals-and-sandbox" in resumed_review


def test_lean_mcp_does_not_prewarm_files(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    chapter_root = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_root.mkdir(parents=True)
    aggregator = tmp_path / "lean" / "Book" / "Chapter01.lean"
    dependency = chapter_root / "Dependencies.lean"
    proof = chapter_root / "Proof.lean"
    other = chapter_root / "Other.lean"
    aggregator.write_text(
        "import Book.Chapter01.Proof\nimport Book.Chapter01.Other\n",
        encoding="utf-8",
    )
    dependency.write_text("import Mathlib\n", encoding="utf-8")
    proof.write_text(
        "import Book.Chapter01.Dependencies\n\ntheorem target : True := by sorry\n",
        encoding="utf-8",
    )
    other.write_text("import Book.Chapter01.Dependencies\n", encoding="utf-8")

    command = CodexExecutor(config, StateStore(config)).command(
        Stage.PROVE,
        chapter=chapter,
        feedback="Coordinator diagnostic: lean/Book/Chapter01/Other.lean:1:1",
    )
    overrides = {
        command[index + 1].split("=", 1)[0]: command[index + 1].split("=", 1)[1]
        for index, item in enumerate(command[:-1])
        if item == "--config"
    }
    assert "mcp_servers.lastlib_lean.env.LEAN_MCP_PREWARM_FILES" not in overrides
    assert json.loads(
        overrides["mcp_servers.lastlib_lean.env.LEAN_MCP_SCRATCH_SLOTS"]
    ) == "1"


@pytest.mark.parametrize("stage", list(Stage))
def test_every_agent_prompt_explains_that_git_is_unavailable(tmp_path: Path, stage: Stage) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(config.chapters[0], stage)

    assert "filesystem is not a Git repository" in prompt
    assert "Do not run `git` commands" in prompt


@pytest.mark.parametrize("stage", list(Stage))
def test_every_agent_prompt_preserves_chronological_imports(
    tmp_path: Path, stage: Stage
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = " ".join(executor.build_prompt(config.chapters[0], stage).split())

    assert "a chapter may import only earlier chapters in the same book" in prompt
    assert "never a later chapter or later book" in prompt


@pytest.mark.parametrize("stage", list(Stage))
def test_out_of_scope_source_repairs_use_fixup_findings(
    tmp_path: Path, stage: Stage
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = " ".join(executor.build_prompt(config.chapters[0], stage).split())

    assert "If a source repair requires an out-of-scope write" in prompt
    assert "report it in `fixup_findings` with its exact owner path" in prompt
    assert "if a fix requires an out-of-scope write, report it in `issues`" not in prompt


@pytest.mark.parametrize(
    "event",
    [
        {"type": "error", "message": "Selected model is at capacity. Please try again."},
        {
            "type": "error",
            "message": "Error running remote compact task: Selected model is at capacity.",
        },
        {
            "type": "turn.failed",
            "error": {"message": "Selected model is at capacity. Please try again."},
        },
        {
            "type": "error",
            "message": (
                "exceeded retry limit, last status: 429 Too Many Requests, "
                "request id: example-request"
            ),
        },
        {
            "type": "turn.failed",
            "error": {"message": "unexpected HTTP/2 429 response from upstream"},
        },
    ],
)
def test_detects_capacity_failures(event: dict[str, object]) -> None:
    assert _is_capacity_failure(event)


def test_does_not_retry_unrelated_turn_failures() -> None:
    assert not _is_capacity_failure(
        {"type": "turn.failed", "error": {"message": "authentication failed"}}
    )


def test_capacity_retry_schedule_uses_capped_exponential_backoff() -> None:
    assert [_capacity_resume_delay(15, 120, attempt) for attempt in range(1, 11)] == [
        15,
        30,
        60,
        120,
        120,
        120,
        120,
        120,
        120,
        120,
    ]


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
    command = CodexExecutor(config, StateStore(config)).command(Stage.PROVE)

    assert "--approve-for-me" in command
    assert "--sandbox" not in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


def test_review_uses_write_sandbox_when_full_access_is_disabled(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        settings=replace(
            config.settings,
            bypass_approvals_and_sandbox=False,
            sandbox="workspace-write",
        ),
    )
    command = CodexExecutor(config, StateStore(config)).command(Stage.REVIEW)

    assert "--sandbox" in command
    assert command[command.index("--sandbox") + 1] == "workspace-write"
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


def test_review_prompt_requires_scoped_edits_and_coordinator_rebuild(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(config.chapters[0], Stage.REVIEW)
    normalized_prompt = " ".join(prompt.split())

    assert "directly make every warranted in-scope" in prompt
    assert "Attached Lean MCP" in prompt
    assert "certified the incoming sources and dependencies clean" in normalized_prompt
    assert "rebuilds it" in prompt
    assert "read-only" not in prompt

    command = executor.command(Stage.REVIEW)
    assert any("mcp_servers.lastlib_lean.command" in item for item in command)
    assert any("lean_diagnostic_messages" in item for item in command)


def test_review_prompt_starts_with_book_scoped_files_and_line_numbers(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    root = tmp_path / "lean" / "Book"
    child = root / "Chapter01" / "Section02.lean"
    child.parent.mkdir(parents=True)
    (root / "Chapter01.lean").write_text(
        "import Book.Chapter01.Section02\n\ndef chapter := section\n",
        encoding="utf-8",
    )
    child.write_text("def section := 2\n", encoding="utf-8")
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(config.chapters[0], Stage.REVIEW)

    assert prompt.startswith("# Line-numbered review source set\n")
    assert "counts as the initial complete read" in prompt
    assert "Do not reread complete files from the filesystem" in prompt
    book_header = "## `books/book.md`"
    root_header = "## `lean/Book/Chapter01.lean`"
    child_header = "## `lean/Book/Chapter01/Section02.lean`"
    assert (
        prompt.index(book_header)
        < prompt.index(root_header)
        < prompt.index(child_header)
        < prompt.index("Do A Book chapter 01")
    )
    assert "     1 | # Book" in prompt
    assert "     3 | ## 1. First chapter" in prompt
    assert "## 2. Second chapter" in prompt
    assert "     1 | import Book.Chapter01.Section02" in prompt
    assert "     2 | " in prompt
    assert "     3 | def chapter := section" in prompt
    assert "     1 | def section := 2" in prompt

    prove_prompt = executor.build_prompt(config.chapters[0], Stage.PROVE)
    assert "Line-numbered review source set" not in prove_prompt


def test_review_source_bundle_is_capped_at_500k_characters(tmp_path: Path) -> None:
    assert REVIEW_SOURCE_BUNDLE_MAX_CHARS == 500_000
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    target = tmp_path / "lean" / "Book" / "Chapter01.lean"
    target.parent.mkdir(parents=True)
    target.write_text("x" * (REVIEW_SOURCE_BUNDLE_MAX_CHARS + 10_000), encoding="utf-8")

    bundle = _review_source_bundle(tmp_path, config.chapters[0])

    assert len(bundle) == REVIEW_SOURCE_BUNDLE_MAX_CHARS
    assert bundle.endswith(
        f"[Review source set truncated at {REVIEW_SOURCE_BUNDLE_MAX_CHARS:,} characters.]\n"
    )


def test_executor_can_disable_lean_mcp(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(config, settings=replace(config.settings, lean_mcp=False))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.PROVE)
    prompt = executor.build_prompt(config.chapters[0], Stage.PROVE)

    assert not any("mcp_servers.lastlib_lean" in item for item in command)
    assert "Attached Lean MCP" not in prompt


def test_fixup_prompt_requires_diagnostics_and_sorry(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(config.chapters[0], Stage.FIXUP)
    assert "Attached Lean MCP" in prompt
    assert "lean_prepare_dependencies" in prompt
    assert "maximal affected dependents" in " ".join(prompt.split())


def test_shipped_nonproof_prompts_have_consistent_stage_contracts() -> None:
    prompt_root = Path(__file__).parents[1] / "src" / "lastlib_swarm" / "prompts"
    formalize = " ".join((prompt_root / "formalize.md").read_text().split())
    fixup = " ".join((prompt_root / "fixup.md").read_text().split())
    review = " ".join((prompt_root / "review.md").read_text().split())

    assert "structured `source_issues` ledger" in formalize
    assert "`SOURCE_ISSUE` comment" not in formalize
    assert "Set `complete` to `true` only when the full chapter coverage pass is finished" in (
        formalize
    )
    assert "mathematically natural local dependency guess" in formalize

    assert "authoritative starting evidence" in fixup
    assert "Do not reread the complete chapter or assigned file set" in fixup
    assert "coordinator feedback as the initial diagnostic pass" in fixup
    assert "do not request diagnostics merely because you entered or switched files" in fixup
    assert "request whole-file MCP diagnostics for that edited dependency closure" in fixup
    assert "`SOURCE_ISSUE` comment" not in fixup
    assert "`complete` means that every supplied finding" in fixup
    assert "Never prove" in fixup
    assert "`by sorry`" in fixup

    assert "Diagnose only the edited dependency closure" in review
    assert "recheck the complete chapter" not in review.casefold()


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


def test_bounded_feedback_preserves_first_and_last_diagnostics() -> None:
    feedback = "FIRST DIAGNOSTIC\n" + ("middle\n" * 3000) + "LAST DIAGNOSTIC"

    bounded = _bounded_feedback(feedback)

    assert len(bounded) == 12000
    assert bounded.startswith("FIRST DIAGNOSTIC")
    assert bounded.endswith("LAST DIAGNOSTIC")
    assert "middle of coordinator feedback omitted" in bounded


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
        build_command=("printf '%s\\n' 'warning: Book/Chapter.lean:1:1: declaration uses `sorry`'"),
    )

    validation = await validate(config, chapter)

    assert validation.succeeded
    assert validation.exit_code == 0


@pytest.mark.asyncio
async def test_validation_streams_build_output(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command="printf '%s\\n' 'building dependency' 'compiling chapter'",
    )
    output: list[str] = []

    validation = await validate(config, chapter, on_output=output.append)

    assert validation.succeeded
    assert output == ["building dependency\n", "compiling chapter\n"]


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
report = {"changed": False, "complete": True,
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
async def test_executor_resumes_same_thread_after_capacity_failure(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "invocations.jsonl"
    fake_codex = tmp_path / "capacity-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

prompt = sys.stdin.read()
record = {{"args": sys.argv[1:], "prompt": prompt}}
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\\n")
if "resume" not in sys.argv:
    print(json.dumps({{"type": "thread.started", "thread_id": "capacity-thread"}}))
    message = "Error running remote compact task: Selected model is at capacity."
    print(json.dumps({{"type": "error", "message": message}}))
    print(json.dumps({{"type": "turn.failed", "error": {{"message": message}}}}))
    raise SystemExit(1)
report = {{"changed": False, "complete": True,
          "summary": "resumed successfully", "issues": []}}
print(json.dumps({{"type": "turn.started"}}))
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(
        config,
        settings=replace(
            config.settings,
            codex_bin=str(fake_codex),
            capacity_resume_attempts=2,
            capacity_resume_delay_seconds=0,
        ),
    )
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    result = await executor.run(config.chapters[0], Stage.FORMALIZE, run)

    invocations = [json.loads(line) for line in invocations_path.read_text().splitlines()]
    assert result.succeeded
    assert result.thread_id == "capacity-thread"
    assert result.report["summary"] == "resumed successfully"
    assert len(invocations) == 2
    assert invocations[1]["args"][:2] == ["exec", "resume"]
    assert "capacity-thread" in invocations[1]["args"]
    assert "Continue from the interrupted turn" in invocations[1]["prompt"]
    log = (state.logs_dir / f"{run.id}.jsonl").read_text(encoding="utf-8")
    assert "remote compact task" in log
    assert "resumed successfully" in log
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent succeeded"
    assert any(entry.status == "retrying" for entry in activity.recent)


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
report = {"changed": False, "complete": True,
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
    reloaded = read_full_snapshot(config.settings.state_dir)
    assert reloaded is not None
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
report = {{"changed": False, "complete": True,
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
