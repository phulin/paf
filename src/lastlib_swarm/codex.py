from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import signal
import sys
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lastlib_swarm import json_codec as json
from lastlib_swarm.diagnostics import unexpected_lean_warnings
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus, TokenUsage

REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "changed": {"type": "boolean"},
        "complete": {"type": "boolean"},
        "needs_fixup": {"type": "boolean"},
        "summary": {"type": "string"},
        "issues": {"type": "array", "items": {"type": "string"}},
        "source_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "location": {"type": "string", "minLength": 1},
                    "source_excerpt": {"type": "string", "minLength": 1},
                    "description": {"type": "string", "minLength": 1},
                    "suggested_correction": {"type": "string", "minLength": 1},
                },
                "required": [
                    "location",
                    "source_excerpt",
                    "description",
                    "suggested_correction",
                ],
            },
        },
    },
    "required": [
        "changed",
        "complete",
        "needs_fixup",
        "summary",
        "issues",
        "source_issues",
    ],
}

LEAN_MCP_BASE_TOOLS = (
    "lean_file_outline",
    "lean_diagnostic_messages",
    "lean_hover_info",
    "lean_declaration_file",
    "lean_local_search",
)

LEAN_MCP_PROOF_TOOLS = (
    *LEAN_MCP_BASE_TOOLS,
    "lean_goal",
    "lean_term_goal",
    "lean_completions",
    "lean_multi_attempt",
    "lean_code_actions",
)

LEAN_MCP_FIXUP_TOOLS = (
    *LEAN_MCP_BASE_TOOLS,
    "lean_completions",
    "lean_code_actions",
)

USAGE_POLL_SECONDS = 0.25
PROCESS_GROUP_GRACE_SECONDS = 1.0
COMMON_PROMPT_PATH = Path(__file__).with_name("prompts") / "common.md"
CAPACITY_RESUME_PROMPT = "Continue from the interrupted turn and complete the assigned task."


def lean_mcp_executable() -> Path:
    """Return the Python interpreter used to launch the swarm MCP adapter."""

    return Path(sys.executable)


def lean_mcp_path() -> str:
    """Include elan even when the orchestrator was launched outside a login shell."""

    current = os.environ.get("PATH", "").split(os.pathsep)
    candidates = [lean_mcp_executable().parent]
    if elan_home := os.environ.get("ELAN_HOME"):
        candidates.append(Path(elan_home) / "bin")
    candidates.append(Path.home() / ".elan" / "bin")
    if lake := shutil.which("lake"):
        candidates.append(Path(lake).parent)
    prefixes = [str(path) for path in candidates if (path / "lake").is_file()]
    return os.pathsep.join(dict.fromkeys([*prefixes, *current]))


@dataclass(frozen=True)
class ValidationResult:
    succeeded: bool
    exit_code: int
    output: str
    timed_out: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "exit_code": self.exit_code,
            "output": self.output,
            "timed_out": self.timed_out,
        }


@dataclass(frozen=True)
class AgentResult:
    succeeded: bool
    exit_code: int
    changed: bool
    placeholders: int
    usage: TokenUsage
    report: dict[str, Any] = field(default_factory=dict)
    thread_id: str | None = None
    error: str = ""
    capacity_exhausted: bool = False


def render_prompt(template: str, chapter: Chapter) -> str:
    for key, value in chapter.variables().items():
        template = template.replace("{" + key + "}", value)
    return template


def _bounded_feedback(feedback: str, maximum: int = 12000) -> str:
    """Bound feedback while retaining both its first and final diagnostics."""

    if len(feedback) <= maximum:
        return feedback
    omission = "\n\n... middle of coordinator feedback omitted ...\n\n"
    available = maximum - len(omission)
    head = available // 2
    return feedback[:head] + omission + feedback[-(available - head) :]


def scoped_files(repo: Path, chapter: Chapter) -> list[Path]:
    files: set[Path] = set()
    for pattern in chapter.scope:
        files.update(path for path in repo.glob(pattern) if path.is_file())
    return sorted(files)


def scope_digest(repo: Path, chapter: Chapter) -> str:
    digest = hashlib.sha256()
    for path in scoped_files(repo, chapter):
        digest.update(path.relative_to(repo).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _lean_code(text: str) -> str:
    result: list[str] = []
    index = 0
    block_depth = 0
    in_string = False
    while index < len(text):
        pair = text[index : index + 2]
        char = text[index]
        if block_depth:
            if pair == "/-":
                block_depth += 1
                index += 2
            elif pair == "-/":
                block_depth -= 1
                index += 2
            else:
                index += 1
            continue
        if in_string:
            if char == "\\":
                index += 2
            elif char == '"':
                in_string = False
                index += 1
            else:
                index += 1
            continue
        if pair == "--":
            newline = text.find("\n", index)
            index = len(text) if newline < 0 else newline + 1
        elif pair == "/-":
            block_depth = 1
            index += 2
        elif char == '"':
            in_string = True
            index += 1
        else:
            result.append(char)
            index += 1
    return "".join(result)


def count_placeholders(repo: Path, chapter: Chapter) -> int:
    pattern = re.compile(r"\b(?:sorry|admit)\b")
    return sum(
        len(pattern.findall(_lean_code(path.read_text(encoding="utf-8"))))
        for path in scoped_files(repo, chapter)
    )


def _find_thread_id(event: Any) -> str | None:
    if not isinstance(event, dict):
        return None
    if event.get("type") == "thread.started":
        for key in ("thread_id", "threadId", "id"):
            if isinstance(event.get(key), str):
                return event[key]
    return None


def _find_report(event: Any) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None
    candidates: list[str] = []
    item = event.get("item")
    if (
        isinstance(item, dict)
        and item.get("type") == "agent_message"
        and isinstance(item.get("text"), str)
    ):
        candidates.append(item["text"])
    if event.get("type") in {"agent_message", "message"} and isinstance(event.get("text"), str):
        candidates.append(event["text"])
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and all(
            key in value for key in ("changed", "complete", "needs_fixup", "summary", "issues")
        ):
            value.setdefault("source_issues", [])
            return value
    return None


def _is_capacity_failure(event: Any) -> bool:
    if not isinstance(event, dict) or event.get("type") not in {"error", "turn.failed"}:
        return False
    error = event.get("error")
    messages = [event.get("message")]
    if isinstance(error, dict):
        messages.append(error.get("message"))
    elif isinstance(error, str):
        messages.append(error)
    return any(
        isinstance(message, str) and "at capacity" in message.casefold() for message in messages
    )


def _capacity_resume_delay(initial: float, maximum: float, attempt: int) -> float:
    """Return capped exponential backoff for a one-indexed retry attempt."""

    if attempt < 1:
        raise ValueError("capacity retry attempt must be positive")
    if initial <= 0 or maximum <= 0:
        return 0.0
    delay = min(initial, maximum)
    for _ in range(attempt - 1):
        if delay >= maximum / 2:
            return maximum
        delay *= 2
    return min(delay, maximum)


def _rollout_usage(event: Any) -> TokenUsage | None:
    if not isinstance(event, dict) or event.get("type") != "event_msg":
        return None
    payload = event.get("payload")
    if not isinstance(payload, dict) or payload.get("type") != "token_count":
        return None
    info = payload.get("info")
    if not isinstance(info, dict):
        return None
    return TokenUsage.from_event(info.get("total_token_usage"))


def _codex_rollout(thread_id: str) -> Path | None:
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    sessions = codex_home / "sessions"
    now = datetime.now(UTC)
    for offset in (0, -1):
        day = now + timedelta(days=offset)
        directory = sessions / day.strftime("%Y/%m/%d")
        if matches := tuple(directory.glob(f"rollout-*{thread_id}.jsonl")):
            return max(matches, key=lambda path: path.stat().st_mtime_ns)
    return None


async def _tail_rollout_usage(
    thread_id: str,
    stop: asyncio.Event,
    update: Callable[[TokenUsage], Awaitable[None]],
) -> None:
    path: Path | None = None
    offset = 0
    pending = bytearray()
    while True:
        if path is None:
            path = await asyncio.to_thread(_codex_rollout, thread_id)
        if path is not None:
            try:
                with path.open("rb") as handle:
                    handle.seek(offset)
                    chunk = handle.read()
                    offset = handle.tell()
            except (FileNotFoundError, OSError):
                path = None
                offset = 0
                pending.clear()
            else:
                pending.extend(chunk)
                while (newline := pending.find(b"\n")) >= 0:
                    line = bytes(pending[:newline])
                    del pending[: newline + 1]
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if usage := _rollout_usage(event):
                        await update(usage)
        if stop.is_set():
            return
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=USAGE_POLL_SECONDS)


class CodexExecutor:
    def __init__(self, config: PipelineConfig, state: StateStore) -> None:
        self.config = config
        self.state = state
        self.schema_path = config.settings.state_dir / "agent-report.schema.json"

    async def prepare(self) -> None:
        self.schema_path.parent.mkdir(parents=True, exist_ok=True)
        self.schema_path.write_text(json.dumps(REPORT_SCHEMA, indent=2), encoding="utf-8")

    def build_prompt(
        self,
        chapter: Chapter,
        stage: Stage,
        *,
        feedback: str = "",
    ) -> str:
        template = self.config.stages[stage].prompt.read_text(encoding="utf-8")
        base = render_prompt(template, chapter)
        common = render_prompt(COMMON_PROMPT_PATH.read_text(encoding="utf-8"), chapter)
        scope = "\n".join(f"- `{item}`" for item in chapter.scope)
        stage_contract = {
            Stage.FORMALIZE: """This is one optimistic drafting attempt. The coordinator merges
accepted scoped changes without running Lean. Compiler failures are deferred to the global fixup
loop.""",
            Stage.FIXUP: """Use the attached Lean MCP and the coordinator diagnostics or review
findings appended to this prompt. This attempt is dependency-ready and may run beside unrelated
fixups. As soon as it finishes, the coordinator merges it, rescans observed imports, and rebuilds it
when its refined predecessors are clean; unrelated agents continue running. Never prove propositions
in this stage: replace an obstructing proof body with `by sorry`.
Request fresh whole-file diagnostics after every edit.""",
            Stage.REVIEW: """This attempt is read-only. Return findings in the structured report and
set `needs_fixup` when source changes are required. The coordinator discards any attempted file
change.""",
            Stage.PROVE: """The project entered this attempt with a clean reviewed build. After the
attempt, the coordinator builds the assigned chapter against its single writable cache.""",
        }[stage]
        contract = f"""

## Runtime contract

### Scope and lifecycle

Your exclusive edit scope is:
{scope}

This is a hard write boundary: edit only the paths listed above. You may read files elsewhere for
context, but do not create, modify, move, delete, format, or otherwise write any path outside this
scope. In particular, do not edit `.swarm`, `SWARM.md`, repository-level documentation, prompts,
scripts, orchestration code, configuration, or tests, even if changing them seems useful for this
task. You may investigate tooling or infrastructure problems and use in-scope workarounds, but if a
fix requires an out-of-scope write, report it in `issues` instead of making that write. Before
every edit, verify that its target matches one of the listed scope paths. Any out-of-scope write
causes the coordinator to reject the entire attempt.

Do not commit and do not wait for another worker.
Do not run `lake build`, `lake env lean`, raw `lean`, or another compiler command. Builds belong to
the coordinator and use its single writable build cache. {stage_contract}

### Final response

Return the required structured report. Set `changed` from the actual scoped diff and `complete` from
the stage's definition of done. Set `needs_fixup` only when a statement or declaration interface
must change; ordinary unfinished or broken proof code is not statement fixup. Summarize the work
concisely and list every unresolved issue.

Record each genuine defect in the informal textbook in `source_issues`, with its precise heading or
other location, an exact identifying excerpt, a mathematical explanation, and the smallest suggested
replacement. Do not use this ledger for missing Lean APIs, proof failures, or tooling problems. A
source issue is not a reason to stop: make the minimal principled accommodation permitted by this
stage, clearly preserve or report the obstruction, and continue as far as possible through every
unaffected part of the chapter. The coordinator independently checks scoped hashes, placeholders,
diagnostics, and `{chapter.build_command}`.
"""
        if self.config.settings.lean_mcp and stage in (Stage.FIXUP, Stage.PROVE):
            capabilities = (
                "whole-file diagnostics, outline, hover, declaration lookup, local search, "
                "completions, and code actions"
                if stage is Stage.FIXUP
                else "whole-file diagnostics, goals, hover, declaration lookup, code actions, "
                "completions, tactic trials, and local search"
            )
            contract += f"""

### Attached Lean MCP

A private `lastlib_lean` MCP server is attached to this attempt. It points at the attempt's private
Lean project. Use its {capabilities}. It intentionally does not expose `lean_build` or remote
search. Paths passed to its tools are relative to the Lean project root: use `LastLib/...`, not
`lean/LastLib/...`.
Whenever you switch from one Lean file to another,
make a whole-file diagnostic call for the destination before using other Lean tools. The MCP treats
that switch as a reopen with one dependency-build pass. Do the same when switching back, especially
after editing a file that it imports. Do not start another language server or work around stale
imports with a compiler command.
"""
        if feedback:
            contract += (
                "\n## Feedback from the preceding attempt\n\n```text\n"
                f"{_bounded_feedback(feedback)}\n```\n"
            )
        return f"{base.rstrip()}\n\n{common.rstrip()}\n{contract}"

    def command(
        self,
        stage: Stage,
        workspace_root: Path | None = None,
        *,
        resume_thread_id: str | None = None,
    ) -> list[str]:
        settings = self.config.settings
        root = workspace_root or settings.repo
        command = [settings.codex_bin, "exec"]
        if resume_thread_id is None:
            command.extend(["--json", "--color", "never", "--cd", str(root)])
        else:
            command.extend(["resume", "--json"])
        command.extend(["--output-schema", str(self.schema_path)])
        if root != settings.repo:
            command.append("--skip-git-repo-check")
        if stage is Stage.REVIEW:
            if resume_thread_id is None:
                command.extend(["--sandbox", "read-only"])
        elif settings.bypass_approvals_and_sandbox:
            command.append("--dangerously-bypass-approvals-and-sandbox")
        elif settings.approve_for_me and resume_thread_id is None:
            # Current Codex versions make --approve-for-me mutually exclusive
            # with --sandbox; approve-for-me itself selects workspace-write.
            command.append("--approve-for-me")
        else:
            command.extend(["--sandbox", settings.sandbox])
        if settings.model:
            command.extend(["--model", settings.model])
        if settings.reasoning_effort:
            command.extend(["--config", f'model_reasoning_effort="{settings.reasoning_effort}"'])
        if settings.lean_mcp and stage in (Stage.FIXUP, Stage.PROVE):
            lean_project = (root / settings.lean_project).resolve()
            enabled_tools = LEAN_MCP_FIXUP_TOOLS if stage is Stage.FIXUP else LEAN_MCP_PROOF_TOOLS
            mcp_config = {
                "mcp_servers.lastlib_lean.command": str(lean_mcp_executable()),
                "mcp_servers.lastlib_lean.args": ["-m", "lastlib_swarm.lean_mcp"],
                "mcp_servers.lastlib_lean.cwd": str(lean_project),
                "mcp_servers.lastlib_lean.required": True,
                "mcp_servers.lastlib_lean.startup_timeout_sec": 60,
                "mcp_servers.lastlib_lean.tool_timeout_sec": (
                    settings.lean_mcp_tool_timeout_seconds
                ),
                "mcp_servers.lastlib_lean.default_tools_approval_mode": "auto",
                "mcp_servers.lastlib_lean.enabled_tools": list(enabled_tools),
                "mcp_servers.lastlib_lean.env.PATH": lean_mcp_path(),
                "mcp_servers.lastlib_lean.env.LEAN_PROJECT_PATH": str(lean_project),
                "mcp_servers.lastlib_lean.env.LEAN_LOG_LEVEL": "NONE",
                "mcp_servers.lastlib_lean.env.PYTHONWARNINGS": "ignore",
            }
            for key, value in mcp_config.items():
                command.extend(["--config", f"{key}={json.dumps(value)}"])
        if resume_thread_id is not None:
            command.append(resume_thread_id)
        command.append("-")
        return command

    async def run(
        self,
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        root = workspace_root or self.config.settings.repo
        prompt = self.build_prompt(chapter, stage, feedback=feedback)
        before = scope_digest(root, chapter)
        log_path = self.state.logs_dir / f"{run.id}.jsonl"
        usage = TokenUsage()
        report: dict[str, Any] = {}
        thread_id: str | None = None
        activity = self.state.activities.start(run.id, chapter.id, stage.value)
        usage_stop = asyncio.Event()
        usage_monitor: asyncio.Task[None] | None = None

        async def update_usage(found: TokenUsage) -> None:
            nonlocal usage
            if found.total_tokens < usage.total_tokens:
                return
            usage = found
            await self.state.update_run(run, usage=usage)

        async def stop_usage_monitor() -> None:
            usage_stop.set()
            if usage_monitor is not None:
                await usage_monitor

        async def invoke(
            command: list[str], input_text: str, *, append_log: bool
        ) -> tuple[int, bool, bool]:
            nonlocal usage, report, thread_id, usage_monitor
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=root,
                start_new_session=True,
            )
            await self.state.update_run(run, pid=process.pid, log_path=str(log_path))
            if process.stdin is None or process.stdout is None:
                raise RuntimeError("failed to open Codex subprocess pipes")
            stdin = process.stdin
            stdout = process.stdout
            stdin.write(input_text.encode())
            await stdin.drain()
            stdin.close()
            capacity_failure = False

            async def consume() -> None:
                nonlocal usage, report, thread_id, usage_monitor, capacity_failure
                mode = "ab" if append_log else "wb"
                with log_path.open(mode, buffering=0) as log:
                    pending = bytearray()

                    async def consume_line(line: bytes) -> None:
                        nonlocal usage, report, thread_id, usage_monitor, capacity_failure
                        try:
                            event = json.loads(line)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            return
                        activity.consume(event, workspace_root=root)
                        self.state.activities.save(activity)
                        capacity_failure = capacity_failure or _is_capacity_failure(event)
                        if found := _find_thread_id(event):
                            thread_id = found
                            await self.state.update_run(run, thread_id=found)
                            if usage_monitor is None:
                                usage_monitor = asyncio.create_task(
                                    _tail_rollout_usage(found, usage_stop, update_usage)
                                )
                        if found_usage := TokenUsage.from_event(event):
                            await update_usage(found_usage)
                        if found_report := _find_report(event):
                            report = found_report

                    # Codex command events can contain multi-megabyte aggregated output.
                    # StreamReader.readline() has a 64 KiB default limit and stops draining
                    # the child when one such JSONL record exceeds it. Frame records from
                    # fixed-size chunks instead, without imposing an artificial line cap.
                    while chunk := await stdout.read(64 * 1024):
                        log.write(chunk)
                        pending.extend(chunk)
                        while (newline := pending.find(b"\n")) >= 0:
                            line = bytes(pending[:newline])
                            del pending[: newline + 1]
                            await consume_line(line)
                    if pending:
                        await consume_line(bytes(pending))

            consumer = asyncio.create_task(consume())
            timed_out = False
            try:
                async with asyncio.timeout(self.config.settings.agent_timeout_seconds):
                    exit_code = await _wait_for_parent_exit(process)
            except TimeoutError:
                timed_out = True
                await _terminate(process)
                exit_code = 124
            except asyncio.CancelledError:
                await _terminate(process)
                consumer.cancel()
                with suppress(asyncio.CancelledError):
                    await consumer
                raise
            if not timed_out:
                # Codex can exit before its MCP/LSP descendants. Reap the complete
                # process group before integration so no language server retains the
                # overlay or races the coordinator-owned build.
                await _terminate(process)
            await consumer
            return exit_code, timed_out, capacity_failure

        resume_attempt = 0
        capacity_failure = False
        timed_out = False
        try:
            while True:
                if resume_attempt:
                    assert thread_id is not None
                    command = self.command(stage, root, resume_thread_id=thread_id)
                    input_text = CAPACITY_RESUME_PROMPT
                else:
                    command = self.command(stage, root)
                    input_text = prompt
                exit_code, timed_out, capacity_failure = await invoke(
                    command, input_text, append_log=bool(resume_attempt)
                )
                if (
                    exit_code == 0
                    or not capacity_failure
                    or thread_id is None
                    or resume_attempt >= self.config.settings.capacity_resume_attempts
                ):
                    break
                resume_attempt += 1
                activity.retry(
                    f"capacity retry {resume_attempt}/"
                    f"{self.config.settings.capacity_resume_attempts}: resuming {thread_id}"
                )
                self.state.activities.save(activity)
                await asyncio.sleep(
                    _capacity_resume_delay(
                        self.config.settings.capacity_resume_delay_seconds,
                        self.config.settings.capacity_resume_max_delay_seconds,
                        resume_attempt,
                    )
                )
        except asyncio.CancelledError:
            await stop_usage_monitor()
            activity.finish("cancelled", "agent cancelled by orchestrator")
            self.state.activities.save(activity)
            raise
        finally:
            await stop_usage_monitor()
        changed = before != scope_digest(root, chapter)
        placeholders = count_placeholders(root, chapter)
        error = "agent timed out" if timed_out else ""
        if capacity_failure and exit_code != 0:
            error = "Codex capacity retries exhausted"
        if exit_code == 0 and not report:
            error = "Codex returned no structured final report"
        succeeded = exit_code == 0 and bool(report)
        activity.finish("succeeded" if succeeded else "failed", error)
        self.state.activities.save(activity)
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED if succeeded else TaskStatus.FAILED,
            exit_code=exit_code,
            changed=changed,
            placeholders=placeholders,
            report=report or None,
            usage=usage,
            thread_id=thread_id,
        )
        return AgentResult(
            succeeded=succeeded,
            exit_code=exit_code,
            changed=changed,
            placeholders=placeholders,
            usage=usage,
            report=report,
            thread_id=thread_id,
            error=error,
            capacity_exhausted=capacity_failure and exit_code != 0,
        )


async def validate(
    config: PipelineConfig,
    chapter: Chapter,
    *,
    workspace_root: Path | None = None,
    on_output: Callable[[str], None] | None = None,
) -> ValidationResult:
    root = workspace_root or config.settings.repo
    process = await asyncio.create_subprocess_exec(
        "bash",
        "-lc",
        chapter.build_command,
        cwd=root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    if process.stdout is None:
        raise RuntimeError("failed to open validation subprocess output")
    output_parts: list[bytes] = []
    try:
        async with asyncio.timeout(config.settings.validation_timeout_seconds):
            while line := await process.stdout.readline():
                output_parts.append(line)
                if on_output is not None:
                    on_output(line.decode(errors="replace"))
            await process.wait()
        exit_code = process.returncode or 0
        timed_out = False
    except TimeoutError:
        await _terminate(process)
        timeout_message = b"validation timed out"
        output_parts.append(timeout_message)
        if on_output is not None:
            on_output(timeout_message.decode())
        exit_code = 124
        timed_out = True
    except asyncio.CancelledError:
        await _terminate(process)
        raise
    output_bytes = b"".join(output_parts)
    complete_output = output_bytes.decode(errors="replace")
    warnings = unexpected_lean_warnings(complete_output)
    output = complete_output[-20000:]
    if warnings:
        warning_summary = "\n".join(warnings[-50:])
        output = (
            f"{output}\n\nCoordinator rejected {len(warnings)} non-sorry Lean warning(s):\n"
            f"{warning_summary}"
        )[-20000:]
        if exit_code == 0:
            exit_code = 1
    return ValidationResult(exit_code == 0 and not warnings, exit_code, output, timed_out)


async def _wait_for_parent_exit(process: asyncio.subprocess.Process) -> int:
    """Wait for the direct child without waiting for descendant-held pipes.

    ``Process.wait`` may not resolve until inherited stdout descriptors close.
    Polling ``returncode`` observes the child watcher immediately, allowing the
    caller to reap an exited Codex process's surviving MCP/LSP process group.
    """

    while process.returncode is None:
        await asyncio.sleep(0.05)
    return process.returncode


async def _terminate(process: asyncio.subprocess.Process) -> None:
    process_group = process.pid
    with suppress(ProcessLookupError):
        os.killpg(process_group, signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            await process.wait()
    except TimeoutError:
        with suppress(ProcessLookupError):
            os.killpg(process_group, signal.SIGKILL)
        await process.wait()
        return

    # Codex may exit before its MCP/LSP grandchildren. Those descendants keep the
    # private overlay busy even after reparenting to PID 1, so wait for the entire
    # process group—not only its original leader—then force-kill any survivors.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + PROCESS_GROUP_GRACE_SECONDS
    while _process_group_exists(process_group) and loop.time() < deadline:
        await asyncio.sleep(0.05)
    if _process_group_exists(process_group):
        with suppress(ProcessLookupError):
            os.killpg(process_group, signal.SIGKILL)


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
