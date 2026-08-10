from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import signal
import sys
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus, TokenUsage

REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "changed": {"type": "boolean"},
        "complete": {"type": "boolean"},
        "needs_repair": {"type": "boolean"},
        "summary": {"type": "string"},
        "issues": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["changed", "complete", "needs_repair", "summary", "issues"],
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


def lean_mcp_executable() -> Path:
    """Return the console script installed beside this package's Python interpreter."""

    return Path(sys.executable).with_name("lean-lsp-mcp")


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


def render_prompt(template: str, chapter: Chapter) -> str:
    for key, value in chapter.variables().items():
        template = template.replace("{" + key + "}", value)
    return template


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
            key in value for key in ("changed", "complete", "needs_repair", "summary", "issues")
        ):
            return value
    return None


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
        scope = "\n".join(f"- `{item}`" for item in chapter.scope)
        contract = f"""

## Orchestrator contract

Your exclusive edit scope is:
{scope}

Do not edit orchestration state under `.swarm`, do not commit, and do not wait for another worker.
When isolation is enabled, all out-of-scope changes are rejected rather than merged.
Return the required structured final report. Set `needs_repair` only when a statement or declaration
interface must change; ordinary unfinished or broken proof code is not statement repair. The
orchestrator independently checks scoped file hashes, placeholders, and `{chapter.build_command}`.
"""
        if self.config.settings.lean_mcp:
            contract += """

## Lean MCP workflow

A private `lastlib_lean` MCP server is attached to this attempt. It points at the attempt's private
Lean project. Prefer its diagnostics, goals, hover, declaration lookup, code actions, and local
search tools for interactive checking. The MCP intentionally does not expose `lean_build` or remote
search. Do not start another language server.
"""
            if stage == Stage.PROVE:
                contract += """
Before editing, inspect every assigned Lean file and inventory all placeholders. Make one coherent
proof-writing pass over the entire assigned file set: attempt every mathematically sound proof once,
without stopping to check each proof separately. Then request whole-file diagnostics for every
assigned file. Iterate only over the proofs and dependent declarations that fail, using proof goals,
batched tactic attempts, code actions, and fresh whole-file diagnostics until no unexpected
diagnostics remain. Run the configured Lake build only as the final acceptance check.
"""
            else:
                contract += """
Use whole-file diagnostics after each coherent batch of edits. Run the configured Lake build only as
the final acceptance check, not as the interactive edit/check loop.
"""
        if feedback:
            contract += (
                f"\n## Feedback from the preceding attempt\n\n```text\n{feedback[-12000:]}\n```\n"
            )
        return base + contract

    def command(self, stage: Stage, workspace_root: Path | None = None) -> list[str]:
        settings = self.config.settings
        root = workspace_root or settings.repo
        command = [
            settings.codex_bin,
            "exec",
            "--json",
            "--color",
            "never",
            "--cd",
            str(root),
            "--output-schema",
            str(self.schema_path),
        ]
        if root != settings.repo:
            command.append("--skip-git-repo-check")
        if settings.bypass_approvals_and_sandbox:
            command.append("--dangerously-bypass-approvals-and-sandbox")
        elif settings.approve_for_me:
            # Current Codex versions make --approve-for-me mutually exclusive
            # with --sandbox; approve-for-me itself selects workspace-write.
            command.append("--approve-for-me")
        else:
            command.extend(["--sandbox", settings.sandbox])
        if settings.model:
            command.extend(["--model", settings.model])
        if settings.reasoning_effort:
            command.extend(["--config", f'model_reasoning_effort="{settings.reasoning_effort}"'])
        if settings.lean_mcp:
            lean_project = (root / settings.lean_project).resolve()
            tools = (
                LEAN_MCP_PROOF_TOOLS
                if stage in (Stage.PROVE, Stage.REPAIR)
                else LEAN_MCP_BASE_TOOLS
            )
            mcp_config = {
                "mcp_servers.lastlib_lean.command": str(lean_mcp_executable()),
                "mcp_servers.lastlib_lean.cwd": str(lean_project),
                "mcp_servers.lastlib_lean.required": True,
                "mcp_servers.lastlib_lean.startup_timeout_sec": 60,
                "mcp_servers.lastlib_lean.tool_timeout_sec": (
                    settings.lean_mcp_tool_timeout_seconds
                ),
                "mcp_servers.lastlib_lean.default_tools_approval_mode": "auto",
                "mcp_servers.lastlib_lean.enabled_tools": list(tools),
                "mcp_servers.lastlib_lean.env.PATH": lean_mcp_path(),
                "mcp_servers.lastlib_lean.env.LEAN_PROJECT_PATH": str(lean_project),
                "mcp_servers.lastlib_lean.env.LEAN_LOG_LEVEL": "NONE",
                "mcp_servers.lastlib_lean.env.PYTHONWARNINGS": "ignore",
            }
            for key, value in mcp_config.items():
                command.extend(["--config", f"{key}={json.dumps(value)}"])
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
        process = await asyncio.create_subprocess_exec(
            *self.command(stage, root),
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
        stdin.write(prompt.encode())
        await stdin.drain()
        stdin.close()
        usage = TokenUsage()
        report: dict[str, Any] = {}
        thread_id: str | None = None
        activity = self.state.activities.start(run.id, chapter.id, stage.value)

        async def consume() -> None:
            nonlocal usage, report, thread_id
            with log_path.open("wb", buffering=0) as log:
                pending = bytearray()

                async def consume_line(line: bytes) -> None:
                    nonlocal usage, report, thread_id
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        return
                    activity.consume(event, workspace_root=root)
                    self.state.activities.save(activity)
                    if found := _find_thread_id(event):
                        thread_id = found
                        await self.state.update_run(run, thread_id=found)
                    if found_usage := TokenUsage.from_event(event):
                        usage = found_usage
                        await self.state.update_run(run, usage=usage)
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
                exit_code = await process.wait()
        except TimeoutError:
            timed_out = True
            await _terminate(process)
            exit_code = 124
        except asyncio.CancelledError:
            await _terminate(process)
            consumer.cancel()
            with suppress(asyncio.CancelledError):
                await consumer
            activity.finish("cancelled", "agent cancelled by orchestrator")
            self.state.activities.save(activity)
            raise
        await consumer
        changed = before != scope_digest(root, chapter)
        placeholders = count_placeholders(root, chapter)
        error = "agent timed out" if timed_out else ""
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
        )


async def validate(
    config: PipelineConfig, chapter: Chapter, *, workspace_root: Path | None = None
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
    try:
        async with asyncio.timeout(config.settings.validation_timeout_seconds):
            output_bytes, _ = await process.communicate()
        exit_code = process.returncode or 0
        timed_out = False
    except TimeoutError:
        await _terminate(process)
        output_bytes = b"validation timed out"
        exit_code = 124
        timed_out = True
    except asyncio.CancelledError:
        await _terminate(process)
        raise
    output = output_bytes.decode(errors="replace")[-20000:]
    return ValidationResult(exit_code == 0, exit_code, output, timed_out)


async def _terminate(process: asyncio.subprocess.Process) -> None:
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            await process.wait()
    except TimeoutError:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()
