from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import re
import shutil
import signal
import sys
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any

from paf import json_codec as json
from paf.activity import EVENT_TIMESTAMP_FIELD, activity_timestamp
from paf.backends import LeanBackend
from paf.diagnostics import unexpected_lean_warnings
from paf.models import PipelineConfig, Stage, WorkUnitLike
from paf.scope import ScopeMatcher
from paf.state import RunRecord, StateStore, TaskStatus, TokenUsage

REPORT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "changed": {"type": "boolean"},
        "complete": {"type": "boolean"},
        "summary": {
            "type": "string",
            "minLength": 1,
            "pattern": "\\S",
            "description": "Self-contained, change-focused prose suitable for a commit body.",
        },
        "issues": {"type": "array", "items": {"type": "string"}},
        "source_dependencies": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "description": "Direct prerequisite work-unit ids found during source discovery.",
        },
        "fixup_findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "description": {"type": "string", "minLength": 1},
                    "owner_paths": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                    },
                },
                "required": ["description", "owner_paths"],
            },
        },
        "upstream_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "blocked_declaration": {"type": "string", "minLength": 1},
                    "consumer_path": {"type": "string", "minLength": 1},
                    "residual_goal": {"type": "string", "minLength": 1},
                    "needed_result": {"type": "string", "minLength": 1},
                    "owner_chapter_id": {"type": "string", "minLength": 1},
                    "owner_paths": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                    },
                    "attempted_alternatives": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 2,
                    },
                },
                "required": [
                    "blocked_declaration",
                    "consumer_path",
                    "residual_goal",
                    "needed_result",
                    "owner_chapter_id",
                    "owner_paths",
                    "attempted_alternatives",
                ],
            },
        },
        "upstream_answers": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "request_ids": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                    },
                    "disposition": {
                        "type": "string",
                        "enum": ["added", "existing", "downstream"],
                    },
                    "declarations": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                    },
                    "usage_guidance": {"type": "string"},
                    "rejection_reason": {"type": "string"},
                },
                "required": [
                    "request_ids",
                    "disposition",
                    "declarations",
                    "usage_guidance",
                    "rejection_reason",
                ],
            },
        },
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
        "summary",
        "issues",
        "source_dependencies",
        "fixup_findings",
        "upstream_requests",
        "upstream_answers",
        "source_issues",
    ],
}

LEAN_MCP_BASE_TOOLS = (
    "lean_diagnostic_messages",
    "lean_prepare_dependencies",
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

LEAN_MCP_FORMALIZE_TOOLS = (
    *LEAN_MCP_BASE_TOOLS,
    "lean_completions",
    "lean_code_actions",
)

USAGE_POLL_SECONDS = 1.0
ROLLOUT_READ_BYTES = 1024 * 1024
PROCESS_GROUP_GRACE_SECONDS = 1.0
_PROMPT_RESOURCES = files("paf.prompts")
COMMON_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("common.md")))
PROOF_REVIEW_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("proof_review.md")))
UPSTREAM_REPAIR_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("upstream_repair.md")))
UPSTREAM_REPAIR_ROLE = "upstream_repair"
DOWNSTREAM_RETRY_ROLE = "downstream_retry"
CAPACITY_RESUME_PROMPT = "Continue from the interrupted turn and complete the assigned task."
UPSTREAM_SOURCE_BUNDLE_MAX_CHARS = 240_000
LEAN_DECLARATION_RE = re.compile(
    r"^[ \t]*(?:(?:noncomputable|private|protected|unsafe|opaque)[ \t]+)*"
    r"(?:theorem|lemma|def|abbrev|structure|class|instance)[ \t]+"
    r"(?P<name>[^\s([{:=]+)",
    re.MULTILINE,
)


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


class FatalCodexInvocationError(RuntimeError):
    """A non-retryable Codex request/configuration failure."""


def render_prompt(template: str, chapter: WorkUnitLike) -> str:
    for key, value in chapter.variables().items():
        template = template.replace("{" + key + "}", value)
    return template


def _bounded_feedback(feedback: str, maximum: int = 48_000) -> str:
    """Bound feedback while retaining endpoints and an index of omitted diagnostics."""

    if len(feedback) <= maximum:
        return feedback
    provisional_head = maximum // 3
    provisional_tail = maximum // 2
    omitted = feedback[provisional_head : len(feedback) - provisional_tail]
    identifying_lines: list[str] = []
    for line in omitted.splitlines():
        stripped = line.strip()
        if not stripped or stripped in identifying_lines:
            continue
        if (
            stripped.startswith(("error:", "Proof attempt ", "Review finding "))
            or "Requested edit paths" in stripped
            or re.search(r"[^\s`]+\.lean(?::\d+)?", stripped)
        ):
            identifying_lines.append(stripped[:300])
    index = "\n".join(identifying_lines)
    if len(index) > maximum // 6:
        index = index[: maximum // 6].rsplit("\n", 1)[0]
        index += "\n... additional omitted identifiers ..."
    omission = "\n\n... coordinator feedback body omitted ..."
    if index:
        omission += f"\nOmitted diagnostic/finding index:\n{index}"
    omission += "\n\n"
    available = maximum - len(omission)
    head = available // 3
    return feedback[:head] + omission + feedback[-(available - head) :]


def scoped_files(repo: Path, chapter: WorkUnitLike) -> list[Path]:
    return ScopeMatcher(chapter.scope).files(repo)


def _display_path(repo: Path, path: Path) -> str:
    try:
        return path.relative_to(repo).as_posix()
    except ValueError:
        return path.as_posix()


def _line_numbered(lines: list[str], *, start: int = 1) -> str:
    if not lines:
        return f"{start:6d} | \n"
    return "".join(f"{number:6d} | {line}\n" for number, line in enumerate(lines, start))


def _textbook_chapter_excerpt(repo: Path, chapter: WorkUnitLike) -> tuple[str, str]:
    source = chapter.source if chapter.source.is_absolute() else repo / chapter.source
    if not source.is_file():
        return _display_path(repo, source), "[Textbook source is missing.]\n"
    lines = source.read_text(encoding="utf-8", errors="replace").splitlines()
    start = max(chapter.source_span.start_line - 1, 0)
    stop = min(chapter.source_span.end_line, len(lines))
    # A legacy numbered-Markdown source may have changed after discovery. Keep
    # its historical heading-to-heading excerpt behavior while all other
    # adapters use the format-neutral recorded span.
    if start < len(lines) and re.match(r"^##\s+\d+\.\s+", lines[start]):
        stop = next(
            (
                index
                for index in range(start + 1, len(lines))
                if re.match(r"^##\s+\d+\.\s+", lines[index])
            ),
            len(lines),
        )
    if start >= len(lines) or stop <= start:
        return _display_path(repo, source), "[Configured source span was not found.]\n"
    return _display_path(repo, source), _line_numbered(lines[start:stop], start=start + 1)


def _declaration_excerpt(path: Path, declaration: str) -> tuple[int, list[str]] | None:
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = list(LEAN_DECLARATION_RE.finditer(text))
    short_name = declaration.rsplit(".", 1)[-1]
    for index, match in enumerate(matches):
        found = match.group("name")
        if found not in {declaration, short_name} and not declaration.endswith("." + found):
            continue
        line_start = text.count("\n", 0, match.start())
        # Include a small doc/attribute prelude, stopping at the previous declaration.
        excerpt_start = max(line_start - 5, 0)
        if index:
            previous_end_line = text.count("\n", 0, matches[index - 1].start()) + 1
            excerpt_start = max(excerpt_start, previous_end_line)
        line_stop = (
            text.count("\n", 0, matches[index + 1].start())
            if index + 1 < len(matches)
            else len(text.splitlines())
        )
        lines = text.splitlines()[excerpt_start:line_stop]
        return excerpt_start + 1, lines
    return None


def declaration_uses_placeholder(repo: Path, path: str, declaration: str) -> bool | None:
    """Return whether one named declaration still contains ``sorry``/``admit``."""

    target = (repo / path).resolve()
    try:
        target.relative_to(repo.resolve())
    except ValueError:
        return None
    excerpt = _declaration_excerpt(target, declaration)
    if excerpt is None:
        return None
    _, lines = excerpt
    return re.search(r"\b(?:sorry|admit)\b", _lean_code("\n".join(lines))) is not None


def declaration_uses_placeholder_in_chapter(
    repo: Path,
    chapter: WorkUnitLike,
    declaration: str,
) -> bool | None:
    """Resolve a reported declaration inside one chapter's configured source scope.

    ``None`` means that no matching declaration was found. Multiple short-name matches are
    treated conservatively: any unresolved match makes the reported interface unresolved too.
    """

    matches: list[bool] = []
    for path in scoped_files(repo, chapter):
        relative = path.relative_to(repo).as_posix()
        status = declaration_uses_placeholder(repo, relative, declaration)
        if status is not None:
            matches.append(status)
    return any(matches) if matches else None


def _upstream_source_bundle(
    repo: Path,
    owner: WorkUnitLike,
    requests: Iterable[dict[str, Any]],
    chapters: Iterable[WorkUnitLike],
    maximum: int = UPSTREAM_SOURCE_BUNDLE_MAX_CHARS,
) -> str:
    """Supply exact requests and their relevant source evidence to one repair agent."""

    selected = tuple(requests)
    by_id = {chapter.id: chapter for chapter in chapters}
    parts = [
        "# Targeted upstream repair evidence\n\n",
        "This line-numbered snapshot supplies the request batch, relevant owner files, consumer "
        "statements, and textbook excerpts. Read other files only for focused dependency lookup.\n",
        "\n## Durable request batch\n",
    ]
    for request in selected:
        request_id = str(request.get("id", "unknown"))
        parts.extend(
            [
                f"\n### Request `{request_id}`\n\n",
                f"- Consumer chapter: `{request.get('consumer_chapter_id', '')}`\n",
                f"- Blocked declaration: `{request.get('blocked_declaration', '')}`\n",
                f"- Consumer path: `{request.get('consumer_path', '')}`\n",
                f"- Residual goal: `{request.get('residual_goal', '')}`\n",
                f"- Requested result: {request.get('needed_result', '')}\n",
                "- Attempted alternatives:\n",
            ]
        )
        attempted = request.get("attempted_alternatives")
        if isinstance(attempted, list):
            parts.extend(f"  - {item}\n" for item in attempted if isinstance(item, str))
        previous = request.get("previous_attempts")
        if isinstance(previous, str) and previous.strip():
            parts.extend(["- Previous proof-attempt ledger:\n\n", "```text\n", previous, "\n```\n"])

    # Keep the exact consumer declarations ahead of potentially large textbook and owner-source
    # excerpts so the bounded evidence packet cannot truncate the statements the repair must serve.
    parts.append("\n## Relevant consumer declarations\n")
    for request in selected:
        relative = str(request.get("consumer_path", ""))
        declaration = str(request.get("blocked_declaration", ""))
        excerpt = _declaration_excerpt(repo / relative, declaration)
        parts.append(f"\n### `{relative}` — `{declaration}`\n\n")
        if excerpt is None:
            parts.append("[The named consumer declaration could not be extracted.]\n")
            continue
        start, lines = excerpt
        parts.append(_line_numbered(lines, start=start))

    excerpt_chapters = {owner.id}
    excerpt_chapters.update(str(request.get("consumer_chapter_id", "")) for request in selected)
    parts.append("\n## Relevant textbook excerpts\n")
    for chapter_id in sorted(excerpt_chapters):
        chapter = by_id.get(chapter_id)
        if chapter is None:
            continue
        path, excerpt = _textbook_chapter_excerpt(repo, chapter)
        parts.extend([f"\n### `{path}` — {chapter.id}\n\n", excerpt])

    matcher = ScopeMatcher(owner.scope)
    owner_path_set: set[str] = set()
    for request in selected:
        raw_paths = request.get("owner_paths")
        if not isinstance(raw_paths, list):
            continue
        owner_path_set.update(
            path for path in raw_paths if isinstance(path, str) and matcher.matches(path)
        )
    owner_paths = sorted(owner_path_set)
    parts.append("\n## Requested upstream source paths\n")
    for relative in owner_paths:
        path = repo / relative
        parts.append(f"\n### `{relative}`\n\n")
        if not path.is_file():
            parts.append("[File is missing; create it only if the normal owner scope permits.]\n")
            continue
        parts.append(
            _line_numbered(path.read_text(encoding="utf-8", errors="replace").splitlines())
        )

    bundle = "".join(parts)
    if len(bundle) <= maximum:
        return bundle
    marker = f"\n[Upstream repair evidence truncated at {maximum:,} characters.]\n"
    if len(marker) >= maximum:
        return marker[:maximum]
    return bundle[: maximum - len(marker)] + marker


def scope_digest(repo: Path, chapter: WorkUnitLike) -> str:
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


def count_placeholders(repo: Path, chapter: WorkUnitLike) -> int:
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
            key in value for key in ("changed", "complete", "summary", "issues")
        ):
            value.setdefault("fixup_findings", [])
            value.setdefault("source_dependencies", [])
            value.setdefault("upstream_requests", [])
            value.setdefault("upstream_answers", [])
            value.setdefault("source_issues", [])
            return value
    return None


def _event_error_messages(event: Any) -> tuple[str, ...]:
    if not isinstance(event, dict) or event.get("type") not in {"error", "turn.failed"}:
        return ()
    values: list[Any] = [event.get("message")]
    error = event.get("error")
    if isinstance(error, dict):
        values.append(error.get("message"))
    elif isinstance(error, str):
        values.append(error)
    return tuple(value for value in values if isinstance(value, str) and value.strip())


def _event_error_message(event: Any) -> str:
    """Extract the most useful human-readable error from a Codex JSONL event."""

    messages = _event_error_messages(event)
    for message in messages:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        error = payload.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return str(error["message"])
    return messages[-1] if messages else ""


def _is_fatal_invocation_failure(event: Any) -> bool:
    """Whether Codex rejected the invocation itself and retrying cannot help."""

    return any(
        "invalid_json_schema" in message
        or "invalid schema for response_format" in message.casefold()
        for message in _event_error_messages(event)
    )


def _is_capacity_failure(event: Any) -> bool:
    return any(
        (
            "at capacity" in message.casefold()
            or "too many requests" in message.casefold()
            or re.search(r"\b(?:http(?:/\S+)?\s+)?429\b", message, re.IGNORECASE) is not None
        )
        for message in _event_error_messages(event)
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


def _complete_lines(pending: bytearray, chunk: bytes) -> tuple[bytes, ...]:
    """Append a chunk and remove complete lines without repeatedly shifting the buffer."""

    pending.extend(chunk)
    lines: list[bytes] = []
    start = 0
    while (newline := pending.find(b"\n", start)) >= 0:
        lines.append(bytes(pending[start:newline]))
        start = newline + 1
    if start:
        del pending[:start]
    return tuple(lines)


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
        chunk = b""
        if path is None:
            path = await asyncio.to_thread(_codex_rollout, thread_id)
        if path is not None:
            try:
                with path.open("rb") as handle:
                    handle.seek(offset)
                    # Rollouts contain full tool results as well as tiny token-count
                    # records. Bound each synchronous read so a burst from many agents
                    # cannot monopolize the TUI's event loop.
                    chunk = handle.read(ROLLOUT_READ_BYTES)
                    offset = handle.tell()
            except (FileNotFoundError, OSError):
                path = None
                offset = 0
                pending.clear()
            else:
                for line in _complete_lines(pending, chunk):
                    # Token accounting does not need to decode the much larger tool,
                    # message, and reasoning records duplicated in the rollout.
                    if b'"token_count"' not in line:
                        continue
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if usage := _rollout_usage(event):
                        await update(usage)
        if stop.is_set() and (path is None or len(chunk) < ROLLOUT_READ_BYTES):
            return
        if len(chunk) == ROLLOUT_READ_BYTES:
            # Catch up on a large or resumed rollout one bounded chunk at a time,
            # explicitly giving terminal input and render callbacks a turn between them.
            await asyncio.sleep(0)
            continue
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
        chapter: Any,
        stage: Stage,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
        role: str = "",
        upstream_requests: Iterable[dict[str, Any]] = (),
    ) -> str:
        if role == UPSTREAM_REPAIR_ROLE:
            prompt_path = UPSTREAM_REPAIR_PROMPT_PATH
        elif stage is Stage.REVIEW and feedback:
            prompt_path = PROOF_REVIEW_PROMPT_PATH
        else:
            prompt_path = self.config.stages[stage].prompt
        template = prompt_path.read_text(encoding="utf-8")
        base = render_prompt(template, chapter)
        common = render_prompt(COMMON_PROMPT_PATH.read_text(encoding="utf-8"), chapter)
        scope = "\n".join(f"- `{item}`" for item in chapter.scope)
        input_catalog = "\n".join(
            f"- `{unit.id}` — {unit.title} ({unit.source.as_posix()}:{unit.source_span.start_line}-"
            f"{unit.source_span.end_line})"
            for unit in self.config.work_units
        )
        proof_retry_contract = ""
        if stage is Stage.PROVE and feedback and role != UPSTREAM_REPAIR_ROLE:
            proof_retry_contract = """
This is a retry. The cumulative attempt ledger appended below is prior inventory, not a conclusion
to echo. Do not merely repeat its full-file reads, clean diagnostics, searches, or prior proof
experiments.
Select one remaining placeholder and either refine a previous proof shape using specific new
evidence or perform a materially different concrete experiment: search for another earlier theorem,
unfold the local interface, prove a focused helper, construct the object directly, or change tactic
structure. Persist through several checked approaches; a retry is not exhausted by one new failed
tactic. Add and prove focused local or private helper lemmas when they unlock the result. Stop only
after sustained concrete work exposes the same hard obstruction, or after a specific mathematical
argument shows that the statement cannot follow from its assumptions. If the latter establishes
that an earlier declaration or interface must change, return a minimal structured
`upstream_requests` entry instead of another unchanged \"no pinned API\" report."""
        stage_contract = {
            Stage.DISCOVER: """This is a read-only source discovery attempt. Inspect the assigned
source chapter and identify its direct prerequisite input nodes. Do not edit any file and leave all
non-discovery report ledgers empty.""",
            Stage.FORMALIZE: """This attempt owns source-faithful formalization and elaboration.
The chapter's discovered predecessors are already clean. Create or repair the complete assigned
scope, use the attached Lean MCP to clear every diagnostic except declarations using `sorry`, and
return only after the scope is ready for the coordinator's authoritative build. Unrelated
dependency-ready formalizers may run concurrently.""",
            Stage.REVIEW: """Audit and directly make every warranted in-scope statement or API
change across the entire assigned chapter. When proof findings are attached, independently evaluate
each one while still re-reviewing the complete scope. Preserve proof placeholders and do not spend
time proving propositions.
The coordinator has certified the incoming sources and dependencies clean except for permitted
`sorry` warnings. The coordinator merges the scoped patch, then rebuilds it and returns
compiler-only failures to formalization.""",
            Stage.PROVE: """The project entered this attempt with a clean reviewed build. This is a
proof-writing attempt, not an audit. Work directly on unresolved placeholders and do not diagnose
untouched files merely to reconfirm the clean build. After the attempt, the coordinator builds the
assigned chapter against its single writable cache."""
            + proof_retry_contract,
        }[stage]
        if role == UPSTREAM_REPAIR_ROLE:
            stage_contract = """This temporary agent owns one batched upstream-interface repair.
Use the proof-capable Lean MCP, edit only the owner chapter, and fully prove every new declaration.
The coordinator independently merges and builds the owner before releasing fresh consumer retries.
Do not perform an ordinary owner-chapter placeholder pass."""
        validation_contract = {
            Stage.DISCOVER: "The coordinator validates and persists the reported source tree.",
            Stage.FORMALIZE: "The coordinator independently checks scoped hashes, placeholders, "
            "diagnostics, and the dependency-ordered build after integration.",
            Stage.REVIEW: "The coordinator independently checks scoped hashes, placeholders, and "
            "the chapter build after integration.",
            Stage.PROVE: "The coordinator independently checks scoped hashes, placeholders, "
            "diagnostics, and the chapter build.",
        }[stage]
        if role == UPSTREAM_REPAIR_ROLE:
            upstream_contract = """For this repair batch, leave `upstream_requests` empty and
return an `upstream_answers` entry covering every supplied request id. For `added` or `existing`,
give exact fully qualified declaration names and concrete usage guidance. For `downstream`, leave
declaration names empty and give both downstream guidance and the precise rejection reason. Do not
omit or discard a request merely because no upstream edit was appropriate."""
        elif stage is Stage.PROVE:
            upstream_contract = """When sustained checked work shows that one blocked declaration
needs a specific reusable result from an earlier chapter, record it in `upstream_requests`,
including the exact declaration and consumer path, residual Lean goal, minimal needed result,
proposed earlier owner chapter and paths, and at least two materially different attempted
alternatives. Continue through independent declarations before finishing. Use `fixup_findings`, not
`upstream_requests`,
for an inaccurate consumer statement or another statement/API defect that requires editing existing
interfaces. Leave `upstream_answers` empty; only targeted repair agents answer requests."""
        else:
            upstream_contract = """This agent does not create or answer proof-to-upstream handoffs.
Leave both `upstream_requests` and `upstream_answers` empty."""
        contract = f"""

## Runtime contract

### Scope and lifecycle

Your exclusive edit scope is:
{scope}

This is a hard write boundary: edit only the paths listed above. You may read files elsewhere for
context, but do not create, modify, move, delete, format, or otherwise write any path outside this
scope. In particular, do not edit `.paf`, `README.md`, repository-level documentation, prompts,
scripts, orchestration code, configuration, or tests, even if changing them seems useful for this
task. If a source repair requires an out-of-scope write, do not make that edit; report it in
`fixup_findings` with its exact owner path. Report tooling or infrastructure problems that require
no source edit in `issues`. Before every edit, verify that its target matches one of the listed
scope paths. Any out-of-scope write causes the coordinator to reject the entire attempt.

Do not commit and do not wait for another worker.
Do not run `lake build`, `lake env lean`, raw `lean`, or another compiler command. Builds belong to
the coordinator and use its single writable build cache. {stage_contract}

### Final response

Emit the required structured report exactly once, as the final response after tool use and edits
have stopped. It must describe the stable on-disk state, never planned future work. Set `changed` to
true exactly when you made a scoped filesystem edit that remains at the end, and set `complete` from
the stage's definition of done. When `changed` is true, write a concise, self-contained `summary` in
past tense that describes the actual scoped edits and their purpose, names the key files or
declarations, and is suitable for use verbatim as a Git commit body. Keep progress, future work, and
unresolved problems out of the summary and list them in `issues` instead. When `changed` is false,
briefly explain why no edit was needed.

For every unresolved actionable issue that requires another source edit, add one `fixup_findings`
entry. Its
`description` must state the complete minimal repair, and `owner_paths` must list the exact
repository-relative Lean paths that need edits, including paths outside this attempt's scope. Use a
prospective path when the repair requires creating a missing file. Split findings whose repairs have
different owners. Leave `fixup_findings` empty exactly when no source edit is requested. The
coordinator routes these entries to the chapters that own those paths.

{upstream_contract}

Set `source_dependencies` only during discovery. Use direct work-unit ids from the input catalog;
do not include the assigned work unit itself, transitive prerequisites, target-code imports, or a
dependency inferred solely from chapter numbering. Every other stage must return an empty list.

### Input catalog

{input_catalog}

Record each genuine defect in the informal textbook in `source_issues`, with its precise heading or
other location, an exact identifying excerpt, a mathematical explanation, and the smallest suggested
replacement. Do not use this ledger for missing Lean APIs, proof failures, or tooling problems. A
source issue is not a reason to stop: make the minimal principled accommodation permitted by this
stage, clearly preserve or report the obstruction, and continue as far as possible through every
unaffected part of the chapter. {validation_contract}
"""
        if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            capabilities = (
                "dependency preparation, whole-file diagnostics, hover, declaration lookup, "
                "local search, completions, and code actions"
                if stage in (Stage.FORMALIZE, Stage.REVIEW)
                else "dependency preparation, whole-file diagnostics, goals, hover, declaration "
                "lookup, code actions, completions, tactic trials, and local search"
            )
            mcp_workflow = {
                Stage.FORMALIZE: """The MCP opens and synchronizes a destination file when any Lean
tool uses it. Follow the formalization workflow above for diagnostic timing and scope.""",
                Stage.REVIEW: """Any Lean tool opens and synchronizes its destination file. Follow
the review workflow above for diagnostic timing and scope.""",
                Stage.PROVE: """The MCP opens and synchronizes a destination file when any Lean tool
uses it. Do not request diagnostics merely because you switched files. After editing, use goals and
fresh diagnostics as needed to establish that the changed proof is clean.""",
            }[stage]
            contract += f"""

### Attached Lean MCP

A private `paf_lean` MCP server is attached to this attempt. It points at the attempt's private
Lean project. Use its {capabilities}. It intentionally does not expose `lean_build` or remote
search. Paths passed to its tools are relative to the Lean project root: use `LastLib/...`, not
`lean/LastLib/...`.
{mcp_workflow}
The MCP automatically reopens a document with one dependency-build pass only when Lean reports stale
imports. Do not start another language server or work around stale imports with a compiler command.
When checking more than one edited file, call `lean_prepare_dependencies` once with only the maximal
affected dependents (the files in the changed closure that no other changed file imports). This
warms their complete imported closure with coalesced dependency preparation. Then request the final
whole-file diagnostics in import order; do not prepare every file separately.
"""
        if feedback:
            feedback_heading = (
                "Targeted downstream retry handoff"
                if role == DOWNSTREAM_RETRY_ROLE
                else {
                    Stage.DISCOVER: "Discovery feedback",
                    Stage.FORMALIZE: "Coordinator diagnostics and routed findings",
                    Stage.REVIEW: "Failed proof findings to evaluate",
                    Stage.PROVE: "Cumulative proof-attempt ledger",
                }[stage]
            )
            contract += f"\n## {feedback_heading}\n\n```text\n{_bounded_feedback(feedback)}\n```\n"
        prefix = ""
        if role == UPSTREAM_REPAIR_ROLE:
            root = workspace_root or self.config.settings.repo
            prefix = (
                _upstream_source_bundle(
                    root,
                    chapter,
                    upstream_requests,
                    self.config.chapters,
                ).rstrip()
                + "\n\n"
            )
        return f"{prefix}{base.rstrip()}\n\n{common.rstrip()}\n{contract}"

    def command(
        self,
        stage: Stage,
        workspace_root: Path | None = None,
        *,
        chapter: WorkUnitLike | None = None,
        feedback: str = "",
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
        if settings.bypass_approvals_and_sandbox:
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
        if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            backend = self.config.backend or LeanBackend(
                project=settings.lean_project,
                mcp_tool_timeout_seconds=settings.lean_mcp_tool_timeout_seconds,
            )
            mcp_config = backend.mcp_config(root, stage)
            for key, value in mcp_config.items():
                command.extend(["--config", f"{key}={json.dumps(value)}"])
        if resume_thread_id is not None:
            command.append(resume_thread_id)
        command.append("-")
        return command

    async def run(
        self,
        chapter: Any,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        root = workspace_root or self.config.settings.repo
        prompt = self.build_prompt(chapter, stage, feedback=feedback, workspace_root=root)
        return await self._run_prompt(
            chapter,
            stage,
            run,
            prompt=prompt,
            feedback=feedback,
            workspace_root=root,
        )

    async def run_upstream_repair(
        self,
        chapter: WorkUnitLike,
        run: RunRecord,
        requests: Iterable[dict[str, Any]],
        *,
        workspace_root: Path | None = None,
    ) -> AgentResult:
        """Run one proof-capable temporary agent over an owner-grouped request batch."""

        root = workspace_root or self.config.settings.repo
        selected = tuple(requests)
        prompt = self.build_prompt(
            chapter,
            Stage.PROVE,
            workspace_root=root,
            role=UPSTREAM_REPAIR_ROLE,
            upstream_requests=selected,
        )
        return await self._run_prompt(
            chapter,
            Stage.PROVE,
            run,
            prompt=prompt,
            workspace_root=root,
        )

    async def _run_prompt(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        run: RunRecord,
        *,
        prompt: str,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        root = workspace_root or self.config.settings.repo
        prompt_path = self.state.logs_dir / f"{run.id}.prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        before = scope_digest(root, chapter)
        log_path = self.state.logs_dir / f"{run.id}.jsonl"
        usage = TokenUsage()
        report: dict[str, Any] = {}
        thread_id: str | None = None
        invocation_error = ""
        fatal_invocation_failure = False
        activity = self.state.activities.start(run.id, chapter.id, run.role or stage.value)
        usage_stop = asyncio.Event()
        usage_monitor: asyncio.Task[None] | None = None
        attempt_deadline = (
            asyncio.get_running_loop().time() + self.config.settings.agent_timeout_seconds
        )

        async def update_usage(found: TokenUsage) -> None:
            nonlocal usage
            if found.total_tokens < usage.total_tokens:
                return
            usage = found
            # Live UI reads the in-memory record. Let another state transition or
            # the final run flush batch these high-frequency rollout updates.
            await self.state.update_run(run, usage=usage, deferred=True)

        async def stop_usage_monitor() -> None:
            usage_stop.set()
            if usage_monitor is not None:
                await usage_monitor

        async def invoke(
            command: list[str], input_text: str, *, append_log: bool
        ) -> tuple[int, bool, bool, int]:
            nonlocal usage, report, thread_id, usage_monitor
            nonlocal invocation_error, fatal_invocation_failure
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=root,
                start_new_session=True,
            )
            process_tree = _ProcessTreeTracker(process.pid)
            try:
                await self.state.update_run(run, pid=process.pid, log_path=str(log_path))
            except BaseException:
                # Cancellation can arrive immediately after spawning, before the
                # normal invocation cleanup guard exists. Reap the process here
                # so a cancelled scheduler task cannot leak Codex or its mount.
                with suppress(BaseException):
                    await _terminate(process, process_tree)
                raise
            if process.stdin is None or process.stdout is None:
                await _terminate(process)
                raise RuntimeError("failed to open Codex subprocess pipes")
            stdin = process.stdin
            stdout = process.stdout
            capacity_failure = False

            async def consume() -> None:
                nonlocal usage, report, thread_id, usage_monitor, capacity_failure
                nonlocal invocation_error, fatal_invocation_failure
                mode = "ab" if append_log else "wb"
                with log_path.open(mode, buffering=0) as log:
                    pending = bytearray()

                    async def consume_line(line: bytes, *, terminated: bool = True) -> None:
                        nonlocal usage, report, thread_id, usage_monitor, capacity_failure
                        nonlocal invocation_error, fatal_invocation_failure
                        try:
                            event = json.loads(line)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            log.write(line + (b"\n" if terminated else b""))
                            return
                        received_at = activity_timestamp()
                        if isinstance(event, dict):
                            event = {**event, EVENT_TIMESTAMP_FIELD: received_at}
                            log.write(json.dumpb(event) + b"\n")
                        else:
                            log.write(line + (b"\n" if terminated else b""))
                        activity.consume(event, workspace_root=root, at=received_at)
                        self.state.activities.save_throttled(activity)
                        capacity_failure = capacity_failure or _is_capacity_failure(event)
                        fatal_invocation_failure = (
                            fatal_invocation_failure or _is_fatal_invocation_failure(event)
                        )
                        if found_error := _event_error_message(event):
                            invocation_error = found_error
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
                        for line in _complete_lines(pending, chunk):
                            await consume_line(line)
                    if pending:
                        await consume_line(bytes(pending), terminated=False)

            consumer = asyncio.create_task(consume())
            timed_out = False
            fd_pressure = 0
            exit_wait: asyncio.Task[int] | None = None
            pressure_wait: asyncio.Task[int] | None = None
            try:
                stdin.write(input_text.encode())
                await stdin.drain()
                stdin.close()
                with suppress(BrokenPipeError, ConnectionResetError):
                    await stdin.wait_closed()
                exit_wait = asyncio.create_task(_wait_for_parent_exit(process))
                pressure_wait = asyncio.create_task(
                    _wait_for_fd_pressure(
                        process,
                        process_tree,
                        self.config.settings.codex_fd_recycle_threshold,
                        lambda: thread_id is not None,
                    )
                )
                async with asyncio.timeout_at(attempt_deadline):
                    done, _ = await asyncio.wait(
                        (exit_wait, pressure_wait), return_when=asyncio.FIRST_COMPLETED
                    )
                    if pressure_wait in done and (fd_pressure := pressure_wait.result()):
                        await _terminate(process, process_tree)
                        exit_code = 75
                    else:
                        exit_code = await exit_wait
            except TimeoutError:
                timed_out = True
                await _terminate(process, process_tree)
                exit_code = 124
            except BaseException:
                stdin.close()
                with suppress(BrokenPipeError, ConnectionResetError):
                    await stdin.wait_closed()
                with suppress(BaseException):
                    await _terminate(process, process_tree)
                if not consumer.done():
                    consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
                raise
            finally:
                pending = [
                    task
                    for task in (exit_wait, pressure_wait)
                    if task is not None and not task.done()
                ]
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            if not timed_out:
                # Codex can exit before its MCP/LSP descendants. Reap the complete
                # process tree before integration so no language server retains the
                # overlay or races the coordinator-owned build.
                await _terminate(process, process_tree)
            await consumer
            return exit_code, timed_out, capacity_failure, fd_pressure

        resume_attempt = 0
        capacity_retries = 0
        fd_recycles = 0
        capacity_failure = False
        timed_out = False
        try:
            while True:
                if asyncio.get_running_loop().time() >= attempt_deadline:
                    timed_out = True
                    exit_code = 124
                    break
                if resume_attempt:
                    assert thread_id is not None
                    command = self.command(
                        stage,
                        root,
                        chapter=chapter,
                        feedback=feedback,
                        resume_thread_id=thread_id,
                    )
                    input_text = CAPACITY_RESUME_PROMPT
                else:
                    command = self.command(
                        stage,
                        root,
                        chapter=chapter,
                        feedback=feedback,
                    )
                    input_text = prompt
                exit_code, timed_out, capacity_failure, fd_pressure = await invoke(
                    command, input_text, append_log=bool(resume_attempt)
                )
                if exit_code == 0 or thread_id is None:
                    break
                if fd_pressure:
                    if fd_recycles >= self.config.settings.codex_fd_recycle_attempts:
                        break
                    fd_recycles += 1
                    resume_attempt += 1
                    activity.retry(
                        f"resource recycle {fd_recycles}/"
                        f"{self.config.settings.codex_fd_recycle_attempts}: Codex reached "
                        f"{fd_pressure} open descriptors; resuming {thread_id}"
                    )
                    self.state.activities.save(activity)
                    continue
                if capacity_failure:
                    if capacity_retries >= self.config.settings.capacity_resume_attempts:
                        break
                    capacity_retries += 1
                    resume_attempt += 1
                    activity.retry(
                        f"capacity retry {capacity_retries}/"
                        f"{self.config.settings.capacity_resume_attempts}: resuming {thread_id}"
                    )
                    self.state.activities.save(activity)
                    delay = _capacity_resume_delay(
                        self.config.settings.capacity_resume_delay_seconds,
                        self.config.settings.capacity_resume_max_delay_seconds,
                        capacity_retries,
                    )
                    remaining = attempt_deadline - asyncio.get_running_loop().time()
                    if delay >= remaining:
                        if remaining > 0:
                            await asyncio.sleep(remaining)
                        timed_out = True
                        exit_code = 124
                        break
                    await asyncio.sleep(delay)
                    continue
                break
        except asyncio.CancelledError:
            await stop_usage_monitor()
            activity.finish("cancelled", "agent cancelled by orchestrator")
            self.state.activities.save(activity)
            await self.state.update_run(run, usage=usage)
            raise
        finally:
            await stop_usage_monitor()
        changed = before != scope_digest(root, chapter)
        placeholders = count_placeholders(root, chapter)
        error = "agent timed out" if timed_out else ""
        if fd_pressure and exit_code != 0:
            error = (
                f"Codex descriptor leak persisted after {fd_recycles} resource recycles "
                f"({fd_pressure} open descriptors)"
            )
        if capacity_failure and exit_code != 0:
            error = "Codex capacity retries exhausted"
        elif exit_code != 0 and invocation_error and not timed_out:
            error = invocation_error
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
        result = AgentResult(
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
        if fatal_invocation_failure and exit_code != 0:
            raise FatalCodexInvocationError(error or "Codex rejected the invocation")
        return result


async def validate(
    config: PipelineConfig,
    chapter: WorkUnitLike,
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


def _process_identity(pid: int) -> tuple[int, int, str] | None:
    """Return ``(parent pid, start time, state)`` for a Linux process."""

    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return None
    end = stat.rfind(")")
    if end < 0:
        return None
    fields = stat[end + 2 :].split()
    try:
        return int(fields[1]), int(fields[19]), fields[0]
    except (IndexError, ValueError):
        return None


@dataclass
class _ProcessTreeTracker:
    """Track descendants even when they create new sessions or become orphaned."""

    root_pid: int
    known: dict[int, int] = field(default_factory=dict)

    def scan(self) -> set[int]:
        descendants: set[int] = set()
        # Previously observed children remain traversal roots after reparenting.
        # This lets a surviving code-mode host reveal newly spawned MCP/LSP
        # grandchildren even after the direct Codex process has exited.
        pending = [self.root_pid, *self.known]
        while pending:
            pid = pending.pop()
            if pid in descendants:
                continue
            identity = _process_identity(pid)
            if identity is None:
                continue
            if (known_start := self.known.get(pid)) is not None and identity[1] != known_start:
                continue
            descendants.add(pid)
            self.known[pid] = identity[1]
            try:
                children = Path(f"/proc/{pid}/task/{pid}/children").read_text(encoding="utf-8")
            except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
                continue
            pending.extend(int(child) for child in children.split())
        return descendants

    def live_known(self) -> set[int]:
        live: set[int] = set()
        for pid, started in self.known.items():
            identity = _process_identity(pid)
            if identity is not None and identity[1] == started and identity[2] != "Z":
                live.add(pid)
        return live

    def descriptor_count(self) -> int:
        # Include remembered descendants that detached or were reparented after a
        # previous scan; Codex's code-mode and Lean transports both use setsid().
        processes = self.scan() | self.live_known()
        return sum(_open_descriptor_count(pid) for pid in processes)


def _open_descriptor_count(pid: int) -> int:
    """Read one Linux process's descriptor count without retaining handles."""

    try:
        with os.scandir(f"/proc/{pid}/fd") as entries:
            return sum(1 for _ in entries)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return 0
    except OSError as exc:
        # If even procfs cannot allocate a descriptor, force an immediate recycle
        # instead of treating the failed observation as a healthy count of zero.
        if exc.errno in {errno.EMFILE, errno.ENFILE}:
            return 2**31 - 1
        return 0


async def _wait_for_fd_pressure(
    process: asyncio.subprocess.Process,
    process_tree: _ProcessTreeTracker,
    threshold: int,
    resumable: Callable[[], bool],
) -> int:
    """Return when a Codex process tree approaches descriptor exhaustion."""

    if threshold <= 0:
        await process.wait()
        return 0
    while process.returncode is None:
        count = process_tree.descriptor_count()
        if resumable() and count >= threshold:
            return count
        await asyncio.sleep(1)
    return 0


def _signal_processes(pids: set[int], sig: signal.Signals) -> None:
    for pid in pids:
        with suppress(ProcessLookupError, PermissionError):
            os.kill(pid, sig)


async def _terminate(
    process: asyncio.subprocess.Process,
    process_tree: _ProcessTreeTracker | None = None,
) -> None:
    process_tree = process_tree or _ProcessTreeTracker(process.pid)
    process_tree.scan()
    descendants = process_tree.live_known() - {process.pid}
    _signal_processes(descendants, signal.SIGTERM)
    process_group = process.pid
    with suppress(ProcessLookupError):
        os.killpg(process_group, signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            await process.wait()
    except TimeoutError:
        _signal_processes(process_tree.live_known(), signal.SIGKILL)
        with suppress(ProcessLookupError):
            os.killpg(process_group, signal.SIGKILL)
        await process.wait()

    # Codex's code-mode host, MCP servers, and Lean watchdogs can each call
    # setsid(). Remember their identities before the parent exits so they can be
    # reaped after reparenting instead of escaping a process-group-only kill.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + PROCESS_GROUP_GRACE_SECONDS
    while process_tree.live_known() and loop.time() < deadline:
        await asyncio.sleep(0.05)
    _signal_processes(process_tree.live_known(), signal.SIGKILL)
    deadline = loop.time() + PROCESS_GROUP_GRACE_SECONDS
    while process_tree.live_known() and loop.time() < deadline:
        await asyncio.sleep(0.05)
