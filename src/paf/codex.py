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
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Any, BinaryIO

from paf import json_codec as json
from paf.activity import EVENT_TIMESTAMP_FIELD, activity_timestamp
from paf.backends import LeanBackend
from paf.diagnostics import unexpected_lean_warnings
from paf.hashing import (
    ALGORITHM,
    STABLE_ALGORITHM,
    new_digest,
    stable_digest_bytes,
)
from paf.models import PipelineConfig, ProofObligation, ProofTarget, Stage, WorkUnitLike
from paf.scope import ScopeMatcher
from paf.state import RunRecord, StateStore, TaskStatus, TokenUsage

_REPORT_BASE_PROPERTIES: dict[str, Any] = {
    "complete": {"type": "boolean"},
    "summary": {
        "type": "string",
        "minLength": 1,
        "pattern": "\\S",
        "description": "Self-contained, change-focused prose suitable for a commit body.",
    },
    "issues": {"type": "array", "items": {"type": "string"}},
}

_SOURCE_DEPENDENCIES_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {"type": "string", "minLength": 1},
    "description": "Ids of the earlier chapters directly required by this chapter.",
}

_SOURCE_ISSUES_PROPERTY: dict[str, Any] = {
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
        "required": ["location", "source_excerpt", "description", "suggested_correction"],
    },
}

_FAILED_ATTEMPTS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "declaration": {"type": "string", "minLength": 1},
            "attempts": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "minItems": 2,
            },
            "remaining_goal": {"type": "string", "minLength": 1},
            "obstruction": {"type": "string", "minLength": 1},
            "disposition": {
                "type": "string",
                "enum": [
                    "retry",
                    "missing_upstream",
                    "statement_review",
                    "interface_review",
                    "genuine_blocker",
                ],
            },
        },
        "required": [
            "path",
            "declaration",
            "attempts",
            "remaining_goal",
            "obstruction",
            "disposition",
        ],
    },
}

_BLOCKER_REFS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {"type": "string", "pattern": "^B[0-9]+$"},
    "description": "Durable blocker IDs whose fingerprint and evidence are unchanged.",
}

_UPSTREAM_REQUESTS_PROPERTY: dict[str, Any] = {
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
}

_UPSTREAM_ANSWERS_PROPERTY: dict[str, Any] = {
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
}

_FINDING_ASSESSMENTS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "finding_id": {"type": "string", "minLength": 1},
            "finding": {"type": "string", "minLength": 1},
            "assessment": {
                "type": "string",
                "enum": ["confirmed", "rejected", "reframed"],
            },
            "explanation": {"type": "string", "minLength": 1},
        },
        "required": ["finding_id", "finding", "assessment", "explanation"],
    },
}

_SHEPHERD_DISPOSITIONS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "case_id": {"type": "string", "minLength": 1},
            "disposition": {
                "type": "string",
                "enum": ["repair", "defer", "ignore"],
            },
            "reason": {"type": "string", "minLength": 1},
        },
        "required": ["case_id", "disposition", "reason"],
    },
}

_SHEPHERD_WORK_UNITS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "key": {"type": "string", "minLength": 1, "pattern": "^[a-zA-Z0-9_-]+$"},
            "case_ids": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "minItems": 1,
            },
            "owner_chapter_id": {"type": "string", "minLength": 1},
            "target_stage": {
                "type": "string",
                "enum": [stage.value for stage in Stage],
            },
            "objective": {"type": "string", "minLength": 1},
            "depends_on": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            "effort": {"type": "string", "enum": ["small", "medium", "large"]},
        },
        "required": [
            "key",
            "case_ids",
            "owner_chapter_id",
            "target_stage",
            "objective",
            "depends_on",
            "effort",
        ],
    },
}


def _report_schema(title: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": title,
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(properties),
    }


REPORT_SCHEMAS: dict[str, dict[str, Any]] = {
    "shepherd": _report_schema(
        "PAF Shepherd repair plan",
        {
            "complete": _REPORT_BASE_PROPERTIES["complete"],
            "summary": _REPORT_BASE_PROPERTIES["summary"],
            "issues": _REPORT_BASE_PROPERTIES["issues"],
            "dispositions": _SHEPHERD_DISPOSITIONS_PROPERTY,
            "work_units": _SHEPHERD_WORK_UNITS_PROPERTY,
        },
    ),
    "discover": _report_schema(
        "PAF discovery report",
        _REPORT_BASE_PROPERTIES | {"source_dependencies": _SOURCE_DEPENDENCIES_PROPERTY},
    ),
    "formalize": _report_schema(
        "PAF formalization report",
        _REPORT_BASE_PROPERTIES | {"source_issues": _SOURCE_ISSUES_PROPERTY},
    ),
    "review": _report_schema(
        "PAF statement review report",
        _REPORT_BASE_PROPERTIES | {"source_issues": _SOURCE_ISSUES_PROPERTY},
    ),
    "diagnostic_review": _report_schema(
        "PAF diagnostic repair report",
        _REPORT_BASE_PROPERTIES | {"source_issues": _SOURCE_ISSUES_PROPERTY},
    ),
    "proof_review": _report_schema(
        "PAF failed-proof review report",
        _REPORT_BASE_PROPERTIES
        | {
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "finding_assessments": _FINDING_ASSESSMENTS_PROPERTY,
        },
    ),
    "prove": _report_schema(
        "PAF proof report",
        _REPORT_BASE_PROPERTIES
        | {
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "failed_attempts": _FAILED_ATTEMPTS_PROPERTY,
            "blocker_refs": _BLOCKER_REFS_PROPERTY,
            "upstream_requests": _UPSTREAM_REQUESTS_PROPERTY,
        },
    ),
    "downstream_retry": _report_schema(
        "PAF downstream proof retry report",
        _REPORT_BASE_PROPERTIES
        | {
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "failed_attempts": _FAILED_ATTEMPTS_PROPERTY,
            "blocker_refs": _BLOCKER_REFS_PROPERTY,
            "upstream_requests": _UPSTREAM_REQUESTS_PROPERTY,
        },
    ),
    "upstream_repair": _report_schema(
        "PAF upstream proof repair report",
        _REPORT_BASE_PROPERTIES
        | {
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "failed_attempts": _FAILED_ATTEMPTS_PROPERTY,
            "upstream_answers": _UPSTREAM_ANSWERS_PROPERTY,
        },
    ),
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
PROCESS_EXIT_POLL_SECONDS = 0.005
_PROMPT_RESOURCES = files("paf.prompts")
COMMON_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("common.md")))
PROOF_REVIEW_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("proof_review.md")))
DIAGNOSTIC_REVIEW_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("diagnostic_review.md")))
SHEPHERD_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("shepherd.md")))
UPSTREAM_REPAIR_ROLE = "upstream_repair"
DOWNSTREAM_RETRY_ROLE = "downstream_retry"
SHEPHERD_ROLE = "shepherd"
REPAIR_WORKER_ROLE = "repair_worker"
DIAGNOSTIC_REVIEW_ROLE = "diagnostic_review"
WARNING_REVIEW_ROLE = "warning_review"
DIAGNOSTIC_REVIEW_ROLES = frozenset({DIAGNOSTIC_REVIEW_ROLE, WARNING_REVIEW_ROLE})


def report_schema_key(stage: Stage, *, role: str = "", feedback: str = "") -> str:
    if role == SHEPHERD_ROLE:
        return SHEPHERD_ROLE
    if role == UPSTREAM_REPAIR_ROLE:
        return UPSTREAM_REPAIR_ROLE
    if role == DOWNSTREAM_RETRY_ROLE:
        return DOWNSTREAM_RETRY_ROLE
    if role in DIAGNOSTIC_REVIEW_ROLES:
        return DIAGNOSTIC_REVIEW_ROLE
    if stage is Stage.REVIEW and feedback:
        return "proof_review"
    return stage.value


def render_review_variant(template: str, *, role: str = "") -> str:
    if role == UPSTREAM_REPAIR_ROLE:
        values = {
            "review_assignment": """This is a targeted upstream-support review. One or more later
proofs request reusable mathematics from this earlier chapter. Answer those requests together; do
not audit the rest of the chapter or work on unrelated proof placeholders.""",
            "review_goal_details": """For every request, decide whether the needed result already
exists, naturally belongs in this earlier chapter as a new declaration, or depends on later material
and must remain downstream. Fully prove every declaration you add. This assignment permits no new
placeholders.""",
            "review_workflow_details": """Cover every supplied upstream request ID and only the
earlier-chapter declarations needed to answer it. Group requests that need the same result. For an
existing result, record its exact fully qualified name and concrete use. For a genuine earlier gap,
add and fully prove the smallest reusable result. For a downstream result, explain the ownership
boundary and give a viable later-chapter direction.""",
            "review_guardrails": """Do not change existing interfaces or edit the requesting
chapter. Do not add `sorry`, `admit`, axioms, unsafe declarations, unused helpers, cosmetic aliases,
or a theorem tailored merely to restate a later proof's final goal.""",
            "review_definition_of_done": """Every supplied request ID has one evidence-backed
answer, every added declaration is fully proved and free of diagnostics, all requesting proofs have
concrete usage guidance or a justified downstream disposition, and no unrelated source was
changed.""",
            "review_output_format": """Return the structured report once, after tool use and edits
have stopped. It must describe the stable files on disk, not planned work. Use only these fields:

- `complete`: `true` only when the definition of done is met.
- `summary`: if edits remain, concise past-tense prose naming the added declarations and their
  purpose, suitable for a commit body; otherwise, why no edit was needed.
- `issues`: tooling, diagnostic, or out-of-scope blockers; otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry
  must give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`.
- `failed_attempts`: any new supporting declaration that could not be proved; otherwise an empty
  list. Each entry must give its repository-relative `path`, fully qualified `declaration`, at least
  two meaningfully different checked `attempts`, exact `remaining_goal`, concrete `obstruction`, and
  a `disposition`: `retry`, `missing_upstream`, `statement_review`, `interface_review`, or
  `genuine_blocker`.
- `upstream_answers`: one answer for every supplied request id. Each entry gives `request_ids`, a
  `disposition` of `added`, `existing`, or `downstream`, exact fully qualified `declarations`,
  concrete `usage_guidance`, and a `rejection_reason`. For `downstream`, leave `declarations` empty
  and explain why this earlier chapter is not the right owner. For the other dispositions, leave
  `rejection_reason` empty.""",
        }
    else:
        values = {
            "review_assignment": """This is a full-chapter statement and interface re-review
triggered by failed proof evidence. Review the complete assigned chapter, not only the declarations
named in the supplied findings.""",
            "review_goal_details": """This remains review work, not a second proof attempt. Repair
every genuine statement or supporting-interface problem, but preserve a sound interface when only
the proof strategy failed.""",
            "review_workflow_details": """Cover every numbered source section and every assigned
Lean declaration, followed by a chapter-wide coverage, import, and diagnostic check. For each main
theorem, verify the statement's mathematical meaning and a plausible route through earlier results.
Account for every supplied finding ID as confirmed, rejected, or reframed.""",
            "review_guardrails": """Do not spend the assignment proving existing proposition
placeholders or restrict review to the failed declarations. A no-change review needs no diagnostic
calls only when the handoff contains no PAF build or validation diagnostics. Supplied diagnostics
must all be resolved except for explicitly permitted exact `sorry` warnings.""",
            "review_definition_of_done": """The complete assigned chapter has been re-reviewed,
every supplied finding has an evidence-backed assessment, all warranted in-scope repairs have been
made, established APIs have been reused wherever possible, imports remain chronological, and edited
files have no diagnostics except permitted exact `sorry` warnings.""",
            "review_output_format": """Return the structured report once, after tool use and edits
have stopped. It must describe the stable files on disk, not planned work. Use only these fields:

- `complete`: `true` only when the definition of done is met.
- `summary`: if edits remain, concise past-tense prose naming the main files or declarations and the
  purpose of the edits, suitable for a commit body; otherwise, why no edit was needed.
- `issues`: precise remaining statement, interface, diagnostic, tooling, or out-of-scope blockers;
  otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry
  must give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`.
- `finding_assessments`: one entry for each supplied proof finding. Copy its exact `finding_id`,
  copy a concise identifying `finding`, classify its `assessment` as `confirmed`, `rejected`,
  or `reframed`, and give the evidence in `explanation`.""",
        }
    for key, value in values.items():
        template = template.replace("{" + key + "}", value)
    return template


CAPACITY_RESUME_PROMPT = "Continue from the interrupted turn and complete the assigned task."
UPSTREAM_SOURCE_BUNDLE_MAX_CHARS = 240_000
LEAN_DECLARATION_RE = re.compile(
    r"^[ \t]*(?:(?:noncomputable|private|protected|unsafe|opaque)[ \t]+)*"
    r"(?:theorem|lemma|def|abbrev|structure|class|instance)[ \t]+"
    r"(?P<name>[^\s([{:=]+)",
    re.MULTILINE,
)
LEAN_PROOF_DECLARATION_RE = re.compile(
    r"^[ \t]*(?:(?:noncomputable|private|protected|unsafe|opaque)[ \t]+)*"
    r"(?:(?:theorem|lemma|def|abbrev|structure|class|instance)[ \t]+"
    r"(?P<name>[^\s([{:=]+)|(?P<anonymous>example)\b)",
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


class ValidationStatus(StrEnum):
    CLEAN = "clean"
    DEFERRED = "deferred"
    TARGET_FAILED = "target_failed"
    UPSTREAM_FAILED = "upstream_failed"
    UNATTRIBUTED_BUILD_FAILURE = "unattributed_build_failure"
    STALE_SNAPSHOT = "stale_snapshot"


@dataclass(frozen=True)
class ValidationResult:
    succeeded: bool
    exit_code: int
    output: str
    timed_out: bool = False
    # ``exit_code`` includes PAF's warning policy.  Keep the subprocess exit
    # status separately so a successful Lake batch with one rejected warning
    # can still publish its artifacts and certify unrelated targets.
    process_exit_code: int | None = None
    status: ValidationStatus | None = None
    blocked_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status is None:
            object.__setattr__(
                self,
                "status",
                ValidationStatus.CLEAN if self.succeeded else ValidationStatus.TARGET_FAILED,
            )

    @property
    def compiler_succeeded(self) -> bool:
        code = self.exit_code if self.process_exit_code is None else self.process_exit_code
        return code == 0 and not self.timed_out

    def as_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "exit_code": self.exit_code,
            "output": self.output,
            "timed_out": self.timed_out,
            "process_exit_code": self.process_exit_code,
            "status": self.status,
            "blocked_by": list(self.blocked_by),
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
        "## Evidence supplied by PAF\n\n",
        "The following line-numbered material contains the requests to answer, the relevant files "
        "from this chapter, the later statements that need help, and the related book excerpts. "
        "Read other files only when a focused search requires them.\n",
        "\n### Requests to answer\n",
    ]
    for request in selected:
        request_id = str(request.get("id", "unknown"))
        parts.extend(
            [
                f"\n### Request `{request_id}`\n\n",
                f"- Requesting chapter: `{request.get('consumer_chapter_id', '')}`\n",
                f"- Blocked declaration: `{request.get('blocked_declaration', '')}`\n",
                f"- Requesting file: `{request.get('consumer_path', '')}`\n",
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
            parts.extend(["- Earlier proof attempts:\n\n", "```text\n", previous, "\n```\n"])

    # Keep the exact requesting declarations ahead of potentially large textbook and owner-source
    # excerpts so the bounded evidence packet cannot truncate the statements the repair must serve.
    parts.append("\n### Relevant declarations from the requesting chapters\n")
    for request in selected:
        relative = str(request.get("consumer_path", ""))
        declaration = str(request.get("blocked_declaration", ""))
        excerpt = _declaration_excerpt(repo / relative, declaration)
        parts.append(f"\n### `{relative}` — `{declaration}`\n\n")
        if excerpt is None:
            parts.append("[The named declaration from the requesting chapter was not found.]\n")
            continue
        start, lines = excerpt
        parts.append(_line_numbered(lines, start=start))

    excerpt_chapters = {owner.id}
    excerpt_chapters.update(str(request.get("consumer_chapter_id", "")) for request in selected)
    parts.append("\n### Relevant book excerpts\n")
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
    parts.append("\n### Files in this earlier chapter\n")
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
    marker = f"\n[Earlier-chapter repair evidence truncated at {maximum:,} characters.]\n"
    if len(marker) >= maximum:
        return marker[:maximum]
    return bundle[: maximum - len(marker)] + marker


def scope_digest(repo: Path, chapter: WorkUnitLike) -> str:
    digest = new_digest()
    for path in scoped_files(repo, chapter):
        digest.update(path.relative_to(repo).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"{ALGORITHM}:{digest.hexdigest()}"


def migrate_scope_digests(
    repo: Path,
    chapter: WorkUnitLike,
    stored: Iterable[str],
) -> dict[str, str]:
    """Verify old scope digests and return their canonical XXH replacements."""

    values = set(stored)
    if not values:
        return {}
    current = new_digest()
    needs_legacy = any(
        not value.startswith(f"{ALGORITHM}:") and len(value) != 16 for value in values
    )
    legacy = hashlib.sha256() if needs_legacy else None
    for path in scoped_files(repo, chapter):
        chunks = (
            path.relative_to(repo).as_posix().encode(),
            b"\0",
            path.read_bytes(),
            b"\0",
        )
        for chunk in chunks:
            current.update(chunk)
            if legacy is not None:
                legacy.update(chunk)
    current_raw = current.hexdigest()
    canonical = f"{ALGORITHM}:{current_raw}"
    compatible = {canonical, current_raw}
    if legacy is not None:
        legacy_raw = legacy.hexdigest()
        compatible.update({legacy_raw, f"{STABLE_ALGORITHM}:{legacy_raw}"})
    return {value: canonical for value in values if value in compatible}


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


def _placeholder_offsets(text: str) -> tuple[int, ...]:
    """Return source offsets of Lean placeholders, ignoring comments and strings."""

    offsets: list[int] = []
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
            continue
        if pair == "/-":
            block_depth = 1
            index += 2
            continue
        if char == '"':
            in_string = True
            index += 1
            continue
        match = re.match(r"(?:sorry|admit)\b", text[index:])
        if match and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_")):
            offsets.append(index)
            index += len(match.group(0))
            continue
        index += 1
    return tuple(offsets)


def _placeholder_context(text: str, offset: int) -> str:
    """Return a compact source excerpt containing one proof hole.

    The excerpt must be anchored to the placeholder itself.  Looking backwards for a
    nonempty line is tempting for standalone ``sorry`` terms, but it can attach a hole
    to an unrelated declaration line or even to doc-comment punctuation.
    """

    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    if line_end < 0:
        line_end = len(text)
    raw_line = text[line_start:line_end]
    leading_whitespace = len(raw_line) - len(raw_line.lstrip())
    line = raw_line.strip()
    if len(line) <= 160:
        return line or text[offset : offset + 160].strip() or "proof obligation"

    # Keep the placeholder visible when it occurs on an unusually long source line.
    column = max(0, offset - line_start - leading_whitespace)
    window_start = max(0, min(column - 70, len(line) - 157))
    excerpt = line[window_start : window_start + 157]
    return ("…" if window_start else "") + excerpt + ("…" if window_start + 157 < len(line) else "")


def _placeholder_signature(text: str, offset: int) -> str:
    """Describe a hole from nearby source so its identity survives other holes being solved."""

    before = _placeholder_context(text, offset)
    after = ""
    for line in text[offset:].splitlines()[1:7]:
        stripped = line.strip()
        if stripped and not stripped.startswith(("/-", "--", "*")):
            after = stripped[:160]
            break
    return f"{before}\0{after}"


def count_placeholders(repo: Path, chapter: WorkUnitLike) -> int:
    pattern = re.compile(r"\b(?:sorry|admit)\b")
    return sum(
        len(pattern.findall(_lean_code(path.read_text(encoding="utf-8"))))
        for path in scoped_files(repo, chapter)
    )


def _proof_declarations(repo: Path, chapter: WorkUnitLike) -> tuple[ProofTarget, ...]:
    """Return every proof-capable declaration with its current source span."""

    declarations: list[ProofTarget] = []
    for path in scoped_files(repo, chapter):
        text = path.read_text(encoding="utf-8")
        matches = list(LEAN_PROOF_DECLARATION_RE.finditer(text))
        name_ordinals: dict[str, int] = {}
        for index, match in enumerate(matches):
            declaration = match.group("name") or "example"
            ordinal = name_ordinals.get(declaration, 0)
            name_ordinals[declaration] = ordinal + 1
            stop = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            start_line = text.count("\n", 0, match.start()) + 1
            end_line = (
                text.count("\n", 0, stop)
                if stop > 0 and text[stop - 1] == "\n"
                else text.count("\n", 0, stop) + 1
            )
            declaration_text = text[match.start() : stop]
            placeholder_offsets = _placeholder_offsets(declaration_text)
            placeholder_count = len(placeholder_offsets)
            display_name = declaration if match.group("name") else f"example #{ordinal + 1}"
            relative = path.relative_to(repo).as_posix()
            identity = f"{relative}\0{declaration}\0{ordinal}".encode()
            target_fingerprint = stable_digest_bytes(identity)[:16]
            signature_ordinals: dict[str, int] = {}
            obligation_specs: list[tuple[int, str, int]] = []
            for offset in placeholder_offsets:
                signature = _placeholder_signature(declaration_text, offset)
                signature_ordinal = signature_ordinals.get(signature, 0)
                signature_ordinals[signature] = signature_ordinal + 1
                obligation_specs.append((offset, signature, signature_ordinal))
            obligations = tuple(
                ProofObligation(
                    ordinal=obligation_index,
                    line=text.count("\n", 0, match.start() + offset) + 1,
                    context=_placeholder_context(declaration_text, offset),
                    fingerprint=stable_digest_bytes(
                        f"{target_fingerprint}\0{signature}\0{signature_ordinal}".encode()
                    )[:16],
                )
                for obligation_index, (offset, signature, signature_ordinal) in enumerate(
                    obligation_specs, start=1
                )
            )
            declarations.append(
                ProofTarget(
                    path=relative,
                    declaration=display_name,
                    line=start_line,
                    end_line=max(start_line, end_line),
                    placeholder_count=placeholder_count,
                    fingerprint=target_fingerprint,
                    obligations=obligations,
                )
            )
    return tuple(declarations)


def proof_targets(repo: Path, chapter: WorkUnitLike) -> tuple[ProofTarget, ...]:
    """Return unresolved declarations in stable source order.

    A declaration is the smallest safe proof assignment: placeholders within one declaration
    often depend on local terms and must stay with the same agent. The ordinal disambiguates equal
    short names in different namespaces without making the fingerprint sensitive to line movement.
    """

    return tuple(
        declaration
        for declaration in _proof_declarations(repo, chapter)
        if declaration.placeholder_count
    )


def proof_target_spans(
    repo: Path,
    chapter: WorkUnitLike,
    targets: Iterable[ProofTarget],
) -> tuple[ProofTarget, ...]:
    """Refresh assigned declaration spans after an agent may have moved or expanded them."""

    current = {
        declaration.fingerprint: declaration for declaration in _proof_declarations(repo, chapter)
    }
    return tuple(current.get(target.fingerprint, target) for target in targets)


def proof_target_chunk(
    targets: Iterable[ProofTarget],
    chunk_size: int,
) -> tuple[ProofTarget, ...]:
    """Select the next source-ordered chunk without splitting a declaration."""

    selected: list[ProofTarget] = []
    assigned = 0
    for target in targets:
        if selected and assigned + target.placeholder_count > chunk_size:
            break
        selected.append(target)
        assigned += target.placeholder_count
        if assigned >= chunk_size:
            break
    return tuple(selected)


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
            key in value for key in ("complete", "summary", "issues")
        ):
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


def _record_jsonl_line(
    log: BinaryIO,
    line: bytes,
    *,
    terminated: bool,
) -> tuple[Any, str | None]:
    """Decode, timestamp, and record one event outside the asyncio loop."""

    try:
        event = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        received_at = activity_timestamp()
        event = {
            "type": "paf.raw_output",
            "text": line.decode("utf-8", errors="replace"),
            "terminated": terminated,
            EVENT_TIMESTAMP_FIELD: received_at,
        }
        log.write(json.dumpb(event) + b"\n")
        return event, received_at
    received_at = activity_timestamp()
    if isinstance(event, dict):
        event = {**event, EVENT_TIMESTAMP_FIELD: received_at}
        persisted = event
        item = event.get("item")
        if isinstance(item, dict) and item.get("type") == "mcp_tool_call":
            result = item.get("result")
            if isinstance(result, dict) and result.get("structured_content") is not None:
                # FastMCP mirrors structured results into a JSON text content
                # block. Codex has already delivered both forms to the agent;
                # retaining both nearly doubles every PAF transcript.
                persisted = {
                    **event,
                    "item": {
                        **item,
                        "result": {key: value for key, value in result.items() if key != "content"},
                    },
                }
        log.write(json.dumpb(persisted) + b"\n")
    else:
        log.write(json.dumpb(event) + b"\n")
    return event, received_at


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
    def __init__(
        self,
        config: PipelineConfig,
        state: StateStore,
        *,
        resume_agents: bool = False,
    ) -> None:
        self.config = config
        self.state = state
        self.resume_agents = resume_agents
        self.schema_paths = {
            key: config.settings.state_dir / f"agent-report-{key}.schema.json"
            for key in REPORT_SCHEMAS
        }

    def _resumable_run(
        self,
        run: RunRecord,
        stage: Stage,
    ) -> RunRecord | None:
        runs = self.state.task(run.chapter_id, stage).runs
        prior = runs[-2] if len(runs) >= 2 and runs[-1].id == run.id else None
        return (
            prior
            if prior is not None
            and prior.status == TaskStatus.INTERRUPTED
            and prior.thread_id
            and (prior.role or prior.stage) == (run.role or run.stage)
            and prior.request_ids == run.request_ids
            and prior.proof_targets == run.proof_targets
            and (not prior.prompt_kind or prior.prompt_kind == run.prompt_kind)
            else None
        )

    async def prepare(self) -> None:
        self.config.settings.state_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = self.config.settings.state_dir / "agent-report.schema.json"
        legacy_path.unlink(missing_ok=True)
        for key, schema in REPORT_SCHEMAS.items():
            self.schema_paths[key].write_text(json.dumps(schema, indent=2), encoding="utf-8")

    def build_prompt(
        self,
        chapter: Any,
        stage: Stage,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
        role: str = "",
        upstream_requests: Iterable[dict[str, Any]] = (),
        proof_targets: Iterable[ProofTarget | dict[str, Any]] = (),
    ) -> str:
        if role in DIAGNOSTIC_REVIEW_ROLES:
            prompt_path = DIAGNOSTIC_REVIEW_PROMPT_PATH
        elif role == UPSTREAM_REPAIR_ROLE or (stage is Stage.REVIEW and feedback):
            prompt_path = PROOF_REVIEW_PROMPT_PATH
        else:
            prompt_path = self.config.stages[stage].prompt
        template = prompt_path.read_text(encoding="utf-8")
        if prompt_path == PROOF_REVIEW_PROMPT_PATH:
            template = render_review_variant(template, role=role)
        elif prompt_path == DIAGNOSTIC_REVIEW_PROMPT_PATH:
            diagnostic_trigger = (
                "The Lean build completed, but PAF rejected one or more non-`sorry` warnings. "
                "The build did not fail and this is not failed proof evidence."
                if role == WARNING_REVIEW_ROLE
                else "The coordinator build reported Lean errors, possibly together with "
                "non-`sorry` warnings."
            )
            template = template.replace("{diagnostic_trigger}", diagnostic_trigger)
        base = render_prompt(template, chapter)
        if role == REPAIR_WORKER_ROLE:
            instruction = feedback
            try:
                repair_dossier = json.loads(feedback)
            except json.JSONDecodeError:
                repair_dossier = None
            if isinstance(repair_dossier, dict) and isinstance(
                repair_dossier.get("objective"), str
            ):
                instruction = repair_dossier["objective"].strip()
            instruction = instruction.strip() or (
                f"Diagnose and repair the reported {stage.value} stage failure."
            )
            base = f"""# Shepherd repair agent

You are a repair agent assigned to fix a failed `{stage.value}` stage. The repair instruction is
the immediate task. The original prompt for the stage appears below it and remains the contract for
how to perform and report the work.

## Repair instruction

{instruction}

## Original prompt for the `{stage.value}` stage

{base}"""
        common = (
            ""
            if stage in (Stage.DISCOVER, Stage.PROVE)
            else render_prompt(COMMON_PROMPT_PATH.read_text(encoding="utf-8"), chapter)
        )
        scope = "\n".join(f"- `{item}`" for item in chapter.scope)
        input_catalog = ""
        if stage is Stage.DISCOVER:
            previous_units: list[Any] = []
            for unit in self.config.work_units:
                if unit.id == chapter.id:
                    break
                previous_units.append(unit)
            entries = "\n".join(
                f"- `{unit.id}` — {unit.title} "
                f"({unit.source.as_posix()}:{unit.source_span.start_line}-"
                f"{unit.source_span.end_line})"
                for unit in previous_units
            )
            entries = entries or "No earlier chapters are available."
            input_catalog = f"\n### Available chapters and ids\n\n{entries}\n"
        proof_retry_contract = ""
        if stage is Stage.PROVE and feedback and role != UPSTREAM_REPAIR_ROLE:
            proof_retry_contract = """
This is a retry. Use the target-specific handoff below to improve a prior approach or try a
meaningfully different one. Resolve supplied current diagnostics and do not restate unchanged
blocker evidence."""
        selected_proof_targets = tuple(proof_targets)
        proof_assignment = ""
        if stage is Stage.PROVE and selected_proof_targets and role != UPSTREAM_REPAIR_ROLE:
            rendered_targets: list[str] = []
            assigned_placeholders = 0
            for target in selected_proof_targets:
                value = target.as_dict() if isinstance(target, ProofTarget) else target
                count = int(value.get("placeholder_count", 0))
                assigned_placeholders += count
                obligations = value.get("obligations", [])
                rendered_targets.extend(
                    [
                        f"### `{value.get('declaration', '')}`",
                        "",
                        f"- File: `{value.get('path', '')}`",
                        f"- Declaration span: lines {value.get('line', '')}-"
                        f"{value.get('end_line', '')}",
                        f"- Target ID: `{value.get('fingerprint', '')}`",
                        "- Proof holes:",
                    ]
                )
                if isinstance(obligations, list) and obligations:
                    for index, obligation in enumerate(obligations, start=1):
                        if not isinstance(obligation, dict):
                            continue
                        rendered_targets.append(
                            f"  - H{obligation.get('ordinal', index)} at line "
                            f"{obligation.get('line', '')}: "
                            f"`{obligation.get('context', 'proof obligation')}` "
                            f"(`{obligation.get('fingerprint', '')}`)"
                        )
                else:
                    rendered_targets.append(
                        f"  - {count} hole location(s) unavailable in this legacy assignment; "
                        "locate them before editing."
                    )
                rendered_targets.append("")
            root = workspace_root or self.config.settings.repo
            try:
                all_targets = _proof_declarations(root, chapter)
            except OSError:
                all_targets = ()
            selected_ids = {
                str(
                    (target.as_dict() if isinstance(target, ProofTarget) else target).get(
                        "fingerprint", ""
                    )
                )
                for target in selected_proof_targets
            }
            reserved = tuple(
                target
                for target in all_targets
                if target.placeholder_count and target.fingerprint not in selected_ids
            )
            reservation = (
                "There are no unassigned placeholders in this chapter."
                if not reserved
                else "Reserved for later agents: "
                + ", ".join(f"`{target.declaration}`" for target in reserved)
                + "."
            )
            declaration_count = len(selected_proof_targets)
            declaration_label = "declaration" if declaration_count == 1 else "declarations"
            hole_label = "proof hole" if assigned_placeholders == 1 else "proof holes"
            chunk_size = self.config.stages[Stage.PROVE].chunk_size or 4
            overflow = (
                f" The configured chunk size is {chunk_size}, but PAF keeps all holes in one "
                "declaration with the same agent because they can share local terms."
                if declaration_count == 1 and assigned_placeholders > chunk_size
                else ""
            )
            proof_assignment = f"""## Authoritative assignment

This attempt owns exactly {declaration_count} {declaration_label} containing
{assigned_placeholders} {hole_label}.{overflow}

{chr(10).join(rendered_targets).rstrip()}

{reservation}

Work only on the assigned declaration bodies and focused private helpers they require. Resolve every
listed hole and every diagnostic in the assigned span. Set `complete` to `true` only when all listed
holes are gone and no non-`sorry` diagnostic remains.
"""
            workflow_heading = "\n## Working method\n"
            if workflow_heading in base:
                base = base.replace(
                    workflow_heading,
                    f"\n{proof_assignment}\n## Working method\n",
                    1,
                )
            else:
                base = f"{base.rstrip()}\n\n{proof_assignment}"
        stage_contract = {
            Stage.DISCOVER: """This is read-only source analysis. Identify the earlier chapters
that this chapter directly needs. Do not edit any file.""",
            Stage.FORMALIZE: """This attempt is responsible for accurately translating the chapter
into Lean and leaving it free of diagnostics other than permitted `sorry` warnings. The earlier
chapters it needs are already clean. Other independent chapters may be formalized at the same time.
PAF will run the authoritative build after your work.""",
            Stage.REVIEW: """Review the entire assigned chapter and make every warranted statement
or interface change that belongs in its files. When proof findings are attached, evaluate them
independently while still reviewing the complete chapter. Preserve proof placeholders and do not
spend time proving propositions. PAF has already built the incoming files unless validation
diagnostics supplied below say otherwise; those diagnostics describe the current source and override
the earlier clean-build fact. PAF will rebuild any changes.""",
            Stage.PROVE: """The assigned chapter passed review and was clean before proof work
began. Work directly on the listed holes rather than auditing untouched files. The assigned span
must finish without errors or warnings. PAF will build the chapter after the attempt; supplied
current diagnostics override the earlier clean-build fact."""
            + proof_retry_contract,
        }[stage]
        if role == UPSTREAM_REPAIR_ROLE:
            stage_contract = """This temporary attempt answers a group of requests for mathematical
support from an earlier chapter. Use the attached Lean tools, edit only that earlier chapter, and
fully prove every new declaration. PAF will merge and build the changes before retrying the later
            proofs. Do not work on unrelated placeholders."""
        elif role == REPAIR_WORKER_ROLE:
            stage_contract = f"""This is a bounded Shepherd repair work unit targeting the existing
{stage.value} stage. Diagnose and fix the concrete blocker in the attached repair dossier. Keep the
change as small as possible, do not broaden into unrelated cleanup, and satisfy the ordinary
{stage.value} stage contract. PAF will independently validate and integrate the result."""
        validation_contract = {
            Stage.DISCOVER: "PAF validates and saves the reported source dependencies.",
            Stage.FORMALIZE: "PAF independently checks the allowed file changes, placeholders, "
            "diagnostics, and the dependency-ordered build after applying the edits.",
            Stage.REVIEW: "PAF independently checks the allowed file changes, placeholders, and "
            "the chapter build after applying the edits.",
            Stage.PROVE: "PAF independently checks the allowed file changes, placeholders, "
            "diagnostics, and the chapter build.",
        }[stage]
        file_contract = (
            """### Files you may edit

None. Discovery is strictly read-only: do not create, modify, move, delete, format, or otherwise
write any file."""
            if stage is Stage.DISCOVER
            else f"""### Files you may edit

You may edit only these paths:
{scope}

This boundary is strict: reads elsewhere are allowed, but any write outside these paths rejects the
attempt. Report a required out-of-scope repair instead of making it."""
        )
        contract = f"""

## PAF requirements

{file_contract}

Do not commit, wait for another worker, invoke a compiler directly, or inspect `.paf` logs or
isolation trees. Keep command output below roughly 12 KiB. {stage_contract}
{input_catalog}
{validation_contract}
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
                Stage.FORMALIZE: """Using a Lean tool opens and synchronizes the file it targets.
Follow the formalization workflow above for when and where to request diagnostics.""",
                Stage.REVIEW: """Using a Lean tool opens and synchronizes the file it targets.
Follow the review workflow above for when and where to request diagnostics.""",
                Stage.PROVE: """Using a Lean tool opens and synchronizes the file it targets. Do not
request diagnostics merely because you switched files. After editing, use goals and fresh
diagnostics as needed to show that the assigned span has no errors or warnings.""",
            }[stage]
            contract += f"""

### Attached Lean tools (MCP)

A private `paf_lean` server provides {capabilities}. Paths are relative to the Lean project root:
use `LastLib/...`, not `lean/LastLib/...`.
{mcp_workflow}
The server prepares stale imports automatically. For multiple edited files, call
`lean_prepare_dependencies` once on the files at the end of the dependency chain, then request final
diagnostics from prerequisites to dependents.
"""
        if feedback:
            feedback_heading = (
                "Targeted downstream retry handoff"
                if role == DOWNSTREAM_RETRY_ROLE
                else "Shepherd repair dossier"
                if role == REPAIR_WORKER_ROLE
                else "PAF validation diagnostics to repair"
                if role in DIAGNOSTIC_REVIEW_ROLES
                else {
                    Stage.DISCOVER: "Discovery feedback",
                    Stage.FORMALIZE: "PAF build diagnostics and reported findings",
                    Stage.REVIEW: "Proof findings and PAF validation diagnostics",
                    Stage.PROVE: "Earlier proof attempts and PAF validation diagnostics",
                }[stage]
            )
            contract += f"\n## {feedback_heading}\n\n```text\n{_bounded_feedback(feedback)}\n```\n"
        evidence = ""
        if role == UPSTREAM_REPAIR_ROLE:
            root = workspace_root or self.config.settings.repo
            evidence = (
                _upstream_source_bundle(
                    root,
                    chapter,
                    upstream_requests,
                    self.config.chapters,
                ).rstrip()
                + "\n\n"
            )
        return f"{base.rstrip()}\n\n{evidence}{common.rstrip()}\n{contract}"

    def build_shepherd_prompt(
        self,
        failures: Iterable[dict[str, Any]],
        *,
        scheduling: dict[str, Any],
    ) -> str:
        template = SHEPHERD_PROMPT_PATH.read_text(encoding="utf-8")
        dossier = {
            "failures": list(failures),
            "scheduling": scheduling,
            "limits": {
                "maximum_work_units": self.config.shepherd.maximum_work_units_per_sweep,
                "allowed_chapter_ids": [unit.id for unit in self.config.work_units],
                "allowed_stages": [stage.value for stage in Stage],
            },
        }
        payload = json.dumps(dossier, indent=2)
        return f"{template.rstrip()}\n\n## Failure dossier\n\n```json\n{payload}\n```\n"

    def command(
        self,
        stage: Stage,
        workspace_root: Path | None = None,
        *,
        chapter: WorkUnitLike | None = None,
        feedback: str = "",
        role: str = "",
        resume_thread_id: str | None = None,
    ) -> list[str]:
        settings = self.config.settings
        root = workspace_root or settings.repo
        command = [settings.codex_bin, "exec"]
        if resume_thread_id is None:
            command.extend(["--json", "--color", "never", "--cd", str(root)])
        else:
            command.extend(["resume", "--json"])
        schema_key = report_schema_key(stage, role=role, feedback=feedback)
        command.extend(["--output-schema", str(self.schema_paths[schema_key])])
        if root != settings.repo:
            command.append("--skip-git-repo-check")
        if role == SHEPHERD_ROLE and resume_thread_id is None:
            command.extend(["--sandbox", "read-only"])
        elif role == SHEPHERD_ROLE:
            command.extend(["--config", 'sandbox_mode="read-only"'])
        elif settings.bypass_approvals_and_sandbox:
            command.append("--dangerously-bypass-approvals-and-sandbox")
        elif settings.approve_for_me and resume_thread_id is None:
            # Current Codex versions make --approve-for-me mutually exclusive
            # with --sandbox; approve-for-me itself selects workspace-write.
            command.append("--approve-for-me")
        elif resume_thread_id is None:
            command.extend(["--sandbox", settings.sandbox])
        else:
            # `codex exec resume` does not accept the top-level `--sandbox`
            # option, but it does accept the equivalent config override.
            command.extend(["--config", f'sandbox_mode="{settings.sandbox}"'])
        if role == SHEPHERD_ROLE:
            model = self.config.shepherd.model
            reasoning_effort = self.config.shepherd.reasoning_effort
        elif role == REPAIR_WORKER_ROLE:
            model = self.config.shepherd.worker_model
            reasoning_effort = self.config.shepherd.worker_reasoning_effort
        else:
            model = self.config.model_for(stage)
            reasoning_effort = self.config.reasoning_effort_for(stage)
        if model:
            command.extend(["--model", model])
        if reasoning_effort:
            command.extend(["--config", f'model_reasoning_effort="{reasoning_effort}"'])
        if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            backend = self.config.backend or LeanBackend(
                project=settings.lean_project,
                mcp_tool_timeout_seconds=settings.lean_mcp_tool_timeout_seconds,
            )
            tool_stage = Stage.PROVE if role in DIAGNOSTIC_REVIEW_ROLES else stage
            mcp_config = backend.mcp_config(root, tool_stage)
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
        prompt = self.build_prompt(
            chapter,
            stage,
            feedback=feedback,
            workspace_root=root,
            role=run.role,
            proof_targets=run.proof_targets,
        )
        return await self._run_prompt(
            chapter,
            stage,
            run,
            prompt=prompt,
            feedback=feedback,
            workspace_root=root,
        )

    async def resume(
        self,
        chapter: Any,
        stage: Stage,
        run: RunRecord,
        *,
        thread_id: str,
        previous_run_id: str,
        reminder: str,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        """Continue an explicitly selected Codex session with a focused reminder."""

        root = workspace_root or self.config.settings.repo
        prompt = self.build_prompt(
            chapter,
            stage,
            feedback=feedback,
            workspace_root=root,
            role=run.role,
            proof_targets=run.proof_targets,
        )
        return await self._run_prompt(
            chapter,
            stage,
            run,
            prompt=prompt,
            feedback=feedback,
            workspace_root=root,
            resume_thread_id=thread_id,
            resume_run_id=previous_run_id,
            resume_prompt=reminder,
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

    async def run_shepherd(
        self,
        anchor: WorkUnitLike,
        run: RunRecord,
        failures: Iterable[dict[str, Any]],
        *,
        scheduling: dict[str, Any],
    ) -> AgentResult:
        """Ask the strong, read-only Shepherd model for a bounded repair DAG."""

        prompt = self.build_shepherd_prompt(failures, scheduling=scheduling)
        return await self._run_prompt(
            anchor,
            Stage.DISCOVER,
            run,
            prompt=prompt,
            workspace_root=self.config.settings.repo,
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
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = CAPACITY_RESUME_PROMPT,
    ) -> AgentResult:
        root = workspace_root or self.config.settings.repo
        prompt_path = self.state.logs_dir / f"{run.id}.prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        before = await asyncio.to_thread(scope_digest, root, chapter)
        log_path = self.state.logs_dir / f"{run.id}.jsonl"
        usage = TokenUsage()
        cumulative_usage = TokenUsage()
        usage_baseline: TokenUsage | None = None
        report: dict[str, Any] = {}
        resumable_run = self._resumable_run(run, stage)
        thread_id = resume_thread_id
        if thread_id is None and self.resume_agents and resumable_run:
            thread_id = resumable_run.thread_id
        interrupted_resume = thread_id is not None
        if thread_id is not None:
            await self.state.update_run(
                run,
                thread_id=thread_id,
                resumed_from_run_id=(
                    resume_run_id or (resumable_run.id if resumable_run is not None else "")
                ),
            )
        invocation_error = ""
        fatal_invocation_failure = False
        activity = await self.state.activities.start_async(
            run.id, chapter.id, run.role or stage.value
        )
        usage_stop = asyncio.Event()
        usage_monitor: asyncio.Task[None] | None = None
        attempt_deadline = (
            asyncio.get_running_loop().time() + self.config.settings.agent_timeout_seconds
        )

        async def update_usage(found: TokenUsage) -> None:
            nonlocal usage, cumulative_usage, usage_baseline
            if found.total_tokens < cumulative_usage.total_tokens:
                return
            cumulative_usage = found
            if thread_id is not None:
                if usage_baseline is None:
                    usage_baseline = self.state.thread_cumulative_usage.get(thread_id, TokenUsage())
                usage = found.delta_from(usage_baseline)
                await self.state.record_thread_cumulative_usage(thread_id, found, deferred=True)
            else:
                usage = found
            # Live UI reads the in-memory record. Let another state transition or
            # the final run flush batch these high-frequency rollout updates.
            await self.state.update_run(
                run,
                usage=usage,
                cumulative_usage=found,
                deferred=True,
            )

        async def stop_usage_monitor() -> None:
            usage_stop.set()
            if usage_monitor is not None:
                await usage_monitor

        async def invoke(
            command: list[str], input_text: str, *, append_log: bool
        ) -> tuple[int, bool, bool, int]:
            nonlocal usage, report, thread_id, usage_monitor, usage_baseline
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
                        nonlocal usage_baseline
                        nonlocal invocation_error, fatal_invocation_failure
                        recording = asyncio.create_task(
                            asyncio.to_thread(
                                _record_jsonl_line,
                                log,
                                line,
                                terminated=terminated,
                            )
                        )
                        try:
                            event, received_at = await asyncio.shield(recording)
                        except asyncio.CancelledError:
                            # Do not close the log while its worker thread may
                            # still be serializing or writing this record.
                            await recording
                            raise
                        if received_at is None:
                            return
                        activity.consume(event, workspace_root=root, at=received_at)
                        await self.state.activities.save_throttled_async(activity)
                        capacity_failure = capacity_failure or _is_capacity_failure(event)
                        fatal_invocation_failure = (
                            fatal_invocation_failure or _is_fatal_invocation_failure(event)
                        )
                        if found_error := _event_error_message(event):
                            invocation_error = found_error
                        if found := _find_thread_id(event):
                            thread_id = found
                            usage_baseline = self.state.thread_cumulative_usage.get(
                                found, TokenUsage()
                            )
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
        invocation_count = 0
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
                if interrupted_resume or resume_attempt:
                    assert thread_id is not None
                    command = self.command(
                        stage,
                        root,
                        chapter=chapter,
                        feedback=feedback,
                        role=run.role,
                        resume_thread_id=thread_id,
                    )
                    input_text = resume_prompt if invocation_count == 0 else CAPACITY_RESUME_PROMPT
                else:
                    command = self.command(
                        stage,
                        root,
                        chapter=chapter,
                        feedback=feedback,
                        role=run.role,
                    )
                    input_text = prompt
                exit_code, timed_out, capacity_failure, fd_pressure = await invoke(
                    command, input_text, append_log=bool(invocation_count)
                )
                invocation_count += 1
                if interrupted_resume:
                    interrupted_resume = False
                    if exit_code != 0 and not timed_out:
                        activity.retry(
                            f"could not resume Codex session {thread_id}; starting a new agent"
                        )
                        await self.state.activities.save_async(activity)
                        thread_id = None
                        report = {}
                        invocation_error = ""
                        fatal_invocation_failure = False
                        capacity_failure = False
                        await self.state.update_run(run, thread_id=None)
                        continue
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
                    await self.state.activities.save_async(activity)
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
                    await self.state.activities.save_async(activity)
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
            await self.state.activities.save_async(activity)
            await self.state.finish_run(
                run,
                status=TaskStatus.INTERRUPTED,
                usage=usage,
                thread_id=thread_id,
            )
            raise
        finally:
            await stop_usage_monitor()
        after, placeholders = await asyncio.to_thread(
            lambda: (scope_digest(root, chapter), count_placeholders(root, chapter))
        )
        changed = before != after
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
        await self.state.activities.save_async(activity)
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED if succeeded else TaskStatus.FAILED,
            exit_code=exit_code,
            changed=changed,
            placeholders=placeholders,
            report=report or None,
            usage=usage,
            thread_id=thread_id,
            source_digest=after,
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
        process_exit_code = process.returncode or 0
        timed_out = False
    except TimeoutError:
        await _terminate(process)
        timeout_message = b"validation timed out"
        output_parts.append(timeout_message)
        if on_output is not None:
            on_output(timeout_message.decode())
        process_exit_code = 124
        timed_out = True
    except asyncio.CancelledError:
        await _terminate(process)
        raise
    output_bytes = b"".join(output_parts)
    complete_output = output_bytes.decode(errors="replace")
    warnings = unexpected_lean_warnings(complete_output)
    exit_code = process_exit_code
    output = complete_output[-20000:]
    if warnings:
        warning_summary = "\n".join(warnings[-50:])
        output = (
            f"{output}\n\nCoordinator rejected {len(warnings)} non-sorry Lean warning(s):\n"
            f"{warning_summary}"
        )[-20000:]
        if exit_code == 0:
            exit_code = 1
    return ValidationResult(
        exit_code == 0 and not warnings,
        exit_code,
        output,
        timed_out,
        process_exit_code,
    )


async def _wait_for_parent_exit(process: asyncio.subprocess.Process) -> int:
    """Wait for the direct child without waiting for descendant-held pipes.

    ``Process.wait`` may not resolve until inherited stdout descriptors close.
    Polling ``returncode`` observes the child watcher immediately, allowing the
    caller to reap an exited Codex process's surviving MCP/LSP process group.
    """

    while process.returncode is None:
        await asyncio.sleep(PROCESS_EXIT_POLL_SECONDS)
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
        # ``scan`` already starts from every remembered PID and validates its
        # identity, so a second ``live_known`` pass only rereads every proc stat.
        processes = self.scan()
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
        # Trees far below the limit cannot reach it without opening hundreds of
        # descriptors. Poll them less aggressively; large swarms otherwise walk
        # the same procfs process trees once per agent per second.
        interval = 1 if count >= threshold // 2 else 3
        await asyncio.sleep(interval)
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
            # ``Process.wait()`` can wait for descendant-held stdout pipes even
            # after the direct child has exited.  Observe the child watcher
            # instead so detached MCP servers cannot consume the entire
            # termination timeout.
            await _wait_for_parent_exit(process)
    except TimeoutError:
        _signal_processes(process_tree.live_known(), signal.SIGKILL)
        with suppress(ProcessLookupError):
            os.killpg(process_group, signal.SIGKILL)
        await _wait_for_parent_exit(process)

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
