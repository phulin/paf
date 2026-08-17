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
from typing import Any, BinaryIO

from paf import json_codec as json
from paf.activity import EVENT_TIMESTAMP_FIELD, activity_timestamp
from paf.backends import LeanBackend
from paf.diagnostics import unexpected_lean_warnings
from paf.models import PipelineConfig, Stage, WorkUnitLike
from paf.scope import ScopeMatcher
from paf.state import RunRecord, StateStore, TaskStatus, TokenUsage

_REPORT_BASE_PROPERTIES: dict[str, Any] = {
    "changed": {"type": "boolean"},
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
        },
        "required": ["path", "declaration", "attempts", "remaining_goal", "obstruction"],
    },
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
        {key: value for key, value in _REPORT_BASE_PROPERTIES.items() if key != "changed"}
        | {"source_dependencies": _SOURCE_DEPENDENCIES_PROPERTY},
    ),
    "formalize": _report_schema(
        "PAF formalization report",
        _REPORT_BASE_PROPERTIES | {"source_issues": _SOURCE_ISSUES_PROPERTY},
    ),
    "review": _report_schema(
        "PAF statement review report",
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
            "upstream_requests": _UPSTREAM_REQUESTS_PROPERTY,
        },
    ),
    "downstream_retry": _report_schema(
        "PAF downstream proof retry report",
        _REPORT_BASE_PROPERTIES
        | {
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "failed_attempts": _FAILED_ATTEMPTS_PROPERTY,
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
SHEPHERD_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("shepherd.md")))
UPSTREAM_REPAIR_ROLE = "upstream_repair"
DOWNSTREAM_RETRY_ROLE = "downstream_retry"
SHEPHERD_ROLE = "shepherd"
REPAIR_WORKER_ROLE = "repair_worker"


def report_schema_key(stage: Stage, *, role: str = "", feedback: str = "") -> str:
    if role == SHEPHERD_ROLE:
        return SHEPHERD_ROLE
    if role == UPSTREAM_REPAIR_ROLE:
        return UPSTREAM_REPAIR_ROLE
    if role == DOWNSTREAM_RETRY_ROLE:
        return DOWNSTREAM_RETRY_ROLE
    if stage is Stage.REVIEW and feedback:
        return "proof_review"
    return stage.value


def render_review_variant(template: str, *, upstream: bool) -> str:
    if upstream:
        values = {
            "review_assignment": """This is the targeted upstream variant. A later proof asks this
earlier chapter for one or more reusable results. Review those requests and their evidence together;
do not audit the rest of the chapter or work on unrelated placeholders.""",
            "review_goal_details": """For each request, decide whether the needed result already
exists, belongs here as a new fully proved declaration, or depends on later material and should stay
with the requesting chapter. Fully prove every declaration you add; this variant does not permit new
placeholders.""",
            "review_workflow_details": """Group requests that need the same result. If an existing
declaration solves a request, record its exact fully qualified name and concrete usage. If a result
is missing and naturally belongs here, add the smallest reusable version and prove it completely. If
it depends on later-only data, explain why and give a viable downstream direction. Return one answer
for every supplied request id.""",
            "review_guardrails": """Do not change existing interfaces or the requesting chapter. Do
not add `sorry`, `admit`, axioms, unused helpers, cosmetic aliases, or a theorem tailored merely to
restate the later proof's final goal.""",
            "review_definition_of_done": """Every request id has an evidence-backed answer, every
new declaration is fully proved and clean, and no unrelated source was changed.""",
            "review_output_format": """Return the structured report once, after tool use and edits
have stopped. It must describe the stable files on disk, not planned work. Use only these fields:

- `changed`: `true` exactly when an allowed edit remains.
- `complete`: `true` only when the definition of done is met.
- `summary`: when files changed, concise past-tense prose naming the added declarations and their
  purpose, suitable for a commit body; otherwise, why no edit was needed.
- `issues`: tooling, diagnostic, or out-of-scope blockers; otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry
  must give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`.
- `failed_attempts`: any new supporting declaration that could not be proved; otherwise an empty
  list. Each entry must give its repository-relative `path`, fully qualified `declaration`, at least
  two meaningfully different checked `attempts`, exact `remaining_goal`, and concrete `obstruction`.
- `upstream_answers`: one answer for every supplied request id. Each entry gives `request_ids`, a
  `disposition` of `added`, `existing`, or `downstream`, exact fully qualified `declarations`,
  concrete `usage_guidance`, and a `rejection_reason`. For `downstream`, leave `declarations` empty
  and explain why this earlier chapter is not the right owner. For the other dispositions, leave
  `rejection_reason` empty.""",
        }
    else:
        values = {
            "review_assignment": """This is the full-chapter re-review variant.
A proof attempt found evidence that one or more statements or supporting declarations may be wrong
or hard to use.
Review the complete assigned chapter, not only the declarations named in the evidence.""",
            "review_goal_details": """This remains statement review, not proof work. Repair every
genuine statement or interface problem in the assigned files, but preserve sound statements when
only the proof strategy failed. Existing proof placeholders may remain, and new proposition proofs
may use `by sorry` when proving them would distract from the review.""",
            "review_workflow_details": """After resolving the supplied findings, continue through
every declaration in the assigned chapter. Check source coverage, mathematical meaning, hypotheses,
and a plausible proof route through earlier results. Account for every supplied finding as
confirmed, rejected, or reframed.""",
            "review_guardrails": """Do not restrict the review to the failed declarations. A
no-change review needs no diagnostic calls because PAF's incoming build is authoritative.""",
            "review_definition_of_done": """The complete assigned chapter has been re-reviewed,
every supplied finding has been evaluated, all warranted in-scope repairs have been made, existing
library and earlier-chapter APIs have been reused wherever possible, imports remain chronological,
and edited files are clean except for permitted `sorry` warnings.""",
            "review_output_format": """Return the structured report once, after tool use and edits
have stopped. It must describe the stable files on disk, not planned work. Use only these fields:

- `changed`: `true` exactly when an allowed edit remains.
- `complete`: `true` only when the definition of done is met.
- `summary`: when files changed, concise past-tense prose naming the main files or declarations and
  the purpose of the edits, suitable for a commit body; otherwise, why no edit was needed.
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
        log.write(line + (b"\n" if terminated else b""))
        return None, None
    received_at = activity_timestamp()
    if isinstance(event, dict):
        event = {**event, EVENT_TIMESTAMP_FIELD: received_at}
        log.write(json.dumpb(event) + b"\n")
    else:
        log.write(line + (b"\n" if terminated else b""))
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
    ) -> str:
        if role == UPSTREAM_REPAIR_ROLE or (stage is Stage.REVIEW and feedback):
            prompt_path = PROOF_REVIEW_PROMPT_PATH
        else:
            prompt_path = self.config.stages[stage].prompt
        template = prompt_path.read_text(encoding="utf-8")
        if prompt_path == PROOF_REVIEW_PROMPT_PATH:
            template = render_review_variant(template, upstream=role == UPSTREAM_REPAIR_ROLE)
        base = render_prompt(template, chapter)
        common = (
            ""
            if stage is Stage.DISCOVER
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
This is another attempt. The history appended below records earlier work; do not simply repeat or
summarize it. Choose one remaining placeholder and use new evidence to improve a previous approach,
or try a meaningfully different one: search for another earlier theorem, unfold a relevant
definition, prove a focused helper, construct the object directly, or change the tactic structure.
Try several checked approaches before concluding that the same obstruction remains. If a concrete
mathematical argument shows that an earlier declaration must change, report the smallest required
change through the proof report instead of repeating that no library result was found."""
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
spend time proving propositions. PAF has already built the incoming files and will rebuild any
changes.""",
            Stage.PROVE: """The assigned chapter has passed review and builds cleanly. Work directly
on unresolved proofs rather than auditing or rechecking untouched files. PAF will build the chapter
after the attempt."""
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

This is a strict boundary. You may read files elsewhere for
context, but do not create, modify, move, delete, format, or otherwise write any path outside this
scope. In particular, do not edit `.paf`, `README.md`, repository-level documentation, prompts,
scripts, orchestration code, configuration, or tests, even if changing them seems useful for this
task. If a source repair requires an out-of-scope write, do not make that edit; explain the blocker
and exact paths as directed by the stage prompt. Before every edit, verify that its target matches
one of the listed scope paths. Any out-of-scope write causes PAF to reject the entire attempt."""
        )
        contract = f"""

## PAF requirements

{file_contract}

Do not commit and do not wait for another worker.
Do not run `lake build`, `lake env lean`, raw `lean`, or another compiler command. Builds belong to
PAF and use its single writable build cache. {stage_contract}
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
diagnostics as needed to show that the changed proof is clean.""",
            }[stage]
            contract += f"""

### Attached Lean tools (MCP)

A private `paf_lean` tool server is attached to this attempt through MCP. It works on this attempt's
private Lean project. Use it for {capabilities}. It intentionally does not provide a build command
or remote search. Tool paths are relative to the Lean project root: use `LastLib/...`, not
`lean/LastLib/...`.
{mcp_workflow}
The tool server automatically prepares imported files when Lean reports that they are stale. Do not
start another language server or work around stale imports with a compiler command. When checking
more than one edited file, call `lean_prepare_dependencies` once with the edited files at the end of
the dependency chain; the tool will prepare everything they import together. Then request final
whole-file diagnostics from prerequisites to dependents. Do not prepare every file separately.
"""
        if feedback:
            feedback_heading = (
                "Targeted downstream retry handoff"
                if role == DOWNSTREAM_RETRY_ROLE
                else "Shepherd repair dossier"
                if role == REPAIR_WORKER_ROLE
                else {
                    Stage.DISCOVER: "Discovery feedback",
                    Stage.FORMALIZE: "PAF build diagnostics and reported findings",
                    Stage.REVIEW: "Failed proof findings to evaluate",
                    Stage.PROVE: "Earlier proof attempts",
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
        return f"{template.rstrip()}\n\n## Failure dossier\n\n```json\n{json.dumps(dossier, indent=2)}\n```\n"

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
        prompt = self.build_prompt(
            chapter,
            stage,
            feedback=feedback,
            workspace_root=root,
            role=run.role,
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
