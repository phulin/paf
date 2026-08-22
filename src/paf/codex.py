from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import re
import signal
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4

from paf import json_codec as json
from paf.activity import EVENT_TIMESTAMP_FIELD, activity_timestamp
from paf.backends import LeanBackend
from paf.beam import BeamSession
from paf.diagnostics import (
    LeanDiagnostic,
    failed_lean_modules,
    lean_diagnostics,
    unexpected_lean_warnings,
)
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

_UPSTREAM_HYPOTHESIS_PROPERTY: dict[str, Any] = {
    "anyOf": [
        {"type": "null"},
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "capability_key": {"type": "string", "minLength": 1},
                "owner_kind": {
                    "type": "string",
                    "enum": ["chapter", "consumer", "shared", "external"],
                },
                "owner_paths": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                },
                "needed_result": {"type": "string", "minLength": 1},
            },
            "required": ["capability_key", "owner_kind", "owner_paths", "needed_result"],
        },
    ]
}

_CHECKED_PROOF_ATTEMPT_PROPERTY: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "strategy": {"type": "string", "minLength": 1},
        "probe": {"type": "string", "minLength": 1},
        "outcome": {"type": "string", "minLength": 1},
    },
    "required": ["strategy", "probe", "outcome"],
}

_UNRESOLVED_PROOFS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "declaration": {"type": "string", "minLength": 1},
            "attempts": {
                "type": "array",
                "items": _CHECKED_PROOF_ATTEMPT_PROPERTY,
                "minItems": 1,
            },
            "remaining_goal": {"type": "string", "minLength": 1},
            "obstruction": {"type": "string", "minLength": 1},
            "evidence": {"type": "string", "minLength": 1},
            "kind": {
                "type": "string",
                "enum": [
                    "local_proof_failure",
                    "suspected_statement_defect",
                    "suspected_upstream_gap",
                ],
            },
            "upstream_hypothesis": _UPSTREAM_HYPOTHESIS_PROPERTY,
        },
        "required": [
            "path",
            "declaration",
            "attempts",
            "remaining_goal",
            "obstruction",
            "evidence",
            "kind",
            "upstream_hypothesis",
        ],
    },
}

_BLOCKER_REFS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {"type": "string", "pattern": "^B[0-9]+$"},
    "description": "Durable blocker IDs whose fingerprint and evidence are unchanged.",
}

_PROOF_DISPOSITION_PROPERTY: dict[str, Any] = {
    "type": "string",
    "enum": [
        "proved",
        "incomplete",
        "validation_inconsistency",
    ],
    "description": "Whether the assigned proofs are complete or still require coordinator action.",
}

_RETRY_CONTRACT_PROPERTY: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "new_information": {"type": "string", "minLength": 1},
        "declarations": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
        "intermediate_claims": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "critical_probe": {"type": "string", "minLength": 1},
        "known_remaining_gap": {"type": "string"},
    },
    "required": [
        "new_information",
        "declarations",
        "intermediate_claims",
        "critical_probe",
        "known_remaining_gap",
    ],
}

_FINDING_ASSESSMENTS_PROPERTY: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "finding_id": {"type": "string", "minLength": 1},
            "finding": {"type": "string", "minLength": 1},
            "diagnosis": {
                "type": "string",
                "enum": [
                    "statement_defect",
                    "interface_defect",
                    "missing_capability",
                    "consumer_local_proof",
                    "stale_target",
                    "external_gap",
                    "validation_noise",
                    "genuine_blocker",
                ],
            },
            "action": {
                "type": "string",
                "enum": [
                    "repair_and_retry",
                    "retry_with_route",
                    "request_upstream",
                    "wait_for_dependency",
                    "park_external",
                    "drop_stale_target",
                ],
            },
            "explanation": {"type": "string", "minLength": 1},
            "retry_contract": {"anyOf": [{"type": "null"}, _RETRY_CONTRACT_PROPERTY]},
            "upstream_request": _UPSTREAM_HYPOTHESIS_PROPERTY,
            "dependency_ids": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
        },
        "required": [
            "finding_id",
            "finding",
            "diagnosis",
            "action",
            "explanation",
            "retry_contract",
            "upstream_request",
            "dependency_ids",
        ],
    },
}

_PACKAGE_STEP_ITEM: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "step_id": {"type": "string", "minLength": 1, "pattern": "^[A-Za-z0-9._-]+$"},
        "objective": {"type": "string", "minLength": 1},
        "kind": {
            "type": "string",
            "enum": [
                "investigation",
                "interface",
                "supporting_lemma",
                "consumer_integration",
                "statement_revision",
                "validation",
            ],
        },
        "intended_declarations": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "intended_paths": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "depends_on_step_ids": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "validation_commands": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
    },
    "required": [
        "step_id",
        "objective",
        "kind",
        "intended_declarations",
        "intended_paths",
        "depends_on_step_ids",
        "validation_commands",
    ],
}

_PACKAGE_STEWARD_PROPERTIES: dict[str, Any] = {
    "complete": _REPORT_BASE_PROPERTIES["complete"],
    "summary": _REPORT_BASE_PROPERTIES["summary"],
    "issues": _REPORT_BASE_PROPERTIES["issues"],
    "diagnosis": {"type": "string", "minLength": 1},
    "placement_decision": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "kind": {
                "type": "string",
                "enum": [
                    "existing",
                    "consumer_local",
                    "shared",
                    "new_interface",
                    "statement_revision",
                    "external",
                ],
            },
            "paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "declarations": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "rationale": {"type": "string", "minLength": 1},
        },
        "required": ["kind", "paths", "declarations", "rationale"],
    },
    "scope_expansion_requests": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["exclusive_file", "exclusive_subtree"]},
                "reason": {"type": "string", "minLength": 1},
            },
            "required": ["path", "mode", "reason"],
        },
    },
    "plan_revision": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "base_revision": {"type": "integer", "minimum": 0},
            "steps": {"type": "array", "items": _PACKAGE_STEP_ITEM},
            "revision_reason": {"type": "string", "minLength": 1},
        },
        "required": ["base_revision", "steps", "revision_reason"],
    },
    "completed_step_assessments": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "step_id": {"type": "string", "minLength": 1},
                "accepted": {"type": "boolean"},
                "commit_ids": {"type": "array", "items": {"type": "string", "minLength": 1}},
                "validation_evidence": {"type": "string", "minLength": 1},
                "remaining_gap": {"type": "string"},
            },
            "required": [
                "step_id",
                "accepted",
                "commit_ids",
                "validation_evidence",
                "remaining_gap",
            ],
        },
    },
    "worker_assignments": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "step_id": {"type": "string", "minLength": 1},
                "worker_id": {"type": "string", "minLength": 1},
                "objective": {"type": "string", "minLength": 1},
            },
            "required": ["step_id", "worker_id", "objective"],
        },
    },
    "package_dependency_requests": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "package_id": {"type": "string", "minLength": 1},
                "required_revision": {"type": ["string", "null"]},
                "reason": {"type": "string", "minLength": 1},
            },
            "required": ["package_id", "required_revision", "reason"],
        },
    },
    "child_packages": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "capability_key": {"type": "string", "minLength": 1},
                "title": {"type": "string", "minLength": 1},
                "mathematical_objective": {"type": "string", "minLength": 1},
                "write_scope": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                },
                "consumer_ids": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
            },
            "required": [
                "capability_key",
                "title",
                "mathematical_objective",
                "write_scope",
                "consumer_ids",
            ],
        },
    },
    "consumer_assessments": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "consumer_id": {"type": "string", "minLength": 1},
                "disposition": {
                    "type": "string",
                    "enum": ["accepted", "open", "detached", "terminal"],
                },
                "acceptance_evidence": {"type": "string"},
                "detached_package_id": {"type": ["string", "null"]},
                "remaining_obstruction": {"type": "string"},
            },
            "required": [
                "consumer_id",
                "disposition",
                "acceptance_evidence",
                "detached_package_id",
                "remaining_obstruction",
            ],
        },
    },
    "disposition": {
        "type": "string",
        "enum": [
            "continue",
            "complete",
            "waiting_dependency",
            "decomposed",
            "external",
            "statement_revision_required",
            "parked",
        ],
    },
    "remaining_work": {"type": "string"},
}

_PACKAGE_WORKER_PROPERTIES: dict[str, Any] = {
    "complete": _REPORT_BASE_PROPERTIES["complete"],
    "summary": _REPORT_BASE_PROPERTIES["summary"],
    "issues": _REPORT_BASE_PROPERTIES["issues"],
    "step_id": {"type": "string", "minLength": 1},
    "changed_declarations": {"type": "array", "items": {"type": "string", "minLength": 1}},
    "changed_paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
    "focused_validation": {"type": "string", "minLength": 1},
    "remaining_gap": {"type": "string"},
    "new_evidence": {"type": "array", "items": {"type": "string", "minLength": 1}},
}

_UPSTREAM_STEWARD_PROPERTIES: dict[str, Any] = {
    "complete": _REPORT_BASE_PROPERTIES["complete"],
    "summary": _REPORT_BASE_PROPERTIES["summary"],
    "issues": _REPORT_BASE_PROPERTIES["issues"],
    "cases": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "case_id": {"type": "string", "pattern": "^[A-Za-z0-9._-]+$"},
                "title": {"type": "string", "minLength": 1},
                "request_ids": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                },
                "disposition": {
                    "type": "string",
                    "enum": ["repair", "retry_consumers", "reject"],
                },
                "needed_result": {"type": "string", "minLength": 1},
                "context_work_unit_ids": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                },
                "write_work_unit_ids": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "acceptance_tests": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "rationale": {"type": "string", "minLength": 1},
            },
            "required": [
                "case_id",
                "title",
                "request_ids",
                "disposition",
                "needed_result",
                "context_work_unit_ids",
                "write_work_unit_ids",
                "acceptance_tests",
                "rationale",
            ],
        },
    },
}

_UPSTREAM_REPAIR_PROPERTIES: dict[str, Any] = {
    "complete": _REPORT_BASE_PROPERTIES["complete"],
    "summary": _REPORT_BASE_PROPERTIES["summary"],
    "issues": _REPORT_BASE_PROPERTIES["issues"],
    "disposition": {
        "type": "string",
        "enum": ["repaired", "not_needed", "consumer_local", "needs_scope", "failed"],
    },
    "placement": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "declarations": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "rationale": {"type": "string", "minLength": 1},
        },
        "required": ["paths", "declarations", "rationale"],
    },
    "consumer_routes": {
        "type": "array",
        "items": {"type": "string", "minLength": 1},
    },
    "additional_paths": {
        "type": "array",
        "items": {"type": "string", "minLength": 1},
    },
    "deferred_proofs": {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "declaration": {"type": "string", "minLength": 1},
                "reason": {
                    "type": "string",
                    "enum": [
                        "new_proposition",
                        "revised_statement",
                        "invalidated_consumer",
                    ],
                },
            },
            "required": ["path", "declaration", "reason"],
        },
    },
    "validation_evidence": {"type": "string", "minLength": 1},
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
    "upstream_steward": _report_schema(
        "PAF global upstream-request steward report", _UPSTREAM_STEWARD_PROPERTIES
    ),
    "upstream_repair": _report_schema(
        "PAF focused upstream repair report", _UPSTREAM_REPAIR_PROPERTIES
    ),
    "package_steward": _report_schema(
        "PAF capability-package Steward report", _PACKAGE_STEWARD_PROPERTIES
    ),
    "package_worker": _report_schema(
        "PAF capability-package worker report", _PACKAGE_WORKER_PROPERTIES
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
    "warning_cleanup": _report_schema(
        "PAF warning cleanup report",
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
            "disposition": _PROOF_DISPOSITION_PROPERTY,
            "source_issues": _SOURCE_ISSUES_PROPERTY,
            "unresolved_proofs": _UNRESOLVED_PROOFS_PROPERTY,
            "blocker_refs": _BLOCKER_REFS_PROPERTY,
        },
    ),
}

USAGE_POLL_SECONDS = 1.0
ROLLOUT_READ_BYTES = 1024 * 1024
PROCESS_GROUP_GRACE_SECONDS = 1.0
PROCESS_EXIT_POLL_SECONDS = 0.005
_PROMPT_RESOURCES = files("paf.prompts")
COMMON_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("common.md")))
PROOF_REVIEW_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("proof_review.md")))
DIAGNOSTIC_REVIEW_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("diagnostic_review.md")))
WARNING_CLEANUP_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("warning_cleanup.md")))
PACKAGE_STEWARD_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("package_steward.md")))
PACKAGE_WORKER_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("package_worker.md")))
UPSTREAM_STEWARD_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("upstream_steward.md")))
UPSTREAM_REPAIR_PROMPT_PATH = Path(str(_PROMPT_RESOURCES.joinpath("upstream_repair.md")))
PACKAGE_STEWARD_ROLE = "package_steward"
PACKAGE_WORKER_ROLE = "package_worker"
UPSTREAM_STEWARD_ROLE = "upstream_steward"
UPSTREAM_REPAIR_ROLE = "upstream_repair"
DIAGNOSTIC_REVIEW_ROLE = "diagnostic_review"
PROOF_REVIEW_ROLE = "proof_review"
WARNING_CLEANUP_ROLE = "warning_cleanup"
# Compatibility name for callers written before warning cleanup became an
# independent auxiliary workflow.
WARNING_REVIEW_ROLE = WARNING_CLEANUP_ROLE
DIAGNOSTIC_REVIEW_ROLES = frozenset({DIAGNOSTIC_REVIEW_ROLE, WARNING_REVIEW_ROLE})


def report_schema_key(stage: Stage, *, role: str = "", feedback: str = "") -> str:
    if role in {
        PACKAGE_STEWARD_ROLE,
        PACKAGE_WORKER_ROLE,
        UPSTREAM_STEWARD_ROLE,
        UPSTREAM_REPAIR_ROLE,
    }:
        return role
    if role == WARNING_CLEANUP_ROLE:
        return WARNING_CLEANUP_ROLE
    if role == DIAGNOSTIC_REVIEW_ROLE:
        return DIAGNOSTIC_REVIEW_ROLE
    if role == PROOF_REVIEW_ROLE:
        return PROOF_REVIEW_ROLE
    if stage is Stage.REVIEW and feedback:
        return "proof_review"
    return stage.value


def render_review_variant(template: str, *, role: str = "") -> str:
    values = {
        "review_assignment": """This is a focused statement and interface review triggered by
failed proof evidence. Review the named declarations, their immediate supporting interfaces, and
the corresponding source passages. Do not spend time re-auditing unrelated chapter declarations.""",
        "review_goal_details": """This remains review work, not a second proof attempt. Repair
every genuine statement or supporting-interface problem in the focused scope. You are authorized to
make the smallest source-faithful public correction when the evidence confirms a defect. Preserve a
sound interface when only the proof strategy failed.""",
        "review_workflow_details": """Account for every supplied finding ID with one diagnosis
and one machine-actionable next action. First confirm that the declaration is live and the supplied
evidence is current. Check the exact statement against the source, test obvious counterexamples,
and inspect only the focused APIs needed to route it. Call a retry route executable only when every
 substantial step names an exact existing declaration and a focused Lean probe checks the critical
composition. If a required result belongs in an earlier module, make an upstream request naming
the consumer obstruction and the suspected owner paths. Treat unrelated prerequisite diagnostics
as a dependency wait, not a mathematical
proof failure.""",
        "review_guardrails": """Do not spend the assignment proving existing proposition
placeholders. Do not broaden from the named findings into unrelated chapter cleanup. Do not repeat
searches already recorded in the blocker ledger. A no-change review must route each blocker rather
than merely restate it. Do not request a full dependency validation when no source was edited;
focused target diagnostics are sufficient. Supplied target-local build diagnostics remain required
work.""",
        "review_definition_of_done": """Every supplied finding has an evidence-backed diagnosis
and routing action, every in-scope defect has the smallest source-faithful repair, every proof retry
has an executable checked contract, every suspected upstream problem has a request, and edited files
have no diagnostics except permitted exact `sorry` warnings.""",
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
- `finding_assessments`: one entry for each supplied proof finding. Copy its exact `finding_id` and
  a concise identifying `finding`; choose a `diagnosis` and `action`; give checked `explanation`;
  supply an executable `retry_contract` only for `retry_with_route` (otherwise `null`); and supply
  `upstream_request` only for `request_upstream`, and name exact `dependency_ids` for a dependency
  wait. An upstream request must state the result needed by the consumer and the earlier paths the
  evaluator must inspect; it does not assign ownership or prescribe an implementation.""",
    }
    for key, value in values.items():
        template = template.replace("{" + key + "}", value)
    return template


CAPACITY_RESUME_PROMPT = "Continue from the interrupted turn and complete the assigned task."
BEAM_DAEMON_REMINDER = (
    "PAF already owns the persistent Beam daemon for this workspace. Never run "
    "`lean-beam ensure --hold`; call ordinary Beam commands directly."
)
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


class ValidationStatus(StrEnum):
    CLEAN = "clean"
    TARGET_WARNINGS = "target_warnings"
    DEFERRED = "deferred"
    TARGET_FAILED = "target_failed"
    DEPENDENCY_FAILED = "dependency_failed"
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
    diagnostics: tuple[LeanDiagnostic, ...] = ()
    failed_modules: tuple[str, ...] = ()
    raw_log_path: str | None = None

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

    @property
    def warnings_only(self) -> bool:
        """Whether Lean produced usable artifacts rejected only by warning policy."""

        return self.status is ValidationStatus.TARGET_WARNINGS and self.compiler_succeeded

    def as_dict(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "exit_code": self.exit_code,
            "output": self.output,
            "timed_out": self.timed_out,
            "process_exit_code": self.process_exit_code,
            "status": self.status,
            "blocked_by": list(self.blocked_by),
            "diagnostics": [diagnostic.as_dict() for diagnostic in self.diagnostics],
            "failed_modules": list(self.failed_modules),
            "raw_log_path": self.raw_log_path,
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
    infrastructure_failed: bool = False


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


def _is_infrastructure_failure(event: Any) -> bool:
    """Whether the agent failed before useful work because its execution tools did not start."""

    markers = (
        "required mcp servers failed to initialize",
        "handshaking with mcp server failed",
        "failed to initialize session",
        "error creating thread",
        "connection closed: initialize response",
    )
    return any(
        any(marker in message.casefold() for marker in markers)
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

    def interrupted_predecessor(
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
            and (prior.role or prior.stage) == (run.role or run.stage)
            and prior.request_ids == run.request_ids
            and prior.proof_targets == run.proof_targets
            and (not prior.prompt_kind or prior.prompt_kind == run.prompt_kind)
            else None
        )

    def resumable_run(
        self,
        run: RunRecord,
        stage: Stage,
    ) -> RunRecord | None:
        prior = self.interrupted_predecessor(run, stage)
        return prior if prior is not None and prior.thread_id else None

    # Compatibility for integrations which used the former private helper.
    _resumable_run = resumable_run

    async def prepare(self) -> None:
        self.config.settings.state_dir.mkdir(parents=True, exist_ok=True)
        for legacy_name in (
            "agent-report.schema.json",
            "agent-report-upstream_implementation.schema.json",
        ):
            (self.config.settings.state_dir / legacy_name).unlink(missing_ok=True)
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
        proof_targets: Iterable[ProofTarget | dict[str, Any]] = (),
    ) -> str:
        if role == UPSTREAM_REPAIR_ROLE:
            prompt_path = UPSTREAM_REPAIR_PROMPT_PATH
        elif role == PACKAGE_WORKER_ROLE:
            prompt_path = PACKAGE_WORKER_PROMPT_PATH
        elif role == WARNING_CLEANUP_ROLE:
            prompt_path = WARNING_CLEANUP_PROMPT_PATH
        elif role == DIAGNOSTIC_REVIEW_ROLE:
            prompt_path = DIAGNOSTIC_REVIEW_PROMPT_PATH
        elif role == PROOF_REVIEW_ROLE or (stage is Stage.REVIEW and feedback):
            prompt_path = PROOF_REVIEW_PROMPT_PATH
        else:
            prompt_path = self.config.stages[stage].prompt
        template = prompt_path.read_text(encoding="utf-8")
        if prompt_path == PROOF_REVIEW_PROMPT_PATH:
            template = render_review_variant(template, role=role)
        elif prompt_path == DIAGNOSTIC_REVIEW_PROMPT_PATH:
            diagnostic_trigger = (
                "The coordinator build reported Lean errors, possibly together with "
                "non-`sorry` warnings."
            )
            template = template.replace("{diagnostic_trigger}", diagnostic_trigger)
        if role == PACKAGE_WORKER_ROLE:
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
            base = render_prompt(template.replace("{repair_instruction}", instruction), chapter)
        else:
            base = render_prompt(template, chapter)
        omit_common = role not in {PACKAGE_WORKER_ROLE, UPSTREAM_REPAIR_ROLE} and stage in (
            Stage.DISCOVER,
            Stage.PROVE,
        )
        common = (
            ""
            if omit_common
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
        if stage is Stage.PROVE and feedback:
            proof_retry_contract = """
This is a retry. Use the target-specific handoff near the assignment to continue checked work
without repeating known failures. Return an existing blocker reference only when that exact
evidence remains current and the handoff supplies no new source, interface, reviewer guidance, or
viable proof route."""
        selected_proof_targets = tuple(proof_targets)
        proof_assignment = ""
        if stage is Stage.PROVE and selected_proof_targets and role != PACKAGE_WORKER_ROLE:
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
            chunk_size = self.config.stages[Stage.PROVE].chunk_size or 6
            overflow = (
                f" The configured chunk size is {chunk_size}, but PAF keeps all holes in one "
                "declaration with the same agent because they can share local terms."
                if declaration_count == 1 and assigned_placeholders > chunk_size
                else ""
            )
            merged_scope_digest = scope_digest(root, chapter)
            attempt_mode = "retry with a target-specific handoff" if feedback else "initial attempt"
            proof_assignment = f"""## Current merged-source target

Attempt mode: {attempt_mode}
Merged scope digest: `{merged_scope_digest}`

This attempt owns exactly {declaration_count} {declaration_label} containing
{assigned_placeholders} {hole_label}.{overflow}

{chr(10).join(rendered_targets).rstrip()}

{reservation}

Work on the assigned declaration bodies and the focused helper declarations they require within the
editable chapter. Helpers may be public when they record reusable intermediate mathematics.
Resolve every
listed hole and every diagnostic in the assigned span. Set `complete` to `true` only when all listed
holes are gone and no non-`sorry` diagnostic remains.
"""
            if feedback:
                proof_assignment += f"""
## Retry handoff

The coordinator supplied the following target-specific context so you can continue from prior
checked work, distinguish proof evidence from build diagnostics, and avoid repeating known failures.
Treat prior blocker classifications as untrusted hypotheses: a claimed missing capability in this
editable chapter is a local implementation plan, not an upstream blocker.

{_bounded_feedback(feedback)}
"""
            insertion_heading = "\n## Proof workflow\n"
            if insertion_heading in base:
                base = base.replace(
                    insertion_heading,
                    f"\n{proof_assignment}\n## Proof workflow\n",
                    1,
                )
            elif "\n## Working method\n" in base:
                base = base.replace(
                    "\n## Working method\n",
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
        if role == PACKAGE_STEWARD_ROLE:
            stage_contract = """This is a writable capability-package Steward turn. You own the
private package overlay and may edit every reserved path, including shared interfaces and
consumers in several files. Make central interface edits yourself, delegate only bounded leaf
steps, and return a complete package mutation report."""
        elif role == PACKAGE_WORKER_ROLE:
            stage_contract = """This is one bounded capability-package worker step. Edit only the
assigned paths in the package overlay, and report exact validation and
remaining evidence. You may not change placement, scope, dependencies, consumers, or package
lifecycle."""
        elif role == UPSTREAM_REPAIR_ROLE:
            stage_contract = """This is focused cross-module interface repair. You may add or revise
declarations, definitions, imports, and structurally affected uses within the writable scope.
Implement computational content fully, but do not prove newly introduced propositions or repair
proofs invalidated by interface changes. Use permitted proof placeholders for those obligations,
record them precisely, validate interface-level elaboration, and return as soon as the structural
work is complete."""
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
        backend = self.config.backend or LeanBackend(project=self.config.settings.lean_project)
        if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            project = backend.project.as_posix()
            contract += f"""

### Lean Beam workflow

Use the installed `$lean-beam` skill and `{backend.beam_command}` CLI. Run Beam commands from
`{project}` so paths are relative to the Lean project root: use `LastLib/...`, not
`{project}/LastLib/...`.

- {BEAM_DAEMON_REMINDER} It blocks until interrupted.
- Beam reads saved files. Call `lean-beam update FILE` before a version-bound query.
- Use `paf lean search QUERY` for source search across the project, Lake packages, and toolchain.
- Use `paf lean prepare FILE...` to follow Beam's stale-direct-import recovery automatically.
- Run only one Beam or `paf lean prepare` command at a time. Wait for it to finish before starting
  another command that touches Lean; overlapping operations can terminate the same file worker.
- Use `goals`, `todo`, `hover`, `definition`, `references`, `workspace-symbols`, and `run-at` for
  focused inspection and speculative proof attempts.
- After every real source edit, call `lean-beam update FILE` before another probe and
  `lean-beam sync FILE` when you need fresh diagnostics.
- If sync reports stale direct imports, follow its `saveDeps` and `recoveryPlan`, then refresh the
  importer. Do not run `lake build` or `lake clean`; PAF owns dependency-cone and final builds.
- `lean-beam save` checkpoints only one module and does not validate downstream importers.
"""
        if feedback and stage is not Stage.PROVE:
            feedback_heading = (
                "Capability package worker packet"
                if role == PACKAGE_WORKER_ROLE
                else "PAF validation diagnostics to repair"
                if role in DIAGNOSTIC_REVIEW_ROLES
                else {
                    Stage.DISCOVER: "Discovery feedback",
                    Stage.FORMALIZE: "PAF build diagnostics and reported findings",
                    Stage.REVIEW: "Proof findings and PAF validation diagnostics",
                    Stage.PROVE: "Retry handoff",
                }[stage]
            )
            contract += f"\n## {feedback_heading}\n\n```text\n{_bounded_feedback(feedback)}\n```\n"
        elif feedback and stage is Stage.PROVE and not selected_proof_targets:
            contract += f"""

## Retry handoff

The coordinator supplied the following context so you can continue from prior checked work,
distinguish proof evidence from build diagnostics, and avoid repeating known failures.

{_bounded_feedback(feedback)}
"""
        return f"{base.rstrip()}\n\n{common.rstrip()}\n{contract}"

    def build_package_steward_prompt(self, dossier: dict[str, Any]) -> str:
        """Render one complete, package-owned multi-file Steward assignment."""

        template = PACKAGE_STEWARD_PROMPT_PATH.read_text(encoding="utf-8")
        payload = json.dumps(dossier, indent=2)
        return f"{template.rstrip()}\n\n## Package dossier\n\n```json\n{payload}\n```\n"

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
            command.extend(
                ["--ignore-user-config", "--json", "--color", "never", "--cd", str(root)]
            )
        else:
            command.extend(["resume", "--ignore-user-config", "--json"])
        schema_key = report_schema_key(stage, role=role, feedback=feedback)
        command.extend(["--output-schema", str(self.schema_paths[schema_key])])
        if root != settings.repo:
            command.append("--skip-git-repo-check")
        if settings.bypass_approvals_and_sandbox:
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
        if role in {PACKAGE_STEWARD_ROLE, UPSTREAM_STEWARD_ROLE}:
            model = self.config.steward.model
            reasoning_effort = self.config.steward.reasoning_effort
        elif role in {PACKAGE_WORKER_ROLE, UPSTREAM_REPAIR_ROLE}:
            model = self.config.steward.worker_model
            reasoning_effort = self.config.steward.worker_reasoning_effort
        else:
            model = self.config.model_for(stage)
            reasoning_effort = self.config.reasoning_effort_for(stage)
        if model:
            command.extend(["--model", model])
        if reasoning_effort:
            command.extend(["--config", f'model_reasoning_effort="{reasoning_effort}"'])
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
            resume_prompt=(
                f"{reminder}\n\n{BEAM_DAEMON_REMINDER}"
                if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE)
                else reminder
            ),
        )

    async def run_package_steward(
        self,
        anchor: WorkUnitLike,
        run: RunRecord,
        dossier: dict[str, Any],
        *,
        workspace_root: Path,
    ) -> AgentResult:
        """Run the fenced, writable Steward that owns one capability package."""

        prompt = self.build_package_steward_prompt(dossier)
        return await self._run_prompt(
            anchor,
            Stage.PROVE,
            run,
            prompt=prompt,
            workspace_root=workspace_root,
        )

    async def run_package_worker(
        self,
        assignment: WorkUnitLike,
        run: RunRecord,
        packet: dict[str, Any],
        *,
        workspace_root: Path,
    ) -> AgentResult:
        """Execute one bounded sequential worker step in its package overlay."""

        feedback = json.dumps(packet, indent=2)
        prompt = self.build_prompt(
            assignment,
            Stage.PROVE,
            role=PACKAGE_WORKER_ROLE,
            feedback=feedback,
            workspace_root=workspace_root,
        )
        return await self._run_prompt(
            assignment,
            Stage.PROVE,
            run,
            prompt=prompt,
            feedback=feedback,
            workspace_root=workspace_root,
        )

    async def run_upstream_steward(
        self,
        anchor: WorkUnitLike,
        run: RunRecord,
        dossier: dict[str, Any],
        *,
        workspace_root: Path,
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> AgentResult:
        prompt = (
            UPSTREAM_STEWARD_PROMPT_PATH.read_text(encoding="utf-8").rstrip()
            + "\n\n## Outstanding request ledger\n\n```json\n"
            + json.dumps(dossier, indent=2)
            + "\n```\n"
        )
        return await self._run_prompt(
            anchor,
            Stage.DISCOVER,
            run,
            prompt=prompt,
            workspace_root=workspace_root,
            resume_thread_id=resume_thread_id,
            resume_run_id=resume_run_id,
            resume_prompt=resume_prompt or CAPACITY_RESUME_PROMPT,
        )

    async def run_upstream_repair(
        self,
        assignment: WorkUnitLike,
        run: RunRecord,
        dossier: dict[str, Any],
        *,
        workspace_root: Path,
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> AgentResult:
        prompt = self.build_upstream_repair_prompt(assignment, dossier)
        return await self._run_prompt(
            assignment,
            Stage.REVIEW,
            run,
            prompt=prompt,
            workspace_root=workspace_root,
            resume_thread_id=resume_thread_id,
            resume_run_id=resume_run_id,
            resume_prompt=resume_prompt or CAPACITY_RESUME_PROMPT,
        )

    def build_upstream_repair_prompt(
        self,
        assignment: WorkUnitLike,
        dossier: dict[str, Any],
    ) -> str:
        """Render a case as a readable repair assignment without coordinator identifiers."""

        raw_case = dossier.get("case")
        case: dict[str, Any] = raw_case if isinstance(raw_case, dict) else {}
        requests = dossier.get("requests")
        request_values = (
            [value for value in requests.values() if isinstance(value, dict)]
            if isinstance(requests, dict)
            else []
        )
        work_units = dossier.get("work_units")
        work_unit_values = (
            [value for value in work_units if isinstance(value, dict)]
            if isinstance(work_units, list)
            else []
        )

        lines = [
            self.build_prompt(
                assignment,
                Stage.REVIEW,
                role=UPSTREAM_REPAIR_ROLE,
            ).rstrip(),
            "",
            "## Assignment",
            "",
            f"### {str(case.get('title', '')).strip() or 'Repair the shared Lean interface'}",
            "",
            str(case.get("needed_result", "")).strip(),
        ]
        rationale = str(case.get("rationale", "")).strip()
        if rationale:
            lines.extend(["", "Why this needs cross-module investigation:", "", rationale])

        lines.extend(["", "### Downstream failures to repair", ""])
        for index, request in enumerate(request_values, start=1):
            declaration = str(request.get("blocked_declaration", "")).strip() or "Unnamed proof"
            path = str(request.get("consumer_path", "")).strip()
            location = f" in `{path}`" if path else ""
            lines.append(f"{index}. **`{declaration}`{location}**")
            for label, key in (
                ("Observed obstruction", "obstruction"),
                ("Capability believed to be needed", "needed_result"),
                ("Remaining Lean goal", "residual_goal"),
            ):
                value = str(request.get(key, "")).strip()
                if value:
                    lines.extend(
                        [f"   - {label}:", "", *[f"     {line}" for line in value.splitlines()]]
                    )
            attempts = request.get("attempted_alternatives")
            if isinstance(attempts, list) and attempts:
                lines.append("   - Routes already checked:")
                lines.extend(
                    f"     - {str(attempt).strip()}" for attempt in attempts if str(attempt).strip()
                )

        lines.extend(["", "### Relevant modules and source material", ""])
        for unit in work_unit_values:
            title = str(unit.get("title", "")).strip() or "Relevant work unit"
            source = str(unit.get("textbook_source", "")).strip()
            span = unit.get("textbook_lines")
            source_ref = f"`{source}`"
            if isinstance(span, list) and len(span) == 2:
                source_ref += f", lines {span[0]}-{span[1]}"
            lines.extend([f"- **{title}**", f"  - Textbook: {source_ref}"])
            scope = unit.get("lean_scope")
            if isinstance(scope, list):
                lines.extend(f"  - Lean: `{path}`" for path in scope)

        acceptance_tests = list(case.get("acceptance_tests", ()))
        for request in request_values:
            tests = request.get("acceptance_tests")
            if isinstance(tests, list):
                acceptance_tests.extend(tests)
        acceptance_tests = list(
            dict.fromkeys(str(test).strip() for test in acceptance_tests if str(test).strip())
        )
        if acceptance_tests:
            lines.extend(["", "### Acceptance targets", ""])
            lines.extend(f"- {test}" for test in acceptance_tests)

        lines.extend(["", "### Writable locked scope", ""])
        lines.extend(f"- `{path}`" for path in assignment.scope)
        return "\n".join(lines) + "\n"

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
        if (
            stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE)
            and BEAM_DAEMON_REMINDER not in resume_prompt
        ):
            resume_prompt = f"{resume_prompt}\n\n{BEAM_DAEMON_REMINDER}"
        prompt_path = self.state.logs_dir / f"{run.id}.prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        before = await asyncio.to_thread(scope_digest, root, chapter)
        log_path = self.state.logs_dir / f"{run.id}.jsonl"
        continuing_run = resume_thread_id is not None and resume_run_id == run.id
        prior_usage = run.usage if continuing_run else TokenUsage()
        usage = prior_usage
        cumulative_usage = TokenUsage()
        usage_baseline: TokenUsage | None = None
        report: dict[str, Any] = {}
        resumable_run = self._resumable_run(run, stage)
        thread_id = resume_thread_id
        if thread_id is None and self.resume_agents and resumable_run:
            thread_id = resumable_run.thread_id
        interrupted_resume = thread_id is not None
        if thread_id is not None:
            previous_run_id = resume_run_id or (
                resumable_run.id if resumable_run is not None else ""
            )
            changes: dict[str, Any] = {"thread_id": thread_id}
            if previous_run_id and previous_run_id != run.id:
                changes["resumed_from_run_id"] = previous_run_id
            await self.state.update_run(run, **changes)
        invocation_error = ""
        fatal_invocation_failure = False
        infrastructure_failure = False
        activity = self.state.activities.get(run.id) if continuing_run else None
        if activity is None:
            activity = await self.state.activities.start_async(
                run.id, chapter.id, run.role or stage.value
            )
        else:
            activity.finished_at = None
            activity.retry("resuming interrupted Codex session")
            await self.state.activities.save_async(activity)
        usage_stop = asyncio.Event()
        usage_monitor: asyncio.Task[None] | None = None
        attempt_deadline = (
            asyncio.get_running_loop().time() + self.config.settings.agent_timeout_seconds
        )
        backend = self.config.backend or LeanBackend(project=self.config.settings.lean_project)
        beam_session: BeamSession | None = None
        agent_environment = os.environ.copy()
        if stage in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            beam_error = ""
            for beam_attempt in range(self.config.settings.capacity_resume_attempts + 1):
                try:
                    beam_session = await BeamSession.start(
                        command=backend.beam_command,
                        project=(root / backend.project).resolve(),
                        state_dir=self.config.settings.state_dir,
                        run_id=run.id,
                        timeout_seconds=backend.beam_startup_timeout_seconds,
                    )
                    break
                except Exception as error:
                    beam_error = str(error) or type(error).__name__
                    if beam_attempt >= self.config.settings.capacity_resume_attempts:
                        break
                    activity.retry(
                        f"infrastructure retry {beam_attempt + 1}/"
                        f"{self.config.settings.capacity_resume_attempts}: Lean Beam startup "
                        f"failed: {beam_error[:500]}"
                    )
                    await self.state.activities.save_async(activity)
                    await asyncio.sleep(
                        _capacity_resume_delay(
                            self.config.settings.capacity_resume_delay_seconds,
                            self.config.settings.capacity_resume_max_delay_seconds,
                            beam_attempt + 1,
                        )
                    )
            if beam_session is None:
                after, placeholders = await asyncio.to_thread(
                    lambda: (scope_digest(root, chapter), count_placeholders(root, chapter))
                )
                error = f"Lean Beam startup retries exhausted: {beam_error}"
                activity.finish("failed", error)
                await self.state.activities.save_async(activity)
                await self.state.finish_run(
                    run,
                    status=TaskStatus.FAILED,
                    exit_code=1,
                    changed=before != after,
                    placeholders=placeholders,
                    usage=usage,
                    failure_kind="infrastructure",
                    error=error,
                    source_digest=after,
                )
                return AgentResult(
                    succeeded=False,
                    exit_code=1,
                    changed=before != after,
                    placeholders=placeholders,
                    usage=usage,
                    error=error,
                    infrastructure_failed=True,
                )
            agent_environment = beam_session.environment

        async def update_usage(found: TokenUsage) -> None:
            nonlocal usage, cumulative_usage, usage_baseline
            if found.total_tokens < cumulative_usage.total_tokens:
                return
            cumulative_usage = found
            if thread_id is not None:
                if usage_baseline is None:
                    usage_baseline = self.state.thread_cumulative_usage.get(thread_id, TokenUsage())
                usage = prior_usage + found.delta_from(usage_baseline)
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
                env=agent_environment,
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
                        nonlocal infrastructure_failure
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
                        infrastructure_failure = (
                            infrastructure_failure or _is_infrastructure_failure(event)
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
        infrastructure_retries = 0
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
                    command,
                    input_text,
                    append_log=continuing_run or bool(invocation_count),
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
                        infrastructure_failure = False
                        capacity_failure = False
                        await self.state.update_run(run, thread_id=None)
                        continue
                if exit_code == 0 or thread_id is None:
                    if (
                        exit_code != 0
                        and infrastructure_failure
                        and infrastructure_retries < self.config.settings.capacity_resume_attempts
                    ):
                        infrastructure_retries += 1
                        activity.retry(
                            f"infrastructure retry {infrastructure_retries}/"
                            f"{self.config.settings.capacity_resume_attempts}: restarting agent "
                            "after tool/session initialization failure"
                        )
                        await self.state.activities.save_async(activity)
                        delay = _capacity_resume_delay(
                            self.config.settings.capacity_resume_delay_seconds,
                            self.config.settings.capacity_resume_max_delay_seconds,
                            infrastructure_retries,
                        )
                        remaining = attempt_deadline - asyncio.get_running_loop().time()
                        if delay >= remaining:
                            if remaining > 0:
                                await asyncio.sleep(remaining)
                            timed_out = True
                            exit_code = 124
                            break
                        await asyncio.sleep(delay)
                        invocation_error = ""
                        fatal_invocation_failure = False
                        infrastructure_failure = False
                        capacity_failure = False
                        report = {}
                        continue
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
            if beam_session is not None:
                close_task = asyncio.create_task(beam_session.close())
                try:
                    await asyncio.shield(close_task)
                except asyncio.CancelledError:
                    await close_task
                    raise
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
        elif infrastructure_failure and exit_code != 0:
            error = invocation_error or "agent tool/session initialization retries exhausted"
        elif exit_code != 0 and invocation_error and not timed_out:
            error = invocation_error
        if exit_code == 0 and not report:
            error = "Codex returned no structured final report"
        succeeded = exit_code == 0 and bool(report)
        activity.finish("succeeded" if succeeded else "failed", error)
        await self.state.activities.save_async(activity)
        if succeeded and report.get("complete") is True:
            run_status = TaskStatus.SUCCEEDED
        elif succeeded:
            # The process completed, but the assignment remains coordinator work.
            run_status = TaskStatus.BLOCKED
        else:
            run_status = TaskStatus.FAILED
        await self.state.finish_run(
            run,
            status=run_status,
            exit_code=exit_code,
            changed=changed,
            placeholders=placeholders,
            report=report or None,
            usage=usage,
            thread_id=thread_id,
            failure_kind=("infrastructure" if infrastructure_failure and exit_code != 0 else ""),
            error=error,
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
            infrastructure_failed=infrastructure_failure and exit_code != 0,
        )
        if fatal_invocation_failure and exit_code != 0:
            raise FatalCodexInvocationError(error or "Codex rejected the invocation")
        return result


def _bounded_validation_output(output: str, maximum: int = 20_000) -> str:
    """Bound human-facing build output while preserving both endpoints."""

    if len(output) <= maximum:
        return output
    marker = "\n\n... coordinator build output omitted; see raw_log_path ...\n\n"
    available = maximum - len(marker)
    head = available // 3
    return output[:head] + marker + output[-(available - head) :]


COORDINATOR_BUILD_LOG_LIMIT = 100


def _prune_coordinator_build_logs(logs_dir: Path, newest: Path) -> None:
    """Retain the newest coordinator build log and 99 recent predecessors."""

    previous: list[tuple[int, str, Path]] = []
    for path in logs_dir.glob("coordinator-build-*.log"):
        if path == newest:
            continue
        try:
            previous.append((path.stat().st_mtime_ns, path.name, path))
        except FileNotFoundError:
            continue
    previous.sort(reverse=True)
    for _, _, path in previous[COORDINATOR_BUILD_LOG_LIMIT - 1 :]:
        path.unlink(missing_ok=True)


async def validate(
    config: PipelineConfig,
    chapter: WorkUnitLike,
    *,
    workspace_root: Path | None = None,
    on_output: Callable[[str], None] | None = None,
) -> ValidationResult:
    root = workspace_root or config.settings.repo
    logs_dir = config.settings.state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    raw_log_path = logs_dir / f"coordinator-build-{uuid4().hex[:12]}.log"
    raw_log_path.touch(exist_ok=False)
    _prune_coordinator_build_logs(logs_dir, raw_log_path)
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
    with raw_log_path.open("wb") as raw_log:
        try:
            async with asyncio.timeout(config.settings.validation_timeout_seconds):
                while line := await process.stdout.readline():
                    output_parts.append(line)
                    raw_log.write(line)
                    if on_output is not None:
                        on_output(line.decode(errors="replace"))
                await process.wait()
            process_exit_code = process.returncode or 0
            timed_out = False
        except TimeoutError:
            await _terminate(process)
            timeout_message = b"validation timed out"
            output_parts.append(timeout_message)
            raw_log.write(timeout_message)
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
    diagnostics = lean_diagnostics(complete_output)
    failed_modules = failed_lean_modules(complete_output)
    exit_code = process_exit_code
    display_output = complete_output
    if warnings:
        warning_summary = "\n".join(warnings[-50:])
        display_output = (
            f"{display_output}\n\nCoordinator rejected {len(warnings)} non-sorry Lean warning(s):\n"
            f"{warning_summary}"
        )
        if exit_code == 0:
            exit_code = 1
    return ValidationResult(
        exit_code == 0 and not warnings,
        exit_code,
        _bounded_validation_output(display_output),
        timed_out,
        process_exit_code,
        diagnostics=diagnostics,
        failed_modules=failed_modules,
        raw_log_path=str(raw_log_path),
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
