from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from paf import json_codec as json
from paf.activity import ActivityStore, shorten_book_paths
from paf.diagnostics import lean_diagnostic_counts
from paf.hashing import (
    digest_bytes,
    digest_text,
    migrate_digest_bytes,
    stable_digest_bytes,
    stable_digest_text,
    tagged_digest_bytes,
)
from paf.models import PipelineConfig, Stage, WorkUnitLike
from paf.pricing import LEGACY_MODEL, CostEstimate, estimate_cost
from paf.state_db import DATABASE_NAME, DatabaseWrite, StateDatabase, StateWriter

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
LAKE_PROGRESS_RE = re.compile(r"\[(?P<completed>\d+)/(?P<total>\d+)\]\s+\S+\s+(?P<target>\S+)")


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    INTERRUPTED = "interrupted"


class TaskPhase(StrEnum):
    IDLE = "idle"
    AGENT = "agent"
    POSTPROCESS = "postprocess"


class RepairCaseStatus(StrEnum):
    OPEN = "open"
    PLANNED = "planned"
    REPAIRING = "repairing"
    RESOLVED = "resolved"
    EXHAUSTED = "exhausted"


class RepairWorkUnitStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    SUPERSEDED = "superseded"


class UpstreamRequestStatus(StrEnum):
    """Completed durable facts for a missing interface in an earlier chapter."""

    REQUESTED = "requested"
    ANSWERED = "answered"
    CLOSED = "closed"
    ESCALATED = "escalated"


class ProofBlockerStatus(StrEnum):
    OPEN = "open"
    UPSTREAM_REQUESTED = "upstream_requested"
    REVIEW_REQUESTED = "review_requested"
    BLOCKED = "blocked"
    RESOLVED = "resolved"


@dataclass
class TokenUsage:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    reasoning_output_tokens: int = 0
    measured: bool = False

    @property
    def total_tokens(self) -> int:
        """Total input plus output; cached and reasoning tokens are already subsets."""
        return self.input_tokens + self.output_tokens

    def __add__(self, other: TokenUsage) -> TokenUsage:
        return TokenUsage(
            input_tokens=self.input_tokens + other.input_tokens,
            cached_input_tokens=self.cached_input_tokens + other.cached_input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            reasoning_output_tokens=self.reasoning_output_tokens + other.reasoning_output_tokens,
            measured=self.measured or other.measured,
        )

    def delta_from(self, previous: TokenUsage) -> TokenUsage:
        """Return a non-negative delta between cumulative usage counters."""

        return TokenUsage(
            input_tokens=max(0, self.input_tokens - previous.input_tokens),
            cached_input_tokens=max(0, self.cached_input_tokens - previous.cached_input_tokens),
            output_tokens=max(0, self.output_tokens - previous.output_tokens),
            reasoning_output_tokens=max(
                0,
                self.reasoning_output_tokens - previous.reasoning_output_tokens,
            ),
            measured=self.measured or previous.measured,
        )

    @classmethod
    def from_event(cls, event: Any) -> TokenUsage | None:
        aliases = {
            "input_tokens": "input_tokens",
            "inputTokens": "input_tokens",
            "cached_input_tokens": "cached_input_tokens",
            "cachedInputTokens": "cached_input_tokens",
            "output_tokens": "output_tokens",
            "outputTokens": "output_tokens",
            "reasoning_output_tokens": "reasoning_output_tokens",
            "reasoningOutputTokens": "reasoning_output_tokens",
        }

        def visit(value: Any) -> TokenUsage | None:
            if isinstance(value, dict):
                found: dict[str, int] = {}
                for key, target in aliases.items():
                    item = value.get(key)
                    if isinstance(item, int) and not isinstance(item, bool):
                        found[target] = item
                if "input_tokens" in found or "output_tokens" in found:
                    return cls(**found, measured=True)
                preferred = ("usage", "total_token_usage", "token_usage", "info", "payload")
                for key in preferred:
                    if key in value and (usage := visit(value[key])) is not None:
                        return usage
                for item in value.values():
                    if (usage := visit(item)) is not None:
                        return usage
            elif isinstance(value, list):
                for item in value:
                    if (usage := visit(item)) is not None:
                        return usage
            return None

        return visit(event)


@dataclass
class RunRecord:
    id: str
    chapter_id: str
    stage: str
    round: int
    model: str | None = None
    status: str = TaskStatus.RUNNING
    started_at: str = field(default_factory=timestamp)
    finished_at: str | None = None
    pid: int | None = None
    thread_id: str | None = None
    prompt_kind: str = ""
    resumed_from_run_id: str = ""
    exit_code: int | None = None
    changed: bool | None = None
    placeholders: int | None = None
    report: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    isolation: dict[str, Any] | None = None
    usage: TokenUsage = field(default_factory=TokenUsage)
    cumulative_usage: TokenUsage | None = None
    log_path: str | None = None
    role: str = ""
    auxiliary: bool = False
    request_ids: list[str] = field(default_factory=list)
    proof_targets: list[dict[str, Any]] = field(default_factory=list)
    source: str = ""
    source_start_line: int = 1
    source_end_line: int = 1
    project_root: str = ""

    @property
    def work_unit_id(self) -> str:
        return self.chapter_id


@dataclass
class TaskRecord:
    chapter_id: str
    book_id: str
    chapter_number: int
    chapter_title: str
    stage: str
    source: str = ""
    source_start_line: int = 1
    source_end_line: int = 1
    status: str = TaskStatus.PENDING
    phase: str = TaskPhase.IDLE
    detail: str = ""
    # Transient scheduling state: the task is runnable and is waiting for an
    # agent-capacity slot. It remains pending until a run is actually started.
    queued: bool = False
    # Repair work is an overlay on the existing four-stage state machine. The
    # underlying task remains failed/blocked until the repair is validated.
    repairing: bool = False
    repair_work_unit_id: str = ""
    # Set by explicit failed-task retry and propagated through tasks released from its fallout.
    # A later success uses this marker to reopen only causally blocked work.
    recovering_failure: bool = False
    # Proof completion is tied to the exact validated chapter sources. This is
    # populated only on the prove task.
    source_digest: str | None = None
    rounds: int = 0
    updated_at: str = field(default_factory=timestamp)
    runs: list[RunRecord] = field(default_factory=list)

    @property
    def work_unit_id(self) -> str:
        return self.chapter_id

    @property
    def document_id(self) -> str:
        return self.book_id

    @property
    def ordinal(self) -> int:
        return self.chapter_number

    @property
    def unit_title(self) -> str:
        return self.chapter_title


@dataclass
class ShepherdRecord:
    enabled: bool = False
    status: str = "idle"
    model: str = ""
    worker_model: str = ""
    interval_seconds: float = 1200.0
    failure_threshold: int = 10
    current_sweep_id: str = ""
    current_run_id: str = ""
    last_started_at: str | None = None
    last_finished_at: str | None = None
    next_run_at: str | None = None
    last_summary: str = ""
    last_error: str = ""
    pending_failures: int = 0
    planned_units: int = 0
    running_units: int = 0
    succeeded_units: int = 0
    failed_units: int = 0


@dataclass
class RepairCaseRecord:
    id: str
    task_key: str
    chapter_id: str
    stage: str
    fingerprint: str
    status: str = RepairCaseStatus.OPEN
    opened_at: str = field(default_factory=timestamp)
    updated_at: str = field(default_factory=timestamp)
    sweep_id: str = ""
    work_unit_ids: list[str] = field(default_factory=list)


@dataclass
class RepairSweepRecord:
    id: str
    status: str = "planning"
    trigger: str = ""
    failure_count: int = 0
    started_at: str = field(default_factory=timestamp)
    finished_at: str | None = None
    run_id: str = ""
    summary: str = ""
    error: str = ""
    case_ids: list[str] = field(default_factory=list)
    work_unit_ids: list[str] = field(default_factory=list)


@dataclass
class RepairWorkUnitRecord:
    id: str
    sweep_id: str
    case_ids: list[str]
    task_keys: list[str]
    owner_chapter_id: str
    target_stage: str
    objective: str
    depends_on: list[str] = field(default_factory=list)
    effort: str = "medium"
    priority: float = 0.0
    status: str = RepairWorkUnitStatus.PENDING
    detail: str = ""
    run_id: str = ""
    created_at: str = field(default_factory=timestamp)
    started_at: str | None = None
    finished_at: str | None = None


@dataclass
class CoordinatorBuildRecord:
    active: bool = False
    mode: str = ""
    stage: str = ""
    iteration: int = 0
    maximum_iterations: int = 0
    completed: int = 0
    total: int = 0
    target_chapter_ids: list[str] = field(default_factory=list)
    current_chapter_id: str = ""
    error_count: int = 0
    warning_count: int = 0
    output_tail: list[str] = field(default_factory=list)
    updated_at: str = field(default_factory=timestamp)

    @property
    def target_work_unit_ids(self) -> list[str]:
        return self.target_chapter_ids

    @target_work_unit_ids.setter
    def target_work_unit_ids(self, value: list[str]) -> None:
        self.target_chapter_ids = value

    @property
    def current_work_unit_id(self) -> str:
        return self.current_chapter_id

    @current_work_unit_id.setter
    def current_work_unit_id(self, value: str) -> None:
        self.current_chapter_id = value


@dataclass
class SourceIssueRecord:
    id: str
    chapter_id: str
    book_id: str
    chapter_number: int
    chapter_title: str
    source: str
    location: str
    source_excerpt: str
    description: str
    suggested_correction: str
    source_start_line: int = 1
    source_end_line: int = 1
    status: str = "open"
    first_seen_at: str = field(default_factory=timestamp)
    last_seen_at: str = field(default_factory=timestamp)
    sightings: int = 1
    stages: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)

    @property
    def work_unit_id(self) -> str:
        return self.chapter_id

    @property
    def document_id(self) -> str:
        return self.book_id

    @property
    def ordinal(self) -> int:
        return self.chapter_number

    @property
    def unit_title(self) -> str:
        return self.chapter_title


@dataclass(frozen=True)
class ChangeSet:
    revision: int
    work_units: frozenset[str] = frozenset()
    runs: frozenset[str] = frozenset()
    globals: frozenset[str] = frozenset()
    stages: frozenset[str] = frozenset()
    full_resync: bool = False


class ChangeBus:
    """Bounded in-process notifications for projection consumers."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[ChangeSet]] = set()

    def subscribe(self, *, maximum: int = 256) -> asyncio.Queue[ChangeSet]:
        queue: asyncio.Queue[ChangeSet] = asyncio.Queue(maxsize=maximum)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[ChangeSet]) -> None:
        self._subscribers.discard(queue)

    def publish(self, change: ChangeSet) -> None:
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(change)
            except asyncio.QueueFull:
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(ChangeSet(revision=change.revision, full_resync=True))


class StateStore:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.database_path = config.settings.state_dir / DATABASE_NAME
        self.path = self.database_path
        self.legacy_path = config.settings.state_dir / "state.json"
        self.source_issues_path = config.settings.state_dir / "source-issues.json"
        self.logs_dir = config.settings.state_dir / "logs"
        self.change_bus = ChangeBus()
        self.activities = ActivityStore(self.logs_dir, on_visible_change=self._activity_changed)
        self._database = StateDatabase(config.settings.state_dir)
        self._writer = StateWriter(self._database)
        self.revision = 0
        self._flush_lock = asyncio.Lock()
        self._telemetry_flush_task: asyncio.Task[None] | None = None
        self._batch_depth = 0
        self._checkpoint_dirty = False
        self._coordinator_build_dirty = False
        self._static_dirty = False
        self._dirty_task_keys: set[str] = set()
        self._issues_dirty = False
        self._dirty_run_ids: set[str] = set()
        self._prior_run_ids: set[str] = set()
        self._runs_by_id: dict[str, RunRecord] = {}
        self._payload_loaded_run_ids: set[str] = set()
        self._chapter_runs: dict[str, list[RunRecord]] = {}
        self._latest_runs_by_chapter: dict[str, RunRecord] = {}
        self._usage_cache: dict[tuple[bool, str | None], TokenUsage] = {}
        self._cost_cache: dict[tuple[bool, str | None], CostEstimate] = {}
        self._task_snapshot_context_key: tuple[int, ...] | None = None
        self._task_snapshot_context_cache: dict[str, Any] | None = None
        self._indexed_task_states: dict[str, tuple[str, str, bool, str, bool]] = {}
        self._repairable_task_keys: set[str] = set()
        self._active_run_ids: set[str] = set()
        self._active_runs_by_chapter: dict[str, RunRecord] = {}
        self._stage_count_cache: dict[str, dict[str, int]] = {}
        self.tasks: dict[str, TaskRecord] = {}
        self.source_issues: dict[str, SourceIssueRecord] = {}
        self.scheduling: dict[str, Any] = {}
        self.source_dependency_tree: dict[str, Any] = {}
        self.formalize_graph: dict[str, Any] = {}
        self.fixup_requests: dict[str, dict[str, Any]] = {}
        self.proof_review_requests: dict[str, dict[str, Any]] = {}
        self.upstream_requests: dict[str, dict[str, Any]] = {}
        self.proof_blockers: dict[str, dict[str, Any]] = {}
        self.thread_cumulative_usage: dict[str, TokenUsage] = {}
        self.isolation: dict[str, Any] = {}
        self.coordinator_build = CoordinatorBuildRecord()
        shepherd = config.shepherd
        self.shepherd = ShepherdRecord(
            enabled=shepherd.enabled,
            model=shepherd.model,
            worker_model=shepherd.worker_model,
            interval_seconds=shepherd.interval_seconds,
            failure_threshold=shepherd.failure_threshold,
        )
        self.repair_cases: dict[str, RepairCaseRecord] = {}
        self.repair_sweeps: dict[str, RepairSweepRecord] = {}
        self.repair_work_units: dict[str, RepairWorkUnitRecord] = {}
        self.created_at = timestamp()
        self.updated_at = self.created_at
        self._config_fingerprint = ""

    @staticmethod
    def key(chapter_id: str, stage: Stage) -> str:
        return f"{chapter_id}:{stage.value}"

    def _activity_changed(self, activity: Any) -> None:
        self.change_bus.publish(
            ChangeSet(
                revision=self.revision,
                work_units=frozenset({str(activity.work_unit_id)}),
                runs=frozenset({str(activity.run_id)}),
                globals=frozenset({"activity"}),
            )
        )

    @property
    def fixup_graph(self) -> dict[str, Any]:
        """Compatibility view of the pre-discovery build-freshness field."""

        return self.formalize_graph

    @fixup_graph.setter
    def fixup_graph(self, value: dict[str, Any]) -> None:
        self.formalize_graph = value

    async def load_or_create(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._database.initialize)
        self.revision = await asyncio.to_thread(self._database.revision)
        self._writer.start()
        raw, persisted_runs, persisted_source_issues = await asyncio.to_thread(self._database.load)
        if raw is not None:
            self.created_at = str(raw.get("created_at", timestamp()))
            self.updated_at = str(raw.get("updated_at", self.created_at))
            if not self.scheduling and isinstance(raw.get("scheduling"), dict):
                self.scheduling = raw["scheduling"]
            if not self.source_dependency_tree and isinstance(
                raw.get("source_dependency_tree"), dict
            ):
                self.source_dependency_tree = raw["source_dependency_tree"]
            if not self.formalize_graph and isinstance(raw.get("formalize_graph"), dict):
                self.formalize_graph = raw["formalize_graph"]
            if not self.formalize_graph and isinstance(raw.get("fixup_graph"), dict):
                self.formalize_graph = raw["fixup_graph"]
            raw_fixup_requests = raw.get("fixup_requests")
            if isinstance(raw_fixup_requests, dict):
                self.fixup_requests = {
                    request_id: dict(value)
                    for request_id, value in raw_fixup_requests.items()
                    if isinstance(request_id, str) and isinstance(value, dict)
                }
            raw_proof_review_requests = raw.get("proof_review_requests")
            if isinstance(raw_proof_review_requests, dict):
                self.proof_review_requests = {
                    request_id: dict(value)
                    for request_id, value in raw_proof_review_requests.items()
                    if isinstance(request_id, str) and isinstance(value, dict)
                }
            raw_upstream_requests = raw.get("upstream_requests")
            if isinstance(raw_upstream_requests, dict):
                self.upstream_requests = {
                    request_id: dict(value)
                    for request_id, value in raw_upstream_requests.items()
                    if isinstance(request_id, str) and isinstance(value, dict)
                }
            raw_proof_blockers = raw.get("proof_blockers")
            if isinstance(raw_proof_blockers, dict):
                self.proof_blockers = {
                    blocker_id: dict(value)
                    for blocker_id, value in raw_proof_blockers.items()
                    if isinstance(blocker_id, str) and isinstance(value, dict)
                }
            raw_thread_usage = raw.get("thread_cumulative_usage")
            if isinstance(raw_thread_usage, dict):
                self.thread_cumulative_usage = {
                    thread_id: TokenUsage(**value)
                    for thread_id, value in raw_thread_usage.items()
                    if isinstance(thread_id, str) and isinstance(value, dict)
                }
            if not self.isolation and isinstance(raw.get("isolation"), dict):
                self.isolation = raw["isolation"]
            raw_build = raw.get("coordinator_build")
            if isinstance(raw_build, dict):
                raw_build = dict(raw_build)
                raw_build.setdefault(
                    "target_chapter_ids", raw_build.get("target_work_unit_ids", [])
                )
                raw_build.setdefault(
                    "current_chapter_id", raw_build.get("current_work_unit_id", "")
                )
                self.coordinator_build = CoordinatorBuildRecord(
                    **{
                        name: value
                        for name, value in raw_build.items()
                        if name in CoordinatorBuildRecord.__dataclass_fields__
                    }
                )
            raw_shepherd = raw.get("shepherd")
            if isinstance(raw_shepherd, dict):
                persisted = {
                    name: value
                    for name, value in raw_shepherd.items()
                    if name in ShepherdRecord.__dataclass_fields__
                }
                self.shepherd = ShepherdRecord(**persisted)
                # Configuration, rather than old state, controls whether and
                # how often a newly launched orchestrator runs the Shepherd.
                self.shepherd.enabled = self.config.shepherd.enabled
                self.shepherd.model = self.config.shepherd.model
                self.shepherd.worker_model = self.config.shepherd.worker_model
                self.shepherd.interval_seconds = self.config.shepherd.interval_seconds
                self.shepherd.failure_threshold = self.config.shepherd.failure_threshold
            raw_cases = raw.get("repair_cases")
            if isinstance(raw_cases, dict):
                for record_id, value in raw_cases.items():
                    if not isinstance(record_id, str) or not isinstance(value, dict):
                        continue
                    fields = {
                        name: item
                        for name, item in value.items()
                        if name in RepairCaseRecord.__dataclass_fields__
                    }
                    fields.setdefault("id", record_id)
                    try:
                        self.repair_cases[record_id] = RepairCaseRecord(**fields)
                    except (TypeError, ValueError):
                        continue
            raw_sweeps = raw.get("repair_sweeps")
            if isinstance(raw_sweeps, dict):
                for record_id, value in raw_sweeps.items():
                    if not isinstance(record_id, str) or not isinstance(value, dict):
                        continue
                    fields = {
                        name: item
                        for name, item in value.items()
                        if name in RepairSweepRecord.__dataclass_fields__
                    }
                    fields.setdefault("id", record_id)
                    try:
                        self.repair_sweeps[record_id] = RepairSweepRecord(**fields)
                    except (TypeError, ValueError):
                        continue
            raw_units = raw.get("repair_work_units")
            if isinstance(raw_units, dict):
                for record_id, value in raw_units.items():
                    if not isinstance(record_id, str) or not isinstance(value, dict):
                        continue
                    fields = {
                        name: item
                        for name, item in value.items()
                        if name in RepairWorkUnitRecord.__dataclass_fields__
                    }
                    fields.setdefault("id", record_id)
                    try:
                        self.repair_work_units[record_id] = RepairWorkUnitRecord(**fields)
                    except (TypeError, ValueError):
                        continue
            persisted_tasks = raw.get("tasks", {})
            legacy_workflow = isinstance(persisted_tasks, dict) and any(
                isinstance(key, str) and key.endswith(":fixup") for key in persisted_tasks
            )
            if isinstance(persisted_tasks, dict):
                for key, value in persisted_tasks.items():
                    if not isinstance(key, str) or not isinstance(value, dict):
                        continue
                    if legacy_workflow and key.endswith(":formalize"):
                        key = f"{key[: -len(':formalize')]}:discover"
                    elif key.endswith(":repair") or key.endswith(":fixup"):
                        suffix = ":repair" if key.endswith(":repair") else ":fixup"
                        key = f"{key[: -len(suffix)]}:formalize"
                    task_value = {
                        name: item
                        for name, item in value.items()
                        if name in TaskRecord.__dataclass_fields__ and name != "runs"
                    }
                    task_value.setdefault("chapter_id", value.get("work_unit_id"))
                    task_value.setdefault("book_id", value.get("document_id"))
                    task_value.setdefault("chapter_number", value.get("ordinal"))
                    task_value.setdefault("chapter_title", value.get("unit_title"))
                    if legacy_workflow and task_value.get("stage") == "formalize":
                        task_value["stage"] = "discover"
                    elif task_value.get("stage") in {"repair", "fixup"}:
                        task_value["stage"] = "formalize"
                    legacy_review_green = value.get("review_green")
                    if task_value.get("stage") == Stage.REVIEW:
                        if legacy_review_green is True:
                            task_value["status"] = TaskStatus.SUCCEEDED
                        elif (
                            legacy_review_green is False
                            and task_value.get("status") == TaskStatus.SUCCEEDED
                        ):
                            task_value["status"] = TaskStatus.PENDING
                    self.tasks[key] = TaskRecord(**task_value)
        for value in persisted_source_issues:
            if isinstance(value, dict):
                issue_value = dict(value)
                issue_value.setdefault("chapter_id", issue_value.get("work_unit_id"))
                issue_value.setdefault("book_id", issue_value.get("document_id"))
                issue_value.setdefault("chapter_number", issue_value.get("ordinal"))
                issue_value.setdefault("chapter_title", issue_value.get("unit_title"))
                issue = SourceIssueRecord(
                    **{
                        name: item
                        for name, item in issue_value.items()
                        if name in SourceIssueRecord.__dataclass_fields__
                    }
                )
                self.source_issues[issue.id] = issue
        configured = {chapter.id for chapter in self.config.work_units}
        self.tasks = {
            key: task for key, task in self.tasks.items() if task.chapter_id in configured
        }
        self.repair_cases = {
            key: value for key, value in self.repair_cases.items() if value.chapter_id in configured
        }
        self.repair_work_units = {
            key: value
            for key, value in self.repair_work_units.items()
            if value.owner_chapter_id in configured
            and all(case_id in self.repair_cases for case_id in value.case_ids)
        }
        for chapter in self.config.work_units:
            for stage in Stage:
                key = self.key(chapter.id, stage)
                self.tasks.setdefault(key, self._new_task(chapter, stage))
                task = self.tasks[key]
                if not task.source:
                    task.source = chapter.source.as_posix()
                    task.source_start_line = chapter.source_span.start_line
                    task.source_end_line = chapter.source_span.end_line
            formalize = self.task(chapter.id, Stage.FORMALIZE)
            review = self.task(chapter.id, Stage.REVIEW)
            prove = self.task(chapter.id, Stage.PROVE)
            if formalize.status != TaskStatus.SUCCEEDED and (
                review.rounds > 0
                or prove.rounds > 0
                or review.status in (TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
                or prove.status in (TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
            ):
                formalize.status = TaskStatus.SUCCEEDED
                formalize.phase = TaskPhase.IDLE
                formalize.detail = "formalization completed before review"
                formalize.updated_at = timestamp()
        for value in persisted_runs:
            if not isinstance(value, dict):
                continue
            if legacy_workflow and value.get("stage") == "formalize":
                value["stage"] = "discover"
            elif value.get("stage") in {"repair", "fixup"}:
                value["stage"] = "formalize"
            value.setdefault("chapter_id", value.get("work_unit_id"))
            if str(value.get("chapter_id", "")) not in configured:
                continue
            usage_value = value.get("usage")
            usage = TokenUsage(**usage_value) if isinstance(usage_value, dict) else TokenUsage()
            cumulative_value = value.get("cumulative_usage")
            cumulative_usage = (
                TokenUsage(**cumulative_value) if isinstance(cumulative_value, dict) else None
            )
            fields = {
                name: item
                for name, item in value.items()
                if name in RunRecord.__dataclass_fields__
                and name not in {"usage", "cumulative_usage", "report", "validation", "isolation"}
            }
            run = RunRecord(
                **fields,
                usage=usage,
                cumulative_usage=cumulative_usage,
            )
            chapter = self.config.work_unit(run.chapter_id)
            if not run.source:
                run.source = chapter.source.as_posix()
                run.source_start_line = chapter.source_span.start_line
                run.source_end_line = chapter.source_span.end_line
            task = self.task(run.chapter_id, Stage(run.stage))
            task.runs.append(run)
            self._index_run(run)
        for task in self.tasks.values():
            task.runs.sort(key=lambda run: (run.started_at, run.id))
        for runs in self._chapter_runs.values():
            runs.sort(key=lambda run: (run.started_at, run.id))
        migrated_usage_runs = self._normalize_cumulative_thread_usage()
        self._prior_run_ids = set(self._runs_by_id)
        recovered_runs: list[RunRecord] = []
        for task in self.tasks.values():
            for run in task.runs:
                if run.status == TaskStatus.RUNNING:
                    run.status = TaskStatus.INTERRUPTED
                    run.finished_at = timestamp()
                    recovered_runs.append(run)
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.INTERRUPTED
                task.phase = TaskPhase.IDLE
                task.detail = "agent interrupted with the orchestrator"
            if task.queued:
                task.phase = TaskPhase.IDLE
                task.queued = False
                task.detail = "recovered after interrupted orchestrator"
            # A repair execution cannot survive its orchestrator process. Its
            # durable unit is requeued below, so clear the transient cell flag.
            task.repairing = False
            task.repair_work_unit_id = ""
        for unit in self.repair_work_units.values():
            if unit.status == RepairWorkUnitStatus.RUNNING:
                unit.status = RepairWorkUnitStatus.INTERRUPTED
                unit.detail = "repair worker interrupted with the orchestrator"
                unit.finished_at = timestamp()
        if self.shepherd.status in {"planning", "repairing"}:
            self.shepherd.status = "idle"
            self.shepherd.current_run_id = ""
            self.shepherd.running_units = 0
        if self.coordinator_build.active:
            self.coordinator_build.active = False
            self.coordinator_build.current_chapter_id = ""
            self.coordinator_build.updated_at = timestamp()
            self._coordinator_build_dirty = True
        self._normalize_upstream_request_state()
        self._invalidate_aggregates()
        self._rebuild_status_indexes()
        self._checkpoint_dirty = True
        config_payload = json.dumpb(
            {
                "documents": self._document_dicts(),
                "work_units": self._work_unit_dicts(),
            },
            sort_keys=True,
        )
        self._config_fingerprint = tagged_digest_bytes(config_payload)
        persisted_fingerprint = await asyncio.to_thread(self._database.config_fingerprint)
        matching_fingerprint = migrate_digest_bytes(persisted_fingerprint, config_payload)
        self._static_dirty = (
            matching_fingerprint is None or persisted_fingerprint != matching_fingerprint
        )
        self._dirty_task_keys.update(self.tasks)
        self._issues_dirty = True
        self._dirty_run_ids.update(run.id for run in (*recovered_runs, *migrated_usage_runs))
        await self.flush()

    def _normalize_cumulative_thread_usage(self) -> list[RunRecord]:
        """Migrate legacy per-run cumulative counters to thread-local deltas once."""

        previous: dict[str, TokenUsage] = {}
        migrated: list[RunRecord] = []
        ordered = sorted(self._runs_by_id.values(), key=lambda run: (run.started_at, run.id))
        for run in ordered:
            if not run.thread_id:
                continue
            if run.cumulative_usage is None:
                cumulative = run.usage
                run.cumulative_usage = cumulative
                run.usage = cumulative.delta_from(previous.get(run.thread_id, TokenUsage()))
                migrated.append(run)
            else:
                cumulative = run.cumulative_usage
            prior = previous.get(run.thread_id)
            if prior is None or cumulative.total_tokens >= prior.total_tokens:
                previous[run.thread_id] = cumulative
        for thread_id, usage in previous.items():
            persisted = self.thread_cumulative_usage.get(thread_id)
            if persisted is None or usage.total_tokens >= persisted.total_tokens:
                self.thread_cumulative_usage[thread_id] = usage
        return migrated

    def _new_task(self, chapter: WorkUnitLike, stage: Stage) -> TaskRecord:
        return TaskRecord(
            chapter_id=chapter.id,
            book_id=chapter.book_id,
            chapter_number=chapter.number,
            chapter_title=chapter.title,
            stage=stage.value,
            source=chapter.source.as_posix(),
            source_start_line=chapter.source_span.start_line,
            source_end_line=chapter.source_span.end_line,
        )

    @staticmethod
    def _usage_dict(usage: TokenUsage) -> dict[str, Any]:
        return {
            "input_tokens": usage.input_tokens,
            "cached_input_tokens": usage.cached_input_tokens,
            "output_tokens": usage.output_tokens,
            "reasoning_output_tokens": usage.reasoning_output_tokens,
            "measured": usage.measured,
        }

    def _run_dict(self, run: RunRecord, *, include_payload: bool = True) -> dict[str, Any]:
        value = {
            "id": run.id,
            "work_unit_id": run.work_unit_id,
            "chapter_id": run.chapter_id,
            "stage": str(run.stage),
            "round": run.round,
            "model": run.model,
            "status": str(run.status),
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "pid": run.pid,
            "thread_id": run.thread_id,
            "prompt_kind": run.prompt_kind,
            "resumed_from_run_id": run.resumed_from_run_id,
            "exit_code": run.exit_code,
            "changed": run.changed,
            "placeholders": run.placeholders,
            "usage": self._usage_dict(run.usage),
            "cumulative_usage": (
                self._usage_dict(run.cumulative_usage) if run.cumulative_usage is not None else None
            ),
            "log_path": run.log_path,
            "role": run.role,
            "auxiliary": run.auxiliary,
            "request_ids": list(run.request_ids),
            "proof_targets": [dict(target) for target in run.proof_targets],
            "source": run.source,
            "source_start_line": run.source_start_line,
            "source_end_line": run.source_end_line,
            "project_root": run.project_root,
        }
        if include_payload:
            value |= {
                "report": run.report,
                "validation": run.validation,
                "isolation": run.isolation,
            }
        return value

    def _task_snapshot_context(self) -> dict[str, Any]:
        """Precompute build-freshness sets shared by every projected task row."""

        clean_value = self.formalize_graph.get("clean", {})
        interfaces_value = self.formalize_graph.get("interfaces", {})
        dirty_value = self.formalize_graph.get("dirty", ())
        stale_value = self.formalize_graph.get("interface_stale", ())
        import_graph = self.formalize_graph.get("interface_import_graph", {})
        raw_edges = import_graph.get("edges", ()) if isinstance(import_graph, dict) else ()
        key = (
            id(self.formalize_graph),
            id(clean_value),
            id(interfaces_value),
            id(dirty_value),
            id(stale_value),
            id(import_graph),
            id(raw_edges),
        )
        if key == self._task_snapshot_context_key and self._task_snapshot_context_cache is not None:
            return self._task_snapshot_context_cache
        clean = clean_value if isinstance(clean_value, dict) else {}
        interfaces = interfaces_value if isinstance(interfaces_value, dict) else {}
        dirty = set(dirty_value) if isinstance(dirty_value, list) else set()
        stale = set(stale_value) if isinstance(stale_value, list) else set()
        successors: dict[str, list[str]] = {}
        if isinstance(raw_edges, list):
            for edge in raw_edges:
                if (
                    isinstance(edge, list)
                    and len(edge) == 2
                    and all(isinstance(item, str) for item in edge)
                ):
                    successors.setdefault(edge[0], []).append(edge[1])
        stale_dependencies = set(stale)
        pending = list(stale)
        while pending:
            chapter_id = pending.pop()
            for successor in successors.get(chapter_id, ()):
                if successor not in stale_dependencies:
                    stale_dependencies.add(successor)
                    pending.append(successor)
        context = {
            "clean": clean,
            "interfaces": interfaces,
            "dirty": dirty,
            "stale": stale,
            "stale_dependencies": stale_dependencies,
        }
        self._task_snapshot_context_key = key
        self._task_snapshot_context_cache = context
        return context

    def _task_dict(
        self,
        task: TaskRecord,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = context or self._task_snapshot_context()
        clean = context["clean"]
        interfaces = context["interfaces"]
        dirty = context["dirty"]
        stale = context["stale"]
        stale_dependencies = context["stale_dependencies"]
        interface_current = task.chapter_id in interfaces and task.chapter_id not in stale
        dependencies_current = task.chapter_id not in stale_dependencies
        if task.chapter_id in stale or task.chapter_id in dirty:
            head_build_status = "pending"
        elif task.chapter_id in clean:
            head_build_status = "clean"
        else:
            head_build_status = "unknown"
        proof_complete = (
            task.stage == Stage.PROVE
            and task.status == TaskStatus.SUCCEEDED
            and task.source_digest is not None
        )
        return {
            "work_unit_id": task.work_unit_id,
            "document_id": task.document_id,
            "ordinal": task.ordinal,
            "unit_title": task.unit_title,
            "chapter_id": task.chapter_id,
            "book_id": task.book_id,
            "chapter_number": task.chapter_number,
            "chapter_title": task.chapter_title,
            "stage": str(task.stage),
            "status": str(task.status),
            "phase": str(task.phase),
            "detail": task.detail,
            "queued": task.queued,
            "repairing": task.repairing,
            "repair_work_unit_id": task.repair_work_unit_id,
            "recovering_failure": task.recovering_failure,
            "source_digest": task.source_digest,
            "rounds": task.rounds,
            "updated_at": task.updated_at,
            "source": task.source,
            "source_start_line": task.source_start_line,
            "source_end_line": task.source_end_line,
            "proof_complete": proof_complete,
            "interface_current": interface_current,
            "dependencies_current": dependencies_current,
            "head_build_status": head_build_status,
            "fully_certified": (
                proof_complete
                and interface_current
                and dependencies_current
                and head_build_status == "clean"
            ),
        }

    @staticmethod
    def _issue_dict(issue: SourceIssueRecord) -> dict[str, Any]:
        return {name: getattr(issue, name) for name in SourceIssueRecord.__dataclass_fields__} | {
            "work_unit_id": issue.work_unit_id,
            "document_id": issue.document_id,
            "ordinal": issue.ordinal,
            "unit_title": issue.unit_title,
        }

    @staticmethod
    def _build_dict(build: CoordinatorBuildRecord) -> dict[str, Any]:
        return {
            name: getattr(build, name) for name in CoordinatorBuildRecord.__dataclass_fields__
        } | {
            "target_work_unit_ids": list(build.target_work_unit_ids),
            "current_work_unit_id": build.current_work_unit_id,
        }

    def _global_snapshot(self) -> dict[str, Any]:
        usage = self.total_usage()
        invocation_usage = self.invocation_usage()
        cost = self.total_cost()
        invocation_cost = self.invocation_cost()
        return {
            "version": 18,
            "history_database": DATABASE_NAME,
            "project_root": str(
                self.config.project.root
                if self.config.project is not None
                else self.config.settings.repo
            ),
            "config": str(self.config.path),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage": self._usage_dict(usage) | {"total_tokens": usage.total_tokens},
            "invocation_usage": self._usage_dict(invocation_usage)
            | {"total_tokens": invocation_usage.total_tokens},
            "cost": cost.as_dict(),
            "invocation_cost": invocation_cost.as_dict(),
            "agents": self.agent_summary(),
            "scheduling": self.scheduling,
            "source_dependency_tree": self.source_dependency_tree,
            "formalize_graph": self.formalize_graph,
            "fixup_requests": self.fixup_requests,
            "proof_review_requests": self.proof_review_requests,
            "upstream_requests": self.upstream_requests,
            "proof_blockers": self.proof_blockers,
            "thread_cumulative_usage": {
                thread_id: self._usage_dict(usage)
                for thread_id, usage in sorted(self.thread_cumulative_usage.items())
            },
            "upstream_request_batches": self.upstream_request_batches(),
            "isolation": self.isolation,
            "coordinator_build": self._build_dict(self.coordinator_build),
            "shepherd": {
                name: getattr(self.shepherd, name) for name in ShepherdRecord.__dataclass_fields__
            }
            | {"agents": self._shepherd_agent_views()},
            "repair_cases": {
                key: {name: getattr(value, name) for name in RepairCaseRecord.__dataclass_fields__}
                for key, value in sorted(self.repair_cases.items())
            },
            "repair_sweeps": {
                key: {name: getattr(value, name) for name in RepairSweepRecord.__dataclass_fields__}
                for key, value in sorted(self.repair_sweeps.items())
            },
            "repair_work_units": {
                key: {
                    name: getattr(value, name) for name in RepairWorkUnitRecord.__dataclass_fields__
                }
                for key, value in sorted(self.repair_work_units.items())
            },
        }

    def _shepherd_agent_views(self) -> list[dict[str, Any]]:
        """Return the planner and repair workers belonging to the current or latest sweep."""

        sweep = self.repair_sweeps.get(self.shepherd.current_sweep_id)
        if sweep is None and self.repair_sweeps:
            sweep = max(self.repair_sweeps.values(), key=lambda item: (item.started_at, item.id))
        if sweep is None:
            return []

        agents: list[dict[str, Any]] = []
        planner_run_id = sweep.run_id or (
            self.shepherd.current_run_id if self.shepherd.current_sweep_id == sweep.id else ""
        )
        if planner_run_id:
            run = self._runs_by_id.get(planner_run_id)
            agents.append(
                {
                    "run_id": planner_run_id,
                    "role": "shepherd",
                    "work_unit_id": run.chapter_id if run is not None else "",
                    "stage": run.stage if run is not None else "discover",
                    "status": run.status if run is not None else sweep.status,
                    "label": "Shepherd planner",
                    "repair_work_unit_id": "",
                    "objective": sweep.summary,
                }
            )
        for unit_id in sweep.work_unit_ids:
            unit = self.repair_work_units.get(unit_id)
            if unit is None:
                continue
            agents.append(
                {
                    "run_id": unit.run_id,
                    "role": "repair_worker",
                    "work_unit_id": unit.owner_chapter_id,
                    "stage": unit.target_stage,
                    "status": unit.status,
                    "label": f"Repair {unit.target_stage}",
                    "repair_work_unit_id": unit.id,
                    "objective": unit.objective,
                }
            )
        return agents

    def _document_dicts(self) -> list[dict[str, Any]]:
        return [
            {
                "id": document.id,
                "path": document.path.as_posix(),
                "format": document.format,
                "title": document.title,
                "depends_on": list(document.depends_on),
            }
            for document in self.config.documents
        ]

    def _work_unit_dicts(self) -> list[dict[str, Any]]:
        return [
            {
                "id": unit.id,
                "document_id": unit.document_id,
                "title": unit.title,
                "ordinal": unit.ordinal,
                "source": unit.source.as_posix(),
                "source_start_line": unit.source_span.start_line,
                "source_end_line": unit.source_span.end_line,
                "depends_on": list(unit.depends_on),
                "target_scope": list(unit.scope),
            }
            for unit in self.config.work_units
        ]

    def hot_snapshot(self) -> dict[str, Any]:
        task_context = self._task_snapshot_context()
        return self._global_snapshot() | {
            "documents": [dict(value) for value in self._document_dicts()],
            "work_units": [dict(value) for value in self._work_unit_dicts()],
            "tasks": {
                key: self._task_dict(task, task_context)
                | {
                    "run_count": len(task.runs),
                    "latest_run_id": task.runs[-1].id if task.runs else None,
                }
                for key, task in sorted(self.tasks.items())
            },
        }

    def dashboard_snapshot(self, *, maximum_activities: int = 36) -> dict[str, Any]:
        """Return the bounded, hot projection consumed by interactive dashboards.

        This deliberately excludes immutable run payloads.  A dashboard needs current task
        state and compact activity summaries, not every historical report and validation log.
        Keeping this contract on the state boundary also lets native and web clients share the
        same data model without depending on Python objects.
        """

        snapshot = self.hot_snapshot() | {
            "revision": self.revision,
            "source": str(self.database_path),
        }
        tasks = snapshot["tasks"]
        recent_run_ids: list[str] = []
        if isinstance(tasks, dict):
            ordered = sorted(
                (task for task in tasks.values() if isinstance(task, dict)),
                key=lambda task: str(task.get("updated_at", "")),
                reverse=True,
            )
            for task in ordered:
                run_id = task.get("latest_run_id")
                if isinstance(run_id, str) and run_id not in recent_run_ids:
                    recent_run_ids.append(run_id)
                if len(recent_run_ids) >= maximum_activities:
                    break
        for run_id in sorted(self._active_run_ids):
            if run_id not in recent_run_ids:
                recent_run_ids.append(run_id)
        shepherd_snapshot = snapshot.get("shepherd")
        if isinstance(shepherd_snapshot, dict):
            for agent in shepherd_snapshot.get("agents", []):
                run_id = agent.get("run_id") if isinstance(agent, dict) else None
                if isinstance(run_id, str) and run_id and run_id not in recent_run_ids:
                    recent_run_ids.append(run_id)
        snapshot["activities"] = {
            run_id: activity.as_dict()
            for run_id in recent_run_ids
            if (activity := self.activities.get(run_id)) is not None
        }
        tasks_value = snapshot.get("tasks")
        if isinstance(tasks_value, dict):
            for task in tasks_value.values():
                if not isinstance(task, dict):
                    continue
                work_unit_id = str(task.get("work_unit_id", ""))
                task["work_unit_usage"] = self._usage_dict(self.invocation_usage(work_unit_id))
                task["work_unit_cost"] = self.invocation_cost(work_unit_id).as_dict()
        return snapshot

    def dashboard_delta(self, change: ChangeSet) -> dict[str, Any]:
        """Project one in-process change notification into the dashboard wire model."""

        task_keys = sorted(
            key
            for work_unit_id in change.work_units
            for stage in Stage
            if (key := self.key(work_unit_id, stage)) in self.tasks
        )
        task_context = self._task_snapshot_context()
        tasks = {
            key: self._hot_task_dict(self.tasks[key], task_context)
            | {
                "work_unit_usage": self._usage_dict(
                    self.invocation_usage(self.tasks[key].work_unit_id)
                ),
                "work_unit_cost": self.invocation_cost(self.tasks[key].work_unit_id).as_dict(),
            }
            for key in task_keys
        }
        active_run_ids = sorted(self._active_run_ids)
        run_ids = set(change.runs) | set(active_run_ids)
        run_ids.update(
            run_id
            for task in tasks.values()
            if isinstance((run_id := task.get("latest_run_id")), str)
        )
        global_names = change.globals.difference({"activity"})
        if "state" in global_names:
            globals_ = self._global_snapshot() | {"revision": self.revision}
        else:
            globals_ = {}
            if "coordinator_build" in global_names:
                globals_["coordinator_build"] = self._build_dict(self.coordinator_build)
        if change.stages or change.runs:
            globals_["agents"] = self.agent_summary()
        shepherd = globals_.get("shepherd", {})
        if isinstance(shepherd, dict):
            run_ids.update(
                run_id
                for agent in shepherd.get("agents", [])
                if isinstance(agent, dict)
                and isinstance((run_id := agent.get("run_id")), str)
                and run_id
            )
        activities = {
            run_id: activity.as_dict()
            for run_id in sorted(run_ids)
            if (activity := self.activities.get(run_id)) is not None
        }
        changes = [
            *(
                {"revision": change.revision, "entity_type": "work_unit", "entity_id": unit_id}
                for unit_id in sorted(change.work_units)
            ),
            *(
                {"revision": change.revision, "entity_type": "run", "entity_id": run_id}
                for run_id in sorted(change.runs)
            ),
            *(
                {"revision": change.revision, "entity_type": "global", "entity_id": name}
                for name in sorted(change.globals)
            ),
        ]
        return {
            "revision": self.revision,
            "resync_required": change.full_resync,
            "changes": changes,
            "tasks": tasks,
            "removed_task_ids": [],
            "globals": globals_,
            "run_ids": sorted(change.runs),
            "active_run_ids": active_run_ids,
            "activities": activities,
        }

    def status_view(self) -> dict[str, Any]:
        """Return status fields and counters without materializing task rows."""

        counts = {status.value: 0 for status in TaskStatus}
        for stage in Stage:
            stage_counts = self.stage_counts(stage)
            for status in TaskStatus:
                counts[status.value] += stage_counts[status.value]
            counts[TaskStatus.PENDING] += stage_counts["queued"]
        return self._global_snapshot() | {
            "revision": self.revision,
            "task_counts": counts,
        }

    def snapshot(self) -> dict[str, Any]:
        """Return complete state, loading immutable run payloads on demand."""

        snapshot = self.hot_snapshot()
        task_context = self._task_snapshot_context()
        payloads = self._database.run_payloads() if self.database_path.is_file() else {}
        tasks: dict[str, Any] = {}
        for key, task in sorted(self.tasks.items()):
            runs = []
            for run in task.runs:
                value = payloads.get(run.id, self._run_dict(run))
                if (
                    run.report is not None
                    or run.validation is not None
                    or run.isolation is not None
                ):
                    value = self._run_dict(run)
                runs.append(value)
            tasks[key] = self._task_dict(task, task_context) | {"runs": runs}
        snapshot["source_issues"] = [
            self._issue_dict(issue) for _, issue in sorted(self.source_issues.items())
        ]
        snapshot["tasks"] = tasks
        return snapshot

    def _hot_task_dict(
        self,
        task: TaskRecord,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._task_dict(task, context) | {
            "run_count": len(task.runs),
            "latest_run_id": task.runs[-1].id if task.runs else None,
        }

    def _mark_dirty(
        self,
        *,
        task: TaskRecord | None = None,
        tasks: Iterable[TaskRecord] = (),
        run: RunRecord | None = None,
        issues: bool = False,
        static: bool = False,
        global_state: bool = True,
    ) -> None:
        self._checkpoint_dirty = self._checkpoint_dirty or global_state
        changed_tasks = [*tasks]
        if task is not None:
            changed_tasks.append(task)
        for item in changed_tasks:
            self._sync_task_index(item)
        self._dirty_task_keys.update(
            self.key(item.chapter_id, Stage(item.stage)) for item in changed_tasks
        )
        if run is not None:
            self._dirty_run_ids.add(run.id)
            if run.status == TaskStatus.RUNNING:
                self._active_run_ids.add(run.id)
                self._active_runs_by_chapter[run.chapter_id] = run
            else:
                self._active_run_ids.discard(run.id)
                if self._active_runs_by_chapter.get(run.chapter_id) is run:
                    replacement = next(
                        (
                            self._runs_by_id[item]
                            for item in self._active_run_ids
                            if self._runs_by_id[item].chapter_id == run.chapter_id
                        ),
                        None,
                    )
                    if replacement is None:
                        self._active_runs_by_chapter.pop(run.chapter_id, None)
                    else:
                        self._active_runs_by_chapter[run.chapter_id] = replacement
        self._issues_dirty = self._issues_dirty or issues
        self._static_dirty = self._static_dirty or static

    def _mark_coordinator_build_dirty(self) -> None:
        self._coordinator_build_dirty = True

    async def _persist(self) -> None:
        if self._batch_depth:
            return
        await asyncio.sleep(0)
        await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            if not (
                self._checkpoint_dirty
                or self._coordinator_build_dirty
                or self._dirty_task_keys
                or self._dirty_run_ids
                or self._issues_dirty
                or self._static_dirty
            ):
                return
            if not self.database_path.is_file():
                await asyncio.to_thread(self._database.initialize)
            self.updated_at = timestamp()
            globals_dirty = self._checkpoint_dirty
            coordinator_build_dirty = self._coordinator_build_dirty
            task_keys = self._dirty_task_keys
            dirty_runs = self._dirty_run_ids
            issues_dirty = self._issues_dirty
            static_dirty = self._static_dirty
            self._dirty_task_keys = set()
            self._dirty_run_ids = set()
            self._checkpoint_dirty = False
            self._coordinator_build_dirty = False
            self._issues_dirty = False
            self._static_dirty = False
            task_context = self._task_snapshot_context() if task_keys else None
            task_payloads = {
                key: json.dumpb(self._hot_task_dict(self.tasks[key], task_context))
                for key in sorted(task_keys)
                if key in self.tasks
            }
            runs = {
                run_id: (
                    self.key(run.chapter_id, Stage(run.stage)),
                    json.dumpb(
                        self._run_dict(run, include_payload=run.id in self._payload_loaded_run_ids)
                    ),
                )
                for run_id in sorted(dirty_runs)
                if (run := self._runs_by_id.get(run_id)) is not None
            }
            issues = (
                {
                    issue.id: json.dumpb(self._issue_dict(issue))
                    for _, issue in sorted(self.source_issues.items())
                }
                if issues_dirty
                else {}
            )
            documents = (
                {
                    value["id"]: (ordinal, json.dumpb(value))
                    for ordinal, value in enumerate(self._document_dicts())
                }
                if static_dirty
                else {}
            )
            work_units = (
                {
                    value["id"]: (
                        str(value["document_id"]),
                        int(value["ordinal"]),
                        str(value["title"]),
                        str(value["source"]),
                        json.dumpb(value),
                    )
                    for value in self._work_unit_dicts()
                }
                if static_dirty
                else {}
            )
            changed_work_units = {self.tasks[key].chapter_id for key in task_payloads} | {
                run.chapter_id for run_id in runs if (run := self._runs_by_id.get(run_id))
            }
            changed_stages = {self.tasks[key].stage for key in task_payloads} | {
                run.stage for run_id in runs if (run := self._runs_by_id.get(run_id))
            }
            changes = {
                *(("task", key) for key in task_payloads),
                *(("run", run_id) for run_id in runs),
                *(("work_unit", unit_id) for unit_id in changed_work_units),
            }
            if globals_dirty:
                changes.add(("global", "state"))
            if coordinator_build_dirty:
                changes.add(("global", "coordinator_build"))
            if issues_dirty:
                changes.add(("source_issues", "*"))
            if static_dirty:
                changes.add(("resync", "*"))
            write = DatabaseWrite(
                updated_at=self.updated_at,
                globals=({"state": json.dumpb(self._global_snapshot())} if globals_dirty else {})
                | (
                    {"coordinator_build": json.dumpb(self._build_dict(self.coordinator_build))}
                    if coordinator_build_dirty
                    else {}
                ),
                tasks=task_payloads,
                runs=runs,
                source_issues=issues,
                replace_source_issues=issues_dirty,
                documents=documents,
                work_units=work_units,
                config_fingerprint=self._config_fingerprint if static_dirty else None,
                replace_static=static_dirty,
                changes=frozenset(changes),
            )
            revision = await asyncio.wrap_future(self._writer.submit(write))
            assert revision is not None
            self.revision = revision
            self.change_bus.publish(
                ChangeSet(
                    revision=revision,
                    work_units=frozenset(changed_work_units),
                    runs=frozenset(runs),
                    globals=frozenset(
                        ({"state"} if globals_dirty else set())
                        | ({"coordinator_build"} if coordinator_build_dirty else set())
                    ),
                    stages=frozenset(changed_stages),
                    full_resync=static_dirty,
                )
            )

    async def save(self) -> None:
        self._mark_dirty()
        await self._persist()

    async def save_digest_migration(
        self,
        proof_source_digests: dict[str, str],
        *,
        current_discoveries: Iterable[str] = (),
    ) -> None:
        """Persist global cache-digest rewrites and matching proof-task rewrites atomically."""

        changed_tasks: list[TaskRecord] = []
        for chapter_id in current_discoveries:
            task = self.task(chapter_id, Stage.DISCOVER)
            if task.status not in {TaskStatus.PENDING, TaskStatus.INTERRUPTED}:
                continue
            task.status = TaskStatus.SUCCEEDED
            task.phase = TaskPhase.IDLE
            task.queued = False
            task.detail = "verified and migrated legacy source digest"
            task.updated_at = timestamp()
            changed_tasks.append(task)
        for chapter_id, source_digest in proof_source_digests.items():
            task = self.task(chapter_id, Stage.PROVE)
            if task.source_digest == source_digest:
                continue
            task.source_digest = source_digest
            changed_tasks.append(task)
        if changed_tasks:
            self._invalidate_status_summaries()
        self._mark_dirty(tasks=changed_tasks)
        await self._persist()

    def _normalize_upstream_request_state(self) -> None:
        """Migrate legacy request records to completed-fact durable states."""

        legacy_statuses = {
            "open": UpstreamRequestStatus.REQUESTED,
            "repairing": UpstreamRequestStatus.REQUESTED,
            "retrying": UpstreamRequestStatus.ANSWERED,
            "manual_escalation": UpstreamRequestStatus.ESCALATED,
        }
        valid_statuses = {status.value: status for status in UpstreamRequestStatus}
        for request_id, request in self.upstream_requests.items():
            created_at = str(request.get("created_at") or timestamp())
            request.setdefault("id", request_id)
            request.setdefault("created_at", created_at)
            request.setdefault("updated_at", created_at)
            for name in ("origin_run_ids", "owner_paths", "attempted_alternatives"):
                if not isinstance(request.get(name), list):
                    request[name] = []
            if not isinstance(request.get("previous_attempts"), str):
                request["previous_attempts"] = ""
            if request.get("answer") is not None and not isinstance(request.get("answer"), dict):
                request["answer"] = None

            raw_status = str(request.get("status", UpstreamRequestStatus.REQUESTED.value))
            status = valid_statuses.get(raw_status) or legacy_statuses.get(raw_status)
            if status is None:
                status = UpstreamRequestStatus.ESCALATED
                request["escalation_reason"] = "recovered an unknown upstream-request state"
                request.setdefault("escalated_at", timestamp())
            if status is UpstreamRequestStatus.ANSWERED and not isinstance(
                request.get("answer"), dict
            ):
                status = UpstreamRequestStatus.REQUESTED
            elif status is UpstreamRequestStatus.REQUESTED and isinstance(
                request.get("answer"), dict
            ):
                status = UpstreamRequestStatus.ANSWERED
            request["status"] = status.value

            # Older snapshots duplicated execution history that is already retained by RunRecord.
            for legacy_field in ("repair_attempts", "retry_attempts", "answers", "history"):
                request.pop(legacy_field, None)

    def upstream_request_batches(self) -> dict[str, list[str]]:
        """Group unanswered durable requests by their proposed owning chapter."""

        batches: dict[str, list[str]] = {}
        for request_id, request in sorted(self.upstream_requests.items()):
            if request.get("status") != UpstreamRequestStatus.REQUESTED.value:
                continue
            owner = request.get("owner_chapter_id")
            if isinstance(owner, str) and owner:
                batches.setdefault(owner, []).append(request_id)
        return batches

    @staticmethod
    def _proof_blocker_fingerprint(attempt: dict[str, Any]) -> str:
        """Fingerprint the stable obstruction, excluding verbose checked attempts."""

        fields = (
            str(attempt.get("path", "")).strip(),
            str(attempt.get("declaration", "")).strip(),
            str(attempt.get("remaining_goal", "")).strip(),
            str(attempt.get("obstruction", "")).strip(),
        )
        return stable_digest_text("\0".join(fields))[:20]

    @staticmethod
    def _transitional_proof_blocker_fingerprint(attempt: dict[str, Any]) -> str:
        fields = (
            str(attempt.get("path", "")).strip(),
            str(attempt.get("declaration", "")).strip(),
            str(attempt.get("remaining_goal", "")).strip(),
            str(attempt.get("obstruction", "")).strip(),
        )
        return digest_text("\0".join(fields))

    def proof_blockers_for_consumer(
        self,
        chapter_id: str,
        *,
        active_only: bool = True,
    ) -> tuple[dict[str, Any], ...]:
        return tuple(
            blocker
            for _, blocker in sorted(self.proof_blockers.items())
            if blocker.get("consumer_chapter_id") == chapter_id
            and (not active_only or blocker.get("status") == ProofBlockerStatus.OPEN.value)
        )

    async def record_proof_blockers(
        self,
        chapter_id: str,
        *,
        origin_run_id: str,
        failed_attempts: Iterable[dict[str, Any]],
        unchanged_ids: Iterable[str] = (),
        upstream_candidates: Iterable[dict[str, Any]] = (),
    ) -> tuple[dict[str, Any], ...]:
        """Merge proof-failure deltas into a restart-safe declaration ledger."""

        changed: dict[str, dict[str, Any]] = {}
        by_fingerprint = {
            str(value.get("fingerprint", "")): value
            for value in self.proof_blockers.values()
            if value.get("consumer_chapter_id") == chapter_id
        }
        candidates = tuple(value for value in upstream_candidates if isinstance(value, dict))
        for raw in failed_attempts:
            if not isinstance(raw, dict):
                continue
            fingerprint = self._proof_blocker_fingerprint(raw)
            blocker: dict[str, Any] | None = by_fingerprint.get(fingerprint)
            if blocker is None:
                blocker = by_fingerprint.get(self._transitional_proof_blocker_fingerprint(raw))
                if blocker is not None:
                    blocker["fingerprint"] = fingerprint
                    by_fingerprint[fingerprint] = blocker
            if blocker is None:
                prior_numbers = (int(key[1:]) for key in self.proof_blockers if key[1:].isdigit())
                blocker_id = f"B{1 + max(prior_numbers, default=0)}"
                now = timestamp()
                blocker = {
                    "id": blocker_id,
                    "fingerprint": fingerprint,
                    "consumer_chapter_id": chapter_id,
                    "path": str(raw.get("path", "")).strip(),
                    "declaration": str(raw.get("declaration", "")).strip(),
                    "remaining_goal": str(raw.get("remaining_goal", "")).strip(),
                    "obstruction": str(raw.get("obstruction", "")).strip(),
                    "disposition": str(raw.get("disposition", "retry")).strip(),
                    "attempts": [],
                    "origin_run_ids": [],
                    "sightings": 0,
                    "status": ProofBlockerStatus.OPEN.value,
                    "created_at": now,
                    "updated_at": now,
                }
                self.proof_blockers[blocker_id] = blocker
                by_fingerprint[fingerprint] = blocker
            attempts = blocker.setdefault("attempts", [])
            raw_attempts = raw.get("attempts")
            if isinstance(attempts, list) and isinstance(raw_attempts, list):
                for attempt in raw_attempts:
                    text = str(attempt).strip()
                    if text and text not in attempts:
                        attempts.append(text)
            origins = blocker.setdefault("origin_run_ids", [])
            if isinstance(origins, list) and origin_run_id not in origins:
                origins.append(origin_run_id)
                sightings = blocker.get("sightings", 0)
                blocker["sightings"] = (sightings if isinstance(sightings, int) else 0) + 1
            disposition = str(raw.get("disposition", "")).strip()
            if disposition:
                blocker["disposition"] = disposition
            for candidate in candidates:
                if (
                    str(candidate.get("blocked_declaration", "")).strip()
                    == blocker.get("declaration")
                    and str(candidate.get("consumer_path", "")).strip() == blocker.get("path")
                    and str(candidate.get("residual_goal", "")).strip()
                    == blocker.get("remaining_goal")
                ):
                    blocker["upstream_candidate"] = dict(candidate)
                    blocker["disposition"] = "missing_upstream"
                    break
            blocker["updated_at"] = timestamp()
            changed[str(blocker["id"])] = blocker

        for blocker_id in dict.fromkeys(unchanged_ids):
            blocker = self.proof_blockers.get(str(blocker_id))
            if not isinstance(blocker, dict) or blocker.get("consumer_chapter_id") != chapter_id:
                continue
            origins = blocker.setdefault("origin_run_ids", [])
            if isinstance(origins, list) and origin_run_id not in origins:
                origins.append(origin_run_id)
                sightings = blocker.get("sightings", 0)
                blocker["sightings"] = (sightings if isinstance(sightings, int) else 0) + 1
            blocker["updated_at"] = timestamp()
            changed[str(blocker["id"])] = blocker
        if changed:
            self._mark_dirty()
            await self._persist()
        return tuple(changed.values())

    async def set_proof_blocker_status(
        self,
        blocker_ids: Iterable[str],
        status: ProofBlockerStatus,
        *,
        request_id: str = "",
    ) -> None:
        changed = False
        for blocker_id in blocker_ids:
            blocker = self.proof_blockers.get(blocker_id)
            if not isinstance(blocker, dict):
                continue
            blocker["status"] = status.value
            blocker["updated_at"] = timestamp()
            if request_id:
                blocker["request_id"] = request_id
            changed = True
        if changed:
            self._mark_dirty()
            await self._persist()

    def upstream_requests_for_consumer(
        self,
        chapter_id: str,
        *,
        statuses: Iterable[UpstreamRequestStatus] | None = None,
    ) -> tuple[dict[str, Any], ...]:
        selected = set(statuses) if statuses is not None else None
        return tuple(
            request
            for _, request in sorted(self.upstream_requests.items())
            if request.get("consumer_chapter_id") == chapter_id
            and (selected is None or UpstreamRequestStatus(str(request.get("status"))) in selected)
        )

    async def enqueue_upstream_request(
        self,
        request: dict[str, Any],
        *,
        consumer_chapter_id: str,
        origin_run_id: str,
        owner_chapter_id: str,
        previous_attempts: str,
        escalation_reason: str = "",
    ) -> tuple[str, bool]:
        """Persist one proof-to-upstream handoff without collapsing its evidence."""

        fingerprint_fields = (
            consumer_chapter_id,
            str(request.get("blocked_declaration", "")).strip(),
            str(request.get("consumer_path", "")).strip(),
            str(request.get("needed_result", "")).strip(),
            owner_chapter_id,
        )
        identity = "\0".join(fingerprint_fields)
        fingerprint = stable_digest_text(identity)[:16]
        transitional_fingerprint = digest_text(identity)
        for request_id, existing in self.upstream_requests.items():
            if existing.get("fingerprint") not in {fingerprint, transitional_fingerprint}:
                continue
            if existing.get("status") == UpstreamRequestStatus.CLOSED.value:
                continue
            existing["fingerprint"] = fingerprint
            origin_run_ids = existing.setdefault("origin_run_ids", [])
            if isinstance(origin_run_ids, list) and origin_run_id not in origin_run_ids:
                origin_run_ids.append(origin_run_id)
            existing["sightings"] = int(existing.get("sightings", 1)) + 1
            existing["updated_at"] = timestamp()
            self._mark_dirty()
            await self._persist()
            return request_id, False

        request_id = uuid4().hex[:12]
        now = timestamp()
        status = (
            UpstreamRequestStatus.ESCALATED
            if escalation_reason
            else UpstreamRequestStatus.REQUESTED
        )
        attempted = request.get("attempted_alternatives")
        owner_paths = request.get("owner_paths")
        record: dict[str, Any] = {
            "id": request_id,
            "fingerprint": fingerprint,
            "status": status.value,
            "consumer_chapter_id": consumer_chapter_id,
            "origin_run_ids": [origin_run_id],
            "blocked_declaration": str(request.get("blocked_declaration", "")).strip(),
            "consumer_path": str(request.get("consumer_path", "")).strip(),
            "residual_goal": str(request.get("residual_goal", "")).strip(),
            "needed_result": str(request.get("needed_result", "")).strip(),
            "owner_chapter_id": owner_chapter_id,
            "proposed_owner_chapter_id": str(
                request.get("owner_chapter_id", owner_chapter_id)
            ).strip(),
            "owner_paths": sorted(
                {
                    str(path).strip()
                    for path in owner_paths
                    if isinstance(path, str) and path.strip()
                }
            )
            if isinstance(owner_paths, list)
            else [],
            "attempted_alternatives": [
                str(item).strip() for item in attempted if isinstance(item, str) and item.strip()
            ]
            if isinstance(attempted, list)
            else [],
            "previous_attempts": previous_attempts,
            "answer": None,
            "sightings": 1,
            "created_at": now,
            "updated_at": now,
        }
        if escalation_reason:
            record["escalation_reason"] = escalation_reason
            record["escalated_at"] = now
            record["escalated_by_run_id"] = origin_run_id
        self.upstream_requests[request_id] = record
        self._mark_dirty()
        await self._persist()
        return request_id, True

    async def record_upstream_answers(
        self,
        request_ids: Iterable[str],
        *,
        run_id: str | None,
        answers: dict[str, dict[str, Any]],
        error: str = "",
    ) -> None:
        """Persist completed repair answers or terminal repair failures."""

        changed = False
        async with self.batch():
            for request_id in request_ids:
                request = self.upstream_requests.get(request_id)
                if (
                    not isinstance(request, dict)
                    or request.get("status") != UpstreamRequestStatus.REQUESTED.value
                ):
                    continue
                answer = answers.get(request_id)
                if answer is None:
                    reason = error or "targeted upstream repair returned no usable answer"
                    request["status"] = UpstreamRequestStatus.ESCALATED.value
                    request["escalation_reason"] = reason
                    request["escalated_at"] = timestamp()
                    if run_id is not None:
                        request["escalated_by_run_id"] = run_id
                        request["repair_run_id"] = run_id
                    request["updated_at"] = timestamp()
                    changed = True
                    continue
                persisted_answer = dict(answer) | {"answered_at": timestamp()}
                if run_id is not None:
                    persisted_answer["repair_run_id"] = run_id
                request["answer"] = persisted_answer
                if run_id is not None:
                    request["repair_run_id"] = run_id
                request["status"] = UpstreamRequestStatus.ANSWERED.value
                request["updated_at"] = timestamp()
                request.pop("escalation_reason", None)
                request.pop("escalated_at", None)
                request.pop("escalated_by_run_id", None)
                changed = True
            if changed:
                self._mark_dirty()
                await self._persist()

    async def finish_upstream_requests(
        self,
        request_ids: Iterable[str],
        *,
        run_id: str | None,
        succeeded_ids: Iterable[str],
        error: str = "",
        success_detail: str = "blocked declaration succeeded in the targeted downstream retry",
    ) -> tuple[str, ...]:
        """Persist terminal proof success or escalation after a completed retry."""

        succeeded = set(succeeded_ids)
        closed: list[str] = []
        changed = False
        async with self.batch():
            for request_id in request_ids:
                request = self.upstream_requests.get(request_id)
                if not isinstance(request, dict):
                    continue
                status = UpstreamRequestStatus(str(request.get("status")))
                if status is UpstreamRequestStatus.CLOSED:
                    continue
                if request_id in succeeded:
                    request["status"] = UpstreamRequestStatus.CLOSED.value
                    request["closed_at"] = timestamp()
                    request["closed_by_run_id"] = run_id
                    request["closed_reason"] = success_detail
                    if run_id is not None:
                        request["retry_run_id"] = run_id
                    request["updated_at"] = timestamp()
                    request.pop("escalation_reason", None)
                    request.pop("escalated_at", None)
                    request.pop("escalated_by_run_id", None)
                    closed.append(request_id)
                    changed = True
                    continue
                if status is not UpstreamRequestStatus.ANSWERED:
                    continue
                reason = error or (
                    "blocked declaration remained unresolved after its targeted downstream retry"
                )
                request["status"] = UpstreamRequestStatus.ESCALATED.value
                request["escalation_reason"] = reason
                request["escalated_at"] = timestamp()
                if run_id is not None:
                    request["escalated_by_run_id"] = run_id
                    request["retry_run_id"] = run_id
                request["updated_at"] = timestamp()
                changed = True
            if changed:
                self._mark_dirty()
                await self._persist()
        return tuple(closed)

    async def reopen_escalated_upstream_requests(self, chapter_ids: Iterable[str]) -> list[str]:
        """Treat the explicit manual unblock command as authority to retry the handoff."""

        selected = set(chapter_ids)
        reopened: list[str] = []
        for request_id, request in self.upstream_requests.items():
            if (
                request.get("consumer_chapter_id") not in selected
                or request.get("status") != UpstreamRequestStatus.ESCALATED.value
            ):
                continue
            target = (
                UpstreamRequestStatus.ANSWERED
                if isinstance(request.get("answer"), dict)
                else UpstreamRequestStatus.REQUESTED
            )
            request["status"] = target.value
            request["updated_at"] = timestamp()
            request.pop("escalation_reason", None)
            request.pop("escalated_at", None)
            request.pop("escalated_by_run_id", None)
            reopened.append(request_id)
        if reopened:
            self._mark_dirty()
            await self._persist()
        return reopened

    async def enqueue_fixup_request(
        self,
        feedback: dict[str, str],
        target_ids: Iterable[str],
        *,
        origin_run_id: str | None = None,
        request_id: str | None = None,
    ) -> str:
        """Persist a repair request before the in-memory scheduler waits on it."""

        request_id = request_id or uuid4().hex[:12]
        self.fixup_requests[request_id] = {
            "feedback": dict(feedback),
            "target_ids": sorted(set(target_ids)),
            "origin_run_id": origin_run_id,
            "created_at": timestamp(),
        }
        self._mark_dirty()
        await self._persist()
        return request_id

    async def finish_fixup_requests(self, request_ids: Iterable[str]) -> None:
        changed = False
        for request_id in request_ids:
            changed = self.fixup_requests.pop(request_id, None) is not None or changed
        if changed:
            self._mark_dirty()
            await self._persist()

    async def migrate_post_review_fixups(self) -> set[str]:
        """Move legacy post-review repair requests back to the review queue."""

        migrated: set[str] = set()
        if not self.fixup_requests:
            return migrated
        async with self.batch():
            for request_id, value in tuple(self.fixup_requests.items()):
                if not isinstance(value, dict):
                    self.fixup_requests.pop(request_id, None)
                    continue
                raw_feedback = value.get("feedback")
                feedback = (
                    {
                        chapter_id: block
                        for chapter_id, block in raw_feedback.items()
                        if isinstance(chapter_id, str) and isinstance(block, str) and block.strip()
                    }
                    if isinstance(raw_feedback, dict)
                    else {}
                )
                raw_targets = value.get("target_ids")
                targets = (
                    {chapter_id for chapter_id in raw_targets if isinstance(chapter_id, str)}
                    if isinstance(raw_targets, list)
                    else set()
                )
                targets.update(feedback)
                targets = {
                    chapter_id
                    for chapter_id in targets
                    if self.key(chapter_id, Stage.REVIEW) in self.tasks
                }
                for chapter_id in targets:
                    feedback.setdefault(
                        chapter_id,
                        "Re-review this chapter: post-review repair work was incorrectly queued "
                        "as fixup.",
                    )
                if feedback:
                    await self.enqueue_proof_review_request(
                        feedback,
                        origin_run_id=f"legacy-review-fixup:{request_id}",
                        request_id=request_id,
                    )
                self.fixup_requests.pop(request_id, None)
                migrated.update(targets)
            for chapter_id in migrated:
                await self.set_task(
                    chapter_id,
                    Stage.FORMALIZE,
                    TaskStatus.SUCCEEDED,
                    "initial fixup complete; later findings moved to review",
                )
                await self.set_task(
                    chapter_id,
                    Stage.REVIEW,
                    TaskStatus.PENDING,
                    "recovered post-review findings",
                )
            self._mark_dirty()
            await self._persist()
        return migrated

    async def enqueue_proof_review_request(
        self,
        feedback: dict[str, str],
        *,
        origin_run_id: str,
        request_id: str | None = None,
    ) -> tuple[str, bool]:
        """Persist proof findings before invalidating their review closure."""

        for existing_id, value in self.proof_review_requests.items():
            if value.get("origin_run_id") == origin_run_id:
                return existing_id, False
        request_id = request_id or uuid4().hex[:12]
        self.proof_review_requests[request_id] = {
            "feedback": dict(feedback),
            "origin_run_id": origin_run_id,
            "created_at": timestamp(),
        }
        self._mark_dirty()
        await self._persist()
        return request_id, True

    async def finish_proof_review_requests(
        self,
        chapter_id: str,
        request_ids: Iterable[str],
    ) -> None:
        """Acknowledge only the findings consumed by one successful review."""

        changed = False
        for request_id in request_ids:
            value = self.proof_review_requests.get(request_id)
            if not isinstance(value, dict):
                continue
            feedback = value.get("feedback")
            if not isinstance(feedback, dict) or chapter_id not in feedback:
                continue
            feedback.pop(chapter_id, None)
            changed = True
            if not feedback:
                self.proof_review_requests.pop(request_id, None)
        if changed:
            self._mark_dirty()
            await self._persist()

    async def close(self) -> None:
        if self._telemetry_flush_task is not None:
            self._telemetry_flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._telemetry_flush_task
            self._telemetry_flush_task = None
        await self.flush()
        await asyncio.wrap_future(self._writer.stop())

    async def export(self, output: Path) -> Path:
        """Explicitly export the complete durable state as JSON."""

        await self.flush()
        exported = await asyncio.to_thread(self._database.export_snapshot, output)
        if exported is None:
            raise ValueError("no swarm state exists to export")
        return exported

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[None]:
        self._batch_depth += 1
        try:
            yield
        finally:
            self._batch_depth -= 1
            if not self._batch_depth:
                await self.flush()

    def _index_run(self, run: RunRecord) -> None:
        self._runs_by_id[run.id] = run
        self._chapter_runs.setdefault(run.chapter_id, []).append(run)
        latest = self._latest_runs_by_chapter.get(run.chapter_id)
        if latest is None or (run.started_at, run.id) >= (latest.started_at, latest.id):
            self._latest_runs_by_chapter[run.chapter_id] = run

    def chapter_runs(self, chapter_id: str) -> tuple[RunRecord, ...]:
        return tuple(self._chapter_runs.get(chapter_id, ()))

    def dashboard_chapter_runs(
        self, chapter_id: str, *, selected_run_id: str | None = None
    ) -> dict[str, Any]:
        """Return compact run tabs and recent activity for one chapter detail view."""

        runs = sorted(self.chapter_runs(chapter_id), key=lambda run: (run.started_at, run.id))
        selected = next(
            (run for run in runs if run.id == selected_run_id),
            runs[-1] if runs else None,
        )
        activity = self.activities.get(selected.id) if selected is not None else None
        return {
            "work_unit_id": chapter_id,
            "runs": [
                {
                    "id": run.id,
                    "stage": run.stage,
                    "role": run.role,
                    "round": run.round,
                    "status": run.status,
                    "started_at": run.started_at,
                    "finished_at": run.finished_at,
                }
                for run in runs
            ],
            "selected_run_id": selected.id if selected is not None else None,
            "activity": activity.as_dict() if activity is not None else None,
        }

    def dashboard_run_prompt(self, run_id: str) -> str | None:
        """Read a run prompt only for an explicitly opened prompt tab."""

        if run_id not in self._runs_by_id:
            return None
        path = self.logs_dir / f"{run_id}.prompt.md"
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ""

    def dashboard_run_timeline(self, run_id: str) -> dict[str, Any] | None:
        """Replay one complete transcript without replacing the bounded hot cache."""

        run = self._runs_by_id.get(run_id)
        if run is None:
            return None
        if not run.log_path:
            activity = self.activities.get(run_id)
            return activity.as_dict() if activity is not None else None
        activity = self.activities.replay(
            run.id,
            run.chapter_id,
            run.role or run.stage,
            Path(run.log_path),
            workspace_root=Path(run.project_root or self.config.settings.repo),
            maximum_events=None,
            cache=False,
        )
        return activity.as_dict() if activity is not None else None

    def latest_run(self, chapter_id: str) -> RunRecord | None:
        return self.active_run(chapter_id) or self._latest_runs_by_chapter.get(chapter_id)

    def active_run(self, chapter_id: str) -> RunRecord | None:
        return self._active_runs_by_chapter.get(chapter_id)

    def load_run_details(self, run: RunRecord) -> RunRecord:
        value = self._database.run_payload(run.id)
        if value is None:
            return run
        for name in ("report", "validation", "isolation"):
            setattr(run, name, value.get(name))
        self._payload_loaded_run_ids.add(run.id)
        return run

    def _invalidate_aggregates(self) -> None:
        self._usage_cache.clear()
        self._cost_cache.clear()

    def _adjust_aggregate_caches(
        self,
        run: RunRecord,
        *,
        old_usage: TokenUsage,
        old_model: str | None,
    ) -> None:
        if self._usage_cache:
            delta = TokenUsage(
                input_tokens=run.usage.input_tokens - old_usage.input_tokens,
                cached_input_tokens=(run.usage.cached_input_tokens - old_usage.cached_input_tokens),
                output_tokens=run.usage.output_tokens - old_usage.output_tokens,
                reasoning_output_tokens=(
                    run.usage.reasoning_output_tokens - old_usage.reasoning_output_tokens
                ),
                measured=run.usage.measured or old_usage.measured,
            )
            for invocation_only in (False, True):
                if invocation_only and run.id in self._prior_run_ids:
                    continue
                for key in ((invocation_only, None), (invocation_only, run.chapter_id)):
                    if key in self._usage_cache:
                        self._usage_cache[key] = self._usage_cache[key] + delta
        if self._cost_cache:
            old_cost = self._run_cost(old_usage, old_model)
            new_cost = self.run_cost(run)
            delta_cost = CostEstimate(
                estimated_usd=new_cost.estimated_usd - old_cost.estimated_usd,
                priced_tokens=new_cost.priced_tokens - old_cost.priced_tokens,
                unpriced_tokens=new_cost.unpriced_tokens - old_cost.unpriced_tokens,
                inferred_runs=new_cost.inferred_runs - old_cost.inferred_runs,
                unknown_models=new_cost.unknown_models,
            )
            for invocation_only in (False, True):
                if invocation_only and run.id in self._prior_run_ids:
                    continue
                for key in ((invocation_only, None), (invocation_only, run.chapter_id)):
                    if key in self._cost_cache:
                        self._cost_cache[key] = self._cost_cache[key] + delta_cost

    def _invalidate_status_summaries(self) -> None:
        """Compatibility hook; indexes synchronize when dirty entities are marked."""

    def _rebuild_status_indexes(self) -> None:
        self._stage_count_cache = {
            stage.value: {status.value: 0 for status in TaskStatus}
            | {"queued": 0, "postprocess": 0}
            for stage in Stage
        }
        self._indexed_task_states.clear()
        self._repairable_task_keys.clear()
        for key, task in self.tasks.items():
            bucket = "queued" if task.queued else str(task.status)
            self._stage_count_cache[task.stage][bucket] += 1
            if task.status == TaskStatus.RUNNING and task.phase == TaskPhase.POSTPROCESS:
                self._stage_count_cache[task.stage]["postprocess"] += 1
            self._indexed_task_states[key] = (
                task.stage,
                str(task.status),
                task.queued,
                str(task.phase),
                task.repairing,
            )
            if task.status in {TaskStatus.FAILED, TaskStatus.BLOCKED} and not task.repairing:
                self._repairable_task_keys.add(key)
        self._active_run_ids = {
            run.id for run in self._runs_by_id.values() if run.status == TaskStatus.RUNNING
        }
        self._active_runs_by_chapter = {
            run.chapter_id: run
            for run in self._runs_by_id.values()
            if run.status == TaskStatus.RUNNING
        }

    def _sync_task_index(self, task: TaskRecord) -> None:
        if not self._stage_count_cache:
            return
        key = self.key(task.chapter_id, Stage(task.stage))
        previous = self._indexed_task_states.get(key)
        current = (task.stage, str(task.status), task.queued, str(task.phase), task.repairing)
        if previous == current:
            return
        if previous is not None:
            old_stage, old_status, old_queued, old_phase, _old_repairing = previous
            old_bucket = "queued" if old_queued else old_status
            self._stage_count_cache[old_stage][old_bucket] -= 1
            if old_status == TaskStatus.RUNNING and old_phase == TaskPhase.POSTPROCESS:
                self._stage_count_cache[old_stage]["postprocess"] -= 1
        bucket = "queued" if task.queued else str(task.status)
        self._stage_count_cache[task.stage][bucket] += 1
        if task.status == TaskStatus.RUNNING and task.phase == TaskPhase.POSTPROCESS:
            self._stage_count_cache[task.stage]["postprocess"] += 1
        self._indexed_task_states[key] = current
        if task.status in {TaskStatus.FAILED, TaskStatus.BLOCKED} and not task.repairing:
            self._repairable_task_keys.add(key)
        else:
            self._repairable_task_keys.discard(key)

    def agent_summary(self) -> dict[str, Any]:
        if not self._stage_count_cache:
            self._rebuild_status_indexes()
        by_stage = {stage.value: 0 for stage in Stage}
        by_role: dict[str, int] = {}
        for run_id in self._active_run_ids:
            run = self._runs_by_id.get(run_id)
            if run is None or run.stage not in by_stage:
                continue
            by_stage[run.stage] += 1
            role = run.role or run.stage
            by_role[role] = by_role.get(role, 0) + 1
        discovery_max_agents = self.config.stages[Stage.DISCOVER].max_agents
        assert discovery_max_agents is not None
        return {
            "active": sum(by_stage.values()),
            "maximum": discovery_max_agents + self.config.settings.max_agents,
            "maximum_by_pool": {
                "discover": discovery_max_agents,
                "mutating": self.config.settings.max_agents,
            },
            "queued": sum(counts["queued"] for counts in self._stage_count_cache.values()),
            "postprocessing": sum(
                counts["postprocess"] for counts in self._stage_count_cache.values()
            ),
            "postprocessing_by_stage": {
                stage.value: self._stage_count_cache[stage.value]["postprocess"] for stage in Stage
            },
            "by_stage": by_stage,
            "by_role": by_role,
        }

    def stage_counts(self, stage: Stage) -> dict[str, int]:
        if not self._stage_count_cache:
            self._rebuild_status_indexes()
        return self._stage_count_cache[stage.value]

    def _record_source_issues(self, run: RunRecord) -> list[str]:
        report = run.report or {}
        raw_issues = report.get("source_issues", [])
        if not isinstance(raw_issues, list):
            return []
        chapter = self.config.work_unit(run.chapter_id)
        required = ("location", "source_excerpt", "description", "suggested_correction")
        issue_ids: list[str] = []
        for raw in raw_issues:
            if not isinstance(raw, dict) or not all(
                isinstance(raw.get(name), str) and raw[name].strip() for name in required
            ):
                continue
            location = raw["location"].strip()
            excerpt = raw["source_excerpt"].strip()
            description = raw["description"].strip()
            correction = raw["suggested_correction"].strip()
            anchor = excerpt or f"{location}\0{description}"
            fingerprint = "\0".join(
                (chapter.source.as_posix(), chapter.id, anchor.casefold())
            ).encode()
            issue_id = stable_digest_bytes(fingerprint)[:16]
            transitional_issue_id = digest_bytes(fingerprint)
            existing_issue_id = next(
                (
                    candidate
                    for candidate in (issue_id, transitional_issue_id)
                    if candidate in self.source_issues
                ),
                issue_id,
            )
            issue_id = existing_issue_id
            issue_ids.append(issue_id)
            seen_at = run.finished_at or timestamp()
            if existing := self.source_issues.get(issue_id):
                existing.location = location
                existing.source_excerpt = excerpt
                existing.description = description
                existing.suggested_correction = correction
                existing.last_seen_at = seen_at
                existing.sightings += 1
                if run.stage not in existing.stages:
                    existing.stages.append(run.stage)
                if run.id not in existing.run_ids:
                    existing.run_ids.append(run.id)
                continue
            self.source_issues[issue_id] = SourceIssueRecord(
                id=issue_id,
                chapter_id=chapter.id,
                book_id=chapter.book_id,
                chapter_number=chapter.number,
                chapter_title=chapter.title,
                source=chapter.source.as_posix(),
                location=location,
                source_excerpt=excerpt,
                description=description,
                suggested_correction=correction,
                source_start_line=chapter.source_span.start_line,
                source_end_line=chapter.source_span.end_line,
                first_seen_at=seen_at,
                last_seen_at=seen_at,
                stages=[run.stage],
                run_ids=[run.id],
            )
        return issue_ids

    def total_usage(self) -> TokenUsage:
        return self._usage(invocation_only=False)

    def invocation_usage(self, chapter_id: str | None = None) -> TokenUsage:
        """Usage from attempts created by this orchestrator invocation."""

        return self._usage(invocation_only=True, chapter_id=chapter_id)

    def _usage(self, *, invocation_only: bool, chapter_id: str | None = None) -> TokenUsage:
        key = (invocation_only, chapter_id)
        if key in self._usage_cache:
            return self._usage_cache[key]

        by_chapter = {chapter.id: TokenUsage() for chapter in self.config.work_units}
        for run in self._runs_by_id.values():
            if invocation_only and run.id in self._prior_run_ids:
                continue
            by_chapter[run.chapter_id] += run.usage
        total = TokenUsage()
        for usage in by_chapter.values():
            total += usage

        self._usage_cache[(invocation_only, None)] = total
        self._usage_cache.update(
            ((invocation_only, item), usage) for item, usage in by_chapter.items()
        )
        return self._usage_cache.get(key, TokenUsage())

    def _cost(self, *, invocation_only: bool, chapter_id: str | None = None) -> CostEstimate:
        key = (invocation_only, chapter_id)
        if key in self._cost_cache:
            return self._cost_cache[key]

        by_chapter = {chapter.id: CostEstimate() for chapter in self.config.work_units}
        for run in self._runs_by_id.values():
            if invocation_only and run.id in self._prior_run_ids:
                continue
            cost = self.run_cost(run)
            by_chapter[run.chapter_id] += cost
        total = CostEstimate()
        for cost in by_chapter.values():
            total += cost

        self._cost_cache[(invocation_only, None)] = total
        self._cost_cache.update(
            ((invocation_only, item), cost) for item, cost in by_chapter.items()
        )
        return self._cost_cache.get(key, CostEstimate())

    def run_cost(self, run: RunRecord) -> CostEstimate:
        return self._run_cost(run.usage, run.model)

    @staticmethod
    def _run_cost(usage: TokenUsage, model: str | None) -> CostEstimate:
        if not usage.measured:
            return CostEstimate()
        selected_model = model or LEGACY_MODEL
        return estimate_cost(
            model=selected_model,
            input_tokens=usage.input_tokens,
            cached_input_tokens=usage.cached_input_tokens,
            output_tokens=usage.output_tokens,
            inferred=model is None,
        )

    def total_cost(self) -> CostEstimate:
        return self._cost(invocation_only=False)

    def invocation_cost(self, chapter_id: str | None = None) -> CostEstimate:
        return self._cost(invocation_only=True, chapter_id=chapter_id)

    def task(self, chapter_id: str, stage: Stage) -> TaskRecord:
        return self.tasks[self.key(chapter_id, stage)]

    def later_stage_started(self, chapter_id: str) -> bool:
        return any(
            task.rounds > 0 or task.status in (TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
            for task in (
                self.task(chapter_id, Stage.REVIEW),
                self.task(chapter_id, Stage.PROVE),
            )
        )

    async def set_task(
        self,
        chapter_id: str,
        stage: Stage,
        status: TaskStatus,
        detail: str,
        *,
        source_digest: str | None = None,
        queued: bool = False,
    ) -> None:
        task = self.task(chapter_id, stage)
        if task.status == TaskStatus.INTERRUPTED and status not in (
            TaskStatus.RUNNING,
            TaskStatus.SUCCEEDED,
        ):
            return
        if (
            stage is Stage.FORMALIZE
            and status != TaskStatus.SUCCEEDED
            and self.later_stage_started(chapter_id)
        ):
            status = TaskStatus.SUCCEEDED
            detail = "formalization completed before review"
        changed_tasks = [task]
        if stage in (Stage.REVIEW, Stage.PROVE) and status in (
            TaskStatus.RUNNING,
            TaskStatus.SUCCEEDED,
        ):
            formalize = self.task(chapter_id, Stage.FORMALIZE)
            if formalize.status != TaskStatus.SUCCEEDED:
                formalize.status = TaskStatus.SUCCEEDED
                formalize.phase = TaskPhase.IDLE
                formalize.queued = False
                formalize.detail = "formalization completed before review"
                formalize.updated_at = timestamp()
                changed_tasks.append(formalize)
        recovered = task.recovering_failure and status == TaskStatus.SUCCEEDED
        task.status = status
        task.phase = TaskPhase.POSTPROCESS if status == TaskStatus.RUNNING else TaskPhase.IDLE
        task.queued = queued and status == TaskStatus.PENDING
        if stage is Stage.PROVE:
            task.source_digest = source_digest if status == TaskStatus.SUCCEEDED else None
        task.detail = detail
        task.updated_at = timestamp()
        if status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
            task.recovering_failure = False
        if recovered:
            changed_tasks.extend(self._release_failure_blocked_tasks())
        self._invalidate_status_summaries()
        self._mark_dirty(tasks=changed_tasks, global_state=False)
        await self._persist()

    async def set_tasks(
        self,
        chapter_ids: Iterable[str],
        stage: Stage,
        status: TaskStatus,
        detail: str,
    ) -> None:
        changed = False
        changed_tasks: list[TaskRecord] = []
        recovered = False
        updated_at = timestamp()
        for chapter_id in chapter_ids:
            key = self.key(chapter_id, stage)
            if key not in self.tasks:
                continue
            task = self.tasks[key]
            if task.status == TaskStatus.INTERRUPTED and status not in (
                TaskStatus.RUNNING,
                TaskStatus.SUCCEEDED,
            ):
                continue
            task_status = status
            task_detail = detail
            if (
                stage is Stage.FORMALIZE
                and status != TaskStatus.SUCCEEDED
                and self.later_stage_started(chapter_id)
            ):
                task_status = TaskStatus.SUCCEEDED
                task_detail = "formalization completed before review"
            if stage in (Stage.REVIEW, Stage.PROVE) and status in (
                TaskStatus.RUNNING,
                TaskStatus.SUCCEEDED,
            ):
                formalize = self.task(chapter_id, Stage.FORMALIZE)
                if formalize.status != TaskStatus.SUCCEEDED:
                    formalize.status = TaskStatus.SUCCEEDED
                    formalize.phase = TaskPhase.IDLE
                    formalize.queued = False
                    formalize.detail = "formalization completed before review"
                    formalize.updated_at = updated_at
                    changed_tasks.append(formalize)
            recovered = recovered or (
                task.recovering_failure and task_status == TaskStatus.SUCCEEDED
            )
            task.status = task_status
            task.phase = (
                TaskPhase.POSTPROCESS if task_status == TaskStatus.RUNNING else TaskPhase.IDLE
            )
            task.queued = False
            if stage is Stage.PROVE and task_status != TaskStatus.SUCCEEDED:
                task.source_digest = None
            task.detail = task_detail
            task.updated_at = updated_at
            if task_status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
                task.recovering_failure = False
            changed_tasks.append(task)
            changed = True
        if recovered:
            changed_tasks.extend(self._release_failure_blocked_tasks())
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=changed_tasks, global_state=False)
            await self._persist()

    async def set_task_phase(
        self,
        chapter_id: str,
        stage: Stage,
        phase: TaskPhase,
        detail: str,
    ) -> None:
        """Update the live execution phase without changing task completion."""

        task = self.task(chapter_id, stage)
        if task.status != TaskStatus.RUNNING:
            return
        task.phase = phase
        task.detail = detail
        task.updated_at = timestamp()
        self._mark_dirty(task=task, global_state=False)
        await self._persist()

    async def unblock(self) -> list[str]:
        """Reset blocked tasks to pending without discarding attempt history."""
        changed: list[str] = []
        proof_chapters: set[str] = set()
        for key, task in self.tasks.items():
            if task.status != TaskStatus.BLOCKED:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            if task.stage == Stage.PROVE:
                task.source_digest = None
                proof_chapters.add(task.chapter_id)
            task.detail = "manually unblocked"
            task.updated_at = timestamp()
            changed.append(key)
        await self.reopen_escalated_upstream_requests(proof_chapters)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=(self.tasks[key] for key in changed), global_state=False)
            await self._persist()
        return changed

    async def retry_failed(self) -> list[str]:
        """Reset every failed task to pending without discarding attempt history."""

        changed: list[str] = []
        for key, task in self.tasks.items():
            if task.status != TaskStatus.FAILED:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            if task.stage == Stage.PROVE:
                task.source_digest = None
            task.detail = "manually retried"
            task.recovering_failure = True
            task.updated_at = timestamp()
            changed.append(key)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=(self.tasks[key] for key in changed), global_state=False)
            await self._persist()
        return changed

    def _source_dependencies(self, chapter_id: str) -> set[str]:
        dependencies = self.source_dependency_tree.get("dependencies", {})
        raw = dependencies.get(chapter_id, ()) if isinstance(dependencies, dict) else ()
        if isinstance(raw, list):
            return {item for item in raw if isinstance(item, str)}
        nodes = self.source_dependency_tree.get("nodes", {})
        node = nodes.get(chapter_id, {}) if isinstance(nodes, dict) else {}
        raw = node.get("dependencies", ()) if isinstance(node, dict) else ()
        return {item for item in raw if isinstance(item, str)} if isinstance(raw, list) else set()

    def _release_failure_blocked_tasks(self) -> list[TaskRecord]:
        """Release blocked tasks whose failed prerequisites have now recovered."""

        released: list[TaskRecord] = []
        for task in self.tasks.values():
            if task.status != TaskStatus.BLOCKED:
                continue
            stage = Stage(task.stage)
            dependencies = self._source_dependencies(task.chapter_id)
            ready = False
            if stage is Stage.FORMALIZE and task.detail in {
                "blocked by a failed source dependency formalization",
                "blocked by incomplete discovery or a source dependency cycle",
                "blocked because source discovery failed",
            }:
                ready = self.task(task.chapter_id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
                ready = ready and all(
                    self.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
                    for chapter_id in dependencies
                )
            elif stage is Stage.REVIEW and task.detail == (
                "blocked by a failed prerequisite review; unrelated branches completed"
            ):
                ready = self.task(task.chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
                ready = ready and all(
                    self.task(chapter_id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
                    for chapter_id in dependencies
                )
            elif stage is Stage.PROVE and task.detail in {
                "formalization failed; quarantined from proof",
                "blocked because formalization did not complete",
            }:
                ready = self.task(task.chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
            elif (
                stage is Stage.PROVE
                and task.detail == "blocked because statement review did not complete"
            ):
                ready = self.task(task.chapter_id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
            if not ready:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            if stage is Stage.PROVE:
                task.source_digest = None
            task.detail = "automatically unblocked after failed prerequisite recovered"
            task.recovering_failure = True
            task.updated_at = timestamp()
            released.append(task)
        return released

    async def requeue_interrupted(self, *, resume_agents: bool) -> list[str]:
        """Requeue interrupted tasks while retaining their optional session history."""

        changed: list[str] = []
        for key, task in self.tasks.items():
            if task.status != TaskStatus.INTERRUPTED:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            if task.stage == Stage.PROVE:
                task.source_digest = None
            task.detail = (
                "interrupted agent queued for session resume"
                if resume_agents
                else "interrupted agent queued for a fresh retry"
            )
            task.updated_at = timestamp()
            changed.append(key)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=(self.tasks[key] for key in changed), global_state=False)
            await self._persist()
        return changed

    async def start_run(self, chapter_id: str, stage: Stage) -> RunRecord:
        if stage is Stage.FORMALIZE and self.later_stage_started(chapter_id):
            raise RuntimeError(
                f"cannot start formalize for {chapter_id} after review or proof has begun"
            )
        task = self.task(chapter_id, stage)
        chapter = self.config.work_unit(chapter_id)
        task.status = TaskStatus.RUNNING
        task.phase = TaskPhase.AGENT
        task.queued = False
        if stage is Stage.PROVE:
            task.source_digest = None
        task.rounds += 1
        task.updated_at = timestamp()
        run = RunRecord(
            id=uuid4().hex[:12],
            chapter_id=chapter_id,
            stage=stage.value,
            round=task.rounds,
            model=self.config.model_for(stage),
            role=stage.value,
            source=chapter.source.as_posix(),
            source_start_line=chapter.source_span.start_line,
            source_end_line=chapter.source_span.end_line,
            project_root=str(
                self.config.project.root
                if self.config.project is not None
                else self.config.settings.repo
            ),
        )
        task.runs.append(run)
        self._index_run(run)
        self._payload_loaded_run_ids.add(run.id)
        self._invalidate_status_summaries()
        self._mark_dirty(task=task, run=run, global_state=False)
        await self._persist()
        return run

    async def start_auxiliary_run(
        self,
        chapter_id: str,
        stage: Stage,
        *,
        role: str,
        request_ids: Iterable[str],
        model: str | None = None,
    ) -> RunRecord:
        """Record a temporary agent without mutating the owner's chapter-stage state."""

        task = self.task(chapter_id, stage)
        chapter = self.config.work_unit(chapter_id)
        role_round = 1 + sum((run.role or run.stage) == role for run in task.runs)
        run = RunRecord(
            id=uuid4().hex[:12],
            chapter_id=chapter_id,
            stage=stage.value,
            round=role_round,
            model=model if model is not None else self.config.model_for(stage),
            role=role,
            auxiliary=True,
            request_ids=list(dict.fromkeys(request_ids)),
            source=chapter.source.as_posix(),
            source_start_line=chapter.source_span.start_line,
            source_end_line=chapter.source_span.end_line,
            project_root=str(
                self.config.project.root
                if self.config.project is not None
                else self.config.settings.repo
            ),
        )
        task.runs.append(run)
        self._index_run(run)
        self._payload_loaded_run_ids.add(run.id)
        self._invalidate_status_summaries()
        self._mark_dirty(task=task, run=run, global_state=False)
        await self._persist()
        return run

    def repairable_tasks(self) -> list[tuple[str, TaskRecord]]:
        """Current terminal failures eligible for Shepherd triage."""

        if not self._stage_count_cache:
            self._rebuild_status_indexes()
        return sorted(
            ((key, self.tasks[key]) for key in self._repairable_task_keys),
            key=lambda item: (item[1].updated_at, item[0]),
        )

    def ensure_repair_case(self, task_key: str) -> RepairCaseRecord:
        task = self.tasks[task_key]
        latest = task.runs[-1] if task.runs else None
        evidence = {
            "task_key": task_key,
            "status": str(task.status),
            "detail": task.detail,
            "run_id": latest.id if latest is not None else "",
            "exit_code": latest.exit_code if latest is not None else None,
            "validation": latest.validation if latest is not None else None,
        }
        encoded_evidence = json.dumpb(evidence, sort_keys=True)
        fingerprint = stable_digest_bytes(encoded_evidence)
        transitional_fingerprint = digest_bytes(encoded_evidence)
        for case in self.repair_cases.values():
            if case.task_key == task_key and case.fingerprint in {
                fingerprint,
                transitional_fingerprint,
            }:
                case.fingerprint = fingerprint
                return case
        case_id = stable_digest_text(f"{task_key}:{fingerprint}")[:16]
        case = RepairCaseRecord(
            id=case_id,
            task_key=task_key,
            chapter_id=task.chapter_id,
            stage=str(task.stage),
            fingerprint=fingerprint,
        )
        self.repair_cases[case.id] = case
        self._mark_dirty()
        return case

    async def start_repair_sweep(
        self,
        *,
        trigger: str,
        task_keys: Iterable[str],
    ) -> RepairSweepRecord:
        cases = [self.ensure_repair_case(key) for key in dict.fromkeys(task_keys)]
        sweep = RepairSweepRecord(
            id=uuid4().hex[:12],
            trigger=trigger,
            failure_count=len(cases),
            case_ids=[case.id for case in cases],
        )
        self.repair_sweeps[sweep.id] = sweep
        self.shepherd.status = "planning"
        self.shepherd.current_sweep_id = sweep.id
        self.shepherd.last_started_at = sweep.started_at
        self.shepherd.last_error = ""
        self.shepherd.pending_failures = len(cases)
        self.shepherd.planned_units = 0
        self.shepherd.running_units = 0
        self.shepherd.succeeded_units = 0
        self.shepherd.failed_units = 0
        self._mark_dirty()
        await self._persist()
        return sweep

    async def install_repair_plan(
        self,
        sweep_id: str,
        units: Iterable[RepairWorkUnitRecord],
        *,
        summary: str,
        run_id: str,
    ) -> None:
        sweep = self.repair_sweeps[sweep_id]
        installed = list(units)
        for unit in installed:
            if unit.sweep_id != sweep_id:
                raise ValueError(f"repair unit {unit.id} belongs to another sweep")
            self.repair_work_units[unit.id] = unit
            for case_id in unit.case_ids:
                case = self.repair_cases[case_id]
                case.status = RepairCaseStatus.PLANNED
                case.sweep_id = sweep_id
                if unit.id not in case.work_unit_ids:
                    case.work_unit_ids.append(unit.id)
                case.updated_at = timestamp()
        sweep.status = "repairing" if installed else "completed"
        sweep.summary = summary
        sweep.run_id = run_id
        sweep.work_unit_ids = [unit.id for unit in installed]
        self.shepherd.status = "repairing" if installed else "idle"
        self.shepherd.current_run_id = ""
        self.shepherd.last_summary = summary
        self.shepherd.planned_units = len(installed)
        self._mark_dirty()
        await self._persist()

    async def start_repair_work_unit(self, unit_id: str) -> None:
        unit = self.repair_work_units[unit_id]
        unit.status = RepairWorkUnitStatus.RUNNING
        unit.started_at = timestamp()
        unit.finished_at = None
        unit.detail = "repair worker running"
        changed_tasks: list[TaskRecord] = []
        for task_key in unit.task_keys:
            task = self.tasks.get(task_key)
            if task is None:
                continue
            task.repairing = True
            task.repair_work_unit_id = unit.id
            task.updated_at = timestamp()
            changed_tasks.append(task)
        for case_id in unit.case_ids:
            case = self.repair_cases[case_id]
            case.status = RepairCaseStatus.REPAIRING
            case.updated_at = timestamp()
        self.shepherd.running_units += 1
        self._mark_dirty(tasks=changed_tasks)
        await self._persist()

    async def link_repair_work_unit_run(self, unit_id: str, run_id: str) -> None:
        """Expose a repair worker's agent run as soon as the run starts."""

        unit = self.repair_work_units[unit_id]
        unit.run_id = run_id
        self._mark_dirty()
        await self._persist()

    async def finish_repair_work_unit(
        self,
        unit_id: str,
        *,
        status: RepairWorkUnitStatus,
        detail: str,
        run_id: str = "",
    ) -> None:
        unit = self.repair_work_units[unit_id]
        unit.status = status
        unit.detail = detail
        unit.run_id = run_id
        unit.finished_at = timestamp()
        changed_tasks: list[TaskRecord] = []
        for task_key in unit.task_keys:
            task = self.tasks.get(task_key)
            if task is None:
                continue
            if task.repair_work_unit_id == unit.id:
                task.repairing = False
                task.repair_work_unit_id = ""
                task.updated_at = timestamp()
                changed_tasks.append(task)
        self.shepherd.running_units = max(0, self.shepherd.running_units - 1)
        if status == RepairWorkUnitStatus.SUCCEEDED:
            self.shepherd.succeeded_units += 1
        elif status in {RepairWorkUnitStatus.FAILED, RepairWorkUnitStatus.INTERRUPTED}:
            self.shepherd.failed_units += 1
        self._mark_dirty(tasks=changed_tasks)
        await self._persist()

    async def finish_repair_sweep(self, sweep_id: str, *, error: str = "") -> None:
        sweep = self.repair_sweeps[sweep_id]
        sweep.status = "failed" if error else "completed"
        sweep.error = error
        sweep.finished_at = timestamp()
        for case_id in sweep.case_ids:
            case = self.repair_cases[case_id]
            task = self.tasks.get(case.task_key)
            if task is not None and task.status not in {
                TaskStatus.FAILED,
                TaskStatus.BLOCKED,
            }:
                case.status = RepairCaseStatus.RESOLVED
            elif case.status != RepairCaseStatus.RESOLVED:
                case.status = RepairCaseStatus.EXHAUSTED
            case.updated_at = timestamp()
        self.shepherd.status = "error" if error else "idle"
        self.shepherd.current_sweep_id = ""
        self.shepherd.current_run_id = ""
        self.shepherd.last_finished_at = sweep.finished_at
        self.shepherd.last_error = error
        self.shepherd.pending_failures = len(self.repairable_tasks())
        self._mark_dirty()
        await self._persist()

    async def update_run(self, run: RunRecord, *, deferred: bool = False, **changes: Any) -> None:
        old_usage = run.usage
        old_model = run.model
        for name, value in changes.items():
            setattr(run, name, value)
        if "usage" in changes or "model" in changes:
            self._adjust_aggregate_caches(run, old_usage=old_usage, old_model=old_model)
        if "status" in changes:
            self._invalidate_status_summaries()
        self._mark_dirty(
            run=run,
            global_state=False,
        )
        if deferred:
            self._schedule_telemetry_flush()
        else:
            await self._persist()

    async def record_thread_cumulative_usage(
        self,
        thread_id: str,
        usage: TokenUsage,
        *,
        deferred: bool = True,
    ) -> None:
        """Persist the latest monotone cumulative counter emitted by one Codex thread."""

        previous = self.thread_cumulative_usage.get(thread_id)
        if previous is not None and usage.total_tokens < previous.total_tokens:
            return
        self.thread_cumulative_usage[thread_id] = usage
        self._mark_dirty(global_state=True)
        if deferred:
            self._schedule_telemetry_flush()
        else:
            await self._persist()

    def _schedule_telemetry_flush(self) -> None:
        if self._telemetry_flush_task is not None and not self._telemetry_flush_task.done():
            return

        async def persist_later() -> None:
            try:
                await asyncio.sleep(0.5)
                await self.flush()
            finally:
                self._telemetry_flush_task = None

        self._telemetry_flush_task = asyncio.create_task(persist_later())

    async def finish_run(self, run: RunRecord, *, status: TaskStatus, **changes: Any) -> None:
        if run.status == TaskStatus.INTERRUPTED and status != TaskStatus.SUCCEEDED:
            status = TaskStatus.INTERRUPTED
        run.status = status
        run.finished_at = timestamp()
        for name, value in changes.items():
            setattr(run, name, value)
        issue_ids = self._record_source_issues(run)
        if isinstance(run.report, dict) and "source_issues" in run.report:
            report = {key: value for key, value in run.report.items() if key != "source_issues"}
            if issue_ids:
                report["source_issue_ids"] = issue_ids
            run.report = report
        self._invalidate_aggregates()
        self._invalidate_status_summaries()
        changed_task = None
        if status == TaskStatus.INTERRUPTED and not run.auxiliary:
            task = self.task(run.chapter_id, Stage(run.stage))
            task.status = TaskStatus.INTERRUPTED
            task.phase = TaskPhase.IDLE
            task.queued = False
            if task.stage == Stage.PROVE:
                task.source_digest = None
            task.detail = "agent interrupted with the orchestrator"
            task.updated_at = timestamp()
            changed_task = task
        self._mark_dirty(
            task=changed_task,
            run=run,
            issues=bool(issue_ids),
            global_state=False,
        )
        await self._persist()

    async def start_coordinator_build(
        self,
        *,
        mode: str,
        stage: Stage,
        iteration: int,
        maximum_iterations: int,
        total: int,
        target_chapter_ids: Iterable[str] = (),
        target_work_unit_ids: Iterable[str] | None = None,
    ) -> None:
        targets = target_chapter_ids if target_work_unit_ids is None else target_work_unit_ids
        self.coordinator_build = CoordinatorBuildRecord(
            active=True,
            mode=mode,
            stage=stage.value,
            iteration=iteration,
            maximum_iterations=maximum_iterations,
            total=total,
            target_chapter_ids=list(targets),
        )
        self._mark_coordinator_build_dirty()
        await self._persist()

    async def advance_coordinator_build(
        self,
        *,
        work_unit_id: str | None = None,
        chapter_id: str | None = None,
        completed: int,
        command: str | None = None,
    ) -> None:
        unit_id = work_unit_id or chapter_id
        if not unit_id:
            raise ValueError("advance_coordinator_build requires a work-unit id")
        self.coordinator_build.current_work_unit_id = unit_id
        self.coordinator_build.completed = completed
        if command is not None:
            self.append_coordinator_build_output(f"$ {command}")
        self.coordinator_build.updated_at = timestamp()
        self._mark_coordinator_build_dirty()
        await self._persist()

    def append_coordinator_build_output(
        self,
        output: str,
        *,
        maximum: int = 4,
        error_count: int | None = None,
    ) -> None:
        """Retain a small in-memory tail for live status displays."""

        output = ANSI_ESCAPE_RE.sub("", output)
        if self.coordinator_build.active:
            for match in LAKE_PROGRESS_RE.finditer(output):
                completed = int(match.group("completed"))
                total = int(match.group("total"))
                if total > 0:
                    if total != self.coordinator_build.total:
                        self.coordinator_build.completed = completed
                    else:
                        self.coordinator_build.completed = max(
                            self.coordinator_build.completed, completed
                        )
                    self.coordinator_build.total = total
                    self.coordinator_build.current_chapter_id = match.group("target")
        errors, warnings = (
            self.config.backend.diagnostic_counts(output)
            if self.config.backend is not None
            else lean_diagnostic_counts(output)
        )
        self.coordinator_build.error_count += errors if error_count is None else error_count
        self.coordinator_build.warning_count += warnings
        lines = [
            shorten_book_paths(line.rstrip())[-500:]
            for line in output.replace("\r", "\n").splitlines()
        ]
        lines = [line for line in lines if line]
        if not lines:
            return
        self.coordinator_build.output_tail = [
            *self.coordinator_build.output_tail,
            *lines,
        ][-maximum:]
        self.coordinator_build.updated_at = timestamp()
        self._mark_coordinator_build_dirty()

    async def finish_coordinator_build(self) -> None:
        self.coordinator_build.active = False
        self.coordinator_build.current_chapter_id = ""
        self.coordinator_build.updated_at = timestamp()
        self._mark_coordinator_build_dirty()
        await self._persist()
