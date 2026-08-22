from __future__ import annotations

import asyncio
import re
from collections import deque
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
from paf.package_model import (
    CapabilityPackage,
    PackageConsumer,
    PackageDependency,
    PackageDisposition,
    PackageEvidence,
    PackageRecovery,
    PackageState,
    PackageStatus,
    ReservationOwnerKind,
    ReservationResult,
    ReservationSpec,
    StewardLease,
)
from paf.pricing import LEGACY_MODEL, CostEstimate, estimate_cost
from paf.state_db import (
    COLLECTION_SECTIONS,
    DATABASE_NAME,
    GRAPH_SECTIONS,
    CollectionWrite,
    DatabaseWrite,
    GraphSnapshot,
    GraphWrite,
    StateDatabase,
    StateWriter,
    collection_snapshot,
    graph_snapshot,
)

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
LAKE_PROGRESS_RE = re.compile(r"\[(?P<completed>\d+)/(?P<total>\d+)\]\s+\S+\s+(?P<target>\S+)")
BUILD_WARNING_REVIEW_KIND = "build_warning"
WAITING_DETAIL_MAXIMUM = 160


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


class RequirementKind(StrEnum):
    """A durable reason why a pending task is not ready to run."""

    SOURCE_DISCOVERY = "source_discovery"
    SOURCE_DEPENDENCY = "source_dependency"
    STAGE_DEPENDENCY = "stage_dependency"
    COORDINATOR_OWNER = "coordinator_owner"
    BUILD_FRESHNESS = "build_freshness"
    WORKTREE_CLEAN = "worktree_clean"
    PROOF_REVIEW_REQUEST = "proof_review_request"
    UPSTREAM_REQUEST = "upstream_request"
    CAPABILITY_PACKAGE = "capability_package"
    GRAPH = "graph"
    LEGACY_BLOCK = "legacy_block"


@dataclass(frozen=True)
class Requirement:
    kind: RequirementKind
    owner_task_key: str | None = None
    request_id: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class Readiness:
    ready: bool
    waiting: tuple[Requirement, ...] = ()


class ProofBlockerStatus(StrEnum):
    OPEN = "open"
    REVIEW_REQUESTED = "review_requested"
    UPSTREAM_REQUESTED = "upstream_requested"
    WAITING_DEPENDENCY = "waiting_dependency"
    PACKAGE_REQUIRED = "package_required"
    PARKED = "parked"
    BLOCKED = "blocked"
    RESOLVED = "resolved"


class UpstreamRequestStatus(StrEnum):
    """Lifecycle of a downstream observation requiring tandem upstream evaluation."""

    OPEN = "open"
    EVALUATING = "evaluating"
    VERIFIED = "verified"
    REJECTED = "rejected"
    FAILED = "failed"


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
    # Durable failure classification used to distinguish orchestration/tooling outages from
    # mathematical proof failures.  In particular, infrastructure failures may be safely
    # requeued after a restart without spending another proof-chunk budget.
    failure_kind: str = ""
    error: str = ""
    # Digest of the agent result scope. It lets restart reconciliation detect
    # whether that completed result still matches the current source.
    source_digest: str | None = None

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
    # Transient scheduling state: requirements are satisfied and the task is
    # waiting for an agent-capacity slot. A bare pending task is not runnable.
    queued: bool = False
    # Orthogonal readiness state. These requirements explain why a pending task
    # cannot run yet without turning dependency impact into an execution result.
    waiting_on: tuple[Requirement, ...] = ()
    # Set by explicit failed-task retry and propagated through tasks released from its fallout.
    # A later success uses this marker to reopen only causally blocked work.
    recovering_failure: bool = False
    # Proof completion is tied to the exact validated chapter sources. This is
    # populated only on the prove task.
    source_digest: str | None = None
    # Coordinator scans can observe placeholders without creating an agent run.
    # Persist that observation on the prove task so the dashboard does not fall
    # back to a stale count from an older run after a restart.
    sorry_count: int | None = None
    sorry_count_updated_at: str | None = None
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
        self._persisted_section_object_ids: dict[str, int] = {}
        self._persisted_header_object_ids: dict[str, int] = {}
        self._dirty_sections: set[str] = set()
        self._collection_cache: dict[str, dict[str, tuple[int, bytes]]] = {}
        self._graph_cache: dict[str, GraphSnapshot] = {}
        self._coordinator_build_dirty = False
        self._thread_usage_dirty = False
        self._dirty_thread_usage_ids: set[str] = set()
        self._static_dirty = False
        self._dirty_task_keys: set[str] = set()
        self._dirty_projection_work_units: set[str] = set()
        self._issues_dirty = False
        self._dirty_run_ids: set[str] = set()
        self._prior_run_ids: set[str] = set()
        self._runs_by_id: dict[str, RunRecord] = {}
        self._payload_loaded_run_ids: set[str] = set()
        self._chapter_runs: dict[str, list[RunRecord]] = {}
        self._latest_runs_by_chapter: dict[str, RunRecord] = {}
        self._latest_sorry_counts: dict[str, tuple[tuple[str, str], int]] = {}
        self._usage_cache: dict[tuple[bool, str | None], TokenUsage] = {}
        self._cost_cache: dict[tuple[bool, str | None, frozenset[str] | None], CostEstimate] = {}
        self._task_snapshot_context_key: tuple[int, ...] | None = None
        self._task_snapshot_context_cache: dict[str, Any] | None = None
        self._indexed_task_states: dict[str, tuple[str, str, bool, str]] = {}
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
        self.steward_cases: dict[str, dict[str, Any]] = {}
        self.coordination_signals: dict[str, dict[str, Any]] = {}
        self.coordination_cases: dict[str, dict[str, Any]] = {}
        self.package_state = PackageState()
        self.proof_blockers: dict[str, dict[str, Any]] = {}
        self.routing_metrics: dict[str, int] = {}
        self.thread_cumulative_usage: dict[str, TokenUsage] = {}
        self.isolation: dict[str, Any] = {}
        self.coordinator_build = CoordinatorBuildRecord()
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
        self.package_state = await asyncio.to_thread(self._database.load_package_state)
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
            raw_steward_cases = raw.get("steward_cases")
            if isinstance(raw_steward_cases, dict):
                self.steward_cases = {
                    case_id: dict(value)
                    for case_id, value in raw_steward_cases.items()
                    if isinstance(case_id, str) and isinstance(value, dict)
                }
            for section in (
                "coordination_signals",
                "coordination_cases",
            ):
                raw_values = raw.get(section)
                if isinstance(raw_values, dict):
                    setattr(
                        self,
                        section,
                        {
                            str(item_id): dict(value)
                            for item_id, value in raw_values.items()
                            if isinstance(item_id, str) and isinstance(value, dict)
                        },
                    )
            raw_proof_blockers = raw.get("proof_blockers")
            if isinstance(raw_proof_blockers, dict):
                self.proof_blockers = {
                    blocker_id: dict(value)
                    for blocker_id, value in raw_proof_blockers.items()
                    if isinstance(blocker_id, str) and isinstance(value, dict)
                }
            raw_routing_metrics = raw.get("routing_metrics")
            if isinstance(raw_routing_metrics, dict):
                self.routing_metrics = {
                    str(name): int(value)
                    for name, value in raw_routing_metrics.items()
                    if isinstance(value, int)
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
                    raw_waiting = task_value.get("waiting_on", ())
                    task_value["waiting_on"] = tuple(
                        Requirement(
                            kind=RequirementKind(str(item.get("kind"))),
                            owner_task_key=(
                                str(item["owner_task_key"])
                                if item.get("owner_task_key") is not None
                                else None
                            ),
                            request_id=(
                                str(item["request_id"])
                                if item.get("request_id") is not None
                                else None
                            ),
                            detail=str(item.get("detail", "")),
                        )
                        for item in raw_waiting
                        if isinstance(item, dict)
                        and str(item.get("kind", "")) in RequirementKind._value2member_map_
                    )
                    if legacy_workflow and task_value.get("stage") == "formalize":
                        task_value["stage"] = "discover"
                    elif task_value.get("stage") in {"repair", "fixup"}:
                        task_value["stage"] = "formalize"
                    legacy_review_green = value.get("review_green")
                    if task_value.get("stage") == Stage.REVIEW:
                        if (
                            task_value.get("status") == TaskStatus.FAILED
                            and task_value.get("detail") == "formalization did not complete"
                        ):
                            task_value["status"] = TaskStatus.BLOCKED
                        if legacy_review_green is True:
                            task_value["status"] = TaskStatus.SUCCEEDED
                        elif (
                            legacy_review_green is False
                            and task_value.get("status") == TaskStatus.SUCCEEDED
                        ):
                            task_value["status"] = TaskStatus.PENDING
                    if task_value.get("status") == TaskStatus.BLOCKED:
                        # BLOCKED was historically used for terminal proof outcomes, including
                        # consumers awaiting Steward work. Those tasks must stay unschedulable
                        # until an explicit retry or successful repair reopens them.
                        task_value["status"] = TaskStatus.FAILED
                        task_value["waiting_on"] = ()
                    self.tasks[key] = TaskRecord(**task_value)
                for task in self.tasks.values():
                    if task.sorry_count is not None and task.sorry_count_updated_at is not None:
                        self._latest_sorry_counts[task.chapter_id] = (
                            (task.sorry_count_updated_at, "coordinator"),
                            task.sorry_count,
                        )
        self._seed_normalized_caches()
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
        self._normalize_upstream_request_state()
        configured = {chapter.id for chapter in self.config.work_units}
        self.tasks = {
            key: task for key, task in self.tasks.items() if task.chapter_id in configured
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
            if (
                formalize.status != TaskStatus.SUCCEEDED
                and not formalize.recovering_failure
                and (
                    review.rounds > 0
                    or prove.rounds > 0
                    or review.status in (TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
                    or prove.status in (TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
                )
            ):
                formalize.status = TaskStatus.SUCCEEDED
                formalize.phase = TaskPhase.IDLE
                formalize.detail = "formalization completed before review"
                formalize.updated_at = timestamp()
        for task in self.tasks.values():
            self._migrate_legacy_task_wait(task)
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
        if self.coordinator_build.active:
            self.coordinator_build.active = False
            self.coordinator_build.current_chapter_id = ""
            self.coordinator_build.updated_at = timestamp()
            self._coordinator_build_dirty = True
        self._invalidate_aggregates()
        self._rebuild_status_indexes()
        self._checkpoint_dirty = True
        self._dirty_sections.update(COLLECTION_SECTIONS | GRAPH_SECTIONS)
        if raw is None:
            self._coordinator_build_dirty = True
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

    def _normalize_upstream_request_state(self) -> None:
        """Repair imported upstream coordination state and remove its legacy review route."""

        valid_statuses = {value.value for value in UpstreamRequestStatus}
        for request_id, request in tuple(self.proof_review_requests.items()):
            if request.get("kind") == "upstream_request":
                self.proof_review_requests.pop(request_id, None)
        for case in self.steward_cases.values():
            # Before repair agents separated readable context from writable scope, every context
            # work unit was locked. Preserve that authority when importing older state.
            if "write_work_unit_ids" not in case:
                case["write_work_unit_ids"] = list(case.get("context_work_unit_ids", ()))
            if case.get("disposition") == "implement":
                case["disposition"] = "repair"
            if case.get("status") == "implementing":
                case["status"] = "repairing"
            if "repair_run_ids" not in case and "implementation_run_ids" in case:
                case["repair_run_ids"] = list(case.get("implementation_run_ids", ()))
            if (
                "active_repair_generation" not in case
                and "active_implementation_generation" in case
            ):
                case["active_repair_generation"] = case["active_implementation_generation"]
            if "active_repair_run_id" not in case and "active_implementation_run_id" in case:
                case["active_repair_run_id"] = case["active_implementation_run_id"]
            if case.get("status") == "needs_human":
                case["status"] = "failed"
            if case.get("disposition") == "needs_human":
                case["disposition"] = "failed"
        next_number = 1 + max(
            (int(key[1:]) for key in self.proof_blockers if key[1:].isdigit()),
            default=0,
        )
        for request_id, request in self.upstream_requests.items():
            request["id"] = request_id
            if request.get("status") == "needs_human":
                request["status"] = UpstreamRequestStatus.FAILED.value
            if request.get("status") not in valid_statuses:
                request["status"] = UpstreamRequestStatus.OPEN.value
            blocker_ids = [
                str(value)
                for value in request.get("blocker_ids", ())
                if str(value) in self.proof_blockers
            ]
            if not blocker_ids:
                blocker_id = f"B{next_number}"
                next_number += 1
                now = timestamp()
                self.proof_blockers[blocker_id] = {
                    "id": blocker_id,
                    "fingerprint": stable_digest_text(request_id)[:20],
                    "consumer_chapter_id": str(request.get("consumer_chapter_id", "")),
                    "path": str(request.get("consumer_path", "")),
                    "declaration": str(request.get("blocked_declaration", "")),
                    "remaining_goal": str(request.get("residual_goal", "")),
                    "obstruction": str(request.get("needed_result", "")),
                    "disposition": "missing_capability",
                    "attempts": list(request.get("attempted_alternatives", ())),
                    "origin_run_ids": list(request.get("origin_run_ids", ())),
                    "sightings": 1,
                    "status": ProofBlockerStatus.UPSTREAM_REQUESTED.value,
                    "upstream_request_id": request_id,
                    "created_at": str(request.get("created_at") or now),
                    "updated_at": now,
                }
                blocker_ids = [blocker_id]
            request["blocker_ids"] = blocker_ids
            for blocker_id in blocker_ids:
                blocker = self.proof_blockers[blocker_id]
                if blocker.get("status") in {
                    ProofBlockerStatus.PACKAGE_REQUIRED.value,
                    ProofBlockerStatus.WAITING_DEPENDENCY.value,
                } and request.get("status") in {
                    UpstreamRequestStatus.OPEN.value,
                    UpstreamRequestStatus.EVALUATING.value,
                }:
                    blocker["status"] = ProofBlockerStatus.UPSTREAM_REQUESTED.value
                blocker["upstream_request_id"] = request_id
                blocker.pop("package_id", None)

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
            "failure_kind": run.failure_kind,
            "error": run.error,
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
        failure_roots: dict[str, tuple[str, ...]] | None = None,
        readiness_requirements: tuple[Requirement, ...] | None = None,
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
        waiting_on = []
        for requirement in (
            task.waiting_on if readiness_requirements is None else readiness_requirements
        ):
            summary: dict[str, Any] = {
                "kind": str(requirement.kind),
                "detail": requirement.detail[:WAITING_DETAIL_MAXIMUM],
            }
            if requirement.owner_task_key is not None:
                summary["owner_task_key"] = requirement.owner_task_key
            if requirement.request_id is not None:
                summary["request_id"] = requirement.request_id
            waiting_on.append(summary)
        task_key = self.key(task.chapter_id, Stage(task.stage))
        failed_requirements = self.failed_requirements(
            task,
            roots=failure_roots.get(task_key, ()) if failure_roots is not None else None,
        )
        active_run = self._active_runs_by_chapter.get(task.chapter_id)
        active_auxiliary_role = (
            active_run.role
            if active_run is not None
            and active_run.auxiliary
            and active_run.stage == str(task.stage)
            else ""
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
            "active_auxiliary_role": active_auxiliary_role,
            "phase": str(task.phase),
            "detail": task.detail,
            "queued": task.queued,
            "waiting_on": waiting_on,
            "scheduling_status": (
                "queued"
                if task.queued
                else "blocked"
                if failed_requirements
                else "waiting"
                if task.status == TaskStatus.PENDING
                else "executing"
                if task.status == TaskStatus.RUNNING
                else "complete"
            ),
            "blocked_by": [
                requirement.owner_task_key
                for requirement in failed_requirements
                if requirement.owner_task_key is not None
            ],
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
            "sorry_count": (
                latest[1]
                if (latest := self._latest_sorry_counts.get(task.chapter_id)) is not None
                else None
            ),
            "sorry_count_updated_at": task.sorry_count_updated_at,
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

    def _bounded_global_snapshot(self) -> dict[str, Any]:
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
            "isolation": self.isolation,
            "routing_metrics": dict(sorted(self.routing_metrics.items())),
        }

    def record_routing_event(self, name: str, count: int = 1) -> None:
        """Record a bounded blocker-routing counter in the global snapshot."""

        self.routing_metrics[name] = self.routing_metrics.get(name, 0) + count
        self._mark_dirty(global_state=True)

    def _global_snapshot(self) -> dict[str, Any]:
        return (
            self._bounded_global_snapshot()
            | {
                "scheduling": self.scheduling,
                "source_dependency_tree": self.source_dependency_tree,
                "formalize_graph": self.formalize_graph,
                "fixup_requests": self.fixup_requests,
                "proof_review_requests": self.proof_review_requests,
                "upstream_requests": self.upstream_requests,
                "steward_cases": self.steward_cases,
                "coordination_signals": self.coordination_signals,
                "coordination_cases": self.coordination_cases,
                "proof_blockers": self.proof_blockers,
                "thread_cumulative_usage": {
                    thread_id: self._usage_dict(usage)
                    for thread_id, usage in sorted(self.thread_cumulative_usage.items())
                },
                "coordinator_build": self._build_dict(self.coordinator_build),
            }
            | self.package_state.as_dict()
        )

    def _section_value(self, section: str) -> Any:
        if section == "thread_cumulative_usage":
            return {
                thread_id: self._usage_dict(usage)
                for thread_id, usage in sorted(self.thread_cumulative_usage.items())
            }
        if section == "coordinator_targets":
            return self.coordinator_build.target_work_unit_ids
        return getattr(self, section)

    def _section_object_ids(self) -> dict[str, int]:
        values = {
            section: id(getattr(self, section))
            for section in (COLLECTION_SECTIONS | GRAPH_SECTIONS).difference(
                {"coordinator_targets"}
            )
        }
        values["coordinator_targets"] = id(self.coordinator_build.target_work_unit_ids)
        return values

    def _seed_normalized_caches(self) -> None:
        self._collection_cache = {
            section: {
                key: (ordinal, json.dumpb(payload))
                for key, (ordinal, payload) in collection_snapshot(
                    section, self._section_value(section)
                ).items()
            }
            for section in COLLECTION_SECTIONS
        }
        self._graph_cache = {
            section: self._cache_graph_snapshot(
                graph_snapshot(section, self._section_value(section))
            )
            for section in GRAPH_SECTIONS
        }
        self._persisted_section_object_ids = self._section_object_ids()
        self._persisted_header_object_ids = self._header_object_ids()

    @staticmethod
    def _cache_graph_snapshot(snapshot: GraphSnapshot) -> GraphSnapshot:
        """Serialize mutable payloads into compact immutable cache values."""

        return GraphSnapshot(
            {key: json.dumpb(payload) for key, payload in snapshot.metadata.items()},
            {
                key: (ordinal, json.dumpb(payload))
                for key, (ordinal, payload) in snapshot.nodes.items()
            },
            dict(snapshot.edges),
        )

    def _header_object_ids(self) -> dict[str, int]:
        return {"isolation": id(self.isolation)}

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
        failure_roots = self._failure_roots_index()
        readiness_requirements = self._readiness_requirements_subset(self.tasks)
        return self._global_snapshot() | {
            "documents": [dict(value) for value in self._document_dicts()],
            "work_units": [dict(value) for value in self._work_unit_dicts()],
            "tasks": {
                key: self._hot_task_dict(
                    task,
                    task_context,
                    failure_roots,
                    readiness_requirements.get(key, ()),
                )
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
        for legacy_key in (
            "upstream_requests",
            "capability_packages",
            "package_consumers",
            "package_steps",
            "package_evidence",
            "steward_leases",
            "path_reservations",
            "package_dependencies",
            "relevant_read_interfaces",
        ):
            snapshot.pop(legacy_key, None)
        # Task rows already project build freshness.  The native dashboard previously received
        # the complete normalized graph (several MiB on large corpora) only to test clean-member
        # membership while drawing each row.
        snapshot.pop("formalize_graph", None)
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
        failure_roots = self._failure_roots_subset(task_keys) if task_keys else None
        readiness_requirements = self._readiness_requirements_subset(task_keys)
        tasks = {
            key: self._hot_task_dict(
                self.tasks[key],
                task_context,
                failure_roots,
                readiness_requirements.get(key, ()),
            )
            | {
                "work_unit_usage": self._usage_dict(
                    self.invocation_usage(self.tasks[key].work_unit_id)
                ),
                "work_unit_cost": self.invocation_cost(self.tasks[key].work_unit_id).as_dict(),
            }
            for key in task_keys
        }
        active_run_ids = sorted(self._active_run_ids)
        # The initial snapshot seeds every active activity.  Subsequent deltas only need activity
        # records whose run or owning task changed; active_run_ids is a separate membership signal.
        # Including every active activity here made each terminal frame O(active agents), even for
        # a single agent's update.
        run_ids = set(change.runs)
        run_ids.update(
            run_id
            for task in tasks.values()
            if isinstance((run_id := task.get("latest_run_id")), str)
        )
        global_names = change.globals.difference(
            {
                "activity",
                "formalize_graph",
                "upstream_requests",
                "capability_packages",
                "package_consumers",
                "package_steps",
                "package_evidence",
                "steward_leases",
                "path_reservations",
                "package_dependencies",
                "relevant_read_interfaces",
            }
        )
        if "state" in global_names:
            globals_ = self._bounded_global_snapshot() | {"revision": self.revision}
        else:
            globals_ = {}
        if "coordinator_build" in global_names:
            globals_["coordinator_build"] = self._build_dict(self.coordinator_build)
        for section in sorted(
            global_names.intersection(COLLECTION_SECTIONS | GRAPH_SECTIONS).difference(
                {"coordinator_targets"}
            )
        ):
            globals_[section] = self._section_value(section)
        if change.stages or change.runs:
            globals_["agents"] = self.agent_summary()
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
        failure_roots = self._failure_roots_index()
        readiness_requirements = self._readiness_requirements_subset(self.tasks)
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
            tasks[key] = self._task_dict(
                task,
                task_context,
                failure_roots,
                readiness_requirements.get(key, ()),
            ) | {"runs": runs}
        snapshot["source_issues"] = [
            self._issue_dict(issue) for _, issue in sorted(self.source_issues.items())
        ]
        snapshot["tasks"] = tasks
        return snapshot

    def _hot_task_dict(
        self,
        task: TaskRecord,
        context: dict[str, Any] | None = None,
        failure_roots: dict[str, tuple[str, ...]] | None = None,
        readiness_requirements: tuple[Requirement, ...] | None = None,
    ) -> dict[str, Any]:
        dashboard_run = next(
            reversed(task.runs),
            None,
        )
        return self._task_dict(task, context, failure_roots, readiness_requirements) | {
            "run_count": len(task.runs),
            "latest_run_id": dashboard_run.id if dashboard_run is not None else None,
        }

    def _persisted_task_dict(
        self,
        task: TaskRecord,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Serialize canonical task state without derived failure presentation."""

        return self._hot_task_dict(task, context, {})

    def _mark_dirty(
        self,
        *,
        task: TaskRecord | None = None,
        tasks: Iterable[TaskRecord] = (),
        run: RunRecord | None = None,
        issues: bool = False,
        static: bool = False,
        global_state: bool = True,
        sections: Iterable[str] = (),
    ) -> None:
        self._checkpoint_dirty = self._checkpoint_dirty or global_state
        self._dirty_sections.update(sections)
        changed_tasks = [*tasks]
        if task is not None:
            changed_tasks.append(task)
        failure_transitions = {
            self.key(item.chapter_id, Stage(item.stage))
            for item in changed_tasks
            if (
                previous := self._indexed_task_states.get(
                    self.key(item.chapter_id, Stage(item.stage))
                )
            )
            is not None
            and (previous[1] == TaskStatus.FAILED) != (item.status == TaskStatus.FAILED)
        }
        if failure_transitions:
            self._dirty_projection_work_units.update(
                item.chapter_id for item in self._failure_dependents(failure_transitions)
            )
        changed_tasks = list(
            {self.key(item.chapter_id, Stage(item.stage)): item for item in changed_tasks}.values()
        )
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
        self._dirty_sections.add("coordinator_targets")

    def _mark_thread_usage_dirty(self, thread_id: str) -> None:
        self._thread_usage_dirty = True
        self._dirty_thread_usage_ids.add(thread_id)

    async def _persist(self) -> None:
        if self._batch_depth:
            return
        await asyncio.sleep(0)
        await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            section_object_ids = self._section_object_ids()
            header_object_ids = self._header_object_ids()
            if header_object_ids != self._persisted_header_object_ids:
                self._checkpoint_dirty = True
            self._dirty_sections.update(
                section
                for section, object_id in section_object_ids.items()
                if self._persisted_section_object_ids.get(section) != object_id
            )
            if not (
                self._checkpoint_dirty
                or self._coordinator_build_dirty
                or self._thread_usage_dirty
                or self._dirty_thread_usage_ids
                or self._dirty_sections
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
            dirty_thread_usage_ids = self._dirty_thread_usage_ids
            dirty_sections = self._dirty_sections
            task_keys = self._dirty_task_keys
            projection_work_units = self._dirty_projection_work_units
            dirty_runs = self._dirty_run_ids
            issues_dirty = self._issues_dirty
            static_dirty = self._static_dirty
            self._dirty_task_keys = set()
            self._dirty_projection_work_units = set()
            self._dirty_run_ids = set()
            self._checkpoint_dirty = False
            self._coordinator_build_dirty = False
            self._thread_usage_dirty = False
            self._dirty_thread_usage_ids = set()
            self._dirty_sections = set()
            self._issues_dirty = False
            self._static_dirty = False
            task_context = self._task_snapshot_context() if task_keys else None
            task_payloads = {
                key: json.dumpb(self._persisted_task_dict(self.tasks[key], task_context))
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
            collection_writes: dict[str, CollectionWrite] = {}
            next_collection_cache: dict[str, dict[str, tuple[int, bytes]]] = {}
            for section in sorted(dirty_sections.intersection(COLLECTION_SECTIONS)):
                current = collection_snapshot(section, self._section_value(section))
                serialized = {
                    key: (ordinal, json.dumpb(payload))
                    for key, (ordinal, payload) in current.items()
                }
                previous = self._collection_cache.get(section, {})
                upserts = {
                    key: value for key, value in serialized.items() if previous.get(key) != value
                }
                deletes = frozenset(previous.keys() - serialized.keys())
                if upserts or deletes:
                    collection_writes[section] = CollectionWrite(upserts, deletes)
                next_collection_cache[section] = serialized
            incremental_collection_cache_updates: dict[str, dict[str, tuple[int, bytes]]] = {}
            if dirty_thread_usage_ids and "thread_cumulative_usage" not in dirty_sections:
                section = "thread_cumulative_usage"
                previous = self._collection_cache.get(section, {})
                current_values = {
                    thread_id: (
                        previous.get(thread_id, (len(previous), None))[0],
                        self._usage_dict(self.thread_cumulative_usage[thread_id]),
                    )
                    for thread_id in dirty_thread_usage_ids
                    if thread_id in self.thread_cumulative_usage
                }
                current = {
                    key: (ordinal, json.dumpb(payload))
                    for key, (ordinal, payload) in current_values.items()
                }
                upserts = {
                    key: value for key, value in current.items() if previous.get(key) != value
                }
                if upserts:
                    collection_writes[section] = CollectionWrite(upserts, frozenset())
                incremental_collection_cache_updates[section] = current
            graph_writes: dict[str, GraphWrite] = {}
            next_graph_cache: dict[str, GraphSnapshot] = {}
            for section in sorted(dirty_sections.intersection(GRAPH_SECTIONS)):
                current = graph_snapshot(section, self._section_value(section))
                serialized = self._cache_graph_snapshot(current)
                previous = self._graph_cache.get(section, GraphSnapshot({}, {}, {}))
                metadata_upserts = {
                    key: payload
                    for key, payload in serialized.metadata.items()
                    if previous.metadata.get(key) != payload
                }
                node_upserts = {
                    key: value
                    for key, value in serialized.nodes.items()
                    if previous.nodes.get(key) != value
                }
                delta = GraphWrite(
                    metadata_upserts=metadata_upserts,
                    metadata_deletes=frozenset(previous.metadata.keys() - current.metadata.keys()),
                    node_upserts=node_upserts,
                    node_deletes=frozenset(previous.nodes.keys() - current.nodes.keys()),
                    edge_upserts={
                        edge: ordinal
                        for edge, ordinal in current.edges.items()
                        if edge not in previous.edges
                    },
                    edge_deletes=frozenset(previous.edges.keys() - current.edges.keys()),
                )
                if (
                    delta.metadata_upserts
                    or delta.metadata_deletes
                    or delta.node_upserts
                    or delta.node_deletes
                    or delta.edge_upserts
                    or delta.edge_deletes
                ):
                    graph_writes[section] = delta
                next_graph_cache[section] = serialized
            changed_sections = set(collection_writes) | set(graph_writes)
            changed_work_units = {self.tasks[key].chapter_id for key in task_payloads} | {
                run.chapter_id for run_id in runs if (run := self._runs_by_id.get(run_id))
            }
            formalize_write = graph_writes.get("formalize_graph")
            if formalize_write is not None:
                changed_work_units.update(
                    node_id
                    for kind, node_id in (
                        set(formalize_write.node_upserts) | set(formalize_write.node_deletes)
                    )
                    if kind == "dependency"
                )
            persisted_changed_work_units = set(changed_work_units)
            changed_work_units.update(projection_work_units)
            changed_stages = {self.tasks[key].stage for key in task_payloads} | {
                run.stage for run_id in runs if (run := self._runs_by_id.get(run_id))
            }
            changes = {
                *(("task", key) for key in task_payloads),
                *(("run", run_id) for run_id in runs),
                *(("work_unit", unit_id) for unit_id in persisted_changed_work_units),
            }
            if globals_dirty:
                changes.add(("global", "state"))
            if coordinator_build_dirty:
                changes.add(("global", "coordinator_build"))
            changes.update(
                ("global", section)
                for section in changed_sections.difference({"coordinator_targets"})
            )
            if issues_dirty:
                changes.add(("source_issues", "*"))
            if static_dirty:
                changes.add(("resync", "*"))
            write = DatabaseWrite(
                updated_at=self.updated_at,
                globals=(
                    {"state": json.dumpb(self._bounded_global_snapshot())} if globals_dirty else {}
                )
                | (
                    {
                        "coordinator_build": json.dumpb(
                            {
                                key: value
                                for key, value in self._build_dict(self.coordinator_build).items()
                                if key != "target_work_unit_ids"
                            }
                        )
                    }
                    if coordinator_build_dirty
                    else {}
                ),
                collections=collection_writes,
                graphs=graph_writes,
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
            self._collection_cache.update(next_collection_cache)
            for section, updates in incremental_collection_cache_updates.items():
                self._collection_cache.setdefault(section, {}).update(updates)
            self._graph_cache.update(next_graph_cache)
            self._persisted_section_object_ids.update(section_object_ids)
            if globals_dirty:
                self._persisted_header_object_ids = header_object_ids
            self.revision = revision
            self.change_bus.publish(
                ChangeSet(
                    revision=revision,
                    work_units=frozenset(changed_work_units),
                    runs=frozenset(runs),
                    globals=frozenset(
                        ({"state"} if globals_dirty else set())
                        | ({"coordinator_build"} if coordinator_build_dirty else set())
                        | changed_sections.difference({"coordinator_targets"})
                    ),
                    stages=frozenset(changed_stages),
                    full_resync=static_dirty,
                )
            )

    async def save(self, *sections: str) -> None:
        selected = set(sections) if sections else set(COLLECTION_SECTIONS | GRAPH_SECTIONS)
        self._mark_dirty(global_state=not sections or "state" in selected, sections=selected)
        await self._persist()

    def save_deferred(self, *sections: str) -> None:
        """Mark state for a short, coalescing durability flush.

        Use this for high-frequency derived state whose in-memory value is authoritative to the
        running scheduler. Shutdown still flushes the latest value before closing the database.
        """

        selected = set(sections) if sections else set(COLLECTION_SECTIONS | GRAPH_SECTIONS)
        self._mark_dirty(global_state=not sections or "state" in selected, sections=selected)
        self._schedule_telemetry_flush()

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
        self._mark_dirty(
            tasks=changed_tasks,
            global_state=False,
            sections={"source_dependency_tree", "formalize_graph"},
        )
        await self._persist()

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

    @staticmethod
    def _proof_blocker_semantic_key(attempt: dict[str, Any]) -> tuple[str, str, str]:
        """Stable identity for wording variants of the same residual declaration goal."""

        path = str(attempt.get("path", "")).strip()
        declaration = str(attempt.get("declaration", "")).strip().rsplit(".", 1)[-1]
        goal = re.sub(r"\s+", " ", str(attempt.get("remaining_goal", ""))).strip()
        return path, declaration, goal.removeprefix("⊢ ")

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
        capability_candidates: Iterable[dict[str, Any]] = (),
    ) -> tuple[dict[str, Any], ...]:
        """Merge proof-failure deltas into a restart-safe declaration ledger."""

        changed: dict[str, dict[str, Any]] = {}
        by_fingerprint = {
            str(value.get("fingerprint", "")): value
            for value in self.proof_blockers.values()
            if value.get("consumer_chapter_id") == chapter_id
        }
        by_semantic_key = {
            self._proof_blocker_semantic_key(value): value
            for value in self.proof_blockers.values()
            if value.get("consumer_chapter_id") == chapter_id
        }
        candidates = tuple(value for value in capability_candidates if isinstance(value, dict))
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
                blocker = by_semantic_key.get(self._proof_blocker_semantic_key(raw))
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
                by_semantic_key[self._proof_blocker_semantic_key(raw)] = blocker
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
                retry_cause = str(blocker.get("retry_cause_digest", ""))
                if retry_cause and blocker.get("last_attempted_retry_cause_digest") != retry_cause:
                    blocker["last_attempted_retry_cause_digest"] = retry_cause
                    blocker["last_retry_run_id"] = origin_run_id
                    blocker["status"] = ProofBlockerStatus.OPEN.value
                    self.record_routing_event("unchanged_retry_suppressed")
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
                    blocker["capability"] = dict(candidate)
                    blocker["disposition"] = "missing_capability"
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
                retry_cause = str(blocker.get("retry_cause_digest", ""))
                if retry_cause and blocker.get("last_attempted_retry_cause_digest") != retry_cause:
                    blocker["last_attempted_retry_cause_digest"] = retry_cause
                    blocker["last_retry_run_id"] = origin_run_id
                    blocker["status"] = ProofBlockerStatus.OPEN.value
                    self.record_routing_event("unchanged_retry_suppressed")
            blocker["updated_at"] = timestamp()
            changed[str(blocker["id"])] = blocker
        if changed:
            self._mark_dirty(global_state=False, sections={"proof_blockers"})
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
            self._mark_dirty(global_state=False, sections={"proof_blockers"})
            await self._persist()

    async def attach_proof_blockers_to_package(
        self, blocker_ids: Iterable[str], package_id: str
    ) -> None:
        """Move structural blockers under package ownership without a peer request."""

        changed = False
        for blocker_id in blocker_ids:
            blocker = self.proof_blockers.get(str(blocker_id))
            if not isinstance(blocker, dict):
                continue
            blocker["status"] = ProofBlockerStatus.WAITING_DEPENDENCY.value
            blocker["package_id"] = package_id
            blocker.pop("request_id", None)
            blocker["updated_at"] = timestamp()
            changed = True
        if changed:
            self.record_routing_event("proof_blocker_attached_to_package")
            self._mark_dirty(global_state=False, sections={"proof_blockers"})
            await self._persist()

    async def refresh_package_state(self) -> PackageState:
        self.package_state = await asyncio.to_thread(self._database.load_package_state)
        self._mark_dirty(global_state=True)
        await self._persist()
        return self.package_state

    async def claim_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        *,
        expected_revision: int,
        ttl_seconds: float,
        now: str | None = None,
    ) -> StewardLease:
        lease = await asyncio.to_thread(
            self._database.claim_steward_lease,
            package_id,
            agent_id,
            expected_revision=expected_revision,
            ttl_seconds=ttl_seconds,
            now=now,
        )
        await self.refresh_package_state()
        return lease

    async def heartbeat_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        generation: int,
        *,
        ttl_seconds: float,
        now: str | None = None,
    ) -> StewardLease:
        lease = await asyncio.to_thread(
            self._database.heartbeat_steward_lease,
            package_id,
            agent_id,
            generation,
            ttl_seconds=ttl_seconds,
            now=now,
        )
        await self.refresh_package_state()
        return lease

    async def release_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        generation: int,
        *,
        release_reservations: bool = False,
        now: str | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._database.release_steward_lease,
            package_id,
            agent_id,
            generation,
            release_reservations=release_reservations,
            now=now,
        )
        await self.refresh_package_state()

    async def recover_steward_lease(
        self,
        package_id: str,
        agent_id: str,
        *,
        expected_revision: int,
        ttl_seconds: float,
        active_child_workers: tuple[str, ...] = (),
        now: str | None = None,
    ) -> tuple[StewardLease, PackageRecovery]:
        result = await asyncio.to_thread(
            self._database.recover_steward_lease,
            package_id,
            agent_id,
            expected_revision=expected_revision,
            ttl_seconds=ttl_seconds,
            active_child_workers=active_child_workers,
            now=now,
        )
        await self.refresh_package_state()
        return result

    async def claim_ordinary_path_reservations(
        self,
        owner_id: str,
        requested: tuple[ReservationSpec, ...],
        *,
        ttl_seconds: float,
        queue_on_conflict: bool = True,
    ) -> ReservationResult:
        return await asyncio.to_thread(
            self._database.claim_ordinary_path_reservations,
            owner_id,
            requested,
            ttl_seconds=ttl_seconds,
            queue_on_conflict=queue_on_conflict,
        )

    async def release_ordinary_path_reservations(
        self, owner_id: str, fence_generation: int
    ) -> None:
        await asyncio.to_thread(
            self._database.release_path_reservations,
            ReservationOwnerKind.ORDINARY_TASK,
            owner_id,
            fence_generation,
        )

    async def expand_package_write_scope(
        self,
        package_id: str,
        lease_generation: int,
        requested: tuple[ReservationSpec, ...],
        *,
        expected_revision: int,
        queue_on_conflict: bool = True,
    ) -> ReservationResult:
        result = await asyncio.to_thread(
            self._database.expand_package_write_scope,
            package_id,
            lease_generation,
            requested,
            expected_revision=expected_revision,
            queue_on_conflict=queue_on_conflict,
        )
        await self.refresh_package_state()
        return result

    async def create_or_attach_capability_package(
        self,
        package: CapabilityPackage,
        *,
        consumer: PackageConsumer | None = None,
        evidence: tuple[PackageEvidence, ...] = (),
        expected_revision: int | None = None,
    ) -> tuple[CapabilityPackage, bool]:
        result = await asyncio.to_thread(
            self._database.create_or_attach_capability_package,
            package,
            consumer=consumer,
            evidence=evidence,
            expected_revision=expected_revision,
        )
        await self.refresh_package_state()
        return result

    async def attach_package_consumer(
        self,
        package_id: str,
        consumer: PackageConsumer,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> PackageConsumer:
        result = await asyncio.to_thread(
            self._database.attach_package_consumer,
            package_id,
            consumer,
            expected_revision=expected_revision,
            lease_generation=lease_generation,
        )
        await self.refresh_package_state()
        return result

    async def update_package_lifecycle(
        self,
        package_id: str,
        status: PackageStatus,
        *,
        expected_revision: int,
        disposition: PackageDisposition | None = None,
        plan_revision: int | None = None,
        integrated_revision: str | None = None,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        result = await asyncio.to_thread(
            self._database.update_package_lifecycle,
            package_id,
            status,
            expected_revision=expected_revision,
            disposition=disposition,
            plan_revision=plan_revision,
            integrated_revision=integrated_revision,
            lease_generation=lease_generation,
        )
        await self.refresh_package_state()
        return result

    async def add_package_dependency(
        self,
        dependency: PackageDependency,
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        result = await asyncio.to_thread(
            self._database.add_package_dependency,
            dependency,
            expected_revision=expected_revision,
            lease_generation=lease_generation,
        )
        await self.refresh_package_state()
        return result

    async def merge_capability_packages(
        self,
        survivor_id: str,
        merged_id: str,
        *,
        expected_survivor_revision: int,
        expected_merged_revision: int,
        lease_generation: int | None = None,
    ) -> CapabilityPackage:
        result = await asyncio.to_thread(
            self._database.merge_capability_packages,
            survivor_id,
            merged_id,
            expected_survivor_revision=expected_survivor_revision,
            expected_merged_revision=expected_merged_revision,
            lease_generation=lease_generation,
        )
        await self.refresh_package_state()
        return result

    async def split_capability_package(
        self,
        parent_id: str,
        children: tuple[CapabilityPackage, ...],
        consumer_assignments: dict[str, tuple[str, ...]],
        *,
        expected_revision: int,
        lease_generation: int | None = None,
    ) -> tuple[CapabilityPackage, ...]:
        result = await asyncio.to_thread(
            self._database.split_capability_package,
            parent_id,
            children,
            consumer_assignments,
            expected_revision=expected_revision,
            lease_generation=lease_generation,
        )
        await self.refresh_package_state()
        return result

    async def reopen_proof_blockers(self, chapter_ids: Iterable[str]) -> list[str]:
        """Start a fresh retry window for unresolved proof evidence."""

        selected = set(chapter_ids)
        reopened: list[str] = []
        reopened_statuses = {
            ProofBlockerStatus.BLOCKED.value,
        }
        for blocker_id, blocker in self.proof_blockers.items():
            if blocker.get("consumer_chapter_id") not in selected:
                continue
            status = blocker.get("status")
            if status == ProofBlockerStatus.RESOLVED.value:
                continue
            if status in reopened_statuses:
                blocker["status"] = ProofBlockerStatus.OPEN.value
            blocker["retry_sighting_baseline"] = int(blocker.get("sightings", 0))
            blocker["updated_at"] = timestamp()
            reopened.append(blocker_id)
        if reopened:
            self._mark_dirty(global_state=False, sections={"proof_blockers"})
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
        self._mark_dirty(global_state=False, sections={"fixup_requests"})
        await self._persist()
        return request_id

    async def finish_fixup_requests(self, request_ids: Iterable[str]) -> None:
        changed = False
        for request_id in request_ids:
            changed = self.fixup_requests.pop(request_id, None) is not None or changed
        if changed:
            self._mark_dirty(global_state=False, sections={"fixup_requests"})
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
            self._mark_dirty(global_state=False, sections={"fixup_requests"})
            await self._persist()
        return migrated

    async def migrate_stale_snapshot_review_requests(self) -> set[str]:
        """Discard orchestration-only stale builds that were persisted as review findings."""

        markers = (
            "Source dependency scope changed during the coordinator build; retry required.",
            "The source changed after coordinator verification.",
        )
        affected: set[str] = set()
        changed = False
        async with self.batch():
            for request_id, value in tuple(self.proof_review_requests.items()):
                if not isinstance(value, dict):
                    continue
                if value.get("kind") != "build_error":
                    continue
                feedback = value.get("feedback")
                if not isinstance(feedback, dict):
                    continue
                for chapter_id, block in tuple(feedback.items()):
                    if isinstance(block, str) and any(marker in block for marker in markers):
                        feedback.pop(chapter_id, None)
                        affected.add(chapter_id)
                        changed = True
                if not feedback:
                    self.proof_review_requests.pop(request_id, None)
            if changed:
                for chapter_id in affected:
                    has_real_feedback = any(
                        isinstance((feedback := request.get("feedback")), dict)
                        and chapter_id in feedback
                        for request in self.proof_review_requests.values()
                        if isinstance(request, dict)
                    )
                    if not has_real_feedback and self.key(chapter_id, Stage.REVIEW) in self.tasks:
                        await self.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "coordinator verification retry queued",
                        )
                self._mark_dirty(
                    global_state=False,
                    sections={"proof_review_requests"},
                )
                await self._persist()
        return affected

    async def enqueue_upstream_request(
        self,
        request: dict[str, Any],
        *,
        consumer_chapter_id: str,
        owner_chapter_id: str,
        blocker_ids: Iterable[str] = (),
        origin_run_id: str = "",
    ) -> tuple[str, bool]:
        """Persist one consumer observation without assigning an execution owner."""

        selected_blockers = tuple(dict.fromkeys(str(value) for value in blocker_ids if value))
        for request_id, existing in self.upstream_requests.items():
            if selected_blockers and selected_blockers == tuple(existing.get("blocker_ids", ())):
                return request_id, False
        request_id = str(request.get("id", "")).strip() or f"upstream-{uuid4().hex[:12]}"
        now = timestamp()
        value = dict(request)
        value.update(
            {
                "id": request_id,
                "status": UpstreamRequestStatus.OPEN.value,
                "consumer_chapter_id": consumer_chapter_id,
                "owner_chapter_id": owner_chapter_id,
                "blocker_ids": list(selected_blockers),
                "origin_run_ids": [origin_run_id] if origin_run_id else [],
                "created_at": str(value.get("created_at") or now),
                "updated_at": now,
            }
        )
        self.upstream_requests[request_id] = value
        for blocker_id in selected_blockers:
            blocker = self.proof_blockers.get(blocker_id)
            if not isinstance(blocker, dict):
                continue
            blocker["status"] = ProofBlockerStatus.UPSTREAM_REQUESTED.value
            blocker["upstream_request_id"] = request_id
            blocker.pop("package_id", None)
            blocker["updated_at"] = now
        self.record_routing_event("upstream_request.created")
        self._dirty_projection_work_units.update({consumer_chapter_id, owner_chapter_id})
        self._mark_dirty(
            global_state=False,
            sections={"upstream_requests", "proof_blockers"},
        )
        await self._persist()
        return request_id, True

    async def update_upstream_request(
        self,
        request_id: str,
        status: UpstreamRequestStatus,
        *,
        decision: dict[str, Any] | None = None,
        evaluation_run_id: str = "",
    ) -> None:
        request = self.upstream_requests.get(request_id)
        if not isinstance(request, dict):
            return
        request["status"] = status.value
        request["updated_at"] = timestamp()
        if decision is not None:
            request["decision"] = decision
        if evaluation_run_id:
            request["evaluation_run_id"] = evaluation_run_id
        self._mark_dirty(global_state=False, sections={"upstream_requests"})
        await self._persist()

    async def replace_steward_cases(self, cases: Iterable[dict[str, Any]]) -> None:
        """Replace the steward's canonical case set after one global dedupe pass."""

        now = timestamp()
        previous = self.steward_cases
        replacement: dict[str, dict[str, Any]] = {}
        assigned_request_ids: set[str] = set()
        for raw in cases:
            case_id = str(raw.get("case_id", "")).strip()
            if not case_id or case_id in replacement:
                raise ValueError(f"invalid or repeated steward case id: {case_id or '<empty>'}")
            request_ids = [
                str(value)
                for value in raw.get("request_ids", ())
                if str(value) in self.upstream_requests
            ]
            overlap = assigned_request_ids.intersection(request_ids)
            if overlap:
                raise ValueError(
                    "steward assigned request(s) to multiple cases: " + ", ".join(sorted(overlap))
                )
            assigned_request_ids.update(request_ids)
            old = previous.get(case_id, {})
            value = dict(raw)
            value["id"] = case_id
            value["request_ids"] = request_ids
            old_generation = max(1, int(old.get("generation", 1)))
            execution_changed = bool(old) and any(
                tuple(str(item) for item in old.get(key, ()))
                != tuple(str(item) for item in value.get(key, ()))
                for key in ("request_ids", "context_work_unit_ids", "write_work_unit_ids")
            )
            execution_changed = execution_changed or (
                bool(old) and str(old.get("disposition", "")) != str(value.get("disposition", ""))
            )
            value["generation"] = old_generation + 1 if execution_changed else old_generation
            if old and not execution_changed:
                # A global dedupe pass is allowed to improve a case's prose, but it must not
                # resurrect a repair which is already running or terminal.
                value["status"] = str(old.get("status") or value.get("status") or "ready")
                for key in (
                    "active_repair_generation",
                    "active_repair_run_id",
                ):
                    if key in old:
                        value[key] = old[key]
            else:
                value["status"] = str(value.get("status") or "ready")
            value["created_at"] = str(old.get("created_at") or now)
            value["updated_at"] = now
            value["repair_run_ids"] = list(old.get("repair_run_ids", ()))
            replacement[case_id] = value
        for case_id, value in previous.items():
            if case_id not in replacement and value.get("status") in {
                "verified",
                "rejected",
                "resolved",
                "failed",
            }:
                replacement[case_id] = value
        self.steward_cases = replacement
        for request_id, request in self.upstream_requests.items():
            if request_id in assigned_request_ids:
                request["status"] = UpstreamRequestStatus.EVALUATING.value
                request["steward_case_id"] = next(
                    case_id
                    for case_id, case in replacement.items()
                    if request_id in case["request_ids"]
                )
                request["updated_at"] = now
        self._mark_dirty(
            global_state=False,
            sections={"steward_cases", "upstream_requests"},
        )
        await self._persist()

    async def update_steward_case(self, case_id: str, **changes: Any) -> None:
        case = self.steward_cases.get(case_id)
        if not isinstance(case, dict):
            return
        case.update(changes)
        case["updated_at"] = timestamp()
        self._mark_dirty(global_state=False, sections={"steward_cases"})
        await self._persist()

    async def upsert_steward_case(self, raw: dict[str, Any]) -> None:
        """Add one incrementally coordinated case without rewriting unrelated live cases."""

        case_id = str(raw.get("case_id", raw.get("id", ""))).strip()
        if not case_id:
            raise ValueError("steward cases require a stable id")
        now = timestamp()
        previous = self.steward_cases.get(case_id, {})
        value = dict(raw)
        value["id"] = case_id
        value["case_id"] = case_id
        value["generation"] = max(1, int(previous.get("generation", 1)))
        value["created_at"] = str(previous.get("created_at") or now)
        value["updated_at"] = now
        value["repair_run_ids"] = list(previous.get("repair_run_ids", ()))
        self.steward_cases[case_id] = value
        for request_id in value.get("request_ids", ()):
            request = self.upstream_requests.get(str(request_id))
            if not isinstance(request, dict):
                continue
            request["status"] = UpstreamRequestStatus.EVALUATING.value
            request["steward_case_id"] = case_id
            request["updated_at"] = now
        self._mark_dirty(
            global_state=False,
            sections={"steward_cases", "upstream_requests"},
        )
        await self._persist()

    async def set_source_issue_status(self, issue_ids: Iterable[str], status: str) -> None:
        """Record a reviewed source-issue disposition without discarding provenance."""

        changed = False
        now = timestamp()
        for issue_id in dict.fromkeys(map(str, issue_ids)):
            issue = self.source_issues.get(issue_id)
            if issue is None or issue.status == status:
                continue
            issue.status = status
            issue.last_seen_at = now
            changed = True
        if changed:
            self._mark_dirty(issues=True, global_state=False)
            await self._persist()

    async def update_steward_case_generation(
        self,
        case_id: str,
        generation: int,
        **changes: Any,
    ) -> bool:
        """Update one case only while its durable repair generation is current."""

        case = self.steward_cases.get(case_id)
        if not isinstance(case, dict) or max(1, int(case.get("generation", 1))) != generation:
            return False
        case.update(changes)
        case["updated_at"] = timestamp()
        self._mark_dirty(global_state=False, sections={"steward_cases"})
        await self._persist()
        return True

    async def upsert_coordination_signals(
        self,
        signals: Iterable[dict[str, Any]],
    ) -> tuple[str, ...]:
        """Persist new detector evidence without rewriting unchanged signal rows."""

        changed: list[str] = []
        now = timestamp()
        for raw in signals:
            signal_id = str(raw.get("id", "")).strip()
            if not signal_id:
                raise ValueError("coordination signals require a stable id")
            value = dict(raw)
            previous = self.coordination_signals.get(signal_id)
            if previous is not None:
                value["created_at"] = str(previous.get("created_at") or now)
                if value.get("evidence_digest") == previous.get("evidence_digest"):
                    continue
            else:
                value["created_at"] = now
            value["id"] = signal_id
            value["updated_at"] = now
            self.coordination_signals[signal_id] = value
            changed.append(signal_id)
        if changed:
            self._mark_dirty(global_state=False, sections={"coordination_signals"})
            await self._persist()
        return tuple(changed)

    async def sync_coordination_cases(
        self,
        proposals: Iterable[dict[str, Any]],
    ) -> tuple[str, ...]:
        """Merge deterministic signal groups while preserving active case generations."""

        changed: list[str] = []
        now = timestamp()
        terminal = {
            "closed",
            "parked",
            # Compatibility states written by the first incident implementation.
            "resolved",
            "failed",
            "awaiting_source_approval",
        }
        for raw in proposals:
            case_id = str(raw.get("id", "")).strip()
            if not case_id:
                raise ValueError("coordination cases require a stable id")
            signal_ids = tuple(
                dict.fromkeys(
                    str(value)
                    for value in raw.get("signal_ids", ())
                    if str(value) in self.coordination_signals
                )
            )
            if not signal_ids:
                continue
            previous = self.coordination_cases.get(case_id)
            if previous is None:
                value = dict(raw)
                value.update(
                    {
                        "id": case_id,
                        "signal_ids": list(signal_ids),
                        "status": "open",
                        "generation": 1,
                        "created_at": now,
                        "updated_at": now,
                    }
                )
            else:
                prior_ids = tuple(map(str, previous.get("signal_ids", ())))
                additions = tuple(value for value in signal_ids if value not in prior_ids)
                evidence_changed = str(raw.get("evidence_digest", "")) != str(
                    previous.get("evidence_digest", "")
                )
                if not additions and not evidence_changed:
                    continue
                value = dict(previous)
                value["signal_ids"] = [*prior_ids, *additions]
                value["work_unit_ids"] = list(
                    dict.fromkeys(
                        (
                            *map(str, previous.get("work_unit_ids", ())),
                            *map(str, raw.get("work_unit_ids", ())),
                        )
                    )
                )
                if str(previous.get("status")) in terminal:
                    value["status"] = "open"
                    value["generation"] = max(1, int(previous.get("generation", 1))) + 1
                    value["evidence_digest"] = str(raw.get("evidence_digest", ""))
                    value.pop("decision", None)
                    for key in (
                        "attempts",
                        "strong_used",
                        "force_strong",
                        "investigation_attempts",
                        "planner_attempts",
                        "scope_expansions",
                        "action_failures",
                        "force_planner",
                        "pending_evidence_digest",
                        "pending_signal_ids",
                        "operator_action_required",
                    ):
                        value.pop(key, None)
                elif str(previous.get("status")) not in {"open", "triaging"}:
                    value["pending_signal_ids"] = list(
                        dict.fromkeys(
                            (*map(str, previous.get("pending_signal_ids", ())), *additions)
                        )
                    )
                    value["pending_evidence_digest"] = str(raw.get("evidence_digest", ""))
                else:
                    value["evidence_digest"] = str(raw.get("evidence_digest", ""))
                    value["generation"] = max(1, int(previous.get("generation", 1))) + 1
                    for key in (
                        "attempts",
                        "strong_used",
                        "force_strong",
                        "investigation_attempts",
                        "planner_attempts",
                        "scope_expansions",
                        "action_failures",
                        "force_planner",
                        "decision",
                        "failure",
                        "operator_action_required",
                    ):
                        value.pop(key, None)
                value["updated_at"] = now
            self.coordination_cases[case_id] = value
            changed.append(case_id)
        if changed:
            self._mark_dirty(global_state=False, sections={"coordination_cases"})
            await self._persist()
        return tuple(changed)

    async def update_coordination_case_generation(
        self,
        case_id: str,
        generation: int,
        **changes: Any,
    ) -> bool:
        case = self.coordination_cases.get(case_id)
        if not isinstance(case, dict) or max(1, int(case.get("generation", 1))) != generation:
            return False
        case.update(changes)
        if (
            str(changes.get("status", "")) in {"closed", "parked"}
            and case.get("pending_evidence_digest")
            and case.get("pending_evidence_digest") != case.get("evidence_digest")
        ):
            case["prior_generation_outcome"] = {
                key: changes[key]
                for key in (
                    "status",
                    "decision",
                    "stale_decision",
                    "failure",
                    "action_outcome",
                )
                if key in changes
            }
            case["status"] = "open"
            case["generation"] = generation + 1
            case["evidence_digest"] = case.pop("pending_evidence_digest")
            if pending_ids := case.pop("pending_signal_ids", None):
                case["signal_ids"] = list(
                    dict.fromkeys((*map(str, case.get("signal_ids", ())), *map(str, pending_ids)))
                )
            for key in (
                "attempts",
                "strong_used",
                "force_strong",
                "investigation_attempts",
                "planner_attempts",
                "scope_expansions",
                "action_failures",
                "force_planner",
                "decision",
                "stale_decision",
                "failure",
                "operator_action_required",
            ):
                case.pop(key, None)
        case["updated_at"] = timestamp()
        self._mark_dirty(global_state=False, sections={"coordination_cases"})
        await self._persist()
        return True

    async def clear_upstream_coordination(self) -> None:
        """Drop outstanding observations and cases while retaining historical agent runs."""

        request_ids = set(self.upstream_requests)
        consumer_ids = {
            str(request.get("consumer_chapter_id", ""))
            for request in self.upstream_requests.values()
        }
        self.upstream_requests.clear()
        self.steward_cases.clear()
        for request_id, request in tuple(self.proof_review_requests.items()):
            if request_id in request_ids or request.get("kind") == "upstream_request":
                self.proof_review_requests.pop(request_id, None)
        for blocker in self.proof_blockers.values():
            if (
                blocker.get("upstream_request_id") in request_ids
                or blocker.get("status") == ProofBlockerStatus.UPSTREAM_REQUESTED.value
            ):
                blocker.pop("upstream_request_id", None)
                blocker.pop("request_id", None)
                blocker["status"] = ProofBlockerStatus.BLOCKED.value
                blocker["updated_at"] = timestamp()
        self._mark_dirty(
            global_state=False,
            sections={
                "upstream_requests",
                "steward_cases",
                "proof_review_requests",
                "proof_blockers",
            },
        )
        await self._persist()
        for consumer_id in consumer_ids:
            task = self.tasks.get(self.key(consumer_id, Stage.PROVE))
            if task is None:
                continue
            task.waiting_on = tuple(
                requirement
                for requirement in task.waiting_on
                if requirement.kind is not RequirementKind.UPSTREAM_REQUEST
            )
            await self.set_task(
                consumer_id,
                Stage.PROVE,
                TaskStatus.FAILED,
                "outstanding upstream requests were cleared by the operator",
            )

    async def enqueue_proof_review_request(
        self,
        feedback: dict[str, str],
        *,
        origin_run_id: str,
        kind: str = "proof_finding",
        stage: Stage | None = None,
        request_id: str | None = None,
        blocker_ids: Iterable[str] = (),
        source_digests: dict[str, str] | None = None,
    ) -> tuple[str, bool]:
        """Persist proof findings or diagnostics before scheduling their request service."""

        if kind == "upstream_request":
            raise ValueError("legacy upstream-request reviews are no longer supported")
        owned_source_digests = {
            chapter_id: digest
            for chapter_id, digest in (source_digests or {}).items()
            if chapter_id in feedback and isinstance(digest, str) and digest
        }
        for existing_id, value in self.proof_review_requests.items():
            if (
                value.get("origin_run_id") == origin_run_id
                and value.get("source_digests", {}) == owned_source_digests
            ):
                return existing_id, False
        request_id = request_id or uuid4().hex[:12]
        self.proof_review_requests[request_id] = {
            "feedback": dict(feedback),
            "origin_run_id": origin_run_id,
            "kind": kind,
            "stage": stage.value if stage is not None else Stage.REVIEW.value,
            "created_at": timestamp(),
            "blocker_ids": list(dict.fromkeys(blocker_ids)),
            "source_digests": owned_source_digests,
        }
        self._dirty_projection_work_units.update(feedback)
        self._mark_dirty(global_state=False, sections={"proof_review_requests"})
        await self._persist()
        return request_id, True

    async def discard_stale_proof_review_requests(
        self,
        source_digests: dict[str, str],
    ) -> set[str]:
        """Drop owner feedback captured from a different source-scope snapshot."""

        affected: set[str] = set()
        changed = False
        for request_id, value in tuple(self.proof_review_requests.items()):
            if not isinstance(value, dict):
                continue
            feedback = value.get("feedback")
            recorded = value.get("source_digests")
            if not isinstance(feedback, dict) or not isinstance(recorded, dict):
                continue
            for chapter_id in tuple(feedback):
                expected = recorded.get(chapter_id)
                current = source_digests.get(chapter_id)
                if isinstance(expected, str) and isinstance(current, str) and expected != current:
                    feedback.pop(chapter_id, None)
                    recorded.pop(chapter_id, None)
                    affected.add(chapter_id)
                    changed = True
            if not feedback:
                self.proof_review_requests.pop(request_id, None)
        if changed:
            self._dirty_projection_work_units.update(affected)
            self._mark_dirty(global_state=False, sections={"proof_review_requests"})
            await self._persist()
        return affected

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
            source_digests = value.get("source_digests")
            if isinstance(source_digests, dict):
                source_digests.pop(chapter_id, None)
            changed = True
            if not feedback:
                self.proof_review_requests.pop(request_id, None)
        if changed:
            self._dirty_projection_work_units.add(chapter_id)
            self._mark_dirty(global_state=False, sections={"proof_review_requests"})
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
        self._index_sorry_count(run)

    def _index_sorry_count(self, run: RunRecord) -> None:
        """Retain the newest source-wide placeholder count reported by an agent run."""

        if run.placeholders is None:
            return
        key = (run.started_at, run.id)
        latest = self._latest_sorry_counts.get(run.chapter_id)
        if latest is None or key >= latest[0]:
            self._latest_sorry_counts[run.chapter_id] = (key, run.placeholders)

    def chapter_runs(self, chapter_id: str) -> tuple[RunRecord, ...]:
        return tuple(self._chapter_runs.get(chapter_id, ()))

    def dashboard_chapter_runs(
        self, chapter_id: str, *, selected_run_id: str | None = None
    ) -> dict[str, Any]:
        """Return compact run tabs and recent activity for one chapter detail view."""

        runs = sorted(self.chapter_runs(chapter_id), key=lambda run: (run.started_at, run.id))
        return self._dashboard_runs(chapter_id, runs, selected_run_id=selected_run_id)

    def dashboard_package_runs(
        self, package_id: str, *, selected_run_id: str | None = None
    ) -> dict[str, Any]:
        """Return Steward and worker run history for one capability package."""

        runs = sorted(
            (
                run
                for run in self._runs_by_id.values()
                if package_id in run.request_ids
                and run.role in {"package_steward", "package_worker"}
            ),
            key=lambda run: (run.started_at, run.id),
        )
        return self._dashboard_runs(package_id, runs, selected_run_id=selected_run_id)

    def dashboard_steward_case_runs(
        self,
        case_id: str,
        *,
        selected_run_id: str | None = None,
    ) -> dict[str, Any]:
        case = self.steward_cases.get(case_id, {})
        request_ids = {str(value) for value in case.get("request_ids", ())}
        explicit_run_ids = {
            str(case.get("steward_run_id", "")),
            *(str(value) for value in case.get("repair_run_ids", ())),
        }
        runs = [
            run
            for run in self._runs_by_id.values()
            if run.id in explicit_run_ids
            or (
                run.role in {"upstream_steward", "upstream_repair", "upstream_implementation"}
                and request_ids.intersection(run.request_ids)
            )
        ]
        return self._dashboard_runs(case_id, runs, selected_run_id=selected_run_id)

    def _dashboard_runs(
        self,
        owner_id: str,
        runs: Iterable[RunRecord],
        *,
        selected_run_id: str | None,
    ) -> dict[str, Any]:
        selected_runs = tuple(runs)
        selected = next(
            (run for run in selected_runs if run.id == selected_run_id),
            selected_runs[-1] if selected_runs else None,
        )
        activity = self.activities.get(selected.id) if selected is not None else None
        return {
            "work_unit_id": owner_id,
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
                for run in selected_runs
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
            detail_limit=None,
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
            for key, cached in tuple(self._cost_cache.items()):
                invocation_only, chapter_id, roles = key
                if invocation_only and run.id in self._prior_run_ids:
                    continue
                if chapter_id is not None and chapter_id != run.chapter_id:
                    continue
                if roles is not None and run.role not in roles:
                    continue
                self._cost_cache[key] = cached + delta_cost

    def _invalidate_status_summaries(self) -> None:
        """Compatibility hook; indexes synchronize when dirty entities are marked."""

    def _rebuild_status_indexes(self) -> None:
        self._stage_count_cache = {
            stage.value: {status.value: 0 for status in TaskStatus}
            | {"queued": 0, "postprocess": 0}
            for stage in Stage
        }
        self._indexed_task_states.clear()
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
            )
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
        current = (task.stage, str(task.status), task.queued, str(task.phase))
        if previous == current:
            return
        if previous is not None:
            old_stage, old_status, old_queued, old_phase = previous
            old_bucket = "queued" if old_queued else old_status
            self._stage_count_cache[old_stage][old_bucket] -= 1
            if old_status == TaskStatus.RUNNING and old_phase == TaskPhase.POSTPROCESS:
                self._stage_count_cache[old_stage]["postprocess"] -= 1
        bucket = "queued" if task.queued else str(task.status)
        self._stage_count_cache[task.stage][bucket] += 1
        if task.status == TaskStatus.RUNNING and task.phase == TaskPhase.POSTPROCESS:
            self._stage_count_cache[task.stage]["postprocess"] += 1
        self._indexed_task_states[key] = current

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

    def _cost(
        self,
        *,
        invocation_only: bool,
        chapter_id: str | None = None,
        roles: frozenset[str] | None = None,
    ) -> CostEstimate:
        key = (invocation_only, chapter_id, roles)
        if key in self._cost_cache:
            return self._cost_cache[key]

        by_chapter = {chapter.id: CostEstimate() for chapter in self.config.work_units}
        for run in self._runs_by_id.values():
            if invocation_only and run.id in self._prior_run_ids:
                continue
            if roles is not None and run.role not in roles:
                continue
            cost = self.run_cost(run)
            by_chapter[run.chapter_id] += cost
        total = CostEstimate()
        for cost in by_chapter.values():
            total += cost

        self._cost_cache[(invocation_only, None, roles)] = total
        self._cost_cache.update(
            ((invocation_only, item, roles), cost) for item, cost in by_chapter.items()
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
        waiting_on: Iterable[Requirement] | None = None,
        force: bool = False,
    ) -> None:
        task = self.task(chapter_id, stage)
        if (
            not force
            and task.status == TaskStatus.INTERRUPTED
            and status
            not in (
                TaskStatus.RUNNING,
                TaskStatus.SUCCEEDED,
            )
        ):
            return
        if (
            not force
            and stage is Stage.FORMALIZE
            and status == TaskStatus.RUNNING
            and self.later_stage_started(chapter_id)
            and not task.recovering_failure
        ):
            raise RuntimeError(
                f"cannot start formalize for {chapter_id} after review or proof has begun"
            )
        changed_tasks = [task]
        recovered = task.recovering_failure and status == TaskStatus.SUCCEEDED
        task.status = status
        task.phase = TaskPhase.POSTPROCESS if status == TaskStatus.RUNNING else TaskPhase.IDLE
        task.queued = queued and status == TaskStatus.PENDING
        task.waiting_on = (
            tuple(dict.fromkeys(waiting_on or ()))
            if status == TaskStatus.PENDING and not task.queued
            else ()
        )
        if stage is Stage.PROVE:
            task.source_digest = source_digest if status == TaskStatus.SUCCEEDED else None
        task.detail = detail
        task.updated_at = timestamp()
        if status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
            task.recovering_failure = False
        if recovered or status == TaskStatus.SUCCEEDED:
            changed_tasks.extend(self._refresh_waiting_tasks())
        self._invalidate_status_summaries()
        self._mark_dirty(
            tasks=changed_tasks,
            global_state=False,
        )
        await self._persist()

    async def record_sorry_count(self, chapter_id: str, count: int) -> None:
        """Persist a source-wide placeholder count observed by the coordinator."""

        observed_at = timestamp()
        task = self.task(chapter_id, Stage.PROVE)
        task.sorry_count = count
        task.sorry_count_updated_at = observed_at
        self._latest_sorry_counts[chapter_id] = ((observed_at, "coordinator"), count)
        self._mark_dirty(task=task, global_state=False)
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
            if (
                stage is Stage.FORMALIZE
                and status == TaskStatus.RUNNING
                and self.later_stage_started(chapter_id)
                and not task.recovering_failure
            ):
                raise RuntimeError(
                    f"cannot start formalize for {chapter_id} after review or proof has begun"
                )
            task_status = status
            task_detail = detail
            recovered = recovered or (
                task.recovering_failure and task_status == TaskStatus.SUCCEEDED
            )
            task.status = task_status
            task.phase = (
                TaskPhase.POSTPROCESS if task_status == TaskStatus.RUNNING else TaskPhase.IDLE
            )
            task.queued = False
            task.waiting_on = ()
            if stage is Stage.PROVE and task_status != TaskStatus.SUCCEEDED:
                task.source_digest = None
            task.detail = task_detail
            task.updated_at = updated_at
            if task_status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
                task.recovering_failure = False
            changed_tasks.append(task)
            changed = True
        if recovered or status == TaskStatus.SUCCEEDED:
            changed_tasks.extend(self._refresh_waiting_tasks())
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(
                tasks=changed_tasks,
                global_state=False,
            )
            await self._persist()

    async def set_task_waiting(
        self,
        chapter_id: str,
        stage: Stage,
        requirements: Iterable[Requirement],
        detail: str,
    ) -> None:
        """Persist why a pending task cannot run without recording an execution failure."""

        waiting = tuple(dict.fromkeys(requirements))
        if not waiting:
            raise ValueError("a waiting task requires at least one unmet requirement")
        if stage is Stage.FORMALIZE:
            invalid = tuple(
                requirement.owner_task_key
                for requirement in waiting
                if requirement.owner_task_key is not None
                and requirement.owner_task_key.rpartition(":")[2]
                in {Stage.REVIEW.value, Stage.PROVE.value}
            )
            if invalid:
                raise ValueError(
                    "formalize tasks cannot wait on later-stage owners: " + ", ".join(invalid)
                )
        await self.set_task(
            chapter_id,
            stage,
            TaskStatus.PENDING,
            detail,
            waiting_on=waiting,
        )

    async def set_tasks_waiting(
        self,
        chapter_ids: Iterable[str],
        stage: Stage,
        requirements: dict[str, Iterable[Requirement]],
        detail: str,
    ) -> None:
        async with self.batch():
            for chapter_id in chapter_ids:
                waiting = tuple(requirements.get(chapter_id, ()))
                if waiting:
                    await self.set_task_waiting(chapter_id, stage, waiting, detail)

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
        """Clear legacy blocked states and explicit wait metadata."""
        changed: list[str] = []
        proof_chapters: set[str] = set()
        for key, task in self.tasks.items():
            if task.status != TaskStatus.BLOCKED and not task.waiting_on:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            task.waiting_on = ()
            if task.stage == Stage.PROVE:
                task.source_digest = None
                proof_chapters.add(task.chapter_id)
            task.detail = "manually unblocked"
            task.updated_at = timestamp()
            changed.append(key)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=(self.tasks[key] for key in changed), global_state=False)
            await self._persist()
        return changed

    async def retry_failed(self) -> list[str]:
        """Reset every failed task to pending without discarding attempt history."""

        changed: list[str] = []
        proof_chapters: set[str] = set()
        for key, task in self.tasks.items():
            if task.status != TaskStatus.FAILED:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            task.waiting_on = ()
            if task.stage == Stage.PROVE:
                task.source_digest = None
                proof_chapters.add(task.chapter_id)
            task.detail = "manually retried"
            task.recovering_failure = True
            task.updated_at = timestamp()
            changed.append(key)
        await self.reopen_proof_blockers(proof_chapters)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(
                tasks=(self.tasks[key] for key in changed),
                global_state=False,
            )
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

    def _migrate_legacy_task_wait(self, task: TaskRecord) -> None:
        if Stage(task.stage) is Stage.FORMALIZE:
            task.waiting_on = tuple(
                Requirement(
                    requirement.kind,
                    owner_task_key=(
                        requirement.owner_task_key.rpartition(":")[0] + f":{Stage.FORMALIZE.value}"
                        if requirement.kind is RequirementKind.COORDINATOR_OWNER
                        and requirement.owner_task_key is not None
                        and requirement.owner_task_key.rpartition(":")[2]
                        in {Stage.REVIEW.value, Stage.PROVE.value}
                        else requirement.owner_task_key
                    ),
                    request_id=requirement.request_id,
                    detail=requirement.detail,
                )
                for requirement in task.waiting_on
            )
        if not any(
            requirement.kind is RequirementKind.LEGACY_BLOCK for requirement in task.waiting_on
        ):
            return
        stage = Stage(task.stage)
        requirements: tuple[Requirement, ...] = ()
        if stage is Stage.FORMALIZE and task.detail == (
            "blocked by a failed source dependency formalization"
        ):
            requirements = tuple(
                self._task_key_requirement(
                    RequirementKind.SOURCE_DEPENDENCY,
                    dependency,
                    Stage.FORMALIZE,
                    f"waiting for source dependency {dependency}",
                )
                for dependency in sorted(self._source_dependencies(task.chapter_id))
            )
        elif stage is Stage.REVIEW and task.detail == (
            "blocked by a failed prerequisite review; unrelated branches completed"
        ):
            requirements = tuple(
                self._task_key_requirement(
                    RequirementKind.STAGE_DEPENDENCY,
                    dependency,
                    Stage.REVIEW,
                    f"waiting for dependency review {dependency}",
                )
                for dependency in sorted(self._source_dependencies(task.chapter_id))
            )
        elif (
            stage is Stage.REVIEW
            and task.detail
            in {
                "formalization did not complete",
                "formalization failed; quarantined from review",
            }
        ) or (
            stage is Stage.PROVE
            and task.detail
            in {
                "blocked because formalization did not complete",
                "formalization failed; quarantined from proof",
            }
        ):
            requirements = (
                self._task_key_requirement(
                    RequirementKind.STAGE_DEPENDENCY,
                    task.chapter_id,
                    Stage.FORMALIZE,
                    "waiting for clean formalization",
                ),
            )
        elif stage is Stage.PROVE and task.detail == (
            "blocked because statement review did not complete"
        ):
            requirements = (
                self._task_key_requirement(
                    RequirementKind.STAGE_DEPENDENCY,
                    task.chapter_id,
                    Stage.REVIEW,
                    "waiting for successful review",
                ),
            )
        if requirements:
            task.waiting_on = requirements

    def _task_key_requirement(
        self,
        kind: RequirementKind,
        chapter_id: str,
        stage: Stage,
        detail: str,
    ) -> Requirement:
        return Requirement(kind, owner_task_key=self.key(chapter_id, stage), detail=detail)

    def _blocking_proof_review_requests(self) -> dict[str, tuple[str, ...]]:
        """Index semantic proof-review requests without auxiliary warning cleanup."""

        by_chapter: dict[str, list[str]] = {}
        for request_id, request in sorted(self.proof_review_requests.items()):
            if not isinstance(request, dict) or request.get("kind") == BUILD_WARNING_REVIEW_KIND:
                continue
            feedback = request.get("feedback")
            if not isinstance(feedback, dict):
                continue
            for chapter_id in feedback:
                by_chapter.setdefault(chapter_id, []).append(request_id)
        return {chapter_id: tuple(request_ids) for chapter_id, request_ids in by_chapter.items()}

    def _task_requirements(
        self,
        task: TaskRecord,
        proof_review_requests: dict[str, tuple[str, ...]],
    ) -> tuple[Requirement, ...]:
        requirements = list(task.waiting_on)
        stage = Stage(task.stage)
        if stage is Stage.FORMALIZE:
            requirements.append(
                self._task_key_requirement(
                    RequirementKind.SOURCE_DISCOVERY,
                    task.chapter_id,
                    Stage.DISCOVER,
                    "waiting for source discovery",
                )
            )
            requirements.extend(
                self._task_key_requirement(
                    RequirementKind.SOURCE_DEPENDENCY,
                    dependency,
                    Stage.FORMALIZE,
                    f"waiting for source dependency {dependency}",
                )
                for dependency in sorted(self._source_dependencies(task.chapter_id))
            )
        elif stage is Stage.REVIEW:
            requirements.append(
                self._task_key_requirement(
                    RequirementKind.STAGE_DEPENDENCY,
                    task.chapter_id,
                    Stage.FORMALIZE,
                    "waiting for clean formalization",
                )
            )
            # Initial reviews are dependency ordered. Targeted later reviews are not.
            if task.rounds == 0:
                requirements.extend(
                    self._task_key_requirement(
                        RequirementKind.STAGE_DEPENDENCY,
                        dependency,
                        Stage.REVIEW,
                        f"waiting for dependency review {dependency}",
                    )
                    for dependency in sorted(self._source_dependencies(task.chapter_id))
                )
        elif stage is Stage.PROVE:
            requirements.extend(
                (
                    self._task_key_requirement(
                        RequirementKind.STAGE_DEPENDENCY,
                        task.chapter_id,
                        Stage.FORMALIZE,
                        "waiting for clean formalization",
                    ),
                    self._task_key_requirement(
                        RequirementKind.STAGE_DEPENDENCY,
                        task.chapter_id,
                        Stage.REVIEW,
                        "waiting for successful review",
                    ),
                )
            )
            requirements.extend(
                Requirement(
                    RequirementKind.PROOF_REVIEW_REQUEST,
                    owner_task_key=self.key(task.chapter_id, Stage.REVIEW),
                    request_id=request_id,
                    detail="waiting for proof-review request",
                )
                for request_id in proof_review_requests.get(task.chapter_id, ())
            )
        return tuple(
            dict.fromkeys(
                requirement
                for requirement in requirements
                if not self.requirement_satisfied(requirement)
            )
        )

    def task_requirements(self, task: TaskRecord) -> tuple[Requirement, ...]:
        """Return current requirements without mutating descendant task rows."""

        return self._task_requirements(task, self._blocking_proof_review_requests())

    def _readiness_requirements_subset(
        self,
        task_keys: Iterable[str],
    ) -> dict[str, tuple[Requirement, ...]]:
        """Project current readiness blockers with one request-ledger index pass."""

        proof_review_requests = self._blocking_proof_review_requests()
        return {
            key: self._task_requirements(task, proof_review_requests)
            for key in dict.fromkeys(task_keys)
            if (task := self.tasks.get(key)) is not None
        }

    def requirement_satisfied(self, requirement: Requirement) -> bool:
        if (
            requirement.request_id is not None
            and requirement.kind is RequirementKind.PROOF_REVIEW_REQUEST
        ):
            request = self.proof_review_requests.get(requirement.request_id)
            if not isinstance(request, dict):
                return True
            feedback = request.get("feedback")
            if requirement.owner_task_key is None or not isinstance(feedback, dict):
                return False
            chapter_id = requirement.owner_task_key.rpartition(":")[0]
            return chapter_id not in feedback
        if requirement.owner_task_key is not None:
            owner = self.tasks.get(requirement.owner_task_key)
            return owner is not None and owner.status == TaskStatus.SUCCEEDED
        return False

    def readiness(self, task: TaskRecord) -> Readiness:
        waiting = self.task_requirements(task)
        return Readiness(ready=task.status == TaskStatus.PENDING and not waiting, waiting=waiting)

    def failure_roots(self, task: TaskRecord) -> tuple[str, ...]:
        """Find direct execution failures behind a derived pending wait."""

        roots: set[str] = set()
        visited: set[str] = set()

        def visit(candidate: TaskRecord) -> None:
            key = self.key(candidate.chapter_id, Stage(candidate.stage))
            if key in visited:
                return
            visited.add(key)
            for requirement in self.task_requirements(candidate):
                if requirement.owner_task_key is None:
                    continue
                owner = self.tasks.get(requirement.owner_task_key)
                if owner is None:
                    continue
                if owner.status == TaskStatus.FAILED:
                    roots.add(requirement.owner_task_key)
                elif owner.status == TaskStatus.PENDING:
                    visit(owner)

        visit(task)
        return tuple(sorted(roots))

    def _failure_roots_index(self) -> dict[str, tuple[str, ...]]:
        """Resolve failure roots for every task in one graph traversal.

        Snapshot persistence projects every task row together. Walking from each
        row independently repeats long prerequisite prefixes and becomes
        quadratic on corpus-sized dependency chains. Propagating roots through
        the reverse pending-task graph visits each requirement once and shares
        the result among every projected row.
        """

        return self._failure_roots_subset(self.tasks)

    def _failure_owner_keys(self, task: TaskRecord) -> set[str]:
        """Return prerequisite task keys relevant to derived failure routing."""

        owner_keys = {
            requirement.owner_task_key
            for requirement in task.waiting_on
            if requirement.owner_task_key is not None
        }
        stage = Stage(task.stage)
        if stage is Stage.FORMALIZE:
            owner_keys.add(self.key(task.chapter_id, Stage.DISCOVER))
            owner_keys.update(
                self.key(dependency, Stage.FORMALIZE)
                for dependency in self._source_dependencies(task.chapter_id)
            )
        elif stage is Stage.REVIEW:
            owner_keys.add(self.key(task.chapter_id, Stage.FORMALIZE))
            if task.rounds == 0:
                owner_keys.update(
                    self.key(dependency, Stage.REVIEW)
                    for dependency in self._source_dependencies(task.chapter_id)
                )
        elif stage is Stage.PROVE:
            owner_keys.update(
                (
                    self.key(task.chapter_id, Stage.FORMALIZE),
                    self.key(task.chapter_id, Stage.REVIEW),
                )
            )
        return owner_keys

    def _failure_dependents(self, owner_keys: Iterable[str]) -> list[TaskRecord]:
        """Return pending tasks whose derived blocked state can change with an owner."""

        dependents: dict[str, list[str]] = {}
        for key, task in self.tasks.items():
            if task.status != TaskStatus.PENDING:
                continue
            for owner_key in self._failure_owner_keys(task):
                dependents.setdefault(owner_key, []).append(key)

        changed: list[TaskRecord] = []
        pending = deque(dict.fromkeys(owner_keys))
        visited = set(pending)
        while pending:
            owner_key = pending.popleft()
            for dependent_key in dependents.get(owner_key, ()):
                if dependent_key in visited:
                    continue
                visited.add(dependent_key)
                changed.append(self.tasks[dependent_key])
                pending.append(dependent_key)
        return changed

    def _failure_roots_subset(self, task_keys: Iterable[str]) -> dict[str, tuple[str, ...]]:
        """Resolve roots in only the prerequisite closure needed by a projection."""

        requested = tuple(dict.fromkeys(task_keys))
        roots: dict[str, set[str]] = {}
        dependents: dict[str, list[str]] = {}
        unresolved = list(requested)
        while unresolved:
            key = unresolved.pop()
            if key in roots:
                continue
            roots[key] = set()
            task = self.tasks.get(key)
            if task is None:
                continue
            for owner_key in self._failure_owner_keys(task):
                owner = self.tasks.get(owner_key)
                if owner is None:
                    continue
                if owner.status == TaskStatus.FAILED:
                    roots[key].add(owner_key)
                elif owner.status == TaskStatus.PENDING:
                    dependents.setdefault(owner_key, []).append(key)
                    if owner_key not in roots:
                        unresolved.append(owner_key)

        pending = deque(key for key, values in roots.items() if values)
        queued = set(pending)
        while pending:
            owner_key = pending.popleft()
            queued.discard(owner_key)
            owner_roots = roots[owner_key]
            for dependent_key in dependents.get(owner_key, ()):
                dependent_roots = roots[dependent_key]
                previous_size = len(dependent_roots)
                dependent_roots.update(owner_roots)
                if len(dependent_roots) != previous_size and dependent_key not in queued:
                    pending.append(dependent_key)
                    queued.add(dependent_key)
        return {key: tuple(sorted(roots.get(key, ()))) for key in requested}

    def failed_requirements(
        self,
        task: TaskRecord,
        *,
        roots: Iterable[str] | None = None,
    ) -> tuple[Requirement, ...]:
        resolved_roots = self.failure_roots(task) if roots is None else roots
        return tuple(
            Requirement(
                RequirementKind.STAGE_DEPENDENCY,
                owner_task_key=key,
                detail="waiting on a failed prerequisite",
            )
            for key in sorted(set(resolved_roots))
        )

    def _refresh_waiting_tasks(self) -> list[TaskRecord]:
        """Drop satisfied explicit waits to a fixed point, independent of row order."""

        changed: list[TaskRecord] = []
        changed_ids: set[int] = set()
        while True:
            progress = False
            for task in self.tasks.values():
                if task.status != TaskStatus.PENDING or not task.waiting_on:
                    continue
                waiting = tuple(
                    requirement
                    for requirement in task.waiting_on
                    if not self.requirement_satisfied(requirement)
                )
                if waiting == task.waiting_on:
                    continue
                task.waiting_on = waiting
                task.detail = (
                    "waiting for scheduling requirements"
                    if waiting
                    else "requirements satisfied; awaiting scheduler"
                )
                task.updated_at = timestamp()
                if id(task) not in changed_ids:
                    changed.append(task)
                    changed_ids.add(id(task))
                progress = True
            if not progress:
                return changed

    @staticmethod
    def _run_had_infrastructure_failure(run: RunRecord) -> bool:
        """Recognize durable and legacy agent/tool startup failures."""

        if run.failure_kind == "infrastructure":
            return True
        markers = (
            "required mcp servers failed to initialize",
            "handshaking with mcp server failed",
            "failed to initialize session",
            "error creating thread",
            "connection closed: initialize response",
            "lean beam startup retries exhausted",
        )
        evidence = run.error.casefold()
        if any(marker in evidence for marker in markers):
            return True
        if not run.log_path:
            return False
        try:
            log = Path(run.log_path).read_bytes()[-256_000:].decode(errors="replace").casefold()
        except OSError:
            return False
        return any(marker in log for marker in markers)

    async def requeue_interrupted(self, *, resume_agents: bool) -> list[str]:
        """Requeue interrupted and infrastructure-failed tasks with their session history."""

        changed: list[str] = []
        for key, task in self.tasks.items():
            latest = next((run for run in reversed(task.runs) if not run.auxiliary), None)
            infrastructure_failed = (
                task.status == TaskStatus.FAILED
                and latest is not None
                and self._run_had_infrastructure_failure(latest)
            )
            if task.status != TaskStatus.INTERRUPTED and not infrastructure_failed:
                continue
            task.status = TaskStatus.PENDING
            task.phase = TaskPhase.IDLE
            task.queued = False
            task.waiting_on = ()
            if task.stage == Stage.PROVE:
                task.source_digest = None
            task.detail = (
                "infrastructure-failed agent queued for a fresh retry"
                if infrastructure_failed
                else "interrupted agent queued for session resume"
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
        task = self.task(chapter_id, stage)
        if (
            stage is Stage.FORMALIZE
            and self.later_stage_started(chapter_id)
            and not task.recovering_failure
        ):
            raise RuntimeError(
                f"cannot start formalize for {chapter_id} after review or proof has begun"
            )
        chapter = self.config.work_unit(chapter_id)
        task.status = TaskStatus.RUNNING
        task.phase = TaskPhase.AGENT
        task.queued = False
        task.waiting_on = ()
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
        self._mark_dirty(
            task=task,
            run=run,
            global_state=False,
        )
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

    def interrupted_auxiliary_run(
        self,
        *,
        role: str,
        request_ids: Iterable[str],
    ) -> RunRecord | None:
        """Return the newest exact auxiliary assignment that can be resumed in place."""

        selected_request_ids = list(dict.fromkeys(request_ids))
        candidates: list[RunRecord] = [
            run
            for run in self._runs_by_id.values()
            if run.auxiliary
            and run.role == role
            and run.request_ids == selected_request_ids
            and run.status == TaskStatus.INTERRUPTED
            and run.thread_id is not None
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda run: (run.started_at, run.id))

    async def resume_auxiliary_run(self, run: RunRecord) -> RunRecord:
        """Reactivate an interrupted auxiliary run without creating a second TUI run."""

        if self._runs_by_id.get(run.id) is not run or not run.auxiliary:
            raise ValueError(f"unknown auxiliary run: {run.id}")
        if run.status != TaskStatus.INTERRUPTED or not run.thread_id:
            raise ValueError(f"auxiliary run is not resumable: {run.id}")
        run.status = TaskStatus.RUNNING
        run.finished_at = None
        run.pid = None
        run.exit_code = None
        self._invalidate_status_summaries()
        self._mark_dirty(run=run, global_state=False)
        await self._persist()
        return run

    async def recover_interrupted_steward_cases(self) -> list[str]:
        """Make cases orphaned in ``repairing`` schedulable after a restart."""

        recovered: list[str] = []
        for case_id, case in self.steward_cases.items():
            if case.get("status") != "repairing":
                continue
            case["status"] = "ready"
            case["updated_at"] = timestamp()
            recovered.append(case_id)
        if recovered:
            self._mark_dirty(global_state=False, sections={"steward_cases"})
            await self._persist()
        return recovered

    async def recover_interrupted_coordination_cases(self) -> list[str]:
        """Normalize the small incident lifecycle and requeue interrupted diagnoses."""

        recovered: list[str] = []
        for case_id, case in self.coordination_cases.items():
            changed = False
            status = str(case.get("status", "open"))
            interrupted = status in {"investigating", "deciding", "running"}
            normalized_status = {
                "investigating": "open",
                "deciding": "open",
                "running": "open",
                "resolved": "closed",
                "failed": "parked",
                "awaiting_source_approval": "parked",
            }.get(status, status)
            if normalized_status != status:
                case["status"] = normalized_status
                if status == "awaiting_source_approval":
                    case["operator_action_required"] = True
                changed = True
            legacy_planner_attempts = int(case.get("planner_attempts", 0))
            if "attempts" not in case:
                had_legacy_attempts = any(
                    key in case for key in ("investigation_attempts", "planner_attempts")
                )
                legacy_attempts = int(case.pop("investigation_attempts", 0)) + int(
                    case.pop("planner_attempts", 0)
                )
                if legacy_attempts:
                    case["attempts"] = legacy_attempts
                changed = changed or had_legacy_attempts
            else:
                for key in ("investigation_attempts", "planner_attempts"):
                    if key in case:
                        case.pop(key)
                        changed = True
            if "strong_used" not in case and legacy_planner_attempts:
                case["strong_used"] = True
                changed = True
            if interrupted and case.get("strong_used"):
                case["strong_used"] = False
                case["force_strong"] = True
                changed = True
            if case.pop("force_planner", False):
                case["force_strong"] = True
                changed = True
            for key in ("scope_expansions", "action_failures"):
                if key in case:
                    case.pop(key)
                    changed = True
            if changed:
                case["updated_at"] = timestamp()
                recovered.append(case_id)
        if recovered:
            self._mark_dirty(global_state=False, sections={"coordination_cases"})
            await self._persist()
        return recovered

    async def update_run(self, run: RunRecord, *, deferred: bool = False, **changes: Any) -> None:
        old_usage = run.usage
        old_model = run.model
        for name, value in changes.items():
            setattr(run, name, value)
        if "placeholders" in changes:
            self._index_sorry_count(run)
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
        self._mark_thread_usage_dirty(thread_id)
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
        self._index_sorry_count(run)
        issue_ids = self._record_source_issues(run)
        if isinstance(run.report, dict) and "source_issues" in run.report:
            report = {key: value for key, value in run.report.items() if key != "source_issues"}
            if issue_ids:
                report["source_issue_ids"] = issue_ids
            run.report = report
        self._invalidate_aggregates()
        self._invalidate_status_summaries()
        changed_task = self.task(run.chapter_id, Stage(run.stage)) if run.auxiliary else None
        if (
            status == TaskStatus.INTERRUPTED
            and not run.auxiliary
            and (task := self.task(run.chapter_id, Stage(run.stage))).status == TaskStatus.RUNNING
            and task.runs
            and task.runs[-1].id == run.id
        ):
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

    async def record_interface_invalidation(
        self,
        *,
        work_unit_id: str,
        source_file: str,
        old_digest: str | None,
        new_digest: str | None,
        invalidated_work_unit_ids: Iterable[str],
    ) -> None:
        """Persist analysis-only provenance without changing scheduler state."""

        invalidated = tuple(sorted(set(invalidated_work_unit_ids)))
        await asyncio.to_thread(
            self._database.record_interface_invalidation,
            occurred_at=timestamp(),
            work_unit_id=work_unit_id,
            source_file=source_file,
            old_digest=old_digest,
            new_digest=new_digest,
            invalidated_work_unit_ids=invalidated,
        )

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
