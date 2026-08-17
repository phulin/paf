from __future__ import annotations

import asyncio
import hashlib
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


class UpstreamRequestStatus(StrEnum):
    """Completed durable facts for a missing interface in an earlier chapter."""

    REQUESTED = "requested"
    ANSWERED = "answered"
    CLOSED = "closed"
    ESCALATED = "escalated"


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
    log_path: str | None = None
    role: str = ""
    auxiliary: bool = False
    request_ids: list[str] = field(default_factory=list)
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
            fields = {
                name: item
                for name, item in value.items()
                if name in RunRecord.__dataclass_fields__
                and name not in {"usage", "report", "validation", "isolation"}
            }
            run = RunRecord(**fields, usage=usage)
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
        self._normalize_upstream_request_state()
        self._invalidate_aggregates()
        self._rebuild_status_indexes()
        self._checkpoint_dirty = True
        self._config_fingerprint = hashlib.sha256(
            json.dumpb(
                {
                    "documents": self._document_dicts(),
                    "work_units": self._work_unit_dicts(),
                },
                sort_keys=True,
            )
        ).hexdigest()
        persisted_fingerprint = await asyncio.to_thread(self._database.config_fingerprint)
        self._static_dirty = persisted_fingerprint != self._config_fingerprint
        self._dirty_task_keys.update(self.tasks)
        self._issues_dirty = True
        self._dirty_run_ids.update(run.id for run in recovered_runs)
        await self.flush()

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
            "log_path": run.log_path,
            "role": run.role,
            "auxiliary": run.auxiliary,
            "request_ids": list(run.request_ids),
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

    @staticmethod
    def _task_dict(task: TaskRecord) -> dict[str, Any]:
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
            "source_digest": task.source_digest,
            "rounds": task.rounds,
            "updated_at": task.updated_at,
            "source": task.source,
            "source_start_line": task.source_start_line,
            "source_end_line": task.source_end_line,
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
            "version": 15,
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
            "upstream_request_batches": self.upstream_request_batches(),
            "isolation": self.isolation,
            "coordinator_build": self._build_dict(self.coordinator_build),
        }

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
        return self._global_snapshot() | {
            "documents": [dict(value) for value in self._document_dicts()],
            "work_units": [dict(value) for value in self._work_unit_dicts()],
            "tasks": {
                key: self._task_dict(task)
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
            key for key, task in self.tasks.items() if task.chapter_id in change.work_units
        )
        tasks = {
            key: self._hot_task_dict(self.tasks[key])
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
        globals_changed = bool(change.globals.difference({"activity"}))
        globals_ = self._global_snapshot() | {"revision": self.revision} if globals_changed else {}
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
            tasks[key] = self._task_dict(task) | {"runs": runs}
        snapshot["source_issues"] = [
            self._issue_dict(issue) for _, issue in sorted(self.source_issues.items())
        ]
        snapshot["tasks"] = tasks
        return snapshot

    def _hot_task_dict(self, task: TaskRecord) -> dict[str, Any]:
        return self._task_dict(task) | {
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

    async def _persist(self) -> None:
        if self._batch_depth:
            return
        await asyncio.sleep(0)
        await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            if not (
                self._checkpoint_dirty
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
            task_keys = self._dirty_task_keys
            dirty_runs = self._dirty_run_ids
            issues_dirty = self._issues_dirty
            static_dirty = self._static_dirty
            self._dirty_task_keys = set()
            self._dirty_run_ids = set()
            self._checkpoint_dirty = False
            self._issues_dirty = False
            self._static_dirty = False
            task_payloads = {
                key: json.dumpb(self._hot_task_dict(self.tasks[key]))
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
            if issues_dirty:
                changes.add(("source_issues", "*"))
            if static_dirty:
                changes.add(("resync", "*"))
            write = DatabaseWrite(
                updated_at=self.updated_at,
                globals={"state": json.dumpb(self._global_snapshot())} if globals_dirty else {},
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
                    globals=frozenset({"state"} if globals_dirty else ()),
                    stages=frozenset(changed_stages),
                    full_resync=static_dirty,
                )
            )

    async def save(self) -> None:
        self._mark_dirty()
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
        fingerprint = hashlib.sha256("\0".join(fingerprint_fields).encode()).hexdigest()[:16]
        for request_id, existing in self.upstream_requests.items():
            if existing.get("fingerprint") != fingerprint:
                continue
            if existing.get("status") == UpstreamRequestStatus.CLOSED.value:
                continue
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
            issue_id = hashlib.sha256(fingerprint).hexdigest()[:16]
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
        task.status = status
        task.phase = TaskPhase.POSTPROCESS if status == TaskStatus.RUNNING else TaskPhase.IDLE
        task.queued = queued and status == TaskStatus.PENDING
        if stage is Stage.PROVE:
            task.source_digest = source_digest if status == TaskStatus.SUCCEEDED else None
        task.detail = detail
        task.updated_at = timestamp()
        self._invalidate_status_summaries()
        self._mark_dirty(tasks=changed_tasks)
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
            task.status = task_status
            task.phase = (
                TaskPhase.POSTPROCESS if task_status == TaskStatus.RUNNING else TaskPhase.IDLE
            )
            task.queued = False
            if stage is Stage.PROVE and task_status != TaskStatus.SUCCEEDED:
                task.source_digest = None
            task.detail = task_detail
            task.updated_at = updated_at
            changed_tasks.append(task)
            changed = True
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty(tasks=changed_tasks)
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
        self._mark_dirty(task=task)
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
            self._mark_dirty(tasks=(self.tasks[key] for key in changed))
            await self._persist()
        return changed

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
            self._mark_dirty(tasks=(self.tasks[key] for key in changed))
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
        self._mark_dirty(task=task, run=run)
        await self._persist()
        return run

    async def start_auxiliary_run(
        self,
        chapter_id: str,
        stage: Stage,
        *,
        role: str,
        request_ids: Iterable[str],
    ) -> RunRecord:
        """Record a temporary agent without mutating the owner's chapter-stage state."""

        task = self.task(chapter_id, stage)
        role_round = 1 + sum((run.role or run.stage) == role for run in task.runs)
        run = RunRecord(
            id=uuid4().hex[:12],
            chapter_id=chapter_id,
            stage=stage.value,
            round=role_round,
            model=self.config.model_for(stage),
            role=role,
            auxiliary=True,
            request_ids=list(dict.fromkeys(request_ids)),
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
        self._mark_dirty(task=task, run=run)
        await self._persist()
        return run

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
            global_state=bool({"usage", "model", "status"} & changes.keys()),
        )
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
        self._mark_dirty(task=changed_task, run=run, issues=bool(issue_ids))
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
        self._mark_dirty()
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
        self._mark_dirty()
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
        self._mark_dirty()

    async def finish_coordinator_build(self) -> None:
        self.coordinator_build.active = False
        self.coordinator_build.current_chapter_id = ""
        self.coordinator_build.updated_at = timestamp()
        self._mark_dirty()
        await self._persist()
