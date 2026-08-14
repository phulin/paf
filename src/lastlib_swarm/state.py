from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from lastlib_swarm.activity import ActivityStore, shorten_book_paths
from lastlib_swarm.diagnostics import lean_diagnostic_counts
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.pricing import LEGACY_MODEL, CostEstimate, estimate_cost
from lastlib_swarm.state_db import DATABASE_NAME, StateDatabase

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"


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
    exit_code: int | None = None
    changed: bool | None = None
    placeholders: int | None = None
    report: dict[str, Any] | None = None
    validation: dict[str, Any] | None = None
    isolation: dict[str, Any] | None = None
    usage: TokenUsage = field(default_factory=TokenUsage)
    log_path: str | None = None


@dataclass
class TaskRecord:
    chapter_id: str
    book_id: str
    chapter_number: int
    chapter_title: str
    stage: str
    status: str = TaskStatus.PENDING
    detail: str = ""
    # Proof completion is tied to the exact validated chapter sources. This is
    # populated only on the prove task.
    source_digest: str | None = None
    rounds: int = 0
    updated_at: str = field(default_factory=timestamp)
    runs: list[RunRecord] = field(default_factory=list)


@dataclass
class CoordinatorBuildRecord:
    active: bool = False
    mode: str = ""
    stage: str = ""
    iteration: int = 0
    maximum_iterations: int = 0
    completed: int = 0
    total: int = 0
    current_chapter_id: str = ""
    error_count: int = 0
    warning_count: int = 0
    output_tail: list[str] = field(default_factory=list)
    updated_at: str = field(default_factory=timestamp)


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
    status: str = "open"
    first_seen_at: str = field(default_factory=timestamp)
    last_seen_at: str = field(default_factory=timestamp)
    sightings: int = 1
    stages: list[str] = field(default_factory=list)
    run_ids: list[str] = field(default_factory=list)


class StateStore:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.path = config.settings.state_dir / "state.json"
        self.database_path = config.settings.state_dir / DATABASE_NAME
        self.source_issues_path = config.settings.state_dir / "source-issues.json"
        self.logs_dir = config.settings.state_dir / "logs"
        self.activities = ActivityStore(self.logs_dir)
        self._database = StateDatabase(config.settings.state_dir)
        self._flush_lock = asyncio.Lock()
        self._batch_depth = 0
        self._checkpoint_dirty = False
        self._issues_dirty = False
        self._dirty_run_ids: set[str] = set()
        self._prior_run_ids: set[str] = set()
        self._runs_by_id: dict[str, RunRecord] = {}
        self._payload_loaded_run_ids: set[str] = set()
        self._chapter_runs: dict[str, list[RunRecord]] = {}
        self._usage_cache: dict[tuple[bool, str | None], TokenUsage] = {}
        self._cost_cache: dict[tuple[bool, str | None], CostEstimate] = {}
        self._agent_summary_cache: dict[str, Any] | None = None
        self._stage_count_cache: dict[str, dict[str, int]] = {}
        self.tasks: dict[str, TaskRecord] = {}
        self.source_issues: dict[str, SourceIssueRecord] = {}
        self.scheduling: dict[str, Any] = {}
        self.fixup_graph: dict[str, Any] = {}
        self.fixup_requests: dict[str, dict[str, Any]] = {}
        self.proof_review_requests: dict[str, dict[str, Any]] = {}
        self.isolation: dict[str, Any] = {}
        self.coordinator_build = CoordinatorBuildRecord()
        self.created_at = timestamp()
        self.updated_at = self.created_at

    @staticmethod
    def key(chapter_id: str, stage: Stage) -> str:
        return f"{chapter_id}:{stage.value}"

    async def load_or_create(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._database.initialize)
        raw, persisted_runs, persisted_source_issues = await asyncio.to_thread(self._database.load)
        if raw is not None:
            self.created_at = str(raw.get("created_at", timestamp()))
            self.updated_at = str(raw.get("updated_at", self.created_at))
            if not self.scheduling and isinstance(raw.get("scheduling"), dict):
                self.scheduling = raw["scheduling"]
            if not self.fixup_graph and isinstance(raw.get("fixup_graph"), dict):
                self.fixup_graph = raw["fixup_graph"]
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
            if not self.isolation and isinstance(raw.get("isolation"), dict):
                self.isolation = raw["isolation"]
            raw_build = raw.get("coordinator_build")
            if isinstance(raw_build, dict):
                self.coordinator_build = CoordinatorBuildRecord(
                    **{
                        name: value
                        for name, value in raw_build.items()
                        if name in CoordinatorBuildRecord.__dataclass_fields__
                    }
                )
            persisted_tasks = raw.get("tasks", {})
            if isinstance(persisted_tasks, dict):
                for key, value in persisted_tasks.items():
                    if not isinstance(key, str) or not isinstance(value, dict):
                        continue
                    if key.endswith(":repair"):
                        key = f"{key[: -len(':repair')]}:fixup"
                        if key in persisted_tasks:
                            continue
                    task_value = {
                        name: item
                        for name, item in value.items()
                        if name in TaskRecord.__dataclass_fields__ and name != "runs"
                    }
                    if task_value.get("stage") == "repair":
                        task_value["stage"] = "fixup"
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
                issue = SourceIssueRecord(**value)
                self.source_issues[issue.id] = issue
        configured = {chapter.id for chapter in self.config.chapters}
        self.tasks = {
            key: task for key, task in self.tasks.items() if task.chapter_id in configured
        }
        for chapter in self.config.chapters:
            for stage in Stage:
                key = self.key(chapter.id, stage)
                self.tasks.setdefault(key, self._new_task(chapter, stage))
        for value in persisted_runs:
            if not isinstance(value, dict):
                continue
            if value.get("stage") == "repair":
                value["stage"] = "fixup"
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
                    run.status = TaskStatus.FAILED
                    run.finished_at = timestamp()
                    recovered_runs.append(run)
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.PENDING
                task.detail = "recovered after interrupted orchestrator"
        if self.coordinator_build.active:
            self.coordinator_build.active = False
            self.coordinator_build.current_chapter_id = ""
            self.coordinator_build.updated_at = timestamp()
        self._invalidate_aggregates()
        self._invalidate_status_summaries()
        self._checkpoint_dirty = True
        self._issues_dirty = True
        self._dirty_run_ids.update(run.id for run in recovered_runs)
        await self.flush()

    def _new_task(self, chapter: Chapter, stage: Stage) -> TaskRecord:
        return TaskRecord(
            chapter_id=chapter.id,
            book_id=chapter.book_id,
            chapter_number=chapter.number,
            chapter_title=chapter.title,
            stage=stage.value,
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
            "chapter_id": run.chapter_id,
            "stage": str(run.stage),
            "round": run.round,
            "model": run.model,
            "status": str(run.status),
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "pid": run.pid,
            "thread_id": run.thread_id,
            "exit_code": run.exit_code,
            "changed": run.changed,
            "placeholders": run.placeholders,
            "usage": self._usage_dict(run.usage),
            "log_path": run.log_path,
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
            "chapter_id": task.chapter_id,
            "book_id": task.book_id,
            "chapter_number": task.chapter_number,
            "chapter_title": task.chapter_title,
            "stage": str(task.stage),
            "status": str(task.status),
            "detail": task.detail,
            "source_digest": task.source_digest,
            "rounds": task.rounds,
            "updated_at": task.updated_at,
        }

    @staticmethod
    def _issue_dict(issue: SourceIssueRecord) -> dict[str, Any]:
        return {name: getattr(issue, name) for name in SourceIssueRecord.__dataclass_fields__}

    @staticmethod
    def _build_dict(build: CoordinatorBuildRecord) -> dict[str, Any]:
        return {name: getattr(build, name) for name in CoordinatorBuildRecord.__dataclass_fields__}

    def hot_snapshot(self) -> dict[str, Any]:
        usage = self.total_usage()
        invocation_usage = self.invocation_usage()
        cost = self.total_cost()
        invocation_cost = self.invocation_cost()
        return {
            "version": 10,
            "history_database": DATABASE_NAME,
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
            "fixup_graph": self.fixup_graph,
            "fixup_requests": self.fixup_requests,
            "proof_review_requests": self.proof_review_requests,
            "isolation": self.isolation,
            "coordinator_build": self._build_dict(self.coordinator_build),
            "tasks": {
                key: self._task_dict(task)
                | {
                    "run_count": len(task.runs),
                    "latest_run_id": task.runs[-1].id if task.runs else None,
                }
                for key, task in sorted(self.tasks.items())
            },
        }

    def snapshot(self) -> dict[str, Any]:
        """Return a compatibility export, loading immutable run payloads on demand."""

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

    def _mark_dirty(self, *, run: RunRecord | None = None, issues: bool = False) -> None:
        self._checkpoint_dirty = True
        if run is not None:
            self._dirty_run_ids.add(run.id)
        self._issues_dirty = self._issues_dirty or issues

    async def _persist(self) -> None:
        if self._batch_depth:
            return
        await asyncio.sleep(0)
        await self.flush()

    async def flush(self) -> None:
        async with self._flush_lock:
            if not (self._checkpoint_dirty or self._dirty_run_ids or self._issues_dirty):
                return
            if not self.database_path.is_file():
                await asyncio.to_thread(self._database.initialize)
            self.updated_at = timestamp()
            checkpoint = self.hot_snapshot()
            dirty_runs = self._dirty_run_ids
            issues_dirty = self._issues_dirty
            self._dirty_run_ids = set()
            self._checkpoint_dirty = False
            self._issues_dirty = False
            runs = [
                (
                    self.key(run.chapter_id, Stage(run.stage)),
                    self._run_dict(run, include_payload=run.id in self._payload_loaded_run_ids),
                )
                for run_id in sorted(dirty_runs)
                if (run := self._runs_by_id.get(run_id)) is not None
            ]
            issues = (
                [self._issue_dict(issue) for _, issue in sorted(self.source_issues.items())]
                if issues_dirty
                else None
            )
            await asyncio.to_thread(self._database.write_batch, checkpoint, runs, issues)

    async def save(self) -> None:
        self._mark_dirty()
        await self._persist()

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
        await self.flush()

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

    def chapter_runs(self, chapter_id: str) -> tuple[RunRecord, ...]:
        return tuple(self._chapter_runs.get(chapter_id, ()))

    def latest_run(self, chapter_id: str) -> RunRecord | None:
        runs = self._chapter_runs.get(chapter_id, ())
        if not runs:
            return None
        running = [run for run in runs if run.status == TaskStatus.RUNNING]
        return max(running or runs, key=lambda run: (run.started_at, run.id))

    def active_run(self, chapter_id: str) -> RunRecord | None:
        return next(
            (
                run
                for run in self._chapter_runs.get(chapter_id, ())
                if run.status == TaskStatus.RUNNING
            ),
            None,
        )

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

    def _invalidate_status_summaries(self) -> None:
        self._agent_summary_cache = None
        self._stage_count_cache.clear()

    def agent_summary(self) -> dict[str, Any]:
        if self._agent_summary_cache is not None:
            return self._agent_summary_cache
        by_stage = {stage.value: 0 for stage in Stage}
        for task in self.tasks.values():
            for run in task.runs:
                if run.status == TaskStatus.RUNNING and run.stage in by_stage:
                    by_stage[run.stage] += 1
        self._agent_summary_cache = {
            "active": sum(by_stage.values()),
            "maximum": self.config.settings.max_agents,
            "queued": 0,
            "by_stage": by_stage,
        }
        return self._agent_summary_cache

    def stage_counts(self, stage: Stage) -> dict[str, int]:
        if stage.value in self._stage_count_cache:
            return self._stage_count_cache[stage.value]
        statuses = {status.value: 0 for status in TaskStatus}
        for task in self.tasks.values():
            if task.stage != stage.value:
                continue
            statuses[str(task.status)] += 1
        self._stage_count_cache[stage.value] = statuses
        return statuses

    def _record_source_issues(self, run: RunRecord) -> list[str]:
        report = run.report or {}
        raw_issues = report.get("source_issues", [])
        if not isinstance(raw_issues, list):
            return []
        chapter = self.config.chapter(run.chapter_id)
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

        by_chapter = {chapter.id: TokenUsage() for chapter in self.config.chapters}
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

        by_chapter = {chapter.id: CostEstimate() for chapter in self.config.chapters}
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
        if not run.usage.measured:
            return CostEstimate()
        model = run.model or LEGACY_MODEL
        return estimate_cost(
            model=model,
            input_tokens=run.usage.input_tokens,
            cached_input_tokens=run.usage.cached_input_tokens,
            output_tokens=run.usage.output_tokens,
            inferred=run.model is None,
        )

    def total_cost(self) -> CostEstimate:
        return self._cost(invocation_only=False)

    def invocation_cost(self, chapter_id: str | None = None) -> CostEstimate:
        return self._cost(invocation_only=True, chapter_id=chapter_id)

    def task(self, chapter_id: str, stage: Stage) -> TaskRecord:
        return self.tasks[self.key(chapter_id, stage)]

    async def set_task(
        self,
        chapter_id: str,
        stage: Stage,
        status: TaskStatus,
        detail: str,
        *,
        source_digest: str | None = None,
    ) -> None:
        task = self.task(chapter_id, stage)
        task.status = status
        if stage is Stage.PROVE:
            task.source_digest = source_digest if status == TaskStatus.SUCCEEDED else None
        task.detail = detail
        task.updated_at = timestamp()
        self._invalidate_status_summaries()
        self._mark_dirty()
        await self._persist()

    async def set_tasks(
        self,
        chapter_ids: Iterable[str],
        stage: Stage,
        status: TaskStatus,
        detail: str,
    ) -> None:
        changed = False
        updated_at = timestamp()
        for chapter_id in chapter_ids:
            key = self.key(chapter_id, stage)
            if key not in self.tasks:
                continue
            task = self.tasks[key]
            task.status = status
            if stage is Stage.PROVE and status != TaskStatus.SUCCEEDED:
                task.source_digest = None
            task.detail = detail
            task.updated_at = updated_at
            changed = True
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty()
            await self._persist()

    async def unblock(self) -> list[str]:
        """Reset blocked tasks to pending without discarding attempt history."""
        changed: list[str] = []
        for key, task in self.tasks.items():
            if task.status != TaskStatus.BLOCKED:
                continue
            task.status = TaskStatus.PENDING
            if task.stage == Stage.PROVE:
                task.source_digest = None
            task.detail = "manually unblocked"
            task.updated_at = timestamp()
            changed.append(key)
        if changed:
            self._invalidate_status_summaries()
            self._mark_dirty()
            await self._persist()
        return changed

    async def start_run(self, chapter_id: str, stage: Stage) -> RunRecord:
        task = self.task(chapter_id, stage)
        task.status = TaskStatus.RUNNING
        if stage is Stage.PROVE:
            task.source_digest = None
        task.rounds += 1
        task.updated_at = timestamp()
        run = RunRecord(
            id=uuid4().hex[:12],
            chapter_id=chapter_id,
            stage=stage.value,
            round=task.rounds,
            model=self.config.settings.model,
        )
        task.runs.append(run)
        self._index_run(run)
        self._payload_loaded_run_ids.add(run.id)
        self._invalidate_aggregates()
        self._invalidate_status_summaries()
        self._mark_dirty(run=run)
        await self._persist()
        return run

    async def update_run(self, run: RunRecord, *, deferred: bool = False, **changes: Any) -> None:
        for name, value in changes.items():
            setattr(run, name, value)
        if "usage" in changes or "model" in changes:
            self._invalidate_aggregates()
        if "status" in changes:
            self._invalidate_status_summaries()
        self._mark_dirty(run=run)
        if not deferred:
            await self._persist()

    async def finish_run(self, run: RunRecord, *, status: TaskStatus, **changes: Any) -> None:
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
        self._mark_dirty(run=run, issues=bool(issue_ids))
        await self._persist()

    async def start_coordinator_build(
        self,
        *,
        mode: str,
        stage: Stage,
        iteration: int,
        maximum_iterations: int,
        total: int,
    ) -> None:
        self.coordinator_build = CoordinatorBuildRecord(
            active=True,
            mode=mode,
            stage=stage.value,
            iteration=iteration,
            maximum_iterations=maximum_iterations,
            total=total,
        )
        self._mark_dirty()
        await self._persist()

    async def advance_coordinator_build(
        self,
        *,
        chapter_id: str,
        completed: int,
        command: str | None = None,
    ) -> None:
        self.coordinator_build.current_chapter_id = chapter_id
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
        errors, warnings = lean_diagnostic_counts(output)
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

    async def finish_coordinator_build(self) -> None:
        self.coordinator_build.active = False
        self.coordinator_build.current_chapter_id = ""
        self.coordinator_build.updated_at = timestamp()
        self._mark_dirty()
        await self._persist()
