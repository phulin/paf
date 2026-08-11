from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from lastlib_swarm import json_codec as json
from lastlib_swarm.activity import ActivityStore
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.pricing import LEGACY_MODEL, CostEstimate, estimate_cost


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
    rounds: int = 0
    updated_at: str = field(default_factory=timestamp)
    runs: list[RunRecord] = field(default_factory=list)


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
        self.source_issues_path = config.settings.state_dir / "source-issues.json"
        self.logs_dir = config.settings.state_dir / "logs"
        self.activities = ActivityStore(self.logs_dir)
        self._lock = asyncio.Lock()
        self._prior_run_ids: set[str] = set()
        self.tasks: dict[str, TaskRecord] = {}
        self.source_issues: dict[str, SourceIssueRecord] = {}
        self.scheduling: dict[str, Any] = {}
        self.isolation: dict[str, Any] = {}
        self.created_at = timestamp()
        self.updated_at = self.created_at

    @staticmethod
    def key(chapter_id: str, stage: Stage) -> str:
        return f"{chapter_id}:{stage.value}"

    async def load_or_create(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        persisted_source_issues: list[Any] = []
        if self.source_issues_path.is_file():
            ledger = json.loads(self.source_issues_path.read_text(encoding="utf-8"))
            if not isinstance(ledger, dict) or not isinstance(ledger.get("issues"), list):
                raise ValueError(f"invalid source-issue ledger: {self.source_issues_path}")
            persisted_source_issues.extend(ledger["issues"])
        if self.path.is_file():
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.created_at = str(raw.get("created_at", timestamp()))
            self.updated_at = str(raw.get("updated_at", self.created_at))
            if not self.scheduling and isinstance(raw.get("scheduling"), dict):
                self.scheduling = raw["scheduling"]
            if not self.isolation and isinstance(raw.get("isolation"), dict):
                self.isolation = raw["isolation"]
            persisted_tasks = raw.get("tasks", {})
            for key, value in persisted_tasks.items():
                if key.endswith(":repair"):
                    key = f"{key[: -len(':repair')]}:fixup"
                    if key in persisted_tasks:
                        continue
                runs = []
                for item in value.get("runs", []):
                    if item.get("stage") == "repair":
                        item["stage"] = "fixup"
                    usage = TokenUsage(**item.pop("usage", {}))
                    runs.append(RunRecord(**item, usage=usage))
                if value.get("stage") == "repair":
                    value["stage"] = "fixup"
                self.tasks[key] = TaskRecord(
                    **{name: item for name, item in value.items() if name != "runs"}, runs=runs
                )
            raw_source_issues = raw.get("source_issues", [])
            if isinstance(raw_source_issues, list):
                persisted_source_issues.extend(raw_source_issues)
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
        self._prior_run_ids = {run.id for task in self.tasks.values() for run in task.runs}
        for task in self.tasks.values():
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.PENDING
                task.detail = "recovered after interrupted orchestrator"
                for run in task.runs:
                    if run.status == TaskStatus.RUNNING:
                        run.status = TaskStatus.FAILED
                        run.finished_at = timestamp()
        await self.save()

    def _new_task(self, chapter: Chapter, stage: Stage) -> TaskRecord:
        return TaskRecord(
            chapter_id=chapter.id,
            book_id=chapter.book_id,
            chapter_number=chapter.number,
            chapter_title=chapter.title,
            stage=stage.value,
        )

    async def save(self) -> None:
        async with self._lock:
            self.updated_at = timestamp()
            payload = self.snapshot()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
            temporary.write_bytes(json.dumpb(payload, indent=True, sort_keys=True))
            os.replace(temporary, self.path)
            ledger = {
                "version": 1,
                "updated_at": self.updated_at,
                "issues": [asdict(issue) for _, issue in sorted(self.source_issues.items())],
            }
            ledger_temporary = self.source_issues_path.with_name(
                f".{self.source_issues_path.name}.{os.getpid()}.tmp"
            )
            ledger_temporary.write_bytes(json.dumpb(ledger, indent=True, sort_keys=True))
            os.replace(ledger_temporary, self.source_issues_path)

    def snapshot(self) -> dict[str, Any]:
        usage = self.total_usage()
        invocation_usage = self.invocation_usage()
        cost = self.total_cost()
        invocation_cost = self.invocation_cost()
        return {
            "version": 4,
            "config": str(self.config.path),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage": asdict(usage) | {"total_tokens": usage.total_tokens},
            "invocation_usage": asdict(invocation_usage)
            | {"total_tokens": invocation_usage.total_tokens},
            "cost": cost.as_dict(),
            "invocation_cost": invocation_cost.as_dict(),
            "scheduling": self.scheduling,
            "isolation": self.isolation,
            "source_issues": [asdict(issue) for _, issue in sorted(self.source_issues.items())],
            "tasks": {key: asdict(value) for key, value in sorted(self.tasks.items())},
        }

    def _record_source_issues(self, run: RunRecord) -> None:
        report = run.report or {}
        raw_issues = report.get("source_issues", [])
        if not isinstance(raw_issues, list):
            return
        chapter = self.config.chapter(run.chapter_id)
        required = ("location", "source_excerpt", "description", "suggested_correction")
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

    def total_usage(self) -> TokenUsage:
        total = TokenUsage()
        for task in self.tasks.values():
            for run in task.runs:
                total += run.usage
        return total

    def invocation_usage(self, chapter_id: str | None = None) -> TokenUsage:
        """Usage from attempts created by this orchestrator invocation."""

        total = TokenUsage()
        for task in self.tasks.values():
            if chapter_id is not None and task.chapter_id != chapter_id:
                continue
            for run in task.runs:
                if run.id not in self._prior_run_ids:
                    total += run.usage
        return total

    def _cost(self, *, invocation_only: bool, chapter_id: str | None = None) -> CostEstimate:
        total = CostEstimate()
        for task in self.tasks.values():
            if chapter_id is not None and task.chapter_id != chapter_id:
                continue
            for run in task.runs:
                if invocation_only and run.id in self._prior_run_ids:
                    continue
                total += self.run_cost(run)
        return total

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
        self, chapter_id: str, stage: Stage, status: TaskStatus, detail: str
    ) -> None:
        task = self.task(chapter_id, stage)
        task.status = status
        task.detail = detail
        task.updated_at = timestamp()
        await self.save()

    async def unblock(self) -> list[str]:
        """Reset blocked tasks to pending without discarding attempt history."""
        changed: list[str] = []
        for key, task in self.tasks.items():
            if task.status != TaskStatus.BLOCKED:
                continue
            task.status = TaskStatus.PENDING
            task.detail = "manually unblocked"
            task.updated_at = timestamp()
            changed.append(key)
        if changed:
            await self.save()
        return changed

    async def start_run(self, chapter_id: str, stage: Stage) -> RunRecord:
        task = self.task(chapter_id, stage)
        task.status = TaskStatus.RUNNING
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
        await self.save()
        return run

    async def update_run(self, run: RunRecord, **changes: Any) -> None:
        for name, value in changes.items():
            setattr(run, name, value)
        await self.save()

    async def finish_run(self, run: RunRecord, *, status: TaskStatus, **changes: Any) -> None:
        run.status = status
        run.finished_at = timestamp()
        for name, value in changes.items():
            setattr(run, name, value)
        self._record_source_issues(run)
        await self.save()
