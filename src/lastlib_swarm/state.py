from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from lastlib_swarm.models import Chapter, PipelineConfig, Stage


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
    def api_tokens(self) -> int:
        """API-equivalent total; cached tokens are already part of input tokens."""
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


class StateStore:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.path = config.settings.state_dir / "state.json"
        self.logs_dir = config.settings.state_dir / "logs"
        self._lock = asyncio.Lock()
        self.tasks: dict[str, TaskRecord] = {}
        self.created_at = timestamp()
        self.updated_at = self.created_at

    @staticmethod
    def key(chapter_id: str, stage: Stage) -> str:
        return f"{chapter_id}:{stage.value}"

    async def load_or_create(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        if self.path.is_file():
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.created_at = str(raw.get("created_at", timestamp()))
            self.updated_at = str(raw.get("updated_at", self.created_at))
            for key, value in raw.get("tasks", {}).items():
                runs = []
                for item in value.get("runs", []):
                    usage = TokenUsage(**item.pop("usage", {}))
                    runs.append(RunRecord(**item, usage=usage))
                self.tasks[key] = TaskRecord(
                    **{name: item for name, item in value.items() if name != "runs"}, runs=runs
                )
        configured = {chapter.id for chapter in self.config.chapters}
        self.tasks = {
            key: task for key, task in self.tasks.items() if task.chapter_id in configured
        }
        for chapter in self.config.chapters:
            for stage in Stage:
                key = self.key(chapter.id, stage)
                self.tasks.setdefault(key, self._new_task(chapter, stage))
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
            temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temporary, self.path)

    def snapshot(self) -> dict[str, Any]:
        usage = self.total_usage()
        return {
            "version": 1,
            "config": str(self.config.path),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "usage": asdict(usage) | {"api_tokens": usage.api_tokens},
            "tasks": {key: asdict(value) for key, value in sorted(self.tasks.items())},
        }

    def total_usage(self) -> TokenUsage:
        total = TokenUsage()
        for task in self.tasks.values():
            for run in task.runs:
                total += run.usage
        return total

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
        await self.save()
