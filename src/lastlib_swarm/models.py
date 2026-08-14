from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class Stage(StrEnum):
    FORMALIZE = "formalize"
    FIXUP = "fixup"
    REVIEW = "review"
    PROVE = "prove"
    REPAIR = "fixup"  # Backward-compatible source alias.


STATEMENT_STAGES = (Stage.FORMALIZE, Stage.FIXUP, Stage.REVIEW)
PROOF_STAGES = (Stage.PROVE,)


@dataclass(frozen=True)
class StageConfig:
    prompt: Path
    max_rounds: int


@dataclass(frozen=True)
class SwarmSettings:
    repo: Path
    state_dir: Path
    max_agents: int = 16
    codex_bin: str = "codex"
    model: str | None = "gpt-5.6-luna"
    reasoning_effort: str | None = "max"
    sandbox: str = "danger-full-access"
    approve_for_me: bool = False
    bypass_approvals_and_sandbox: bool = True
    agent_timeout_seconds: float = 7200.0
    capacity_resume_attempts: int = 10
    capacity_resume_delay_seconds: float = 15.0
    capacity_resume_max_delay_seconds: float = 120.0
    codex_fd_recycle_threshold: int = 256
    codex_fd_recycle_attempts: int = 20
    validation_timeout_seconds: float = 1800.0
    isolation: str = "auto"
    cache_compaction_layers: int = 32
    lean_mcp: bool = True
    lean_project: Path = Path("lean")
    lean_mcp_tool_timeout_seconds: float = 300.0


@dataclass(frozen=True)
class BookConfig:
    id: str
    title: str
    source: Path
    lean_root: Path
    module: str
    depends_on: tuple[str, ...] = ()
    statement_effort: float | None = None
    proof_effort: float | None = None
    chapters: tuple[int, ...] = ()
    heading_pattern: str = r"^##\s+(?P<number>\d+)\.\s+(?P<title>.+?)\s*$"
    chapter_path: str = "Chapter{chapter_number_padded}"
    chapter_module: str = "{module}.Chapter{chapter_number_padded}"
    build_command: str = "cd lean && lake build +{chapter_module}"
    scope: tuple[str, ...] = (
        "{lean_root}/{chapter_path}.lean",
        "{lean_root}/{chapter_path}/**/*.lean",
    )
    context: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Chapter:
    book_id: str
    book_title: str
    number: int
    title: str
    source: Path
    lean_root: Path
    module: str
    chapter_path: str
    chapter_module: str
    build_command: str
    scope: tuple[str, ...]
    depends_on_books: tuple[str, ...]
    context: dict[str, str] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.book_id}/chapter-{self.number:02d}"

    def variables(self) -> dict[str, str]:
        values = {
            "book_id": self.book_id,
            "book_title": self.book_title,
            "chapter_number": str(self.number),
            "chapter_number_padded": f"{self.number:02d}",
            "chapter_title": self.title,
            "source": self.source.as_posix(),
            "lean_root": self.lean_root.as_posix(),
            "module": self.module,
            "chapter_path": self.chapter_path,
            "chapter_module": self.chapter_module,
            "build_command": self.build_command,
        }
        values.update(self.context)
        return values


@dataclass(frozen=True)
class PipelineConfig:
    path: Path
    settings: SwarmSettings
    stages: dict[Stage, StageConfig]
    books: tuple[BookConfig, ...]
    chapters: tuple[Chapter, ...]

    def chapter(self, chapter_id: str) -> Chapter:
        for chapter in self.chapters:
            if chapter.id == chapter_id:
                return chapter
        raise KeyError(chapter_id)


def as_string_dict(value: Any, *, name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a table")
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
        raise ValueError(f"{name} keys and values must be strings")
    return dict(value)
