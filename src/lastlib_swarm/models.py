from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any


class Stage(StrEnum):
    FORMALIZE = "formalize"
    REVIEW = "review"
    PROVE = "prove"
    REPAIR = "repair"


STATEMENT_STAGES = (Stage.FORMALIZE, Stage.REVIEW)
PROOF_STAGES = (Stage.PROVE, Stage.REPAIR)


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
    model: str | None = None
    reasoning_effort: str | None = None
    sandbox: str = "workspace-write"
    approve_for_me: bool = True
    bypass_approvals_and_sandbox: bool = False
    agent_timeout_seconds: float = 7200.0
    validation_timeout_seconds: float = 1800.0


@dataclass(frozen=True)
class BookConfig:
    id: str
    title: str
    source: Path
    lean_root: Path
    module: str
    depends_on: tuple[str, ...] = ()
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
