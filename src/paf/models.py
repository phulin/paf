from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from paf.project import Project


class Stage(StrEnum):
    DISCOVER = "discover"
    FORMALIZE = "formalize"
    REVIEW = "review"
    PROVE = "prove"


STATEMENT_STAGES = (Stage.DISCOVER, Stage.FORMALIZE, Stage.REVIEW)
PROOF_STAGES = (Stage.PROVE,)


@dataclass(frozen=True)
class StageConfig:
    prompt: Path
    max_rounds: int
    max_agents: int | None = None
    model: str | None = None
    reasoning_effort: str | None = None


@dataclass(frozen=True)
class SwarmSettings:
    repo: Path
    state_dir: Path
    max_agents: int = 16
    codex_bin: str = "codex"
    model: str | None = "gpt-5.6-luna"
    reasoning_effort: str | None = "xhigh"
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
    lean_project: Path = Path("lean")
    lean_mcp_tool_timeout_seconds: float = 300.0


@dataclass(frozen=True)
class ShepherdSettings:
    """Configuration for failure triage and repair work.

    The Shepherd is enabled by default and uses a stronger planning model.
    Projects may opt out explicitly; repair workers keep using the inexpensive
    editing model and a high reasoning effort.
    """

    enabled: bool = True
    model: str = "gpt-5.6-sol"
    reasoning_effort: str = "medium"
    worker_model: str = "gpt-5.6-luna"
    worker_reasoning_effort: str = "xhigh"
    interval_seconds: float = 1200.0
    failure_threshold: int = 10
    maximum_failures_per_sweep: int = 50
    maximum_work_units_per_sweep: int = 32
    maximum_sweeps_per_invocation: int = 3
    max_agents: int = 2


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
    format: str = "markdown"
    adapter_profile: str = "numbered-chapters"
    unit: str | None = None
    follow_includes: bool = False
    delimiter: str | None = None
    verbatim_environments: tuple[str, ...] = ()
    heading_pattern: str | None = r"^##\s+(?P<number>\d+)\.\s+(?P<title>.+?)\s*$"
    chapter_path: str = "Chapter{chapter_number_padded}"
    chapter_module: str = "{module}.Chapter{chapter_number_padded}"
    build_command: str = "cd lean && lake build +{chapter_module}"
    scope: tuple[str, ...] = (
        "{lean_root}/{chapter_path}.lean",
        "{lean_root}/{chapter_path}/**/*.lean",
    )
    context: dict[str, str] = field(default_factory=dict)

    def as_source_document(self) -> SourceDocument:
        """Adapt a legacy ``[[books]]`` entry to the source-model boundary."""
        metadata: dict[str, Any] = {
            "profile": self.adapter_profile,
            "heading_pattern": self.heading_pattern,
        }
        if self.format != "markdown" or self.adapter_profile != "numbered-chapters":
            metadata.update(
                {
                    "unit": self.unit,
                    "follow_includes": self.follow_includes,
                    "delimiter": self.delimiter,
                }
            )
        return SourceDocument(
            id=self.id,
            path=self.source,
            format=self.format,
            title=self.title,
            metadata=metadata,
            depends_on=self.depends_on,
            statement_effort=self.statement_effort,
            proof_effort=self.proof_effort,
        )


@dataclass(frozen=True)
class SourceSpan:
    """A one-based, inclusive line span in a source document."""

    start_line: int
    end_line: int

    def __post_init__(self) -> None:
        if self.start_line < 1:
            raise ValueError("source span start_line must be positive")
        if self.end_line < self.start_line:
            raise ValueError("source span end_line must not precede start_line")


@dataclass(frozen=True)
class SourceDocument:
    """A format-neutral source file, identified independently of its checkout."""

    id: str
    path: Path
    format: str
    title: str
    metadata: dict[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    statement_effort: float | None = None
    proof_effort: float | None = None

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("source document id must not be empty")
        if self.path.is_absolute():
            raise ValueError("source document paths must be repository-relative")
        if not self.format:
            raise ValueError("source document format must not be empty")


@dataclass(frozen=True)
class TargetMapping:
    """Backend-owned target coordinates for one source work unit."""

    root: Path
    module: str
    path: str
    unit_module: str
    build_command: str
    scope: tuple[str, ...]
    backend: str = "lean"

    def __post_init__(self) -> None:
        if not self.backend:
            raise ValueError("target backend must not be empty")
        if self.root.is_absolute() or ".." in self.root.parts:
            raise ValueError("target root must be repository-relative")
        target_path = Path(self.path)
        if not self.path or target_path.is_absolute() or ".." in target_path.parts:
            raise ValueError("target path must be non-empty and repository-relative")
        if not self.unit_module:
            raise ValueError("target module must not be empty")
        if not self.scope:
            raise ValueError("target scope must not be empty")
        if any(Path(item).is_absolute() or ".." in Path(item).parts for item in self.scope):
            raise ValueError("target scopes must be repository-relative")

    @property
    def target_path(self) -> Path:
        return self.root / self.path


# Import compatibility for integrations that used the short-lived 1.1 name.
LegacyTargetMapping = TargetMapping


@dataclass(frozen=True)
class WorkUnit:
    """A schedulable portion of a source document.

    ``id`` is persisted and is intentionally supplied by discovery rather than
    derived from an absolute filesystem path. ``document.path`` is likewise
    repository-relative, so moving an entire project cannot change either.
    """

    id: str
    document: SourceDocument
    title: str
    ordinal: int
    source_span: SourceSpan
    depends_on: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    target: TargetMapping | None = None
    context: dict[str, str] = field(default_factory=dict)

    @property
    def document_id(self) -> str:
        return self.document.id

    @property
    def source(self) -> Path:
        return self.document.path

    # The properties below are the compatibility adapter for existing
    # Markdown/Lean code. New source-facing code uses the fields above.
    @property
    def book_id(self) -> str:
        return self.document_id

    @property
    def book_title(self) -> str:
        return self.document.title

    @property
    def number(self) -> int:
        return self.ordinal

    def _target(self) -> TargetMapping:
        if self.target is None:
            raise ValueError(f"work unit {self.id} has no target mapping")
        return self.target

    # Compatibility for callers from the source-model migration window.
    def _legacy_target(self) -> TargetMapping:
        return self._target()

    @property
    def lean_root(self) -> Path:
        return self._target().root

    @property
    def module(self) -> str:
        return self._target().module

    @property
    def chapter_path(self) -> str:
        return self._target().path

    @property
    def chapter_module(self) -> str:
        return self._target().unit_module

    @property
    def build_command(self) -> str:
        return self._target().build_command

    @property
    def scope(self) -> tuple[str, ...]:
        return self._target().scope

    @property
    def depends_on_books(self) -> tuple[str, ...]:
        return self.document.depends_on

    def variables(self) -> dict[str, str]:
        target = self._target()
        values = {
            "work_unit_id": self.id,
            "document_id": self.document_id,
            "document_title": self.document.title,
            "unit_ordinal": str(self.ordinal),
            "unit_ordinal_padded": f"{self.ordinal:02d}",
            "unit_title": self.title,
            "source": self.source.as_posix(),
            "source_start_line": str(self.source_span.start_line),
            "source_end_line": str(self.source_span.end_line),
            "book_id": self.book_id,
            "book_title": self.book_title,
            "chapter_number": str(self.number),
            "chapter_number_padded": f"{self.number:02d}",
            "chapter_title": self.title,
            "lean_root": target.root.as_posix(),
            "module": target.module,
            "chapter_path": target.path,
            "chapter_module": target.unit_module,
            "build_command": target.build_command,
        }
        values.update(self.context)
        return values


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
    source_span: SourceSpan = field(default_factory=lambda: SourceSpan(1, 1))

    @property
    def id(self) -> str:
        return f"{self.book_id}/chapter-{self.number:02d}"

    @property
    def work_unit_id(self) -> str:
        return self.id

    @property
    def document_id(self) -> str:
        return self.book_id

    @property
    def ordinal(self) -> int:
        return self.number

    @property
    def depends_on(self) -> tuple[str, ...]:
        return ()

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

    def as_work_unit(self, document: SourceDocument) -> WorkUnit:
        """Adapt this compatibility chapter to the canonical source model."""

        return WorkUnit(
            id=self.id,
            document=document,
            title=self.title,
            ordinal=self.number,
            source_span=self.source_span,
            target=TargetMapping(
                backend="lean",
                root=self.lean_root,
                module=self.module,
                path=self.chapter_path,
                unit_module=self.chapter_module,
                build_command=self.build_command,
                scope=self.scope,
            ),
            context=self.context,
        )


# Transitional structural unions used by compatibility-facing call sites.
# New orchestration state is populated with the canonical members.
SourceDocumentLike = SourceDocument | BookConfig
WorkUnitLike = WorkUnit | Chapter


@dataclass(frozen=True)
class PipelineConfig:
    path: Path
    settings: SwarmSettings
    stages: dict[Stage, StageConfig]
    books: tuple[BookConfig, ...]
    chapters: tuple[Chapter, ...]
    shepherd: ShepherdSettings = field(default_factory=ShepherdSettings)
    source_rules: tuple[dict[str, Any], ...] = ()
    source_roots: tuple[Path, ...] = ()
    source_include: tuple[str, ...] = ()
    source_exclude: tuple[str, ...] = ()
    backend: Any | None = None
    canonical_documents: tuple[SourceDocument, ...] | None = None
    canonical_work_units: tuple[WorkUnit, ...] | None = None
    project: Project | None = None

    def model_for(self, stage: Stage) -> str | None:
        """Resolve a stage model override against the swarm-wide default."""

        return self.stages[stage].model or self.settings.model

    def reasoning_effort_for(self, stage: Stage) -> str | None:
        """Resolve a stage reasoning override against the swarm-wide default."""

        return self.stages[stage].reasoning_effort or self.settings.reasoning_effort

    @property
    def documents(self) -> tuple[SourceDocument, ...]:
        """Canonical documents; ``books`` remains a compatibility adapter."""

        if self.canonical_documents is not None:
            return self.canonical_documents
        return tuple(book.as_source_document() for book in self.books)

    @property
    def work_units(self) -> tuple[WorkUnit, ...]:
        """Canonical units; ``chapters`` remains a compatibility adapter."""

        if self.canonical_work_units is not None:
            return self.canonical_work_units
        documents = {document.id: document for document in self.documents}
        return tuple(chapter.as_work_unit(documents[chapter.book_id]) for chapter in self.chapters)

    def work_unit(self, work_unit_id: str) -> WorkUnit:
        for work_unit in self.work_units:
            if work_unit.id == work_unit_id:
                return work_unit
        raise KeyError(work_unit_id)

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
