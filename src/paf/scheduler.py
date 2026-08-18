from __future__ import annotations

import asyncio
import hashlib
import re
from collections import deque
from collections.abc import Callable, Coroutine, Iterable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from paf import json_codec as json
from paf.codex import (
    DIAGNOSTIC_REVIEW_ROLE,
    DIAGNOSTIC_REVIEW_ROLES,
    DOWNSTREAM_RETRY_ROLE,
    REPAIR_WORKER_ROLE,
    SHEPHERD_ROLE,
    UPSTREAM_REPAIR_ROLE,
    WARNING_REVIEW_ROLE,
    AgentResult,
    CodexExecutor,
    ValidationResult,
    ValidationStatus,
    count_placeholders,
    declaration_uses_placeholder,
    declaration_uses_placeholder_in_chapter,
    migrate_scope_digests,
    proof_target_chunk,
    proof_target_spans,
    proof_targets,
    report_schema_key,
    scope_digest,
    scoped_files,
    validate,
)
from paf.coordination import CoordinatorBuildQueue, PriorityLimiter
from paf.corpus import (
    WorkUnitImportGraph,
    build_compiled_import_graph,
    build_corpus_schedule,
    build_source_dependency_graph,
    scheduling_snapshot,
)
from paf.diagnostics import unexpected_lean_warnings
from paf.git import GitCommitError, GitCommitter
from paf.hashing import is_legacy_digest, migrate_digest_text, tagged_digest_text
from paf.interface_fingerprint import (
    FingerprintCollection,
    InterfaceFingerprintError,
    collect_interface_fingerprints,
)
from paf.isolation import IsolationResult, create_isolation
from paf.models import PipelineConfig, ProofTarget, Stage, WorkUnit, WorkUnitLike
from paf.scope import ScopeMatcher
from paf.state import (
    ProofBlockerStatus,
    RepairCaseRecord,
    RepairCaseStatus,
    RepairWorkUnitRecord,
    RepairWorkUnitStatus,
    Requirement,
    RequirementKind,
    RunRecord,
    StateStore,
    TaskPhase,
    TaskStatus,
    UpstreamRequestStatus,
)

REPAIR_EFFORT = {"small": 1.0, "medium": 3.0, "large": 8.0}
REVIEW_REPORT_RETRY_PROMPT = """Your previous review turn did not satisfy the report contract:

{error}

Continue the same review assignment in the current files. Re-read the assignment's definition of
done and structured-output instructions, finish any incomplete review work, and return exactly one
complete structured report. Account for every supplied finding id when the assignment includes
proof findings."""
LIVE_AGENT_RETRY_PROMPT = """An operator requested a targeted retry of this live agent. Continue
the same assignment from the current files and conversation. Re-read the assignment instructions,
correct the issue that prevented the prior turn from completing, and return the required structured
report only after the work is stable."""
PROOF_VALIDATION_RETRY_PROMPT = """PAF's authoritative build rejected the proof changes. Continue
the same proof assignment, use the attached validation diagnostics as required work, and repair the
cause within your assigned proof edits, focused helpers, or imports. Do not edit an unrelated
declaration merely because it reports a downstream symptom. Return only after validation is clean or
you have precise new blocker evidence."""


class ShepherdPlanError(ValueError):
    """The Shepherd returned a plan that cannot safely enter the scheduler."""


async def _gather_cancel_on_error[T](
    operations: Iterable[Coroutine[Any, Any, T]],
) -> list[T]:
    """Cancel and drain sibling operations when any one raises."""

    tasks = [asyncio.create_task(operation) for operation in operations]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass(frozen=True)
class Attempt:
    agent: AgentResult
    validation: ValidationResult
    run: RunRecord

    def feedback(self, *, validation_output: str | None = None) -> str:
        parts = []
        if self.agent.error:
            parts.append(self.agent.error)
        summary = self.agent.report.get("summary")
        if isinstance(summary, str) and summary:
            parts.append(f"Agent summary: {summary}")
        issues = self.agent.report.get("issues")
        if isinstance(issues, list) and issues:
            parts.append("Reported issues:\n" + "\n".join(f"- {issue}" for issue in issues))
        parts.append(
            f"Scoped changes: {self.agent.changed}; remaining proof placeholders: "
            f"{self.agent.placeholders}."
        )
        output = self.validation.output if validation_output is None else validation_output
        if not self.validation.succeeded and output:
            parts.append("Validation failed:\n" + output)
        return "\n\n".join(parts)


class ExecutionDisposition(StrEnum):
    SUCCEEDED = "succeeded"
    WAITING = "waiting"
    FAILED = "failed"


@dataclass(frozen=True)
class StageOutcome:
    disposition: ExecutionDisposition
    waiting_on: tuple[Requirement, ...] = ()
    changed: bool = False
    complete: bool = True
    run_id: str = ""
    report_error: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, ExecutionDisposition):
            raise TypeError("StageOutcome requires an explicit ExecutionDisposition")

    @property
    def succeeded(self) -> bool:
        return self.disposition is ExecutionDisposition.SUCCEEDED

    @property
    def failed(self) -> bool:
        return self.disposition is ExecutionDisposition.FAILED

    @property
    def waiting(self) -> bool:
        return self.disposition is ExecutionDisposition.WAITING

    def __bool__(self) -> bool:
        return self.succeeded


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedBuildSnapshot:
    graph: WorkUnitImportGraph
    source_digests: dict[str, str]
    source_generations: dict[str, int] = field(default_factory=dict)
    fingerprint: dict[str, Any] | None = None
    import_dependencies: tuple[str, ...] = ()
    fingerprint_error: str = ""


@dataclass(frozen=True)
class PendingBuildRequest:
    chapters: tuple[WorkUnitLike, ...]
    publish_if_clean: bool
    mode: str
    iteration: int
    maximum_iterations: int
    stage: Stage
    snapshots: dict[str, ValidatedBuildSnapshot] | None
    future: asyncio.Future[dict[str, ValidationResult]]


@dataclass(frozen=True)
class PendingDiscovery:
    chapter: WorkUnitLike
    dependencies: tuple[str, ...]
    summary: str
    issues: tuple[Any, ...]
    future: asyncio.Future[None]


@dataclass(frozen=True)
class RunningFormalizeStage:
    task: asyncio.Task[bool]
    progress: asyncio.Event
    target_ids: frozenset[str]


@dataclass(frozen=True)
class RunningReview:
    task: asyncio.Task[StageOutcome]
    dependencies: frozenset[str]
    proof_request_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class LeanDiagnostic:
    severity: str
    header: str
    text: str


LEAN_DIAGNOSTIC_RE = re.compile(r"^(?P<severity>error|warning):[ \t]*(?P<message>.*)$")
LEAN_LOCATION_RE = re.compile(r"^(?P<path>.+?\.lean):(?P<line>\d+):(?P<column>\d+):(?:[ \t]|$)")
LAKE_CONTROL_PREFIXES = (
    "⚠ ",
    "✖ ",
    "✔ ",
    "trace:",
    "Some required targets logged failures:",
    "Coordinator rejected ",
)
PROOF_FEEDBACK_MAX_CHARS = 12_000
PROOF_FEEDBACK_ROUNDS = 3
DISCOVERY_BATCH_SECONDS = 0.025
DISCOVERY_BATCH_MAXIMUM = 256
DIAGNOSTIC_OWNER_CACHE_MAXIMUM = 16_384
PROOF_FINDING_REVIEW_KIND = "proof_finding"
BUILD_ERROR_REVIEW_KIND = "build_error"
BUILD_WARNING_REVIEW_KIND = "build_warning"
LEGACY_DIAGNOSTIC_REVIEW_KIND = "diagnostic"
DIAGNOSTIC_REVIEW_KINDS = frozenset(
    {
        BUILD_ERROR_REVIEW_KIND,
        BUILD_WARNING_REVIEW_KIND,
        LEGACY_DIAGNOSTIC_REVIEW_KIND,
    }
)


@dataclass
class _IdentifierTrieNode:
    children: dict[str, _IdentifierTrieNode] = field(default_factory=dict)
    owner_ids: tuple[str, ...] = ()


def _is_identifier_character(character: str) -> bool:
    return character == "_" or (character.isascii() and character.isalnum())


def _bounded_proof_feedback(blocks: Iterable[str]) -> str:
    text = "\n\n".join(blocks)
    if len(text) <= PROOF_FEEDBACK_MAX_CHARS:
        return text
    marker = "\n\n... older proof feedback omitted ...\n\n"
    available = PROOF_FEEDBACK_MAX_CHARS - len(marker)
    head = available // 3
    return text[:head] + marker + text[-(available - head) :]


def _lean_diagnostics(output: str) -> tuple[LeanDiagnostic, ...]:
    """Extract actionable Lean diagnostics without Lake replay/progress chatter."""

    diagnostics: list[LeanDiagnostic] = []
    severity = ""
    header = ""
    lines: list[str] = []

    def finish() -> None:
        nonlocal severity, header, lines
        if not header:
            return
        text = "\n".join(lines).rstrip()
        if severity == "error" or unexpected_lean_warnings(header):
            diagnostics.append(LeanDiagnostic(severity, header, text))
        severity = ""
        header = ""
        lines = []

    for line in output.splitlines():
        match = LEAN_DIAGNOSTIC_RE.match(line)
        if match:
            finish()
            severity = match.group("severity")
            header = line.strip()
            lines = [line.rstrip()]
            continue
        if header and line.startswith(LAKE_CONTROL_PREFIXES):
            finish()
            continue
        if header:
            lines.append(line.rstrip())
    finish()

    # Validation appends a compact list of rejected warnings after the complete
    # output. Prefer the first copy because it retains the diagnostic body.
    unique: dict[str, LeanDiagnostic] = {}
    for diagnostic in diagnostics:
        unique.setdefault(diagnostic.header, diagnostic)
    return tuple(unique.values())


def _failed_modules(output: str) -> tuple[str, ...]:
    marker = "Some required targets logged failures:"
    _, found, suffix = output.rpartition(marker)
    if not found:
        return ()
    modules: list[str] = []
    for line in suffix.splitlines()[1:]:
        if match := re.fullmatch(r"-\s+([A-Za-z0-9_'.]+)", line.strip()):
            modules.append(match.group(1))
        elif modules:
            break
    return tuple(dict.fromkeys(modules))


class RunControl:
    """Cooperative pause/stop control checked between chapter attempts."""

    def __init__(self) -> None:
        self._gate = asyncio.Event()
        self._gate.set()
        self.paused = False
        self.stopping = False
        self.integrate_interrupted_workspaces = False

    async def checkpoint(self) -> None:
        await self._gate.wait()
        if self.stopping:
            raise asyncio.CancelledError

    def pause(self) -> None:
        if not self.stopping:
            self.paused = True
            self._gate.clear()

    def resume(self) -> None:
        if not self.stopping:
            self.paused = False
            self._gate.set()

    def stop(self, *, integrate_interrupted_workspaces: bool = False) -> None:
        self.stopping = True
        self.integrate_interrupted_workspaces |= integrate_interrupted_workspaces
        self.paused = False
        self._gate.set()


def scaffold_directories(
    config: PipelineConfig, chapters: Iterable[WorkUnitLike]
) -> tuple[str, ...]:
    """Create chapter directories deterministically without creating Lean files."""

    created: list[str] = []
    for chapter in chapters:
        paths = (
            config.backend.scaffold_paths(chapter)
            if config.backend is not None and isinstance(chapter, WorkUnit)
            else (chapter.lean_root / chapter.chapter_path,)
        )
        for relative in paths:
            directory = config.settings.repo / relative
            if not directory.is_dir():
                directory.mkdir(parents=True, exist_ok=True)
                created.append(directory.relative_to(config.settings.repo).as_posix())
    return tuple(created)


def _with_build_command(work_unit: WorkUnitLike, command: str) -> WorkUnitLike:
    if isinstance(work_unit, WorkUnit):
        return replace(
            work_unit,
            target=replace(work_unit._target(), build_command=command),
        )
    return replace(work_unit, build_command=command)


def _scope_digests(
    root: Path,
    by_id: dict[str, WorkUnitLike],
    work_unit_ids: Iterable[str],
) -> dict[str, str]:
    return {work_unit_id: scope_digest(root, by_id[work_unit_id]) for work_unit_id in work_unit_ids}


class Orchestrator:
    def __init__(
        self,
        config: PipelineConfig,
        state: StateStore,
        *,
        work_units: Iterable[WorkUnitLike] | None = None,
        chapters: Iterable[WorkUnitLike] | None = None,
        force: bool = False,
        resume_agents: bool = False,
        control: RunControl | None = None,
    ) -> None:
        self.config = config
        self.state = state
        if work_units is not None and chapters is not None:
            raise ValueError("pass work_units or legacy chapters, not both")
        selected_units = work_units if work_units is not None else chapters
        self.work_units = tuple(selected_units if selected_units is not None else config.work_units)
        self._work_units_by_id = {chapter.id: chapter for chapter in self.work_units}
        self._work_unit_order = {chapter.id: index for index, chapter in enumerate(self.work_units)}
        self._path_owners: dict[str, list[str]] = {}
        self._scope_matchers = {
            chapter.id: ScopeMatcher(chapter.scope) for chapter in self.work_units
        }
        self._source_generations = {chapter.id: 0 for chapter in self.work_units}
        self._module_owners: dict[str, list[str]] = {}
        self._identifier_trie = _IdentifierTrieNode()
        self._diagnostic_owner_cache: dict[LeanDiagnostic, tuple[str, ...]] = {}
        self._diagnostic_owner_cache_lock = Lock()
        self._observed_graph_source: dict[str, Any] | None = None
        self._observed_graph_cache: WorkUnitImportGraph | None = None
        self._interface_graph_key: tuple[int, int, int, int] | None = None
        self._interface_graph_cache: WorkUnitImportGraph | None = None
        self._interface_stale_cache: set[str] = set()
        self._compiled_interface_state: dict[str, Any] | None = None
        self._compiled_interface_imports: object | None = None
        self._compiled_interface_fallback: WorkUnitImportGraph | None = None
        self._compiled_interface_graph: WorkUnitImportGraph | None = None
        self._saved_dependency_state: dict[str, Any] | None = None
        self._saved_dependency_edges: object | None = None
        self._saved_dependency_graph: WorkUnitImportGraph | None = None
        self._build_diagnostic_indexes()
        self.force = force
        self.resume_agents = resume_agents
        self.control = control or RunControl()
        self.executor = CodexExecutor(config, state, resume_agents=resume_agents)
        self.isolation = create_isolation(config.settings)
        self.git = GitCommitter(config.settings.repo)
        self.state.isolation = {
            "configured": config.settings.isolation,
            "backend": self.isolation.name,
            "codex_access": (
                "full" if config.settings.bypass_approvals_and_sandbox else config.settings.sandbox
            ),
            "enforcement": (
                "best-effort" if config.settings.bypass_approvals_and_sandbox else "sandboxed"
            ),
            "lake_cache": (
                "immutable-dependency-and-coordinator-delta-layers"
                if self.isolation.name == "fuse-overlay"
                else "coordinator-owned-shared-worktree"
            ),
        }
        selected_documents = {work_unit.document_id for work_unit in self.work_units}
        self.statement_schedule = build_corpus_schedule(
            config.documents,
            self.work_units,
            phase="statements",
            selected_documents=selected_documents,
        )
        self.proof_schedule = build_corpus_schedule(
            config.documents,
            self.work_units,
            phase="proofs",
            selected_documents=selected_documents,
        )
        self.state.scheduling = self.scheduling_snapshot()
        self.agent_slots = PriorityLimiter(config.settings.max_agents)
        discovery_max_agents = config.stages[Stage.DISCOVER].max_agents
        assert discovery_max_agents is not None
        self.discovery_slots = PriorityLimiter(discovery_max_agents)
        self.build_queue = CoordinatorBuildQueue()
        self._pending_build_requests: list[PendingBuildRequest] = []
        self._build_dispatch_task: asyncio.Task[None] | None = None
        # Snapshot creation and scoped source integration need a short
        # consistency barrier with main-worktree builds. Unlike build_queue,
        # this lock is never held for an overlay agent's editing lifetime or
        # for a proof validation outside a coordinator build.
        self.source_lock = asyncio.Lock()
        self._formalize_graph_lock = asyncio.Lock()
        self._discovery_lock = asyncio.Lock()
        self._pending_discoveries: deque[PendingDiscovery] = deque()
        self._discovery_batch_task: asyncio.Task[None] | None = None
        self._invalidated_reviews: set[str] = set()
        self._proof_rechecks: set[str] = set()
        self._review_invalidation_generations: dict[str, int] = {}
        self._review_generation_lock = asyncio.Lock()
        self._chapter_agent_locks = {chapter.id: asyncio.Lock() for chapter in self.work_units}
        self._upstream_repair_tasks: dict[str, asyncio.Task[None]] = {}
        self._shepherd_task: asyncio.Task[None] | None = None
        self._shepherd_lock = asyncio.Lock()
        self._consecutive_no_progress_sweeps = 0
        self._last_shepherd_case_fingerprints: tuple[str, ...] = ()
        self._repair_progress_generation = 0
        self._repair_slots = asyncio.Semaphore(config.shepherd.max_agents)
        self._live_agent_tasks: dict[
            tuple[str, Stage], tuple[RunRecord, asyncio.Task[AgentResult]]
        ] = {}
        self._live_agent_retry_requests: set[str] = set()

    @property
    def chapters(self) -> tuple[WorkUnitLike, ...]:
        """Compatibility view for callers using the previous domain name."""

        return self.work_units

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(
        self,
        progress: Callable[[str, int, int], None] | None = None,
    ) -> None:
        total = 9

        def report(phase: str, completed: int) -> None:
            if progress is not None:
                progress(phase, completed, total)

        report("Preparing the Lean project", 0)
        if self.config.backend is not None:
            await asyncio.to_thread(
                self.config.backend.prepare_project,
                self.config.settings.repo,
                timeout_seconds=self.config.settings.validation_timeout_seconds,
            )
        report("Loading orchestration state", 1)
        await self.state.load_or_create()
        report("Recovering interrupted work", 2)
        await self.state.requeue_interrupted(resume_agents=self.resume_agents)
        report("Scaffolding work-unit directories", 3)
        self.scaffold()
        report("Recovering upstream requests", 4)
        await self._recover_upstream_requests()
        report("Migrating persisted workflow state", 5)
        await self._migrate_persisted_content_digests()
        migrated = await self.state.migrate_post_review_fixups()
        if migrated:
            # The normal review scheduler reports an invalid import graph.
            with suppress(ValueError):
                await self._invalidate_reviews(
                    migrated,
                    detail="recovered post-review findings",
                )
        report("Preparing agent execution", 6)
        await self.executor.prepare()
        report("Preparing isolated workspaces and Lean caches", 7)
        await self.isolation.prepare()
        report("Checking the Git worktree", 8)
        await self.git.prepare()
        if self.config.shepherd.enabled:
            restart_cases = await self._discard_persisted_repair_plans()
            self.state.shepherd.next_run_at = (
                datetime.now(UTC) + timedelta(seconds=self.config.shepherd.interval_seconds)
            ).isoformat()
            await self.state.save("state")
            self._shepherd_task = asyncio.create_task(
                self._shepherd_loop(restart_cases), name="paf-shepherd"
            )
        report("Preparation complete", 9)

    async def _commit_agent_changes(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        agent: AgentResult | None,
        isolated: IsolationResult,
    ) -> IsolationResult:
        if not isolated.accepted or not isolated.changed_paths:
            return isolated
        summary = (
            agent.report.get("summary", "")
            if agent is not None
            else (
                "Integrated partial scoped changes recovered after the agent was interrupted "
                "before returning its final summary."
            )
        )
        commit = await self.git.commit(
            chapter,
            stage,
            summary=summary if isinstance(summary, str) else "",
            changed_paths=isolated.changed_paths,
        )
        return replace(isolated, commit=commit)

    def _mark_source_changed(self, chapter_ids: Iterable[str]) -> None:
        for chapter_id in chapter_ids:
            self._source_generations[chapter_id] = self._source_generations.get(chapter_id, 0) + 1

    async def _possibly_modified_scope_ids(
        self,
        chapter_ids: Iterable[str],
        baseline: dict[str, int],
    ) -> set[str]:
        """Find scopes that changed internally or have uncommitted external edits."""

        selected = set(chapter_ids)
        changed = {
            chapter_id
            for chapter_id in selected
            if self._source_generations.get(chapter_id, 0) != baseline.get(chapter_id, 0)
        }
        if not self.git.enabled:
            return selected
        dirty_paths = await self.git.working_tree_paths()
        if dirty_paths:
            changed.update(
                chapter_id
                for chapter_id in selected.difference(changed)
                if any(self._scope_matchers[chapter_id].matches(path) for path in dirty_paths)
            )
        return changed

    async def shutdown(self) -> None:
        try:
            if self._shepherd_task is not None:
                self._shepherd_task.cancel()
                await asyncio.gather(self._shepherd_task, return_exceptions=True)
                self._shepherd_task = None
            if self._discovery_batch_task is not None:
                self._discovery_batch_task.cancel()
                await asyncio.gather(self._discovery_batch_task, return_exceptions=True)
                self._discovery_batch_task = None
            for pending in self._pending_discoveries:
                if not pending.future.done():
                    pending.future.cancel()
            self._pending_discoveries.clear()
            repairs = tuple(self._upstream_repair_tasks.values())
            for task in repairs:
                task.cancel()
            await asyncio.gather(*repairs, return_exceptions=True)
            self._upstream_repair_tasks.clear()
            if self._build_dispatch_task is not None:
                self._build_dispatch_task.cancel()
                await asyncio.gather(self._build_dispatch_task, return_exceptions=True)
                self._build_dispatch_task = None
            for request in self._pending_build_requests:
                if not request.future.done():
                    request.future.cancel()
            self._pending_build_requests.clear()
            await self.isolation.close()
        finally:
            await self.state.close()

    def _already_done(self, chapter: WorkUnitLike, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

    def scaffold(self) -> None:
        """Create configured chapter directories without creating Lean files."""

        scaffold_directories(self.config, self.work_units)

    def retry_live_agent(self, chapter_selector: str) -> dict[str, object]:
        """Interrupt and continue the single agent currently executing for one chapter."""

        matches = [unit for unit in self.work_units if unit.id == chapter_selector]
        if not matches and chapter_selector.isdigit():
            matches = [unit for unit in self.work_units if unit.ordinal == int(chapter_selector)]
        if not matches:
            raise ValueError(f"work-unit selector matched nothing: {chapter_selector}")
        if len(matches) > 1:
            raise ValueError(
                f"work-unit selector {chapter_selector!r} is ambiguous; pass a complete id"
            )
        chapter_id = matches[0].id
        live = [
            (stage, active)
            for (active_chapter_id, stage), active in self._live_agent_tasks.items()
            if active_chapter_id == chapter_id and not active[1].done()
        ]
        if not live:
            raise ValueError(f"no live agent for {chapter_id}")
        if len(live) > 1:
            raise ValueError(f"multiple live agents unexpectedly found for {chapter_id}")
        stage, active = live[0]
        run, task = active
        if run.id in self._live_agent_retry_requests:
            raise ValueError(f"retry already requested for live run {run.id}")
        self._live_agent_retry_requests.add(run.id)
        task.cancel()
        return {
            "accepted": True,
            "chapter_id": chapter_id,
            "stage": stage.value,
            "interrupted_run_id": run.id,
        }

    def _observed_work_unit_graph(self) -> WorkUnitImportGraph:
        source = self.state.source_dependency_tree
        if source is self._observed_graph_source and self._observed_graph_cache is not None:
            return self._observed_graph_cache
        nodes = source.get("nodes", {})
        graph = build_source_dependency_graph(
            self.work_units,
            nodes if isinstance(nodes, dict) else {},
        )
        self._observed_graph_source = source
        self._observed_graph_cache = graph
        return graph

    def _source_input_digests(
        self,
        chapters: Iterable[WorkUnitLike],
    ) -> dict[str, str]:
        """Hash source excerpts while reading each shared document only once."""

        source_lines: dict[Path, list[str]] = {}
        digests: dict[str, str] = {}
        for chapter in chapters:
            path = self.config.settings.repo / chapter.source
            lines = source_lines.get(path)
            if lines is None:
                lines = path.read_text(encoding="utf-8").splitlines()
                source_lines[path] = lines
            selected = "\n".join(
                lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line]
            )
            digests[chapter.id] = tagged_digest_text(f"{chapter.id}\0{selected}")
        return digests

    def _source_input_digest(self, chapter: WorkUnitLike) -> str:
        return self._source_input_digests((chapter,))[chapter.id]

    async def _migrate_persisted_content_digests(self) -> None:
        """Replace verified SHA/unversioned cache digests with tagged XXH digests."""

        by_id = {chapter.id: chapter for chapter in self.work_units}

        def migrate() -> tuple[bool, dict[str, str], set[str]]:
            changed = False
            current_discoveries: set[str] = set()
            source_lines: dict[Path, list[str]] = {}
            raw_nodes = self.state.source_dependency_tree.get("nodes", {})
            nodes = raw_nodes if isinstance(raw_nodes, dict) else {}
            for chapter_id, raw_record in nodes.items():
                chapter = by_id.get(chapter_id)
                if chapter is None or not isinstance(raw_record, dict):
                    continue
                stored = raw_record.get("source_digest")
                if not isinstance(stored, str):
                    continue
                path = self.config.settings.repo / chapter.source
                lines = source_lines.get(path)
                if lines is None:
                    lines = path.read_text(encoding="utf-8").splitlines()
                    source_lines[path] = lines
                selected = "\n".join(
                    lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line]
                )
                migrated = migrate_digest_text(stored, f"{chapter.id}\0{selected}")
                if migrated is not None and migrated != stored:
                    raw_record["source_digest"] = migrated
                    changed = True
                    if is_legacy_digest(stored):
                        current_discoveries.add(chapter_id)

            raw_clean = self.state.formalize_graph.get("clean", {})
            clean = raw_clean if isinstance(raw_clean, dict) else {}
            proof_migrations: dict[str, str] = {}
            for chapter_id, chapter in by_id.items():
                raw_record = clean.get(chapter_id)
                record = raw_record if isinstance(raw_record, dict) else None
                proof_digest = self.state.task(chapter_id, Stage.PROVE).source_digest
                stored = {
                    value
                    for value in (
                        record.get("source_digest") if record is not None else None,
                        proof_digest,
                    )
                    if isinstance(value, str)
                }
                migrations = migrate_scope_digests(
                    self.config.settings.repo,
                    chapter,
                    stored,
                )
                if record is not None:
                    old = record.get("source_digest")
                    migrated = migrations.get(old) if isinstance(old, str) else None
                    if migrated is not None and migrated != old:
                        record["source_digest"] = migrated
                        changed = True
                if isinstance(proof_digest, str):
                    migrated = migrations.get(proof_digest)
                    if migrated is not None and migrated != proof_digest:
                        proof_migrations[chapter_id] = migrated
            return changed, proof_migrations, current_discoveries

        changed, proof_migrations, current_discoveries = await asyncio.to_thread(migrate)
        if changed or proof_migrations or current_discoveries:
            await self.state.save_digest_migration(
                proof_migrations,
                current_discoveries=current_discoveries,
            )

    def _discovery_is_current(
        self,
        chapter: WorkUnitLike,
        *,
        source_digest: str | None = None,
    ) -> bool:
        nodes = self.state.source_dependency_tree.get("nodes", {})
        record = nodes.get(chapter.id, {}) if isinstance(nodes, dict) else {}
        return (
            self.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
            and isinstance(record, dict)
            and record.get("source_digest")
            == (source_digest if source_digest is not None else self._source_input_digest(chapter))
        )

    async def _persist_source_dependencies(
        self,
        chapter: WorkUnitLike,
        dependencies: Iterable[str],
        report: dict[str, Any],
    ) -> None:
        """Queue one discovery result for a coalesced graph transaction."""

        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        raw_issues = report.get("issues", ())
        self._pending_discoveries.append(
            PendingDiscovery(
                chapter=chapter,
                dependencies=tuple(sorted(set(dependencies))),
                summary=str(report.get("summary", "")),
                issues=tuple(raw_issues) if isinstance(raw_issues, list) else (),
                future=future,
            )
        )
        if self._discovery_batch_task is None or self._discovery_batch_task.done():
            self._discovery_batch_task = asyncio.create_task(
                self._drain_discovery_batches(), name="paf-discovery-persistence"
            )
        await future

    async def _drain_discovery_batches(self) -> None:
        try:
            while self._pending_discoveries:
                await asyncio.sleep(DISCOVERY_BATCH_SECONDS)
                batch = tuple(
                    self._pending_discoveries.popleft()
                    for _ in range(min(len(self._pending_discoveries), DISCOVERY_BATCH_MAXIMUM))
                )
                try:
                    await self._persist_discovery_batch(batch)
                except asyncio.CancelledError:
                    for pending in batch:
                        if not pending.future.done():
                            pending.future.cancel()
                    raise
                except BaseException as error:
                    for pending in batch:
                        if not pending.future.done():
                            pending.future.set_exception(error)
        finally:
            self._discovery_batch_task = None

    async def _persist_discovery_batch(self, batch: tuple[PendingDiscovery, ...]) -> None:
        """Merge a completion burst and promote every valid task atomically."""

        async with self._discovery_lock:
            previous = self.state.source_dependency_tree
            raw_nodes = previous.get("nodes", {}) if isinstance(previous, dict) else {}
            base_nodes: dict[str, object] = (
                {
                    str(chapter_id): dict(node)
                    for chapter_id, node in raw_nodes.items()
                    if isinstance(chapter_id, str) and isinstance(node, dict)
                }
                if isinstance(raw_nodes, dict)
                else {}
            )
            digests = await asyncio.to_thread(
                lambda: {
                    pending.chapter.id: self._source_input_digest(pending.chapter)
                    for pending in batch
                }
            )
            updates: dict[str, object] = {
                pending.chapter.id: {
                    "dependencies": list(pending.dependencies),
                    "source_digest": digests[pending.chapter.id],
                    "summary": pending.summary,
                    "issues": list(pending.issues),
                }
                for pending in batch
            }
            nodes: dict[str, object] = base_nodes | updates
            valid = list(batch)
            rejected: dict[str, ValueError] = {}
            try:
                graph = build_source_dependency_graph(self.work_units, nodes)
            except ValueError:
                # Cycles are exceptional. Identify only the offending results
                # while preserving batching for the normal, valid path.
                nodes = dict(base_nodes)
                valid = []
                graph = build_source_dependency_graph(self.work_units, nodes)
                for pending in batch:
                    trial = nodes | {pending.chapter.id: updates[pending.chapter.id]}
                    try:
                        candidate = build_source_dependency_graph(self.work_units, trial)
                    except ValueError as error:
                        rejected[pending.chapter.id] = error
                        continue
                    nodes = trial
                    graph = candidate
                    valid.append(pending)
            for chapter_id, node in nodes.items():
                if chapter_id in graph.dependencies and isinstance(node, dict):
                    node["dependencies"] = sorted(graph.dependencies[chapter_id])
            previous_edges = previous.get("edges") if isinstance(previous, dict) else None
            edges = [list(edge) for edge in graph.edges]
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if previous_edges != edges:
                revision += 1
            self.state.source_dependency_tree = {
                "algorithm": "source-discovery",
                "revision": revision,
                "order": list(graph.order),
                "edges": edges,
                "dependencies": {
                    chapter_id: sorted(required)
                    for chapter_id, required in sorted(graph.dependencies.items())
                },
                "nodes": nodes,
            }
            async with self.state.batch():
                await self.state.save("source_dependency_tree")
                await self.state.set_tasks(
                    (pending.chapter.id for pending in valid),
                    Stage.DISCOVER,
                    TaskStatus.SUCCEEDED,
                    "source dependency tree persisted",
                )
            for pending in valid:
                if not pending.future.done():
                    pending.future.set_result(None)
            for pending in batch:
                error = rejected.get(pending.chapter.id)
                if error is not None and not pending.future.done():
                    pending.future.set_exception(error)

    async def _retain_formalize_clean(
        self,
        graph: WorkUnitImportGraph,
        records: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Retain records whose own source is unchanged.

        Dependency freshness is represented separately.  A proof-body change
        upstream must not erase a descendant's locally validated build fact.
        """

        def retain() -> dict[str, dict[str, Any]]:
            by_id = {chapter.id: chapter for chapter in self.work_units}
            retained: dict[str, dict[str, Any]] = {}
            for chapter_id in graph.order:
                record = records.get(chapter_id)
                if not isinstance(record, dict):
                    continue
                source = scope_digest(self.config.settings.repo, by_id[chapter_id])
                if record.get("source_digest") == source:
                    retained[chapter_id] = dict(record) | {
                        "source_digest": source,
                        "build_generation": int(record.get("build_generation", 0)),
                    }
            return retained

        return await asyncio.to_thread(retain)

    @staticmethod
    def _copy_formalize_clean(records: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Copy persisted clean records without re-reading the whole corpus.

        Source edits integrated by the orchestrator explicitly invalidate their owners, while
        every consumer checks its own digest before trusting a record.  Graph transactions can
        therefore preserve unrelated records instead of globbing and hashing every clean work
        unit after each completed build.
        """

        return {
            str(chapter_id): dict(record)
            for chapter_id, record in records.items()
            if isinstance(chapter_id, str) and isinstance(record, dict)
        }

    async def _retained_formalize_record(
        self,
        chapter: WorkUnitLike,
        records: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Validate only the clean record a scheduler decision is about to consume."""

        record = records.get(chapter.id)
        if not isinstance(record, dict):
            return None
        source = await asyncio.to_thread(scope_digest, self.config.settings.repo, chapter)
        if record.get("source_digest") != source:
            return None
        return dict(record) | {
            "source_digest": source,
            "build_generation": int(record.get("build_generation", 0)),
        }

    @staticmethod
    def _dependency_closure(graph: WorkUnitImportGraph, chapter_ids: Iterable[str]) -> set[str]:
        required = set(chapter_ids)
        pending = list(required)
        while pending:
            chapter_id = pending.pop()
            for dependency in graph.dependencies.get(chapter_id, frozenset()):
                if dependency not in required:
                    required.add(dependency)
                    pending.append(dependency)
        return required

    @staticmethod
    def _successor_closure(graph: WorkUnitImportGraph, chapter_ids: Iterable[str]) -> set[str]:
        affected = set(chapter_ids)
        pending = list(affected)
        while pending:
            chapter_id = pending.pop()
            for successor in graph.successors.get(chapter_id, frozenset()):
                if successor not in affected:
                    affected.add(successor)
                    pending.append(successor)
        return affected

    @staticmethod
    def _dependency_frontiers_ready(
        graph: WorkUnitImportGraph,
        completed: set[str],
    ) -> set[str]:
        """Return nodes whose entire transitive prerequisite frontier is complete."""

        ready: set[str] = set()
        for chapter_id in graph.order:
            if all(
                dependency in completed and dependency in ready
                for dependency in graph.dependencies.get(chapter_id, frozenset())
            ):
                ready.add(chapter_id)
        return ready

    async def _publish_validated_builds(
        self,
        snapshots: dict[str, ValidatedBuildSnapshot],
    ) -> bool:
        """Publish only if the exact source graph built is still current."""

        if not snapshots:
            return False
        async with self.source_lock:
            graph = self._observed_work_unit_graph()
            captured: dict[str, str] = {}
            captured_generations: dict[str, int] = {}
            required = self._dependency_closure(graph, snapshots)
            for snapshot in snapshots.values():
                if snapshot.graph.edges != graph.edges:
                    return False
                for chapter_id, digest in snapshot.source_digests.items():
                    existing = captured.setdefault(chapter_id, digest)
                    if existing != digest:
                        return False
                for chapter_id, generation in snapshot.source_generations.items():
                    existing_generation = captured_generations.setdefault(chapter_id, generation)
                    if existing_generation != generation:
                        return False
            if not required.issubset(captured):
                return False
            by_id = self._work_units_by_id
            if required.issubset(captured_generations):
                possibly_modified = await self._possibly_modified_scope_ids(
                    required, captured_generations
                )
            else:
                possibly_modified = required
            current = await asyncio.to_thread(
                _scope_digests,
                self.config.settings.repo,
                by_id,
                possibly_modified,
            )
            if any(captured[chapter_id] != digest for chapter_id, digest in current.items()):
                return False

            persisted = self.state.formalize_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            clean = self._copy_formalize_clean(records)
            raw_interfaces = self.state.formalize_graph.get("interfaces", {})
            interfaces = raw_interfaces if isinstance(raw_interfaces, dict) else {}
            raw_imports = self.state.formalize_graph.get("interface_imports", {})
            compiled_imports = (
                {
                    str(chapter_id): tuple(item for item in dependencies if isinstance(item, str))
                    for chapter_id, dependencies in raw_imports.items()
                    if isinstance(chapter_id, str) and isinstance(dependencies, list)
                }
                if isinstance(raw_imports, dict)
                else {}
            )
            interface_updates = {
                chapter_id: snapshot.fingerprint
                for chapter_id, snapshot in snapshots.items()
                if snapshot.fingerprint is not None
            }
            import_updates = {
                chapter_id: snapshot.import_dependencies
                for chapter_id, snapshot in snapshots.items()
                if snapshot.fingerprint is not None
            }
            imports_changed = any(
                compiled_imports.get(chapter_id) != dependencies
                for chapter_id, dependencies in import_updates.items()
            )
            compiled_invalidation_graph = (
                self._interface_invalidation_graph(graph, compiled_imports | import_updates)
                if imports_changed
                else self._persisted_interface_invalidation_graph(graph, compiled_imports)
            )
            invalidation_graph = compiled_invalidation_graph
            if self.config.settings.interface_invalidation != "interface":
                invalidation_graph = graph
            invalidated: set[str] = set()
            previous_dirty = set(self.state.formalize_graph.get("dirty", ()))
            previous_stale = set(self.state.formalize_graph.get("interface_stale", ()))
            metric_updates: dict[str, int] = {}
            if self.config.settings.interface_invalidation != "observe":
                for chapter_id, snapshot in snapshots.items():
                    old = interfaces.get(chapter_id)
                    new = snapshot.fingerprint
                    old_digest = old.get("interface_digest") if isinstance(old, dict) else None
                    new_digest = new.get("interface_digest") if isinstance(new, dict) else None
                    if old_digest and new_digest:
                        if old_digest != new_digest:
                            changed_successors = self._successor_closure(
                                invalidation_graph, (chapter_id,)
                            ) - set(snapshots)
                            invalidated.update(changed_successors)
                            metric_updates["interface_changing_edits"] = (
                                metric_updates.get("interface_changing_edits", 0) + 1
                            )
                            metric_updates["descendants_queued"] = metric_updates.get(
                                "descendants_queued", 0
                            ) + len(changed_successors)
                        elif chapter_id in previous_dirty and chapter_id not in previous_stale:
                            metric_updates["interface_preserving_edits"] = (
                                metric_updates.get("interface_preserving_edits", 0) + 1
                            )
                    elif old_digest and not new_digest:
                        # Fingerprinting failed after the source had taken the
                        # selective path. Fall back to legacy invalidation.
                        invalidated.update(
                            self._successor_closure(graph, (chapter_id,)) - set(snapshots)
                        )
                        metric_updates["fingerprint_failures"] = (
                            metric_updates.get("fingerprint_failures", 0) + 1
                        )
            for chapter_id in invalidated:
                clean.pop(chapter_id, None)
            build_generation = int(self.state.formalize_graph.get("build_generation", 0))
            for chapter_id in graph.order:
                if chapter_id not in required:
                    continue
                source = captured[chapter_id]
                record = clean.get(chapter_id)
                if isinstance(record, dict) and record.get("source_digest") == source:
                    if fingerprint := interface_updates.get(chapter_id):
                        record.update(fingerprint)
                    continue
                build_generation += 1
                clean[chapter_id] = {
                    "source_digest": source,
                    "build_generation": build_generation,
                } | interface_updates.get(chapter_id, {})
            fingerprint_errors = tuple(
                snapshot.fingerprint_error
                for snapshot in snapshots.values()
                if snapshot.fingerprint_error
            )
            automatically_rechecked = len(previous_stale & required)
            if automatically_rechecked:
                metric_updates["automatic_successful_rechecks"] = automatically_rechecked
            await self._save_formalize_graph(
                graph,
                clean,
                build_generation=build_generation,
                invalidated=invalidated,
                validated=required,
                interface_updates=interface_updates,
                import_updates=import_updates,
                fingerprint_error="\n\n".join(fingerprint_errors)[-4000:],
                interface_invalidated=invalidated,
                interface_validated=required,
                metric_updates=metric_updates,
            )
            return True

    async def _publish_validated_build(
        self,
        chapter: WorkUnitLike,
        snapshot: ValidatedBuildSnapshot,
    ) -> bool:
        return await self._publish_validated_builds({chapter.id: snapshot})

    @staticmethod
    def _invalidate_formalize_descendants(
        graph: WorkUnitImportGraph,
        clean: dict[str, dict[str, Any]],
        chapter_ids: Iterable[str],
    ) -> set[str]:
        invalidated = Orchestrator._successor_closure(graph, chapter_ids)
        for chapter_id in invalidated:
            clean.pop(chapter_id, None)
        return invalidated

    async def _invalidate_build_records(self, chapter_ids: Iterable[str]) -> set[str]:
        """Mark edited sources stale while retaining known downstream interfaces."""

        graph = self._observed_work_unit_graph()
        targets = set(chapter_ids)
        persisted = self.state.formalize_graph.get("clean", {})
        clean = self._copy_formalize_clean(persisted if isinstance(persisted, dict) else {})
        raw_interfaces = self.state.formalize_graph.get("interfaces", {})
        interfaces = raw_interfaces if isinstance(raw_interfaces, dict) else {}
        known = all(
            isinstance(interfaces.get(chapter_id, persisted.get(chapter_id)), dict)
            and bool(
                interfaces.get(chapter_id, persisted.get(chapter_id, {})).get("interface_digest")
            )
            for chapter_id in targets
        )
        if self.config.settings.interface_invalidation == "observe" or not known:
            invalidated = self._invalidate_formalize_descendants(graph, clean, targets)
        else:
            invalidated = targets
            for chapter_id in targets:
                clean.pop(chapter_id, None)
        await self._save_formalize_graph(
            graph,
            clean,
            build_generation=int(self.state.formalize_graph.get("build_generation", 0)),
            invalidated=invalidated,
            metric_updates=(
                {"unknown_interface_fallbacks": len(targets)}
                if self.config.settings.interface_invalidation != "observe" and not known
                else None
            ),
        )
        return invalidated

    async def _save_formalize_graph(
        self,
        graph: WorkUnitImportGraph,
        clean: dict[str, dict[str, Any]],
        *,
        build_generation: int,
        invalidated: Iterable[str] = (),
        validated: Iterable[str] = (),
        interface_updates: dict[str, dict[str, Any]] | None = None,
        import_updates: dict[str, tuple[str, ...]] | None = None,
        fingerprint_error: str = "",
        interface_invalidated: Iterable[str] = (),
        interface_validated: Iterable[str] = (),
        metric_updates: dict[str, int] | None = None,
    ) -> int:
        async with self._formalize_graph_lock:
            explicitly_invalidated = set(invalidated)
            explicitly_validated = set(validated)
            previous = self.state.formalize_graph
            graph_unchanged = (
                previous is self._saved_dependency_state
                and previous.get("edges") is self._saved_dependency_edges
                and graph is self._saved_dependency_graph
            )
            if graph_unchanged:
                dependency_snapshot = {
                    key: previous[key]
                    for key in ("algorithm", "order", "edges", "dependencies")
                    if key in previous
                }
            else:
                dependency_snapshot = graph.snapshot()
                previous_edges = previous.get("edges") if isinstance(previous, dict) else None
                graph_unchanged = (
                    previous.get("algorithm") == "source-dependency-tree"
                    and previous_edges == dependency_snapshot["edges"]
                )
            current = previous.get("clean", {}) if isinstance(previous, dict) else {}
            if isinstance(current, dict):
                for chapter_id, record in current.items():
                    if not isinstance(record, dict):
                        continue
                    if chapter_id in explicitly_invalidated:
                        continue
                    local = clean.get(chapter_id)
                    local_generation = (
                        int(local.get("build_generation", 0)) if isinstance(local, dict) else -1
                    )
                    # Merge concurrently published exact builds without
                    # resurrecting records this caller explicitly invalidated.
                    if int(record.get("build_generation", 0)) >= local_generation:
                        clean[chapter_id] = dict(record)
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if not graph_unchanged:
                revision += 1
            build_generation = max(
                build_generation,
                int(previous.get("build_generation", 0)) if isinstance(previous, dict) else 0,
            )
            dirty = set(previous.get("dirty", ())) if isinstance(previous, dict) else set()
            dirty.update(explicitly_invalidated)
            dirty.difference_update(explicitly_validated)
            raw_interfaces = previous.get("interfaces", {})
            interfaces = (
                {
                    str(chapter_id): dict(record)
                    for chapter_id, record in raw_interfaces.items()
                    if isinstance(chapter_id, str) and isinstance(record, dict)
                }
                if isinstance(raw_interfaces, dict)
                else {}
            )
            interfaces.update(interface_updates or {})
            for chapter_id, record in clean.items():
                if record.get("interface_digest") and chapter_id not in interfaces:
                    interfaces[chapter_id] = {
                        key: value
                        for key, value in record.items()
                        if key
                        in {
                            "artifact_digest",
                            "interface_digest",
                            "fingerprint_schema",
                            "lean_version",
                            "modules",
                        }
                    }
            raw_imports = previous.get("interface_imports", {})
            interface_imports = (
                {
                    str(chapter_id): tuple(item for item in required if isinstance(item, str))
                    for chapter_id, required in raw_imports.items()
                    if isinstance(chapter_id, str) and isinstance(required, list)
                }
                if isinstance(raw_imports, dict)
                else {}
            )
            imports_changed = any(
                interface_imports.get(chapter_id) != dependencies
                for chapter_id, dependencies in (import_updates or {}).items()
            )
            interface_imports.update(import_updates or {})
            invalidation_graph = (
                self._interface_invalidation_graph(graph, interface_imports)
                if imports_changed
                else self._persisted_interface_invalidation_graph(graph, interface_imports)
            )
            interface_stale = set(previous.get("interface_stale", ()))
            interface_stale.update(interface_invalidated)
            interface_stale.difference_update(interface_validated)
            raw_metrics = previous.get("fingerprint_metrics", {})
            fingerprint_metrics = (
                {
                    str(name): int(value)
                    for name, value in raw_metrics.items()
                    if isinstance(name, str) and isinstance(value, int)
                }
                if isinstance(raw_metrics, dict)
                else {}
            )
            for name, increment in (metric_updates or {}).items():
                fingerprint_metrics[name] = fingerprint_metrics.get(name, 0) + increment
            previous_interface_graph = previous.get("interface_import_graph")
            interface_graph_snapshot = (
                previous_interface_graph
                if graph_unchanged
                and not imports_changed
                and isinstance(previous_interface_graph, dict)
                else invalidation_graph.snapshot() | {"coverage": len(interface_imports)}
            )
            self.state.formalize_graph = dependency_snapshot | {
                "algorithm": "source-dependency-tree",
                "revision": revision,
                "build_generation": build_generation,
                "clean": clean,
                "dirty": sorted(dirty),
                "interfaces": interfaces,
                "interface_imports": {
                    chapter_id: list(required)
                    for chapter_id, required in sorted(interface_imports.items())
                },
                "interface_import_graph": interface_graph_snapshot,
                "fingerprint_schema": "olean-proof-erased-v1",
                "fingerprint_mode": self.config.settings.interface_invalidation,
                "last_fingerprint_error": fingerprint_error,
                "interface_stale": sorted(interface_stale),
                "fingerprint_metrics": fingerprint_metrics,
            }
            self._saved_dependency_state = self.state.formalize_graph
            self._saved_dependency_edges = self.state.formalize_graph.get("edges")
            self._saved_dependency_graph = graph
            self._compiled_interface_state = self.state.formalize_graph
            self._compiled_interface_imports = self.state.formalize_graph.get("interface_imports")
            self._compiled_interface_fallback = graph
            self._compiled_interface_graph = invalidation_graph
            # Parallel builds often publish in bursts.  The scheduler consumes this graph from
            # memory, so coalesce its expensive normalized database projection without delaying
            # downstream scheduling; StateStore.close still provides a durability barrier.
            self.state.save_deferred("formalize_graph")
            return revision

    def _persisted_interface_invalidation_graph(
        self,
        fallback: WorkUnitImportGraph,
        compiled: dict[str, tuple[str, ...]],
    ) -> WorkUnitImportGraph:
        if (
            self.state.formalize_graph is self._compiled_interface_state
            and self.state.formalize_graph.get("interface_imports")
            is self._compiled_interface_imports
            and fallback is self._compiled_interface_fallback
            and self._compiled_interface_graph is not None
        ):
            return self._compiled_interface_graph
        return self._interface_invalidation_graph(fallback, compiled)

    def _interface_invalidation_graph(
        self,
        fallback: WorkUnitImportGraph,
        compiled: dict[str, tuple[str, ...]],
    ) -> WorkUnitImportGraph:
        dependencies = {
            chapter.id: set(compiled.get(chapter.id, fallback.dependencies[chapter.id]))
            for chapter in self.work_units
        }
        try:
            return build_compiled_import_graph(self.work_units, dependencies)
        except ValueError:
            # Partial migration can temporarily combine two individually valid
            # graph revisions into a work-unit cycle. Stay conservative until
            # the remaining compiled records replace the fallback edges.
            return fallback

    def _interface_dependencies_are_current(
        self,
        graph: WorkUnitImportGraph,
        chapter_id: str,
    ) -> bool:
        stale_value = self.state.formalize_graph.get("interface_stale", ())
        stale = set(stale_value) if isinstance(stale_value, list) else set()
        if not stale:
            return True
        raw_imports = self.state.formalize_graph.get("interface_imports", {})
        cache_key = (
            id(self.state.formalize_graph),
            id(raw_imports),
            id(stale_value),
            id(graph),
        )
        if cache_key == self._interface_graph_key and self._interface_graph_cache is not None:
            return chapter_id not in self._interface_stale_cache
        compiled = (
            {
                str(owner): tuple(item for item in dependencies if isinstance(item, str))
                for owner, dependencies in raw_imports.items()
                if isinstance(owner, str) and isinstance(dependencies, list)
            }
            if isinstance(raw_imports, dict)
            else {}
        )
        interface_graph = self._persisted_interface_invalidation_graph(graph, compiled)
        self._interface_graph_key = cache_key
        self._interface_graph_cache = interface_graph
        self._interface_stale_cache = self._successor_closure(interface_graph, stale)
        return chapter_id not in self._interface_stale_cache

    async def _scope_exists(self, chapter: WorkUnitLike) -> bool:
        return await asyncio.to_thread(
            ScopeMatcher(chapter.scope).has_match_for_each_pattern,
            self.config.settings.repo,
        )

    async def _proof_build_is_fresh(self, chapter: WorkUnitLike) -> bool:
        """Whether the current chapter source belongs to a retained clean build."""

        graph = self._observed_work_unit_graph()
        persisted = self.state.formalize_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        record = await self._retained_formalize_record(chapter, records)
        return record is not None and self._interface_dependencies_are_current(graph, chapter.id)

    async def _integrate_interrupted_workspace(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        workspace: Any,
        *,
        source_lock_held: bool,
        collected: IsolationResult | None = None,
    ) -> dict[str, object]:
        """Best-effort import of a stable workspace after a requested stop."""

        acquired = False
        try:
            if not source_lock_held:
                await self.source_lock.acquire()
                acquired = True
            isolated = collected or await workspace.collect(chapter, integration_lock=None)
            isolated = await self._commit_agent_changes(chapter, stage, None, isolated)
            if isolated.accepted and isolated.changed_paths:
                self._mark_source_changed((chapter.id,))
        except BaseException as error:
            detail = str(error) or type(error).__name__
            return {
                "accepted": False,
                "interrupted": True,
                "error": f"best-effort stop integration failed: {detail}",
            }
        finally:
            if acquired:
                self.source_lock.release()

        payload = isolated.as_dict()
        payload["interrupted"] = True
        if isolated.accepted and isolated.changed_paths and stage is not Stage.DISCOVER:
            try:
                invalidated_builds = await self._invalidate_build_records((chapter.id,))
                if stage in (Stage.FORMALIZE, Stage.REVIEW):
                    self._proof_rechecks.update(invalidated_builds)
            except BaseException as error:
                detail = str(error) or type(error).__name__
                payload["warning"] = f"changes integrated but invalidation failed: {detail}"
        return payload

    async def _attempt(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        *,
        feedback: str = "",
        queue_detail: str = "",
        role: str = "",
        request_ids: Iterable[str] = (),
        upstream_requests: Iterable[dict[str, Any]] = (),
        proof_targets: Iterable[ProofTarget] = (),
        priority_override: float | None = None,
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> Attempt:
        auxiliary = role in {UPSTREAM_REPAIR_ROLE, REPAIR_WORKER_ROLE}
        upstream_repair = role == UPSTREAM_REPAIR_ROLE
        selected_request_ids = tuple(dict.fromkeys(request_ids))
        selected_upstream_requests = tuple(upstream_requests)
        selected_proof_targets = tuple(proof_targets)
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.DISCOVER, Stage.FORMALIZE, Stage.REVIEW)
            else self.proof_schedule
        )
        chapter_lock = self._chapter_agent_locks[chapter.id]
        await chapter_lock.acquire()
        chapter_lock_held = True
        slot_held = False
        slots = (
            self.discovery_slots
            if stage is Stage.DISCOVER and role != UPSTREAM_REPAIR_ROLE
            else self.agent_slots
        )
        try:
            await self.control.checkpoint()
            if not auxiliary:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    queue_detail or f"queued for {stage.value} agent",
                    queued=True,
                )
            await slots.acquire(
                priority_override
                if priority_override is not None
                else schedule.priority(chapter.document_id)
            )
            slot_held = True
        except BaseException:
            chapter_lock.release()
            task = self.state.task(chapter.id, stage)
            if not auxiliary and task.queued:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    "agent start interrupted while queued",
                )
            raise
        run = None
        workspace = None
        source_held = False
        isolated: IsolationResult | None = None
        live_discovery = not auxiliary and stage is Stage.DISCOVER

        async def start_agent_run() -> RunRecord:
            if auxiliary:
                started = await self.state.start_auxiliary_run(
                    chapter.id,
                    stage,
                    role=role,
                    request_ids=selected_request_ids,
                    model=(
                        self.config.shepherd.worker_model if role == REPAIR_WORKER_ROLE else None
                    ),
                )
            else:
                started = await self.state.start_run(chapter.id, stage)
                run_updates: dict[str, Any] = {
                    "prompt_kind": report_schema_key(stage, role=role, feedback=feedback),
                }
                if role:
                    run_updates["role"] = role
                if selected_request_ids:
                    run_updates["request_ids"] = list(selected_request_ids)
                if selected_proof_targets:
                    run_updates["proof_targets"] = [
                        target.as_dict() for target in selected_proof_targets
                    ]
                await self.state.update_run(started, **run_updates)
            return started

        try:
            run = await start_agent_run()
            if role == REPAIR_WORKER_ROLE and selected_request_ids:
                await self.state.link_repair_work_unit_run(selected_request_ids[0], run.id)
            if live_discovery:
                workspace_root = self.config.settings.repo
            elif self.isolation.name == "shared":
                await self.source_lock.acquire()
                source_held = True
                await self.git.ensure_clean(chapter)
                workspace = await self.isolation.acquire(run.id)
                snapshot = getattr(workspace, "snapshot", None)
                if snapshot is not None:
                    await snapshot(chapter)
                workspace_root = workspace.root
            else:
                async with self.source_lock:
                    await self.git.ensure_clean(chapter)
                workspace = await self.isolation.acquire(run.id)
                workspace_root = workspace.root
            while True:
                if resume_thread_id is not None:
                    operation = self.executor.resume(
                        chapter,
                        stage,
                        run,
                        thread_id=resume_thread_id,
                        previous_run_id=resume_run_id,
                        reminder=resume_prompt,
                        feedback=feedback,
                        workspace_root=workspace_root,
                    )
                elif upstream_repair:
                    operation = self.executor.run_upstream_repair(
                        chapter,
                        run,
                        selected_upstream_requests,
                        workspace_root=workspace_root,
                    )
                else:
                    operation = self.executor.run(
                        chapter,
                        stage,
                        run,
                        feedback=feedback,
                        workspace_root=workspace_root,
                    )
                agent_task = asyncio.create_task(operation)
                active_run = run
                live_key = (chapter.id, stage)
                self._live_agent_tasks[live_key] = (active_run, agent_task)
                try:
                    agent = await agent_task
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    targeted_retry = active_run.id in self._live_agent_retry_requests
                    if (
                        not targeted_retry
                        or self.control.stopping
                        or (current is not None and current.cancelling())
                    ):
                        self._live_agent_retry_requests.discard(active_run.id)
                        raise
                    self._live_agent_retry_requests.discard(active_run.id)
                    previous_run = active_run
                    if previous_run.status == TaskStatus.RUNNING:
                        await self.state.finish_run(
                            previous_run,
                            status=TaskStatus.INTERRUPTED,
                            thread_id=previous_run.thread_id,
                        )
                    run = await start_agent_run()
                    resume_thread_id = previous_run.thread_id
                    resume_run_id = previous_run.id
                    resume_prompt = LIVE_AGENT_RETRY_PROMPT
                    continue
                finally:
                    if self._live_agent_tasks.get(live_key) == (active_run, agent_task):
                        self._live_agent_tasks.pop(live_key, None)
                break
            slots.release()
            slot_held = False
            # Agent capacity covers live Codex processes, not integration or a
            # coordinator build queued after they exit.
            if not auxiliary:
                await self.state.set_task_phase(
                    chapter.id,
                    stage,
                    TaskPhase.POSTPROCESS,
                    f"postprocessing completed {stage.value} agent result",
                )
            if live_discovery:
                isolated = IsolationResult(accepted=True, generation=0)
            else:
                if not source_held:
                    await self.source_lock.acquire()
                    source_held = True
                assert workspace is not None
                isolated = await workspace.collect(chapter, integration_lock=None)
                isolated = await self._commit_agent_changes(chapter, stage, agent, isolated)
                if isolated.accepted and isolated.changed_paths:
                    self._mark_source_changed((chapter.id,))
                await workspace.close()
                workspace = None
                if source_held:
                    self.source_lock.release()
                    source_held = False
            if (
                isolated.accepted
                and agent.changed
                and stage
                in (
                    Stage.FORMALIZE,
                    Stage.REVIEW,
                    Stage.PROVE,
                )
            ):
                invalidated_builds = await self._invalidate_build_records((chapter.id,))
                if stage is Stage.REVIEW or auxiliary:
                    self._proof_rechecks.update(invalidated_builds)
            if isolated.accepted:
                if role == REPAIR_WORKER_ROLE:
                    validation = ValidationResult(
                        True,
                        0,
                        "validation deferred to the normal stage scheduler",
                        status=ValidationStatus.DEFERRED,
                    )
                elif stage is Stage.PROVE and (agent.changed or self.force):
                    snapshots: dict[str, ValidatedBuildSnapshot] = {}
                    validation = (
                        await self._build_chapters(
                            (chapter,),
                            publish_if_clean=True,
                            mode=(
                                "upstream-repair-certification"
                                if upstream_repair
                                else "shepherd-repair-certification"
                                if role == REPAIR_WORKER_ROLE
                                else "proof-certification"
                            ),
                            stage=Stage.PROVE,
                            snapshots=snapshots,
                        )
                    )[chapter.id]
                    if validation.succeeded and not await self._publish_validated_build(
                        chapter, snapshots[chapter.id]
                    ):
                        validation = ValidationResult(
                            False,
                            1,
                            "Source scope changed after the coordinator build; retry required.",
                            status=ValidationStatus.STALE_SNAPSHOT,
                        )
                elif stage is Stage.PROVE:
                    build_fresh = await self._proof_build_is_fresh(chapter)
                    validation = ValidationResult(
                        build_fresh,
                        0 if build_fresh else 1,
                        (
                            "unchanged proof source reused the incoming clean build"
                            if build_fresh
                            else "unchanged proof source has no clean coordinator build"
                        ),
                    )
                else:
                    validation = ValidationResult(
                        True,
                        0,
                        "validation deferred to the coordinator formalize loop",
                        status=ValidationStatus.DEFERRED,
                    )
            else:
                validation = ValidationResult(
                    False,
                    1,
                    f"Isolation rejected the agent result: {isolated.error}",
                )
            if not isolated.accepted:
                detail = isolated.error
                if isolated.out_of_scope_paths:
                    detail += ": " + ", ".join(isolated.out_of_scope_paths)
                agent = replace(agent, succeeded=False, error=detail)
                validation = ValidationResult(
                    False,
                    1,
                    f"Isolation rejected the agent result: {detail}\n\n{validation.output}",
                )
                await self.state.update_run(run, status=TaskStatus.FAILED)
            await self.state.update_run(
                run,
                isolation=isolated.as_dict(),
                validation=validation.as_dict(),
            )
        except BaseException as error:
            interrupted_isolation: dict[str, object] | None = None
            if (
                isinstance(error, asyncio.CancelledError)
                and self.control.integrate_interrupted_workspaces
                and workspace is not None
            ):
                interrupted_isolation = await self._integrate_interrupted_workspace(
                    chapter,
                    stage,
                    workspace,
                    source_lock_held=source_held,
                    collected=isolated,
                )
                await workspace.close()
                workspace = None
                if source_held:
                    self.source_lock.release()
                    source_held = False
            if run is not None:
                detail = str(error) or type(error).__name__
                failure_isolation = interrupted_isolation
                if failure_isolation is None and isolated is not None:
                    failure_isolation = isolated.as_dict()
                    failure_isolation["error"] = (
                        f"changes integrated but orchestration failed before completion: {detail}"
                    )
                await self.state.finish_run(
                    run,
                    status=(
                        TaskStatus.INTERRUPTED
                        if isinstance(error, asyncio.CancelledError)
                        else TaskStatus.FAILED
                    ),
                    isolation=failure_isolation
                    or {
                        "accepted": False,
                        "error": f"orchestration failed before completion: {detail}",
                    },
                )
            raise
        finally:
            if slot_held:
                slots.release()
            if workspace is not None:
                await workspace.close()
            if source_held:
                self.source_lock.release()
            if chapter_lock_held:
                chapter_lock.release()
            task = self.state.task(chapter.id, stage)
            if not auxiliary and task.queued:
                await self.state.set_task(
                    chapter.id,
                    stage,
                    TaskStatus.PENDING,
                    "agent start interrupted while queued",
                )
        assert run is not None
        return Attempt(agent=agent, validation=validation, run=run)

    async def _discover(self, chapter: WorkUnitLike, *, rerun: bool = False) -> StageOutcome:
        if not rerun and not self.force and self._discovery_is_current(chapter):
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        attempt = await self._attempt(
            chapter,
            Stage.DISCOVER,
            queue_detail="reading source and discovering direct dependencies",
        )
        complete = bool(attempt.agent.report.get("complete"))
        raw_dependencies = attempt.agent.report.get("source_dependencies", ())
        dependencies = (
            tuple(dict.fromkeys(item for item in raw_dependencies if isinstance(item, str)))
            if isinstance(raw_dependencies, list)
            else ()
        )
        configured = tuple(chapter.depends_on)
        dependencies = tuple(dict.fromkeys((*configured, *dependencies)))
        known = {item.id for item in self.work_units}
        invalid = set(dependencies).difference(known)
        if chapter.id in dependencies:
            invalid.add(chapter.id)
        if attempt.agent.changed:
            await self.state.set_task(
                chapter.id,
                Stage.DISCOVER,
                TaskStatus.FAILED,
                "discovery must be read-only",
            )
            return StageOutcome(ExecutionDisposition.FAILED)
        if attempt.agent.succeeded and attempt.validation.succeeded and complete and not invalid:
            try:
                await self._persist_source_dependencies(chapter, dependencies, attempt.agent.report)
            except ValueError as error:
                await self.state.set_task(
                    chapter.id,
                    Stage.DISCOVER,
                    TaskStatus.FAILED,
                    str(error),
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        await self.state.set_task(
            chapter.id,
            Stage.DISCOVER,
            TaskStatus.FAILED,
            (
                "discovery reported unknown dependency ids: " + ", ".join(sorted(invalid))
                if invalid
                else "source dependency discovery was incomplete"
            ),
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    async def _formalize(self, chapter: WorkUnitLike, *, rerun: bool = False) -> StageOutcome:
        if self._already_done(chapter, Stage.FORMALIZE):
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        maximum = self.config.stages[Stage.FORMALIZE].max_rounds
        feedback = ""
        last_attempt: Attempt | None = None
        for iteration in range(1, maximum + 1):
            if await self._scope_exists(chapter):
                snapshots: dict[str, ValidatedBuildSnapshot] = {}
                validation = (
                    await self._build_chapters(
                        (chapter,),
                        publish_if_clean=True,
                        mode="source-tree-formalize",
                        iteration=iteration,
                        maximum_iterations=maximum,
                        stage=Stage.FORMALIZE,
                        snapshots=snapshots,
                    )
                )[chapter.id]
                if (
                    last_attempt is not None
                    and (previous_run := getattr(last_attempt, "run", None)) is not None
                ):
                    await self.state.update_run(previous_run, validation=validation.as_dict())
                if validation.succeeded and await self._publish_validated_build(
                    chapter, snapshots[chapter.id]
                ):
                    await self.state.set_task(
                        chapter.id,
                        Stage.FORMALIZE,
                        TaskStatus.SUCCEEDED,
                        "clean diagnostics and coordinator build in source dependency order",
                    )
                    return StageOutcome(ExecutionDisposition.SUCCEEDED)
                if validation.status is ValidationStatus.UPSTREAM_FAILED:
                    return await self._block_on_upstream_diagnostics(chapter, validation)
                if validation.blocked_by:
                    await self._block_on_upstream_diagnostics(
                        chapter, validation, block_consumer=False
                    )
                diagnostics = await self._build_feedback_async({chapter.id: validation})
                feedback = diagnostics.actionable.get(chapter.id, validation.output)

            attempt = await self._attempt(
                chapter,
                Stage.FORMALIZE,
                feedback=feedback,
                queue_detail=f"dependency-ready formalization pass {iteration}/{maximum}",
            )
            last_attempt = attempt
            complete = bool(attempt.agent.report.get("complete"))
            if attempt.agent.capacity_exhausted:
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            if not attempt.agent.succeeded or not attempt.validation.succeeded:
                feedback = attempt.feedback()
                continue
            if not complete:
                feedback = attempt.feedback()
                continue

        if await self._scope_exists(chapter):
            snapshots = {}
            validation = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="source-tree-formalize-final",
                    iteration=maximum,
                    maximum_iterations=maximum,
                    stage=Stage.FORMALIZE,
                    snapshots=snapshots,
                )
            )[chapter.id]
            if (
                last_attempt is not None
                and (previous_run := getattr(last_attempt, "run", None)) is not None
            ):
                await self.state.update_run(previous_run, validation=validation.as_dict())
            if validation.succeeded and await self._publish_validated_build(
                chapter, snapshots[chapter.id]
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.SUCCEEDED,
                    "clean diagnostics and coordinator build in source dependency order",
                )
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            if validation.status is ValidationStatus.UPSTREAM_FAILED:
                return await self._block_on_upstream_diagnostics(chapter, validation)
            if validation.blocked_by:
                await self._block_on_upstream_diagnostics(chapter, validation, block_consumer=False)

        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            f"formalization did not reach clean diagnostics in {maximum} attempts",
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    async def _block_on_upstream_diagnostics(
        self,
        consumer: WorkUnitLike,
        validation: ValidationResult,
        *,
        block_consumer: bool = True,
    ) -> StageOutcome:
        """Route a coordinator failure to its owners without spending a consumer round."""

        owners = tuple(
            owner_id
            for owner_id in dict.fromkeys(validation.blocked_by)
            if owner_id in self._work_units_by_id and owner_id != consumer.id
        )
        if not owners:
            return StageOutcome(ExecutionDisposition.FAILED)
        await self._invalidate_build_records(owners)
        formalize_owners = tuple(
            owner_id for owner_id in owners if not self.state.later_stage_started(owner_id)
        )
        review_owners = tuple(
            owner_id for owner_id in owners if self.state.later_stage_started(owner_id)
        )
        if review_owners:
            routed = (await self._build_feedback_async({consumer.id: validation})).actionable
            feedback = {
                owner_id: routed.get(
                    owner_id,
                    (
                        "A downstream coordinator build found a diagnostic owned by this "
                        f"chapter while formalizing {consumer.id}:\n{validation.output[-12000:]}"
                    ),
                )
                for owner_id in review_owners
            }
            digest = hashlib.sha256(validation.output.encode()).hexdigest()[:12]
            await self._queue_review_feedback(
                feedback,
                origin=f"formalize-upstream:{consumer.id}:{digest}",
            )
        requirements = tuple(
            Requirement(
                RequirementKind.COORDINATOR_OWNER,
                owner_task_key=self.state.key(
                    owner_id,
                    Stage.REVIEW if owner_id in review_owners else Stage.FORMALIZE,
                ),
                detail=f"coordinator diagnostic owned by {owner_id}",
            )
            for owner_id in owners
        )
        async with self.state.batch():
            for owner_id in formalize_owners:
                owner_task = self.state.task(owner_id, Stage.FORMALIZE)
                if owner_task.status == TaskStatus.RUNNING:
                    continue
                owner_task.recovering_failure = True
                await self.state.set_task(
                    owner_id,
                    Stage.FORMALIZE,
                    TaskStatus.PENDING,
                    f"requeued as owner of coordinator diagnostics blocking {consumer.id}",
                )
            if block_consumer:
                await self.state.set_task_waiting(
                    consumer.id,
                    Stage.FORMALIZE,
                    requirements,
                    "waiting for upstream coordinator diagnostic owners",
                )
        return StageOutcome(
            ExecutionDisposition.WAITING,
            requirements,
        )

    def _chapter_identifiers(self, chapter: WorkUnitLike) -> tuple[str, ...]:
        root = (chapter.lean_root / chapter.chapter_path).as_posix()
        lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
        without_project = root.removeprefix(lean_prefix)
        return tuple(
            dict.fromkeys(
                (
                    root,
                    without_project,
                    chapter.chapter_module,
                    chapter.chapter_module.replace(".", "/"),
                )
            )
        )

    def _build_diagnostic_indexes(self) -> None:
        """Index immutable work-unit names used to route coordinator diagnostics."""

        lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
        for chapter in self.work_units:
            root = (chapter.lean_root / chapter.chapter_path).as_posix()
            for path_root in dict.fromkeys((root, root.removeprefix(lean_prefix))):
                self._path_owners.setdefault(path_root, []).append(chapter.id)
            self._module_owners.setdefault(chapter.chapter_module, []).append(chapter.id)
            for identifier in self._chapter_identifiers(chapter):
                node = self._identifier_trie
                for character in identifier:
                    node = node.children.setdefault(character, _IdentifierTrieNode())
                if chapter.id not in node.owner_ids:
                    node.owner_ids += (chapter.id,)

    def _ordered_owner_ids(self, owner_ids: Iterable[str]) -> tuple[str, ...]:
        return tuple(sorted(set(owner_ids), key=self._work_unit_order.__getitem__))

    def _path_owner_ids(self, path: str) -> tuple[str, ...]:
        normalized = path.replace("\\", "/")
        repo_prefix = self.config.settings.repo.as_posix().rstrip("/") + "/"
        normalized = normalized.removeprefix(repo_prefix).removeprefix("./")
        owners: list[str] = []
        if normalized.endswith(".lean"):
            owners.extend(self._path_owners.get(normalized.removesuffix(".lean"), ()))
        parent = normalized
        while "/" in parent:
            parent = parent.rsplit("/", 1)[0]
            owners.extend(self._path_owners.get(parent, ()))
        return self._ordered_owner_ids(owners)

    def _is_earlier_work_unit(self, owner: WorkUnitLike, consumer: WorkUnitLike) -> bool:
        if owner.document_id == consumer.document_id:
            return owner.number < consumer.number
        document_order = {
            document.id: index for index, document in enumerate(self.config.documents)
        }
        return document_order.get(owner.document_id, -1) < document_order.get(
            consumer.document_id, -1
        )

    def _normalize_upstream_request(
        self,
        consumer: WorkUnitLike,
        raw: Any,
    ) -> tuple[dict[str, Any], str, str]:
        """Resolve one owner from exact paths and retain why an unsafe handoff was rejected."""

        if not isinstance(raw, dict):
            return {}, "", "proof agent returned a non-object upstream request"
        request = dict(raw)
        required_strings = (
            "blocked_declaration",
            "consumer_path",
            "residual_goal",
            "needed_result",
            "owner_chapter_id",
        )
        missing = [
            name
            for name in required_strings
            if not isinstance(request.get(name), str) or not request[name].strip()
        ]
        owner_paths = request.get("owner_paths")
        alternatives = request.get("attempted_alternatives")
        if not isinstance(owner_paths, list) or not all(
            isinstance(path, str) and path.strip() for path in owner_paths
        ):
            missing.append("owner_paths")
            owner_paths = []
        valid_alternatives = (
            [item for item in alternatives if isinstance(item, str) and item.strip()]
            if isinstance(alternatives, list)
            else []
        )
        if len(valid_alternatives) < 2:
            missing.append("attempted_alternatives")
        proposed_owner = str(request.get("owner_chapter_id", "")).strip()
        if missing:
            return (
                request,
                proposed_owner,
                (
                    "upstream request is missing required evidence: "
                    + ", ".join(sorted(set(missing)))
                ),
            )

        consumer_path = str(request["consumer_path"]).strip()
        if consumer.id not in self._path_owner_ids(consumer_path):
            return (
                request,
                proposed_owner,
                (f"consumer path `{consumer_path}` is not owned by {consumer.id}"),
            )
        path_owners = {
            owner_id for path in owner_paths for owner_id in self._path_owner_ids(str(path))
        }
        if len(path_owners) != 1:
            return (
                request,
                proposed_owner,
                ("upstream owner paths must resolve to exactly one selected chapter"),
            )
        owner_id = next(iter(path_owners))
        by_id = {chapter.id: chapter for chapter in self.work_units}
        owner = by_id.get(owner_id)
        if owner is None:
            return request, owner_id, "path-derived owner chapter is outside this swarm selection"
        if not self._is_earlier_work_unit(owner, consumer):
            return (
                request,
                owner_id,
                (
                    f"path-derived owner {owner_id} is not chronologically earlier than "
                    f"{consumer.id}"
                ),
            )
        request.update({name: str(request[name]).strip() for name in required_strings})
        request["owner_chapter_id"] = owner_id
        request["owner_paths"] = sorted(dict.fromkeys(str(path).strip() for path in owner_paths))
        request["attempted_alternatives"] = [str(item).strip() for item in valid_alternatives]
        return request, owner_id, ""

    async def _record_upstream_requests(
        self,
        chapter: WorkUnitLike,
        run: RunRecord,
        report: dict[str, Any],
        *,
        previous_attempts: str,
        forced_escalation: str = "",
    ) -> tuple[str, ...]:
        raw_requests = report.get("upstream_requests")
        if not isinstance(raw_requests, list):
            return ()
        request_ids: list[str] = []
        for raw in raw_requests:
            request, owner_id, escalation = self._normalize_upstream_request(chapter, raw)
            escalation = forced_escalation or escalation
            request_id, _ = await self.state.enqueue_upstream_request(
                request,
                consumer_chapter_id=chapter.id,
                origin_run_id=run.id,
                owner_chapter_id=owner_id,
                previous_attempts=previous_attempts,
                escalation_reason=escalation,
            )
            request_ids.append(request_id)
        if request_ids:
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.PENDING,
                f"recorded {len(request_ids)} upstream proof request(s)",
            )
        return tuple(dict.fromkeys(request_ids))

    @staticmethod
    def _parse_upstream_answers(
        report: dict[str, Any],
        request_ids: Iterable[str],
    ) -> tuple[dict[str, dict[str, Any]], str]:
        expected = set(request_ids)
        raw_answers = report.get("upstream_answers")
        if not isinstance(raw_answers, list):
            return {}, "targeted upstream repair returned no `upstream_answers` array"
        answers: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        for raw in raw_answers:
            if not isinstance(raw, dict):
                errors.append("an upstream answer was not an object")
                continue
            covered = raw.get("request_ids")
            disposition = raw.get("disposition")
            declarations = raw.get("declarations")
            guidance = raw.get("usage_guidance")
            rejection = raw.get("rejection_reason")
            if not isinstance(covered, list) or not covered:
                errors.append("an upstream answer had no request ids")
                continue
            covered_ids = [item for item in covered if isinstance(item, str)]
            if len(covered_ids) != len(covered) or not set(covered_ids).issubset(expected):
                errors.append("an upstream answer named an unknown request id")
                continue
            if disposition not in {"added", "existing", "downstream"}:
                errors.append("an upstream answer had an invalid disposition")
                continue
            if not isinstance(declarations, list) or not all(
                isinstance(item, str) and item.strip() for item in declarations
            ):
                errors.append("an upstream answer had invalid declaration names")
                continue
            if not isinstance(guidance, str) or not isinstance(rejection, str):
                errors.append("an upstream answer had invalid guidance")
                continue
            if disposition in {"added", "existing"} and (not declarations or not guidance.strip()):
                errors.append(f"a {disposition} answer omitted declarations or usage guidance")
                continue
            if disposition == "downstream" and (
                declarations or not rejection.strip() or not guidance.strip()
            ):
                errors.append(
                    "a downstream answer must reject the upstream placement and guide the consumer"
                )
                continue
            normalized = {
                "disposition": disposition,
                "declarations": list(dict.fromkeys(item.strip() for item in declarations)),
                "usage_guidance": guidance.strip(),
                "rejection_reason": rejection.strip(),
            }
            for request_id in covered_ids:
                if request_id in answers:
                    errors.append(f"request {request_id} received more than one answer")
                    continue
                answers[request_id] = normalized
        missing = expected.difference(answers)
        if missing:
            errors.append("missing answers for: " + ", ".join(sorted(missing)))
        return answers, "; ".join(errors)

    def _validate_upstream_answer_declarations(
        self,
        owner: WorkUnitLike,
        answers: dict[str, dict[str, Any]],
        *,
        agent_changed: bool,
    ) -> tuple[dict[str, dict[str, Any]], str]:
        """Reject claimed interfaces contradicted by the integrated Lean sources.

        New declarations must resolve in the assigned owner chapter and be placeholder-free.
        Existing declarations may come from pinned external dependencies; when they resolve in the
        selected chronological LastLib prefix, they must likewise be placeholder-free. The fresh
        downstream retry remains the semantic check for declarations supplied by Mathlib or an
        unselected earlier book.
        """

        repo = self.config.settings.repo
        chronological_prefix = tuple(
            chapter
            for chapter in self.work_units
            if chapter.id == owner.id or self._is_earlier_work_unit(chapter, owner)
        )
        accepted: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        for request_id, answer in answers.items():
            disposition = str(answer.get("disposition", ""))
            declarations = tuple(
                declaration
                for declaration in answer.get("declarations", [])
                if isinstance(declaration, str)
            )
            request_errors: list[str] = []
            if disposition == "added" and not agent_changed:
                request_errors.append(
                    "reported an added interface without an integrated source edit"
                )
            for declaration in declarations:
                if disposition == "added":
                    status = declaration_uses_placeholder_in_chapter(repo, owner, declaration)
                    if status is None:
                        request_errors.append(
                            f"added declaration `{declaration}` was not found in {owner.id}"
                        )
                    elif status:
                        request_errors.append(
                            f"added declaration `{declaration}` still uses a placeholder"
                        )
                    continue
                statuses = tuple(
                    status
                    for chapter in chronological_prefix
                    if (
                        status := declaration_uses_placeholder_in_chapter(
                            repo,
                            chapter,
                            declaration,
                        )
                    )
                    is not None
                )
                if any(statuses):
                    request_errors.append(
                        f"existing declaration `{declaration}` resolves to an unproved "
                        "LastLib declaration"
                    )
            if request_errors:
                errors.append(f"request {request_id}: " + ", ".join(request_errors))
            else:
                accepted[request_id] = answer
        return accepted, "; ".join(errors)

    async def _repair_upstream_owner(self, owner_id: str) -> None:
        """Coalesce one owner's requested records without persisting an in-flight state."""

        await asyncio.sleep(0)
        request_ids = tuple(self.state.upstream_request_batches().get(owner_id, ()))
        if not request_ids:
            return
        by_id = {chapter.id: chapter for chapter in self.work_units}
        owner = by_id.get(owner_id)
        run_id: str | None = None

        async def escalate(message: str) -> None:
            await self.state.record_upstream_answers(
                request_ids,
                run_id=run_id,
                answers={},
                error=message,
            )

        if owner is None:
            await escalate("upstream owner is outside this swarm selection")
            return
        requests = tuple(self.state.upstream_requests[request_id] for request_id in request_ids)
        try:
            if not await self._proof_build_is_fresh(owner):
                refresh = await self._refresh_stale_proof_build(owner)
                if not refresh.succeeded:
                    await escalate(
                        "owner sources failed coordinator refresh before targeted repair: "
                        + refresh.output[-4000:]
                    )
                    return
            attempt = await self._attempt(
                owner,
                Stage.PROVE,
                role=UPSTREAM_REPAIR_ROLE,
                request_ids=request_ids,
                upstream_requests=requests,
            )
            run_id = attempt.run.id
            if not (
                attempt.agent.succeeded
                and attempt.validation.succeeded
                and attempt.agent.report.get("complete") is True
            ):
                await escalate(
                    "targeted upstream repair did not complete with a clean owner build:\n"
                    + attempt.feedback()[-6000:]
                )
                return
            answers, answer_error = self._parse_upstream_answers(
                attempt.agent.report,
                request_ids,
            )
            answers, declaration_error = self._validate_upstream_answer_declarations(
                owner,
                answers,
                agent_changed=attempt.agent.changed,
            )
            answer_error = "; ".join(error for error in (answer_error, declaration_error) if error)
            await self.state.record_upstream_answers(
                request_ids,
                run_id=run_id,
                answers=answers,
                error=answer_error,
            )
            owner_proof = self.state.task(owner.id, Stage.PROVE)
            owner_placeholders = await asyncio.to_thread(
                count_placeholders, self.config.settings.repo, owner
            )
            if (
                attempt.agent.changed
                and owner_proof.status == TaskStatus.SUCCEEDED
                and owner_placeholders == 0
            ):
                owner_digest = await asyncio.to_thread(
                    scope_digest, self.config.settings.repo, owner
                )
                await self.state.set_task(
                    owner.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    "proved upstream interface repair validated",
                    source_digest=owner_digest,
                )
        except Exception as error:
            await escalate(f"targeted upstream repair orchestration failed: {error}")

    async def _ensure_upstream_answers(self, request_ids: Iterable[str]) -> tuple[str, ...]:
        """Run owner batches until every supplied request is answered or escalated."""

        selected = tuple(dict.fromkeys(request_ids))
        while True:
            requested_by_owner: dict[str, list[str]] = {}
            for request_id in selected:
                request = self.state.upstream_requests.get(request_id)
                if not isinstance(request, dict):
                    continue
                if request.get("status") != UpstreamRequestStatus.REQUESTED.value:
                    continue
                owner_id = str(request.get("owner_chapter_id", ""))
                if owner_id:
                    requested_by_owner.setdefault(owner_id, []).append(request_id)
            if not requested_by_owner:
                break
            tasks: list[asyncio.Task[None]] = []
            for owner_id in requested_by_owner:
                task = self._upstream_repair_tasks.get(owner_id)
                if task is None or task.done():
                    task = asyncio.create_task(self._repair_upstream_owner(owner_id))
                    self._upstream_repair_tasks[owner_id] = task
                tasks.append(task)
            await asyncio.gather(*tasks)
            for owner_id, task in tuple(self._upstream_repair_tasks.items()):
                if task.done():
                    self._upstream_repair_tasks.pop(owner_id, None)
        return tuple(
            request_id
            for request_id in selected
            if (
                isinstance((request := self.state.upstream_requests.get(request_id)), dict)
                and request.get("status") == UpstreamRequestStatus.ANSWERED.value
            )
        )

    def _upstream_retry_feedback(
        self,
        request_ids: Iterable[str],
        previous_attempts: str,
    ) -> str:
        blocks = [
            "This is the single targeted downstream retry for the durable upstream handoff. "
            "Prove each named blocked declaration using the answer, or use the rejection guidance "
            "to construct the bridge locally. Continue any independent proof work, but do not "
            "repeat or silently replace the upstream request."
        ]
        for request_id in request_ids:
            request = self.state.upstream_requests[request_id]
            raw_answer = request.get("answer")
            answer: dict[str, Any] = raw_answer if isinstance(raw_answer, dict) else {}
            blocks.append(
                f"Request `{request_id}`\n"
                f"Blocked declaration: `{request.get('blocked_declaration', '')}` in "
                f"`{request.get('consumer_path', '')}`\n"
                f"Original residual goal:\n{request.get('residual_goal', '')}\n"
                f"Original upstream result requested:\n{request.get('needed_result', '')}\n"
                "Original attempted alternatives:\n- "
                + "\n- ".join(request.get("attempted_alternatives", []))
                + f"\nUpstream disposition: {answer.get('disposition', '')}\n"
                + "Exact declarations: "
                + ", ".join(answer.get("declarations", []))
                + f"\nUsage guidance:\n{answer.get('usage_guidance', '')}\n"
                + f"Rejection reason:\n{answer.get('rejection_reason', '')}"
            )
        if previous_attempts:
            blocks.append("Previous proof-attempt ledger:\n" + previous_attempts)
        return _bounded_proof_feedback(blocks)

    def _succeeded_upstream_request_ids(
        self,
        request_ids: Iterable[str],
        validation: ValidationResult,
    ) -> tuple[str, ...]:
        if not validation.succeeded:
            return ()
        succeeded: list[str] = []
        for request_id in request_ids:
            request = self.state.upstream_requests.get(request_id)
            if not isinstance(request, dict):
                continue
            has_placeholder = declaration_uses_placeholder(
                self.config.settings.repo,
                str(request.get("consumer_path", "")),
                str(request.get("blocked_declaration", "")),
            )
            if has_placeholder is False:
                succeeded.append(request_id)
        return tuple(succeeded)

    async def _close_previously_satisfied_upstream_requests(
        self,
        chapter: WorkUnitLike,
        *,
        build_fresh: bool,
    ) -> tuple[str, ...]:
        if not build_fresh:
            return ()
        requests = self.state.upstream_requests_for_consumer(chapter.id)
        candidates = tuple(
            str(request.get("id", ""))
            for request in requests
            if request.get("status") != UpstreamRequestStatus.CLOSED.value
        )
        succeeded = self._succeeded_upstream_request_ids(
            candidates,
            ValidationResult(True, 0, "existing clean coordinator build"),
        )
        if succeeded:
            await self.state.finish_upstream_requests(
                succeeded,
                run_id=None,
                succeeded_ids=succeeded,
                success_detail=(
                    "blocked declaration was already proved in validated current sources"
                ),
            )
        return succeeded

    @staticmethod
    def _failed_attempt_feedback(report: dict[str, Any]) -> str:
        """Render structured proof failures for an independent chapter review."""

        raw_attempts = report.get("failed_attempts")
        if not isinstance(raw_attempts, list):
            return ""
        blocks: list[str] = []
        for raw in raw_attempts:
            if not isinstance(raw, dict):
                continue
            path = str(raw.get("path", "")).strip()
            declaration = str(raw.get("declaration", "")).strip()
            remaining_goal = str(raw.get("remaining_goal", "")).strip()
            obstruction = str(raw.get("obstruction", "")).strip()
            attempts = raw.get("attempts")
            checked = (
                [str(item).strip() for item in attempts if str(item).strip()]
                if isinstance(attempts, list)
                else []
            )
            if not (path and declaration and remaining_goal and obstruction and checked):
                continue
            blocks.append(
                f"Failed proof `{declaration}` in `{path}`:\n"
                + "Checked attempts:\n"
                + "\n".join(f"- {item}" for item in checked)
                + f"\nRemaining goal:\n{remaining_goal}\nObserved obstruction:\n{obstruction}"
            )
        return "\n\n".join(blocks)

    @staticmethod
    def _obsolete_dependency_blocker(blocker: dict[str, Any]) -> bool:
        text = " ".join(
            str(blocker.get(field, "")) for field in ("remaining_goal", "obstruction")
        ).casefold()
        return ("dependency" in text or "imported" in text or "prerequisite" in text) and (
            "fail" in text or "before" in text or "could not" in text
        )

    @staticmethod
    def _normalized_blocker_goal(blocker: dict[str, Any]) -> str:
        goal = re.sub(r"\s+", " ", str(blocker.get("remaining_goal", ""))).strip()
        goal = goal.removeprefix("⊢ ")
        if goal.startswith("Nonempty (") and goal.endswith(")"):
            goal = goal[len("Nonempty (") : -1]
        goal = goal.replace("BaseChangeData.leftSquare B", "B.leftSquare")
        return goal.replace("BaseChangeData.outerSquare B", "B.outerSquare")

    async def _resolve_obsolete_dependency_blockers(self, chapter_id: str) -> None:
        stale = (
            str(blocker["id"])
            for blocker in self.state.proof_blockers_for_consumer(chapter_id)
            if self._obsolete_dependency_blocker(blocker)
        )
        await self.state.set_proof_blocker_status(stale, ProofBlockerStatus.RESOLVED)

    def _durable_blocker_feedback(
        self,
        chapter_id: str,
        targets: Iterable[ProofTarget] = (),
    ) -> str:
        """Render deduplicated blockers relevant to the current proof assignment."""

        selected = tuple(targets)

        def matches(blocker: dict[str, Any]) -> bool:
            if not selected:
                return True
            path = str(blocker.get("path", ""))
            declaration = str(blocker.get("declaration", ""))
            return any(
                path == target.path
                and (
                    declaration == target.declaration
                    or declaration.endswith("." + target.declaration)
                )
                for target in selected
            )

        deduplicated: dict[tuple[str, str, str], dict[str, Any]] = {}
        for blocker in self.state.proof_blockers_for_consumer(chapter_id):
            if not matches(blocker):
                continue
            key = (
                str(blocker.get("path", "")),
                str(blocker.get("declaration", "")).rsplit(".", 1)[-1],
                self._normalized_blocker_goal(blocker),
            )
            previous = deduplicated.get(key)
            if previous is None or str(blocker.get("updated_at", "")) >= str(
                previous.get("updated_at", "")
            ):
                deduplicated[key] = blocker

        blocks = []
        for blocker in sorted(deduplicated.values(), key=lambda value: str(value.get("id", ""))):
            blocks.append(
                f"{blocker['id']} — `{blocker.get('declaration', '')}` in "
                f"`{blocker.get('path', '')}` (seen {blocker.get('sightings', 1)} time(s))\n"
                f"Residual goal: {str(blocker.get('remaining_goal', ''))[:2000]}\n"
                f"Obstruction: {str(blocker.get('obstruction', ''))[:1200]}"
            )
        if not blocks:
            return ""
        return _bounded_proof_feedback(
            (
                "Durable blocker ledger. Do not repeat its evidence. Put unchanged IDs in "
                "`blocker_refs`; use `failed_attempts` only for new or materially changed "
                "blockers:\n\n" + "\n\n".join(blocks),
            )
        )

    async def _record_proof_blocker_deltas(
        self,
        chapter: WorkUnitLike,
        run: RunRecord,
        report: dict[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        attempts = report.get("failed_attempts")
        refs = report.get("blocker_refs")
        upstream = report.get("upstream_requests")
        return await self.state.record_proof_blockers(
            chapter.id,
            origin_run_id=run.id,
            failed_attempts=(attempts if isinstance(attempts, list) else ()),
            unchanged_ids=(refs if isinstance(refs, list) else ()),
            upstream_candidates=(upstream if isinstance(upstream, list) else ()),
        )

    async def _resolve_satisfied_proof_blockers(self, chapter: WorkUnitLike) -> None:
        resolved = []
        for blocker in self.state.proof_blockers_for_consumer(chapter.id, active_only=False):
            if (
                declaration_uses_placeholder(
                    self.config.settings.repo,
                    str(blocker.get("path", "")),
                    str(blocker.get("declaration", "")),
                )
                is False
            ):
                resolved.append(str(blocker["id"]))
        await self.state.set_proof_blocker_status(resolved, ProofBlockerStatus.RESOLVED)

    @staticmethod
    def _blocker_needs_review(blocker: dict[str, Any]) -> bool:
        disposition = str(blocker.get("disposition", ""))
        if disposition in {"statement_review", "interface_review"}:
            return True
        evidence = (
            str(blocker.get("obstruction", "")) + " " + str(blocker.get("remaining_goal", ""))
        ).lower()
        return any(
            marker in evidence
            for marker in (
                "statement is false",
                "missing hypothesis",
                "missing assumption",
                "statement/interface",
                "interface mismatch",
            )
        )

    @staticmethod
    def _blocker_report(blocker: dict[str, Any]) -> dict[str, Any]:
        return {
            "failed_attempts": [
                {
                    "path": blocker.get("path", ""),
                    "declaration": blocker.get("declaration", ""),
                    "attempts": list(blocker.get("attempts", ()))
                    or [
                        "Durable proof attempts are recorded in the blocker ledger.",
                        "The residual obstruction was unchanged on a later proof cycle.",
                    ],
                    "remaining_goal": blocker.get("remaining_goal", ""),
                    "obstruction": blocker.get("obstruction", ""),
                }
            ]
        }

    def _proof_review_feedback(
        self,
        chapter_id: str,
    ) -> tuple[str, tuple[str, ...]]:
        entries: list[tuple[str, str, str]] = []
        for request_id, value in self.state.proof_review_requests.items():
            feedback = value.get("feedback") if isinstance(value, dict) else None
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if not isinstance(block, str) or not block.strip():
                continue
            kind = str(value.get("kind", PROOF_FINDING_REVIEW_KIND))
            entries.append((request_id, kind, block))
        diagnostic_entries = [entry for entry in entries if entry[1] in DIAGNOSTIC_REVIEW_KINDS]
        selected = diagnostic_entries or entries
        blocks: dict[str, None] = {}
        request_ids: list[str] = []
        for request_id, kind, block in selected:
            request_ids.append(request_id)
            rendered = (
                block
                if kind in DIAGNOSTIC_REVIEW_KINDS
                else self._tag_proof_findings(request_id, block)
            )
            blocks[rendered] = None
        return "\n\n".join(blocks), tuple(request_ids)

    def _proof_review_role(self, request_ids: Iterable[str]) -> str:
        """Select a reason-specific re-review role for one homogeneous request batch."""

        kinds = {
            str(value.get("kind", PROOF_FINDING_REVIEW_KIND))
            for request_id in request_ids
            for value in (self.state.proof_review_requests.get(request_id),)
            if isinstance(value, dict)
        }
        if kinds == {BUILD_WARNING_REVIEW_KIND}:
            return WARNING_REVIEW_ROLE
        if kinds and kinds.issubset(DIAGNOSTIC_REVIEW_KINDS):
            return DIAGNOSTIC_REVIEW_ROLE
        return ""

    @staticmethod
    def _proof_finding_ids(request_id: str, feedback: str) -> tuple[str, ...]:
        count = len(re.findall(r"(?m)^Failed proof `", feedback))
        return tuple(f"{request_id}:{index}" for index in range(1, count + 1))

    @classmethod
    def _tag_proof_findings(cls, request_id: str, feedback: str) -> str:
        finding_ids = iter(cls._proof_finding_ids(request_id, feedback))
        return re.sub(
            r"(?m)^Failed proof `",
            lambda _match: f"Finding ID: `{next(finding_ids)}`\nFailed proof `",
            feedback,
        )

    def _expected_proof_finding_ids(
        self,
        chapter_id: str,
        request_ids: Iterable[str],
    ) -> tuple[str, ...]:
        expected: list[str] = []
        for request_id in request_ids:
            value = self.state.proof_review_requests.get(request_id)
            feedback = value.get("feedback") if isinstance(value, dict) else None
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if isinstance(block, str):
                expected.extend(self._proof_finding_ids(request_id, block))
        return tuple(expected)

    def _proof_review_assessment_error(
        self,
        chapter_id: str,
        run_id: str,
        expected_ids: Iterable[str],
    ) -> str:
        expected = tuple(expected_ids)
        if not expected:
            return ""
        run = next(
            (item for item in self.state.task(chapter_id, Stage.REVIEW).runs if item.id == run_id),
            None,
        )
        if run is None:
            return "proof-review run disappeared before finding assessments were validated"
        self.state.load_run_details(run)
        report = run.report if isinstance(run.report, dict) else {}
        assessments = report.get("finding_assessments")
        received = (
            tuple(str(item.get("finding_id", "")) for item in assessments if isinstance(item, dict))
            if isinstance(assessments, list)
            else ()
        )
        if len(received) == len(expected) and set(received) == set(expected):
            return ""
        missing = sorted(set(expected).difference(received))
        unexpected = sorted(set(received).difference(expected))
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        if len(received) != len(set(received)):
            details.append("duplicate finding ids")
        return (
            "proof-review finding assessments did not match supplied findings ("
            + "; ".join(details)
            + ")"
        )

    async def _queue_proof_review(
        self,
        chapter: WorkUnitLike,
        report: dict[str, Any],
        *,
        origin_run_id: str,
    ) -> set[str]:
        """Durably hand failed proof evidence to a full-scope chapter review."""

        attempt_feedback = self._failed_attempt_feedback(report)
        if not attempt_feedback:
            return set()
        feedback = {
            chapter.id: (
                f"Proof work in `{chapter.id}` left checked failures. Evaluate this evidence while "
                "re-reviewing the complete assigned scope:\n\n" + attempt_feedback
            )
        }
        _, created = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin_run_id,
            kind=PROOF_FINDING_REVIEW_KIND,
        )
        targets = {chapter.id}
        if not created:
            return targets
        invalidated_reviews = await self._invalidate_reviews(
            targets,
            detail="review invalidated by failed-proof findings",
        )
        return invalidated_reviews

    def _has_completed_green_review(self, chapter_id: str) -> bool:
        """Whether history contains a completed no-change review pass."""

        runs = self.state.task(chapter_id, Stage.REVIEW).runs
        for run in reversed(runs):
            if run.status != TaskStatus.SUCCEEDED or run.changed:
                continue
            self.state.load_run_details(run)
            report = run.report if isinstance(run.report, dict) else {}
            if report.get("complete") is True:
                return True
        return False

    async def _restore_unaffected_review_successes(
        self,
        pending_owners: set[str],
    ) -> None:
        """Repair review greens erased by the former closure-wide policy."""

        restored: set[str] = set()
        synthetic_failures = {
            "formalization failed; quarantined from review",
            "formalization failed; quarantined from proof",
        }
        for chapter in self.work_units:
            if chapter.id in pending_owners:
                continue
            task = self.state.task(chapter.id, Stage.REVIEW)
            recoverable = task.status in (TaskStatus.PENDING, TaskStatus.BLOCKED) or (
                task.status == TaskStatus.FAILED and task.detail in synthetic_failures
            )
            if recoverable and self._has_completed_green_review(chapter.id):
                restored.add(chapter.id)
        if restored:
            await self.state.set_tasks(
                restored,
                Stage.REVIEW,
                TaskStatus.SUCCEEDED,
                "durable review remains green; no pending findings for this chapter",
            )

    async def _recover_upstream_requests(self) -> None:
        """Recover a proof report persisted just before its request handoff was checkpointed."""

        persisted_origins: set[str] = set()
        for request in self.state.upstream_requests.values():
            origin_run_ids = request.get("origin_run_ids")
            if not isinstance(origin_run_ids, list):
                continue
            persisted_origins.update(origin for origin in origin_run_ids if isinstance(origin, str))
        for chapter in self.work_units:
            for run in self.state.task(chapter.id, Stage.PROVE).runs:
                if run.auxiliary or run.id in persisted_origins:
                    continue
                self.state.load_run_details(run)
                report = run.report if isinstance(run.report, dict) else {}
                if not report.get("upstream_requests"):
                    continue
                issues = report.get("issues")
                previous = "Recovered upstream handoff from completed proof run " + run.id
                if isinstance(issues, list) and issues:
                    previous += ":\n- " + "\n- ".join(
                        str(issue) for issue in issues if isinstance(issue, str)
                    )
                await self._record_upstream_requests(
                    chapter,
                    run,
                    report,
                    previous_attempts=previous,
                )
                persisted_origins.add(run.id)

    async def _recover_proof_review_requests(self) -> None:
        """Recover durable blockers and only escalate repeated review evidence."""

        persisted_origins = {
            origin
            for value in self.state.proof_blockers.values()
            if isinstance(value, dict)
            for origin in value.get("origin_run_ids", ())
            if isinstance(origin, str)
        }
        for chapter in self.work_units:
            for run in self.state.task(chapter.id, Stage.PROVE).runs:
                if run.auxiliary or run.id in persisted_origins:
                    continue
                self.state.load_run_details(run)
                report = run.report if isinstance(run.report, dict) else {}
                if not report.get("failed_attempts") and not report.get("blocker_refs"):
                    continue
                blockers = await self._record_proof_blocker_deltas(chapter, run, report)
                for blocker in blockers:
                    if (
                        int(blocker.get("sightings", 0)) >= 2
                        and blocker.get("status") == ProofBlockerStatus.OPEN.value
                        and self._blocker_needs_review(blocker)
                    ):
                        await self._queue_proof_review(
                            chapter,
                            self._blocker_report(blocker),
                            origin_run_id=run.id,
                        )
                        await self.state.set_proof_blocker_status(
                            (str(blocker["id"]),),
                            ProofBlockerStatus.REVIEW_REQUESTED,
                        )
                persisted_origins.add(run.id)

        by_id = {chapter.id: chapter for chapter in self.work_units}
        pending_owners = {
            chapter_id
            for value in self.state.proof_review_requests.values()
            if isinstance(value, dict)
            for feedback in (value.get("feedback"),)
            if isinstance(feedback, dict)
            for chapter_id in feedback
            if chapter_id in by_id
        }
        stale_reviews = {
            chapter_id
            for chapter_id in pending_owners
            if self.state.task(chapter_id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
        }
        if stale_reviews:
            await self._invalidate_reviews(
                stale_reviews,
                detail="recovering pending failed-proof review findings",
            )
        await self._restore_unaffected_review_successes(pending_owners)

    def _module_owner_ids(self, module: str) -> tuple[str, ...]:
        owners: list[str] = []
        candidate = module
        while candidate:
            owners.extend(self._module_owners.get(candidate, ()))
            candidate = candidate.rpartition(".")[0]
        return self._ordered_owner_ids(owners)

    def _fingerprint_dependencies(
        self,
        work_unit_id: str,
        fingerprint: dict[str, Any],
    ) -> tuple[str, ...]:
        dependencies: set[str] = set()
        modules = fingerprint.get("modules", ())
        if not isinstance(modules, list):
            return ()
        for module in modules:
            if not isinstance(module, dict) or not isinstance(module.get("imports"), list):
                continue
            for imported in module["imports"]:
                if not isinstance(imported, str):
                    continue
                dependencies.update(self._module_owner_ids(imported))
        dependencies.discard(work_unit_id)
        return self._ordered_owner_ids(dependencies)

    def _identifier_owner_ids(self, text: str) -> tuple[str, ...]:
        owners: list[str] = []
        for start, character in enumerate(text):
            if start and _is_identifier_character(text[start - 1]):
                continue
            node = self._identifier_trie.children.get(character)
            if node is None:
                continue
            end = start + 1
            while True:
                if node.owner_ids and (end == len(text) or not _is_identifier_character(text[end])):
                    owners.extend(node.owner_ids)
                if end == len(text):
                    break
                node = node.children.get(text[end])
                if node is None:
                    break
                end += 1
        return self._ordered_owner_ids(owners)

    def _diagnostic_owner_ids(self, diagnostic: LeanDiagnostic) -> tuple[str, ...]:
        with self._diagnostic_owner_cache_lock:
            if diagnostic in self._diagnostic_owner_cache:
                return self._diagnostic_owner_cache[diagnostic]

        message = diagnostic.header.split(":", 1)[1].lstrip()
        if location := LEAN_LOCATION_RE.match(message):
            owners = self._path_owner_ids(location.group("path"))
        else:
            # Some orchestration failures have no line/column but do name one or
            # more modules or source roots. Matching only this diagnostic block is
            # intentional: matching the complete Lake transcript over-assigns every
            # replayed dependency and permitted `sorry` warning.
            owners = self._identifier_owner_ids(diagnostic.text)

        with self._diagnostic_owner_cache_lock:
            if len(self._diagnostic_owner_cache) >= DIAGNOSTIC_OWNER_CACHE_MAXIMUM:
                self._diagnostic_owner_cache.pop(next(iter(self._diagnostic_owner_cache)))
            self._diagnostic_owner_cache[diagnostic] = owners
        return owners

    def _partition_build_diagnostics(
        self,
        result: ValidationResult,
        target_ids: Iterable[str],
        graph: WorkUnitImportGraph,
    ) -> dict[str, ValidationResult]:
        """Project every combined Lake result onto each target's dependency closure."""

        ids = tuple(target_ids)
        if result.succeeded:
            return {
                target_id: replace(
                    result,
                    status=ValidationStatus.CLEAN,
                    blocked_by=(),
                )
                for target_id in ids
            }
        diagnostics = _lean_diagnostics(result.output)
        owned = tuple(
            (diagnostic, set(self._diagnostic_owner_ids(diagnostic))) for diagnostic in diagnostics
        )
        unattributed = not diagnostics or any(not owners for _, owners in owned)

        partitioned: dict[str, ValidationResult] = {}
        for target_id in ids:
            closure = self._dependency_closure(graph, (target_id,))
            relevant = [
                (diagnostic, owners & closure) for diagnostic, owners in owned if closure & owners
            ]
            if relevant:
                output = "\n\n".join(diagnostic.text for diagnostic, _owners in relevant)
                relevant_owners = set().union(*(owners for _diagnostic, owners in relevant))
                blocked_by = tuple(sorted(relevant_owners - {target_id}))
                status = (
                    ValidationStatus.TARGET_FAILED
                    if target_id in relevant_owners
                    else ValidationStatus.UPSTREAM_FAILED
                )
                output += (
                    f"\n\nCoordinator found {len(relevant)} Lean diagnostic(s) relevant "
                    f"to {target_id}."
                )
                partitioned[target_id] = ValidationResult(
                    False,
                    result.exit_code,
                    output[-20_000:],
                    timed_out=result.timed_out,
                    process_exit_code=result.process_exit_code,
                    status=status,
                    blocked_by=blocked_by,
                )
            elif result.compiler_succeeded and not unattributed:
                partitioned[target_id] = ValidationResult(
                    True,
                    0,
                    "Lake build succeeded; diagnostics belonged to other batch targets.",
                    process_exit_code=0,
                    status=ValidationStatus.CLEAN,
                )
            else:
                partitioned[target_id] = ValidationResult(
                    False,
                    result.exit_code,
                    result.output[-20_000:],
                    timed_out=result.timed_out,
                    process_exit_code=result.process_exit_code,
                    status=ValidationStatus.UNATTRIBUTED_BUILD_FAILURE,
                )
        return partitioned

    @staticmethod
    def _proof_chunk_validation(
        result: ValidationResult,
        targets: Iterable[ProofTarget],
    ) -> ValidationResult:
        """Project whole-build diagnostics onto the assigned declaration spans.

        The coordinator still builds the chapter and its imported closure.  A proof
        worker, however, can only act on its assigned declarations, so located Lean
        errors and rejected warnings outside those declarations must not spend that
        chunk's retry budget.
        """

        selected = tuple(targets)
        if result.succeeded or not selected:
            return result
        diagnostics = _lean_diagnostics(result.output)
        if not diagnostics:
            # Timeouts and process failures without a parsed Lean diagnostic do not
            # establish that the assigned declarations are clean.
            return result

        def same_path(left: str, right: str) -> bool:
            normalized_left = Path(left).as_posix().lstrip("./")
            normalized_right = Path(right).as_posix().lstrip("./")
            return (
                normalized_left == normalized_right
                or normalized_left.endswith("/" + normalized_right)
                or normalized_right.endswith("/" + normalized_left)
            )

        relevant: list[LeanDiagnostic] = []
        for diagnostic in diagnostics:
            message = diagnostic.header.split(":", 1)[1].lstrip()
            location = LEAN_LOCATION_RE.match(message)
            if location is None:
                continue
            line = int(location.group("line"))
            if any(
                same_path(location.group("path"), target.path)
                and target.line <= line <= target.end_line
                for target in selected
            ):
                relevant.append(diagnostic)

        if relevant:
            output = "\n\n".join(diagnostic.text for diagnostic in relevant)
            output += (
                f"\n\nCoordinator rejected {len(relevant)} Lean diagnostic(s) relevant "
                "to the assigned proof chunk."
            )
            return ValidationResult(
                False,
                result.exit_code,
                output[-20_000:],
                timed_out=result.timed_out,
                process_exit_code=result.process_exit_code,
            )

        return ValidationResult(
            True,
            0,
            "Whole-chapter build failed, but no located Lean errors or rejected warnings "
            "belonged to the assigned proof chunk.",
            process_exit_code=result.process_exit_code,
        )

    async def _build_chapters(
        self,
        chapters: Iterable[WorkUnitLike],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FORMALIZE,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Coalesce pending coordinator requests and return this caller's target results."""

        selected = tuple(dict.fromkeys(chapter.id for chapter in chapters))
        if not selected:
            return {}
        by_id = {chapter.id: chapter for chapter in self.work_units}
        request_chapters = tuple(by_id[chapter_id] for chapter_id in selected)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, ValidationResult]] = loop.create_future()
        request = PendingBuildRequest(
            chapters=request_chapters,
            publish_if_clean=publish_if_clean,
            mode=mode,
            iteration=iteration,
            maximum_iterations=maximum_iterations,
            stage=stage,
            snapshots=snapshots,
            future=future,
        )
        self._pending_build_requests.append(request)
        if self._build_dispatch_task is None or self._build_dispatch_task.done():
            self._build_dispatch_task = asyncio.create_task(self._dispatch_build_requests())
        return await future

    async def _dispatch_build_requests(self) -> None:
        """Drain all build callers through one rolling, cross-stage batch lane."""

        try:
            while True:
                # Include every caller that became runnable in the current event-loop turn.
                await asyncio.sleep(0)
                requests = tuple(
                    request
                    for request in self._pending_build_requests
                    if not request.future.cancelled()
                )
                self._pending_build_requests.clear()
                if not requests:
                    return
                await self._run_build_batch(requests)
        finally:
            self._build_dispatch_task = None

    async def _run_build_batch(self, requests: tuple[PendingBuildRequest, ...]) -> None:
        """Execute and partition a coalesced batch until every caller has a precise result."""

        requested_ids = {chapter.id for request in requests for chapter in request.chapters}
        remaining = set(requested_ids)
        results_by_id: dict[str, ValidationResult] = {}
        snapshots_by_id: dict[str, ValidatedBuildSnapshot] = {}

        def finish_ready_requests() -> None:
            for request in requests:
                if request.future.done():
                    continue
                ids = tuple(chapter.id for chapter in request.chapters)
                if not all(chapter_id in results_by_id for chapter_id in ids):
                    continue
                if request.snapshots is not None:
                    request.snapshots.update(
                        {
                            chapter_id: snapshots_by_id[chapter_id]
                            for chapter_id in ids
                            if chapter_id in snapshots_by_id
                        }
                    )
                request.future.set_result(
                    {chapter_id: results_by_id[chapter_id] for chapter_id in ids}
                )

        try:
            while remaining:
                active = tuple(
                    request
                    for request in requests
                    if not request.future.done()
                    and any(chapter.id in remaining for chapter in request.chapters)
                )
                candidate_ids = {
                    chapter.id
                    for request in active
                    for chapter in request.chapters
                    if chapter.id in remaining
                }
                if not candidate_ids:
                    break
                selected_by_id: dict[str, WorkUnitLike] = {}
                for request in active:
                    for chapter in request.chapters:
                        if chapter.id in candidate_ids:
                            selected_by_id.setdefault(chapter.id, chapter)
                candidates = tuple(selected_by_id.values())
                first_prefix = candidates[0].build_command.strip().rpartition(" ")[0]
                compatible = tuple(
                    chapter
                    for chapter in candidates
                    if chapter.build_command.strip().rpartition(" ")[0] == first_prefix
                )
                selected = compatible
                active_ids = {chapter.id for chapter in selected}
                attempt_requests = tuple(
                    request
                    for request in active
                    if any(chapter.id in active_ids for chapter in request.chapters)
                )
                modes = {request.mode for request in attempt_requests}
                mode = next(iter(modes)) if len(modes) == 1 else "batched"
                owner = attempt_requests[0]
                attempt_snapshots: dict[str, ValidatedBuildSnapshot] = {}
                capture_snapshots = any(
                    request.snapshots is not None for request in attempt_requests
                )
                attempt_results = await self._execute_build_chapters(
                    selected,
                    publish_if_clean=any(request.publish_if_clean for request in attempt_requests),
                    mode=mode,
                    iteration=max(request.iteration for request in attempt_requests),
                    maximum_iterations=max(
                        request.maximum_iterations for request in attempt_requests
                    ),
                    stage=owner.stage,
                    snapshots=attempt_snapshots if capture_snapshots else None,
                )
                succeeded_ids = {
                    chapter_id for chapter_id, result in attempt_results.items() if result.succeeded
                }
                if succeeded_ids:
                    results_by_id.update(
                        (chapter_id, attempt_results[chapter_id]) for chapter_id in succeeded_ids
                    )
                    snapshots_by_id.update(
                        (chapter_id, attempt_snapshots[chapter_id])
                        for chapter_id in succeeded_ids
                        if chapter_id in attempt_snapshots
                    )
                    remaining.difference_update(succeeded_ids)
                    finish_ready_requests()
                failed_ids = active_ids.difference(succeeded_ids)
                if not failed_ids:
                    continue
                unattributed = {
                    chapter_id
                    for chapter_id in failed_ids
                    if attempt_results[chapter_id].status
                    is ValidationStatus.UNATTRIBUTED_BUILD_FAILURE
                }
                if unattributed and len(active_ids) > 1:
                    # A failed process may stop before compiling the other targets.
                    # Remove every attributed failure, then retry the uncertain
                    # targets together. A wholly unattributed process failure is
                    # shared by the batch below; probing subsets adds no evidence.
                    attributed = failed_ids.difference(unattributed)
                    if attributed:
                        results_by_id.update(
                            (chapter_id, attempt_results[chapter_id]) for chapter_id in attributed
                        )
                        remaining.difference_update(attributed)
                        finish_ready_requests()
                        continue
                affected = failed_ids
                results_by_id.update(
                    (chapter_id, attempt_results[chapter_id]) for chapter_id in affected
                )
                remaining.difference_update(affected)
                finish_ready_requests()
        except BaseException as error:
            for request in requests:
                if request.future.done():
                    continue
                if isinstance(error, asyncio.CancelledError):
                    request.future.cancel()
                else:
                    request.future.set_exception(error)
        finally:
            finish_ready_requests()

    async def _execute_build_chapters(
        self,
        chapters: Iterable[WorkUnitLike],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FORMALIZE,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Execute one deterministic Lake invocation against the coordinator cache."""

        selected = tuple(chapters)
        if not selected:
            return {}
        ids = tuple(chapter.id for chapter in selected)
        label = f"{stage.value} {mode}: " + ", ".join(ids)

        await self.control.checkpoint()
        lease = await self.build_queue.acquire(label=label, stage=stage)
        try:
            results: dict[str, ValidationResult] = {}
            source_held = False
            build_workspace = None
            progress_flush: asyncio.Task[None] | None = None
            build_source_digests_task: asyncio.Task[dict[str, str]] | None = None

            async def flush_build_progress() -> None:
                await asyncio.sleep(0.25)
                await self.state.flush()

            try:
                await self.source_lock.acquire()
                source_held = True
                build_workspace = await self.isolation.acquire_build(label)
                async with self.state.batch():
                    await self.state.start_coordinator_build(
                        mode=mode,
                        stage=stage,
                        iteration=iteration,
                        maximum_iterations=maximum_iterations,
                        total=len(selected),
                        target_work_unit_ids=ids,
                    )
                build_graph = self._observed_work_unit_graph()
                by_id = {item.id: item for item in self.work_units}
                build_required_ids = self._dependency_closure(build_graph, ids)
                build_source_generations = {
                    chapter_id: self._source_generations.get(chapter_id, 0)
                    for chapter_id in build_required_ids
                }
                clean_value = self.state.formalize_graph.get("clean", {})
                clean_records = clean_value if isinstance(clean_value, dict) else {}
                reusable_digests = {
                    chapter_id: digest
                    for chapter_id in build_required_ids.difference(ids)
                    if isinstance((record := clean_records.get(chapter_id)), dict)
                    and isinstance((digest := record.get("source_digest")), str)
                }
                digest_ids = build_required_ids.difference(reusable_digests)
                build_root = build_workspace.root
                build_source_digests_task = asyncio.create_task(
                    asyncio.to_thread(
                        _scope_digests,
                        build_root,
                        by_id,
                        digest_ids,
                    )
                )
                # Digest capture reads the immutable build workspace. It can run
                # alongside Lake instead of delaying process startup, and a FUSE
                # build no longer needs the live-source barrier for this scan.
                if self.isolation.name != "shared":
                    self.source_lock.release()
                    source_held = False
                if len(selected) > 1:
                    targets = []
                    prefixes = []
                    for chapter in selected:
                        command = chapter.build_command.strip()
                        prefix, separator, target = command.rpartition(" ")
                        if not separator or not target.startswith("+"):
                            raise ValueError(
                                "cannot combine build command without a trailing "
                                f"Lake target: {command}"
                            )
                        prefixes.append(prefix.rstrip())
                        targets.append(target)
                    if len(set(prefixes)) != 1:
                        raise ValueError("build commands do not share a common prefix")
                    combined = f"{prefixes[0]} {' '.join(targets)}"
                    build_units = ((selected[0], combined, ids, len(selected)),)
                else:
                    chapter = selected[0]
                    build_units = ((chapter, chapter.build_command, (chapter.id,), 1),)
                for chapter, command, result_ids, _completed in build_units:
                    await self.state.advance_coordinator_build(
                        work_unit_id=chapter.id,
                        completed=0,
                        command=command,
                    )

                    def append_output(output: str) -> None:
                        nonlocal progress_flush
                        error_count = sum(
                            diagnostic.severity == "error"
                            and bool(set(self._diagnostic_owner_ids(diagnostic)) & set(ids))
                            for diagnostic in _lean_diagnostics(output)
                        )
                        self.state.append_coordinator_build_output(
                            output,
                            error_count=error_count,
                        )
                        if progress_flush is None or progress_flush.done():
                            progress_flush = asyncio.create_task(flush_build_progress())

                    validation = asyncio.create_task(
                        validate(
                            self.config,
                            _with_build_command(chapter, command),
                            workspace_root=build_workspace.root,
                            on_output=append_output,
                        )
                    )
                    result = await validation
                    results.update(
                        self._partition_build_diagnostics(result, result_ids, build_graph)
                    )
                    await self.state.advance_coordinator_build(
                        work_unit_id=chapter.id,
                        completed=self.state.coordinator_build.total,
                    )
                build_source_digests = reusable_digests | await build_source_digests_task
                artifacts_built = bool(results) and all(
                    result.compiler_succeeded for result in results.values()
                )
                fingerprints: FingerprintCollection | None = None
                fingerprint_error = ""
                if artifacts_built and snapshots is not None:
                    cached = self.state.formalize_graph.get("interfaces", {})
                    try:
                        fingerprints = await asyncio.to_thread(
                            collect_interface_fingerprints,
                            self.config,
                            selected,
                            root=build_workspace.root,
                            cached_records=cached if isinstance(cached, dict) else {},
                        )
                    except InterfaceFingerprintError as error:
                        fingerprint_error = str(error)[-4000:]
                if artifacts_built:
                    if not source_held:
                        await self.source_lock.acquire()
                        source_held = True
                    current_graph = self._observed_work_unit_graph()
                    possibly_modified = await self._possibly_modified_scope_ids(
                        build_required_ids,
                        build_source_generations,
                    )
                    current_source_digests = await asyncio.to_thread(
                        _scope_digests,
                        self.config.settings.repo,
                        by_id,
                        possibly_modified,
                    )
                    source_is_current = current_graph.edges == build_graph.edges and all(
                        build_source_digests[chapter_id] == digest
                        for chapter_id, digest in current_source_digests.items()
                    )
                    if not source_is_current:
                        stale = ValidationResult(
                            False,
                            1,
                            "Source dependency scope changed during the coordinator build; "
                            "retry required.",
                            status=ValidationStatus.STALE_SNAPSHOT,
                        )
                        results = {chapter_id: stale for chapter_id in results}
                        artifacts_built = False
                    elif snapshots is not None:
                        for chapter in selected:
                            required = self._dependency_closure(build_graph, (chapter.id,))
                            fingerprint = (
                                fingerprints.records[chapter.id].as_dict()
                                if fingerprints is not None and chapter.id in fingerprints.records
                                else None
                            )
                            snapshots[chapter.id] = ValidatedBuildSnapshot(
                                graph=build_graph,
                                source_digests={
                                    chapter_id: build_source_digests[chapter_id]
                                    for chapter_id in required
                                },
                                source_generations={
                                    chapter_id: build_source_generations[chapter_id]
                                    for chapter_id in required
                                },
                                fingerprint=fingerprint,
                                import_dependencies=(
                                    self._fingerprint_dependencies(chapter.id, fingerprint)
                                    if fingerprint is not None
                                    else ()
                                ),
                                fingerprint_error=fingerprint_error,
                            )
                await build_workspace.finish(
                    succeeded=artifacts_built,
                    publish=publish_if_clean and artifacts_built,
                )
                build_workspace = None
            except BaseException:
                raise
        finally:
            try:
                if progress_flush is not None:
                    if not progress_flush.done():
                        progress_flush.cancel()
                    await asyncio.gather(progress_flush, return_exceptions=True)
                if build_source_digests_task is not None:
                    await asyncio.gather(build_source_digests_task, return_exceptions=True)
                if build_workspace is not None:
                    await build_workspace.close()
                if source_held:
                    await self.state.finish_coordinator_build()
            finally:
                if source_held:
                    self.source_lock.release()
                self.build_queue.release(lease)
        return results

    async def _build_all(
        self, *, iteration: int = 1, maximum_iterations: int = 1
    ) -> dict[str, ValidationResult]:
        """Build the full selection and publish only a globally clean cache snapshot."""

        return await self._build_chapters(
            self.work_units,
            publish_if_clean=True,
            mode="global",
            iteration=iteration,
            maximum_iterations=maximum_iterations,
        )

    def _build_feedback(
        self,
        results: dict[str, ValidationResult],
        *,
        blocked_owner_ids: set[str] | frozenset[str] = frozenset(),
    ) -> BuildDiagnostics:
        feedback: dict[str, dict[str, None]] = {}
        by_id = self._work_units_by_id

        def source_location(owner_id: str) -> str:
            owner = by_id[owner_id]
            return f"{owner.source}:{owner.source_span.start_line}-{owner.source_span.end_line}"

        targets_by_result: dict[ValidationResult, list[str]] = {}
        for target_id, result in results.items():
            if result.succeeded:
                continue
            targets_by_result.setdefault(result, []).append(target_id)

        for result, target_ids in targets_by_result.items():
            routed = False
            for diagnostic in _lean_diagnostics(result.output):
                owners = self._diagnostic_owner_ids(diagnostic)
                if not owners:
                    continue
                routed = True
                for owner in owners:
                    block = (
                        f"Informal source: {source_location(owner)}\n"
                        f"Coordinator diagnostic:\n{diagnostic.text}"
                    )
                    feedback.setdefault(owner, {})[block] = None

            # A truncated Lake log can retain its failed-module summary while
            # still retaining an unrelated warning. Always route the precise
            # failed module as well as any source-located diagnostics.
            for module in _failed_modules(result.output):
                owners = self._module_owner_ids(module)
                if not owners:
                    continue
                routed = True
                for owner in owners:
                    block = (
                        f"Informal source: {source_location(owner)}\n"
                        f"Coordinator reported failed module `{module}`."
                    )
                    feedback.setdefault(owner, {})[block] = None

            if not routed:
                for target_id in target_ids:
                    block = (
                        f"Informal source: {source_location(target_id)}\n"
                        f"Coordinator build of {by_id[target_id].chapter_module} failed without a "
                        f"source-located diagnostic:\n{result.output[-12000:]}"
                    )
                    feedback.setdefault(target_id, {})[block] = None

        deferred = tuple(sorted(blocked_owner_ids.intersection(feedback)))
        return BuildDiagnostics(
            actionable={
                chapter_id: "\n\n".join(blocks)
                for chapter_id, blocks in feedback.items()
                if chapter_id not in blocked_owner_ids
            },
            deferred_owner_ids=deferred,
        )

    async def _build_feedback_async(
        self,
        results: dict[str, ValidationResult],
        *,
        blocked_owner_ids: set[str] | frozenset[str] = frozenset(),
    ) -> BuildDiagnostics:
        """Route build output without monopolizing the asyncio control thread."""

        return await asyncio.to_thread(
            self._build_feedback,
            results,
            blocked_owner_ids=blocked_owner_ids,
        )

    async def _discover_all(self) -> bool:
        """Discover inputs with bounded scheduling and batched promotion."""

        pending = deque(self.work_units)
        results: list[StageOutcome] = []
        maximum = self.config.stages[Stage.DISCOVER].max_agents
        assert maximum is not None

        async def worker() -> None:
            while pending:
                chapter = pending.popleft()
                results.append(await self._discover(chapter))

        workers = tuple(worker() for _ in range(min(len(pending), maximum * 2)))
        await _gather_cancel_on_error(workers)
        return all(result.succeeded for result in results)

    async def _discover_and_formalize(
        self,
        *,
        progress_event: asyncio.Event | None = None,
        discover: bool = True,
    ) -> bool:
        """Pipeline discovery into dependency-ready formalization without a stage gate."""

        by_id = {chapter.id: chapter for chapter in self.work_units}
        pending_discoveries: deque[WorkUnitLike] = deque()
        discovery_tasks: dict[str, asyncio.Task[StageOutcome]] = {}
        if discover:
            discovery_digests = (
                {}
                if self.force
                else await asyncio.to_thread(self._source_input_digests, self.work_units)
            )
            for chapter in self.work_units:
                if self.force or not self._discovery_is_current(
                    chapter,
                    source_digest=discovery_digests[chapter.id],
                ):
                    pending_discoveries.append(chapter)
        discovery_maximum = self.config.stages[Stage.DISCOVER].max_agents
        assert discovery_maximum is not None
        discovery_window = discovery_maximum * 2

        def fill_discovery_window() -> None:
            while pending_discoveries and len(discovery_tasks) < discovery_window:
                chapter = pending_discoveries.popleft()
                discovery_tasks[chapter.id] = asyncio.create_task(
                    # This queue contains only chapters already classified as stale.
                    self._discover(chapter, rerun=True)
                )

        fill_discovery_window()
        formalize_tasks: dict[str, asyncio.Task[StageOutcome]] = {}

        async def cancel_all() -> None:
            tasks = [*discovery_tasks.values(), *formalize_tasks.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                fill_discovery_window()
                graph = self._observed_work_unit_graph()
                succeeded = {
                    chapter_id
                    for chapter_id in by_id
                    if self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
                }
                for chapter_id in graph.order:
                    formalize_task = self.state.task(chapter_id, Stage.FORMALIZE)
                    if (
                        chapter_id in succeeded
                        or chapter_id in formalize_tasks
                        or formalize_task.status
                        in {TaskStatus.FAILED, TaskStatus.INTERRUPTED, TaskStatus.RUNNING}
                    ):
                        continue
                    if self.state.task(chapter_id, Stage.DISCOVER).status != TaskStatus.SUCCEEDED:
                        continue
                    dependencies = graph.dependencies[chapter_id]
                    if (
                        dependencies.issubset(succeeded)
                        and self.state.readiness(formalize_task).ready
                    ):
                        formalize_tasks[chapter_id] = asyncio.create_task(
                            self._formalize(by_id[chapter_id])
                        )

                live = [*discovery_tasks.values(), *formalize_tasks.values()]
                if not live:
                    break

                done, _ = await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                for chapter_id, task in tuple(discovery_tasks.items()):
                    if task not in done:
                        continue
                    discovery_tasks.pop(chapter_id)
                    outcome = task.result()
                    if (
                        outcome.failed
                        and self.state.task(chapter_id, Stage.DISCOVER).status != TaskStatus.FAILED
                    ):
                        await self.state.set_task(
                            chapter_id,
                            Stage.DISCOVER,
                            TaskStatus.FAILED,
                            "source discovery failed",
                        )
                for chapter_id, task in tuple(formalize_tasks.items()):
                    if task not in done:
                        continue
                    formalize_tasks.pop(chapter_id)
                    outcome = task.result()
                    if (
                        outcome.failed
                        and self.state.task(chapter_id, Stage.FORMALIZE).status != TaskStatus.FAILED
                    ):
                        await self.state.set_task(
                            chapter_id,
                            Stage.FORMALIZE,
                            TaskStatus.FAILED,
                            "formalization execution failed",
                        )
                if progress_event is not None:
                    progress_event.set()
        except BaseException:
            await cancel_all()
            raise

        return all(
            self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
            for chapter_id in by_id
        )

    async def _formalize_all(self) -> bool:
        return await self._discover_and_formalize(discover=True)

    async def _review_once(
        self,
        chapter: WorkUnitLike,
        *,
        rerun: bool = False,
        feedback: str = "",
        role: str = "",
        request_ids: Iterable[str] = (),
        resume_thread_id: str | None = None,
        resume_run_id: str = "",
        resume_prompt: str = "",
    ) -> StageOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return StageOutcome(ExecutionDisposition.SUCCEEDED, changed=False, complete=True)
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            feedback=feedback,
            role=role,
            request_ids=request_ids,
            queue_detail=(
                "targeted cleanup of coordinator warnings"
                if role == WARNING_REVIEW_ROLE
                else "targeted repair of coordinator diagnostics"
                if role == DIAGNOSTIC_REVIEW_ROLE
                else "full-scope review of failed-proof findings"
                if feedback
                else "source-faithful editing review"
            ),
            resume_thread_id=resume_thread_id,
            resume_run_id=resume_run_id,
            resume_prompt=resume_prompt,
        )
        if attempt.agent.capacity_exhausted:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.FAILED,
                "model capacity remained unavailable after the configured retries",
            )
            return StageOutcome(
                ExecutionDisposition.FAILED,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
            )
        report_error = ""
        if not attempt.agent.report and not attempt.agent.capacity_exhausted:
            report_error = attempt.agent.error or "review returned no structured final report"
        succeeded = attempt.agent.succeeded and attempt.validation.succeeded
        complete = bool(attempt.agent.report.get("complete"))
        if succeeded and complete:
            if attempt.agent.changed:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "review changes merged; coordinator verification queued",
                )
            return StageOutcome(
                ExecutionDisposition.SUCCEEDED,
                changed=attempt.agent.changed,
                complete=True,
                run_id=attempt.run.id,
            )
        if report_error:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.RUNNING,
                "invalid review report; session retry queued",
            )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
                report_error=report_error,
            )
        if succeeded:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.RUNNING,
                "incomplete review report; session retry queued",
            )
            return StageOutcome(
                ExecutionDisposition.WAITING,
                changed=attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
            )
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            "editing review failed",
        )
        return StageOutcome(
            ExecutionDisposition.SUCCEEDED if succeeded else ExecutionDisposition.FAILED,
            changed=attempt.agent.changed,
            complete=complete,
            run_id=attempt.run.id,
        )

    def _review_invalidation_generation(self, chapter_id: str) -> int:
        return self._review_invalidation_generations.get(chapter_id, 0)

    async def _complete_review(
        self,
        chapter: WorkUnitLike,
        detail: str,
        *,
        expected_generation: int | None = None,
        proof_request_ids: Iterable[str] = (),
    ) -> bool:
        async with self._review_generation_lock:
            if (
                expected_generation is not None
                and self._review_invalidation_generation(chapter.id) != expected_generation
            ):
                return False
            async with self.state.batch():
                await self.state.finish_proof_review_requests(
                    chapter.id,
                    proof_request_ids,
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.SUCCEEDED,
                    detail,
                )
            return True

    async def _invalidate_reviews(
        self,
        chapter_ids: Iterable[str],
        *,
        exclude: Iterable[str] = (),
        detail: str,
    ) -> set[str]:
        """Invalidate only reviews that received new findings."""

        targets = set(chapter_ids).difference(exclude)
        if not targets:
            return set()
        async with self._review_generation_lock:
            for chapter_id in targets:
                self._review_invalidation_generations[chapter_id] = (
                    self._review_invalidation_generation(chapter_id) + 1
                )
            self._invalidated_reviews.update(targets)
            changed_targets = {
                chapter_id
                for chapter_id in targets
                if self.state.task(chapter_id, Stage.REVIEW).status != TaskStatus.PENDING
            }
            if changed_targets:
                await self.state.set_tasks(
                    changed_targets,
                    Stage.REVIEW,
                    TaskStatus.PENDING,
                    detail,
                )
        return targets

    async def _review_build(self, chapter: WorkUnitLike) -> dict[str, str]:
        """Build review output once, returning diagnostics to review rather than formalization."""

        snapshots: dict[str, ValidatedBuildSnapshot] = {}
        result = (
            await self._build_chapters(
                (chapter,),
                publish_if_clean=True,
                mode="review-verification",
                stage=Stage.REVIEW,
                snapshots=snapshots,
            )
        )[chapter.id]
        if result.succeeded:
            if await self._publish_validated_build(chapter, snapshots[chapter.id]):
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "coordinator verification clean; continuing editing review",
                )
                return {}
            return {
                chapter.id: (
                    "The source changed after coordinator verification. Re-read the current "
                    "scope and complete the review against the fresh source."
                )
            }
        feedback = (await self._build_feedback_async({chapter.id: result})).actionable
        return feedback or {chapter.id: result.output}

    async def _queue_review_feedback(
        self,
        feedback: dict[str, str],
        *,
        origin: str,
        exclude_from_invalidation: Iterable[str] = (),
    ) -> tuple[str, set[str]]:
        """Persist follow-up work and reopen only its direct owners."""

        diagnostics = tuple(
            diagnostic for block in feedback.values() for diagnostic in _lean_diagnostics(block)
        )
        kind = (
            BUILD_WARNING_REVIEW_KIND
            if diagnostics and all(item.severity == "warning" for item in diagnostics)
            else BUILD_ERROR_REVIEW_KIND
        )
        request_id, created = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin,
            kind=kind,
        )
        if not created:
            return request_id, set()
        invalidated = await self._invalidate_reviews(
            feedback,
            exclude=exclude_from_invalidation,
            detail="review invalidated by follow-up findings",
        )
        return request_id, invalidated

    async def _review_chapter_to_clean(
        self,
        chapter: WorkUnitLike,
        rounds_used: dict[str, int],
        *,
        rerun: bool = False,
        feedback: str = "",
        role: str = "",
        proof_request_ids: tuple[str, ...] = (),
    ) -> StageOutcome:
        """Run at most five edit/rebuild cycles for one reviewable chapter."""

        review_generation = self._review_invalidation_generation(chapter.id)
        if self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED:
            return StageOutcome(ExecutionDisposition.SUCCEEDED)
        request_ids = list(proof_request_ids)
        review_feedback = feedback

        async def route_feedback(items: dict[str, str], *, origin: str) -> bool:
            nonlocal request_ids, review_feedback, role
            if not items:
                return True
            request_id, _ = await self._queue_review_feedback(
                items,
                origin=origin,
                exclude_from_invalidation={chapter.id},
            )
            if chapter.id in items:
                block = items[chapter.id]
                if role in DIAGNOSTIC_REVIEW_ROLES:
                    if request_id not in request_ids:
                        request_ids.append(request_id)
                    if block not in review_feedback:
                        review_feedback = (
                            f"{review_feedback}\n\n{block}" if review_feedback else block
                        )
                else:
                    # A newly discovered coordinator diagnostic takes precedence
                    # over initial review or proof-finding work. Keep those older
                    # requests durable for their own clean follow-up pass.
                    request_ids = [request_id]
                    review_feedback = block
                role = self._proof_review_role(request_ids)
            if chapter.id in items:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "review follow-up queued",
                )
            return True

        persisted = self.state.formalize_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        was_clean = await self._retained_formalize_record(chapter, records) is not None
        # Diagnostic reports and coordinator certification are separate facts. Remember a clean
        # verification obtained during this operation so a later dependency invalidation does not
        # turn a valid report into an agent-report retry.
        coordinator_verified = was_clean
        if not was_clean and not rerun:
            build_feedback = await self._review_build(chapter)
            if build_feedback and not await route_feedback(
                build_feedback,
                origin=f"review-build:{chapter.id}:{uuid4().hex[:12]}",
            ):
                return StageOutcome(ExecutionDisposition.FAILED)
            coordinator_verified = not build_feedback

        maximum = min(self.config.stages[Stage.REVIEW].max_rounds, 5)
        resume_thread_id: str | None = None
        resume_run_id = ""
        resume_prompt = ""

        async def queue_report_retry(outcome: StageOutcome, error: str) -> bool:
            nonlocal resume_thread_id, resume_run_id, resume_prompt
            if rounds_used[chapter.id] >= maximum:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.FAILED,
                    f"{error}; review report retry cap reached after {maximum} cycles",
                )
                return False
            run = next(
                (
                    item
                    for item in self.state.task(chapter.id, Stage.REVIEW).runs
                    if item.id == outcome.run_id
                ),
                None,
            )
            if run is not None and run.thread_id:
                resume_thread_id = run.thread_id
                resume_run_id = run.id
                resume_prompt = REVIEW_REPORT_RETRY_PROMPT.format(error=error)
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.RUNNING,
                (
                    "resuming review session after invalid report "
                    if resume_thread_id is not None
                    else "retrying review after invalid report "
                )
                + f"({rounds_used[chapter.id]}/{maximum})",
            )
            return True

        while rounds_used[chapter.id] < maximum:
            rounds_used[chapter.id] += 1
            review_rerun = rerun or rounds_used[chapter.id] > 1
            finding_guided = bool(review_feedback)
            attempt_feedback = review_feedback
            source_digest_before = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            review_options: dict[str, Any] = {"rerun": review_rerun}
            if review_feedback:
                review_options.update(
                    feedback=review_feedback,
                    role=role,
                    request_ids=request_ids,
                )
            if resume_thread_id is not None:
                review_options.update(
                    resume_thread_id=resume_thread_id,
                    resume_run_id=resume_run_id,
                    resume_prompt=resume_prompt,
                )
            outcome = await self._review_once(chapter, **review_options)
            source_digest_after = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            source_changed = source_digest_after != source_digest_before
            if source_changed:
                coordinator_verified = False
            resume_thread_id = None
            resume_run_id = ""
            resume_prompt = ""
            review_feedback = ""
            if outcome.report_error:
                review_feedback = attempt_feedback
                if source_changed:
                    malformed_build_feedback = await self._review_build(chapter)
                    if malformed_build_feedback and not await route_feedback(
                        malformed_build_feedback,
                        origin=f"review-build:{outcome.run_id or uuid4().hex[:12]}",
                    ):
                        return StageOutcome(ExecutionDisposition.FAILED)
                    coordinator_verified = not malformed_build_feedback
                if await queue_report_retry(outcome, outcome.report_error):
                    continue
            if outcome.failed:
                return StageOutcome(ExecutionDisposition.FAILED)
            expected_finding_ids = self._expected_proof_finding_ids(chapter.id, request_ids)
            if assessment_error := self._proof_review_assessment_error(
                chapter.id,
                outcome.run_id,
                expected_finding_ids,
            ):
                review_feedback = attempt_feedback
                if await queue_report_retry(outcome, assessment_error):
                    continue
                return StageOutcome(ExecutionDisposition.FAILED)
            build_feedback: dict[str, str] = {}
            if source_changed:
                build_feedback = await self._review_build(chapter)
                if build_feedback and not await route_feedback(
                    build_feedback,
                    origin=f"review-build:{outcome.run_id or uuid4().hex[:12]}",
                ):
                    return StageOutcome(ExecutionDisposition.FAILED)
                coordinator_verified = not build_feedback
            if review_feedback:
                if rounds_used[chapter.id] >= maximum:
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        f"review follow-up remained unresolved after {maximum} cycles",
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                continue
            if not outcome.complete:
                review_feedback = attempt_feedback
                if await queue_report_retry(
                    outcome, "review report marked the assignment incomplete"
                ):
                    continue
                return StageOutcome(ExecutionDisposition.FAILED)
            if role in DIAGNOSTIC_REVIEW_ROLES and not coordinator_verified:
                certification_feedback = await self._review_build(chapter)
                if certification_feedback:
                    if not await route_feedback(
                        certification_feedback,
                        origin=f"review-certification:{outcome.run_id or uuid4().hex[:12]}",
                    ):
                        return StageOutcome(ExecutionDisposition.FAILED)
                    if review_feedback:
                        if rounds_used[chapter.id] < maximum:
                            continue
                        await self.state.set_task(
                            chapter.id,
                            Stage.REVIEW,
                            TaskStatus.FAILED,
                            f"review follow-up remained unresolved after {maximum} cycles",
                        )
                        return StageOutcome(ExecutionDisposition.FAILED)
                    owner_ids = tuple(
                        owner_id for owner_id in certification_feedback if owner_id != chapter.id
                    )
                    if owner_ids:
                        requirements = tuple(
                            Requirement(
                                RequirementKind.COORDINATOR_OWNER,
                                owner_task_key=self.state.key(owner_id, Stage.REVIEW),
                                detail=f"coordinator diagnostic owned by {owner_id}",
                            )
                            for owner_id in owner_ids
                        )
                        await self.state.set_task_waiting(
                            chapter.id,
                            Stage.REVIEW,
                            requirements,
                            "waiting for upstream coordinator diagnostic owners",
                        )
                        return StageOutcome(ExecutionDisposition.WAITING, requirements)
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        "coordinator verification failed without actionable feedback",
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                coordinator_verified = True
            if finding_guided:
                completed = await self._complete_review(
                    chapter,
                    "targeted review completed with no pending findings",
                    expected_generation=review_generation,
                    proof_request_ids=request_ids,
                )
                return StageOutcome(
                    ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
                )
            if not source_changed:
                completed = await self._complete_review(
                    chapter,
                    "editing review found no actionable issues",
                    expected_generation=review_generation,
                    proof_request_ids=request_ids,
                )
                return StageOutcome(
                    ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
                )
        completed = await self._complete_review(
            chapter,
            f"review/rebuild cap reached after {maximum} cycles",
            expected_generation=review_generation,
            proof_request_ids=request_ids,
        )
        return StageOutcome(
            ExecutionDisposition.SUCCEEDED if completed else ExecutionDisposition.WAITING
        )

    async def _review_tree(
        self,
        *,
        rerun: bool = False,
        prove: bool = False,
        quarantined: Iterable[str] = (),
        formalize: RunningFormalizeStage | None = None,
    ) -> bool:
        """Release each review and proof when its local dependency frontier is done."""

        await self._recover_proof_review_requests()
        by_id = {chapter.id: chapter for chapter in self.work_units}
        quarantined_ids = set(quarantined).intersection(by_id)
        try:
            initial_graph = self._observed_work_unit_graph()
        except ValueError as error:
            self.state.scheduling["graph_failure"] = {
                "kind": "source_dependency_graph",
                "detail": str(error),
                "members": sorted(by_id),
                "updated_at": datetime.now(UTC).isoformat(),
            }
            await self.state.save("state")
            return False
        if self.force:
            await self._invalidate_reviews(
                by_id,
                detail="review explicitly forced by this invocation",
            )
        reviewed = {
            chapter.id
            for chapter in self.work_units
            if chapter.id not in quarantined_ids
            and self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
        }
        review_failures: set[str] = set(quarantined_ids)
        review_blocked: set[str] = set()
        attempted: set[str] = set()
        rounds_used = {chapter_id: 0 for chapter_id in by_id}
        review_tasks: dict[str, RunningReview] = {}
        rebuild_tasks: dict[str, asyncio.Task[bool]] = {}
        proof_tasks: dict[str, asyncio.Task[StageOutcome]] = {}
        failed_rebuilds: set[str] = set()
        persisted_clean = self.state.formalize_graph.get("clean", {})
        clean = await self._retain_formalize_clean(
            initial_graph,
            persisted_clean if isinstance(persisted_clean, dict) else {},
        )
        proof_results = {
            chapter_id: True
            for chapter_id in reviewed
            if (
                self.state.task(chapter_id, Stage.PROVE).status == TaskStatus.SUCCEEDED
                and isinstance(clean.get(chapter_id), dict)
                and self.state.task(chapter_id, Stage.PROVE).source_digest
                == clean[chapter_id].get("source_digest")
            )
        }
        proof_reviews = {chapter_id: 0 for chapter_id in by_id}
        formalize_failures_applied = False

        def formalize_ready(chapter_id: str) -> bool:
            return self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED

        async def cancel_all() -> None:
            tasks: list[asyncio.Task[Any]] = [handle.task for handle in review_tasks.values()]
            tasks.extend(rebuild_tasks.values())
            tasks.extend(proof_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                if formalize is not None:
                    # Clear before inspecting state so a subsequent formalize
                    # transition cannot be lost between the scan and wait.
                    formalize.progress.clear()
                try:
                    graph = self._observed_work_unit_graph()
                except ValueError as error:
                    self.state.scheduling["graph_failure"] = {
                        "kind": "source_dependency_graph",
                        "detail": str(error),
                        "members": sorted(by_id),
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                    await self.state.save("state")
                    return False

                if (
                    formalize is not None
                    and formalize.task.done()
                    and not formalize_failures_applied
                ):
                    # Propagate orchestration failures, then quarantine only
                    # chapters whose own formalization did not succeed. Independent
                    # clean branches remain eligible for review and proof.
                    formalize.task.result()
                    failed_formalizations = {
                        chapter_id
                        for chapter_id in formalize.target_ids
                        if self.state.task(chapter_id, Stage.FORMALIZE).status
                        != TaskStatus.SUCCEEDED
                    }
                    if failed_formalizations:
                        reviewed.difference_update(failed_formalizations)
                        review_failures.update(failed_formalizations)
                        cancelled_proofs = [
                            proof_tasks.pop(chapter_id)
                            for chapter_id in failed_formalizations
                            if chapter_id in proof_tasks
                        ]
                        for task in cancelled_proofs:
                            task.cancel()
                        await asyncio.gather(*cancelled_proofs, return_exceptions=True)
                        for chapter_id in failed_formalizations:
                            proof_results.pop(chapter_id, None)
                        await self._invalidate_reviews(
                            failed_formalizations,
                            detail="review blocked by failed formalization",
                        )
                    formalize_failures_applied = True

                # Pull durable successes into readiness and remove reviews with
                # direct findings. Source edits separately trigger build rechecks.
                reviewed.difference_update(self._invalidated_reviews)
                self._invalidated_reviews.clear()
                new_rechecks = set(self._proof_rechecks)
                self._proof_rechecks.clear()
                failed_rebuilds.difference_update(new_rechecks)
                dirty_value = self.state.formalize_graph.get("dirty", ())
                dirty_builds = set(dirty_value) if isinstance(dirty_value, list) else set()
                for chapter_id in dirty_builds:
                    proof_results.pop(chapter_id, None)
                reviewed.update(
                    chapter_id
                    for chapter_id in by_id
                    if self.state.task(chapter_id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
                )
                for chapter_id in tuple(proof_results):
                    if chapter_id not in reviewed:
                        proof_results.pop(chapter_id, None)
                # Findings reopen only their direct owners. Descendant agents
                # that were already released against the previous clean build
                # remain pinned to that snapshot and are allowed to drain.
                stale_reviews = [
                    chapter_id
                    for chapter_id, handle in review_tasks.items()
                    if self.state.task(chapter_id, Stage.REVIEW).status
                    not in {TaskStatus.RUNNING, TaskStatus.SUCCEEDED}
                ]
                cancelled_reviews: list[asyncio.Task[Any]] = []
                for chapter_id in stale_reviews:
                    handle = review_tasks.pop(chapter_id)
                    handle.task.cancel()
                    cancelled_reviews.append(handle.task)
                if cancelled_reviews:
                    await asyncio.gather(*cancelled_reviews, return_exceptions=True)
                    async with self.state.batch():
                        for chapter_id in stale_reviews:
                            if (
                                self.state.task(chapter_id, Stage.REVIEW).status
                                == TaskStatus.RUNNING
                            ):
                                await self.state.set_task(
                                    chapter_id,
                                    Stage.REVIEW,
                                    TaskStatus.PENDING,
                                    "prerequisite review was invalidated during review",
                                )

                review_frontiers_ready = self._dependency_frontiers_ready(graph, reviewed)
                for chapter_id in graph.order:
                    review_task = self.state.task(chapter_id, Stage.REVIEW)
                    # A forced pipeline run is not itself evidence that this node has
                    # already been reviewed. Only actual prior/active review work may
                    # bypass dependency-review ordering.
                    rereview = chapter_id in attempted or review_task.rounds > 0
                    if (
                        chapter_id not in reviewed
                        and chapter_id not in review_failures
                        and chapter_id not in review_blocked
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and formalize_ready(chapter_id)
                        and self.state.readiness(review_task).ready
                        and (rereview or chapter_id in review_frontiers_ready)
                    ):
                        dependencies = graph.dependencies[chapter_id]
                        proof_feedback, proof_request_ids = self._proof_review_feedback(chapter_id)
                        proof_review_role = self._proof_review_role(proof_request_ids)
                        review_rerun = rerun or chapter_id in attempted or review_task.rounds > 0
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.RUNNING,
                            (
                                "non-sorry warning cleanup queued"
                                if proof_review_role == WARNING_REVIEW_ROLE
                                else "coordinator diagnostic repair queued"
                                if proof_review_role == DIAGNOSTIC_REVIEW_ROLE
                                else "failed-proof statement re-review queued"
                                if proof_feedback
                                else "targeted re-review queued"
                                if rereview
                                else "waiting for dependency-ordered coordinator build"
                            ),
                        )
                        review_feedback_options: dict[str, Any] = {
                            "rerun": review_rerun,
                            "feedback": proof_feedback,
                            "proof_request_ids": proof_request_ids,
                        }
                        if proof_review_role:
                            review_feedback_options["role"] = proof_review_role
                        review_operation = (
                            self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                **review_feedback_options,
                            )
                            if proof_feedback
                            else self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                rerun=review_rerun,
                            )
                        )
                        review_tasks[chapter_id] = RunningReview(
                            task=asyncio.create_task(review_operation),
                            dependencies=dependencies,
                            proof_request_ids=proof_request_ids,
                        )
                        attempted.add(chapter_id)

                for chapter_id in graph.order:
                    if (
                        chapter_id in dirty_builds
                        and chapter_id not in failed_rebuilds
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and formalize_ready(chapter_id)
                    ):
                        rebuild_tasks[chapter_id] = asyncio.create_task(
                            self._rebuild_dirty_chapter(by_id[chapter_id])
                        )

                if prove:
                    for chapter_id in graph.order:
                        if (
                            chapter_id in reviewed
                            and chapter_id not in proof_results
                            and chapter_id not in proof_tasks
                            and chapter_id not in review_tasks
                            and chapter_id not in rebuild_tasks
                            and formalize_ready(chapter_id)
                            and self.state.readiness(self.state.task(chapter_id, Stage.PROVE)).ready
                        ):
                            proof_tasks[chapter_id] = asyncio.create_task(
                                self._prove(by_id[chapter_id], defer_review=True)
                            )

                live_tasks: list[asyncio.Task[Any]] = [
                    handle.task for handle in review_tasks.values()
                ]
                live_tasks.extend(rebuild_tasks.values())
                live_tasks.extend(proof_tasks.values())
                progress_waiter: asyncio.Task[bool] | None = None
                if formalize is not None and not formalize.task.done():
                    progress_waiter = asyncio.create_task(formalize.progress.wait())
                    live_tasks.extend((formalize.task, progress_waiter))
                if not live_tasks:
                    unresolved = set(by_id).difference(reviewed | review_failures | review_blocked)
                    if unresolved:
                        if not review_failures:
                            raise RuntimeError(
                                "review scheduler has unresolved chapters but no runnable tasks"
                            )
                        review_blocked.update(unresolved)
                        break
                    if not prove or all(chapter_id in proof_results for chapter_id in reviewed):
                        break
                    raise RuntimeError(
                        "review scheduler has unfinished proofs but no runnable tasks"
                    )
                done, _ = await asyncio.wait(live_tasks, return_when=asyncio.FIRST_COMPLETED)
                if progress_waiter is not None and progress_waiter not in done:
                    progress_waiter.cancel()
                    await asyncio.gather(progress_waiter, return_exceptions=True)

                completed_reviews = [
                    chapter_id for chapter_id, handle in review_tasks.items() if handle.task in done
                ]
                for chapter_id in completed_reviews:
                    handle = review_tasks.pop(chapter_id)
                    outcome = handle.task.result()
                    if outcome.waiting:
                        rounds_used[chapter_id] = 0
                        continue
                    if outcome.failed:
                        task_record = self.state.task(chapter_id, Stage.REVIEW)
                        if task_record.status == TaskStatus.PENDING:
                            rounds_used[chapter_id] = 0
                            continue
                        review_failures.add(chapter_id)
                        if task_record.status != TaskStatus.FAILED:
                            await self.state.set_task(
                                chapter_id,
                                Stage.REVIEW,
                                TaskStatus.FAILED,
                                "chapter-local review failed; unrelated branches continue",
                            )
                        continue
                    current_graph = self._observed_work_unit_graph()
                    current_dependencies = current_graph.dependencies[chapter_id]
                    if not current_dependencies.issubset(handle.dependencies):
                        continue
                    if self.state.task(chapter_id, Stage.REVIEW).status != TaskStatus.SUCCEEDED:
                        await self._complete_review(
                            by_id[chapter_id],
                            "editing review completed",
                            proof_request_ids=handle.proof_request_ids,
                        )
                    remaining_feedback, _ = self._proof_review_feedback(chapter_id)
                    if remaining_feedback:
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "additional reason-specific re-review queued",
                        )
                        continue
                    reviewed.add(chapter_id)

                completed_rebuilds = [
                    chapter_id for chapter_id, task in rebuild_tasks.items() if task in done
                ]
                for chapter_id in completed_rebuilds:
                    succeeded = rebuild_tasks.pop(chapter_id).result()
                    if not succeeded:
                        failed_rebuilds.add(chapter_id)

                completed_proofs = [
                    chapter_id for chapter_id, task in proof_tasks.items() if task in done
                ]
                for chapter_id in completed_proofs:
                    if chapter_id not in proof_tasks:
                        continue
                    proof_outcome = proof_tasks.pop(chapter_id).result()
                    if proof_outcome.succeeded:
                        proof_results[chapter_id] = True
                        continue
                    if proof_outcome.waiting:
                        proof_results.pop(chapter_id, None)
                        continue

                    proof_task = self.state.task(chapter_id, Stage.PROVE)
                    primary_runs = [run for run in proof_task.runs if not run.auxiliary]
                    report = primary_runs[-1].report if primary_runs else None
                    if not isinstance(report, dict) or not report.get("failed_attempts"):
                        proof_results[chapter_id] = False
                        continue
                    if proof_reviews[chapter_id] >= self.config.stages[Stage.REVIEW].max_rounds:
                        proof_results[chapter_id] = False
                        await self.state.set_task(
                            chapter_id,
                            Stage.PROVE,
                            TaskStatus.FAILED,
                            "proof findings persisted after the review retry cap",
                        )
                        continue
                    proof_reviews[chapter_id] += 1

                    chapter = by_id[chapter_id]
                    proof_run = primary_runs[-1]
                    invalidated = await self._queue_proof_review(
                        chapter,
                        report,
                        origin_run_id=proof_run.id,
                    )
                    proof_request_ids = self._proof_review_feedback(chapter_id)[1]
                    if proof_request_ids:
                        await self.state.set_task_waiting(
                            chapter_id,
                            Stage.PROVE,
                            (
                                Requirement(
                                    RequirementKind.PROOF_REVIEW_REQUEST,
                                    owner_task_key=self.state.key(chapter_id, Stage.REVIEW),
                                    request_id=request_id,
                                    detail="waiting for proof-review findings",
                                )
                                for request_id in proof_request_ids
                            ),
                            "waiting for proof-review findings to be resolved",
                        )

                    cancelled: list[asyncio.Task[Any]] = []
                    for invalidated_id in invalidated:
                        if handle := review_tasks.pop(invalidated_id, None):
                            handle.task.cancel()
                            cancelled.append(handle.task)
                        reviewed.discard(invalidated_id)
                        rounds_used[invalidated_id] = 0
                    await asyncio.gather(*cancelled, return_exceptions=True)
        finally:
            await cancel_all()

        # Proofs are terminal leaves in the task graph. Their durable FAILED status
        # remains visible and retryable, but cannot block any downstream work
        # or turn an otherwise drained review tree into an orchestration failure.
        return not review_failures and not review_blocked

    async def _review_until_clean(self, *, rerun: bool = False) -> bool:
        return await self._review_tree(rerun=rerun)

    async def _refresh_stale_proof_build(
        self,
        chapter: WorkUnitLike,
    ) -> ValidationResult:
        """Refresh an exact coordinator build before launching a proof agent."""

        await self.control.checkpoint()
        snapshots: dict[str, ValidatedBuildSnapshot] = {}
        validation = (
            await self._build_chapters(
                (chapter,),
                publish_if_clean=True,
                mode="proof-refresh",
                stage=Stage.PROVE,
                snapshots=snapshots,
            )
        )[chapter.id]
        if validation.succeeded and not await self._publish_validated_build(
            chapter, snapshots[chapter.id]
        ):
            return ValidationResult(
                False,
                1,
                "Source scope changed after the coordinator build; retry required.",
                status=ValidationStatus.STALE_SNAPSHOT,
            )
        return validation

    async def _rebuild_dirty_chapter(self, chapter: WorkUnitLike) -> bool:
        """Refresh one invalidated exact build while its chapter has no agent."""

        validation = await self._refresh_stale_proof_build(chapter)
        return validation.succeeded

    async def _prove(self, chapter: WorkUnitLike, *, defer_review: bool = False) -> StageOutcome:
        build_fresh = False
        if not self.force:
            graph = self._observed_work_unit_graph()
            persisted = self.state.formalize_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            proof_task = self.state.task(chapter.id, Stage.PROVE)
            record = await self._retained_formalize_record(chapter, records)
            dependencies_current = self._interface_dependencies_are_current(graph, chapter.id)
            if (
                proof_task.status == TaskStatus.SUCCEEDED
                and isinstance(record, dict)
                and proof_task.source_digest == record.get("source_digest")
                and dependencies_current
            ):
                await self._close_previously_satisfied_upstream_requests(
                    chapter,
                    build_fresh=True,
                )
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            files = scoped_files(self.config.settings.repo, chapter)
            if files:
                placeholders = await asyncio.to_thread(
                    count_placeholders, self.config.settings.repo, chapter
                )
                build_fresh = isinstance(record, dict) and dependencies_current
                if not build_fresh:
                    revalidation = await self._refresh_stale_proof_build(chapter)
                    if not revalidation.succeeded:
                        routed = (
                            await self._build_feedback_async({chapter.id: revalidation})
                        ).actionable
                        diagnostic_feedback = routed or {
                            chapter.id: (
                                "Coordinator validation of the current sources failed before proof "
                                "work:\n" + revalidation.output
                            )
                        }
                        origin = (
                            f"proof-refresh:{chapter.id}:"
                            + hashlib.sha256(revalidation.output.encode()).hexdigest()[:12]
                        )
                        await self._queue_review_feedback(
                            diagnostic_feedback,
                            origin=origin,
                        )
                        await self.state.set_task(
                            chapter.id,
                            Stage.PROVE,
                            TaskStatus.PENDING,
                            "pre-existing coordinator diagnostics routed before proof work",
                        )
                        return StageOutcome(
                            ExecutionDisposition.WAITING,
                            (
                                Requirement(
                                    RequirementKind.PROOF_REVIEW_REQUEST,
                                    detail="coordinator diagnostics require review",
                                ),
                            ),
                        )
                    else:
                        build_fresh = True
                        if placeholders > 0:
                            await self.state.set_task(
                                chapter.id,
                                Stage.PROVE,
                                TaskStatus.PENDING,
                                f"fresh exact build confirmed; {placeholders} placeholders remain",
                            )
                if placeholders == 0 and build_fresh:
                    await self._close_previously_satisfied_upstream_requests(
                        chapter,
                        build_fresh=True,
                    )
                    source_digest = await asyncio.to_thread(
                        scope_digest, self.config.settings.repo, chapter
                    )
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.SUCCEEDED,
                        "placeholder-free sources validated without an agent",
                        source_digest=source_digest,
                    )
                    return StageOutcome(ExecutionDisposition.SUCCEEDED)
        proof_maximum = self.config.stages[Stage.PROVE].max_rounds
        proof_chunk_size = self.config.stages[Stage.PROVE].chunk_size or 4
        discovered_targets = await asyncio.to_thread(
            proof_targets, self.config.settings.repo, chapter
        )
        if build_fresh:
            await self._resolve_obsolete_dependency_blockers(chapter.id)
        chunked_proofs = bool(discovered_targets)
        assigned_targets: tuple[ProofTarget, ...] = ()
        chunk_round = 0
        skipped_target_ids: set[str] = set()

        def matches_target(value: dict[str, Any], target: ProofTarget) -> bool:
            path = str(value.get("path", value.get("consumer_path", "")))
            declaration = str(value.get("declaration", value.get("blocked_declaration", "")))
            same_declaration = declaration == target.declaration or declaration.endswith(
                "." + target.declaration
            )
            return path == target.path and same_declaration

        def unavailable_target_ids(targets: Iterable[ProofTarget]) -> set[str]:
            blockers = self.state.proof_blockers_for_consumer(chapter.id, active_only=False)
            return {
                target.fingerprint
                for target in targets
                if any(
                    blocker.get("status") != ProofBlockerStatus.OPEN.value
                    and matches_target(blocker, target)
                    for blocker in blockers
                )
            }

        pending_review = any(
            isinstance(value, dict)
            and isinstance(value.get("feedback"), dict)
            and chapter.id in value["feedback"]
            for value in self.state.proof_review_requests.values()
        )
        if not pending_review:
            reviewed_blockers = (
                str(blocker["id"])
                for blocker in self.state.proof_blockers_for_consumer(chapter.id, active_only=False)
                if blocker.get("status") == ProofBlockerStatus.REVIEW_REQUESTED.value
            )
            await self.state.set_proof_blocker_status(reviewed_blockers, ProofBlockerStatus.OPEN)
        feedback = ""
        feedback_ledger: deque[str] = deque(maxlen=PROOF_FEEDBACK_ROUNDS)
        stalled_rounds = 0
        previous_placeholders: int | None = None
        proof_round = 0
        await self._close_previously_satisfied_upstream_requests(
            chapter,
            build_fresh=build_fresh,
        )
        durable_requests = self.state.upstream_requests_for_consumer(
            chapter.id,
            statuses=(
                UpstreamRequestStatus.REQUESTED,
                UpstreamRequestStatus.ANSWERED,
                UpstreamRequestStatus.ESCALATED,
            ),
        )
        durable_ids = tuple(str(request["id"]) for request in durable_requests)
        answered_ids = await self._ensure_upstream_answers(durable_ids)
        escalated = tuple(
            request_id
            for request_id in durable_ids
            if self.state.upstream_requests[request_id].get("status")
            == UpstreamRequestStatus.ESCALATED.value
        )
        if escalated:
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.FAILED,
                "upstream request requires manual escalation: " + ", ".join(escalated),
            )
            return StageOutcome(ExecutionDisposition.FAILED)
        targeted_request_ids = answered_ids
        proof_resume_thread_id: str | None = None
        proof_resume_run_id = ""
        proof_resume_prompt = ""
        if targeted_request_ids:
            feedback = self._upstream_retry_feedback(
                targeted_request_ids,
                _bounded_proof_feedback(feedback_ledger),
            )
        while chunked_proofs or proof_round < proof_maximum or targeted_request_ids:
            targeted_retry = bool(targeted_request_ids)
            if targeted_retry:
                # The durable fact remains `answered` until this fresh run has a terminal result.
                targeted_request_ids = tuple(
                    request_id
                    for request_id in targeted_request_ids
                    if self.state.upstream_requests[request_id].get("status")
                    == UpstreamRequestStatus.ANSWERED.value
                )
                targeted_retry = bool(targeted_request_ids)
                if not targeted_retry:
                    continue
            if chunked_proofs and not assigned_targets:
                discovered_targets = await asyncio.to_thread(
                    proof_targets, self.config.settings.repo, chapter
                )
                if not discovered_targets:
                    remaining_placeholders = await asyncio.to_thread(
                        count_placeholders, self.config.settings.repo, chapter
                    )
                    if remaining_placeholders == 0:
                        source_digest = await asyncio.to_thread(
                            scope_digest, self.config.settings.repo, chapter
                        )
                        await self._resolve_satisfied_proof_blockers(chapter)
                        await self.state.set_task(
                            chapter.id,
                            Stage.PROVE,
                            TaskStatus.SUCCEEDED,
                            "all proof chunks completed and chapter elaborates",
                            source_digest=source_digest,
                        )
                        return StageOutcome(ExecutionDisposition.SUCCEEDED)
                    # Keep unusual declaration syntax safe: after all recognized chunks, hand any
                    # remaining raw placeholders to the established whole-chapter fallback.
                    chunked_proofs = False
                    proof_round = 0
                    assigned_targets = ()
                if chunked_proofs:
                    unavailable = skipped_target_ids | unavailable_target_ids(discovered_targets)
                    candidates = tuple(
                        target
                        for target in discovered_targets
                        if target.fingerprint not in unavailable
                    )
                    if not candidates:
                        break
                    if targeted_retry:
                        requests = tuple(
                            self.state.upstream_requests[request_id]
                            for request_id in targeted_request_ids
                            if request_id in self.state.upstream_requests
                        )
                        requested = tuple(
                            target
                            for target in candidates
                            if any(matches_target(request, target) for request in requests)
                        )
                        if requested:
                            candidates = requested
                    assigned_targets = proof_target_chunk(candidates, proof_chunk_size)
                    chunk_round = 0
                    if not feedback and (
                        durable_feedback := self._durable_blocker_feedback(
                            chapter.id, assigned_targets
                        )
                    ):
                        feedback_ledger.append(durable_feedback)
                        feedback = _bounded_proof_feedback(feedback_ledger)
            try:
                attempt = await self._attempt(
                    chapter,
                    Stage.PROVE,
                    feedback=feedback,
                    queue_detail=(
                        "targeted downstream retry for upstream request(s): "
                        + ", ".join(targeted_request_ids)
                        if targeted_retry
                        else (
                            f"proof chunk retry {chunk_round + 1}/{proof_maximum}: "
                            + ", ".join(target.declaration for target in assigned_targets)
                            if chunked_proofs
                            else f"proof round {proof_round + 1}/{proof_maximum}"
                        )
                    ),
                    role=DOWNSTREAM_RETRY_ROLE if targeted_retry else "",
                    request_ids=targeted_request_ids,
                    proof_targets=assigned_targets,
                    resume_thread_id=proof_resume_thread_id,
                    resume_run_id=proof_resume_run_id,
                    resume_prompt=proof_resume_prompt,
                )
            except GitCommitError as error:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    f"proof retry deferred until dirty exclusive scope is reconciled: {error}",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.BUILD_FRESHNESS,
                            detail="dirty exclusive scope must be reconciled",
                        ),
                    ),
                )
            if attempt.agent.capacity_exhausted:
                if targeted_retry:
                    await self.state.finish_upstream_requests(
                        targeted_request_ids,
                        run_id=attempt.run.id,
                        succeeded_ids=(),
                        error="targeted downstream retry exhausted model capacity",
                    )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    (
                        "targeted downstream retry requires manual escalation"
                        if targeted_retry
                        else "model capacity remained unavailable after the configured retries"
                    ),
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            coordinator_validation = attempt.validation
            if chunked_proofs and assigned_targets:
                validation_targets = await asyncio.to_thread(
                    proof_target_spans,
                    self.config.settings.repo,
                    chapter,
                    assigned_targets,
                )
                attempt = replace(
                    attempt,
                    validation=self._proof_chunk_validation(
                        coordinator_validation,
                        validation_targets,
                    ),
                )
            proof_resume_thread_id = None
            proof_resume_run_id = ""
            proof_resume_prompt = ""
            proof_round += 1
            if chunked_proofs:
                chunk_round += 1
            routed_validation_output = attempt.validation.output
            foreign_validation_feedback: dict[str, str] = {}
            if not coordinator_validation.succeeded:
                routed_validation = (
                    await self._build_feedback_async({chapter.id: coordinator_validation})
                ).actionable
                local_validation_output = routed_validation.pop(chapter.id, "")
                foreign_validation_feedback = routed_validation
                if not attempt.validation.succeeded:
                    routed_validation_output = local_validation_output or attempt.validation.output
                else:
                    # Mainline scopes chunk retries to their assigned declarations.
                    # A same-chapter diagnostic outside those spans must not spend
                    # this chunk's budget, though diagnostics owned by other work
                    # units still need to be routed below.
                    routed_validation_output = ""
                if (
                    not routed_validation_output
                    and not foreign_validation_feedback
                    and not attempt.validation.succeeded
                ):
                    # Keep unattributed compiler failures with the originating
                    # proof assignment instead of silently dropping them.
                    routed_validation_output = attempt.validation.output
                if foreign_validation_feedback:
                    await self._queue_review_feedback(
                        foreign_validation_feedback,
                        origin=f"proof-validation:{attempt.run.id}",
                    )
                if (
                    not attempt.validation.succeeded
                    and routed_validation_output
                    and attempt.agent.thread_id
                ):
                    proof_resume_thread_id = attempt.agent.thread_id
                    proof_resume_run_id = attempt.run.id
                    proof_resume_prompt = PROOF_VALIDATION_RETRY_PROMPT
            if targeted_retry:
                succeeded_request_ids = self._succeeded_upstream_request_ids(
                    targeted_request_ids,
                    attempt.validation,
                )
                unresolved = set(targeted_request_ids).difference(succeeded_request_ids)
                retry_error = ""
                if unresolved:
                    retry_error = (
                        "blocked declaration remained unresolved after a clean targeted retry"
                        if attempt.validation.succeeded
                        else "targeted retry did not validate: "
                        + (
                            routed_validation_output
                            or "diagnostics were routed to their source owners"
                        )[-4000:]
                    )
                await self.state.finish_upstream_requests(
                    targeted_request_ids,
                    run_id=attempt.run.id,
                    succeeded_ids=succeeded_request_ids,
                    error=retry_error,
                )
                targeted_request_ids = ()
                if unresolved:
                    feedback_ledger.append(
                        f"Targeted downstream retry {proof_round}:\n"
                        + attempt.feedback(validation_output=routed_validation_output)
                    )
                    await self._record_upstream_requests(
                        chapter,
                        attempt.run,
                        attempt.agent.report,
                        previous_attempts=_bounded_proof_feedback(feedback_ledger),
                        forced_escalation=(
                            "targeted downstream retry requested another upstream cycle; "
                            "manual evaluation is required"
                        ),
                    )
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.FAILED,
                        "targeted downstream retry did not prove: " + ", ".join(sorted(unresolved)),
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
            if (
                not coordinator_validation.succeeded
                and attempt.validation.succeeded
                and not routed_validation_output
                and foreign_validation_feedback
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof validation diagnostics routed to their source owners",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            detail="validation diagnostics were routed to source owners",
                        ),
                    ),
                )
            remaining_targets = (
                await asyncio.to_thread(proof_targets, self.config.settings.repo, chapter)
                if chunked_proofs
                else ()
            )
            remaining_placeholder_count = await asyncio.to_thread(
                count_placeholders, self.config.settings.repo, chapter
            )
            assigned_ids = {target.fingerprint for target in assigned_targets}
            remaining_assigned = tuple(
                target for target in remaining_targets if target.fingerprint in assigned_ids
            )
            if (
                attempt.agent.succeeded
                and coordinator_validation.succeeded
                and (
                    remaining_placeholder_count == 0
                    if chunked_proofs
                    else attempt.agent.placeholders == 0
                )
            ):
                await self._resolve_satisfied_proof_blockers(chapter)
                source_digest = await asyncio.to_thread(
                    scope_digest, self.config.settings.repo, chapter
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    "no placeholders and chapter elaborates",
                    source_digest=source_digest,
                )
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            if (
                chunked_proofs
                and assigned_targets
                and not remaining_assigned
                and remaining_placeholder_count == 0
                and attempt.agent.succeeded
                and attempt.validation.succeeded
                and not coordinator_validation.succeeded
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "all proof chunks completed; coordinator diagnostics remain outside the "
                    "assigned declarations",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.BUILD_FRESHNESS,
                            detail="coordinator diagnostics remain outside the proof chunk",
                        ),
                    ),
                )
            if (
                chunked_proofs
                and assigned_targets
                and not remaining_assigned
                and attempt.agent.succeeded
                and attempt.validation.succeeded
            ):
                await self._resolve_satisfied_proof_blockers(chapter)
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    f"completed proof chunk with {len(remaining_targets)} declaration(s) remaining",
                )
                assigned_targets = ()
                chunk_round = 0
                feedback_ledger.clear()
                feedback = ""
                continue

            feedback_ledger.clear()
            feedback_ledger.append(
                f"Proof attempt {proof_round}:\n"
                + attempt.feedback(validation_output=routed_validation_output)
            )
            blockers = await self._record_proof_blocker_deltas(
                chapter, attempt.run, attempt.agent.report
            )
            if durable_feedback := self._durable_blocker_feedback(chapter.id, assigned_targets):
                feedback_ledger.append(durable_feedback)
            feedback = _bounded_proof_feedback(feedback_ledger)
            raw_upstream = attempt.agent.report.get("upstream_requests")
            reported_upstream = (
                tuple(item for item in raw_upstream if isinstance(item, dict))
                if isinstance(raw_upstream, list)
                else ()
            )
            attached_upstream = tuple(
                blocker.get("upstream_candidate")
                for blocker in blockers
                if isinstance(blocker.get("upstream_candidate"), dict)
            )
            unmatched_upstream = tuple(
                item for item in reported_upstream if item not in attached_upstream
            )
            upstream_request_ids = (
                await self._record_upstream_requests(
                    chapter,
                    attempt.run,
                    {"upstream_requests": list(unmatched_upstream)},
                    previous_attempts=feedback,
                )
                if unmatched_upstream
                else ()
            )
            review_queued = False
            terminal_blockers: list[str] = []
            for blocker in blockers:
                sightings = int(blocker.get("sightings", 0))
                retry_baseline = int(blocker.get("retry_sighting_baseline", 0))
                if sightings - retry_baseline < 2:
                    continue
                blocker_id = str(blocker.get("id", ""))
                candidate = blocker.get("upstream_candidate")
                if isinstance(candidate, dict):
                    ids = await self._record_upstream_requests(
                        chapter,
                        attempt.run,
                        {"upstream_requests": [candidate]},
                        previous_attempts=feedback,
                    )
                    upstream_request_ids += ids
                    await self.state.set_proof_blocker_status(
                        (blocker_id,),
                        ProofBlockerStatus.UPSTREAM_REQUESTED,
                        request_id=ids[0] if ids else "",
                    )
                elif self._blocker_needs_review(blocker):
                    report = self._blocker_report(blocker)
                    await self._queue_proof_review(
                        chapter,
                        report,
                        origin_run_id=attempt.run.id,
                    )
                    await self.state.set_proof_blocker_status(
                        (blocker_id,), ProofBlockerStatus.REVIEW_REQUESTED
                    )
                    review_queued = True
                else:
                    await self.state.set_proof_blocker_status(
                        (blocker_id,), ProofBlockerStatus.BLOCKED
                    )
                    terminal_blockers.append(blocker_id)
            upstream_request_ids = tuple(dict.fromkeys(upstream_request_ids))
            if review_queued:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "unchanged statement/interface blocker queued for focused review",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    tuple(
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            request_id=request_id,
                            detail="proof blocker queued for focused review",
                        )
                        for request_id in self._proof_review_feedback(chapter.id)[1]
                    ),
                )
            if upstream_request_ids:
                answered_ids = await self._ensure_upstream_answers(upstream_request_ids)
                escalated = tuple(
                    request_id
                    for request_id in upstream_request_ids
                    if self.state.upstream_requests[request_id].get("status")
                    == UpstreamRequestStatus.ESCALATED.value
                )
                if escalated:
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.FAILED,
                        "upstream request requires manual escalation: " + ", ".join(escalated),
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                targeted_request_ids = answered_ids
                feedback = self._upstream_retry_feedback(
                    targeted_request_ids,
                    feedback,
                )
                continue
            if (
                chunked_proofs
                and terminal_blockers
                and not self.state.proof_blockers_for_consumer(chapter.id)
            ):
                skipped_target_ids.update(target.fingerprint for target in assigned_targets)
                assigned_targets = ()
                chunk_round = 0
                feedback_ledger.clear()
                feedback = ""
                continue
            if terminal_blockers and not self.state.proof_blockers_for_consumer(chapter.id):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "unchanged proof blocker(s): " + ", ".join(terminal_blockers),
                )
                return StageOutcome(ExecutionDisposition.FAILED)
            placeholders = attempt.agent.placeholders
            validation_retry_exhausted = (
                not attempt.validation.succeeded
                and bool(routed_validation_output)
                and (
                    chunk_round >= proof_maximum if chunked_proofs else proof_round >= proof_maximum
                )
            )
            if validation_retry_exhausted:
                await self._queue_review_feedback(
                    {chapter.id: routed_validation_output},
                    origin=f"proof-validation-exhausted:{attempt.run.id}",
                )
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof-local validation retries exhausted; diagnostic repair queued",
                )
                return StageOutcome(
                    ExecutionDisposition.WAITING,
                    (
                        Requirement(
                            RequirementKind.PROOF_REVIEW_REQUEST,
                            detail="proof-local diagnostic repair queued",
                        ),
                    ),
                )
            if chunked_proofs:
                if chunk_round >= proof_maximum:
                    skipped_target_ids.update(target.fingerprint for target in assigned_targets)
                    assigned_targets = ()
                    chunk_round = 0
                    feedback_ledger.clear()
                    feedback = ""
                elif remaining_assigned:
                    # Refresh line numbers and placeholder counts after partial progress while
                    # retaining the retry budget for this logical chunk.
                    assigned_targets = remaining_assigned
                continue
            if previous_placeholders is not None and placeholders >= previous_placeholders:
                stalled_rounds += 1
            else:
                stalled_rounds = 0
            previous_placeholders = placeholders
            if stalled_rounds >= 2:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    f"proof pass stalled with {placeholders} placeholders",
                )
                return StageOutcome(ExecutionDisposition.FAILED)

        unresolved_placeholders = await asyncio.to_thread(
            count_placeholders, self.config.settings.repo, chapter
        )
        await self.state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.FAILED,
            (
                "proof chunks exhausted retries with "
                f"{unresolved_placeholders} "
                "placeholder(s) remaining; durable blockers retained"
                if chunked_proofs
                else (
                    f"proof pass did not converge in {proof_maximum} rounds; "
                    "durable blockers retained"
                )
            ),
        )
        return StageOutcome(ExecutionDisposition.FAILED)

    def _repair_cases(self, *, periodic: bool) -> list[RepairCaseRecord]:
        selected = {chapter.id for chapter in self.work_units}
        cases: list[RepairCaseRecord] = []
        for task_key, task in self.state.shepherd_repairable_tasks():
            if task.chapter_id not in selected:
                continue
            case = self.state.ensure_repair_case(task_key)
            if case.status == RepairCaseStatus.OPEN or (
                periodic and case.status == RepairCaseStatus.EXHAUSTED
            ):
                cases.append(case)
        return cases

    def _repair_failure_evidence(self, case: RepairCaseRecord) -> dict[str, Any]:
        task = self.state.tasks[case.task_key]
        run = task.runs[-1] if task.runs else None
        report: dict[str, Any] = {}
        validation: dict[str, Any] = {}
        if run is not None and isinstance(run.report, dict):
            for key in (
                "summary",
                "issues",
                "failed_attempts",
                "upstream_requests",
                "finding_assessments",
            ):
                if key in run.report:
                    report[key] = run.report[key]
        if run is not None and isinstance(run.validation, dict):
            validation = dict(run.validation)
            output = validation.get("output")
            if isinstance(output, str) and len(output) > 12_000:
                validation["output"] = output[-12_000:]
        return {
            "case_id": case.id,
            "task_key": case.task_key,
            "work_unit_id": case.chapter_id,
            "stage": case.stage,
            "status": str(task.status),
            "detail": task.detail,
            "updated_at": task.updated_at,
            "latest_run": (
                {
                    "id": run.id,
                    "role": run.role,
                    "model": run.model,
                    "exit_code": run.exit_code,
                    "changed": run.changed,
                    "placeholders": run.placeholders,
                    "report": report,
                    "validation": validation,
                }
                if run is not None
                else None
            ),
        }

    async def _reconcile_stale_formalizations(self, task_keys: Iterable[str]) -> bool:
        """Resolve stale formalize failures from durable facts before hiring an agent."""

        progressed = False
        clean_value = self.state.formalize_graph.get("clean", {})
        clean = clean_value if isinstance(clean_value, dict) else {}
        for task_key in dict.fromkeys(task_keys):
            task = self.state.tasks.get(task_key)
            if task is None:
                continue
            if task.stage != Stage.FORMALIZE or task.status != TaskStatus.FAILED:
                continue
            chapter = self._work_units_by_id.get(task.chapter_id)
            if chapter is None:
                continue
            current_digest = await asyncio.to_thread(
                scope_digest, self.config.settings.repo, chapter
            )
            record = clean.get(chapter.id)
            if isinstance(record, dict) and record.get("source_digest") == current_digest:
                task.recovering_failure = True
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.SUCCEEDED,
                    "reconciled from matching published clean build",
                )
                progressed = True
                continue
            latest = task.runs[-1] if task.runs else None
            if latest is None or latest.status != TaskStatus.SUCCEEDED:
                continue
            self.state.load_run_details(latest)
            report = latest.report if isinstance(latest.report, dict) else {}
            source_unchanged = latest.source_digest == current_digest
            if report.get("complete") is not True or not source_unchanged:
                continue
            task.recovering_failure = True
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.PENDING,
                "completed unchanged agent queued for normal coordinator build",
            )
            progressed = True
        return progressed

    def _validate_shepherd_plan(
        self,
        sweep_id: str,
        cases: Iterable[RepairCaseRecord],
        report: dict[str, Any],
    ) -> list[RepairWorkUnitRecord]:
        selected_cases = {case.id: case for case in cases}
        raw_dispositions = report.get("dispositions")
        raw_units = report.get("work_units")
        if not report.get("complete"):
            raise ShepherdPlanError("Shepherd did not complete the repair plan")
        if not isinstance(raw_dispositions, list) or not isinstance(raw_units, list):
            raise ShepherdPlanError("Shepherd plan is missing dispositions or work units")
        if len(raw_units) > self.config.shepherd.maximum_work_units_per_sweep:
            raise ShepherdPlanError("Shepherd plan exceeds the configured work-unit limit")

        dispositions: dict[str, str] = {}
        for value in raw_dispositions:
            if not isinstance(value, dict):
                raise ShepherdPlanError("Shepherd disposition must be an object")
            case_id = value.get("case_id")
            disposition = value.get("disposition")
            if not isinstance(case_id, str) or case_id not in selected_cases:
                raise ShepherdPlanError(f"Shepherd disposition has unknown case id: {case_id}")
            if case_id in dispositions:
                raise ShepherdPlanError(f"Shepherd disposition repeats case id: {case_id}")
            if disposition not in {"repair", "defer", "ignore"}:
                raise ShepherdPlanError(f"Shepherd disposition is invalid for case {case_id}")
            dispositions[case_id] = disposition
        if set(dispositions) != set(selected_cases):
            missing = sorted(set(selected_cases).difference(dispositions))
            raise ShepherdPlanError("Shepherd omitted case dispositions: " + ", ".join(missing))

        selected_chapters = {chapter.id: chapter for chapter in self.work_units}
        by_key: dict[str, dict[str, Any]] = {}
        covered_cases: set[str] = set()
        for value in raw_units:
            if not isinstance(value, dict):
                raise ShepherdPlanError("Shepherd work unit must be an object")
            key = value.get("key")
            if not isinstance(key, str) or not key:
                raise ShepherdPlanError("Shepherd work unit has no local key")
            if key in by_key:
                raise ShepherdPlanError(f"Shepherd work-unit key is repeated: {key}")
            case_ids = value.get("case_ids")
            if not isinstance(case_ids, list) or not case_ids:
                raise ShepherdPlanError(f"Shepherd work unit {key} has no cases")
            if any(case_id not in selected_cases for case_id in case_ids):
                raise ShepherdPlanError(f"Shepherd work unit {key} references an unknown case")
            if any(dispositions[case_id] != "repair" for case_id in case_ids):
                raise ShepherdPlanError(f"Shepherd work unit {key} references a deferred case")
            owner = value.get("owner_chapter_id")
            if not isinstance(owner, str) or owner not in selected_chapters:
                raise ShepherdPlanError(f"Shepherd work unit {key} has an unknown owner")
            try:
                Stage(str(value.get("target_stage")))
            except ValueError as error:
                raise ShepherdPlanError(
                    f"Shepherd work unit {key} has an invalid target stage"
                ) from error
            effort = value.get("effort")
            if effort not in REPAIR_EFFORT:
                raise ShepherdPlanError(f"Shepherd work unit {key} has invalid effort")
            objective = value.get("objective")
            if not isinstance(objective, str) or not objective.strip():
                raise ShepherdPlanError(f"Shepherd work unit {key} has no objective")
            dependencies = value.get("depends_on")
            if not isinstance(dependencies, list) or not all(
                isinstance(item, str) for item in dependencies
            ):
                raise ShepherdPlanError(f"Shepherd work unit {key} has invalid dependencies")
            by_key[key] = value
            covered_cases.update(case_ids)

        required_cases = {
            case_id for case_id, disposition in dispositions.items() if disposition == "repair"
        }
        if covered_cases != required_cases:
            missing = sorted(required_cases.difference(covered_cases))
            extra = sorted(covered_cases.difference(required_cases))
            raise ShepherdPlanError(
                "Shepherd work-unit coverage does not match repair dispositions"
                + (f"; missing: {', '.join(missing)}" if missing else "")
                + (f"; extra: {', '.join(extra)}" if extra else "")
            )
        for key, value in by_key.items():
            dependencies = list(dict.fromkeys(value["depends_on"]))
            if key in dependencies or any(item not in by_key for item in dependencies):
                raise ShepherdPlanError(f"Shepherd work unit {key} has an invalid dependency")
            value["depends_on"] = dependencies

        successors: dict[str, list[str]] = {key: [] for key in by_key}
        for key, value in by_key.items():
            for dependency in value["depends_on"]:
                successors[dependency].append(key)
        visiting: set[str] = set()
        dynamic_ranks: dict[str, float] = {}

        def dynamic_rank(key: str) -> float:
            if key in dynamic_ranks:
                return dynamic_ranks[key]
            if key in visiting:
                raise ShepherdPlanError("Shepherd work-unit dependencies contain a cycle")
            visiting.add(key)
            successor_rank = max(
                (dynamic_rank(successor) for successor in successors[key]), default=0.0
            )
            visiting.remove(key)
            rank = REPAIR_EFFORT[str(by_key[key]["effort"])] + successor_rank
            dynamic_ranks[key] = rank
            return rank

        for key in by_key:
            dynamic_rank(key)

        id_by_key = {key: f"{sweep_id}-{key}" for key in by_key}
        units: list[RepairWorkUnitRecord] = []
        for key, value in by_key.items():
            owner = selected_chapters[str(value["owner_chapter_id"])]
            stage = Stage(str(value["target_stage"]))
            schedule = self.statement_schedule if stage is not Stage.PROVE else self.proof_schedule
            units.append(
                RepairWorkUnitRecord(
                    id=id_by_key[key],
                    sweep_id=sweep_id,
                    case_ids=list(dict.fromkeys(value["case_ids"])),
                    task_keys=[self.state.key(owner.id, stage)],
                    owner_chapter_id=owner.id,
                    target_stage=stage.value,
                    objective=str(value["objective"]),
                    depends_on=[id_by_key[item] for item in value["depends_on"]],
                    effort=str(value["effort"]),
                    priority=schedule.priority(owner.document_id) + dynamic_ranks[key],
                )
            )
        return units

    async def _run_repair_work_unit(self, unit: RepairWorkUnitRecord) -> StageOutcome:
        active_cases = [
            self.state.repair_cases[case_id]
            for case_id in unit.case_ids
            if self.state.tasks[self.state.repair_cases[case_id].task_key].status
            == TaskStatus.FAILED
        ]
        if not active_cases:
            await self.state.finish_repair_work_unit(
                unit.id,
                status=RepairWorkUnitStatus.SUPERSEDED,
                detail="all covered failures were resolved before this repair ran",
            )
            return StageOutcome(ExecutionDisposition.WAITING)
        async with self._repair_slots:
            await self.state.start_repair_work_unit(unit.id)
            chapter = self.config.work_unit(unit.owner_chapter_id)
            stage = Stage(unit.target_stage)
            before_placeholders = (
                await asyncio.to_thread(count_placeholders, self.config.settings.repo, chapter)
                if stage is Stage.PROVE
                else None
            )
            dossier = {
                "repair_work_unit_id": unit.id,
                "objective": unit.objective,
                "target_stage": unit.target_stage,
                "effort": unit.effort,
                "covered_failures": [self._repair_failure_evidence(case) for case in active_cases],
            }
            try:
                attempt = await self._attempt(
                    chapter,
                    stage,
                    feedback=json.dumps(dossier, indent=2),
                    queue_detail=f"Shepherd repair {unit.id}",
                    role=REPAIR_WORKER_ROLE,
                    request_ids=[unit.id, *(case.id for case in active_cases)],
                    priority_override=unit.priority,
                )
                complete = bool(attempt.agent.report.get("complete"))
                validation = attempt.validation
                accepted = attempt.agent.succeeded and validation.succeeded and complete
                if stage is Stage.DISCOVER:
                    raw_dependencies = attempt.agent.report.get("source_dependencies")
                    dependencies = (
                        tuple(item for item in raw_dependencies if isinstance(item, str))
                        if isinstance(raw_dependencies, list)
                        else ()
                    )
                    known = {chapter.id for chapter in self.work_units}
                    accepted = (
                        accepted
                        and not attempt.agent.changed
                        and isinstance(raw_dependencies, list)
                        and len(dependencies) == len(raw_dependencies)
                        and all(item in known for item in dependencies)
                        and chapter.id not in dependencies
                    )
                    if accepted:
                        await self._persist_source_dependencies(
                            chapter,
                            dependencies,
                            attempt.agent.report,
                        )
                elif stage is Stage.PROVE and before_placeholders is not None:
                    accepted = accepted and (
                        attempt.agent.placeholders == 0
                        or attempt.agent.placeholders < before_placeholders
                    )
                if not accepted:
                    detail = attempt.feedback()
                    await self.state.finish_repair_work_unit(
                        unit.id,
                        status=RepairWorkUnitStatus.FAILED,
                        detail=detail,
                        run_id=attempt.run.id,
                    )
                    return StageOutcome(ExecutionDisposition.FAILED)
                await self._accept_repair_work_unit(unit, active_cases, run_id=attempt.run.id)
                return StageOutcome(ExecutionDisposition.SUCCEEDED)
            except asyncio.CancelledError:
                await self.state.finish_repair_work_unit(
                    unit.id,
                    status=RepairWorkUnitStatus.INTERRUPTED,
                    detail="repair interrupted with the orchestrator",
                )
                raise
            except BaseException as error:
                await self.state.finish_repair_work_unit(
                    unit.id,
                    status=RepairWorkUnitStatus.FAILED,
                    detail=str(error) or type(error).__name__,
                )
                return StageOutcome(ExecutionDisposition.FAILED)

    async def _accept_repair_work_unit(
        self,
        unit: RepairWorkUnitRecord,
        active_cases: Iterable[RepairCaseRecord],
        *,
        run_id: str | None = None,
    ) -> None:
        async with self.state.batch():
            for case in active_cases:
                task = self.state.tasks[case.task_key]
                if task.status == TaskStatus.FAILED:
                    await self.state.set_task(
                        task.chapter_id,
                        Stage(task.stage),
                        TaskStatus.PENDING,
                        f"Shepherd repair {unit.id} accepted; stage retry required",
                    )
            await self.state.finish_repair_work_unit(
                unit.id,
                status=RepairWorkUnitStatus.SUCCEEDED,
                detail="repair integrated; normal stage validation queued",
                run_id=unit.run_id if run_id is None else run_id,
            )
        self._repair_progress_generation += 1

    async def _execute_repair_plan(self, units: Iterable[RepairWorkUnitRecord]) -> bool:
        pending = {unit.id: unit for unit in units}
        progressed = False
        while pending:
            terminal_failures = {
                unit_id
                for unit_id, unit in self.state.repair_work_units.items()
                if unit.status
                in {
                    RepairWorkUnitStatus.FAILED,
                    RepairWorkUnitStatus.SUPERSEDED,
                }
            }
            blocked = [
                unit
                for unit in pending.values()
                if set(unit.depends_on).intersection(terminal_failures)
            ]
            for unit in blocked:
                await self.state.finish_repair_work_unit(
                    unit.id,
                    status=RepairWorkUnitStatus.SUPERSEDED,
                    detail="repair dependency did not succeed",
                )
                pending.pop(unit.id)
            succeeded = {
                unit_id
                for unit_id, unit in self.state.repair_work_units.items()
                if self._repair_unit_validated(unit)
            }
            ready = [unit for unit in pending.values() if set(unit.depends_on).issubset(succeeded)]
            if not ready:
                break
            async with self.state.batch():
                for unit in ready:
                    await self.state.queue_repair_work_unit(unit.id)
            results = await _gather_cancel_on_error(
                self._run_repair_work_unit(unit) for unit in ready
            )
            progressed = progressed or any(results)
            for unit in ready:
                pending.pop(unit.id)
        return progressed

    def _repair_unit_validated(self, unit: RepairWorkUnitRecord) -> bool:
        """A repaired dependency is ready only after the normal stage scheduler accepts it."""

        return unit.status == RepairWorkUnitStatus.SUCCEEDED and all(
            task_key in self.state.tasks
            and self.state.tasks[task_key].status == TaskStatus.SUCCEEDED
            for task_key in unit.task_keys
        )

    async def _continue_repair_dags(self) -> bool:
        """Continue a plan created by this orchestrator while its dependencies settle."""

        retryable = {
            RepairWorkUnitStatus.PENDING,
            RepairWorkUnitStatus.INTERRUPTED,
        }
        repairable_keys = {key for key, _task in self.state.shepherd_repairable_tasks()}
        candidates: list[RepairWorkUnitRecord] = []
        pruned = False
        for unit in self.state.repair_work_units.values():
            if unit.status not in retryable:
                continue
            eligible_case_ids = [
                case_id
                for case_id in unit.case_ids
                if (case := self.state.repair_cases.get(case_id)) is not None
                and case.task_key in repairable_keys
            ]
            if not eligible_case_ids:
                await self.state.finish_repair_work_unit(
                    unit.id,
                    status=RepairWorkUnitStatus.SUPERSEDED,
                    detail="covered failures are causal blockers, not repair targets",
                )
                continue
            if eligible_case_ids != unit.case_ids:
                unit.case_ids = eligible_case_ids
                pruned = True
            unchanged = True
            for case_id in unit.case_ids:
                case = self.state.repair_cases.get(case_id)
                if case is None or case.task_key not in self.state.tasks:
                    unchanged = False
                    break
                if self.state.ensure_repair_case(case.task_key).id != case_id:
                    unchanged = False
                    break
            if unchanged:
                candidates.append(unit)
            else:
                await self.state.finish_repair_work_unit(
                    unit.id,
                    status=RepairWorkUnitStatus.SUPERSEDED,
                    detail="repair evidence fingerprint changed",
                )
        if pruned:
            await self.state.save("repair_work_units")
        if not candidates:
            return False
        candidate_ids = {unit.id for unit in candidates}
        succeeded_ids = {
            unit.id
            for unit in self.state.repair_work_units.values()
            if self._repair_unit_validated(unit)
        }
        candidates = [
            unit
            for unit in candidates
            if set(unit.depends_on).issubset(candidate_ids | succeeded_ids)
        ]
        if not candidates:
            return False
        async with self._shepherd_lock:
            progressed = await self._execute_repair_plan(candidates)
            for sweep_id in {unit.sweep_id for unit in candidates}:
                if sweep_id in self.state.repair_sweeps:
                    await self.state.finish_repair_sweep(sweep_id)
            return progressed

    async def _discard_persisted_repair_plans(self) -> list[RepairCaseRecord]:
        """Discard unfinished plans from an earlier orchestrator and reopen current cases."""

        retryable = {
            RepairWorkUnitStatus.PENDING,
            RepairWorkUnitStatus.INTERRUPTED,
        }
        stale_sweep_ids = {
            sweep.id
            for sweep in self.state.repair_sweeps.values()
            if sweep.status in {"planning", "repairing", "waiting"}
            or "interrupted with the orchestrator" in sweep.error
        }
        stale_sweep_ids.update(
            unit.sweep_id
            for unit in self.state.repair_work_units.values()
            if unit.status in retryable
        )
        if not stale_sweep_ids:
            return []

        stale_case_ids = {
            case_id
            for sweep_id in stale_sweep_ids
            if (sweep := self.state.repair_sweeps.get(sweep_id)) is not None
            for case_id in sweep.case_ids
        }
        stale_unit_ids = {
            unit_id
            for unit_id, unit in self.state.repair_work_units.items()
            if unit.sweep_id in stale_sweep_ids
        }
        stale_case_ids.update(
            case_id
            for unit_id in stale_unit_ids
            for case_id in self.state.repair_work_units[unit_id].case_ids
        )
        for unit_id in stale_unit_ids:
            self.state.repair_work_units.pop(unit_id, None)
        for sweep_id in stale_sweep_ids:
            self.state.repair_sweeps.pop(sweep_id, None)

        repairable_keys = {key for key, _task in self.state.shepherd_repairable_tasks()}
        reopened: dict[str, RepairCaseRecord] = {}
        now = datetime.now(UTC).isoformat()
        for case in tuple(self.state.repair_cases.values()):
            if case.id not in stale_case_ids and not set(case.work_unit_ids).intersection(
                stale_unit_ids
            ):
                continue
            case.work_unit_ids = [
                unit_id for unit_id in case.work_unit_ids if unit_id not in stale_unit_ids
            ]
            if case.sweep_id in stale_sweep_ids:
                case.sweep_id = ""
            if case.task_key in repairable_keys:
                current = self.state.ensure_repair_case(case.task_key)
                current.status = RepairCaseStatus.OPEN
                current.sweep_id = ""
                current.work_unit_ids = [
                    unit_id for unit_id in current.work_unit_ids if unit_id not in stale_unit_ids
                ]
                current.updated_at = now
                reopened[current.task_key] = current
                if current.id != case.id:
                    case.status = RepairCaseStatus.EXHAUSTED
            elif (task := self.state.tasks.get(case.task_key)) is not None and task.status != (
                TaskStatus.FAILED
            ):
                case.status = RepairCaseStatus.RESOLVED
            else:
                case.status = RepairCaseStatus.EXHAUSTED
            case.updated_at = now

        self.state.shepherd.current_sweep_id = ""
        self.state.shepherd.current_run_id = ""
        self.state.shepherd.planned_units = 0
        self.state.shepherd.running_units = 0
        await self.state.save(
            "state",
            "repair_cases",
            "repair_sweeps",
            "repair_work_units",
        )
        return sorted(reopened.values(), key=lambda case: (case.updated_at, case.task_key))

    async def _run_shepherd_sweep(
        self,
        *,
        trigger: str,
        cases: Iterable[RepairCaseRecord],
    ) -> bool:
        candidate_cases = list(cases)[: self.config.shepherd.maximum_failures_per_sweep]
        await self._reconcile_stale_formalizations(case.task_key for case in candidate_cases)
        repairable_keys = {key for key, _task in self.state.shepherd_repairable_tasks()}
        selected_cases = [case for case in candidate_cases if case.task_key in repairable_keys]
        if not selected_cases:
            return False
        fingerprints = tuple(sorted(case.fingerprint for case in selected_cases))
        if fingerprints != self._last_shepherd_case_fingerprints:
            self._consecutive_no_progress_sweeps = 0
            self._last_shepherd_case_fingerprints = fingerprints
        async with self._shepherd_lock:
            if (
                self._consecutive_no_progress_sweeps
                >= self.config.shepherd.maximum_consecutive_no_progress_sweeps
            ):
                return False
            sweep = await self.state.start_repair_sweep(
                trigger=trigger,
                task_keys=[case.task_key for case in selected_cases],
            )
            anchor = self.config.work_unit(selected_cases[0].chapter_id)
            run: RunRecord | None = None
            planning_slot = False
            try:
                await self.agent_slots.acquire(1_000_000.0)
                planning_slot = True
                run = await self.state.start_auxiliary_run(
                    anchor.id,
                    Stage.DISCOVER,
                    role=SHEPHERD_ROLE,
                    request_ids=[case.id for case in selected_cases],
                    model=self.config.shepherd.model,
                )
                self.state.shepherd.current_run_id = run.id
                await self.state.save("state")
                result = await self.executor.run_shepherd(
                    anchor,
                    run,
                    (self._repair_failure_evidence(case) for case in selected_cases),
                    scheduling=self.state.scheduling,
                )
                self.agent_slots.release()
                planning_slot = False
                if not result.succeeded:
                    raise ShepherdPlanError(result.error or "Shepherd planning agent failed")
                units = self._validate_shepherd_plan(sweep.id, selected_cases, result.report)
                await self.state.install_repair_plan(
                    sweep.id,
                    units,
                    summary=str(result.report.get("summary", "")),
                    run_id=run.id,
                )
                progressed = await self._execute_repair_plan(units)
                await self.state.finish_repair_sweep(sweep.id)
                if progressed:
                    self._consecutive_no_progress_sweeps = 0
                else:
                    self._consecutive_no_progress_sweeps += 1
                return progressed
            except asyncio.CancelledError:
                await self.state.finish_repair_sweep(
                    sweep.id, error="Shepherd sweep interrupted with the orchestrator"
                )
                raise
            except BaseException as error:
                await self.state.finish_repair_sweep(
                    sweep.id,
                    error=str(error) or type(error).__name__,
                )
                self._consecutive_no_progress_sweeps += 1
                return False
            finally:
                if planning_slot:
                    self.agent_slots.release()

    async def _trigger_threshold_shepherd(self) -> bool:
        if not self.config.shepherd.enabled:
            return False
        cases = self._repair_cases(periodic=False)
        if len(cases) < self.config.shepherd.failure_threshold:
            return False
        return await self._run_shepherd_sweep(trigger="failure-threshold", cases=cases)

    async def _shepherd_loop(self, restart_cases: Iterable[RepairCaseRecord] = ()) -> None:
        changes = self.state.change_bus.subscribe()
        interval = self.config.shepherd.interval_seconds
        due = datetime.now(UTC) + timedelta(seconds=interval)
        try:
            restart_cases = tuple(restart_cases)
            if restart_cases:
                await self._run_shepherd_sweep(trigger="restart", cases=restart_cases)
            while True:
                remaining = max(0.0, (due - datetime.now(UTC)).total_seconds())
                timed_out = False
                change = None
                try:
                    change = await asyncio.wait_for(changes.get(), timeout=remaining)
                except TimeoutError:
                    timed_out = True
                if change is not None and change.stages:
                    await self._continue_repair_dags()
                cases = self._repair_cases(periodic=timed_out)
                pending = len(self.state.shepherd_repairable_tasks())
                if self.state.shepherd.pending_failures != pending:
                    self.state.shepherd.pending_failures = pending
                    await self.state.save("state")
                if not timed_out and len(cases) >= self.config.shepherd.failure_threshold:
                    await self._run_shepherd_sweep(trigger="failure-threshold", cases=cases)
                elif timed_out:
                    if cases:
                        await self._run_shepherd_sweep(trigger="interval", cases=cases)
                    due = datetime.now(UTC) + timedelta(seconds=interval)
                    self.state.shepherd.next_run_at = due.isoformat()
                    await self.state.save("state")
        finally:
            self.state.change_bus.unsubscribe(changes)

    async def _drain_active_shepherd(self) -> None:
        if self._shepherd_lock.locked():
            async with self._shepherd_lock:
                pass

    async def _run_stage_once(self, stage: Stage) -> bool:
        if stage is Stage.DISCOVER:
            return await self._discover_all()
        if stage is Stage.FORMALIZE:
            return await self._discover_and_formalize(discover=True)
        if stage is Stage.REVIEW:
            return await self._review_until_clean()
        return await self._review_tree(prove=True)

    async def run_stage(self, stage: Stage) -> bool:
        generation = self._repair_progress_generation
        result = await self._run_stage_once(stage)
        await self._trigger_threshold_shepherd()
        await self._drain_active_shepherd()
        if self._repair_progress_generation != generation:
            result = await self._run_stage_once(stage)
        return result

    async def _run_pipeline_once(self) -> bool:
        progress = asyncio.Event()
        formalize_task = asyncio.create_task(
            self._discover_and_formalize(progress_event=progress, discover=True)
        )
        handle = RunningFormalizeStage(
            task=formalize_task,
            progress=progress,
            target_ids=frozenset(chapter.id for chapter in self.work_units),
        )
        try:
            reviewed = await self._review_tree(prove=True, formalize=handle)
            formalized = formalize_task.result() if formalize_task.done() else await formalize_task
            return formalized and reviewed
        except BaseException:
            formalize_task.cancel()
            await asyncio.gather(formalize_task, return_exceptions=True)
            raise

    async def run_pipeline(self) -> bool:
        generation = self._repair_progress_generation
        result = await self._run_pipeline_once()
        await self._trigger_threshold_shepherd()
        await self._drain_active_shepherd()
        if self._repair_progress_generation != generation:
            result = await self._run_pipeline_once()
        return result
